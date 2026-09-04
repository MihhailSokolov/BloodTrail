// SPDX-License-Identifier: Apache-2.0

// Package bloodtrail is a DAWGS graph database driver for BloodHound CE.
//
// The driver embeds the PostgreSQL driver, which stays the system of record
// for every write. Reads run under ReadTransaction get a chance to be
// served instead from an in-memory path engine (internal/engine), rebuilt
// from PostgreSQL on a poller's cadence and invalidated by writes; whenever
// the engine cannot -- or, per BLOODTRAIL_ENGINE, must not -- serve a
// query, it falls straight through to PostgreSQL, so results are always
// correct even while the engine's snapshot is cold, stale, or disabled.
package bloodtrail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// DriverName is the value BloodHound's graph_driver setting selects.
const DriverName = "bloodtrail"

// Version is stamped by the image build (see build/build-image.sh).
var Version = "dev"

// Driver wraps the PostgreSQL driver. Embedding the concrete type promotes
// every method, including the capability methods BloodHound discovers
// through graph.AsDriver (KindMapper, OptimizeStorage, and the four
// overridden below). ReadTransaction, WriteTransaction, BatchOperation and
// Close are overridden to wire the in-memory engine into the
// read/write/lifecycle path; Run, WipeGraph, DeleteNodesByKinds and
// DeleteRelationshipsByKinds are overridden separately because embedding has
// no virtual dispatch -- each of those, called on the embedded *pg.Driver,
// resolves internally to the *pg.Driver's own WriteTransaction (Run,
// WipeGraph) or a raw pooled connection (the two Delete* methods), never to
// this package's own WriteTransaction override, so without an explicit
// override here none of the four would ever reach engine.NoteWrite().
// OptimizeStorage and AssertSchema/kind assertion are deliberately left
// promoted unmodified: neither one changes graph data the engine's snapshot
// could go stale over.
type Driver struct {
	*pg.Driver
	settings Settings
	engine   *engine.Engine
}

// Settings returns the driver's parsed configuration.
func (s *Driver) Settings() Settings {
	return s.settings
}

func init() {
	dawgs.Register(DriverName, Open)
}

// Open is the dawgs.DriverConstructor for BloodTrail. It requires the same
// dawgs.Config BloodHound builds for the PostgreSQL driver, pool included.
func Open(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
	settings, err := SettingsFromEnv(os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("bloodtrail: %w", err)
	}

	if cfg.Pool == nil {
		return nil, errors.New("bloodtrail: a PostgreSQL connection pool is required (dawgs.Config.Pool is nil)")
	}

	backend, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		return nil, fmt.Errorf("bloodtrail: opening PostgreSQL backend: %w", err)
	}

	pgDriver, ok := backend.(*pg.Driver)
	if !ok {
		_ = backend.Close(ctx)
		return nil, fmt.Errorf("bloodtrail: unexpected PostgreSQL driver type %T", backend)
	}

	eng := engine.New(pgDriver, cfg.Pool, engine.Config{
		Enabled:      settings.Engine,
		PollInterval: settings.EnginePollInterval,
		MemoryLimit:  settings.MemoryLimit,
		Log:          slog.Default(),
	})
	// A driver-scoped background context, deliberately not ctx: the poller
	// goroutine Start launches must outlive this Open call and keep running
	// for as long as the driver itself is open, regardless of whether the
	// caller's ctx is later canceled. Close stops it via engine.Stop.
	eng.Start(context.Background())

	slog.InfoContext(ctx, "BloodTrail driver active",
		slog.String("version", Version),
		slog.String("mode", "delegate"),
		slog.String("backend", pg.DriverName),
		slog.Bool("engine", settings.Engine),
	)

	return &Driver{Driver: pgDriver, settings: settings, engine: eng}, nil
}

// ReadTransaction opens a read transaction on the embedded PostgreSQL driver
// and hands txDelegate a wrappedTransaction, giving every read a chance to
// be served from the in-memory engine instead. A fresh wrapper is
// constructed on every invocation of the inner delegate below -- including a
// retried invocation, should the embedded driver ever retry a
// TransactionDelegate -- so wrapper state (declined) never leaks from one
// invocation to another.
func (d *Driver) ReadTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	return d.Driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return txDelegate(&wrappedTransaction{Transaction: tx, engine: d.engine})
	}, options...)
}

// WriteTransaction runs txDelegate against the embedded PostgreSQL driver
// unwrapped -- writes always go straight to PostgreSQL, the system of
// record -- and notifies the engine of the write once the transaction
// commits successfully, invalidating any snapshot the engine is currently
// serving from until its next rebuild picks up the change.
func (d *Driver) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	if err := d.Driver.WriteTransaction(ctx, txDelegate, options...); err != nil {
		return err
	}
	d.engine.NoteWrite()
	return nil
}

// BatchOperation runs batchDelegate against the embedded PostgreSQL driver
// and notifies the engine of the write once the batch completes
// successfully, the same way WriteTransaction does.
func (d *Driver) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	if err := d.Driver.BatchOperation(ctx, batchDelegate, options...); err != nil {
		return err
	}
	d.engine.NoteWrite()
	return nil
}

// Close stops the engine's poller before closing the embedded PostgreSQL
// driver, so no engine goroutine outlives the driver.
func (d *Driver) Close(ctx context.Context) error {
	d.engine.Stop()
	return d.Driver.Close(ctx)
}

// Run executes query against the embedded PostgreSQL driver and notifies the
// engine of the write once it completes successfully, the same way
// WriteTransaction does. This override exists because embedding has no
// virtual dispatch: pg.Driver.Run calls its own WriteTransaction internally
// (a concrete, same-package call), which would never reach Driver's override
// above and so would never invalidate the engine's snapshot without this.
func (d *Driver) Run(ctx context.Context, query string, parameters map[string]any) error {
	if err := d.Driver.Run(ctx, query, parameters); err != nil {
		return err
	}
	d.engine.NoteWrite()
	return nil
}

// WipeGraph truncates the graph through the embedded PostgreSQL driver and
// notifies the engine of the write once it completes successfully. Without
// this override -- BloodHound's "clear database" action -- the engine would
// keep serving shortest paths through data PostgreSQL no longer has, until
// an unrelated write or analysis run happened to advance the write
// generation. See Run's doc for why an override is needed at all.
func (d *Driver) WipeGraph(ctx context.Context, retain graph.TransactionDelegate) error {
	if err := d.Driver.WipeGraph(ctx, retain); err != nil {
		return err
	}
	d.engine.NoteWrite()
	return nil
}

// DeleteNodesByKinds deletes nodes through the embedded PostgreSQL driver
// (a raw pooled connection, not a WriteTransaction/BatchOperation call) and
// notifies the engine of the write once it completes successfully. See
// Run's doc for why an override is needed at all.
func (d *Driver) DeleteNodesByKinds(ctx context.Context, includeAny graph.Kinds, excludeAny graph.Kinds) error {
	if err := d.Driver.DeleteNodesByKinds(ctx, includeAny, excludeAny); err != nil {
		return err
	}
	d.engine.NoteWrite()
	return nil
}

// DeleteRelationshipsByKinds deletes relationships through the embedded
// PostgreSQL driver (a raw pooled connection, not a
// WriteTransaction/BatchOperation call) and notifies the engine of the write
// once it completes successfully. See Run's doc for why an override is
// needed at all.
func (d *Driver) DeleteRelationshipsByKinds(ctx context.Context, kinds graph.Kinds) error {
	if err := d.Driver.DeleteRelationshipsByKinds(ctx, kinds); err != nil {
		return err
	}
	d.engine.NoteWrite()
	return nil
}
