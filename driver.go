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

// Driver wraps the PostgreSQL driver. Embedding the concrete type promotes every
// method, including the capability methods BloodHound discovers through
// graph.AsDriver (WipeGraph, DeleteNodesByKinds, DeleteRelationshipsByKinds,
// KindMapper, OptimizeStorage). ReadTransaction, WriteTransaction,
// BatchOperation and Close are overridden below to wire the in-memory engine
// into the read/write/lifecycle path.
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
