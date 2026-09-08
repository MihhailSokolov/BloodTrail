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
// through graph.AsDriver (KindMapper, OptimizeStorage, and the five
// overridden below). ReadTransaction, WriteTransaction, BatchOperation and
// Close are overridden to wire the in-memory engine into the
// read/write/lifecycle path; Run, WipeGraph, SetDefaultGraph,
// DeleteNodesByKinds and DeleteRelationshipsByKinds are overridden
// separately because embedding has no virtual dispatch -- each of those,
// called on the embedded *pg.Driver, resolves internally to the
// *pg.Driver's own WriteTransaction (Run, WipeGraph), ReadTransaction
// (SetDefaultGraph), or a raw pooled connection (the two Delete* methods),
// never to this package's own WriteTransaction override, so without an
// explicit override here none of the five would ever reach
// engine.NoteWrite(). OptimizeStorage and AssertSchema/kind assertion are
// deliberately left promoted unmodified: neither one changes graph data the
// engine's snapshot could go stale over.
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

// debugOverrideHandler lets an explicitly-set BLOODTRAIL_LOG_LEVEL force
// additional log levels through slog.Default()'s existing handler without
// narrowing it: Enabled reports true whenever either the wrapped handler
// would already accept the record, or the record's level meets h.level.
//
// This is an OR, not a replacement, deliberately: it only ever widens
// whatever already configures slog.Default() (in production, BloodHound's
// own bhlog package, gated by its own independent log-level config; in
// tests, installLogCapture in engine_serving_integration_test.go and its
// staleness_integration_test.go counterpart, which set slog.Default() to a
// Debug-level handler directly and must keep working unmodified by this).
// Setting BLOODTRAIL_LOG_LEVEL=debug only ever adds visibility for
// BloodTrail's own Debug-level lines (e.g. "bloodtrail: builder engine
// served"); it can't be used to suppress logging BloodHound's own
// configuration already enables, matching Settings' documented promise not
// to touch BloodHound's configuration.
//
// buildLogger (below) is what makes "unset is a no-op" actually true: Open
// only ever constructs a debugOverrideHandler when settings.LogLevelSet is
// true. Without that gate, settings.LogLevel's zero-adjacent default of
// slog.LevelInfo would itself act as an implicit floor -- silently
// re-widening a deployment that turned its ambient logging down to Warn or
// Error back up to Info for every BloodTrail line. This type's own Enabled
// method has no way to tell "explicitly Info" apart from "defaulted to
// Info", so that distinction has to be enforced by never constructing the
// wrapper at all when it doesn't apply -- see Settings.LogLevelSet's doc.
//
// Handle is left promoted from the embedded Handler: none of the standard
// library handlers (nor bhlog's contextHandler, which BloodHound wraps them
// in) re-check level inside Handle, so a record this Enabled waves through
// is written unconditionally.
type debugOverrideHandler struct {
	slog.Handler
	level slog.Level
}

func (h debugOverrideHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level || h.Handler.Enabled(ctx, level)
}

// WithAttrs and WithGroup re-wrap the derived handler so the override
// survives deriving a child handler (e.g. a future cfg.Log.With(...)):
// without these, the embedded slog.Handler's own WithAttrs/WithGroup would
// return a plain, un-overridden handler, silently dropping
// BLOODTRAIL_LOG_LEVEL's effect on anything logged through the derived
// logger.
func (h debugOverrideHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return debugOverrideHandler{Handler: h.Handler.WithAttrs(attrs), level: h.level}
}

func (h debugOverrideHandler) WithGroup(name string) slog.Handler {
	return debugOverrideHandler{Handler: h.Handler.WithGroup(name), level: h.level}
}

// buildLogger builds the *slog.Logger Open hands to the engine, applying
// debugOverrideHandler over base only when settings.LogLevelSet is true --
// i.e. only when BLOODTRAIL_LOG_LEVEL was explicitly present and valid in
// the environment. When it is false, base is used unwrapped: leaving
// BLOODTRAIL_LOG_LEVEL unset must be a genuine no-op, never an implicit
// "widen to Info" (see debugOverrideHandler's doc and
// Settings.LogLevelSet's).
func buildLogger(settings Settings, base slog.Handler) *slog.Logger {
	if !settings.LogLevelSet {
		return slog.New(base)
	}
	return slog.New(debugOverrideHandler{Handler: base, level: settings.LogLevel})
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

	logger := buildLogger(settings, slog.Default().Handler())

	eng := engine.New(pgDriver, cfg.Pool, engine.Config{
		Enabled:      settings.Engine,
		PollInterval: settings.EnginePollInterval,
		MemoryLimit:  settings.MemoryLimit,
		Log:          logger,
	})
	// A driver-scoped background context, deliberately not ctx: the poller
	// goroutine Start launches must outlive this Open call and keep running
	// for as long as the driver itself is open, regardless of whether the
	// caller's ctx is later canceled. Close stops it via engine.Stop.
	eng.Start(context.Background())

	mode := "delegate"
	if settings.Engine {
		mode = "engine"
	}

	logger.InfoContext(ctx, "BloodTrail driver active",
		slog.String("version", Version),
		slog.String("mode", mode),
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

// WriteTransaction runs txDelegate against the embedded PostgreSQL driver --
// writes always go straight to PostgreSQL, the system of record -- with the
// delegate's tx wrapped in an observingTransaction (write_observer.go) that
// records, into a single WriteScope shared for the life of this call,
// exactly which node and edge kinds the delegate's calls touched. On success
// that scope is handed to engine.NoteWrite, invalidating only the kinds the
// write actually reached instead of the whole snapshot -- a call whose
// scope stays empty (touched nothing this package's tracking recognizes,
// e.g. a transaction that only read) still bumps the engine's generation
// counter (NoteWrite's own doc), so Fresh()'s plain staleness check is
// unaffected by this change.
//
// observer is declared once, outside the delegate closure below, then
// reconstructed fresh inside it on every invocation -- matching
// BatchOperation's "declared outside, built inside" pattern (see its doc).
// This method's final NoteWrite call reads observer.scope's *current* value,
// which observingTransaction.Commit may have already reset to a fresh,
// still-accumulating WriteScope (by a delegate-issued mid-transaction commit)
// by the time txDelegate returns. Reading through observer, rather than a
// separately captured scope variable, is what makes that reset visible here;
// see observingTransaction.Commit's own doc. Unlike BatchOperation, which
// builds the observer once outside the closure (same scope across retries),
// WriteTransaction builds a fresh observer inside the closure per invocation
// (each attempt gets a fresh scope -- deliberately tighter).
func (d *Driver) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	var observer *observingTransaction
	if err := d.Driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		observer = &observingTransaction{Transaction: tx, scope: engine.NewWriteScope(), eng: d.engine}
		return txDelegate(observer)
	}, options...); err != nil {
		return err
	}
	d.engine.NoteWrite(observer.scope)
	return nil
}

// BatchOperation runs batchDelegate against the embedded PostgreSQL driver,
// the same way WriteTransaction does for a graph.Batch instead of a
// graph.Transaction: the delegate's batch is wrapped in an observingBatch
// (write_observer.go), which also gets d.engine directly -- unlike a
// transaction, a batch documents that Commit may be called mid-delegate to
// flush early and keep receiving operations, so observingBatch.Commit calls
// NoteWrite itself at that moment instead of waiting for this method's own
// call below. observer is declared once, outside the delegate closure below,
// so that this method's final NoteWrite call reads observer.scope's *current*
// value -- which observingBatch.Commit may have already reset to a fresh,
// still-accumulating WriteScope by the time batchDelegate returns.
func (d *Driver) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	observer := &observingBatch{scope: engine.NewWriteScope(), eng: d.engine}
	if err := d.Driver.BatchOperation(ctx, func(batch graph.Batch) error {
		observer.Batch = batch
		return batchDelegate(observer)
	}, options...); err != nil {
		return err
	}
	d.engine.NoteWrite(observer.scope)
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
//
// The scope handed to NoteWrite has TouchAll() called on it -- touching
// every node and edge kind, exactly like the nil scope this used to pass --
// plus a ChangeSet fallback record: raw Cypher run outside a transaction is
// exactly as opaque to this package's tracking as observingTransaction.
// Query's own mutating-Cypher sniff (write_observer.go) already treats a
// mutating statement inside a transaction, so the two are recorded the same
// way. See marks_test.go's TestNoteWriteTouchAllScopeMatchesNilScope for the
// verification that a TouchAll scope and a nil scope stamp identical marks.
func (d *Driver) Run(ctx context.Context, query string, parameters map[string]any) error {
	if err := d.Driver.Run(ctx, query, parameters); err != nil {
		return err
	}
	scope := engine.NewWriteScope()
	scope.TouchAll()
	scope.Changes().RecordFallback("Run: raw Cypher outside a transaction escapes changelog tracking")
	d.engine.NoteWrite(scope)
	return nil
}

// WipeGraph truncates the graph through the embedded PostgreSQL driver and
// notifies the engine of the write once it completes successfully. Without
// this override -- BloodHound's "clear database" action -- the engine would
// keep serving shortest paths through data PostgreSQL no longer has, until
// an unrelated write or analysis run happened to advance the write
// generation. See Run's doc for why an override is needed at all, and for
// why the scope handed to NoteWrite is a TouchAll scope carrying a
// ChangeSet fallback rather than a bare nil.
func (d *Driver) WipeGraph(ctx context.Context, retain graph.TransactionDelegate) error {
	if err := d.Driver.WipeGraph(ctx, retain); err != nil {
		return err
	}
	scope := engine.NewWriteScope()
	scope.TouchAll()
	scope.Changes().RecordFallback("WipeGraph: full graph truncation escapes changelog tracking")
	d.engine.NoteWrite(scope)
	return nil
}

// SetDefaultGraph retargets the embedded PostgreSQL driver's default graph
// and notifies the engine of the write once it completes successfully. See
// Run's doc for why an override is needed at all: pg.Driver.SetDefaultGraph
// resolves internally to the *pg.Driver's own ReadTransaction
// (drivers/pg/manager.go's SchemaManager.SetDefaultGraph), a concrete,
// same-package call that never reaches this package's own WriteTransaction
// override, so without this override the engine would keep serving
// snapshots -- and this driver's own mapKind/mapKindNames lookups
// (internal/engine's KindMapper seam) would keep resolving kind names --
// built against whichever graph was default *before* this call, potentially
// indefinitely (nothing else advances the write generation on its own).
//
// A TouchAll scope carrying a ChangeSet fallback is used, exactly like
// Run/WipeGraph: retargeting the default graph changes which nodes, edges,
// and kinds "the graph" even refers to, which is outside anything this
// package's kind-scoped write tracking (engine/marks.go, write_observer.go's
// WriteScope) reasons about -- the same "outside what kind-scoped tracking
// can reason about" call observingTransaction.WithGraph and observingBatch.
// WithGraph (write_observer.go) already make for a mid-transaction graph
// retarget, via their own TouchAll() plus RecordFallback.
func (d *Driver) SetDefaultGraph(ctx context.Context, graphSchema graph.Graph) error {
	if err := d.Driver.SetDefaultGraph(ctx, graphSchema); err != nil {
		return err
	}
	scope := engine.NewWriteScope()
	scope.TouchAll()
	scope.Changes().RecordFallback("SetDefaultGraph: default graph retarget escapes changelog tracking")
	d.engine.NoteWrite(scope)
	return nil
}

// DeleteNodesByKinds deletes nodes through the embedded PostgreSQL driver
// (a raw pooled connection, not a WriteTransaction/BatchOperation call) and
// notifies the engine of the write once it completes successfully, scoped to
// TouchAllNodes and TouchAllEdges rather than a nil (touch-everything)
// scope: this is still conservative on the node side (includeAny/excludeAny
// name which nodes qualify for deletion, but a deleted node's own kinds are
// never narrower than "could be anything" from here, since a node can carry
// several kinds and this call only filters, it doesn't report which ones
// existed) and, on the edge side, deleting a node cascades to delete every
// edge incident to it, which may carry kinds having nothing to do with
// includeAny at all -- so TouchAllEdges, not TouchEdgeKinds(includeAny). See
// Run's doc for why an override is needed at all.
func (d *Driver) DeleteNodesByKinds(ctx context.Context, includeAny graph.Kinds, excludeAny graph.Kinds) error {
	if err := d.Driver.DeleteNodesByKinds(ctx, includeAny, excludeAny); err != nil {
		return err
	}
	scope := engine.NewWriteScope()
	scope.TouchAllNodes()
	scope.TouchAllEdges()
	scope.Changes().RecordDeleteNodesByKinds(includeAny, excludeAny)
	d.engine.NoteWrite(scope)
	return nil
}

// DeleteRelationshipsByKinds deletes relationships through the embedded
// PostgreSQL driver (a raw pooled connection, not a
// WriteTransaction/BatchOperation call) and notifies the engine of the write
// once it completes successfully, scoped to exactly kinds: unlike
// DeleteNodesByKinds, deleting relationships has no cascade -- removing an
// edge never removes a node or any other edge -- so kinds fully describes
// what this call could possibly have touched. See Run's doc for why an
// override is needed at all.
func (d *Driver) DeleteRelationshipsByKinds(ctx context.Context, kinds graph.Kinds) error {
	if err := d.Driver.DeleteRelationshipsByKinds(ctx, kinds); err != nil {
		return err
	}
	scope := engine.NewWriteScope()
	scope.TouchEdgeKinds(kinds)
	scope.Changes().RecordDeleteRelationshipsByKinds(kinds)
	d.engine.NoteWrite(scope)
	return nil
}
