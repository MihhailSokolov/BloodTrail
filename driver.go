// SPDX-License-Identifier: Apache-2.0

// Package bloodtrail is a DAWGS graph database driver for BloodHound CE.
//
// The driver embeds the PostgreSQL driver, which stays the system of record
// for every write. Reads run under ReadTransaction get a chance to be
// served instead from an in-memory path engine (internal/engine), whose
// snapshot Start loads once at boot and every subsequent write replays
// into directly (write-through, engine/apply.go) -- there is no separate
// rebuild cadence to fall behind; whenever the engine cannot -- or, per
// BLOODTRAIL_ENGINE, must not -- serve a query, it falls straight through
// to PostgreSQL, so results are always correct even while the engine's
// snapshot is cold, in fallback, or disabled.
package bloodtrail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// DriverName is the value BloodHound's graph_driver setting selects.
const DriverName = "bloodtrail"

// snapshotSaveTimeout bounds only the shutdown snapshot save's single-row
// PostgreSQL watermark read (saveSnapshotProbe's ReadWatermark round trip,
// internal/engine/persist.go) -- see Close's own doc for why that read runs
// on a context deliberately detached from the shutdown's own cancellation
// in the first place, and for why the fold and file write that follow it
// (snapshot.Fold, snapshot.WriteSnapshotFile) take no context at all and so
// are bounded by neither this constant nor anything else, regardless of how
// large the graph being saved is.
//
// Sized to stay comfortably inside a default container stop grace period
// (`docker stop` and `docker compose restart` both allow 10s before
// SIGKILL) so that, in the one case THIS timeout exists for -- a database
// that has stopped answering that watermark read -- the read gives up on
// its own and lets the rest of the shutdown finish, rather than being
// killed partway through it. It says nothing about whether the unbounded
// fold+write that follow a successful read fit inside that same grace
// period at scale: measured at ~350ms for 167K nodes/955K edges
// (bench/applybench), a figure that does not bound larger graphs, so a
// SIGKILL can still land mid-write for one -- safely for the .btsnap file
// itself (WriteSnapshotFile's temp-file-plus-rename, per Close's own doc),
// but not without leaving a stray temp file behind. That leftover is what
// internal/engine/boot.go's sweepStaleSnapshotTempFiles reaps at the next
// boot, rather than something this timeout attempts to prevent.
const snapshotSaveTimeout = 5 * time.Second

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
// explicit override here none of the five would ever reach engine.Apply().
// OptimizeStorage and AssertSchema/kind assertion are deliberately left
// promoted unmodified: neither one changes graph data the engine's replica
// would have to reflect.
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
		Enabled:        settings.Engine,
		MemoryLimit:    settings.MemoryLimit,
		SnapshotDir:    settings.SnapshotDir,
		CompactEntries: settings.CompactEntries,
		CompactBytes:   settings.CompactBytes,
		Log:            logger,
	})
	// A driver-scoped background context, deliberately not ctx: the boot-load
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
// records, into a single WriteScope shared for the life of this call, both
// which kinds the delegate's calls touched and the change log (ChangeSet)
// naming every key they wrote. On success that scope is handed to
// engine.Apply, which reads those keys back from PostgreSQL and publishes
// the resulting delta into the in-memory replica before this method returns
// -- so the very next query already sees this transaction's writes.
//
// Apply is called only on success, deliberately: a WriteTransaction that
// returns an error rolled back, leaving PostgreSQL exactly as it was, so
// there is no committed effect to replay. (BatchOperation differs -- see its
// own doc -- because a batch's earlier chunks are already durable by the
// time a later one fails.)
//
// The error branch splits on WHERE the error arose, because only one of the
// two sources proves a rollback. A delegate-returned error means the
// embedded driver rolled the transaction back: resolveAbandonedWrite
// (write_observer.go) resolves the eager bump without an Apply -- the
// watermark protocol's own spec amendment (internal/engine/watermark.go's
// BumpWatermark doc) requires the pg counter to advance even for a write
// that rolled back, so that bump has to fold into e.appliedWatermark and
// retire its e.inflightBumps entry regardless, and a bump that FAILED has to
// settle its own trust generation -- a rolled-back transaction has no
// committed effect for a read-back to replay. But an error with a
// SUCCESSFUL delegate arose in the embedded driver's own final Commit, whose
// outcome is ambiguous (the write may be durable); that branch records a
// fallback and Applies instead -- see its in-body comment. observer can be
// nil here only if d.Driver.WriteTransaction's own delegate closure never
// ran at all (never observed in the pinned pg driver, but checked
// defensively).
//
// observer is declared once, outside the delegate closure below, then
// reconstructed fresh inside it on every invocation -- matching
// BatchOperation's "declared outside, built inside" pattern (see its doc).
// This method's final Apply call reads observer.scope's *current* value,
// which observingTransaction.Commit may have already reset to a fresh,
// still-accumulating WriteScope (by a delegate-issued mid-transaction commit)
// by the time txDelegate returns. Reading through observer, rather than a
// separately captured scope variable, is what makes that reset visible here;
// see observingTransaction.Commit's own doc. Unlike BatchOperation, which
// builds the observer once outside the closure (same scope across retries),
// WriteTransaction builds a fresh observer inside the closure per invocation
// (each attempt gets a fresh scope -- deliberately tighter).
//
// Forward-looking guard note, watermark protocol specifically: "a fresh
// scope per invocation" is exactly what would ORPHAN a bump if
// d.Driver.WriteTransaction (the embedded pg driver) ever retried
// txDelegate internally -- e.g. on a serialization failure -- rather than
// only ever invoking it once per call, as the pinned dawgs pg driver is
// believed to today (unverified against its source, the same caveat
// observingTransaction.Commit's own doc already carries for a different
// retry question). A retried attempt #2 would construct its own fresh
// WriteScope (line below), ensureBumped its own eager bump, and overwrite
// the `observer` variable this method reads -- so attempt #1's own bump,
// if it landed before the retry, would never be resolved by the success or
// error branch below at all: its e.inflightBumps entry would never be
// retired, permanently breaking watermarkConverged (watermark.go's own
// doc), and its counter value would never fold into e.appliedWatermark.
//
// A second, independent consequence of the same orphaning: if attempt #1's
// bump instead FAILED (rather than succeeded), its scope's bump-failure mark
// (WriteScope.markWatermarkBumpFailed, changes_scope.go) is orphaned right
// alongside it. That mark is the only thing settleWatermarkFailure/
// ResolveAbandonedWrite could ever consume to retire the dirtyGen generation
// NoteWatermarkBumpFailure already opened for attempt #1 -- but attempt #1's
// own scope is never seen by the success/error branch below either (only
// attempt #2's `observer` is), so nothing ever calls either settling method
// on it. dirtyGen would have advanced with no corresponding settledDirtyGen
// increment ever possible, permanently distrusting the engine (WatermarkTrusted's
// own doc: dirtyGen == resolvedGen can never hold again). Both consequences
// fail CLOSED, not open -- a permanently unresolved inflightBumps entry keeps
// watermarkConverged false forever, and a permanently unsettled generation
// keeps WatermarkTrusted false forever, so a retry of this shape could make
// this engine wrongly distrust a good snapshot, never wrongly trust a bad
// one.
//
// BatchOperation does not share either risk -- its observer/scope is built
// ONCE outside its own delegate closure (see its doc), so ensureBumped's
// own bumped-flag guard correctly skips re-bumping on any retry the
// embedded driver's BatchOperation might perform, and a scope whose mark it
// already set stays that same scope across every retry, so it is still the
// one the eventual success/error branch settles. If the pinned dawgs pg
// driver ever starts retrying WriteTransaction's delegate internally, this
// method would need to stop reconstructing a fresh WriteScope per
// invocation (or otherwise resolve every attempt's own bump AND its own
// bump-failure mark, not just the last attempt's) before either could be
// trusted.
func (d *Driver) WriteTransaction(ctx context.Context, txDelegate graph.TransactionDelegate, options ...graph.TransactionOption) error {
	var (
		observer    *observingTransaction
		delegateErr error
	)
	if err := d.Driver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		observer = &observingTransaction{Transaction: tx, scope: engine.NewWriteScope(), eng: d.engine, ctx: ctx}
		delegateErr = txDelegate(observer)
		return delegateErr
	}, options...); err != nil {
		resolveWriteTransactionFailure(ctx, d.engine, observer, delegateErr, err)
		return err
	}
	d.engine.Apply(ctx, observer.scope)
	return nil
}

// resolveWriteTransactionFailure settles the engine's accounting for a
// Driver.WriteTransaction call whose OUTER call errored, split three ways
// (see WriteTransaction's doc for the full reasoning):
//
//   - observer == nil: the delegate closure never ran (e.g. BEGIN failed) --
//     no scope exists, nothing was bumped, nothing to resolve.
//   - delegateErr != nil: the delegate's own error made the embedded driver
//     roll back -- resolveAbandonedWrite settles the eager bump without an
//     Apply, since a rolled-back transaction left no committed effect.
//   - delegateErr == nil: the error arose AFTER the delegate, in the
//     embedded driver's own final Commit, whose outcome is AMBIGUOUS: the
//     error can come from PostgreSQL's COMMIT itself, and the write may be
//     durable. Assuming rollback would leave the replica permanently missing
//     a committed write while trusting its own watermark accounting. Record
//     a fallback and Apply instead: the rebuild reloads PostgreSQL's actual
//     outcome whichever way the commit went, and Apply resolves the bump
//     through the ordinary path (the same reasoning as
//     observingTransaction.Commit's own failed-commit branch, which covers a
//     DELEGATE-issued commit; this covers the embedded driver's final one,
//     which no wrapper ever sees).
func resolveWriteTransactionFailure(ctx context.Context, eng *engine.Engine, observer *observingTransaction, delegateErr, outerErr error) {
	if observer == nil {
		return
	}
	if delegateErr == nil {
		observer.scope.Changes().RecordFallback(fmt.Sprintf("WriteTransaction: commit outcome ambiguous: %v", outerErr))
		eng.Apply(ctx, observer.scope)
		return
	}
	resolveAbandonedWrite(ctx, eng, observer.scope)
}

// BatchOperation runs batchDelegate against the embedded PostgreSQL driver,
// the same way WriteTransaction does for a graph.Batch instead of a
// graph.Transaction: the delegate's batch is wrapped in an observingBatch
// (write_observer.go), which also gets d.engine directly -- unlike a
// transaction, a batch documents that Commit may be called mid-delegate to
// flush early and keep receiving operations, so observingBatch.Commit applies
// its own accumulated scope at that moment instead of waiting for this
// method's own call below. observer is declared once, outside the delegate
// closure below, so that this method's final Apply call reads observer.scope's
// *current* value -- which observingBatch.Commit may have already reset to a
// fresh, still-accumulating WriteScope by the time batchDelegate returns.
//
// Apply runs whether or not the delegate reported an error, unlike
// WriteTransaction's success-only call, because a batch's chunks are
// durable as they flush, not held back for one final commit the way a
// transaction's writes are. Verified against the pinned dawgs v0.8.0 pg
// driver source rather than assumed: pg.newBatch (drivers/pg/batch.go:59-73)
// builds its transaction wrapper with allocateTransaction=false
// (batch.go:60), so transaction.tx (drivers/pg/transaction.go) stays nil for
// the whole batch; transaction.driver() (transaction.go:75-85) returns the
// raw pooled connection, not a *pgx.Tx, whenever tx is nil, so every
// buffered flush (batch.go's tryFlush and the flushNode*/flushRelationship*
// helpers it calls) executes directly on the connection under PostgreSQL's
// ordinary per-statement autocommit -- there is no single transaction
// wrapping the whole delegate that a later failure could roll back. This is
// also why batch.Commit's own call to transaction.Commit (batch.go:860-866)
// is a no-op: transaction.Commit (transaction.go:300-306) only calls
// tx.Commit when tx is non-nil. (The one exception, batch.largeUpdate
// (batch.go:174-219), used only when a single UpdateNodes call exceeds
// LargeNodeUpdateThreshold = 1,000,000 nodes, opens its own real *pgx.Tx for
// that one call's staging-table COPY+MERGE and commits or rolls it back
// atomically -- but that transaction is scoped to a single flush unit, not
// the whole batch delegate, so it does not contradict "no transaction wraps
// the whole batch": a call that fails there rolls back only its own
// staging-table work, and whatever flushed via ordinary tryFlush calls
// before or after it is durable regardless.)
//
// So whatever chunks flushed before a delegate failure are already durable
// in PostgreSQL, and skipping the apply would leave the replica missing
// them. Applying is safe for the operations that did NOT land, too, and
// this safety does not actually depend on the autocommit-vs-transactional
// question above -- because read-back reads PostgreSQL's own committed
// state per key -- a key whose write never landed simply reads back as it
// already was (or as absent), never as the write that failed.
func (d *Driver) BatchOperation(ctx context.Context, batchDelegate graph.BatchDelegate, options ...graph.BatchOption) error {
	observer := &observingBatch{scope: engine.NewWriteScope(), eng: d.engine, ctx: ctx}
	err := d.Driver.BatchOperation(ctx, func(batch graph.Batch) error {
		observer.Batch = batch
		return batchDelegate(observer)
	}, options...)
	d.engine.Apply(ctx, observer.scope)
	return err
}

// Close stops the engine's background goroutines (boot load, fallback
// recovery), then gives the engine one best-effort chance to persist its
// current View to a snapshot file (engine.SaveSnapshot) while the embedded
// PostgreSQL driver -- and therefore its connection pool -- is still open,
// before finally closing that driver, so no engine goroutine outlives the
// driver.
//
// SaveSnapshot's own error is deliberately ignored here, not just left
// unlogged: it already logs (Warn) any failure itself, and a snapshot file
// is purely an optimization for the NEXT boot (BLOODTRAIL_SNAPSHOT_DIR
// unset, or a save that fails outright, simply means the next boot falls
// back to its own PostgreSQL rebuild, exactly as it always has) -- never a
// reason a graceful shutdown should block or report an error of its own.
//
// # Why the save does not run on ctx
//
// ctx here is, in production, ALWAYS already cancelled. BloodHound's
// bootstrap.Initializer.Launch (upstream cmd/api/src/bootstrap/
// initializer.go) builds a single signal-driven daemon context, defers this
// Close against it, and blocks on it:
//
//	ctx = NewDaemonContext(parentCtx)   // cancelled by SIGTERM/SIGINT
//	defer databaseConnections.Graph.Close(ctx)
//	...
//	<-ctx.Done()                        // the signal arrives -> shutdown
//
// The cancellation is the very thing that releases that wait and so reaches
// this deferred call: on any signal-driven shutdown -- which is every
// ordinary container stop, `docker compose restart` included -- there is no
// path here on which ctx is still live.
//
// Handing that context to SaveSnapshot made the shutdown save impossible
// rather than merely slower. The save's own probe runs a live PostgreSQL
// watermark round trip (internal/engine/persist.go's saveSnapshotProbe ->
// ReadWatermark) to learn the counter to stamp the file with; a cancelled
// context fails that query before it reaches the database, and
// watermarkConverged reports (0, false) for any read error, which fails
// saveSnapshotPreconditionsFor. So every graceful shutdown declined to
// write, logging only a Debug "snapshot file not written" with
// converged=false -- no error, no warning, and BLOODTRAIL_SNAPSHOT_DIR
// silently never producing a file outside of a compaction's own save
// (internal/engine/compact.go). context.WithoutCancel detaches the save
// from that cancellation; it is the shutdown's LAST piece of work, not
// something the shutdown is waiting to abandon.
//
// snapshotSaveTimeout then re-bounds it, so detaching cannot turn a wedged
// database into a shutdown that hangs until the container runtime's own
// SIGKILL. In practice it bounds only that watermark round trip: the fold
// and the file write past it (snapshot.Fold, snapshot.WriteSnapshotFile)
// take no context at all, and WriteSnapshotFile is temp-file-plus-rename,
// so even a SIGKILL landing mid-write leaves a stray .tmp rather than a
// half-written .btsnap a later boot could read.
func (d *Driver) Close(ctx context.Context) error {
	d.engine.Stop()

	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), snapshotSaveTimeout)
	_ = d.engine.SaveSnapshot(saveCtx)
	cancel()

	return d.Driver.Close(ctx)
}

// Run executes query against the embedded PostgreSQL driver and notifies the
// engine of the write once it completes successfully, the same way
// WriteTransaction does. This override exists because embedding has no
// virtual dispatch: pg.Driver.Run calls its own WriteTransaction internally
// (a concrete, same-package call), which would never reach Driver's override
// above and so would never invalidate the engine's snapshot without this.
//
// The scope handed to Apply carries a ChangeSet fallback record: raw Cypher
// run outside a transaction is exactly as opaque to this package's tracking
// as observingTransaction.Query's own mutating-Cypher sniff
// (write_observer.go) already treats a mutating statement inside a
// transaction, so the two are recorded the same way. That fallback record is
// what makes Apply give up on replaying this write narrowly and rebuild the
// replica instead (engine.enterFallback), which is the only sound answer
// for a write nothing in this package can describe.
//
// ensureBumped runs first, before d.Driver.Run even attempts its own pg
// effect -- the watermark protocol's spec amendment (internal/engine/
// watermark.go's BumpWatermark doc) requires every mutating entry point to
// bump eagerly, this one included. On failure, resolveAbandonedWrite resolves
// that bump directly (no read-back to perform -- the call never reached
// PostgreSQL at all) rather than calling the full Apply this method uses on
// success.
func (d *Driver) Run(ctx context.Context, query string, parameters map[string]any) error {
	scope := engine.NewWriteScope()
	ensureBumped(ctx, d.engine, scope)

	if err := d.Driver.Run(ctx, query, parameters); err != nil {
		resolveAbandonedWrite(ctx, d.engine, scope)
		return err
	}
	scope.Changes().RecordFallback("Run: raw Cypher outside a transaction escapes changelog tracking")
	d.engine.Apply(ctx, scope)
	return nil
}

// WipeGraph truncates the graph through the embedded PostgreSQL driver and
// notifies the engine of the write once it completes successfully. Without
// this override -- BloodHound's "clear database" action -- the engine would
// keep serving shortest paths through data PostgreSQL no longer has. See
// Run's doc for why an override is needed at all, for why the scope handed
// to Apply carries a ChangeSet fallback rather than replaying anything
// narrowly, and for why ensureBumped/resolveAbandonedWrite bracket the call the
// same way.
func (d *Driver) WipeGraph(ctx context.Context, retain graph.TransactionDelegate) error {
	scope := engine.NewWriteScope()
	ensureBumped(ctx, d.engine, scope)

	if err := d.Driver.WipeGraph(ctx, retain); err != nil {
		resolveAbandonedWrite(ctx, d.engine, scope)
		return err
	}
	scope.Changes().RecordFallback("WipeGraph: full graph truncation escapes changelog tracking")
	d.engine.Apply(ctx, scope)
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
// built against whichever graph was default *before* this call,
// potentially indefinitely.
//
// A ChangeSet fallback is recorded, exactly like Run/WipeGraph (so Apply
// rebuilds rather than guesses): retargeting the default graph changes
// which nodes, edges, and kinds "the graph" even refers to, which is
// outside anything this package's write tracking (write_observer.go's
// WriteScope/ChangeSet) reasons about -- the same "outside what tracking
// can reason about" call observingTransaction.WithGraph and
// observingBatch.WithGraph (write_observer.go) already make for a
// mid-transaction graph retarget. ensureBumped/resolveAbandonedWrite bracket the
// call exactly as Run's doc describes.
func (d *Driver) SetDefaultGraph(ctx context.Context, graphSchema graph.Graph) error {
	scope := engine.NewWriteScope()
	ensureBumped(ctx, d.engine, scope)

	if err := d.Driver.SetDefaultGraph(ctx, graphSchema); err != nil {
		resolveAbandonedWrite(ctx, d.engine, scope)
		return err
	}
	scope.Changes().RecordFallback("SetDefaultGraph: default graph retarget escapes changelog tracking")
	d.engine.Apply(ctx, scope)
	return nil
}

// DeleteNodesByKinds deletes nodes through the embedded PostgreSQL driver
// (a raw pooled connection, not a WriteTransaction/BatchOperation call) and
// notifies the engine of the write once it completes successfully via a
// ChangeSet kind-scoped delete criteria, replayed by the applier
// (apply.go's applyNodeKindCriteria) the same way it replays
// includeAny/excludeAny against the in-memory replica -- including the
// cascade to every edge incident to a deleted node, which the applier's own
// tombstoneNodeWithCascade derives directly from the View rather than
// needing this call to report anything about edges at all. See Run's doc
// for why an override is needed at all, and for why ensureBumped/
// resolveAbandonedWrite bracket the call the same way.
func (d *Driver) DeleteNodesByKinds(ctx context.Context, includeAny graph.Kinds, excludeAny graph.Kinds) error {
	scope := engine.NewWriteScope()
	ensureBumped(ctx, d.engine, scope)

	if err := d.Driver.DeleteNodesByKinds(ctx, includeAny, excludeAny); err != nil {
		resolveAbandonedWrite(ctx, d.engine, scope)
		return err
	}
	scope.Changes().RecordDeleteNodesByKinds(includeAny, excludeAny)
	d.engine.Apply(ctx, scope)
	return nil
}

// DeleteRelationshipsByKinds deletes relationships through the embedded
// PostgreSQL driver (a raw pooled connection, not a
// WriteTransaction/BatchOperation call) and notifies the engine of the write
// once it completes successfully via a ChangeSet kind-scoped delete
// criteria: unlike DeleteNodesByKinds, deleting relationships has no
// cascade -- removing an edge never removes a node or any other edge -- so
// kinds fully describes what this call could possibly have touched. See
// Run's doc for why an override is needed at all, and for why ensureBumped/
// resolveAbandonedWrite bracket the call the same way.
func (d *Driver) DeleteRelationshipsByKinds(ctx context.Context, kinds graph.Kinds) error {
	scope := engine.NewWriteScope()
	ensureBumped(ctx, d.engine, scope)

	if err := d.Driver.DeleteRelationshipsByKinds(ctx, kinds); err != nil {
		resolveAbandonedWrite(ctx, d.engine, scope)
		return err
	}
	scope.Changes().RecordDeleteRelationshipsByKinds(kinds)
	d.engine.Apply(ctx, scope)
	return nil
}
