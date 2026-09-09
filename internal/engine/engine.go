// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/interpret"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/traverse"
)

// Config configures an Engine.
type Config struct {
	// Enabled gates whether TryAllShortestPaths ever attempts to serve a
	// query. false declines every call immediately (reason "disabled").
	Enabled bool

	// MemoryLimit bounds a rebuilt snapshot's approximate resident size
	// (BLOODTRAIL_MEMORY_LIMIT). Zero means unbounded.
	MemoryLimit size.Size

	// Log receives the engine's rebuild/serve/decline events. New defaults
	// this to slog.Default() when nil, so a zero Config is still safe to
	// log with.
	Log *slog.Logger
}

// Engine is BloodTrail's in-memory graph replica: a snapshot.Snapshot loaded
// from PostgreSQL (RebuildNow), kept current by replaying every committed
// write into it as a delta segment (Apply, apply.go), and served for
// TryAllShortestPaths/TryCypher/the builder-serving entry points while the
// engine is in stateServing.
//
// The replica is never knowingly stale: a write either replays into it
// before the writing call returns, or -- when it cannot be replayed --
// trips the engine into stateFallback, where every query goes to PostgreSQL
// until a background rebuild restores a trustworthy snapshot (enterFallback).
//
// The zero value is not usable; construct with New. Every exported method is
// safe for concurrent use. The current View is stored atomically and
// published only under applyMu, so concurrent queries from many request
// goroutines -- including queries concurrent with an Apply or a RebuildNow --
// are expected and safe: each one runs against whichever immutable View it
// loaded.
type Engine struct {
	pgDriver *pg.Driver
	pool     *pgxpool.Pool
	cfg      Config

	snap atomic.Pointer[snapshot.View]

	// state is the serving state (stateServing/stateFallback, apply.go),
	// read by every serving entry point through serveState and written by
	// enterFallback and the fallback recovery goroutine. Its zero value is
	// stateServing: a fresh Engine is willing to serve, and is stopped from
	// actually doing so only by having no snapshot yet.
	state atomic.Int32

	// applyMu serializes Apply calls with each other and with a rebuild's
	// own publish step (adoptRebuiltView), so that two writes can never
	// derive Views from the same base and drop one another's delta, and a
	// rebuild can never overwrite a delta it did not include.
	applyMu sync.Mutex

	// applyEpoch counts Apply calls. A rebuild reads it before it starts
	// loading and compares it again before publishing (adoptRebuiltView):
	// an unchanged value proves no write was applied while the load ran, and
	// therefore that the freshly loaded snapshot cannot be missing one.
	applyEpoch atomic.Uint64

	// fallbackRebuilding is set while EITHER of the engine's two
	// retry-until-adopted rebuild loops -- Start's boot-load goroutine
	// (runBootLoad, boot.go) or the fallback recovery goroutine
	// (runFallbackRebuild, apply.go) -- is running. Both claim it through
	// the same method (claimRebuildLoop, apply.go) before launching, so at
	// most one of the two is ever running regardless of which reaches it
	// first, and a burst of failing writes starts one recovery rebuild
	// rather than one per write.
	fallbackRebuilding atomic.Bool

	// bgCtx/bgCancel scope the engine's own background work (today: the
	// fallback recovery goroutine) to the engine's lifetime rather than to
	// any one caller's request context: the write whose failure tripped the
	// fallback has long returned by the time recovery finishes, and its
	// ctx being cancelled says nothing about whether the replica should
	// recover. Stop cancels bgCtx, which is what makes an in-flight
	// recovery goroutine exit promptly.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	// overBudget records whether the most recent RebuildNow refused to
	// adopt its freshly loaded snapshot because ApproxBytes() exceeded
	// cfg.MemoryLimit. Boot load and the fallback recovery goroutine
	// (boot.go, apply.go) read this after every RebuildNow call, via
	// fallbackRetryDelay, to decide whether to back off their retry.
	overBudget atomic.Bool

	// refusalLastLoggedNano rate-limits RebuildNow's "snapshot rebuild
	// refused" warning (shouldLogRefusal), stored as UnixNano so it can be
	// read/written with a plain atomic rather than a mutex-guarded
	// time.Time; 0 means "never logged", so the very first refusal always
	// logs.
	refusalLastLoggedNano atomic.Int64

	// rebuildAttempts counts every RebuildNow call that actually reached
	// LoadSnapshot, refused or not. Nothing in production reads it; it
	// exists purely for test observability, through RebuildCount, for the
	// write-through tests' central claim that a write is served without any
	// rebuild at all.
	rebuildAttempts atomic.Uint64

	// appliedWatermark is the largest pg watermark counter value any bumped
	// WriteScope's Apply/AdvanceWatermark call has folded in so far (see
	// watermark.go) -- a monotonic max, not a plain overwrite, since
	// concurrent writers' bumped scopes can finish applying out of order
	// (AdvanceWatermark's own doc).
	appliedWatermark atomic.Uint64

	// inflightBumps counts every bumped WriteScope whose write has not yet
	// been resolved by Apply or AdvanceWatermark: incremented the instant
	// BumpWatermark's own UPDATE commits, decremented exactly once per
	// bumped scope by AdvanceWatermark. Zero is the value watermarkConverged
	// requires: nonzero means some write's pg watermark counter has already
	// advanced while that write's own effect (committed or rolled back) is
	// not yet known to be reconciled.
	inflightBumps atomic.Int64

	// dirtyGen, settledDirtyGen and resolvedDirtyGen are the watermark
	// protocol's trust generations (watermark.go). Trust is COMPUTED from
	// them -- WatermarkTrusted, whose doc carries the full soundness
	// argument -- and never stored, so nothing can ever clear a trust flag
	// out from under a write that is still in flight. All three are
	// monotonically increasing, and the invariant
	// resolvedDirtyGen <= settledDirtyGen <= dirtyGen holds at all times.
	//
	//   - dirtyGen counts every genuine watermark failure this engine has
	//     ever seen: a BumpWatermark call that genuinely failed
	//     (NoteWatermarkBumpFailure -- never ErrWatermarkUnavailable, which
	//     means this engine was never tracking a watermark at all), and a
	//     failed watermark-table DDL exec (ensureWatermarkTable). Each such
	//     failure means the pg watermark counter and this engine's own
	//     bookkeeping may have silently diverged -- a write may have landed
	//     in PostgreSQL without ever being counted -- which a snapshot-file
	//     writer must never trust as convergence even if the numbers happen
	//     to line up again by coincidence.
	//   - settledDirtyGen counts those same failures once the write each one
	//     guarded is SETTLED in PostgreSQL: committed (its Apply ran) or
	//     known to have produced nothing (its driver-level call returned an
	//     error, ResolveAbandonedWrite). A DDL failure guards no write at
	//     all, so it settles itself immediately. settledDirtyGen ==
	//     dirtyGen therefore means "every failure ever noted belongs to a
	//     write whose outcome is already final in PostgreSQL".
	//   - resolvedDirtyGen is the largest settledDirtyGen value that was
	//     read BEFORE the load of a snapshot this engine went on to ADOPT
	//     (rebuildOnce/adoptRebuiltView). It is the only one of the three
	//     that is not advanced by a failure at all: it is advanced purely by
	//     evidence -- an adopted snapshot whose load began after those
	//     failures' writes had already settled, and which therefore contains
	//     them.
	dirtyGen         atomic.Uint64
	settledDirtyGen  atomic.Uint64
	resolvedDirtyGen atomic.Uint64

	// mapKind resolves a graph.Kind name to its KindID, as
	// e.pgDriver.KindMapper().MapKind would. The builder-serving path
	// (serve_builder.go: TryNodeCount/TryNodeFetchIDs/TryNodeFetchKinds and
	// Task 7's rel-query siblings) calls this instead of reaching into
	// pgDriver directly, purely so unit tests can fake kind-name resolution
	// without standing up a real KindMapper (which needs a live PostgreSQL
	// connection), adapted to a field since these methods are themselves the
	// entry points under test, with no wrapper to hand a fake resolver into
	// at the call site. New defaults this to the real KindMapper call;
	// servePathQuery's own kind-mapping calls are deliberately left
	// untouched, still going through e.pgDriver.KindMapper() directly.
	mapKind func(ctx context.Context, kind graph.Kind) (int16, error)

	// mapKindNames resolves KindIDs back to their graph.Kind names, as
	// e.kindNamesByID (below) would. Same seam, same motivation, and same
	// New default (e.kindNamesByID) as mapKind above, just for the opposite
	// direction -- needed by TryNodeFetchKinds to render its
	// graph.KindsResult output.
	mapKindNames func(ids []snapshot.KindID) (graph.Kinds, error)
}

// New constructs an Engine bound to pgDriver/pool. It does not load a
// snapshot: every serving entry point declines (reason "no_snapshot") until
// RebuildNow succeeds at least once.
func New(pgDriver *pg.Driver, pool *pgxpool.Pool, cfg Config) *Engine {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	e := &Engine{pgDriver: pgDriver, pool: pool, cfg: cfg}
	e.bgCtx, e.bgCancel = context.WithCancel(context.Background())
	e.mapKind = func(ctx context.Context, kind graph.Kind) (int16, error) {
		return e.pgDriver.KindMapper().MapKind(ctx, kind)
	}
	e.mapKindNames = e.kindNamesByID
	return e
}

// kindNamesByID resolves KindIDs back to their graph.Kind names via the
// driver's KindMapper -- mapKindNames' production default (New, above).
// context.Background() is used deliberately: this has no ctx of its own to
// plumb through from mapKindNames' callers, and a kind lookup that outlives
// whatever request triggered it is fine here, the same way RebuildNow's own
// background boot-load/recovery goroutines outlive any one caller.
func (e *Engine) kindNamesByID(ids []snapshot.KindID) (graph.Kinds, error) {
	return e.pgDriver.KindMapper().MapKindIDs(context.Background(), ids)
}

// ApplyCount returns how many times Apply (apply.go) has been called,
// successfully or not: Apply bumps applyEpoch unconditionally, before any of
// its own early returns can fire, so this is an external, black-box way to
// prove "Apply ran" (or "did not run yet"). It exists purely for test
// observability -- the root package's driver tests assert a mutating
// capability method calls Apply on success and does not on error, and
// write_observer_test.go pins Apply's own call-ordering guarantees (e.g.
// that it runs after, not before, a delegate-issued mid-transaction commit)
// -- neither of which can reach the unexported applyEpoch field directly
// from outside this package.
func (e *Engine) ApplyCount() uint64 {
	return e.applyEpoch.Load()
}

// RebuildCount returns how many times the engine has loaded a snapshot from
// PostgreSQL (every RebuildNow call that reached LoadSnapshot, adopted or
// not). It exists for test observability: the write-through tests' central
// claim is that a write is served from the replica with no rebuild in
// between, which is exactly "this count did not change".
func (e *Engine) RebuildCount() uint64 {
	return e.rebuildAttempts.Load()
}

// Fresh returns the engine's current View and whether the engine is
// currently willing to serve from it: a snapshot has been adopted and the
// engine is in stateServing.
//
// The first return value distinguishes the two reasons a false comes back:
// nil means no snapshot has ever been adopted (no rebuild has succeeded, or
// every attempt so far exceeded MemoryLimit); a non-nil View with false means
// the engine is in fallback, replaying-into-the-replica having failed for
// some write, until the recovery rebuild completes (apply.go).
//
// Kept as a small test-observability wrapper around serveState now that the
// poller -- its last production caller -- is retired: integration tests
// across this package and the root package still use it to assert that
// boot load or fallback recovery actually converged on a serving snapshot.
// Serving paths use serveState directly, which answers the same question
// without this method's "freshness" framing, retired along with the
// generation/marks staleness checks it used to report on.
func (e *Engine) Fresh() (*snapshot.View, bool) {
	return e.serveState()
}

// serveState returns the View a query should run against, and whether the
// engine may serve that query at all: a snapshot must have been adopted, and
// the engine must be in stateServing.
//
// This is the single serving gate every entry point shares. It replaced the
// generation/marks freshness checks retired with write-through: a published
// View already reflects every committed write (Apply publishes before the
// writing call returns), so there is no staleness left for a query to check
// against -- only whether the replica is trustworthy at all right now.
//
// The View is returned even when ok is false, so a caller can tell
// "no snapshot yet" (nil) from "in fallback" (non-nil) when choosing its
// decline reason.
func (e *Engine) serveState() (*snapshot.View, bool) {
	view := e.snap.Load()
	return view, view != nil && e.state.Load() == stateServing
}

// RebuildNow loads a fresh snapshot.Snapshot from PostgreSQL and, if its
// approximate size fits within cfg.MemoryLimit and no write was applied
// while it was loading, adopts it atomically as the engine's current View.
//
// It is the thin, error-only wrapper every caller outside this file uses;
// rebuildOnce carries the whole implementation, plus the "was it actually
// adopted" answer that boot load (Start, boot.go) and the fallback recovery
// goroutine (apply.go) need to decide whether to keep retrying.
func (e *Engine) RebuildNow(ctx context.Context, trigger string) error {
	_, err := e.rebuildOnce(ctx, trigger)
	return err
}

// rebuildOnce is RebuildNow's implementation, additionally reporting whether
// the freshly loaded snapshot was actually adopted as the engine's current
// View. adopted is false, with a nil error, in the two ways a successful load
// can still fail to be published: it exceeded cfg.MemoryLimit, or a write was
// applied while it was loading (see adoptRebuiltView).
//
// trigger names why this call is happening (triggerStartup from Start's
// boot-load goroutine, triggerFallback from the fallback recovery goroutine,
// or triggerManual for every other caller, boot.go); it is logged verbatim
// in the "trigger" attr on the success, refusal, and not-adopted log lines
// below, so log consumers can tell a boot-load rebuild from a recovery or
// manual one.
//
// A snapshot whose ApproxBytes() exceeds a nonzero cfg.MemoryLimit is
// dropped rather than adopted: whatever View was previously current (nil or
// otherwise) stays current, and the refusal is remembered via overBudget for
// the caller (boot load or fallback recovery, via fallbackRetryDelay) to back
// off on. This is not treated as a failure -- the load itself succeeded --
// so the error return stays nil.
//
// The refusal warning itself is rate-limited (shouldLogRefusal): the very
// first refusal always logs, and any later refusal logs again only if at
// least refusalLogInterval has passed since the last one logged --
// regardless of whether the calls in between were a retry against the exact
// same reading or a genuinely new attempt. This caps log volume for a
// sustained over-budget condition without depending on the caller's own
// retry backoff to do it alone.
func (e *Engine) rebuildOnce(ctx context.Context, trigger string) (bool, error) {
	start := time.Now()
	// Read BEFORE the load begins: see adoptRebuiltView for why an unchanged
	// epoch at publish time proves this snapshot cannot be missing an applied
	// write.
	epoch := e.applyEpoch.Load()
	// Also read before the load begins, and deliberately AFTER epoch: this is
	// the watermark trust generation an adoption resolves (adoptRebuiltView,
	// below). Reading it before LoadSnapshot is what gives it its meaning --
	// every watermark failure counted in it belongs to a write whose outcome
	// was already final in PostgreSQL when this value was read, so a load
	// starting after that read sees those writes' committed rows. Reading it
	// after epoch is what keeps a failure that settles DURING this load from
	// being silently skipped: Apply settles a failure before it bumps
	// applyEpoch (apply.go), so a settle this read missed necessarily bumped
	// the epoch after this call read it, and the adoption below is refused
	// rather than wrongly resolving a generation it does not contain. See
	// WatermarkTrusted (watermark.go) for the whole argument.
	settledGen := e.settledDirtyGen.Load()
	// Deferred (not incremented up front) so that by the time a test
	// observes rebuildAttempts advance, this call's logging decision
	// (shouldLogRefusal or the InfoContext below) has already run --
	// otherwise a test polling rebuildAttempts could race ahead of a still
	// in-flight LoadSnapshot and observe the count before the corresponding
	// log line (if any) was actually emitted.
	defer e.rebuildAttempts.Add(1)

	snap, err := LoadSnapshot(ctx, e.pgDriver, e.pool)
	if err != nil {
		return false, fmt.Errorf("engine: RebuildNow: %w", err)
	}

	approxBytes := snap.ApproxBytes()
	if e.cfg.MemoryLimit > 0 && approxBytes > uint64(e.cfg.MemoryLimit) {
		e.overBudget.Store(true)
		if e.shouldLogRefusal() {
			e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot rebuild refused: exceeds memory limit",
				slog.Int("nodes", snap.NodeCount()),
				slog.Int("edges", snap.EdgeCount()),
				slog.Uint64("bytes", approxBytes),
				slog.Uint64("limit", uint64(e.cfg.MemoryLimit)),
				slog.String("trigger", trigger),
			)
		}
		return false, nil
	}
	e.overBudget.Store(false)

	if !e.adoptRebuiltView(ctx, snapshot.NewView(snap), epoch, settledGen) {
		e.cfg.Log.DebugContext(ctx, "bloodtrail: snapshot rebuild not adopted: a write was applied while it loaded",
			slog.String("trigger", trigger),
			slog.Duration("duration", time.Since(start)),
		)
		return false, nil
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot rebuilt",
		slog.Int("nodes", snap.NodeCount()),
		slog.Int("edges", snap.EdgeCount()),
		slog.Uint64("bytes", approxBytes),
		slog.Duration("duration", time.Since(start)),
		slog.String("trigger", trigger),
	)
	return true, nil
}

// adoptRebuiltView publishes view as the engine's current View, but only if
// no Apply has run since epoch was read -- which rebuildOnce reads before it
// starts loading.
//
// This is what keeps a rebuild from silently discarding a written-through
// delta. A rebuild's snapshot reflects PostgreSQL as of the moment its read
// transaction began; a write that commits after that moment is invisible to
// it, while that write's own Apply has already published (or is about to
// publish) a delta segment over the OLD View. Storing the rebuilt snapshot
// unconditionally would drop that segment and serve a replica missing a
// committed write.
//
// The epoch comparison is the proof, and it is conservative in the safe
// direction. Apply bumps applyEpoch before it does anything else, and it can
// only run after its write has committed; so an unchanged epoch means every
// write applied so far had already committed before this load's read
// transaction began, and is therefore included in the loaded snapshot. A
// changed epoch may or may not mean a write is actually missing -- an Apply
// for a write that committed before the load began also changes it -- so the
// rebuild is simply retried, never wrongly published.
//
// Publishing under applyMu is what makes the check meaningful: it serializes
// with Apply's own publish, so no Apply can slip between the comparison and
// the Store.
//
// Adoption is also what ENDS a fallback, whoever triggered the rebuild --
// the recovery goroutine, boot load, or a manual call. The reasoning is the
// same epoch argument: an adopted snapshot holds every write committed before
// its load began, and the epoch check rules out any write applied since, so
// the replica is complete and current again regardless of which write
// originally tripped the fallback. Tying recovery to adoption rather than to
// one goroutine is what keeps "the engine is serving again" a property of
// the data, not of who happened to reload it.
//
// settledGen is the watermark trust generation rebuildOnce read before this
// snapshot's load began (see its own doc for why "before" is load-bearing,
// and WatermarkTrusted's for the full argument). Adoption -- and ONLY
// adoption, never a load that was refused or dropped -- advances
// resolvedDirtyGen to it, which is the sole way a watermark failure is ever
// resolved. The monotonic max is what keeps a slow rebuild that started
// before a faster one from ever regressing it; every write to
// resolvedDirtyGen happens here, under applyMu, so the load/store pair needs
// no CAS loop of its own.
func (e *Engine) adoptRebuiltView(ctx context.Context, view *snapshot.View, epoch uint64, settledGen uint64) bool {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()

	if e.applyEpoch.Load() != epoch {
		return false
	}
	e.snap.Store(view)
	e.resolvedDirtyGen.Store(maxWatermark(e.resolvedDirtyGen.Load(), settledGen))

	if e.state.CompareAndSwap(stateFallback, stateServing) {
		e.cfg.Log.InfoContext(ctx, "bloodtrail: fallback exited")
	}
	return true
}

// refusalLogInterval rate-limits RebuildNow's "snapshot rebuild refused"
// warning (shouldLogRefusal): a sustained over-budget condition should not
// spam the log on every retry attempt.
const refusalLogInterval = 10 * time.Minute

// shouldLogRefusal reports whether RebuildNow's memory-limit refusal warning
// should be logged now: true the very first time it is ever called (the
// zero value of refusalLastLoggedNano means "never logged"), then at most
// once per refusalLogInterval after that, regardless of how many refused
// RebuildNow calls happen in between -- a pure sliding window, with no
// notion of a successful rebuild "resetting" the window.
func (e *Engine) shouldLogRefusal() bool {
	now := time.Now()
	if last := e.refusalLastLoggedNano.Load(); last != 0 && now.Sub(time.Unix(0, last)) < refusalLogInterval {
		return false
	}
	e.refusalLastLoggedNano.Store(now.UnixNano())
	return true
}

// Decline reasons TryAllShortestPaths and TryCypher log at Debug under the
// "reason" attr.
const (
	reasonDisabled   = "disabled"
	reasonNoSnapshot = "no_snapshot"
	// reasonFallback fires whenever the engine is in stateFallback (apply.go):
	// some write could not be replayed into the in-memory replica, so the
	// replica is not known to match PostgreSQL and nothing may be served from
	// it until the recovery rebuild completes. It is the single reason that
	// replaced the retired freshness declines ("stale", from the
	// write-generation check, and "kind_stale", from the kind-scoped marks):
	// with write-through, a published View already reflects every committed
	// write, so a query is never declined for being behind -- only for the
	// replica being untrustworthy as a whole.
	reasonFallback     = "fallback"
	reasonUnresolvable = "unresolvable"
	reasonTooLarge     = "too_large"
	reasonMemoryLimit  = "memory_limit"
	reasonHydration    = "hydration"
	reasonError        = "error"
	// reasonSelfEndpoint fires when the resolved roots and terminals share a
	// node and pq.ExcludeSelf is false: PostgreSQL's own shortest-path query
	// aborts entirely the instant a root that is also a terminal has an
	// outgoing edge (see traverse.SelfEndpointConflict's doc for the underlying
	// SQL guard), so the engine declines outright rather than risk serving
	// an answer PostgreSQL itself cannot produce for the same request.
	reasonSelfEndpoint = "self_endpoint"
	// reasonParams is TryCypher-only: a non-empty params map means the
	// caller intends to bind $parameters, which BloodHound's cypher endpoint
	// (ops.FetchByQuery) never sends on its own -- it always calls
	// tx.Query(query, map[string]any{}) -- so a non-empty map can only mean
	// a caller bound $parameters the interpreter has no way to honor
	// (interpret.Plan's accepted shape rejects any $parameter reference
	// outright, so this is also a cheap, snapshot-free pre-check before
	// parsing is even attempted).
	reasonParams = "params"
	// reasonUnsupported is TryCypher-only: covers both a text that fails to
	// parse at all (frontend.ParseCypher, the same dawgs Cypher frontend and
	// zero-filter *frontend.Context the real pg-backed driver's own
	// compileText uses -- see drivers/pg/compiler.go@v0.8.0 -- returned an
	// error) and a text that parses but interpret.Plan declines (ok=false):
	// both mean exactly the same thing to a caller of TryCypher --
	// "delegate the whole query to PostgreSQL, which will parse/plan it
	// itself" -- and PostgreSQL's own driver re-parses the identical text
	// from scratch, so a parse failure's own error content is deliberately
	// never logged here (only the fact that this reason fired); logging it
	// would just duplicate whatever error the caller's own eventual
	// PostgreSQL round trip already surfaces.
	reasonUnsupported = "unsupported"
	// reasonMultiGraph is TryCypher-only: the interpreter has no notion of
	// which graph a query is scoped to -- unlike servePathQuery, whose
	// resolveEndpoint/traverse machinery only ever walks the one snapshot it
	// was given -- so serving from a database snapshot.LoadSnapshot flagged
	// as holding more than one graph (Snapshot.MultiGraph) risks silently
	// answering across a graph boundary PostgreSQL itself would respect.
	reasonMultiGraph = "multi_graph"
	// reasonTranslateGate is TryCypher-only: translateGateOK (gate.go)
	// reported that dawgs' own PostgreSQL translator would not also accept
	// this query, so serving it from memory could produce a result
	// PostgreSQL itself would never return. See gate.go's package doc for
	// why this check exists at all.
	reasonTranslateGate = "translate_gate"
	// reasonBudget is TryCypher-only: interpret.Execute reported
	// interpret.ErrBudget -- the query's row or work budget (maxCypherRows/
	// maxCypherWork, serve_cypher.go) was exceeded during materialization.
	reasonBudget = "budget"
	// reasonCollation is TryCypher-only: interpret.Execute reported
	// interpret.ErrCollation -- an ORDER BY or comparison whose result
	// depends on PostgreSQL's own collation (string ordering), which this
	// interpreter never attempts to reproduce locally.
	reasonCollation = "collation"
	// reasonPanic is TryCypher-only: safeExecuteCypher or
	// buildCypherRowsResult (serve_cypher.go) recovered a panic that
	// occurred while executing the query or materializing its result -- a
	// fail-safe backstop. This is deliberately never
	// expected to fire in practice (every panic this recovers from would
	// itself be a bug elsewhere in the interpreter/materialization layer),
	// but its existence is what makes "a panic reaching TryCypher's caller
	// after TryCypher has already returned true" impossible regardless of
	// what future such bug might otherwise cause one -- an unrecoverable
	// false serve, unlike every other decline reason here, which merely
	// falls back to PostgreSQL.
	reasonPanic = "panic"
)

// TryAllShortestPaths attempts to serve pq entirely from the engine's
// current snapshot, returning (paths, true) on success. It returns
// (nil, false) whenever the engine cannot, or chooses not to, serve the
// query itself, in which case the caller must delegate to PostgreSQL. Every
// decline is logged at Debug with a "reason" attr (see the reason* consts
// above); a successful serve is logged at Info.
//
// The actual pipeline lives in servePathQuery, shared with TryCypher; see
// its doc for the seven numbered steps.
func (e *Engine) TryAllShortestPaths(ctx context.Context, tx graph.Transaction, pq recognize.PathQuery) (graph.PathSet, bool) {
	start := time.Now()

	hydrated, served := e.servePathQuery(ctx, tx, pq)
	if !served {
		return nil, false
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: path engine served",
		slog.String("mode", modeLabel(pq.Mode)),
		slog.Int("paths", len(hydrated)),
		slog.Duration("duration", time.Since(start)),
	)

	return hydrated, true
}

// cypherServedLogMessage is the exact message TryCypher logs, at Debug --
// the spec'd level for cypher serving, one level quieter than
// servePathQuery's own Info "bloodtrail: path engine served" line (a cypher
// query is expected to run far more often than a shortest-path one, the
// same volume argument serve_builder.go's own Debug-level served line makes)
// -- whenever it serves a query entirely from the interpreter/snapshot, so
// an e2e/observability test can grep for it. It gets its own distinct
// marker text rather than reusing servePathQuery's: unlike that pipeline,
// TryCypher no longer routes through servePathQuery at all, so the two
// entry points' served events need to stay independently distinguishable
// in the log regardless of level.
const cypherServedLogMessage = "bloodtrail: cypher engine served"

// TryCypher attempts to serve text (a Cypher query string, as sent to
// BloodHound's cypher endpoint) entirely from the engine's current snapshot,
// returning (result, true) on success. It returns (nil, false) whenever the
// engine cannot, or chooses not to, serve the query itself, in which case
// the caller must delegate to PostgreSQL -- always correct, since every
// decline below either means the interpreter never claimed to support this
// shape, or means dawgs' own PostgreSQL translator itself would not accept
// it either (translateGateOK), or means only PostgreSQL can settle the
// question authoritatively (an engine in fallback, a collation-dependent
// comparison).
//
// tx is accepted purely to keep this signature identical to
// TryAllShortestPaths' and to wrappedTransaction.Query's one call site
// (transaction.go): unlike the retired recognize.FromCypher/servePathQuery
// pipeline (which used tx for live-PG Criteria endpoint resolution),
// interpret.Plan/Execute never touch a transaction at all -- the
// interpreter's entire read happens against the in-memory snapshot, so tx
// goes completely unused here.
//
// params must be empty (nil or a zero-length map) to be served -- see
// reasonParams' own doc for why a non-empty map can only mean unhonorable
// bound parameters.
//
// Pipeline (each step's failure declines with its own reason -- see the
// reason* consts above -- logged at Debug by e.decline; every false return
// leaves the caller to delegate to PostgreSQL, which is always correct):
//
//  1. cfg.Enabled -- decline reasonDisabled, matching servePathQuery's own
//     identical first check: an operator disabling the engine must disable
//     every serving path, cypher included, not just TryAllShortestPaths.
//
//  2. len(params) > 0 -- decline reasonParams.
//
//  3. text fails to parse (frontend.ParseCypher, the same zero-filter
//     *frontend.Context and dawgs Cypher frontend the real pg-backed
//     driver's own compileText uses) -- decline reasonUnsupported, logging
//     only the reason, never the parse error's own content (see
//     reasonUnsupported's doc).
//
//  4. no snapshot has ever been adopted (serveState returns a nil View) --
//     decline reasonNoSnapshot.
//
//  5. interpret.Plan(rq, snap) not ok -- decline reasonUnsupported.
//
//  6. snap.MultiGraph -- decline reasonMultiGraph.
//
//  7. translateGateOK(ctx, cypher.Copy(rq), snap) false -- decline
//     reasonTranslateGate. A *copy* of rq is handed to the gate, never rq
//     itself: dawgs' own translator's optimizer can mutate the AST it is
//     given in place (translateGateOK's own doc), and interpret.Plan's
//     Query IR (already built from rq at step 5) holds direct pointers into
//     rq's own WHERE expression nodes for interpret.Execute to evaluate at
//     step 8 -- handing the gate rq itself could silently corrupt those
//     nodes out from under Execute before it ever runs. See
//     TestTranslateGateOKCopyLeavesOriginalASTUntouched and
//     TestQueryIRIndependentOfASTCopyMutation (gate_test.go) for the
//     regression tests proving this can never leak into a served result.
//
//  8. the engine is in fallback (serveState's own bool, captured at step 4
//     alongside the View) -- decline reasonFallback. Deliberately checked
//     here, after the gate rather than immediately after step 4, preserving
//     the pipeline order the retired freshness check had: whether to even
//     attempt planning and gating a query does not depend on the serving
//     state, only whether to actually execute it against snap does. There is
//     no re-check after this one: with write-through, snap is not a
//     might-be-behind copy that a concurrent write invalidates mid-query --
//     it is an immutable View that was current when this call captured it,
//     and a write landing during execution publishes a NEW View for the next
//     query rather than making this one wrong.
//
//  9. interpret.Execute, run via safeExecuteCypher (serve_cypher.go) rather
//     than called directly -- its sentinel errors map to specific reasons
//     (see cypherExecReason), any other error declines reasonError, and a
//     recovered PANIC (a fail-safe backstop: this
//     interpreter is not proven panic-free by construction, and a panic
//     reaching a caller after TryCypher has already returned true would be
//     an unrecoverable false serve) declines reasonPanic.
//
//  10. Collect every database edge id any OutEdge/OutPath column in the
//     result references (collectEdgeIDs); if any exist, hydrate their
//     properties (hydrateEdgePropsByID) -- decline reasonHydration on
//     failure. Edge properties are the one thing the replica does not hold,
//     so this round trip reads them straight from PostgreSQL.
//
//  11. Build the served result via buildCypherRowsResult (serve_cypher.go),
//     which -- the same recovered-panic fail-safe as step 9 -- eagerly materializes
//     every row right there, under its own recover, rather than the lazy,
//     one-row-at-a-time materialization a graph.Result normally performs
//     inside Next(): a panic during materialization must be caught HERE,
//     before TryCypher returns true, not later inside some caller's own
//     Next() loop after TryCypher already committed to serving. Declines
//     reasonPanic on failure; rows are already capped at maxCypherRows, so
//     materializing all of them upfront costs nothing eager evaluation
//     wouldn't have cost lazily anyway.
//
// A successful serve logs cypherServedLogMessage at Debug.
func (e *Engine) TryCypher(ctx context.Context, tx graph.Transaction, text string, params map[string]any) (graph.Result, bool) {
	start := time.Now()

	if !e.cfg.Enabled {
		e.decline(ctx, reasonDisabled, nil)
		return nil, false
	}

	if len(params) > 0 {
		e.decline(ctx, reasonParams, nil)
		return nil, false
	}

	rq, err := frontend.ParseCypher(frontend.NewContext(), text)
	if err != nil || rq == nil {
		e.decline(ctx, reasonUnsupported, nil)
		return nil, false
	}

	snap, serving := e.serveState()
	if snap == nil {
		e.decline(ctx, reasonNoSnapshot, nil)
		return nil, false
	}

	q, ok := interpret.Plan(rq, snap)
	if !ok {
		e.decline(ctx, reasonUnsupported, nil)
		return nil, false
	}

	if snap.MultiGraph() {
		e.decline(ctx, reasonMultiGraph, nil)
		return nil, false
	}

	// A fresh copy, never rq itself -- see this method's own step 7 doc.
	if !translateGateOK(ctx, cypher.Copy[*cypher.RegularQuery](rq), snap) {
		e.decline(ctx, reasonTranslateGate, nil)
		return nil, false
	}

	if !serving {
		e.decline(ctx, reasonFallback, nil)
		return nil, false
	}

	rs, err := safeExecuteCypher(&interpret.Env{Snap: snap, Now: time.Now()}, q, interpret.Budgets{MaxRows: maxCypherRows, MaxWork: maxCypherWork})
	if err != nil {
		e.decline(ctx, cypherExecReason(err), err)
		return nil, false
	}

	var edgeProps map[uint64]*graph.Properties
	if edgeIDs := collectEdgeIDs(snap, rs); len(edgeIDs) > 0 {
		// Edge properties are the one part of an edge the replica does not
		// hold, so they are read from PostgreSQL directly. No post-I/O
		// re-check follows: snap is an immutable View that was current when
		// this call captured it, and a write landing during this round trip
		// publishes a new View for the next query rather than invalidating
		// this one -- the write-through model's whole point (apply.go).
		edgeProps, err = hydrateEdgePropsByID(ctx, e.pool, snap.Base().GraphID, edgeIDs)
		if err != nil {
			e.decline(ctx, reasonHydration, err)
			return nil, false
		}
	}

	result, ok := buildCypherRowsResult(snap, rs, projectionValueKinds(q), edgeProps)
	if !ok {
		e.decline(ctx, reasonPanic, nil)
		return nil, false
	}

	e.cfg.Log.DebugContext(ctx, cypherServedLogMessage,
		slog.String("op", "cypher"),
		slog.Int("rows", len(rs.Rows)),
		slog.Duration("duration", time.Since(start)),
	)

	return result, true
}

// servePathQuery is the pipeline TryAllShortestPaths and TryCypher both
// serve a recognized recognize.PathQuery through, returning (paths, true) on
// success and (nil, false) the instant any step declines (each decline
// already logged at Debug by the failing step; callers add nothing further
// on the false path).
//
// Pipeline:
//  1. cfg.Enabled, then serveState() -- decline "disabled" / "no_snapshot" /
//     "fallback".
//  2. Resolve pq.Start/pq.End into traverse.Endpoint values (decline
//     "unresolvable" on error, including a kindMapper.MapKind failure for a
//     Kinds-constrained endpoint -- see resolveKindsEndpoint's doc).
//  3. Unless pq.ExcludeSelf, decline "self_endpoint" if the resolved roots
//     and terminals share a node with an outgoing edge
//     (traverse.SelfEndpointConflict): PostgreSQL's own shortest-path query
//     cannot serve that request either, so the engine defers rather than
//     risk an answer PostgreSQL itself would refuse.
//  4. Build the edge KindMask from pq.EdgeKinds (decline "error" if mapping
//     one of pq.EdgeKinds through kindMapper.MapKind fails).
//  5. traverse.AllShortestPaths (decline "too_large" / "memory_limit" /
//     "error").
//  6. Hydrate the resulting dense paths into graph.Path values (decline
//     "hydration" on error).
//
// There is no post-execution re-check: snap is an immutable View that was
// current the moment step 1 captured it, and every committed write publishes
// a new View of its own (apply.go) rather than invalidating an in-flight
// query's. The retired generation re-check existed because a snapshot could
// silently fall behind PostgreSQL mid-query; with write-through it cannot.
func (e *Engine) servePathQuery(ctx context.Context, tx graph.Transaction, pq recognize.PathQuery) (graph.PathSet, bool) {
	if !e.cfg.Enabled {
		e.decline(ctx, reasonDisabled, nil)
		return nil, false
	}

	snap, serving := e.serveState()
	if !serving {
		reason := reasonFallback
		if snap == nil {
			reason = reasonNoSnapshot
		}
		e.decline(ctx, reason, nil)
		return nil, false
	}

	kindMapper := e.pgDriver.KindMapper()

	roots, err := resolveEndpoint(ctx, tx, kindMapper, snap, pq.Start)
	if err != nil {
		e.decline(ctx, reasonUnresolvable, err)
		return nil, false
	}
	terminals, err := resolveEndpoint(ctx, tx, kindMapper, snap, pq.End)
	if err != nil {
		e.decline(ctx, reasonUnresolvable, err)
		return nil, false
	}

	if !pq.ExcludeSelf && traverse.SelfEndpointConflict(snap, roots, terminals) {
		e.decline(ctx, reasonSelfEndpoint, nil)
		return nil, false
	}

	edgeKinds, err := buildKindMask(ctx, kindMapper, snap.MaxKindID(), pq.EdgeKinds)
	if err != nil {
		e.decline(ctx, reasonError, err)
		return nil, false
	}

	tq := traverse.Query{
		Roots:       roots,
		Terminals:   terminals,
		Kinds:       edgeKinds,
		Mode:        convertMode(pq.Mode),
		ExcludeSelf: pq.ExcludeSelf,
		Limit:       pq.Limit,
		MemoryLimit: uint64(tx.GraphQueryMemoryLimit()),
	}

	dense, err := traverse.AllShortestPaths(snap, tq)
	if err != nil {
		switch {
		case errors.Is(err, traverse.ErrTooLarge):
			e.decline(ctx, reasonTooLarge, nil)
		case errors.Is(err, traverse.ErrMemoryLimit):
			// The in-memory engine's own budget was exceeded. Rather than
			// surface this to the caller as an error, decline so the query
			// falls back to PostgreSQL, which enforces its own graph query
			// memory limit independently.
			e.decline(ctx, reasonMemoryLimit, nil)
		default:
			e.decline(ctx, reasonError, err)
		}
		return nil, false
	}

	hydrated, err := hydratePaths(ctx, e.pool, kindMapper, snap, dense)
	if err != nil {
		e.decline(ctx, reasonHydration, err)
		return nil, false
	}

	return hydrated, true
}

// decline logs one Debug "bloodtrail: path engine declined" event. err is
// optional context (the underlying failure for reasonUnresolvable, reasonHydration,
// and reasonError); every other reason logs with just the reason attr.
func (e *Engine) decline(ctx context.Context, reason string, err error) {
	attrs := make([]any, 0, 2)
	attrs = append(attrs, slog.String("reason", reason))
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	e.cfg.Log.DebugContext(ctx, "bloodtrail: path engine declined", attrs...)
}

// resolveEndpoint translates a recognized recognize.Endpoint into the
// traverse.Endpoint the in-memory engine understands, following the
// priority order recognize.Endpoint documents: explicit IDs first (Criteria
// is ignored if a query somehow carries both, mirroring traverse.Endpoint's
// own IDs-over-Bits precedence), then Criteria, then Kinds, then
// unconstrained.
//
// Two branches can fail: Criteria runs a live query through tx, and Kinds
// maps each label through kindMapper.MapKind, which can itself fail for
// reasons the dawgs pg driver doesn't distinguish from a genuinely unknown
// kind (see resolveKindsEndpoint's doc). The IDs branch never errors --
// including an id that snap doesn't recognize, which correctly narrows the
// endpoint to zero matches (see resolveIDEndpoint) rather than failing the
// call -- nor does the unconstrained default branch.
func resolveEndpoint(ctx context.Context, tx graph.Transaction, kindMapper pg.KindMapper, snap *snapshot.View, ep recognize.Endpoint) (traverse.Endpoint, error) {
	switch {
	case len(ep.IDs) > 0:
		return resolveIDEndpoint(snap, ep.IDs), nil
	case ep.Criteria != nil:
		return resolveCriteriaEndpoint(ctx, tx, snap, ep.Criteria)
	case len(ep.Kinds) > 0:
		return resolveKindsEndpoint(ctx, kindMapper, snap, ep.Kinds)
	default:
		return traverse.Endpoint{}, nil
	}
}

// resolveIDEndpoint maps ep's explicit database ids to their dense NodeIDs,
// deduplicating and sorting ascending (traverse.Endpoint.IDs' documented
// contract). An id absent from snap -- deleted, or never existed -- is
// dropped rather than erroring: it must narrow the endpoint towards zero
// matches, not fall back to the unconstrained "match everything" meaning a
// nil IDs slice carries. So the returned slice is always non-nil, even when
// every id drops out, matching PostgreSQL finding no paths for a node id
// that does not exist.
func resolveIDEndpoint(snap *snapshot.View, ids []graph.ID) traverse.Endpoint {
	seen := make(map[snapshot.NodeID]struct{}, len(ids))
	dense := make([]snapshot.NodeID, 0, len(ids))

	for _, id := range ids {
		if d, ok := snap.Dense(uint64(id)); ok {
			if _, dup := seen[d]; !dup {
				seen[d] = struct{}{}
				dense = append(dense, d)
			}
		}
	}

	sort.Slice(dense, func(i, j int) bool { return dense[i] < dense[j] })

	return traverse.Endpoint{IDs: dense}
}

// resolveCriteriaEndpoint runs criteria through tx -- the same live
// transaction the caller is querying under, so it sees the same data a
// PostgreSQL fallback would -- to collect matching database ids, then
// Dense-maps them exactly like resolveIDEndpoint: unknown ids are dropped,
// and the result is always a non-nil IDs slice, even when criteria matches
// nothing.
//
// ctx is accepted (unused) to keep the same signature shape as
// resolveEndpoint's other branches; tx already carries its own context from
// when the caller's transaction was opened.
func resolveCriteriaEndpoint(_ context.Context, tx graph.Transaction, snap *snapshot.View, criteria graph.Criteria) (traverse.Endpoint, error) {
	var dbIDs []graph.ID

	err := tx.Nodes().Filter(criteria).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
		for id := range cursor.Chan() {
			dbIDs = append(dbIDs, id)
		}
		return cursor.Error()
	})
	if err != nil {
		return traverse.Endpoint{}, fmt.Errorf("engine: resolveEndpoint: filter nodes: %w", err)
	}

	dense := make([]snapshot.NodeID, 0, len(dbIDs))
	for _, id := range dbIDs {
		if d, ok := snap.Dense(uint64(id)); ok {
			dense = append(dense, d)
		}
	}
	sort.Slice(dense, func(i, j int) bool { return dense[i] < dense[j] })

	return traverse.Endpoint{IDs: dense}, nil
}

// resolveKindsEndpoint intersects the NodesOfKind bitmap for every kind in
// kinds: a node pattern like (n:A:B) requires ALL of its labels at once, so
// multiple kinds narrow the match rather than widen it.
//
// kindMapper.MapKind failing for one of kinds declines the whole call
// (distinct from an empty-but-successful result) rather than treating the
// failure as "this label matches zero nodes": dawgs' pg.SchemaManager
// implementation (the only one BloodTrail runs against) returns the same
// plain error, "unable to map kind: <kind>", both when the kind genuinely
// doesn't exist yet and when the re-fetch it tries first fails outright
// (e.g. the database is unreachable) -- there is no sentinel or typed error
// to tell those two cases apart. Silently collapsing every MapKind error to
// "zero matches" (this function's behavior before this doc) would be wrong
// for the latter case: it would confidently report no paths exist when the
// truth is simply unknown. Declining is the safe choice either way -- the
// caller falls back to PostgreSQL, which answers a genuinely unknown label
// with zero matches on its own, so the only cost is the rarer, slower
// fallback path for that case.
func resolveKindsEndpoint(ctx context.Context, kindMapper pg.KindMapper, snap *snapshot.View, kinds graph.Kinds) (traverse.Endpoint, error) {
	bitmaps := make([]*snapshot.Bitset, 0, len(kinds))
	for _, kind := range kinds {
		kindID, err := kindMapper.MapKind(ctx, kind)
		if err != nil {
			return traverse.Endpoint{}, fmt.Errorf("engine: resolveKindsEndpoint: map kind %s: %w", kind, err)
		}
		bitmaps = append(bitmaps, snap.NodesOfKind(kindID))
	}

	if len(bitmaps) == 1 {
		return traverse.Endpoint{Bits: bitmaps[0]}, nil
	}
	return traverse.Endpoint{Bits: intersectBitmaps(snap.NodeCount(), bitmaps)}, nil
}

// intersectBitmaps returns a fresh Bitset (sized for n dense NodeIDs)
// holding the intersection of every bitmap in bitmaps. It iterates whichever
// input bitmap is smallest, so the cost is proportional to the smallest
// candidate set rather than to n.
func intersectBitmaps(n int, bitmaps []*snapshot.Bitset) *snapshot.Bitset {
	smallest := bitmaps[0]
	for _, bm := range bitmaps[1:] {
		if bm.Count() < smallest.Count() {
			smallest = bm
		}
	}

	result := snapshot.NewBitset(n)
	smallest.Iterate(func(id snapshot.NodeID) bool {
		for _, bm := range bitmaps {
			if bm != smallest && !bm.Has(id) {
				return true
			}
		}
		result.Set(id)
		return true
	})

	return result
}

// buildKindMask translates pq.EdgeKinds into the traverse.KindMask
// AllShortestPaths expects: an empty EdgeKinds means "every kind allowed"
// (SetAll); otherwise every kind must map to a KindID known to the database.
//
// A kindMapper.MapKind failure declines the whole call rather than silently
// leaving that kind unset in the mask, for the same reason resolveKindsEndpoint
// declines rather than treating the failure as "matches nothing": dawgs' pg
// driver returns the same error for a genuinely unknown kind and for a
// fetch failure along the way, so leaving the kind unset on any error risked
// quietly serving an incomplete edge-kind filter (fewer kinds allowed than
// the query asked for) instead of falling back to PostgreSQL. See
// resolveKindsEndpoint's doc for the full reasoning.
func buildKindMask(ctx context.Context, kindMapper pg.KindMapper, maxKindID snapshot.KindID, edgeKinds graph.Kinds) (*snapshot.KindMask, error) {
	mask := snapshot.NewKindMask(maxKindID)

	if len(edgeKinds) == 0 {
		mask.SetAll()
		return mask, nil
	}

	for _, kind := range edgeKinds {
		kindID, err := kindMapper.MapKind(ctx, kind)
		if err != nil {
			return nil, fmt.Errorf("engine: buildKindMask: map kind %s: %w", kind, err)
		}
		mask.Set(kindID)
	}

	return mask, nil
}

// convertMode translates recognize.Mode to traverse.Mode. The two types are
// deliberately kept separate (see the recognize package doc, which avoids a
// recognize<->traverse import cycle) but their constants are defined in the
// same order, so this mapping is total and always successful.
func convertMode(m recognize.Mode) traverse.Mode {
	if m == recognize.ModeOne {
		return traverse.ModeOne
	}
	return traverse.ModeAll
}

// modeLabel renders m for the "bloodtrail: path engine served" log line.
func modeLabel(m recognize.Mode) string {
	if m == recognize.ModeOne {
		return "one"
	}
	return "all"
}
