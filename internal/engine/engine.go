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

	// PollInterval is the poller's rebuild cadence: poller.go reads it to
	// pace RebuildNow calls. It lives on Config, rather than on the poller
	// itself, so Config stays the one place BLOODTRAIL_* settings land.
	PollInterval time.Duration

	// MemoryLimit bounds a rebuilt snapshot's approximate resident size
	// (BLOODTRAIL_MEMORY_LIMIT). Zero means unbounded.
	MemoryLimit size.Size

	// Log receives the engine's rebuild/serve/decline events. New defaults
	// this to slog.Default() when nil, so a zero Config is still safe to
	// log with.
	Log *slog.Logger
}

// Engine is BloodTrail's in-memory path-finding engine: a snapshot.Snapshot
// rebuilt from PostgreSQL on demand (RebuildNow), served for
// TryAllShortestPaths calls only while it is fresh enough for correctness
// (Fresh, NoteWrite).
//
// The zero value is not usable; construct with New. Every exported method is
// safe for concurrent use. The current snapshot and the write-generation
// counter are both stored atomically: RebuildNow is expected to run from a
// single poller goroutine at a time, but concurrent TryAllShortestPaths
// calls from many request goroutines -- including calls concurrent with a
// RebuildNow or a NoteWrite -- are expected and safe.
type Engine struct {
	pgDriver *pg.Driver
	pool     *pgxpool.Pool
	cfg      Config

	snap       atomic.Pointer[snapshot.Snapshot]
	generation atomic.Uint64

	// marks is the kind-scoped complement to generation above: which kind
	// names NoteWrite has touched, and at which generation. See marks.go.
	marks marks

	// overBudget records whether the most recent RebuildNow refused to
	// adopt its freshly loaded snapshot because ApproxBytes() exceeded
	// cfg.MemoryLimit. The poller (poller.go) reads this after every
	// RebuildNow call to decide whether to remember the refusal.
	overBudget atomic.Bool

	// refusalLastLoggedNano rate-limits RebuildNow's "snapshot rebuild
	// refused" warning (shouldLogRefusal), stored as UnixNano so it can be
	// read/written with a plain atomic rather than a mutex-guarded
	// time.Time; 0 means "never logged", so the very first refusal always
	// logs.
	refusalLastLoggedNano atomic.Int64

	// rebuildAttempts counts every RebuildNow call that actually reached
	// LoadSnapshot, refused or not. Nothing in production reads it; it
	// exists purely for white-box test observability (poller_integration_
	// test.go) of the poller's retry-suppression fix, which the "refusal
	// warning" log alone cannot distinguish from a repeated LoadSnapshot
	// attempt whose warning happened to be rate-limited (refusalLogInterval).
	rebuildAttempts atomic.Uint64

	// pollStop and pollDone coordinate Start/Stop's poller goroutine
	// lifecycle (poller.go): Stop closes pollStop to signal the goroutine to
	// exit, and waits on pollDone, which the goroutine closes as it returns.
	// Both are nil until Start actually launches the goroutine (Start is a
	// no-op when !cfg.Enabled); pollStopOnce makes Stop idempotent and safe
	// to call even then.
	pollStop     chan struct{}
	pollDone     chan struct{}
	pollStopOnce sync.Once

	// mapKind resolves a graph.Kind name to its KindID, as
	// e.pgDriver.KindMapper().MapKind would. The builder-serving path
	// (serve_builder.go: TryNodeCount/TryNodeFetchIDs/TryNodeFetchKinds and
	// Task 7's rel-query siblings) calls this instead of reaching into
	// pgDriver directly, purely so unit tests can fake kind-name resolution
	// without standing up a real KindMapper (which needs a live PostgreSQL
	// connection) -- the same motivation noteResolved's resolve parameter
	// serves for marks.go, adapted to a field since these methods are
	// themselves the entry points under test, with no wrapper to hand a
	// fake resolver into at the call site. New defaults this to the real
	// KindMapper call; servePathQuery's own kind-mapping calls are
	// deliberately left untouched, still going through
	// e.pgDriver.KindMapper() directly.
	mapKind func(ctx context.Context, kind graph.Kind) (int16, error)

	// mapKindNames resolves KindIDs back to their graph.Kind names, as
	// e.kindNamesByID (marks.go) would. Same seam, same motivation, and same
	// New default (e.kindNamesByID) as mapKind above, just for the opposite
	// direction -- needed by TryNodeFetchKinds to render its
	// graph.KindsResult output.
	mapKindNames func(ids []snapshot.KindID) (graph.Kinds, error)

	// cypherHydrationRaceHook, when non-nil, runs synchronously inside
	// TryCypher immediately after collectEdgeIDs has determined whether
	// hydration is needed at all -- either immediately before the
	// hydrateEdgePropsByID call that performs it (when it is), or, when the
	// result carries no edge/path column at all (nothing to hydrate), at the
	// equivalent point on that pure-snapshot path instead, immediately
	// before step 11's own unconditional recheck. Either way, it always
	// fires exactly once per TryCypher call, at whichever point is that
	// call's own last chance to force a race before its final freshness
	// check. Production code never sets this field -- the zero value is a
	// complete no-op, so every real caller pays nothing for its existence.
	//
	// It exists purely as an integration-test seam (see the root package's
	// staleness_integration_test.go, TestCypherHydrationRecheck and
	// TestCypherStaleRecheckNoHydration), installed via
	// SetCypherHydrationRaceHookForTest, for forcing a write to land
	// deterministically in the exact window step 10/11's snapshotStillCurrent
	// recheck (TryCypher's own doc) exists to catch: a single TryCypher call
	// is one synchronous function with no other externally observable point
	// between Execute completing and the final recheck for a caller outside
	// this package to intervene at, short of a timing-dependent goroutine
	// race against a real concurrent write. The hook body is free to call
	// NoteWrite directly (the minimal way to reproduce a write's effect on
	// the write-generation counter) or drive a real write through the root
	// package's own Driver -- pgxpool hands out independent connections, so
	// a write issued from here would not deadlock against the read
	// transaction TryCypher itself is running under.
	cypherHydrationRaceHook func()
}

// SetCypherHydrationRaceHookForTest installs (or, given nil, clears) fn as
// the engine's cypherHydrationRaceHook -- see that field's own doc for what
// it is and why it exists. Exported so an integration test in another
// package (the root package's staleness_integration_test.go) can install
// it.
//
// Not safe to call concurrently with an in-flight TryCypher call: this
// field is a plain, unsynchronized func value, deliberately not an
// atomic.Pointer, since its only intended caller is test code that arranges
// its own happens-before ordering (set the hook, then issue the one TryCypher
// call it is meant to intercept, all from the same goroutine).
func (e *Engine) SetCypherHydrationRaceHookForTest(fn func()) {
	e.cypherHydrationRaceHook = fn
}

// New constructs an Engine bound to pgDriver/pool. It does not load a
// snapshot: TryAllShortestPaths declines every call (reason "no_snapshot")
// until RebuildNow succeeds at least once.
func New(pgDriver *pg.Driver, pool *pgxpool.Pool, cfg Config) *Engine {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	e := &Engine{pgDriver: pgDriver, pool: pool, cfg: cfg}
	e.mapKind = func(ctx context.Context, kind graph.Kind) (int16, error) {
		return e.pgDriver.KindMapper().MapKind(ctx, kind)
	}
	e.mapKindNames = e.kindNamesByID
	return e
}

// Generation returns the engine's current write-generation counter, the same
// value NoteWrite advances and snapshotStillCurrent compares snapshots
// against. It exists purely for test observability (the root package's
// driver tests assert a mutating capability method bumps this on success and
// leaves it unchanged on error) -- nothing in the engine's own serving path
// needs to read it from outside the package.
func (e *Engine) Generation() uint64 {
	return e.generation.Load()
}

// Fresh returns the engine's current snapshot and whether it is fresh:
// non-nil and stamped with the write-generation counter's current value.
//
// The first return value distinguishes why a stale result is stale: nil
// means no snapshot has ever been adopted (RebuildNow has never succeeded,
// or every attempt so far exceeded MemoryLimit); a non-nil, not-fresh result
// means a snapshot exists but a NoteWrite landed after it was built.
func (e *Engine) Fresh() (*snapshot.Snapshot, bool) {
	snap := e.snap.Load()
	if snap == nil {
		return nil, false
	}
	return snap, e.snapshotStillCurrent(snap)
}

// snapshotStillCurrent reports whether snap's own Generation still matches
// the engine's live write-generation counter.
//
// This is the predicate both Fresh (for whatever snapshot e.snap currently
// holds) and TryAllShortestPaths' step-6 recheck (for the specific snapshot
// pointer captured at step 1 and used for the entire computation) need --
// and they are not interchangeable via a second Fresh() call: Fresh() reads
// e.snap.Load() itself, so after a concurrent RebuildNow adopts a new
// snapshot, a second Fresh() call judges that *different*, newly-adopted
// snapshot instead of the one the caller actually served results from. A
// new snapshot's Generation can coincidentally match the live counter (e.g.
// generation 5->6, then RebuildNow adopts a snapshot stamped 6) even though
// the original snap is now stale -- exactly the "results may mix two eras"
// case the step-6 check exists to catch. Passing the captured snap
// explicitly, rather than re-deriving it, is what makes the check correct.
func (e *Engine) snapshotStillCurrent(snap *snapshot.Snapshot) bool {
	return snap.Generation == e.generation.Load()
}

// RebuildNow loads a fresh snapshot.Snapshot from PostgreSQL and, if its
// approximate size fits within cfg.MemoryLimit, adopts it atomically as the
// engine's current snapshot.
//
// trigger names why this call is happening (triggerStartup, triggerAnalysis,
// triggerIdleStale, or triggerAnalyzing from the poller, or triggerManual
// for every other caller); it is logged verbatim in the "trigger" attr on
// both the success and memory-limit-refusal log lines below, so log
// consumers can tell a poller-driven rebuild from a manual one.
// analysisStamp is stamped onto the snapshot's AnalysisStamp field before
// the memory-limit check (so it is set
// whether or not the snapshot is actually adopted -- irrelevant either way
// for a dropped snapshot, but keeping the assignment unconditional avoids a
// second, easy-to-forget branch); callers with no meaningful reading (every
// caller but the poller) pass the zero time.Time, leaving AnalysisStamp
// zero, same as before this parameter existed.
//
// The new snapshot's Generation is stamped with the write-generation
// counter's value as read at the very start of this call, before
// LoadSnapshot's own read transaction begins: any write that lands
// concurrently with the load is therefore still correctly reflected as
// having invalidated the freshly adopted snapshot (Fresh will report it
// stale), rather than silently missing that write.
//
// A snapshot whose ApproxBytes() exceeds a nonzero cfg.MemoryLimit is
// dropped rather than adopted: whatever snapshot was previously current (nil
// or otherwise) stays current, and the refusal is remembered via overBudget
// for the poller (poller.go) to act on. This is not treated as a RebuildNow
// failure -- the load itself succeeded -- so the error return stays nil.
//
// The refusal warning itself is rate-limited (shouldLogRefusal), the same
// way the poller's own query-error warning is (queryErrorLogInterval): the
// very first refusal always logs, and any later refusal logs again only if
// at least refusalLogInterval has passed since the last one logged --
// regardless of whether the calls in between were the poller retrying the
// exact same reading or genuinely new attempts (e.g. rule (c) retrying
// after a new write lands while a prior refusal is still in effect). This
// caps log volume for a sustained over-budget condition without depending
// on decideRebuild's own retry gating to do it alone.
func (e *Engine) RebuildNow(ctx context.Context, trigger string, analysisStamp time.Time) error {
	start := time.Now()
	generation := e.generation.Load()
	// Deferred (not incremented up front) so that by the time a test
	// observes rebuildAttempts advance, this call's logging decision
	// (shouldLogRefusal or the InfoContext below) has already run --
	// otherwise a test polling rebuildAttempts could race ahead of a still
	// in-flight LoadSnapshot and observe the count before the corresponding
	// log line (if any) was actually emitted.
	defer e.rebuildAttempts.Add(1)

	snap, err := LoadSnapshot(ctx, e.pgDriver, e.pool)
	if err != nil {
		return fmt.Errorf("engine: RebuildNow: %w", err)
	}
	snap.Generation = generation
	snap.AnalysisStamp = analysisStamp

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
		return nil
	}
	e.overBudget.Store(false)

	e.snap.Store(snap)

	e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot rebuilt",
		slog.Int("nodes", snap.NodeCount()),
		slog.Int("edges", snap.EdgeCount()),
		slog.Uint64("bytes", approxBytes),
		slog.Duration("duration", time.Since(start)),
		slog.String("trigger", trigger),
	)
	return nil
}

// refusalLogInterval rate-limits RebuildNow's "snapshot rebuild refused"
// warning (shouldLogRefusal): a sustained over-budget condition should not
// spam the log once per PollInterval. Matches the poller's own
// queryErrorLogInterval (poller.go).
const refusalLogInterval = 10 * time.Minute

// shouldLogRefusal reports whether RebuildNow's memory-limit refusal warning
// should be logged now: true the very first time it is ever called (the
// zero value of refusalLastLoggedNano means "never logged"), then at most
// once per refusalLogInterval after that, regardless of how many refused
// RebuildNow calls happen in between -- a pure sliding window, mirroring
// the poller's own queryErrorLogInterval check, with no notion of a
// successful rebuild "resetting" the window.
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
	reasonDisabled     = "disabled"
	reasonNoSnapshot   = "no_snapshot"
	reasonStale        = "stale"
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
// question authoritatively (a stale snapshot, a collation-dependent
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
//  4. no snapshot has ever been adopted (e.Fresh() returns a nil snapshot)
//     -- decline reasonNoSnapshot.
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
//  8. the snapshot captured at step 4 is no longer fresh (a write landed
//     while steps 5-7 ran) -- decline reasonStale. Deliberately checked
//     here, after the gate rather than immediately after step 4, per the
//     milestone's pinned pipeline order: whether to even attempt planning
//     and gating a query does not depend on freshness, only whether to
//     actually execute it against snap does.
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
//     properties (hydrateEdgePropsByID) and re-check snap is still current
//     (snapshotStillCurrent) -- decline reasonHydration/reasonStale on
//     failure, exactly the same "did a write land while we were doing I/O"
//     recheck servePathQuery's own step 7 performs, applied here only when
//     hydration actually did I/O. cypherHydrationRaceHook (see its own doc)
//     fires immediately before hydrateEdgePropsByID whenever this branch is
//     taken, purely as a test seam for forcing this exact race.
//
//  11. Unconditional final recheck (snapshotStillCurrent(snap) again,
//     regardless of whether step 10 hydrated anything or even ran) --
//     decline reasonStale on failure. interpret.Execute itself does no I/O,
//     but it is not instantaneous either: a write can land, and a newer
//     snapshot be adopted, at any point between step 8's check and here,
//     even along the pure-snapshot path (no edge/path column at all) that
//     never reaches step 10's own recheck. snap stays internally consistent
//     regardless (immutable once built), but serving from a snapshot that
//     is no longer current would silently return data pg's own read at this
//     same moment would no longer produce.
//
//  12. Build the served result via buildCypherRowsResult (serve_cypher.go),
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

	snap, fresh := e.Fresh()
	if snap == nil {
		e.decline(ctx, reasonNoSnapshot, nil)
		return nil, false
	}

	view := snapshot.NewView(snap)

	q, ok := interpret.Plan(rq, view)
	if !ok {
		e.decline(ctx, reasonUnsupported, nil)
		return nil, false
	}

	if snap.MultiGraph {
		e.decline(ctx, reasonMultiGraph, nil)
		return nil, false
	}

	// A fresh copy, never rq itself -- see this method's own step 7 doc.
	if !translateGateOK(ctx, cypher.Copy[*cypher.RegularQuery](rq), view) {
		e.decline(ctx, reasonTranslateGate, nil)
		return nil, false
	}

	if !fresh {
		e.decline(ctx, reasonStale, nil)
		return nil, false
	}

	rs, err := safeExecuteCypher(&interpret.Env{Snap: view, Now: time.Now()}, q, interpret.Budgets{MaxRows: maxCypherRows, MaxWork: maxCypherWork})
	if err != nil {
		e.decline(ctx, cypherExecReason(err), err)
		return nil, false
	}

	var edgeProps map[uint64]*graph.Properties
	if edgeIDs := collectEdgeIDs(view, rs); len(edgeIDs) > 0 {
		if e.cypherHydrationRaceHook != nil {
			e.cypherHydrationRaceHook()
		}
		edgeProps, err = hydrateEdgePropsByID(ctx, e.pool, snap.GraphID, edgeIDs)
		if err != nil {
			e.decline(ctx, reasonHydration, err)
			return nil, false
		}
		if !e.snapshotStillCurrent(snap) {
			e.decline(ctx, reasonStale, nil)
			return nil, false
		}
	} else if e.cypherHydrationRaceHook != nil {
		// A pure-snapshot result (no edge/path column, so nothing to
		// hydrate) never reaches the branch above -- fire the identical
		// test seam here instead, its own last chance to force a race
		// before step 11's unconditional recheck below. See
		// cypherHydrationRaceHook's own doc.
		e.cypherHydrationRaceHook()
	}

	// Unconditional recheck, regardless of whether hydration ran: Execute
	// itself does no I/O, but it is not instantaneous either (an expensive
	// query can spend real wall-clock time against its work budget), so a
	// write can land -- and a newer snapshot be adopted -- at any point
	// between step 8's pre-execution freshness check and here. snap itself
	// stays internally consistent either way (immutable once built), but
	// serving from it once it is no longer current would silently return
	// data pg's own read at this same moment would no longer produce. This
	// is deliberately a SEPARATE check from the hydration branch's own one
	// above (not a replacement for it): that one catches staleness
	// introduced specifically by hydrateEdgePropsByID's own I/O as early as
	// possible; this one is the unconditional backstop for every path,
	// hydration or not.
	if !e.snapshotStillCurrent(snap) {
		e.decline(ctx, reasonStale, nil)
		return nil, false
	}

	result, ok := buildCypherRowsResult(view, rs, projectionValueKinds(q), edgeProps)
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
//  1. cfg.Enabled, then Fresh() -- decline "disabled" / "no_snapshot" /
//     "stale".
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
//  7. Re-check that the exact snapshot captured at step 1 is still current
//     (snapshotStillCurrent(snap), not a fresh Fresh() call): a write that
//     landed while steps 2-6 ran could mean the served result mixes two
//     generations, so a generation change on that specific snapshot declines
//     the whole call even though the work already completed -- even if a
//     concurrent RebuildNow has since adopted a newer snapshot that itself
//     reports fresh.
func (e *Engine) servePathQuery(ctx context.Context, tx graph.Transaction, pq recognize.PathQuery) (graph.PathSet, bool) {
	if !e.cfg.Enabled {
		e.decline(ctx, reasonDisabled, nil)
		return nil, false
	}

	snap, fresh := e.Fresh()
	if !fresh {
		reason := reasonStale
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

	edgeKinds, err := buildKindMask(ctx, kindMapper, snap.MaxKindID, pq.EdgeKinds)
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

	if !e.snapshotStillCurrent(snap) {
		e.decline(ctx, reasonStale, nil)
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
func resolveEndpoint(ctx context.Context, tx graph.Transaction, kindMapper pg.KindMapper, snap *snapshot.Snapshot, ep recognize.Endpoint) (traverse.Endpoint, error) {
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
func resolveIDEndpoint(snap *snapshot.Snapshot, ids []graph.ID) traverse.Endpoint {
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
func resolveCriteriaEndpoint(_ context.Context, tx graph.Transaction, snap *snapshot.Snapshot, criteria graph.Criteria) (traverse.Endpoint, error) {
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
func resolveKindsEndpoint(ctx context.Context, kindMapper pg.KindMapper, snap *snapshot.Snapshot, kinds graph.Kinds) (traverse.Endpoint, error) {
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
