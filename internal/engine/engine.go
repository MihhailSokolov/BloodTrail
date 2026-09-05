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
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

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
// triggerIdleStale from the poller, or triggerManual for every other
// caller); it is logged verbatim in the "trigger" attr on both the success
// and memory-limit-refusal log lines below, so log consumers can tell a
// poller-driven rebuild from a manual one. analysisStamp is stamped onto the
// snapshot's AnalysisStamp field before the memory-limit check (so it is set
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
	// caller intends to bind $parameters, which recognize.FromCypher never
	// produces (its accepted shape rejects any conjunct containing a
	// $parameter), so the engine cannot know it would honor them correctly.
	reasonParams = "params"
	// reasonUnrecognized is TryCypher-only: recognize.FromCypher did not
	// recognize text as one of the accepted shortestPath/allShortestPaths
	// shapes.
	reasonUnrecognized = "unrecognized"
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

// TryCypher attempts to serve text (a Cypher query string, as sent to
// BloodHound's cypher endpoint) entirely from the engine's current snapshot,
// returning (result, true) on success. It returns (nil, false) whenever the
// engine cannot, or chooses not to, serve the query itself, in which case
// the caller must delegate to PostgreSQL.
//
// params must be empty (nil or a zero-length map) to be served: BloodHound's
// cypher endpoint calls ops.FetchByQuery, which always calls
// tx.Query(query, map[string]any{}) -- an empty, non-nil map -- so a
// non-empty params map can only mean a caller bound $parameters the engine
// has no way to honor (recognize.FromCypher's accepted shape rejects any
// $parameter reference outright), and is declined (reason "params") without
// even attempting to recognize text.
//
// text must recognize.FromCypher into a recognize.PathQuery (decline reason
// "unrecognized" otherwise); the recognized query is then served by the same
// servePathQuery pipeline TryAllShortestPaths uses, and the resulting
// graph.PathSet is wrapped in a newPathResult so the caller can drain it
// exactly as it would drain a live database Result (see ops.FetchByQuery,
// the real consumer this is modeled on). A successful serve is logged at
// Info with query="cypher" (TryAllShortestPaths' equivalent log line carries
// no such field, so the two entry points' served events stay distinguishable
// in the log).
func (e *Engine) TryCypher(ctx context.Context, tx graph.Transaction, text string, params map[string]any) (graph.Result, bool) {
	start := time.Now()

	if len(params) > 0 {
		e.decline(ctx, reasonParams, nil)
		return nil, false
	}

	pq, ok := recognize.FromCypher(text)
	if !ok {
		e.decline(ctx, reasonUnrecognized, nil)
		return nil, false
	}

	hydrated, served := e.servePathQuery(ctx, tx, pq)
	if !served {
		return nil, false
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: path engine served",
		slog.String("query", "cypher"),
		slog.String("mode", modeLabel(pq.Mode)),
		slog.Int("paths", len(hydrated)),
		slog.Duration("duration", time.Since(start)),
	)

	return newPathResult(hydrated), true
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
