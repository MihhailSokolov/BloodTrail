// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
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

	// PollInterval is the poller's rebuild cadence (a later task); Engine
	// itself never reads it, but it lives here so Config is the one place
	// BLOODTRAIL_* settings land.
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

	// overBudget records whether the most recent RebuildNow refused to
	// adopt its freshly loaded snapshot because ApproxBytes() exceeded
	// cfg.MemoryLimit. A future poller task reads this to decide whether/how
	// to keep retrying; Engine itself never reads it back.
	overBudget atomic.Bool
}

// New constructs an Engine bound to pgDriver/pool. It does not load a
// snapshot: TryAllShortestPaths declines every call (reason "no_snapshot")
// until RebuildNow succeeds at least once.
func New(pgDriver *pg.Driver, pool *pgxpool.Pool, cfg Config) *Engine {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Engine{pgDriver: pgDriver, pool: pool, cfg: cfg}
}

// NoteWrite records that the underlying graph changed, invalidating the
// current snapshot for TryAllShortestPaths purposes until the next
// RebuildNow picks up a snapshot stamped with the new generation. Safe for
// concurrent use; intended to be called from the driver's write path.
func (e *Engine) NoteWrite() {
	e.generation.Add(1)
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
	return snap, snap.Generation == e.generation.Load()
}

// rebuildTrigger is logged on every successful rebuild. RebuildNow's
// signature carries no trigger parameter (that is a poller-task concern), so
// this stands in for now: every call this milestone makes is, definitionally,
// a manual one.
const rebuildTrigger = "manual"

// RebuildNow loads a fresh snapshot.Snapshot from PostgreSQL and, if its
// approximate size fits within cfg.MemoryLimit, adopts it atomically as the
// engine's current snapshot.
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
// or otherwise) stays current, a warning is logged, and the refusal is
// remembered for a future poller task to act on. This is not treated as a
// RebuildNow failure -- the load itself succeeded -- so the error return
// stays nil.
func (e *Engine) RebuildNow(ctx context.Context) error {
	start := time.Now()
	generation := e.generation.Load()

	snap, err := LoadSnapshot(ctx, e.pgDriver, e.pool)
	if err != nil {
		return fmt.Errorf("engine: RebuildNow: %w", err)
	}
	snap.Generation = generation

	approxBytes := snap.ApproxBytes()
	if e.cfg.MemoryLimit > 0 && approxBytes > uint64(e.cfg.MemoryLimit) {
		e.overBudget.Store(true)
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot rebuild refused: exceeds memory limit",
			slog.Int("nodes", snap.NodeCount()),
			slog.Int("edges", snap.EdgeCount()),
			slog.Uint64("bytes", approxBytes),
			slog.Uint64("limit", uint64(e.cfg.MemoryLimit)),
		)
		return nil
	}
	e.overBudget.Store(false)

	e.snap.Store(snap)

	e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot rebuilt",
		slog.Int("nodes", snap.NodeCount()),
		slog.Int("edges", snap.EdgeCount()),
		slog.Uint64("bytes", approxBytes),
		slog.Duration("duration", time.Since(start)),
		slog.String("trigger", rebuildTrigger),
	)
	return nil
}

// Decline reasons TryAllShortestPaths logs at Debug under the "reason" attr.
const (
	reasonDisabled     = "disabled"
	reasonNoSnapshot   = "no_snapshot"
	reasonStale        = "stale"
	reasonUnresolvable = "unresolvable"
	reasonTooLarge     = "too_large"
	reasonMemoryLimit  = "memory_limit"
	reasonHydration    = "hydration"
	reasonError        = "error"
)

// TryAllShortestPaths attempts to serve pq entirely from the engine's
// current snapshot, returning (paths, true) on success. It returns
// (nil, false) whenever the engine cannot, or chooses not to, serve the
// query itself, in which case the caller must delegate to PostgreSQL. Every
// decline is logged at Debug with a "reason" attr (see the reason* consts
// above); a successful serve is logged at Info.
//
// Pipeline:
//  1. Fresh() or decline (no_snapshot / stale).
//  2. Resolve pq.Start/pq.End into traverse.Endpoint values.
//  3. Build the edge KindMask from pq.EdgeKinds.
//  4. traverse.AllShortestPaths.
//  5. Hydrate the resulting dense paths into graph.Path values (decline
//     "hydration" on error).
//  6. Re-check freshness: a write that landed while steps 2-5 ran could mean
//     the served result mixes two generations, so a generation change here
//     declines the whole call even though the work already completed.
func (e *Engine) TryAllShortestPaths(ctx context.Context, tx graph.Transaction, pq recognize.PathQuery) (graph.PathSet, bool) {
	start := time.Now()

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

	tq := traverse.Query{
		Roots:       roots,
		Terminals:   terminals,
		Kinds:       buildKindMask(ctx, kindMapper, snap.MaxKindID, pq.EdgeKinds),
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

	if _, stillFresh := e.Fresh(); !stillFresh {
		e.decline(ctx, reasonStale, nil)
		return nil, false
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: path engine served",
		slog.String("mode", modeLabel(pq.Mode)),
		slog.Int("paths", len(hydrated)),
		slog.Duration("duration", time.Since(start)),
	)

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
// The only branch that can fail is Criteria: it runs a live query through
// tx. Every other branch works entirely off snap and never errors --
// including an id or kind that snap doesn't recognize, which correctly
// narrows the endpoint to zero matches (see resolveIDEndpoint /
// resolveKindsEndpoint) rather than failing the call.
func resolveEndpoint(ctx context.Context, tx graph.Transaction, kindMapper pg.KindMapper, snap *snapshot.Snapshot, ep recognize.Endpoint) (traverse.Endpoint, error) {
	switch {
	case len(ep.IDs) > 0:
		return resolveIDEndpoint(snap, ep.IDs), nil
	case ep.Criteria != nil:
		return resolveCriteriaEndpoint(ctx, tx, snap, ep.Criteria)
	case len(ep.Kinds) > 0:
		return resolveKindsEndpoint(ctx, kindMapper, snap, ep.Kinds), nil
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
// multiple kinds narrow the match rather than widen it. A kind the database
// doesn't (yet) know -- kindMapper.MapKind fails for it -- can never match a
// real node, so it collapses the whole intersection to empty immediately,
// the same way PostgreSQL matches zero nodes for a label nothing carries.
func resolveKindsEndpoint(ctx context.Context, kindMapper pg.KindMapper, snap *snapshot.Snapshot, kinds graph.Kinds) traverse.Endpoint {
	bitmaps := make([]*snapshot.Bitset, 0, len(kinds))
	for _, kind := range kinds {
		kindID, err := kindMapper.MapKind(ctx, kind)
		if err != nil {
			return traverse.Endpoint{Bits: snapshot.NewBitset(0)}
		}
		bitmaps = append(bitmaps, snap.NodesOfKind(kindID))
	}

	if len(bitmaps) == 1 {
		return traverse.Endpoint{Bits: bitmaps[0]}
	}
	return traverse.Endpoint{Bits: intersectBitmaps(snap.NodeCount(), bitmaps)}
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
// (SetAll); otherwise only kinds that map to a KindID known to the database
// are set. A kind the database doesn't know yet is silently left unset --
// no real edge can carry it, matching PostgreSQL finding no edges for a kind
// nothing carries -- rather than failing the whole query.
func buildKindMask(ctx context.Context, kindMapper pg.KindMapper, maxKindID snapshot.KindID, edgeKinds graph.Kinds) *snapshot.KindMask {
	mask := snapshot.NewKindMask(maxKindID)

	if len(edgeKinds) == 0 {
		mask.SetAll()
		return mask
	}

	for _, kind := range edgeKinds {
		if kindID, err := kindMapper.MapKind(ctx, kind); err == nil {
			mask.Set(kindID)
		}
	}

	return mask
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
