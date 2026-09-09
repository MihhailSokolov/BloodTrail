// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Engine serving states, held in Engine.state.
//
// stateServing is the zero value, so a freshly constructed Engine starts out
// willing to serve -- which is not the same as being able to: every serving
// entry point also requires a non-nil snapshot (serveState), and no snapshot
// exists until the first rebuild adopts one.
//
// stateFallback means the in-memory replica can no longer be trusted to
// match PostgreSQL: some write could not be replayed into it (see
// enterFallback's callers). Every serving path declines while it holds, so
// every query goes to PostgreSQL, and one background goroutine rebuilds the
// snapshot from scratch until it succeeds -- at which point the state
// returns to stateServing.
const (
	stateServing int32 = iota
	stateFallback
)

// triggerFallback labels the RebuildNow calls the fallback recovery
// goroutine makes, alongside boot.go's own triggerStartup and triggerManual,
// so a rebuild driven by a failed apply is distinguishable in the log from a
// boot-load- or test-driven one.
const triggerFallback = "fallback"

// fallbackRetryInterval and fallbackRetryMax bound the fallback recovery
// goroutine's retry cadence (shared with Start's boot-load goroutine,
// boot.go): the first retry after a failed (or not-adopted) rebuild waits
// fallbackRetryInterval, and each further retry doubles the wait up to
// fallbackRetryMax. This mirrors the rate-limiting spirit of RebuildNow's
// own refusal warning (refusalLogInterval) -- a database that stays
// unreachable, or a graph that stays over cfg.MemoryLimit, must not turn
// recovery into a hot loop -- while still recovering promptly from a
// transient failure.
const (
	fallbackRetryInterval = 100 * time.Millisecond
	fallbackRetryMax      = 30 * time.Second
)

// Apply replays one committed write into the in-memory replica, so that the
// very next query can be served from it: no rebuild, and no staleness window
// between the write committing and the replica reflecting it.
//
// scope is the WriteScope the driver's write observers filled in for this
// write (write_observer.go); scope.Changes() is the ChangeSet naming every
// key the write touched. Apply is expected to be called once per committed
// write, from the driver's write path, after the write has actually landed
// in PostgreSQL -- read-back (below) reads committed state on the pool, so
// calling it before the commit would replay the pre-write state.
//
// Sequence, all of it serialized under applyMu so two concurrent writes can
// never publish Views derived from the same base and lose one another's
// delta:
//
//  1. Bump applyEpoch, the counter a concurrent rebuild compares against to
//     decide whether its freshly loaded snapshot can still be adopted (see
//     adoptRebuiltView). Bumped for every call, before anything else can
//     decide to return early, so a rebuild is never allowed to adopt a
//     snapshot that could have missed this write.
//  2. Give up early -- correctly, and without any PostgreSQL round trip --
//     when there is nothing to keep up to date: a disabled engine (which
//     never serves), an engine already in fallback (whose pending rebuild
//     reads post-write state anyway), or an engine with no snapshot adopted
//     yet (the boot-load rebuild will read post-write state anyway, for the
//     same reason).
//  3. A nil scope, or a ChangeSet carrying a fallback record, means this
//     write's effect cannot be expressed as a delta at all: enterFallback,
//     which is the honest answer rather than a guess.
//  4. Read back every key the ChangeSet named from PostgreSQL (readBack,
//     readback.go): present rows are the write's post-state, absent keys are
//     deletions. A read-back error is not survivable either -- the write's
//     effect is unknown -- so it enters fallback too.
//  5. Turn that read-back result, plus the kind-scoped delete criteria the
//     ChangeSet carries, into one immutable delta Segment (buildApplySegment).
//  6. Layer the segment onto the current View and publish the result, unless
//     the result would exceed cfg.MemoryLimit -- the same limit RebuildNow
//     applies to a freshly loaded snapshot, applied here to the View the
//     delta produces -- in which case enterFallback instead.
//
// Safe for concurrent use. A caller that has no ctx of its own may pass
// context.Background(); ctx bounds the read-back queries only.
//
// Watermark bookkeeping (watermark.go) is folded into this same sequence,
// deliberately unconditional on everything below: if scope was bumped
// (scope.Watermark's own flag), a deferred AdvanceWatermark call is queued
// BEFORE any of the branches below can return, so it runs on every one of
// them -- the disabled-engine return, every enterFallback branch, the
// empty-ChangeSet no-op, and the ordinary published-segment path alike.
// The pg watermark counter already advanced the instant BumpWatermark's
// own UPDATE committed, independent of what this call goes on to do with
// the write's own effect, so this scope's bump has to resolve regardless of
// which of those branches actually fires. Also serialized under applyMu,
// for simplicity: nothing about it strictly requires the lock, but running
// it there costs nothing in the common case (watermarkDirty is false, so
// AdvanceWatermark never reaches the pool at all) and keeps this method's
// whole sequence easy to reason about as one block.
func (e *Engine) Apply(ctx context.Context, scope *WriteScope) {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()

	if scope != nil {
		if counter, bumped := scope.Watermark(); bumped {
			defer e.AdvanceWatermark(ctx, counter)
		}
	}

	e.applyEpoch.Add(1)

	if !e.cfg.Enabled {
		return
	}

	if scope == nil {
		// Nothing names what changed, so nothing can be replayed narrowly.
		e.enterFallback(ctx, "nil write scope")
		return
	}

	cs := scope.Changes()
	if hasFallback, reasons := cs.HasFallback(); hasFallback {
		e.enterFallback(ctx, strings.Join(reasons, "; "))
		return
	}

	if cs.Empty() {
		// A write that touched nothing this package tracks (e.g. a
		// transaction that only read). There is nothing to publish, and
		// publishing an empty segment would only grow the segment stack.
		return
	}

	if e.state.Load() != stateServing {
		// A rebuild is already pending, and it reads PostgreSQL's own
		// post-write state -- applying this delta to a View nothing is
		// serving from would be pure waste.
		//
		// startFallbackRebuild is called defensively before returning, even
		// though enterFallback already calls it on every path that enters
		// fallback: it is idempotent (fallbackRebuilding's CAS is a no-op
		// once a recovery goroutine is already in flight), so this costs
		// nothing in the common case, and self-heals the one window where it
		// is not a no-op -- the engine observed in fallback here with no
		// recovery goroutine actually running (see runFallbackRebuild's doc
		// for the race that can otherwise leave it stranded that way).
		e.startFallbackRebuild()
		return
	}

	current := e.snap.Load()
	if current == nil {
		// No snapshot has ever been adopted: there is no base to layer a
		// delta onto, and whichever rebuild eventually adopts one will read
		// this write's own post-commit state from PostgreSQL anyway.
		return
	}

	rb, err := e.readBack(ctx, cs)
	if err != nil {
		e.enterFallback(ctx, fmt.Sprintf("read-back failed: %v", err))
		return
	}

	seg, err := buildApplySegment(current, rb, cs)
	if err != nil {
		e.enterFallback(ctx, fmt.Sprintf("segment build failed: %v", err))
		return
	}

	newView := current.WithSegment(seg)

	// Guarded on the limit being configured at all, deliberately: ApproxBytes
	// is not free on an overlay View (it force-computes the memoized overlay
	// projections so the estimate does not depend on which accessors happen
	// to have run yet), and this runs on every write. With no limit set --
	// the default -- there is nothing to compare against, so it is skipped
	// rather than computed and discarded.
	if e.cfg.MemoryLimit > 0 {
		if approxBytes := newView.ApproxBytes(); approxBytes > uint64(e.cfg.MemoryLimit) {
			e.enterFallback(ctx, fmt.Sprintf("applied view exceeds memory limit (%d > %d bytes)", approxBytes, uint64(e.cfg.MemoryLimit)))
			return
		}
	}

	e.snap.Store(newView)

	e.cfg.Log.DebugContext(ctx, "bloodtrail: write-through applied",
		slog.Int("nodes", seg.NodeCount()),
		slog.Int("edges", seg.EdgeCount()),
		slog.Int("segments", newView.SegmentCount()),
	)
}

// buildApplySegment turns one read-back result, plus whatever kind-scoped
// delete criteria cs recorded, into the delta Segment Apply layers onto
// view.
//
// Staging order is load-bearing, since SegmentBuilder resolves a repeated id
// by last-call-wins (its own doc):
//
//  1. Newly resolved kinds (AddKind), so the segment can name the kind ids
//     the node/edge states below carry.
//  2. Every tombstone: node ids and objectids read-back reported absent
//     (each cascading to the edges incident to that node in view -- deleting
//     a node deletes its edges, and the base CSR slots for those edges would
//     otherwise keep them visible), edge ids and (start, end, kind) triples
//     read-back reported absent, and the nodes/edges matching cs's own
//     delete criteria.
//  3. Every present node and edge state read-back returned, staged LAST so
//     that PostgreSQL's own post-commit truth wins over any tombstone step 2
//     derived from the (possibly stale) View -- e.g. a node the View still
//     believes carries a kind some criteria targets, which PostgreSQL's own
//     row shows still exists.
//
// The error return covers the two ways a segment cannot be built faithfully:
// a node's property bag failing to parse or exhausting the segment's PropID
// space (AddNodeState), and a delete criteria naming an exclude kind this
// View's kind table cannot resolve (applyNodeKindCriteria) -- both of which
// Apply turns into a fallback rather than publishing a partial delta.
func buildApplySegment(view *snapshot.View, rb *readbackResult, cs *ChangeSet) (*snapshot.Segment, error) {
	var b snapshot.SegmentBuilder

	for id, name := range rb.resolvedKinds {
		b.AddKind(id, name)
	}

	for _, id := range rb.absentNodeIDs {
		tombstoneNodeWithCascade(&b, view, id)
	}
	for _, objectID := range rb.absentObjectIDs {
		// An objectid that matched no row after the write: whatever nodes
		// the View still knows under it are gone from PostgreSQL. This also
		// covers an edge-triple-by-objectid whose endpoint is unresolvable
		// -- read-back reports that only through this same list (its own
		// doc), and a tombstoned endpoint already cascades to the edge.
		if dense, ok := view.NodesByObjectID(objectID); ok {
			for _, n := range dense {
				tombstoneNodeWithCascade(&b, view, view.GraphID(n))
			}
		}
	}
	for _, id := range rb.absentEdgeIDs {
		b.TombstoneEdge(id)
	}
	for _, triple := range rb.absentTriples {
		tombstoneAbsentTriple(&b, view, triple)
	}

	for _, criteria := range cs.NodeKindCriteria() {
		if err := applyNodeKindCriteria(&b, view, criteria); err != nil {
			return nil, err
		}
	}
	for _, kinds := range cs.EdgeKindCriteria() {
		applyEdgeKindCriteria(&b, view, kinds)
	}

	for _, ns := range rb.nodes {
		if err := b.AddNodeState(ns.id, ns.kindIDs, ns.propsJSON); err != nil {
			return nil, fmt.Errorf("engine: Apply: node %d: %w", ns.id, err)
		}
	}
	for _, es := range rb.edges {
		b.AddEdgeState(es.id, es.start, es.end, es.kindID)
	}

	return b.Build(), nil
}

// tombstoneNodeWithCascade stages node pgID as removed, along with every
// edge incident to it in view -- in both directions, since deleting a node
// deletes the edges pointing at it just as surely as the ones leaving it.
//
// The cascade is what keeps the base snapshot's own CSR slots from
// resurrecting those edges: View.OutEdges/InEdges skip a base slot only
// when the merged delta carries a record for that edge id (or when an
// endpoint is not Alive, which already hides them for a walk that starts
// elsewhere) -- but an edge id reached through EdgeStateByID, or counted by
// a full edge scan, needs its own tombstone to disappear.
//
// A pgID view does not know (never loaded, or already tombstoned by an
// earlier segment) needs no cascade: OutEdges/InEdges yield nothing for a
// node that is not Alive, and there are no base slots to hide for a node
// that was never in the base at all.
func tombstoneNodeWithCascade(b *snapshot.SegmentBuilder, view *snapshot.View, pgID uint64) {
	b.TombstoneNode(pgID)

	dense, ok := view.Dense(pgID)
	if !ok {
		return
	}
	view.OutEdges(dense, func(_ snapshot.NodeID, _ snapshot.KindID, edgeID uint64) bool {
		b.TombstoneEdge(edgeID)
		return true
	})
	view.InEdges(dense, func(_ snapshot.NodeID, _ snapshot.KindID, edgeID uint64) bool {
		b.TombstoneEdge(edgeID)
		return true
	})
}

// tombstoneAbsentTriple stages whichever edge in view matches the
// (start, end, kind) triple read-back could not find in PostgreSQL. The
// triple names pg node ids, so both endpoints are resolved through
// View.Dense first; the edge itself is found by walking the start node's
// out-edges, since a Segment tombstone is keyed by edge id and a triple
// alone names none.
//
// Two cases resolve to "nothing to tombstone", both correctly: a triple
// whose kind never resolved to a real KindID at all (unresolvedTripleKind,
// the -1 sentinel readBack records for a kind PostgreSQL has never
// asserted, which can therefore never match an edge in view), and a triple
// whose endpoint view does not know -- an edge between nodes this replica
// never held cannot be in it either.
func tombstoneAbsentTriple(b *snapshot.SegmentBuilder, view *snapshot.View, triple tripleKey) {
	if triple.kindID == unresolvedTripleKind {
		return
	}

	start, ok := view.Dense(triple.start)
	if !ok {
		return
	}
	end, ok := view.Dense(triple.end)
	if !ok {
		return
	}

	view.OutEdges(start, func(target snapshot.NodeID, kind snapshot.KindID, edgeID uint64) bool {
		if target == end && kind == triple.kindID {
			b.TombstoneEdge(edgeID)
		}
		return true
	})
}

// applyNodeKindCriteria stages every node in view that a
// DeleteNodesByKinds-shaped delete would have removed, plus each one's edge
// cascade -- replaying the criteria itself rather than an id list captured
// before the delete ran (RecordDeleteNodesByKinds' own doc).
//
// The matching rule mirrors dawgs' pg driver exactly (drivers/pg/driver.go's
// DeleteNodesByKinds and buildNodeDeleteStatement, v0.8.0): a node is
// deleted when its kinds overlap Include -- or, when Include is empty, for
// every node -- and do not overlap Exclude. An Include kind this View's kind
// table does not know matches nothing, exactly as an include kind
// PostgreSQL has never asserted maps to no kind id and matches no row.
//
// An Exclude kind the View cannot resolve is the one case that cannot be
// replayed soundly: PostgreSQL refuses that delete outright (it fails
// closed rather than silently widening the delete), so reaching this point
// with one means the View's kind table and PostgreSQL's disagree, and
// guessing either way could tombstone nodes that still exist. It returns an
// error instead, which Apply turns into a fallback.
//
// Enumeration walks the kind bitmaps when Include names any kind -- the
// common case, and far cheaper than a full node scan -- and only falls back
// to scanning every node when Include is empty, i.e. when the delete
// genuinely targets the whole graph.
func applyNodeKindCriteria(b *snapshot.SegmentBuilder, view *snapshot.View, criteria NodeKindDeleteCriteria) error {
	table := view.Kinds()

	include := make([]snapshot.KindID, 0, len(criteria.Include))
	for _, kind := range criteria.Include {
		if id, ok := table.ID(kindName(kind)); ok {
			include = append(include, id)
		}
	}

	exclude := make([]snapshot.KindID, 0, len(criteria.Exclude))
	for _, kind := range criteria.Exclude {
		id, ok := table.ID(kindName(kind))
		if !ok {
			return fmt.Errorf("engine: Apply: node delete criteria excludes kind %q, which the current view cannot resolve", kindName(kind))
		}
		exclude = append(exclude, id)
	}

	tombstone := func(dense snapshot.NodeID) {
		if !view.Alive(dense) {
			return
		}
		if len(exclude) > 0 && kindsIntersect(view.KindIDsOf(dense), exclude) {
			return
		}
		tombstoneNodeWithCascade(b, view, view.GraphID(dense))
	}

	if len(criteria.Include) > 0 {
		// Repeats are harmless: a node carrying two Include kinds is staged
		// twice, and every SegmentBuilder call for a given id is idempotent
		// (last call wins, and both calls are the same tombstone).
		for _, kindID := range include {
			view.NodesOfKind(kindID).Iterate(func(dense snapshot.NodeID) bool {
				tombstone(dense)
				return true
			})
		}
		return nil
	}

	for n := 0; n < view.NodeCount(); n++ {
		tombstone(snapshot.NodeID(n))
	}
	return nil
}

// applyEdgeKindCriteria stages every edge in view whose kind is named by
// kinds -- the replay of a DeleteRelationshipsByKinds-shaped delete, which
// removes exactly those edges and nothing else (no cascade: deleting an
// edge never removes a node).
//
// A kind this View's kind table does not know is skipped, matching dawgs'
// own tolerant mapping (drivers/pg/driver.go: kinds that are not defined in
// the database map to no ids and delete nothing); an empty kinds set
// therefore stages nothing at all, exactly as the pg driver's own empty-set
// early return deletes nothing.
//
// Enumeration is a full scan: every alive node's out-edges, which visits
// every edge in the View exactly once (each edge has exactly one source).
// That is O(nodes + edges) per criteria, deliberately -- there is no
// kind-keyed edge index on a View, and a kind-scoped relationship delete is
// a rare, bulk operation (BloodHound's analysis reset, and the driver-level
// DeleteRelationshipsByKinds entry point), not a hot path.
func applyEdgeKindCriteria(b *snapshot.SegmentBuilder, view *snapshot.View, kinds graph.Kinds) {
	table := view.Kinds()

	wanted := make(map[snapshot.KindID]struct{}, len(kinds))
	for _, kind := range kinds {
		if id, ok := table.ID(kindName(kind)); ok {
			wanted[id] = struct{}{}
		}
	}
	if len(wanted) == 0 {
		return
	}

	for n := 0; n < view.NodeCount(); n++ {
		dense := snapshot.NodeID(n)
		if !view.Alive(dense) {
			continue
		}
		view.OutEdges(dense, func(_ snapshot.NodeID, kind snapshot.KindID, edgeID uint64) bool {
			if _, ok := wanted[kind]; ok {
				b.TombstoneEdge(edgeID)
			}
			return true
		})
	}
}

// kindsIntersect reports whether have and want share any kind id. Both are
// expected to be tiny (a node's own kind list, and one criteria's kind
// list), so a nested scan beats building any auxiliary set.
func kindsIntersect(have, want []snapshot.KindID) bool {
	for _, h := range have {
		for _, w := range want {
			if h == w {
				return true
			}
		}
	}
	return false
}

// enterFallback records that the in-memory replica can no longer be trusted
// to match PostgreSQL, for the stated reason, and starts recovering.
//
// Two effects, both idempotent so that a burst of failing writes costs one
// log line and one rebuild goroutine rather than one of each per write:
//
//   - The state flips to stateFallback (once; a call made while already in
//     fallback logs nothing further). Every serving entry point declines
//     from that moment on, so every query goes to PostgreSQL -- always
//     correct, just slower.
//   - One background goroutine is started, if none is already running, to
//     rebuild the snapshot from scratch and return the engine to
//     stateServing (runFallbackRebuild).
//
// Callers hold applyMu; nothing here acquires it (the rebuild goroutine
// acquires it later, on its own, when it publishes).
func (e *Engine) enterFallback(ctx context.Context, reason string) {
	if e.state.CompareAndSwap(stateServing, stateFallback) {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: fallback entered", slog.String("reason", reason))
	}
	e.startFallbackRebuild()
}

// startFallbackRebuild launches the single fallback recovery goroutine, if
// claimRebuildLoop's CAS on fallbackRebuilding says none is already running
// -- whether that's a previous fallback trip's own recovery goroutine, or
// Start's boot-load goroutine (boot.go), which claims the same flag through
// the same method. Either way, one loop is enough: whichever is running
// will adopt a snapshot and end both jobs at once (adoptRebuiltView adopts,
// and clears fallback, regardless of which trigger asked for the rebuild
// that succeeds).
func (e *Engine) startFallbackRebuild() {
	if !e.claimRebuildLoop() {
		return
	}
	go e.runFallbackRebuild()
}

// claimRebuildLoop attempts to claim fallbackRebuilding: the single gate
// shared by every retry-until-adopted rebuild loop the engine ever runs --
// Start's boot-load goroutine (runBootLoad, boot.go) and the fallback
// recovery goroutine (runFallbackRebuild, below) both claim it through this
// same method before launching, rather than each carrying its own flag, so
// that at most one such loop is EVER running regardless of which of the two
// reaches this first. true means the caller just became that one loop and
// must launch it; false means one is already running, and the caller must
// not start a second -- see startFallbackRebuild's and boot.go's Start doc
// for why the loop that IS running is always sufficient either way.
//
// A disabled engine claims nothing: it declines every query regardless of
// state, so there is no serving to restore, and rebuilding a replica nobody
// reads would be pure cost. (This also keeps enterFallback/startFallbackRebuild
// usable from unit tests holding an Engine with no database behind it.)
func (e *Engine) claimRebuildLoop() bool {
	if !e.cfg.Enabled {
		return false
	}
	return e.fallbackRebuilding.CompareAndSwap(false, true)
}

// runFallbackRebuild rebuilds the snapshot until one is actually adopted,
// then returns the engine to stateServing and logs the "fallback exited"
// marker.
//
// It runs on the engine's own background context (bgCtx, cancelled by Stop),
// not on the context of whichever write happened to trip the fallback: that
// write's caller is long gone by the time recovery finishes, and its ctx
// being cancelled says nothing about whether the replica should recover.
//
// A rebuild that fails outright, or cannot be adopted because a write landed
// while it was loading (see adoptRebuiltView), is retried on a doubling
// backoff between fallbackRetryInterval and fallbackRetryMax -- transient
// conditions expected to clear within seconds. A rebuild refused for
// exceeding cfg.MemoryLimit (Engine.overBudget) is different: retrying a
// full LoadSnapshot on that same fast schedule would repeat, forever, a load
// whose outcome cannot change until an operator raises the limit or the
// graph shrinks, so fallbackRetryDelay backs that case off to
// fallbackBudgetRetryInterval instead and leaves backoff itself untouched --
// see fallbackRetryDelay's own doc for why that is what makes recovery snap
// back to a fast retry the moment the refusal lifts.
//
// Only an adopted rebuild exits fallback: adopting is what makes the
// published View both complete and current, and serving from anything less
// is exactly what fallback exists to prevent. The state flip itself happens
// inside adoptRebuiltView, alongside the publish it belongs with, so that a
// rebuild from any other source (Start's boot-load goroutine, a manual
// call) ends the fallback too rather than leaving the engine declining
// behind an already-trustworthy replica.
//
// The adopted case hands off to finishFallbackRebuild rather than clearing
// fallbackRebuilding itself -- see that function's doc for the stranding
// race it closes. The other two return points (context cancelled, either at
// the top of the loop or while waiting out a retry) clear the flag directly
// and do NOT run that recheck: Stop() cancelling bgCtx means the engine is
// shutting down, and relaunching recovery in response to a state this
// goroutine is about to stop observing anyway would only start a goroutine
// with nothing left to wait for it.
func (e *Engine) runFallbackRebuild() {
	backoff := fallbackRetryInterval
	for {
		if e.bgCtx.Err() != nil {
			e.fallbackRebuilding.Store(false)
			return
		}

		adopted, err := e.rebuildOnce(e.bgCtx, triggerFallback)
		switch {
		case err != nil:
			e.cfg.Log.WarnContext(e.bgCtx, "bloodtrail: fallback rebuild failed", slog.Any("error", err))
		case adopted:
			// adoptRebuiltView already flipped the state back to serving and
			// logged the "fallback exited" marker.
			e.finishFallbackRebuild()
			return
		}

		wait, next := fallbackRetryDelay(err == nil && e.overBudget.Load(), backoff)
		backoff = next

		select {
		case <-e.bgCtx.Done():
			e.fallbackRebuilding.Store(false)
			return
		case <-time.After(wait):
		}
	}
}

// finishFallbackRebuild clears fallbackRebuilding, then relaunches recovery
// if the engine has already raced back into stateFallback by the time it
// does. Called from both loops that share the flag (runFallbackRebuild
// below, and runBootLoad's identical adopted exit, boot.go) -- whichever one
// just adopted a snapshot, the race it closes and the reasoning are the
// same.
//
// The race it closes: adoptRebuiltView (engine.go) publishes the rebuilt
// View and flips state back to stateServing BEFORE this goroutine gets a
// chance to run at all (rebuildOnce returns to its caller, which calls this
// function, only after adoptRebuiltView has already returned). If some
// OTHER write's Apply fails in the window between that state flip and this
// function's own Store(false) below, its enterFallback call flips state
// back to stateFallback and calls startFallbackRebuild -- which finds
// fallbackRebuilding still true (this goroutine has not cleared it yet) and
// gives up silently, exactly as it is meant to when a recovery goroutine is
// genuinely already in flight. But here one is NOT still doing useful work:
// it is moments from exiting, having already committed to leaving fallback
// exited. Without this recheck, the engine would be left stranded in
// stateFallback with no goroutine ever again scheduled to recover it, every
// query declining to PostgreSQL forever: there is no background poller left
// to paper over that by rebuilding on its own cadence (the poller was
// retired alongside write-through's freshness gates, boot.go), so this
// recheck is the only thing that can.
//
// Only called from an adopted-rebuild return path, never from a
// context-cancelled return: relaunching in response to a state change the
// exiting goroutine is about to stop observing anyway, right as the engine
// is shutting down, would just start a new goroutine with nothing left to
// wait for it.
func (e *Engine) finishFallbackRebuild() {
	e.fallbackRebuilding.Store(false)
	if e.state.Load() == stateFallback {
		e.startFallbackRebuild()
	}
}

// fallbackBudgetRetryInterval is the recovery goroutine's retry cadence for
// a rebuild attempt refused for exceeding cfg.MemoryLimit (Engine.overBudget
// -- see rebuildOnce, engine.go). It reuses refusalLogInterval (engine.go)
// rather than a second constant for the same underlying judgment: how often
// it is worth re-checking a condition an operator, not the passage of a few
// seconds, has to resolve (raising the limit, or the graph shrinking) --
// exactly what RebuildNow's own refusal warning is already rate-limited to.
// Retrying a full LoadSnapshot every fallbackRetryMax (30s) forever while
// the graph stays over budget would cost a full snapshot load for a result
// already known.
const fallbackBudgetRetryInterval = refusalLogInterval

// fallbackRetryDelay is runFallbackRebuild's retry-cadence decision,
// extracted as a pure function for unit testing: given whether the rebuild
// attempt that just ran was refused specifically for being over budget, and
// the doubling backoff carried in from the previous iteration, it returns
// how long to wait before the next attempt and the backoff value to carry
// into the iteration after that.
//
// A budget refusal always waits fallbackBudgetRetryInterval and returns
// backoff UNCHANGED -- deliberately not advanced, and not derived from it at
// all. The doubling schedule exists for transient conditions (a database
// blip, an epoch race against a concurrent Apply) expected to clear within
// seconds, not for a capacity problem nothing but an operator, or the graph
// shrinking, resolves; leaving backoff untouched is what makes recovery snap
// back to a fast retry the instant the refusal lifts -- the very next
// non-budget outcome (a transient failure, an epoch race, or a success)
// sees backoff still at whatever it was before the over-budget episode
// began, most commonly still fallbackRetryInterval, since a sustained
// over-budget stretch never advances it while it lasts.
//
// Every other case -- overBudget false, whether because the attempt
// genuinely failed, lost an epoch race, or simply was not over budget --
// keeps the pre-existing doubling schedule unchanged: wait the current
// backoff, then double it, capped at fallbackRetryMax.
func fallbackRetryDelay(overBudget bool, backoff time.Duration) (wait time.Duration, nextBackoff time.Duration) {
	if overBudget {
		return fallbackBudgetRetryInterval, backoff
	}

	wait = backoff
	backoff *= 2
	if backoff > fallbackRetryMax {
		backoff = fallbackRetryMax
	}
	return wait, backoff
}
