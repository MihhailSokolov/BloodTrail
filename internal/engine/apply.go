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
// goroutine makes, alongside boot.go's own triggerStartup, so a rebuild
// driven by a failed apply is distinguishable in the log from a boot-load-
// or test-driven one (every manual/test caller passes its own literal
// "manual" -- see triggerStartup's doc for why there is no shared constant
// for that one).
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
//     same reason -- and when the boot is instead trying its snapshot FILE,
//     the boot gap buffer has already recorded this scope, right after the
//     watermark bookkeeping, so the file can be adopted despite this write
//     by replaying it: bootgap.go, adoptSnapshotFileView).
//  3. A nil scope, or a ChangeSet carrying a fallback record, means this
//     write's effect cannot be expressed as a delta at all: enterFallback,
//     which is the honest answer rather than a guess.
//  4. Read back every key the ChangeSet named from PostgreSQL (readBack,
//     readback.go): present rows are the write's post-state, absent keys are
//     deletions. The rows a kind-scoped delete criteria could have removed,
//     and the nodes the View knows under an objectid found on no row, are
//     keys too -- the current View's candidates, re-read the same way
//     (viewCandidates) -- so a write never removes a row PostgreSQL kept,
//     whichever order concurrent writes' Applies run in. A read-back error
//     is not survivable either -- the write's effect is unknown -- so it
//     enters fallback too.
//  5. Turn that read-back result into one immutable delta Segment
//     (buildApplySegment).
//  6. Layer the segment onto the current View and publish the result, unless
//     the result would exceed cfg.MemoryLimit -- the same limit RebuildNow
//     applies to a freshly loaded snapshot, applied here to the View the
//     delta produces -- in which case enterFallback instead.
//  7. Still under applyMu: run the two size-triggered maintenance steps
//     compact.go implements -- collapse the segment stack if it has grown
//     past maxSegments, and spawn a background compaction if the delta has
//     grown past cfg.CompactEntries/CompactBytes (maintainAfterPublish).
//
// Safe for concurrent use. A caller that has no ctx of its own may pass
// context.Background(); ctx bounds the read-back queries only.
//
// Watermark bookkeeping (watermark.go) is folded into this same sequence,
// deliberately unconditional on everything below: both of its steps run on
// every one of the branches below -- the disabled-engine return, every
// enterFallback branch, the empty-ChangeSet no-op, and the ordinary
// published-segment path alike -- because both are statements about a write
// PostgreSQL has already seen, independent of what this call goes on to do
// with the write's own effect:
//
//   - If scope bumped (scope.Watermark's own flag), AdvanceWatermark folds
//     its counter into e.appliedWatermark and retires its e.inflightBumps
//     entry. The pg counter already advanced the instant BumpWatermark's own
//     UPDATE committed, so this scope's bump has to resolve regardless of
//     which branch fires.
//   - If scope's own eager bump FAILED (settleWatermarkFailure), this call is
//     the proof that the write it guarded has landed -- Apply is only ever
//     called once a write has actually committed -- which is exactly what
//     settles that failure's watermark trust generation. Settling it BEFORE
//     the applyEpoch bump below is what lets a concurrent rebuild's
//     epoch check catch a settle it did not observe (rebuildOnce's own doc);
//     the rebuild request that follows is what guarantees an adopted snapshot
//     will eventually resolve the generation (WatermarkTrusted's own doc).
//
// Both are cheap atomic bookkeeping with no pg round trip of their own, so
// they cost the applyMu-holding sequence nothing measurable. Trust itself is
// never computed here: WatermarkTrusted derives it on demand from the
// generations, so no Apply call has to reason about whether some other
// write's failure has been made good yet.
func (e *Engine) Apply(ctx context.Context, scope *WriteScope) {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()

	if scope != nil {
		if counter, bumped := scope.Watermark(); bumped {
			e.AdvanceWatermark(counter)
		}
	}
	settledFailure := e.settleWatermarkFailure(scope)

	e.applyEpoch.Add(1)

	// Recorded before ANY of the early returns below, so a bumped write
	// whose ChangeSet is empty -- dropped at the cs.Empty() branch -- still
	// gets its counter accounted, and a write the buffer cannot faithfully
	// replay poisons it (observe's own doc). A no-op past one atomic load
	// whenever the buffer is not armed, which is everywhere outside the
	// boot file-attempt window.
	e.bootGap.observe(scope)

	if settledFailure {
		// Belt and braces. The write carrying a failed bump also carries the
		// ChangeSet fallback record ensureBumped (write_observer.go) pairs
		// with it, so the cs.HasFallback() branch below already enters
		// fallback and starts a recovery rebuild -- but only on the branches
		// that reach it, and only while that pairing holds. Requesting the
		// rebuild here instead makes "a settled failure always has a rebuild
		// coming" a property of this method alone. It only ever fires for a
		// write whose watermark bump genuinely failed, and goes through the
		// trust-rebuild rate limiter rather than launching directly: when
		// the pairing holds, the fallback branch below launches immediately
		// anyway (a suspect replica is never made to wait), so the limiter
		// only ever delays the pure trust-restoration case.
		e.requestTrustRebuild()
	}

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

	rb, err := e.readBack(ctx, current, cs)
	if err != nil {
		e.enterFallback(ctx, fmt.Sprintf("read-back failed: %v", err))
		return
	}

	seg, err := buildApplySegment(current, rb)
	if err != nil {
		e.enterFallback(ctx, fmt.Sprintf("segment build failed: %v", err))
		return
	}

	newView := current.WithSegment(seg)

	// The overlay's derived projections are built HERE rather than by
	// whichever query arrives first after this commit -- see View.Warm.
	newView.Warm()

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

	// Two size-triggered maintenance steps, still under applyMu (this
	// method's own defer hasn't unlocked yet): a synchronous collapse of an
	// overgrown segment stack, and the decision to spawn a background
	// compaction -- see compact.go's maintainAfterPublish for why both need
	// to run inside this same critical section.
	e.maintainAfterPublish(ctx, newView)
}

// buildApplySegment turns one read-back result into the delta Segment Apply
// layers onto view.
//
// Everything it stages is PostgreSQL's own answer for one key. readBack has
// already resolved every row the write could have touched -- the ids,
// objectids and triples its ChangeSet names, plus the View rows a
// kind-scoped delete criteria or an objectid found on no row make
// candidates (viewCandidates) -- to present rows and absent keys, so nothing
// here is derived from the write's payload or replayed as an instruction
// over whatever the View holds by the time this runs. That is what makes
// the result independent of the order concurrent writes' Applies run in,
// and of the boot gap's replay order (adoptSnapshotFileView): a tombstone
// is only ever staged for a row PostgreSQL no longer holds -- ids are never
// reused, so that stays true -- and a present row carries PostgreSQL's
// state as read after this write committed, so a later Apply naming the
// same row can only restage a later truth.
//
// Staging order is load-bearing, since SegmentBuilder resolves a repeated id
// by last-call-wins (its own doc):
//
//  1. Newly resolved kinds (AddKind), so the segment can name the kind ids
//     the node/edge states below carry.
//  2. Every tombstone: node ids read-back reported absent (each cascading
//     to the edges incident to that node in view -- deleting a node deletes
//     its edges, and the base CSR slots for those edges would otherwise keep
//     them visible), and edge ids and (start, end, kind) triples read-back
//     reported absent.
//  3. Every present node and edge state read-back returned, staged LAST so
//     that a row read-back found wins over a cascade tombstone the View
//     derived -- an edge found present although its endpoint, read by a
//     later query, was gone by then (the dead endpoint keeps it hidden).
//
// The error return covers the one way a segment cannot be built faithfully:
// a node's property bag failing to parse or exhausting the segment's PropID
// space (AddNodeState), which Apply turns into a fallback rather than
// publishing a partial delta.
func buildApplySegment(view *snapshot.View, rb *readbackResult) (*snapshot.Segment, error) {
	var b snapshot.SegmentBuilder

	for id, name := range rb.resolvedKinds {
		b.AddKind(id, name)
	}

	for _, id := range rb.absentNodeIDs {
		tombstoneNodeWithCascade(&b, view, id)
	}
	for _, id := range rb.absentEdgeIDs {
		b.TombstoneEdge(id)
	}
	for _, triple := range rb.absentTriples {
		tombstoneAbsentTriple(&b, view, triple)
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

// kindLookup resolves a delete criteria's kind names against the kind table
// of the View whose candidates readBack enumerates: that View's own table,
// then every criteria kind read-back resolved from PostgreSQL itself
// (resolveCriteriaKinds). The two never disagree about a name both hold,
// since a kind's id never changes once PostgreSQL has assigned it.
type kindLookup struct {
	table *snapshot.KindTable
	added map[string]snapshot.KindID
}

// newKindLookup builds view's kindLookup, inverting resolved (id->name, as
// resolveCriteriaKinds returns it) into the name->id direction criteria
// need.
func newKindLookup(view *snapshot.View, resolved map[snapshot.KindID]string) kindLookup {
	added := make(map[string]snapshot.KindID, len(resolved))
	for id, name := range resolved {
		added[name] = id
	}
	return kindLookup{table: view.Kinds(), added: added}
}

// id returns kind's id, and whether either source resolves it.
func (l kindLookup) id(kind graph.Kind) (snapshot.KindID, bool) {
	name := kindName(kind)
	if id, ok := l.table.ID(name); ok {
		return id, true
	}
	id, ok := l.added[name]
	return id, ok
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
// alone names none. An edge never changes its endpoints or kind (dawgs
// updates only an edge's properties) and the edge table holds each triple
// at most once, so the edge found is the one PostgreSQL no longer holds.
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

// viewCandidates collects the rows of one View that readBack must re-read
// from PostgreSQL because a write may have removed them without naming them
// by key: the nodes a kind-scoped node delete matches in the View
// (addNodeKindCriteria), the edges a kind-scoped relationship delete matches
// (addEdgeKindCriteria), and the nodes the View still knows under an
// objectid read-back found on no row (addObjectID).
//
// The View only chooses WHICH rows to ask about; the answer is always
// PostgreSQL's. A candidate the write did remove comes back absent and is
// tombstoned; one PostgreSQL kept comes back present and is restaged as it
// is now -- an edge created after the DELETE's snapshot and applied before
// the delete's own Apply, a node whose objectid was only rewritten. So the
// set has to cover every row the write may have removed that the View
// holds, and may safely cover more (an extra candidate only costs its
// re-read). A row the write removed that the View does not hold yet belongs
// to another write whose Apply is still to come, and that Apply's own
// read-back finds it absent.
//
// Enumeration touches only the View, in memory, once per Apply that carries
// a criteria or an absent objectid -- never a query's read path.
type viewCandidates struct {
	view  *snapshot.View
	nodes *snapshot.Bitset // dense node ids; nil until the first candidate
	edges []uint64         // edge ids, each at most once
}

// collectViewCandidates enumerates view's candidates for one write: the
// nodes view knows under each of absentObjectIDs (objectids read-back found
// on no row) and the rows cs's kind-scoped delete criteria match, resolving
// the criteria's kind names through view's kind table and criteriaKinds
// (resolveCriteriaKinds' id->name result). Its one error is
// addNodeKindCriteria's unresolvable exclusion.
func collectViewCandidates(view *snapshot.View, criteriaKinds map[snapshot.KindID]string, cs *ChangeSet, absentObjectIDs []string) (*viewCandidates, error) {
	c := &viewCandidates{view: view}
	for _, objectID := range absentObjectIDs {
		c.addObjectID(objectID)
	}

	lookup := newKindLookup(view, criteriaKinds)
	for _, criteria := range cs.NodeKindCriteria() {
		if err := c.addNodeKindCriteria(lookup, criteria); err != nil {
			return nil, err
		}
	}
	c.addEdgeKindCriteria(lookup, cs.EdgeKindCriteria())
	return c, nil
}

// addNode marks dense node n as a candidate.
func (c *viewCandidates) addNode(n snapshot.NodeID) {
	if c.nodes == nil {
		c.nodes = snapshot.NewBitset(c.view.NodeCount())
	}
	c.nodes.Set(n)
}

// nodeIDs returns every candidate node's database id, each once.
func (c *viewCandidates) nodeIDs() []uint64 {
	if c.nodes == nil {
		return nil
	}
	ids := make([]uint64, 0, c.nodes.Count())
	c.nodes.Iterate(func(n snapshot.NodeID) bool {
		ids = append(ids, c.view.GraphID(n))
		return true
	})
	return ids
}

// addObjectID marks every live node the View knows under objectID -- an
// objectid read-back found on no row. That the objectid matches nothing
// any more does not mean those nodes are gone: a node whose objectid was
// rewritten keeps its id, its row and its edges, so each one is re-read by
// id, and only one that is actually absent is tombstoned (with its edge
// cascade).
func (c *viewCandidates) addObjectID(objectID string) {
	dense, ok := c.view.NodesByObjectID(objectID)
	if !ok {
		return
	}
	for _, n := range dense {
		c.addNode(n)
	}
}

// addNodeKindCriteria marks every live View node a DeleteNodesByKinds-shaped
// delete with criteria removes.
//
// The matching rule mirrors dawgs' pg driver exactly (drivers/pg/driver.go's
// DeleteNodesByKinds and buildNodeDeleteStatement, v0.8.0): a node is
// deleted when its kinds overlap Include -- or, when Include is empty, for
// every node -- and do not overlap Exclude. Kind names resolve through
// lookup (the View's own table, then whatever read-back resolved from
// PostgreSQL -- resolveCriteriaKinds) and match by id against each node's
// own kind ids, so a kind PostgreSQL registered after this View's last full
// load still resolves when no node carries it, and then excludes (or
// includes) nothing, exactly as it does in PostgreSQL. An Include kind that
// resolves nowhere matches nothing, exactly as an include kind PostgreSQL
// has never asserted maps to no kind id and matches no row.
//
// An Exclude kind that resolves nowhere is the one case that cannot be
// replayed soundly: PostgreSQL refuses that delete outright (it fails
// closed rather than silently widening the delete), so reaching this point
// with one means the applier and PostgreSQL disagree about what the delete
// even was. It returns an error instead, which Apply turns into a fallback.
//
// Enumeration walks the kind bitmaps when Include names any kind -- the
// common case, and far cheaper than a full node scan -- and only scans
// every node when Include is empty, i.e. when the delete genuinely targets
// the whole graph.
func (c *viewCandidates) addNodeKindCriteria(lookup kindLookup, criteria NodeKindDeleteCriteria) error {
	include := make([]snapshot.KindID, 0, len(criteria.Include))
	for _, kind := range criteria.Include {
		if id, ok := lookup.id(kind); ok {
			include = append(include, id)
		}
	}

	exclude := make([]snapshot.KindID, 0, len(criteria.Exclude))
	for _, kind := range criteria.Exclude {
		id, ok := lookup.id(kind)
		if !ok {
			return fmt.Errorf("engine: Apply: node delete criteria excludes kind %q, which neither the current view nor read-back could resolve", kindName(kind))
		}
		exclude = append(exclude, id)
	}

	consider := func(n snapshot.NodeID) bool {
		if !c.view.Alive(n) {
			return true
		}
		if len(exclude) > 0 && kindsIntersect(c.view.KindIDsOf(n), exclude) {
			return true
		}
		c.addNode(n)
		return true
	}

	if len(criteria.Include) > 0 {
		for _, kindID := range include {
			c.view.NodesOfKind(kindID).Iterate(consider)
		}
		return nil
	}

	for n := 0; n < c.view.NodeCount(); n++ {
		consider(snapshot.NodeID(n))
	}
	return nil
}

// addEdgeKindCriteria marks every View edge whose kind one of criteria (each
// a DeleteRelationshipsByKinds-shaped kind set) names. Deleting an edge
// never removes a node, so no node is marked.
//
// Kind names resolve through lookup exactly as addNodeKindCriteria's do. A
// kind that resolves nowhere is skipped, matching dawgs' own tolerant
// mapping (drivers/pg/driver.go: kinds that are not defined in the database
// map to no ids and delete nothing); an empty kind set therefore marks
// nothing at all, exactly as the pg driver's own empty-set early return
// deletes nothing.
//
// Every edge record the View carries counts, not just the ones OutEdges
// shows: an edge whose endpoint the View does not know yet (its node's own
// Apply has not run) is hidden only until that endpoint lands, and would
// then reappear although PostgreSQL deleted it. So the scan reads the delta
// segments' own records -- newest first, since the newest record for an id
// decides it, and a tombstone there needs nothing -- and then every base
// forward-CSR slot the delta does not decide. That is one pass over the
// base's edge-kind column plus one over the delta's edge records per Apply
// carrying a relationship criteria: a rare, bulk operation (BloodHound's
// DeleteCollectedGraphData), never a query's read path.
func (c *viewCandidates) addEdgeKindCriteria(lookup kindLookup, criteria []graph.Kinds) {
	var wanted []snapshot.KindID
	for _, kinds := range criteria {
		for _, kind := range kinds {
			if id, ok := lookup.id(kind); ok {
				wanted = append(wanted, id)
			}
		}
	}
	if len(wanted) == 0 {
		return
	}
	maxWanted := wanted[0]
	for _, id := range wanted {
		maxWanted = max(maxWanted, id)
	}
	mask := snapshot.NewKindMask(maxWanted)
	for _, id := range wanted {
		mask.Set(id)
	}

	decided := make(map[uint64]struct{})
	segments := c.view.Segments()
	for i := len(segments) - 1; i >= 0; i-- {
		segments[i].IterEdges(func(id uint64, st snapshot.EdgeSegState) bool {
			if _, ok := decided[id]; ok {
				return true
			}
			decided[id] = struct{}{}
			if !st.Tombstoned && mask.Has(st.Kind) {
				c.edges = append(c.edges, id)
			}
			return true
		})
	}

	base := c.view.Base()
	for slot, kind := range base.OutKinds {
		if !mask.Has(kind) {
			continue
		}
		id := base.OutEdgeIDs[slot]
		if _, ok := decided[id]; ok {
			continue
		}
		c.edges = append(c.edges, id)
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
//
// Every winning CAS also bumps rebuildLoopStarts (engine.go), in this same
// calling goroutine, before the caller's own "go" statement runs the loop --
// that is what makes rebuildLoopStarts a race-free way for a test to observe
// "a relaunch happened" even when the relaunched goroutine goes on to clear
// fallbackRebuilding again immediately.
func (e *Engine) claimRebuildLoop() bool {
	if !e.cfg.Enabled {
		return false
	}
	won := e.fallbackRebuilding.CompareAndSwap(false, true)
	if won {
		e.rebuildLoopStarts.Add(1)
	}
	return won
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

// trustRebuildMinInterval rate-limits trust-restoring rebuild launches
// (requestTrustRebuild, below): at most one per interval. The judgment is
// fallbackRetryMax's, applied to launches instead of retries -- a
// trust-only rebuild runs while the engine is serving correctly (only
// WatermarkTrusted, and therefore snapshot-file stamping, waits on it), so
// there is no urgency that would justify letting a sustained stream of
// settling bump failures turn into back-to-back full snapshot loads, each
// tens of seconds of load at production scale, forever. One load per
// interval bounds that cost to what a single fallback recovery retry cycle
// already tolerates, while a quiet engine still gets its trust back on the
// very first request.
const trustRebuildMinInterval = fallbackRetryMax

// trustRebuildDelay is requestTrustRebuild's pure timing decision, extracted
// for unit testing (mirroring fallbackRetryDelay's identical treatment):
// given now and the last trust launch, both UnixNano, it returns whether a
// launch may happen immediately, and otherwise how long a delayed launcher
// must wait for the interval to elapse.
func trustRebuildDelay(nowNano, lastNano int64) (launchNow bool, wait time.Duration) {
	elapsed := nowNano - lastNano
	if elapsed >= int64(trustRebuildMinInterval) {
		return true, 0
	}
	return false, time.Duration(int64(trustRebuildMinInterval) - elapsed)
}

// requestTrustRebuild asks for the adopted rebuild that resolves a settled
// watermark trust generation (WatermarkTrusted's liveness section), through
// a rate limiter: the first request on a quiet engine launches immediately,
// and requests inside trustRebuildMinInterval of the last launch coalesce
// into one delayed launcher that fires when the interval is up -- re-checking
// that the generations still disagree first, since an adoption during the
// wait is exactly the resolution being waited for.
//
// This exists for the one launch path that is not urgent. Genuine fallback
// recovery -- a suspect replica, every query declining -- never comes
// through here and is never delayed: enterFallback and the
// state-based relaunches call startFallbackRebuild directly. What does come
// through here is trust restoration while the engine serves correctly
// (ResolveAbandonedWrite's settle, Apply's belt-and-braces settle, and
// finishFallbackRebuild's generations-only recheck), where an unlimited
// launch rate would let a sustained stream of settling bump failures --
// e.g. a watermark table that errors while the data tables still work --
// run full snapshot loads back to back indefinitely for no serving benefit.
//
// Liveness is preserved, not traded away: every request either launches,
// coalesces into a launcher that will run within the interval and re-request
// through the same startFallbackRebuild the direct path uses, or finds the
// generations already equal -- and a launcher cut short by bgCtx belongs to
// an engine that is shutting down. The pending flag never strands: its
// holder clears it on every exit path.
//
// A disabled engine returns immediately: claimRebuildLoop would refuse the
// launch anyway (a replica nobody reads is never rebuilt), and spawning a
// delayed launcher for it would be a goroutine with nothing to ever do.
func (e *Engine) requestTrustRebuild() {
	if !e.cfg.Enabled {
		return
	}

	now := time.Now().UnixNano()
	last := e.lastTrustRebuildNano.Load()
	if launchNow, _ := trustRebuildDelay(now, last); launchNow {
		if e.lastTrustRebuildNano.CompareAndSwap(last, now) {
			e.startFallbackRebuild()
			return
		}
		// Lost the stamp to a concurrent request that is launching right
		// now; coalesce with it below exactly as an inside-the-interval
		// request would.
	}

	if !e.trustRebuildPending.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer e.trustRebuildPending.Store(false)

		// Recompute against the stamp as it is NOW -- the winner of a lost
		// CAS above has stored a fresher value than this goroutine's caller
		// read -- so the wait always measures from the actual last launch.
		_, wait := trustRebuildDelay(time.Now().UnixNano(), e.lastTrustRebuildNano.Load())

		select {
		case <-e.bgCtx.Done():
			return
		case <-time.After(wait):
		}

		if e.settledDirtyGen.Load() == e.resolvedDirtyGen.Load() {
			// Resolved while waiting -- an adoption happened, which is the
			// outcome this launcher existed to cause.
			return
		}
		e.lastTrustRebuildNano.Store(time.Now().UnixNano())
		e.startFallbackRebuild()
	}()
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
// The watermark trust generations get the identical recheck, for the
// identical race in its other guise: this goroutine's own adoption resolved
// whatever settledDirtyGen value it read before its load began
// (adoptRebuiltView), and a watermark failure that settled after that read --
// including one settled by a write that produced no effect at all, which
// never enters fallback and so would leave the state check above unmoved --
// needs another adoption to resolve it. Its own settle already called
// startFallbackRebuild, but that call finds this goroutine's flag still held
// and gives up, exactly as it should when a recovery goroutine is genuinely
// still working; here one is not. Without this second half of the recheck the
// engine would keep serving correctly but stay permanently distrusted
// (WatermarkTrusted), with nothing left scheduled that could ever change
// that.
//
// Only called from an adopted-rebuild return path, never from a
// context-cancelled return: relaunching in response to a state change the
// exiting goroutine is about to stop observing anyway, right as the engine
// is shutting down, would just start a new goroutine with nothing left to
// wait for it.
func (e *Engine) finishFallbackRebuild() {
	e.fallbackRebuilding.Store(false)
	switch relaunchAfterAdoption(e.state.Load(), e.settledDirtyGen.Load(), e.resolvedDirtyGen.Load()) {
	case relaunchRecovery:
		e.startFallbackRebuild()
	case relaunchTrust:
		e.requestTrustRebuild()
	}
}

// relaunchKind is relaunchAfterAdoption's answer: whether the rebuild
// goroutine that just cleared fallbackRebuilding must relaunch, and with
// what urgency.
type relaunchKind int

const (
	relaunchNone     relaunchKind = iota // nothing left behind
	relaunchRecovery                     // state raced back to fallback: relaunch immediately
	relaunchTrust                        // only a trust generation is unresolved: rate-limited
)

// relaunchAfterAdoption is finishFallbackRebuild's own decision, extracted
// as a pure function for unit testing (mirroring fallbackRetryDelay's
// identical treatment above): given the engine's state and its watermark
// settled/resolved generations at the moment the exiting rebuild goroutine
// cleared the flag, it reports whether another rebuild has to be launched,
// and through which path.
//
// The two conditions cover the two independent reasons an adopted rebuild
// can leave work behind -- a write that tripped fallback again in the
// window before the flag cleared, and a watermark failure that settled
// after this rebuild's own load began (and whose own request to rebuild
// therefore found the flag still held) -- see finishFallbackRebuild's doc
// for both races in full. They differ in urgency, which is why the answer
// is a kind rather than a bool: a state stuck in fallback means every query
// is declining and recovery relaunches immediately, while an unresolved
// trust generation on a serving engine only delays snapshot-file stamping
// and goes through requestTrustRebuild's rate limiter, so a sustained
// stream of settling failures cannot turn adopted rebuilds into
// back-to-back full loads.
func relaunchAfterAdoption(state int32, settledGen, resolvedGen uint64) relaunchKind {
	if state == stateFallback {
		return relaunchRecovery
	}
	if settledGen != resolvedGen {
		return relaunchTrust
	}
	return relaunchNone
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
