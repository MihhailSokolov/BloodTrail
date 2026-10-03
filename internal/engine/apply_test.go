// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Kind ids for this file's fixture, with names registered in the snapshot's
// own kind table so the criteria helpers (which resolve graph.Kind names
// through View.Kinds()) have something to resolve against.
const (
	applyKindUser     snapshot.KindID = 1
	applyKindComputer snapshot.KindID = 2
	applyKindTag      snapshot.KindID = 3
	applyKindAdminTo  snapshot.KindID = 4
	applyKindMemberOf snapshot.KindID = 5
)

func applyKindNames() map[snapshot.KindID]string {
	return map[snapshot.KindID]string{
		applyKindUser:     "User",
		applyKindComputer: "Computer",
		applyKindTag:      "Tag",
		applyKindAdminTo:  "AdminTo",
		applyKindMemberOf: "MemberOf",
	}
}

// buildApplyView builds the fixture every test below layers a segment onto:
//
//	node 1 (User, objectid "oid-1") -[10:AdminTo]-> node 2 (Computer)
//	node 2 (Computer)               -[11:MemberOf]-> node 3 (User, Tag)
//	node 3 (User, Tag)              -[12:AdminTo]-> node 1
func buildApplyView(t *testing.T) *snapshot.View {
	t.Helper()

	b := snapshot.NewBuilder(1)
	b.SetKinds(applyKindNames())

	if err := b.AddNode(1, []snapshot.KindID{applyKindUser}, []byte(`{"objectid":"oid-1"}`)); err != nil {
		t.Fatalf("AddNode(1): %v", err)
	}
	if err := b.AddNode(2, []snapshot.KindID{applyKindComputer}, nil); err != nil {
		t.Fatalf("AddNode(2): %v", err)
	}
	if err := b.AddNode(3, []snapshot.KindID{applyKindUser, applyKindTag}, nil); err != nil {
		t.Fatalf("AddNode(3): %v", err)
	}

	b.AddEdge(10, 1, 2, applyKindAdminTo)
	b.AddEdge(11, 2, 3, applyKindMemberOf)
	b.AddEdge(12, 3, 1, applyKindAdminTo)

	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(snap)
}

// requireNodeTombstoned/requireEdgeTombstoned/requireNodeUpserted are this
// file's segment assertions: what a Segment records for one id, and whether
// it records anything at all.
func requireNodeTombstoned(t *testing.T, seg *snapshot.Segment, id uint64) {
	t.Helper()
	st, ok := seg.NodeState(id)
	if !ok {
		t.Fatalf("segment carries no record for node %d, want a tombstone", id)
	}
	if !st.Tombstoned {
		t.Fatalf("segment records node %d as present, want tombstoned", id)
	}
}

func requireEdgeTombstoned(t *testing.T, seg *snapshot.Segment, id uint64) {
	t.Helper()
	st, ok := seg.EdgeState(id)
	if !ok {
		t.Fatalf("segment carries no record for edge %d, want a tombstone", id)
	}
	if !st.Tombstoned {
		t.Fatalf("segment records edge %d as present, want tombstoned", id)
	}
}

func requireNoEdgeRecord(t *testing.T, seg *snapshot.Segment, id uint64) {
	t.Helper()
	if _, ok := seg.EdgeState(id); ok {
		t.Fatalf("segment carries a record for edge %d, want none", id)
	}
}

// TestBuildApplySegmentUpsertsPresentStateAndTombstonesAbsent covers the
// read-back half of the applier: present rows become upserts carrying
// PostgreSQL's own post-write state, absent keys become tombstones, and an
// absent NODE additionally cascades to every edge incident to it in the
// current View -- without which the base snapshot's own CSR slots would keep
// those edges visible after the node they hang off is gone.
func TestBuildApplySegmentUpsertsPresentStateAndTombstonesAbsent(t *testing.T) {
	view := buildApplyView(t)

	rb := &readbackResult{
		nodes: []nodeState{
			{id: 2, kindIDs: []snapshot.KindID{applyKindComputer, applyKindTag}, propsJSON: []byte(`{"name":"after"}`)},
		},
		absentNodeIDs: []uint64{3},
		absentEdgeIDs: []uint64{10},
		resolvedKinds: map[snapshot.KindID]string{applyKindTag: "Tag"},
	}

	seg, err := buildApplySegment(view, rb)
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	st, ok := seg.NodeState(2)
	if !ok || st.Tombstoned {
		t.Fatalf("node 2 state = (%+v, %v), want a present upsert", st, ok)
	}
	if len(st.KindIDs) != 2 || st.KindIDs[0] != applyKindComputer || st.KindIDs[1] != applyKindTag {
		t.Fatalf("node 2 kinds = %v, want [%d %d]", st.KindIDs, applyKindComputer, applyKindTag)
	}
	if got, ok := st.PropValueByName("name"); !ok || got != "after" {
		t.Fatalf("node 2 name = (%v, %v), want (\"after\", true)", got, ok)
	}

	requireNodeTombstoned(t, seg, 3)
	// Node 3's cascade: edge 11 (into it) and edge 12 (out of it).
	requireEdgeTombstoned(t, seg, 11)
	requireEdgeTombstoned(t, seg, 12)
	// And the edge read-back itself reported absent.
	requireEdgeTombstoned(t, seg, 10)

	if name, ok := seg.AddedKinds()[applyKindTag]; !ok || name != "Tag" {
		t.Fatalf("segment added kinds = %v, want the newly resolved Tag kind", seg.AddedKinds())
	}
}

// TestBuildApplySegmentAbsentTriples covers both triple cases: a triple that
// still exists in the View is tombstoned by finding its edge id through the
// start node's adjacency, while a triple carrying read-back's
// unresolvedTripleKind sentinel (a kind PostgreSQL never asserted, which can
// therefore never match a real edge) records nothing at all.
func TestBuildApplySegmentAbsentTriples(t *testing.T) {
	view := buildApplyView(t)

	rb := &readbackResult{
		absentTriples: []tripleKey{
			{start: 2, end: 3, kindID: applyKindMemberOf},
			{start: 1, end: 2, kindID: unresolvedTripleKind},
			{start: 1, end: 2, kindID: applyKindMemberOf}, // right endpoints, wrong kind
			{start: 99, end: 2, kindID: applyKindAdminTo}, // endpoint unknown to the view
		},
	}

	seg, err := buildApplySegment(view, rb)
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	requireEdgeTombstoned(t, seg, 11)
	requireNoEdgeRecord(t, seg, 10)
	requireNoEdgeRecord(t, seg, 12)
}

// TestBuildApplySegmentPresentStateWinsOverCascadeTombstone pins the
// staging order buildApplySegment's own doc describes: a row read-back found
// is staged after every tombstone, so it wins over a cascade the View
// derived from an absent endpoint.
func TestBuildApplySegmentPresentStateWinsOverCascadeTombstone(t *testing.T) {
	view := buildApplyView(t)

	rb := &readbackResult{
		absentNodeIDs: []uint64{3},
		edges:         []edgeState{{id: 11, start: 2, end: 3, kindID: applyKindMemberOf}},
	}

	seg, err := buildApplySegment(view, rb)
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	requireNodeTombstoned(t, seg, 3)
	requireEdgeTombstoned(t, seg, 12) // node 3's cascade, not read back
	if st, ok := seg.EdgeState(11); !ok || st.Tombstoned {
		t.Fatalf("edge 11 state = (%+v, %v), want the present read-back row to win over node 3's cascade", st, ok)
	}
}

// requireCandidates collects view's candidates for cs and absentObjectIDs
// (the kind names in resolved stand for what resolveCriteriaKinds found in
// PostgreSQL) and asserts they are exactly wantNodes and wantEdges.
func requireCandidates(t *testing.T, view *snapshot.View, resolved map[snapshot.KindID]string, cs *ChangeSet, absentObjectIDs []string, wantNodes, wantEdges []uint64) {
	t.Helper()

	c, err := collectViewCandidates(view, resolved, cs, absentObjectIDs)
	if err != nil {
		t.Fatalf("collectViewCandidates: %v", err)
	}
	gotNodes := c.nodeIDs()
	gotEdges := append([]uint64(nil), c.edges...)
	sort.Slice(gotNodes, func(i, j int) bool { return gotNodes[i] < gotNodes[j] })
	sort.Slice(gotEdges, func(i, j int) bool { return gotEdges[i] < gotEdges[j] })
	if !reflect.DeepEqual(gotNodes, wantNodes) {
		t.Fatalf("candidate nodes = %v, want %v", gotNodes, wantNodes)
	}
	if !reflect.DeepEqual(gotEdges, wantEdges) {
		t.Fatalf("candidate edges = %v, want %v", gotEdges, wantEdges)
	}
}

// TestViewCandidatesAbsentObjectIDNamesEveryLiveNodeUnderIt: an objectid that
// matched no row makes every node the View still knows under it a candidate
// -- to re-read by id, not to tombstone: its objectid may only have been
// rewritten.
func TestViewCandidatesAbsentObjectIDNamesEveryLiveNodeUnderIt(t *testing.T) {
	view := buildApplyView(t)

	requireCandidates(t, view, nil, &ChangeSet{}, []string{"oid-1", "oid-unknown"}, []uint64{1}, nil)
}

// TestViewCandidatesNodeKindCriteria pins the node criteria's candidates to
// dawgs' own DeleteNodesByKinds rule: a node matches when its kinds overlap
// Include and do not overlap Exclude.
func TestViewCandidatesNodeKindCriteria(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("User")}, graph.Kinds{graph.StringKind("Tag")})

	// Node 1 is a User carrying no Tag; node 3 is a User but carries Tag;
	// node 2 is no User at all.
	requireCandidates(t, view, nil, cs, nil, []uint64{1}, nil)
}

// TestViewCandidatesNodeKindCriteriaEmptyIncludeMatchesEveryNode covers
// dawgs' own "when includeAny is empty, for every node" rule -- the shape
// BloodHound's "delete sourceless data" uses, where only the exclusions
// narrow the delete.
func TestViewCandidatesNodeKindCriteriaEmptyIncludeMatchesEveryNode(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(nil, graph.Kinds{graph.StringKind("Computer")})

	requireCandidates(t, view, nil, cs, nil, []uint64{1, 3}, nil)
}

// TestViewCandidatesNodeKindCriteriaUnresolvableExcludeErrors covers the one
// criteria case that cannot be replayed soundly: PostgreSQL refuses a delete
// whose exclusion names an undefined kind rather than silently widening it,
// so an exclude kind that neither this View nor read-back could resolve
// means the applier and PostgreSQL disagree about what the delete even was
// -- an error (which Apply turns into a fallback), never a guess. An
// unresolvable INCLUDE kind, by contrast, legitimately matches nothing,
// exactly as it does in PostgreSQL.
func TestViewCandidatesNodeKindCriteriaUnresolvableExcludeErrors(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("User")}, graph.Kinds{graph.StringKind("NeverAsserted")})
	if _, err := collectViewCandidates(view, nil, cs, nil); err == nil {
		t.Fatalf("collectViewCandidates with an unresolvable exclude kind = nil error, want an error")
	}

	unknownInclude := &ChangeSet{}
	unknownInclude.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("NeverAsserted")}, nil)
	requireCandidates(t, view, nil, unknownInclude, nil, nil, nil)
}

// TestViewCandidatesNodeKindCriteriaExcludeResolvedByReadBack covers
// BloodHound's "delete sourceless data" shape -- no include kinds, every
// registered source kind excluded -- when one of those source kinds was
// registered after this View's last full load and no row carries it, so
// only read-back could resolve it: the unknown kind excludes nothing (no
// node carries it), and the known one still protects its node.
func TestViewCandidatesNodeKindCriteriaExcludeResolvedByReadBack(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(nil, graph.Kinds{graph.StringKind("Tag"), graph.StringKind("RowlessSource")})

	requireCandidates(t, view, map[snapshot.KindID]string{42: "RowlessSource"}, cs, nil, []uint64{1, 2}, nil)
}

// TestViewCandidatesKindCriteriaMatchByID pins why a kind only read-back
// could resolve is safe to use: matching is by kind id against every node's
// and edge's own kind ids, not by what the View's kind table names. A node
// or edge carrying such an id -- one the table never named -- is therefore
// still excluded, included or matched exactly as PostgreSQL treats it.
func TestViewCandidatesKindCriteriaMatchByID(t *testing.T) {
	const (
		unnamedNodeKind snapshot.KindID = 42
		unnamedEdgeKind snapshot.KindID = 43
	)

	b := snapshot.NewBuilder(1)
	b.SetKinds(applyKindNames())
	if err := b.AddNode(1, []snapshot.KindID{applyKindUser}, nil); err != nil {
		t.Fatalf("AddNode(1): %v", err)
	}
	if err := b.AddNode(2, []snapshot.KindID{applyKindUser, unnamedNodeKind}, nil); err != nil {
		t.Fatalf("AddNode(2): %v", err)
	}
	b.AddEdge(10, 1, 2, applyKindAdminTo)
	b.AddEdge(11, 2, 1, unnamedEdgeKind)
	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	view := snapshot.NewView(snap)

	resolved := map[snapshot.KindID]string{
		unnamedNodeKind: "UnnamedNode",
		unnamedEdgeKind: "UnnamedEdge",
	}

	t.Run("exclude", func(t *testing.T) {
		cs := &ChangeSet{}
		cs.RecordDeleteNodesByKinds(nil, graph.Kinds{graph.StringKind("UnnamedNode")})
		requireCandidates(t, view, resolved, cs, nil, []uint64{1}, nil)
	})

	t.Run("include", func(t *testing.T) {
		cs := &ChangeSet{}
		cs.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("UnnamedNode")}, nil)
		requireCandidates(t, view, resolved, cs, nil, []uint64{2}, nil)
	})

	t.Run("relationships", func(t *testing.T) {
		cs := &ChangeSet{}
		cs.RecordDeleteRelationshipsByKinds(graph.Kinds{graph.StringKind("UnnamedEdge")})
		requireCandidates(t, view, resolved, cs, nil, nil, []uint64{11})
	})
}

// TestViewCandidatesEdgeKindCriteria covers the relationship criteria: every
// edge of the named kinds is a candidate, no other edge is, and no node is
// (deleting an edge never removes a node). An unresolvable kind name
// matches nothing, mirroring dawgs' own tolerant mapping.
func TestViewCandidatesEdgeKindCriteria(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteRelationshipsByKinds(graph.Kinds{graph.StringKind("AdminTo"), graph.StringKind("NeverAsserted")})

	requireCandidates(t, view, nil, cs, nil, nil, []uint64{10, 12})
}

// TestViewCandidatesEdgeKindCriteriaCoversEveryDeltaRecord pins the edge
// scan against an overlay: the newest delta record for an id decides it (a
// tombstoned base edge is no candidate, an upserted one is named once), and
// a delta edge OutEdges does not show -- one whose endpoint the View does not
// know yet -- is a candidate all the same, since it reappears the moment
// that endpoint lands.
func TestViewCandidatesEdgeKindCriteriaCoversEveryDeltaRecord(t *testing.T) {
	view := buildApplyView(t)

	var older snapshot.SegmentBuilder
	older.TombstoneEdge(12)
	older.AddEdgeState(20, 2, 99, applyKindAdminTo) // node 99 is unknown: dangling
	older.AddEdgeState(21, 1, 3, applyKindAdminTo)
	var newer snapshot.SegmentBuilder
	newer.AddEdgeState(10, 1, 2, applyKindAdminTo) // re-staged base edge
	newer.TombstoneEdge(21)
	view = view.WithSegment(older.Build()).WithSegment(newer.Build())

	cs := &ChangeSet{}
	cs.RecordDeleteRelationshipsByKinds(graph.Kinds{graph.StringKind("AdminTo")})

	requireCandidates(t, view, nil, cs, nil, nil, []uint64{10, 20})
}

// TestEnterFallbackFlipsStateOnceAndStopsServing covers the state half of
// the fallback: the first call flips SERVING to FALLBACK (and every serving
// gate then declines), and further calls are no-ops on an already-fallen-back
// engine. The engine here is disabled, so no recovery goroutine is started --
// startFallbackRebuild's own documented behavior, which is what lets this run
// without a database.
func TestEnterFallbackFlipsStateOnceAndStopsServing(t *testing.T) {
	e := New(nil, nil, Config{})
	e.snap.Store(snapshot.NewView(&snapshot.Snapshot{}))

	if _, ok := e.serveState(); !ok {
		t.Fatalf("serveState() before any fallback = false, want true")
	}

	e.enterFallback(context.Background(), "first")
	if got := e.state.Load(); got != stateFallback {
		t.Fatalf("state after enterFallback = %d, want stateFallback (%d)", got, stateFallback)
	}
	if _, ok := e.serveState(); ok {
		t.Fatalf("serveState() in fallback = true, want false")
	}

	e.enterFallback(context.Background(), "second")
	if got := e.state.Load(); got != stateFallback {
		t.Fatalf("state after a second enterFallback = %d, want it to stay stateFallback (%d)", got, stateFallback)
	}
	if e.fallbackRebuilding.Load() {
		t.Fatalf("a disabled engine started a fallback recovery goroutine")
	}
}

// TestApplyBumpsEpochBeforeAnyEarlyReturn pins the one thing Apply does
// before any of its early returns can fire: the epoch bump a concurrent
// rebuild's adoption check depends on (adoptRebuiltView). The engine here
// has no snapshot, so Apply returns before it would ever reach read-back.
func TestApplyBumpsEpochBeforeAnyEarlyReturn(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})

	scope := NewWriteScope()
	scope.Changes().RecordNodeID(7)

	epochBefore := e.applyEpoch.Load()

	e.Apply(context.Background(), scope)

	if got := e.applyEpoch.Load(); got != epochBefore+1 {
		t.Fatalf("applyEpoch = %d after Apply, want %d", got, epochBefore+1)
	}
	if e.state.Load() != stateServing {
		t.Fatalf("Apply with no snapshot adopted entered fallback, want it to stay serving")
	}
}

// TestApplyPanicEntersFallback: a panic part-way through Apply -- here from
// read-back, since this engine has no PostgreSQL driver at all -- must not
// leave the engine serving a replica that never received the committed
// write, and must not reach Apply's caller either: the write has already
// committed, so its caller must not be told otherwise.
//
// The rebuild-loop gate is held for the test, as nothing here could run a
// real recovery load.
func TestApplyPanicEntersFallback(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	e.snap.Store(buildApplyView(t))
	e.fallbackRebuilding.Store(true)

	scope := NewWriteScope()
	scope.Changes().RecordNodeID(7)

	var escaped any
	func() {
		defer func() { escaped = recover() }()
		e.Apply(context.Background(), scope)
	}()

	if _, serving := e.serveState(); serving {
		t.Fatalf("engine still serving after Apply panicked before replaying the write (panic: %v)", escaped)
	}
	if escaped != nil {
		t.Fatalf("Apply's panic reached its caller, whose write had already committed: %v", escaped)
	}
}

// TestApplyPanicBeforeTheEpochBumpStillStopsARacedRebuild covers the one
// window the epoch bump itself does not cover: a panic between Apply's
// watermark bookkeeping and the bump (the AdvanceWatermark/
// settleWatermarkFailure lines above it). The engine enters fallback, and a
// rebuild already loading when this write committed then finds the epoch it
// read before its load unchanged -- so it adopts a snapshot that predates
// this write, which ends the fallback and leaves the engine serving without
// the write, for as long as nothing else writes. fallBackOnApplyPanic bumps
// the epoch itself, so such a rebuild is refused and retried, exactly as it
// is for a panic anywhere later in Apply.
//
// fallBackOnApplyPanic is driven directly: nothing in that window takes an
// argument or a seam a test could make panic, and the window is two
// statements wide. The rebuild-loop gate is held, as nothing here could run
// a real recovery load.
func TestApplyPanicBeforeTheEpochBumpStillStopsARacedRebuild(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	applied := buildApplyView(t)
	e.snap.Store(applied)
	e.fallbackRebuilding.Store(true)

	// What a rebuild already in flight read before it started loading.
	epoch := e.applyEpoch.Load()

	func() {
		defer e.fallBackOnApplyPanic(context.Background())
		panic("between the watermark bookkeeping and the epoch bump")
	}()

	if _, serving := e.serveState(); serving {
		t.Fatalf("engine still serving after an Apply panic")
	}
	if got := e.applyEpoch.Load(); got == epoch {
		t.Fatalf("applyEpoch = %d after the panic, want it bumped past %d", got, epoch)
	}

	stale := snapshot.NewView(&snapshot.Snapshot{})
	if e.adoptRebuiltView(context.Background(), stale, epoch, e.settledDirtyGen.Load()) {
		t.Fatalf("a rebuild whose load predates the panicking write was adopted; the write is lost")
	}
	if got := e.snap.Load(); got != applied {
		t.Fatalf("the stale rebuild replaced the published view")
	}
	if got := e.state.Load(); got != stateFallback {
		t.Fatalf("state = %d after refusing the stale rebuild, want it to stay stateFallback (%d)", got, stateFallback)
	}
}

// -----------------------------------------------------------------------
// F4: the fallback recovery goroutine must never leave the engine stranded
// in stateFallback with nothing running to recover it.
// -----------------------------------------------------------------------

// TestFinishFallbackRebuildRelaunchesWhenStateRacedBackToFallback pins the
// exact race finishFallbackRebuild closes: the recovery goroutine has
// already decided to exit (rebuildOnce adopted, adoptRebuiltView already
// flipped state to stateServing) but has not yet cleared
// fallbackRebuilding, when some OTHER write's Apply fails and calls
// enterFallback -- flipping state back to stateFallback and finding
// fallbackRebuilding still true, so its own startFallbackRebuild call gives
// up silently (a recovery goroutine looks like it is already in flight).
// Without the fix, clearing the flag afterward would leave the engine
// stranded: state == stateFallback with no goroutine ever again scheduled
// to notice.
//
// bgCancel is called first so that IF this relaunches a real goroutine (it
// must), that goroutine's own top-of-loop bgCtx check returns immediately
// instead of reaching rebuildOnce -- which would call LoadSnapshot against
// this test's nil pgDriver/pool and panic.
//
// The relaunch is asserted through rebuildLoopStarts (engine.go), not
// fallbackRebuilding: the CAS that proves a relaunch happened runs in this
// goroutine, synchronously, before the "go" statement -- but the spawned
// goroutine, finding bgCtx already cancelled, immediately clears
// fallbackRebuilding and returns. That clear races the assertion with no
// ordering guarantee between them (the Go memory model gives none for two
// unsynchronized atomics touched from different goroutines), so asserting
// the flag itself is flaky: on an unlucky schedule the spawned goroutine's
// own Store(false) can complete before this goroutine's next statement
// runs. rebuildLoopStarts only ever increases and is bumped in this
// goroutine before the spawn, so comparing it against its value from
// before the call is race-free regardless of how the spawned goroutine is
// scheduled.
func TestFinishFallbackRebuildRelaunchesWhenStateRacedBackToFallback(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.bgCancel()

	e.fallbackRebuilding.Store(true) // the "not yet cleared" half of the race
	e.state.Store(stateFallback)     // ...and a concurrent Apply already lost it back to fallback

	startsBefore := e.rebuildLoopStarts.Load()

	e.finishFallbackRebuild()

	if got := e.rebuildLoopStarts.Load(); got != startsBefore+1 {
		t.Fatalf("finishFallbackRebuild left the engine stranded: rebuildLoopStarts = %d, want %d (state = stateFallback should have relaunched recovery exactly once)", got, startsBefore+1)
	}
}

// TestFinishFallbackRebuildDoesNothingWhenAlreadyServing covers the normal,
// non-races path: when the state is stateServing by the time
// finishFallbackRebuild runs (the common case -- nothing raced), it must
// just clear the flag, not spawn another goroutine.
func TestFinishFallbackRebuildDoesNothingWhenAlreadyServing(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})

	e.fallbackRebuilding.Store(true)
	e.state.Store(stateServing)

	e.finishFallbackRebuild()

	if e.fallbackRebuilding.Load() {
		t.Fatalf("finishFallbackRebuild relaunched recovery while already serving, want it to just clear the flag")
	}
}

// TestApplyEarlyReturnRelaunchesRecoveryWhenNoneIsRunning is F4's other half:
// Apply's state != stateServing early return must itself call
// startFallbackRebuild before returning, so that if the engine is
// (unexpectedly) in fallback with no recovery goroutine actually in flight
// -- fallbackRebuilding left false here, standing in for whatever window
// left it that way -- the very next declined write self-heals instead of
// declining forever.
//
// bgCancel is called for the same reason as
// TestFinishFallbackRebuildRelaunchesWhenStateRacedBackToFallback: it lets
// the relaunched goroutine's own top-of-loop check return immediately
// rather than reach rebuildOnce with no real database behind it.
//
// The relaunch is asserted through rebuildLoopStarts rather than
// fallbackRebuilding, for the same reason given in that sibling test's doc:
// the spawned goroutine clears fallbackRebuilding again the instant it
// observes bgCtx already cancelled, and nothing orders that clear after
// this goroutine's assertion, so the flag itself is flaky under -race on a
// loaded scheduler. rebuildLoopStarts is bumped synchronously by the
// winning CAS, in this goroutine, before Apply ever returns, and never
// moves backwards, so it proves the relaunch happened regardless of when
// the spawned goroutine runs.
func TestApplyEarlyReturnRelaunchesRecoveryWhenNoneIsRunning(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.bgCancel()
	e.state.Store(stateFallback)

	scope := NewWriteScope()
	scope.Changes().RecordNodeID(7)

	startsBefore := e.rebuildLoopStarts.Load()

	e.Apply(context.Background(), scope)

	if got := e.rebuildLoopStarts.Load(); got != startsBefore+1 {
		t.Fatalf("Apply's early return while already in fallback did not relaunch recovery: rebuildLoopStarts = %d, want %d", got, startsBefore+1)
	}
}

// -----------------------------------------------------------------------
// Trust-only rebuild launches are rate-limited (requestTrustRebuild):
// a sustained stream of settling bump failures must not run full snapshot
// loads back to back on an engine that is serving correctly.
// -----------------------------------------------------------------------

// TestTrustRebuildDelay pins the limiter's pure timing decision: launch
// immediately once trustRebuildMinInterval has elapsed since the last
// launch (a never-launched engine's zero stamp trivially qualifies), and
// otherwise wait exactly the remainder.
func TestTrustRebuildDelay(t *testing.T) {
	if launchNow, wait := trustRebuildDelay(int64(trustRebuildMinInterval), 0); !launchNow || wait != 0 {
		t.Fatalf("trustRebuildDelay(interval, 0) = (%v, %v), want (true, 0)", launchNow, wait)
	}
	if launchNow, wait := trustRebuildDelay(1, 0); launchNow || wait != trustRebuildMinInterval-1 {
		t.Fatalf("trustRebuildDelay(1, 0) = (%v, %v), want (false, interval-1ns)", launchNow, wait)
	}
	now := int64(10 * trustRebuildMinInterval)
	if launchNow, wait := trustRebuildDelay(now, now-int64(trustRebuildMinInterval)/2); launchNow || wait != trustRebuildMinInterval/2 {
		t.Fatalf("trustRebuildDelay(now, now-interval/2) = (%v, %v), want (false, interval/2)", launchNow, wait)
	}
}

// TestRequestTrustRebuildLaunchesImmediatelyWhenQuiet pins the common case:
// nothing has launched within the interval (the zero stamp of a fresh
// engine), so the request launches recovery right away -- asserted through
// rebuildLoopStarts for the same race-freedom reason the F4 tests give.
// bgCancel keeps the spawned loop from ever reaching a nil database.
func TestRequestTrustRebuildLaunchesImmediatelyWhenQuiet(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.bgCancel()
	e.settledDirtyGen.Store(1) // one settled failure, unresolved

	startsBefore := e.rebuildLoopStarts.Load()
	e.requestTrustRebuild()
	if got := e.rebuildLoopStarts.Load(); got != startsBefore+1 {
		t.Fatalf("rebuildLoopStarts = %d after a quiet-engine trust request, want %d (should launch immediately)", got, startsBefore+1)
	}
}

// TestRequestTrustRebuildCoalescesInsideTheInterval pins the limit itself:
// with the last launch stamped just now, further requests must not launch
// another loop synchronously -- they hand off to the one delayed launcher
// (whose own firing is untestable here without waiting out the interval;
// bgCancel makes it exit promptly instead, which is also what proves the
// synchronous path launched nothing).
func TestRequestTrustRebuildCoalescesInsideTheInterval(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.bgCancel()
	e.settledDirtyGen.Store(1)
	e.lastTrustRebuildNano.Store(time.Now().UnixNano())

	startsBefore := e.rebuildLoopStarts.Load()
	e.requestTrustRebuild()
	e.requestTrustRebuild()
	e.requestTrustRebuild()
	if got := e.rebuildLoopStarts.Load(); got != startsBefore {
		t.Fatalf("rebuildLoopStarts = %d after requests inside the interval, want %d (no synchronous launch)", got, startsBefore)
	}
}

// TestRequestTrustRebuildDisabledEngineIsANoOp: a disabled engine never
// rebuilds (claimRebuildLoop), so the limiter must not even spawn its
// delayed launcher for one.
func TestRequestTrustRebuildDisabledEngineIsANoOp(t *testing.T) {
	e := New(nil, nil, Config{})
	e.settledDirtyGen.Store(1)

	e.requestTrustRebuild()
	if e.rebuildLoopStarts.Load() != 0 {
		t.Fatalf("a disabled engine launched a trust rebuild")
	}
	if e.trustRebuildPending.Load() {
		t.Fatalf("a disabled engine armed a delayed trust launcher")
	}
}

// TestFinishFallbackRebuildRateLimitsTrustOnlyRelaunch pins the routing:
// an adopted exit that leaves only a trust generation behind goes through
// the limiter (no synchronous relaunch inside the interval; an immediate
// one when quiet), while the raced-back-to-fallback case in the F4 test
// above stays immediate unconditionally.
func TestFinishFallbackRebuildRateLimitsTrustOnlyRelaunch(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.bgCancel()
	e.state.Store(stateServing)
	e.settledDirtyGen.Store(1) // settled after the exiting rebuild's load began
	e.fallbackRebuilding.Store(true)

	// Inside the interval: no synchronous relaunch.
	e.lastTrustRebuildNano.Store(time.Now().UnixNano())
	startsBefore := e.rebuildLoopStarts.Load()
	e.finishFallbackRebuild()
	if got := e.rebuildLoopStarts.Load(); got != startsBefore {
		t.Fatalf("rebuildLoopStarts = %d after a trust-only relaunch inside the interval, want %d (rate-limited)", got, startsBefore)
	}

	// Quiet: the same relaunch fires immediately.
	e.lastTrustRebuildNano.Store(0)
	e.fallbackRebuilding.Store(true)
	startsBefore = e.rebuildLoopStarts.Load()
	e.finishFallbackRebuild()
	if got := e.rebuildLoopStarts.Load(); got != startsBefore+1 {
		t.Fatalf("rebuildLoopStarts = %d after a quiet trust-only relaunch, want %d", got, startsBefore+1)
	}
}

// -----------------------------------------------------------------------
// F5: a budget-refused rebuild must back off far slower than a transient
// failure or epoch race.
// -----------------------------------------------------------------------

func TestFallbackRetryDelay(t *testing.T) {
	// A budget refusal always waits fallbackBudgetRetryInterval and leaves
	// backoff untouched, regardless of what backoff was carrying.
	if wait, next := fallbackRetryDelay(true, fallbackRetryInterval); wait != fallbackBudgetRetryInterval || next != fallbackRetryInterval {
		t.Fatalf("fallbackRetryDelay(true, fallbackRetryInterval) = (%v, %v), want (%v, %v)", wait, next, fallbackBudgetRetryInterval, fallbackRetryInterval)
	}
	if wait, next := fallbackRetryDelay(true, fallbackRetryMax); wait != fallbackBudgetRetryInterval || next != fallbackRetryMax {
		t.Fatalf("fallbackRetryDelay(true, fallbackRetryMax) = (%v, %v), want (%v, %v)", wait, next, fallbackBudgetRetryInterval, fallbackRetryMax)
	}

	// A non-budget outcome keeps the pre-existing doubling schedule: wait
	// the current backoff, then double it, capped at fallbackRetryMax.
	if wait, next := fallbackRetryDelay(false, fallbackRetryInterval); wait != fallbackRetryInterval || next != 2*fallbackRetryInterval {
		t.Fatalf("fallbackRetryDelay(false, fallbackRetryInterval) = (%v, %v), want (%v, %v)", wait, next, fallbackRetryInterval, 2*fallbackRetryInterval)
	}
	if wait, next := fallbackRetryDelay(false, fallbackRetryMax); wait != fallbackRetryMax || next != fallbackRetryMax {
		t.Fatalf("fallbackRetryDelay(false, fallbackRetryMax) = (%v, %v), want (%v, %v): must stay capped", wait, next, fallbackRetryMax, fallbackRetryMax)
	}
	// A near-max backoff doubles past fallbackRetryMax and must clamp, not
	// overshoot.
	if wait, next := fallbackRetryDelay(false, fallbackRetryMax-1); wait != fallbackRetryMax-1 || next != fallbackRetryMax {
		t.Fatalf("fallbackRetryDelay(false, fallbackRetryMax-1) = (%v, %v), want (%v, %v)", wait, next, fallbackRetryMax-1, fallbackRetryMax)
	}

	// The moment a budget episode ends, the very next non-budget call sees
	// backoff exactly as it was before the episode began -- proving the
	// episode never advanced it.
	preEpisode := fallbackRetryInterval
	_, duringEpisode := fallbackRetryDelay(true, preEpisode)
	_, stillDuringEpisode := fallbackRetryDelay(true, duringEpisode)
	if duringEpisode != preEpisode || stillDuringEpisode != preEpisode {
		t.Fatalf("a budget episode advanced backoff: got %v then %v, want both to stay %v", duringEpisode, stillDuringEpisode, preEpisode)
	}
	if wait, _ := fallbackRetryDelay(false, stillDuringEpisode); wait != preEpisode {
		t.Fatalf("the first non-budget retry after a budget episode waited %v, want the pre-episode backoff %v", wait, preEpisode)
	}
}
