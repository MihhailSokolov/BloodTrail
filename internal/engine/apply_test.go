// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"

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

	seg, err := buildApplySegment(view, rb, &ChangeSet{})
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

// TestBuildApplySegmentAbsentObjectIDTombstonesEveryMatch covers the
// objectid half of read-back's absence reporting: an objectid that matched
// no row after the write means every node the View still knows under it is
// gone, cascade included.
func TestBuildApplySegmentAbsentObjectIDTombstonesEveryMatch(t *testing.T) {
	view := buildApplyView(t)

	rb := &readbackResult{absentObjectIDs: []string{"oid-1"}}

	seg, err := buildApplySegment(view, rb, &ChangeSet{})
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	requireNodeTombstoned(t, seg, 1)
	requireEdgeTombstoned(t, seg, 10) // out of node 1
	requireEdgeTombstoned(t, seg, 12) // into node 1
	requireNoEdgeRecord(t, seg, 11)   // untouched by node 1's cascade
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

	seg, err := buildApplySegment(view, rb, &ChangeSet{})
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	requireEdgeTombstoned(t, seg, 11)
	requireNoEdgeRecord(t, seg, 10)
	requireNoEdgeRecord(t, seg, 12)
}

// TestBuildApplySegmentNodeKindCriteria pins the criteria replay to dawgs'
// own DeleteNodesByKinds rule: a node is deleted when its kinds overlap
// Include and do not overlap Exclude, and every deleted node cascades to its
// edges.
func TestBuildApplySegmentNodeKindCriteria(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("User")}, graph.Kinds{graph.StringKind("Tag")})

	seg, err := buildApplySegment(view, &readbackResult{}, cs)
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	// Node 1 is a User and carries no Tag: deleted, cascading to edges 10/12.
	requireNodeTombstoned(t, seg, 1)
	requireEdgeTombstoned(t, seg, 10)
	requireEdgeTombstoned(t, seg, 12)

	// Node 3 is a User too, but carries Tag: excluded, so it survives.
	if _, ok := seg.NodeState(3); ok {
		t.Fatalf("node 3 was tombstoned despite carrying the excluded Tag kind")
	}
	// Node 2 is not a User at all.
	if _, ok := seg.NodeState(2); ok {
		t.Fatalf("node 2 was tombstoned despite not carrying the included User kind")
	}
	requireNoEdgeRecord(t, seg, 11)
}

// TestBuildApplySegmentNodeKindCriteriaEmptyIncludeMatchesEveryNode covers
// dawgs' own "when includeAny is empty, for every node" rule -- the shape
// BloodHound's guarded database wipe uses, where only the exclusions narrow
// the delete.
func TestBuildApplySegmentNodeKindCriteriaEmptyIncludeMatchesEveryNode(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(nil, graph.Kinds{graph.StringKind("Computer")})

	seg, err := buildApplySegment(view, &readbackResult{}, cs)
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	requireNodeTombstoned(t, seg, 1)
	requireNodeTombstoned(t, seg, 3)
	if _, ok := seg.NodeState(2); ok {
		t.Fatalf("node 2 was tombstoned despite carrying the excluded Computer kind")
	}
}

// TestBuildApplySegmentNodeKindCriteriaUnknownExcludeErrors covers the one
// criteria case that cannot be replayed soundly: PostgreSQL refuses a delete
// whose exclusion names an undefined kind rather than silently widening it,
// so an exclude kind this View cannot resolve means the two disagree about
// what the delete even was -- an error (which Apply turns into a fallback),
// never a guess. An unknown INCLUDE kind, by contrast, legitimately matches
// nothing, exactly as it does in PostgreSQL.
func TestBuildApplySegmentNodeKindCriteriaUnknownExcludeErrors(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("User")}, graph.Kinds{graph.StringKind("NeverAsserted")})

	if _, err := buildApplySegment(view, &readbackResult{}, cs); err == nil {
		t.Fatalf("buildApplySegment with an unresolvable exclude kind = nil error, want an error")
	}

	unknownInclude := &ChangeSet{}
	unknownInclude.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("NeverAsserted")}, nil)

	seg, err := buildApplySegment(view, &readbackResult{}, unknownInclude)
	if err != nil {
		t.Fatalf("buildApplySegment with an unresolvable include kind: %v", err)
	}
	if seg.NodeCount() != 0 || seg.EdgeCount() != 0 {
		t.Fatalf("an unresolvable include kind tombstoned %d nodes / %d edges, want none", seg.NodeCount(), seg.EdgeCount())
	}
}

// TestBuildApplySegmentEdgeKindCriteria covers the relationship-delete
// replay: every edge of the named kinds is tombstoned, no other edge is, and
// no node is (deleting an edge never removes a node). An unresolvable kind
// name matches nothing, mirroring dawgs' own tolerant mapping.
func TestBuildApplySegmentEdgeKindCriteria(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteRelationshipsByKinds(graph.Kinds{graph.StringKind("AdminTo"), graph.StringKind("NeverAsserted")})

	seg, err := buildApplySegment(view, &readbackResult{}, cs)
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	requireEdgeTombstoned(t, seg, 10)
	requireEdgeTombstoned(t, seg, 12)
	requireNoEdgeRecord(t, seg, 11)
	if seg.NodeCount() != 0 {
		t.Fatalf("a relationship-kind delete tombstoned %d nodes, want none", seg.NodeCount())
	}
}

// TestBuildApplySegmentPresentStateWinsOverCriteriaTombstone pins the
// staging order buildApplySegment's own doc describes: read-back's present
// rows are PostgreSQL's post-commit truth, so they are staged last and win
// over any tombstone derived from the (possibly already stale) View.
func TestBuildApplySegmentPresentStateWinsOverCriteriaTombstone(t *testing.T) {
	view := buildApplyView(t)

	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("User")}, nil)

	rb := &readbackResult{
		nodes: []nodeState{{id: 1, kindIDs: []snapshot.KindID{applyKindUser}, propsJSON: []byte(`{"objectid":"oid-1"}`)}},
	}

	seg, err := buildApplySegment(view, rb, cs)
	if err != nil {
		t.Fatalf("buildApplySegment: %v", err)
	}

	st, ok := seg.NodeState(1)
	if !ok || st.Tombstoned {
		t.Fatalf("node 1 state = (%+v, %v), want the present read-back row to win over the criteria tombstone", st, ok)
	}
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
