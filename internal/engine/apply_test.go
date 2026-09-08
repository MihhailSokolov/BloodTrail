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

// TestApplyBumpsEpochAndKeepsMarksBookkeeping pins the two things Apply does
// before any of its early returns can apply: the epoch bump a concurrent
// rebuild's adoption check depends on (adoptRebuiltView), and the interim
// generation/marks bookkeeping the poller still reads. The engine here has
// no snapshot, so Apply returns before it would ever reach read-back.
func TestApplyBumpsEpochAndKeepsMarksBookkeeping(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})

	scope := NewWriteScope()
	scope.TouchNodeKinds(graph.Kinds{graph.StringKind("User")})
	scope.Changes().RecordNodeID(7)

	epochBefore := e.applyEpoch.Load()
	genBefore := e.Generation()

	e.Apply(context.Background(), scope)

	if got := e.applyEpoch.Load(); got != epochBefore+1 {
		t.Fatalf("applyEpoch = %d after Apply, want %d", got, epochBefore+1)
	}
	if got := e.Generation(); got != genBefore+1 {
		t.Fatalf("Generation = %d after Apply, want %d", got, genBefore+1)
	}
	if e.state.Load() != stateServing {
		t.Fatalf("Apply with no snapshot adopted entered fallback, want it to stay serving")
	}
}
