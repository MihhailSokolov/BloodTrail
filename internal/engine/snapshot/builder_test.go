// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"reflect"
	"testing"
)

func TestBuilderBuildsSnapshot(t *testing.T) {
	// nodes: 100(kinds 1), 200(kinds 1,2), 300(kinds 2), 400(kinds 1)
	// edges: 100->200 k10, 100->300 k11, 200->300 k10, 400->100 k10, 100->200 k11
	b := NewBuilder(7)
	for _, n := range []struct {
		id    uint64
		kinds []KindID
	}{
		{100, []KindID{1}}, {200, []KindID{1, 2}}, {300, []KindID{2}}, {400, []KindID{1}},
	} {
		if err := b.AddNode(n.id, n.kinds); err != nil {
			t.Fatal(err)
		}
	}
	b.AddEdge(1, 100, 300, 11)
	b.AddEdge(2, 400, 100, 10)
	b.AddEdge(3, 100, 200, 10)
	b.AddEdge(4, 200, 300, 10)
	b.AddEdge(5, 100, 200, 11)
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if got := s.NodeCount(); got != 4 {
		t.Fatalf("NodeCount() = %d, want 4", got)
	}
	if got := s.EdgeCount(); got != 5 {
		t.Fatalf("EdgeCount() = %d, want 5", got)
	}
	wantGraphIDs := []uint64{100, 200, 300, 400}
	if !reflect.DeepEqual(s.GraphIDs, wantGraphIDs) {
		t.Fatalf("GraphIDs = %v, want %v", s.GraphIDs, wantGraphIDs)
	}

	if id, ok := s.Dense(300); !ok || id != 2 {
		t.Fatalf("Dense(300) = (%d, %v), want (2, true)", id, ok)
	}
	if _, ok := s.Dense(150); ok {
		t.Fatalf("Dense(150) found, want not found")
	}

	targets, kinds := s.Out(0)
	if !reflect.DeepEqual(targets, []NodeID{1, 1, 2}) {
		t.Fatalf("Out(0) targets = %v, want [1 1 2]", targets)
	}
	if !reflect.DeepEqual(kinds, []KindID{10, 11, 11}) {
		t.Fatalf("Out(0) kinds = %v, want [10 11 11]", kinds)
	}

	inTargets, inKinds := s.In(2)
	if !reflect.DeepEqual(inTargets, []NodeID{0, 1}) {
		t.Fatalf("In(2) targets = %v, want [0 1]", inTargets)
	}
	if !reflect.DeepEqual(inKinds, []KindID{11, 10}) {
		t.Fatalf("In(2) kinds = %v, want [11 10]", inKinds)
	}

	wantOutOffsets := []uint64{0, 3, 4, 4, 5}
	if !reflect.DeepEqual(s.OutOffsets, wantOutOffsets) {
		t.Fatalf("OutOffsets = %v, want %v", s.OutOffsets, wantOutOffsets)
	}
	wantInOffsets := []uint64{0, 1, 3, 5, 5}
	if !reflect.DeepEqual(s.InOffsets, wantInOffsets) {
		t.Fatalf("InOffsets = %v, want %v", s.InOffsets, wantInOffsets)
	}

	k1 := s.NodesOfKind(1)
	for _, want := range []NodeID{0, 1, 3} {
		if !k1.Has(want) {
			t.Fatalf("NodesOfKind(1) missing dense %d", want)
		}
	}
	if k1.Count() != 3 {
		t.Fatalf("NodesOfKind(1).Count() = %d, want 3", k1.Count())
	}

	k2 := s.NodesOfKind(2)
	for _, want := range []NodeID{1, 2} {
		if !k2.Has(want) {
			t.Fatalf("NodesOfKind(2) missing dense %d", want)
		}
	}
	if k2.Count() != 2 {
		t.Fatalf("NodesOfKind(2).Count() = %d, want 2", k2.Count())
	}

	k99 := s.NodesOfKind(99)
	if k99.Count() != 0 {
		t.Fatalf("NodesOfKind(99).Count() = %d, want 0", k99.Count())
	}

	if s.MaxKindID != 11 {
		t.Fatalf("MaxKindID = %d, want 11", s.MaxKindID)
	}
	if s.ApproxBytes() == 0 {
		t.Fatal("ApproxBytes() = 0, want > 0")
	}
}

func TestAddNodeOutOfOrder(t *testing.T) {
	b := NewBuilder(1)
	if err := b.AddNode(100, []KindID{1}); err != nil {
		t.Fatalf("AddNode(100) unexpected error: %v", err)
	}
	if err := b.AddNode(100, []KindID{1}); err == nil {
		t.Fatal("AddNode(100) again: want error for non-ascending id, got nil")
	}
	if err := b.AddNode(50, []KindID{1}); err == nil {
		t.Fatal("AddNode(50) after 100: want error for non-ascending id, got nil")
	}
}

// mustAddNode stages a node via AddNode and fails the test immediately if
// staging it errors, so fixture setup in the tests below reads as a flat
// sequence of calls rather than a chain of if-err checks.
func mustAddNode(t *testing.T, b *Builder, databaseID uint64, kinds []KindID) {
	t.Helper()
	if err := b.AddNode(databaseID, kinds); err != nil {
		t.Fatalf("AddNode(%d): %v", databaseID, err)
	}
}

// TestBuildCarriesEdgeIDs checks that each edge's database id survives Build
// aligned with its forward-CSR slot (OutEdgeIDs), and that a reverse-CSR
// slot's InEdgeIdx resolves back to the correct forward slot (and therefore
// the correct id) -- not just that the ids appear somewhere in the
// snapshot. It also exercises EdgeByID end to end, including the
// not-found case.
func TestBuildCarriesEdgeIDs(t *testing.T) {
	b := NewBuilder(1)
	mustAddNode(t, b, 10, []KindID{1})
	mustAddNode(t, b, 20, []KindID{1})
	mustAddNode(t, b, 30, []KindID{2})
	b.AddEdge(100, 10, 20, 5)
	b.AddEdge(101, 10, 30, 6)
	b.AddEdge(102, 30, 10, 5)

	s, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	// Forward slots aligned: node 10's out edges sorted by (target, kind).
	targets, kinds := s.Out(0)
	if len(targets) != 2 || s.OutEdgeIDs[s.OutOffsets[0]] != 100 || s.OutEdgeIDs[s.OutOffsets[0]+1] != 101 {
		t.Fatalf("forward edge ids misaligned: targets=%v kinds=%v ids=%v", targets, kinds, s.OutEdgeIDs)
	}
	// Reverse slot for node 10 (dense 0) points back at edge 102's forward slot.
	inLo := s.InOffsets[0]
	if got := s.OutEdgeIDs[s.InEdgeIdx[inLo]]; got != 102 {
		t.Fatalf("reverse slot resolves edge id %d, want 102", got)
	}
	// EdgeByID finds every edge and rejects unknowns.
	for _, id := range []uint64{100, 101, 102} {
		if fwd, ok := s.EdgeByID(id); !ok || s.OutEdgeIDs[fwd] != id {
			t.Fatalf("EdgeByID(%d) = (%d, %v)", id, fwd, ok)
		}
	}
	if _, ok := s.EdgeByID(999); ok {
		t.Fatal("EdgeByID(999) should not resolve")
	}
}

// TestBuildDropsDanglingEdges checks that Build tolerates edges referencing
// an unstaged node id -- on either end -- by dropping them and counting
// them in DroppedEdges, rather than failing the whole build.
func TestBuildDropsDanglingEdges(t *testing.T) {
	b := NewBuilder(1)
	mustAddNode(t, b, 10, []KindID{1})
	b.AddEdge(100, 10, 999, 5) // end node never added
	b.AddEdge(101, 999, 10, 5) // start node never added
	s, err := b.Build()
	if err != nil {
		t.Fatalf("dangling edges must be dropped, not fail the build: %v", err)
	}
	if s.EdgeCount() != 0 || s.DroppedEdges != 2 {
		t.Fatalf("EdgeCount=%d DroppedEdges=%d, want 0 and 2", s.EdgeCount(), s.DroppedEdges)
	}
}

// TestBuildEdgeIDsWithSegmentReorder exercises a forward segment sort that
// actually reorders elements (performs real Swaps) while verifying that
// OutEdgeIDs remain correctly aligned with OutTargets. This test would fail
// if forwardSegment.Swap forgot to swap the edgeIDs slice.
func TestBuildEdgeIDsWithSegmentReorder(t *testing.T) {
	b := NewBuilder(1)
	mustAddNode(t, b, 10, []KindID{1})
	mustAddNode(t, b, 20, []KindID{1})
	mustAddNode(t, b, 30, []KindID{1})
	// Add edges from node 10 in OUT-OF-ORDER fashion (by target density):
	// Dense node IDs: 10->0, 20->1, 30->2
	// Edges added: target=2, then target=1 (descending, will be reordered by sort)
	b.AddEdge(400, 10, 30, 5) // 10->30 (target dense id 2)
	b.AddEdge(401, 10, 20, 5) // 10->20 (target dense id 1)
	// After sort by (target, kind), should be ordered as:
	// edge 401->20, edge 400->30

	s, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}

	// Verify that the forward segment for node 10 (dense 0) is sorted by target.
	targets, kinds := s.Out(0)
	if !reflect.DeepEqual(targets, []NodeID{1, 2}) {
		t.Fatalf("Out(0) targets = %v, want [1 2]", targets)
	}
	if !reflect.DeepEqual(kinds, []KindID{5, 5}) {
		t.Fatalf("Out(0) kinds = %v, want [5 5]", kinds)
	}

	// Verify that OutEdgeIDs are correctly aligned with targets.
	// After the segment sort reordered the (target, kind, edgeID) triplets,
	// the edgeID should still correspond to the correct target.
	outLo, outHi := s.OutOffsets[0], s.OutOffsets[1]
	if outHi-outLo != 2 {
		t.Fatalf("Out(0) has %d edges, want 2", outHi-outLo)
	}
	// outLo+0 should be edge 401 (target 1, i.e., node 20)
	// outLo+1 should be edge 400 (target 2, i.e., node 30)
	if s.OutEdgeIDs[outLo] != 401 || s.OutEdgeIDs[outLo+1] != 400 {
		t.Fatalf("OutEdgeIDs for node 10 = [%d, %d], want [401, 400]", s.OutEdgeIDs[outLo], s.OutEdgeIDs[outLo+1])
	}

	// Verify EdgeByID can find both edges.
	for id, wantTarget := range map[uint64]NodeID{401: 1, 400: 2} {
		fwd, ok := s.EdgeByID(id)
		if !ok {
			t.Fatalf("EdgeByID(%d) not found", id)
		}
		gotTarget := s.OutTargets[fwd]
		if gotTarget != wantTarget {
			t.Fatalf("EdgeByID(%d) points to target %d, want %d", id, gotTarget, wantTarget)
		}
	}
}
