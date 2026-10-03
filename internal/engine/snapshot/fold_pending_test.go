// SPDX-License-Identifier: Apache-2.0
package snapshot

import "testing"

// TestFoldWithPendingEdgesSortsDeltaEdgesByTheirEndpoints covers the three
// fates a delta edge can meet in a fold, over the overlay fixture (base
// nodes 10..60): both endpoints live -> folded into the base; an endpoint
// the delta tombstones -> dropped; an endpoint neither the base nor the
// delta knows -> carried out as a pending edge, and nowhere in the base.
func TestFoldWithPendingEdgesSortsDeltaEdgesByTheirEndpoints(t *testing.T) {
	base, _ := buildOverlayFixture(t)

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 70, []KindID{1}, `{"objectid":"S-obj-70"}`)
	sb.TombstoneNode(50)
	sb.AddEdgeState(2001, 10, 70, 5)   // live: base node -> delta node
	sb.AddEdgeState(2002, 10, 50, 5)   // gone: 50 is tombstoned
	sb.AddEdgeState(2003, 10, 900, 5)  // pending: 900 is known nowhere
	sb.AddEdgeState(2004, 901, 902, 6) // pending: neither endpoint known
	sb.AddEdgeState(2005, 50, 903, 6)  // gone beats pending
	seg := sb.Build()

	folded, pending, err := FoldWithPendingEdges(base, []*Segment{seg})
	if err != nil {
		t.Fatalf("FoldWithPendingEdges: %v", err)
	}

	if _, ok := folded.EdgeByID(2001); !ok {
		t.Error("live delta edge 2001 missing from the folded base")
	}
	for _, id := range []uint64{2002, 2003, 2004, 2005} {
		if _, ok := folded.EdgeByID(id); ok {
			t.Errorf("edge %d folded into the base, but an endpoint is not live", id)
		}
	}

	if pending == nil {
		t.Fatal("pending = nil, want edges 2003 and 2004")
	}
	if pending.NodeCount() != 0 {
		t.Errorf("pending segment carries %d node records, want only edges", pending.NodeCount())
	}
	want := map[uint64]EdgeSegState{
		2003: {StartID: 10, EndID: 900, Kind: 5},
		2004: {StartID: 901, EndID: 902, Kind: 6},
	}
	got := map[uint64]EdgeSegState{}
	pending.IterEdges(func(id uint64, st EdgeSegState) bool {
		got[id] = st
		return true
	})
	if len(got) != len(want) {
		t.Fatalf("pending edges = %v, want %v", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("pending edge %d = %+v, want %+v", id, got[id], w)
		}
	}

	// Fold keeps its old contract: the same fold, pending edges dropped.
	plain, err := Fold(base, []*Segment{seg})
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	compareViewContents(t, NewView(plain), NewView(folded))
}

// TestFoldWithPendingEdgesNoneWhenEveryEdgeResolves pins the ordinary case:
// a delta whose edges all resolve leaves nothing pending, so the compactor
// can publish the folded base with no segment on top.
func TestFoldWithPendingEdgesNoneWhenEveryEdgeResolves(t *testing.T) {
	base, _ := buildOverlayFixture(t)

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 70, []KindID{1}, `{}`)
	sb.AddEdgeState(2001, 70, 10, 5)
	sb.TombstoneEdge(1001)

	_, pending, err := FoldWithPendingEdges(base, []*Segment{sb.Build()})
	if err != nil {
		t.Fatalf("FoldWithPendingEdges: %v", err)
	}
	if pending != nil {
		t.Fatalf("pending = %d edges, want nil", pending.EdgeCount())
	}
}

// TestFoldedBasePlusPendingEdgesMatchesTheStack is the property the
// compactor relies on: the folded base with the pending edges and then a
// later segment on top is the same graph as the never-folded stack with that
// later segment on top -- here the segment that finally delivers a pending
// edge's endpoint, which must make the edge appear.
func TestFoldedBasePlusPendingEdgesMatchesTheStack(t *testing.T) {
	base, _ := buildOverlayFixture(t)

	early := &SegmentBuilder{}
	early.AddEdgeState(2003, 10, 900, 5)
	earlySeg := early.Build()

	late := &SegmentBuilder{}
	mustAddNodeState(t, late, 900, []KindID{2}, `{"objectid":"S-obj-900"}`)
	lateSeg := late.Build()

	folded, pending, err := FoldWithPendingEdges(base, []*Segment{earlySeg})
	if err != nil {
		t.Fatalf("FoldWithPendingEdges: %v", err)
	}
	if pending == nil {
		t.Fatal("pending = nil, want edge 2003")
	}

	carried := NewView(folded).WithSegment(MergeSegments([]*Segment{pending, lateSeg}))
	stacked := NewView(base).WithSegment(earlySeg).WithSegment(lateSeg)

	if _, _, _, ok := carried.EdgeStateByID(2003); !ok {
		t.Fatal("edge 2003 not visible once its endpoint arrived on top of the folded base")
	}
	compareViewContents(t, carried, stacked)
	if err := CheckViewConsistent(carried); err != nil {
		t.Fatalf("CheckViewConsistent: %v", err)
	}
}
