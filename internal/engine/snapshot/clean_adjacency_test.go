// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"math/rand"
	"testing"
)

type adjEdge struct {
	other NodeID
	kind  KindID
}

func collectOut(v *View, n NodeID) []adjEdge {
	var out []adjEdge
	v.OutEdges(n, func(w NodeID, k KindID, _ uint64) bool {
		out = append(out, adjEdge{w, k})
		return true
	})
	return out
}

func collectIn(v *View, n NodeID) []adjEdge {
	var out []adjEdge
	v.InEdges(n, func(w NodeID, k KindID, _ uint64) bool {
		out = append(out, adjEdge{w, k})
		return true
	})
	return out
}

func collectOutIDs(v *View, n NodeID) []uint64 {
	var out []uint64
	v.OutEdges(n, func(_ NodeID, _ KindID, id uint64) bool { out = append(out, id); return true })
	return out
}

func collectInIDs(v *View, n NodeID) []uint64 {
	var out []uint64
	v.InEdges(n, func(_ NodeID, _ KindID, id uint64) bool { out = append(out, id); return true })
	return out
}

func sameIDs(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func zipAdj(nodes []NodeID, kinds []KindID) []adjEdge {
	out := make([]adjEdge, len(nodes))
	for i := range nodes {
		out[i] = adjEdge{nodes[i], kinds[i]}
	}
	return out
}

func sameAdj(a, b []adjEdge) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestCleanAdjacencyAgreesWithTheWalk pins CleanOut/CleanIn's whole contract:
// whenever they answer ok, the base slices they return must be EXACTLY what
// OutEdges/InEdges yield for that node on the same View -- same neighbours,
// same kinds, same order. A single mismatch means a traversal reading the
// fast path sees an edge that is gone, misses one that was added, or follows
// one into a deleted node.
//
// The overlays are randomized across every way a delta can change adjacency:
// tombstoned nodes (hiding every edge into and out of them), tombstoned base
// edges, base edges upserted with a new endpoint or kind, brand-new edges
// between base nodes, and new virtual nodes wired into the base graph.
func TestCleanAdjacencyAgreesWithTheWalk(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		const nodes, edges = 120, 500

		b := NewBuilder(nodes)
		b.SetKinds(map[KindID]string{1: "A", 2: "B", 10: "E", 11: "F"})
		for i := 0; i < nodes; i++ {
			if err := b.AddNode(uint64(i+1), []KindID{KindID(1 + i%2)}, []byte(`{}`)); err != nil {
				t.Fatalf("AddNode: %v", err)
			}
		}
		type spec struct{ id, s, e uint64 }
		var base []spec
		for i := 0; i < edges; i++ {
			s, e := uint64(rng.Intn(nodes)+1), uint64(rng.Intn(nodes)+1)
			id := uint64(10000 + i)
			b.AddEdge(id, s, e, KindID(10+rng.Intn(2)))
			base = append(base, spec{id, s, e})
		}
		snap, err := b.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}

		var sb SegmentBuilder
		sb.AddKind(1, "A")
		sb.AddKind(10, "E")
		sb.AddKind(11, "F")
		for i := 0; i < rng.Intn(4); i++ {
			sb.TombstoneNode(uint64(rng.Intn(nodes) + 1))
		}
		for i := 0; i < rng.Intn(15); i++ {
			sb.TombstoneEdge(base[rng.Intn(len(base))].id)
		}
		for i := 0; i < rng.Intn(15); i++ {
			// Upsert an existing edge id with a different target and kind.
			e := base[rng.Intn(len(base))]
			sb.AddEdgeState(e.id, e.s, uint64(rng.Intn(nodes)+1), KindID(10+rng.Intn(2)))
		}
		for i := 0; i < rng.Intn(25); i++ {
			sb.AddEdgeState(uint64(50000+i), uint64(rng.Intn(nodes)+1), uint64(rng.Intn(nodes)+1), 10)
		}
		for i := 0; i < rng.Intn(3); i++ {
			vid := uint64(90000 + i)
			if err := sb.AddNodeState(vid, []KindID{1}, []byte(`{}`)); err != nil {
				t.Fatalf("AddNodeState: %v", err)
			}
			sb.AddEdgeState(uint64(70000+i), vid, uint64(rng.Intn(nodes)+1), 11)
			sb.AddEdgeState(uint64(80000+i), uint64(rng.Intn(nodes)+1), vid, 11)
		}
		v := NewView(snap).WithSegment(sb.Build())

		clean, dirty := 0, 0
		for n := NodeID(0); int(n) < v.NodeCount(); n++ {
			if targets, kinds, ok := v.CleanOut(n); ok {
				clean++
				if got, want := zipAdj(targets, kinds), collectOut(v, n); !sameAdj(got, want) {
					t.Fatalf("seed %d node %d: CleanOut %v, OutEdges %v", seed, n, got, want)
				}
			} else {
				dirty++
			}
			if sources, kinds, ok := v.CleanIn(n); ok {
				if got, want := zipAdj(sources, kinds), collectIn(v, n); !sameAdj(got, want) {
					t.Fatalf("seed %d node %d: CleanIn %v, InEdges %v", seed, n, got, want)
				}
			}
		}
		// The test proves nothing if everything was declared dirty.
		if clean == 0 {
			t.Fatalf("seed %d: no node was reported clean (%d dirty)", seed, dirty)
		}

		// CleanOutSlots/CleanInSlots hand back BASE CSR slot ranges, which a
		// caller indexes for targets, kinds AND edge ids. A wrong range is a
		// wrong edge, so they are checked against the same walk -- edge ids
		// included this time, since that is the whole reason slots exist.
		for n := NodeID(0); int(n) < v.NodeCount(); n++ {
			if lo, hi, ok := v.CleanOutSlots(n); ok {
				got := make([]adjEdge, 0, hi-lo)
				ids := make([]uint64, 0, hi-lo)
				for i := lo; i < hi; i++ {
					got = append(got, adjEdge{snap.OutTargets[i], snap.OutKinds[i]})
					ids = append(ids, snap.OutEdgeIDs[i])
				}
				if !sameAdj(got, collectOut(v, n)) {
					t.Fatalf("seed %d node %d: CleanOutSlots %v, OutEdges %v", seed, n, got, collectOut(v, n))
				}
				if want := collectOutIDs(v, n); !sameIDs(ids, want) {
					t.Fatalf("seed %d node %d: CleanOutSlots edge ids %v, OutEdges %v", seed, n, ids, want)
				}
			}
			if lo, hi, ok := v.CleanInSlots(n); ok {
				got := make([]adjEdge, 0, hi-lo)
				ids := make([]uint64, 0, hi-lo)
				for i := lo; i < hi; i++ {
					got = append(got, adjEdge{snap.InTargets[i], snap.InKinds[i]})
					ids = append(ids, snap.OutEdgeIDs[snap.InEdgeIdx[i]])
				}
				if !sameAdj(got, collectIn(v, n)) {
					t.Fatalf("seed %d node %d: CleanInSlots %v, InEdges %v", seed, n, got, collectIn(v, n))
				}
				if want := collectInIDs(v, n); !sameIDs(ids, want) {
					t.Fatalf("seed %d node %d: CleanInSlots edge ids %v, InEdges %v", seed, n, ids, want)
				}
			}
		}

		// OutSlices/InSlices answer for EVERY node, dirty and virtual
		// included, so every node is checked -- and a run where nothing was
		// dirty would never exercise the materialized half at all.
		if dirty == 0 {
			t.Fatalf("seed %d: no node was dirty, the merged path went untested", seed)
		}
		for n := NodeID(0); int(n) < v.NodeCount(); n++ {
			targets, kinds, ok := v.OutSlices(n)
			if !ok {
				t.Fatalf("seed %d node %d: OutSlices declined an in-range node", seed, n)
			}
			if got, want := zipAdj(targets, kinds), collectOut(v, n); !sameAdj(got, want) {
				t.Fatalf("seed %d node %d: OutSlices %v, OutEdges %v", seed, n, got, want)
			}
			sources, kinds, ok := v.InSlices(n)
			if !ok {
				t.Fatalf("seed %d node %d: InSlices declined an in-range node", seed, n)
			}
			if got, want := zipAdj(sources, kinds), collectIn(v, n); !sameAdj(got, want) {
				t.Fatalf("seed %d node %d: InSlices %v, InEdges %v", seed, n, got, want)
			}
		}
	}
}

// TestCleanAdjacencyRefusesVirtualNodes: a delta-added node has no base CSR
// row at all, so there is nothing clean to hand back -- though OutSlices, which
// materializes it, must still answer.
func TestCleanAdjacencyRefusesVirtualNodes(t *testing.T) {
	b := NewBuilder(2)
	b.SetKinds(map[KindID]string{1: "A", 10: "E"})
	for i := uint64(1); i <= 2; i++ {
		if err := b.AddNode(i, []KindID{1}, []byte(`{}`)); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var sb SegmentBuilder
	if err := sb.AddNodeState(99, []KindID{1}, []byte(`{}`)); err != nil {
		t.Fatalf("AddNodeState: %v", err)
	}
	v := NewView(snap).WithSegment(sb.Build())
	virtual, ok := v.Dense(99)
	if !ok {
		t.Fatal("virtual node did not resolve")
	}
	if _, _, ok := v.CleanOut(virtual); ok {
		t.Fatal("CleanOut answered for a virtual node")
	}
	if _, _, ok := v.CleanIn(virtual); ok {
		t.Fatal("CleanIn answered for a virtual node")
	}
	if _, _, ok := v.OutSlices(virtual); !ok {
		t.Fatal("OutSlices declined a virtual node it should have materialized")
	}
	if _, _, ok := v.OutSlices(NodeID(v.NodeCount())); ok {
		t.Fatal("OutSlices answered for an id past the end of the View")
	}
}

// TestWarmLeavesTheSameAnswers pins that pre-building the overlay projections
// on the write path changes nothing a reader sees. Warm exists to move cost
// off the first query after a commit; if it also moved an answer it would be
// trading a latency spike for a wrong result.
func TestWarmLeavesTheSameAnswers(t *testing.T) {
	build := func() *View {
		b := NewBuilder(60)
		b.SetKinds(map[KindID]string{1: "A", 10: "E"})
		for i := uint64(1); i <= 60; i++ {
			if err := b.AddNode(i, []KindID{1}, []byte(`{"objectid":"N"}`)); err != nil {
				t.Fatalf("AddNode: %v", err)
			}
		}
		for i := uint64(1); i < 60; i++ {
			b.AddEdge(1000+i, i, i+1, 10)
		}
		snap, err := b.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		var sb SegmentBuilder
		sb.AddKind(1, "A")
		sb.AddKind(10, "E")
		sb.TombstoneNode(7)
		sb.TombstoneEdge(1020)
		sb.AddEdgeState(5000, 3, 40, 10)
		if err := sb.AddNodeState(999, []KindID{1}, []byte(`{"objectid":"NEW"}`)); err != nil {
			t.Fatalf("AddNodeState: %v", err)
		}
		sb.AddEdgeState(5001, 999, 2, 10)
		return NewView(snap).WithSegment(sb.Build())
	}

	cold, warm := build(), build()
	warm.Warm()
	if cold.NodeCount() != warm.NodeCount() {
		t.Fatalf("NodeCount differs: %d vs %d", cold.NodeCount(), warm.NodeCount())
	}
	for n := NodeID(0); int(n) < cold.NodeCount(); n++ {
		if cold.Alive(n) != warm.Alive(n) {
			t.Fatalf("node %d: Alive differs", n)
		}
		if !sameAdj(collectOut(cold, n), collectOut(warm, n)) {
			t.Fatalf("node %d: OutEdges differ", n)
		}
		if !sameAdj(collectIn(cold, n), collectIn(warm, n)) {
			t.Fatalf("node %d: InEdges differ", n)
		}
		co, ck, cok := cold.OutSlices(n)
		wo, wk, wok := warm.OutSlices(n)
		if cok != wok || !sameAdj(zipAdj(co, ck), zipAdj(wo, wk)) {
			t.Fatalf("node %d: OutSlices differ", n)
		}
	}
}
