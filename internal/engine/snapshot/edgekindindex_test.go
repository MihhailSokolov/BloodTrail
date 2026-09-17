// SPDX-License-Identifier: Apache-2.0

package snapshot

import "testing"

// buildEdgeKindEndpointFixture: two node kinds and three edge kinds with very
// different densities -- the shape the index exists for. `Rare` has exactly
// one edge in the whole graph, mirroring AZGlobalAdmin on the benchmark graph.
func buildEdgeKindEndpointFixture(t *testing.T) *View {
	t.Helper()
	const (
		kiA    KindID = 1
		kiB    KindID = 2
		keRare KindID = 3
		keMany KindID = 4
		keNone KindID = 5
	)
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{kiA: "A", kiB: "B", keRare: "Rare", keMany: "Many", keNone: "None"})
	const n = 100
	for i := 0; i < n; i++ {
		kind := kiA
		if i%10 == 0 {
			kind = kiB
		}
		if err := b.AddNode(uint64(i+1), []KindID{kind}, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	var eid uint64
	next := func() uint64 { eid++; return eid }
	// Exactly one Rare edge: node 5 -> node 7 (dense 4 -> 6).
	b.AddEdge(next(), 5, 7, keRare)
	// Many edges among the first 40 nodes.
	for i := 0; i < 40; i++ {
		b.AddEdge(next(), uint64(i+1), uint64((i+3)%40+1), keMany)
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return NewView(snap)
}

func TestEdgeKindEndpoints(t *testing.T) {
	v := buildEdgeKindEndpointFixture(t)
	const (
		keRare KindID = 3
		keMany KindID = 4
		keNone KindID = 5
	)

	t.Run("a rare kind resolves to its handful of endpoints", func(t *testing.T) {
		out, ok := v.EdgeKindEndpoints([]KindID{keRare}, true)
		if !ok {
			t.Fatal("not answered")
		}
		if len(out) != 1 {
			t.Fatalf("got %d out-endpoints, want 1: the whole point is that one "+
				"edge does not cost a scan of every node carrying the source kind", len(out))
		}
		want, _ := v.Dense(5)
		if out[0] != want {
			t.Fatalf("got endpoint %d, want %d", out[0], want)
		}
		in, _ := v.EdgeKindEndpoints([]KindID{keRare}, false)
		wantIn, _ := v.Dense(7)
		if len(in) != 1 || in[0] != wantIn {
			t.Fatalf("got in-endpoints %v, want [%d]", in, wantIn)
		}
	})

	t.Run("a kind with no edges resolves to nothing", func(t *testing.T) {
		out, ok := v.EdgeKindEndpoints([]KindID{keNone}, true)
		if !ok || len(out) != 0 {
			t.Fatalf("got %v (ok=%v), want empty", out, ok)
		}
	})

	t.Run("endpoints are sorted and deduplicated", func(t *testing.T) {
		for _, outgoing := range []bool{true, false} {
			ids, _ := v.EdgeKindEndpoints([]KindID{keMany}, outgoing)
			if len(ids) == 0 {
				t.Fatal("expected endpoints")
			}
			for i := 1; i < len(ids); i++ {
				if ids[i] <= ids[i-1] {
					t.Fatalf("outgoing=%v not strictly ascending at %d: %v", outgoing, i, ids[:i+1])
				}
			}
		}
	})

	t.Run("an alternation is the union of its kinds", func(t *testing.T) {
		rare, _ := v.EdgeKindEndpoints([]KindID{keRare}, true)
		many, _ := v.EdgeKindEndpoints([]KindID{keMany}, true)
		both, ok := v.EdgeKindEndpoints([]KindID{keRare, keMany}, true)
		if !ok {
			t.Fatal("not answered")
		}
		seen := map[NodeID]bool{}
		for _, id := range both {
			if seen[id] {
				t.Fatalf("alternation repeated %d: a candidate source must be a set", id)
			}
			seen[id] = true
		}
		for _, id := range append(append([]NodeID{}, rare...), many...) {
			if !seen[id] {
				t.Fatalf("alternation dropped %d", id)
			}
		}
	})

	t.Run("no kinds means no narrowing", func(t *testing.T) {
		if _, ok := v.EdgeKindEndpoints(nil, true); ok {
			t.Fatal(`an "any relationship type" pattern must not claim to narrow`)
		}
	})

	t.Run("agrees with the CSR it indexes", func(t *testing.T) {
		// Independent recomputation straight from the adjacency, so the index
		// is checked against the graph rather than against itself.
		for _, k := range []KindID{keRare, keMany, keNone} {
			wantOut := map[NodeID]bool{}
			wantIn := map[NodeID]bool{}
			for node := 0; node < v.NodeCount(); node++ {
				targets, kinds, _ := v.Out(NodeID(node))
				for i, tgt := range targets {
					if kinds[i] == k {
						wantOut[NodeID(node)] = true
						wantIn[tgt] = true
					}
				}
			}
			gotOut, _ := v.EdgeKindEndpoints([]KindID{k}, true)
			gotIn, _ := v.EdgeKindEndpoints([]KindID{k}, false)
			if len(gotOut) != len(wantOut) || len(gotIn) != len(wantIn) {
				t.Fatalf("kind %d: got %d out / %d in, want %d / %d",
					k, len(gotOut), len(gotIn), len(wantOut), len(wantIn))
			}
			for _, id := range gotOut {
				if !wantOut[id] {
					t.Fatalf("kind %d: out-endpoint %d not in the CSR", k, id)
				}
			}
			for _, id := range gotIn {
				if !wantIn[id] {
					t.Fatalf("kind %d: in-endpoint %d not in the CSR", k, id)
				}
			}
		}
	})
}

// TestEdgeKindEndpointsOverlay pins the superset contract: a delta segment can
// add an edge of a kind the base never carried, and the index must include
// its endpoints or the executor will skip a node that can match.
func TestEdgeKindEndpointsOverlay(t *testing.T) {
	const (
		kiA    KindID = 1
		keRare KindID = 3
		keNew  KindID = 9
	)
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{kiA: "A", keRare: "Rare", keNew: "New"})
	for i := 0; i < 4; i++ {
		if err := b.AddNode(uint64(i+1), []KindID{kiA}, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	b.AddEdge(1, 1, 2, keRare)
	snap, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}

	var sb SegmentBuilder
	sb.AddKind(keNew, "New")
	sb.AddEdgeState(100, 3, 4, keNew)
	v := NewView(snap).WithSegment(sb.Build())

	out, ok := v.EdgeKindEndpoints([]KindID{keNew}, true)
	if !ok {
		t.Fatal("not answered")
	}
	want, _ := v.Dense(3)
	found := false
	for _, id := range out {
		if id == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("got %v, missing the delta-added edge's source %d: a candidate "+
			"source that is SHORT makes the executor serve a wrong answer", out, want)
	}
}
