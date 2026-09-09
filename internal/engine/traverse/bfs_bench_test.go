// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// hubInDegree is the in-degree of the hub node buildHubFixture centers its
// graph on. Large enough that any per-call allocation sized to a node's
// in-degree dominates the measurement rather than hiding in noise, and that
// the gather such an allocation would need touches enough of the reverse CSR
// to be realistic -- a hub with a six-figure in-degree is an ordinary shape
// in an Active Directory graph (Domain Users, Everyone, Authenticated Users),
// which is exactly where a backward expansion gets expensive.
const hubInDegree = 100_000

// buildHubFixture builds a snapshot shaped to isolate ONE backward (In-CSR)
// expansion over a high-in-degree node:
//
//	node 0        the seed a backward BFS starts from
//	node 1        the hub: 1 -> 0, so backward level 1 settles exactly {hub}
//	nodes 2..N+1  hubInDegree spokes, each spoke -> hub, so backward level 2
//	              is one single In() read of the hub's whole in-edge list
//
// Every node carries node kind 1 and every edge kind 1; kind filtering is
// not what these benchmarks measure.
func buildHubFixture(tb testing.TB) *snapshot.View {
	tb.Helper()
	b := snapshot.NewBuilder(1)
	total := uint64(hubInDegree) + 2
	for i := uint64(0); i < total; i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
			tb.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	b.AddEdge(0, 1, 0, 1) // hub -> seed
	for i := uint64(0); i < hubInDegree; i++ {
		b.AddEdge(i+1, i+2, 1, 1) // spoke -> hub
	}
	s, err := b.Build()
	if err != nil {
		tb.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(s)
}

// BenchmarkBackwardExpandHub measures the adjacency read at the heart of one
// backward expansion: reading a hub's entire in-edge list and walking it,
// exactly as bfsFrom/enumerate/pairShortest do on their !Overlay() branch.
//
// This is the narrowest measurement of the work View.In does per expanded
// node, so it is the one that shows an allocation there at full strength.
// It must report 0 allocs/op: In hands back slice views aliasing the base
// snapshot's reverse CSR and materializes nothing. An earlier signature also
// resolved and returned a database-edge-id slice, which could not alias
// (the reverse CSR stores forward-array indices, not ids) and so allocated
// an in-degree-sized slice on every call -- 800 KB per expansion at this
// hub's degree -- for a value no caller ever read. See View.In's own doc.
func BenchmarkBackwardExpandHub(b *testing.B) {
	s := buildHubFixture(b)
	const hub = snapshot.NodeID(1)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sources, kinds := s.In(hub)
		var acc int
		for j, w := range sources {
			acc += int(w) + int(kinds[j])
		}
		if acc == 0 {
			b.Fatal("empty expansion: fixture is wrong")
		}
	}
}

// BenchmarkBackwardBFSHub measures a whole backward bfsFrom over the same
// fixture, so the expansion above is seen in the context that actually runs
// it. Unlike BenchmarkBackwardExpandHub this is not expected to reach zero
// allocations -- a BFS necessarily allocates its own frontier slices, which
// is work proportional to the nodes it settles rather than per-call waste.
func BenchmarkBackwardBFSHub(b *testing.B) {
	s := buildHubFixture(b)
	sc := newScratch(s.NodeCount())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := bfsFrom(s, 0, false, nil, MaxDepth, sc); got != 2 {
			b.Fatalf("deepest = %d, want 2", got)
		}
	}
}
