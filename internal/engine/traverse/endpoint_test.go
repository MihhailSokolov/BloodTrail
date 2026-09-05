// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// bitsOf builds a *snapshot.Bitset over a universe of size total with bits
// set exactly at ids.
func bitsOf(total int, ids ...snapshot.NodeID) *snapshot.Bitset {
	b := snapshot.NewBitset(total)
	for _, id := range ids {
		b.Set(id)
	}
	return b
}

// TestEndpointHas exercises Endpoint.Has's three branches directly: IDs
// (binary search over the documented-ascending slice, checked at the first,
// last, and middle elements plus values that fall below, above, and in a
// gap between entries), Bits (delegates to snapshot.Bitset.Has), and the
// unconstrained default (matches every id).
func TestEndpointHas(t *testing.T) {
	t.Run("IDs", func(t *testing.T) {
		e := Endpoint{IDs: []snapshot.NodeID{2, 5, 9}}

		for _, want := range []struct {
			id   snapshot.NodeID
			want bool
		}{
			{0, false},  // below the first entry
			{2, true},   // first entry
			{4, false},  // gap between entries
			{5, true},   // middle entry
			{9, true},   // last entry
			{10, false}, // above the last entry
		} {
			if got := e.Has(want.id); got != want.want {
				t.Errorf("Has(%d) = %v, want %v", want.id, got, want.want)
			}
		}
	})

	t.Run("IDs empty but non-nil matches nothing", func(t *testing.T) {
		e := Endpoint{IDs: []snapshot.NodeID{}}
		if e.Has(0) {
			t.Errorf("Has(0) = true for an empty (non-nil) IDs endpoint, want false")
		}
	})

	t.Run("Bits", func(t *testing.T) {
		e := Endpoint{Bits: bitsOf(10, 3, 7)}
		if !e.Has(3) || !e.Has(7) {
			t.Errorf("Has(3)/Has(7) = false, want true for a bitset with those bits set")
		}
		if e.Has(4) {
			t.Errorf("Has(4) = true, want false (bit not set)")
		}
	})

	t.Run("unconstrained matches every id", func(t *testing.T) {
		var e Endpoint
		if !e.Unconstrained() {
			t.Fatalf("zero-value Endpoint.Unconstrained() = false, want true")
		}
		for _, id := range []snapshot.NodeID{0, 1, 1000} {
			if !e.Has(id) {
				t.Errorf("Has(%d) = false for an unconstrained endpoint, want true", id)
			}
		}
	})
}

// buildSelfConflictFixture builds a 5-node snapshot for TestSelfEndpointConflict:
//
//	0 --edge--> 1      (0 has an outgoing edge; 1 is a sink)
//	2                  (isolated: no edges at all)
//	3 --edge--> 4      (3 has an outgoing edge; 4 is a sink)
//
// So nodes 0 and 3 have out-degree > 0; nodes 1, 2, and 4 have out-degree 0.
// The specific edge kind (1) is irrelevant: SelfEndpointConflict counts any
// outgoing edge regardless of kind (see its doc for why).
func buildSelfConflictFixture(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < 5; i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	b.AddEdge(1, 0, 1, 1)
	b.AddEdge(2, 3, 4, 1)
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// TestSelfEndpointConflict covers SelfEndpointConflict's two conditions
// together -- roots and terminals must both overlap AND the overlapping
// node must have an outgoing edge -- across explicit IDs, kind-derived
// Bits, and unconstrained endpoints, since it picks whichever side
// Count(total) reports as smaller and probes it against the other, so both
// argument orders need coverage too.
func TestSelfEndpointConflict(t *testing.T) {
	s := buildSelfConflictFixture(t)

	tests := []struct {
		name        string
		roots, term Endpoint
		want        bool
	}{
		{
			name:  "same node, has an outgoing edge -> conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{0}},
			term:  Endpoint{IDs: []snapshot.NodeID{0}},
			want:  true,
		},
		{
			name:  "same node, no outgoing edge (sink) -> no conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{1}},
			term:  Endpoint{IDs: []snapshot.NodeID{1}},
			want:  false,
		},
		{
			name:  "same node, isolated -> no conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{2}},
			term:  Endpoint{IDs: []snapshot.NodeID{2}},
			want:  false,
		},
		{
			name:  "disjoint sets -> no conflict regardless of edges",
			roots: Endpoint{IDs: []snapshot.NodeID{0, 3}},
			term:  Endpoint{IDs: []snapshot.NodeID{1, 2, 4}},
			want:  false,
		},
		{
			name:  "overlap only on a sink -> no conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{0, 1}},
			term:  Endpoint{IDs: []snapshot.NodeID{1, 4}},
			want:  false,
		},
		{
			name:  "overlap includes a node with an edge -> conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{0, 1}},
			term:  Endpoint{IDs: []snapshot.NodeID{0, 4}},
			want:  true,
		},
		{
			name:  "multiple overlapping nodes, only one has an edge -> conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{1, 3}},
			term:  Endpoint{IDs: []snapshot.NodeID{1, 3}},
			want:  true,
		},
		{
			name:  "IDs vs Bits, overlap has an edge -> conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{0, 1}},
			term:  Endpoint{Bits: bitsOf(5, 0, 2)},
			want:  true,
		},
		{
			name:  "IDs vs Bits, overlap is a sink -> no conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{1, 2}},
			term:  Endpoint{Bits: bitsOf(5, 1, 4)},
			want:  false,
		},
		{
			name:  "Bits vs Bits, disjoint -> no conflict",
			roots: Endpoint{Bits: bitsOf(5, 0, 1)},
			term:  Endpoint{Bits: bitsOf(5, 3, 4)},
			want:  false,
		},
		{
			name:  "unconstrained vs a sink-only id set -> no conflict",
			roots: Endpoint{},
			term:  Endpoint{IDs: []snapshot.NodeID{1}},
			want:  false,
		},
		{
			name:  "unconstrained vs an id set that has an edge -> conflict",
			roots: Endpoint{},
			term:  Endpoint{IDs: []snapshot.NodeID{3}},
			want:  true,
		},
		{
			name:  "unconstrained vs unconstrained -> conflict (some node has an edge)",
			roots: Endpoint{},
			term:  Endpoint{},
			want:  true,
		},
		{
			name:  "empty (non-nil) IDs never conflict",
			roots: Endpoint{IDs: []snapshot.NodeID{}},
			term:  Endpoint{},
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := SelfEndpointConflict(s, tc.roots, tc.term); got != tc.want {
				t.Errorf("SelfEndpointConflict(roots, terminals) = %v, want %v", got, tc.want)
			}
			// The result must not depend on which argument is roots vs terminals.
			if got := SelfEndpointConflict(s, tc.term, tc.roots); got != tc.want {
				t.Errorf("SelfEndpointConflict(terminals, roots) = %v, want %v (should be symmetric)", got, tc.want)
			}
		})
	}
}
