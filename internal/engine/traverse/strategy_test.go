// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"errors"
	"reflect"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// isolatedKind and node6MarkerKind are node kinds (distinct from the fixture's
// edge kinds 1-3) used only to exercise Endpoint.Bits: isolatedKind marks the
// SideBudget+1 synthetic nodes added for the strategy-C test, and
// node6MarkerKind marks fixture node 6 exclusively so NodesOfKind(node6MarkerKind)
// is equivalent to an explicit {6} id list.
const (
	isolatedKind    snapshot.KindID = 7
	node6MarkerKind snapshot.KindID = 8
)

// buildStrategyFixture extends buildFixture's ten-node graph (see its doc
// comment for the edge layout) with:
//   - an extra node kind on node 6 (node6MarkerKind), letting a Bits
//     endpoint built from NodesOfKind stand in for an explicit {6} id list;
//   - SideBudget+1 extra isolated nodes (10..10+SideBudget) sharing
//     isolatedKind and no edges, letting a Bits endpoint exceed SideBudget
//     to exercise strategy C.
//
// Node kinds are a separate namespace from the edge kinds these traversals
// filter on, so neither addition changes any existing edge-kind-filtered
// traversal result.
func buildStrategyFixture(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < 10; i++ {
		kinds := []snapshot.KindID{1}
		if i == 6 {
			kinds = append(kinds, node6MarkerKind)
		}
		if err := b.AddNode(i, kinds); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	for i := uint64(10); i < uint64(10+SideBudget+1); i++ {
		if err := b.AddNode(i, []snapshot.KindID{isolatedKind}); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	type edge struct {
		start, end uint64
		kind       snapshot.KindID
	}
	for _, e := range []edge{
		{0, 1, 1}, {1, 2, 1}, {2, 3, 2},
		{0, 4, 1}, {0, 5, 2}, {4, 6, 1}, {5, 6, 1},
		{0, 6, 3},
		{7, 8, 1}, {8, 7, 1}, {7, 9, 1},
	} {
		b.AddEdge(e.start, e.end, e.kind)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// assertGroupedByRoot checks that got, partitioned into contiguous runs of
// len(wantGroups[i]) starting at each group's offset, matches wantGroups
// group by group: within a group, order is unspecified (assertPathSet), but
// group order (and therefore each group's shared root) must match exactly.
func assertGroupedByRoot(t *testing.T, got []Path, wantGroups [][]Path) {
	t.Helper()
	pos := 0
	for gi, group := range wantGroups {
		if pos+len(group) > len(got) {
			t.Fatalf("group %d: got only %d paths remaining at position %d, want %d more\ngot:  %+v", gi, len(got)-pos, pos, len(group), got)
		}
		assertPathSet(t, got[pos:pos+len(group)], group)
		pos += len(group)
	}
	if pos != len(got) {
		t.Fatalf("got %d paths total, want %d\ngot: %+v", len(got), pos, got)
	}
}

func TestAllShortestPaths(t *testing.T) {
	t.Run("strategy A: pair strategy, both endpoints constrained", func(t *testing.T) {
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)
		want := []Path{
			{Nodes: []snapshot.NodeID{0, 4, 6}, Kinds: []snapshot.KindID{1, 1}},
			{Nodes: []snapshot.NodeID{0, 5, 6}, Kinds: []snapshot.KindID{2, 1}},
		}

		q := Query{
			Roots:     Endpoint{IDs: []snapshot.NodeID{0}},
			Terminals: Endpoint{IDs: []snapshot.NodeID{6}},
			Kinds:     kinds,
			Mode:      ModeAll,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths (ModeAll): %v", err)
		}
		assertPathSet(t, got, want)
		for _, p := range got {
			if !isValidPath(s, p, kinds) {
				t.Fatalf("invalid path %+v", p)
			}
		}

		q.Mode = ModeOne
		got, err = AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths (ModeOne): %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("len(got) = %d, want 1", len(got))
		}
		matched := false
		for _, opt := range want {
			if pathKey(got[0]) == pathKey(opt) {
				matched = true
			}
		}
		if !matched {
			t.Fatalf("ModeOne path %+v is not one of the two diamond branches", got[0])
		}
	})

	t.Run("strategy B: small side = terminals, roots unconstrained", func(t *testing.T) {
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)

		q := Query{
			Roots:     Endpoint{}, // unconstrained
			Terminals: Endpoint{IDs: []snapshot.NodeID{6}},
			Kinds:     kinds,
			Mode:      ModeAll,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		assertGroupedByRoot(t, got, [][]Path{
			{
				{Nodes: []snapshot.NodeID{0, 4, 6}, Kinds: []snapshot.KindID{1, 1}},
				{Nodes: []snapshot.NodeID{0, 5, 6}, Kinds: []snapshot.KindID{2, 1}},
			},
			{{Nodes: []snapshot.NodeID{4, 6}, Kinds: []snapshot.KindID{1}}},
			{{Nodes: []snapshot.NodeID{5, 6}, Kinds: []snapshot.KindID{1}}},
		})
		for _, p := range got {
			if !isValidPath(s, p, kinds) {
				t.Fatalf("invalid path %+v", p)
			}
		}
	})

	t.Run("strategy B: Limit truncates to the first root's paths", func(t *testing.T) {
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)

		q := Query{
			Roots:     Endpoint{},
			Terminals: Endpoint{IDs: []snapshot.NodeID{6}},
			Kinds:     kinds,
			Mode:      ModeAll,
			Limit:     2,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		want := []Path{
			{Nodes: []snapshot.NodeID{0, 4, 6}, Kinds: []snapshot.KindID{1, 1}},
			{Nodes: []snapshot.NodeID{0, 5, 6}, Kinds: []snapshot.KindID{2, 1}},
		}
		assertPathSet(t, got, want)
	})

	t.Run("ExcludeSelf skips r==t without error", func(t *testing.T) {
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)

		q := Query{
			Roots:       Endpoint{IDs: []snapshot.NodeID{6}},
			Terminals:   Endpoint{IDs: []snapshot.NodeID{6}},
			Kinds:       kinds,
			Mode:        ModeAll,
			ExcludeSelf: true,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("got = %+v, want empty", got)
		}
	})

	t.Run("strategy C: unconstrained roots and an over-budget terminal bitset", func(t *testing.T) {
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)

		terminals := s.NodesOfKind(isolatedKind)
		if terminals.Count() != SideBudget+1 {
			t.Fatalf("NodesOfKind(isolatedKind).Count() = %d, want %d", terminals.Count(), SideBudget+1)
		}

		q := Query{
			Roots:     Endpoint{},
			Terminals: Endpoint{Bits: terminals},
			Kinds:     kinds,
			Mode:      ModeAll,
		}
		got, err := AllShortestPaths(s, q)
		if !errors.Is(err, ErrTooLarge) {
			t.Fatalf("err = %v, want ErrTooLarge", err)
		}
		if got != nil {
			t.Fatalf("got = %+v, want nil", got)
		}
	})

	t.Run("Bits endpoint matches the equivalent explicit-id endpoint", func(t *testing.T) {
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)

		marker := s.NodesOfKind(node6MarkerKind)
		if marker.Count() != 1 || !marker.Has(6) {
			t.Fatalf("NodesOfKind(node6MarkerKind) = %+v, want exactly {6}", marker)
		}

		bitsQ := Query{
			Roots:     Endpoint{},
			Terminals: Endpoint{Bits: marker},
			Kinds:     kinds,
			Mode:      ModeAll,
		}
		idsQ := bitsQ
		idsQ.Terminals = Endpoint{IDs: []snapshot.NodeID{6}}

		gotBits, err := AllShortestPaths(s, bitsQ)
		if err != nil {
			t.Fatalf("AllShortestPaths (Bits): %v", err)
		}
		gotIDs, err := AllShortestPaths(s, idsQ)
		if err != nil {
			t.Fatalf("AllShortestPaths (IDs): %v", err)
		}
		if !reflect.DeepEqual(gotBits, gotIDs) {
			t.Fatalf("Bits result %+v != IDs result %+v", gotBits, gotIDs)
		}
	})

	t.Run("strategy B: small side = roots, forward BFS + backward enumeration", func(t *testing.T) {
		// Exercises the direction the terminal-small-side case doesn't: BFS
		// runs forward from the (small) root, so enumerate must walk the
		// In-CSR mirror from each reached terminal back toward the root and
		// reverse the result. Root 0 reaches every other reachable node in
		// the fixture at kinds {1,2}, including a 3-hop chain (0-1-2-3),
		// which exercises reversal of a path longer than 2 nodes.
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)

		q := Query{
			Roots:     Endpoint{IDs: []snapshot.NodeID{0}},
			Terminals: Endpoint{}, // unconstrained
			Kinds:     kinds,
			Mode:      ModeAll,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		assertGroupedByRoot(t, got, [][]Path{
			{{Nodes: []snapshot.NodeID{0, 1}, Kinds: []snapshot.KindID{1}}},
			{{Nodes: []snapshot.NodeID{0, 1, 2}, Kinds: []snapshot.KindID{1, 1}}},
			{{Nodes: []snapshot.NodeID{0, 1, 2, 3}, Kinds: []snapshot.KindID{1, 1, 2}}},
			{{Nodes: []snapshot.NodeID{0, 4}, Kinds: []snapshot.KindID{1}}},
			{{Nodes: []snapshot.NodeID{0, 5}, Kinds: []snapshot.KindID{2}}},
			{
				{Nodes: []snapshot.NodeID{0, 4, 6}, Kinds: []snapshot.KindID{1, 1}},
				{Nodes: []snapshot.NodeID{0, 5, 6}, Kinds: []snapshot.KindID{2, 1}},
			},
		})
		for _, p := range got {
			if !isValidPath(s, p, kinds) {
				t.Fatalf("invalid path %+v", p)
			}
		}
	})

	t.Run("determinism: parallel fan-out does not reorder output", func(t *testing.T) {
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)

		q := Query{
			Roots:     Endpoint{},
			Terminals: Endpoint{IDs: []snapshot.NodeID{6}},
			Kinds:     kinds,
			Mode:      ModeAll,
		}
		got1, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths (run 1): %v", err)
		}
		got2, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths (run 2): %v", err)
		}
		if !reflect.DeepEqual(got1, got2) {
			t.Fatalf("run 1 = %+v\nrun 2 = %+v\nwant byte-identical results", got1, got2)
		}
	})
}
