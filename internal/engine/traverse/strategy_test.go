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
func buildStrategyFixture(t *testing.T) *snapshot.View {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < 10; i++ {
		kinds := []snapshot.KindID{1}
		if i == 6 {
			kinds = append(kinds, node6MarkerKind)
		}
		if err := b.AddNode(i, kinds, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	for i := uint64(10); i < uint64(10+SideBudget+1); i++ {
		if err := b.AddNode(i, []snapshot.KindID{isolatedKind}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	type edge struct {
		start, end uint64
		kind       snapshot.KindID
	}
	for i, e := range []edge{
		{0, 1, 1}, {1, 2, 1}, {2, 3, 2},
		{0, 4, 1}, {0, 5, 2}, {4, 6, 1}, {5, 6, 1},
		{0, 6, 3},
		{7, 8, 1}, {8, 7, 1}, {7, 9, 1},
	} {
		b.AddEdge(uint64(i), e.start, e.end, e.kind)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(s)
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

	t.Run("Query.SideBudget override accepts the same over-budget terminal bitset", func(t *testing.T) {
		// Same exact query as "strategy C" above (which declines ErrTooLarge
		// under the package's own SideBudget default), except Query.SideBudget
		// is set high enough to admit it -- see Query's own doc comment for
		// why a caller (internal/engine/interpret/expand.go's
		// strategyBudgetOverrides) might legitimately need to override this
		// per call rather than change the package constant every other
		// caller (servePathQuery, pathbench) still relies on unconditionally.
		s := buildStrategyFixture(t)
		kinds := maskOf(3, 1, 2)
		terminals := s.NodesOfKind(isolatedKind)

		q := Query{
			Roots:      Endpoint{},
			Terminals:  Endpoint{Bits: terminals},
			Kinds:      kinds,
			Mode:       ModeAll,
			SideBudget: SideBudget + 1,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		// Every isolated node has no edges at all, so no path exists to any
		// of them from any root -- this pins that the override changed
		// *dispatch* (which strategy runs) without changing the query's
		// actual answer: zero real edges still means zero paths.
		if len(got) != 0 {
			t.Fatalf("got = %+v, want empty (isolated nodes have no edges)", got)
		}
	})

	t.Run("Query.PairBudget override accepts a pair count above the package default", func(t *testing.T) {
		// Both endpoints constrained by an explicit id list, each side
		// larger than SideBudget (so strategy B is unavailable on either
		// side) and their product larger than PairBudget (so strategy A is
		// unavailable too by default): declines ErrTooLarge without an
		// override, succeeds once Query.PairBudget covers the product.
		// wideCount is chosen so both wideCount > SideBudget and
		// wideCount*wideCount > PairBudget hold regardless of either
		// constant's current value.
		wideCount := SideBudget + 1
		for wideCount*wideCount <= PairBudget {
			wideCount *= 2
		}

		b := snapshot.NewBuilder(1)
		ids := make([]snapshot.NodeID, wideCount)
		for i := 0; i < wideCount; i++ {
			if err := b.AddNode(uint64(i), nil, nil); err != nil {
				t.Fatalf("AddNode(%d): %v", i, err)
			}
			ids[i] = snapshot.NodeID(i)
		}
		built, err := b.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		s := snapshot.NewView(built)

		q := Query{
			Roots:     Endpoint{IDs: ids},
			Terminals: Endpoint{IDs: ids},
			Mode:      ModeAll,
		}
		got, err := AllShortestPaths(s, q)
		if !errors.Is(err, ErrTooLarge) {
			t.Fatalf("err = %v, want ErrTooLarge (sanity: over both PairBudget and SideBudget without an override)", err)
		}
		if got != nil {
			t.Fatalf("got = %+v, want nil", got)
		}

		q.PairBudget = wideCount * wideCount
		got, err = AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths with PairBudget override: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("got = %+v, want empty (no edges in this fixture)", got)
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

// buildCapFixture builds a graph purpose-built for exercising Limit and
// ModeOne truncation across more than one pair or BFS element: three roots
// (0, 1, 7), each with two co-minimal 2-hop paths to a shared terminal (2)
// through its own pair of intermediates, all edges kind 1. Roots sit at the
// lowest dense ids (0, 1) or are otherwise reached only after Limit has
// already been exhausted in every test below (7), and every intermediate
// (3-6, 8-9) has a higher dense id than the root whose truncation it is
// meant to interrupt — so even though an unconstrained Roots endpoint would
// eventually visit intermediates too (each has its own direct, 1-hop edge
// into the terminal), Limit always stops dense iteration before it gets
// there.
func buildCapFixture(t *testing.T) *snapshot.View {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < 10; i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	type edge struct{ start, end uint64 }
	for i, e := range []edge{
		{0, 3}, {3, 2}, {0, 4}, {4, 2}, // root 0: two co-minimal paths via 3, 4
		{1, 5}, {5, 2}, {1, 6}, {6, 2}, // root 1: two co-minimal paths via 5, 6
		{7, 8}, {8, 2}, {7, 9}, {9, 2}, // root 7: two co-minimal paths via 8, 9
	} {
		b.AddEdge(uint64(i), e.start, e.end, 1)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(s)
}

// TestCapTruncation is a permanent regression suite for a review finding on
// commit f996be2: Limit and ModeOne truncation compared a relative
// "remaining budget" against enumerate/pairEnumerate's cumulative len(out)
// counter, which is checked across every pair or BFS element a strategy
// merges, not reset per call. A delta compared against a cumulative counter
// either overran (once a later pair's call restarted counting from a
// nonzero out) or, for ModeOne once out already held half of Limit or more,
// never fired at all. The fix (traverse.go's pathCap) always passes
// pairPaths/enumerate an absolute target instead.
func TestCapTruncation(t *testing.T) {
	kinds := maskOf(1, 1)

	root0Paths := []Path{
		{Nodes: []snapshot.NodeID{0, 3, 2}, Kinds: []snapshot.KindID{1, 1}},
		{Nodes: []snapshot.NodeID{0, 4, 2}, Kinds: []snapshot.KindID{1, 1}},
	}
	root1Options := map[string]bool{
		pathKey(Path{Nodes: []snapshot.NodeID{1, 5, 2}, Kinds: []snapshot.KindID{1, 1}}): true,
		pathKey(Path{Nodes: []snapshot.NodeID{1, 6, 2}, Kinds: []snapshot.KindID{1, 1}}): true,
	}

	t.Run("strategy A: Limit is an absolute target across pairs, not a per-pair delta", func(t *testing.T) {
		s := buildCapFixture(t)
		q := Query{
			Roots:     Endpoint{IDs: []snapshot.NodeID{0, 1}},
			Terminals: Endpoint{IDs: []snapshot.NodeID{2}},
			Kinds:     kinds,
			Mode:      ModeAll,
			Limit:     3,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("len(got) = %d, want exactly 3\ngot: %+v", len(got), got)
		}
		assertPathSet(t, got[:2], root0Paths)
		if !root1Options[pathKey(got[2])] {
			t.Fatalf("3rd path %+v is not one of root 1's two co-minimal paths", got[2])
		}
	})

	t.Run("strategy B: Limit is an absolute target across BFS elements, not a per-element delta", func(t *testing.T) {
		s := buildCapFixture(t)
		q := Query{
			Roots:     Endpoint{}, // unconstrained -> small side = terminals
			Terminals: Endpoint{IDs: []snapshot.NodeID{2}},
			Kinds:     kinds,
			Mode:      ModeAll,
			Limit:     3,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("len(got) = %d, want exactly 3\ngot: %+v", len(got), got)
		}
		assertPathSet(t, got[:2], root0Paths)
		if !root1Options[pathKey(got[2])] {
			t.Fatalf("3rd path %+v is not one of root 1's two co-minimal paths", got[2])
		}
	})

	t.Run("ModeOne returns exactly one path per pair across multiple pairs", func(t *testing.T) {
		s := buildCapFixture(t)
		q := Query{
			Roots:     Endpoint{IDs: []snapshot.NodeID{0, 1}},
			Terminals: Endpoint{IDs: []snapshot.NodeID{2}},
			Kinds:     kinds,
			Mode:      ModeOne,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len(got) = %d, want exactly 2 (one per pair)\ngot: %+v", len(got), got)
		}
		root0Options := map[string]bool{pathKey(root0Paths[0]): true, pathKey(root0Paths[1]): true}
		if !root0Options[pathKey(got[0])] {
			t.Fatalf("1st path %+v is not one of root 0's two co-minimal paths", got[0])
		}
		if !root1Options[pathKey(got[1])] {
			t.Fatalf("2nd path %+v is not one of root 1's two co-minimal paths", got[1])
		}
	})

	t.Run("ModeOne + Limit combined: Limit stops iteration before a 3rd pair is even started", func(t *testing.T) {
		s := buildCapFixture(t)
		q := Query{
			Roots:     Endpoint{IDs: []snapshot.NodeID{0, 1, 7}},
			Terminals: Endpoint{IDs: []snapshot.NodeID{2}},
			Kinds:     kinds,
			Mode:      ModeOne,
			Limit:     2,
		}
		got, err := AllShortestPaths(s, q)
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("len(got) = %d, want exactly 2 (Limit=2 over 3 pairs, one path per pair)\ngot: %+v", len(got), got)
		}
		root0Options := map[string]bool{pathKey(root0Paths[0]): true, pathKey(root0Paths[1]): true}
		if !root0Options[pathKey(got[0])] {
			t.Fatalf("1st path %+v is not one of root 0's two co-minimal paths", got[0])
		}
		if !root1Options[pathKey(got[1])] {
			t.Fatalf("2nd path %+v (want a root 1 path; root 7 must never be reached once Limit=2 is hit)", got[1])
		}
	})
}
