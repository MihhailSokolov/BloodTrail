// SPDX-License-Identifier: Apache-2.0

package traverse

import (
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildLevelFixture builds four roots at three different distances from one
// terminal (9), all edges kind 1, with the FARTHEST root at the lowest dense
// id so every strategy meets the longer pairs first:
//
//	root 0: 0 -> 1 -> 2 -> 9   (3 hops)
//	root 4: 4 -> 5 -> 9        (2 hops)
//	root 6: 6 -> 9             (1 hop)
//	root 7: 7 -> 9             (1 hop)
//
// Intermediates 1, 2, 5 are roots too whenever Roots is unconstrained: 2 and
// 5 are then one hop from the terminal, 1 is two.
func buildLevelFixture(t *testing.T) *snapshot.View {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < 10; i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	type edge struct{ start, end uint64 }
	for i, e := range []edge{{0, 1}, {1, 2}, {2, 9}, {4, 5}, {5, 9}, {6, 9}, {7, 9}} {
		b.AddEdge(uint64(i), e.start, e.end, 1)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(s)
}

func hop(nodes ...snapshot.NodeID) Path {
	kinds := make([]snapshot.KindID, len(nodes)-1)
	for i := range kinds {
		kinds[i] = 1
	}
	return Path{Nodes: nodes, Kinds: kinds}
}

// levelStrategies drives the fixture's four constrained roots through each
// of AllShortestPaths' three enumeration loops: strategyPairs by default,
// then mergeSmallRoots and mergeSmallTerminals by overriding the budgets that
// pick between them.
var levelStrategies = []struct {
	name                   string
	pairBudget, sideBudget int
}{
	{"strategy A (pairs)", 0, 0},
	{"strategy B, small side = roots", 1, 4},
	{"strategy B, small side = terminals", 1, 1},
}

// TestAllShortestPathsKeepsOnlyTheShortestLengthOverall pins ModeAll to
// dawgs' pg semantics for allShortestPaths(): its unidirectional and
// bidirectional asp harnesses expand from the whole root set at once and
// stop at the first depth where ANY root reaches ANY terminal, returning
// every path of that length and nothing longer. A pair whose own shortest
// path is longer than that contributes nothing, even though it has one.
func TestAllShortestPathsKeepsOnlyTheShortestLengthOverall(t *testing.T) {
	want := []Path{hop(6, 9), hop(7, 9)}

	for _, st := range levelStrategies {
		t.Run(st.name, func(t *testing.T) {
			got, err := AllShortestPaths(buildLevelFixture(t), Query{
				Roots:      Endpoint{IDs: []snapshot.NodeID{0, 4, 6, 7}},
				Terminals:  Endpoint{IDs: []snapshot.NodeID{9}},
				Kinds:      maskOf(1, 1),
				Mode:       ModeAll,
				PairBudget: st.pairBudget,
				SideBudget: st.sideBudget,
			})
			if err != nil {
				t.Fatalf("AllShortestPaths: %v", err)
			}
			assertPathSet(t, got, want)
		})
	}

	t.Run("unconstrained roots", func(t *testing.T) {
		got, err := AllShortestPaths(buildLevelFixture(t), Query{
			Terminals: Endpoint{IDs: []snapshot.NodeID{9}},
			Kinds:     maskOf(1, 1),
			Mode:      ModeAll,
		})
		if err != nil {
			t.Fatalf("AllShortestPaths: %v", err)
		}
		assertPathSet(t, got, []Path{hop(2, 9), hop(5, 9), hop(6, 9), hop(7, 9)})
	})
}

// TestAllShortestPathsLimitNeverStopsTheSearchForAShorterPair pins how Limit
// composes with that rule: a Limit already reached at a longer length must
// not end the scan, because a pair met later can still be shorter -- and
// then the longer paths collected so far are dropped, not kept. Limit 1
// reaches its target at 3 hops (root 0) and again at 2 (root 4) before root
// 6's one-hop path replaces both.
func TestAllShortestPathsLimitNeverStopsTheSearchForAShorterPair(t *testing.T) {
	for _, st := range levelStrategies {
		t.Run(st.name, func(t *testing.T) {
			got, err := AllShortestPaths(buildLevelFixture(t), Query{
				Roots:      Endpoint{IDs: []snapshot.NodeID{0, 4, 6, 7}},
				Terminals:  Endpoint{IDs: []snapshot.NodeID{9}},
				Kinds:      maskOf(1, 1),
				Mode:       ModeAll,
				Limit:      1,
				PairBudget: st.pairBudget,
				SideBudget: st.sideBudget,
			})
			if err != nil {
				t.Fatalf("AllShortestPaths: %v", err)
			}
			if len(got) != 1 || len(got[0].Nodes) != 2 {
				t.Fatalf("got %+v, want exactly one one-hop path", got)
			}
		})
	}
}

// TestAllShortestPathsPerPairKeepsEveryPairsLength pins ModeAllPerPair to
// bidirectional_asp_harness's pair-filter semantics: each pair is resolved
// at its own shortest length, so the 3- and 2-hop pairs that ModeAll drops
// come back alongside the 1-hop ones.
func TestAllShortestPathsPerPairKeepsEveryPairsLength(t *testing.T) {
	want := []Path{hop(0, 1, 2, 9), hop(4, 5, 9), hop(6, 9), hop(7, 9)}

	for _, st := range levelStrategies {
		t.Run(st.name, func(t *testing.T) {
			got, err := AllShortestPaths(buildLevelFixture(t), Query{
				Roots:      Endpoint{IDs: []snapshot.NodeID{0, 4, 6, 7}},
				Terminals:  Endpoint{IDs: []snapshot.NodeID{9}},
				Kinds:      maskOf(1, 1),
				Mode:       ModeAllPerPair,
				PairBudget: st.pairBudget,
				SideBudget: st.sideBudget,
			})
			if err != nil {
				t.Fatalf("AllShortestPaths: %v", err)
			}
			assertPathSet(t, got, want)
		})
	}
}

// TestShortestPathStaysPerPair pins that ModeOne is untouched: dawgs' sp
// harness keeps a visited set per root and keeps expanding until every root
// has found its terminals, so shortestPath() returns one path for EVERY
// pair that has one, whatever its length.
func TestShortestPathStaysPerPair(t *testing.T) {
	want := []Path{hop(0, 1, 2, 9), hop(4, 5, 9), hop(6, 9), hop(7, 9)}

	for _, st := range levelStrategies {
		t.Run(st.name, func(t *testing.T) {
			got, err := AllShortestPaths(buildLevelFixture(t), Query{
				Roots:      Endpoint{IDs: []snapshot.NodeID{0, 4, 6, 7}},
				Terminals:  Endpoint{IDs: []snapshot.NodeID{9}},
				Kinds:      maskOf(1, 1),
				Mode:       ModeOne,
				PairBudget: st.pairBudget,
				SideBudget: st.sideBudget,
			})
			if err != nil {
				t.Fatalf("AllShortestPaths: %v", err)
			}
			assertPathSet(t, got, want)
		})
	}
}
