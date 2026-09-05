// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"math/rand"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildRandomGraph builds a random directed graph of numNodes nodes and
// numEdges edges (kinds drawn uniformly from 1..maxKind, endpoints uniform
// over all nodes; self-loops and parallel edges are not excluded, matching
// how AddEdge/Build treat them elsewhere in the package). Every node carries
// node kind 1, irrelevant to these edge-kind filtered traversals.
func buildRandomGraph(t *testing.T, rng *rand.Rand, numNodes, numEdges int, maxKind snapshot.KindID) *snapshot.Snapshot {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := 0; i < numNodes; i++ {
		if err := b.AddNode(uint64(i), []snapshot.KindID{1}); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	for i := 0; i < numEdges; i++ {
		start := uint64(rng.Intn(numNodes))
		end := uint64(rng.Intn(numNodes))
		kind := snapshot.KindID(1 + rng.Intn(int(maxKind)))
		b.AddEdge(uint64(i), start, end, kind)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// randomKindMask picks a random non-empty subset of 1-3 distinct kinds from
// 1..max.
func randomKindMask(rng *rand.Rand, max snapshot.KindID) *snapshot.KindMask {
	m := snapshot.NewKindMask(max)
	perm := rng.Perm(int(max))
	count := 1 + rng.Intn(3)
	for i := 0; i < count; i++ {
		m.Set(snapshot.KindID(perm[i] + 1))
	}
	return m
}

func TestPairShortest(t *testing.T) {
	t.Run("diamond distance", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2)
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())

		got := pairShortest(s, 0, 6, kinds, MaxDepth, scF, scT, scTmp)
		if got != 2 {
			t.Fatalf("pairShortest(0,6,{1,2}) = %d, want 2", got)
		}
	})

	t.Run("shortcut kind shortens distance", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2, 3)
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())

		got := pairShortest(s, 0, 6, kinds, MaxDepth, scF, scT, scTmp)
		if got != 1 {
			t.Fatalf("pairShortest(0,6,{1,2,3}) = %d, want 1", got)
		}
	})

	t.Run("disconnected pair", func(t *testing.T) {
		s := buildFixture(t)
		kinds := snapshot.NewKindMask(3)
		kinds.SetAll()
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())

		got := pairShortest(s, 0, 9, kinds, MaxDepth, scF, scT, scTmp)
		if got != -1 {
			t.Fatalf("pairShortest(0,9,any) = %d, want -1 (disconnected)", got)
		}
	})

	t.Run("direction is respected", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2)
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())

		got := pairShortest(s, 3, 0, kinds, MaxDepth, scF, scT, scTmp)
		if got != -1 {
			t.Fatalf("pairShortest(3,0,{1,2}) = %d, want -1 (edges run 0->3, not 3->0)", got)
		}
	})

	t.Run("r == t never yields a zero-length path", func(t *testing.T) {
		s := buildFixture(t)
		kinds := snapshot.NewKindMask(3)
		kinds.SetAll()
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())

		got := pairShortest(s, 7, 7, kinds, MaxDepth, scF, scT, scTmp)
		if got != -1 {
			t.Fatalf("pairShortest(7,7,any) = %d, want -1 (even though 7 sits on a cycle)", got)
		}
	})
}

func TestPairPaths(t *testing.T) {
	t.Run("cap=0 (unbounded) returns both diamond paths", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2)
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())
		budget := &memBudget{}

		out, err := pairPaths(s, 0, 6, kinds, MaxDepth, 0, budget, scF, scT, scTmp, nil)
		if err != nil {
			t.Fatalf("pairPaths: %v", err)
		}
		want := []Path{
			{Nodes: []snapshot.NodeID{0, 4, 6}, Kinds: []snapshot.KindID{1, 1}},
			{Nodes: []snapshot.NodeID{0, 5, 6}, Kinds: []snapshot.KindID{2, 1}},
		}
		assertPathSet(t, out, want)
		for _, p := range out {
			if !isValidPath(s, p, kinds) {
				t.Fatalf("invalid path %+v", p)
			}
		}
	})

	t.Run("cap=1 (ModeOne's per-pair cap) returns exactly one valid 2-hop path", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2)
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())
		budget := &memBudget{}

		out, err := pairPaths(s, 0, 6, kinds, MaxDepth, 1, budget, scF, scT, scTmp, nil)
		if err != nil {
			t.Fatalf("pairPaths: %v", err)
		}
		if len(out) != 1 {
			t.Fatalf("len(out) = %d, want 1", len(out))
		}
		p := out[0]
		if !isValidPath(s, p, kinds) {
			t.Fatalf("invalid path %+v", p)
		}
		validOptions := []string{
			pathKey(Path{Nodes: []snapshot.NodeID{0, 4, 6}, Kinds: []snapshot.KindID{1, 1}}),
			pathKey(Path{Nodes: []snapshot.NodeID{0, 5, 6}, Kinds: []snapshot.KindID{2, 1}}),
		}
		got := pathKey(p)
		matched := false
		for _, opt := range validOptions {
			if got == opt {
				matched = true
			}
		}
		if !matched {
			t.Fatalf("path %+v is not one of the two diamond branches", p)
		}
	})

	t.Run("no path yields empty, no error", func(t *testing.T) {
		s := buildFixture(t)
		kinds := snapshot.NewKindMask(3)
		kinds.SetAll()
		scF, scT, scTmp := newScratch(s.NodeCount()), newScratch(s.NodeCount()), newScratch(s.NodeCount())
		budget := &memBudget{}

		out, err := pairPaths(s, 0, 9, kinds, MaxDepth, 0, budget, scF, scT, scTmp, nil)
		if err != nil {
			t.Fatalf("pairPaths: %v", err)
		}
		if len(out) != 0 {
			t.Fatalf("out = %v, want empty (disconnected pair)", out)
		}
	})
}

// TestPairPathsCrossCheck cross-checks pairPaths(ModeAll) against a
// reference computed purely with Task 3 primitives (a full reverse
// bfsFrom(t) followed by enumerate(r)) on 50 seeded random graphs. The two
// approaches share no code path beyond bfsFrom itself, so agreement here is
// strong evidence that pairShortest's phase-1 termination bound and
// pairEnumerate's two-buffer hop condition are both correct, not just
// correct on the hand-built fixture above.
func TestPairPathsCrossCheck(t *testing.T) {
	const (
		numNodes = 30
		numEdges = 90
		maxKind  = snapshot.KindID(4)
	)

	for seed := int64(0); seed < 50; seed++ {
		rng := rand.New(rand.NewSource(seed))
		s := buildRandomGraph(t, rng, numNodes, numEdges, maxKind)
		kinds := randomKindMask(rng, maxKind)
		r := snapshot.NodeID(rng.Intn(numNodes))
		tgt := snapshot.NodeID(rng.Intn(numNodes))

		scF := newScratch(s.NodeCount())
		scT := newScratch(s.NodeCount())
		scTmp := newScratch(s.NodeCount())
		budget := &memBudget{}

		got, err := pairPaths(s, r, tgt, kinds, MaxDepth, 0, budget, scF, scT, scTmp, nil)
		if err != nil {
			t.Fatalf("seed %d: pairPaths: %v", seed, err)
		}

		refSc := newScratch(s.NodeCount())
		bfsFrom(s, tgt, false, kinds, MaxDepth, refSc)
		refBudget := &memBudget{}
		want, err := enumerate(s, r, refSc, kinds, 0, refBudget, nil, true)
		if err != nil {
			t.Fatalf("seed %d: enumerate (reference): %v", seed, err)
		}

		if len(got) != len(want) {
			t.Fatalf("seed %d (r=%d, t=%d): pairPaths returned %d paths, reference returned %d\npairPaths: %+v\nreference: %+v", seed, r, tgt, len(got), len(want), got, want)
		}
		assertPathSet(t, got, want)
	}
}
