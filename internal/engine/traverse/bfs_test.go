// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildFixture builds the ten-node fixture shared by every subtest:
//
//	edges (kind): 0->1(1) 1->2(1) 2->3(2)        chain
//	              0->4(1) 0->5(2) 4->6(1) 5->6(1) diamond: two 2-hop 0->6 paths
//	              0->6(3)                          1-hop shortcut, kind 3
//	              7->8(1) 8->7(1) 7->9(1)          cycle feeding 9
//
// Database id equals dense id (nodes 0..9 added in order), and every node
// carries a single node kind (1), which is irrelevant to these edge-kind
// filtered traversals.
func buildFixture(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < 10; i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
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
	return s
}

func maskOf(max snapshot.KindID, kinds ...snapshot.KindID) *snapshot.KindMask {
	m := snapshot.NewKindMask(max)
	for _, k := range kinds {
		m.Set(k)
	}
	return m
}

// pathKey renders a Path as a comparable string of nodes and kinds, used to
// compare enumerate's output as an unordered set.
func pathKey(p Path) string {
	var b strings.Builder
	for _, n := range p.Nodes {
		fmt.Fprintf(&b, "n%d,", n)
	}
	b.WriteByte('|')
	for _, k := range p.Kinds {
		fmt.Fprintf(&b, "k%d,", k)
	}
	return b.String()
}

// assertPathSet checks that got and want contain exactly the same paths
// (nodes and kinds), ignoring order.
func assertPathSet(t *testing.T, got, want []Path) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d paths, want %d\ngot:  %+v\nwant: %+v", len(got), len(want), got, want)
	}
	remaining := make(map[string]int, len(want))
	for _, p := range want {
		remaining[pathKey(p)]++
	}
	for _, p := range got {
		k := pathKey(p)
		if remaining[k] == 0 {
			t.Fatalf("unexpected path %+v not in want set\ngot:  %+v\nwant: %+v", p, got, want)
		}
		remaining[k]--
	}
}

// isValidPath independently verifies that p is a real, kind-allowed walk
// through s: every hop exists in the Out-adjacency of its source with the
// recorded kind, and the recorded kind is allowed by kinds.
func isValidPath(s *snapshot.Snapshot, p Path, kinds *snapshot.KindMask) bool {
	if len(p.Nodes) < 2 {
		return false
	}
	if len(p.Kinds) != len(p.Nodes)-1 {
		return false
	}
	if len(p.Nodes) > MaxDepth+1 {
		return false
	}
	for i := 0; i < len(p.Kinds); i++ {
		u, w, k := p.Nodes[i], p.Nodes[i+1], p.Kinds[i]
		if !kinds.Has(k) {
			return false
		}
		targets, edgeKinds := s.Out(u)
		found := false
		for j, t := range targets {
			if t == w && edgeKinds[j] == k {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestScratchEpoch(t *testing.T) {
	sc := newScratch(3)
	sc.reset()
	sc.set(1, 5)

	if d, ok := sc.get(1); !ok || d != 5 {
		t.Fatalf("get(1) = (%d, %v), want (5, true)", d, ok)
	}
	if _, ok := sc.get(0); ok {
		t.Fatalf("get(0) = ok, want unset")
	}
	if _, ok := sc.get(2); ok {
		t.Fatalf("get(2) = ok, want unset")
	}

	sc.reset()
	if _, ok := sc.get(1); ok {
		t.Fatalf("get(1) after reset = ok, want unset (epoch bump should invalidate)")
	}
	sc.set(1, 9)
	if d, ok := sc.get(1); !ok || d != 9 {
		t.Fatalf("get(1) after re-set = (%d, %v), want (9, true)", d, ok)
	}
}

func TestBFSAndEnumerate(t *testing.T) {
	t.Run("diamond: two co-minimal paths at kinds {1,2}", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2)
		sc := newScratch(s.NodeCount())

		deepest := bfsFrom(s, 6, false, kinds, MaxDepth, sc)
		if deepest != 2 {
			t.Fatalf("bfsFrom deepest = %d, want 2", deepest)
		}
		if d, ok := sc.get(0); !ok || d != 2 {
			t.Fatalf("dist[0] = (%d, %v), want (2, true)", d, ok)
		}
		if d, ok := sc.get(4); !ok || d != 1 {
			t.Fatalf("dist[4] = (%d, %v), want (1, true)", d, ok)
		}
		if d, ok := sc.get(5); !ok || d != 1 {
			t.Fatalf("dist[5] = (%d, %v), want (1, true)", d, ok)
		}
		if _, ok := sc.get(3); ok {
			t.Fatalf("dist[3] = ok, want unmarked")
		}

		budget := &memBudget{}
		out, err := enumerate(s, 0, sc, kinds, 0, budget, nil, true)
		if err != nil {
			t.Fatalf("enumerate: %v", err)
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

	t.Run("shortcut kind narrows to the direct edge", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2, 3)
		sc := newScratch(s.NodeCount())

		deepest := bfsFrom(s, 6, false, kinds, MaxDepth, sc)
		if deepest != 1 {
			t.Fatalf("bfsFrom deepest = %d, want 1", deepest)
		}
		if d, ok := sc.get(0); !ok || d != 1 {
			t.Fatalf("dist[0] = (%d, %v), want (1, true)", d, ok)
		}

		budget := &memBudget{}
		out, err := enumerate(s, 0, sc, kinds, 0, budget, nil, true)
		if err != nil {
			t.Fatalf("enumerate: %v", err)
		}
		want := []Path{
			{Nodes: []snapshot.NodeID{0, 6}, Kinds: []snapshot.KindID{3}},
		}
		assertPathSet(t, out, want)
	})

	t.Run("cap stops after exactly one path", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2)
		sc := newScratch(s.NodeCount())
		bfsFrom(s, 6, false, kinds, MaxDepth, sc)

		budget := &memBudget{}
		out, err := enumerate(s, 0, sc, kinds, 1, budget, nil, true)
		if err != nil {
			t.Fatalf("enumerate: %v", err)
		}
		if len(out) != 1 {
			t.Fatalf("len(out) = %d, want 1", len(out))
		}
		p := out[0]
		if !isValidPath(s, p, kinds) {
			t.Fatalf("invalid path %+v", p)
		}
		validOptions := [][]snapshot.NodeID{
			{0, 4, 6},
			{0, 5, 6},
		}
		matched := false
		for _, opt := range validOptions {
			if reflect.DeepEqual(p.Nodes, opt) {
				matched = true
			}
		}
		if !matched {
			t.Fatalf("path nodes = %v, want one of %v", p.Nodes, validOptions)
		}
	})

	t.Run("cycle terminates without infinite loop", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1)
		sc := newScratch(s.NodeCount())

		deepest := bfsFrom(s, 9, false, kinds, MaxDepth, sc)
		if deepest != 2 {
			t.Fatalf("bfsFrom deepest = %d, want 2", deepest)
		}
		if d, ok := sc.get(7); !ok || d != 1 {
			t.Fatalf("dist[7] = (%d, %v), want (1, true)", d, ok)
		}
		if d, ok := sc.get(8); !ok || d != 2 {
			t.Fatalf("dist[8] = (%d, %v), want (2, true)", d, ok)
		}

		budget := &memBudget{}
		out, err := enumerate(s, 8, sc, kinds, 0, budget, nil, true)
		if err != nil {
			t.Fatalf("enumerate: %v", err)
		}
		want := []Path{
			{Nodes: []snapshot.NodeID{8, 7, 9}, Kinds: []snapshot.KindID{1, 1}},
		}
		assertPathSet(t, out, want)
	})

	t.Run("depth cap limits reverse bfs", func(t *testing.T) {
		s := buildFixture(t)
		kinds := snapshot.NewKindMask(3)
		kinds.SetAll()
		sc := newScratch(s.NodeCount())

		deepest := bfsFrom(s, 3, false, kinds, 2, sc)
		if deepest != 2 {
			t.Fatalf("bfsFrom deepest = %d, want 2", deepest)
		}
		if d, ok := sc.get(2); !ok || d != 1 {
			t.Fatalf("dist[2] = (%d, %v), want (1, true)", d, ok)
		}
		if d, ok := sc.get(1); !ok || d != 2 {
			t.Fatalf("dist[1] = (%d, %v), want (2, true)", d, ok)
		}
		if _, ok := sc.get(0); ok {
			t.Fatalf("dist[0] = ok, want unmarked (beyond the depth cap)")
		}
	})

	t.Run("memory budget stops enumeration", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2)
		sc := newScratch(s.NodeCount())
		bfsFrom(s, 6, false, kinds, MaxDepth, sc)

		budget := &memBudget{limit: 1}
		out, err := enumerate(s, 0, sc, kinds, 0, budget, nil, true)
		if !errors.Is(err, ErrMemoryLimit) {
			t.Fatalf("err = %v, want ErrMemoryLimit", err)
		}
		if len(out) != 0 {
			t.Fatalf("out = %v, want empty (first path already exceeds the 1-byte budget)", out)
		}
	})

	t.Run("zero-length paths are never produced", func(t *testing.T) {
		s := buildFixture(t)
		kinds := maskOf(3, 1, 2, 3)
		sc := newScratch(s.NodeCount())
		bfsFrom(s, 0, false, kinds, MaxDepth, sc)

		budget := &memBudget{}
		out, err := enumerate(s, 0, sc, kinds, 0, budget, nil, true)
		if err != nil {
			t.Fatalf("enumerate: %v", err)
		}
		if len(out) != 0 {
			t.Fatalf("out = %v, want empty (distTo(0) == 0, source is destination)", out)
		}
	})
}
