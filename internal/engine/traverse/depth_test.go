// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"errors"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// chainView builds a straight 0->1->...->hops chain of one kind, the shape
// that makes a traversal's depth equal to its length.
func chainView(t *testing.T, hops int) *snapshot.View {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := 0; i <= hops; i++ {
		if err := b.AddNode(uint64(i+1), []snapshot.KindID{1}, nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < hops; i++ {
		b.AddEdge(uint64(i+1), uint64(i+1), uint64(i+2), 1)
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return snapshot.NewView(snap)
}

// TestDepthBeyondTheDistanceBufferDeclines pins that a depth bound past what
// scratch.dist can represent is declined rather than answered. An int8
// distance wraps negative past 127, and both strategies then produce wrong
// results rather than incomplete ones: strategy A pruned at the first hop and
// returned no path at all for a 128-hop chain that has one, and strategy B
// read a wrapped 0 as the seed's own distance and emitted paths beginning at
// an unrelated node. ErrTooLarge sends the query to PostgreSQL, which honors
// the explicit bound the query text asked for.
func TestDepthBeyondTheDistanceBufferDeclines(t *testing.T) {
	view := chainView(t, 130)

	for _, depth := range []int{MaxRepresentableDepth + 1, 200, 1000} {
		if _, err := AllShortestPaths(view, Query{
			Roots:     Endpoint{IDs: []snapshot.NodeID{0}},
			Terminals: Endpoint{IDs: []snapshot.NodeID{130}},
			MaxDepth:  depth,
			Limit:     10,
		}); !errors.Is(err, ErrTooLarge) {
			t.Errorf("MaxDepth %d: err = %v, want ErrTooLarge", depth, err)
		}
		// Strategy B (one unconstrained side) has to decline on the same
		// bound: this is where the wrapped distances fabricated paths.
		if _, err := AllShortestPaths(view, Query{
			Roots:    Endpoint{IDs: []snapshot.NodeID{0}},
			MaxDepth: depth,
			Limit:    10,
		}); !errors.Is(err, ErrTooLarge) {
			t.Errorf("MaxDepth %d, unconstrained terminals: err = %v, want ErrTooLarge", depth, err)
		}
	}

	// The deepest representable bound is still served, and correctly.
	paths, err := AllShortestPaths(view, Query{
		Roots:     Endpoint{IDs: []snapshot.NodeID{0}},
		Terminals: Endpoint{IDs: []snapshot.NodeID{127}},
		MaxDepth:  MaxRepresentableDepth,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("MaxDepth %d: %v", MaxRepresentableDepth, err)
	}
	if len(paths) != 1 || len(paths[0].Nodes) != 128 || paths[0].Nodes[0] != 0 {
		t.Fatalf("deepest representable chain: got %d paths, first of %d nodes", len(paths), len(paths[0].Nodes))
	}
}

// TestEveryReturnedPathStartsAtARoot is the invariant the wrapped distances
// broke, checked over the depth range the engine actually serves: hydrate and
// interpret's path conversion both trust that a returned path begins at a node
// the query asked about.
func TestEveryReturnedPathStartsAtARoot(t *testing.T) {
	for _, hops := range []int{2, 15, 100, MaxRepresentableDepth} {
		view := chainView(t, hops)
		paths, err := AllShortestPaths(view, Query{
			Roots:    Endpoint{IDs: []snapshot.NodeID{0}},
			MaxDepth: hops,
			Limit:    10000,
		})
		if err != nil {
			t.Fatalf("hops=%d: %v", hops, err)
		}
		if len(paths) != hops {
			t.Errorf("hops=%d: got %d paths, want %d", hops, len(paths), hops)
		}
		for _, p := range paths {
			if p.Nodes[0] != 0 {
				t.Fatalf("hops=%d: path starts at %d, not at the root: %v", hops, p.Nodes[0], p.Nodes)
			}
		}
	}
}
