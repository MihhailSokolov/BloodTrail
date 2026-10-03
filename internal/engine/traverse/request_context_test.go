// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// traverseCancelAfter reports itself live for its first `live` Err calls and
// cancelled from then on: a request cancelled while its traversal runs, at a
// point no timing can race. Done() is context.Background()'s (nil), which
// nothing in this package consults -- every check goes through Err().
type traverseCancelAfter struct {
	context.Context
	live  int
	calls int
}

func (c *traverseCancelAfter) Err() error {
	c.calls++
	if c.calls > c.live {
		return context.Canceled
	}
	return nil
}

// buildRequestContextFixture builds a wide fixture for the context checks:
// node 0 reaches every one of the `terminals` nodes 1..terminals in one hop
// of kind 1, and node 0 is also reachable from node 1 (so a reverse BFS has
// something to find). terminals is deliberately larger than
// cancelCheckInterval in the merge-phase subtest, so a merge walk over the
// whole terminal side actually reaches its batched check.
func buildRequestContextFixture(t *testing.T, terminals int) *snapshot.View {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i <= uint64(terminals); i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	for i := uint64(1); i <= uint64(terminals); i++ {
		b.AddEdge(i, 0, i, 1)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(s)
}

// TestAllShortestPathsHonoursRequestContext: a traversal serving a request
// ignored that request's context entirely. A caller cancelled before, or
// during, its shortest-path query was traversed in full and answered with a
// nil error, where the PostgreSQL shortest-path query this package stands in
// for answers the same context with its own cancellation. Query.Ctx now
// fails the call on entry when it is already done, and at the next check of
// whichever strategy ran otherwise -- once per pair in strategy A, once per
// small-side BFS element in strategy B, and on the batched cadence in a
// merge phase's endpoint walk.
func TestAllShortestPathsHonoursRequestContext(t *testing.T) {
	snap := buildRequestContextFixture(t, 4)
	wide := buildRequestContextFixture(t, cancelCheckInterval+16)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancelExpired()

	// Strategy A: both sides are explicit id lists, so the pair loop runs.
	pairs := Query{
		Roots:     Endpoint{IDs: []snapshot.NodeID{0}},
		Terminals: Endpoint{IDs: []snapshot.NodeID{1, 2, 3, 4}},
		Mode:      ModeAll,
	}
	// Strategy B: roots are an id list under SideBudget, terminals are
	// unconstrained, so one BFS per root runs and mergeSmallRoots walks every
	// node on the terminal side.
	smallSide := Query{
		Roots: Endpoint{IDs: []snapshot.NodeID{0}},
		Mode:  ModeAll,
	}

	// A cancellation found mid-traversal is reported alongside whatever the
	// strategy had collected so far, exactly as ErrMemoryLimit already is:
	// every caller treats a non-nil error as "decline, delegate", so the
	// partial slice is never served. Only the entry check, which runs before
	// any work, can promise an empty one -- maxPaths says which is expected.
	for name, tc := range map[string]struct {
		snap     *snapshot.View
		q        Query
		ctx      context.Context
		want     error
		maxPaths int
	}{
		"strategy A, cancelled before the traversal": {snap, pairs, cancelled, context.Canceled, 0},
		"strategy A, deadline passed":                {snap, pairs, expired, context.DeadlineExceeded, 0},
		"strategy A, cancelled at the first pair": {
			snap, pairs, &traverseCancelAfter{Context: context.Background(), live: 1}, context.Canceled, 0,
		},
		"strategy B, cancelled before the traversal": {snap, smallSide, cancelled, context.Canceled, 0},
		"strategy B, cancelled at the first BFS element": {
			snap, smallSide, &traverseCancelAfter{Context: context.Background(), live: 1}, context.Canceled, 0,
		},
		// The merge walk's check is batched, so it stops within
		// cancelCheckInterval terminals rather than at the first one.
		"strategy B, cancelled during the merge walk": {
			wide, smallSide, &traverseCancelAfter{Context: context.Background(), live: 2}, context.Canceled,
			cancelCheckInterval,
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := tc.q
			q.Ctx = tc.ctx
			paths, err := AllShortestPaths(tc.snap, q)
			if !errors.Is(err, tc.want) {
				t.Fatalf("AllShortestPaths = (%d paths, %v), want err %v", len(paths), err, tc.want)
			}
			if len(paths) > tc.maxPaths {
				t.Fatalf("AllShortestPaths collected %d paths before stopping, want at most %d", len(paths), tc.maxPaths)
			}
		})
	}

	// A live context, and no context at all, serve the whole answer.
	for name, ctx := range map[string]context.Context{
		"live context": context.Background(),
		"no context":   nil,
	} {
		t.Run(name, func(t *testing.T) {
			for shape, q := range map[string]Query{"strategy A": pairs, "strategy B": smallSide} {
				q.Ctx = ctx
				paths, err := AllShortestPaths(snap, q)
				if err != nil {
					t.Fatalf("%s: AllShortestPaths: %v", shape, err)
				}
				if len(paths) != 4 {
					t.Fatalf("%s: got %d paths, want 4 (one per terminal)", shape, len(paths))
				}
			}
		})
	}
}
