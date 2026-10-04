// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"errors"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildScratchBudgetFixture builds a wide, nearly edgeless graph: `nodes`
// nodes of which only 0 -> 1 and 0 -> 2 are connected. A scratch costs 5
// bytes per node of the WHOLE snapshot however few of them the search
// reaches, so this separates the strategy's scratch cost (large) from its
// output cost (two one-hop paths).
func buildScratchBudgetFixture(t *testing.T, nodes int) *snapshot.View {
	t.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < uint64(nodes); i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	b.AddEdge(1, 0, 1, 1)
	b.AddEdge(2, 0, 2, 1)
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(s)
}

// TestStrategySmallSideChargesItsScratchToTheMemoryLimit: strategy B holds
// one distance buffer per small-side element from its parallel BFS fan-out
// until the merge has finished, and nothing accounted for them -- the memory
// limit charged only the paths enumerated into the output. A caller that
// raises SideBudget from its own work budget (interpret's
// strategyBudgetOverrides) scales that set with it, up to roughly 1.3 GB on
// a sparse graph at the interpreter's default work budget, while no
// documented budget was exceeded. The set is now reserved against the
// query's MemoryLimit before any of it is allocated, and a query whose
// scratch alone does not fit declines ErrTooLarge so the caller delegates.
func TestStrategySmallSideChargesItsScratchToTheMemoryLimit(t *testing.T) {
	const nodes = 1100
	s := buildScratchBudgetFixture(t, nodes)
	// Roots are one id (under SideBudget) and terminals are unconstrained,
	// which is strategy B's dispatch.
	base := Query{Roots: Endpoint{IDs: []snapshot.NodeID{0}}, Mode: ModeAll}

	scratch := scratchBytes(nodes)
	if scratch != nodes*5 {
		t.Fatalf("scratchBytes(%d) = %d, want %d", nodes, scratch, nodes*5)
	}

	// The whole answer is two one-hop paths, 72 bytes each by enumerate's own
	// formula, so a limit that the scratch alone overruns is still far more
	// than the output needs: what declines here is the scratch, nothing else.
	q := base
	q.MemoryLimit = scratch - 1
	if paths, err := AllShortestPaths(s, q); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("AllShortestPaths under a %d-byte limit = (%d paths, %v), want ErrTooLarge",
			q.MemoryLimit, len(paths), err)
	}

	// Room for the scratch and the output: served in full.
	for name, limit := range map[string]uint64{
		"scratch plus output fits": scratch + 1024,
		"unbounded":                0,
	} {
		t.Run(name, func(t *testing.T) {
			q := base
			q.MemoryLimit = limit
			paths, err := AllShortestPaths(s, q)
			if err != nil {
				t.Fatalf("AllShortestPaths: %v", err)
			}
			if len(paths) != 2 {
				t.Fatalf("got %d paths, want 2", len(paths))
			}
		})
	}
}

// TestMemBudgetKeepsReservationsAcrossReset: the overall-shortest mode
// forgets the output bytes it has charged whenever a shorter pair turns up
// (shortestLevel.admit), but the scratch a strategy reserved is still
// allocated at that moment, so the reservation must survive the reset -- or
// a query would keep rediscovering the same headroom it never actually had.
func TestMemBudgetKeepsReservationsAcrossReset(t *testing.T) {
	b := &memBudget{limit: 100}

	if !b.reserve(80) {
		t.Fatalf("reserve(80) against a 100-byte limit was refused")
	}
	if b.reserve(21) {
		t.Fatalf("reserve(21) on top of 80 was granted against a 100-byte limit")
	}
	if err := b.add(30); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("add(30) on top of an 80-byte reservation = %v, want ErrMemoryLimit", err)
	}
	if err := b.add(20); err != nil {
		t.Fatalf("add(20) on top of an 80-byte reservation: %v", err)
	}

	b.reset()

	if err := b.add(20); err != nil {
		t.Fatalf("add(20) after the reset: %v", err)
	}
	if err := b.add(1); !errors.Is(err, ErrMemoryLimit) {
		t.Fatalf("add(1) at 80 reserved + 20 used = %v, want ErrMemoryLimit (the reset dropped the reservation)", err)
	}
}
