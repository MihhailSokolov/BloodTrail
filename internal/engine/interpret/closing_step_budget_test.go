// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestClosingStepFanOutChargesLiveRowBudget: a step closing a cycle in the
// pattern (`(x)-[:E]->(y), (y)-->(x)`) emits one row per qualifying edge
// between two already-bound nodes, so parallel edges multiply its rows just
// as a tree step's fan-out does -- BloodHound keeps one edge per kind between
// a pair, but a pair can carry many kinds (GenericAll, WriteDacl, Owns, ...).
// The tree step's rows were charged to MaxLiveRows; the closing step's were
// not, and 600 rows were served against a 500-row cap.
func TestClosingStepFanOutChargesLiveRowBudget(t *testing.T) {
	const (
		kA, kB snapshot.KindID = 1, 2
		kE     snapshot.KindID = 10
		back                   = 600
	)
	kinds := map[snapshot.KindID]string{kA: "A", kB: "B", kE: "E"}
	edges := []execEdgeSpec{{1, 1, 2, kE}}
	for i := 0; i < back; i++ {
		kind := snapshot.KindID(100 + i)
		kinds[kind] = "R" + itoa(i)
		edges = append(edges, execEdgeSpec{uint64(1000 + i), 2, 1, kind})
	}
	snap := buildExecSnapshot(t, kinds, []execNodeSpec{
		{1, []snapshot.KindID{kA}, nil},
		{2, []snapshot.KindID{kB}, nil},
	}, edges)
	q := planQuery(t, snap, `MATCH (x:A)-[:E]->(y:B), (y)-->(x) RETURN x, y`)

	meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxLiveRows: 500}}
	if _, err := runQuery(&Env{Snap: snap}, q, meter); !errors.Is(err, ErrBudget) {
		t.Fatalf("err = %v, want ErrBudget (%d closing-step rows against a 500-row live cap)", err, back)
	}

	// Served whole once the cap admits it: the bound refuses the size, not
	// the shape.
	rs, err := runQuery(&Env{Snap: snap}, q, &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxLiveRows: 1000}})
	if err != nil {
		t.Fatalf("runQuery with a sufficient cap: %v", err)
	}
	if len(rs.Rows) != back {
		t.Fatalf("got %d rows, want %d (one per closing edge)", len(rs.Rows), back)
	}
}
