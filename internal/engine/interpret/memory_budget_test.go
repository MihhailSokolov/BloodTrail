// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildFanOutFixture: one hub every node points at, so a single expansion
// step multiplies rows without bound -- the shape that made the executor
// accumulate tens of gigabytes of live rows under a work budget loose
// enough to permit it.
func buildFanOutFixture(t *testing.T, users int) *snapshot.View {
	t.Helper()
	const (
		fkUser  snapshot.KindID = 1
		fkGroup snapshot.KindID = 2
		fkMem   snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{fkUser: "User", fkGroup: "Group", fkMem: "MemberOf"}
	nodes := []execNodeSpec{{1, []snapshot.KindID{fkGroup}, map[string]any{"objectid": "HUB"}}}
	var edges []execEdgeSpec
	for i := 0; i < users; i++ {
		id := uint64(1000 + i)
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{fkUser},
			map[string]any{"objectid": fmt.Sprintf("U-%d", i)}})
		edges = append(edges, execEdgeSpec{uint64(9_000_000 + i), id, 1, fkMem})
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestLiveRowBudgetDeclinesInsteadOfAccumulating pins the memory bound: a
// query whose intermediate row set exceeds MaxLiveRows is refused with
// ErrBudget -- which the driver turns into a delegation to PostgreSQL --
// rather than being allowed to allocate its way to an OOM kill. The work
// budget deliberately stays unlimited here, because that is exactly the
// configuration the bound exists for: work units cannot express this.
func TestLiveRowBudgetDeclinesInsteadOfAccumulating(t *testing.T) {
	snap := buildFanOutFixture(t, 2000)
	q := planQuery(t, snap, `MATCH (u:User)-[:MemberOf]->(g:Group) RETURN u, g`)

	meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxWork: 0, MaxLiveRows: 500}}
	if _, err := runQuery(&Env{Snap: snap}, q, meter); !errors.Is(err, ErrBudget) {
		t.Fatalf("runQuery error = %v, want ErrBudget (2000 rows against a 500-row live cap)", err)
	}

	// The same query is served whole once the cap admits its row set, so
	// the bound refuses oversized work rather than the shape itself.
	meter = &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxWork: 0, MaxLiveRows: 5000}}
	rs, err := runQuery(&Env{Snap: snap}, q, meter)
	if err != nil {
		t.Fatalf("runQuery with a sufficient cap: %v", err)
	}
	if len(rs.Rows) != 2000 {
		t.Fatalf("got %d rows, want 2000", len(rs.Rows))
	}
	if meter.peakRows < 2000 {
		t.Fatalf("peakRows = %d, want at least the 2000-row intermediate", meter.peakRows)
	}
}

// TestLiveRowBudgetUnlimitedByZero keeps the documented convention the other
// two budget fields use: zero means no cap.
func TestLiveRowBudgetUnlimitedByZero(t *testing.T) {
	snap := buildFanOutFixture(t, 500)
	q := planQuery(t, snap, `MATCH (u:User)-[:MemberOf]->(g:Group) RETURN u, g`)
	meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxWork: 0, MaxLiveRows: 0}}
	if _, err := runQuery(&Env{Snap: snap}, q, meter); err != nil {
		t.Fatalf("MaxLiveRows=0 must mean unlimited, got %v", err)
	}
}
