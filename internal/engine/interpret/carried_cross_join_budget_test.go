// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// carriedCrossJoinFixture: n users and n groups, v = 0..n-1 on each side,
// and one MemberOf edge from user i to group i.
func carriedCrossJoinFixture(t *testing.T, n int) *snapshot.View {
	t.Helper()
	const (
		kUser, kGroup snapshot.KindID = 1, 2
		kMemberOf     snapshot.KindID = 10
	)
	var nodes []execNodeSpec
	var edges []execEdgeSpec
	for i := 0; i < n; i++ {
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), []snapshot.KindID{kUser}, map[string]any{"v": float64(i)}})
		edges = append(edges, execEdgeSpec{uint64(9_000_000 + i), uint64(1000 + i), uint64(50_000 + i), kMemberOf})
	}
	for i := 0; i < n; i++ {
		nodes = append(nodes, execNodeSpec{uint64(50_000 + i), []snapshot.KindID{kGroup}, map[string]any{"v": float64(i)}})
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{kUser: "User", kGroup: "Group", kMemberOf: "MemberOf"}, nodes, edges)
}

// TestCarriedCrossJoinChargesLiveRowBudget: a WITH boundary followed by a
// MATCH sharing no variable with it is a cross join -- every carried row
// against every row the MATCH finds -- and its product is a materialized row
// set like any other. It used to be built whole with nothing consulting
// MaxLiveRows, so the budget that refuses the same product written as one
// MATCH (cartesianJoin) never tripped: 60x60 rows were served against a
// 500-row cap, and at production budgets a 1,500x1,500 join held 2.25
// million rows (about 1.4 GB) before its WHERE ran.
func TestCarriedCrossJoinChargesLiveRowBudget(t *testing.T) {
	snap := carriedCrossJoinFixture(t, 60)
	for _, query := range []string{
		`MATCH (u:User) WITH u MATCH (g:Group) RETURN u, g`,
		`MATCH (u:User) WITH u MATCH (g:Group) RETURN count(*)`,
		`MATCH (u:User) WITH u MATCH (g:Group) RETURN u, g LIMIT 100000`,
		// The same product in one MATCH: refused already, the reference.
		`MATCH (u:User), (g:Group) WHERE g.v + 0.5 = u.v + 0.5 RETURN u, g`,
	} {
		meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxLiveRows: 500}}
		if _, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query), meter); !errors.Is(err, ErrBudget) {
			t.Errorf("%s\n    err = %v, want ErrBudget (a 3600-row product against a 500-row live cap)", query, err)
		}
	}
}

// TestCarriedCrossJoinFiltersEachSeedBeforeAccumulating: the cross join's
// WHERE runs on each carried row's matches before they accumulate, so a
// selective join serves its answer without ever holding the product -- the
// live-row cap refuses a product only when one is actually held.
func TestCarriedCrossJoinFiltersEachSeedBeforeAccumulating(t *testing.T) {
	snap := carriedCrossJoinFixture(t, 60)
	const query = `MATCH (u:User) WITH u MATCH (g:Group) WHERE g.v = u.v RETURN u, g`
	meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxLiveRows: 500}}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query), meter)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if len(rs.Rows) != 60 {
		t.Fatalf("got %d rows, want 60 (one group per user)", len(rs.Rows))
	}
	for _, row := range rs.Rows {
		if snap.GraphID(row[1].Node)-50_000 != snap.GraphID(row[0].Node)-1000 {
			t.Fatalf("row pairs user %d with group %d, whose v differs", snap.GraphID(row[0].Node), snap.GraphID(row[1].Node))
		}
	}
	if meter.peakRows > 500 {
		t.Fatalf("peakRows = %d, want the product never held (at most 500)", meter.peakRows)
	}
}

// TestCarriedCrossJoinRefusesUnaffordableProductUpFront: every row of the
// product costs its carried row's own match at least one work unit, so a
// product larger than MaxWork cannot finish. It is refused once the first
// carried row has shown how many rows the MATCH yields, instead of after
// the whole budget has been spent finding out.
func TestCarriedCrossJoinRefusesUnaffordableProductUpFront(t *testing.T) {
	snap := carriedCrossJoinFixture(t, 60)
	const query = `MATCH (u:User) WITH u MATCH (g:Group) WHERE g.v = u.v RETURN u, g`
	meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxWork: 2000}}
	if _, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query), meter); !errors.Is(err, ErrBudget) {
		t.Fatalf("err = %v, want ErrBudget (a 3600-row product against a 2000-unit work budget)", err)
	}
	if meter.work >= meter.budget.MaxWork {
		t.Fatalf("refused after spending %d work units, want it refused before the %d-unit budget ran out", meter.work, meter.budget.MaxWork)
	}
}

// TestLimitedDriverChargesAccumulatedRowsToLiveRowBudget: the chunked LIMIT
// driver expands one chunk of anchor rows at a time, and each chunk's own
// expansion stays under the cap -- but the rows it keeps accumulate across
// chunks, and nothing charged them. Under a LIMIT larger than the answer it
// held the same rows the unlimited form is refused for.
func TestLimitedDriverChargesAccumulatedRowsToLiveRowBudget(t *testing.T) {
	snap := carriedCrossJoinFixture(t, 3000)
	for _, query := range []string{
		`MATCH (u:User)-[:MemberOf]->(g:Group) RETURN u, g`,
		`MATCH (u:User)-[:MemberOf]->(g:Group) RETURN u, g LIMIT 100000`,
	} {
		meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxLiveRows: 2000}}
		if _, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query), meter); !errors.Is(err, ErrBudget) {
			t.Errorf("%s\n    err = %v, want ErrBudget (3000 rows against a 2000-row live cap)", query, err)
		}
	}
}
