// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

const dsKindUser snapshot.KindID = 1

func buildDistinctFixture(t *testing.T, users int) *snapshot.View {
	t.Helper()
	nodes := make([]execNodeSpec, 0, users)
	for i := 0; i < users; i++ {
		nodes = append(nodes, execNodeSpec{uint64(i + 1), []snapshot.KindID{dsKindUser}, map[string]any{
			"enabled": i%2 == 0,
			"dept":    fmt.Sprintf("DEPT%d", i%7),
			"name":    fmt.Sprintf("USER%05d", i),
		}})
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{dsKindUser: "User"}, nodes, nil)
}

// TestDistinctStreamsInsteadOfDeclining pins the behaviour change: a DISTINCT
// projection over a scan far larger than the row budget is SERVED, because
// the budget bounds the answer and the answer is small.
//
// Before, `MATCH (u:User) RETURN DISTINCT u.enabled LIMIT 1000` materialized
// one row per User, blew MaxRows, and was handed to PostgreSQL -- after
// spending the scan, or (later) after an early decline that avoided the scan
// but still delegated. Two rows is the whole answer.
func TestDistinctStreamsInsteadOfDeclining(t *testing.T) {
	const users = 20000
	snap := buildDistinctFixture(t, users)

	// A row budget two orders of magnitude below the candidate count.
	meter := &workMeter{budget: Budgets{MaxRows: 100, MaxWork: 10_000_000, MaxLiveRows: 100}}
	rs, err := runQuery(&Env{Snap: snap},
		planQuery(t, snap, `MATCH (u:User) RETURN DISTINCT u.enabled LIMIT 1000`), meter)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (enabled is true or false)", len(rs.Rows))
	}
}

// TestDistinctStreamingSelectsTheRightRows is the correctness half. The
// fixture is built so every expected count is derivable rather than observed:
// 400 users, `enabled` alternating, `dept` cycling through seven values. Two
// and seven are coprime, so every (enabled, dept) pair occurs.
func TestDistinctStreamingSelectsTheRightRows(t *testing.T) {
	snap := buildDistinctFixture(t, 400)
	generous := Budgets{MaxRows: 1_000_000, MaxWork: 10_000_000, MaxLiveRows: 1_000_000}

	for _, tc := range []struct {
		query string
		want  int
	}{
		{`MATCH (u:User) RETURN DISTINCT u.enabled`, 2},
		{`MATCH (u:User) RETURN DISTINCT u.dept`, 7},
		{`MATCH (u:User) RETURN DISTINCT u.dept LIMIT 3`, 3},
		{`MATCH (u:User) RETURN DISTINCT u.dept SKIP 2 LIMIT 3`, 3},
		{`MATCH (u:User) RETURN DISTINCT u.dept SKIP 5`, 2},
		{`MATCH (u:User) RETURN DISTINCT u.dept SKIP 99`, 0},
		{`MATCH (u:User) RETURN DISTINCT u.enabled, u.dept`, 14},
		{`MATCH (u:User) WHERE u.enabled = true RETURN DISTINCT u.dept`, 7},
		{`MATCH (u:User) WHERE u.dept = 'DEPT3' RETURN DISTINCT u.dept`, 1},
		{`MATCH (u:User) WHERE u.dept = 'NOPE' RETURN DISTINCT u.dept`, 0},
		{`MATCH (u:User) RETURN DISTINCT u.name LIMIT 5`, 5},
		{`MATCH (u:User) RETURN DISTINCT u.name`, 400},
	} {
		t.Run(tc.query, func(t *testing.T) {
			rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, tc.query), &workMeter{budget: generous})
			if err != nil {
				t.Fatalf("runQuery: %v", err)
			}
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
			seen := map[string]struct{}{}
			for _, r := range rs.Rows {
				k := rowDistinctKey(&Env{Snap: snap}, r)
				if _, dup := seen[k]; dup {
					t.Fatalf("a DISTINCT projection yielded the same tuple twice")
				}
				seen[k] = struct{}{}
			}
		})
	}
}

// TestDistinctStreamingStopsEarly pins the early termination: once SKIP+LIMIT
// distinct tuples exist, no later row can change the result, so the scan does
// not finish. The property has seven values and the query asks for two.
func TestDistinctStreamingStopsEarly(t *testing.T) {
	const users = 50000
	snap := buildDistinctFixture(t, users)

	full := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxWork: 100_000_000, MaxLiveRows: 1_000_000}}
	if _, err := runQuery(&Env{Snap: snap},
		planQuery(t, snap, `MATCH (u:User) RETURN DISTINCT u.dept`), full); err != nil {
		t.Fatalf("full: %v", err)
	}

	early := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxWork: 100_000_000, MaxLiveRows: 1_000_000}}
	rs, err := runQuery(&Env{Snap: snap},
		planQuery(t, snap, `MATCH (u:User) RETURN DISTINCT u.dept LIMIT 2`), early)
	if err != nil {
		t.Fatalf("early: %v", err)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rs.Rows))
	}
	if early.work >= full.work {
		t.Fatalf("LIMIT 2 spent %d work against the full scan's %d: it did not stop early",
			early.work, full.work)
	}
}
