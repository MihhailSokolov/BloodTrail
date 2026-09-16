// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildOptionalFixture: 6 users, of which u0 is MemberOf TWO groups, u1 is
// MemberOf one, u2 has an edge of the WRONG kind, u3 has an edge to a
// non-Group, and u4/u5 have no edges at all. The point is that every
// null-padding reason is represented -- no edge, wrong edge kind, wrong
// neighbour kind -- and that one row FANS OUT to two.
func buildOptionalFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const (
		kiUser     snapshot.KindID = 1
		kiGroup    snapshot.KindID = 2
		kiComputer snapshot.KindID = 3
		keMemberOf snapshot.KindID = 4
		keSession  snapshot.KindID = 5
	)
	kinds := map[snapshot.KindID]string{
		kiUser: "User", kiGroup: "Group", kiComputer: "Computer",
		keMemberOf: "MemberOf", keSession: "HasSession",
	}

	var nodes []execNodeSpec
	for i := 0; i < 6; i++ {
		nodes = append(nodes, execNodeSpec{uint64(100 + i), []snapshot.KindID{kiUser},
			map[string]any{"name": fmt.Sprintf("U%d", i), "admincount": true}})
	}
	nodes = append(nodes,
		execNodeSpec{200, []snapshot.KindID{kiGroup}, map[string]any{"name": "GA"}},
		execNodeSpec{201, []snapshot.KindID{kiGroup}, map[string]any{"name": "GB"}},
		execNodeSpec{202, []snapshot.KindID{kiComputer}, map[string]any{"name": "C0"}},
	)

	var edges []execEdgeSpec
	var eid uint64
	add := func(from, to uint64, k snapshot.KindID) {
		eid++
		edges = append(edges, execEdgeSpec{eid, from, to, k})
	}
	add(100, 200, keMemberOf) // u0 -> GA
	add(100, 201, keMemberOf) // u0 -> GB   (fan-out to two rows)
	add(101, 200, keMemberOf) // u1 -> GA
	add(102, 200, keSession)  // u2 -> GA, wrong EDGE kind
	add(103, 202, keMemberOf) // u3 -> C0, wrong NEIGHBOUR kind

	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestOptionalMatchIsALeftJoin pins OPTIONAL MATCH against the semantics
// PostgreSQL was observed to have: every row of the mandatory match survives,
// a matching row is emitted once per match, and an unmatched one keeps the
// optional symbols UNBOUND (which projects as a null column -- the live pg
// oracle returns a plain nil for exactly this query, not a node-shaped
// placeholder).
func TestOptionalMatchIsALeftJoin(t *testing.T) {
	snap := buildOptionalFixture(t)
	budget := Budgets{MaxRows: 1000, MaxWork: 100000, MaxLiveRows: 10000}

	t.Run("row count is left-join, not inner-join", func(t *testing.T) {
		// 6 users: u0 fans out to 2, u1 matches once, u2/u3/u4/u5 null-pad.
		rs := mustExec(t, snap,
			`MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`, budget)
		if len(rs.Rows) != 7 {
			t.Fatalf("got %d rows, want 7 (u0 twice, u1 once, four null-padded)", len(rs.Rows))
		}
	})

	t.Run("the unmatched column projects as null, the matched one as a node", func(t *testing.T) {
		rs := mustExec(t, snap,
			`MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`, budget)
		matched, nulls := 0, 0
		for _, out := range rs.Rows {
			if len(out) != 2 {
				t.Fatalf("got %d columns, want 2", len(out))
			}
			if out[0].Kind != OutNode {
				t.Fatalf("column u projected as %v, want OutNode", out[0].Kind)
			}
			switch out[1].Kind {
			case OutNode:
				matched++
			case OutScalar:
				// The null column: a scalar with no value, which
				// materializes to the plain nil PostgreSQL returns.
				if out[1].Scalar != nil {
					t.Fatalf("null g column carried a value: %#v", out[1].Scalar)
				}
				nulls++
			default:
				t.Fatalf("column g projected as %v", out[1].Kind)
			}
		}
		if matched != 3 || nulls != 4 {
			t.Fatalf("got %d node / %d null g columns, want 3 / 4", matched, nulls)
		}
	})

	t.Run("WHERE on the mandatory side runs BEFORE the join", func(t *testing.T) {
		// Restricting to u0 must still yield u0's two group rows.
		rs := mustExec(t, snap,
			`MATCH (u:User) WHERE u.name = 'U0' OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`, budget)
		if len(rs.Rows) != 2 {
			t.Fatalf("got %d rows, want 2", len(rs.Rows))
		}
	})

	t.Run("a mandatory row with no optional match still survives a WHERE", func(t *testing.T) {
		rs := mustExec(t, snap,
			`MATCH (u:User) WHERE u.name = 'U4' OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`, budget)
		if len(rs.Rows) != 1 {
			t.Fatalf("got %d rows, want 1 (null-padded, not dropped)", len(rs.Rows))
		}
		if rs.Rows[0][1].Kind != OutScalar {
			t.Fatal("g must project as a null column")
		}
	})

	t.Run("optional pattern matching nothing at all null-pads every row", func(t *testing.T) {
		rs := mustExec(t, snap,
			`MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf]->(c:Computer) RETURN u, c`, budget)
		// Only u3 has a MemberOf edge to a Computer.
		if len(rs.Rows) != 6 {
			t.Fatalf("got %d rows, want 6", len(rs.Rows))
		}
		bound := 0
		for _, out := range rs.Rows {
			if out[1].Kind == OutNode {
				bound++
			}
		}
		if bound != 1 {
			t.Fatalf("got %d bound c, want 1", bound)
		}
	})
}

// TestOptionalMatchRejectedShapes pins what stays delegated. Each of these
// would need machinery the left join does not have, and declining is safe --
// PostgreSQL answers.
func TestOptionalMatchRejectedShapes(t *testing.T) {
	snap := buildOptionalFixture(t)
	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			// Nothing to its left to join against.
			name:  "leading OPTIONAL MATCH",
			query: `OPTIONAL MATCH (u:User)-[:MemberOf]->(g:Group) RETURN u, g`,
		},
		{
			// A mandatory clause after one would filter rows the optional
			// clause exists to preserve.
			name:  "mandatory MATCH after an OPTIONAL MATCH",
			query: `MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) MATCH (c:Computer) RETURN u, g, c`,
		},
		{
			// Shares no symbol with the mandatory side: a null-padded
			// product, not a lookup.
			name:  "disjoint OPTIONAL MATCH",
			query: `MATCH (u:User) OPTIONAL MATCH (c:Computer) RETURN u, c`,
		},
		{
			// A Part after a WITH boundary is matched by a different driver
			// that never reaches the join.
			name:  "OPTIONAL MATCH after a WITH boundary",
			query: `MATCH (u:User) WITH u OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ok := planNoFail(t, snap, tc.query)
			if !ok || q == nil {
				return // declined at plan time, which is the point
			}
			// Accepted at plan time is only correct if it also declines at
			// run time; anything else means it is being served.
			if _, err := Execute(&Env{Snap: snap}, q,
				Budgets{MaxRows: 1000, MaxWork: 100000, MaxLiveRows: 10000}); err == nil {
				t.Fatal("served a shape the left join cannot answer correctly")
			}
		})
	}
}
