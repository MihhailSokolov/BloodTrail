// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

const (
	osUser     snapshot.KindID = 1
	osGroup    snapshot.KindID = 2
	osMemberOf snapshot.KindID = 3
)

// buildOptionalSeedFixture: `users` users, of which `admins` carry
// admincount=true. Every user is a member of `groupsPer` groups, except the
// LAST admin, who is a member of none -- so a left join must keep that admin
// with its optional side unbound, which is the property a correlated
// OPTIONAL MATCH can most easily lose.
func buildOptionalSeedFixture(t *testing.T, users, admins, groupsPer int) *snapshot.View {
	t.Helper()
	kinds := map[snapshot.KindID]string{osUser: "User", osGroup: "Group", osMemberOf: "MemberOf"}
	const groups = 50
	var nodes []execNodeSpec
	for g := 0; g < groups; g++ {
		nodes = append(nodes, execNodeSpec{uint64(1 + g), []snapshot.KindID{osGroup},
			map[string]any{"name": fmt.Sprintf("G%d", g)}})
	}
	for i := 0; i < users; i++ {
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), []snapshot.KindID{osUser},
			map[string]any{"admincount": i < admins, "name": fmt.Sprintf("U%d", i)}})
	}
	var edges []execEdgeSpec
	eid := uint64(0)
	for i := 0; i < users; i++ {
		if i == admins-1 {
			continue // the admin with no groups
		}
		for k := 0; k < groupsPer; k++ {
			eid++
			edges = append(edges, execEdgeSpec{eid, uint64(1000 + i), uint64(1 + (i+k)%groups), osMemberOf})
		}
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestOptionalMatchIsSeededFromTheMandatoryRows pins the performance property:
// the optional pattern is matched outward from the nodes the mandatory side
// bound, not across the whole graph and joined afterwards.
//
// The budget covers the handful of admins and their memberships and nowhere
// near every user's, which an uncorrelated match would have to enumerate.
func TestOptionalMatchIsSeededFromTheMandatoryRows(t *testing.T) {
	const users, admins, groupsPer = 5000, 4, 3
	snap := buildOptionalSeedFixture(t, users, admins, groupsPer)

	tight := Budgets{MaxRows: 1000, MaxWork: 500, MaxLiveRows: 1000}
	rs := mustExec(t, snap,
		`MATCH (u:User) WHERE u.admincount = true OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`,
		tight)
	// Three admins with three groups each, plus the one with none.
	if want := (admins-1)*groupsPer + 1; len(rs.Rows) != want {
		t.Fatalf("got %d rows, want %d", len(rs.Rows), want)
	}
}

// TestOptionalMatchSeedKeepsTheAnswer is the correctness half, across the
// shapes the seed must not disturb.
func TestOptionalMatchSeedKeepsTheAnswer(t *testing.T) {
	const users, admins, groupsPer = 300, 5, 2
	snap := buildOptionalSeedFixture(t, users, admins, groupsPer)
	loose := Budgets{MaxRows: 1_000_000, MaxWork: 100_000_000, MaxLiveRows: 1_000_000}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			name:  "unmatched mandatory rows survive with the optional side unbound",
			query: `MATCH (u:User) WHERE u.admincount = true OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`,
			want:  (admins-1)*groupsPer + 1,
		},
		{
			name: "a predicate on the optional side narrows only the optional rows",
			// Every non-matching admin still survives, now with g unbound.
			query: `MATCH (u:User) WHERE u.admincount = true OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) WHERE g.name = 'G1' RETURN u, g`,
			want:  admins,
		},
		{
			name:  "a wide mandatory side still joins every row",
			query: `MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`,
			want:  (users-1)*groupsPer + 1,
		},
		{
			name:  "a mandatory side matching nothing yields nothing",
			query: `MATCH (u:User) WHERE u.name = 'NOBODY' OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`,
			want:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, loose)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}

// TestSeedOptionalPartLeavesThePlanUntouched: NodeConstraints are read-only
// once planned and a Part is reused by every later execution, so seeding
// must work on a copy.
func TestSeedOptionalPartLeavesThePlanUntouched(t *testing.T) {
	snap := buildOptionalSeedFixture(t, 40, 3, 1)
	q := planQuery(t, snap,
		`MATCH (u:User) WHERE u.admincount = true OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) RETURN u, g`)
	part := &q.Parts[0]
	if part.Optional == nil {
		t.Fatal("fixture query did not plan an OPTIONAL part")
	}
	before := part.Optional.Nodes["u"]
	var beforeIndexed bool
	if before != nil {
		beforeIndexed = before.PropIndexed
	}

	loose := Budgets{MaxRows: 1_000_000, MaxWork: 100_000_000, MaxLiveRows: 1_000_000}
	for i := 0; i < 3; i++ {
		if _, err := Execute(&Env{Snap: snap}, q, loose); err != nil {
			t.Fatalf("Execute: %v", err)
		}
	}
	after := part.Optional.Nodes["u"]
	if after != before {
		t.Fatal("the plan's own NodeConstraint pointer was replaced")
	}
	if after != nil && after.PropIndexed != beforeIndexed {
		t.Fatal("the plan's own NodeConstraint was mutated by a seed")
	}
}
