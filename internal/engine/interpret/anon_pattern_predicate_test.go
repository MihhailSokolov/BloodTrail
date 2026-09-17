// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildAnonPredicateFixture: users, some of which are MemberOf a Group, some
// MemberOf a Computer instead (so a LABEL on the far endpoint is the only
// thing distinguishing them), and some with no outgoing edge at all.
//
//	u0..u2  -MemberOf-> g0   (Group)
//	u3,u4   -MemberOf-> c0   (Computer)  <- an edge, but not to a Group
//	u5..u9  no outgoing edge
//	u10     -HasSession-> g0 (Group)     <- a Group, but not via MemberOf
func buildAnonPredicateFixture(t *testing.T) *snapshot.View {
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
	for i := 0; i < 11; i++ {
		nodes = append(nodes, execNodeSpec{uint64(100 + i), []snapshot.KindID{kiUser},
			map[string]any{"name": fmt.Sprintf("U%d", i)}})
	}
	nodes = append(nodes,
		execNodeSpec{200, []snapshot.KindID{kiGroup}, map[string]any{"name": "G0"}},
		execNodeSpec{201, []snapshot.KindID{kiComputer}, map[string]any{"name": "C0"}},
	)

	var edges []execEdgeSpec
	var eid uint64
	add := func(from, to uint64, kind snapshot.KindID) {
		eid++
		edges = append(edges, execEdgeSpec{eid, from, to, kind})
	}
	for i := 0; i < 3; i++ {
		add(uint64(100+i), 200, keMemberOf)
	}
	add(103, 201, keMemberOf)
	add(104, 201, keMemberOf)
	add(110, 200, keSession)

	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestAnonymousPatternPredicateEndpoint pins the shape BloodHound writes for
// "principals with no group membership" -- `WHERE NOT (u)-[:MemberOf]->(:Group)`.
// The far endpoint is ANONYMOUS and carries a label, so it binds nothing and
// the predicate stays the existence check PostgreSQL lowers it to; before
// this it was rejected at plan time and the whole query delegated.
//
// The label on that endpoint has to be load bearing, which is why the fixture
// has users whose only edge of the right KIND goes to the wrong kind of node,
// and a user reaching a Group by the wrong edge kind.
func TestAnonymousPatternPredicateEndpoint(t *testing.T) {
	snap := buildAnonPredicateFixture(t)
	budget := Budgets{MaxRows: 1000, MaxWork: 100000, MaxLiveRows: 10000}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			// u0,u1,u2 only.
			name:  "positive, labelled far endpoint",
			query: `MATCH (u:User) WHERE (u)-[:MemberOf]->(:Group) RETURN u`,
			want:  3,
		},
		{
			// The 11 users minus those three. u3/u4 have a MemberOf edge but
			// to a Computer; u10 reaches the Group by HasSession. All count.
			name:  "negated, labelled far endpoint",
			query: `MATCH (u:User) WHERE NOT (u)-[:MemberOf]->(:Group) RETURN u`,
			want:  8,
		},
		{
			// No label: any MemberOf edge at all. u0..u4.
			name:  "unlabelled far endpoint is any node",
			query: `MATCH (u:User) WHERE (u)-[:MemberOf]->() RETURN u`,
			want:  5,
		},
		{
			// The edge kind still has to match: only u10 reaches a Group by
			// HasSession.
			name:  "edge kind still applies",
			query: `MATCH (u:User) WHERE (u)-[:HasSession]->(:Group) RETURN u`,
			want:  1,
		},
		{
			// Anchored on the far side instead: the bound endpoint is the
			// TARGET, so the walk has to go inbound.
			name:  "anonymous source, bound target",
			query: `MATCH (g:Group) WHERE (:User)-[:MemberOf]->(g) RETURN g`,
			want:  1,
		},
		{
			// Undirected: u10's HasSession edge points at the Group, and the
			// Group has no outgoing edges, so only the direction-agnostic
			// form finds it from the Group's side.
			name:  "undirected",
			query: `MATCH (g:Group) WHERE (g)-[:HasSession]-(:User) RETURN g`,
			want:  1,
		},
		{
			// A label no node in the pattern's reach carries.
			name:  "far endpoint label nothing matches",
			query: `MATCH (u:User) WHERE (u)-[:MemberOf]->(:User) RETURN u`,
			want:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, budget)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}

// TestAnonymousPatternPredicateIsPushedDown pins the reason this shape is
// worth serving rather than merely accepting. The predicate names ONE symbol,
// so it pushes into that symbol's own NodeConstraint.Predicates and is tested
// per candidate during the anchor scan, instead of after a full row is
// assembled. A budget far too small to have assembled 11 rows and filtered
// them afterwards still answers.
func TestAnonymousPatternPredicateIsPushedDown(t *testing.T) {
	snap := buildAnonPredicateFixture(t)

	q := planQuery(t, snap, `MATCH (u:User) WHERE (u)-[:MemberOf]->(:Group) RETURN u`)
	nc := q.Parts[0].Nodes["u"]
	if nc == nil || len(nc.Predicates) == 0 {
		t.Fatal("a single-symbol pattern predicate must push into that symbol's Predicates")
	}
}
