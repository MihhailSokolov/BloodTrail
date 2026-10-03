// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// orderByNameFixture: four groups whose v is 5, a stored null, absent and 1,
// and four users with 1..4 memberships each group's way.
func orderByNameFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const (
		kUser, kGroup snapshot.KindID = 1, 2
		kMemberOf     snapshot.KindID = 10
	)
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kUser}, map[string]any{"name": "u1"}},
		{2, []snapshot.KindID{kUser}, map[string]any{"name": "u2"}},
		{3, []snapshot.KindID{kUser}, map[string]any{"name": "u3"}},
		{4, []snapshot.KindID{kUser}, map[string]any{"name": "u4"}},
		{11, []snapshot.KindID{kGroup}, map[string]any{"name": "g1", "v": 5}},
		{12, []snapshot.KindID{kGroup}, map[string]any{"name": "g2", "v": nil}},
		{13, []snapshot.KindID{kGroup}, map[string]any{"name": "g3"}},
		{14, []snapshot.KindID{kGroup}, map[string]any{"name": "g4", "v": 1}},
	}
	edges := []execEdgeSpec{
		{100, 1, 11, kMemberOf},
		{101, 1, 12, kMemberOf}, {102, 2, 12, kMemberOf},
		{103, 1, 13, kMemberOf}, {104, 2, 13, kMemberOf}, {105, 3, 13, kMemberOf},
		{106, 1, 14, kMemberOf}, {107, 2, 14, kMemberOf}, {108, 3, 14, kMemberOf}, {109, 4, 14, kMemberOf},
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{kUser: "User", kGroup: "Group", kMemberOf: "MemberOf"}, nodes, edges)
}

// TestPlanDeclinesOrderByNameDAWGSResolvesElsewhere: planOrder resolves a
// bare ORDER BY name to the RETURN item carrying that name. dawgs does not.
// A name the MATCH or WITH scope binds -- a node, an edge, a carried COUNT
// or constant -- sorts by that binding whatever a RETURN alias of the same
// name projects (`RETURN g.v AS g ORDER BY g` is `order by s0.n0`, the
// node), and any other name is emitted as the unquoted output alias, which
// PostgreSQL folds to lower case. (A path variable is emitted as the alias,
// which the engine also sorts by; declining it is a deliberate
// over-decline.) The engine therefore served a different sort key --
// different rows under LIMIT -- or served where PostgreSQL rejects the SQL:
// a shadowed aggregate (42803), a shadowed DISTINCT key (42P10), two
// aliases that fold together (42702). Each such query must decline; the
// shapes where both readings agree must still plan.
func TestPlanDeclinesOrderByNameDAWGSResolvesElsewhere(t *testing.T) {
	snap := orderByNameFixture(t)
	const chain = `MATCH (u:User)-[:MemberOf]->(g:Group) `
	const counted = chain + `WITH g, count(u) AS c `

	declined := []string{
		// A MATCH variable shadowed by a RETURN alias.
		`MATCH (g:Group) RETURN g.v AS g ORDER BY g`,
		`MATCH (g:Group) RETURN g.v AS g ORDER BY g LIMIT 1`,
		`MATCH (g:Group) RETURN g.v AS g ORDER BY g DESC`,
		`MATCH (g:Group) RETURN id(g) AS g ORDER BY g DESC`,
		`MATCH (g:Group) RETURN DISTINCT g.v AS g ORDER BY g`,
		chain + `RETURN g.v AS u ORDER BY u LIMIT 3`,
		chain + `RETURN g.name AS name, g.v AS u ORDER BY u DESC LIMIT 3`,
		chain + `RETURN g.name AS name, id(u) AS g ORDER BY g DESC LIMIT 2`,
		`MATCH (u:User)-[r:MemberOf]->(g:Group) RETURN id(g) AS r ORDER BY r DESC LIMIT 3`,
		`MATCH p = (u:User)-[:MemberOf]->(g:Group) RETURN id(u) AS p ORDER BY p`,
		// ... and one an aggregate folds (pg: 42803, not grouped).
		chain + `RETURN count(u) AS u ORDER BY u`,
		chain + `RETURN g, count(u) AS u ORDER BY u DESC`,
		// A carried COUNT or constant shadowed by a RETURN alias.
		counted + `RETURN g, g.v AS c ORDER BY c`,
		counted + `RETURN g, g.v AS c ORDER BY c DESC`,
		counted + `RETURN g.name AS name, g.v AS c ORDER BY c LIMIT 2`,
		counted + `RETURN g.name AS name, g.v AS c ORDER BY c DESC LIMIT 1`,
		counted + `RETURN c AS n, g.v AS c ORDER BY c`,
		counted + `RETURN DISTINCT g.v AS c ORDER BY c`,
		counted + `RETURN g.v AS g ORDER BY g`,
		counted + `RETURN g.v AS g ORDER BY g DESC LIMIT 2`,
		`MATCH (g:Group) WITH g, 1 AS k RETURN g.v AS k ORDER BY k`,
		// Two output columns PostgreSQL folds to the same name (42702).
		`MATCH (g:Group) RETURN g.v AS x, id(g) AS X ORDER BY X DESC`,
		`MATCH (g:Group) RETURN id(g) AS x, g.v AS X ORDER BY x DESC`,
		chain + `RETURN g, count(u) AS c, count(DISTINCT u) AS C ORDER BY c DESC`,
		// A parenthesised name: dawgs rewrites only a bare identifier to the
		// projection, so `ORDER BY (i)` is emitted as `order by (i0)`, a
		// column that does not exist (42703).
		`MATCH (g:Group) RETURN id(g) AS i ORDER BY (i)`,
		`MATCH (g:Group) RETURN g, id(g) AS i ORDER BY (i) LIMIT 1`,
		`MATCH (g:Group) RETURN g.v AS gv ORDER BY ((gv)) DESC`,
		chain + `RETURN g, count(u) AS c ORDER BY (c)`,
		`MATCH (g:Group) RETURN count(g) AS c ORDER BY (c)`,
		counted + `RETURN g, c ORDER BY (c)`,
		// A name PostgreSQL reserves, emitted unquoted: a syntax error (42601)
		// or, for the reserved words that are SQL value functions (user,
		// current_date, true), a constant sort key.
		`MATCH (g:Group) RETURN id(g) AS select ORDER BY select`,
		`MATCH (g:Group) RETURN id(g) AS table ORDER BY table`,
		`MATCH (g:Group) RETURN id(g) AS group ORDER BY group DESC`,
		`MATCH (g:Group) RETURN id(g) AS SELECT ORDER BY SELECT`,
		`MATCH (g:Group) RETURN count(g) AS select ORDER BY select`,
		`MATCH (g:Group) RETURN g.v AS user ORDER BY user`,
		`MATCH (g:Group) RETURN g.v AS left ORDER BY left LIMIT 2`,
	}
	served := []string{
		counted + `RETURN g, c ORDER BY c`,
		counted + `RETURN g, c ORDER BY c DESC LIMIT 2`,
		counted + `RETURN g ORDER BY c DESC`,
		counted + `RETURN g.name AS g ORDER BY c`,
		chain + `RETURN g.name AS name, count(u) AS c ORDER BY c DESC`,
		chain + `RETURN g, count(u) AS g2 ORDER BY g2 DESC`,
		`MATCH (g:Group) RETURN g.v AS gv ORDER BY gv`,
		`MATCH (g:Group) RETURN g.v AS g ORDER BY g.v`,
		`MATCH (g:Group) RETURN id(g) AS i ORDER BY i DESC`,
		// A keyword PostgreSQL does not reserve is an ordinary column name.
		`MATCH (g:Group) RETURN id(g) AS name ORDER BY name`,
		`MATCH (g:Group) RETURN id(g) AS value ORDER BY value DESC`,
	}

	plans := func(query string) bool {
		t.Helper()
		rq, err := frontend.ParseCypher(frontend.NewContext(), query)
		if err != nil {
			t.Fatalf("ParseCypher(%q): %v", query, err)
		}
		_, ok := Plan(rq, snap)
		return ok
	}
	for _, query := range declined {
		if plans(query) {
			t.Errorf("planned, want a decline: %s", query)
		}
	}
	for _, query := range served {
		if !plans(query) {
			t.Errorf("declined, want it planned: %s", query)
		}
	}
}
