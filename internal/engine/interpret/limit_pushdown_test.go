// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Two routes stop producing rows at the query's own LIMIT instead of leaving
// that to the pipeline: the reverse var-length walk (reverseTrailRowCap) and
// shortestPath (shortestPathLimit). Both count rows BEFORE Part.Where runs,
// so each may stop early only if Part.Where cannot reject anything it
// produced. They used to treat every conjunct pushed onto an endpoint -- and
// the `a <> t` endpoint inequality -- as already applied, when the walk
// tested its near endpoint on kinds alone, a non-narrowing shortestPath side
// reached traverse as a bare kind bitmap, and no var-length walk checks the
// inequality at all. Each such query stopped at LIMIT rows, Part.Where then
// dropped some, and a short answer came back. The tests below pin every row
// count against the unlimited answer: LIMIT k must serve min(k, all) rows,
// every one of them a row of the unlimited answer.

// assertLimitServesPrefix runs query (a format string taking the LIMIT) for
// every LIMIT from 1 to one past the unlimited row count, asserting each
// serves exactly min(LIMIT, unlimited) rows drawn from the unlimited answer.
func assertLimitServesPrefix(t *testing.T, snap *snapshot.View, unlimited, limited string) {
	t.Helper()
	all := mustExec(t, snap, unlimited, generousBudget)
	if len(all.Rows) < 2 {
		t.Fatalf("fixture too small to observe a short answer: %q returns %d rows", unlimited, len(all.Rows))
	}
	valid := map[string]bool{}
	for _, k := range rowKeys(all.Rows) {
		valid[k] = true
	}
	for limit := 1; limit <= len(all.Rows)+1; limit++ {
		query := fmt.Sprintf(limited, limit)
		rs := mustExec(t, snap, query, generousBudget)
		if want := min(limit, len(all.Rows)); len(rs.Rows) != want {
			t.Fatalf("%s: served %d rows, want %d (the unlimited query returns %d)", query, len(rs.Rows), want, len(all.Rows))
		}
		for _, k := range rowKeys(rs.Rows) {
			if !valid[k] {
				t.Fatalf("%s: served %s, which is not a row of the unlimited answer", query, k)
			}
		}
	}
}

// buildNestedAdminsFixture is the "All Domain Admins" prebuilt's own shape:
// DA (a Group whose objectid ends in -512) <- a nested group NG <- n users,
// all over MemberOf. The reverse walk from DA reaches NG first, so NG is the
// first row a capped walk produces -- and the first one the query's WHERE
// rejects.
func buildNestedAdminsFixture(t *testing.T, n int) *snapshot.View {
	t.Helper()
	const (
		kiUser     snapshot.KindID = 1
		kiGroup    snapshot.KindID = 2
		kiComputer snapshot.KindID = 3
		keMemberOf snapshot.KindID = 4
	)
	kinds := map[snapshot.KindID]string{kiUser: "User", kiGroup: "Group", kiComputer: "Computer", keMemberOf: "MemberOf"}
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kiGroup}, map[string]any{"name": "DA", "objectid": "S-1-5-21-1-512"}},
		{2, []snapshot.KindID{kiGroup}, map[string]any{"name": "NG", "objectid": "S-1-5-21-1-1105"}},
	}
	edges := []execEdgeSpec{{1, 2, 1, keMemberOf}}
	for i := 0; i < n; i++ {
		id := uint64(100 + i)
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{kiUser}, map[string]any{"name": fmt.Sprintf("U%d", i), "objectid": fmt.Sprintf("S-1-5-21-1-%d", 2000+i)}})
		edges = append(edges, execEdgeSpec{uint64(1000 + i), id, 2, keMemberOf})
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestReverseTrailCapCountsOnlyAdmittedNearEndpoints: the near endpoint's
// pushed predicates -- a kind disjunction, a negated property test -- are
// enforced by the walk itself, so a capped walk counts only rows the WHERE
// keeps. It used to count NG toward the LIMIT: `LIMIT 5` served four rows.
// The cap must stay ON for these shapes (the shipped prebuilt is one of
// them), so the fix is pinned as "counts right", not "stopped capping".
func TestReverseTrailCapCountsOnlyAdmittedNearEndpoints(t *testing.T) {
	snap := buildNestedAdminsFixture(t, 20)

	for _, where := range []string{
		`(a:User OR a:Computer) AND t.objectid ENDS WITH '-512'`,
		`NOT a.objectid ENDS WITH '-1105' AND t.objectid ENDS WITH '-512'`,
	} {
		for _, ret := range []string{`RETURN a`, `RETURN p`} {
			unlimited := `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE ` + where + ` ` + ret
			t.Run(unlimited, func(t *testing.T) {
				part, step := varLengthPartAndStep(t, snap, unlimited)
				if !varLengthReverseEligible(&Env{Snap: snap}, part, step) {
					t.Fatalf("not routed through the reverse walk; this test would be vacuous")
				}
				if got := reverseTrailRowCap(&workMeter{limitTargetSet: true, limitTarget: 5}, part, step); got != 5 {
					t.Fatalf("reverseTrailRowCap = %d, want the LIMIT (5): the near endpoint's predicates are enforced by the walk, so the cap must stay on", got)
				}
				assertLimitServesPrefix(t, snap, unlimited, unlimited+` LIMIT %d`)
			})
		}
	}
}

// TestReverseTrailCapLeavesEndpointInequalityResidual: no var-length walk
// checks `a <> t`, and a trail that cycles back to its own seed binds a ==
// t. Here T <-> X is a two-cycle, so the reverse walk from T reaches T again
// through X -- and, with these ids, does so before any user. The inequality
// used to be waived as if the walk enforced it, so the self row counted
// toward the LIMIT and the WHERE then dropped it: `LIMIT 2` served one row.
func TestReverseTrailCapLeavesEndpointInequalityResidual(t *testing.T) {
	const (
		kiUser     snapshot.KindID = 1
		kiGroup    snapshot.KindID = 2
		keMemberOf snapshot.KindID = 3
	)
	kinds := map[snapshot.KindID]string{kiUser: "User", kiGroup: "Group", keMemberOf: "MemberOf"}
	nodes := []execNodeSpec{{2, []snapshot.KindID{kiGroup}, map[string]any{"name": "X", "objectid": "S-1-5-21-1-1105"}}}
	var edges []execEdgeSpec
	for i := 0; i < 20; i++ {
		id := uint64(100 + i)
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{kiUser}, map[string]any{"name": fmt.Sprintf("U%d", i)}})
		edges = append(edges, execEdgeSpec{uint64(1000 + i), id, 2, keMemberOf})
	}
	nodes = append(nodes, execNodeSpec{500, []snapshot.KindID{kiGroup}, map[string]any{"name": "T", "objectid": "S-1-5-21-1-512"}})
	edges = append(edges, execEdgeSpec{1, 500, 2, keMemberOf}, execEdgeSpec{2, 2, 500, keMemberOf})
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	unlimited := `MATCH (t:Group)<-[:MemberOf*1..]-(a) WHERE t.objectid ENDS WITH '-512' AND a <> t RETURN a`
	part, step := varLengthPartAndStep(t, snap, unlimited)
	if !varLengthReverseEligible(&Env{Snap: snap}, part, step) {
		t.Fatalf("not routed through the reverse walk; this test would be vacuous")
	}
	if got := reverseTrailRowCap(&workMeter{limitTargetSet: true, limitTarget: 2}, part, step); got != 0 {
		t.Fatalf("reverseTrailRowCap = %d, want 0: the walk does not enforce a <> t", got)
	}
	assertLimitServesPrefix(t, snap, unlimited, unlimited+` LIMIT %d`)
}

// buildShortestPathTerminalFixture: ten users, each MemberOf the group G and
// MemberOf a node of kind Other whose name starts with X. Every user has a
// shortest path to both, so an unfiltered terminal set holds twice the pairs
// the WHERE keeps.
func buildShortestPathTerminalFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const (
		kiUser     snapshot.KindID = 1
		kiGroup    snapshot.KindID = 2
		kiOther    snapshot.KindID = 3
		kiDomain   snapshot.KindID = 4
		keMemberOf snapshot.KindID = 5
	)
	kinds := map[snapshot.KindID]string{kiUser: "User", kiGroup: "Group", kiOther: "Other", kiDomain: "Domain", keMemberOf: "MemberOf"}
	nodes := []execNodeSpec{{1, []snapshot.KindID{kiGroup}, map[string]any{"name": "G"}}}
	var edges []execEdgeSpec
	for i := 0; i < 10; i++ {
		nodes = append(nodes, execNodeSpec{uint64(100 + i), []snapshot.KindID{kiUser}, map[string]any{"name": fmt.Sprintf("U%d", i)}})
	}
	for i := 0; i < 10; i++ {
		nodes = append(nodes, execNodeSpec{uint64(200 + i), []snapshot.KindID{kiOther}, map[string]any{"name": fmt.Sprintf("X%d", i)}})
		edges = append(edges,
			execEdgeSpec{uint64(1000 + i), uint64(100 + i), uint64(200 + i), keMemberOf},
			execEdgeSpec{uint64(2000 + i), uint64(100 + i), 1, keMemberOf})
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestShortestPathLimitCountsOnlyEnforcedEndpoints: a shortestPath side
// that does not narrow reaches traverse as a kind bitmap (or as every node),
// so a kind disjunction or a negation pushed onto it is applied only by
// Part.Where, after traverse has already stopped at LIMIT paths. Counting it
// as applied served two paths for `LIMIT 3`. A kind test folded into the
// bitmap (`WHERE (t:Group)`) IS enforced by traverse and keeps its pushdown.
func TestShortestPathLimitCountsOnlyEnforcedEndpoints(t *testing.T) {
	snap := buildShortestPathTerminalFixture(t)

	for _, where := range []string{
		`(t:Group OR t:Domain) AND s <> t`,
		`NOT t.name STARTS WITH 'X' AND s <> t`,
		`(t:Group) AND s <> t`,
	} {
		unlimited := `MATCH p = shortestPath((s:User)-[:MemberOf*1..]->(t)) WHERE ` + where + ` RETURN p`
		t.Run(unlimited, func(t *testing.T) {
			assertLimitServesPrefix(t, snap, unlimited, unlimited+` LIMIT %d`)
		})
	}
}

// TestResolveEndpointReportsPredicateEnforcement pins which shortestPath
// endpoint shapes resolveEndpoint evaluates every pushed predicate of --
// the input shortestPathLimit relies on to decide whether a LIMIT may stop
// traverse early.
func TestResolveEndpointReportsPredicateEnforcement(t *testing.T) {
	snap := buildShortestPathTerminalFixture(t)
	env := &Env{Snap: snap}

	for _, tc := range []struct {
		query    string
		enforced bool
	}{
		// A kind disjunction or a negation does not narrow, so t reaches
		// traverse unmaterialized and neither is evaluated.
		{`MATCH p = shortestPath((s:User)-[:MemberOf*1..]->(t)) WHERE (t:Group OR t:Domain) AND s <> t RETURN p`, false},
		{`MATCH p = shortestPath((s:User)-[:MemberOf*1..]->(t)) WHERE NOT t.name STARTS WITH 'X' AND s <> t RETURN p`, false},
		{`MATCH p = shortestPath((s:User)-[:MemberOf*1..]->(t:Group)) WHERE NOT t:Domain AND s <> t RETURN p`, false},
		// A single kind is folded into t's Kinds, so the bitmap enforces it.
		{`MATCH p = shortestPath((s:User)-[:MemberOf*1..]->(t)) WHERE (t:Group) AND s <> t RETURN p`, true},
		{`MATCH p = shortestPath((s:User)-[:MemberOf*1..]->(t:Group)) WHERE s <> t RETURN p`, true},
		// A narrowing predicate materializes t through scanAnchor, which
		// evaluates every predicate -- the negation along with the rest.
		{`MATCH p = shortestPath((s:User)-[:MemberOf*1..]->(t)) WHERE t.name = 'G' AND NOT t.name STARTS WITH 'X' AND s <> t RETURN p`, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			part, step := shortestPathPartAndStep(t, snap, tc.query)
			if _, enforced, err := resolveEndpoint(env, &workMeter{budget: generousBudget}, step.FromSym, part.Nodes[step.FromSym]); err != nil || !enforced {
				t.Fatalf("resolveEndpoint(s) = enforced %v, err %v; s carries no predicate, so nothing can be left unenforced", enforced, err)
			}
			_, enforced, err := resolveEndpoint(env, &workMeter{budget: generousBudget}, step.ToSym, part.Nodes[step.ToSym])
			if err != nil {
				t.Fatalf("resolveEndpoint(t): %v", err)
			}
			if enforced != tc.enforced {
				t.Fatalf("resolveEndpoint(t) reports predicates enforced = %v, want %v", enforced, tc.enforced)
			}
		})
	}
}
