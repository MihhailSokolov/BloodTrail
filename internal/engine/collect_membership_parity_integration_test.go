// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherParenthesisedCollectMembershipMatchesOracle compares the
// WITH COLLECT(...) anti-join spellings with PostgreSQL. dawgs lowers only
// the bare `x IN ex` to id membership (`n.id = any(<id array>)`). A
// parenthesised operand is translated as a plain comparison instead: `x IN
// (ex)` compares a node with an array of node composites, and `(x) IN ex` a
// node with bigint -- errors PostgreSQL raises before reading a row. The
// planner unwrapped the parentheses and served the membership answer.
// Parentheses around the whole comparison, `NOT (x IN ex)`, keep the bare
// operands and stay served.
func TestTryCypherParenthesisedCollectMembershipMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"CmUser"}, props: map[string]any{"name": "u0"}},
		{kinds: []string{"CmUser"}, props: map[string]any{"name": "u1"}},
		{kinds: []string{"CmGroup"}, props: map[string]any{"name": "g0"}},
		{kinds: []string{"CmGroup"}, props: map[string]any{"name": "g1"}},
	}, []plannerShapeEdge{{0, 2, "CmMember"}})

	const collect = `MATCH (u:CmUser)-[:CmMember]->(g:CmGroup) WITH COLLECT(g) AS ex MATCH (x:CmGroup) `
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{collect + `WHERE NOT x IN ex RETURN x`, true},
		{collect + `WHERE x IN ex RETURN x`, true},
		{collect + `WHERE NOT (x IN ex) RETURN x`, true},

		{collect + `WHERE NOT x IN (ex) RETURN x`, false},
		{collect + `WHERE x IN (ex) RETURN x`, false},
		{collect + `WHERE NOT (x) IN ex RETURN x`, false},
		{collect + `WHERE (x) IN ex RETURN x`, false},
		{collect + `WHERE NOT (x) IN (ex) RETURN x`, false},
	})
}
