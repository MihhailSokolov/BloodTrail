// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherOptionalMatchAnonymousNodesMatchesOracle compares OPTIONAL
// MATCH queries with anonymous nodes on both sides with PostgreSQL. The
// planner named each side's anonymous nodes from a counter that restarted
// for the optional pattern, so the optional `(:G)` got the same internal
// name as the mandatory `()` and the two were joined as one node: a row
// whose mandatory `()` was not that group was null-padded instead of
// extended.
func TestTryCypherOptionalMatchAnonymousNodesMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"OaU"}, props: map[string]any{"name": "u0"}},
		{kinds: []string{"OaU"}, props: map[string]any{"name": "u1"}},
		{kinds: []string{"OaG"}, props: map[string]any{"name": "g0"}},
		{kinds: []string{"OaG"}, props: map[string]any{"name": "g1"}},
		{kinds: []string{"OaX"}, props: map[string]any{"name": "x"}},
	}, []plannerShapeEdge{{0, 2, "OaM"}, {0, 3, "OaM"}, {1, 4, "OaM"}})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (u:OaU), () OPTIONAL MATCH (u)-[:OaM]->(:OaG) RETURN u`, false},
		{`MATCH (u:OaU), () OPTIONAL MATCH (u)-[:OaM]->(:OaG) RETURN count(*) AS c`, false},
	})
}
