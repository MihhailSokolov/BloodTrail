// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherOptionalMatchPrunedMandatoryNodeMatchesOracle compares an
// OPTIONAL MATCH after a mandatory pattern with a node nothing else in the
// query refers to with PostgreSQL. dawgs does not carry such a node into
// the mandatory CTE -- `MATCH (u)-[:M]->(x) OPTIONAL MATCH ...` selects
// only u -- so two mandatory rows that differ only in x are the same row
// to it, and its left join, built FROM that CTE and joined back ON it,
// returns each of their optional matches k*k times rather than k. The
// engine's repeated-row guard compared x as well and saw no repeat.
// Projecting the node, or DISTINCT, keeps the answer the same in both.
func TestTryCypherOptionalMatchPrunedMandatoryNodeMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"OpU"}, props: map[string]any{"name": "u0"}},
		{kinds: []string{"OpU"}, props: map[string]any{"name": "u1"}},
		{kinds: []string{"OpG"}, props: map[string]any{"name": "g0"}},
		{kinds: []string{"OpG"}, props: map[string]any{"name": "g1"}},
		{kinds: []string{"OpX"}, props: map[string]any{"name": "x"}},
	}, []plannerShapeEdge{{0, 2, "OpM"}, {0, 3, "OpM"}, {1, 4, "OpM"}})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (u:OpU)-[:OpM]->(x) OPTIONAL MATCH (u)-[:OpM]->(g:OpG) RETURN u, x, g`, true},
		{`MATCH (u:OpU)-[:OpM]->(x) OPTIONAL MATCH (u)-[:OpM]->(g:OpG) RETURN DISTINCT u, g`, true},

		{`MATCH (u:OpU)-[:OpM]->(x) OPTIONAL MATCH (u)-[:OpM]->(g:OpG) RETURN u, g`, false},
		{`MATCH (u:OpU)-[:OpM]->(x) OPTIONAL MATCH (u)-[:OpM]->(g:OpG) RETURN count(*) AS c`, false},
		{`MATCH (u:OpU)-[:OpM]->() OPTIONAL MATCH (u)-[:OpM]->(g:OpG) RETURN u, g`, false},
	})
}
