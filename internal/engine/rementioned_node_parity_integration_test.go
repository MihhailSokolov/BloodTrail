// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherRementionedNodeMatchesOracle compares a pattern that is
// nothing but an already-bound node variable -- a second comma-separated
// part, a later MATCH clause, an OPTIONAL MATCH, a MATCH after a WITH --
// with PostgreSQL. dawgs does not join it to the bound node: `MATCH
// (s)-[:R]->(t) MATCH (t)` lowers to `from s0, node n1`, with no condition
// tying n1 to anything, so every row comes back once per node in the graph.
// The engine joined by identity and returned each row once. Re-mentioning
// the node inside a longer pattern, or mentioning it on its own first, is a
// real join in dawgs' SQL and stays served.
func TestTryCypherRementionedNodeMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"RmA"}, props: map[string]any{"name": "a1"}},
		{kinds: []string{"RmB"}, props: map[string]any{"name": "b1"}},
		{kinds: []string{"RmX"}, props: map[string]any{"name": "x"}},
	}, []plannerShapeEdge{{0, 1, "RmR"}})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:RmA), (n)-[:RmR]->(m) RETURN n, m`, true},
		{`MATCH (t:RmB), (s:RmA)-[:RmR]->(t) RETURN s, t`, true},
		{`MATCH (s:RmA) MATCH (s)-[:RmR]->(t) RETURN s, t`, true},
		{`MATCH (s:RmA)-[:RmR]->(t) MATCH (t)<-[:RmR]-(s) RETURN s, t`, true},

		{`MATCH (n:RmA), (n:RmA) RETURN n`, false},
		{`MATCH (n:RmA), (n) RETURN n`, false},
		{`MATCH (n:RmA) MATCH (n) RETURN n`, false},
		{`MATCH (n:RmA) MATCH (n {name: 'a1'}) RETURN n`, false},
		{`MATCH (s:RmA)-[:RmR]->(t) MATCH (t) RETURN count(*) AS c`, false},
		{`MATCH (s:RmA)-[:RmR]->(t) MATCH (s) RETURN s, t`, false},
		{`MATCH (s:RmA)-[:RmR]->(t), (s) RETURN s, t`, false},
		{`MATCH (n:RmA) OPTIONAL MATCH (n) RETURN n`, false},
		{`MATCH (n:RmA) OPTIONAL MATCH (n:RmA) RETURN n`, false},
		{`MATCH (s:RmA)-[:RmR]->(t) WITH s, t MATCH (t) RETURN s, t`, false},
		{`MATCH (n:RmA) WITH n MATCH (n) RETURN n`, false},
	})
}
