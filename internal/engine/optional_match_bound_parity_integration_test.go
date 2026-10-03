// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherOptionalMatchBetweenBoundNodesMatchesOracle compares a
// one-step OPTIONAL MATCH whose two endpoints the mandatory pattern already
// bound with PostgreSQL. For a directed fixed-length step dawgs joins the
// edge to the first bound node and then re-joins the node table for the
// second -- `join node n1 on (s1.n1).id = e0.end_id` -- a condition that
// never mentions n1, so each match comes back once per node in the graph.
// The engine matched the edge once. With a fresh far end the join is real
// and stays served; a variable-length step between the two bound nodes is
// lowered differently, agrees with PostgreSQL, and stays served too.
func TestTryCypherOptionalMatchBetweenBoundNodesMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"ObA"}, props: map[string]any{"name": "a1"}},
		{kinds: []string{"ObB"}, props: map[string]any{"name": "b1"}},
		{kinds: []string{"ObB"}, props: map[string]any{"name": "b2"}},
		{kinds: []string{"ObX"}, props: map[string]any{"name": "x"}},
		{kinds: []string{"ObB"}, props: map[string]any{"name": "b3"}},
	}, []plannerShapeEdge{
		{0, 1, "ObR"}, {1, 1, "ObR"},
		// A kind with no self-loop for the variable-length step: a
		// self-loop makes the engine decline variable-length steps over
		// its kind. b2 is reached twice, b3 not at all.
		{0, 1, "ObV"}, {1, 2, "ObV"}, {0, 2, "ObV"},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (a:ObA) OPTIONAL MATCH (a)-[:ObR]->(b:ObB) RETURN a, b`, true},
		{`MATCH (b:ObB) OPTIONAL MATCH (a:ObA)-[:ObR]->(b) RETURN a, b`, true},
		{`MATCH (a:ObA), (b:ObB) OPTIONAL MATCH (a)-[:ObV*1..]->(b) RETURN a, b`, true},
		{`MATCH (a:ObA), (b:ObB) OPTIONAL MATCH (b)<-[:ObV*1..]-(a) RETURN a, b`, true},
		{`MATCH (a:ObA), (b:ObB) OPTIONAL MATCH (a)-[:ObV*2..]->(b) RETURN a, b`, true},

		{`MATCH (a:ObA), (b:ObB) OPTIONAL MATCH (a)-[:ObR]->(b) RETURN a, b`, false},
		{`MATCH (a:ObA), (b:ObB) OPTIONAL MATCH (b)<-[:ObR]-(a) RETURN a, b`, false},
		{`MATCH (a:ObA) MATCH (b:ObB) OPTIONAL MATCH (a)-[:ObR]->(b) RETURN a, b`, false},
		{`MATCH (a:ObA)-[:ObR]->(b:ObB) OPTIONAL MATCH (a)-[:ObR]->(b) RETURN a, b`, false},
		{`MATCH (b:ObB) OPTIONAL MATCH (b)-[:ObR]->(b) RETURN b`, false},
	})
}
