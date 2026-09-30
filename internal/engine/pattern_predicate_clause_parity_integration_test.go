// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherPatternPredicateOutsideFirstClauseMatchesOracle compares
// WHERE-clause pattern predicates by the MATCH clause that writes them with
// PostgreSQL. In the first MATCH clause of a query part dawgs places the
// predicate's subquery in the final select, where the clause's CTE is in
// scope. In any later MATCH clause, and in an OPTIONAL MATCH, it places the
// subquery inside the very CTE that clause is still defining, and
// PostgreSQL rejects the self-reference (42P01, missing FROM-clause entry)
// before reading a row -- for every direction, NOT and anonymous-endpoint
// variant. The engine evaluated the predicate and served rows.
func TestTryCypherPatternPredicateOutsideFirstClauseMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"PcA"}, props: map[string]any{"name": "a1"}},
		{kinds: []string{"PcB"}, props: map[string]any{"name": "b1"}},
		{kinds: []string{"PcB"}, props: map[string]any{"name": "b2"}},
		{kinds: []string{"PcX"}, props: map[string]any{"name": "x"}},
	}, []plannerShapeEdge{{0, 1, "PcR"}, {1, 1, "PcR"}})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		// The first MATCH clause of a query part, however many patterns it
		// has -- including the first clause after a WITH.
		{`MATCH (a:PcA), (b:PcB) WHERE (a)-[:PcR]->(b) RETURN a, b`, true},
		{`MATCH (a:PcA) WHERE (a)-[:PcR]->() WITH a MATCH (b:PcB) RETURN a, b`, true},
		{`MATCH (a:PcA) WITH a MATCH (b:PcB) WHERE (a)-[:PcR]->(b) RETURN a, b`, true},
		{`MATCH (a:PcA) WITH a MATCH (b:PcB) WHERE NOT (a)-[:PcR]->(b) RETURN a, b`, true},

		// A later MATCH clause.
		{`MATCH (a:PcA) MATCH (b:PcB) WHERE (a)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (a:PcA) MATCH (b:PcB) WHERE NOT (a)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (a:PcA) MATCH (b:PcB) WHERE (b)<-[:PcR]-(a) RETURN a, b`, false},
		{`MATCH (a:PcA) MATCH (b:PcB) WHERE (b)<-[:PcR]-() RETURN a, b`, false},
		{`MATCH (a:PcA) MATCH (b:PcB) WHERE (a)-[:PcR]->(:PcB) RETURN a, b`, false},
		{`MATCH (b:PcB) MATCH (a:PcA) MATCH (x:PcX) WHERE (a)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (a:PcA) WITH a MATCH (b:PcB) MATCH (c:PcX) WHERE (a)-[:PcR]->(b) RETURN a, b, c`, false},

		// An OPTIONAL MATCH.
		{`MATCH (a:PcA) OPTIONAL MATCH (a)-[:PcR]->(b:PcB) WHERE NOT (b)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (a:PcA) OPTIONAL MATCH (a)-[:PcR]->(b:PcB) WHERE (b)-[:PcR]->(b) RETURN a, b`, false},
	})
}
