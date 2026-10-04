// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherPatternPredicateOutsideFirstClauseMatchesOracle compares
// WHERE-clause pattern predicates in query parts of one and of several
// reading clauses with PostgreSQL. In a query part with a single MATCH
// clause dawgs places the predicate's subquery in the final select, where
// that clause's CTE is in scope. In a part with more than one reading
// clause -- several MATCH clauses, or an OPTIONAL MATCH -- it places every
// predicate of the part, the first clause's included, inside the CTE the
// last clause is still defining, and the subquery refers to that CTE by
// name, which PostgreSQL rejects (42P01, missing FROM-clause entry) before
// reading a row -- for every direction, NOT and anonymous-endpoint
// variant. The engine evaluated the predicate and served rows.
func TestTryCypherPatternPredicateOutsideFirstClauseMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"PcA"}, props: map[string]any{"name": "a1"}},
		{kinds: []string{"PcB"}, props: map[string]any{"name": "b1"}},
		{kinds: []string{"PcB"}, props: map[string]any{"name": "b2"}},
		{kinds: []string{"PcX"}, props: map[string]any{"name": "x"}},
	}, []plannerShapeEdge{{0, 1, "PcR"}, {1, 1, "PcR"}, {1, 3, "PcR"}})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		// A query part of one MATCH clause, however many patterns it has --
		// including a part that ends in, or begins after, a WITH.
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

		// The first MATCH clause of a part with a later MATCH clause.
		{`MATCH (a:PcA) WHERE (a)-[:PcR]->() MATCH (b:PcB) RETURN a, b`, false},
		{`MATCH (a:PcA) WHERE (a)-[:PcR]->(:PcB) MATCH (b:PcB) RETURN a, b`, false},
		{`MATCH (a:PcA) WHERE NOT (a)-[:PcR]->() MATCH (b:PcB) RETURN a, b`, false},
		{`MATCH (a:PcA), (b:PcB) WHERE (a)-[:PcR]->(b) MATCH (x:PcX) RETURN a, b, x`, false},
		{`MATCH (a:PcA) WHERE (a)-[:PcR]->() MATCH (a)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (b:PcB) WHERE (b)-[:PcR]->(:PcX) MATCH (a:PcA)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (a:PcA) WITH a MATCH (b:PcB) WHERE (a)-[:PcR]->(b) MATCH (x:PcX) RETURN a, b, x`, false},
		{`MATCH (a:PcA) WHERE (a)-[:PcR]->() MATCH (b:PcB) WITH a, b RETURN a, b`, false},

		// An OPTIONAL MATCH, and the first MATCH clause of a part with one.
		{`MATCH (a:PcA) OPTIONAL MATCH (a)-[:PcR]->(b:PcB) WHERE NOT (b)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (a:PcA) OPTIONAL MATCH (a)-[:PcR]->(b:PcB) WHERE (b)-[:PcR]->(b) RETURN a, b`, false},
		{`MATCH (a:PcA) WHERE (a)-[:PcR]->() OPTIONAL MATCH (a)-[:PcR]->(b:PcB) RETURN a, b`, false},
		{`MATCH (a:PcA) WHERE (a)-[:PcR]-(:PcB) OPTIONAL MATCH (a)-[:PcR]->(b:PcB) RETURN a, b`, false},
	})
}
