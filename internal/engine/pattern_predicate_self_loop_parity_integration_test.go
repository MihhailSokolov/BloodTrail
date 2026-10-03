// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherUndirectedPatternPredicateSelfLoopMatchesOracle compares
// pattern predicates whose only witnessing edge is a self-loop with
// PostgreSQL. For an undirected predicate with an anonymous LABELLED end,
// `(a)-[:R]-(:Z)`, dawgs joins that end to either endpoint of the edge and
// requires it to differ from the bound node -- `(s0.n0).id <> n1.id` -- so a
// self-loop does not count, and `loop` is not a match. The evaluator walked
// the bound node's adjacency and counted the loop. Every other form -- a
// directed arrow, an unlabelled `()` end (a plain EXISTS over the edges),
// two bound endpoints -- counts the self-loop in dawgs' SQL as well. All of
// them are served.
func TestTryCypherUndirectedPatternPredicateSelfLoopMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"SlNode"}, props: map[string]any{"name": "loop"}},
		{kinds: []string{"SlNode"}, props: map[string]any{"name": "src"}},
		{kinds: []string{"SlNode"}, props: map[string]any{"name": "dst"}},
		{kinds: []string{"SlNode"}, props: map[string]any{"name": "alone"}},
		{kinds: []string{"SlOther"}, props: map[string]any{"name": "other"}},
	}, []plannerShapeEdge{{0, 0, "SlRel"}, {1, 2, "SlRel"}, {1, 4, "SlAux"}, {4, 4, "SlAux"}})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (a:SlNode) WHERE (a)-[:SlRel]->(:SlNode) RETURN a`, true},
		{`MATCH (a:SlNode) WHERE NOT (a)-[:SlRel]->(:SlNode) RETURN a`, true},
		{`MATCH (a:SlNode) WHERE (a)<-[:SlRel]-(:SlNode) RETURN a`, true},
		{`MATCH (a:SlNode) WHERE (a)-[:SlRel]-() RETURN a`, true},
		{`MATCH (a:SlNode) WHERE NOT (a)-[:SlRel]-() RETURN a`, true},
		{`MATCH (a:SlNode) WHERE (a)-[:SlRel]-(a) RETURN a`, true},
		{`MATCH (a:SlNode), (b:SlNode) WHERE (a)-[:SlRel]-(b) RETURN a, b`, true},

		{`MATCH (a:SlNode) WHERE (a)-[:SlRel]-(:SlNode) RETURN a`, true},
		{`MATCH (a:SlNode) WHERE NOT (a)-[:SlRel]-(:SlNode) RETURN a`, true},
		{`MATCH (a:SlNode) WHERE (:SlNode)-[:SlRel]-(a) RETURN a`, true},
		{`MATCH (a:SlNode) WHERE (a)-[]-(:SlNode) RETURN a`, true},
		{`MATCH (a) WHERE (a)-[:SlAux]-(:SlOther) RETURN a`, true},
	})
}
