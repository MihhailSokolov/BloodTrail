// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherOptionalMatchShapesMatchOracle compares OPTIONAL MATCH
// patterns of more than one step and of more than one part with
// PostgreSQL. dawgs builds the optional side hop by hop and left-joins only
// the LAST hop: every earlier hop is an inner join, so a row whose first
// hop fails is dropped, and a row whose first hop succeeds keeps it with
// only the last hop null-padded. The engine null-padded the whole pattern
// instead. One step, fixed or variable length, is the left join the engine
// computes and stays served. (A node-only `OPTIONAL MATCH (u)` is a
// re-mentioned node: TestTryCypherRementionedNodeMatchesOracle.)
func TestTryCypherOptionalMatchShapesMatchOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"OmU"}, props: map[string]any{"name": "u0"}},
		{kinds: []string{"OmU"}, props: map[string]any{"name": "u1"}},
		{kinds: []string{"OmU"}, props: map[string]any{"name": "u2"}},
		{kinds: []string{"OmG"}, props: map[string]any{"name": "g0"}},
		{kinds: []string{"OmG"}, props: map[string]any{"name": "g1"}},
	}, []plannerShapeEdge{
		{0, 3, "OmM"}, {0, 4, "OmM"}, {1, 4, "OmM"}, {3, 3, "OmM"},
		// A kind with no self-loop for the variable-length control: a
		// self-loop makes the engine decline variable-length steps over
		// its kind.
		{0, 4, "OmV"}, {1, 3, "OmV"}, {3, 4, "OmV"},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (u:OmU) OPTIONAL MATCH (u)-[:OmM]->(g:OmG) RETURN u, g`, true},
		{`MATCH (u:OmU) OPTIONAL MATCH (g:OmG)<-[:OmM]-(u) RETURN u, g`, true},
		{`MATCH (u:OmU) OPTIONAL MATCH (u)-[:OmM]->(g:OmG) WHERE g.name = 'g1' RETURN u, g`, true},
		{`MATCH (u:OmU)-[:OmM]->(g:OmG) OPTIONAL MATCH (g)-[:OmM]->(h:OmG) RETURN u, g, h`, true},
		{`MATCH (u:OmU) OPTIONAL MATCH (u)-[:OmV*1..]->(g:OmG) WHERE g.name = 'g1' RETURN u, g`, true},
		{`MATCH (u:OmU) OPTIONAL MATCH (u)-[:OmV*2..]->(g:OmG) RETURN u, g`, true},

		{`MATCH (u:OmU) OPTIONAL MATCH (u)-[:OmM]->(g:OmG)-[:OmM]->(h) RETURN u, g, h`, false},
		{`MATCH (u:OmU) OPTIONAL MATCH (u)-[:OmM]->(g:OmG), (u)-[:OmM]->(h:OmG) RETURN u, g, h`, false},
		{`MATCH (u:OmU) OPTIONAL MATCH (u)-[:OmM]->(g:OmG), (h:OmG) RETURN u, g, h`, false},
	})
}
