// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import "testing"

// TestTryCypherEntityComparedWithScalarMatchesOracle compares a node or edge
// variable against a scalar, and against a list of scalars, with
// PostgreSQL. dawgs compares the variable's composite value as it is --
// `n0 <> 5`, `n0 = any(array[1, 2]::int8[])` -- which PostgreSQL rejects
// before reading a row: `operator does not exist: node <> integer`
// (42883), or `input of anonymous composite types is not implemented`
// (0A000) for a string, `cannot cast type node to integer` (42846) against
// size(). The planner typed the variable as unknown, which it treated as
// comparable with anything, and the engine served every node for `n <>
// 'a'`. A null, another node and an empty list stay served.
func TestTryCypherEntityComparedWithScalarMatchesOracle(t *testing.T) {
	pgDriver, _, eng, _ := seedPlannerShapeGraph(t, []plannerShapeNode{
		{kinds: []string{"EcNode"}, props: map[string]any{"name": "a", "tags": []any{"t1", "t2"}}},
		{kinds: []string{"EcNode"}, props: map[string]any{"name": "b"}},
	}, []plannerShapeEdge{{0, 1, "EcRel"}})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:EcNode) WHERE n = null RETURN n`, true},
		{`MATCH (n:EcNode) WHERE n <> null RETURN n`, true},
		{`MATCH (n:EcNode) WHERE NOT n IN [] RETURN n`, true},
		{`MATCH (a)-[:EcRel]->(b) WHERE a <> b RETURN a`, true},

		{`MATCH (n:EcNode) WHERE n = 5 RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n <> 5 RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n = 'x' RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n <> 'a' RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n = true RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n <> false RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n = 1.5 RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n = -1 RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n <> id(n) RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n <> size(n.tags) RETURN n`, false},
		{`MATCH (n:EcNode) WHERE (n) <> 5 RETURN n`, false},
		{`MATCH (n:EcNode) WHERE n IN [1, 2] RETURN n`, false},
		{`MATCH (n:EcNode) WHERE NOT n IN [1, 2] RETURN n`, false},
		{`MATCH (n:EcNode) WHERE NOT n IN ['a'] RETURN n`, false},
		{`MATCH (a)-[r:EcRel]->(b) WHERE r <> 5 RETURN a`, false},
		{`MATCH (a)-[r:EcRel]->(b) WHERE NOT r IN [1] RETURN a`, false},
	})
}
