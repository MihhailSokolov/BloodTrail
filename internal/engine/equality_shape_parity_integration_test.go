// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherEqualityShapesMatchOracle compares the `=`/`<>` operand
// pairings dawgs lowers to SQL of their own with PostgreSQL (see
// interpret's equalityShapeServed for the SQL of each), over data chosen so
// every declined shape either raises an error there or answers differently
// from the evaluator's structural comparison:
//
//   - a number or boolean literal LEFT of a property re-parses the
//     property's text as JSON: the string '1' equals 1, 'abc' is an error;
//   - a parenthesised property is `->>` text: `(n.v) = 1` is `text =
//     integer`, and a JSON null is NULL under `(n.v) <> 'a'`;
//   - a list literal casts the property to an array (an error for any
//     scalar), and `[[1]]` inside IN is flattened into a match for 1;
//   - labels() drops out of the WHERE clause, id() casts a string literal to
//     bigint, and two literals of different types are an error;
//   - XOR over comparisons is a syntax error.
//
// The served controls pin what still matches: a property left of a literal,
// a string literal on either side, `n.v <> []` with a stored JSON null
// (NULL, not TRUE), `null <> 0` (NULL), and a parenthesised property in a
// string IN list, which compares an object's text (FALSE, not NULL).
func TestTryCypherEqualityShapesMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"EqJSON", map[string]any{"name": "j0", "v": "1"}},
		{"EqJSON", map[string]any{"name": "j1", "v": 1}},
		{"EqJSON", map[string]any{"name": "j2", "v": "true"}},
		{"EqJSON", map[string]any{"name": "j3", "v": 2}},
		{"EqDirty", map[string]any{"name": "d0", "v": "abc"}},
		{"EqDirty", map[string]any{"name": "d1", "v": 1}},
		{"EqNull", map[string]any{"name": "u0", "v": nil}},
		{"EqNull", map[string]any{"name": "u1", "v": []any{}}},
		{"EqNull", map[string]any{"name": "u2", "v": []any{1}}},
		{"EqNull", map[string]any{"name": "u3", "v": "a"}},
		{"EqNull", map[string]any{"name": "u4"}},
		{"EqObj", map[string]any{"name": "o0", "v": map[string]any{"k": "a"}}},
		{"EqObj", map[string]any{"name": "o1", "v": "a"}},
		{"EqObj", map[string]any{"name": "o2", "v": 5}},
		{"EqInt", map[string]any{"name": "i0", "v": 1}},
		{"EqInt", map[string]any{"name": "i1", "v": 2}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:EqJSON) WHERE n.v = 1 RETURN n`, true},
		{`MATCH (n:EqJSON) WHERE n.v <> 1 RETURN n`, true},
		{`MATCH (n:EqJSON) WHERE '1' = n.v RETURN n`, true},
		{`MATCH (n:EqNull) WHERE n.v <> [] RETURN n`, true},
		{`MATCH (n:EqNull) WHERE NOT n.v = [] RETURN n`, true},
		{`MATCH (n:EqNull) WHERE [] = n.v RETURN n`, true},
		{`MATCH (n:EqNull) WHERE null <> 0 RETURN n`, true},
		{`MATCH (n:EqObj) WHERE (n.v) IN ['a'] RETURN n`, true},
		{`MATCH (n:EqObj) WHERE NOT (n.v) IN ['a'] RETURN n`, true},

		{`MATCH (n:EqJSON) WHERE 1 = n.v RETURN n`, false},
		{`MATCH (n:EqJSON) WHERE NOT 1 = n.v RETURN n`, false},
		{`MATCH (n:EqJSON) WHERE true = n.v RETURN n`, false},
		{`MATCH (n:EqDirty) WHERE 1 = n.v RETURN n`, false},
		{`MATCH (n:EqInt) WHERE (n.v) = 1 RETURN n`, false},
		{`MATCH (n:EqNull) WHERE (n.v) <> 'a' RETURN n`, false},
		{`MATCH (n:EqObj) WHERE (n.v) = '5' RETURN n`, false},
		{`MATCH (n:EqNull) WHERE n.v = [1] RETURN n`, false},
		{`MATCH (n:EqInt) WHERE n.v IN [[1]] RETURN n`, false},
		{`MATCH (n:EqInt) WHERE labels(n) = 'EqInt' RETURN n`, false},
		{`MATCH (n:EqInt) WHERE labels(n) <> ['EqInt'] RETURN n`, false},
		{`MATCH (n:EqInt) WHERE id(n) = 'a' RETURN n`, false},
		{`MATCH (n:EqInt) WHERE 0 = 'a' RETURN n`, false},
		{`MATCH (n:EqInt) WHERE n.v = 1 XOR n.name = 'i1' RETURN n`, false},
		{`MATCH (n:EqInt) WHERE n.v = id(n) RETURN n`, false},
	})
}

// TestTryCypherWithAliasShapesMatchOracle compares WITH-carried constants
// with PostgreSQL. A WITH that carries nothing but constants after a MATCH
// is projected without a FROM -- one row, whatever the MATCH produced --
// where the evaluator kept one per input row; and a property compared with
// a bare alias is left uncast, `text = integer`, which PostgreSQL rejects,
// while arithmetic over the alias casts the property and still serves.
func TestTryCypherWithAliasShapesMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"WaUser", map[string]any{"name": "a0", "v": 1}},
		{"WaUser", map[string]any{"name": "a1", "v": 2}},
		{"WaUser", map[string]any{"name": "a2", "v": 3}},
		{"WaComputer", map[string]any{"name": "c0", "v": 1}},
		{"WaComputer", map[string]any{"name": "c1", "v": 5}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`WITH 1 AS x MATCH (n:WaUser) WHERE n.v > x * 1 RETURN n`, true},
		{`WITH 2 AS x MATCH (n:WaUser) WHERE n.v IN [x, 3] RETURN n`, false},
		{`MATCH (m:WaComputer) WITH m, 1 AS x MATCH (n:WaUser) WHERE n.v > x * 1 RETURN n, m`, true},

		{`MATCH (m:WaComputer) WITH 0 AS x RETURN count(x)`, false},
		{`MATCH (m:WaComputer) WITH 'a' AS s MATCH (n:WaUser) RETURN count(n)`, false},
		{`MATCH (m:WaComputer) WITH 1 AS x MATCH (n:WaUser) WHERE n.v > x * 1 RETURN n`, false},
		{`WITH 1 AS x MATCH (n:WaUser) WHERE n.v = x RETURN n`, false},
		{`WITH 1 AS x MATCH (n:WaUser) WHERE n.v < x RETURN n`, false},
		{`WITH 1 AS x MATCH (n:WaUser) WHERE x < n.v RETURN n`, false},
		{`WITH 1 AS x MATCH (n:WaUser) WHERE n.v < (x) RETURN n`, false},
		{`WITH '1' AS x MATCH (n:WaUser) WHERE n.v = x RETURN n`, false},
		{`MATCH (n:WaUser) WHERE (n.v) < 2 RETURN n`, false},
		{`MATCH (n:WaUser) WHERE 2 > (n.v) RETURN n`, false},
	})
}

// TestTryCypherStringPredicateShapesMatchOracle compares STARTS WITH, ENDS
// WITH, CONTAINS, =~ and IN over the operand shapes dawgs lowers
// differently from a plain property with PostgreSQL:
//
//   - a literal needle against anything but a plain property goes into LIKE
//     unescaped, so _ and % are wildcards and a trailing backslash is an
//     error; the evaluator compared the needle literally;
//   - the coalesce(..., ”) rewrite of a negation covers a plain property
//     only, and never a regex, so `NOT toLower(n.name) STARTS WITH 'a'` and
//     `NOT n.name =~ 'a'` are NULL over a missing name, where the evaluator
//     answered TRUE;
//   - a subject that is not text (`(n.num + 1) STARTS WITH '1'`) and an IN
//     right-hand side that is not an array (`'a' IN (n.tags)`) are errors.
//
// The coalesce anchor case also needs the per-value index to stay a
// superset under LIKE: 'admin-tier-0' matches 'admin_tier_0'.
func TestTryCypherStringPredicateShapesMatchOracle(t *testing.T) {
	nodes := []typedNode{
		{"SpUser", map[string]any{"name": "s0", "label": "abc", "num": 1, "tags": []any{"a"}}},
		{"SpUser", map[string]any{"name": "s1", "label": "a_c", "num": 12}},
		{"SpUser", map[string]any{"name": "s2", "label": "axc", "num": 2}},
		{"SpUser", map[string]any{"name": "s3", "label": "ac", "num": 3}},
		{"SpUser", map[string]any{"name": "s4", "num": 4}},
		{"SpGroup", map[string]any{"name": "t0", "system_tags": "admin_tier_0"}},
		{"SpGroup", map[string]any{"name": "t1", "system_tags": "admin-tier-0 owned"}},
		{"SpGroup", map[string]any{"name": "t2", "system_tags": "owned"}},
	}
	for i := 0; i < 30; i++ {
		nodes = append(nodes, typedNode{"SpGroup", map[string]any{"name": fmt.Sprintf("g%d", i)}})
	}
	pgDriver, eng := seedTypedGraph(t, nodes)

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:SpUser) WHERE toLower(n.label) CONTAINS 'a_c' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE NOT toLower(n.label) ENDS WITH '_c' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE toLower(n.label) STARTS WITH 'a%c' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE toLower(n.label) STARTS WITH ('a_c') RETURN n`, true},
		{`MATCH (n:SpUser) WHERE n.label CONTAINS 'a_c' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE NOT n.label STARTS WITH 'a' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE NOT toLower(n.label) STARTS WITH 'a' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE NOT (n.label) CONTAINS 'b' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE NOT n.label =~ 'a.*' RETURN n`, true},
		{`MATCH (n:SpUser) WHERE 'a' IN split(n.label, '_') RETURN n`, true},
		{`MATCH (t:SpGroup) WHERE COALESCE(t.system_tags, '') CONTAINS 'admin_tier_0' RETURN t`, true},

		{`MATCH (n:SpUser) WHERE toLower(n.label) ENDS WITH '\\' RETURN n`, false},
		{`MATCH (n:SpUser) WHERE n.num + 1 =~ '1.*' RETURN n`, false},
		{`MATCH (n:SpUser) WHERE (n.num + 1) STARTS WITH '1' RETURN n`, false},
		{`MATCH (n:SpUser) WHERE 'a' IN (n.tags) RETURN n`, false},
		{`MATCH (n:SpUser) WHERE 1 IN split(n.label, '_') RETURN n`, false},
		{`MATCH (n:SpUser) WHERE n.label + 'x' IN [1] RETURN n`, false},
	})
}

// TestTryCypherListAndTextEqualityMatchOracle compares a property against a
// typed list literal -- `jsonb_to_text_array(p -> 'v')::text[] = array
// [...]`, element text against element text -- and against a text
// concatenation -- `(p ->> 'v') = 'x' || ...`, plain text -- with
// PostgreSQL, and the typed-operand pairs it rejects.
func TestTryCypherListAndTextEqualityMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"LeList", map[string]any{"name": "l0", "v": []any{"1"}}},
		{"LeList", map[string]any{"name": "l1", "v": []any{1}}},
		{"LeList", map[string]any{"name": "l2", "v": []any{1.5}}},
		{"LeList", map[string]any{"name": "l3", "v": []any{}}},
		{"LeList", map[string]any{"name": "l4", "v": nil}},
		{"LeList", map[string]any{"name": "l5"}},
		{"LeList", map[string]any{"name": "l6", "v": []any{"1", "2"}}},
		{"LeScalar", map[string]any{"name": "s0", "v": "1"}},
		{"LeText", map[string]any{"name": "t0", "v": 5, "w": "5"}},
		{"LeText", map[string]any{"name": "t1", "v": "x5", "w": "5"}},
		{"LeText", map[string]any{"name": "t2", "v": "5", "w": nil}},
		{"LeText", map[string]any{"name": "t3", "w": "5"}},
		{"LeBool", map[string]any{"name": "b0", "v": true}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:LeList) WHERE n.v = ['1'] RETURN n`, true},
		{`MATCH (n:LeList) WHERE NOT n.v = ['1'] RETURN n`, true},
		{`MATCH (n:LeList) WHERE n.v <> ['1', '2'] RETURN n`, true},
		{`MATCH (n:LeList) WHERE ['1'] = n.v RETURN n`, true},
		{`MATCH (n:LeText) WHERE n.v = '' + n.w RETURN n`, true},
		{`MATCH (n:LeText) WHERE n.v = 'x' + n.w RETURN n`, true},
		{`MATCH (n:LeText) WHERE NOT n.v = 'x' + n.w RETURN n`, true},

		{`MATCH (n:LeList) WHERE n.v = [1] RETURN n`, false},
		{`MATCH (n:LeScalar) WHERE n.v = ['1'] RETURN n`, false},
		{`MATCH (n:LeText) WHERE n.v + 'x' = 1 RETURN n`, false},
		{`MATCH (n:LeBool) WHERE 1 <> coalesce(n.v, true) RETURN n`, false},
		{`MATCH (n:LeBool) WHERE 'true' <> coalesce(n.v, true) RETURN n`, false},
		{`WITH 1 AS t MATCH (n:LeText) WHERE t <> 'a' RETURN n`, false},
		{`WITH 'a' AS t MATCH (n:LeText) WHERE t = 1 RETURN n`, false},
		{`WITH null AS t MATCH (n:LeText) WHERE t <> 'a' RETURN n`, false},
	})
}

// TestTryCypherPatternShapesMatchOracle compares pattern shapes whose SQL
// matches differently from Cypher's reading with PostgreSQL: relationship
// uniqueness holds within one pattern only, so two comma-separated
// patterns or two MATCH clauses may reuse an edge; an undirected step past
// a pattern's first returns the near node as a far one too, and
// `(a)--(a)` every edge touching a; and allShortestPaths over several
// pairs, unless both endpoints carry a property or id condition (dawgs'
// pair filter), keeps only the paths of the overall shortest length any
// root reaches any terminal at -- the engine's traverse.ModeAll, chosen
// from the translation's harness calls. The declined shapes may not serve.
func TestTryCypherPatternShapesMatchOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()
	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	var (
		user, group, comp = graph.StringKind("PuUser"), graph.StringKind("PuGroup"), graph.StringKind("PuComp")
		member, admin     = graph.StringKind("PuMember"), graph.StringKind("PuAdmin")
	)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		nodes := map[string]*graph.Node{}
		for _, spec := range []struct {
			name string
			kind graph.Kind
		}{
			{"u0", user}, {"u1", user}, {"u2", user}, {"u3", user},
			{"g0", group}, {"g1", group}, {"g2", group},
			{"c0", comp}, {"c1", comp}, {"c2", comp},
		} {
			n, err := tx.CreateNode(graph.NewProperties().Set("name", spec.name), spec.kind)
			if err != nil {
				return err
			}
			nodes[spec.name] = n
		}
		for _, e := range []struct {
			from, to string
			kind     graph.Kind
		}{
			{"u0", "g0", member}, {"u1", "g0", member}, {"u2", "g1", member}, {"u3", "g1", member},
			{"g0", "g2", member}, {"g1", "g2", member}, {"g1", "g1", member},
			{"g2", "c0", admin}, {"g0", "c1", admin}, {"u3", "c2", admin},
		} {
			if _, err := tx.CreateRelationshipByIDs(nodes[e.from].ID, nodes[e.to].ID, e.kind, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (x:PuUser)-[:PuMember]->(g), (y:PuUser)-[:PuMember]->(g) RETURN x.name, y.name`, true},
		{`MATCH (x:PuUser)-[:PuMember]->(g) MATCH (y:PuUser)-[:PuMember]->(g) RETURN x.name, y.name`, true},
		{`MATCH (x:PuUser)-[:PuMember]->(g)<-[:PuMember]-(y:PuUser) RETURN x.name, y.name`, true},
		{`MATCH (a:PuGroup)-[:PuMember]-(b) RETURN a.name, b.name`, true},
		{`MATCH p = allShortestPaths((s:PuUser)-[:PuMember|PuAdmin*1..]->(t:PuComp)) WHERE s.name = 'u0' AND t.name = 'c0' RETURN s.name, t.name`, true},
		{`MATCH p = allShortestPaths((s:PuUser)-[:PuMember|PuAdmin*1..]->(t:PuComp)) RETURN s.name, t.name`, true},
		{`MATCH p = allShortestPaths((s:PuUser)-[:PuMember|PuAdmin*1..]->(t:PuComp)) WHERE s.name = 'u0' RETURN s.name, t.name`, true},

		{`MATCH (a:PuUser)-[:PuMember]->(b)-[:PuMember]-(c) RETURN a.name, c.name`, false},
		{`MATCH (a:PuGroup)-[:PuMember]-(a) RETURN a.name`, false},
	})
}
