// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// typedNode is one fixture node for the typed-comparison differentials: a
// single kind and a property bag whose values span JSON types.
type typedNode struct {
	kind  string
	props map[string]any
}

// seedTypedGraph wipes the graph, writes nodes, and returns an engine
// serving a snapshot of them.
func seedTypedGraph(t *testing.T, nodes []typedNode) (*pg.Driver, *Engine) {
	t.Helper()
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, n := range nodes {
			props := graph.NewProperties()
			for k, v := range n.props {
				props.Set(k, v)
			}
			if _, err := tx.CreateNode(props, graph.StringKind(n.kind)); err != nil {
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
	return pgDriver, eng
}

// renderTypedRows reads every row into a sorted list: a node as its name, a
// scalar as its Go type and value (the type is part of what is compared).
func renderTypedRows(result graph.Result) ([]string, error) {
	defer result.Close()
	var rows []string
	for result.Next() {
		row := ""
		for _, v := range result.Values() {
			var node graph.Node
			switch {
			case v == nil:
				row += "null|"
			case isScalarValue(v):
				row += fmt.Sprintf("%T:%v|", v, v)
			case result.Mapper().Map(v, &node) && node.Properties != nil && node.Properties.Exists("name"):
				name, _ := node.Properties.Get("name").String()
				row += "node:" + name + "|"
			default:
				row += fmt.Sprintf("%T:%v|", v, v)
			}
		}
		rows = append(rows, row)
	}
	if err := result.Error(); err != nil {
		return nil, err
	}
	sort.Strings(rows)
	return rows, nil
}

// typedCase is one differential query. mustServe marks a shape the engine
// is meant to answer, so a decline fails the test instead of silently
// dropping the comparison; every other case may decline, but if it is
// served it must be served with exactly PostgreSQL's answer -- and never
// where PostgreSQL raises an error.
type typedCase struct {
	query     string
	mustServe bool
}

func assertTypedCasesMatchOracle(t *testing.T, pgDriver *pg.Driver, eng *Engine, cases []typedCase) {
	t.Helper()
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			var (
				served     bool
				engineRows []string
				engineErr  error
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				var result graph.Result
				if result, served = eng.TryCypher(ctx, tx, tc.query, nil); served {
					engineRows, engineErr = renderTypedRows(result)
				}
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (engine): %v", err)
			}
			if !served {
				if tc.mustServe {
					t.Fatalf("TryCypher declined a shape this test exists to compare")
				}
				return // PostgreSQL answers, and is right by definition
			}
			if engineErr != nil {
				t.Fatalf("served result failed: %v", engineErr)
			}

			var (
				oracleRows []string
				oracleErr  error
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				oracleRows, oracleErr = renderTypedRows(tx.Query(tc.query, map[string]any{}))
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (oracle): %v", err)
			}
			if oracleErr != nil {
				t.Fatalf("engine served %v; PostgreSQL raises an error: %v", engineRows, oracleErr)
			}
			if !reflect.DeepEqual(engineRows, oracleRows) {
				t.Fatalf("engine served %v, PostgreSQL returns %v", engineRows, oracleRows)
			}
		})
	}
}

// TestTryCypherUntypedCoalesceMatchesOracle compares comparisons over a
// coalesce() with only property arguments -- which dawgs types by its
// context -- and the neighbouring shapes whose SQL typing differs from the
// evaluator's value model, with PostgreSQL:
//
//   - `coalesce(n.a, n.b) = 1` is `coalesce(...)::int8 = 1` (likewise
//     float8 and bool); the engine compared the text and served nothing;
//   - `coalesce(n.a, n.b) IN [1, 2]` is the constant `false`, and the
//     engine matched rows;
//   - a text value against a number is PostgreSQL's `operator does not
//     exist` error, and the engine answered FALSE;
//   - IS [NOT] NULL over anything but a plain property is dropped from the
//     SQL, and the engine filtered.
func TestTryCypherUntypedCoalesceMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"TypNum", map[string]any{"name": "n0", "a": "1"}},
		{"TypNum", map[string]any{"name": "n1", "a": 1}},
		{"TypNum", map[string]any{"name": "n2", "b": "1"}},
		{"TypNum", map[string]any{"name": "n3", "a": nil, "b": 1}},
		{"TypNum", map[string]any{"name": "n4", "a": 2}},
		{"TypNum", map[string]any{"name": "n5"}},
		{"TypFloat", map[string]any{"name": "f0", "a": "1.5"}},
		{"TypFloat", map[string]any{"name": "f1", "a": 1.5}},
		{"TypFloat", map[string]any{"name": "f2", "b": "2"}},
		{"TypBool", map[string]any{"name": "b0", "a": "true"}},
		{"TypBool", map[string]any{"name": "b1", "a": true}},
		{"TypBool", map[string]any{"name": "b2", "b": false}},
		{"TypDirty", map[string]any{"name": "d0", "a": "abc"}},
		{"TypStr", map[string]any{"name": "s0", "a": "x"}},
		{"TypStr", map[string]any{"name": "s1", "a": "1"}},
		{"TypStr", map[string]any{"name": "s2"}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) = 1 RETURN n`, true},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) <> 1 RETURN n`, true},
		{`MATCH (n:TypNum) WHERE 1 = coalesce(n.a, n.b) RETURN n`, true},
		{`MATCH (n:TypNum) WHERE NOT coalesce(n.a, n.b) = 1 RETURN n`, true},
		{`MATCH (n:TypNum) WHERE coalesce(n.a) = 1 RETURN n`, true},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) = (1) RETURN n`, true},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) = '1' RETURN n`, true},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) = 1 AND n.name = 'n0' RETURN n`, true},
		{`MATCH (n:TypFloat) WHERE coalesce(n.a, n.b) = 1.5 RETURN n`, true},
		{`MATCH (n:TypFloat) WHERE coalesce(n.a, n.b) = 2.0 RETURN n`, true},
		{`MATCH (n:TypBool) WHERE coalesce(n.a, n.b) = true RETURN n`, true},
		{`MATCH (n:TypBool) WHERE coalesce(n.a, n.b) = false RETURN n`, true},
		{`MATCH (n:TypBool) WHERE coalesce(n.a, n.b) <> true RETURN n`, true},
		{`MATCH (n:TypStr) WHERE (coalesce(n.a, n.b)) = '1' RETURN n`, true},
		{`MATCH (n:TypStr) WHERE coalesce(n.a, n.b) = toLower(n.a) RETURN n`, true},

		{`MATCH (n:TypDirty) WHERE coalesce(n.a, n.b) = 1 RETURN n`, false},
		{`MATCH (n:TypDirty) WHERE coalesce(n.a, n.b) = 1.5 RETURN n`, false},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) = true RETURN n`, false},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) = id(n) RETURN n`, false},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) = coalesce(n.x, 1) RETURN n`, false},
		{`MATCH (n:TypStr) WHERE (coalesce(n.a, n.b)) = 1 RETURN n`, false},
		{`MATCH (n:TypStr) WHERE toLower(n.a) = 1 RETURN n`, false},
		{`MATCH (n:TypStr) WHERE toUpper(n.a) = true RETURN n`, false},
		{`MATCH (n:TypStr) WHERE toLower(n.a) <> 1 RETURN n`, false},
		{`MATCH (n:TypStr) WHERE toLower(n.name) = id(n) RETURN n`, false},

		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) IN [1, 2] RETURN n`, false},
		{`MATCH (n:TypNum) WHERE NOT coalesce(n.a, n.b) IN [1, 2] RETURN n`, false},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) IN ['1', '2'] RETURN n`, false},

		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) IS NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, n.b) IS NOT NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE coalesce(n.a, '') IS NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE id(n) IS NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE (n.a) IS NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE n IS NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE n.name = 'n0' AND id(n) IS NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE n.name = 'n0' OR id(n) IS NOT NULL RETURN n`, false},
		{`MATCH (n:TypNum) WHERE n.a IS NULL RETURN n`, true},
		{`MATCH (n:TypNum) WHERE n.a IS NOT NULL RETURN n`, true},
	})
}

// TestTryCypherPropertyCastsMatchOracle compares the shapes where dawgs
// casts a property's `->>` text to the other operand's SQL type before
// comparing or computing -- `n.p IN [1, 3]` is `(p ->> 'p')::int8 =
// any(...)`, `n.v + 1` is `(p ->> 'v')::int8 + 1` -- with PostgreSQL, and the
// shapes it cannot type at all. The engine answered all of them on its own
// value model: ParseFloat matched '1.0' where the cast raises an error, an
// object IN a string list was NULL where pg's text compare is FALSE, 5.5 + 1
// computed where the int8 cast raises an error, and `n.a * n.b`, `-n.v` and
// `id(n) IN ['1']` were answered where PostgreSQL has no such operator.
func TestTryCypherPropertyCastsMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"CastIn", map[string]any{"name": "i0", "p": 1}},
		{"CastIn", map[string]any{"name": "i1", "p": "1"}},
		{"CastIn", map[string]any{"name": "i2", "p": 2}},
		{"CastIn", map[string]any{"name": "i3"}},
		{"CastInFloat", map[string]any{"name": "f0", "p": 1.5}},
		{"CastInFloat", map[string]any{"name": "f1", "p": "1.5"}},
		{"CastInFloat", map[string]any{"name": "f2", "p": 2}},
		{"CastObj", map[string]any{"name": "o0", "p": map[string]any{"k": "a"}}},
		{"CastObj", map[string]any{"name": "o1", "p": "a"}},
		{"CastObj", map[string]any{"name": "o2", "p": "c"}},
		{"CastObj", map[string]any{"name": "o3", "p": []any{1}}},
		{"CastObj", map[string]any{"name": "o4", "p": []any{"a"}}},
		{"CastArith", map[string]any{"name": "v0", "v": 5, "a": 2, "b": 3}},
		{"CastArith", map[string]any{"name": "v1", "v": "5", "a": 4, "b": 1}},
		{"CastArith", map[string]any{"name": "v2", "v": 6, "a": 1, "b": 1}},
		{"CastArith", map[string]any{"name": "v3"}},
		{"CastDirtyA", map[string]any{"name": "d0", "p": "1.0", "v": 5.5}},
		{"CastDirtyB", map[string]any{"name": "d1", "p": "1e0", "v": "5.0"}},
		{"CastDirtyC", map[string]any{"name": "d2", "p": 1.5}},
		{"CastDirtyD", map[string]any{"name": "d3", "p": []any{1}}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:CastIn) WHERE n.p IN [1, 3] RETURN n`, true},
		{`MATCH (n:CastIn) WHERE NOT n.p IN [1, 3] RETURN n`, true},
		{`MATCH (n:CastInFloat) WHERE n.p IN [1.5, 2.0] RETURN n`, true},
		{`MATCH (n:CastObj) WHERE n.p IN ['a', 'b'] RETURN n`, true},
		{`MATCH (n:CastObj) WHERE NOT n.p IN ['a', 'b'] RETURN n`, true},
		{`MATCH (n:CastArith) WHERE n.v + 1 = 6 RETURN n`, true},
		{`MATCH (n:CastArith) WHERE 1 + n.v = 6 RETURN n`, true},
		{`MATCH (n:CastArith) WHERE n.v * 2 + 1 = 11 RETURN n`, true},
		{`MATCH (n:CastArith) WHERE n.v - 1 = 4 RETURN n`, true},
		{`MATCH (n:CastArith) WHERE n.v + 1.5 = 6.5 RETURN n`, true},
		{`MATCH (n:CastArith) WHERE 2 * n.v + n.a = 12 RETURN n`, true},
		{`MATCH (n:CastArith) WHERE 1 * n.a * n.b = 6 RETURN n`, true},
		{`MATCH (n:CastArith) WHERE n.v + 1 IN [6, 7] RETURN n`, true},
		{`MATCH (n:CastIn) WHERE (n.p) IN ['1'] RETURN n`, true},
		{`MATCH (n:CastIn) WHERE id(n) IN [1] RETURN n`, true},

		{`MATCH (n:CastDirtyA) WHERE n.p IN [1, 3] RETURN n`, false},
		{`MATCH (n:CastDirtyB) WHERE n.p IN [1, 3] RETURN n`, false},
		{`MATCH (n:CastDirtyC) WHERE n.p IN [1, 3] RETURN n`, false},
		{`MATCH (n:CastDirtyD) WHERE n.p IN [1, 3] RETURN n`, false},
		{`MATCH (n:CastObj) WHERE n.p IN ['{"k": "a"}'] RETURN n`, false},
		{`MATCH (n:CastObj) WHERE NOT n.p IN ['[1]'] RETURN n`, false},
		{`MATCH (n:CastDirtyA) WHERE n.v + 1 = 6 RETURN n`, false},
		{`MATCH (n:CastDirtyB) WHERE n.v + 1 = 6 RETURN n`, false},
		{`MATCH (n:CastArith) WHERE n.a * n.b = 6 RETURN n`, false},
		{`MATCH (n:CastArith) WHERE n.a - n.b = -1 RETURN n`, false},
		{`MATCH (n:CastArith) WHERE (n.v) + 1 = 6 RETURN n`, false},
		{`MATCH (n:CastArith) WHERE -n.v = -5 RETURN n`, false},
		{`MATCH (n:CastIn) WHERE id(n) IN ['1'] RETURN n`, false},
		{`MATCH (n:CastIn) WHERE (n.p) IN [1] RETURN n`, false},
	})
}

// TestTryCypherNumericColumnsMatchOracle compares projected number columns
// with PostgreSQL TYPE for type. datetime().epochseconds/.epochmillis are
// numeric there (`extract(epoch from now())::numeric`, microseconds
// included), and so is arithmetic over float literals alone; the engine
// served the first as a truncated int64 -- or, as a group key next to
// count(), a float64 -- and the second as a float64. float8 arithmetic over a
// property and the integer calls are the served controls.
func TestTryCypherNumericColumnsMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"NumCol", map[string]any{"name": "c0", "f": 1.5, "l": []any{"a"}}},
		{"NumCol", map[string]any{"name": "c1", "f": 2.5, "l": []any{"a", "b"}}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:NumCol) RETURN n.f * 2.0 AS x`, true},
		{`MATCH (n:NumCol) RETURN 2.0 * n.f + 0.5 AS x`, true},
		{`MATCH (n:NumCol) RETURN size(n.l) AS s`, true},
		{`MATCH (n:NumCol) RETURN n.name, count(n)`, true},

		{`MATCH (n:NumCol) RETURN datetime().epochseconds AS e`, false},
		{`MATCH (n:NumCol) RETURN datetime().epochmillis AS e`, false},
		{`MATCH (n:NumCol) RETURN datetime().epochseconds, count(n)`, false},
		{`MATCH (n:NumCol) RETURN 1.5 * 2.0 AS x`, false},
		{`MATCH (n:NumCol) RETURN 2.5 + 1.5 AS x`, false},
	})
}
