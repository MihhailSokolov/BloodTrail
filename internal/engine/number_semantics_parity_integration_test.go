// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherFloat8UnderflowMatchesOracle compares the shapes that cast a
// property's text to float8 with PostgreSQL over strings float8in rejects as
// out of range: a mantissa that is not zero but rounds to zero ('1e-400',
// '2e-324'). strconv.ParseFloat returns 0 without an error for them, so the
// engine compared a zero and served rows where PostgreSQL raises an error.
// A denormal ('1e-310') and a zero mantissa ('0e-400') are accepted by both
// and stay served.
func TestTryCypherFloat8UnderflowMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"UflTiny", map[string]any{"name": "u1", "s": "1e-400"}},
		{"UflHalf", map[string]any{"name": "u2", "s": "2e-324"}},
		{"UflNeg", map[string]any{"name": "u3", "s": "-1e-400"}},
		{"UflOk", map[string]any{"name": "o1", "s": "1e-310"}},
		{"UflOk", map[string]any{"name": "o2", "s": "0e-400"}},
		{"UflOk", map[string]any{"name": "o3", "s": "2.5e-324"}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:UflTiny) WHERE n.s < 1.5 RETURN n`, false},
		{`MATCH (n:UflTiny) WHERE n.s IN [0.0, 1.5] RETURN n`, false},
		{`MATCH (n:UflTiny) WHERE coalesce(n.s, 0.5) = 0.0 RETURN n`, false},
		{`MATCH (n:UflHalf) WHERE n.s < 1.5 RETURN n`, false},
		{`MATCH (n:UflNeg) WHERE n.s > -1.5 RETURN n`, false},
		{`MATCH (n:UflNeg) WHERE NOT n.s IN [-1.0, 0.0] RETURN n`, false},

		{`MATCH (n:UflOk) WHERE n.s < 1.5 RETURN n`, true},
		{`MATCH (n:UflOk) WHERE n.s IN [0.0, 1.5] RETURN n`, true},
		{`MATCH (n:UflOk) WHERE coalesce(n.s, 0.5) = 0.0 RETURN n`, true},
	})
}

// TestTryCypherIntegerArithmeticMatchesOracle compares integer arithmetic
// and integer literals with PostgreSQL. dawgs prints an integer literal as
// it is written, and PostgreSQL types it int4 when it fits (int8 otherwise):
// `365 * 86400 * 1000` is an int4 product that overflows, `2147483647 + 1`
// too, and `size(n.l) * 1000000000` (size() is `::int`) likewise -- each an
// "integer out of range" error, where the evaluator's float64 arithmetic
// answered. And float64 holds integers exactly only up to 2^53: a literal
// past it rounded onto its neighbour (`n.v = 9007199254740993` matched a
// stored 9007199254740992), and so did a sum (`n.v + 1`).
func TestTryCypherIntegerArithmeticMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"IntOvf", map[string]any{"name": "o1", "i": 1, "l": []any{"a", "b", "c"}}},
		{"IntBig", map[string]any{"name": "b1", "v": float64(9007199254740992)}},
		{"IntBig", map[string]any{"name": "b2", "v": float64(9007199254740990)}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:IntOvf) WHERE n.i < datetime().epochmillis - 365 * 86400 * 1000 RETURN n`, false},
		{`MATCH (n:IntOvf) WHERE 2147483647 + 1 > 0 RETURN n`, false},
		{`MATCH (n:IntOvf) WHERE 2147483647 * 2 > 0 RETURN n`, false},
		{`MATCH (n:IntOvf) WHERE size(n.l) * 1000000000 > 0 RETURN n`, false},
		{`MATCH (n:IntOvf) WHERE n.i < 365 * 86400 * 1000 RETURN n`, false},
		{`MATCH (n:IntOvf) WHERE n.i + 9223372036854775807 = 5 RETURN n`, false},
		{`MATCH (n:IntOvf) WHERE n.i * 9223372036854775807 = 5 RETURN n`, false},
		{`MATCH (n:IntOvf) WHERE id(n) * 2147483647 * 2147483647 > 0 RETURN n`, false},
		{`WITH 30000 AS d MATCH (n:IntOvf) WHERE n.i < (datetime().epochseconds - (d * 86400)) RETURN n`, false},
		{`MATCH (n:IntBig) WHERE n.v = 9007199254740993 RETURN n`, false},
		{`MATCH (n:IntBig) WHERE n.v < 9007199254740993 RETURN n`, false},
		{`MATCH (n:IntBig) WHERE n.v IN [9007199254740993] RETURN n`, false},
		{`MATCH (n:IntBig) WHERE coalesce(n.v, 0) = 9007199254740993 RETURN n`, false},
		{`MATCH (n:IntBig) WHERE n.v + 1 = 9007199254740992 RETURN n`, false},
		{`MATCH (n:IntBig) WHERE n.v <= 9007199254740992 RETURN n`, false},

		{`MATCH (n:IntOvf) WHERE n.i < 365 * 86400 * 10 RETURN n`, true},
		{`MATCH (n:IntOvf) WHERE 2147483647 + 0 > 0 RETURN n`, true},
		{`MATCH (n:IntOvf) WHERE 2147483648 * 2 > 0 RETURN n`, true},
		{`MATCH (n:IntOvf) WHERE -2147483648 < 0 RETURN n`, true},
		{`MATCH (n:IntOvf) WHERE size(n.l) * 100 > 0 RETURN n`, true},
		{`MATCH (n:IntOvf) WHERE n.i + 3000000000 = 3000000001 RETURN n`, true},
		{`MATCH (n:IntOvf) WHERE size(n.l) + 2147483645 = 2147483648 RETURN n`, false},
		{`WITH 60 AS d MATCH (n:IntOvf) WHERE n.i < (datetime().epochseconds - (d * 86400)) RETURN n`, true},
		{`WITH 3000000000 AS d MATCH (n:IntOvf) WHERE n.i < d * 2 RETURN n`, true},
		{`MATCH (n:IntBig) WHERE n.v = 9007199254740990 RETURN n`, true},
	})
}

// TestTryCypherFloatLiteralArithmeticMatchesOracle compares arithmetic over
// float literals with PostgreSQL. dawgs prints a float literal with
// FormatFloat('f', -1): `2.0` reaches PostgreSQL as the int4 2 and `0.1` as
// an exact numeric; only a plain property next to a float literal is cast
// to float8. So `size(n.l) / 2.0` and `5.0 / 2` divide in integers,
// `0.1 + 0.2 = 0.3` holds exactly, `n.i % 2.0` has no operator
// (`double precision % integer`), and `WITH 5.0 AS x` is an int4 column --
// where the evaluator computed every one of them in float64. float8
// arithmetic itself raises an error on overflow and on a product that
// underflows to zero; the evaluator returned Inf and 0.
func TestTryCypherFloatLiteralArithmeticMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"FltDiv", map[string]any{"name": "d1", "l": []any{"a", "b", "c"}}},
		{"FltLit", map[string]any{"name": "t1", "i": 1, "f": 1.0}},
		{"FltLit", map[string]any{"name": "t2", "i": 2, "f": 1.5}},
		{"FltBig", map[string]any{"name": "g1", "f": 10.0}},
		{"FltTiny", map[string]any{"name": "s1", "f": 1e-300}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:FltDiv) WHERE size(n.l) / 2.0 = 1.5 RETURN n`, false},
		{`MATCH (n:FltDiv) WHERE size(n.l) / 2.0 > 1 RETURN n`, false},
		{`MATCH (n:FltDiv) WHERE size(n.l) * 1.0 / 2 = 1.5 RETURN n`, false},
		{`MATCH (n:FltDiv) WHERE size(n.l) / 2.0 < 1.2 RETURN n`, false},
		{`MATCH (n:FltDiv) RETURN size(n.l) / 2.0 AS x`, false},
		{`MATCH (n:FltLit) WHERE 5.0 / 2 = 2.5 RETURN n`, false},
		{`MATCH (n:FltLit) WHERE 0.1 + 0.2 = 0.3 RETURN n`, false},
		{`MATCH (n:FltLit) WHERE (0.1 + 0.2) * n.f = 0.3 RETURN n`, false},
		{`MATCH (n:FltLit) WHERE n.i % 2.0 = 1 RETURN n`, false},
		{`MATCH (n:FltLit) WHERE 7.0 % 2 = 1 RETURN n`, false},
		{`WITH 5.0 AS x MATCH (n:FltLit) WHERE x / 2.0 = 2.5 RETURN n`, false},
		{`WITH 5.5 AS x MATCH (n:FltLit) WHERE x * 2 = 11 RETURN n`, false},
		{`MATCH (n:FltBig) WHERE n.f * 1e308 = 0 RETURN n`, false},
		{`MATCH (n:FltTiny) WHERE n.f * 1e-300 = 0 RETURN n`, false},

		{`MATCH (n:FltLit) WHERE n.i / 2.0 = 0.5 RETURN n`, true},
		{`MATCH (n:FltLit) WHERE n.f * 2.0 = 3.0 RETURN n`, true},
		{`MATCH (n:FltLit) WHERE 2.0 + 3.0 + n.i = 6 RETURN n`, true},
		{`MATCH (n:FltLit) WHERE n.i * -2.5 = -2.5 RETURN n`, true},
		{`MATCH (n:FltLit) WHERE (n.i + 1.0) / 2 = 1.5 RETURN n`, true},
		{`MATCH (n:FltLit) RETURN n.i / -2.0 AS x`, true},
		{`MATCH (n:FltLit) RETURN 2.0 * n.f + 0.5 AS x`, true},
	})
}

// TestTryCypherPropertyCastWidthMatchesOracle compares the shapes where
// dawgs casts a plain property to the type it infers for the property's
// partner with PostgreSQL. Next to size() -- `jsonb_array_length(...)::int`
// -- that is int4, `(p ->> 'v')::int`, which raises "out of range for type
// integer" for 3000000000; the evaluator cast every property as int8. Next
// to a WITH alias dawgs infers nothing and leaves the property text, so `n.v
// * d` is `text * integer`, an error, where the evaluator cast and computed.
// Next to a signed float literal it is float8, which the evaluator's
// bare-literal test missed (it cast int8 and declined the text '1.5').
func TestTryCypherPropertyCastWidthMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"CwWide", map[string]any{"name": "l1", "v": 3000000000, "l": []any{"a"}}},
		{"CwNarrow", map[string]any{"name": "s1", "v": 5, "l": []any{"a"}}},
		{"CwNarrow", map[string]any{"name": "s2", "v": "1.5", "l": []any{"a", "b"}}},
		{"CwAlias", map[string]any{"name": "a1", "v": 5}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:CwWide) WHERE n.v < size(n.l) RETURN n`, false},
		{`MATCH (n:CwWide) WHERE size(n.l) > n.v RETURN n`, false},
		{`MATCH (n:CwWide) WHERE n.v + size(n.l) = 5 RETURN n`, false},
		{`MATCH (n:CwWide) WHERE size(n.l) + n.v = 5 RETURN n`, false},
		{`MATCH (n:CwWide) WHERE n.v * size(n.l) = 5 RETURN n`, false},
		{`WITH 2 AS d MATCH (n:CwAlias) WHERE n.v * d = 10 RETURN n`, false},
		{`WITH 2 AS d MATCH (n:CwAlias) WHERE d * n.v = 10 RETURN n`, false},
		{`WITH 2 AS d MATCH (n:CwAlias) WHERE n.v + d = 7 RETURN n`, false},

		{`MATCH (n:CwWide) WHERE n.v < size(n.l) * 2 RETURN n`, true},
		{`MATCH (n:CwWide) WHERE n.v < id(n) RETURN n`, true},
		{`MATCH (n:CwNarrow) WHERE n.v < -2.5 RETURN n`, true},
		{`MATCH (n:CwNarrow) WHERE n.v > -2.5 RETURN n`, true},
		{`WITH 2 AS d MATCH (n:CwWide) WHERE n.v > d * 1 RETURN n`, true},
		{`MATCH (n:CwWide) WHERE n.v < datetime().epochseconds RETURN n`, true},
	})
}

// TestTryCypherBigNumbersMatchOracle compares queries over integers past
// 2^53 with PostgreSQL. jsonb keeps 9007199254740993 and 9007199254740992
// apart as numerics; the replica decodes both to the float64
// 9007199254740992, so DISTINCT and grouping merged them, a join and an
// equality with 9007199254740992 matched both, and an ORDER BY saw a tie.
func TestTryCypherBigNumbersMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"BigNum", map[string]any{"name": "x1", "big": json.Number("9007199254740993")}},
		{"BigNum", map[string]any{"name": "x2", "big": json.Number("9007199254740992")}},
		{"BigNum2", map[string]any{"name": "y1", "big": json.Number("9007199254740992")}},
		{"BigSafe", map[string]any{"name": "z1", "safe": json.Number("9007199254740991")}},
		{"BigSafe", map[string]any{"name": "z2", "safe": json.Number("12")}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:BigNum) RETURN DISTINCT n.big`, false},
		{`MATCH (n:BigNum) RETURN n.big, count(n)`, false},
		{`MATCH (a:BigNum), (b:BigNum2) WHERE a.big = b.big RETURN a.name, b.name`, false},
		{`MATCH (a:BigNum) WITH a MATCH (b:BigNum2) WHERE a.big = b.big RETURN a.name, b.name`, false},
		{`MATCH (n:BigNum) WHERE n.big = 9007199254740992 RETURN n.name`, false},
		{`MATCH (n:BigNum) WHERE n.big <> 9007199254740992 RETURN n.name`, false},
		{`MATCH (n:BigNum {big: 9007199254740992}) RETURN n.name`, false},
		{`MATCH (n:BigNum) RETURN n.name ORDER BY n.big DESC LIMIT 1`, false},

		{`MATCH (n:BigSafe) RETURN DISTINCT n.safe`, true},
		{`MATCH (n:BigSafe) RETURN n.safe, count(n)`, true},
		{`MATCH (n:BigSafe) WHERE n.safe = 9007199254740991 RETURN n.name`, true},
		{`MATCH (n:BigSafe) RETURN n.name ORDER BY n.safe DESC LIMIT 1`, true},
	})
}

// TestTryCypherNonCanonicalNumbersMatchOracle compares queries over numbers
// stored with a spelling the float64 does not reproduce -- 1.0, 2.50, as
// JSON written by other means, or by dawgs' own `SET n.x = 0.5 + 0.5` --
// with PostgreSQL. Its `->>` keeps the spelling: '1.0'::int8 is an error, and
// '2.50' is neither '2.5' nor ends in '.5'. The replica decodes both to a
// float64 and answered from Go's rendering of it.
func TestTryCypherNonCanonicalNumbersMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"NcNum", map[string]any{"name": "n1", "x": json.Number("1.0")}},
		{"NcNum", map[string]any{"name": "n2", "x": json.Number("2.50")}},
		{"NcList", map[string]any{"name": "l1", "xs": []any{json.Number("1.0"), json.Number("2")}}},
		{"NcSet", map[string]any{"name": "m1", "x": 7}},
		{"NcOk", map[string]any{"name": "k1", "y": json.Number("1.5"), "ys": []any{1, 2}}},
	})
	if err := pgDriver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		res := tx.Query(`MATCH (n:NcSet) SET n.x = 0.5 + 0.5`, map[string]any{})
		defer res.Close()
		for res.Next() {
		}
		return res.Error()
	}); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := eng.RebuildNow(context.Background(), "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:NcSet) WHERE n.x < 5 RETURN n`, false},
		{`MATCH (n:NcNum) WHERE n.x IN ['1', '2.5'] RETURN n`, false},
		{`MATCH (n:NcNum) WHERE coalesce(n.x, '') = '2.5' RETURN n`, false},
		{`MATCH (n:NcNum) WHERE coalesce(n.x, '') ENDS WITH '.50' RETURN n`, false},
		{`MATCH (n:NcNum) WHERE n.x = 1 RETURN n`, false},
		{`MATCH (n:NcList) WHERE '1' IN n.xs RETURN n`, false},
		{`MATCH (n:NcList) WHERE 1 IN n.xs RETURN n`, false},
		{`MATCH (n:NcList) WHERE n.xs = [1, 2] RETURN n`, false},

		{`MATCH (n:NcOk) WHERE n.y < 5.0 RETURN n`, true},
		{`MATCH (n:NcOk) WHERE n.y IN ['1.5'] RETURN n`, true},
		{`MATCH (n:NcOk) WHERE '1' IN n.ys RETURN n`, true},
	})
}

// TestTryCypherOverlayNonCanonicalNumbersMatchOracle checks the number
// spelling fact holds over a delta: the base stores only canonical numbers,
// and a committed write -- read back into a segment -- stores 9007199254740993
// and 1.0. The overlay is the steady state of a live BloodHound, so a fact
// taken from the base alone would serve the collisions above.
func TestTryCypherOverlayNonCanonicalNumbersMatchOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()
	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	var written []*graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, props := range []map[string]any{
			{"name": "o1", "big": 9007199254740992, "x": 1},
			{"name": "o2", "big": 9007199254740992, "x": 2},
			{"name": "o3", "big": 5, "x": 3},
		} {
			n, err := tx.CreateNode(graph.AsProperties(props), graph.StringKind("OvlNum"))
			if err != nil {
				return err
			}
			written = append(written, n)
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
		{`MATCH (n:OvlNum) RETURN DISTINCT n.big`, true},
		{`MATCH (n:OvlNum) WHERE n.x < 5 RETURN n`, true},
	})

	n := written[0]
	if _, err := pool.Exec(ctx, `update node set properties = properties || '{"big": 9007199254740993, "x": 1.0}'::jsonb where id = $1`, int64(n.ID)); err != nil {
		t.Fatalf("rewrite node: %v", err)
	}
	scope := NewWriteScope()
	scope.Changes().RecordNodeID(n.ID)
	eng.Apply(ctx, scope)
	if snap, serving := eng.serveState(); snap == nil || !snap.Overlay() || !serving {
		t.Fatalf("the rewrite did not leave the engine serving an overlay; this test would be vacuous")
	}

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:OvlNum) RETURN DISTINCT n.big`, false},
		{`MATCH (n:OvlNum) RETURN n.big, count(n)`, false},
		{`MATCH (n:OvlNum) WHERE n.big = 9007199254740992 RETURN n.name`, false},
		{`MATCH (n:OvlNum) WHERE n.x < 5 RETURN n`, false},
		{`MATCH (n:OvlNum) WHERE n.x IN ['1', '2'] RETURN n`, false},
		{`MATCH (n:OvlNum) WHERE n.name = 'o3' RETURN n.name`, true},
	})
}
