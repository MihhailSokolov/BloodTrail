// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"testing"
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
