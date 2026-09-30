// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"testing"
)

// TestTryCypherSplitNullSeparatorMatchesOracle compares split() with a
// missing or null separator with PostgreSQL. dawgs emits
// string_to_array(s, sep), which splits s into its characters when sep is
// NULL; the evaluator answered NULL instead, so `'a' IN split(n.s, n.sep)`
// dropped every row PostgreSQL returns.
func TestTryCypherSplitNullSeparatorMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"SplNull", map[string]any{"name": "p1", "s": "abc"}},
		{"SplNull", map[string]any{"name": "p2", "s": "José", "sep": nil}},
		{"SplNull", map[string]any{"name": "p3", "s": ""}},
		{"SplNull", map[string]any{"name": "p4"}},
		{"SplNull", map[string]any{"name": "p5", "s": "a,b", "sep": ","}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:SplNull) WHERE 'a' IN split(n.s, n.sep) RETURN n`, true},
		{`MATCH (n:SplNull) WHERE 'a' IN split(n.s, null) RETURN n`, true},
		{`MATCH (n:SplNull) WHERE 'é' IN split(n.s, n.sep) RETURN n`, true},
		{`MATCH (n:SplNull) WHERE NOT 'a' IN split(n.s, n.sep) RETURN n`, true},
		{`MATCH (n:SplNull) WHERE 'b' IN split(n.s, ',') RETURN n`, true},
	})
}

// TestTryCypherInListNonLiteralElementMatchesOracle compares IN over a list
// literal whose elements are not all literals with PostgreSQL. dawgs types
// the array from its literal elements and casts the rest -- `n.v IN [1, 1 +
// 1]` is `(p ->> 'v')::int8 = any(array [1, 1 + 1]::int8[])` -- or lowers
// the whole test to the constant `false` (`n.v IN [size(n.l)]`); the
// evaluator fell back to value.go's In, which parses text as a float,
// compares structurally once a NULL element is present and checks no types
// (`id(n) IN [toLower(n.v)]` is `bigint = text` there, and `n.v IN [x, 3]`
// over a WITH alias x missed the int8 cast's error for 2.5). Such a list
// now declines. A signed number literal is still a literal to dawgs -- the
// pre-built `NOT u.pwdlastset IN [-1.0, 0.0]` is `::float8 = any(array [-
// 1, 0]::float8[])` -- so it stays served, through the same cast as an
// unsigned list.
func TestTryCypherInListNonLiteralElementMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"InSize", map[string]any{"name": "i1", "v": 2, "l": []any{"a", "b"}}},
		{"InArith", map[string]any{"name": "f1", "v": 1.5}},
		{"InArith", map[string]any{"name": "f2", "v": "2.0"}},
		{"InText", map[string]any{"name": "s1", "v": 5}},
		{"InText", map[string]any{"name": "s2", "v": "a"}},
		{"InStr", map[string]any{"name": "t1", "v": "a"}},
		{"InAlias", map[string]any{"name": "w1", "v": 2.5}},
		{"InAlias", map[string]any{"name": "w2", "v": 3}},
		{"InSigned", map[string]any{"name": "p1", "pwdlastset": -1}},
		{"InSigned", map[string]any{"name": "p2", "pwdlastset": 0}},
		{"InSigned", map[string]any{"name": "p3", "pwdlastset": 1700000000}},
		{"InSigned", map[string]any{"name": "p4", "pwdlastset": "7"}},
		{"InSigned", map[string]any{"name": "p5"}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:InSize) WHERE n.v IN [size(n.l)] RETURN n`, false},
		{`MATCH (n:InArith) WHERE n.v IN [1, 1 + 1] RETURN n`, false},
		{`MATCH (n:InText) WHERE n.v IN ['5', toLower(n.w)] RETURN n`, false},
		{`MATCH (n:InText) WHERE NOT n.v IN ['b', toLower(n.w)] RETURN n`, false},
		{`MATCH (n:InText) WHERE toLower(n.v) IN ['b', toLower(n.w)] RETURN n`, false},
		{`MATCH (n:InStr) WHERE id(n) IN [toLower(n.v)] RETURN n`, false},
		{`MATCH (n:InStr) WHERE NOT id(n) IN [toLower(n.v)] RETURN n`, false},
		{`MATCH (n:InText) WHERE 'a' IN [n.v, n.w] RETURN n`, false},
		{`MATCH (n:InSigned) WHERE n.pwdlastset IN [-1, 0.0] RETURN n`, false},
		{`WITH 2 AS x MATCH (n:InAlias) WHERE n.v IN [x, 3] RETURN n`, false},

		{`MATCH (n:InSigned) WHERE NOT n.pwdlastset IN [-1.0, 0.0] RETURN n`, true},
		{`MATCH (n:InSigned) WHERE n.pwdlastset IN [-1.0, 0.0] RETURN n`, true},
		{`MATCH (n:InSigned) WHERE n.pwdlastset IN [-1, 0] RETURN n`, true},
		{`MATCH (n:InSigned) WHERE n.pwdlastset IN [7, (0)] RETURN n`, true},
		{`MATCH (n:InSize) WHERE size(n.l) IN [1, 2] RETURN n`, true},
	})
}
