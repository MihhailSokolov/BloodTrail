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
