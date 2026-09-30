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
