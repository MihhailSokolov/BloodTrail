// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestNumericTypedProjectionsDecline pins the RETURN columns PostgreSQL
// types numeric, which the float64 value model has no rendering for. dawgs
// projects datetime().epochseconds as `extract(epoch from now())::numeric`
// -- the transaction clock with microseconds -- and a float literal keeps
// pg's numeric type unless a property cast to float8 is in the expression,
// so `1.5 * 2.0` is a numeric 3.0. The engine served the first as a
// truncated int64 (bare) or a float64 (as a group key next to count()), and
// the second as a float64. The controls stay served: float8 arithmetic over
// a property, the integer calls, and a plain property group key.
func TestNumericTypedProjectionsDecline(t *testing.T) {
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{1: "User"},
		[]execNodeSpec{{1, []snapshot.KindID{1}, map[string]any{"name": "u", "f": 1.5, "l": []any{"a"}}}}, nil)

	for _, tc := range []struct {
		query  string
		served bool
	}{
		{`MATCH (n:User) RETURN datetime().epochseconds AS e`, false},
		{`MATCH (n:User) RETURN datetime().epochmillis AS e`, false},
		{`MATCH (n:User) RETURN datetime().epochseconds, count(n)`, false},
		{`MATCH (n:User) RETURN datetime().epochmillis AS m, count(n) AS c`, false},
		{`MATCH (n:User) RETURN 1.5 * 2.0 AS x`, false},
		{`MATCH (n:User) RETURN 2.5 + 1.5 AS x`, false},
		{`MATCH (n:User) RETURN datetime().epochseconds * 1.0 AS x`, false},

		{`MATCH (n:User) RETURN n.f * 2.0 AS x`, true},
		{`MATCH (n:User) RETURN 2.0 * n.f + 0.5 AS x`, true},
		{`MATCH (n:User) RETURN id(n) AS i`, true},
		{`MATCH (n:User) RETURN size(n.l) AS s`, true},
		{`MATCH (n:User) RETURN n.name, count(n)`, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			if _, ok := planNoFail(t, snap, tc.query); ok != tc.served {
				t.Fatalf("Plan() served = %v, want %v", ok, tc.served)
			}
		})
	}
}
