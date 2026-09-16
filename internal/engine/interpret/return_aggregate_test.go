// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"testing"
)

// TestReturnAggregates pins RETURN-position aggregation: the shapes, the
// grouping, and -- critically -- the COLUMN NAMES, which were taken from a
// live PostgreSQL oracle rather than assumed (an unaliased aggregate is
// named after its function, an unaliased property lookup is pg's `?column?`
// placeholder).
func TestReturnAggregates(t *testing.T) {
	snap := buildPropAnchorFixture(t, 20)

	for _, tc := range []struct {
		name     string
		query    string
		wantKeys []string
		wantRows int
		wantIt   func(t *testing.T, rs *ResultSet)
	}{
		{
			name:     "count over the whole match",
			query:    `MATCH (u:User) RETURN count(u)`,
			wantKeys: []string{"count"},
			wantRows: 1,
			wantIt: func(t *testing.T, rs *ResultSet) {
				if got := rs.Rows[0][0].Scalar; got != float64(20) {
					t.Fatalf("count = %v, want 20", got)
				}
			},
		},
		{
			name:     "count star",
			query:    `MATCH (u:User) RETURN count(*)`,
			wantKeys: []string{"count"},
			wantRows: 1,
		},
		{
			name:     "explicit alias wins over the function name",
			query:    `MATCH (u:User) RETURN count(u) AS c`,
			wantKeys: []string{"c"},
			wantRows: 1,
		},
		{
			// An aggregate over zero matches still produces its one row --
			// the SQL "aggregate with no GROUP BY" rule, which is why the
			// grouping happens in the match pipeline and not above it.
			name:     "count over no matches is one row of zero",
			query:    `MATCH (u:User) WHERE u.name = 'nobody' RETURN count(u)`,
			wantKeys: []string{"count"},
			wantRows: 1,
			wantIt: func(t *testing.T, rs *ResultSet) {
				if got := rs.Rows[0][0].Scalar; got != float64(0) {
					t.Fatalf("count = %v, want 0", got)
				}
			},
		},
		{
			// The grouping key is a PROPERTY, which only works because a
			// computed key is evaluated onto the input rows before grouping.
			name:     "grouped by a property",
			query:    `MATCH (u:User) RETURN u.name, count(u)`,
			wantKeys: []string{"?column?", "count"},
			wantRows: 20,
		},
		{
			name:     "ORDER BY the aggregate itself",
			query:    `MATCH (u:User) RETURN u.name, count(u) ORDER BY count(u) DESC LIMIT 5`,
			wantKeys: []string{"?column?", "count"},
			wantRows: 5,
		},
		{
			name:     "grouped by a property that repeats",
			query:    `MATCH (u:User) RETURN u.enabled, count(u)`,
			wantKeys: []string{"?column?", "count"},
			wantRows: 1, // every fixture user is enabled: one group
			wantIt: func(t *testing.T, rs *ResultSet) {
				if got := rs.Rows[0][1].Scalar; got != float64(20) {
					t.Fatalf("group count = %v, want 20", got)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, generousBudget)
			if len(rs.Keys) != len(tc.wantKeys) {
				t.Fatalf("keys = %v, want %v", rs.Keys, tc.wantKeys)
			}
			for i := range rs.Keys {
				if rs.Keys[i] != tc.wantKeys[i] {
					t.Fatalf("keys = %v, want %v", rs.Keys, tc.wantKeys)
				}
			}
			if len(rs.Rows) != tc.wantRows {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.wantRows)
			}
			if tc.wantIt != nil {
				tc.wantIt(t, rs)
			}
		})
	}
}

// TestReturnAggregateRefusals keeps the desugaring narrow: an aggregate
// nested inside a larger expression would have to be folded BEFORE the
// surrounding arithmetic is applied, which this projection layer does not
// do, so it declines rather than group by an expression containing its own
// aggregate.
func TestReturnAggregateRefusals(t *testing.T) {
	snap := buildPropAnchorFixture(t, 10)
	for _, q := range []string{
		`MATCH (u:User) RETURN count(u) + 1`,
		`MATCH (u:User) RETURN count(u.name)`,
		// COLLECT is servable only as WITH's id-set membership aggregate;
		// projecting it would return node ids where pg returns node
		// composites, so RETURN-position COLLECT stays delegated.
		`MATCH (u:User) RETURN collect(u)`,
	} {
		if _, ok := planNoFail(t, snap, q); ok {
			t.Fatalf("expected a decline for %q", q)
		}
	}
}
