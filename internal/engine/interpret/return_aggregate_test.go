// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
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

// TestReturnAggregateNodeGroupKey: a bare node variable next to an aggregate
// groups by node IDENTITY and projects the node, as pg does. It used to be
// evaluated as a computed expression, which yields the node's property map:
// the column came back as a map instead of a node, and two distinct nodes
// with identical properties merged into one group.
func TestReturnAggregateNodeGroupKey(t *testing.T) {
	const (
		kiUser     snapshot.KindID = 1
		kiGroup    snapshot.KindID = 2
		keMemberOf snapshot.KindID = 3
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kiUser: "User", kiGroup: "Group", keMemberOf: "MemberOf"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kiUser}, map[string]any{"name": "twin"}},
			{2, []snapshot.KindID{kiUser}, map[string]any{"name": "twin"}},
			{3, []snapshot.KindID{kiGroup}, map[string]any{"name": "G"}},
		},
		[]execEdgeSpec{{10, 1, 3, keMemberOf}, {11, 2, 3, keMemberOf}},
	)

	for _, q := range []string{
		`MATCH (u:User) RETURN u, count(u)`,
		`MATCH (u:User)-[:MemberOf]->(g:Group) RETURN u, count(g)`,
		`MATCH (u:User) RETURN u AS who, count(*)`,
	} {
		rs := mustExec(t, snap, q, generousBudget)
		if len(rs.Rows) != 2 {
			t.Fatalf("%s: got %d rows, want 2 (one per node, not per property bag)", q, len(rs.Rows))
		}
		for _, row := range rs.Rows {
			if row[0].Kind != OutNode {
				t.Fatalf("%s: grouping column projected as %v (%#v), want OutNode", q, row[0].Kind, row[0].Scalar)
			}
			if row[1].Scalar != float64(1) {
				t.Fatalf("%s: count = %v, want 1", q, row[1].Scalar)
			}
		}
	}

	rs := mustExec(t, snap, `MATCH (u:User)-[r:MemberOf]->(g:Group) RETURN r, count(u)`, generousBudget)
	if len(rs.Rows) != 2 || rs.Rows[0][0].Kind != OutEdge {
		t.Fatalf("edge group key: got %+v, want 2 rows with an OutEdge column", rs.Rows)
	}
}

// TestReturnAggregateUndeclaredVariableNotServed: pg rejects a RETURN that
// names an undeclared variable, so the engine must not answer it -- a
// plan-time decline or an Execute error both delegate. (desugarReturnAggregates
// routes node/edge variables to group keys; isEntitySymbol's presence check
// keeps an undeclared one, whose zero-value kind reads as symNode, off that
// route.)
func TestReturnAggregateUndeclaredVariableNotServed(t *testing.T) {
	snap := buildPropAnchorFixture(t, 10)
	const q = `MATCH (u:User) RETURN nope, count(u)`
	pq, ok := planNoFail(t, snap, q)
	if !ok {
		return
	}
	if rs, err := Execute(&Env{Snap: snap}, pq, generousBudget); err == nil {
		t.Fatalf("served %q: %+v", q, rs.Rows)
	}
}

// TestReturnAggregateComputedKeysArePropertiesOnly: a computed group key is
// rewritten to a synthetic alias, which loses the typing and naming
// planReturn would give the expression -- `id(u)` came back float64 where pg
// returns int8, `toLower(u.name)` was named "tolower" where pg names it
// "lower", a carried count alias lost its int8. Only a bare property lookup,
// the corpus's shape, is served.
func TestReturnAggregateComputedKeysArePropertiesOnly(t *testing.T) {
	snap := buildPropAnchorFixture(t, 10)
	for _, q := range []string{
		`MATCH (u:User) RETURN id(u), count(u)`,
		`MATCH (u:User) RETURN toLower(u.name), count(u)`,
		`MATCH (u:User) RETURN size(u.name), count(u)`,
		`MATCH (u:User) WITH u, count(*) AS n RETURN n, count(u)`,
	} {
		if _, ok := planNoFail(t, snap, q); ok {
			t.Errorf("expected a decline for %q", q)
		}
	}
	if _, ok := planNoFail(t, snap, `MATCH (u:User) RETURN u.enabled, count(u)`); !ok {
		t.Error("a bare property group key must still be served")
	}
}

// TestReturnColumnNamesFoldLikePostgres: dawgs writes every alias unquoted
// (`select s0.n0 as adminCount`), so PostgreSQL reports it folded to lower
// case -- `admincount`, which is what BloodHound's own
// `WITH u, COUNT(c) AS adminCount RETURN u, adminCount` prebuilt gets back.
// A backticked alias reaches pg with its backticks, a syntax error there.
func TestReturnColumnNamesFoldLikePostgres(t *testing.T) {
	snap := buildPropAnchorFixture(t, 10)
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`MATCH (u:User) RETURN u AS Foo`, []string{"foo"}},
		{`MATCH (U:User) RETURN U`, []string{"u"}},
		{`MATCH (u:User) RETURN u.name AS Name, u.name`, []string{"name", "?column?"}},
		{`MATCH (u:User) WITH u, count(u) AS adminCount RETURN u, adminCount`, []string{"u", "admincount"}},
		{`MATCH (u:User) RETURN count(u) AS adminCount`, []string{"admincount"}},
	} {
		rs := mustExec(t, snap, tc.query, generousBudget)
		if fmt.Sprint(rs.Keys) != fmt.Sprint(tc.want) {
			t.Errorf("%s: keys %v, want %v", tc.query, rs.Keys, tc.want)
		}
	}
	if _, ok := planNoFail(t, snap, "MATCH (u:User) RETURN u.name AS `u.x`"); ok {
		t.Error("a backticked alias must decline")
	}
}
