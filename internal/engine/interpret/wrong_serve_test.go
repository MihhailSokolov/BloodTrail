// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// This file pins a batch of confirmed wrong serves: queries the engine used
// to ANSWER, with a result that differs from the one the dawgs PostgreSQL
// translation produces for the same Cypher. Each one is now either answered
// the way pg answers it or declined -- never answered differently.

// expectDecline plans query (which must plan) and executes it, failing the
// test unless Execute declines with one of the two errors that delegate a
// query to PostgreSQL.
func expectDecline(t *testing.T, snap *snapshot.View, query string) {
	t.Helper()
	if _, ok := planNoFail(t, snap, query); !ok {
		return // declined at plan time
	}
	err := execExpectErr(t, snap, query, generousBudget)
	if !errors.Is(err, errUnsupportedStep) && !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Execute(%q): got %v, want a decline (errUnsupportedStep or ErrUnsupported)", query, err)
	}
}

// buildNestedGroupFixture: n users, each MemberOf group H, and H MemberOf
// group G. Every user reaches G by exactly one trail, u->H->G, and H by
// exactly one, u->H -- so no mandatory node tuple ever repeats.
func buildNestedGroupFixture(t *testing.T, n int) *snapshot.View {
	t.Helper()
	const (
		kiUser     snapshot.KindID = 1
		kiGroup    snapshot.KindID = 2
		keMemberOf snapshot.KindID = 3
	)
	kinds := map[snapshot.KindID]string{kiUser: "User", kiGroup: "Group", keMemberOf: "MemberOf"}
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kiGroup}, map[string]any{"name": "H"}},
		{2, []snapshot.KindID{kiGroup}, map[string]any{"name": "G"}},
	}
	edges := []execEdgeSpec{{1, 1, 2, keMemberOf}}
	for i := 0; i < n; i++ {
		id := uint64(100 + i)
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{kiUser}, map[string]any{"name": fmt.Sprintf("U%d", i)}})
		edges = append(edges, execEdgeSpec{uint64(1000 + i), id, 1, keMemberOf})
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestLimitDoesNotTruncateReturnAggregate: a RETURN aggregate's LIMIT counts
// GROUPS, which exist only after every matched row has been folded -- so it
// can never be pushed below the grouping into the pattern match. It used to
// be: limitTarget ignored ReturnGroup, so the chunked driver stopped after
// one 1024-row chunk and COUNT folded a prefix (1024 instead of 3000), and
// the same target capped the reverse var-length expansion to a single trail
// (1 instead of 50).
func TestLimitDoesNotTruncateReturnAggregate(t *testing.T) {
	many := buildManyEnabledUsers(t, 3000)
	nested := buildNestedGroupFixture(t, 50)

	for _, tc := range []struct {
		name  string
		snap  *snapshot.View
		query string
		want  []string
	}{
		{"count under LIMIT", many,
			`MATCH (u:User) WHERE u.enabled = true RETURN count(u) LIMIT 1`, []string{"S3000|"}},
		{"grouped count under LIMIT", many,
			`MATCH (u:User) RETURN u.enabled, count(u) LIMIT 1`, []string{"Strue|S3000|"}},
		{"count over a var-length trail under LIMIT", nested,
			`MATCH (u:User)-[:MemberOf*1..]->(g:Group) WHERE g.name = 'G' RETURN count(u) LIMIT 1`, []string{"S50|"}},
		{"WITH aggregate boundary under LIMIT", many,
			`MATCH (u:User) WITH count(u) AS n RETURN n LIMIT 1`, []string{"S3000|"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRowSet(t, mustExec(t, tc.snap, tc.query, generousBudget), tc.want)
		})
	}
}

// TestLimitDoesNotCapOptionalSide: the final LIMIT bounds the OUTPUT rows of
// the left join, not the rows the OPTIONAL MATCH may produce. Threading it
// onto the meter let the reverse var-length expansion inside the optional
// part stop after one trail, which null-padded every other user -- and LIMIT
// 1 then returned (U0, null), a row the unlimited answer does not contain
// (every user reaches G).
func TestLimitDoesNotCapOptionalSide(t *testing.T) {
	snap := buildNestedGroupFixture(t, 50)
	const base = `MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf*1..]->(g:Group) WHERE g.name = 'G' RETURN u, g`

	all := mustExec(t, snap, base, generousBudget)
	if len(all.Rows) != 50 {
		t.Fatalf("unlimited: got %d rows, want 50", len(all.Rows))
	}
	valid := map[string]bool{}
	for _, k := range rowKeys(all.Rows) {
		valid[k] = true
	}

	rs := mustExec(t, snap, base+` LIMIT 1`, generousBudget)
	if len(rs.Rows) != 1 {
		t.Fatalf("LIMIT 1: got %d rows, want 1", len(rs.Rows))
	}
	if k := rowKey(rs.Rows[0]); !valid[k] {
		t.Fatalf("LIMIT 1 served %s, which is not a row of the unlimited answer", k)
	}
}

// buildRepeatedTupleFixture: U(User), G1/G2(Group), C(Computer), with
// U-MemberOf->G1, G1-MemberOf->G2, U-MemberOf->G2, G2-AdminTo->C,
// U-HasSession->C, U-AdminTo->G1. (U, G2) is reached by two MemberOf trails,
// and (U, G1) by two parallel edges of an admitted kind -- both repeat a
// mandatory node tuple.
func buildRepeatedTupleFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const (
		kiUser     snapshot.KindID = 1
		kiGroup    snapshot.KindID = 2
		kiComputer snapshot.KindID = 3
		keMemberOf snapshot.KindID = 4
		keAdminTo  snapshot.KindID = 5
		keSession  snapshot.KindID = 6
	)
	kinds := map[snapshot.KindID]string{
		kiUser: "User", kiGroup: "Group", kiComputer: "Computer",
		keMemberOf: "MemberOf", keAdminTo: "AdminTo", keSession: "HasSession",
	}
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kiUser}, map[string]any{"name": "U"}},
		{2, []snapshot.KindID{kiGroup}, map[string]any{"name": "G1"}},
		{3, []snapshot.KindID{kiGroup}, map[string]any{"name": "G2"}},
		{4, []snapshot.KindID{kiComputer}, map[string]any{"name": "C"}},
	}
	edges := []execEdgeSpec{
		{10, 1, 2, keMemberOf},
		{11, 2, 3, keMemberOf},
		{12, 1, 3, keMemberOf},
		{13, 3, 4, keAdminTo},
		{14, 1, 4, keSession},
		{15, 1, 2, keAdminTo},
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestOptionalMatchDeclinesRepeatedMandatoryTuples: dawgs builds the
// OPTIONAL MATCH's CTE FROM the mandatory CTE and then left-joins the two on
// EVERY mandatory node column, so a node tuple the mandatory side produces k
// times, with m optional matches, comes out k*k*m times -- not the k*m a
// per-row left join gives. The engine cannot reproduce that on purpose
// without pinning dawgs' plan shape, so a repeated tuple that the optional
// side matches declines; pg answers 5 rows for both queries below, the
// engine used to answer 3.
func TestOptionalMatchDeclinesRepeatedMandatoryTuples(t *testing.T) {
	snap := buildRepeatedTupleFixture(t)

	t.Run("repeat from two var-length trails", func(t *testing.T) {
		expectDecline(t, snap,
			`MATCH (u:User)-[:MemberOf*1..]->(g:Group) OPTIONAL MATCH (g)-[:AdminTo]->(c:Computer) RETURN u, g, c`)
	})
	t.Run("repeat from parallel edges", func(t *testing.T) {
		expectDecline(t, snap,
			`MATCH (u:User)-[:MemberOf|AdminTo]->(g:Group) OPTIONAL MATCH (u)-[:HasSession]->(c:Computer) RETURN u, g, c`)
	})
	t.Run("a repeated tuple the optional side does not match is still served", func(t *testing.T) {
		// (U, G1) repeats, but neither G1 nor G2 has a HasSession edge: every
		// row null-pads, and the left join yields exactly the k rows it had
		// -- the same multiset pg's join produces.
		rs := mustExec(t, snap,
			`MATCH (u:User)-[:MemberOf|AdminTo]->(g:Group) OPTIONAL MATCH (g)-[:HasSession]->(c:Computer) RETURN u, g, c`,
			generousBudget)
		assertRowSet(t, rs, []string{"N0|N1|S<nil>|", "N0|N1|S<nil>|", "N0|N2|S<nil>|"})
	})
}

// TestDistinctStreamingAppliesUnpushedWhere: a conjunct that touches no
// pattern symbol (`1 = 2`, `false`) is never pushed into a candidate source,
// so it survives only in Part.Where. The streaming DISTINCT path's
// node-only branch emitted each scanned candidate without evaluating
// Part.Where at all, answering every name where pg answers none.
func TestDistinctStreamingAppliesUnpushedWhere(t *testing.T) {
	snap := buildDistinctFixture(t, 20)
	for _, q := range []string{
		`MATCH (u:User) WHERE 1 = 2 RETURN DISTINCT u.name`,
		`MATCH (u:User) WHERE false RETURN DISTINCT u.name`,
		`MATCH (u:User) WHERE false RETURN DISTINCT u.name LIMIT 5`,
	} {
		t.Run(q, func(t *testing.T) {
			if rs := mustExec(t, snap, q, generousBudget); len(rs.Rows) != 0 {
				t.Fatalf("got %d rows, want 0", len(rs.Rows))
			}
		})
	}

	// A WHERE that does keep rows still dedups correctly.
	rs := mustExec(t, snap, `MATCH (u:User) WHERE 1 = 1 RETURN DISTINCT u.enabled`, generousBudget)
	assertRowSet(t, rs, []string{"Sfalse|", "Strue|"})
}

// TestReturnAggregateOrderByUngroupedPropertyDeclines: dawgs lowers `RETURN
// count(u) ORDER BY u.x` to `select count(s0.n0) from s0 order by
// (s0.n0).properties -> 'x'`, which PostgreSQL rejects -- the sort key is
// neither grouped nor aggregated. The engine sorted the grouped rows by a
// property of a symbol those rows no longer bind, and with one group served
// [3]. A row-evaluated sort key under a RETURN aggregate now declines.
func TestReturnAggregateOrderByUngroupedPropertyDeclines(t *testing.T) {
	snap := buildOrderFixture(t)
	expectDecline(t, snap, `MATCH (u:N) RETURN count(u) ORDER BY u.score DESC`)
}

// TestPatternPredicateOnNullOptionalSymbolDeclines: an OPTIONAL MATCH symbol
// a row left null, used as a pattern-predicate endpoint later on, used to
// read as "not bound" -- and the predicate then fell into the ANONYMOUS
// endpoint branch, which asks whether SOME node fits. A null endpoint is not
// a wildcard: pg answers 0 rows and 7 rows for the two queries below, the
// engine answered 4 and 3. A named endpoint the row does not bind now
// declines.
func TestPatternPredicateOnNullOptionalSymbolDeclines(t *testing.T) {
	snap := buildOptionalFixture(t)
	expectDecline(t, snap,
		`MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) WITH u, g MATCH (c:Computer) WHERE (g)-[:MemberOf]->(c) RETURN u, c`)
	expectDecline(t, snap,
		`MATCH (u:User) OPTIONAL MATCH (u)-[:MemberOf]->(g:Group) WITH u, g MATCH (c:Computer) WHERE NOT (g)-[:MemberOf]->(c) RETURN u, c`)
}

// TestReturnAggregateSeparatesJSONNullFromAbsent: pg groups by
// `(s0.n0).properties -> 'x'`, where an absent key is SQL NULL and a stored
// JSON null is the non-NULL jsonb 'null' -- two different groups. The engine
// bound both as a Go nil and merged them into one group of 3.
func TestReturnAggregateSeparatesJSONNullFromAbsent(t *testing.T) {
	const kiUser snapshot.KindID = 1
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{kiUser: "User"}, []execNodeSpec{
		{1, []snapshot.KindID{kiUser}, map[string]any{"name": "a", "x": nil}},
		{2, []snapshot.KindID{kiUser}, map[string]any{"name": "b", "x": nil}},
		{3, []snapshot.KindID{kiUser}, map[string]any{"name": "c"}},
		{4, []snapshot.KindID{kiUser}, map[string]any{"name": "d", "x": 5}},
	}, nil)

	for _, q := range []string{
		`MATCH (u:User) RETURN u.x, count(u)`,
		`MATCH (u:User) RETURN u.x AS x, count(u) AS n`,
		`MATCH (u:User) RETURN u.x, count(u) ORDER BY count(u) DESC`,
	} {
		t.Run(q, func(t *testing.T) {
			rs := mustExec(t, snap, q, generousBudget)
			got := map[string]int64{}
			for _, row := range rs.Rows {
				if len(row) != 2 {
					t.Fatalf("got %d columns, want 2", len(row))
				}
				var key string
				switch {
				case row[0].ScalarAbsent:
					key = "absent"
				case row[0].Scalar == nil:
					key = "json-null"
				default:
					key = fmt.Sprint(row[0].Scalar)
				}
				n, _ := row[1].Scalar.(int64)
				if f, ok := row[1].Scalar.(float64); ok {
					n = int64(f)
				}
				got[key] = n
			}
			want := map[string]int64{"json-null": 2, "absent": 1, "5": 1}
			if len(got) != len(want) || len(rs.Rows) != len(want) {
				t.Fatalf("got groups %v, want %v", got, want)
			}
			for k, v := range want {
				if got[k] != v {
					t.Fatalf("got groups %v, want %v", got, want)
				}
			}
		})
	}
}
