// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// --- corpus aggregation shape: grouping, ordering, limit --------------------

// TestPipelineCorpusAggregationEndToEnd mirrors plan_test.go's
// corpusAggregationQuery shape end to end on a crafted snapshot: `WITH
// DISTINCT u, COUNT(c) AS adminCount RETURN u ORDER BY adminCount DESC LIMIT
// N` must group by u, count every (u, c) trail per group, order the groups
// by that count descending, and keep only the top N -- asserted as an exact,
// ordered row sequence (not merely a set), since ORDER BY/LIMIT's whole
// point is the sequence.
func TestPipelineCorpusAggregationEndToEnd(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindComputer snapshot.KindID = 2
		kindGroup    snapshot.KindID = 3
		kindMemberOf snapshot.KindID = 10
		kindAdminTo  snapshot.KindID = 11
	)

	qualifying := func(objectid string) map[string]any {
		return map[string]any{
			"hasspn":   true,
			"enabled":  true,
			"objectid": objectid,
		}
	}

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{
			kindUser: "User", kindComputer: "Computer", kindGroup: "Group",
			kindMemberOf: "MemberOf", kindAdminTo: "AdminTo",
		},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, qualifying("S-1-1")},                                                                // u1: 3 direct AdminTo -> adminCount 3
			{2, []snapshot.KindID{kindUser}, qualifying("S-1-2")},                                                                // u2: 1 direct AdminTo -> adminCount 1
			{3, []snapshot.KindID{kindUser}, map[string]any{"hasspn": false, "enabled": true, "objectid": "S-1-3"}},              // u3: hasspn false -> excluded
			{4, []snapshot.KindID{kindUser}, qualifying("S-1-4")},                                                                // u4: MemberOf -> group -> 2 computers -> adminCount 2
			{5, []snapshot.KindID{kindUser}, qualifying("S-1-500-502")},                                                          // u5: objectid ends -502 -> excluded
			{6, []snapshot.KindID{kindUser}, map[string]any{"hasspn": true, "enabled": false, "objectid": "S-1-6"}},              // u6: not enabled -> excluded
			{7, []snapshot.KindID{kindUser}, map[string]any{"hasspn": true, "enabled": true, "objectid": "S-1-7", "gmsa": true}}, // u7: gmsa -> excluded
			{8, []snapshot.KindID{kindUser}, map[string]any{"hasspn": true, "enabled": true, "objectid": "S-1-8", "msa": true}},  // u8: msa -> excluded

			{201, []snapshot.KindID{kindComputer}, nil}, // c1 (u1)
			{202, []snapshot.KindID{kindComputer}, nil}, // c2 (u1)
			{203, []snapshot.KindID{kindComputer}, nil}, // c3 (u1)
			{204, []snapshot.KindID{kindComputer}, nil}, // c4 (u2)
			{205, []snapshot.KindID{kindComputer}, nil}, // c5 (u3, must never surface)
			{206, []snapshot.KindID{kindComputer}, nil}, // c6 (u4, via group)
			{207, []snapshot.KindID{kindComputer}, nil}, // c7 (u4, via group)
			{208, []snapshot.KindID{kindComputer}, nil}, // c8 (u5, excluded user)
			{209, []snapshot.KindID{kindComputer}, nil}, // c9 (u6, excluded user)
			{210, []snapshot.KindID{kindComputer}, nil}, // c10 (u7, excluded user)
			{211, []snapshot.KindID{kindComputer}, nil}, // c11 (u8, excluded user)

			{301, []snapshot.KindID{kindGroup}, nil}, // g1 (u4's group)
		},
		[]execEdgeSpec{
			{1000, 1, 201, kindAdminTo}, {1001, 1, 202, kindAdminTo}, {1002, 1, 203, kindAdminTo},
			{1003, 2, 204, kindAdminTo},
			{1004, 3, 205, kindAdminTo},
			{1005, 4, 301, kindMemberOf}, {1006, 301, 206, kindAdminTo}, {1007, 301, 207, kindAdminTo},
			{1008, 5, 208, kindAdminTo},
			{1009, 6, 209, kindAdminTo},
			{1010, 7, 210, kindAdminTo},
			{1011, 8, 211, kindAdminTo},
		},
	)

	query := `MATCH (u:User)
WHERE u.hasspn = true
  AND u.enabled = true
  AND NOT u.objectid ENDS WITH '-502'
  AND NOT COALESCE(u.gmsa, false) = true
  AND NOT COALESCE(u.msa, false) = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 2`

	rs := mustExec(t, snap, query, generousBudget)

	if want := []string{"u"}; !equalStrings(rs.Keys, want) {
		t.Fatalf("Keys = %v, want %v", rs.Keys, want)
	}
	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (LIMIT 2): %v", len(rs.Rows), rowKeys(rs.Rows))
	}

	u1, _ := snap.Dense(1)
	u4, _ := snap.Dense(4)
	wantOrder := []snapshot.NodeID{u1, u4} // adminCount 3, then 2 -- DESC
	for i, want := range wantOrder {
		got := rs.Rows[i]
		if len(got) != 1 || got[0].Kind != OutNode || got[0].Node != want {
			t.Fatalf("row %d = %+v, want OutNode(%d)", i, got, want)
		}
	}
}

// --- COLLECT anti-join: structural id-set membership ------------------------

// TestPipelineCollectMembershipAntiJoinEndToEnd exercises the corpus's
// `WITH COLLECT(s) AS exclude ... WHERE NOT c IN exclude` shape end to end,
// including the *0.. range from the corpus query (plan_test.go's
// corpusCollectAntiJoinQuery) for the first MATCH. The corpus's own second
// MATCH mixes a fixed-length step with a var-length step in one connected
// component, a shape expand.go's runComponent declines outright (a
// documented "mixed-Step component" scope decline) -- so this test's own
// second Part uses a plain, single-step pattern instead, preserving exactly
// the COLLECT-anti-join mechanism under test without depending on a shape
// that decline rejects.
func TestPipelineCollectMembershipAntiJoinEndToEnd(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindGroup    snapshot.KindID = 2
		kindMemberOf snapshot.KindID = 10
	)

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindGroup: "Group", kindMemberOf: "MemberOf"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, nil},                                             // s1: member of the -516 group -> excluded
			{2, []snapshot.KindID{kindUser}, nil},                                             // s2: member of an unrelated group -> survives
			{3, []snapshot.KindID{kindUser}, nil},                                             // s3: no group membership at all -> survives
			{301, []snapshot.KindID{kindGroup}, map[string]any{"objectid": "S-1-5-21-1-516"}}, // g1: Domain Admins-shaped
			{302, []snapshot.KindID{kindGroup}, map[string]any{"objectid": "S-1-5-21-1-999"}}, // g2: unrelated
		},
		[]execEdgeSpec{
			{2000, 1, 301, kindMemberOf},
			{2001, 2, 302, kindMemberOf},
		},
	)

	query := `MATCH (s:User)-[:MemberOf*0..]->(g:Group)
WHERE g.objectid ENDS WITH '-516'
WITH COLLECT(s) AS exclude
MATCH (c:User)
WHERE NOT c IN exclude
RETURN c`

	rs := mustExec(t, snap, query, generousBudget)

	s2, _ := snap.Dense(2)
	s3, _ := snap.Dense(3)
	want := []string{
		rowKey([]OutVal{{Kind: OutNode, Node: s2}}),
		rowKey([]OutVal{{Kind: OutNode, Node: s3}}),
	}
	assertRowSet(t, rs, want)
}

// --- sortRows over a string-valued alias: ErrCollation abort ----------------
//
// Plan itself never constructs an OrderKey for a string-valued alias any
// more (see planOrder's own doc: ORDER BY now
// requires a count-aggregate alias or a statically-numeric scalar, and a
// property lookup like `n.name` is neither) -- so this exercises sortRows
// directly, bypassing Plan/Execute entirely, to keep proving the plumbing
// itself still aborts correctly (rather than swallowing the error or
// silently falling back to some other order) for the day a future OrderKey
// source ever hands it a string, and because value.go's Compare unit tests
// (TestCompareString*, value_test.go) already assume this end-to-end
// propagation path exists and is exercised somewhere.
func TestPipelineSortRowsStringErrCollation(t *testing.T) {
	rowA, rowB := NewRow(), NewRow()
	outA := []OutVal{{Kind: OutScalar, Scalar: "Alice"}}
	outB := []OutVal{{Kind: OutScalar, Scalar: "Bob"}}

	err := sortRows(
		[]*Row{rowA, rowB},
		[][]OutVal{outA, outB},
		[]OrderKey{{Symbol: "name"}},
		map[string]int{"name": 0},
	)
	if !errors.Is(err, ErrCollation) {
		t.Fatalf("sortRows: err = %v, want ErrCollation", err)
	}
}

// --- RETURN DISTINCT: node dedup by id --------------------------------------

func TestPipelineDistinctNodeDedupByID(t *testing.T) {
	const (
		kindUser  snapshot.KindID = 1
		kindGroup snapshot.KindID = 2
		kindE     snapshot.KindID = 10
	)

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindGroup: "Group", kindE: "E"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, nil},
			{2, []snapshot.KindID{kindUser}, nil},
			{3, []snapshot.KindID{kindUser}, nil},
			{500, []snapshot.KindID{kindGroup}, nil},
		},
		[]execEdgeSpec{
			{9000, 1, 500, kindE},
			{9001, 2, 500, kindE},
			{9002, 3, 500, kindE},
		},
	)

	rs := mustExec(t, snap, `MATCH (a:User)-[:E]->(b:Group) RETURN DISTINCT b`, generousBudget)

	g, _ := snap.Dense(500)
	assertRowSet(t, rs, []string{rowKey([]OutVal{{Kind: OutNode, Node: g}})})
}

// TestPipelineDistinctKeepsAbsentAndExplicitNullApart is a regression test:
// PostgreSQL's DISTINCT groups an absent property
// (jsonb `->` on a missing key -> SQL NULL) separately from a genuinely
// stored JSON null (a non-NULL 'null'::jsonb value) -- SQL NULL groups with
// SQL NULL, and 'null'::jsonb groups with 'null'::jsonb, but the two never
// group together. Before OutVal.ScalarAbsent existed, this package's own
// dedup key rendered both as the identical "absent/nil" byte tag, collapsing
// what pg keeps as two separate groups into one.
//
// Fixture: four Users' "x" property is absent, present-null, "A", and "B"
// respectively. `RETURN DISTINCT x.x` must produce 4 rows (matching pg): the
// buggy behavior collapsed the absent and present-null rows into one,
// producing only 3.
func TestPipelineDistinctKeepsAbsentAndExplicitNullApart(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{}},         // x absent
			{2, []snapshot.KindID{kindUser}, map[string]any{"x": nil}}, // x present, explicit JSON null
			{3, []snapshot.KindID{kindUser}, map[string]any{"x": "A"}},
			{4, []snapshot.KindID{kindUser}, map[string]any{"x": "B"}},
		},
		nil,
	)

	rs := mustExec(t, snap, `MATCH (n:User) RETURN DISTINCT n.x AS x`, generousBudget)

	if len(rs.Rows) != 4 {
		t.Fatalf("RETURN DISTINCT row count = %d, want 4 (absent, explicit-null, \"A\", \"B\" each their own group); got %#v", len(rs.Rows), rs.Rows)
	}

	var absentCount, presentNullCount int
	values := map[string]bool{}
	for _, row := range rs.Rows {
		v := row[0]
		if v.Kind != OutScalar {
			t.Fatalf("row column kind = %v, want OutScalar", v.Kind)
		}
		switch {
		case v.ScalarAbsent:
			absentCount++
		case v.Scalar == nil:
			presentNullCount++
		default:
			values[fmt.Sprintf("%v", v.Scalar)] = true
		}
	}
	if absentCount != 1 {
		t.Fatalf("absent-group rows = %d, want 1", absentCount)
	}
	if presentNullCount != 1 {
		t.Fatalf("present-null-group rows = %d, want 1", presentNullCount)
	}
	if !values["A"] || !values["B"] {
		t.Fatalf("value groups = %v, want {A, B}", values)
	}
}

// TestGroupKeyDistinguishesAbsentFromPresentNullScalar is the grouping-key
// counterpart to TestPipelineDistinctKeepsAbsentAndExplicitNullApart above:
// appendSymbolKey (used to build both WITH's GroupKeys group key and
// COUNT(DISTINCT scalar)'s dedup key) must not collapse a scalar symbol
// that was never bound in one row with one bound to an explicit JSON null
// in another -- mirroring outValKey's identical RETURN DISTINCT fix (see
// OutVal.ScalarAbsent's doc) for exactly the same reason: pg groups SQL
// NULL together with other SQL NULLs, but never with the non-NULL jsonb
// value 'null'::jsonb.
//
// No query this planner currently accepts reaches this function with a
// scalar symbol (WithClause.GroupKeys is populated exclusively from
// Part[0], which has no scalar bindings before its own first WITH -- see
// this file's package doc, and countAggregate's own doc on its identical,
// currently-unreachable scalar COUNT(DISTINCT ...) branch), so this drives
// appendSymbolKey directly against hand-built Rows rather than through
// Plan/Execute, purely to pin the encoding contract for whenever a future
// change does make it reachable.
func TestGroupKeyDistinguishesAbsentFromPresentNullScalar(t *testing.T) {
	env := &Env{}

	unbound := NewRow()
	presentNull := NewRow()
	presentNull.SetScalar("x", nil)

	unboundKey := string(appendSymbolKey(env, nil, unbound, "x"))
	presentNullKey := string(appendSymbolKey(env, nil, presentNull, "x"))

	if unboundKey == presentNullKey {
		t.Fatalf("appendSymbolKey: unbound and present-null scalar produced the same key %q, want distinct", unboundKey)
	}
}

// --- WITH constant carry-over feeding a later predicate ---------------------

func TestPipelineWithConstantCarryOverFeedsPredicate(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{"objectid": "anchor", "threshold": float64(5)}},
			{2, []snapshot.KindID{kindUser}, map[string]any{"objectid": "m1", "threshold": float64(100)}}, // > 60 -> survives
			{3, []snapshot.KindID{kindUser}, map[string]any{"objectid": "m2", "threshold": float64(10)}},  // <= 60 -> excluded
			{4, []snapshot.KindID{kindUser}, map[string]any{"objectid": "m3"}},                            // no threshold -> excluded
		},
		nil,
	)

	query := `MATCH (n:User) WHERE n.objectid = 'anchor'
WITH 60 AS days
MATCH (m:User)
WHERE m.threshold > days
RETURN m`

	rs := mustExec(t, snap, query, generousBudget)

	m1, _ := snap.Dense(2)
	assertRowSet(t, rs, []string{rowKey([]OutVal{{Kind: OutNode, Node: m1}})})
}

// TestPipelineLeadingWithNoPrecedingMatchFeedsPredicate is a regression test
// for a real serving bug the dawgs corpus's own "bind a numeric literal as a
// WITH variable and use it in arithmetic in the next MATCH" case
// (integration/testdata/cases/multipart.json) surfaced once TryCypher was
// wired to this package: `WITH <expr> AS x MATCH ... WHERE ... x ...` -- a
// WITH with *no* MATCH before it at all, unlike
// TestPipelineWithConstantCarryOverFeedsPredicate above, whose Part[0] does
// have its own leading `MATCH (n:User) WHERE ...` -- served a completely
// empty ResultSet regardless of what Part[1]'s own MATCH would otherwise
// have matched.
//
// Root cause: Plan's planPart returns a valid Part{Nodes: map[string]
// *NodeConstraint{}} (empty, not nil) for a stage with zero ReadingClauses
// -- exactly what a leading, MATCH-less WITH stage plans to -- but runQuery
// (pipeline.go) called matchPart directly for Part[0] with no special
// handling for that empty-Nodes case, so groupComponents found zero
// components (it iterates part.Nodes' keys) and matchPart returned zero
// rows instead of the one solution ("the empty binding") the query
// actually has at that point. This silently discarded Part[0]'s only row
// before Part[1]'s own MATCH -- and the WITH-carried scalar it needed --
// ever ran, regardless of what that MATCH would otherwise have found.
// runCarriedPart already special-cases the identical "zero Nodes" shape for
// Part[1] (pipeline.go, `if len(part.Nodes) == 0`); the fix (exec.go's
// matchPart) gives Part[0] the same one-row treatment.
func TestPipelineLeadingWithNoPrecedingMatchFeedsPredicate(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{"objectid": "m1", "threshold": float64(100)}}, // > 60 -> survives
			{2, []snapshot.KindID{kindUser}, map[string]any{"objectid": "m2", "threshold": float64(10)}},  // <= 60 -> excluded
		},
		nil,
	)

	query := `WITH 60 AS days
MATCH (m:User)
WHERE m.threshold > days
RETURN m`

	rs := mustExec(t, snap, query, generousBudget)

	m1, _ := snap.Dense(1)
	assertRowSet(t, rs, []string{rowKey([]OutVal{{Kind: OutNode, Node: m1}})})
}

// --- COUNT vs COUNT(DISTINCT ...) -------------------------------------------

func TestPipelineCountVsCountDistinct(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindComputer snapshot.KindID = 2
		kindE        snapshot.KindID = 10
	)

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindComputer: "Computer", kindE: "E"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, nil},
			{201, []snapshot.KindID{kindComputer}, nil},
			{202, []snapshot.KindID{kindComputer}, nil},
		},
		[]execEdgeSpec{
			{5000, 1, 201, kindE}, // trail A: u1 -> c1
			{5001, 1, 201, kindE}, // trail B: u1 -> c1, a parallel edge -- a second, distinct trail to the SAME computer
			{5002, 1, 202, kindE}, // trail C: u1 -> c2
		},
	)

	rs := mustExec(t, snap, `MATCH (u:User)-[:E*1..]->(c:Computer) WITH COUNT(c) AS cnt, COUNT(DISTINCT c) AS cntDistinct RETURN cnt, cntDistinct`, generousBudget)

	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rs.Rows))
	}
	row := rs.Rows[0]
	if row[0].Kind != OutScalar || row[0].Scalar != float64(3) {
		t.Fatalf("cnt = %+v, want OutScalar(3)", row[0])
	}
	if row[1].Kind != OutScalar || row[1].Scalar != float64(2) {
		t.Fatalf("cntDistinct = %+v, want OutScalar(2)", row[1])
	}
}

// --- empty-group edge cases --------------------------------------------------

// TestPipelineEmptyGroupWithGroupKeysYieldsZeroRows pins SQL's GROUP BY rule:
// aggregating over zero matched-and-filtered input rows with a non-empty
// GroupKeys list produces zero groups, hence zero output rows.
func TestPipelineEmptyGroupWithGroupKeysYieldsZeroRows(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{"objectid": "present"}},
		},
		nil,
	)

	rs := mustExec(t, snap, `MATCH (u:User) WHERE u.objectid = 'nonexistent' WITH u, COUNT(u) AS cnt RETURN u, cnt`, generousBudget)

	if len(rs.Rows) != 0 {
		t.Fatalf("got %d rows, want 0 (zero input rows, non-empty GroupKeys -> zero groups): %v", len(rs.Rows), rowKeys(rs.Rows))
	}
}

// TestPipelineEmptyGroupNoGroupKeysYieldsOneRow pins SQL's other aggregation
// rule: an aggregate with NO group keys at all (the COLLECT anti-join's own
// `WITH COLLECT(s) AS exclude` shape) produces exactly ONE row over zero
// input rows, not zero -- so a WHERE that matches nothing upstream still
// yields an (empty) collected set downstream, not a vanished antijoin.
func TestPipelineEmptyGroupNoGroupKeysYieldsOneRow(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindComputer snapshot.KindID = 2
	)

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindComputer: "Computer"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{"objectid": "present"}},
			{201, []snapshot.KindID{kindComputer}, nil},
			{202, []snapshot.KindID{kindComputer}, nil},
		},
		nil,
	)

	query := `MATCH (s:User) WHERE s.objectid = 'nonexistent'
WITH COLLECT(s) AS exclude
MATCH (c:Computer)
WHERE NOT c IN exclude
RETURN c`

	rs := mustExec(t, snap, query, generousBudget)

	c1, _ := snap.Dense(201)
	c2, _ := snap.Dense(202)
	want := []string{
		rowKey([]OutVal{{Kind: OutNode, Node: c1}}),
		rowKey([]OutVal{{Kind: OutNode, Node: c2}}),
	}
	assertRowSet(t, rs, want)
}

// --- work accounting: one canonical charge per served row -------------------

// TestPipelineWorkAccountingSinglePartNoDoubleCharge pins the exact
// Budgets.MaxWork unit count for a trivial single-Part query with a WHERE
// clause, exercising the exact double-charge this test guards against: a row
// that survives filterRows and is then admitted as a final row must be
// charged exactly once for that (via workMeter.addFinalRow), not once more
// by filterRows itself for surviving the predicate.
//
// The snapshot has 3 User nodes; scanAnchor's own two-tier accounting
// (Budgets' doc comment: "+1 per node visited ... +1 per row produced
// anywhere") charges every visited node 1, and only candidates that BECOME
// rows a second 1. `WHERE n.enabled = true` is a pushed single-symbol
// predicate, evaluated at admit time (scanAnchorVisit's predicatesAdmit):
// nodes 1 and 2 pass and produce rows (2 each = 4), node 3 fails and costs
// only its visit (1) -- it never becomes a row, so under the documented
// model it earns no row charge. Anchor total: 5. The 2 surviving rows are
// admitted as final rows, charged exactly once each by addFinalRow = 2.
// Total: 5 + 2 = 7. (Historical totals for this query: 10 when filterRows
// double-charged survivors, 8 when the double charge was removed but
// predicates still ran only in filterRows so the failing node was charged
// as a row it never needed to be.)
func TestPipelineWorkAccountingSinglePartNoDoubleCharge(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{"enabled": true}},
			{2, []snapshot.KindID{kindUser}, map[string]any{"enabled": true}},
			{3, []snapshot.KindID{kindUser}, map[string]any{"enabled": false}},
		},
		nil,
	)

	q := planQuery(t, snap, `MATCH (n:User) WHERE n.enabled = true RETURN n`)
	meter := &workMeter{budget: Budgets{MaxRows: 1000, MaxWork: 1_000_000}}
	rs, err := runQuery(&Env{Snap: snap}, q, meter)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rs.Rows))
	}
	if meter.work != 7 {
		t.Fatalf("meter.work = %d, want 7 (5 scanAnchor with admit-time predicate filtering + 2 addFinalRow)", meter.work)
	}
	if meter.finalRows != 2 {
		t.Fatalf("meter.finalRows = %d, want 2", meter.finalRows)
	}
}

// TestPipelineWorkAccountingCarriedPartNoDoubleCharge pins the exact
// Budgets.MaxWork unit count for a 2-Part WITH query whose Part[1] has its
// own real pattern (not the empty-pattern short circuit), exercising
// runCarriedPart's own merge step -- the other half of this fix, alongside
// filterRows.
//
// The snapshot has 3 User nodes: node 1 is Part[0]'s sole match (its
// objectid anchor -- see TestExecObjectIDAnchor -- visits exactly that one
// node, which also passes its own pushed predicate: 1 visit + 1 produced =
// 2). `WITH n` passes that single row through unchanged
// (runWithPassThrough's own, unrelated per-row charge: 1). Part[1]
// (`MATCH (m:User)`) re-scans all 3 User nodes for that one carried row;
// `WHERE m.flag = true` is a pushed single-symbol predicate evaluated at
// admit time, so node 2 passes and produces a row (2) while nodes 1 and 3
// fail and cost only their visits (1 each) -- anchor total 4. The merge
// step charges nothing of its own, and the single surviving merged row is
// admitted once as a final row (addFinalRow: 1). Total: 2 + 1 + 4 + 1 = 8.
// (Historical totals for this query: 15 with the filterRows/runCarriedPart
// double charges, 10 with those removed but predicates still evaluated
// only in filterRows, so the two failing nodes were charged as rows.)
func TestPipelineWorkAccountingCarriedPartNoDoubleCharge(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{"objectid": "anchor"}},
			{2, []snapshot.KindID{kindUser}, map[string]any{"flag": true}},
			{3, []snapshot.KindID{kindUser}, map[string]any{"flag": false}},
		},
		nil,
	)

	q := planQuery(t, snap, `MATCH (n:User) WHERE n.objectid = 'anchor' WITH n MATCH (m:User) WHERE m.flag = true RETURN n, m`)
	meter := &workMeter{budget: Budgets{MaxRows: 1000, MaxWork: 1_000_000}}
	rs, err := runQuery(&Env{Snap: snap}, q, meter)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rs.Rows))
	}
	if meter.work != 8 {
		t.Fatalf("meter.work = %d, want 8 (2 anchor + 1 WITH pass-through + 4 Part[1] scan with admit-time predicate filtering + 1 addFinalRow)", meter.work)
	}
	if meter.finalRows != 1 {
		t.Fatalf("meter.finalRows = %d, want 1", meter.finalRows)
	}
}

// --- LIMIT early termination -------------------------------------------------

// TestLimitTarget pins limitTarget's exact eligibility/target arithmetic in
// isolation, ahead of any behavioral (runQuery-level) test.
func TestLimitTarget(t *testing.T) {
	tests := []struct {
		name string
		q    *Query
		want int64
	}{
		{"no LIMIT written", &Query{Limit: -1}, -1},
		{"LIMIT only", &Query{Limit: 10}, 10},
		{"LIMIT with SKIP", &Query{Limit: 10, Skip: 5}, 15},
		{"LIMIT with ORDER BY", &Query{Limit: 10, Order: []OrderKey{{Symbol: "x"}}}, -1},
		{"LIMIT with RETURN DISTINCT", &Query{Limit: 10, Returning: Projection{Distinct: true}}, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := limitTarget(tc.q); got != tc.want {
				t.Fatalf("limitTarget(%+v) = %d, want %d", tc.q, got, tc.want)
			}
		})
	}
}

// buildManyEnabledUsers builds n User nodes (ids 1..n), each with
// enabled=true, for LIMIT early-termination tests that need a candidate pool
// LARGER than limitChunk (1024): with fewer anchor candidates than one
// chunk, scanAnchorVisit would exhaust the whole anchor set (and produce its
// one, unconditional trailing flush) before the chunk-boundary check inside
// runComponentLimited ever runs even once -- no early stop, and no work
// reduction, would be observable at all. This is deliberately NOT the small
// fixture size the rest of this file's tests use.
func buildManyEnabledUsers(t *testing.T, n int) *snapshot.View {
	t.Helper()
	const kindUser snapshot.KindID = 1
	nodes := make([]execNodeSpec, n)
	for i := 0; i < n; i++ {
		nodes[i] = execNodeSpec{uint64(i + 1), []snapshot.KindID{kindUser}, map[string]any{"enabled": true}}
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{kindUser: "User"}, nodes, nil)
}

// TestLimitEarlyTermination is this feature's basic case: every anchor
// candidate survives WHERE, so an unlimited run over the full pool and a
// LIMIT 3 run both return real work-vs-row-count numbers to compare -- the
// LIMIT run must stop scanning once 3 post-Where rows exist, spending
// strictly less meter.work than the unlimited baseline measured for the
// identical query (minus LIMIT) run first.
func TestLimitEarlyTermination(t *testing.T) {
	const n = 3000
	snap := buildManyEnabledUsers(t, n)

	baseline := &workMeter{budget: generousBudget}
	baseRS, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) WHERE n.enabled = true RETURN n`), baseline)
	if err != nil {
		t.Fatalf("baseline runQuery: %v", err)
	}
	if len(baseRS.Rows) != n {
		t.Fatalf("baseline row count = %d, want %d (sanity: every node matches WHERE)", len(baseRS.Rows), n)
	}

	limited := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) WHERE n.enabled = true RETURN n LIMIT 3`), limited)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (LIMIT 3)", len(rs.Rows))
	}
	if limited.work >= baseline.work {
		t.Fatalf("meter.work = %d, want strictly below the full-scan figure %d", limited.work, baseline.work)
	}
}

// TestLimitEarlyTerminationSkipCountsTowardTarget pins limitTarget's
// Skip+Limit arithmetic end to end: over a `SKIP 2 LIMIT 3` query it checks
// both that exactly 3 rows come back (SKIP+LIMIT applied correctly on top
// of the chunked path, exactly like the unlimited path already produces)
// and that real early termination happened (meter.work strictly below an
// unlimited baseline's over the same 3000-row fixture) rather than a full
// scan. It does NOT, despite appearances, distinguish a driver that counts
// only LIMIT (3) toward the chunked target from one that correctly counts
// SKIP+LIMIT (5): limitChunk is 1024, far larger than either quantity, and
// every row in this fixture matches WHERE, so the very first chunk already
// gathers well more than either target and the driver stops after that one
// chunk regardless of which of the two it was aiming for -- a genuine
// LIMIT-only bug would need a fixture where the two targets straddle a
// chunk boundary to be caught here.
func TestLimitEarlyTerminationSkipCountsTowardTarget(t *testing.T) {
	const n = 3000
	snap := buildManyEnabledUsers(t, n)

	baseline := &workMeter{budget: generousBudget}
	if _, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) WHERE n.enabled = true RETURN n`), baseline); err != nil {
		t.Fatalf("baseline runQuery: %v", err)
	}

	limited := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) WHERE n.enabled = true RETURN n SKIP 2 LIMIT 3`), limited)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (SKIP 2 LIMIT 3 over %d uniformly-matching rows)", len(rs.Rows), n)
	}
	if limited.work >= baseline.work {
		t.Fatalf("meter.work = %d, want strictly below the full-scan figure %d (SKIP must count toward the early-termination target)", limited.work, baseline.work)
	}
}

// TestLimitEarlyTerminationSparseMatchesDoesNotUnderReturn is the regression
// test for expand.go's own documented shortestPath trap, applied to this
// driver: counting PRE-filter anchor rows (or any other pre-Where quantity)
// toward the target under-returns whenever most anchor candidates fail
// WHERE. 2000 nodes -- more than one limitChunk (1024) batch's worth -- but
// only the LAST 5 (highest ids, visited last: scanAnchorVisit's full-range
// branch iterates ascending dense ids) satisfy WHERE. The first full batch
// (ids 1..1024) is entirely filtered away (zero survivors); a driver that
// wrongly counted the 1024 scanned anchor rows themselves against the
// target would conclude "enough" right there and stop with zero result
// rows, never reaching the batch the 5 real matches live in.
//
// The node pattern here is deliberately unconstrained (`(n)`, no label at
// all), so the anchor scan runs through scanAnchorVisit's full-range/
// default candidate source -- this package's other LIMIT early-termination
// tests all anchor on a `:User` kind bitmap instead, so this is also this
// suite's one exercise of that fourth candidate-source branch.
func TestLimitEarlyTerminationSparseMatchesDoesNotUnderReturn(t *testing.T) {
	const n = 2000
	const sparseSurvivors = 5

	nodes := make([]execNodeSpec, n)
	for i := 0; i < n; i++ {
		nodes[i] = execNodeSpec{uint64(i + 1), nil, map[string]any{"flag": i >= n-sparseSurvivors}}
	}
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{}, nodes, nil)

	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n) WHERE n.flag = true RETURN n LIMIT 3`), &workMeter{budget: generousBudget})
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (LIMIT 3 over %d sparse survivors out of %d candidates): %v", len(rs.Rows), sparseSurvivors, n, rowKeys(rs.Rows))
	}
}

// TestLimitWithOrderByTakesFullPath is the negative-eligibility counterpart
// to the above: ORDER BY makes limitTarget return -1 (this package's own
// deterministic pre-sort materialization order is not the order sorting
// would produce, so an early-stopped scan could keep the wrong rows), and an
// ineligible query must take EXACTLY the pre-existing, unlimited code path
// -- asserted here as meter.work being not merely "close to" but IDENTICAL
// between an ORDER BY/LIMIT run and the same MATCH/RETURN with neither.
func TestLimitWithOrderByTakesFullPath(t *testing.T) {
	const kindUser snapshot.KindID = 1
	nodes := make([]execNodeSpec, 10)
	for i := 0; i < 10; i++ {
		nodes[i] = execNodeSpec{uint64(i + 1), []snapshot.KindID{kindUser}, nil}
	}
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{kindUser: "User"}, nodes, nil)

	baseline := &workMeter{budget: generousBudget}
	if _, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) RETURN n, id(n) AS rid`), baseline); err != nil {
		t.Fatalf("baseline runQuery: %v", err)
	}

	limited := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) RETURN n, id(n) AS rid ORDER BY rid LIMIT 3`), limited)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (LIMIT 3)", len(rs.Rows))
	}
	if limited.work != baseline.work {
		t.Fatalf("meter.work = %d, want %d (ORDER BY must take the unlimited path unchanged)", limited.work, baseline.work)
	}
}

// TestLimitWithDistinctTakesFullPath is TestLimitWithOrderByTakesFullPath's
// RETURN DISTINCT counterpart: limitTarget returns -1 because DISTINCT's own
// dedup counts deduped PROJECTED rows, a quantity this pre-projection driver
// cannot track, so the whole query must fall through to the unchanged
// unlimited path.
func TestLimitWithDistinctTakesFullPath(t *testing.T) {
	const (
		kindUser  snapshot.KindID = 1
		kindGroup snapshot.KindID = 2
		kindE     snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindGroup: "Group", kindE: "E"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, nil},
			{2, []snapshot.KindID{kindUser}, nil},
			{3, []snapshot.KindID{kindUser}, nil},
			{500, []snapshot.KindID{kindGroup}, nil},
		},
		[]execEdgeSpec{
			{9000, 1, 500, kindE},
			{9001, 2, 500, kindE},
			{9002, 3, 500, kindE},
		},
	)

	baseline := &workMeter{budget: generousBudget}
	if _, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (a:User)-[:E]->(b:Group) RETURN b`), baseline); err != nil {
		t.Fatalf("baseline runQuery: %v", err)
	}

	limited := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (a:User)-[:E]->(b:Group) RETURN DISTINCT b LIMIT 1`), limited)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (RETURN DISTINCT ... LIMIT 1 over 3 rows all sharing one group)", len(rs.Rows))
	}
	if limited.work != baseline.work {
		t.Fatalf("meter.work = %d, want %d (RETURN DISTINCT must take the unlimited path unchanged)", limited.work, baseline.work)
	}
}

// TestLimitEarlyTerminationCarriedPart exercises the OTHER wiring point
// (runQuery's seed loop, Part[1] after a WITH boundary): Stage 0's own
// aggregate (COUNT) always runs to completion first (this package's own
// documented rule -- aggregates only ever attach to Part[0]'s WITH, and
// carrying that stage's own row set forward is unaffected by any LIMIT on
// the final RETURN), grouping into 3 carried rows (one per Group). Stage 1's
// own pattern (`MATCH (m:User)`) is then re-run once per carried row on the
// unlimited path -- the seed loop here must instead stop AFTER however many
// of those 3 seeds are actually needed to secure LIMIT 2's target, skipping
// the rest entirely (each seed's own Part[1] scan touches a large,
// independent User pool, so skipping even one is a measurable saving).
func TestLimitEarlyTerminationCarriedPart(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindGroup    snapshot.KindID = 2
		kindMemberOf snapshot.KindID = 10
	)

	var nodes []execNodeSpec
	var edges []execEdgeSpec
	// 3 groups, each with exactly one member -- enough for WITH's per-group
	// COUNT(x) to have something to count; the count value itself is not
	// what this test is about.
	// buildExecSnapshot's underlying builder requires strictly increasing
	// database ids across the whole AddNode sequence, so every group is
	// added before any member (rather than interleaved group/member pairs).
	for g := 1; g <= 3; g++ {
		nodes = append(nodes, execNodeSpec{uint64(1000 + g), []snapshot.KindID{kindGroup}, nil})
	}
	for g := 1; g <= 3; g++ {
		groupID := uint64(1000 + g)
		memberID := uint64(2000 + g)
		nodes = append(nodes, execNodeSpec{memberID, []snapshot.KindID{kindUser}, nil})
		edges = append(edges, execEdgeSpec{uint64(9_000_000 + g), memberID, groupID, kindMemberOf})
	}
	// A pool of unrelated Users that Part[1]'s own `MATCH (m:User)` re-scans
	// once PER carried seed on the unlimited path -- large enough that
	// skipping two of the three seeds (this test's whole point) is a
	// measurable saving, without needing anywhere near limitChunk (1024)
	// candidates: the saving here comes from skipping whole seeds, not from
	// a within-seed chunk boundary.
	const plainUsers = 300
	for i := 0; i < plainUsers; i++ {
		nodes = append(nodes, execNodeSpec{uint64(4000 + i), []snapshot.KindID{kindUser}, nil})
	}

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindGroup: "Group", kindMemberOf: "MemberOf"},
		nodes, edges,
	)

	query := `MATCH (x:User)-[:MemberOf]->(g:Group)
WITH g, count(x) AS c
MATCH (m:User)
RETURN g, m, c`

	baseline := &workMeter{budget: generousBudget}
	if _, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query), baseline); err != nil {
		t.Fatalf("baseline runQuery: %v", err)
	}

	limited := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query+"\nLIMIT 2"), limited)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want 2 (LIMIT 2)", len(rs.Rows))
	}
	if limited.work >= baseline.work {
		t.Fatalf("meter.work = %d, want strictly below the full-scan figure %d (stage 1's seed loop must stop once enough seeds are processed, not run all 3)", limited.work, baseline.work)
	}
}

// TestLimitEarlyTerminationVarLength exercises the chunked driver's
// single-var-length-step dispatch (expandVarLengthComponentFrom, via
// runComponentFrom/componentAnchorSym): a large `:User` anchor pool, each
// with a one-hop MemberOf trail (satisfying `*1..2`) to a single shared
// Group, so a LIMIT 3 run should stop after roughly one anchor-scan batch
// instead of walking every user's own trail.
func TestLimitEarlyTerminationVarLength(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindGroup    snapshot.KindID = 2
		kindMemberOf snapshot.KindID = 10
	)
	const n = 2000
	const groupID = uint64(1)

	nodes := []execNodeSpec{{groupID, []snapshot.KindID{kindGroup}, nil}}
	var edges []execEdgeSpec
	for i := 0; i < n; i++ {
		userID := uint64(1000 + i)
		nodes = append(nodes, execNodeSpec{userID, []snapshot.KindID{kindUser}, nil})
		edges = append(edges, execEdgeSpec{uint64(9_000_000 + i), userID, groupID, kindMemberOf})
	}
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{kindUser: "User", kindGroup: "Group", kindMemberOf: "MemberOf"}, nodes, edges)

	query := `MATCH (a:User)-[:MemberOf*1..2]->(b:Group) RETURN a`

	baseline := &workMeter{budget: generousBudget}
	baseRS, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query), baseline)
	if err != nil {
		t.Fatalf("baseline runQuery: %v", err)
	}
	if len(baseRS.Rows) != n {
		t.Fatalf("baseline row count = %d, want %d (sanity: every user has exactly one qualifying trail)", len(baseRS.Rows), n)
	}

	limited := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query+" LIMIT 3"), limited)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (LIMIT 3)", len(rs.Rows))
	}
	if limited.work >= baseline.work {
		t.Fatalf("meter.work = %d, want strictly below the full-scan figure %d", limited.work, baseline.work)
	}
}

// TestLimitEarlyTerminationNamedPathChain exercises the chunked driver's
// named-path/chain dispatch end to end: componentAnchorSym's pathSym != ""
// branch picks the chain's own leftmost symbol as anchor (rather than
// chooseAnchor's cost-ranked pick), and runComponentFrom's own pathSym != ""
// branch (guarded by isStrictLinearChain) routes through
// expandChainComponentFrom -- the same anchor/dispatch pairing
// TestRunComponentFromDispatchMatchesRunComponent's "named-path chain"
// subtest exercises directly, here driven through the full runQuery/LIMIT
// path instead. A large :User anchor pool, each with a one-hop MemberOf
// edge to a single shared Group, all under one named path `p`: a LIMIT 3
// run must stop after roughly one anchor-scan batch instead of assembling
// every user's own path, while every returned path must still be one that
// the unlimited run itself produces.
func TestLimitEarlyTerminationNamedPathChain(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindGroup    snapshot.KindID = 2
		kindMemberOf snapshot.KindID = 10
	)
	const n = 2000
	const groupID = uint64(1)

	nodes := []execNodeSpec{{groupID, []snapshot.KindID{kindGroup}, nil}}
	var edges []execEdgeSpec
	for i := 0; i < n; i++ {
		userID := uint64(1000 + i)
		nodes = append(nodes, execNodeSpec{userID, []snapshot.KindID{kindUser}, nil})
		edges = append(edges, execEdgeSpec{uint64(9_000_000 + i), userID, groupID, kindMemberOf})
	}
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{kindUser: "User", kindGroup: "Group", kindMemberOf: "MemberOf"}, nodes, edges)

	const query = `MATCH p = (a:User)-[:MemberOf]->(b:Group) RETURN p`

	baseline := &workMeter{budget: generousBudget}
	baseRS, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query), baseline)
	if err != nil {
		t.Fatalf("baseline runQuery: %v", err)
	}
	if len(baseRS.Rows) != n {
		t.Fatalf("baseline row count = %d, want %d (sanity: every user has exactly one qualifying MemberOf edge)", len(baseRS.Rows), n)
	}
	wantSigs := make(map[string]bool, n)
	for _, sig := range pathSigsAtColumn(t, snap, baseRS, 0) {
		wantSigs[sig] = true
	}

	limited := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, query+" LIMIT 3"), limited)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (LIMIT 3)", len(rs.Rows))
	}
	for _, sig := range pathSigsAtColumn(t, snap, rs, 0) {
		if !wantSigs[sig] {
			t.Fatalf("limited path %q is not among the unlimited run's own %d paths", sig, n)
		}
	}
	if limited.work >= baseline.work {
		t.Fatalf("meter.work = %d, want strictly below the full-scan figure %d", limited.work, baseline.work)
	}
}

// --- shortestPath LIMIT pushdown scoped to single-part queries (C1) --------

// TestShortestPathLimitPushdownNotAppliedAcrossWithBoundary_Count: the final
// RETURN's own LIMIT must never be pushed into a shortestPath component that
// lives in Part[0] of a two-Part (WITH) query. Part[0]'s own row count and
// the whole query's final row count are different quantities once a WITH
// stage groups/aggregates between them, so capping Part[0]'s enumeration to
// the FINAL limit is a correctness bug, not just a missed optimization.
//
// Fixture: 5 Root nodes, each one hop from a single shared Target node --
// shortestPath(s,t) produces one dense (s,t) pair per Root, five total.
// `WITH t, COUNT(s) AS n RETURN n LIMIT 1` groups all five pairs into one
// row (n=5): the final LIMIT 1 caps the WITH-stage's own OUTPUT (one group)
// to one row, not the number of (s,t) pairs Part[0]'s shortestPath produces
// on the way there. runQuery threads the final LIMIT's target (here, 1)
// onto the meter unconditionally, and expandShortestPathComponent's own
// pushdown reads it regardless of which Part it is serving -- for a 2-Part
// query that caps Part[0]'s traverse.Query.Limit to 1 too, so only ONE of
// the five (s,t) pairs is ever produced and n comes out 1, a row PostgreSQL
// could never have returned (a raw COUNT can never come out lower than the
// number of things being counted).
func TestShortestPathLimitPushdownNotAppliedAcrossWithBoundary_Count(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"}
	var nodes []execNodeSpec
	var edges []execEdgeSpec
	for i := uint64(1); i <= 5; i++ {
		nodes = append(nodes, execNodeSpec{i, []snapshot.KindID{kindRoot}, nil})
		edges = append(edges, execEdgeSpec{i, i, 10, kindE})
	}
	nodes = append(nodes, execNodeSpec{10, []snapshot.KindID{kindTarget}, nil})
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	rs := mustExec(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WITH t, COUNT(s) AS n RETURN n LIMIT 1`,
		generousBudget)

	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (LIMIT 1 applies to the WITH-stage's own single group)", len(rs.Rows))
	}
	got := rs.Rows[0][0]
	if got.Kind != OutScalar || got.Scalar != float64(5) {
		t.Fatalf("n = %+v, want OutScalar(5) -- all 5 Root->Target pairs counted, not truncated by the final LIMIT", got)
	}
}

// TestShortestPathLimitPushdownNotAppliedAcrossWithBoundary_UnderServe: same
// bug as the COUNT case above, but observed as silently missing rows instead
// of a too-small aggregate. Fixture: 5 Root nodes (s1..s5) each one hop from
// a shared Target; three of the five (s3, s4, s5 -- the highest dense ids,
// so enumerated LAST by traverse's ascending-dense-id order) have a matching
// :Other node by name, the other two (s1, s2) do not. `WITH s MATCH
// (u:Other) WHERE u.name = s.name RETURN u LIMIT 3` carries every s forward,
// joins each against Other by name, and its own LIMIT 3 happens to be large
// enough to admit every genuine match (there are exactly 3): the correct
// result is 3 rows. Before the C1 fix, the final LIMIT 3 leaks into Part[0]'s
// shortestPath pushdown and caps traverse's enumeration to the first 3
// dense-ascending (s,t) pairs (s1, s2, s3) -- discarding s4 and s5 before
// Part[1] ever runs -- so only s3's match survives the join and the query
// under-serves.
func TestShortestPathLimitPushdownNotAppliedAcrossWithBoundary_UnderServe(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindOther  snapshot.KindID = 3
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindOther: "Other", kindE: "E"}
	var nodes []execNodeSpec
	var edges []execEdgeSpec
	for i := uint64(1); i <= 5; i++ {
		nodes = append(nodes, execNodeSpec{i, []snapshot.KindID{kindRoot}, map[string]any{"name": fmt.Sprintf("s%d", i)}})
		edges = append(edges, execEdgeSpec{i, i, 10, kindE})
	}
	nodes = append(nodes, execNodeSpec{10, []snapshot.KindID{kindTarget}, nil})
	// Other nodes matching s3, s4, s5 by name; s1/s2 have no match.
	for _, i := range []uint64{3, 4, 5} {
		nodes = append(nodes, execNodeSpec{200 + i, []snapshot.KindID{kindOther}, map[string]any{"name": fmt.Sprintf("s%d", i)}})
	}
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	rs := mustExec(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WITH s MATCH (u:Other) WHERE u.name = s.name RETURN u LIMIT 3`,
		generousBudget)

	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3 (s3/s4/s5's matches, not truncated to s1/s2/s3 by the leaked final LIMIT)", len(rs.Rows))
	}
}
