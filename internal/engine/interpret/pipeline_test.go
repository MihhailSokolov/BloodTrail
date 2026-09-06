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
// anywhere") charges 2 work units per node visited by the kind-bitmap anchor
// (1 visit + 1 for the row it produces, all 3 satisfy the trivial :User
// constraint) = 6, regardless of this fix. Of those 3 rows, exactly 2 survive
// `WHERE n.enabled = true` and are admitted as final rows, each charged
// exactly once by addFinalRow = 2. Total: 6 + 2 = 8. Before this fix,
// filterRows' own per-survivor spend(1) added a second charge for each of
// those same 2 survivors, making the (wrong) pre-fix total 10.
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
	if meter.work != 8 {
		t.Fatalf("meter.work = %d, want 8 (6 scanAnchor + 2 addFinalRow, no filterRows double charge)", meter.work)
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
// node: 1 visit + 1 produced = 2). `WITH n` passes that single row through
// unchanged (runWithPassThrough's own, unrelated per-row charge: 1). Part[1]
// (`MATCH (m:User)`) re-scans all 3 User nodes for that one carried row (kind
// -bitmap anchor: 2 work units per node x 3 = 6), merges each of those 3 rows
// with the carried row (runCarriedPart's merge step -- charges nothing of
// its own, per this fix), and `WHERE m.flag = true` keeps exactly 1 of the 3
// merged rows, admitted once as a final row (addFinalRow: 1). Total:
// 2 + 1 + 6 + 1 = 10. Before this fix, runCarriedPart's own per-merged-row
// spend(1) added 3 (one per merged row, regardless of WHERE), and filterRows'
// per-survivor spend(1) added 1 more for the single WHERE survivor, making
// the (wrong) pre-fix total 15.
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
	if meter.work != 10 {
		t.Fatalf("meter.work = %d, want 10 (2 anchor + 1 WITH pass-through + 6 Part[1] scan + 1 addFinalRow, no filterRows/runCarriedPart double charge)", meter.work)
	}
	if meter.finalRows != 1 {
		t.Fatalf("meter.finalRows = %d, want 1", meter.finalRows)
	}
}
