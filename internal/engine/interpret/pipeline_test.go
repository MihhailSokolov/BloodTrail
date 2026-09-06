// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
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
// component, a shape expand.go's runComponent declines outright regardless
// of this task (task-8-report.md's documented "mixed-Step component" scope
// decline) -- so this test's own second Part uses a plain, single-step
// pattern instead, preserving exactly the COLLECT-anti-join mechanism this
// task owns without depending on a shape a different, already-frozen task
// declined.
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

// --- ORDER BY over a string-valued alias: ErrCollation abort ----------------

func TestPipelineOrderByStringErrCollation(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, map[string]any{"name": "Alice"}},
			{2, []snapshot.KindID{kindUser}, map[string]any{"name": "Bob"}},
		},
		nil,
	)

	q := planQuery(t, snap, `MATCH (n:User) RETURN n.name AS name ORDER BY name`)
	_, err := Execute(&Env{Snap: snap}, q, generousBudget)
	if !errors.Is(err, ErrCollation) {
		t.Fatalf("Execute: err = %v, want ErrCollation", err)
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
