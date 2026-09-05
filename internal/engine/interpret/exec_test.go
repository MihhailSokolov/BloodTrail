// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// --- test helpers: snapshot construction + query execution ------------------

type execNodeSpec struct {
	id    uint64
	kinds []snapshot.KindID
	props map[string]any
}

type execEdgeSpec struct {
	id         uint64
	start, end uint64
	kind       snapshot.KindID
}

// buildExecSnapshot builds a hand-crafted snapshot for one test scenario.
func buildExecSnapshot(t *testing.T, kindTable map[snapshot.KindID]string, nodes []execNodeSpec, edges []execEdgeSpec) *snapshot.Snapshot {
	t.Helper()

	b := snapshot.NewBuilder(1)
	b.SetKinds(kindTable)

	for _, n := range nodes {
		propsJSON, err := json.Marshal(n.props)
		if err != nil {
			t.Fatalf("marshal props for node %d: %v", n.id, err)
		}
		if err := b.AddNode(n.id, n.kinds, propsJSON); err != nil {
			t.Fatalf("AddNode(%d): %v", n.id, err)
		}
	}
	for _, e := range edges {
		b.AddEdge(e.id, e.start, e.end, e.kind)
	}

	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snap
}

// planQuery parses and plans query against snap, failing the test if either
// step does not succeed (every test in this file uses queries this
// milestone's planner is expected to serve).
func planQuery(t *testing.T, snap *snapshot.Snapshot, query string) *Query {
	t.Helper()
	rq, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		t.Fatalf("ParseCypher(%q): %v", query, err)
	}
	q, ok := Plan(rq, snap)
	if !ok {
		t.Fatalf("Plan(%q): not served", query)
	}
	return q
}

// mustExec plans and executes query, failing the test on any error.
func mustExec(t *testing.T, snap *snapshot.Snapshot, query string, b Budgets) *ResultSet {
	t.Helper()
	q := planQuery(t, snap, query)
	rs, err := Execute(&Env{Snap: snap}, q, b)
	if err != nil {
		t.Fatalf("Execute(%q): %v", query, err)
	}
	return rs
}

// generousBudget is large enough to never trip for this file's small
// fixtures, for tests that are not themselves about budgets.
var generousBudget = Budgets{MaxRows: 10000, MaxWork: 10_000_000}

// rowKey renders one result row as a comparable string, for set-equality
// assertions over queries whose row order this package does not promise.
func rowKey(row []OutVal) string {
	s := ""
	for _, v := range row {
		switch v.Kind {
		case OutNode:
			s += fmt.Sprintf("N%d|", v.Node)
		case OutEdge:
			s += fmt.Sprintf("E%d|", v.Edge.Fwd)
		case OutPath:
			s += "P|"
		case OutScalar:
			s += fmt.Sprintf("S%v|", v.Scalar)
		}
	}
	return s
}

func rowKeys(rows [][]OutVal) []string {
	keys := make([]string, len(rows))
	for i, r := range rows {
		keys[i] = rowKey(r)
	}
	sort.Strings(keys)
	return keys
}

func assertRowSet(t *testing.T, rs *ResultSet, want []string) {
	t.Helper()
	got := rowKeys(rs.Rows)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("row set mismatch\ngot:  %v\nwant: %v", got, want)
		}
	}
}

// --- 2-hop chain with edge-kind disjunction ---------------------------------

func TestExecTwoHopChainEdgeKindDisjunction(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindGroup    snapshot.KindID = 2
		kindComputer snapshot.KindID = 3
		kindMemberOf snapshot.KindID = 10
		kindAdminTo  snapshot.KindID = 11
		kindOwns     snapshot.KindID = 12
		kindSession  snapshot.KindID = 13
	)

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{
			kindUser: "User", kindGroup: "Group", kindComputer: "Computer",
			kindMemberOf: "MemberOf", kindAdminTo: "AdminTo", kindOwns: "Owns", kindSession: "HasSession",
		},
		[]execNodeSpec{
			{100, []snapshot.KindID{kindUser}, nil},     // a
			{200, []snapshot.KindID{kindGroup}, nil},    // b (correct target, via MemberOf)
			{201, []snapshot.KindID{kindGroup}, nil},    // decoy target, via Owns (wrong kind)
			{300, []snapshot.KindID{kindComputer}, nil}, // c
		},
		[]execEdgeSpec{
			{1000, 100, 200, kindMemberOf}, // a --MemberOf--> b   (matches disjunction)
			{1001, 100, 201, kindOwns},     // a --Owns--> decoy   (wrong kind, must be excluded)
			{1002, 200, 300, kindSession},  // b --HasSession--> c
			{1003, 201, 300, kindSession},  // decoy --HasSession--> c (proves hop-1 kind filter, not hop-2 absence, excludes decoy)
		},
	)

	rs := mustExec(t, snap, `MATCH (a:User)-[:MemberOf|AdminTo]->(b:Group)-[:HasSession]->(c:Computer) RETURN a,b,c`, generousBudget)

	if want := []string{"a", "b", "c"}; !equalStrings(rs.Keys, want) {
		t.Fatalf("Keys = %v, want %v", rs.Keys, want)
	}

	a, _ := snap.Dense(100)
	b, _ := snap.Dense(200)
	c, _ := snap.Dense(300)
	want := rowKey([]OutVal{{Kind: OutNode, Node: a}, {Kind: OutNode, Node: b}, {Kind: OutNode, Node: c}})
	assertRowSet(t, rs, []string{want})
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- multi-pattern join on a shared symbol ----------------------------------

func TestExecMultiPatternJoinOnSharedSymbol(t *testing.T) {
	const (
		kindA snapshot.KindID = 1
		kindC snapshot.KindID = 2
		kindB snapshot.KindID = 3
		kindE snapshot.KindID = 10
	)

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindA: "TypeA", kindC: "TypeC", kindB: "TypeB", kindE: "E"},
		[]execNodeSpec{
			{10, []snapshot.KindID{kindA}, nil}, // a1 -> b1 (joins with c1)
			{11, []snapshot.KindID{kindA}, nil}, // a2 -> b2 (no TypeC predecessor of b2)
			{20, []snapshot.KindID{kindC}, nil}, // c1 -> b1
			{21, []snapshot.KindID{kindC}, nil}, // c2, no edges at all
			{30, []snapshot.KindID{kindB}, nil}, // b1
			{31, []snapshot.KindID{kindB}, nil}, // b2
		},
		[]execEdgeSpec{
			{2000, 10, 30, kindE}, // a1 -> b1
			{2001, 11, 31, kindE}, // a2 -> b2
			{2002, 20, 30, kindE}, // c1 -> b1
		},
	)

	rs := mustExec(t, snap, `MATCH (a:TypeA)-[:E]->(b), (c:TypeC)-[:E]->(b) RETURN a,b,c`, generousBudget)

	a1, _ := snap.Dense(10)
	b1, _ := snap.Dense(30)
	c1, _ := snap.Dense(20)
	want := rowKey([]OutVal{{Kind: OutNode, Node: a1}, {Kind: OutNode, Node: b1}, {Kind: OutNode, Node: c1}})
	assertRowSet(t, rs, []string{want})
}

// --- objectid anchor hits the index ------------------------------------------

// manyUsersSnapshot builds n decoy User nodes (ids 1..n) plus a distinguished
// target node with a matching objectid, for testing that an id()/objectid
// anchor visits O(1) nodes rather than scanning every decoy: a tiny MaxWork
// budget that a full scan could never satisfy still succeeds when the
// anchor is actually used.
func manyUsersSnapshot(t *testing.T, n int) (snap *snapshot.Snapshot, targetDBID uint64) {
	t.Helper()
	const kindUser snapshot.KindID = 1

	var nodes []execNodeSpec
	for i := 1; i <= n; i++ {
		nodes = append(nodes, execNodeSpec{uint64(i), []snapshot.KindID{kindUser}, nil})
	}
	targetDBID = uint64(n + 1)
	nodes = append(nodes, execNodeSpec{targetDBID, []snapshot.KindID{kindUser}, map[string]any{"objectid": "target-oid"}})

	snap = buildExecSnapshot(t, map[snapshot.KindID]string{kindUser: "User"}, nodes, nil)
	return snap, targetDBID
}

func TestExecObjectIDAnchor(t *testing.T) {
	snap, targetDBID := manyUsersSnapshot(t, 50)

	// A tiny work budget that a 51-node full scan could never satisfy still
	// succeeds: proof the objectid anchor (not a kind-bitmap scan) is what
	// actually ran.
	rs, err := Execute(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) WHERE n.objectid = 'target-oid' RETURN n`), Budgets{MaxRows: 10, MaxWork: 10})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	want, _ := snap.Dense(targetDBID)
	assertRowSet(t, rs, []string{rowKey([]OutVal{{Kind: OutNode, Node: want}})})
}

// --- budgets: MaxRows -------------------------------------------------------

func TestExecMaxRowsBudget(t *testing.T) {
	const (
		kindUser  snapshot.KindID = 1
		kindGroup snapshot.KindID = 2
		kindE     snapshot.KindID = 10
	)

	var nodes []execNodeSpec
	var edges []execEdgeSpec
	for i := 1; i <= 5; i++ {
		nodes = append(nodes, execNodeSpec{uint64(i), []snapshot.KindID{kindUser}, nil})
		edges = append(edges, execEdgeSpec{uint64(1000 + i), uint64(i), 100, kindE})
	}
	nodes = append(nodes, execNodeSpec{100, []snapshot.KindID{kindGroup}, nil})

	snap := buildExecSnapshot(t, map[snapshot.KindID]string{kindUser: "User", kindGroup: "Group", kindE: "E"}, nodes, edges)

	query := `MATCH (a:User)-[:E]->(b:Group) RETURN a,b`

	// Sanity: under a generous budget the query really does produce 5 rows.
	rs := mustExec(t, snap, query, generousBudget)
	if len(rs.Rows) != 5 {
		t.Fatalf("sanity check: got %d rows, want 5", len(rs.Rows))
	}

	_, err := Execute(&Env{Snap: snap}, planQuery(t, snap, query), Budgets{MaxRows: 2, MaxWork: 1_000_000})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("Execute with MaxRows=2: err = %v, want ErrBudget", err)
	}
}

// --- budgets: MaxWork --------------------------------------------------------

func TestExecMaxWorkBudget(t *testing.T) {
	snap, _ := manyUsersSnapshot(t, 50)

	_, err := Execute(&Env{Snap: snap}, planQuery(t, snap, `MATCH (n:User) RETURN n`), Budgets{MaxRows: 1000, MaxWork: 3})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("Execute with MaxWork=3 over 51 nodes: err = %v, want ErrBudget", err)
	}
}

// --- undirected fixed step: excludes self, matches both orientations -------

func TestExecUndirectedStepExcludesSelfBothOrientations(t *testing.T) {
	const kindE snapshot.KindID = 10

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindE: "E"},
		[]execNodeSpec{
			{10, nil, nil}, // a
			{20, nil, nil}, // b
			{30, nil, nil}, // s (self-loop)
		},
		[]execEdgeSpec{
			{5000, 10, 20, kindE}, // a -> b
			{5001, 30, 30, kindE}, // s -> s (self-loop; must be excluded)
		},
	)

	rs := mustExec(t, snap, `MATCH (x)-[:E]-(y) RETURN x,y`, generousBudget)

	a, _ := snap.Dense(10)
	b, _ := snap.Dense(20)
	want := []string{
		rowKey([]OutVal{{Kind: OutNode, Node: a}, {Kind: OutNode, Node: b}}),
		rowKey([]OutVal{{Kind: OutNode, Node: b}, {Kind: OutNode, Node: a}}),
	}
	assertRowSet(t, rs, want)
}

// --- property predicate filtering via WHERE ---------------------------------

func TestExecWherePropertyPredicate(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{100, []snapshot.KindID{kindUser}, map[string]any{"flag": true}},
			{200, []snapshot.KindID{kindUser}, map[string]any{"flag": false}},
			{300, []snapshot.KindID{kindUser}, map[string]any{}},
		},
		nil,
	)

	rs := mustExec(t, snap, `MATCH (n:User) WHERE n.flag = true RETURN n`, generousBudget)

	want, _ := snap.Dense(100)
	assertRowSet(t, rs, []string{rowKey([]OutVal{{Kind: OutNode, Node: want}})})
}

// --- RETURN of node + scalar projections ------------------------------------

func TestExecReturnNodeAndScalar(t *testing.T) {
	const kindUser snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User"},
		[]execNodeSpec{
			{100, []snapshot.KindID{kindUser}, map[string]any{"name": "Alice"}},
		},
		nil,
	)

	rs := mustExec(t, snap, `MATCH (n:User) RETURN n, n.name AS name`, generousBudget)

	if want := []string{"n", "name"}; !equalStrings(rs.Keys, want) {
		t.Fatalf("Keys = %v, want %v", rs.Keys, want)
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rs.Rows))
	}
	row := rs.Rows[0]
	dense, _ := snap.Dense(100)
	if row[0].Kind != OutNode || row[0].Node != dense {
		t.Fatalf("row[0] = %+v, want OutNode(%d)", row[0], dense)
	}
	if row[1].Kind != OutScalar || row[1].Scalar != "Alice" {
		t.Fatalf("row[1] = %+v, want OutScalar(\"Alice\")", row[1])
	}
}

// --- bonus: closing-edge cycle (documented-legal shape, not in the brief's
// required list, but explicitly called out in plan.go's declareSymbol doc
// comment as a shape this executor must join by identity rather than reject
// or silently mishandle) ------------------------------------------------------

func TestExecClosingStepCycle(t *testing.T) {
	const kindT snapshot.KindID = 1

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindT: "T"},
		[]execNodeSpec{
			{10, []snapshot.KindID{kindT}, nil}, // p
			{20, []snapshot.KindID{kindT}, nil}, // q
		},
		[]execEdgeSpec{
			{7000, 10, 20, 0}, // p -> q
			{7001, 20, 10, 0}, // q -> p
		},
	)

	rs := mustExec(t, snap, `MATCH (a)-->(b), (b)-->(a) RETURN a,b`, generousBudget)

	p, _ := snap.Dense(10)
	q, _ := snap.Dense(20)
	want := []string{
		rowKey([]OutVal{{Kind: OutNode, Node: p}, {Kind: OutNode, Node: q}}),
		rowKey([]OutVal{{Kind: OutNode, Node: q}, {Kind: OutNode, Node: p}}),
	}
	assertRowSet(t, rs, want)
}
