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

// --- undirected same-symbol step: self-loop pattern (n)-[:E]-(n) -----------

// TestExecUndirectedSameSymbolSelfLoop pins two things at once, per the
// review finding that sent us back to dawgs' own SQL:
//
//  1. A literal same-symbol undirected pattern must actually find self-loop
//     edges (it deterministically found zero before this fix, because
//     adjacency's DirectionBoth "other == bound" self-exclusion fired
//     unconditionally, even when bound is the *only* node the step can ever
//     match against).
//
//  2. Multiplicity: exactly ONE row per self-loop edge, not two (once per
//     CSR orientation). This mirrors dawgs@v0.8.0's
//     buildSelfReferentialDirectionlessTraversalRoot (cypher/models/pgsql/
//     translate/traversal_directionless.go), confirmed by generating its
//     actual SQL for `match (u)-[]-(u) return u`:
//
//     with s0 as (select ... from edge e0 join node n0
//     on (n0.id = e0.end_id or n0.id = e0.start_id)
//     where (n0.id = e0.end_id or n0.id = e0.start_id)) ...
//
// -- a single INNER JOIN with one OR-condition shared by the ON and WHERE
// clauses, not a union of a forward and a reverse traversal, so it emits
// exactly one row per matching edge.
func TestExecUndirectedSameSymbolSelfLoop(t *testing.T) {
	const kindE snapshot.KindID = 10

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindE: "E"},
		[]execNodeSpec{
			{40, nil, nil}, // s (self-loop)
			{41, nil, nil}, // t (has an outgoing edge, but not a self-loop)
			{42, nil, nil}, // w (t's non-self-loop neighbor)
		},
		[]execEdgeSpec{
			{6000, 40, 40, kindE}, // s -> s (self-loop; must be found, exactly once)
			{6001, 41, 42, kindE}, // t -> w (not a self-loop; must never satisfy (n)-[:E]-(n))
		},
	)

	rs := mustExec(t, snap, `MATCH (n)-[:E]-(n) RETURN n`, generousBudget)

	s, _ := snap.Dense(40)
	assertRowSet(t, rs, []string{rowKey([]OutVal{{Kind: OutNode, Node: s}})})
}

// TestExecUndirectedDifferentSymbolClosingStepExcludesRuntimeSelfLoop is the
// control the same finding asked for: two *different* pattern symbols ("a",
// "b") that happen to alias to the same node at runtime (via an earlier
// directed self-loop step binding a == b) must still have their undirected
// closing step's self-exclusion apply. The guard is keyed on the step's
// symbols (FromSym != ToSym), not on a runtime id comparison -- matching
// dawgs' own static, per-identifier branching in
// buildPairwiseDirectionlessTraversalPatternRoot ("Only apply endpoint
// inequality when the bound nodes are different"), which decides the SQL
// shape from the identifiers alone at translate time, never from a runtime
// value. Without this, relaxing the self-exclusion for the same-symbol case
// (this fix's Finding 1) could easily have been over-broadened into also
// admitting this different-symbol-but-runtime-equal case, which must still
// be excluded.
func TestExecUndirectedDifferentSymbolClosingStepExcludesRuntimeSelfLoop(t *testing.T) {
	const (
		kindE snapshot.KindID = 10
		kindF snapshot.KindID = 11
	)

	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindE: "E", kindF: "F"},
		[]execNodeSpec{
			{50, nil, nil}, // s: self-loop on both E and F
		},
		[]execEdgeSpec{
			{8000, 50, 50, kindE}, // s -[:E]-> s: binds a == b == s via the directed step
			{8001, 50, 50, kindF}, // s -[:F]- s: would satisfy the undirected closing step
			// only if the self-exclusion were incorrectly skipped for a's/b's
			// coincidental runtime equality.
		},
	)

	rs := mustExec(t, snap, `MATCH (a)-[:E]->(b), (a)-[:F]-(b) RETURN a,b`, generousBudget)

	if len(rs.Rows) != 0 {
		t.Fatalf("got %d rows, want 0 (different-symbol undirected closing step must still exclude a runtime self-loop coincidence): %v", len(rs.Rows), rowKeys(rs.Rows))
	}
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

// --- Task 8b: mixed fixed/var-length chains + named-path assembly ---------
//
// BloodHound's prebuilt queries routinely mix fixed and variable-length
// steps in one pattern chain (e.g. `(c:Computer)-[:HasSession]->(u:User)-
// [:MemberOf*1..]->(g:Group)`), including multiple variable-length steps per
// chain, and 66 of them bind a named path over a plain (non-shortestPath)
// pattern and `RETURN p`. exec.go's expandChainComponent (dispatched from
// runComponent whenever a component has 2+ Steps with at least one
// variable-length Step and no shortestPath Step, or carries a uniform
// PathSym) is what closes this gap; see expandChainComponent's own doc
// comment for the "always walk left-to-right from the chain's own leftmost
// symbol" anchoring choice this section's tests exercise.

// TestExecChainMixedFixedVarWithoutNamedPath: deliverable 1 in isolation --
// a mixed fixed-then-var chain with no path variable at all must still
// execute correctly (both the 1-hop and 2-hop MemberOf extensions from the
// same HasSession-bound user), and a decoy HasSession target with no further
// MemberOf edges must contribute nothing.
func TestExecChainMixedFixedVarWithoutNamedPath(t *testing.T) {
	const (
		kindComputer snapshot.KindID = 1
		kindUser     snapshot.KindID = 2
		kindGroup    snapshot.KindID = 3
		kindSession  snapshot.KindID = 10
		kindMemberOf snapshot.KindID = 11
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{
			kindComputer: "Computer", kindUser: "User", kindGroup: "Group",
			kindSession: "HasSession", kindMemberOf: "MemberOf",
		},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindComputer}, nil}, // c
			{2, []snapshot.KindID{kindUser}, nil},     // u
			{3, []snapshot.KindID{kindGroup}, nil},    // g1 (1 hop)
			{4, []snapshot.KindID{kindGroup}, nil},    // g2 (2 hops, via g1)
			{5, []snapshot.KindID{kindComputer}, nil}, // decoy computer
			{6, []snapshot.KindID{kindUser}, nil},     // decoy user: HasSession but no MemberOf at all
		},
		[]execEdgeSpec{
			{100, 1, 2, kindSession},
			{101, 2, 3, kindMemberOf},
			{102, 3, 4, kindMemberOf},
			{103, 5, 6, kindSession},
		},
	)

	rs := mustExec(t, snap, `MATCH (c:Computer)-[:HasSession]->(u:User)-[:MemberOf*1..]->(g:Group) RETURN g`, generousBudget)

	g1, _ := snap.Dense(3)
	g2, _ := snap.Dense(4)
	want := []string{
		rowKey([]OutVal{{Kind: OutNode, Node: g1}}),
		rowKey([]OutVal{{Kind: OutNode, Node: g2}}),
	}
	assertRowSet(t, rs, want)
}

// TestExecChainFixedThenVarReturnPathContent: fixed step (HasSession) then a
// variable-length step (MemberOf*1..), named path -- hardcoded expected
// PathVal edge signatures for both the 1-hop and 2-hop rows. A wrong-kind
// decoy edge off the fixed step's own anchor must not participate (proving
// the fixed step's own edge-kind filter still applies inside
// expandChainComponent's reuse of expandStep).
func TestExecChainFixedThenVarReturnPathContent(t *testing.T) {
	const (
		kindComputer snapshot.KindID = 1
		kindUser     snapshot.KindID = 2
		kindGroup    snapshot.KindID = 3
		kindSession  snapshot.KindID = 10
		kindMemberOf snapshot.KindID = 11
		kindOwns     snapshot.KindID = 12
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{
			kindComputer: "Computer", kindUser: "User", kindGroup: "Group",
			kindSession: "HasSession", kindMemberOf: "MemberOf", kindOwns: "Owns",
		},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindComputer}, nil}, // c
			{2, []snapshot.KindID{kindUser}, nil},     // u
			{3, []snapshot.KindID{kindGroup}, nil},    // g1
			{4, []snapshot.KindID{kindGroup}, nil},    // g2
			{5, nil, nil},                             // decoy target of a wrong-kind edge from c
		},
		[]execEdgeSpec{
			{10, 1, 2, kindSession},  // c -HasSession-> u
			{11, 2, 3, kindMemberOf}, // u -MemberOf-> g1 (depth 1)
			{12, 3, 4, kindMemberOf}, // g1 -MemberOf-> g2 (depth 2)
			{13, 1, 5, kindOwns},     // c -Owns-> decoy: wrong kind, must not participate
		},
	)

	assertPathSigs(t, snap,
		`MATCH p = (c:Computer)-[:HasSession]->(u:User)-[:MemberOf*1..]->(g:Group) RETURN p`, 0,
		[]string{
			"N:1,2,3,|E:10,11,",
			"N:1,2,3,4,|E:10,11,12,",
		})
}

// TestExecChainVarThenFixedReturnPathContent: a variable-length step
// (MemberOf*1..) then a fixed step (AdminTo), named path. Only the depth-2
// var-length landing spot (g2) carries an AdminTo edge, so the depth-1
// landing spot (g1) must NOT survive into the final result even though it
// satisfies the var-length step's own Group constraint on its own -- proving
// the fixed step's own admission check still runs per var-length candidate.
// A wrong-kind decoy edge off the chain's own anchor must not leak through
// either.
func TestExecChainVarThenFixedReturnPathContent(t *testing.T) {
	const (
		kindUser     snapshot.KindID = 1
		kindGroup    snapshot.KindID = 2
		kindComputer snapshot.KindID = 3
		kindMemberOf snapshot.KindID = 10
		kindAdminTo  snapshot.KindID = 11
		kindOwns     snapshot.KindID = 12
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{
			kindUser: "User", kindGroup: "Group", kindComputer: "Computer",
			kindMemberOf: "MemberOf", kindAdminTo: "AdminTo", kindOwns: "Owns",
		},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, nil},     // u
			{2, []snapshot.KindID{kindGroup}, nil},    // g1 (depth 1, no AdminTo of its own)
			{3, []snapshot.KindID{kindGroup}, nil},    // g2 (depth 2, has AdminTo)
			{4, []snapshot.KindID{kindComputer}, nil}, // c
			{5, []snapshot.KindID{kindGroup}, nil},    // decoy group, wrong-kind edge from u
		},
		[]execEdgeSpec{
			{20, 1, 2, kindMemberOf},
			{21, 2, 3, kindMemberOf},
			{22, 3, 4, kindAdminTo},
			{23, 1, 5, kindOwns},
		},
	)

	assertPathSigs(t, snap,
		`MATCH p = (u:User)-[:MemberOf*1..]->(g:Group)-[:AdminTo]->(c:Computer) RETURN p`, 0,
		[]string{"N:1,2,3,4,|E:20,21,22,"})
}

// TestExecChainVarFixedVarLeadingZeroLengthSkipsStep: a 3-step var->fixed->
// var chain whose LEADING step is `*0..`. u1 satisfies both the "u" pattern
// (User) and the "g" pattern (Group) directly and has no outgoing MemberOf
// edges at all, so the leading step's only surviving row is its zero-length
// one (g merges into u1 itself); the assembled path must SKIP that step
// entirely (no extra node/edge for it) rather than emit a spurious
// self-referencing hop. u2 -- User but not Group, and with no edges either
// -- proves the zero-length merge genuinely depends on satisfying the
// target constraint rather than firing unconditionally (it contributes zero
// rows).
func TestExecChainVarFixedVarLeadingZeroLengthSkipsStep(t *testing.T) {
	const (
		kindUser  snapshot.KindID = 1
		kindGroup snapshot.KindID = 2
		kindCT    snapshot.KindID = 3
		kindCA    snapshot.KindID = 4
		kindA     snapshot.KindID = 10 // MemberOf
		kindB     snapshot.KindID = 11 // Enroll
		kindC     snapshot.KindID = 12 // PublishedTo
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{
			kindUser: "User", kindGroup: "Group", kindCT: "CertTemplate", kindCA: "CA",
			kindA: "MemberOf", kindB: "Enroll", kindC: "PublishedTo",
		},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser, kindGroup}, nil}, // u1: satisfies BOTH u and g -- the zero-length case
			{2, []snapshot.KindID{kindCT}, nil},              // ct1
			{3, []snapshot.KindID{kindCA}, nil},              // ca1
			{4, []snapshot.KindID{kindUser}, nil},            // u2: decoy, User only (not Group), no edges
		},
		[]execEdgeSpec{
			{100, 1, 2, kindB}, // u1 -Enroll-> ct1
			{101, 2, 3, kindC}, // ct1 -PublishedTo-> ca1
		},
	)

	assertPathSigs(t, snap,
		`MATCH p = (u:User)-[:MemberOf*0..]->(g:Group)-[:Enroll]->(ct:CertTemplate)-[:PublishedTo*1..]->(ca:CA) RETURN p`, 0,
		[]string{"N:1,2,3,|E:100,101,"})
}

// TestExecChainSameEdgeReusedAcrossDifferentVarStepsAllowed: two adjacent
// variable-length steps (a-[*1..3]->m-[*1..3]->b) whose only qualifying
// trails happen to reuse the SAME physical edge (X->Y) once in each step's
// own trail. Trail-edge uniqueness is scoped to a single step's own DFS
// (see expand.go's package doc, unchanged by Task 8b's refactor into
// expandVarLengthTrailsForSeed) -- never across steps of the same chain --
// so this row must survive, with the shared edge's id appearing TWICE in
// the assembled path's own Edges list. The companion "forbidden WITHIN one
// step" half of this pin is already covered, unchanged, by
// TestExpandVarLengthEdgeReuseForbiddenTriangleWithChord.
func TestExecChainSameEdgeReusedAcrossDifferentVarStepsAllowed(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindMid    snapshot.KindID = 2
		kindTarget snapshot.KindID = 3
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindMid: "Mid", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindRoot}, nil},   // a
			{2, nil, nil},                           // X
			{3, nil, nil},                           // Y
			{4, []snapshot.KindID{kindMid}, nil},    // m
			{5, []snapshot.KindID{kindTarget}, nil}, // b
		},
		[]execEdgeSpec{
			{200, 1, 2, kindE}, // a -> X
			{201, 2, 3, kindE}, // X -> Y: the edge reused by both steps' trails
			{202, 3, 4, kindE}, // Y -> m
			{203, 4, 2, kindE}, // m -> X: back edge, lets step 2 reach X again
			{204, 3, 5, kindE}, // Y -> b
		},
	)

	assertPathSigs(t, snap,
		`MATCH p = (a:Root)-[:E*1..3]->(m:Mid)-[:E*1..3]->(b:Target) RETURN p`, 0,
		[]string{"N:1,2,3,4,2,3,5,|E:200,201,202,203,201,204,"})
}

// TestExecChainBudgetExhaustionMidChain: a fixed step (a -F-> m) followed by
// a variable-length step from m into an 8-node clique, under a work budget
// far too small for the clique's own fan-out (mirroring
// TestExpandVarLengthBudgetExhaustion's proven scale) -- Execute must abort
// with ErrBudget partway through the chain's SECOND step, not merely at the
// very end.
func TestExecChainBudgetExhaustionMidChain(t *testing.T) {
	const (
		kindRoot snapshot.KindID = 1
		kindMid  snapshot.KindID = 2
		kindE    snapshot.KindID = 10
		kindF    snapshot.KindID = 11
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindMid: "Mid", kindE: "E", kindF: "F"}
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kindRoot}, nil}, // a
		{2, []snapshot.KindID{kindMid}, nil},  // m (also a clique member below)
	}
	for i := uint64(3); i <= 9; i++ {
		nodes = append(nodes, execNodeSpec{id: i})
	}
	edges := []execEdgeSpec{{1000, 1, 2, kindF}} // a -F-> m

	nextID := uint64(1)
	for u := uint64(2); u <= 9; u++ {
		for v := uint64(2); v <= 9; v++ {
			if u == v {
				continue
			}
			edges = append(edges, execEdgeSpec{nextID, u, v, kindE})
			nextID++
		}
	}
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	err := execExpectErr(t, snap,
		`MATCH p = (a:Root)-[:F]->(m:Mid)-[:E*1..4]->(b) RETURN p`,
		Budgets{MaxRows: 1_000_000, MaxWork: 1200})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("Execute() error = %v, want ErrBudget", err)
	}
}

// TestExecChainThreeStepFixedVarFixedReturnPathContent: a 3-step mixed chain
// (fixed A, then var B*1.., then fixed C) whose var-length step lands on two
// distinct depths (both satisfying the middle kind), each independently
// continuing through the trailing fixed step -- both resulting rows' full
// PathVal content is asserted exactly.
func TestExecChainThreeStepFixedVarFixedReturnPathContent(t *testing.T) {
	const (
		kindK1 snapshot.KindID = 1
		kindK2 snapshot.KindID = 2
		kindK3 snapshot.KindID = 3
		kindK4 snapshot.KindID = 4
		kindA  snapshot.KindID = 10
		kindB  snapshot.KindID = 11
		kindC  snapshot.KindID = 12
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{
			kindK1: "K1", kindK2: "K2", kindK3: "K3", kindK4: "K4",
			kindA: "A", kindB: "B", kindC: "C",
		},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindK1}, nil},
			{2, []snapshot.KindID{kindK2}, nil},
			{3, []snapshot.KindID{kindK3}, nil},
			{4, []snapshot.KindID{kindK4}, nil},
			{5, []snapshot.KindID{kindK3}, nil},
			{6, []snapshot.KindID{kindK4}, nil},
		},
		[]execEdgeSpec{
			{100, 1, 2, kindA},
			{101, 2, 3, kindB},
			{102, 3, 4, kindC},
			{103, 3, 5, kindB},
			{104, 5, 6, kindC},
		},
	)

	assertPathSigs(t, snap,
		`MATCH p = (n1:K1)-[:A]->(n2:K2)-[:B*1..]->(n3:K3)-[:C]->(n4:K4) RETURN p`, 0,
		[]string{
			"N:1,2,3,4,|E:100,101,102,",
			"N:1,2,3,5,6,|E:100,101,103,104,",
		})
}

// TestExecChainSingleFixedStepNamedPath: the simplest of the corpus's 66
// plain-pattern named-path queries -- one fixed hop, no var-length at all --
// bridging plan_test.go's existing plan-only "plain named path projected"
// acceptance case with an actual execution/PathVal assertion.
func TestExecChainSingleFixedStepNamedPath(t *testing.T) {
	const (
		kindUser snapshot.KindID = 1
		kindX    snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindX: "X"},
		[]execNodeSpec{
			{1, []snapshot.KindID{kindUser}, nil},
			{2, []snapshot.KindID{kindUser}, nil},
		},
		[]execEdgeSpec{{500, 1, 2, kindX}},
	)

	assertPathSigs(t, snap, `MATCH p = (a:User)-[:X]->(b:User) RETURN p`, 0, []string{"N:1,2,|E:500,"})
}
