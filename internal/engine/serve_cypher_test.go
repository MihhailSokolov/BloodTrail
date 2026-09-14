// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/interpret"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestCypherBudgetConstants pins the exact values of maxCypherRows/
// maxCypherWork/edgePropsBatchSize -- serve_cypher.go's own doc comments
// explain what each bounds; this test exists purely to catch an accidental
// change to one of the pinned numbers (and, incidentally, gives
// edgePropsBatchSize -- not yet consumed by any pipeline code, since that is
// the hydration wiring's job -- a reference, so it is not flagged as dead
// code before that wiring lands).
func TestCypherBudgetConstants(t *testing.T) {
	if maxCypherRows != 100_000 {
		t.Fatalf("maxCypherRows = %d, want 100_000", maxCypherRows)
	}
	if maxCypherWork != 1<<28 {
		t.Fatalf("maxCypherWork = %d, want 1<<28", maxCypherWork)
	}
	if edgePropsBatchSize != 10_000 {
		t.Fatalf("edgePropsBatchSize = %d, want 10_000", edgePropsBatchSize)
	}
}

// --- test snapshot builder ------------------------------------------------

type cypherTestNode struct {
	id    uint64
	kinds []snapshot.KindID
	props map[string]any
}

type cypherTestEdge struct {
	id         uint64
	start, end uint64
	kind       snapshot.KindID
}

// buildCypherTestSnapshot builds a hand-crafted snapshot for one test
// scenario, mirroring interpret/exec_test.go's identical buildExecSnapshot
// helper (duplicated here rather than exported from interpret, since this
// package's tests have no other reason to depend on interpret's test-only
// code).
func buildCypherTestSnapshot(t *testing.T, kindTable map[snapshot.KindID]string, nodes []cypherTestNode, edges []cypherTestEdge) *snapshot.Snapshot {
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

// --- materializeNode -------------------------------------------------------

func TestMaterializeNode(t *testing.T) {
	const (
		kindUser  snapshot.KindID = 1
		kindAdmin snapshot.KindID = 2
	)
	snap := buildCypherTestSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindAdmin: "Admin"},
		[]cypherTestNode{
			{id: 100, kinds: []snapshot.KindID{kindUser, kindAdmin}, props: map[string]any{
				"name": "Alice",
				"note": nil, // present JSON null
			}},
		},
		nil,
	)

	got := materializeNode(snapshot.NewView(snap), 0)

	want := graph.NewNode(
		graph.ID(100),
		graph.AsProperties(map[string]any{"name": "Alice", "note": nil}),
		graph.StringKind("User"), graph.StringKind("Admin"),
	)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("materializeNode = %#v, want %#v", got, want)
	}
	// Kind order must match the node's own kind_ids array order (User before
	// Admin, as staged), not e.g. alphabetical.
	if len(got.Kinds) != 2 || got.Kinds[0].String() != "User" || got.Kinds[1].String() != "Admin" {
		t.Fatalf("materializeNode kinds order = %v, want [User Admin]", got.Kinds)
	}
	if v, ok := got.Properties.Map["note"]; !ok || v != nil {
		t.Fatalf("materializeNode Properties.Map[note] = (%v, %v), want (nil, true)", v, ok)
	}
}

// --- materializeEdge --------------------------------------------------------

// TestMaterializeEdgeForwardAndReverseDiscovered builds a tiny 3-node graph
// with a gap node (zero out-degree) in the middle of the CSR to exercise
// edgeSource's binary search across a non-trivial OutOffsets shape, and
// asserts materializeEdge produces the same (id, start, end, kind) triple
// regardless of whether the caller's EdgeRef was the "natural" forward slot
// (as if discovered by walking Out(a)) or a reverse-discovered one (as if
// discovered by walking In(b) and translated via InEdgeIdx, per EdgeRef's
// own doc comment) -- both cases resolve to the identical forward index, so
// this also pins down that materializeEdge itself has no notion of "which
// direction discovered this edge".
func TestMaterializeEdgeForwardAndReverseDiscovered(t *testing.T) {
	const kindKnows snapshot.KindID = 1

	// a(10) --e777--> c(30); b(20) has no edges at all, so its OutOffsets
	// segment is empty and sits between a's and c's non-empty segments.
	snap := buildCypherTestSnapshot(t,
		map[snapshot.KindID]string{kindKnows: "Knows"},
		[]cypherTestNode{
			{id: 10, kinds: nil, props: nil},
			{id: 20, kinds: nil, props: nil},
			{id: 30, kinds: nil, props: nil},
		},
		[]cypherTestEdge{
			{id: 777, start: 10, end: 30, kind: kindKnows},
		},
	)

	// Sanity: the single edge lives at forward index 0, under node a (dense
	// 0)'s segment, with b (dense 1) contributing an empty segment.
	if got, want := snap.OutOffsets, []uint64{0, 1, 1, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("OutOffsets = %v, want %v (test fixture assumption violated)", got, want)
	}

	fwdRef := interpret.EdgeRef{Fwd: 0}

	// The reverse CSR discovers the same edge via In(c) (dense 2); its
	// InEdgeIdx entry names the identical forward index. There is exactly
	// one reverse slot in this fixture.
	if len(snap.InEdgeIdx) != 1 {
		t.Fatalf("InEdgeIdx = %v, want exactly one entry", snap.InEdgeIdx)
	}
	reverseDiscoveredRef := interpret.EdgeRef{Fwd: uint64(snap.InEdgeIdx[0])}

	for name, ref := range map[string]interpret.EdgeRef{
		"forward-natural":    fwdRef,
		"reverse-discovered": reverseDiscoveredRef,
	} {
		t.Run(name, func(t *testing.T) {
			props := graph.NewProperties()
			got := materializeEdge(snapshot.NewView(snap), ref, props)

			want := graph.NewRelationship(graph.ID(777), graph.ID(10), graph.ID(30), props, graph.StringKind("Knows"))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("materializeEdge(%+v) = %#v, want %#v", ref, got, want)
			}
		})
	}
}

// --- materializePath ---------------------------------------------------

func TestMaterializePathOrder(t *testing.T) {
	const kindKnows snapshot.KindID = 1
	snap := buildCypherTestSnapshot(t,
		map[snapshot.KindID]string{kindKnows: "Knows"},
		[]cypherTestNode{
			{id: 10, props: map[string]any{"n": "a"}},
			{id: 20, props: map[string]any{"n": "b"}},
			{id: 30, props: map[string]any{"n": "c"}},
		},
		[]cypherTestEdge{
			{id: 100, start: 10, end: 20, kind: kindKnows},
			{id: 200, start: 20, end: 30, kind: kindKnows},
		},
	)

	// a(0) --100--> b(1) --200--> c(2)
	pv := &interpret.PathVal{
		Nodes: []snapshot.NodeID{0, 1, 2},
		Edges: []interpret.EdgeRef{{Fwd: 0}, {Fwd: 1}},
	}

	edgeProps := map[uint64]*graph.Properties{
		100: graph.AsProperties(map[string]any{"since": "2020"}),
	}

	got := materializePath(snapshot.NewView(snap), pv, edgeProps)

	if len(got.Nodes) != 3 || len(got.Edges) != 2 {
		t.Fatalf("materializePath shape = %d nodes, %d edges, want 3 nodes, 2 edges", len(got.Nodes), len(got.Edges))
	}

	wantNodeIDs := []graph.ID{10, 20, 30}
	for i, n := range got.Nodes {
		if n.ID != wantNodeIDs[i] {
			t.Fatalf("Nodes[%d].ID = %d, want %d (path order)", i, n.ID, wantNodeIDs[i])
		}
	}

	if got.Edges[0].ID != 100 || got.Edges[0].StartID != 10 || got.Edges[0].EndID != 20 {
		t.Fatalf("Edges[0] = %+v, want id=100 start=10 end=20", got.Edges[0])
	}
	if got.Edges[0].Properties.Map["since"] != "2020" {
		t.Fatalf("Edges[0].Properties = %+v, want hydrated {since: 2020}", got.Edges[0].Properties)
	}

	if got.Edges[1].ID != 200 || got.Edges[1].StartID != 20 || got.Edges[1].EndID != 30 {
		t.Fatalf("Edges[1] = %+v, want id=200 start=20 end=30", got.Edges[1])
	}
	// No edgeProps entry for edge 200: materializePath must fall back to an
	// empty, non-nil Properties rather than erroring or leaving it nil.
	if got.Edges[1].Properties == nil {
		t.Fatalf("Edges[1].Properties = nil, want empty non-nil Properties for an unhydrated edge")
	}
	if len(got.Edges[1].Properties.Map) != 0 {
		t.Fatalf("Edges[1].Properties.Map = %v, want empty", got.Edges[1].Properties.Map)
	}
}

func TestMaterializePathNilIsZeroPath(t *testing.T) {
	snap := buildCypherTestSnapshot(t, nil, nil, nil)
	got := materializePath(snapshot.NewView(snap), nil, nil)
	if len(got.Nodes) != 0 || len(got.Edges) != 0 {
		t.Fatalf("materializePath(nil) = %+v, want zero graph.Path", got)
	}
}

// --- scalar double-decode ------------------------------------------------

func TestDecodeScalarString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  any
	}{
		{"object", `{"a":1}`, map[string]any{"a": float64(1)}},
		{"array", `[1]`, []any{float64(1)}},
		{"quoted string", `"x"`, "x"},
		{"plain string unchanged", "plain", "plain"},
		{"malformed object unchanged", "{oops", "{oops"},
		{"empty string unchanged", "", ""},
		{"leading/trailing space trimmed for detection", "  {\"a\":1}  ", map[string]any{"a": float64(1)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeScalarString(tc.input)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("decodeScalarString(%q) = %#v, want %#v", tc.input, got, tc.want)
			}
		})
	}
}

// --- projectionValueKinds --------------------------------------------------

// variable is a small helper constructing a bare *cypher.Variable
// ProjectionOutput.Expr for a RETURN item, e.g. `RETURN u`.
func variable(sym string) cypher.Expression {
	return &cypher.Variable{Symbol: sym}
}

func TestProjectionValueKindsBareCallKinds(t *testing.T) {
	q := &interpret.Query{
		Returning: interpret.Projection{
			Items: []interpret.ProjectionOutput{
				{Alias: "u", Expr: variable("u")}, // node/edge/path/scalar carry-over: default
				{Alias: "idx", Expr: variable("_"), BareCallKind: "id"},
				{Alias: "sz", Expr: variable("_"), BareCallKind: "size"},
				{Alias: "es", Expr: variable("_"), BareCallKind: "epochseconds"},
				{Alias: "em", Expr: variable("_"), BareCallKind: "epochmillis"},
				{Alias: "n.prop", Expr: &cypher.PropertyLookup{Atom: variable("n"), Symbol: "prop"}},
			},
		},
	}

	got := projectionValueKinds(q)
	want := []valueKind{valueDefault, valueInt64, valueInt32, valueInt64, valueInt64, valueDefault}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projectionValueKinds = %v, want %v", got, want)
	}
}

// TestProjectionValueKindsCountAlias exercises the amendment's WITH-count-
// alias gap directly: a RETURN item that bares-references a WITH COUNT(...)
// alias (with no BareCallKind of its own, since planReturn never sets one
// for a bare Variable) must still resolve to valueInt64, and a RETURN item
// unrelated to that alias (even in the very same query) must not.
func TestProjectionValueKindsCountAlias(t *testing.T) {
	q := &interpret.Query{
		Parts: []interpret.Part{
			{With: &interpret.WithClause{
				GroupKeys:  []string{"u"},
				Aggregates: []interpret.WithAggregate{{Alias: "adminCount", Count: &interpret.CountAgg{Sym: "c"}}},
			}},
			{},
		},
		Returning: interpret.Projection{
			Items: []interpret.ProjectionOutput{
				{Alias: "u", Expr: variable("u")},
				{Alias: "adminCount", Expr: variable("adminCount")},
			},
		},
	}

	got := projectionValueKinds(q)
	want := []valueKind{valueDefault, valueInt64}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projectionValueKinds = %v, want %v", got, want)
	}
}

// TestProjectionValueKindsCountAliasNotProjected is the "WITH-count-alias-
// ordered query" shape: the alias is declared by WITH and used only in
// ORDER BY (not modeled in Query here, since OrderKey resolution is
// entirely the executor's concern), never re-projected in RETURN. The
// resolver must not spuriously flag the one RETURN item ("u") just because
// some count alias exists somewhere in the query.
func TestProjectionValueKindsCountAliasNotProjected(t *testing.T) {
	q := &interpret.Query{
		Parts: []interpret.Part{
			{With: &interpret.WithClause{
				GroupKeys:  []string{"u"},
				Aggregates: []interpret.WithAggregate{{Alias: "adminCount", Count: &interpret.CountAgg{Sym: "c"}}},
			}},
			{},
		},
		Returning: interpret.Projection{
			Items: []interpret.ProjectionOutput{
				{Alias: "u", Expr: variable("u")},
			},
		},
		Order: []interpret.OrderKey{{Symbol: "adminCount", Descending: true}},
	}

	got := projectionValueKinds(q)
	want := []valueKind{valueDefault}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projectionValueKinds = %v, want %v", got, want)
	}
}

// TestProjectionValueKindsEndToEndCount plans and executes a real query
// through a WITH COUNT(...) boundary, confirming the resolved valueInt64
// kind actually converts the interpreter's float64 count into an int64 once
// routed through materializeScalar -- not just that the resolver's *label*
// is right, but that the conversion it drives produces the pinned pg-parity
// type end to end.
func TestProjectionValueKindsEndToEndCount(t *testing.T) {
	const (
		kindUser   snapshot.KindID = 1
		kindAdmin  snapshot.KindID = 2
		kindAdmin2 snapshot.KindID = 3 // relationship kind: "AdminTo"
	)
	snap := buildCypherTestSnapshot(t,
		map[snapshot.KindID]string{kindUser: "User", kindAdmin: "Admin", kindAdmin2: "AdminTo"},
		[]cypherTestNode{
			{id: 1, kinds: []snapshot.KindID{kindUser}},
			{id: 2, kinds: []snapshot.KindID{kindAdmin}},
			{id: 3, kinds: []snapshot.KindID{kindAdmin}},
		},
		[]cypherTestEdge{
			{id: 1001, start: 1, end: 2, kind: kindAdmin2},
			{id: 1002, start: 1, end: 3, kind: kindAdmin2},
		},
	)

	q := planAndExec(t, snap, `MATCH (u:User)-[:AdminTo]->(c:Admin) WITH u, COUNT(c) AS adminCount RETURN adminCount`)
	kinds := projectionValueKinds(q.query)

	if len(q.rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(q.rs.Rows))
	}
	row := q.rs.Rows[0]
	if len(row) != 1 || row[0].Kind != interpret.OutScalar {
		t.Fatalf("row = %+v, want a single OutScalar column", row)
	}
	if f, ok := row[0].Scalar.(float64); !ok || f != 2 {
		t.Fatalf("raw interpreter scalar = %#v, want float64(2)", row[0].Scalar)
	}

	got := materializeScalar(row[0].Scalar, kinds[0])
	if got != int64(2) {
		t.Fatalf("materializeScalar(count) = %#v (%T), want int64(2)", got, got)
	}
}

// TestProjectionValueKindsPropertyStaysDefault confirms a plain property
// projection is never mistaken for a bare amendment call: its interpreter
// value passes through materializeScalar unconverted (float64 stays
// float64, string stays string).
func TestProjectionValueKindsPropertyStaysDefault(t *testing.T) {
	snap := buildCypherTestSnapshot(t, nil,
		[]cypherTestNode{
			{id: 1, props: map[string]any{"age": float64(30), "name": "Alice"}},
		}, nil,
	)

	q := planAndExec(t, snap, `MATCH (n) RETURN n.age, n.name`)
	kinds := projectionValueKinds(q.query)
	if !reflect.DeepEqual(kinds, []valueKind{valueDefault, valueDefault}) {
		t.Fatalf("kinds = %v, want all default", kinds)
	}

	row := q.rs.Rows[0]
	if got := materializeScalar(row[0].Scalar, kinds[0]); got != float64(30) {
		t.Fatalf("age = %#v, want float64(30)", got)
	}
	if got := materializeScalar(row[1].Scalar, kinds[1]); got != "Alice" {
		t.Fatalf("name = %#v, want \"Alice\"", got)
	}
}

// TestProjectionValueKindsKeysOrder confirms Keys() preserves RETURN order
// and mirrors the pg driver's own behavior exactly (pinned live by
// TestTryCypherKeysMatchOracle): nil before the first Next() call, the
// pg-named columns afterwards -- an UNALIASED property lookup is pg's
// `?column?` placeholder (dawgs emits it with no SQL alias), a bare
// variable keeps its symbol -- and the names persist past exhaustion.
func TestProjectionValueKindsKeysOrder(t *testing.T) {
	snap := buildCypherTestSnapshot(t, nil,
		[]cypherTestNode{{id: 1, props: map[string]any{"age": float64(30), "name": "Alice"}}}, nil,
	)
	q := planAndExec(t, snap, `MATCH (n) RETURN n.name, n.age, n`)

	result := newCypherRowsResult(snapshot.NewView(snap), q.rs, projectionValueKinds(q.query), nil)
	if got := result.Keys(); got != nil {
		t.Fatalf("Keys() before the first Next() = %v, want nil (pg driver populates keys in Next)", got)
	}
	if !result.Next() {
		t.Fatal("Next() = false, want a row")
	}
	want := []string{"?column?", "?column?", "n"}
	if got := result.Keys(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Keys() = %v, want %v", got, want)
	}
	for result.Next() {
	}
	if got := result.Keys(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Keys() after exhaustion = %v, want %v (names persist once a row was read)", got, want)
	}
}

// --- planAndExec test helper ---------------------------------------------

type plannedQuery struct {
	query *interpret.Query
	rs    *interpret.ResultSet
}

// planAndExec parses, plans, and executes query against snap, failing the
// test on any error -- the engine-package counterpart to interpret/
// exec_test.go's mustExec, needed here because plan/exec details
// (interpret.Query, interpret.ResultSet) are what projectionValueKinds and
// cypherRowsResult actually consume.
func planAndExec(t *testing.T, snap *snapshot.Snapshot, text string) plannedQuery {
	t.Helper()
	rq, err := frontend.ParseCypher(frontend.NewContext(), text)
	if err != nil {
		t.Fatalf("ParseCypher(%q): %v", text, err)
	}
	view := snapshot.NewView(snap)
	q, ok := interpret.Plan(rq, view)
	if !ok {
		t.Fatalf("Plan(%q): not served", text)
	}
	rs, err := interpret.Execute(&interpret.Env{Snap: view}, q, interpret.Budgets{MaxRows: maxCypherRows, MaxWork: maxCypherWork})
	if err != nil {
		t.Fatalf("Execute(%q): %v", text, err)
	}
	return plannedQuery{query: q, rs: rs}
}

// --- cypherRowsResult / Mapper ---------------------------------------------

func TestCypherRowsResultMapperDeclinesForeignTargets(t *testing.T) {
	snap := buildCypherTestSnapshot(t,
		map[snapshot.KindID]string{1: "User"},
		[]cypherTestNode{{id: 1, kinds: []snapshot.KindID{1}, props: map[string]any{"name": "Alice"}}},
		nil,
	)
	view := snapshot.NewView(snap)
	node := materializeNode(view, 0)

	result := newCypherRowsResult(view, &interpret.ResultSet{
		Keys: []string{"n"},
		Rows: [][]interpret.OutVal{{{Kind: interpret.OutNode, Node: 0}}},
	}, []valueKind{valueDefault}, nil)

	if !result.Next() {
		t.Fatalf("Next() = false, want true")
	}
	values := result.Values()
	if len(values) != 1 {
		t.Fatalf("Values() = %v, want one column", values)
	}

	mapper := result.Mapper()

	var kinds graph.Kinds
	if mapper.Map(values[0], &kinds) {
		t.Fatalf("mapper unexpectedly mapped a node value into *graph.Kinds: %v", kinds)
	}

	var gotNode graph.Node
	if !mapper.Map(values[0], &gotNode) {
		t.Fatalf("mapper failed to map a node value into *graph.Node")
	}
	if !reflect.DeepEqual(&gotNode, node) {
		t.Fatalf("mapped node = %#v, want %#v", &gotNode, node)
	}

	if result.Next() {
		t.Fatalf("Next() after last row = true, want false")
	}
	if v := result.Values(); v != nil {
		t.Fatalf("Values() after exhausted = %v, want nil", v)
	}
}

func TestCypherRowsResultErrorAndClose(t *testing.T) {
	result := newCypherRowsResult(nil, &interpret.ResultSet{}, nil, nil)
	if err := result.Error(); err != nil {
		t.Fatalf("Error() = %v, want nil", err)
	}
	result.Close() // must not panic
}

// --- panic backstops ---------------------------------------------------------

// TestSafeExecuteCypherRecoversPanic is a regression test for the
// execution side: safeExecuteCypher must convert a panic anywhere
// inside interpret.Execute into an ordinary error (wrapping errCypherPanic,
// which cypherExecReason then maps to reasonPanic) rather than letting it
// propagate to its own caller -- TryCypher, and beyond it, whatever
// goroutine is holding TryCypher's own caller. Since it is not otherwise
// known how to reliably provoke a genuine panic from outside the interpret
// package, this substitutes executeCypher (the package-level indirection
// safeExecuteCypher calls through for exactly this purpose) with a function
// that panics deliberately.
func TestSafeExecuteCypherRecoversPanic(t *testing.T) {
	orig := executeCypher
	t.Cleanup(func() { executeCypher = orig })
	executeCypher = func(*interpret.Env, *interpret.Query, interpret.Budgets) (*interpret.ResultSet, error) {
		panic("injected panic for the panic-backstop regression test")
	}

	rs, err := safeExecuteCypher(&interpret.Env{}, &interpret.Query{}, interpret.Budgets{})
	if rs != nil {
		t.Fatalf("safeExecuteCypher: rs = %v, want nil", rs)
	}
	if !errors.Is(err, errCypherPanic) {
		t.Fatalf("safeExecuteCypher: err = %v, want errCypherPanic", err)
	}
	if got := cypherExecReason(err); got != reasonPanic {
		t.Fatalf("cypherExecReason(err) = %q, want %q", got, reasonPanic)
	}
}

// TestBuildCypherRowsResultRecoversPanic is a regression test for the
// materialization side: buildCypherRowsResult must recover a panic during
// newCypherRowsResult's eager row materialization and report ok == false,
// rather than letting it escape to TryCypher's own caller. Unlike the
// execution-side test above, this reaches a REAL panic via a crafted
// poison value: an OutNode column whose NodeID is out of range for the
// snapshot it is materialized against, which materializeNode's own
// snap.KindOffsets[n] indexing panics on (index out of range) -- exactly
// the kind of bug this backstop exists to guard against, not merely a
// synthetic stand-in for one. interpret.Execute itself is expected to
// never actually produce such a row, but this backstop's whole point is to
// stay safe even if it someday did.
func TestBuildCypherRowsResultRecoversPanic(t *testing.T) {
	snap := buildCypherTestSnapshot(t,
		map[snapshot.KindID]string{1: "User"},
		[]cypherTestNode{{id: 1, kinds: []snapshot.KindID{1}, props: nil}},
		nil,
	)

	rs := &interpret.ResultSet{
		Keys: []string{"n"},
		Rows: [][]interpret.OutVal{
			{{Kind: interpret.OutNode, Node: 9999}}, // out of range: snap has one node
		},
	}

	result, ok := buildCypherRowsResult(snapshot.NewView(snap), rs, []valueKind{valueDefault}, nil)
	if ok {
		t.Fatalf("buildCypherRowsResult: ok = true, want false (poison NodeID should panic and be recovered)")
	}
	if result != nil {
		t.Fatalf("buildCypherRowsResult: result = %v, want nil", result)
	}
}
