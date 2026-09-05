// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/traverse"
)

// --- test helpers: path signatures + error-expecting execution -------------

// pathSig renders one *PathVal as a comparable string keyed by database ids
// (node ids and edge ids, not dense/CSR ones), so two structurally identical
// trails compare equal regardless of which dense ids the snapshot happened
// to assign, and two trails that used different parallel edges (or a
// different node order) compare unequal.
func pathSig(snap *snapshot.Snapshot, pv *PathVal) string {
	s := "N:"
	for _, n := range pv.Nodes {
		s += fmt.Sprintf("%d,", snap.GraphIDs[n])
	}
	s += "|E:"
	for _, e := range pv.Edges {
		s += fmt.Sprintf("%d,", snap.OutEdgeIDs[e.Fwd])
	}
	return s
}

// pathSigsAtColumn extracts pathSig for every row's col'th projected value,
// asserting each is actually OutPath, sorted for order-independent
// comparison.
func pathSigsAtColumn(t *testing.T, snap *snapshot.Snapshot, rs *ResultSet, col int) []string {
	t.Helper()
	out := make([]string, len(rs.Rows))
	for i, row := range rs.Rows {
		v := row[col]
		if v.Kind != OutPath {
			t.Fatalf("row %d column %d kind = %v, want OutPath", i, col, v.Kind)
		}
		out[i] = pathSig(snap, v.Path)
	}
	sort.Strings(out)
	return out
}

// assertPathSigs plans and executes query, then asserts the col'th
// projection column's set of path signatures equals want exactly
// (multiplicity included -- two identical want entries require two matching
// rows).
func assertPathSigs(t *testing.T, snap *snapshot.Snapshot, query string, col int, want []string) {
	t.Helper()
	rs := mustExec(t, snap, query, generousBudget)
	got := pathSigsAtColumn(t, snap, rs, col)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("query %q: got %d paths, want %d\ngot:  %v\nwant: %v", query, len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("query %q: path set mismatch\ngot:  %v\nwant: %v", query, got, want)
		}
	}
}

// execExpectErr plans and executes query, failing the test if Execute does
// not return an error, and returning that error for the caller to inspect
// (typically via errors.Is).
func execExpectErr(t *testing.T, snap *snapshot.Snapshot, query string, b Budgets) error {
	t.Helper()
	q := planQuery(t, snap, query)
	_, err := Execute(&Env{Snap: snap}, q, b)
	if err == nil {
		t.Fatalf("Execute(%q): want error, got nil", query)
	}
	return err
}

// --- var-length trail expansion --------------------------------------------

// TestExpandVarLengthDiamondBothTrails: a-x-b and a-y-b, *1..3 from a
// (Root) to b (Target); both length-2 trails through the diamond's two
// distinct intermediate nodes must be enumerated as separate rows.
func TestExpandVarLengthDiamondBothTrails(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}},
			{id: 2}, // x
			{id: 3}, // y
			{id: 4, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 10, start: 1, end: 2, kind: kindE}, // a->x
			{id: 11, start: 2, end: 4, kind: kindE}, // x->b
			{id: 12, start: 1, end: 3, kind: kindE}, // a->y
			{id: 13, start: 3, end: 4, kind: kindE}, // y->b
		},
	)

	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:E*1..3]->(b:Target) RETURN p`, 0, []string{
		"N:1,2,4,|E:10,11,",
		"N:1,3,4,|E:12,13,",
	})
}

// TestExpandVarLengthParallelEdgesDistinctTrails: two parallel edges of the
// same kind between the same two nodes must produce two distinct rows (one
// per physical edge), not one.
func TestExpandVarLengthParallelEdgesDistinctTrails(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}},
			{id: 2, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 100, start: 1, end: 2, kind: kindE},
			{id: 101, start: 1, end: 2, kind: kindE},
		},
	)

	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:E*1..1]->(b:Target) RETURN p`, 0, []string{
		"N:1,2,|E:100,",
		"N:1,2,|E:101,",
	})
}

// TestExpandVarLengthEdgeReuseForbiddenTriangleWithChord: a triangle
// (a->b->c->a) plus a chord (a->c) forbids reusing an edge id within one
// trail, so only the two acyclic returns to `a` (a-c-a and a-b-c-a) survive
// out of a *1..6 search back to id(a) -- a reuse-permitting implementation
// would additionally emit a depth-6 trail retracing the triangle twice
// (a-b-c-a-b-c-a).
func TestExpandVarLengthEdgeReuseForbiddenTriangleWithChord(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}}, // a
			{id: 2}, // b
			{id: 3}, // c
		},
		[]execEdgeSpec{
			{id: 1001, start: 1, end: 2, kind: kindE}, // a->b
			{id: 1002, start: 2, end: 3, kind: kindE}, // b->c
			{id: 1003, start: 3, end: 1, kind: kindE}, // c->a
			{id: 1004, start: 1, end: 3, kind: kindE}, // chord a->c
		},
	)

	assertPathSigs(t, snap,
		`MATCH p = (a:Root)-[:E*1..6]->(x) WHERE id(x) = 1 RETURN p`, 0,
		[]string{
			"N:1,3,1,|E:1004,1003,",
			"N:1,2,3,1,|E:1001,1002,1003,",
		})
}

// TestExpandVarLengthNodeRevisitViaDistinctEdges: a figure-eight through a
// shared node `m`, revisited via two distinct edges (m->c and c->m) --
// revisiting a NODE (unlike an edge id) is legal trail semantics, so both
// the short a-m-b trail and the longer a-m-c-m-b trail through the revisit
// must be enumerated.
func TestExpandVarLengthNodeRevisitViaDistinctEdges(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}}, // a
			{id: 2}, // m
			{id: 3}, // c
			{id: 4, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 21, start: 1, end: 2, kind: kindE}, // a->m
			{id: 22, start: 2, end: 3, kind: kindE}, // m->c
			{id: 23, start: 3, end: 2, kind: kindE}, // c->m (revisit)
			{id: 24, start: 2, end: 4, kind: kindE}, // m->b
		},
	)

	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:E*1..4]->(b:Target) RETURN p`, 0, []string{
		"N:1,2,4,|E:21,24,",
		"N:1,2,3,2,4,|E:21,22,23,24,",
	})
}

// TestExpandVarLengthZeroLengthBindsSameNode: `*0..0` binds a=b to the same
// node with an empty PathVal (per PathVal's own doc comment: both slices
// nil) for every root, including a root with no outgoing edges at all.
func TestExpandVarLengthZeroLengthBindsSameNode(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}}, // connected root
			{id: 2, kinds: []snapshot.KindID{kindRoot}}, // isolated root
			{id: 3},
		},
		[]execEdgeSpec{
			{id: 30, start: 1, end: 3, kind: kindE},
		},
	)

	rs := mustExec(t, snap, `MATCH p = (a:Root)-[:E*0..0]->(b) RETURN a, b, p`, generousBudget)
	if len(rs.Rows) != 2 {
		t.Fatalf("row count = %d, want 2\nrows: %v", len(rs.Rows), rs.Rows)
	}

	gotRoots := make(map[uint64]bool, 2)
	for _, row := range rs.Rows {
		a, b, p := row[0], row[1], row[2]
		if a.Kind != OutNode || b.Kind != OutNode {
			t.Fatalf("row = %v, want a and b both OutNode", row)
		}
		if a.Node != b.Node {
			t.Fatalf("row a=%v b=%v, want a == b (*0.. binds both endpoints to the same node)", a.Node, b.Node)
		}
		if p.Kind != OutPath {
			t.Fatalf("row p kind = %v, want OutPath", p.Kind)
		}
		if len(p.Path.Nodes) != 0 || len(p.Path.Edges) != 0 {
			t.Fatalf("zero-length PathVal = %+v, want both slices empty", p.Path)
		}
		gotRoots[snap.GraphIDs[a.Node]] = true
	}
	if !gotRoots[1] || !gotRoots[2] {
		t.Fatalf("roots seen = %v, want both database ids 1 (connected) and 2 (isolated)", gotRoots)
	}
}

// TestExpandVarLengthRangeCapHonored: `*1..2` over a 3-edge chain must
// enumerate depth-1 and depth-2 trails but never reach the 3rd hop.
func TestExpandVarLengthRangeCapHonored(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}},
			{id: 2}, {id: 3}, {id: 4},
		},
		[]execEdgeSpec{
			{id: 40, start: 1, end: 2, kind: kindE},
			{id: 41, start: 2, end: 3, kind: kindE},
			{id: 42, start: 3, end: 4, kind: kindE},
		},
	)

	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:E*1..2]->(b) RETURN p`, 0, []string{
		"N:1,2,|E:40,",
		"N:1,2,3,|E:40,41,",
	})
}

// TestExpandVarLengthMinDepthPostFilter: `*3..5` over the same 3-edge chain
// filters out the depth-1/depth-2 trails, keeping only the depth-3 trail
// that actually reaches the chain's end.
func TestExpandVarLengthMinDepthPostFilter(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}},
			{id: 2}, {id: 3}, {id: 4},
		},
		[]execEdgeSpec{
			{id: 50, start: 1, end: 2, kind: kindE},
			{id: 51, start: 2, end: 3, kind: kindE},
			{id: 52, start: 3, end: 4, kind: kindE},
		},
	)

	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:E*3..5]->(b) RETURN p`, 0, []string{
		"N:1,2,3,4,|E:50,51,52,",
	})
}

// TestExpandVarLengthFirstEdgeSelfLoopNotExtended: root `a` carries both a
// self-loop and a normal edge to `b`. The self-loop trail must appear at
// depth 1 and must NOT be extended into a depth-2 trail through it (a-a-b);
// the ordinary a-b trail is unaffected.
func TestExpandVarLengthFirstEdgeSelfLoopNotExtended(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}},
			{id: 2},
		},
		[]execEdgeSpec{
			{id: 100, start: 1, end: 1, kind: kindE}, // self-loop
			{id: 101, start: 1, end: 2, kind: kindE},
		},
	)

	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:E*1..3]->(x) RETURN p`, 0, []string{
		"N:1,1,|E:100,",
		"N:1,2,|E:101,",
	})
}

// TestExpandVarLengthMidPathSelfLoopExtendable: `x` (reached via a's normal
// edge, so the self-loop is not the trail's first edge) carries a self-loop
// that CAN be extended further -- both the direct a-x-y trail and the
// longer a-x-x-y trail through the self-loop must be enumerated.
func TestExpandVarLengthMidPathSelfLoopExtendable(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}}, // a
			{id: 2}, // x
			{id: 3, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 200, start: 1, end: 2, kind: kindE}, // a->x
			{id: 201, start: 2, end: 2, kind: kindE}, // x self-loop
			{id: 202, start: 2, end: 3, kind: kindE}, // x->y
		},
	)

	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:E*1..3]->(y:Target) RETURN p`, 0, []string{
		"N:1,2,3,|E:200,202,",
		"N:1,2,2,3,|E:200,201,202,",
	})
}

// TestExpandVarLengthNamedRelationshipVariableDeclined: a variable-length
// pattern with a named relationship variable has no value shape this
// package's Row/EdgeRef model can represent (real Cypher binds it to a
// *list* of relationships) -- Execute must decline rather than silently
// bind something wrong.
func TestExpandVarLengthNamedRelationshipVariableDeclined(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{{id: 1, kinds: []snapshot.KindID{kindRoot}}, {id: 2}},
		[]execEdgeSpec{{id: 1, start: 1, end: 2, kind: kindE}},
	)

	q := planQuery(t, snap, `MATCH (a:Root)-[r:E*1..3]->(b) RETURN b`)
	if _, err := Execute(&Env{Snap: snap}, q, generousBudget); !errors.Is(err, errUnsupportedStep) {
		t.Fatalf("Execute() error = %v, want errUnsupportedStep", err)
	}
}

// TestExpandMixedComponentDeclined: a component that mixes a var-length Step
// with an ordinary fixed-length Step sharing a symbol is outside this task's
// required shapes (see runComponent's doc comment) and must decline rather
// than guess at a composed semantics.
func TestExpandMixedComponentDeclined(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{{id: 1, kinds: []snapshot.KindID{kindRoot}}, {id: 2}, {id: 3}},
		[]execEdgeSpec{
			{id: 1, start: 1, end: 2, kind: kindE},
			{id: 2, start: 1, end: 3, kind: kindE},
		},
	)

	q := planQuery(t, snap, `MATCH (a:Root)-[:E*1..2]->(b), (a)-[:E]->(c) RETURN b, c`)
	if _, err := Execute(&Env{Snap: snap}, q, generousBudget); !errors.Is(err, errUnsupportedStep) {
		t.Fatalf("Execute() error = %v, want errUnsupportedStep", err)
	}
}

// TestExpandVarLengthBudgetExhaustion: an 8-node clique (56 directed edges)
// searched `*1..4` from its one Root-kind node has far more distinct trails
// than a tiny MaxWork budget allows; Execute must abort mid-enumeration with
// ErrBudget rather than complete the (very large) full search.
func TestExpandVarLengthBudgetExhaustion(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10

	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"}
	nodes := []execNodeSpec{{id: 1, kinds: []snapshot.KindID{kindRoot}}}
	for i := uint64(2); i <= 8; i++ {
		nodes = append(nodes, execNodeSpec{id: i})
	}
	var edges []execEdgeSpec
	nextEdgeID := uint64(1)
	for u := uint64(1); u <= 8; u++ {
		for v := uint64(1); v <= 8; v++ {
			if u == v {
				continue
			}
			edges = append(edges, execEdgeSpec{id: nextEdgeID, start: u, end: v, kind: kindE})
			nextEdgeID++
		}
	}
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	err := execExpectErr(t, snap, `MATCH p = (a:Root)-[:E*1..4]->(b) RETURN p`, Budgets{MaxRows: 1_000_000, MaxWork: 1200})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("Execute() error = %v, want ErrBudget", err)
	}
}

// --- shortestPath / allShortestPaths execution ------------------------------

// shortestPathParityFixture builds a small diamond snapshot with two
// co-equal length-2 shortest paths (s-a-t and s-b-t) between one Root node
// and one Target node, plus a helper edge-kind mask -- shared by the
// ModeOne/ModeAll parity subtests below.
func shortestPathParityFixture(t *testing.T) (*snapshot.Snapshot, *snapshot.KindMask) {
	t.Helper()
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}}, // s
			{id: 2}, // a
			{id: 3}, // b
			{id: 4, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 300, start: 1, end: 2, kind: kindE},
			{id: 301, start: 2, end: 4, kind: kindE},
			{id: 302, start: 1, end: 3, kind: kindE},
			{id: 303, start: 3, end: 4, kind: kindE},
		},
	)
	mask := snapshot.NewKindMask(snap.MaxKindID)
	mask.Set(kindE)
	return snap, mask
}

// TestExpandShortestPathMatchesTraverseDirectly runs shortestPath() and
// allShortestPaths() through the full Plan/Execute pipeline over a corpus-
// shaped pattern (kind-constrained endpoints, an edge-kind disjunction,
// unbounded `*1..`, an explicit `s<>t`, mirroring
// testdata/prebuilt_shortest_path.json's shape minus its LIMIT, which Task 9
// -- not this one -- implements) and asserts the resulting paths equal a
// traverse.AllShortestPaths call built independently from the query's own
// semantics (same roots/terminals/kind mask/mode/ExcludeSelf), not merely
// re-using expand.go's own code path.
func TestExpandShortestPathMatchesTraverseDirectly(t *testing.T) {
	snap, mask := shortestPathParityFixture(t)
	env := &Env{Snap: snap}

	t.Run("shortestPath (ModeOne)", func(t *testing.T) {
		dense, err := traverse.AllShortestPaths(snap, traverse.Query{
			Roots:       traverse.Endpoint{IDs: []snapshot.NodeID{0}},
			Terminals:   traverse.Endpoint{IDs: []snapshot.NodeID{3}},
			Kinds:       mask,
			Mode:        traverse.ModeOne,
			ExcludeSelf: true,
		})
		if err != nil {
			t.Fatalf("traverse.AllShortestPaths: %v", err)
		}
		want := make([]string, len(dense))
		for i, p := range dense {
			want[i] = pathSig(snap, convertPath(env, p))
		}

		assertPathSigs(t, snap,
			`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, 0, want)
	})

	t.Run("allShortestPaths (ModeAll)", func(t *testing.T) {
		dense, err := traverse.AllShortestPaths(snap, traverse.Query{
			Roots:       traverse.Endpoint{IDs: []snapshot.NodeID{0}},
			Terminals:   traverse.Endpoint{IDs: []snapshot.NodeID{3}},
			Kinds:       mask,
			Mode:        traverse.ModeAll,
			ExcludeSelf: true,
		})
		if err != nil {
			t.Fatalf("traverse.AllShortestPaths: %v", err)
		}
		want := make([]string, len(dense))
		for i, p := range dense {
			want[i] = pathSig(snap, convertPath(env, p))
		}
		if len(want) != 2 {
			t.Fatalf("fixture sanity check: traverse found %d shortest paths, want 2 (s-a-t and s-b-t)", len(want))
		}

		assertPathSigs(t, snap,
			`MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, 0, want)
	})
}

// TestExpandShortestPathEndpointPredicateNarrowsSet: a pushed single-symbol
// WHERE predicate over a shortestPath endpoint (`s.active = true`) must
// narrow the resolved root set exactly like it would narrow pg's own seed
// query, not merely rely on Execute's later Part.Where pass over already-
// produced rows -- verified by comparing against a traverse call seeded with
// only the one node whose property actually matches.
func TestExpandShortestPathEndpointPredicateNarrowsSet(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}, props: map[string]any{"active": true}},
			{id: 2, kinds: []snapshot.KindID{kindRoot}, props: map[string]any{"active": false}},
			{id: 3, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 400, start: 1, end: 3, kind: kindE},
			{id: 401, start: 2, end: 3, kind: kindE},
		},
	)
	env := &Env{Snap: snap}
	mask := snapshot.NewKindMask(snap.MaxKindID)
	mask.Set(kindE)

	dense, err := traverse.AllShortestPaths(snap, traverse.Query{
		Roots:       traverse.Endpoint{IDs: []snapshot.NodeID{0}}, // only node 1 (the active root)
		Terminals:   traverse.Endpoint{IDs: []snapshot.NodeID{2}},
		Kinds:       mask,
		Mode:        traverse.ModeOne,
		ExcludeSelf: true,
	})
	if err != nil {
		t.Fatalf("traverse.AllShortestPaths: %v", err)
	}
	want := make([]string, len(dense))
	for i, p := range dense {
		want[i] = pathSig(snap, convertPath(env, p))
	}
	if len(want) != 1 {
		t.Fatalf("fixture sanity check: traverse found %d shortest paths, want 1", len(want))
	}

	assertPathSigs(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s.active = true AND s<>t RETURN p`, 0, want)
}

// TestExpandShortestPathSelfEndpointWithoutInequality: root and terminal
// resolve to the same kind-constrained set (they overlap) and the query
// carries no `s<>t`/`id(s)<>id(t)` -- Execute must decline with
// ErrSelfEndpoint rather than serve pg's own SQLSTATE-22023 case itself.
func TestExpandShortestPathSelfEndpointWithoutInequality(t *testing.T) {
	const kindRoot snapshot.KindID = 1
	const kindE snapshot.KindID = 10
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}},
			{id: 2, kinds: []snapshot.KindID{kindRoot}},
		},
		[]execEdgeSpec{{id: 1, start: 1, end: 2, kind: kindE}},
	)

	err := execExpectErr(t, snap, `MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Root)) RETURN p`, generousBudget)
	if !errors.Is(err, ErrSelfEndpoint) {
		t.Fatalf("Execute() error = %v, want ErrSelfEndpoint", err)
	}
}

// TestExpandShortestPathSelfEndpointWithInequalityDropsSelfPairs: the same
// intersecting endpoint sets as above, but with an explicit `s<>t` --
// Execute must proceed (no ErrSelfEndpoint) and never emit a path whose root
// and terminal are the same node (traverse's own ExcludeSelf/zero-length
// guarantees this; this test asserts Execute actually wires
// HasExplicitEndpointInequality through rather than declining anyway).
func TestExpandShortestPathSelfEndpointWithInequalityDropsSelfPairs(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot, kindTarget}}, // both Root and Target: sets overlap
			{id: 2, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{{id: 1, start: 1, end: 2, kind: kindE}},
	)
	env := &Env{Snap: snap}
	mask := snapshot.NewKindMask(snap.MaxKindID)
	mask.Set(kindE)

	dense, err := traverse.AllShortestPaths(snap, traverse.Query{
		Roots:       traverse.Endpoint{IDs: []snapshot.NodeID{0}},
		Terminals:   traverse.Endpoint{IDs: []snapshot.NodeID{0, 1}},
		Kinds:       mask,
		Mode:        traverse.ModeOne,
		ExcludeSelf: true,
	})
	if err != nil {
		t.Fatalf("traverse.AllShortestPaths: %v", err)
	}
	want := make([]string, len(dense))
	for i, p := range dense {
		want[i] = pathSig(snap, convertPath(env, p))
	}
	if len(want) != 1 {
		t.Fatalf("fixture sanity check: traverse found %d shortest paths, want 1", len(want))
	}

	assertPathSigs(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, 0, want)
}
