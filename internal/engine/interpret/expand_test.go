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
func pathSig(snap *snapshot.View, pv *PathVal) string {
	s := "N:"
	for _, n := range pv.Nodes {
		s += fmt.Sprintf("%d,", snap.GraphID(n))
	}
	s += "|E:"
	for _, e := range pv.Edges {
		s += fmt.Sprintf("%d,", snap.Base().OutEdgeIDs[e.Fwd])
	}
	return s
}

// pathSigsAtColumn extracts pathSig for every row's col'th projected value,
// asserting each is actually OutPath, sorted for order-independent
// comparison.
func pathSigsAtColumn(t *testing.T, snap *snapshot.View, rs *ResultSet, col int) []string {
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
func assertPathSigs(t *testing.T, snap *snapshot.View, query string, col int, want []string) {
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
func execExpectErr(t *testing.T, snap *snapshot.View, query string, b Budgets) error {
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
		gotRoots[snap.GraphID(a.Node)] = true
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

// TestExpandVarLengthSelfLoopHazardDeclines: any self-loop of an admitted
// kind in the view -- whether it would sit on the trail's first edge or
// mid-trail -- declines the whole var-length pattern instead of serving it:
// pg's recursive CTE applies its self-loop dead-end guard to whichever
// pattern edge sits on the CTE's own seed side, chosen per query by dawgs
// heuristics this engine deliberately does not mirror (see expand.go's
// SELF-LOOPS package-doc bullet). A second pattern over a kind with no
// self-loops must keep serving from the same snapshot.
func TestExpandVarLengthSelfLoopHazardDeclines(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
		kindF      snapshot.KindID = 11
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E", kindF: "F"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}}, // a
			{id: 2}, // x
			{id: 3, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 200, start: 1, end: 2, kind: kindE}, // a->x
			{id: 201, start: 2, end: 2, kind: kindE}, // x self-loop: the hazard
			{id: 202, start: 2, end: 3, kind: kindE}, // x->y
			{id: 203, start: 1, end: 3, kind: kindF}, // a->y, self-loop-free kind
		},
	)

	if err := execExpectErr(t, snap, `MATCH p = (a:Root)-[:E*1..3]->(y:Target) RETURN p`, generousBudget); !errors.Is(err, errUnsupportedStep) {
		t.Fatalf("E-kind pattern: err = %v, want errUnsupportedStep", err)
	}
	// An empty relationship-type list admits every kind, so the E self-loop
	// is a hazard for it too.
	if err := execExpectErr(t, snap, `MATCH p = (a:Root)-[*1..3]->(y:Target) RETURN p`, generousBudget); !errors.Is(err, errUnsupportedStep) {
		t.Fatalf("any-kind pattern: err = %v, want errUnsupportedStep", err)
	}
	assertPathSigs(t, snap, `MATCH p = (a:Root)-[:F*1..3]->(y:Target) RETURN p`, 0, []string{
		"N:1,3,|E:203,",
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
// with an ordinary fixed-length Step sharing a symbol is outside this
// package's supported shapes (see runComponent's doc comment) and must decline rather
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
func shortestPathParityFixture(t *testing.T) (*snapshot.View, *snapshot.KindMask) {
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
	mask := snapshot.NewKindMask(snap.Base().MaxKindID)
	mask.Set(kindE)
	return snap, mask
}

// TestExpandShortestPathMatchesTraverseDirectly runs shortestPath() and
// allShortestPaths() through the full Plan/Execute pipeline over a corpus-
// shaped pattern (kind-constrained endpoints, an edge-kind disjunction,
// unbounded `*1..`, an explicit `s<>t`, mirroring
// testdata/prebuilt_shortest_path.json's shape minus its LIMIT, which
// pipeline.go implements) and asserts the resulting paths equal a
// hardcoded expected signature set, computed by hand from the fixture's own
// known graph shape rather than derived at test time via convertPath (the
// very function these rows are produced through) or a second
// traverse.AllShortestPaths call: either would make the assertion
// structurally blind to a regression in the thing it's supposed to be
// checking, since a bug shared by both the "got" and "want" side of a
// comparison never shows up as a mismatch. See TestExpandVarLengthDiamondBothTrails
// and its siblings for the same hardcoded-signature style this mirrors.
//
// shortestPathParityFixture is a symmetric diamond: s-a-t (edges 300, 301)
// and s-b-t (edges 302, 303) are both length-2 shortest paths. ModeOne's
// single winner (s-b-t) follows directly from traverse's own documented
// pairEnumerate mechanics (bfs.go): s's out-adjacency yields a (edge 300)
// before b (edge 302), pairEnumerate pushes candidate hops onto an explicit
// LIFO stack in that same order, so the last-pushed branch (via b) is popped
// and walked to completion -- and therefore found -- first.
func TestExpandShortestPathMatchesTraverseDirectly(t *testing.T) {
	snap, _ := shortestPathParityFixture(t)

	t.Run("shortestPath (ModeOne)", func(t *testing.T) {
		assertPathSigs(t, snap,
			`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, 0,
			[]string{"N:1,3,4,|E:302,303,"})
	})

	t.Run("allShortestPaths (ModeAll)", func(t *testing.T) {
		assertPathSigs(t, snap,
			`MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, 0,
			[]string{
				"N:1,2,4,|E:300,301,",
				"N:1,3,4,|E:302,303,",
			})
	})

	// The identical pattern written with a backward
	// arrow (`(t)<-[...]-  (s)`) must render p's node/edge sequence in the
	// pattern's WRITTEN order (t first, then s) -- the exact reverse of the
	// forward-arrow cases above, which is also the exact reverse of the
	// TRAVERSAL order buildStep's inbound-arrow swap makes the executor
	// actually walk in (FromSym=s, ToSym=t internally either way). A
	// live-pg differential probe against the corpus's own
	// `shortestPath((t:Group)<-[:R*1..]-(s:Base))` shape found this
	// reversed before the fix (reversePathVal, expand.go).
	t.Run("shortestPath (ModeOne), backward arrow renders written order", func(t *testing.T) {
		assertPathSigs(t, snap,
			`MATCH p = shortestPath((t:Target)<-[:E*1..]-(s:Root)) WHERE s<>t RETURN p`, 0,
			[]string{"N:4,3,1,|E:303,302,"})
	})
}

// TestExpandVarLengthBackwardArrowNamedPathWrittenOrder is a regression
// test for the standalone (non-shortestPath) var-length case:
// a named path pattern written with a backward arrow
// (`(b:Target)<-[:E*1..3]-(a:Root)`) must render p's Nodes/Edges in the
// pattern's WRITTEN order -- b first, then the intermediate node(s), then a
// last -- not the TRAVERSAL order (a first) buildStep's inbound-arrow swap
// makes expandVarLengthTrailsForSeed actually walk in. Mirrors
// TestExpandVarLengthDiamondBothTrails' own single-branch fixture (a-x-b),
// written backward.
func TestExpandVarLengthBackwardArrowNamedPathWrittenOrder(t *testing.T) {
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
			{id: 4, kinds: []snapshot.KindID{kindTarget}},
		},
		[]execEdgeSpec{
			{id: 10, start: 1, end: 2, kind: kindE}, // a->x
			{id: 11, start: 2, end: 4, kind: kindE}, // x->b
		},
	)

	assertPathSigs(t, snap, `MATCH p = (b:Target)<-[:E*1..3]-(a:Root) RETURN p`, 0, []string{
		"N:4,2,1,|E:11,10,",
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

	// Only node 1 (the active root) may seed the search, so the only
	// possible path is s(1)-t(3) via edge 400; node 2's edge 401 must never
	// appear. Hardcoded rather than derived via convertPath or a second
	// traverse.AllShortestPaths call -- see TestExpandShortestPathMatchesTraverseDirectly's
	// doc comment for why.
	assertPathSigs(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s.active = true AND s<>t RETURN p`, 0,
		[]string{"N:1,3,|E:400,"})
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

	// Roots = {1}, Terminals = {1, 2}; s<>t drops the (1,1) self-pair, so
	// the only surviving path is 1-2 via edge 1. Hardcoded rather than
	// derived via convertPath or a second traverse.AllShortestPaths call --
	// see TestExpandShortestPathMatchesTraverseDirectly's doc comment for
	// why.
	assertPathSigs(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, 0,
		[]string{"N:1,2,|E:1,"})
}

// TestExpandShortestPathKindsOnlySideNotMaterialized: a shortestPath source
// symbol constrained by a kind label alone (no ids/objectid/predicates) must
// be handed to traverse as a lazy kind bitmap rather than materialized one
// candidate at a time -- 200 `Base`-kind nodes as the source side, a
// 2-member `Target` kind narrowed further to exactly one node via an
// objectid predicate on the terminal side (forcing the terminal's own
// resolution through the narrowing/materialized path, so this test isolates
// the source side's own behavior).
//
// Correctness (the query still serves the one real path) and cost (the
// query's own total work stays far below what materializing all 200 source
// candidates would have cost) are both asserted: materializing that side the
// old way spends exactly 2 work units per candidate (one to admit it, one
// for the row it produces -- see scanAnchorVisit's own doc comment), a cost
// this test measures directly via resolveEndpointSet (still the mechanism a
// narrowing side uses) against the identical 200-node kind constraint,
// rather than hard-coding the arithmetic, so this test keeps working even if
// that per-candidate charge ever changes.
func TestExpandShortestPathKindsOnlySideNotMaterialized(t *testing.T) {
	const (
		kindBase   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
		numBase                    = 200
	)

	var nodes []execNodeSpec
	for i := uint64(1); i <= numBase; i++ {
		nodes = append(nodes, execNodeSpec{id: i, kinds: []snapshot.KindID{kindBase}})
	}
	nodes = append(nodes,
		execNodeSpec{id: 300, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"objectid": "target-300"}},
		execNodeSpec{id: 301, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"objectid": "target-301"}},
	)
	edges := []execEdgeSpec{
		{id: 1000, start: 1, end: 300, kind: kindE},
	}
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindBase: "Base", kindTarget: "Target", kindE: "E"},
		nodes, edges)

	// Calibrate: what resolveEndpointSet itself would charge to materialize
	// this exact 200-node kind-only candidate set, measured directly rather
	// than assumed.
	calibMeter := &workMeter{budget: generousBudget}
	baseNC := &NodeConstraint{Kinds: []snapshot.KindID{kindBase}}
	if _, err := resolveEndpointSet(&Env{Snap: snap}, calibMeter, "s", baseNC); err != nil {
		t.Fatalf("resolveEndpointSet calibration: %v", err)
	}
	materializedCost := calibMeter.work
	if materializedCost < 2*numBase {
		t.Fatalf("calibration: materializedCost = %d, want >= %d (2 work units per candidate)", materializedCost, 2*numBase)
	}

	q := planQuery(t, snap, `MATCH p = shortestPath((s:Base)-[:E*1..]->(t:Target)) WHERE t.objectid = 'target-300' AND s<>t RETURN p`)
	meter := &workMeter{budget: generousBudget}
	rs, err := runQuery(&Env{Snap: snap}, q, meter)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}

	got := pathSigsAtColumn(t, snap, rs, 0)
	want := []string{"N:1,300,|E:1000,"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("path set mismatch\ngot:  %v\nwant: %v", got, want)
	}

	if meter.work >= materializedCost/2 {
		t.Fatalf("meter.work = %d, want far below the %d-unit cost of materializing the wide Base side (kinds-only side was materialized despite the fix)", meter.work, materializedCost)
	}
}

// TestExpandShortestPathLimitPushdownServesWithinBudget: mirrors
// TestExpandShortestPathBudgetDeclinesOnHighFanOut's fixture -- one root and
// one target joined by many (fanOut) distinct co-equal shortest paths -- but
// runs it through the full runQuery pipeline with a `LIMIT 3` appended, no
// ORDER BY/DISTINCT and no residual WHERE beyond the endpoint inequality.
// Under the same tiny MaxWork the un-limited query legitimately declines
// ErrBudget with (more true paths exist than the remaining work budget could
// ever admit -- confirmed directly below as a baseline), reaching traverse's
// own Query.Limit with the user's LIMIT instead of the full rowCap+1 lets
// enumeration stop the moment 3 dense paths exist, comfortably inside
// budget, so the query must SERVE exactly 3 rows rather than decline.
func TestExpandShortestPathLimitPushdownServesWithinBudget(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"}
	nodes := []execNodeSpec{
		{id: 1, kinds: []snapshot.KindID{kindRoot}},
		{id: 2, kinds: []snapshot.KindID{kindTarget}},
	}
	var edges []execEdgeSpec
	nextEdgeID := uint64(1)
	const fanOut = 200
	for i := uint64(0); i < fanOut; i++ {
		mid := 100 + i
		nodes = append(nodes, execNodeSpec{id: mid})
		edges = append(edges,
			execEdgeSpec{id: nextEdgeID, start: 1, end: mid, kind: kindE},
			execEdgeSpec{id: nextEdgeID + 1, start: mid, end: 2, kind: kindE},
		)
		nextEdgeID += 2
	}
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	budget := Budgets{MaxRows: 1_000_000, MaxWork: 10}

	// Baseline: the identical query with no LIMIT still declines ErrBudget
	// under this tiny MaxWork -- proof the budget really is the binding
	// constraint this test's own LIMIT must overcome, not something already
	// fixed elsewhere (or too generous a budget to exercise anything).
	baseErr := execExpectErr(t, snap, `MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, budget)
	if !errors.Is(baseErr, ErrBudget) {
		t.Fatalf("no-LIMIT baseline: error = %v, want ErrBudget", baseErr)
	}

	rs := mustExec(t, snap, `MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p LIMIT 3`, budget)
	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rs.Rows))
	}
}

// TestExpandShortestPathLimitPushdownStillDeclinesOnResidualWhere: the same
// high-fan-out fixture and the same LIMIT as
// TestExpandShortestPathLimitPushdownServesWithinBudget, but with an
// additional residual cross-symbol WHERE conjunct (`s.group = t.group`) Plan
// cannot push into either endpoint's own NodeConstraint.Predicates. The
// pushdown must stay off for this query -- serving a truncated first-3-dense
// prefix here could silently drop paths Part.Where's own post-executor pass
// would have kept while enumeration never even reached them -- so this must
// still decline ErrBudget exactly like the no-LIMIT case, not silently
// under-serve fewer than 3 correct rows.
func TestExpandShortestPathLimitPushdownStillDeclinesOnResidualWhere(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"}
	nodes := []execNodeSpec{
		{id: 1, kinds: []snapshot.KindID{kindRoot}, props: map[string]any{"group": "A"}},
		{id: 2, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"group": "A"}},
	}
	var edges []execEdgeSpec
	nextEdgeID := uint64(1)
	const fanOut = 200
	for i := uint64(0); i < fanOut; i++ {
		mid := 100 + i
		nodes = append(nodes, execNodeSpec{id: mid})
		edges = append(edges,
			execEdgeSpec{id: nextEdgeID, start: 1, end: mid, kind: kindE},
			execEdgeSpec{id: nextEdgeID + 1, start: mid, end: 2, kind: kindE},
		)
		nextEdgeID += 2
	}
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	err := execExpectErr(t, snap,
		`MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s.group = t.group AND s<>t RETURN p LIMIT 3`,
		Budgets{MaxRows: 1_000_000, MaxWork: 10})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("error = %v, want ErrBudget (residual WHERE must block the LIMIT pushdown)", err)
	}
}

// TestExpandShortestPathLimitPushdownStillDeclinesWhenLimitExceedsBudget:
// same fixture again, but with a LIMIT larger than the tiny MaxWork budget
// could ever admit -- min(target, rowCap+1) must still floor at rowCap+1, so
// this declines ErrBudget exactly as the un-limited query does: the BUDGET,
// not the user's LIMIT, is the binding constraint here, and the pushdown
// must never treat a large LIMIT as license to widen the budget's own cap.
func TestExpandShortestPathLimitPushdownStillDeclinesWhenLimitExceedsBudget(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"}
	nodes := []execNodeSpec{
		{id: 1, kinds: []snapshot.KindID{kindRoot}},
		{id: 2, kinds: []snapshot.KindID{kindTarget}},
	}
	var edges []execEdgeSpec
	nextEdgeID := uint64(1)
	const fanOut = 200
	for i := uint64(0); i < fanOut; i++ {
		mid := 100 + i
		nodes = append(nodes, execNodeSpec{id: mid})
		edges = append(edges,
			execEdgeSpec{id: nextEdgeID, start: 1, end: mid, kind: kindE},
			execEdgeSpec{id: nextEdgeID + 1, start: mid, end: 2, kind: kindE},
		)
		nextEdgeID += 2
	}
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	err := execExpectErr(t, snap,
		`MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p LIMIT 1000`,
		Budgets{MaxRows: 1_000_000, MaxWork: 10})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("error = %v, want ErrBudget (a LIMIT the budget cannot afford must still decline)", err)
	}
}

// TestShortestPathLimit (I2, white-box): directly exercises
// shortestPathLimit's four independent conditions -- this is the one place
// the I2 fix's actual effect (which value shortestPathLimit itself returns)
// is checked directly, deliberately bypassing traverse.AllShortestPaths.
// That bypass matters: traverse's own memory-budget backstop is calibrated
// off the query's worst-case possible path depth (shortestPathBudget's own
// memLimit doc), not the depth paths in a given graph actually turn out to
// have, so on every fixture this file's black-box shortestPath LIMIT tests
// can build it is far looser than rowCapPlusOne and independently produces
// the identical final ErrBudget a broken shortestPathLimit would also
// produce -- confirmed by direct instrumentation while developing this fix:
// reintroducing the I2 bug (accepting limitTarget == 0) against
// TestShortestPathLimitZeroDoesNotDisableTraverseEnumerationCutoff's tight-
// MaxWork/high-fan-out fixture makes traverse enumerate 31 real dense paths
// via that looser memory backstop before erroring, instead of the 11 a
// correctly-guarded rowCapPlusOne cutoff enumerates -- yet both surface as
// the same ErrBudget to the caller, so no assertion on Execute's return
// value alone can tell the bug apart from the fix on that fixture, or on
// any fixture: shortestPathBudget's memLimit is *always* at least as loose
// as rowCapPlusOne (it is derived from the same rowCap, scaled up by a
// worst-case-depth/actual-depth ratio that is always >= 1), so the
// len(dense) > rowCap decline below fires at-or-before whatever count the
// memory backstop would have stopped at regardless of which value
// shortestPathLimit returns. TestShortestPathLimitZeroDoesNotDisableTraverseEnumerationCutoff
// (below) still pins the end-to-end behavior -- correct regardless of which
// mechanism produces it -- but this test is what would actually have caught
// the bug.
func TestShortestPathLimit(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"}
	snap := buildExecSnapshot(t, kinds,
		[]execNodeSpec{
			{1, []snapshot.KindID{kindRoot}, nil},
			{2, []snapshot.KindID{kindTarget}, nil},
		},
		[]execEdgeSpec{{100, 1, 2, kindE}},
	)

	noResidualPart, noResidualStep := shortestPathPartAndStep(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`)
	residualPart, residualStep := shortestPathPartAndStep(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s.group = t.group AND s<>t RETURN p`)

	tests := []struct {
		name           string
		limitTargetSet bool
		limitTarget    int64
		rowCapPlusOne  int64
		part           *Part
		step           *Step
		want           int64
	}{
		{
			name:          "no target threaded in: falls back to rowCapPlusOne",
			rowCapPlusOne: 11,
			part:          noResidualPart,
			step:          noResidualStep,
			want:          11,
		},
		{
			// The I2 case: without the `limitTarget > 0` guard, this would
			// wrongly return 0 -- which traverse.Query.Limit reads as
			// "unbounded" (traverse.go), the opposite of a cutoff.
			name:           "target zero (LIMIT 0): falls back to rowCapPlusOne, never 0",
			limitTargetSet: true,
			limitTarget:    0,
			rowCapPlusOne:  11,
			part:           noResidualPart,
			step:           noResidualStep,
			want:           11,
		},
		{
			name:           "target positive and smaller than the cap: narrows to target",
			limitTargetSet: true,
			limitTarget:    3,
			rowCapPlusOne:  11,
			part:           noResidualPart,
			step:           noResidualStep,
			want:           3,
		},
		{
			name:           "target at least as large as the cap: never widens",
			limitTargetSet: true,
			limitTarget:    11,
			rowCapPlusOne:  11,
			part:           noResidualPart,
			step:           noResidualStep,
			want:           11,
		},
		{
			name:           "residual WHERE present: never narrows even for an otherwise-eligible target",
			limitTargetSet: true,
			limitTarget:    3,
			rowCapPlusOne:  11,
			part:           residualPart,
			step:           residualStep,
			want:           11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meter := &workMeter{limitTargetSet: tt.limitTargetSet, limitTarget: tt.limitTarget}
			got := shortestPathLimit(tt.rowCapPlusOne, meter, tt.part, tt.step)
			if got != tt.want {
				t.Fatalf("shortestPathLimit() = %d, want %d", got, tt.want)
			}
		})
	}
}

// shortestPathPartAndStep plans query against snap and returns its single
// shortestPath component's Part and Step, for tests that call
// shortestPathLimit/noResidualWhere directly rather than through the full
// runQuery/Execute pipeline (mirrors the groupComponents/Chains lookup
// TestExpandShortestPathBudgetDeclinesOnHighFanOut already uses inline).
func shortestPathPartAndStep(t *testing.T, snap *snapshot.View, query string) (*Part, *Step) {
	t.Helper()
	q := planQuery(t, snap, query)
	part := &q.Parts[0]
	comps := groupComponents(part)
	if len(comps) != 1 || len(comps[0].stepIdxs) != 1 {
		t.Fatalf("unexpected component shape for query %q: %+v", query, comps)
	}
	return part, &part.Chains[comps[0].stepIdxs[0]]
}

// TestShortestPathLimitZeroDoesNotDisableTraverseEnumerationCutoff (I2):
// end-to-end pin of LIMIT 0's observable behavior through the full
// Execute pipeline. traverse.Query.Limit's own contract is "0 => unbounded"
// (traverse.go), the OPPOSITE of what a literal `LIMIT 0` should ever
// cause, so this must be guarded off (shortestPathLimit, tested directly
// above) and fall back to shortestPathBudget's own pre-existing rowCap+1
// cutoff instead. As TestShortestPathLimit's own doc explains, this
// end-to-end test cannot by itself distinguish the guarded fix from the I2
// bug -- traverse's own memory-budget backstop happens to produce the same
// final ErrBudget either way on any fixture reachable from here -- but it
// still pins that the observable behavior is correct, and would catch a
// wrong *alternative* fix (e.g. special-casing LIMIT 0 to skip the
// component and return an empty success unconditionally, bypassing the
// budget check entirely).
//
// Two fixtures pin both directions:
//   - A budget that comfortably affords the whole (small) result: LIMIT 0
//     must still SERVE (not decline) with exactly 0 rows -- ordinary SQL
//     LIMIT 0 semantics, unaffected by which internal cap traverse used.
//   - The high-fan-out fixture under the same tight MaxWork that makes the
//     unlimited query decline (TestExpandShortestPathBudgetDeclinesOnHighFanOut):
//     LIMIT 0 must ALSO decline ErrBudget, proving the enumeration cutoff
//     still applies -- not a hang (traverse never learns to stop early) and
//     not an empty success (silently reporting "0 rows, no error" for a
//     query the budget genuinely cannot afford would hide a real
//     over-budget condition behind LIMIT 0's own "0 rows" shape).
func TestShortestPathLimitZeroDoesNotDisableTraverseEnumerationCutoff(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"}

	t.Run("affordable budget serves zero rows, not an error", func(t *testing.T) {
		snap := buildExecSnapshot(t, kinds,
			[]execNodeSpec{
				{1, []snapshot.KindID{kindRoot}, nil},
				{2, []snapshot.KindID{kindTarget}, nil},
			},
			[]execEdgeSpec{{100, 1, 2, kindE}},
		)
		rs := mustExec(t, snap,
			`MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p LIMIT 0`,
			generousBudget)
		if len(rs.Rows) != 0 {
			t.Fatalf("got %d rows, want 0 (LIMIT 0)", len(rs.Rows))
		}
	})

	t.Run("tight budget on high fan-out still declines ErrBudget", func(t *testing.T) {
		nodes := []execNodeSpec{
			{1, []snapshot.KindID{kindRoot}, nil},
			{2, []snapshot.KindID{kindTarget}, nil},
		}
		var edges []execEdgeSpec
		nextEdgeID := uint64(1)
		const fanOut = 200
		for i := uint64(0); i < fanOut; i++ {
			mid := 100 + i
			nodes = append(nodes, execNodeSpec{mid, nil, nil})
			edges = append(edges,
				execEdgeSpec{nextEdgeID, 1, mid, kindE},
				execEdgeSpec{nextEdgeID + 1, mid, 2, kindE},
			)
			nextEdgeID += 2
		}
		snap := buildExecSnapshot(t, kinds, nodes, edges)

		err := execExpectErr(t, snap,
			`MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p LIMIT 0`,
			Budgets{MaxRows: 1_000_000, MaxWork: 10})
		if !errors.Is(err, ErrBudget) {
			t.Fatalf("error = %v, want ErrBudget (LIMIT 0 must not disable the budget's own enumeration cutoff)", err)
		}
	})
}

// --- shortestPath / allShortestPaths budget wiring --------------------------

// TestExpandShortestPathBudget exercises shortestPathBudget's arithmetic
// directly: the component cap is derived *purely* from Budgets.MaxWork's
// remaining capacity -- Budgets.MaxRows never contributes, since MaxRows
// counts rows admitted after Part.Where filtering (workMeter.addFinalRow)
// everywhere else in this package, while this component's own dense output
// is still pre-filter (see shortestPathBudget's doc comment). "No MaxWork
// budget set at all" reports unbounded regardless of MaxRows
// (traverse.Query.Limit/MemoryLimit left at their own zero/"unbounded"
// value), and a MaxWork budget already exhausted (remaining < 0) clamps to
// zero rather than going negative -- a negative rowCap would make
// expandShortestPathComponent's own `rowCap+1` overflow back toward a
// nonsensical (or even negative) traverse.Query.Limit instead of declining
// outright.
func TestExpandShortestPathBudget(t *testing.T) {
	cases := []struct {
		name       string
		budget     Budgets
		work       int64
		finalRows  int
		wantBound  bool // true => a finite rowCap is expected (not unbounded)
		wantRowCap int
	}{
		{
			name:      "no budget set at all is unbounded",
			budget:    Budgets{},
			wantBound: false,
		},
		{
			name:      "MaxRows alone bounds nothing: no MaxWork means unbounded regardless of MaxRows/finalRows",
			budget:    Budgets{MaxRows: 20},
			finalRows: 5,
			wantBound: false,
		},
		{
			name:       "MaxWork only",
			budget:     Budgets{MaxWork: 100},
			work:       30,
			wantBound:  true,
			wantRowCap: 70,
		},
		{
			name:       "MaxRows set alongside MaxWork has no effect on rowCap -- only MaxWork's remaining capacity does",
			budget:     Budgets{MaxRows: 5, MaxWork: 1000},
			work:       10,
			wantBound:  true,
			wantRowCap: 990,
		},
		{
			name:       "MaxWork already exhausted clamps to zero, not negative",
			budget:     Budgets{MaxWork: 10},
			work:       25,
			wantBound:  true,
			wantRowCap: 0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			meter := &workMeter{budget: c.budget, work: c.work, finalRows: c.finalRows}
			rowCap, memLimit, unbounded := shortestPathBudget(meter, 0)
			if unbounded == c.wantBound {
				t.Fatalf("unbounded = %v, want %v", unbounded, !c.wantBound)
			}
			if unbounded {
				return
			}
			if rowCap != c.wantRowCap {
				t.Fatalf("rowCap = %d, want %d", rowCap, c.wantRowCap)
			}
			if memLimit == 0 {
				t.Fatalf("memLimit = 0, want a positive byte cap for a bounded query")
			}
		})
	}
}

// TestExpandShortestPathBudgetDeclinesOnHighFanOut: allShortestPaths()
// (ModeAll) between one root and one target joined by many distinct
// length-2 co-equal shortest paths must decline with ErrBudget under a
// small work budget *without* first materializing anywhere near the full
// fan-out's worth of dense paths. traverse.AllShortestPaths' own
// strategy-selection guards (PairBudget/SideBudget) only bound how many
// (root, terminal) *pairs*/BFS runs a strategy attempts -- with exactly one
// root and one target this fixture always qualifies for the cheapest
// strategy regardless of fan-out, so nothing in traverse's own dispatch
// would ever refuse it on that basis alone.
//
// This calls expandShortestPathComponent directly (the way runComponent
// does) rather than going through Execute, and inspects the workMeter
// afterward, deliberately bypassing Execute's own unconditional
// end-of-query work check (exec.go's meter.check(), run once before any
// successful ResultSet is returned): that check is a correctness safety net
// that will eventually surface ErrBudget for an over-budget result no
// matter how wastefully it was produced, so asserting only "Execute()
// returns ErrBudget" would pass even if expandShortestPathComponent fully
// materialized every one of the fan-out's paths first and only got caught
// downstream. Checking meter.work directly isolates the actual property
// this fix adds: traverse.AllShortestPaths itself must never be allowed to
// return anywhere near the full fan-out's worth of paths in the first
// place (see shortestPathBudget's own doc comment).
func TestExpandShortestPathBudgetDeclinesOnHighFanOut(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"}
	nodes := []execNodeSpec{
		{id: 1, kinds: []snapshot.KindID{kindRoot}},
		{id: 2, kinds: []snapshot.KindID{kindTarget}},
	}
	var edges []execEdgeSpec
	nextEdgeID := uint64(1)
	const fanOut = 200
	for i := uint64(0); i < fanOut; i++ {
		mid := 100 + i
		nodes = append(nodes, execNodeSpec{id: mid})
		edges = append(edges,
			execEdgeSpec{id: nextEdgeID, start: 1, end: mid, kind: kindE},
			execEdgeSpec{id: nextEdgeID + 1, start: mid, end: 2, kind: kindE},
		)
		nextEdgeID += 2
	}
	snap := buildExecSnapshot(t, kinds, nodes, edges)

	q := planQuery(t, snap, `MATCH p = allShortestPaths((s:Root)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`)
	part := &q.Parts[0]
	comps := groupComponents(part)
	if len(comps) != 1 || len(comps[0].stepIdxs) != 1 {
		t.Fatalf("unexpected component shape for a single shortestPath pattern: %+v", comps)
	}
	step := &part.Chains[comps[0].stepIdxs[0]]

	meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxWork: 10}}
	_, err := expandShortestPathComponent(&Env{Snap: snap}, meter, part, step)
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("expandShortestPathComponent() error = %v, want ErrBudget", err)
	}
	if meter.work >= fanOut {
		t.Fatalf("meter.work = %d after declining, want it to stay far below the %d-path fan-out (traverse materialized too much before declining)", meter.work, fanOut)
	}
}

// TestExpandShortestPathResidualWhereConjunctDoesNotSpuriouslyDeclineOnMaxRows:
// shortestPathBudget's component-level cap must be derived purely from the
// remaining Budgets.MaxWork, never from Budgets.MaxRows. MaxRows counts rows
// admitted *after* Part.Where filtering everywhere else in this package
// (workMeter.addFinalRow), but the dense []Row this component produces is
// still pre-filter -- a shortestPath query carrying a residual cross-symbol
// WHERE conjunct that Plan cannot push into either endpoint's
// NodeConstraint.Predicates (`s.group = t.group`, referencing both pattern
// variables) can have a raw path count comfortably above MaxRows while its
// *filtered* result still fits well under it. This must SERVE, not decline.
//
// Fixture: one Root (s, group "A") reaches five Target nodes by a single
// edge each; two of the five targets share s's group, three do not. Raw
// dense component output is 5 rows (one shortest path per reachable
// (s, target) pair); `WHERE s.group = t.group` filters that down to 2.
// MaxRows is set to 3 -- below the raw 5, above the filtered 2 -- so the
// pre-fix component cap (derived from MaxRows' remaining capacity) would
// decline this whole query with ErrBudget even though the real, filtered
// result fits comfortably; this test pins that it now serves the correct 2
// rows instead.
func TestExpandShortestPathResidualWhereConjunctDoesNotSpuriouslyDeclineOnMaxRows(t *testing.T) {
	const (
		kindRoot   snapshot.KindID = 1
		kindTarget snapshot.KindID = 2
		kindE      snapshot.KindID = 10
	)
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindRoot: "Root", kindTarget: "Target", kindE: "E"},
		[]execNodeSpec{
			{id: 1, kinds: []snapshot.KindID{kindRoot}, props: map[string]any{"group": "A"}},
			{id: 2, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"group": "A"}}, // matches
			{id: 3, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"group": "A"}}, // matches
			{id: 4, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"group": "B"}},
			{id: 5, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"group": "B"}},
			{id: 6, kinds: []snapshot.KindID{kindTarget}, props: map[string]any{"group": "B"}},
		},
		[]execEdgeSpec{
			{id: 100, start: 1, end: 2, kind: kindE},
			{id: 101, start: 1, end: 3, kind: kindE},
			{id: 102, start: 1, end: 4, kind: kindE},
			{id: 103, start: 1, end: 5, kind: kindE},
			{id: 104, start: 1, end: 6, kind: kindE},
		},
	)

	rs := mustExec(t, snap,
		`MATCH p = shortestPath((s:Root)-[:E*1..]->(t:Target)) WHERE s.group = t.group RETURN p`,
		Budgets{MaxRows: 3, MaxWork: 10_000_000})
	got := pathSigsAtColumn(t, snap, rs, 0)
	want := []string{"N:1,2,|E:100,", "N:1,3,|E:101,"}
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("got %d paths, want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("path set mismatch\ngot:  %v\nwant: %v", got, want)
		}
	}
}

// --- strategyBudgetOverrides / traverse.Query.SideBudget-PairBudget --------

// buildWideShortestPathSnapshot builds a snapshot shaped like the real gap
// (c) regression this section pins: an effectively unconstrained source
// symbol (every node in the graph is a candidate `s`, exactly like
// resolveEndpointSet's own full-scan doc) reaching a 20-member `t:Target`
// kind set, each via its own distinct single-hop Source node. 20 exceeds
// traverse.SideBudget (16); with 200 additional isolated filler nodes
// padding the "unconstrained root" side to 240, rootCount*termCount =
// 240*20 = 4800 also exceeds traverse.PairBudget (4096) -- so under
// traverse's own unmodified package-level defaults, NONE of AllShortestPaths'
// three strategies accept this query's endpoint shape and it declines
// ErrTooLarge, exactly reproducing this project's own corpus fixture finding
// (see strategyBudgetOverrides' doc comment in expand.go for the full
// investigation) at a much smaller, hand-built scale.
func buildWideShortestPathSnapshot(t *testing.T) (snap *snapshot.View, wantSigs []string) {
	t.Helper()
	const (
		kindTarget snapshot.KindID = 1
		kindE      snapshot.KindID = 10
		numFiller                  = 200
		numTargets                 = 20
	)

	var nodes []execNodeSpec
	var edges []execEdgeSpec
	nextID := uint64(1)

	for i := 0; i < numFiller; i++ {
		nodes = append(nodes, execNodeSpec{id: nextID})
		nextID++
	}
	for i := 0; i < numTargets; i++ {
		source := nextID
		nodes = append(nodes, execNodeSpec{id: source})
		nextID++
		target := nextID
		nodes = append(nodes, execNodeSpec{id: target, kinds: []snapshot.KindID{kindTarget}})
		nextID++
		edges = append(edges, execEdgeSpec{id: nextID, start: source, end: target, kind: kindE})
		nextID++
		wantSigs = append(wantSigs, fmt.Sprintf("N:%d,%d,|E:%d,", source, target, edges[len(edges)-1].id))
	}

	snap = buildExecSnapshot(t, map[snapshot.KindID]string{kindTarget: "Target", kindE: "E"}, nodes, edges)
	sort.Strings(wantSigs)
	return snap, wantSigs
}

// TestExpandShortestPathSideBudgetOverrideServesWideTerminalSet: the exact
// shape buildWideShortestPathSnapshot documents must decline ErrTooLarge
// under traverse's raw package defaults, but serve all 20 correct paths once
// expandShortestPathComponent's strategyBudgetOverrides widens
// traverse.Query.SideBudget/PairBudget in proportion to the executor's own
// generous remaining MaxWork.
func TestExpandShortestPathSideBudgetOverrideServesWideTerminalSet(t *testing.T) {
	snap, want := buildWideShortestPathSnapshot(t)

	rs := mustExec(t, snap, `MATCH p = shortestPath((s)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, generousBudget)
	got := pathSigsAtColumn(t, snap, rs, 0)
	if len(got) != len(want) {
		t.Fatalf("got %d paths, want %d\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("path set mismatch\ngot:  %v\nwant: %v", got, want)
		}
	}
}

// TestExpandShortestPathSideBudgetOverrideDoesNotFireOnTightBudget: the same
// query and snapshot as the override-succeeds test above, but with a small
// MaxWork whose derived affordance (remaining MaxWork / (nodes+edges)) does
// not exceed traverse.SideBudget/PairBudget -- strategyBudgetOverrides must
// return (0, 0) ("use the package defaults") rather than always widening
// unconditionally, so this must still decline ErrBudget exactly like it did
// before the override existed. Pins the "floor" behavior: an override can
// only ever widen dispatch, never substitute a smaller number that would
// somehow narrow it, and a caller with little budget to spare keeps today's
// conservative decline.
func TestExpandShortestPathSideBudgetOverrideDoesNotFireOnTightBudget(t *testing.T) {
	snap, _ := buildWideShortestPathSnapshot(t)

	err := execExpectErr(t, snap, `MATCH p = shortestPath((s)-[:E*1..]->(t:Target)) WHERE s<>t RETURN p`, Budgets{MaxRows: 10000, MaxWork: 3000})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("Execute() error = %v, want ErrBudget", err)
	}
}

// TestStrategyBudgetOverrides pins strategyBudgetOverrides' own arithmetic
// directly, table-driven, the same style as TestExpandShortestPathBudget.
func TestStrategyBudgetOverrides(t *testing.T) {
	// A tiny two-node/one-edge snapshot: perRun = NodeCount()+EdgeCount() = 3
	// for every case below, so remaining/perRun is easy to hand-verify.
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{1: "E"},
		[]execNodeSpec{{id: 1}, {id: 2}},
		[]execEdgeSpec{{id: 100, start: 1, end: 2, kind: 1}},
	)

	cases := []struct {
		name           string
		budget         Budgets
		work           int64
		wantSideBudget int
		wantPairBudget int
	}{
		{
			name:           "no MaxWork configured: no override",
			budget:         Budgets{},
			wantSideBudget: 0,
			wantPairBudget: 0,
		},
		{
			name:           "MaxWork fully spent: no override",
			budget:         Budgets{MaxWork: 100},
			work:           100,
			wantSideBudget: 0,
			wantPairBudget: 0,
		},
		{
			// remaining=9, perRun=3 -> affordable=3, which exceeds neither
			// package default (16, 4096): both stay at "use the default".
			name:           "affordable at or below both defaults: no override",
			budget:         Budgets{MaxWork: 9},
			wantSideBudget: 0,
			wantPairBudget: 0,
		},
		{
			// remaining=60, perRun=3 -> affordable=20: exceeds SideBudget
			// (16) but not PairBudget (4096).
			name:           "affordable above SideBudget only",
			budget:         Budgets{MaxWork: 60},
			wantSideBudget: 20,
			wantPairBudget: 0,
		},
		{
			// remaining=15000, perRun=3 -> affordable=5000: exceeds both.
			name:           "affordable above both defaults",
			budget:         Budgets{MaxWork: 15000},
			wantSideBudget: 5000,
			wantPairBudget: 5000,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			meter := &workMeter{budget: c.budget, work: c.work}
			sideBudget, pairBudget := strategyBudgetOverrides(meter, snap)
			if sideBudget != c.wantSideBudget {
				t.Errorf("sideBudget = %d, want %d", sideBudget, c.wantSideBudget)
			}
			if pairBudget != c.wantPairBudget {
				t.Errorf("pairBudget = %d, want %d", pairBudget, c.wantPairBudget)
			}
		})
	}
}

// --- convertPath -------------------------------------------------------

// TestExpandConvertPathErrorsOnNoMatchingForwardEdge: a fabricated traverse.Path
// whose hop names a (target, kind) pair that has no matching slot in the
// source node's own forward-CSR segment must return an error rather than
// silently falling back to a fabricated EdgeRef{Fwd: 0} -- which would alias
// whatever real, unrelated edge happens to occupy forward-CSR slot 0. Per
// convertPath's own doc comment this should be unreachable for any
// traverse.Path traverse.AllShortestPaths actually returns against the same
// snapshot; exercised here only by hand-fabricating the mismatch directly.
func TestExpandConvertPathErrorsOnNoMatchingForwardEdge(t *testing.T) {
	const kindE snapshot.KindID = 10
	const kindGhost snapshot.KindID = 99
	snap := buildExecSnapshot(t,
		map[snapshot.KindID]string{kindE: "E", kindGhost: "Ghost"},
		[]execNodeSpec{{id: 1}, {id: 2}},
		[]execEdgeSpec{{id: 1, start: 1, end: 2, kind: kindE}},
	)
	env := &Env{Snap: snap}

	n1, ok := snap.Dense(1)
	if !ok {
		t.Fatalf("Dense(1): not found")
	}
	n2, ok := snap.Dense(2)
	if !ok {
		t.Fatalf("Dense(2): not found")
	}

	// node 1 -> node 2 exists in the CSR, but only under kind kindE, never
	// kindGhost: no forward-CSR slot matches this fabricated hop.
	_, err := convertPath(env, traverse.Path{
		Nodes: []snapshot.NodeID{n1, n2},
		Kinds: []snapshot.KindID{kindGhost},
	})
	if !errors.Is(err, errConvertPathEdgeNotFound) {
		t.Fatalf("convertPath() error = %v, want errConvertPathEdgeNotFound", err)
	}
}
