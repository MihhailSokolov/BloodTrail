// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/interpret"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// This file exercises every serving path directly against a genuine overlay
// View (Overlay() == true): a base snapshot with one delta Segment layered
// on top, built by hand via the snapshot package's own exported
// constructors. No production code path ever builds an overlay View today
// (WithSegment is unreachable from any Engine method), so these are the
// first tests in the repository to run a real executor against one. Every
// test here is self-contained (its own base + segment), unit-only (no
// PostgreSQL), and asserts exact rows rather than merely "did not panic".

// newOverlayTestEngine wires an *Engine for this file's tests: enabled, snap
// set directly to view (which may or may not be an overlay View), with
// mapKind/mapKindNames faked via byName/names -- mirroring
// newNodeSpecEngine/newRelSpecEngine (serve_builder_test.go), generalized to
// accept an arbitrary View instead of always wrapping a bare base Snapshot.
// Freshness is never checked by TryNodeCount/TryNodeFetchIDs/TryRelFetchTriples
// (serveGate's own doc), so there is no generation/marks setup to do here.
func newOverlayTestEngine(t *testing.T, view *snapshot.View, byName map[string]snapshot.KindID, names map[snapshot.KindID]graph.Kind) *Engine {
	t.Helper()

	e := New(nil, nil, Config{Enabled: true})
	e.mapKind = fakeKindMapper(byName)
	e.mapKindNames = fakeResolver(t, names)
	e.snap.Store(view)
	return e
}

// mustPlanAndExecute parses, plans, and executes query against snap directly
// through the interpret package's own entry points -- interpret.Plan/
// interpret.Execute never touch PostgreSQL at all (see interpret.Env's own
// doc), so this is the pg-free way to exercise exec.go/expand.go's
// executors, bypassing engine.TryCypher's translateGateOK/hydration
// machinery entirely (irrelevant to this file's concern: whether the
// executors themselves handle an overlay View correctly).
func mustPlanAndExecute(t *testing.T, snap *snapshot.View, query string, b interpret.Budgets) *interpret.ResultSet {
	t.Helper()

	rq, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		t.Fatalf("ParseCypher(%q): %v", query, err)
	}
	q, ok := interpret.Plan(rq, snap)
	if !ok {
		t.Fatalf("Plan(%q): not served", query)
	}
	rs, err := interpret.Execute(&interpret.Env{Snap: snap}, q, b)
	if err != nil {
		t.Fatalf("Execute(%q): %v", query, err)
	}
	return rs
}

// overlayBudget is generous enough to never trip for this file's small
// fixtures.
var overlayBudget = interpret.Budgets{MaxRows: 10_000, MaxWork: 10_000_000}

// --- builder Count/FetchIDs over a kind with added+tombstoned nodes -------

// TestOverlayNodeCountAndFetchIDs builds buildNodeSpecSnapshot's five-node
// base fixture, then layers a segment that tombstones User node 1 and adds a
// brand-new virtual User node 6. TryNodeCount/TryNodeFetchIDs read purely
// through NodesOfKind/Dense/GraphID, which are already overlay-correct on
// their own -- this pins that TryNodeCount/TryNodeFetchIDs still agree once
// a real segment is layered on, with no executor-level dual path needed for
// this specific pair (unlike every other test in this file).
func TestOverlayNodeCountAndFetchIDs(t *testing.T) {
	base := buildNodeSpecSnapshot(t)
	v0 := snapshot.NewView(base)

	sb := &snapshot.SegmentBuilder{}
	sb.TombstoneNode(1)
	if err := sb.AddNodeState(6, []snapshot.KindID{kindUser}, nil); err != nil {
		t.Fatalf("AddNodeState(6): %v", err)
	}
	view := v0.WithSegment(sb.Build())
	if !view.Overlay() {
		t.Fatal("view.Overlay() = false, want true")
	}

	e := newOverlayTestEngine(t, view, nodeSpecKindByName(), nodeSpecKindNames())
	ctx := context.Background()
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok {
		t.Fatal("TryNodeCount: declined, want served")
	}
	if count != 3 {
		t.Fatalf("TryNodeCount = %d, want 3 (users 2, 5, 6 -- 1 tombstoned)", count)
	}

	cursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatal("TryNodeFetchIDs: declined, want served")
	}
	assertIDs(t, drainIDs(t, cursor), []graph.ID{2, 5, 6})
}

// --- relationship triple fetch crossing a delta edge -----------------------

// TestOverlayRelFetchTriples builds buildRelSpecSnapshot's seven-edge base
// fixture, then layers a segment that:
//   - tombstones node 5 (Group), which must cascade-remove base edges 101
//     (1->5) and 102 (2->5) even though neither edge itself carries a
//     tombstone -- View.OutEdges/InEdges' own dead-endpoint filtering, per
//     its doc comment;
//   - adds a brand-new virtual node 6 (Group);
//   - adds a delta edge (201) between two existing, still-alive base nodes
//     (3->4);
//   - adds a delta edge (202) from an existing base node to the new virtual
//     one (1->6).
//
// A fully unconstrained RelSpec{} forces relScanIter's full-scan strategy
// (no start/end anchor), the branch newRelScanIter's default case builds --
// exercising the dual-path rewrite of relScanIter.next/advanceNear directly,
// not just the anchored-scan branches. Before this dual path existed,
// relScanIter read Base()'s raw CSR arrays unconditionally: it would not
// panic against an overlay View (only View.Out/In do that), but would
// silently miss both delta edges, still include the two edges that should
// have been tombstone-cascaded away, and never notice node 6 exists at all.
func TestOverlayRelFetchTriples(t *testing.T) {
	base := buildRelSpecSnapshot(t)
	v0 := snapshot.NewView(base)

	sb := &snapshot.SegmentBuilder{}
	sb.TombstoneNode(5)
	if err := sb.AddNodeState(6, []snapshot.KindID{kindGroup}, nil); err != nil {
		t.Fatalf("AddNodeState(6): %v", err)
	}
	sb.AddEdgeState(201, 3, 4, kindHasSession)
	sb.AddEdgeState(202, 1, 6, kindMemberOf)
	view := v0.WithSegment(sb.Build())
	if !view.Overlay() {
		t.Fatal("view.Overlay() = false, want true")
	}

	e := newOverlayTestEngine(t, view, relSpecKindByName(), relSpecKindNames())
	ctx := context.Background()

	cursor, ok := e.TryRelFetchTriples(ctx, recognize.RelSpec{})
	if !ok {
		t.Fatal("TryRelFetchTriples: declined, want served")
	}
	got := drainTriples(t, cursor)
	want := []graph.RelationshipTripleResult{
		{ID: 103, StartID: 1, EndID: 3},
		{ID: 104, StartID: 2, EndID: 4},
		{ID: 105, StartID: 3, EndID: 1},
		{ID: 106, StartID: 4, EndID: 2},
		{ID: 107, StartID: 1, EndID: 4},
		{ID: 201, StartID: 3, EndID: 4},
		{ID: 202, StartID: 1, EndID: 6},
	}
	assertTriples(t, got, want)
}

// --- interpret chain query: base -> delta edge -> virtual node ------------

// overlayChainKinds names the two kinds this section's fixture uses: node
// kind "N" (id 1) and edge kind "E" (id 5).
var overlayChainKinds = map[snapshot.KindID]string{1: "N", 5: "E"}

// buildOverlayChainFixture builds a base snapshot of three "N" nodes (10,
// 20, and 40, the last one never referenced by any edge -- a deliberately
// isolated node this section's Query C tombstones), one base edge
// 1001 (10->20, E), then layers a segment tombstoning node 40 and adding a
// virtual "N" node 30 plus a delta edge 1002 (20->30, E).
func buildOverlayChainFixture(t *testing.T) *snapshot.View {
	t.Helper()

	b := snapshot.NewBuilder(1)
	b.SetKinds(overlayChainKinds)
	for _, id := range []uint64{10, 20, 40} {
		if err := b.AddNode(id, []snapshot.KindID{1}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
	}
	b.AddEdge(1001, 10, 20, 5)
	base, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v0 := snapshot.NewView(base)

	sb := &snapshot.SegmentBuilder{}
	sb.TombstoneNode(40)
	if err := sb.AddNodeState(30, []snapshot.KindID{1}, nil); err != nil {
		t.Fatalf("AddNodeState(30): %v", err)
	}
	sb.AddEdgeState(1002, 20, 30, 5)

	view := v0.WithSegment(sb.Build())
	if !view.Overlay() {
		t.Fatal("view.Overlay() = false, want true")
	}
	return view
}

// TestOverlayInterpretChainQuery drives buildOverlayChainFixture's View
// through three interpret.Execute queries, pinning three distinct executor
// concerns at once:
//
// Query A is a fixed two-hop chain, (a)-[r1]->(b)-[r2]->(c), whose only
// match walks straight through the delta edge into the virtual node: this
// exercises exec.go's adjacency() (visitOut, both hops) and its EdgeRef
// construction under Overlay() -- r1 (a base-CSR edge, reached because the
// bound node's OWN adjacency is walked through OutEdges once Overlay() is
// true, regardless of whether that specific edge itself came from base or
// delta) and r2 (a genuine delta edge, with no forward-CSR slot to name at
// all) both round-trip correctly through id()/type(), which is what forces
// eval.go's EdgeRef.DatabaseID/Kind helpers to resolve correctly too.
//
// Query B counts distinct relationships across the same fixture
// (COUNT(DISTINCT r)), which must count edges 1001 and 1002 as two distinct
// relationships despite one carrying an EdgeRef.Fwd and the other an
// EdgeRef.EdgeID -- pinning pipeline.go's countAggregate dedup key, the
// production code most directly at risk of silently UNDER-counting distinct
// edges if the two representations were ever conflated or one were ignored.
//
// Query C counts every node with no kind constraint at all (a full
// 0..NodeCount() scan, scanAnchorVisit's default branch): the tombstoned
// node 40 must not be counted. This is the one query in this file that
// exercises scanAnchorVisit's own Alive-skip directly, independent of any
// adjacency walk.
func TestOverlayInterpretChainQuery(t *testing.T) {
	view := buildOverlayChainFixture(t)

	t.Run("chain through delta edge into virtual node", func(t *testing.T) {
		rs := mustPlanAndExecute(t, view,
			`MATCH (a:N)-[r1:E]->(b:N)-[r2:E]->(c:N) RETURN a, b, c, id(r1) AS rid1, id(r2) AS rid2, type(r2) AS rtype2`,
			overlayBudget)
		if len(rs.Rows) != 1 {
			t.Fatalf("rows = %d, want 1 (got %+v)", len(rs.Rows), rs.Rows)
		}
		row := rs.Rows[0]
		if got := view.GraphID(row[0].Node); got != 10 {
			t.Fatalf("a = %d, want 10", got)
		}
		if got := view.GraphID(row[1].Node); got != 20 {
			t.Fatalf("b = %d, want 20", got)
		}
		if got := view.GraphID(row[2].Node); got != 30 {
			t.Fatalf("c = %d, want 30 (the virtual node)", got)
		}
		if got := row[3].Scalar; got != float64(1001) {
			t.Fatalf("id(r1) = %v, want 1001", got)
		}
		if got := row[4].Scalar; got != float64(1002) {
			t.Fatalf("id(r2) = %v, want 1002", got)
		}
		if got := row[5].Scalar; got != "E" {
			t.Fatalf("type(r2) = %v, want E", got)
		}
	})

	t.Run("COUNT DISTINCT r across base and delta edges", func(t *testing.T) {
		rs := mustPlanAndExecute(t, view,
			`MATCH (x)-[r:E]->(y) WITH count(DISTINCT r) AS c RETURN c`,
			overlayBudget)
		if len(rs.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(rs.Rows))
		}
		if got := rs.Rows[0][0].Scalar; got != float64(2) {
			t.Fatalf("count(DISTINCT r) = %v, want 2", got)
		}
	})

	t.Run("unconstrained node count excludes tombstoned node", func(t *testing.T) {
		rs := mustPlanAndExecute(t, view,
			`MATCH (x) WITH count(x) AS c RETURN c`,
			overlayBudget)
		if len(rs.Rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(rs.Rows))
		}
		if got := rs.Rows[0][0].Scalar; got != float64(3) {
			t.Fatalf("count(x) = %v, want 3 (10, 20, 30 -- 40 is tombstoned)", got)
		}
	})
}

// --- shortestPath whose only path uses delta edges -------------------------

// TestOverlayShortestPathUsesOnlyDeltaEdges builds a base snapshot with an
// "S" node (10) and a "T" node (30) and NO edge between them at all -- the
// base graph alone has no path from s to t. A segment then adds a virtual
// "M" node (20) plus two delta edges, 1001 (10->20) and 1002 (20->30): the
// only path from s to t exists entirely through the overlay. This exercises
// traverse.go/bfs.go's dual-pathed adjacency loops (via
// expand.go's expandShortestPathComponent -> traverse.AllShortestPaths) and,
// by materializing the resulting PathVal via materializePath (serve_cypher.go,
// pg-free since edgeProps may be nil -- see edgePropsFor's own doc), also
// pins materializeEdge's overlay-aware EdgeStateByID resolution end to end.
func TestOverlayShortestPathUsesOnlyDeltaEdges(t *testing.T) {
	kinds := map[snapshot.KindID]string{1: "S", 2: "M", 3: "T", 5: "R"}

	b := snapshot.NewBuilder(1)
	b.SetKinds(kinds)
	if err := b.AddNode(10, []snapshot.KindID{1}, nil); err != nil {
		t.Fatalf("AddNode(10): %v", err)
	}
	if err := b.AddNode(30, []snapshot.KindID{3}, nil); err != nil {
		t.Fatalf("AddNode(30): %v", err)
	}
	base, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v0 := snapshot.NewView(base)

	sb := &snapshot.SegmentBuilder{}
	if err := sb.AddNodeState(20, []snapshot.KindID{2}, nil); err != nil {
		t.Fatalf("AddNodeState(20): %v", err)
	}
	sb.AddEdgeState(1001, 10, 20, 5)
	sb.AddEdgeState(1002, 20, 30, 5)
	view := v0.WithSegment(sb.Build())
	if !view.Overlay() {
		t.Fatal("view.Overlay() = false, want true")
	}

	rs := mustPlanAndExecute(t, view,
		`MATCH p = shortestPath((s:S)-[:R*1..5]->(t:T)) WHERE s<>t RETURN p`,
		overlayBudget)
	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (got %+v)", len(rs.Rows), rs.Rows)
	}

	pv := rs.Rows[0][0].Path
	if pv == nil {
		t.Fatal("Path = nil, want a *PathVal")
	}
	path := materializePath(view, pv, nil)

	if len(path.Nodes) != 3 {
		t.Fatalf("path.Nodes = %d, want 3", len(path.Nodes))
	}
	if got := uint64(path.Nodes[0].ID); got != 10 {
		t.Fatalf("path.Nodes[0].ID = %d, want 10", got)
	}
	if got := uint64(path.Nodes[1].ID); got != 20 {
		t.Fatalf("path.Nodes[1].ID = %d, want 20 (the virtual node)", got)
	}
	if got := uint64(path.Nodes[2].ID); got != 30 {
		t.Fatalf("path.Nodes[2].ID = %d, want 30", got)
	}

	if len(path.Edges) != 2 {
		t.Fatalf("path.Edges = %d, want 2", len(path.Edges))
	}
	e0, e1 := path.Edges[0], path.Edges[1]
	if got := uint64(e0.ID); got != 1001 {
		t.Fatalf("path.Edges[0].ID = %d, want 1001", got)
	}
	if got, want := uint64(e0.StartID), uint64(10); got != want {
		t.Fatalf("path.Edges[0].StartID = %d, want %d", got, want)
	}
	if got, want := uint64(e0.EndID), uint64(20); got != want {
		t.Fatalf("path.Edges[0].EndID = %d, want %d", got, want)
	}
	if got := uint64(e1.ID); got != 1002 {
		t.Fatalf("path.Edges[1].ID = %d, want 1002", got)
	}
	if got, want := uint64(e1.StartID), uint64(20); got != want {
		t.Fatalf("path.Edges[1].StartID = %d, want %d", got, want)
	}
	if got, want := uint64(e1.EndID), uint64(30); got != want {
		t.Fatalf("path.Edges[1].EndID = %d, want %d", got, want)
	}
	if e0.Kind == nil || e0.Kind.String() != "R" {
		t.Fatalf("path.Edges[0].Kind = %v, want R", e0.Kind)
	}
}

// --- var-length trail blocked by an edge tombstone --------------------------

// TestOverlayVarLengthTrailBlockedByEdgeTombstone builds a three-node base
// chain (10 -[1001]-> 20 -[1002]-> 30, kinds S/N/N over edge kind R), then
// layers a segment that tombstones edge 1002 -- and nothing else. A
// `*1..5` variable-length pattern rooted at 10 must still reach 20 (one hop,
// via the still-live 1001) but must NOT reach 30 (the only edge into it is
// gone). Before this dual path existed, expand.go's adjacency-driving loops
// read Out/In directly and would panic the instant this query ran against
// an overlay View.
func TestOverlayVarLengthTrailBlockedByEdgeTombstone(t *testing.T) {
	kinds := map[snapshot.KindID]string{1: "S", 2: "N", 5: "R"}

	b := snapshot.NewBuilder(1)
	b.SetKinds(kinds)
	if err := b.AddNode(10, []snapshot.KindID{1}, nil); err != nil {
		t.Fatalf("AddNode(10): %v", err)
	}
	if err := b.AddNode(20, []snapshot.KindID{2}, nil); err != nil {
		t.Fatalf("AddNode(20): %v", err)
	}
	if err := b.AddNode(30, []snapshot.KindID{2}, nil); err != nil {
		t.Fatalf("AddNode(30): %v", err)
	}
	b.AddEdge(1001, 10, 20, 5)
	b.AddEdge(1002, 20, 30, 5)
	base, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v0 := snapshot.NewView(base)

	sb := &snapshot.SegmentBuilder{}
	sb.TombstoneEdge(1002)
	view := v0.WithSegment(sb.Build())
	if !view.Overlay() {
		t.Fatal("view.Overlay() = false, want true")
	}

	rs := mustPlanAndExecute(t, view, `MATCH (a:S)-[:R*1..5]->(b) RETURN b`, overlayBudget)

	if len(rs.Rows) != 1 {
		t.Fatalf("rows = %d, want 1 (got %+v)", len(rs.Rows), rs.Rows)
	}
	if got := view.GraphID(rs.Rows[0][0].Node); got != 20 {
		t.Fatalf("b = %d, want 20 (30 must be unreachable: edge 1002 is tombstoned)", got)
	}
}
