// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/traverse"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

const hydrateFixturePath = "../../testdata/dawgs/traversal_shapes.json"

// chainPath hand-builds the dense c0->c1->c2 path from ids (the opengraph
// document-id-to-database-id map), resolving each hop's edge kind from the
// snapshot's own Out adjacency rather than hardcoding a kind id.
func chainPath(t *testing.T, snap *snapshot.Snapshot, ids map[string]graph.ID) traverse.Path {
	t.Helper()

	dense := func(name string) snapshot.NodeID {
		id, ok := snap.Dense(uint64(ids[name]))
		if !ok {
			t.Fatalf("Dense(%s): not found in snapshot", name)
		}
		return id
	}

	kindOf := func(from, to snapshot.NodeID) snapshot.KindID {
		targets, kinds := snap.Out(from)
		for i, target := range targets {
			if target == to {
				return kinds[i]
			}
		}
		t.Fatalf("no edge %d -> %d in snapshot", from, to)
		return 0
	}

	c0, c1, c2 := dense("c0"), dense("c1"), dense("c2")

	return traverse.Path{
		Nodes: []snapshot.NodeID{c0, c1, c2},
		Kinds: []snapshot.KindID{kindOf(c0, c1), kindOf(c1, c2)},
	}
}

// fetchNodeProperties fetches ids through a plain read transaction (the
// pg driver's own query path), keyed by database id, as the oracle for
// hydratePaths' property maps.
func fetchNodeProperties(t *testing.T, db graph.Database, ids []graph.ID) map[graph.ID]map[string]any {
	t.Helper()

	got := make(map[graph.ID]map[string]any, len(ids))

	err := db.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		return tx.Nodes().Filter(query.InIDs(query.NodeID(), ids...)).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
			for node := range cursor.Chan() {
				got[node.ID] = node.Properties.MapOrEmpty()
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("fetch node properties: %v", err)
	}

	return got
}

func TestHydratePaths(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	snap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	path := chainPath(t, snap, ids)

	paths, err := hydratePaths(ctx, pool, pgDriver.KindMapper(), snapshot.NewView(snap), []traverse.Path{path})
	if err != nil {
		t.Fatalf("hydratePaths: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("hydratePaths returned %d paths, want 1", len(paths))
	}

	got := paths[0]

	wantNodeIDs := []graph.ID{ids["c0"], ids["c1"], ids["c2"]}
	if len(got.Nodes) != len(wantNodeIDs) {
		t.Fatalf("got %d nodes, want %d", len(got.Nodes), len(wantNodeIDs))
	}
	for i, node := range got.Nodes {
		if node.ID != wantNodeIDs[i] {
			t.Fatalf("Nodes[%d].ID = %d, want %d", i, node.ID, wantNodeIDs[i])
		}
	}

	wantHops := [][2]graph.ID{
		{ids["c0"], ids["c1"]},
		{ids["c1"], ids["c2"]},
	}
	if len(got.Edges) != len(wantHops) {
		t.Fatalf("got %d edges, want %d", len(got.Edges), len(wantHops))
	}
	for i, edge := range got.Edges {
		if edge.StartID != wantHops[i][0] || edge.EndID != wantHops[i][1] {
			t.Fatalf("Edges[%d] = (%d -> %d), want (%d -> %d)", i, edge.StartID, edge.EndID, wantHops[i][0], wantHops[i][1])
		}
		if !edge.Kind.Is(graph.StringKind("ChainEdge")) {
			t.Fatalf("Edges[%d].Kind = %s, want ChainEdge", i, edge.Kind.String())
		}
	}

	// Properties must match what the pg driver itself returns for the same
	// nodes.
	wantProps := fetchNodeProperties(t, pgDriver, wantNodeIDs)
	for i, node := range got.Nodes {
		want := wantProps[wantNodeIDs[i]]
		if !reflect.DeepEqual(node.Properties.MapOrEmpty(), want) {
			t.Fatalf("Nodes[%d].Properties = %#v, want %#v", i, node.Properties.MapOrEmpty(), want)
		}
	}
}

func TestHydratePathsMissingNode(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	snap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	path := chainPath(t, snap, ids)

	// Simulate a node deleted between the snapshot's load and hydration.
	if _, err := pool.Exec(ctx, "DELETE FROM node WHERE graph_id = $1 AND id = $2", snap.GraphID, ids["c1"]); err != nil {
		t.Fatalf("delete node c1: %v", err)
	}

	if _, err := hydratePaths(ctx, pool, pgDriver.KindMapper(), snapshot.NewView(snap), []traverse.Path{path}); err == nil {
		t.Fatalf("hydratePaths: expected an error for a path referencing a node deleted since the snapshot was loaded")
	}
}

// TestHydratePathsEdgeBatchBoundary exercises hydrateEdges across more than
// one edgeBatchSize-sized VALUES batch: a root node fans out to
// edgeBatchSize+100 leaves (600 with the current 500 batch size), so the
// edge query must be split into a full 500-triple batch plus a 100-triple
// remainder and their results merged correctly.
func TestHydratePathsEdgeBatchBoundary(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	const leafCount = edgeBatchSize + 100

	leafKind := graph.StringKind("BatchLeaf")
	edgeKind := graph.StringKind("BatchEdge")

	// Relationship creation (unlike node creation) requires the kind to
	// already be registered: CreateRelationshipByIDs maps straight through
	// kindMapper.MapKind rather than lazily asserting like CreateNodes does.
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{leafKind, edgeKind}); err != nil {
		t.Fatalf("assert batch-boundary kinds: %v", err)
	}

	var createdIDs []graph.ID
	err := pgDriver.BatchOperation(ctx, func(b graph.Batch) error {
		creator, ok := b.(graph.NodeBatchCreator)
		if !ok {
			return fmt.Errorf("batch does not implement graph.NodeBatchCreator")
		}

		nodes := make([]*graph.Node, leafCount+1) // + root
		for i := range nodes {
			nodes[i] = graph.PrepareNode(graph.NewProperties(), leafKind)
		}

		ids, err := creator.CreateNodes(nodes)
		if err != nil {
			return err
		}
		createdIDs = ids

		for _, leafID := range ids[1:] {
			if err := b.CreateRelationshipByIDs(ids[0], leafID, edgeKind, graph.NewProperties()); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("seed batch-boundary graph: %v", err)
	}

	snap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	rootID := createdIDs[0]
	leafIDs := createdIDs[1:]

	rootDense, ok := snap.Dense(uint64(rootID))
	if !ok {
		t.Fatalf("Dense(root): not found in snapshot")
	}

	targets, kinds := snap.Out(rootDense)
	if len(targets) != leafCount {
		t.Fatalf("root has %d outgoing edges in snapshot, want %d", len(targets), leafCount)
	}
	batchEdgeKind := kinds[0]

	paths := make([]traverse.Path, leafCount)
	for i, leafID := range leafIDs {
		leafDense, ok := snap.Dense(uint64(leafID))
		if !ok {
			t.Fatalf("Dense(leaf %d): not found in snapshot", leafID)
		}
		paths[i] = traverse.Path{
			Nodes: []snapshot.NodeID{rootDense, leafDense},
			Kinds: []snapshot.KindID{batchEdgeKind},
		}
	}

	got, err := hydratePaths(ctx, pool, pgDriver.KindMapper(), snapshot.NewView(snap), paths)
	if err != nil {
		t.Fatalf("hydratePaths: %v", err)
	}
	if len(got) != leafCount {
		t.Fatalf("hydratePaths returned %d paths, want %d", len(got), leafCount)
	}

	for i, p := range got {
		wantLeaf := leafIDs[i]
		if len(p.Edges) != 1 {
			t.Fatalf("path %d: got %d edges, want 1", i, len(p.Edges))
		}
		if p.Edges[0].StartID != rootID || p.Edges[0].EndID != wantLeaf {
			t.Fatalf("path %d: edge = (%d -> %d), want (%d -> %d)", i, p.Edges[0].StartID, p.Edges[0].EndID, rootID, wantLeaf)
		}
		if !p.Edges[0].Kind.Is(edgeKind) {
			t.Fatalf("path %d: edge kind = %s, want %s", i, p.Edges[0].Kind.String(), edgeKind.String())
		}
	}
}

// --- hydrateEdgePropsByID --------------------------------------------------

// seedEdgeProps creates a small chain of nodes joined by PropEdge
// relationships directly through the pg driver's write transaction, each
// edge carrying a distinct, non-empty string property -- the checked-in
// traversal_shapes.json fixture (used by the hydratePaths tests above)
// carries no edge properties at all, so it cannot exercise property
// hydration. String values are used deliberately so the comparison against
// the pg driver's own jsonb-decoded oracle (fetchEdgeProperties) cannot be
// confused by any numeric int/float64 decode difference between this
// package's plain map[string]any pgx scan and the driver's own decode path.
func seedEdgeProps(t *testing.T, pgDriver *pg.Driver, count int) []graph.ID {
	t.Helper()
	ctx := context.Background()

	edgeKind := graph.StringKind("PropEdge")

	edgeIDs := make([]graph.ID, count)
	err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		prev, err := tx.CreateNode(graph.NewProperties())
		if err != nil {
			return err
		}

		for i := 0; i < count; i++ {
			next, err := tx.CreateNode(graph.NewProperties())
			if err != nil {
				return err
			}

			edge, err := tx.CreateRelationshipByIDs(prev.ID, next.ID, edgeKind, graph.NewProperties().Set("label", fmt.Sprintf("edge-%d", i)))
			if err != nil {
				return err
			}
			edgeIDs[i] = edge.ID

			prev = next
		}

		return nil
	})
	if err != nil {
		t.Fatalf("seed edge-props graph: %v", err)
	}

	return edgeIDs
}

// fetchEdgeProperties fetches ids' relationships through a plain read
// transaction (the pg driver's own query path), keyed by database id, as the
// oracle for hydrateEdgePropsByID's property map -- mirroring
// fetchNodeProperties' identical role for hydratePaths' node properties.
func fetchEdgeProperties(t *testing.T, db graph.Database, ids []graph.ID) map[graph.ID]map[string]any {
	t.Helper()

	got := make(map[graph.ID]map[string]any, len(ids))

	err := db.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		return tx.Relationships().Filter(query.InIDs(query.RelationshipID(), ids...)).Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
			for rel := range cursor.Chan() {
				got[rel.ID] = rel.Properties.MapOrEmpty()
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("fetch edge properties: %v", err)
	}

	return got
}

// TestHydrateEdgePropsByID hydrates two known edge ids and checks the
// returned properties against what the pg driver itself returns for the
// same edges (fetchEdgeProperties, the oracle), then re-runs the same
// request with the first id duplicated to confirm duplicate input ids are
// deduplicated rather than erroring or double-counting.
func TestHydrateEdgePropsByID(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	edgeIDs := seedEdgeProps(t, pgDriver, 2)

	snap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	ids := []uint64{uint64(edgeIDs[0]), uint64(edgeIDs[1])}
	got, err := hydrateEdgePropsByID(ctx, pool, snap.GraphID, ids)
	if err != nil {
		t.Fatalf("hydrateEdgePropsByID: %v", err)
	}
	if len(got) != len(edgeIDs) {
		t.Fatalf("hydrateEdgePropsByID returned %d entries, want %d", len(got), len(edgeIDs))
	}

	want := fetchEdgeProperties(t, pgDriver, edgeIDs)
	for _, id := range edgeIDs {
		gotProps, ok := got[uint64(id)]
		if !ok {
			t.Fatalf("hydrateEdgePropsByID: missing entry for edge %d", id)
		}
		if !reflect.DeepEqual(gotProps.MapOrEmpty(), want[id]) {
			t.Fatalf("edge %d properties = %#v, want %#v", id, gotProps.MapOrEmpty(), want[id])
		}
	}

	dupIDs := []uint64{uint64(edgeIDs[0]), uint64(edgeIDs[0]), uint64(edgeIDs[1])}
	gotDup, err := hydrateEdgePropsByID(ctx, pool, snap.GraphID, dupIDs)
	if err != nil {
		t.Fatalf("hydrateEdgePropsByID (duplicate ids): %v", err)
	}
	if len(gotDup) != len(edgeIDs) {
		t.Fatalf("hydrateEdgePropsByID (duplicate ids) returned %d entries, want %d", len(gotDup), len(edgeIDs))
	}
}

// TestHydrateEdgePropsByIDMissingID checks that a fabricated edge id -- one
// that was never assigned by this graph -- produces an error naming it,
// rather than a silently incomplete map, mirroring hydrateNodes' identical
// missing-id contract (though with a per-id message here; see
// hydrateEdgePropsByID's own doc comment for why).
func TestHydrateEdgePropsByIDMissingID(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	edgeIDs := seedEdgeProps(t, pgDriver, 1)

	snap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	fabricatedID := uint64(edgeIDs[0]) + 1_000_000

	_, err = hydrateEdgePropsByID(ctx, pool, snap.GraphID, []uint64{uint64(edgeIDs[0]), fabricatedID})
	if err == nil {
		t.Fatalf("hydrateEdgePropsByID: expected an error for a fabricated edge id")
	}
	if !strings.Contains(err.Error(), "vanished") {
		t.Fatalf("hydrateEdgePropsByID error = %q, want it to contain %q", err.Error(), "vanished")
	}
}

// TestHydrateEdgePropsByIDEmpty checks that an empty id slice returns an
// empty, non-nil map without issuing any query. The pool is closed before
// the call: if hydrateEdgePropsByID issued a query anyway, acquiring a
// connection from a closed pool would fail and the call would return a
// non-nil error, so a nil error here doubles as proof no query was made.
func TestHydrateEdgePropsByIDEmpty(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	_, pool := graphtest.OpenPG(t, dsn)
	pool.Close()

	got, err := hydrateEdgePropsByID(ctx, pool, 1, nil)
	if err != nil {
		t.Fatalf("hydrateEdgePropsByID(empty ids) on a closed pool: %v", err)
	}
	if got == nil {
		t.Fatalf("hydrateEdgePropsByID(empty ids) = nil map, want a non-nil empty map")
	}
	if len(got) != 0 {
		t.Fatalf("hydrateEdgePropsByID(empty ids) = %d entries, want 0", len(got))
	}
}

// TestHydrateEdgePropsByIDBatchBoundary exercises
// hydrateEdgePropsByIDBatched across more than one batch: 7 edges hydrated
// with a batch size of 3 forces a 3+3+1 split, so results from all three
// batches must merge correctly into one map.
func TestHydrateEdgePropsByIDBatchBoundary(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	const (
		edgeCount     = 7
		testBatchSize = 3
	)

	edgeIDs := seedEdgeProps(t, pgDriver, edgeCount)

	snap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	ids := make([]uint64, len(edgeIDs))
	for i, id := range edgeIDs {
		ids[i] = uint64(id)
	}

	got, err := hydrateEdgePropsByIDBatched(ctx, pool, snap.GraphID, ids, testBatchSize)
	if err != nil {
		t.Fatalf("hydrateEdgePropsByIDBatched: %v", err)
	}
	if len(got) != edgeCount {
		t.Fatalf("hydrateEdgePropsByIDBatched returned %d entries, want %d", len(got), edgeCount)
	}

	want := fetchEdgeProperties(t, pgDriver, edgeIDs)
	for _, id := range edgeIDs {
		gotProps, ok := got[uint64(id)]
		if !ok {
			t.Fatalf("hydrateEdgePropsByIDBatched: missing entry for edge %d", id)
		}
		if !reflect.DeepEqual(gotProps.MapOrEmpty(), want[id]) {
			t.Fatalf("edge %d properties = %#v, want %#v", id, gotProps.MapOrEmpty(), want[id])
		}
	}
}
