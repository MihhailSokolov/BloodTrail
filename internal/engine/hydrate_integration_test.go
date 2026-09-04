// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

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

	paths, err := hydratePaths(ctx, pool, pgDriver.KindMapper(), snap, []traverse.Path{path})
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

	if _, err := hydratePaths(ctx, pool, pgDriver.KindMapper(), snap, []traverse.Path{path}); err == nil {
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

	got, err := hydratePaths(ctx, pool, pgDriver.KindMapper(), snap, paths)
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
