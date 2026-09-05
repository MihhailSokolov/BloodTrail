// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine_test

import (
	"context"
	"os"
	"testing"

	"github.com/specterops/dawgs/opengraph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

const traversalShapesPath = "../../testdata/dawgs/traversal_shapes.json"

// fixtureCounts parses the traversal_shapes.json fixture directly (bypassing
// the database) so the test has an independent node/edge count to assert
// LoadSnapshot against.
func fixtureCounts(t *testing.T) (nodes, edges int) {
	t.Helper()

	f, err := os.Open(traversalShapesPath)
	if err != nil {
		t.Fatalf("open %s: %v", traversalShapesPath, err)
	}
	defer f.Close()

	doc, err := opengraph.ParseDocument(f)
	if err != nil {
		t.Fatalf("parse %s: %v", traversalShapesPath, err)
	}

	return len(doc.Graph.Nodes), len(doc.Graph.Edges)
}

func TestLoadSnapshot(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, traversalShapesPath)

	wantNodes, wantEdges := fixtureCounts(t)

	snap, err := engine.LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if got := snap.NodeCount(); got != wantNodes {
		t.Fatalf("NodeCount() = %d, want %d", got, wantNodes)
	}
	if got := snap.EdgeCount(); got != wantEdges {
		t.Fatalf("EdgeCount() = %d, want %d", got, wantEdges)
	}

	// A known chain edge (c0 -> c1) must be reachable via Dense + Out.
	c0, ok := snap.Dense(uint64(ids["c0"]))
	if !ok {
		t.Fatalf("Dense(c0 database id): not found in snapshot")
	}
	c1, ok := snap.Dense(uint64(ids["c1"]))
	if !ok {
		t.Fatalf("Dense(c1 database id): not found in snapshot")
	}

	targets, _ := snap.Out(c0)
	found := false
	for _, target := range targets {
		if target == c1 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected c0 -> c1 edge in Out(c0), got targets %v", targets)
	}

	// GraphIDs must be strictly ascending (the dense NodeID assignment order).
	for i := 1; i < len(snap.GraphIDs); i++ {
		if snap.GraphIDs[i-1] >= snap.GraphIDs[i] {
			t.Fatalf("GraphIDs not strictly ascending at index %d: %d >= %d", i, snap.GraphIDs[i-1], snap.GraphIDs[i])
		}
	}
}

// TestLoadSnapshotResolvesEdgeID checks that an edge's real database id
// survives the SQL round trip into the snapshot's CSR arrays and back out
// through EdgeByID. It inserts one edge directly via SQL (bypassing the
// dawgs write path entirely) between two nodes the traversal_shapes fixture
// already loaded, so the test controls the edge's row -- and therefore its
// generated id -- precisely: c3 -> c0 (reversing the fixture's own c0 -> c1
// -> ... chain direction) with a kind_id no fixture edge uses, so the
// insert cannot collide with the edge table's
// (start_id, end_id, kind_id, graph_id) uniqueness constraint.
func TestLoadSnapshotResolvesEdgeID(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, traversalShapesPath)

	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("no default graph is set")
	}

	const syntheticKind = 250 // not used by any edge in traversal_shapes.json

	var edgeID int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO edge (graph_id, start_id, end_id, kind_id, properties) VALUES ($1, $2, $3, $4, '{}') RETURNING id",
		graphModel.ID, int64(ids["c3"]), int64(ids["c0"]), syntheticKind,
	).Scan(&edgeID); err != nil {
		t.Fatalf("insert edge directly via SQL: %v", err)
	}

	snap, err := engine.LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	fwdIdx, ok := snap.EdgeByID(uint64(edgeID))
	if !ok {
		t.Fatalf("EdgeByID(%d): edge not found in snapshot", edgeID)
	}

	c3, ok := snap.Dense(uint64(ids["c3"]))
	if !ok {
		t.Fatalf("Dense(c3 database id): not found in snapshot")
	}
	c0, ok := snap.Dense(uint64(ids["c0"]))
	if !ok {
		t.Fatalf("Dense(c0 database id): not found in snapshot")
	}

	if snap.OutTargets[fwdIdx] != c0 {
		t.Fatalf("OutTargets[%d] = %d, want %d (c0's dense id)", fwdIdx, snap.OutTargets[fwdIdx], c0)
	}
	if snap.OutKinds[fwdIdx] != syntheticKind {
		t.Fatalf("OutKinds[%d] = %d, want %d", fwdIdx, snap.OutKinds[fwdIdx], syntheticKind)
	}
	lo, hi := snap.OutOffsets[c3], snap.OutOffsets[c3+1]
	if fwdIdx < lo || fwdIdx >= hi {
		t.Fatalf("fwdIdx %d falls outside c3's forward segment [%d, %d)", fwdIdx, lo, hi)
	}

	if _, ok := snap.EdgeByID(uint64(edgeID) + 1_000_000); ok {
		t.Fatal("EdgeByID on an id nothing was inserted with should not resolve")
	}
}
