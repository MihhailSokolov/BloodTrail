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
