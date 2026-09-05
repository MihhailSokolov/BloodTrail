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

	// The kind table is global (not graph-scoped): a kind every node in the
	// traversal_shapes fixture carries must resolve, and round-trip back
	// through Name.
	const wantKind = "TraversalNode"
	kindID, ok := snap.Kinds.ID(wantKind)
	if !ok {
		t.Fatalf("Kinds.ID(%q): not found", wantKind)
	}
	if name, ok := snap.Kinds.Name(kindID); !ok || name != wantKind {
		t.Fatalf("Kinds.Name(%d) = (%q, %v), want (%q, true)", kindID, name, ok, wantKind)
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

// TestLoadSnapshotProperties checks that LoadSnapshot hydrates node property
// bags into Snapshot.Props and sets MultiGraph correctly on a single-graph
// database.
//
// graphtest.LoadRandom seeds its 60 nodes via graph.NewProperties(), i.e.
// with an empty property bag, so it cannot exercise the properties column by
// itself; this test additionally inserts two nodes directly via the pg
// driver's own pool, each carrying a real property bag, and asserts both
// round-trip through Props correctly.
func TestLoadSnapshotProperties(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	randomIDs := graphtest.LoadRandom(t, pgDriver, 1)

	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("no default graph is set")
	}

	var firstID, secondID int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO node (graph_id, kind_ids, properties) VALUES ($1, '{}', $2) RETURNING id",
		graphModel.ID, `{"objectid":"S-1-Z","enabled":true}`,
	).Scan(&firstID); err != nil {
		t.Fatalf("insert first properties node: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"INSERT INTO node (graph_id, kind_ids, properties) VALUES ($1, '{}', $2) RETURNING id",
		graphModel.ID, `{"objectid":"S-1-Q","score":42}`,
	).Scan(&secondID); err != nil {
		t.Fatalf("insert second properties node: %v", err)
	}

	snap, err := engine.LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if snap.MultiGraph {
		t.Fatal("MultiGraph = true, want false on a single-graph database")
	}

	// A LoadRandom-seeded node carries no properties -- confirms the premise
	// above and that an empty jsonb bag round-trips as an empty map rather
	// than, say, a spurious entry.
	randomDense, ok := snap.Dense(uint64(randomIDs[0]))
	if !ok {
		t.Fatalf("Dense(%d): not found in snapshot", randomIDs[0])
	}
	if m := snap.Props.NodeMap(randomDense); len(m) != 0 {
		t.Fatalf("NodeMap(random node) = %v, want empty", m)
	}

	objectIDProp, ok := snap.Props.IDByName("objectid")
	if !ok {
		t.Fatal("Props.IDByName(\"objectid\"): not found")
	}
	enabledProp, ok := snap.Props.IDByName("enabled")
	if !ok {
		t.Fatal("Props.IDByName(\"enabled\"): not found")
	}
	scoreProp, ok := snap.Props.IDByName("score")
	if !ok {
		t.Fatal("Props.IDByName(\"score\"): not found")
	}

	firstDense, ok := snap.Dense(uint64(firstID))
	if !ok {
		t.Fatalf("Dense(%d): not found in snapshot", firstID)
	}
	if v, ok := snap.Props.Value(firstDense, objectIDProp); !ok || v != "S-1-Z" {
		t.Fatalf("Props.Value(firstDense, objectid) = (%v, %v), want (\"S-1-Z\", true)", v, ok)
	}
	if v, ok := snap.Props.Value(firstDense, enabledProp); !ok || v != true {
		t.Fatalf("Props.Value(firstDense, enabled) = (%v, %v), want (true, true)", v, ok)
	}

	secondDense, ok := snap.Dense(uint64(secondID))
	if !ok {
		t.Fatalf("Dense(%d): not found in snapshot", secondID)
	}
	if v, ok := snap.Props.Value(secondDense, objectIDProp); !ok || v != "S-1-Q" {
		t.Fatalf("Props.Value(secondDense, objectid) = (%v, %v), want (\"S-1-Q\", true)", v, ok)
	}
	if v, ok := snap.Props.Value(secondDense, scoreProp); !ok || v != float64(42) {
		t.Fatalf("Props.Value(secondDense, score) = (%v, %v), want (42, true)", v, ok)
	}

	if dense, ok := snap.Props.NodeByObjectID("S-1-Z"); !ok || dense != firstDense {
		t.Fatalf("NodeByObjectID(\"S-1-Z\") = (%d, %v), want (%d, true)", dense, ok, firstDense)
	}
}
