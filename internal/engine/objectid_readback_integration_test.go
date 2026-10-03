// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestObjectIDFoundOnNoRowKeepsARekeyedNodeAndItsEdges: a write recorded by
// objectid (a batch UpdateNodeBy) is read back after another write rewrote
// that node's objectid, so the objectid now matches no row. The node is not
// gone -- it kept its id, its row and its edges -- so the replica must keep
// serving it and its edge, both after that write's Apply and after the
// rewriting write's own. A node whose objectid matches no row because the
// node really was deleted must still go, edges included.
func TestObjectIDFoundOnNoRowKeepsARekeyedNodeAndItsEdges(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	nodeKind := graph.StringKind("RekeyNode")
	edgeKind := graph.StringKind("RekeyEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	var rekeyed, deleted, target *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		if rekeyed, err = tx.CreateNode(graph.NewProperties().Set("name", "rekeyed").Set("objectid", "S-1-5-21-2-1100"), nodeKind); err != nil {
			return err
		}
		if deleted, err = tx.CreateNode(graph.NewProperties().Set("name", "deleted").Set("objectid", "S-1-5-21-2-1101"), nodeKind); err != nil {
			return err
		}
		if target, err = tx.CreateNode(graph.NewProperties().Set("name", "target").Set("objectid", "S-1-5-21-2-1102"), nodeKind); err != nil {
			return err
		}
		if _, err = tx.CreateRelationshipByIDs(rekeyed.ID, target.ID, edgeKind, graph.NewProperties()); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(deleted.ID, target.ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// One write rewrites the first node's objectid (a transaction
	// UpdateNode, recorded by id), another deletes the second node (a batch
	// DeleteNode, recorded by id); the write recorded by the two old
	// objectids is read back after both committed.
	rekeyed.Properties.Set("objectid", "S-1-5-21-2-1200")
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(rekeyed)
	}); err != nil {
		t.Fatalf("rewrite objectid: %v", err)
	}
	if err := pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteNode(deleted.ID)
	}); err != nil {
		t.Fatalf("delete node: %v", err)
	}

	byObjectID := NewWriteScope()
	byObjectID.Changes().RecordNodeObjectID("S-1-5-21-2-1100")
	byObjectID.Changes().RecordNodeObjectID("S-1-5-21-2-1101")
	eng.Apply(ctx, byObjectID)

	cases := []typedCase{
		{`MATCH (s:RekeyNode) RETURN s`, true},
		{`MATCH (s:RekeyNode)-[:RekeyEdge]->(e:RekeyNode) RETURN s, e`, true},
		{`MATCH (s) WHERE s.objectid = 'S-1-5-21-2-1200' RETURN s`, true},
	}
	t.Run("after the objectid-keyed write", func(t *testing.T) {
		if _, serving := eng.serveState(); !serving {
			t.Fatalf("engine left serving; the comparison below would be vacuous")
		}
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})

	byID := NewWriteScope()
	byID.Changes().RecordNodeID(rekeyed.ID)
	byID.Changes().RecordNodeID(deleted.ID)
	eng.Apply(ctx, byID)

	t.Run("after the id-keyed writes", func(t *testing.T) {
		if _, serving := eng.serveState(); !serving {
			t.Fatalf("engine left serving; the comparison below would be vacuous")
		}
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})
}
