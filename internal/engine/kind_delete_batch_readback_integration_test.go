// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestBatchEdgeKindDeleteRemovesADanglingDeltaEdgeEndToEnd is the BATCH form
// of TestEdgeKindDeleteRemovesADanglingDeltaEdgeEndToEnd
// (kind_delete_readback_integration_test.go), and it is not redundant with
// it: the two write paths hand Apply a different ChangeSet key for the same
// edge, and read-back resolves the two by different queries.
//
//	observingTransaction.CreateRelationshipByIDs -> Changes().RecordEdgeID(rel.ID)
//	observingBatch.CreateRelationshipByIDs       -> Changes().RecordEdgeTriple(start, end, kind)
//
// (write_observer.go). A batch's create reports success or failure only and
// never the new edge's id, so the batch path can only name the edge by its
// (start, end, kind) triple -- which read-back resolves through
// readBackEdgesByTriple rather than readBackEdgesByID (readback.go). Only
// after that resolution do the two paths converge on the same segment build,
// and the transaction-form test exercises exactly one of them.
//
// No test seam is needed to construct this. Both forms converge on a single
// d.engine.Apply(ctx, observer.scope) call (driver.go's WriteTransaction and
// BatchOperation), and this test drives that call directly at the moment it
// chooses -- exactly as the transaction-form test does -- while the pg effect
// itself goes through a real graph.Batch and the ChangeSet is built with the
// same one call observingBatch makes. A mid-batch observingBatch.Commit
// applies immediately rather than deferring (write_observer.go), so there is
// no ordering inside a single BatchOperation that would reproduce this
// anyway.
//
// The sequence, in the order the Applies run:
//
//	W1 commits a node -- and does not apply
//	W2 commits an edge into that node through a BATCH, and applies: read-back
//	   resolves it by triple and stages the edge record alone, never its
//	   endpoints, so the delta holds an edge pointing at a node the replica
//	   does not know. The edge is hidden.
//	W3 deletes every edge of the kind and applies. PostgreSQL removes both
//	   edges of that kind; the replica has to tombstone a record it cannot
//	   itself see.
//	W1 applies last, and the endpoint lands.
//
// If the kind delete's delta scan ever misses the triple-keyed record, the
// edge reappears in the replica at that last step while PostgreSQL does not
// have it: a served answer with an edge that was deleted.
func TestBatchEdgeKindDeleteRemovesADanglingDeltaEdgeEndToEnd(t *testing.T) {
	ctx := context.Background()
	nodeKind := graph.StringKind("BatchDanglingNode")
	edgeKind := graph.StringKind("BatchDanglingEdge")
	eng, nodes := seedKindDeleteOrderGraph(t, ctx, nodeKind, edgeKind)

	// W1 commits, and deliberately does not apply yet.
	var late *graph.Node
	if err := eng.pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		late, err = tx.CreateNode(graph.NewProperties().Set("name", "late"), nodeKind)
		return err
	}); err != nil {
		t.Fatalf("W1 create node: %v", err)
	}
	nodeScope := NewWriteScope()
	nodeScope.Changes().RecordNodeID(late.ID)

	// W2 commits an edge into W1's node through a batch, and applies. The
	// ChangeSet is the one observingBatch.CreateRelationshipByIDs records:
	// the triple, never an id the batch never reported.
	if err := eng.pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		//nolint:staticcheck // SA1019: the deprecated call is exactly what observingBatch passes through.
		return batch.CreateRelationshipByIDs(nodes["c"].ID, late.ID, edgeKind, graph.NewProperties())
	}); err != nil {
		t.Fatalf("W2 batch create edge: %v", err)
	}
	edgeScope := NewWriteScope()
	edgeScope.Changes().RecordEdgeTriple(nodes["c"].ID, late.ID, edgeKind)
	eng.Apply(ctx, edgeScope)

	// The premise, asserted rather than assumed, twice over: the triple
	// really did resolve to an edge record in the delta (otherwise read-back
	// found nothing and the delete below faces nothing at all), and that
	// record really is one the View cannot show.
	var dangling uint64
	if segs := eng.snap.Load().Segments(); len(segs) > 0 {
		snapshot.MergeSegments(segs).IterEdges(func(id uint64, st snapshot.EdgeSegState) bool {
			if !st.Tombstoned && st.EndID == uint64(late.ID) {
				dangling = id
			}
			return true
		})
	}
	if dangling == 0 {
		t.Fatalf("the batch's edge (triple %d -> %d) was never staged as a delta record; read-back resolved the triple to nothing and this test proves nothing",
			nodes["c"].ID, late.ID)
	}
	if _, _, _, visible := eng.snap.Load().EdgeStateByID(dangling); visible {
		t.Fatalf("edge %d is visible although its endpoint %d has not applied; there is no dangling record here", dangling, late.ID)
	}

	// W3 deletes every edge of the kind, and applies.
	if err := eng.pgDriver.DeleteRelationshipsByKinds(ctx, graph.Kinds{edgeKind}); err != nil {
		t.Fatalf("W3 delete: %v", err)
	}
	deleteScope := NewWriteScope()
	deleteScope.Changes().RecordDeleteRelationshipsByKinds(graph.Kinds{edgeKind})
	eng.Apply(ctx, deleteScope)

	// W1's endpoint lands last: nothing hides the record any more.
	eng.Apply(ctx, nodeScope)

	if _, known := eng.snap.Load().Dense(uint64(late.ID)); !known {
		t.Fatalf("node %d never reached the replica, so the edge record stayed hidden for a reason this test is not about", late.ID)
	}
	if _, _, _, visible := eng.snap.Load().EdgeStateByID(dangling); visible {
		t.Errorf("edge %d is back in the replica once its endpoint landed, although the kind delete removed it from PostgreSQL", dangling)
	}
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine left serving; the comparisons below would be vacuous")
	}

	const (
		edgeQuery = `MATCH (s:BatchDanglingNode)-[:BatchDanglingEdge]->(e:BatchDanglingNode) RETURN s, e`
		nodeQuery = `MATCH (n:BatchDanglingNode) RETURN n.name`
	)
	// PostgreSQL's own answers, so neither comparison can agree by both sides
	// being empty for unrelated reasons: no edge of the kind survives, and all
	// four nodes do -- including the one whose late Apply is the whole point.
	requireOracleRowTotal(t, eng.pgDriver, edgeQuery, 0)
	requireOracleRowTotal(t, eng.pgDriver, nodeQuery, 4)
	assertTypedCasesMatchOracle(t, eng.pgDriver, eng, []typedCase{
		{edgeQuery, true},
		{nodeQuery, true},
	})
}
