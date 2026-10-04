// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// seedUnappliedEndpoints creates the two endpoint nodes of the upsert under
// test AFTER the engine's View was built, so the View knows nothing under
// either objectid -- "an endpoint another write of this process created
// whose Apply has not run", one of the two ways rekeyedTripleKeys' doc
// describes of being in that state, and the one that hands a test the node
// handle a re-key needs. Both carry a "name" property because that is how
// renderTypedRows identifies a node in the oracle comparison.
func seedUnappliedEndpoints(t *testing.T, pgDriver *pg.Driver, startOID, endOID string, nodeKind graph.Kind) *graph.Node {
	t.Helper()

	var start *graph.Node
	if err := pgDriver.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
		var err error
		if start, err = tx.CreateNode(graph.NewProperties().Set("name", "start").Set("objectid", startOID), nodeKind); err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.NewProperties().Set("name", "end").Set("objectid", endOID), nodeKind)
		return err
	}); err != nil {
		t.Fatalf("create the not-yet-applied endpoints: %v", err)
	}
	return start
}

// TestUnappliedRekeyedEndpointFallsBackRatherThanDroppingTheEdge is the
// shape rekeyedEndpointIDs cannot resolve: an objectid-keyed edge upsert
// commits cleanly, then another writer re-keys an endpoint whose node this
// process has not applied yet, so that endpoint is named by neither
// PostgreSQL's objectid lookup (the objectid is gone) nor the View (which
// never held the node under it).
//
// The edge is in PostgreSQL. It cannot be named from read-back, and the
// endpoint's absence is indistinguishable from a delete, so the only sound
// answer for a write that committed cleanly is to fail closed: record a
// fallback and let the rebuild reload what PostgreSQL holds. Skipping it --
// the behaviour before this test -- left a committed edge out of the replica
// while the engine kept serving, which is a missing row in a served answer.
//
// The rebuild loop is parked (parkRebuildLoop) so the fallback's own
// recovery goroutine cannot adopt a snapshot at an unpredictable moment:
// both rebuilds here are driven explicitly.
func TestUnappliedRekeyedEndpointFallsBackRatherThanDroppingTheEdge(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	withObjectIDUniqueIndex(t, pgDriver, pool)

	nodeKind := graph.StringKind("UnappliedRekeyNode")
	edgeKind := graph.StringKind("UnappliedRekeyEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	const (
		startOID    = "S-1-5-21-9-1100"
		endOID      = "S-1-5-21-9-1101"
		rekeyedToID = "S-1-5-21-9-1200"
	)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer stopEngineAndCloseWritePool(eng)
	parkRebuildLoop(eng)

	// The View is built before either endpoint exists, so it knows nothing
	// under either objectid.
	adoptOneRebuild(t, ctx, eng)

	start := seedUnappliedEndpoints(t, pgDriver, startOID, endOID, nodeKind)

	// The write under test: one objectid-keyed edge upsert through dawgs'
	// own pg upsert, which commits cleanly.
	if err := pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateRelationshipBy(rekeyedEndpointUpdate(startOID, endOID, nodeKind, edgeKind))
	}); err != nil {
		t.Fatalf("upsert relationship: %v", err)
	}

	// The race: the start endpoint is re-keyed after that edge committed but
	// before the upsert's Apply runs.
	start.Properties.Set("objectid", rekeyedToID)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(start)
	}); err != nil {
		t.Fatalf("rewrite objectid: %v", err)
	}

	// Exactly what write_observer.go's recordRelationshipUpsertIdentity
	// records for that batch call, on a scope carrying no write-incomplete
	// mark: the batch returned nil.
	scope := NewWriteScope()
	scope.Changes().RecordEdgeTripleByObjectID(startOID, endOID, edgeKind)
	scope.Changes().RecordNodeObjectID(startOID)
	scope.Changes().RecordNodeObjectID(endOID)
	eng.Apply(ctx, scope)

	if got := eng.state.Load(); got != stateFallback {
		t.Fatalf("state = %d after a clean commit left an objectid-keyed endpoint resolvable to no node, want stateFallback (%d): the engine is still serving a replica the committed edge never reached",
			got, stateFallback)
	}

	// Convergence: one rebuild reloads what PostgreSQL actually holds, and
	// the edge is there.
	adoptOneRebuild(t, ctx, eng)
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after the recovery rebuild; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (s:UnappliedRekeyNode)-[:UnappliedRekeyEdge]->(e:UnappliedRekeyNode) RETURN s, e`, true},
		{`MATCH (s:UnappliedRekeyNode) RETURN s`, true},
		{`MATCH (s) WHERE s.objectid = '` + rekeyedToID + `' RETURN s`, true},
	})
}

// TestIncompleteWriteWithAnUnresolvableEndpointDoesNotFallBack pins the cost
// side of the same decision. The read-back shape is exactly the one above --
// an objectid-keyed triple whose endpoint is named by neither PostgreSQL's
// objectid lookup nor the View -- reached the benign way: the write FAILED,
// so its endpoint node was never created, and there is no committed edge to
// miss.
//
// Driver.BatchOperation applies even when the batch reported an error (a
// batch's chunks are durable as they flush, so skipping the apply would lose
// the ones that did), which means failed ingest batches reach this code
// routinely. Falling back for them would rebuild the whole replica after
// every failed ingest batch carrying relationship upserts, which is a
// common-path cost no correctness argument asks for.
func TestIncompleteWriteWithAnUnresolvableEndpointDoesNotFallBack(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	nodeKind := graph.StringKind("FailedUpsertNode")
	edgeKind := graph.StringKind("FailedUpsertEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	const (
		startOID = "S-1-5-21-8-1100"
		endOID   = "S-1-5-21-8-1101"
	)

	// One unrelated node, so the comparison below has something to agree
	// about besides the absence of the upsert's own rows.
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("name", "bystander").Set("objectid", "S-1-5-21-8-1000"), nodeKind)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer stopEngineAndCloseWritePool(eng)
	parkRebuildLoop(eng)
	adoptOneRebuild(t, ctx, eng)

	// What write_observer.go records for a relationship upsert inside a
	// batch that went on to fail: the keys, plus the mark driver.go's
	// settleBatchOutcome puts on a scope whose batch returned an error.
	scope := NewWriteScope()
	scope.Changes().RecordEdgeTripleByObjectID(startOID, endOID, edgeKind)
	scope.Changes().RecordNodeObjectID(startOID)
	scope.Changes().RecordNodeObjectID(endOID)
	scope.Changes().RecordWriteIncomplete("BatchOperation: the batch failed: simulated ingest failure")
	eng.Apply(ctx, scope)

	if got := eng.state.Load(); got != stateServing {
		t.Fatalf("state = %d after a FAILED write left an objectid-keyed endpoint resolvable to no node, want stateServing (%d): every failed ingest batch carrying relationship upserts would cost a full rebuild",
			got, stateServing)
	}
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after a failed write's apply; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (s:FailedUpsertNode) RETURN s`, true},
		{`MATCH (s:FailedUpsertNode)-[:FailedUpsertEdge]->(e:FailedUpsertNode) RETURN s, e`, true},
	})
}
