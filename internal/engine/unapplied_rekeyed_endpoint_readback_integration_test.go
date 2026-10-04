// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"reflect"
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

// TestUnappliedRekeyedEndpointIsFoundByItsOtherEndpoint is the shape
// rekeyedEndpointIDs cannot resolve: an objectid-keyed edge upsert commits
// cleanly, then another writer re-keys an endpoint whose node this process
// has not applied yet, so that endpoint is named by neither PostgreSQL's
// objectid lookup (the objectid is gone) nor the View (which never held the
// node under it).
//
// The edge is in PostgreSQL, and the endpoint that CAN be named plus the edge
// kind are two thirds of the edge table's own unique key, so read-back asks
// for it instead of reasoning about it: one endpoint-keyed query
// (readBackEdgesByEndpoint) returns the committed edge, the endpoint nobody
// could name is read by the id that row carries, and both are staged. The
// engine keeps serving, and what it serves is PostgreSQL's own state.
//
// This used to cost a fallback, because the unnameable endpoint's absence is
// indistinguishable from a delete and failing closed was the only sound
// answer available from inside the engine. It is no longer the only one: a
// fan-out that comes back EMPTY is PostgreSQL saying the edge is not there,
// which is exactly the delete case, so the two no longer have to be told
// apart at all.
//
// The rebuild loop is parked (parkRebuildLoop) so no recovery goroutine can
// adopt a snapshot at an unpredictable moment -- which is also what makes
// "state is still stateServing" mean "no fallback was entered", since nothing
// else can leave fallback. The one rebuild here is driven explicitly.
func TestUnappliedRekeyedEndpointIsFoundByItsOtherEndpoint(t *testing.T) {
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

	// No fallback, and therefore no reload: the fan-out found the edge, so
	// nothing had to be inferred and nothing had to fail closed.
	if got := eng.state.Load(); got != stateServing {
		t.Fatalf("state = %d after a clean commit whose committed edge read-back can look up by its other endpoint, want stateServing (%d): the endpoint-keyed fan-out stages PostgreSQL's own rows, so this must not cost a reload",
			got, stateServing)
	}
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after the apply; the comparison below would be vacuous")
	}

	// The edge and both endpoints are served, from the delta alone: the
	// re-keyed start node under its NEW objectid, read by the id the edge row
	// named. mustServe on all three is what keeps this from passing on a
	// decline.
	edgeCases := []typedCase{
		{`MATCH (s:UnappliedRekeyNode)-[:UnappliedRekeyEdge]->(e:UnappliedRekeyNode) RETURN s, e`, true},
		{`MATCH (s:UnappliedRekeyNode) RETURN s`, true},
		{`MATCH (s) WHERE s.objectid = '` + rekeyedToID + `' RETURN s`, true},
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, edgeCases)

	// The comparison above would also hold if BOTH sides were empty, so name
	// the edge explicitly: one row, the re-keyed start to the end endpoint.
	servedRows, oracleRows := servedAndOracleRows(t, pgDriver, eng,
		`MATCH (s:UnappliedRekeyNode)-[:UnappliedRekeyEdge]->(e:UnappliedRekeyNode) RETURN s.objectid, e.objectid`)
	want := []string{"string:" + rekeyedToID + "|string:" + endOID + "|"}
	if !reflect.DeepEqual(servedRows, want) || !reflect.DeepEqual(oracleRows, want) {
		t.Fatalf("engine served %v, PostgreSQL returns %v, want both %v", servedRows, oracleRows, want)
	}

	// Convergence is unchanged, and still checked: a rebuild reloads what
	// PostgreSQL holds and agrees with the delta the apply published.
	adoptOneRebuild(t, ctx, eng)
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after the rebuild; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, edgeCases)
}

// TestCleanCommitWithNoNameableEndpointStillFallsBack is the coverage the
// test above gives up, and the residual's remaining half: an objectid-keyed
// upsert that commits cleanly and then has BOTH its endpoints re-keyed before
// its apply runs.
//
// An endpoint-keyed fan-out needs one nameable endpoint to anchor its query
// on, and this triple has none: PostgreSQL's objectid lookup finds neither
// objectid, and the View -- built before either node existed -- knows neither
// either. There is nothing left to ask, and an endpoint that was deleted
// looks exactly like one that was re-keyed, so the only sound answer for a
// write that committed cleanly is still to fail closed
// (unresolvableOIDEndpointFallback) and let the rebuild reload what
// PostgreSQL holds.
//
// Its cost-side twin is TestIncompleteWriteWithAnUnresolvableEndpointDoesNotFallBack
// below, which runs the same read-back shape for a write that FAILED and must
// not fall back.
func TestCleanCommitWithNoNameableEndpointStillFallsBack(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	withObjectIDUniqueIndex(t, pgDriver, pool)

	nodeKind := graph.StringKind("BothRekeyedNode")
	edgeKind := graph.StringKind("BothRekeyedEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	const (
		startOID       = "S-1-5-21-14-1100"
		endOID         = "S-1-5-21-14-1101"
		rekeyedStartID = "S-1-5-21-14-1200"
		rekeyedEndID   = "S-1-5-21-14-1201"
	)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer stopEngineAndCloseWritePool(eng)
	parkRebuildLoop(eng)

	// The View is built before either endpoint exists, so it knows nothing
	// under either objectid.
	adoptOneRebuild(t, ctx, eng)

	// The write under test: one objectid-keyed edge upsert through dawgs' own
	// pg upsert, which creates both endpoints and commits cleanly.
	if err := pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateRelationshipBy(rekeyedEndpointUpdate(startOID, endOID, nodeKind, edgeKind))
	}); err != nil {
		t.Fatalf("upsert relationship: %v", err)
	}

	// The race, on both endpoints at once: after the edge committed, before
	// the upsert's apply runs.
	for _, rekey := range [][2]string{{startOID, rekeyedStartID}, {endOID, rekeyedEndID}} {
		node := fetchNodeByObjectID(t, pgDriver, rekey[0])
		node.Properties.Set("objectid", rekey[1])
		if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.UpdateNode(node)
		}); err != nil {
			t.Fatalf("rewrite objectid %q: %v", rekey[0], err)
		}
	}

	scope := NewWriteScope()
	scope.Changes().RecordEdgeTripleByObjectID(startOID, endOID, edgeKind)
	scope.Changes().RecordNodeObjectID(startOID)
	scope.Changes().RecordNodeObjectID(endOID)
	eng.Apply(ctx, scope)

	if got := eng.state.Load(); got != stateFallback {
		t.Fatalf("state = %d after a clean commit left BOTH objectid-keyed endpoints resolvable to no node, want stateFallback (%d): no fan-out can anchor on such a triple, so the engine would be serving a replica the committed edge never reached",
			got, stateFallback)
	}

	// Convergence: one rebuild reloads what PostgreSQL actually holds, and
	// the edge is there under both new objectids.
	adoptOneRebuild(t, ctx, eng)
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after the recovery rebuild; the comparison below would be vacuous")
	}
	// Projected as objectids rather than whole nodes: these endpoints were
	// created by the upsert itself, so they carry no "name" property for
	// renderTypedRows to identify a node by.
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (s:BothRekeyedNode)-[:BothRekeyedEdge]->(e:BothRekeyedNode) RETURN s.objectid, e.objectid`, true},
		{`MATCH (s:BothRekeyedNode) RETURN s.objectid`, true},
	})

	servedRows, oracleRows := servedAndOracleRows(t, pgDriver, eng,
		`MATCH (s:BothRekeyedNode)-[:BothRekeyedEdge]->(e:BothRekeyedNode) RETURN s.objectid, e.objectid`)
	want := []string{"string:" + rekeyedStartID + "|string:" + rekeyedEndID + "|"}
	if !reflect.DeepEqual(servedRows, want) || !reflect.DeepEqual(oracleRows, want) {
		t.Fatalf("after the rebuild the engine served %v, PostgreSQL returns %v, want both %v", servedRows, oracleRows, want)
	}
}

// TestIncompleteWriteWithAnUnresolvableEndpointDoesNotFallBack pins the cost
// side of the same decision. The read-back shape is exactly the one above --
// an objectid-keyed triple NEITHER of whose endpoints is named by
// PostgreSQL's objectid lookup or the View, so no fan-out can anchor on it --
// reached the benign way: the write FAILED, so neither endpoint node was ever
// created, and there is no committed edge to miss.
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
