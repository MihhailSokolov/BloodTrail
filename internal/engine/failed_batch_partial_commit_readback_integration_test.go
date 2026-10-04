// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// fetchNodeByObjectID reads one node back through dawgs' own pg driver by its
// "objectid" property, so a test that let an upsert CREATE its endpoints can
// still get the node handle a re-key needs (tx.UpdateNode wants a
// *graph.Node, and the upsert returns none).
func fetchNodeByObjectID(t *testing.T, pgDriver *pg.Driver, objectID string) *graph.Node {
	t.Helper()

	var node *graph.Node
	if err := pgDriver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		var err error
		node, err = tx.Nodes().Filter(query.Equals(query.NodeProperty("objectid"), objectID)).First()
		return err
	}); err != nil {
		t.Fatalf("fetch the node carrying objectid %q: %v", objectID, err)
	}
	return node
}

// commitThenFailBatch runs one BatchOperation through dawgs' real pg path
// that makes every one of its own relationship upserts DURABLE and then
// fails: graph.WithBatchSize(1) sets the pg batch's own batchWriteSize to 1,
// so the second UpdateRelationshipBy call pushes the relationship-upsert
// buffer past that threshold and dawgs flushes it (drivers/pg/batch.go's
// tryFlush/tryFlushRelationshipUpdateByBuffer) before the delegate's error is
// ever returned.
//
// That flush is durable, not merely staged: newBatch builds its
// innerTransaction with allocateTransaction = false, so the wrapper holds no
// pgx transaction at all (transaction.driver() falls through to the pooled
// connection) and every flush Execs in autocommit. batch.Commit's own
// innerTransaction.Commit() is then a no-op -- which is exactly why
// Driver.BatchOperation applies a failed batch instead of skipping it, and
// why 2,000 is the default flush threshold a production-sized ingest window
// (BloodHound's own graphify commits every 20,000 operations) crosses many
// times over before any failure.
//
// The delegate's own error is asserted here rather than returned, so a caller
// cannot accidentally exercise a batch that succeeded.
func commitThenFailBatch(t *testing.T, pgDriver *pg.Driver, updates ...graph.RelationshipUpdate) {
	t.Helper()

	failure := errors.New("simulated ingest failure after the upserts flushed")
	err := pgDriver.BatchOperation(context.Background(), func(batch graph.Batch) error {
		for _, update := range updates {
			if err := batch.UpdateRelationshipBy(update); err != nil {
				return err
			}
		}
		return failure
	}, graph.WithBatchSize(1))
	if !errors.Is(err, failure) {
		t.Fatalf("BatchOperation error = %v, want the delegate's own %v", err, failure)
	}
}

// assertEdgeCount fails unless PostgreSQL holds exactly want edges of kind.
// Used to prove a batch's chunks really did land (or really did not) before
// the apply under test runs, so neither test below can pass on a database
// that disagrees with the shape it claims to be exercising.
func assertEdgeCount(t *testing.T, pgDriver *pg.Driver, kind graph.Kind, want int) {
	t.Helper()

	var got int64
	if err := pgDriver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		var err error
		got, err = tx.Relationships().Filter(query.Kind(query.Relationship(), kind)).Count()
		return err
	}); err != nil {
		t.Fatalf("count %s edges: %v", kind.String(), err)
	}
	if got != int64(want) {
		t.Fatalf("PostgreSQL holds %d %s edges, want %d", got, kind.String(), want)
	}
}

// TestPartiallyCommittedFailedBatchFallsBackForAnUnnameableEndpoint is the
// residual WHITEPAPER 19 documents, made executable: an objectid-keyed upsert
// that committed CLEANLY inside a batch that later FAILED, racing a re-key of
// one of its endpoints.
//
// Read-back's split (readBack's "The one fallback this records") keys off the
// write's own outcome: a clean commit that cannot name an objectid-keyed
// endpoint fails closed, a write that returned an error does not, because for
// the latter an unresolvable objectid is the ordinary benign outcome of a node
// that was never created. This shape sits in the gap -- the edge IS in
// PostgreSQL, the batch reported failure anyway, and the triple is skipped
// with the rest of that batch's keys, so the committed edge never reaches the
// replica while the engine keeps serving.
//
// This test PINS that divergence rather than asserting it away, and it is the
// only test in this package that does: it measures one served answer that is
// missing a row PostgreSQL returns. It is a tripwire in both directions.
// Should the engine start falling back here, or start serving the edge, the
// residual has been closed and WHITEPAPER 19, WHITEPAPER 12.3, readBack's own
// doc and the README's documented-differences paragraph all have to say so.
// Should the divergence ever grow beyond the one raced edge, something else
// has broken. The two tests below it say why the gate is still a gate: every
// evidence read-back holds inside a failed batch fires on batches where
// falling back would be pure cost.
//
// The rebuild loop is parked (parkRebuildLoop) so no recovery goroutine can
// adopt a snapshot at an unpredictable moment; the one rebuild here is driven
// explicitly.
func TestPartiallyCommittedFailedBatchResidualIsNotFollowed(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	withObjectIDUniqueIndex(t, pgDriver, pool)

	nodeKind := graph.StringKind("PartialCommitNode")
	edgeKind := graph.StringKind("PartialCommitEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	const (
		racedStartOID = "S-1-5-21-10-1100"
		racedEndOID   = "S-1-5-21-10-1101"
		otherStartOID = "S-1-5-21-10-1200"
		otherEndOID   = "S-1-5-21-10-1201"
		rekeyedToID   = "S-1-5-21-10-1300"
	)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer stopEngineAndCloseWritePool(eng)
	parkRebuildLoop(eng)

	// The View is built before any of the four endpoints exists, so it knows
	// nothing under any of their objectids: every endpoint below is one the
	// upsert itself creates.
	adoptOneRebuild(t, ctx, eng)

	commitThenFailBatch(t, pgDriver,
		rekeyedEndpointUpdate(racedStartOID, racedEndOID, nodeKind, edgeKind),
		rekeyedEndpointUpdate(otherStartOID, otherEndOID, nodeKind, edgeKind))

	// Both upserts are durable although the batch failed -- the premise this
	// test rests on, asserted rather than assumed.
	assertEdgeCount(t, pgDriver, edgeKind, 2)

	// The race: one endpoint of the first upsert is re-keyed after that edge
	// committed but before the batch's Apply runs, so neither PostgreSQL's
	// objectid lookup nor the View can name it.
	racedStart := fetchNodeByObjectID(t, pgDriver, racedStartOID)
	racedStart.Properties.Set("objectid", rekeyedToID)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(racedStart)
	}); err != nil {
		t.Fatalf("rewrite objectid: %v", err)
	}

	// Exactly what write_observer.go's recordRelationshipUpsertIdentity
	// records for those two batch calls, plus the mark driver.go's
	// settleBatchOutcome puts on a scope whose batch returned an error.
	scope := NewWriteScope()
	scope.Changes().RecordEdgeTripleByObjectID(racedStartOID, racedEndOID, edgeKind)
	scope.Changes().RecordNodeObjectID(racedStartOID)
	scope.Changes().RecordNodeObjectID(racedEndOID)
	scope.Changes().RecordEdgeTripleByObjectID(otherStartOID, otherEndOID, edgeKind)
	scope.Changes().RecordNodeObjectID(otherStartOID)
	scope.Changes().RecordNodeObjectID(otherEndOID)
	scope.Changes().RecordWriteIncomplete("BatchOperation: the batch failed: simulated ingest failure after the upserts flushed")
	eng.Apply(ctx, scope)

	if got := eng.state.Load(); got != stateFallback {
		t.Logf("state = %d (stateServing): the documented residual -- read-back skipped the unnameable endpoint of a FAILED batch, as it does for every failed batch",
			got)
	} else {
		t.Fatalf("state = stateFallback (%d): read-back now fails closed for a batch that COMMITTED its upserts and then failed. That closes WHITEPAPER 19's write-race residual -- update WHITEPAPER 19 and 12.3, readBack's doc and the README's documented-differences paragraph, and replace the divergence assertion below",
			stateFallback)
	}

	// The divergence itself, measured: the engine serves the upsert that was
	// not raced, and PostgreSQL additionally returns the committed edge whose
	// start endpoint was re-keyed out from under read-back.
	const edgeQuery = `MATCH (s:PartialCommitNode)-[:PartialCommitEdge]->(e:PartialCommitNode) RETURN s.objectid, e.objectid`
	servedRows, oracleRows := servedAndOracleRows(t, pgDriver, eng, edgeQuery)
	wantServed := []string{"string:" + otherStartOID + "|string:" + otherEndOID + "|"}
	wantOracle := append([]string{"string:" + rekeyedToID + "|string:" + racedEndOID + "|"}, wantServed...)
	sort.Strings(wantOracle)
	if !reflect.DeepEqual(servedRows, wantServed) || !reflect.DeepEqual(oracleRows, wantOracle) {
		t.Fatalf("engine served %v (want %v), PostgreSQL returns %v (want %v): the residual this test pins is one missing row, exactly the raced edge",
			servedRows, wantServed, oracleRows, wantOracle)
	}

	// Convergence: one rebuild reloads what PostgreSQL actually holds, and
	// the edge is there.
	adoptOneRebuild(t, ctx, eng)
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after the rebuild; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{edgeQuery, true},
		{`MATCH (s:PartialCommitNode) RETURN s.objectid`, true},
	})
}

// servedAndOracleRows renders one query's answer twice -- from the engine and
// from PostgreSQL -- through renderTypedRows, the same renderer
// assertTypedCasesMatchOracle compares with. Unlike that helper it returns
// both answers instead of requiring them to be equal, which is what lets the
// test above pin a divergence precisely rather than merely observe one.
func servedAndOracleRows(t *testing.T, pgDriver *pg.Driver, eng *Engine, q string) (served, oracle []string) {
	t.Helper()
	ctx := context.Background()

	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result, ok := eng.TryCypher(ctx, tx, q, nil)
		if !ok {
			return fmt.Errorf("TryCypher declined a shape this test exists to compare")
		}
		var err error
		served, err = renderTypedRows(result)
		if err != nil {
			return err
		}
		oracle, err = renderTypedRows(tx.Query(q, map[string]any{}))
		return err
	}); err != nil {
		t.Fatalf("render the engine's and PostgreSQL's answers: %v", err)
	}
	return served, oracle
}

// TestWhollyFailedBatchStillReadsBackPresentKeys is why the write-incomplete
// gate cannot be narrowed by asking whether any OTHER recorded key of the
// same scope read back present.
//
// The appeal of that test is that it looks like evidence of a partial commit:
// "if something of this batch is in PostgreSQL, chunks flushed, so the
// unnameable endpoint may well be a committed edge." It is not evidence of
// anything. Read-back holds no before-image of a keyed row, and the keys a
// BloodHound ingest batch records are objectid values
// (recordRelationshipUpsertIdentity records both endpoints' objectids for
// every UpdateRelationshipBy), so a principal that existed before the batch
// began reads back present whether that batch landed one row or none. Steady
// state re-ingest upserts the same principals over and over, which makes that
// the ordinary case rather than a corner.
//
// So this test runs the benign shape the gate exists for -- a batch that
// landed NOTHING (no flush: the default batchWriteSize of 2,000 is never
// crossed by one call) whose relationship upsert names a brand-new endpoint
// -- and asserts both halves at once:
//
//   - PostgreSQL holds no row the batch wrote, and the triple's endpoint
//     objectid is named by neither PostgreSQL nor the View, so read-back
//     reaches rekeyedTripleKeys' unresolved count;
//   - a recorded key of that very scope nonetheless reads back PRESENT.
//
// A "some key of this scope is present" trigger would therefore fire here,
// and a full rebuild would follow every failed ingest batch whose principals
// already exist -- the exact cost the gate was added to avoid. Read-back
// keeps skipping instead, and WHITEPAPER 19 keeps the residual.
func TestWhollyFailedBatchStillReadsBackPresentKeys(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	nodeKind := graph.StringKind("WhollyFailedNode")
	edgeKind := graph.StringKind("WhollyFailedEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	const (
		existingOID = "S-1-5-21-11-1100"
		brandNewOID = "S-1-5-21-11-1101"
	)

	// The re-ingest shape: one endpoint principal already exists, and the
	// View built below therefore knows it.
	var existing graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		node, err := tx.CreateNode(graph.NewProperties().Set("name", "existing").Set("objectid", existingOID), nodeKind)
		if err != nil {
			return err
		}
		existing = node.ID
		return nil
	}); err != nil {
		t.Fatalf("seed the pre-existing endpoint: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer stopEngineAndCloseWritePool(eng)
	parkRebuildLoop(eng)
	adoptOneRebuild(t, ctx, eng)

	// One relationship upsert, no flush (the default batchWriteSize is 2,000),
	// then a failure: nothing this batch wrote is durable.
	failure := errors.New("simulated ingest failure before any flush")
	if err := pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		if err := batch.UpdateRelationshipBy(rekeyedEndpointUpdate(existingOID, brandNewOID, nodeKind, edgeKind)); err != nil {
			return err
		}
		return failure
	}); !errors.Is(err, failure) {
		t.Fatalf("BatchOperation error = %v, want the delegate's own %v", err, failure)
	}
	assertEdgeCount(t, pgDriver, edgeKind, 0)

	cs := &ChangeSet{}
	cs.RecordEdgeTripleByObjectID(existingOID, brandNewOID, edgeKind)
	cs.RecordNodeObjectID(existingOID)
	cs.RecordNodeObjectID(brandNewOID)
	cs.RecordWriteIncomplete("BatchOperation: the batch failed: " + failure.Error())

	result, err := eng.readBack(ctx, eng.snap.Load(), cs)
	if err != nil {
		t.Fatalf("readBack: %v", err)
	}

	// The gate held: read-back recorded no fallback for this scope.
	if ok, reasons := cs.HasFallback(); ok {
		t.Fatalf("readBack recorded a fallback for a batch that landed nothing: %v", reasons)
	}

	// The triple's endpoint is named by nobody, so read-back reached the
	// unresolved count: an endpoint that HAD resolved would have left the
	// triple either in result.edges or in absentTriples, and an unresolved
	// one contributes neither (rekeyedTripleKeys' own doc).
	if len(result.edges) != 0 || len(result.absentTriples) != 0 {
		t.Fatalf("readBack resolved the triple after all: edges = %+v, absentTriples = %+v", result.edges, result.absentTriples)
	}

	// And yet a recorded key of this scope read back present -- the whole
	// point. The brand-new objectid matched nothing, as it must.
	var presentOIDs []string
	presentExisting := false
	for _, ns := range result.nodes {
		oid, _ := objectIDFromProps(ns.propsJSON)
		presentOIDs = append(presentOIDs, oid)
		if ns.id == uint64(existing) {
			presentExisting = true
		}
		if oid == brandNewOID {
			t.Fatalf("node %d carries the brand-new objectid %q, which this batch never created", ns.id, brandNewOID)
		}
	}
	if !presentExisting {
		t.Fatalf("read-back found no row for the recorded objectid %q (node %d); present objectids = %v: this test's premise is that a pre-existing key DOES read back present",
			existingOID, existing, presentOIDs)
	}

	// Apply the same keys the ordinary way: still serving, and in agreement
	// with PostgreSQL.
	scope := NewWriteScope()
	scope.Changes().RecordEdgeTripleByObjectID(existingOID, brandNewOID, edgeKind)
	scope.Changes().RecordNodeObjectID(existingOID)
	scope.Changes().RecordNodeObjectID(brandNewOID)
	scope.Changes().RecordWriteIncomplete("BatchOperation: the batch failed: " + failure.Error())
	eng.Apply(ctx, scope)

	if got := eng.state.Load(); got != stateServing {
		t.Fatalf("state = %d after a batch that landed nothing, want stateServing (%d)", got, stateServing)
	}
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after a failed batch's apply; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (s:WhollyFailedNode) RETURN s`, true},
		{`MATCH (s:WhollyFailedNode)-[:WhollyFailedEdge]->(e:WhollyFailedNode) RETURN s, e`, true},
	})
}

// TestFlushedFailedBatchCannotTellABenignUnnameableEndpointApart closes the
// refinement the previous test leaves open. "A recorded key read back
// present" is refuted there because a pre-existing principal reads back
// present for free; the obvious repair is to count only rows the View does
// NOT already know, which really are rows that appeared after the snapshot
// and plausibly came from this batch.
//
// That repair fires here, where falling back is pure cost. The batch flushes
// two relationship upserts -- four endpoint nodes and two edges, every one of
// them new and unknown to the View -- and then fails with a third upsert
// still buffered, so that third triple's two endpoints were never created and
// are named by neither PostgreSQL nor the View. Nothing is missing from the
// replica: the two committed edges are staged by the ordinary path, and the
// oracle comparison below proves the engine already agrees with PostgreSQL
// exactly. A rebuild here buys nothing and costs a full snapshot load, and
// this is the ordinary shape of a failed ingest window -- dawgs flushes a
// buffer once it passes 2,000 entries and BloodHound's graphify commits every
// 20,000 operations, so a failure almost always lands behind at least one
// flush.
//
// Which is the whole judgment: inside a failed batch, read-back can see that
// the batch committed something, and cannot see WHICH of its keys that
// something was. Only the second question decides whether an unnameable
// endpoint hides a committed edge.
func TestFlushedFailedBatchCannotTellABenignUnnameableEndpointApart(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	withObjectIDUniqueIndex(t, pgDriver, pool)

	nodeKind := graph.StringKind("BenignUnnameableNode")
	edgeKind := graph.StringKind("BenignUnnameableEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	flushed := [][2]string{
		{"S-1-5-21-12-1100", "S-1-5-21-12-1101"},
		{"S-1-5-21-12-1200", "S-1-5-21-12-1201"},
	}
	buffered := [2]string{"S-1-5-21-12-1300", "S-1-5-21-12-1301"}
	recorded := append(append([][2]string{}, flushed...), buffered)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer stopEngineAndCloseWritePool(eng)
	parkRebuildLoop(eng)
	adoptOneRebuild(t, ctx, eng)

	// The first two upserts cross batchWriteSize = 1 and flush; the third
	// re-fills an empty buffer, so it is still buffered when the delegate
	// fails and never reaches PostgreSQL at all.
	failure := errors.New("simulated ingest failure with one upsert still buffered")
	if err := pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		for _, pair := range flushed {
			if err := batch.UpdateRelationshipBy(rekeyedEndpointUpdate(pair[0], pair[1], nodeKind, edgeKind)); err != nil {
				return err
			}
		}
		if err := batch.UpdateRelationshipBy(rekeyedEndpointUpdate(buffered[0], buffered[1], nodeKind, edgeKind)); err != nil {
			return err
		}
		return failure
	}, graph.WithBatchSize(1)); !errors.Is(err, failure) {
		t.Fatalf("BatchOperation error = %v, want the delegate's own %v", err, failure)
	}
	assertEdgeCount(t, pgDriver, edgeKind, len(flushed))

	cs := &ChangeSet{}
	for _, pair := range recorded {
		cs.RecordEdgeTripleByObjectID(pair[0], pair[1], edgeKind)
		cs.RecordNodeObjectID(pair[0])
		cs.RecordNodeObjectID(pair[1])
	}
	cs.RecordWriteIncomplete("BatchOperation: the batch failed: " + failure.Error())

	view := eng.snap.Load()
	result, err := eng.readBack(ctx, view, cs)
	if err != nil {
		t.Fatalf("readBack: %v", err)
	}
	if ok, reasons := cs.HasFallback(); ok {
		t.Fatalf("readBack recorded a fallback for a failed batch whose only unnameable endpoint was never created: %v", reasons)
	}

	// The buffered upsert's endpoints exist nowhere, so its triple reaches
	// rekeyedTripleKeys' unresolved count -- and benignly: there is no
	// committed edge to miss.
	newRows := 0
	for _, ns := range result.nodes {
		if oid, ok := objectIDFromProps(ns.propsJSON); ok && (oid == buffered[0] || oid == buffered[1]) {
			t.Fatalf("node %d carries objectid %q, which the still-buffered upsert never created", ns.id, oid)
		}
		if _, known := view.Dense(ns.id); !known {
			newRows++
		}
	}
	if newRows == 0 {
		t.Fatalf("read-back found no row this View did not already know, among %d: this test's premise is that the flush DID land new rows",
			len(result.nodes))
	}

	scope := NewWriteScope()
	for _, pair := range recorded {
		scope.Changes().RecordEdgeTripleByObjectID(pair[0], pair[1], edgeKind)
		scope.Changes().RecordNodeObjectID(pair[0])
		scope.Changes().RecordNodeObjectID(pair[1])
	}
	scope.Changes().RecordWriteIncomplete("BatchOperation: the batch failed: " + failure.Error())
	eng.Apply(ctx, scope)

	if got := eng.state.Load(); got != stateServing {
		t.Fatalf("state = %d after a failed batch whose only unnameable endpoint was never created, want stateServing (%d)", got, stateServing)
	}
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine not serving after the apply; the comparison below would be vacuous")
	}
	// Nothing is missing: a fallback here would be pure cost.
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (s:BenignUnnameableNode)-[:BenignUnnameableEdge]->(e:BenignUnnameableNode) RETURN s.objectid, e.objectid`, true},
		{`MATCH (s:BenignUnnameableNode) RETURN s.objectid`, true},
	})
}
