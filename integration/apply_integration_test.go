// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file is the applier's own end-to-end suite: one test per write shape
// the change log recognizes, each proving that the shape is replayed into the
// in-memory replica before the writing call returns, plus one for the shape
// it deliberately cannot replay (raw Cypher through Run), proving the
// fallback path recovers on its own.
//
// # File placement: root package, not internal/engine
//
// The applier itself lives in internal/engine (apply.go), but every write
// shape below is only reachable through the real driver -- Driver.
// BatchOperation, Driver.WriteTransaction, Driver.DeleteRelationshipsByKinds,
// Driver.Run -- which lives in the root package and imports internal/engine.
// A test inside internal/engine therefore cannot drive these writes without
// an import cycle. This file lives in package bloodtrail (not
// bloodtrail_test) for the same reason staleness_integration_test.go does,
// and for the same second reason: it needs Driver's unexported engine field
// to call RebuildNow/RebuildCount.
//
// # Evidence
//
// Every assertion pairs a served-log marker delta with a value check, using
// the helpers staleness_integration_test.go already defines (requireMarkerDelta,
// nodeCountByKind, relCountByKind, cypherStringValue, installLogCapture).
// A marker delta of 1 plus a correct value means "served from the replica,
// and the replica was right"; RebuildCount is checked alongside, since an
// answer that came from a rebuilt snapshot would prove nothing about
// write-through.

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// This file's fixture kinds, named distinctly from every other integration
// test's in this shared, long-lived database (see stalenessNodeKindA's doc
// for why that matters).
var (
	applyUpsertNodeKind = graph.StringKind("ApplyUpsertNode")
	applyMergeNodeKind  = graph.StringKind("ApplyMergeNode")
	applyDeleteNodeKind = graph.StringKind("ApplyDeleteNode")
	applyDeleteEdgeKind = graph.StringKind("ApplyDeleteEdge")
	applyRelKeptKind    = graph.StringKind("ApplyRelKept")
	applyRelDroppedKind = graph.StringKind("ApplyRelDropped")
	applyRelNodeKind    = graph.StringKind("ApplyRelNode")
	applyRunNodeKind    = graph.StringKind("ApplyRunNode")
)

// openApplyDriver is this file's shared setup: Debug-level log capture, a
// wiped graph, and a wrapped driver with the default graph asserted --
// returned only once boot load's own first rebuild has actually finished
// (waitForBootLoad), so every RebuildCount comparison a test makes below is
// against a stable baseline: boot load runs exactly once, ever, so once it
// has adopted, nothing else in this file's own control triggers a rebuild
// unasked.
func openApplyDriver(t *testing.T) (*bloodtrail.Driver, graph.Database, *lockedBuffer, context.Context) {
	t.Helper()

	dsn := graphtest.PGAvailable(t)

	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })

	d, ok := bt.(*bloodtrail.Driver)
	if !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", bt)
	}

	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	waitForBootLoad(t, d)

	return d, bt, buf, ctx
}

// waitForBootLoad blocks until eng's Start-launched boot-load goroutine has
// adopted its first snapshot, up to a generous deadline. Start (driver.go's
// Open) launches that goroutine asynchronously, so without this a test's own
// "before" RebuildCount baseline could race against boot load's one and only
// rebuild attempt landing later, inside the test's own measurement window.
func waitForBootLoad(t *testing.T, d *bloodtrail.Driver) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, fresh := bloodtrail.TestingEngine(d).Fresh(); fresh {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("boot load did not produce a serving snapshot within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// applyRelTriples fetches every relationship of kind as a triple through the
// real driver -- the shape engine.TryRelFetchTriples may serve. It is this
// file's own copy of a helper the bloodtrail_test-package suites also carry;
// package boundaries keep them from sharing one.
func applyRelTriples(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) []graph.RelationshipTripleResult {
	t.Helper()

	var triples []graph.RelationshipTripleResult
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(query.Kind(query.Relationship(), kind)).FetchTriples(func(cursor graph.Cursor[graph.RelationshipTripleResult]) error {
			for triple := range cursor.Chan() {
				triples = append(triples, triple)
			}
			return cursor.Error()
		})
	}); err != nil {
		t.Fatalf("Relationships().Filter(Kind(%s)).FetchTriples(): %v", kind, err)
	}
	return triples
}

// TestApplyObjectIDUpsertServesNewNode covers the objectid-keyed write path:
// a batch create of a node whose database id this package never learns (a
// batch INSERT reports success only, never the generated id), so the change
// log records the node's "objectid" property instead and the applier
// resolves the new row by reading that objectid back from PostgreSQL
// (readback.go's readBackNodesByObjectID -- the same lookup an
// UpdateNodeBy-by-objectid upsert's read-back performs, and the reason both
// shapes share one code path). A Cypher lookup by that same objectid must
// then serve the brand-new node from the replica, with no rebuild.
//
// graph.PrepareNode is what makes this the objectid branch rather than the
// preset-id one: it leaves ID at graph.UnregisteredNodeID, which is exactly
// the "let PostgreSQL assign an id" shape recordBatchCreateNodeIdentity
// (write_observer.go) keys on. The sibling UpdateNodeBy shape is deliberately
// NOT exercised here: it compiles to `on conflict ((properties->>'objectid'))`,
// which PostgreSQL rejects unless a matching unique index exists on the graph
// partition -- BloodHound's production schema declares one, this shared test
// schema does not, and adding one to a long-lived database every other suite
// shares would break their duplicate-objectid fixtures. The applier path under
// test is identical either way: RecordNodeObjectID, then an objectid read-back.
func TestApplyObjectIDUpsertServesNewNode(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)

	// A rebuild against the (empty) graph, so the replica exists and every
	// answer below is a delta layered onto it.
	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := bloodtrail.TestingEngine(d).RebuildCount()

	const objectID = "APPLY-UPSERT-1"

	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.CreateNode(graph.PrepareNode(
			graph.NewProperties().Set("objectid", objectID).Set("name", "created"),
			applyUpsertNodeKind,
		))
	}); err != nil {
		t.Fatalf("BatchOperation (objectid-keyed create): %v", err)
	}

	text := fmt.Sprintf(`MATCH (n:ApplyUpsertNode) WHERE n.objectid = '%s' RETURN n.name`, objectID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "the objectid lookup serves the just-upserted node",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "created")

	requireMarkerDelta(t, buf, builderServedMarker, 1, "the new node is counted by its kind, served",
		func() int64 { return nodeCountByKind(t, ctx, bt, applyUpsertNodeKind) }, 1)

	if got := bloodtrail.TestingEngine(d).RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- an objectid upsert must be served without any rebuild", got, rebuilds)
	}
	assertNoFallback(t, buf)
}

// TestApplyPropertyMergeServesMergedBag covers the property half of an
// upsert: dawgs' UpdateNode MERGES the properties it is given into whatever
// the row already carries, so a segment that stored only the written keys
// would silently drop the rest. Read-back returns the row's FULL post-write
// property bag, and a Segment's bags are complete replacements, so both the
// pre-existing key and the newly written one must be readable from the
// replica afterward.
func TestApplyPropertyMergeServesMergedBag(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)

	var nodeID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("first", "one"), applyMergeNodeKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := bloodtrail.TestingEngine(d).RebuildCount()

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(&graph.Node{ID: nodeID, Properties: graph.NewProperties().Set("second", "two")})
	}); err != nil {
		t.Fatalf("WriteTransaction (property merge): %v", err)
	}

	firstText := fmt.Sprintf(`MATCH (n:ApplyMergeNode) WHERE id(n) = %d RETURN n.first`, nodeID)
	secondText := fmt.Sprintf(`MATCH (n:ApplyMergeNode) WHERE id(n) = %d RETURN n.second`, nodeID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "the newly written property serves",
		func() string { return cypherStringValue(t, ctx, bt, secondText) }, "two")

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "the pre-existing property survives the merge and still serves",
		func() string { return cypherStringValue(t, ctx, bt, firstText) }, "one")

	if got := bloodtrail.TestingEngine(d).RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- a property merge must be served without any rebuild", got, rebuilds)
	}
	assertNoFallback(t, buf)
}

// TestApplyBatchDeleteNodeRemovesNodeAndIncidentEdges covers the tombstone
// cascade: deleting a node in PostgreSQL cascades to its edges (the
// delete_node_edges trigger), and the applier must reproduce that in the
// replica -- the node's own tombstone alone would leave the base snapshot's
// CSR slots still naming its edges.
func TestApplyBatchDeleteNodeRemovesNodeAndIncidentEdges(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)

	var startID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		s, err := tx.CreateNode(graph.NewProperties(), applyDeleteNodeKind)
		if err != nil {
			return err
		}
		e, err := tx.CreateNode(graph.NewProperties(), applyDeleteNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(s.ID, e.ID, applyDeleteEdgeKind, graph.NewProperties()); err != nil {
			return err
		}
		startID = s.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := bloodtrail.TestingEngine(d).RebuildCount()

	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: both nodes are counted",
		func() int64 { return nodeCountByKind(t, ctx, bt, applyDeleteNodeKind) }, 2)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the edge is fetched as a triple",
		func() int { return len(applyRelTriples(t, ctx, bt, applyDeleteEdgeKind)) }, 1)

	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteNode(startID)
	}); err != nil {
		t.Fatalf("BatchOperation (delete node): %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "the deleted node is gone from the served count",
		func() int64 { return nodeCountByKind(t, ctx, bt, applyDeleteNodeKind) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "its incident edge is gone from the served triple fetch (the cascade)",
		func() int { return len(applyRelTriples(t, ctx, bt, applyDeleteEdgeKind)) }, 0)

	if got := bloodtrail.TestingEngine(d).RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- a node delete must be served without any rebuild", got, rebuilds)
	}

	// Differential check: a snapshot loaded from scratch must agree.
	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow (post-delete): %v", err)
	}
	requireMarkerDelta(t, buf, builderServedMarker, 1, "post-rebuild: the node count agrees with the replica's own answer",
		func() int64 { return nodeCountByKind(t, ctx, bt, applyDeleteNodeKind) }, 1)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "post-rebuild: the triple fetch agrees with the replica's own answer",
		func() int { return len(applyRelTriples(t, ctx, bt, applyDeleteEdgeKind)) }, 0)

	assertNoFallback(t, buf)
}

// TestApplyDeleteRelationshipsByKindsRemovesOnlyThatKind covers the
// criteria-shaped write: Driver.DeleteRelationshipsByKinds records the kinds
// it deleted rather than an id list (nothing enumerates the affected edges),
// so the applier replays the criteria against the View -- tombstoning every
// edge of those kinds, and nothing else.
func TestApplyDeleteRelationshipsByKindsRemovesOnlyThatKind(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.NewProperties(), applyRelNodeKind)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.NewProperties(), applyRelNodeKind)
		if err != nil {
			return err
		}
		c, err := tx.CreateNode(graph.NewProperties(), applyRelNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(a.ID, b.ID, applyRelDroppedKind, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(b.ID, c.ID, applyRelDroppedKind, graph.NewProperties()); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, c.ID, applyRelKeptKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := bloodtrail.TestingEngine(d).RebuildCount()

	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: both edges of the doomed kind are counted",
		func() int64 { return relCountByKind(t, ctx, bt, applyRelDroppedKind) }, 2)

	if err := d.DeleteRelationshipsByKinds(ctx, graph.Kinds{applyRelDroppedKind}); err != nil {
		t.Fatalf("DeleteRelationshipsByKinds: %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "every edge of the deleted kind is gone from the served count",
		func() int64 { return relCountByKind(t, ctx, bt, applyRelDroppedKind) }, 0)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "the other kind's edge is untouched and still serves",
		func() int64 { return relCountByKind(t, ctx, bt, applyRelKeptKind) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "no node was removed by a relationship-kind delete",
		func() int64 { return nodeCountByKind(t, ctx, bt, applyRelNodeKind) }, 3)

	if got := bloodtrail.TestingEngine(d).RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- a kind-scoped relationship delete must be served without any rebuild", got, rebuilds)
	}

	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow (post-delete): %v", err)
	}
	requireMarkerDelta(t, buf, builderServedMarker, 1, "post-rebuild: the deleted kind's count agrees with the replica's own answer",
		func() int64 { return relCountByKind(t, ctx, bt, applyRelDroppedKind) }, 0)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "post-rebuild: the kept kind's count agrees with the replica's own answer",
		func() int64 { return relCountByKind(t, ctx, bt, applyRelKeptKind) }, 1)

	assertNoFallback(t, buf)
}

// TestApplyMutatingRunFallsBackAndRecovers covers the shape the change log
// deliberately cannot express: a raw statement run through Driver.Run, which
// this package never interprets, and therefore records as a fallback. The applier
// must refuse to guess -- entering fallback, logging the marker, and
// rebuilding once in the background -- and must come back serving, with the
// data the raw statement wrote.
//
// The evidence is the fallback's own log markers rather than a query issued
// mid-fallback: recovery is asynchronous and can complete in microseconds, so
// "is the engine declining right now" is not a raceless thing to ask, while
// "fallback entered exactly once, then exited, with exactly one rebuild in
// between" is.
func TestApplyMutatingRunFallsBackAndRecovers(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)

	var nodeID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("name", "before"), applyRunNodeKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := bloodtrail.TestingEngine(d).RebuildCount()

	text := fmt.Sprintf(`MATCH (n:ApplyRunNode) WHERE id(n) = %d RETURN n.name`, nodeID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: the property read serves",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "before")

	enteredBefore := markerCount(buf, fallbackEnteredMarker)
	exitedBefore := markerCount(buf, fallbackExitedMarker)

	// Driver.Run's statement reaches the embedded pg driver's tx.Raw, which
	// executes it as SQL rather than Cypher (dawgs drivers/pg/driver.go's own
	// Run). Either way it is a mutating statement this package cannot
	// interpret, which is exactly the shape under test.
	if err := d.Run(ctx, fmt.Sprintf(`UPDATE node SET properties = properties || '{"name":"after"}'::jsonb WHERE id = %d`, nodeID), nil); err != nil {
		t.Fatalf("Run (mutating raw statement): %v", err)
	}

	if delta := markerCount(buf, fallbackEnteredMarker) - enteredBefore; delta != 1 {
		t.Fatalf("%q log count changed by %d after a mutating Run, want exactly 1\ncaptured log:\n%s", fallbackEnteredMarker, delta, buf.String())
	}

	deadline := time.Now().Add(30 * time.Second)
	for markerCount(buf, fallbackExitedMarker) == exitedBefore {
		if time.Now().After(deadline) {
			t.Fatalf("the engine never logged %q within 30s of entering fallback\ncaptured log:\n%s", fallbackExitedMarker, buf.String())
		}
		time.Sleep(20 * time.Millisecond)
	}

	if delta := markerCount(buf, fallbackExitedMarker) - exitedBefore; delta != 1 {
		t.Fatalf("%q log count changed by %d, want exactly 1", fallbackExitedMarker, delta)
	}
	if got := bloodtrail.TestingEngine(d).RebuildCount(); got != rebuilds+1 {
		t.Fatalf("RebuildCount = %d, want %d -- recovering from a fallback must cost exactly one rebuild", got, rebuilds+1)
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "after recovery: the property read serves the value the raw statement wrote",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "after")

	if _, fresh := bloodtrail.TestingEngine(d).Fresh(); !fresh {
		t.Fatalf("the engine is still not serving after logging %q", fallbackExitedMarker)
	}
}

// fallbackEnteredMarker/fallbackExitedMarker are the exact messages
// internal/engine/apply.go logs when the replica stops and resumes being
// trusted -- duplicated here (rather than exported) for the same reason
// servedMarker is, and greppable by the e2e script for the same reason.
const (
	fallbackEnteredMarker = "bloodtrail: fallback entered"
	fallbackExitedMarker  = "bloodtrail: fallback exited"
)

// assertNoFallback is a small guard some tests above could grow into: it
// reports every fallback reason captured in buf, so a test that unexpectedly
// tripped one fails with the reason rather than with a confusing served-marker
// count. It is used by the write-shape tests, none of which may ever fall
// back.
func assertNoFallback(t *testing.T, buf *lockedBuffer) {
	t.Helper()
	if n := strings.Count(buf.String(), fallbackEnteredMarker); n != 0 {
		t.Fatalf("the engine entered fallback %d time(s) during a write shape that must be replayable\ncaptured log:\n%s", n, buf.String())
	}
}
