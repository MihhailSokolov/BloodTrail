// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file is the write-through model's own exit-criterion suite: one
// sequential subtest per mutation shape the change log recognizes (plus the
// one it deliberately cannot, and a concurrent-writer stress case), each
// proving the same three things at once: the write is replayed into the
// in-memory replica before the writing call returns (a served-log marker
// fires on the very next read), the replica's answer is correct (it agrees
// with a plain PostgreSQL driver reading the same, real, committed rows),
// and no rebuild was needed to get there.
//
// # File placement and package
//
// Like staleness_integration_test.go and apply_integration_test.go, this
// lives in package bloodtrail (not bloodtrail_test): every write shape below
// is only reachable through the real driver (Driver.WriteTransaction,
// BatchOperation, DeleteNodesByKinds, DeleteRelationshipsByKinds, Run), which
// live in this root package and import internal/engine, so a test that needs
// to drive them cannot itself live in internal/engine without an import
// cycle. Being in package bloodtrail also gives this file the same
// unexported access to Driver's own engine field that apply_integration_test.go
// uses for engine.RebuildNow/RebuildCount/Fresh.
//
// # Comparator and log-capture reuse
//
// Every helper this file calls -- installLogCapture, markerCount,
// requireMarkerDelta, waitForBootLoad, nodeCountByKind, relCountByKind,
// cypherStringValue, applyRelTriples, cypherServedMarker, builderServedMarker,
// fallbackEnteredMarker/fallbackExitedMarker, assertStringMultiset, and the
// canonNode signature type -- is already defined elsewhere in this same
// package (staleness_integration_test.go, apply_integration_test.go,
// dawgs_corpus_integration_test.go, prebuilt_corpus_integration_test.go).
// None of it is forked here; this file only adds the one new helper those
// files have no reason to carry (nodeSignaturesByCypher, a thin
// ops.FetchByQuery wrapper over canonNode -- see its own doc).
//
// # The objectid unique-index gap this suite closes
//
// UpdateNodeBy (and the node-upsert half of UpdateRelationshipBy) compile to
// `insert ... on conflict ((properties->>'objectid')) do update ...`
// (dawgs' pg driver, drivers/pg/query/format.go's FormatNodeUpsert), which
// PostgreSQL refuses unless a matching unique index already exists. Every
// other integration suite in this package asserts a schema with no
// NodeConstraints (see apply_integration_test.go's own doc on why it
// deliberately never exercises this path: adding the index to a shared,
// long-lived database would break other suites' duplicate-objectid
// fixtures), which left this exact path untested end-to-end since write-
// through first went live. This suite is the one place that gap gets
// closed: it wipes the graph (truncating every node and edge, but never
// the kind/index catalog) BEFORE opening its own driver, then asserts a
// schema whose DefaultGraph.NodeConstraints names the objectid expression
// index -- mirroring
// production's own graphschema.DefaultGraphSchema() NodeConstraints shape --
// against that now-empty partition, where no pre-existing duplicate
// objectid can possibly conflict. Because dawgs' AssertGraph diffs and
// syncs a partition's indexes/constraints to exactly the set the CURRENT
// call named (drivers/pg/query/query.go's AssertGraph), asserting the
// index here is self-contained: the next suite's own AssertSchema call
// (with no NodeConstraints of its own) removes it again before that suite
// writes anything, so this suite's index never outlives its own run.
package bloodtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// nodeSignaturesByCypher runs text -- a Cypher query projecting bare nodes,
// e.g. "MATCH (n:Kind) RETURN n" -- against db via ops.FetchByQuery and
// renders every returned node as a canonNode signature (its database id plus
// its FULL property bag), reusing prebuilt_corpus_integration_test.go's own
// canonNode type rather than forking a parallel one (see this file's own
// package doc). ops.FetchByQuery packs every row's node value into one
// shared graph.Path rather than resetting per row (see its own doc, quoted
// by prebuilt_corpus_integration_test.go's adminCountSignatures) -- for a
// bare "RETURN n" query that never also returns a relationship, that shared
// path simply ends up holding every matched node, in encounter order, and
// no edges, so walking every path's Nodes (there is at most one non-empty
// path for this shape) recovers exactly the matched node set. The result is
// unordered by construction (test callers compare it via
// assertStringMultiset, never assertStringSequence).
func nodeSignaturesByCypher(t *testing.T, ctx context.Context, db graph.Database, text string) []string {
	t.Helper()

	var qr ops.QueryResult
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		qr, err = ops.FetchByQuery(tx, text)
		return err
	}); err != nil {
		t.Fatalf("FetchByQuery(%q): %v", text, err)
	}

	var sigs []string
	for _, path := range qr.Paths {
		for _, n := range path.Nodes {
			b, err := json.Marshal(canonNode{ID: uint64(n.ID), Props: n.Properties.MapOrEmpty()})
			if err != nil {
				t.Fatalf("marshal canonNode: %v", err)
			}
			sigs = append(sigs, string(b))
		}
	}
	return sigs
}

// cypherValueOrNil runs text -- a Cypher query returning exactly one row
// with exactly one column that may be absent (nil) or a string -- through
// db and returns that raw value, unlike cypherStringValue (staleness_
// integration_test.go), which requires the column to already be a string.
// This file uses it to prove a deleted property is genuinely gone (nil)
// rather than merely absent from view, without routing through coalesce()
// or toString(): FINDINGS (this file's own report) notes that the
// interpreter declines -- correctly, just unserved -- any Cypher text
// query built around either function, so a test that used one to probe a
// possibly-missing property would never observe a served marker even
// though the underlying write-through delta is correct.
func cypherValueOrNil(t *testing.T, ctx context.Context, db graph.Database, text string) any {
	t.Helper()

	var value any
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(text, nil)
		defer result.Close()

		if !result.Next() {
			return fmt.Errorf("no rows")
		}
		values := result.Values()
		if len(values) != 1 {
			return fmt.Errorf("%d columns, want 1", len(values))
		}
		value = values[0]

		if result.Next() {
			return fmt.Errorf("more than one row")
		}
		return result.Error()
	}); err != nil {
		t.Fatalf("cypherValueOrNil(%q): %v", text, err)
	}
	return value
}

// assertRebuildCountUnchanged is this file's own cross-cutting assertion,
// mirroring apply_integration_test.go's inline check: label's write shape
// must be served entirely from the write-through delta, never by way of a
// rebuild -- a rebuilt snapshot would prove nothing about write-through.
func assertRebuildCountUnchanged(t *testing.T, d *Driver, before uint64, label string) {
	t.Helper()
	if got := d.engine.RebuildCount(); got != before {
		t.Fatalf("%s: RebuildCount = %d, want %d -- must be served without any rebuild", label, got, before)
	}
}

// assertNoNewFallback fails if the engine entered fallback even once since
// before was captured (markerCount(buf, fallbackEnteredMarker)) -- every
// class but the dedicated fallback one (13) must never trip it.
func assertNoNewFallback(t *testing.T, buf *lockedBuffer, before int, label string) {
	t.Helper()
	if after := markerCount(buf, fallbackEnteredMarker); after != before {
		t.Fatalf("%s: the engine entered fallback %d time(s) during a write shape that must stay servable\ncaptured log:\n%s", label, after-before, buf.String())
	}
}

// objectIDUpdate builds a graph.NodeUpdate identifying its target purely by
// a bare "objectid" property -- the one shape write_observer.go's
// nodeUpsertObjectID recognizes (IdentityProperties == ["objectid"], a
// string-valued "objectid" property) -- with kinds optionally empty for the
// kind-less upsert shape (class 3).
func objectIDUpdate(objectID string, props *graph.Properties, kinds ...graph.Kind) graph.NodeUpdate {
	if props == nil {
		props = graph.NewProperties()
	}
	props.Set("objectid", objectID)
	return graph.NodeUpdate{
		Node:               graph.NewNode(graph.UnregisteredNodeID, props, kinds...),
		IdentityProperties: []string{"objectid"},
	}
}

// TestWriteThroughDifferential is the write-through model's exit-criterion
// suite: one fresh driver+engine boot, wiped and schema-asserted with the
// objectid unique index (see the package doc above), then one sequential
// subtest per mutation class this package's write path recognizes -- plus
// the one shape it deliberately cannot (fallback) and a concurrent-writer
// case. Subtests share one open driver and DELIBERATELY do not re-wipe
// between each other (each uses its own, disjoint kind/objectid namespace),
// so a later class's fixtures never interact with an earlier class's.
func TestWriteThroughDifferential(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	buf := installLogCapture(t)

	ctx := context.Background()

	// Wipe through the RAW pg driver, before the bloodtrail engine even
	// exists (dawgs.Open below is what calls engine.Start) -- exactly
	// openApplyDriver's own ordering, and for the same reason: a wipe issued
	// through the bloodtrail driver's own overridden WipeGraph trips
	// fallback by design (driver.go's own doc), but a wipe issued through
	// the raw driver, before boot, has no engine to notify at all.
	oracle, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, oracle)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	// Assert the objectid unique expression index UpdateNodeBy's ON
	// CONFLICT target requires -- see the package doc's "objectid
	// unique-index gap" section for why this is safe against the now-empty
	// (just-wiped) partition and self-contained across suites.
	schema := graph.Schema{DefaultGraph: graph.Graph{
		Name:            graphtest.GraphName,
		NodeConstraints: []graph.Constraint{{Field: "objectid", Type: graph.BTreeIndex}},
	}}
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema with objectid constraint: %v", err)
	}

	waitForBootLoad(t, d)

	// -----------------------------------------------------------------
	// Class 1: objectid upsert creating a brand-new node (ingest).
	// -----------------------------------------------------------------
	t.Run("ObjectIDUpsertCreatesNode", func(t *testing.T) {
		kind := graph.StringKind("WT1Node")
		const objectID = "WT1-1"

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateNodeBy(objectIDUpdate(objectID, graph.NewProperties().Set("name", "created"), kind))
		}); err != nil {
			t.Fatalf("BatchOperation (objectid upsert create): %v", err)
		}

		text := fmt.Sprintf(`MATCH (n:WT1Node) WHERE n.objectid = '%s' RETURN n.name`, objectID)
		want := cypherStringValue(t, ctx, oracle, text)

		requireMarkerDelta(t, buf, cypherServedMarker, 1, "the new node's property serves",
			func() string { return cypherStringValue(t, ctx, bt, text) }, want)

		wantCount := nodeCountByKind(t, ctx, oracle, kind)
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the new node is counted by kind",
			func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, wantCount)

		assertRebuildCountUnchanged(t, d, rebuilds, "ObjectIDUpsertCreatesNode")
		assertNoNewFallback(t, buf, fallbacks, "ObjectIDUpsertCreatesNode")
	})

	// -----------------------------------------------------------------
	// Class 2: objectid upsert merging properties and unioning kinds onto
	// an EXISTING node.
	// -----------------------------------------------------------------
	t.Run("ObjectIDUpsertMergesPropsUnionsKinds", func(t *testing.T) {
		kindA := graph.StringKind("WT2NodeA")
		kindB := graph.StringKind("WT2NodeB")
		const objectID = "WT2-1"

		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			_, err := tx.CreateNode(graph.NewProperties().Set("objectid", objectID).Set("first", "one"), kindA)
			return err
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateNodeBy(objectIDUpdate(objectID, graph.NewProperties().Set("second", "two"), kindB))
		}); err != nil {
			t.Fatalf("BatchOperation (objectid upsert merge): %v", err)
		}

		firstText := fmt.Sprintf(`MATCH (n) WHERE n.objectid = '%s' RETURN n.first`, objectID)
		secondText := fmt.Sprintf(`MATCH (n) WHERE n.objectid = '%s' RETURN n.second`, objectID)

		requireMarkerDelta(t, buf, cypherServedMarker, 1, "the pre-existing property survives the merge",
			func() string { return cypherStringValue(t, ctx, bt, firstText) }, cypherStringValue(t, ctx, oracle, firstText))
		requireMarkerDelta(t, buf, cypherServedMarker, 1, "the newly written property serves",
			func() string { return cypherStringValue(t, ctx, bt, secondText) }, cypherStringValue(t, ctx, oracle, secondText))

		requireMarkerDelta(t, buf, builderServedMarker, 1, "the original kind is preserved (union, not replace)",
			func() int64 { return nodeCountByKind(t, ctx, bt, kindA) }, nodeCountByKind(t, ctx, oracle, kindA))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the new kind is also present on the same node",
			func() int64 { return nodeCountByKind(t, ctx, bt, kindB) }, nodeCountByKind(t, ctx, oracle, kindB))

		gotFull := nodeSignaturesByCypher(t, ctx, bt, `MATCH (n:WT2NodeA) RETURN n`)
		wantFull := nodeSignaturesByCypher(t, ctx, oracle, `MATCH (n:WT2NodeA) RETURN n`)
		assertStringMultiset(t, gotFull, wantFull, "full node row (id+properties) after the merge")

		assertRebuildCountUnchanged(t, d, rebuilds, "ObjectIDUpsertMergesPropsUnionsKinds")
		assertNoNewFallback(t, buf, fallbacks, "ObjectIDUpsertMergesPropsUnionsKinds")
	})

	// -----------------------------------------------------------------
	// Class 3: kind-less lastseen-only upsert (the changelog shape), once
	// against an existing node and once against an absent objectid.
	// -----------------------------------------------------------------
	t.Run("KindlessLastSeenUpsert", func(t *testing.T) {
		kind := graph.StringKind("WT3Node")
		const existingOID = "WT3-EXIST"
		const absentOID = "WT3-NEW"

		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			_, err := tx.CreateNode(graph.NewProperties().Set("objectid", existingOID).Set("name", "orig"), kind)
			return err
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			if err := batch.UpdateNodeBy(objectIDUpdate(existingOID, graph.NewProperties().Set("lastseen", "2024-01-01"))); err != nil {
				return err
			}
			return batch.UpdateNodeBy(objectIDUpdate(absentOID, graph.NewProperties().Set("lastseen", "2024-02-02")))
		}); err != nil {
			t.Fatalf("BatchOperation (kind-less upserts): %v", err)
		}

		// Existing objectid: kind must survive (a kind-less upsert unions
		// in an EMPTY kind set, never clearing what was already there),
		// and the pre-existing property plus the new one must both serve.
		existingLastSeenText := fmt.Sprintf(`MATCH (n) WHERE n.objectid = '%s' RETURN n.lastseen`, existingOID)
		existingNameText := fmt.Sprintf(`MATCH (n) WHERE n.objectid = '%s' RETURN n.name`, existingOID)

		requireMarkerDelta(t, buf, cypherServedMarker, 1, "the existing node's new lastseen property serves",
			func() string { return cypherStringValue(t, ctx, bt, existingLastSeenText) }, cypherStringValue(t, ctx, oracle, existingLastSeenText))
		requireMarkerDelta(t, buf, cypherServedMarker, 1, "the existing node's kind survives a kind-less upsert",
			func() string { return cypherStringValue(t, ctx, bt, existingNameText) }, cypherStringValue(t, ctx, oracle, existingNameText))

		requireMarkerDelta(t, buf, builderServedMarker, 1, "the existing node's kind count is unaffected",
			func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, nodeCountByKind(t, ctx, oracle, kind))

		// Absent objectid: a brand-new, kind-less node must exist and
		// carry the property the upsert wrote. Correctness (does the
		// replica agree with PostgreSQL) is asserted unconditionally;
		// whether this particular unconstrained, kind-less lookup happens
		// to be servable is not this class's claim, so it is only checked
		// informationally, and never allowed to cause a fallback.
		absentText := fmt.Sprintf(`MATCH (n) WHERE n.objectid = '%s' RETURN n.lastseen`, absentOID)
		wantAbsent := cypherStringValue(t, ctx, oracle, absentText)
		if got := cypherStringValue(t, ctx, bt, absentText); got != wantAbsent {
			t.Fatalf("kind-less node created by an upsert on an absent objectid: got %q, want %q", got, wantAbsent)
		}

		assertRebuildCountUnchanged(t, d, rebuilds, "KindlessLastSeenUpsert")
		assertNoNewFallback(t, buf, fallbacks, "KindlessLastSeenUpsert")
	})

	// -----------------------------------------------------------------
	// Class 4: UpdateRelationshipBy keyed by both endpoints' objectids
	// (ingest edges).
	// -----------------------------------------------------------------
	t.Run("UpdateRelationshipByObjectIDEndpoints", func(t *testing.T) {
		nodeKind := graph.StringKind("WT4Node")
		edgeKind := graph.StringKind("WT4Edge")
		const startOID = "WT4-S1"
		const endOID = "WT4-E1"

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateRelationshipBy(graph.RelationshipUpdate{
				Relationship:            graph.NewRelationship(0, 0, 0, graph.NewProperties().Set("weight", "w5"), edgeKind),
				Start:                   graph.NewNode(graph.UnregisteredNodeID, graph.NewProperties().Set("objectid", startOID), nodeKind),
				StartIdentityProperties: []string{"objectid"},
				End:                     graph.NewNode(graph.UnregisteredNodeID, graph.NewProperties().Set("objectid", endOID), nodeKind),
				EndIdentityProperties:   []string{"objectid"},
			})
		}); err != nil {
			t.Fatalf("BatchOperation (UpdateRelationshipBy): %v", err)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "both endpoint nodes exist, served",
			func() int64 { return nodeCountByKind(t, ctx, bt, nodeKind) }, nodeCountByKind(t, ctx, oracle, nodeKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the edge between them exists, served",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKind) }, relCountByKind(t, ctx, oracle, edgeKind))

		// Correctness (does the replica agree with PostgreSQL) is asserted
		// unconditionally, but NOT wrapped in requireMarkerDelta's served
		// check: interpret/plan.go's checkPropertyLookup rejects an edge
		// variable's own PropertyLookup outright, by design and in its own
		// doc comment ("edge property access is rejected outright -- the
		// snapshot has no edge property store"), so `RETURN r.weight`
		// always declines to PostgreSQL, correctly, regardless of
		// write-through. That is a documented Cypher-interpreter coverage
		// boundary, not a write-through defect -- this class's actual
		// claim (the upsert replayed both endpoints and the edge between
		// them) is already proven above by the two served, marker-checked
		// structural counts.
		weightText := fmt.Sprintf(`MATCH (a:WT4Node)-[r:WT4Edge]->(b:WT4Node) WHERE a.objectid = '%s' AND b.objectid = '%s' RETURN r.weight`, startOID, endOID)
		if got, want := cypherStringValue(t, ctx, bt, weightText), cypherStringValue(t, ctx, oracle, weightText); got != want {
			t.Fatalf("the edge's own property: got %q, want %q", got, want)
		}

		assertRebuildCountUnchanged(t, d, rebuilds, "UpdateRelationshipByObjectIDEndpoints")
		assertNoNewFallback(t, buf, fallbacks, "UpdateRelationshipByObjectIDEndpoints")
	})

	// -----------------------------------------------------------------
	// Class 5: batch.UpdateNodes carrying AddedKinds/DeletedKinds and a
	// deleted property (tagging).
	// -----------------------------------------------------------------
	t.Run("BatchUpdateNodesKindsAndPropertyDeletion", func(t *testing.T) {
		oldKind := graph.StringKind("WT5Old")
		newKind := graph.StringKind("WT5New")

		var nodeID graph.ID
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			n, err := tx.CreateNode(graph.NewProperties().Set("objectid", "WT5-1").Set("stale", "gone").Set("keep", "yes"), oldKind)
			if err != nil {
				return err
			}
			nodeID = n.ID
			return nil
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		// dawgs' pg driver reads a plain batch.UpdateNodes' Node.Kinds field
		// as the SQL statement's own "added_kinds" positional parameter
		// (drivers/pg/batch.go's NodeUpdateParameters.Append passes
		// node.Kinds, never node.AddedKinds, into the same slot
		// FormatNodesUpdate's SQL unions into kind_ids) -- Node.AddedKinds
		// is a real field on the struct, but this particular write path
		// never reads it. Kinds is therefore the field that actually adds
		// newKind here; DeletedKinds is read as documented.
		update := &graph.Node{ID: nodeID, Kinds: graph.Kinds{newKind}, DeletedKinds: graph.Kinds{oldKind}, Properties: graph.NewProperties()}
		update.Properties.Delete("stale")

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateNodes([]*graph.Node{update})
		}); err != nil {
			t.Fatalf("BatchOperation (UpdateNodes): %v", err)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "the new kind is present",
			func() int64 { return nodeCountByKind(t, ctx, bt, newKind) }, nodeCountByKind(t, ctx, oracle, newKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the old kind is gone",
			func() int64 { return nodeCountByKind(t, ctx, bt, oldKind) }, nodeCountByKind(t, ctx, oracle, oldKind))

		keepText := fmt.Sprintf(`MATCH (n) WHERE id(n) = %d RETURN n.keep`, nodeID)
		staleText := fmt.Sprintf(`MATCH (n) WHERE id(n) = %d RETURN n.stale`, nodeID)

		requireMarkerDelta(t, buf, cypherServedMarker, 1, "the untouched property survives",
			func() string { return cypherStringValue(t, ctx, bt, keepText) }, cypherStringValue(t, ctx, oracle, keepText))
		requireMarkerDelta(t, buf, cypherServedMarker, 1, "the deleted property is gone",
			func() any { return cypherValueOrNil(t, ctx, bt, staleText) }, cypherValueOrNil(t, ctx, oracle, staleText))

		gotFull := nodeSignaturesByCypher(t, ctx, bt, `MATCH (n:WT5New) RETURN n`)
		wantFull := nodeSignaturesByCypher(t, ctx, oracle, `MATCH (n:WT5New) RETURN n`)
		assertStringMultiset(t, gotFull, wantFull, "full node row (id+properties) after UpdateNodes")

		assertRebuildCountUnchanged(t, d, rebuilds, "BatchUpdateNodesKindsAndPropertyDeletion")
		assertNoNewFallback(t, buf, fallbacks, "BatchUpdateNodesKindsAndPropertyDeletion")
	})

	// -----------------------------------------------------------------
	// Class 6: plain transaction CreateNode + CreateRelationshipByIDs
	// (analysis creates).
	// -----------------------------------------------------------------
	t.Run("TxCreateNodeAndRelationship", func(t *testing.T) {
		nodeKind := graph.StringKind("WT6Node")
		edgeKind := graph.StringKind("WT6Edge")

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			a, err := tx.CreateNode(graph.NewProperties().Set("objectid", "WT6-A"), nodeKind)
			if err != nil {
				return err
			}
			b, err := tx.CreateNode(graph.NewProperties().Set("objectid", "WT6-B"), nodeKind)
			if err != nil {
				return err
			}
			_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.NewProperties())
			return err
		}); err != nil {
			t.Fatalf("WriteTransaction (create node + relationship): %v", err)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "both created nodes are counted",
			func() int64 { return nodeCountByKind(t, ctx, bt, nodeKind) }, nodeCountByKind(t, ctx, oracle, nodeKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the created relationship is counted",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKind) }, relCountByKind(t, ctx, oracle, edgeKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the created relationship's triple fetches",
			func() int { return len(applyRelTriples(t, ctx, bt, edgeKind)) }, len(applyRelTriples(t, ctx, oracle, edgeKind)))

		assertRebuildCountUnchanged(t, d, rebuilds, "TxCreateNodeAndRelationship")
		assertNoNewFallback(t, buf, fallbacks, "TxCreateNodeAndRelationship")
	})

	// -----------------------------------------------------------------
	// Class 7: batch.DeleteRelationship by id (DeleteTransitEdges shape).
	// -----------------------------------------------------------------
	t.Run("BatchDeleteRelationshipByID", func(t *testing.T) {
		nodeKind := graph.StringKind("WT7Node")
		edgeKind := graph.StringKind("WT7Edge")

		var relID graph.ID
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			a, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			b, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			rel, err := tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.NewProperties())
			if err != nil {
				return err
			}
			relID = rel.ID
			return nil
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the edge is counted",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKind) }, int64(1))

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.DeleteRelationship(relID)
		}); err != nil {
			t.Fatalf("BatchOperation (DeleteRelationship): %v", err)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "the deleted edge is gone from the served count",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKind) }, relCountByKind(t, ctx, oracle, edgeKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "neither endpoint node was removed by an edge-only delete",
			func() int64 { return nodeCountByKind(t, ctx, bt, nodeKind) }, nodeCountByKind(t, ctx, oracle, nodeKind))

		assertRebuildCountUnchanged(t, d, rebuilds, "BatchDeleteRelationshipByID")
		assertNoNewFallback(t, buf, fallbacks, "BatchDeleteRelationshipByID")
	})

	// -----------------------------------------------------------------
	// Class 8: batch.DeleteNode cascading to its incident edges.
	// -----------------------------------------------------------------
	t.Run("BatchDeleteNodeCascadesEdges", func(t *testing.T) {
		nodeKind := graph.StringKind("WT8Node")
		edgeKind := graph.StringKind("WT8Edge")

		var startID graph.ID
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			s, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			e, err := tx.CreateNode(graph.NewProperties(), nodeKind)
			if err != nil {
				return err
			}
			if _, err := tx.CreateRelationshipByIDs(s.ID, e.ID, edgeKind, graph.NewProperties()); err != nil {
				return err
			}
			startID = s.ID
			return nil
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: both nodes are counted",
			func() int64 { return nodeCountByKind(t, ctx, bt, nodeKind) }, int64(2))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the edge is fetched as a triple",
			func() int { return len(applyRelTriples(t, ctx, bt, edgeKind)) }, 1)

		if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.DeleteNode(startID)
		}); err != nil {
			t.Fatalf("BatchOperation (DeleteNode): %v", err)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "the deleted node is gone from the served count",
			func() int64 { return nodeCountByKind(t, ctx, bt, nodeKind) }, nodeCountByKind(t, ctx, oracle, nodeKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "its incident edge is gone too (the cascade)",
			func() int { return len(applyRelTriples(t, ctx, bt, edgeKind)) }, len(applyRelTriples(t, ctx, oracle, edgeKind)))

		assertRebuildCountUnchanged(t, d, rebuilds, "BatchDeleteNodeCascadesEdges")
		assertNoNewFallback(t, buf, fallbacks, "BatchDeleteNodeCascadesEdges")
	})

	// -----------------------------------------------------------------
	// Class 9: DeleteNodesByKinds (include + exclude) and
	// DeleteRelationshipsByKinds, driver-level deletes.
	// -----------------------------------------------------------------
	t.Run("DeleteByKindsIncludeExcludeAndRelationships", func(t *testing.T) {
		kindA := graph.StringKind("WT9NodeA")
		kindExcl := graph.StringKind("WT9NodeExcl")
		kindOther := graph.StringKind("WT9NodeOther")

		relNodeKind := graph.StringKind("WT9RelNode")
		edgeKindDropped := graph.StringKind("WT9EdgeDropped")
		edgeKindKept := graph.StringKind("WT9EdgeKept")

		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties(), kindA); err != nil {
				return err
			}
			if _, err := tx.CreateNode(graph.NewProperties(), kindA, kindExcl); err != nil {
				return err
			}
			if _, err := tx.CreateNode(graph.NewProperties(), kindOther); err != nil {
				return err
			}

			a, err := tx.CreateNode(graph.NewProperties(), relNodeKind)
			if err != nil {
				return err
			}
			b, err := tx.CreateNode(graph.NewProperties(), relNodeKind)
			if err != nil {
				return err
			}
			c, err := tx.CreateNode(graph.NewProperties(), relNodeKind)
			if err != nil {
				return err
			}
			if _, err := tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKindDropped, graph.NewProperties()); err != nil {
				return err
			}
			if _, err := tx.CreateRelationshipByIDs(b.ID, c.ID, edgeKindDropped, graph.NewProperties()); err != nil {
				return err
			}
			_, err = tx.CreateRelationshipByIDs(a.ID, c.ID, edgeKindKept, graph.NewProperties())
			return err
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: two nodes carry kindA (one also excluded)",
			func() int64 { return nodeCountByKind(t, ctx, bt, kindA) }, int64(2))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: both dropped-kind edges are counted",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKindDropped) }, int64(2))

		if err := d.DeleteNodesByKinds(ctx, graph.Kinds{kindA}, graph.Kinds{kindExcl}); err != nil {
			t.Fatalf("DeleteNodesByKinds: %v", err)
		}
		if err := d.DeleteRelationshipsByKinds(ctx, graph.Kinds{edgeKindDropped}); err != nil {
			t.Fatalf("DeleteRelationshipsByKinds: %v", err)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "kindA count drops to just the excluded (protected) node",
			func() int64 { return nodeCountByKind(t, ctx, bt, kindA) }, nodeCountByKind(t, ctx, oracle, kindA))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the excluded node itself is untouched",
			func() int64 { return nodeCountByKind(t, ctx, bt, kindExcl) }, nodeCountByKind(t, ctx, oracle, kindExcl))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "an unrelated kind is untouched",
			func() int64 { return nodeCountByKind(t, ctx, bt, kindOther) }, nodeCountByKind(t, ctx, oracle, kindOther))

		requireMarkerDelta(t, buf, builderServedMarker, 1, "every dropped-kind edge is gone",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKindDropped) }, relCountByKind(t, ctx, oracle, edgeKindDropped))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the kept-kind edge survives",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKindKept) }, relCountByKind(t, ctx, oracle, edgeKindKept))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "no node was removed by a relationship-kind delete",
			func() int64 { return nodeCountByKind(t, ctx, bt, relNodeKind) }, nodeCountByKind(t, ctx, oracle, relNodeKind))

		assertRebuildCountUnchanged(t, d, rebuilds, "DeleteByKindsIncludeExcludeAndRelationships")
		assertNoNewFallback(t, buf, fallbacks, "DeleteByKindsIncludeExcludeAndRelationships")
	})

	// -----------------------------------------------------------------
	// Class 10: InIDs Nodes().Update property write (the AGI tagging
	// shape).
	// -----------------------------------------------------------------
	t.Run("InIDsNodeQueryUpdate", func(t *testing.T) {
		kind := graph.StringKind("WT10Node")

		var idA, idB, idC graph.ID
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			a, err := tx.CreateNode(graph.NewProperties(), kind)
			if err != nil {
				return err
			}
			b, err := tx.CreateNode(graph.NewProperties(), kind)
			if err != nil {
				return err
			}
			c, err := tx.CreateNode(graph.NewProperties(), kind)
			if err != nil {
				return err
			}
			idA, idB, idC = a.ID, b.ID, c.ID
			return nil
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Nodes().Filter(query.InIDs(query.NodeID(), idA, idB)).Update(graph.NewProperties().Set("tagged", "yes"))
		}); err != nil {
			t.Fatalf("WriteTransaction (InIDs Update): %v", err)
		}

		taggedText := func(id graph.ID) string {
			return fmt.Sprintf(`MATCH (n) WHERE id(n) = %d RETURN n.tagged`, id)
		}

		for _, id := range []graph.ID{idA, idB} {
			text := taggedText(id)
			requireMarkerDelta(t, buf, cypherServedMarker, 1, fmt.Sprintf("node %d in the InIDs target list is tagged", id),
				func() string { return cypherStringValue(t, ctx, bt, text) }, cypherStringValue(t, ctx, oracle, text))
		}

		untouchedText := taggedText(idC)
		requireMarkerDelta(t, buf, cypherServedMarker, 1, "a node outside the InIDs target list is untagged",
			func() any { return cypherValueOrNil(t, ctx, bt, untouchedText) }, cypherValueOrNil(t, ctx, oracle, untouchedText))

		assertRebuildCountUnchanged(t, d, rebuilds, "InIDsNodeQueryUpdate")
		assertNoNewFallback(t, buf, fallbacks, "InIDsNodeQueryUpdate")
	})

	// -----------------------------------------------------------------
	// Class 11: a write carrying a brand-new kind, never before asserted
	// anywhere in this database's history (runtime kind registration).
	// -----------------------------------------------------------------
	t.Run("RuntimeKindRegistration", func(t *testing.T) {
		// A timestamp-suffixed name, deliberately: the global `kind` table
		// (drivers/pg WipeGraph never truncates it, and this file's own
		// suite-level wipe runs once, before boot, over whatever history
		// this shared long-lived database already carries) would otherwise
		// make loadKinds' boot-time scan (internal/engine/load.go) already
		// know about a FIXED kind name from some earlier run, defeating
		// this class's entire point: proving the applier can resolve a
		// kind id its own View has never seen before at all.
		kind := graph.StringKind(fmt.Sprintf("WT11New%d", time.Now().UnixNano()))
		const objectID = "WT11-1"

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			_, err := tx.CreateNode(graph.NewProperties().Set("objectid", objectID).Set("name", "brand-new-kind"), kind)
			return err
		}); err != nil {
			t.Fatalf("WriteTransaction (brand-new kind create): %v", err)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "the brand-new kind is counted",
			func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, nodeCountByKind(t, ctx, oracle, kind))

		text := fmt.Sprintf(`MATCH (n) WHERE n.objectid = '%s' RETURN n.name`, objectID)
		requireMarkerDelta(t, buf, cypherServedMarker, 1, "a property read against the brand-new kind's node serves",
			func() string { return cypherStringValue(t, ctx, bt, text) }, cypherStringValue(t, ctx, oracle, text))

		assertRebuildCountUnchanged(t, d, rebuilds, "RuntimeKindRegistration")
		assertNoNewFallback(t, buf, fallbacks, "RuntimeKindRegistration")
	})

	// -----------------------------------------------------------------
	// Class 12: a batch whose delegate errors after more than
	// WithBatchSize operations -- the already-flushed chunk stays durable;
	// the replica must converge to PostgreSQL's own committed truth
	// regardless.
	// -----------------------------------------------------------------
	t.Run("PartiallyFailedBatchConverges", func(t *testing.T) {
		kind := graph.StringKind("WT12Node")
		const attempted = 5

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		injected := fmt.Errorf("wt12: injected batch failure")
		err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
			for i := 1; i <= attempted; i++ {
				if err := batch.CreateNode(graph.PrepareNode(graph.NewProperties().Set("objectid", fmt.Sprintf("WT12-%d", i)), kind)); err != nil {
					return err
				}
			}
			return injected
		}, graph.WithBatchSize(2))

		if err == nil {
			t.Fatalf("BatchOperation: got nil error, want the injected failure to propagate")
		}

		wantCommitted := nodeCountByKind(t, ctx, oracle, kind)
		if wantCommitted <= 0 || wantCommitted >= int64(attempted) {
			t.Fatalf("test fixture assumption violated: PostgreSQL committed %d of %d attempted creates, want strictly between 0 and %d (a genuine partial flush)", wantCommitted, attempted, attempted)
		}

		requireMarkerDelta(t, buf, builderServedMarker, 1, "the replica converges to PostgreSQL's own partially-flushed truth",
			func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, wantCommitted)

		gotFull := nodeSignaturesByCypher(t, ctx, bt, `MATCH (n:WT12Node) RETURN n`)
		wantFull := nodeSignaturesByCypher(t, ctx, oracle, `MATCH (n:WT12Node) RETURN n`)
		assertStringMultiset(t, gotFull, wantFull, "full node set after the partially-failed batch")

		assertRebuildCountUnchanged(t, d, rebuilds, "PartiallyFailedBatchConverges")
		assertNoNewFallback(t, buf, fallbacks, "PartiallyFailedBatchConverges")
	})

	// -----------------------------------------------------------------
	// Class 13: fallback -- raw mutating Cypher through Run, the one
	// shape this package deliberately cannot replay narrowly. Exactly one
	// rebuild, then resumed serving with the raw statement's own effect
	// visible.
	// -----------------------------------------------------------------
	t.Run("FallbackViaRawCypherRecovers", func(t *testing.T) {
		kind := graph.StringKind("WT13Node")

		var nodeID graph.ID
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			n, err := tx.CreateNode(graph.NewProperties().Set("name", "before"), kind)
			if err != nil {
				return err
			}
			nodeID = n.ID
			return nil
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()

		text := fmt.Sprintf(`MATCH (n) WHERE id(n) = %d RETURN n.name`, nodeID)
		requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: the property read serves",
			func() string { return cypherStringValue(t, ctx, bt, text) }, "before")

		enteredBefore := markerCount(buf, fallbackEnteredMarker)
		exitedBefore := markerCount(buf, fallbackExitedMarker)

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
		if got := d.engine.RebuildCount(); got != rebuilds+1 {
			t.Fatalf("RebuildCount = %d, want %d -- recovering from a fallback must cost exactly one rebuild", got, rebuilds+1)
		}

		requireMarkerDelta(t, buf, cypherServedMarker, 1, "after recovery: the property read serves the raw statement's own write",
			func() string { return cypherStringValue(t, ctx, bt, text) }, cypherStringValue(t, ctx, oracle, text))

		if _, fresh := d.engine.Fresh(); !fresh {
			t.Fatalf("the engine is still not serving after logging %q", fallbackExitedMarker)
		}
	})

	// -----------------------------------------------------------------
	// Class 14: interleaved concurrent writers -- one goroutine driving
	// class 1's shape (objectid upsert creates), another driving class
	// 7's (batch.DeleteRelationship by id) -- final equality once both
	// finish.
	// -----------------------------------------------------------------
	t.Run("ConcurrentWriters", func(t *testing.T) {
		createKind := graph.StringKind("WT14Create")
		relNodeKind := graph.StringKind("WT14RelNode")
		edgeKind := graph.StringKind("WT14Edge")

		const n = 20

		// The delete goroutine's own fixture -- n relationships to delete
		// concurrently with the create goroutine's writes -- is seeded
		// sequentially, up front, so the only actual CONCURRENCY under
		// test is between the two goroutines' own writes below, not
		// between fixture setup and either of them.
		relIDs := make([]graph.ID, n)
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			for i := 0; i < n; i++ {
				a, err := tx.CreateNode(graph.NewProperties(), relNodeKind)
				if err != nil {
					return err
				}
				b, err := tx.CreateNode(graph.NewProperties(), relNodeKind)
				if err != nil {
					return err
				}
				rel, err := tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.NewProperties())
				if err != nil {
					return err
				}
				relIDs[i] = rel.ID
			}
			return nil
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		rebuilds := d.engine.RebuildCount()
		fallbacks := markerCount(buf, fallbackEnteredMarker)

		var wg sync.WaitGroup
		errs := make(chan error, n*2)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				objectID := fmt.Sprintf("WT14-CREATE-%d", i)
				if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
					return batch.UpdateNodeBy(objectIDUpdate(objectID, graph.NewProperties().Set("i", fmt.Sprintf("%d", i)), createKind))
				}); err != nil {
					errs <- fmt.Errorf("create goroutine, i=%d: %w", i, err)
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, id := range relIDs {
				if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
					return batch.DeleteRelationship(id)
				}); err != nil {
					errs <- fmt.Errorf("delete goroutine, id=%d: %w", id, err)
				}
			}
		}()

		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent write failed: %v", err)
		}

		// Verification runs strictly after both goroutines have finished,
		// so the served-marker deltas below are once again attributable
		// to exactly the one call each wraps (see requireMarkerDelta's own
		// doc on why that discipline matters).
		requireMarkerDelta(t, buf, builderServedMarker, 1, "every concurrently-created node is present",
			func() int64 { return nodeCountByKind(t, ctx, bt, createKind) }, nodeCountByKind(t, ctx, oracle, createKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "every concurrently-deleted relationship is gone",
			func() int64 { return relCountByKind(t, ctx, bt, edgeKind) }, relCountByKind(t, ctx, oracle, edgeKind))
		requireMarkerDelta(t, buf, builderServedMarker, 1, "the deleted relationships' endpoint nodes were untouched",
			func() int64 { return nodeCountByKind(t, ctx, bt, relNodeKind) }, nodeCountByKind(t, ctx, oracle, relNodeKind))

		gotFull := nodeSignaturesByCypher(t, ctx, bt, `MATCH (n:WT14Create) RETURN n`)
		wantFull := nodeSignaturesByCypher(t, ctx, oracle, `MATCH (n:WT14Create) RETURN n`)
		assertStringMultiset(t, gotFull, wantFull, "full concurrently-created node set")

		assertRebuildCountUnchanged(t, d, rebuilds, "ConcurrentWriters")
		assertNoNewFallback(t, buf, fallbacks, "ConcurrentWriters")
	})
}
