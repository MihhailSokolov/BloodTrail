// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// withObjectIDUniqueIndex creates the unique index on the default graph's
// node partition that BloodHound's own graph schema declares for objectid
// (graph.Constraint{Field: "objectid"}) and that dawgs' objectid-keyed
// upserts need as their ON CONFLICT target, dropping it again on cleanup so
// a later test in this package can still write duplicate objectids.
//
// Issued as DDL rather than through pgDriver.AssertSchema because
// SchemaManager.AssertGraph (dawgs v0.8.0, drivers/pg/manager.go) returns
// the cached graph definition without reconciling any index once the same
// driver instance has already asserted that graph -- which graphtest.OpenPG
// has, with a schema declaring no constraints. The index name and shape
// mirror dawgs' own (model.ConstraintName, query.formatCreatePropertyConstraint).
func withObjectIDUniqueIndex(t *testing.T, pgDriver *pg.Driver, pool *pgxpool.Pool) {
	t.Helper()

	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatalf("no default graph is set")
	}
	table := graphModel.Partitions.Node.Name
	name := table + "_objectid_constraint"

	if _, err := pool.Exec(context.Background(), fmt.Sprintf(
		"create unique index %s on %s using btree ((%s.properties->>'objectid'))", name, table, table)); err != nil {
		t.Fatalf("create the objectid unique index: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "drop index if exists "+name); err != nil {
			t.Errorf("drop the objectid unique index: %v", err)
		}
	})
}

// rekeyedEndpointUpdate builds the graph.RelationshipUpdate shape
// BloodHound's ingest uses for an edge whose endpoints are identified by
// objectid (services/graphify's IngestRelationships; the same shape
// integration/opengraph_integration_test.go replays), so the write below
// goes through dawgs' real pg upsert rather than hand-written SQL.
func rekeyedEndpointUpdate(startOID, endOID string, nodeKind, edgeKind graph.Kind) graph.RelationshipUpdate {
	endpoint := func(objectID string) *graph.Node {
		return graph.PrepareNode(graph.AsProperties(map[string]any{"objectid": objectID}), nodeKind)
	}
	return graph.RelationshipUpdate{
		Start:                   endpoint(startOID),
		StartIdentityKind:       nodeKind,
		StartIdentityProperties: []string{"objectid"},
		End:                     endpoint(endOID),
		EndIdentityKind:         nodeKind,
		EndIdentityProperties:   []string{"objectid"},
		Relationship:            graph.PrepareRelationship(graph.NewProperties(), edgeKind),
	}
}

// TestRekeyedEndpointStagesTheUpsertedEdge races an UpdateRelationshipBy
// upsert against a re-key of one of its endpoints: the edge is created by
// the upsert, then another write rewrites the start node's objectid before
// the upsert's own Apply runs, so read-back's objectid lookup matches no
// row for that endpoint.
//
// The endpoint is not gone -- it kept its id, its row and its edges -- so
// the View node read-back re-reads under the old objectid comes back
// PRESENT, which settles the NODE but says nothing about the edge. The
// replica must still reflect the committed edge while it serves: a served
// read has to agree with the pg driver reading the same database.
func TestRekeyedEndpointStagesTheUpsertedEdge(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	withObjectIDUniqueIndex(t, pgDriver, pool)

	nodeKind := graph.StringKind("RekeyEndpointNode")
	edgeKind := graph.StringKind("RekeyEndpointEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	const (
		startOID    = "S-1-5-21-7-1100"
		endOID      = "S-1-5-21-7-1101"
		rekeyedToID = "S-1-5-21-7-1200"
	)

	var start *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		if start, err = tx.CreateNode(graph.NewProperties().Set("name", "start").Set("objectid", startOID), nodeKind); err != nil {
			return err
		}
		_, err = tx.CreateNode(graph.NewProperties().Set("name", "end").Set("objectid", endOID), nodeKind)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// The write: one objectid-keyed edge upsert, through the real pg driver.
	if err := pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateRelationshipBy(rekeyedEndpointUpdate(startOID, endOID, nodeKind, edgeKind))
	}); err != nil {
		t.Fatalf("upsert relationship: %v", err)
	}

	// The race: the start endpoint is re-keyed after that edge committed but
	// before the upsert's Apply runs, so the upsert's own objectid no longer
	// matches any row.
	start.Properties.Set("objectid", rekeyedToID)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(start)
	}); err != nil {
		t.Fatalf("rewrite objectid: %v", err)
	}

	// Exactly what write_observer.go's recordRelationshipUpsertIdentity
	// records for that batch call: the objectid-keyed triple plus both
	// endpoints' own objectids.
	scope := NewWriteScope()
	scope.Changes().RecordEdgeTripleByObjectID(startOID, endOID, edgeKind)
	scope.Changes().RecordNodeObjectID(startOID)
	scope.Changes().RecordNodeObjectID(endOID)
	eng.Apply(ctx, scope)

	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine left serving; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (s:RekeyEndpointNode)-[:RekeyEndpointEdge]->(e:RekeyEndpointNode) RETURN s, e`, true},
		{`MATCH (s:RekeyEndpointNode) RETURN s`, true},
		{`MATCH (s) WHERE s.objectid = '` + rekeyedToID + `' RETURN s`, true},
	})
}
