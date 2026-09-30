// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// This file's fixture kinds and second-graph name, distinct from every other
// suite's so no other test's data lands in its counts.
var (
	multiGraphBuilderNodeKind = graph.StringKind("MultiGraphBuilderNode")
	multiGraphBuilderEdgeKind = graph.StringKind("MultiGraphBuilderEdge")
)

const multiGraphBuilderSecondGraphName = "bloodtrail_test_builder_second_graph"

// TestBuilderServingDeclinesInMultiGraphDatabase pins the builder-serving
// half of the multi-graph guard (TestCypherMultiGraphGuard pins TryCypher's).
// dawgs' PostgreSQL reads are not scoped by graph -- its translator uses the
// graph id only for CREATE -- so with a second graph holding nodes, a count
// by kind, a relationship count and a shortest path between the other
// graph's nodes all span every graph, while the replica holds only the
// default one and would answer short. Before the second graph exists the
// builder shapes serve from the replica; once it does they must decline and
// return PostgreSQL's answers.
func TestBuilderServingDeclinesInMultiGraphDatabase(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.NewProperties(), multiGraphBuilderNodeKind)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.NewProperties(), multiGraphBuilderNodeKind)
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, multiGraphBuilderEdgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("default-graph fixture: %v", err)
	}
	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "single graph: node count serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, multiGraphBuilderNodeKind) }, 2)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "single graph: relationship count serves",
		func() int64 { return relCountByKind(t, ctx, bt, multiGraphBuilderEdgeKind) }, 1)

	// The second graph is written through the raw pg driver, as dawgs
	// creates a graph lazily on its first node. Its nodes and edges are wiped
	// when the test ends so no later test in this database sees a multi-graph
	// database; the graph's catalog row stays behind, which the engine's
	// multi-graph probe ignores (it counts only graphs holding a node).
	oracle, _ := graphtest.OpenPG(t, graphtest.PGAvailable(t))
	t.Cleanup(func() { graphtest.WipeGraph(t, oracle) })

	var otherA, otherB graph.ID
	if err := oracle.WriteTransaction(ctx, func(tx graph.Transaction) error {
		other := tx.WithGraph(graph.Graph{Name: multiGraphBuilderSecondGraphName})
		a, err := other.CreateNode(graph.NewProperties(), multiGraphBuilderNodeKind)
		if err != nil {
			return err
		}
		b, err := other.CreateNode(graph.NewProperties(), multiGraphBuilderNodeKind)
		if err != nil {
			return err
		}
		otherA, otherB = a.ID, b.ID
		_, err = other.CreateRelationshipByIDs(a.ID, b.ID, multiGraphBuilderEdgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("second-graph fixture: %v", err)
	}
	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	wantNodes := nodeCountByKind(t, ctx, oracle, multiGraphBuilderNodeKind)
	wantRels := relCountByKind(t, ctx, oracle, multiGraphBuilderEdgeKind)
	wantPaths := shortestPathCount(t, ctx, oracle, otherA, otherB)
	if wantNodes != 4 || wantRels != 2 || wantPaths != 1 {
		t.Fatalf("PostgreSQL answers nodes=%d rels=%d paths=%d, want 4, 2, 1: the fixture no longer spans both graphs", wantNodes, wantRels, wantPaths)
	}

	requireDecline(t, buf, "multi_graph", "multi graph: node count declines and matches PostgreSQL",
		func() int64 { return nodeCountByKind(t, ctx, bt, multiGraphBuilderNodeKind) }, wantNodes)
	requireDecline(t, buf, "multi_graph", "multi graph: relationship count declines and matches PostgreSQL",
		func() int64 { return relCountByKind(t, ctx, bt, multiGraphBuilderEdgeKind) }, wantRels)
	requireDecline(t, buf, "multi_graph", "multi graph: shortest path in the other graph declines and matches PostgreSQL",
		func() int { return shortestPathCount(t, ctx, bt, otherA, otherB) }, wantPaths)
}
