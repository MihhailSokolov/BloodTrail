// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestCompactionKeepsAnEdgeWhoseEndpointAppliesLater drives the reordering
// against PostgreSQL: W1 creates node n and commits, W2 creates edge m -> n
// and commits after it, and W2's Apply wins applyMu first -- nothing ties
// the order writers reach Apply to the order they committed. A compaction
// triggered off W2's publish captures the stack with the edge's endpoint
// still unknown; W1 applies while it folds; the fold is adopted. Before and
// after the adoption the served answer must be PostgreSQL's, which has the
// edge.
func TestCompactionKeepsAnEdgeWhoseEndpointAppliesLater(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	nodeKind := graph.StringKind("PendingEdgeNode")
	edgeKind := graph.StringKind("PendingEdgeRel")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	var m *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		m, err = tx.CreateNode(graph.NewProperties().Set("name", "m"), nodeKind)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	t.Cleanup(eng.Stop)
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// W1 commits: node n.
	var n *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		n, err = tx.CreateNode(graph.NewProperties().Set("name", "n"), nodeKind)
		return err
	}); err != nil {
		t.Fatalf("W1: %v", err)
	}
	scope1 := NewWriteScope()
	scope1.Changes().RecordNodeID(n.ID)

	// W2 commits after W1: edge m -> n.
	var rel *graph.Relationship
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		rel, err = tx.CreateRelationshipByIDs(m.ID, n.ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("W2: %v", err)
	}
	scope2 := NewWriteScope()
	scope2.Changes().RecordEdgeID(rel.ID)

	// W2 applies first, and a compaction captures (base, segments) off its
	// publish, exactly as maybeStartCompaction does.
	eng.Apply(ctx, scope2)
	captured := eng.snap.Load()
	capturedBase, capturedSegs := captured.Base(), captured.Segments()

	// W1 applies while the fold runs.
	eng.Apply(ctx, scope1)

	cases := []typedCase{{`MATCH (s:PendingEdgeNode)-[:PendingEdgeRel]->(e:PendingEdgeNode) RETURN s, e`, true}}
	requireOracleRowTotal(t, pgDriver, cases[0].query, 1)
	t.Run("before the compaction is adopted", func(t *testing.T) {
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})

	// The compaction goroutine's body: fold the captured stack, adopt it
	// with W1's segment as the rebased tail.
	eng.runCompaction(capturedBase, capturedSegs)
	if got := eng.CompactionCount(); got != 1 {
		t.Fatalf("CompactionCount() = %d, want 1", got)
	}
	if _, serving := eng.serveState(); !serving {
		t.Fatal("engine left serving; the comparison below would be vacuous")
	}
	t.Run("after the compaction is adopted", func(t *testing.T) {
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})
}
