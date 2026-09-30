// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file pin one property of kind-scoped deletes
// (DeleteNodesByKinds, DeleteRelationshipsByKinds, a kind-matcher
// Relationships().Delete()): the replica ends up holding exactly what
// PostgreSQL holds, whatever order the Applies of concurrent writes run in.
// Nothing orders applyMu -- or the boot gap's replay -- by commit order, so
// a delete's Apply can run after the Apply of a write that committed after
// it, and PostgreSQL's delete never saw that write's rows.

// seedKindDeleteOrderGraph wipes the graph, asserts kinds and writes nodes
// named a, b and c of nodeKind plus one a -> b edge of edgeKind, and returns
// an engine serving a snapshot of it.
func seedKindDeleteOrderGraph(t *testing.T, ctx context.Context, nodeKind, edgeKind graph.Kind) (*Engine, map[string]*graph.Node) {
	t.Helper()

	pgDriver, pool := graphtest.OpenPG(t, graphtest.PGAvailable(t))
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	nodes := make(map[string]*graph.Node, 3)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, name := range []string{"a", "b", "c"} {
			n, err := tx.CreateNode(graph.NewProperties().Set("name", name), nodeKind)
			if err != nil {
				return err
			}
			nodes[name] = n
		}
		_, err := tx.CreateRelationshipByIDs(nodes["a"].ID, nodes["b"].ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	return eng, nodes
}

// TestEdgeKindDeleteAppliedAfterLaterCreateKeepsTheCreatedEdge: W1 deletes
// every edge of a kind and commits; W2 creates an edge of that kind and
// commits after it, so PostgreSQL keeps W2's edge -- but W2's Apply wins
// applyMu first. W1's Apply must not take W2's edge with it.
func TestEdgeKindDeleteAppliedAfterLaterCreateKeepsTheCreatedEdge(t *testing.T) {
	ctx := context.Background()
	nodeKind := graph.StringKind("KindDelOrderNode")
	edgeKind := graph.StringKind("KindDelOrderEdge")
	eng, nodes := seedKindDeleteOrderGraph(t, ctx, nodeKind, edgeKind)

	if err := eng.pgDriver.DeleteRelationshipsByKinds(ctx, graph.Kinds{edgeKind}); err != nil {
		t.Fatalf("W1 delete: %v", err)
	}
	deleteScope := NewWriteScope()
	deleteScope.Changes().RecordDeleteRelationshipsByKinds(graph.Kinds{edgeKind})

	var created *graph.Relationship
	if err := eng.pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		created, err = tx.CreateRelationshipByIDs(nodes["b"].ID, nodes["c"].ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("W2 create: %v", err)
	}
	createScope := NewWriteScope()
	createScope.Changes().RecordEdgeID(created.ID)

	eng.Apply(ctx, createScope)
	eng.Apply(ctx, deleteScope)

	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine left serving; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, eng.pgDriver, eng, []typedCase{
		{`MATCH (s:KindDelOrderNode)-[:KindDelOrderEdge]->(e:KindDelOrderNode) RETURN s, e`, true},
	})
}

// TestSourcelessNodeDeleteAppliedAfterLaterCreateKeepsTheCreatedNodes is the
// node-side twin, in the shape BloodHound's "delete sourceless data" issues:
// no include kinds, the MigrationData kind and every source kind excluded.
// The base MigrationData node carries no objectid, exactly as BloodHound
// writes it. W2 creates a sourceless node and a kind-less one (the shape of
// the changelog's lastseen-only upsert creating a node) after W1's delete
// committed; both survive in PostgreSQL, and must survive W1's late Apply.
func TestSourcelessNodeDeleteAppliedAfterLaterCreateKeepsTheCreatedNodes(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	var (
		migrationData = graph.StringKind("MigrationData")
		adSource      = graph.StringKind("Base")
		azureSource   = graph.StringKind("AZBase")
		user          = graph.StringKind("User")
	)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{migrationData, adSource, azureSource, user}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateNode(graph.NewProperties().Set("Major", 9).Set("Minor", 7).Set("Patch", 1), migrationData); err != nil {
			return err
		}
		if _, err := tx.CreateNode(graph.NewProperties().Set("name", "sourced").Set("objectid", "S-1-5-21-1-1001"), user, adSource); err != nil {
			return err
		}
		_, err := tx.CreateNode(graph.NewProperties().Set("name", "old sourceless").Set("objectid", "S-1-5-21-1-1002"), user)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	excludeSourceless := graph.Kinds{migrationData, adSource, azureSource}
	if err := pgDriver.DeleteNodesByKinds(ctx, nil, excludeSourceless); err != nil {
		t.Fatalf("W1 delete: %v", err)
	}
	deleteScope := NewWriteScope()
	deleteScope.Changes().RecordDeleteNodesByKinds(nil, excludeSourceless)

	var fresh, kindless *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		if fresh, err = tx.CreateNode(graph.NewProperties().Set("name", "fresh sourceless").Set("objectid", "S-1-5-21-1-1003"), user); err != nil {
			return err
		}
		kindless, err = tx.CreateNode(graph.NewProperties().Set("name", "kindless").Set("objectid", "S-1-5-21-1-1004"))
		return err
	}); err != nil {
		t.Fatalf("W2 create: %v", err)
	}
	createScope := NewWriteScope()
	createScope.Changes().RecordNodeID(fresh.ID)
	createScope.Changes().RecordNodeID(kindless.ID)

	eng.Apply(ctx, createScope)
	eng.Apply(ctx, deleteScope)

	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine left serving; the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:User) RETURN n`, true},
		{`MATCH (n) WHERE n.objectid = 'S-1-5-21-1-1004' RETURN n`, true},
		{`MATCH (n:MigrationData) RETURN n.Major`, true},
	})
}

// TestBootReplayKeepsNodeCommittedAfterKindDeleteWithEarlierCounter is the
// boot-gap form. The snapshot-file adoption replays the writes buffered
// during boot in ascending watermark-counter order, and a write takes its
// counter at its FIRST mutating call, not at its commit: B bumps and creates
// a node inside a transaction that stays open; D bumps after it, deletes
// every node of that kind -- which cannot see B's uncommitted row -- and
// commits first; B commits last. PostgreSQL keeps B's node, and so must the
// adopted replica, although the replay runs B before D.
func TestBootReplayKeepsNodeCommittedAfterKindDeleteWithEarlierCounter(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.captureStartState(ctx) // what Start does, in its order
	eng.bootGap.activate()

	counterB, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("B's bump: %v", err)
	}
	created := make(chan graph.ID, 1)
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
			n, err := tx.CreateNode(graph.NewProperties().Set("name", "late").Set("objectid", "late-commit"), fileBootKind)
			if err != nil {
				return err
			}
			created <- n.ID
			<-release
			return nil
		})
	}()
	nodeB := <-created

	counterD, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("D's bump: %v", err)
	}
	if err := pgDriver.DeleteNodesByKinds(ctx, graph.Kinds{fileBootKind}, nil); err != nil {
		t.Fatalf("D's delete: %v", err)
	}
	scopeD := NewWriteScope()
	scopeD.SetWatermark(counterD)
	scopeD.Changes().RecordDeleteNodesByKinds(graph.Kinds{fileBootKind}, nil)
	eng.Apply(ctx, scopeD)

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("B's commit: %v", err)
	}
	scopeB := NewWriteScope()
	scopeB.SetWatermark(counterB)
	scopeB.Changes().RecordNodeID(nodeB)
	eng.Apply(ctx, scopeB)

	if !eng.tryLoadSnapshotFile(ctx) {
		t.Fatalf("snapshot file not adopted:\n%s", buf.String())
	}
	view, serving := eng.Fresh()
	if !serving {
		t.Fatalf("not serving after the file was adopted")
	}
	dense, known := view.Dense(uint64(nodeB))
	if !known || !view.Alive(dense) {
		t.Errorf("node %d (counter %d) committed after the kind delete (counter %d) and PostgreSQL keeps it, but the adopted replica does not", nodeB, counterB, counterD)
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:FileBootNode) RETURN n`, true},
	})
}
