// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"fmt"
	"sync"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestKindDeleteTransactionKeepsEdgeCommittedWhileItWasOpen: a transaction
// deletes every relationship of a kind and stays open; meanwhile an ordinary
// writer creates an edge of that kind, commits, and is applied. PostgreSQL's
// DELETE never saw that edge, so it survives the deleting transaction's
// commit -- and the replica must still serve it once that transaction's own
// Apply has run.
func TestKindDeleteTransactionKeepsEdgeCommittedWhileItWasOpen(t *testing.T) {
	_, bt, buf, ctx := openApplyDriver(t)
	pgDriver, _ := graphtest.OpenPG(t, graphtest.PGAvailable(t))

	nodeKind := graph.StringKind("KindDelTxNode")
	edgeKind := graph.StringKind("KindDelTxEdge")

	var a, b, c *graph.Node
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		if a, err = tx.CreateNode(graph.NewProperties().Set("name", "a"), nodeKind); err != nil {
			return err
		}
		if b, err = tx.CreateNode(graph.NewProperties().Set("name", "b"), nodeKind); err != nil {
			return err
		}
		if c, err = tx.CreateNode(graph.NewProperties().Set("name", "c"), nodeKind); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	deleted := make(chan struct{})
	proceed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			if err := tx.Relationships().Filter(query.Kind(query.Relationship(), edgeKind)).Delete(); err != nil {
				return err
			}
			close(deleted)
			<-proceed
			return nil
		})
	}()
	<-deleted

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(b.ID, c.ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("concurrent create: %v", err)
	}

	close(proceed)
	if err := <-done; err != nil {
		t.Fatalf("deleting transaction: %v", err)
	}

	pgCount := relCountByKind(t, ctx, pgDriver, edgeKind)
	if pgCount != 1 {
		t.Fatalf("PostgreSQL holds %d %s edges, want the 1 created while the delete was open", pgCount, edgeKind)
	}
	requireMarkerDelta(t, buf, builderServedMarker, 1, "served relationship count equals PostgreSQL's",
		func() int64 { return relCountByKind(t, ctx, bt, edgeKind) }, pgCount)
	assertNoFallback(t, buf)
}

// TestDeleteRelationshipsByKindsRacingWritersServesPostgreSQLCount is the
// same property with no staging at all: writers keep creating edges of a
// kind while Driver.DeleteRelationshipsByKinds deletes that kind. Edges that
// commit after the DELETE's snapshot survive in PostgreSQL, and some of them
// are applied before the delete's own Apply; after every round the served
// count must equal PostgreSQL's.
func TestDeleteRelationshipsByKindsRacingWritersServesPostgreSQLCount(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)
	pgDriver, _ := graphtest.OpenPG(t, graphtest.PGAvailable(t))

	const writers, perWriter, rounds = 4, 30, 8
	nodeKind := graph.StringKind("KindDelRaceNode")

	var sources, targets []graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < writers+perWriter; i++ {
			n, err := tx.CreateNode(graph.NewProperties().Set("name", fmt.Sprintf("n%d", i)), nodeKind)
			if err != nil {
				return err
			}
			if i < writers {
				sources = append(sources, n.ID)
			} else {
				targets = append(targets, n.ID)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	for round := 0; round < rounds; round++ {
		edgeKind := graph.StringKind(fmt.Sprintf("KindDelRaceEdge%d", round))
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			_, err := tx.CreateRelationshipByIDs(targets[0], targets[1], edgeKind, graph.NewProperties())
			return err
		}); err != nil {
			t.Fatalf("round %d: seed edge: %v", round, err)
		}

		var wg sync.WaitGroup
		started := make(chan struct{})
		var startOnce sync.Once
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < perWriter; i++ {
					if i == perWriter/6 {
						startOnce.Do(func() { close(started) })
					}
					if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
						_, err := tx.CreateRelationshipByIDs(sources[w], targets[i], edgeKind, graph.NewProperties())
						return err
					}); err != nil {
						t.Errorf("round %d: writer %d: %v", round, w, err)
						return
					}
				}
			}(w)
		}
		<-started
		if err := d.DeleteRelationshipsByKinds(ctx, graph.Kinds{edgeKind}); err != nil {
			t.Fatalf("round %d: delete: %v", round, err)
		}
		wg.Wait()

		pgCount := relCountByKind(t, ctx, pgDriver, edgeKind)
		requireMarkerDelta(t, buf, builderServedMarker, 1, fmt.Sprintf("round %d: served relationship count equals PostgreSQL's", round),
			func() int64 { return relCountByKind(t, ctx, bt, edgeKind) }, pgCount)
	}
	assertNoFallback(t, buf)
}
