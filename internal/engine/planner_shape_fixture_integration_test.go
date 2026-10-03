// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// plannerShapeNode is one fixture node for the planner-shape differentials:
// any number of kinds, and a property bag.
type plannerShapeNode struct {
	kinds []string
	props map[string]any
}

// plannerShapeEdge is one fixture edge, between two plannerShapeNode indexes.
type plannerShapeEdge struct {
	from, to int
	kind     string
}

// seedPlannerShapeGraph wipes the graph, writes nodes and edges, and returns
// an engine serving a snapshot of them, with the pool and the node ids a
// test needs to go on writing deltas over that snapshot.
func seedPlannerShapeGraph(t *testing.T, nodes []plannerShapeNode, edges []plannerShapeEdge) (*pg.Driver, *pgxpool.Pool, *Engine, []graph.ID) {
	t.Helper()
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	ids := make([]graph.ID, len(nodes))
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i, n := range nodes {
			props := graph.NewProperties()
			for k, v := range n.props {
				props.Set(k, v)
			}
			var kinds graph.Kinds
			for _, k := range n.kinds {
				kinds = append(kinds, graph.StringKind(k))
			}
			created, err := tx.CreateNode(props, kinds...)
			if err != nil {
				return err
			}
			ids[i] = created.ID
		}
		for _, e := range edges {
			if _, err := tx.CreateRelationshipByIDs(ids[e.from], ids[e.to], graph.StringKind(e.kind), graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	return pgDriver, pool, eng, ids
}
