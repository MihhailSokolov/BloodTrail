// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherMixedChainEdgeUniquenessMatchesOracle is the live-oracle pin
// for cross-step relationship uniqueness in mixed fixed/var-length chains
// (Row.trailEdges, interpret): dawgs emits `e != all (path)` between a fixed
// step and a variable-length expansion of the same pattern in BOTH orders,
// so over a two-node cycle the engine must (a) refuse to extend a trail
// across the fixed step's own edge and (b) refuse to bind a fixed step to an
// edge the preceding trail consumed -- exactly what PostgreSQL itself
// returns for the same queries. Both queries must be SERVED: a decline here
// would silently drop the differential this test exists to make.
func TestTryCypherMixedChainEdgeUniquenessMatchesOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	var (
		rootKind   = graph.StringKind("ChainRoot")
		midKind    = graph.StringKind("ChainMid")
		targetKind = graph.StringKind("ChainTarget")
		edgeKind   = graph.StringKind("ChainE")
	)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.NewProperties(), rootKind, targetKind)
		if err != nil {
			return err
		}
		x, err := tx.CreateNode(graph.NewProperties(), midKind, targetKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(a.ID, x.ID, edgeKind, graph.NewProperties()); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(x.ID, a.ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	for _, tc := range []struct {
		name       string
		query      string
		allowEmpty bool
	}{
		{
			// The trail may take x->a but never re-take the fixed step's own
			// a->x: exactly one path exists.
			name:  "fixed step then trail",
			query: `MATCH p = (a:ChainRoot)-[:ChainE]->(m:ChainMid)-[:ChainE*1..2]->(b:ChainTarget) RETURN p`,
		},
		{
			// The trail consumes both edges of the cycle, so the closing fixed
			// step has nothing left to bind: zero paths, on both sides.
			name:       "trail then fixed step",
			query:      `MATCH p = (a:ChainRoot)-[:ChainE*2..2]->(m:ChainRoot)-[:ChainE]->(b:ChainMid) RETURN p`,
			allowEmpty: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var enginePaths graph.PathSet
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result, served := eng.TryCypher(ctx, tx, tc.query, nil)
				if !served {
					t.Fatalf("TryCypher declined; this shape must be served: %s", tc.query)
				}
				enginePaths = drainEngineResult(t, result, tc.query)
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (engine): %v", err)
			}

			var oraclePaths graph.PathSet
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				oraclePaths = drainOracleResult(t, tx, tc.query)
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (oracle): %v", err)
			}

			assertSameSet(t, enginePaths, oraclePaths, tc.allowEmpty)
		})
	}
}

// TestTryCypherSelfLoopVarLengthDeclines is the live pin for the self-loop
// hazard gate (View.SelfLoopHazard, interpret's SELF-LOOPS decline): with a
// self-loop of the pattern's own edge kind in the graph, a variable-length
// pattern must NOT be served from memory -- PostgreSQL's recursive CTE
// places its seed-side is_cycle guard on a pattern edge the engine cannot
// predict without mirroring dawgs' optimizer heuristics -- while the same
// pattern over a self-loop-free kind in the same graph keeps serving, equal
// to the oracle.
func TestTryCypherSelfLoopVarLengthDeclines(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	var (
		targetKind = graph.StringKind("SLTarget")
		loopKind   = graph.StringKind("SLE")
		cleanKind  = graph.StringKind("SLF")
	)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n1, err := tx.CreateNode(graph.NewProperties(), targetKind)
		if err != nil {
			return err
		}
		n2, err := tx.CreateNode(graph.NewProperties(), targetKind)
		if err != nil {
			return err
		}
		n3, err := tx.CreateNode(graph.NewProperties(), targetKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(n1.ID, n1.ID, loopKind, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(n1.ID, n2.ID, loopKind, graph.NewProperties()); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(n3.ID, n1.ID, cleanKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	t.Run("hazardous kind declines", func(t *testing.T) {
		const query = `MATCH p = (s)-[:SLE*1..3]->(t:SLTarget) RETURN p`
		if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			result, served := eng.TryCypher(ctx, tx, query, nil)
			if served {
				result.Close()
				t.Fatalf("TryCypher served a var-length pattern over a kind with a self-loop; want a decline: %s", query)
			}
			return nil
		}); err != nil {
			t.Fatalf("ReadTransaction: %v", err)
		}
	})

	t.Run("clean kind still serves and matches the oracle", func(t *testing.T) {
		const query = `MATCH p = (s)-[:SLF*1..3]->(t:SLTarget) RETURN p`
		var enginePaths graph.PathSet
		if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			result, served := eng.TryCypher(ctx, tx, query, nil)
			if !served {
				t.Fatalf("TryCypher declined the self-loop-free kind; want served: %s", query)
			}
			enginePaths = drainEngineResult(t, result, query)
			return nil
		}); err != nil {
			t.Fatalf("ReadTransaction (engine): %v", err)
		}

		var oraclePaths graph.PathSet
		if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			oraclePaths = drainOracleResult(t, tx, query)
			return nil
		}); err != nil {
			t.Fatalf("ReadTransaction (oracle): %v", err)
		}
		assertSameSet(t, enginePaths, oraclePaths, false)
	})
}
