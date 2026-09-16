// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherKeysMatchOracle pins Result.Keys() parity between the engine
// and the pg driver for every projection-naming shape the interpreter
// serves: an explicit alias, a bare variable, and an unaliased property
// lookup (where PostgreSQL emits its placeholder column name, since dawgs
// generates the projection with no SQL alias). The oracle's own Keys()
// are the authority -- including their lifecycle: the pg driver's Keys()
// is populated by Next() (nil before the first call, and never populated
// at all for a zero-row result), so both sides are read the way a real
// caller reads them, before, during and after iteration.
func TestTryCypherKeysMatchOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	kind := graph.StringKind("KeysNode")
	edgeKind := graph.StringKind("KeysEdge")
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a, err := tx.CreateNode(graph.NewProperties().Set("name", "alice").Set("objectid", "K-1"), kind)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.NewProperties().Set("name", "bob").Set("objectid", "K-2"), kind)
		if err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// keysTrace reads a Result the way a caller does, recording Keys() at
	// every lifecycle point: before the first Next(), after each successful
	// Next(), and after Next() has returned false.
	keysTrace := func(result graph.Result) [][]string {
		defer result.Close()
		trace := [][]string{append([]string(nil), result.Keys()...)}
		for result.Next() {
			trace = append(trace, append([]string(nil), result.Keys()...))
		}
		return append(trace, append([]string(nil), result.Keys()...))
	}

	for _, query := range []string{
		`MATCH (n:KeysNode) RETURN n`,
		`MATCH (n:KeysNode)-[r:KeysEdge]->(m:KeysNode) RETURN n, r, m`,
		`MATCH p = (n:KeysNode)-[:KeysEdge]->(m:KeysNode) RETURN p`,
		`MATCH (n:KeysNode) RETURN n.name`,
		`MATCH (n:KeysNode) RETURN n.name AS name`,
		`MATCH (n:KeysNode) RETURN n, n.name`,
		`MATCH (n:KeysNode) RETURN n.name, n.objectid`,
		`MATCH (n:KeysNode) RETURN toLower(n.name) AS lowered, n`,
		`MATCH (n:KeysNode) RETURN count(n) AS c`,
		// RETURN-position aggregation: pg names an unaliased aggregate
		// after its function and an unaliased property lookup `?column?`,
		// which is where those rules in desugarReturnAggregates come from.
		`MATCH (n:KeysNode) RETURN count(n)`,
		`MATCH (n:KeysNode) RETURN count(*)`,
		`MATCH (n:KeysNode) RETURN n.name, count(n)`,
		`MATCH (n:KeysNode) RETURN n.name, count(n) ORDER BY count(n) DESC`,
		// Zero rows: the oracle never populates Keys at all for these.
		`MATCH (n:KeysNode) WHERE n.name = 'nobody' RETURN n.name`,
		`MATCH (n:KeysNode) WHERE n.name = 'nobody' RETURN n AS renamed`,
	} {
		t.Run(query, func(t *testing.T) {
			var engineTrace [][]string
			var served bool
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result, ok := eng.TryCypher(ctx, tx, query, nil)
				served = ok
				if !ok {
					return nil
				}
				engineTrace = keysTrace(result)
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (engine): %v", err)
			}
			if !served {
				t.Skipf("TryCypher declined this shape: %s", query)
			}

			var oracleTrace [][]string
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				oracleTrace = keysTrace(tx.Query(query, map[string]any{}))
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (oracle): %v", err)
			}

			if !reflect.DeepEqual(engineTrace, oracleTrace) {
				t.Fatalf("Keys() lifecycle diverges\nengine: %#v\noracle: %#v\nquery: %s", engineTrace, oracleTrace, query)
			}
		})
	}
}
