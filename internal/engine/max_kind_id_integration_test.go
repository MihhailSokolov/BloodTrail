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

// largestSmallserialKindID is the last id PostgreSQL's `kind.id smallserial`
// column can issue.
const largestSmallserialKindID = 32767

// maxKindIDTestKinds are the kind names the tests below register; each run
// removes them again so the kind id sequence can be put back where it was.
var maxKindIDTestKinds = []string{"MaxIDNodeKind", "MaxIDEdgeKind", "MaxIDEnd", "MaxIDLateKind"}

// resetMaxKindIDTestKinds removes every row carrying the tests' kinds and the
// kinds themselves, then points the kind id sequence just past the largest id
// still in use -- undoing claimLargestSmallserialKindID. Run before a test as
// well as after it, since a run that crashed would otherwise leave the
// sequence exhausted for every later test in the database.
//
// An empty kind table sets the sequence to hand out 1 next (is_called false),
// not 2: a gap at id 1 would stay in the database for good, and builder
// serving that resolves every id up to the ceiling declines over any gap.
func resetMaxKindIDTestKinds(t *testing.T, pgDriver *pg.Driver, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pool.Exec(ctx, `delete from kind where name = any($1)`, maxKindIDTestKinds); err != nil {
		t.Fatalf("remove the test kinds: %v", err)
	}
	if _, err := pool.Exec(ctx, `select setval(pg_get_serial_sequence('kind', 'id'), coalesce(max(id), 1), max(id) is not null) from kind`); err != nil {
		t.Fatalf("restore the kind id sequence: %v", err)
	}
}

// claimLargestSmallserialKindID moves the kind id sequence so the next kind
// PostgreSQL registers gets id 32767.
func claimLargestSmallserialKindID(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `select setval(pg_get_serial_sequence('kind', 'id'), $1)`, largestSmallserialKindID-1); err != nil {
		t.Fatalf("setval: %v", err)
	}
}

// requireKindID fails the test unless PostgreSQL registered kind name under
// id want -- the fixture assumption every test below rests on.
func requireKindID(t *testing.T, pool *pgxpool.Pool, name string, want int) {
	t.Helper()
	var id int
	if err := pool.QueryRow(context.Background(), `select id from kind where name = $1`, name).Scan(&id); err != nil {
		t.Fatalf("read the id of kind %q: %v", name, err)
	}
	if id != want {
		t.Fatalf("kind %q has id %d, want %d (fixture assumption)", name, id, want)
	}
}

// requireOracleRowTotal fails the test unless PostgreSQL answers query
// with exactly want rows, so a served comparison over it cannot pass by both
// sides returning nothing.
func requireOracleRowTotal(t *testing.T, pgDriver *pg.Driver, query string, want int) {
	t.Helper()
	var (
		rows     []string
		queryErr error
	)
	if err := pgDriver.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
		rows, queryErr = renderTypedRows(tx.Query(query, map[string]any{}))
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if queryErr != nil {
		t.Fatalf("PostgreSQL: %s: %v", query, queryErr)
	}
	if len(rows) != want {
		t.Fatalf("PostgreSQL returns %d rows for %s, want %d (fixture assumption)", len(rows), query, want)
	}
}

// TestRebuildServesTheLargestSmallserialKindID loads a graph whose kind
// table has reached id 32767, carried by a node in one case and by an edge
// in the other, and serves a query over it. The rebuild is the one boot load
// and fallback recovery both retry; a panic in it crashed the process on
// every restart for as long as the kind existed.
func TestRebuildServesTheLargestSmallserialKindID(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		maxKind string
		seed    func(t *testing.T, pgDriver *pg.Driver, pool *pgxpool.Pool)
		query   string
	}{
		{
			name:    "node kind",
			maxKind: "MaxIDNodeKind",
			seed: func(t *testing.T, pgDriver *pg.Driver, pool *pgxpool.Pool) {
				claimLargestSmallserialKindID(t, pool)
				if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					_, err := tx.CreateNode(graph.NewProperties().Set("name", "n"), graph.StringKind("MaxIDNodeKind"))
					return err
				}); err != nil {
					t.Fatalf("create node: %v", err)
				}
			},
			query: `MATCH (n:MaxIDNodeKind) RETURN n`,
		},
		{
			name:    "edge kind",
			maxKind: "MaxIDEdgeKind",
			seed: func(t *testing.T, pgDriver *pg.Driver, pool *pgxpool.Pool) {
				var a, b *graph.Node
				if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					var err error
					if a, err = tx.CreateNode(graph.NewProperties().Set("name", "a"), graph.StringKind("MaxIDEnd")); err != nil {
						return err
					}
					b, err = tx.CreateNode(graph.NewProperties().Set("name", "b"), graph.StringKind("MaxIDEnd"))
					return err
				}); err != nil {
					t.Fatalf("create endpoints: %v", err)
				}
				claimLargestSmallserialKindID(t, pool)
				if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
					_, err := tx.CreateRelationshipByIDs(a.ID, b.ID, graph.StringKind("MaxIDEdgeKind"), graph.NewProperties())
					return err
				}); err != nil {
					t.Fatalf("create edge: %v", err)
				}
			},
			query: `MATCH (a:MaxIDEnd)-[:MaxIDEdgeKind]->(b:MaxIDEnd) RETURN a, b`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pgDriver, pool := graphtest.OpenPG(t, dsn)
			resetMaxKindIDTestKinds(t, pgDriver, pool)
			t.Cleanup(func() { resetMaxKindIDTestKinds(t, pgDriver, pool) })

			tc.seed(t, pgDriver, pool)
			requireKindID(t, pool, tc.maxKind, largestSmallserialKindID)
			requireOracleRowTotal(t, pgDriver, tc.query, 1)

			eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
			t.Cleanup(eng.Stop)
			if err := eng.RebuildNow(ctx, "manual"); err != nil {
				t.Fatalf("RebuildNow: %v", err)
			}
			if _, serving := eng.serveState(); !serving {
				t.Fatal("engine not serving after the rebuild")
			}
			assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{{tc.query, true}})
		})
	}
}

// TestCompactionFoldsTheLargestSmallserialKindID reaches kind id 32767
// through the steady state instead: the replica is loaded first, then a
// write registers the kind and creates a node carrying it. The overlay
// serves that node; the background compaction that folds it into a new base
// must too, rather than crash the process.
func TestCompactionFoldsTheLargestSmallserialKindID(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	resetMaxKindIDTestKinds(t, pgDriver, pool)
	t.Cleanup(func() { resetMaxKindIDTestKinds(t, pgDriver, pool) })

	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("name", "m"), graph.StringKind("MaxIDEnd"))
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	t.Cleanup(eng.Stop)
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	claimLargestSmallserialKindID(t, pool)
	var n *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		n, err = tx.CreateNode(graph.NewProperties().Set("name", "n"), graph.StringKind("MaxIDLateKind"))
		return err
	}); err != nil {
		t.Fatalf("write: %v", err)
	}
	requireKindID(t, pool, "MaxIDLateKind", largestSmallserialKindID)
	scope := NewWriteScope()
	scope.Changes().RecordNodeID(n.ID)
	eng.Apply(ctx, scope)

	cases := []typedCase{{`MATCH (n:MaxIDLateKind) RETURN n`, true}}
	requireOracleRowTotal(t, pgDriver, cases[0].query, 1)
	t.Run("overlay", func(t *testing.T) {
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})

	// The compaction goroutine's own body, run synchronously over exactly
	// what maybeStartCompaction would have captured.
	view := eng.snap.Load()
	eng.runCompaction(view.Base(), view.Segments())
	if got := eng.CompactionCount(); got != 1 {
		t.Fatalf("CompactionCount() = %d, want 1", got)
	}
	if _, serving := eng.serveState(); !serving {
		t.Fatal("engine not serving after the compaction")
	}
	t.Run("compacted", func(t *testing.T) {
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})
}
