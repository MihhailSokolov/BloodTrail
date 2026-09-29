// SPDX-License-Identifier: Apache-2.0

//go:build integration

package dbswitch

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file run the installer's own statements against a real
// PostgreSQL, through graphtest.PSQLRunner, in each state a deployment's
// database can be in when `bloodtrail install` or `rollback` reaches them.
// The case that matters most -- a table the engine itself created, and a
// snapshot file the next boot must refuse -- is pinned end to end by
// internal/engine's lineage_integration_test.go.

// pgStore returns a Store whose psql statements run against the test
// database, and that database's pool for the test to inspect it with.
func pgStore(t *testing.T) (Store, *pgxpool.Pool) {
	t.Helper()
	pgDriver, pool := graphtest.OpenPG(t, graphtest.PGAvailable(t))
	graphtest.WipeGraph(t, pgDriver)
	return Store{
		Compose:  dockerx.Compose{Runner: graphtest.PSQLRunner{Pool: pool}, File: "docker-compose.yml", ProjectDir: "."},
		Service:  "app-db",
		User:     "bloodhound",
		Database: "bloodhound",
	}, pool
}

// resetWatermarkTable drops bloodtrail_watermark and, when columns is not
// empty, creates it again with exactly those columns and the one row -- and
// drops it once more when the test ends, for the next engine start to
// recreate in its own current shape.
func resetWatermarkTable(t *testing.T, pool *pgxpool.Pool, columns string) {
	t.Helper()
	ctx := context.Background()

	if _, err := pool.Exec(ctx, "drop table if exists bloodtrail_watermark"); err != nil {
		t.Fatalf("drop bloodtrail_watermark: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "drop table if exists bloodtrail_watermark")
	})
	if columns == "" {
		return
	}
	if _, err := pool.Exec(ctx, "create table bloodtrail_watermark ("+columns+"); insert into bloodtrail_watermark (id, counter) values (1, 41)"); err != nil {
		t.Fatalf("create bloodtrail_watermark: %v", err)
	}
}

// preLineageColumns is bloodtrail_watermark as engines before watermark
// lineages created it.
const preLineageColumns = "id smallint primary key default 1 check (id = 1), counter bigint not null default 0, updated_at timestamptz not null default now()"

// lineageColumns adds the column internal/engine's watermarkLineageDDL gives
// it.
const lineageColumns = preLineageColumns + ", lineage uuid not null default gen_random_uuid()"

func watermarkCounter(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var counter int64
	if err := pool.QueryRow(context.Background(), "select counter from bloodtrail_watermark where id = 1").Scan(&counter); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	return counter
}

func watermarkLineage(t *testing.T, pool *pgxpool.Pool) [16]byte {
	t.Helper()
	var lineage [16]byte
	if err := pool.QueryRow(context.Background(), "select lineage from bloodtrail_watermark where id = 1").Scan(&lineage); err != nil {
		t.Fatalf("read lineage: %v", err)
	}
	return lineage
}

// TestEndWatermarkLineageAgainstPostgreSQL runs the statement in the three
// shapes a deployment's database can be in.
func TestEndWatermarkLineageAgainstPostgreSQL(t *testing.T) {
	ctx := context.Background()

	// A first install: BloodTrail never ran, so there is no lineage to end
	// and nothing to create either -- the engine creates the table, with a
	// lineage of its own, when it first starts.
	t.Run("no BloodTrail table", func(t *testing.T) {
		store, pool := pgStore(t)
		resetWatermarkTable(t, pool, "")

		if err := store.EndWatermarkLineage(ctx); err != nil {
			t.Fatalf("EndWatermarkLineage: %v", err)
		}
		var exists bool
		if err := pool.QueryRow(ctx, "select to_regclass('bloodtrail_watermark') is not null").Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("ending a lineage created bloodtrail_watermark; the installer must leave BloodTrail's schema to the engine")
		}
	})

	// A database an engine from before lineages last ran against: the
	// counter advance is what makes such an engine refuse its file.
	t.Run("table from before lineages", func(t *testing.T) {
		store, pool := pgStore(t)
		resetWatermarkTable(t, pool, preLineageColumns)

		if err := store.EndWatermarkLineage(ctx); err != nil {
			t.Fatalf("EndWatermarkLineage: %v", err)
		}
		if got := watermarkCounter(t, pool); got != 42 {
			t.Fatalf("counter = %d after ending the lineage, want 42 (one advance past 41)", got)
		}
	})

	t.Run("table with a lineage", func(t *testing.T) {
		store, pool := pgStore(t)
		resetWatermarkTable(t, pool, lineageColumns)
		before := watermarkLineage(t, pool)

		if err := store.EndWatermarkLineage(ctx); err != nil {
			t.Fatalf("EndWatermarkLineage: %v", err)
		}
		if after := watermarkLineage(t, pool); after == before {
			t.Fatalf("lineage unchanged by ending it: %x", after)
		}
		if got := watermarkCounter(t, pool); got != 42 {
			t.Fatalf("counter = %d after ending the lineage, want 42", got)
		}
	})
}

// TestClearGraphEndsTheLineageAgainstPostgreSQL pins what
// --replace-postgres-graph does to a real database: the graph is gone and
// the lineage has ended, both.
func TestClearGraphEndsTheLineageAgainstPostgreSQL(t *testing.T) {
	ctx := context.Background()
	pgDriver, pool := graphtest.OpenPG(t, graphtest.PGAvailable(t))
	graphtest.WipeGraph(t, pgDriver)
	store := Store{
		Compose:  dockerx.Compose{Runner: graphtest.PSQLRunner{Pool: pool}, File: "docker-compose.yml", ProjectDir: "."},
		Service:  "app-db",
		User:     "bloodhound",
		Database: "bloodhound",
	}
	resetWatermarkTable(t, pool, lineageColumns)
	before := watermarkLineage(t, pool)

	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("objectid", "replaced"), graph.StringKind("ReplacedNode"))
		return err
	}); err != nil {
		t.Fatalf("create node: %v", err)
	}

	if err := store.ClearGraph(ctx); err != nil {
		t.Fatalf("ClearGraph: %v", err)
	}
	nodes, edges, err := store.CountGraph(ctx)
	if err != nil {
		t.Fatalf("CountGraph: %v", err)
	}
	if nodes != 0 || edges != 0 {
		t.Fatalf("graph holds %d nodes and %d edges after ClearGraph, want none", nodes, edges)
	}
	if after := watermarkLineage(t, pool); after == before {
		t.Fatalf("ClearGraph left the watermark lineage unchanged: %x", after)
	}
}
