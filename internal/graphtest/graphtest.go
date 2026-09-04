// SPDX-License-Identifier: Apache-2.0

//go:build integration

// Package graphtest holds shared fixtures for integration tests that need a
// live PostgreSQL-backed DAWGS graph: opening a schema-asserted driver,
// wiping it between tests, and loading opengraph datasets. It lifts the
// pattern established by the repo-root driver_integration_test.go so other
// packages (the engine's snapshot loader, later bench tooling) do not have
// to reimplement it.
package graphtest

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"
)

// TestPGEnv names the environment variable carrying the DSN of a PostgreSQL
// instance available for integration tests.
const TestPGEnv = "BLOODTRAIL_TEST_PG"

// GraphName is the name asserted as the default graph by OpenPG, matching
// the name driver_integration_test.go uses at the repo root.
const GraphName = "bloodtrail_test"

// PGAvailable returns the DSN from BLOODTRAIL_TEST_PG, or skips the test if
// it is not set.
func PGAvailable(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv(TestPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", TestPGEnv)
	}
	return dsn
}

// OpenPG opens a pg.Driver and pgxpool.Pool against dsn and asserts a
// BloodHound-ish schema with GraphName as the default graph. Node and edge
// kinds are not predeclared: AssertKinds defines them lazily the first time
// LoadDataset writes a node or edge of a kind not seen before.
//
// t.Cleanup closes the pool.
func OpenPG(t *testing.T, dsn string) (*pg.Driver, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("graphtest: parse dsn: %v", err)
	}

	pool, err := pg.NewPool(cfg)
	if err != nil {
		t.Fatalf("graphtest: new pool: %v", err)
	}
	t.Cleanup(pool.Close)

	driver := pg.NewDriver(size.Gibibyte, pool)

	schema := graph.Schema{DefaultGraph: graph.Graph{Name: GraphName}}
	if err := driver.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("graphtest: assert schema: %v", err)
	}

	return driver, pool
}

// WipeGraph truncates every node and edge across all graphs, leaving the
// graph and kind catalogs intact.
func WipeGraph(t *testing.T, d *pg.Driver) {
	t.Helper()

	if err := d.WipeGraph(context.Background(), nil); err != nil {
		t.Fatalf("graphtest: wipe graph: %v", err)
	}
}

// LoadDataset reads and loads an opengraph JSON dataset at path into d's
// default graph, returning the mapping from the dataset's document node IDs
// to the database IDs they were assigned.
func LoadDataset(t *testing.T, d *pg.Driver, path string) opengraph.IDMap {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("graphtest: open %s: %v", path, err)
	}
	defer f.Close()

	ids, err := opengraph.Load(context.Background(), d, f)
	if err != nil {
		t.Fatalf("graphtest: load %s: %v", path, err)
	}

	return ids
}
