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
	"math/rand"
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
// kinds are not predeclared here: LoadDataset asserts each dataset's own
// kind alphabet immediately before loading it (see its doc for why that
// can't simply be left to lazy definition), and LoadRandom does the same
// for its fixed kind alphabet.
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
//
// Before writing anything, it asserts a schema declaring the dataset's own
// node and edge kinds under GraphName. This is not optional bookkeeping:
// opengraph.WriteGraph (called below, the same helper opengraph.Load itself
// wraps) writes the dataset's nodes through a write transaction --
// tx.CreateNode, which lazily defines an unseen kind via
// SchemaManager.AssertKinds -- but writes its edges through BatchOperation,
// whose relationship path only maps kind names already in the catalog
// (SchemaManager.MapKind) and errors "unable to map kind: X" on one that
// isn't (see LoadRandom's doc for the same gap, and its identical fix). A
// database that already has every dataset's kinds defined from a prior run
// masks this; a freshly provisioned one does not, so the assertion belongs
// here rather than in each individual caller.
func LoadDataset(t *testing.T, d *pg.Driver, path string) opengraph.IDMap {
	t.Helper()
	ctx := context.Background()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("graphtest: open %s: %v", path, err)
	}
	defer f.Close()

	doc, err := opengraph.ParseDocument(f)
	if err != nil {
		t.Fatalf("graphtest: parse %s: %v", path, err)
	}

	nodeKinds, edgeKinds := doc.Graph.Kinds()
	schema := graph.Schema{Graphs: []graph.Graph{{Name: GraphName, Nodes: nodeKinds, Edges: edgeKinds}}}
	if err := d.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("graphtest: assert schema for %s: %v", path, err)
	}

	ids, err := opengraph.WriteGraph(ctx, d, &doc.Graph)
	if err != nil {
		t.Fatalf("graphtest: load %s: %v", path, err)
	}

	return ids
}

// RandomNodeKindCount and RandomEdgeKindCount size the kind alphabets
// LoadRandom draws from -- 5 node kinds (A..E), 6 edge kinds (R1..R6) -- and
// are exported so callers building queries against a LoadRandom graph (e.g.
// a random edge-kind subset) can stay in sync without hardcoding the counts
// twice.
const (
	RandomNodeKindCount = 5
	RandomEdgeKindCount = 6
)

// RandomNodeKinds and RandomEdgeKinds are the fixed kind alphabets LoadRandom
// assigns nodes and edges from, exposed so callers can build queries (e.g. a
// random EdgeKinds subset) against exactly the kinds a LoadRandom graph uses.
var (
	RandomNodeKinds = randomKinds("A", "B", "C", "D", "E")
	RandomEdgeKinds = randomKinds("R1", "R2", "R3", "R4", "R5", "R6")
)

func randomKinds(names ...string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// LoadRandom generates a pseudo-random graph from seed -- 60 nodes drawn
// from RandomNodeKinds, 180 edges drawn from RandomEdgeKinds -- and loads it
// into d's default graph through the pg driver's write/batch APIs (nodes via
// a write transaction, so their assigned ids are available for edges; edges
// via BatchOperation, matching the bulk-load path production code uses).
// Edges are chosen independently and uniformly over the node list on both
// ends, so self-loops and parallel edges (including same-kind parallel
// edges, which the pg driver's batch insert merges into a single stored
// edge -- see drivers/pg/batch.go's relationshipCreateBatchBuilder) both
// occur naturally.
//
// The returned slice holds the database ids assigned to the 60 generated
// nodes, in generation order. Calling LoadRandom with the same seed against
// a freshly wiped graph reproduces the same graph shape every time, since
// rand.New(rand.NewSource(seed)) is deterministic and the node ids a fresh
// sequence assigns are stable.
func LoadRandom(t *testing.T, d *pg.Driver, seed int64) []graph.ID {
	t.Helper()
	ctx := context.Background()

	const (
		numNodes = 60
		numEdges = 180
	)

	rng := rand.New(rand.NewSource(seed))

	// Edge kinds must exist in the driver's kind catalog before
	// CreateRelationshipByIDs's batch path will accept them: unlike node
	// creation (tx.CreateNode calls AssertKinds, which defines missing kinds
	// lazily), the batch relationship path only maps existing kind ids
	// (SchemaManager.MapKind) and errors on an unknown one. AssertSchema only
	// reads kinds off schema.Graphs (a Schema's DefaultGraph field is used
	// for partition/index bookkeeping, not kind definition), so the kinds
	// must be declared there. Node kinds don't strictly need this
	// pre-assertion (CreateNode already lazily defines them), but asserting
	// both here keeps LoadRandom idempotent about kind declaration
	// regardless of driver behavior changes.
	schema := graph.Schema{Graphs: []graph.Graph{{Name: GraphName, Nodes: RandomNodeKinds, Edges: RandomEdgeKinds}}}
	if err := d.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("graphtest: LoadRandom(seed=%d): assert schema: %v", seed, err)
	}

	ids := make([]graph.ID, numNodes)
	if err := d.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < numNodes; i++ {
			kind := RandomNodeKinds[rng.Intn(len(RandomNodeKinds))]

			node, err := tx.CreateNode(graph.NewProperties(), kind)
			if err != nil {
				return err
			}
			ids[i] = node.ID
		}
		return nil
	}); err != nil {
		t.Fatalf("graphtest: LoadRandom(seed=%d): create nodes: %v", seed, err)
	}

	if err := d.BatchOperation(ctx, func(batch graph.Batch) error {
		for i := 0; i < numEdges; i++ {
			start := ids[rng.Intn(numNodes)]
			end := ids[rng.Intn(numNodes)]
			kind := RandomEdgeKinds[rng.Intn(len(RandomEdgeKinds))]

			if err := batch.CreateRelationshipByIDs(start, end, kind, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("graphtest: LoadRandom(seed=%d): create edges: %v", seed, err)
	}

	return ids
}
