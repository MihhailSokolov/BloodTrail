// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bloodtrail_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/util/size"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

const testPGEnv = "BLOODTRAIL_TEST_PG"

var datasets = []string{"testdata/dawgs/traversal_shapes.json", "testdata/dawgs/adcs_fanout.json"}

// equivalenceQuery builds one of the queries every driver must answer identically,
// given the document-ID-to-database-ID mapping produced by loading the datasets.
// The three cases cover the shapes this project cares about: shortest path, all
// shortest paths, bounded expansion.
//
// The datasets' nodes carry no "name" (or any other identifying) property - see
// testdata/dawgs/traversal_shapes.json and adcs_fanout.json, where only the ADCS
// fixture's "n" node has a property at all (objectid). So node identity for these
// queries comes from id(s)/id(e) against the graph.ID each node was assigned on
// load, looked up by the datasets' own document IDs via the opengraph.IDMap.
// This mirrors how DAWGS's own benchmark scenarios
// (cmd/benchmark/scenarios.go, baseScenarios) identify fixture nodes.
type equivalenceQuery struct {
	name   string
	cypher func(ids opengraph.IDMap) string
}

var equivalenceQueries = []equivalenceQuery{
	{
		// traversal_shapes.json: a straight 10-hop ChainNode chain c0->c1->...->c10.
		name: "shortestPath over a chain",
		cypher: func(ids opengraph.IDMap) string {
			return fmt.Sprintf(
				"MATCH p = shortestPath((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
				ids["c0"], ids["c10"],
			)
		},
	},
	{
		// traversal_shapes.json: a diamond d0->{d1,d2,d3}->d4, three distinct
		// shortest (2-hop) paths from d0 to d4.
		name: "allShortestPaths over a diamond",
		cypher: func(ids opengraph.IDMap) string {
			return fmt.Sprintf(
				"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
				ids["d0"], ids["d4"],
			)
		},
	},
	{
		// traversal_shapes.json: f0 fans out to f1..f3, then f1a..f3b, then
		// f1a1..f3b1 - a three-level, branching-factor-2 fan-out.
		name: "bounded expansion over a fan-out",
		cypher: func(ids opengraph.IDMap) string {
			return fmt.Sprintf(
				"MATCH p = (s)-[*1..3]->(e) WHERE id(s) = %d RETURN p",
				ids["f0"],
			)
		},
	},
}

func openPool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	pool, err := pg.NewPool(cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func schemaFromDatasets(t *testing.T) graph.Schema {
	t.Helper()
	var nodeKinds, edgeKinds graph.Kinds
	for _, path := range datasets {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		doc, err := opengraph.ParseDocument(f)
		_ = f.Close()
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		nk, ek := doc.Graph.Kinds()
		nodeKinds = nodeKinds.Add(nk...)
		edgeKinds = edgeKinds.Add(ek...)
	}
	return graph.Schema{
		Graphs:       []graph.Graph{{Name: "bloodtrail_test", Nodes: nodeKinds, Edges: edgeKinds}},
		DefaultGraph: graph.Graph{Name: "bloodtrail_test"},
	}
}

// loadDatasets clears the graph, loads every dataset, and returns a merged
// document-ID-to-database-ID map (the datasets use disjoint ID namespaces, so a
// flat merge is safe).
func loadDatasets(t *testing.T, ctx context.Context, db graph.Database) opengraph.IDMap {
	t.Helper()
	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error { return tx.Nodes().Delete() }); err != nil {
		t.Fatalf("clear graph: %v", err)
	}
	ids := make(opengraph.IDMap)
	for _, path := range datasets {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		loaded, err := opengraph.Load(ctx, db, f)
		_ = f.Close()
		if err != nil {
			t.Fatalf("load %s: %v", path, err)
		}
		for docID, dbID := range loaded {
			ids[docID] = dbID
		}
	}
	return ids
}

func pathSignatures(t *testing.T, ctx context.Context, db graph.Database, cypher string) []string {
	t.Helper()
	var sigs []string
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()
		for result.Next() {
			var p graph.Path
			if err := result.Scan(&p); err != nil {
				return err
			}
			parts := make([]string, 0, len(p.Nodes))
			for _, n := range p.Nodes {
				parts = append(parts, n.ID.String())
			}
			sigs = append(sigs, strings.Join(parts, ">"))
		}
		return result.Error()
	})
	if err != nil {
		t.Fatalf("query %q: %v", cypher, err)
	}
	sort.Strings(sigs)
	return sigs
}

func TestBloodTrailMatchesPostgresDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}
	ctx := context.Background()
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: openPool(t, ctx, dsn)}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	if _, ok := bt.(*bloodtrail.Driver); !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", bt)
	}

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	// A plain pg driver on the same pool and data is the oracle.
	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	for _, eq := range equivalenceQueries {
		cypher := eq.cypher(ids)
		t.Run(eq.name, func(t *testing.T) {
			got := pathSignatures(t, ctx, bt, cypher)
			want := pathSignatures(t, ctx, oracle, cypher)
			if len(want) == 0 {
				t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("paths differ\n got: %v\nwant: %v", got, want)
			}
		})
	}

	// Capability methods must be reachable through the embedded driver.
	if err := bt.(*bloodtrail.Driver).DeleteRelationshipsByKinds(ctx, graph.Kinds{}); err != nil {
		t.Fatalf("DeleteRelationshipsByKinds: %v", err)
	}
}
