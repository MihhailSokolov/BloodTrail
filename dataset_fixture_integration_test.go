// SPDX-License-Identifier: Apache-2.0

//go:build integration

// Shared opengraph-dataset fixture helpers for this external test package
// (engine_serving_integration_test.go's live-driver suites): the two dawgs
// datasets, the equivalence queries every driver must answer identically,
// and the loaders/pool plumbing around them. driver_integration_test.go
// used to define these here; it moved into package bloodtrail (it needs
// d.engine to force a rebuild and assert served markers), so this package
// keeps its own copy -- the same deliberate, kept-in-sync-by-eye
// duplication this repo's two test packages already practice for
// lockedBuffer/installLogCapture (see engine_serving_integration_test.go).
package bloodtrail_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
)

const testPGEnv = "BLOODTRAIL_TEST_PG"

var datasets = []string{"testdata/dawgs/traversal_shapes.json", "testdata/dawgs/adcs_fanout.json"}

// equivalenceQuery builds one of the queries every driver must answer
// identically, given the document-ID-to-database-ID mapping produced by
// loading the datasets -- see driver_integration_test.go's identical type
// for the full shape rationale.
type equivalenceQuery struct {
	name   string
	cypher func(ids opengraph.IDMap) string
}

var equivalenceQueries = []equivalenceQuery{
	{
		name: "shortestPath over a chain",
		cypher: func(ids opengraph.IDMap) string {
			return fmt.Sprintf(
				"MATCH p = shortestPath((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
				ids["c0"], ids["c10"],
			)
		},
	},
	{
		name: "allShortestPaths over a diamond",
		cypher: func(ids opengraph.IDMap) string {
			return fmt.Sprintf(
				"MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p",
				ids["d0"], ids["d4"],
			)
		},
	},
	{
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
// document-ID-to-database-ID map (the datasets use disjoint ID namespaces,
// so a flat merge is safe).
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

// canonNode/canonEdge/canonPath/canonicalizePath mirror package bloodtrail's
// identically-named canonical path comparator (prebuilt_corpus_integration_
// test.go, itself a port of package engine's) -- node ids AND properties,
// edge kinds AND properties, in path order. A node-id-only reduction (what
// this package's pathSignatures used before the 2026-09-13 sweep flagged
// it) lets a wrong edge or a dropped property compare as "equal".
type canonNode struct {
	ID    uint64         `json:"id"`
	Props map[string]any `json:"props"`
}
type canonEdge struct {
	Kind  string         `json:"kind"`
	Props map[string]any `json:"props"`
}
type canonPath struct {
	Nodes []canonNode `json:"nodes"`
	Edges []canonEdge `json:"edges"`
}

func canonicalizePath(p graph.Path) canonPath {
	cp := canonPath{Nodes: make([]canonNode, len(p.Nodes)), Edges: make([]canonEdge, len(p.Edges))}
	for i, n := range p.Nodes {
		cp.Nodes[i] = canonNode{ID: uint64(n.ID), Props: n.Properties.MapOrEmpty()}
	}
	for i, e := range p.Edges {
		cp.Edges[i] = canonEdge{Kind: e.Kind.String(), Props: e.Properties.MapOrEmpty()}
	}
	return cp
}

// pathSignatures runs cypher and renders every returned path canonically,
// sorted for order-independent comparison, duplicates preserved.
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
			b, err := json.Marshal(canonicalizePath(p))
			if err != nil {
				return err
			}
			sigs = append(sigs, string(b))
		}
		return result.Error()
	})
	if err != nil {
		t.Fatalf("query %q: %v", cypher, err)
	}
	sort.Strings(sigs)
	return sigs
}
