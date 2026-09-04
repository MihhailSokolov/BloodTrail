// SPDX-License-Identifier: Apache-2.0

// Command adgen generates a deterministic, AD-shaped synthetic graph (see
// generate.go for the shape) and bulk-loads it directly into a PostgreSQL
// database's node/edge tables through the dawgs pg driver, bypassing the
// driver's row-at-a-time write path in favor of pgx.CopyFrom.
//
// adgen writes straight into the graph tables of whatever database -dsn
// points at (schema-asserting the same "bloodtrail_test" graph name the
// project's integration tests use). It is a bench/dev tool: never point it
// at a production database. See README.md for the full writeup.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"
)

// graphName is the default graph adgen asserts and loads into. It matches
// internal/graphtest.GraphName and the repo-root driver_integration_test.go
// so pathbench and the engine's snapshot loader find the data by resolving
// the driver's default graph the same way the rest of the project does.
// adgen intentionally does not import internal/graphtest (an
// integration-test-only package, gated by a build tag) -- this constant and
// the AssertSchema call below are a deliberate, self-contained replica of
// that package's OpenPG pattern.
const graphName = "bloodtrail_test"

// progressEvery controls how often the load path reports progress to
// stderr while copying rows, per the task brief ("progress output to
// stderr every ~100k rows at large scales").
const progressEvery = 100_000

func main() {
	var (
		dsn   = flag.String("dsn", "", "PostgreSQL connection string, e.g. postgresql://user:pass@host:port/db")
		users = flag.Int("users", 1000, "number of User principals to generate; drives every other count (see README.md)")
		seed  = flag.Int64("seed", 1, "random seed; the same seed and -users always produce the same graph")
		wipe  = flag.Bool("wipe", false, "truncate the node/edge tables (every graph, not just this one) before loading")
	)
	flag.Parse()

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "adgen: -dsn is required")
		os.Exit(2)
	}
	if *users <= 0 {
		fmt.Fprintln(os.Stderr, "adgen: -users must be positive")
		os.Exit(2)
	}

	if err := run(context.Background(), *dsn, *users, *seed, *wipe); err != nil {
		log.Fatalf("adgen: %v", err)
	}
}

func run(ctx context.Context, dsn string, users int, seed int64, wipe bool) error {
	fmt.Fprintf(os.Stderr, "adgen: generating graph for users=%d seed=%d ...\n", users, seed)
	t0 := time.Now()
	g := Generate(Spec{Users: users, Seed: seed})
	fmt.Fprintf(os.Stderr, "adgen: generated %d nodes, %d edges in %v\n", len(g.Nodes), len(g.Edges), time.Since(t0))

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pg.NewPool(cfg)
	if err != nil {
		return fmt.Errorf("new pool: %w", err)
	}
	defer pool.Close()

	driver := pg.NewDriver(size.Gibibyte, pool)

	nodeKinds := stringKinds(NodeKinds)
	edgeKinds := stringKinds(EdgeKinds)

	schema := graph.Schema{
		Graphs:       []graph.Graph{{Name: graphName, Nodes: nodeKinds, Edges: edgeKinds}},
		DefaultGraph: graph.Graph{Name: graphName},
	}
	if err := driver.AssertSchema(ctx, schema); err != nil {
		return fmt.Errorf("assert schema: %w", err)
	}

	if wipe {
		fmt.Fprintln(os.Stderr, "adgen: -wipe: truncating node/edge tables")
		if err := driver.WipeGraph(ctx, nil); err != nil {
			return fmt.Errorf("wipe graph: %w", err)
		}
	}

	graphModel, ok := driver.SchemaManager.DefaultGraph()
	if !ok {
		return fmt.Errorf("no default graph resolved after AssertSchema")
	}

	allKinds := append(append([]graph.Kind{}, nodeKinds...), edgeKinds...)
	kindIDs, err := driver.KindMapper().AssertKinds(ctx, allKinds)
	if err != nil {
		return fmt.Errorf("assert kinds: %w", err)
	}
	kindIDByName := make(map[string]int16, len(allKinds))
	for i, k := range allKinds {
		kindIDByName[k.String()] = kindIDs[i]
	}

	return loadGraph(ctx, pool, graphModel.ID, g, kindIDByName)
}

func stringKinds(names []string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// loadGraph bulk-loads g into graphID's node/edge partitions via
// pgx.CopyFrom, then fixes up the id sequences so that any writer using the
// driver's normal (nextval-based) insert path afterward does not collide
// with the ids adgen just assigned explicitly.
//
// Node and edge ids are global across every graph's partition (the "node"
// and "edge" tables are list-partitioned on graph_id, but bigserial id is
// one sequence shared by the partitioned parent), so the starting id for
// this load is max(id) over the whole table, not just this graph's rows.
// COPY targets the partitioned parent tables directly (graph_id included in
// every row) and lets PostgreSQL route each row to the matching partition,
// which AssertSchema has already created.
func loadGraph(ctx context.Context, pool *pgxpool.Pool, graphID int32, g Graph, kindIDByName map[string]int16) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	nodeBase, err := maxID(ctx, tx, "node")
	if err != nil {
		return fmt.Errorf("query max node id: %w", err)
	}
	edgeBase, err := maxID(ctx, tx, "edge")
	if err != nil {
		return fmt.Errorf("query max edge id: %w", err)
	}

	nodeIDs := make([]int64, len(g.Nodes))
	for i := range g.Nodes {
		nodeIDs[i] = nodeBase + 1 + int64(i)
	}

	t0 := time.Now()
	nodeSrc, err := newNodeCopySource(g.Nodes, nodeIDs, graphID, kindIDByName)
	if err != nil {
		return fmt.Errorf("prepare node rows: %w", err)
	}
	nodeCount, err := tx.Conn().CopyFrom(ctx, pgx.Identifier{"node"}, []string{"id", "graph_id", "kind_ids", "properties"}, nodeSrc)
	if err != nil {
		return fmt.Errorf("copy nodes: %w", err)
	}
	fmt.Fprintf(os.Stderr, "adgen: loaded %d nodes in %v\n", nodeCount, time.Since(t0))

	t0 = time.Now()
	edgeSrc, err := newEdgeCopySource(g.Edges, nodeIDs, edgeBase, graphID, kindIDByName)
	if err != nil {
		return fmt.Errorf("prepare edge rows: %w", err)
	}
	edgeCount, err := tx.Conn().CopyFrom(ctx, pgx.Identifier{"edge"}, []string{"id", "graph_id", "start_id", "end_id", "kind_id", "properties"}, edgeSrc)
	if err != nil {
		return fmt.Errorf("copy edges: %w", err)
	}
	fmt.Fprintf(os.Stderr, "adgen: loaded %d edges in %v\n", edgeCount, time.Since(t0))

	if len(g.Nodes) > 0 {
		if err := setSequence(ctx, tx, "node", nodeBase+int64(len(g.Nodes))); err != nil {
			return fmt.Errorf("fix node id sequence: %w", err)
		}
	}
	if len(g.Edges) > 0 {
		if err := setSequence(ctx, tx, "edge", edgeBase+int64(len(g.Edges))); err != nil {
			return fmt.Errorf("fix edge id sequence: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// maxID returns the highest id currently in table (0 if the table is
// empty). table is one of the fixed strings "node" or "edge" -- never
// caller-supplied -- so building the query by fmt.Sprintf is safe here.
func maxID(ctx context.Context, tx pgx.Tx, table string) (int64, error) {
	var highest int64
	if err := tx.QueryRow(ctx, fmt.Sprintf("select coalesce(max(id), 0) from %s", table)).Scan(&highest); err != nil {
		return 0, err
	}
	return highest, nil
}

// setSequence bumps table's id sequence so that its next nextval() returns
// newMax+1, past every id adgen just wrote explicitly via COPY (which does
// not consume the sequence). pg_get_serial_sequence resolves the sequence
// owned by the bigserial column on the partitioned parent table itself
// (schema_up.sql declares "id bigserial" directly on "node" and "edge",
// which is where the owning sequence lives regardless of partitioning), so
// this does not need per-partition handling.
func setSequence(ctx context.Context, tx pgx.Tx, table string, newMax int64) error {
	_, err := tx.Exec(ctx, "select setval(pg_get_serial_sequence($1, 'id'), $2)", table, newMax)
	return err
}

// nodeCopySource streams Graph.Nodes as pgx.CopyFrom rows in (id, graph_id,
// kind_ids, properties) order, printing progress to stderr every
// progressEvery rows.
func newNodeCopySource(nodes []Node, ids []int64, graphID int32, kindIDByName map[string]int16) (pgx.CopyFromSource, error) {
	kindIDs := make([][]int16, len(nodes))
	for i, n := range nodes {
		row := make([]int16, len(n.Kinds))
		for j, k := range n.Kinds {
			id, ok := kindIDByName[k]
			if !ok {
				return nil, fmt.Errorf("node %q: kind %q was not pre-asserted", n.ObjectID, k)
			}
			row[j] = id
		}
		kindIDs[i] = row
	}

	return pgx.CopyFromSlice(len(nodes), func(i int) ([]any, error) {
		if i > 0 && i%progressEvery == 0 {
			fmt.Fprintf(os.Stderr, "adgen: ... %d/%d nodes\n", i, len(nodes))
		}
		props, err := json.Marshal(nodes[i].Props)
		if err != nil {
			return nil, fmt.Errorf("marshal properties for node %q: %w", nodes[i].ObjectID, err)
		}
		return []any{ids[i], graphID, kindIDs[i], props}, nil
	}), nil
}

// edgeCopySource streams Graph.Edges as pgx.CopyFrom rows in (id, graph_id,
// start_id, end_id, kind_id, properties) order. nodeIDs maps a Graph.Nodes
// index to the database id it was assigned (nodeCopySource assigns them
// sequentially in Nodes order, so edges built from Node indices resolve
// directly through this slice).
func newEdgeCopySource(edges []Edge, nodeIDs []int64, edgeBase int64, graphID int32, kindIDByName map[string]int16) (pgx.CopyFromSource, error) {
	kindIDs := make([]int16, len(edges))
	for i, e := range edges {
		id, ok := kindIDByName[e.Kind]
		if !ok {
			return nil, fmt.Errorf("edge[%d]: kind %q was not pre-asserted", i, e.Kind)
		}
		kindIDs[i] = id
	}

	empty := json.RawMessage("{}")

	return pgx.CopyFromSlice(len(edges), func(i int) ([]any, error) {
		if i > 0 && i%progressEvery == 0 {
			fmt.Fprintf(os.Stderr, "adgen: ... %d/%d edges\n", i, len(edges))
		}
		e := edges[i]
		if e.StartIdx < 0 || e.StartIdx >= len(nodeIDs) || e.EndIdx < 0 || e.EndIdx >= len(nodeIDs) {
			return nil, fmt.Errorf("edge[%d] references an out-of-range node index (start=%d end=%d, nodes=%d)", i, e.StartIdx, e.EndIdx, len(nodeIDs))
		}
		return []any{edgeBase + 1 + int64(i), graphID, nodeIDs[e.StartIdx], nodeIDs[e.EndIdx], kindIDs[i], []byte(empty)}, nil
	}), nil
}
