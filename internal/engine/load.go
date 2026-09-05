// SPDX-License-Identifier: Apache-2.0

// Package engine hosts the BloodTrail in-memory graph engine: loading a
// PostgreSQL-backed graph into an immutable snapshot.Snapshot for
// cache-friendly traversal.
package engine

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// LoadSnapshot loads pgDriver's default graph from PostgreSQL into an
// immutable snapshot.Snapshot.
//
// It resolves the default graph via the driver's SchemaManager (returning an
// error if none is set), then runs a single repeatable-read, read-only
// transaction that scans the global kind id/name table, then streams every
// node (including its property bag) ordered by id, then every edge, feeding
// all three into a snapshot.Builder, and finally runs the multi-graph probe
// (see probeMultiGraph). Build assigns dense NodeIDs in the node scan's
// ascending order and stamps the snapshot's BuiltAt; Generation and
// AnalysisStamp are left zero for the engine and poller (Tasks 10 and 12) to
// set later.
func LoadSnapshot(ctx context.Context, pgDriver *pg.Driver, pool *pgxpool.Pool) (*snapshot.Snapshot, error) {
	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		return nil, fmt.Errorf("engine: LoadSnapshot: no default graph is set")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("engine: LoadSnapshot: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	builder := snapshot.NewBuilder(graphModel.ID)

	if err := loadKinds(ctx, tx, builder); err != nil {
		return nil, err
	}
	if err := loadNodes(ctx, tx, graphModel.ID, builder); err != nil {
		return nil, err
	}
	if err := loadEdges(ctx, tx, graphModel.ID, builder); err != nil {
		return nil, err
	}

	multiGraph, err := probeMultiGraph(ctx, tx)
	if err != nil {
		return nil, err
	}

	snap, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("engine: LoadSnapshot: build snapshot: %w", err)
	}
	snap.MultiGraph = multiGraph

	return snap, nil
}

// loadKinds scans the entire `kind` table -- global, not scoped to any one
// graph_id -- into builder via Builder.SetKinds. It runs before loadNodes so
// the resulting Snapshot's Kinds table is populated regardless of which
// kinds this particular graph's nodes and edges use.
func loadKinds(ctx context.Context, tx pgx.Tx, builder *snapshot.Builder) error {
	rows, err := tx.Query(ctx, "SELECT id, name FROM kind")
	if err != nil {
		return fmt.Errorf("engine: LoadSnapshot: query kinds: %w", err)
	}
	defer rows.Close()

	pairs := make(map[snapshot.KindID]string)
	for rows.Next() {
		var (
			id   snapshot.KindID
			name string
		)
		if err := rows.Scan(&id, &name); err != nil {
			return fmt.Errorf("engine: LoadSnapshot: scan kind: %w", err)
		}
		pairs[id] = name
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: LoadSnapshot: kind rows: %w", err)
	}

	builder.SetKinds(pairs)
	return nil
}

// loadNodes streams every node of graphID, ordered by id, into builder. The
// ascending id order is required: it becomes the Builder's dense NodeID
// assignment.
//
// Each row's properties column (jsonb) is scanned directly into a []byte and
// handed to Builder.AddNode as-is: pgx returns jsonb's wire text verbatim,
// which is already valid JSON, so nothing needs stripping or re-encoding.
// This keeps the scan single-pass -- kinds and properties are both read off
// the same row, with no second query needed to backfill property bags.
func loadNodes(ctx context.Context, tx pgx.Tx, graphID int32, builder *snapshot.Builder) error {
	rows, err := tx.Query(ctx, "SELECT id, kind_ids, properties FROM node WHERE graph_id = $1 ORDER BY id", graphID)
	if err != nil {
		return fmt.Errorf("engine: LoadSnapshot: query nodes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id        int64
			kindIDs   []snapshot.KindID
			propsJSON []byte
		)
		if err := rows.Scan(&id, &kindIDs, &propsJSON); err != nil {
			return fmt.Errorf("engine: LoadSnapshot: scan node: %w", err)
		}
		if err := builder.AddNode(uint64(id), kindIDs, propsJSON); err != nil {
			return fmt.Errorf("engine: LoadSnapshot: add node: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: LoadSnapshot: node rows: %w", err)
	}

	return nil
}

// loadEdges streams every edge of graphID into builder. Edges may arrive in
// any order; the Builder resolves and sorts them at Build time, dropping
// (and counting in the resulting Snapshot's DroppedEdges) any edge whose
// endpoint doesn't resolve to a node loadNodes staged.
func loadEdges(ctx context.Context, tx pgx.Tx, graphID int32, builder *snapshot.Builder) error {
	rows, err := tx.Query(ctx, "SELECT id, start_id, end_id, kind_id FROM edge WHERE graph_id = $1", graphID)
	if err != nil {
		return fmt.Errorf("engine: LoadSnapshot: query edges: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, start, end int64
			kind           snapshot.KindID
		)
		if err := rows.Scan(&id, &start, &end, &kind); err != nil {
			return fmt.Errorf("engine: LoadSnapshot: scan edge: %w", err)
		}
		builder.AddEdge(uint64(id), uint64(start), uint64(end), kind)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: LoadSnapshot: edge rows: %w", err)
	}

	return nil
}

// probeMultiGraph reports whether the database holds two or more distinct
// graphs that each have at least one node, for Snapshot.MultiGraph. The
// LIMIT 2 keeps this O(1) regardless of how many graphs or nodes exist: the
// query stops as soon as a second qualifying graph is found, so it never
// scans the full `graph` or `node` tables on a large, single-graph database.
func probeMultiGraph(ctx context.Context, tx pgx.Tx) (bool, error) {
	rows, err := tx.Query(ctx, "SELECT g.id FROM graph g WHERE EXISTS (SELECT 1 FROM node n WHERE n.graph_id = g.id) LIMIT 2")
	if err != nil {
		return false, fmt.Errorf("engine: LoadSnapshot: query multi-graph probe: %w", err)
	}
	defer rows.Close()

	graphsWithNodes := 0
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return false, fmt.Errorf("engine: LoadSnapshot: scan multi-graph probe: %w", err)
		}
		graphsWithNodes++
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("engine: LoadSnapshot: multi-graph probe rows: %w", err)
	}

	return graphsWithNodes >= 2, nil
}
