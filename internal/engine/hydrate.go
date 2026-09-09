// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/traverse"
)

// edgeBatchSize caps how many (start_id, end_id, kind_id) triples one
// VALUES-list edge query carries. 500 triples is 1500 bound parameters,
// comfortably inside PostgreSQL's per-statement parameter limit while
// keeping each round trip large.
const edgeBatchSize = 500

// edgeKey identifies one edge by the (start, end, kind) triple the edge
// table's unique constraint enforces, using database node ids rather than
// dense snapshot.NodeIDs.
type edgeKey struct {
	start, end uint64
	kind       snapshot.KindID
}

// hydratePaths maps dense paths to graph.Path values with full nodes and
// relationships fetched from PostgreSQL. Node and edge kinds are resolved
// through kindMapper and jsonb property columns are carried through as
// map[string]any, matching what the pg driver itself returns for the same
// entities.
//
// A node or edge that paths reference but that no longer exists in the
// database (deleted between the snapshot's load and this call) is reported
// as an error rather than silently dropped or substituted.
func hydratePaths(ctx context.Context, pool *pgxpool.Pool, kindMapper pg.KindMapper, snap *snapshot.View, paths []traverse.Path) (graph.PathSet, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	nodeIDs, edgeKeys := hydrationKeys(snap, paths)

	nodes, err := hydrateNodes(ctx, pool, kindMapper, snap.Base().GraphID, nodeIDs)
	if err != nil {
		return nil, err
	}

	edges, err := hydrateEdges(ctx, pool, kindMapper, snap.Base().GraphID, edgeKeys)
	if err != nil {
		return nil, err
	}

	out := make(graph.PathSet, len(paths))
	for i, p := range paths {
		gp, err := assemblePath(snap, p, nodes, edges)
		if err != nil {
			return nil, err
		}
		out[i] = gp
	}

	return out, nil
}

// hydrationKeys collects every database node id and (start, end, kind) edge
// triple that paths reference, each deduplicated across all of paths and
// returned in first-seen order.
func hydrationKeys(snap *snapshot.View, paths []traverse.Path) ([]uint64, []edgeKey) {
	seenNodes := make(map[uint64]struct{})
	var nodeIDs []uint64

	seenEdges := make(map[edgeKey]struct{})
	var edgeKeys []edgeKey

	for _, p := range paths {
		for _, dense := range p.Nodes {
			id := snap.GraphID(dense)
			if _, ok := seenNodes[id]; !ok {
				seenNodes[id] = struct{}{}
				nodeIDs = append(nodeIDs, id)
			}
		}

		for i, kind := range p.Kinds {
			key := edgeKey{start: snap.GraphID(p.Nodes[i]), end: snap.GraphID(p.Nodes[i+1]), kind: kind}
			if _, ok := seenEdges[key]; !ok {
				seenEdges[key] = struct{}{}
				edgeKeys = append(edgeKeys, key)
			}
		}
	}

	return nodeIDs, edgeKeys
}

// hydrateNodes fetches every node in ids from graphID in one query, keyed by
// database id, erroring if any requested id is missing from the result.
func hydrateNodes(ctx context.Context, pool *pgxpool.Pool, kindMapper pg.KindMapper, graphID int32, ids []uint64) (map[uint64]*graph.Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	queryIDs := make([]int64, len(ids))
	for i, id := range ids {
		queryIDs[i] = int64(id)
	}

	rows, err := pool.Query(ctx, "SELECT id, kind_ids, properties FROM node WHERE graph_id = $1 AND id = ANY($2)", graphID, queryIDs)
	if err != nil {
		return nil, fmt.Errorf("engine: hydratePaths: query nodes: %w", err)
	}
	defer rows.Close()

	out := make(map[uint64]*graph.Node, len(ids))
	for rows.Next() {
		var (
			id         int64
			kindIDs    []int16
			properties map[string]any
		)
		if err := rows.Scan(&id, &kindIDs, &properties); err != nil {
			return nil, fmt.Errorf("engine: hydratePaths: scan node: %w", err)
		}

		kinds, err := kindMapper.MapKindIDs(ctx, kindIDs)
		if err != nil {
			return nil, fmt.Errorf("engine: hydratePaths: map kinds for node %d: %w", id, err)
		}

		out[uint64(id)] = graph.NewNode(graph.ID(uint64(id)), graph.AsProperties(properties), kinds...)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("engine: hydratePaths: node rows: %w", err)
	}

	if len(out) != len(ids) {
		return nil, fmt.Errorf("engine: hydratePaths: %d of %d requested nodes not found in the database (deleted since the snapshot was loaded)", len(ids)-len(out), len(ids))
	}

	return out, nil
}

// hydrateEdges fetches every edge in keys from graphID, in batches of at
// most edgeBatchSize triples, keyed by (start, end, kind). It errors if any
// requested triple is missing from the result.
func hydrateEdges(ctx context.Context, pool *pgxpool.Pool, kindMapper pg.KindMapper, graphID int32, keys []edgeKey) (map[edgeKey]*graph.Relationship, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	out := make(map[edgeKey]*graph.Relationship, len(keys))

	for lo := 0; lo < len(keys); lo += edgeBatchSize {
		hi := lo + edgeBatchSize
		if hi > len(keys) {
			hi = len(keys)
		}

		if err := hydrateEdgeBatch(ctx, pool, kindMapper, graphID, keys[lo:hi], out); err != nil {
			return nil, err
		}
	}

	if len(out) != len(keys) {
		return nil, fmt.Errorf("engine: hydratePaths: %d of %d requested edges not found in the database (deleted since the snapshot was loaded)", len(keys)-len(out), len(keys))
	}

	return out, nil
}

// hydrateEdgeBatch runs one edge query over a single batch (at most
// edgeBatchSize triples), writing results into out.
func hydrateEdgeBatch(ctx context.Context, pool *pgxpool.Pool, kindMapper pg.KindMapper, graphID int32, batch []edgeKey, out map[edgeKey]*graph.Relationship) error {
	sql, args := edgeBatchQuery(graphID, batch)

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("engine: hydratePaths: query edges: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, start, end int64
			kind           snapshot.KindID
			properties     map[string]any
		)
		if err := rows.Scan(&id, &start, &end, &kind, &properties); err != nil {
			return fmt.Errorf("engine: hydratePaths: scan edge: %w", err)
		}

		mappedKind, err := kindMapper.MapKindID(ctx, kind)
		if err != nil {
			return fmt.Errorf("engine: hydratePaths: map kind for edge %d: %w", id, err)
		}

		key := edgeKey{start: uint64(start), end: uint64(end), kind: kind}
		out[key] = graph.NewRelationship(graph.ID(uint64(id)), graph.ID(uint64(start)), graph.ID(uint64(end)), graph.AsProperties(properties), mappedKind)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: hydratePaths: edge rows: %w", err)
	}

	return nil
}

// hydrateEdgePropsByID fetches the properties of every edge in ids from
// graphID, keyed by database edge id, in batches of at most
// edgePropsBatchSize (serve_cypher.go's own constant) ids per `WHERE id
// = ANY($1)` round trip. Unlike hydrateNodes/hydrateEdges (which report a
// missing entity as an aggregate count), a missing id here is reported
// individually -- "bloodtrail: edge %d vanished during hydration" -- since
// this is the shape the TryCypher pipeline is expected to surface to a
// caller as a declined query, and a single concrete id is more actionable
// there than a bare count. Duplicate ids in the input are deduplicated
// before any query is issued, so a caller (e.g. multiple paths sharing an
// edge) never pays for or reports the same id twice. An empty ids returns an
// empty, non-nil map without issuing any query.
func hydrateEdgePropsByID(ctx context.Context, pool *pgxpool.Pool, graphID int32, ids []uint64) (map[uint64]*graph.Properties, error) {
	return hydrateEdgePropsByIDBatched(ctx, pool, graphID, ids, edgePropsBatchSize)
}

// hydrateEdgePropsByIDBatched is hydrateEdgePropsByID's implementation with
// an explicit batch size, split out so integration tests can exercise the
// multi-batch path deterministically without seeding edgePropsBatchSize
// (10_000) edges just to cross one batch boundary.
func hydrateEdgePropsByIDBatched(ctx context.Context, pool *pgxpool.Pool, graphID int32, ids []uint64, batchSize int) (map[uint64]*graph.Properties, error) {
	uniqueIDs := dedupeUint64s(ids)
	if len(uniqueIDs) == 0 {
		return map[uint64]*graph.Properties{}, nil
	}

	out := make(map[uint64]*graph.Properties, len(uniqueIDs))

	for lo := 0; lo < len(uniqueIDs); lo += batchSize {
		hi := lo + batchSize
		if hi > len(uniqueIDs) {
			hi = len(uniqueIDs)
		}

		if err := hydrateEdgePropsBatch(ctx, pool, graphID, uniqueIDs[lo:hi], out); err != nil {
			return nil, err
		}
	}

	for _, id := range uniqueIDs {
		if _, ok := out[id]; !ok {
			return nil, fmt.Errorf("bloodtrail: edge %d vanished during hydration", id)
		}
	}

	return out, nil
}

// hydrateEdgePropsBatch runs one `id = ANY($2)` query over a single batch of
// database edge ids, writing decoded properties into out keyed by id.
func hydrateEdgePropsBatch(ctx context.Context, pool *pgxpool.Pool, graphID int32, batch []uint64, out map[uint64]*graph.Properties) error {
	queryIDs := make([]int64, len(batch))
	for i, id := range batch {
		queryIDs[i] = int64(id)
	}

	rows, err := pool.Query(ctx, "SELECT id, properties FROM edge WHERE graph_id = $1 AND id = ANY($2)", graphID, queryIDs)
	if err != nil {
		return fmt.Errorf("engine: hydrateEdgePropsByID: query edge properties: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id         int64
			properties map[string]any
		)
		if err := rows.Scan(&id, &properties); err != nil {
			return fmt.Errorf("engine: hydrateEdgePropsByID: scan edge properties: %w", err)
		}

		out[uint64(id)] = graph.AsProperties(properties)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: hydrateEdgePropsByID: edge property rows: %w", err)
	}

	return nil
}

// dedupeUint64s returns ids with duplicates removed, preserving first-seen
// order, or nil if ids is empty. First-seen order keeps
// hydrateEdgePropsByIDBatched's missing-id error deterministic across
// repeated calls with the same input.
func dedupeUint64s(ids []uint64) []uint64 {
	if len(ids) == 0 {
		return nil
	}

	seen := make(map[uint64]struct{}, len(ids))
	out := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

// edgeBatchQuery builds the parameterized SELECT ... WHERE (start_id,
// end_id, kind_id) IN (VALUES ...) statement for one batch, along with its
// positional arguments ($1 = graphID, the rest batch's triples in order).
// Explicit casts on each VALUES tuple pin pgx's inferred parameter types
// (bigint, bigint, smallint) so the IN comparison type-checks against the
// edge table's columns.
func edgeBatchQuery(graphID int32, batch []edgeKey) (string, []any) {
	var sql strings.Builder
	sql.WriteString("SELECT id, start_id, end_id, kind_id, properties FROM edge WHERE graph_id = $1 AND (start_id, end_id, kind_id) IN (VALUES ")

	args := make([]any, 0, 1+len(batch)*3)
	args = append(args, graphID)

	for i, key := range batch {
		if i > 0 {
			sql.WriteString(", ")
		}
		n := len(args)
		fmt.Fprintf(&sql, "($%d::bigint, $%d::bigint, $%d::smallint)", n+1, n+2, n+3)
		args = append(args, int64(key.start), int64(key.end), key.kind)
	}

	sql.WriteString(")")

	return sql.String(), args
}

// assemblePath builds one graph.Path from p, looking node and edge pointers
// up in the already-hydrated nodes/edges maps by database id / edge key.
// hydrateNodes and hydrateEdges already error out on any globally missing
// entity, so a lookup miss here would indicate an internal bug rather than
// a real deletion; the check stays as cheap insurance against a nil-pointer
// panic.
func assemblePath(snap *snapshot.View, p traverse.Path, nodes map[uint64]*graph.Node, edges map[edgeKey]*graph.Relationship) (graph.Path, error) {
	gp := graph.Path{
		Nodes: make([]*graph.Node, len(p.Nodes)),
		Edges: make([]*graph.Relationship, len(p.Kinds)),
	}

	for i, dense := range p.Nodes {
		id := snap.GraphID(dense)
		node, ok := nodes[id]
		if !ok {
			return graph.Path{}, fmt.Errorf("engine: hydratePaths: node %d missing from hydrated set", id)
		}
		gp.Nodes[i] = node
	}

	for i, kind := range p.Kinds {
		key := edgeKey{start: snap.GraphID(p.Nodes[i]), end: snap.GraphID(p.Nodes[i+1]), kind: kind}
		edge, ok := edges[key]
		if !ok {
			return graph.Path{}, fmt.Errorf("engine: hydratePaths: edge (%d, %d, kind %d) missing from hydrated set", key.start, key.end, key.kind)
		}
		gp.Edges[i] = edge
	}

	return gp, nil
}
