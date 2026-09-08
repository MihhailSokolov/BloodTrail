// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// readbackNodeIDChunk caps how many node (or edge) database ids one direct
// `id = ANY($2)` read-back query carries per round trip. It is not tied to
// edgeBatchSize (hydrate.go's VALUES-list batch cap): a plain `= ANY($2)`
// array parameter has none of the per-tuple bound-parameter cost a
// VALUES-list triple query does, so a single query can carry far more ids
// before hitting PostgreSQL's own limits.
const readbackNodeIDChunk = 50_000

// readbackObjectIDChunk caps how many "objectid" property values one
// `properties->>'objectid' = ANY($2::text[])` read-back query carries per
// round trip -- smaller than readbackNodeIDChunk since each element is a
// variable-length string rather than a fixed-width integer.
const readbackObjectIDChunk = 5_000

// unresolvedTripleKind marks an absentTriples entry whose kind name never
// resolved to a KindID at all -- reusing dawgs' own SchemaManager.MapKind
// sentinel return value (drivers/pg/manager.go) for a kind it doesn't
// recognize, since no real kind_id column value can ever be negative. A
// kind pg has never heard of cannot possibly back a real edge row, so the
// triple this marks was never actually queried against the edge table --
// see readBack's own doc for why that's a safe shortcut, not merely an
// optimization.
const unresolvedTripleKind int16 = -1

// nodeState is one row read-back read from the `node` table: its full kind
// and property state as PostgreSQL holds it right now, in the same raw
// shape hydrateNodes' own row scan uses (kindIDs straight off the kind_ids
// column, propsJSON the jsonb column's wire bytes, undecoded) -- read-back
// hands the applier raw material to build a delta segment from, rather than
// a fully hydrated graph.Node the way hydratePaths' callers need.
type nodeState struct {
	id        uint64
	kindIDs   []int16
	propsJSON []byte
}

// edgeState is edgeState's edge equivalent: one row read back from the
// `edge` table. Read-back never needs an edge's properties (the applier's
// delta segments carry edge state as pure adjacency -- endpoints and kind --
// with properties hydrated lazily the same way hydrateEdgePropsByID already
// does for a served query), so this carries no propsJSON field.
type edgeState struct {
	id, start, end uint64
	kindID         int16
}

// tripleKey identifies one (start, end, kind) edge triple absentTriples
// reports as not found -- either because the triple was queried against the
// edge table and no row matched, or because its kind never resolved to a
// KindID at all (kindID == unresolvedTripleKind), in which case no query
// was ever issued for it.
type tripleKey struct {
	start, end uint64
	kindID     int16
}

// readbackResult is readBack's output: the current PostgreSQL state (post
// commit) of every key a ChangeSet named, split into what was found and
// what wasn't. A later write-through applier (not yet built) replays nodes/
// edges into the in-memory engine's delta segments and treats every
// absent* entry as a tombstone -- the write deleted the row, or (for a
// triple) never actually produced it.
//
// absentObjectIDs covers only node objectids (NodeObjectIDs) that matched
// zero rows; an edge-triple-by-objectid whose endpoint is unresolvable is
// deliberately NOT given its own entry anywhere on this type -- see
// readBack's own doc for why the corresponding absentObjectIDs entry
// already carries that information for the applier.
//
// resolvedKinds carries the name of every kind id this read-back
// encountered (in a node's kindIDs, or an edge's kindID) that the engine's
// current snapshot.View didn't already know about -- typically a kind
// created or asserted after that snapshot was built. The applier feeds
// these straight into SegmentBuilder.AddKind so the delta segment it builds
// from this result can name the kind at all.
type readbackResult struct {
	nodes           []nodeState
	absentNodeIDs   []uint64
	absentObjectIDs []string

	edges         []edgeState
	absentEdgeIDs []uint64
	absentTriples []tripleKey

	resolvedKinds map[int16]string
}

// readBack queries PostgreSQL, on e.pool (post-commit visibility -- the same
// connection pool hydrate.go's own queries run on, not any specific
// transaction, so it always sees a write that has already committed), for
// the current state of every key cs recorded. It never retries and performs
// no I/O beyond what's described below; a caller that gets a transient error
// back is expected to decide whether/how to retry on its own.
//
// Five independent lookups, each described in more detail on its own
// helper:
//
//  1. cs.NodeIDs() by database id (readBackNodesByID) -- an id absent from
//     the result is reported via absentNodeIDs.
//  2. cs.NodeObjectIDs() by the `objectid` property (readBackNodesByObjectID)
//     -- an objectid can legitimately match more than one row (PostgreSQL
//     enforces no uniqueness constraint on it), so every match is kept, and
//     an objectid matching zero rows is reported via absentObjectIDs. Every
//     match is also merged into the same node result set (1) uses, keyed by
//     its own database id -- a node named by both its id and (separately) by
//     an objectid it happens to carry is reported exactly once.
//  3. cs.EdgeIDs() by database id (readBackEdgesByID), mirroring (1).
//  4. cs.EdgeTriples() by (start, end, kind name) -- resolveTripleKindIDs
//     resolves each distinct kind name to its current KindID via
//     e.pgDriver.KindMapper().MapKind; a name that doesn't resolve at all
//     means the edge cannot possibly exist (PostgreSQL has never heard of
//     that kind), so the triple is reported absent (kindID
//     unresolvedTripleKind) without ever being queried. Every triple whose
//     kind does resolve is queried in one shared batch pass together with
//     (5) below (readBackEdgesByTriple, adapted from hydrate.go's own
//     edgeBatchQuery); a triple not found in the result is reported via
//     absentTriples with its real, resolved kindID.
//  5. cs.EdgeTriplesByObjectID() -- each endpoint objectid is resolved
//     against (2)'s own result rather than queried again: a pair where
//     either endpoint's objectid matched zero rows in (2) is reported as
//     nothing at all here (the corresponding absentObjectIDs entry from (2)
//     already tells the applier that endpoint -- and therefore this triple
//     -- can't be resolved; the write either failed or the node was deleted,
//     and both are safe to treat as absence). A pair where both endpoints
//     resolve is expanded into every (start id, end id) combination across
//     both endpoints' matches (deduplicated, and bounded by how many rows
//     (2) actually returned for those two objectids) and folded into the
//     same batch pass (4) runs.
//
// Finally, every kind id encountered in a returned node or edge row is
// checked against the engine's current snapshot.View (e.snap.Load(); a nil
// view treats every kind id as unknown, per readbackResult's own doc). Any
// id the view doesn't recognize is resolved, in one batched call, via
// e.pgDriver.KindMapper().MapKindIDs and recorded in resolvedKinds; a
// mapper failure here is returned as an error rather than silently
// producing an incomplete resolvedKinds map, since the applier has no safe
// way to build a delta segment naming a kind it can't resolve -- its only
// sound response is to fall back to a full resync, exactly as ChangeSet's
// own RecordFallback path already does for other unrepresentable writes.
func (e *Engine) readBack(ctx context.Context, cs *ChangeSet) (*readbackResult, error) {
	graphModel, ok := e.pgDriver.DefaultGraph()
	if !ok {
		return nil, fmt.Errorf("engine: readBack: no default graph is set")
	}
	graphID := graphModel.ID

	result := &readbackResult{}
	nodesByID := make(map[uint64]nodeState)

	nodeIDs := cs.NodeIDs()
	foundByID, err := readBackNodesByID(ctx, e.pool, graphID, nodeIDs)
	if err != nil {
		return nil, err
	}
	for id, ns := range foundByID {
		nodesByID[id] = ns
	}
	for _, id := range nodeIDs {
		if _, ok := foundByID[id]; !ok {
			result.absentNodeIDs = append(result.absentNodeIDs, id)
		}
	}

	objectIDs := cs.NodeObjectIDs()
	oidRows, oidToIDs, err := readBackNodesByObjectID(ctx, e.pool, graphID, objectIDs)
	if err != nil {
		return nil, err
	}
	for _, ns := range oidRows {
		nodesByID[ns.id] = ns
	}
	for _, oid := range objectIDs {
		if len(oidToIDs[oid]) == 0 {
			result.absentObjectIDs = append(result.absentObjectIDs, oid)
		}
	}

	result.nodes = sortedNodeStates(nodesByID)
	sort.Slice(result.absentNodeIDs, func(i, j int) bool { return result.absentNodeIDs[i] < result.absentNodeIDs[j] })
	sort.Strings(result.absentObjectIDs)

	edgesByID := make(map[uint64]edgeState)

	edgeIDs := cs.EdgeIDs()
	foundEdgesByID, err := readBackEdgesByID(ctx, e.pool, graphID, edgeIDs)
	if err != nil {
		return nil, err
	}
	for id, es := range foundEdgesByID {
		edgesByID[id] = es
	}
	for _, id := range edgeIDs {
		if _, ok := foundEdgesByID[id]; !ok {
			result.absentEdgeIDs = append(result.absentEdgeIDs, id)
		}
	}
	sort.Slice(result.absentEdgeIDs, func(i, j int) bool { return result.absentEdgeIDs[i] < result.absentEdgeIDs[j] })

	kindMapper := e.pgDriver.KindMapper()

	triples := cs.EdgeTriples()
	oidTriples := cs.EdgeTriplesByObjectID()

	kindNames := make([]graph.Kind, 0, len(triples)+len(oidTriples))
	for _, t := range triples {
		kindNames = append(kindNames, t.Kind)
	}
	for _, t := range oidTriples {
		kindNames = append(kindNames, t.Kind)
	}
	resolvedTripleKinds := resolveTripleKindIDs(ctx, kindMapper, kindNames)

	pending := make(map[edgeKey]struct{})
	absentTriples := make(map[tripleKey]struct{})

	for _, t := range triples {
		kindID, ok := resolvedTripleKinds[kindName(t.Kind)]
		if !ok {
			absentTriples[tripleKey{start: t.Start, end: t.End, kindID: unresolvedTripleKind}] = struct{}{}
			continue
		}
		pending[edgeKey{start: t.Start, end: t.End, kind: kindID}] = struct{}{}
	}

	for _, t := range oidTriples {
		startIDs, endIDs := oidToIDs[t.StartOID], oidToIDs[t.EndOID]
		if len(startIDs) == 0 || len(endIDs) == 0 {
			// Unresolvable endpoint: the write failed, or the node was
			// deleted since. The corresponding absentObjectIDs entry
			// already reports this; nothing further to record here.
			continue
		}

		kindID, ok := resolvedTripleKinds[kindName(t.Kind)]
		for _, start := range startIDs {
			for _, end := range endIDs {
				if !ok {
					absentTriples[tripleKey{start: start, end: end, kindID: unresolvedTripleKind}] = struct{}{}
					continue
				}
				pending[edgeKey{start: start, end: end, kind: kindID}] = struct{}{}
			}
		}
	}

	pendingKeys := make([]edgeKey, 0, len(pending))
	for k := range pending {
		pendingKeys = append(pendingKeys, k)
	}

	foundTriples, err := readBackEdgesByTriple(ctx, e.pool, graphID, pendingKeys)
	if err != nil {
		return nil, err
	}
	for k := range pending {
		if es, ok := foundTriples[k]; ok {
			edgesByID[es.id] = es
		} else {
			absentTriples[tripleKey{start: k.start, end: k.end, kindID: k.kind}] = struct{}{}
		}
	}

	result.edges = sortedEdgeStates(edgesByID)
	result.absentTriples = sortedTripleKeys(absentTriples)

	resolvedKinds, err := e.resolveUnknownKinds(ctx, kindMapper, result.nodes, result.edges)
	if err != nil {
		return nil, err
	}
	result.resolvedKinds = resolvedKinds

	return result, nil
}

// resolveUnknownKinds collects every kind id referenced by nodes/edges that
// the engine's current snapshot.View doesn't already recognize, resolves
// them in one batched call via kindMapper.MapKindIDs, and returns the
// result as an id->name map (nil if every kind id was already known). See
// readBack's own doc for why a nil view treats every kind id as unknown,
// and why a mapper failure here is a hard error rather than a partial map.
func (e *Engine) resolveUnknownKinds(ctx context.Context, kindMapper pg.KindMapper, nodes []nodeState, edges []edgeState) (map[int16]string, error) {
	view := e.snap.Load()

	unknown := make(map[int16]struct{})
	for _, ns := range nodes {
		for _, kindID := range ns.kindIDs {
			if !kindKnownToView(view, kindID) {
				unknown[kindID] = struct{}{}
			}
		}
	}
	for _, es := range edges {
		if !kindKnownToView(view, es.kindID) {
			unknown[es.kindID] = struct{}{}
		}
	}

	if len(unknown) == 0 {
		return nil, nil
	}

	ids := make([]int16, 0, len(unknown))
	for id := range unknown {
		ids = append(ids, id)
	}

	kinds, err := kindMapper.MapKindIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("engine: readBack: map unknown kind ids: %w", err)
	}

	resolved := make(map[int16]string, len(ids))
	for i, kind := range kinds {
		if kind != nil {
			resolved[ids[i]] = kind.String()
		}
	}
	return resolved, nil
}

// kindKnownToView reports whether view's own kind table already names id --
// false for a nil view (readBack's doc: "nil view -> all kinds
// unknown-but-resolvable"), never true for unresolvedTripleKind (a KindTable
// never registers a negative id).
func kindKnownToView(view *snapshot.View, id int16) bool {
	if view == nil {
		return false
	}
	_, ok := view.Kinds().Name(id)
	return ok
}

// resolveTripleKindIDs resolves every distinct kind name among kinds to its
// current PostgreSQL KindID via kindMapper.MapKind, deduplicating by name so
// a kind shared by many triples costs one call, not one per triple. A name
// kindMapper doesn't recognize is simply absent from the result -- callers
// treat that as "this triple's kind was never asserted, so the edge cannot
// exist" rather than as an error, per readBack's own doc.
func resolveTripleKindIDs(ctx context.Context, kindMapper pg.KindMapper, kinds []graph.Kind) map[string]int16 {
	resolved := make(map[string]int16, len(kinds))
	seen := make(map[string]struct{}, len(kinds))

	for _, kind := range kinds {
		name := kindName(kind)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}

		if id, err := kindMapper.MapKind(ctx, kind); err == nil {
			resolved[name] = id
		}
	}

	return resolved
}

// readBackNodesByID fetches every node in ids from graphID, in batches of at
// most readbackNodeIDChunk ids, keyed by database id. Unlike hydrateNodes
// (hydrate.go), a missing id is not an error: it's reported by the caller as
// an absentNodeIDs entry, since read-back's whole job is to tell present
// apart from absent, not to assume every named key still exists.
func readBackNodesByID(ctx context.Context, pool *pgxpool.Pool, graphID int32, ids []uint64) (map[uint64]nodeState, error) {
	out := make(map[uint64]nodeState, len(ids))

	for lo := 0; lo < len(ids); lo += readbackNodeIDChunk {
		hi := lo + readbackNodeIDChunk
		if hi > len(ids) {
			hi = len(ids)
		}
		if err := readBackNodesByIDBatch(ctx, pool, graphID, ids[lo:hi], out); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// readBackNodesByIDBatch runs one `id = ANY($2)` query over a single batch,
// writing results into out.
func readBackNodesByIDBatch(ctx context.Context, pool *pgxpool.Pool, graphID int32, batch []uint64, out map[uint64]nodeState) error {
	queryIDs := make([]int64, len(batch))
	for i, id := range batch {
		queryIDs[i] = int64(id)
	}

	rows, err := pool.Query(ctx, "SELECT id, kind_ids, properties FROM node WHERE graph_id = $1 AND id = ANY($2)", graphID, queryIDs)
	if err != nil {
		return fmt.Errorf("engine: readBack: query nodes by id: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id        int64
			kindIDs   []int16
			propsJSON []byte
		)
		if err := rows.Scan(&id, &kindIDs, &propsJSON); err != nil {
			return fmt.Errorf("engine: readBack: scan node by id: %w", err)
		}
		out[uint64(id)] = nodeState{id: uint64(id), kindIDs: kindIDs, propsJSON: propsJSON}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: readBack: node-by-id rows: %w", err)
	}

	return nil
}

// readBackNodesByObjectID fetches every node whose `objectid` property
// matches one of objectIDs, in batches of at most readbackObjectIDChunk
// values, using the partition's own `objectid` expression index. It returns
// every matching row (an objectid can legitimately match more than one
// node -- PostgreSQL enforces no uniqueness constraint on it) alongside
// oidToIDs, which attributes each returned row back to the objectid
// value(s) it actually carries, read out of the row's own decoded property
// bag rather than assumed from which batch it was queried in (a single
// batch can carry many distinct objectid values at once).
func readBackNodesByObjectID(ctx context.Context, pool *pgxpool.Pool, graphID int32, objectIDs []string) ([]nodeState, map[string][]uint64, error) {
	var rows []nodeState
	oidToIDs := make(map[string][]uint64)

	for lo := 0; lo < len(objectIDs); lo += readbackObjectIDChunk {
		hi := lo + readbackObjectIDChunk
		if hi > len(objectIDs) {
			hi = len(objectIDs)
		}
		if err := readBackNodesByObjectIDBatch(ctx, pool, graphID, objectIDs[lo:hi], &rows, oidToIDs); err != nil {
			return nil, nil, err
		}
	}

	return rows, oidToIDs, nil
}

// readBackNodesByObjectIDBatch runs one `properties->>'objectid' =
// ANY($2::text[])` query over a single batch, appending every matching row
// to *rows and indexing it into oidToIDs by its own decoded objectid value.
func readBackNodesByObjectIDBatch(ctx context.Context, pool *pgxpool.Pool, graphID int32, batch []string, rows *[]nodeState, oidToIDs map[string][]uint64) error {
	r, err := pool.Query(ctx, "SELECT id, kind_ids, properties FROM node WHERE graph_id = $1 AND properties->>'objectid' = ANY($2::text[])", graphID, batch)
	if err != nil {
		return fmt.Errorf("engine: readBack: query nodes by objectid: %w", err)
	}
	defer r.Close()

	for r.Next() {
		var (
			id        int64
			kindIDs   []int16
			propsJSON []byte
		)
		if err := r.Scan(&id, &kindIDs, &propsJSON); err != nil {
			return fmt.Errorf("engine: readBack: scan node by objectid: %w", err)
		}

		ns := nodeState{id: uint64(id), kindIDs: kindIDs, propsJSON: propsJSON}
		*rows = append(*rows, ns)

		if oid, ok := objectIDFromProps(propsJSON); ok {
			oidToIDs[oid] = append(oidToIDs[oid], ns.id)
		}
	}
	if err := r.Err(); err != nil {
		return fmt.Errorf("engine: readBack: node-by-objectid rows: %w", err)
	}

	return nil
}

// objectIDFromProps decodes propsJSON just far enough to read its
// "objectid" key as a string, without decoding the rest of the property
// bag. ok is false for a missing key, a non-string value, or malformed
// JSON (not expected from a jsonb column, but handled the same
// non-panicking way ParseProps' own callers would want).
func objectIDFromProps(propsJSON []byte) (string, bool) {
	if len(propsJSON) == 0 {
		return "", false
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(propsJSON, &raw); err != nil {
		return "", false
	}

	val, ok := raw["objectid"]
	if !ok {
		return "", false
	}

	var s string
	if err := json.Unmarshal(val, &s); err != nil {
		return "", false
	}
	return s, true
}

// readBackEdgesByID is readBackNodesByID's edge equivalent, over the `edge`
// table's own id column.
func readBackEdgesByID(ctx context.Context, pool *pgxpool.Pool, graphID int32, ids []uint64) (map[uint64]edgeState, error) {
	out := make(map[uint64]edgeState, len(ids))

	for lo := 0; lo < len(ids); lo += readbackNodeIDChunk {
		hi := lo + readbackNodeIDChunk
		if hi > len(ids) {
			hi = len(ids)
		}
		if err := readBackEdgesByIDBatch(ctx, pool, graphID, ids[lo:hi], out); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// readBackEdgesByIDBatch runs one `id = ANY($2)` query over a single batch
// of edge ids, writing results into out.
func readBackEdgesByIDBatch(ctx context.Context, pool *pgxpool.Pool, graphID int32, batch []uint64, out map[uint64]edgeState) error {
	queryIDs := make([]int64, len(batch))
	for i, id := range batch {
		queryIDs[i] = int64(id)
	}

	rows, err := pool.Query(ctx, "SELECT id, start_id, end_id, kind_id FROM edge WHERE graph_id = $1 AND id = ANY($2)", graphID, queryIDs)
	if err != nil {
		return fmt.Errorf("engine: readBack: query edges by id: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, start, end int64
			kind           int16
		)
		if err := rows.Scan(&id, &start, &end, &kind); err != nil {
			return fmt.Errorf("engine: readBack: scan edge by id: %w", err)
		}
		out[uint64(id)] = edgeState{id: uint64(id), start: uint64(start), end: uint64(end), kindID: kind}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: readBack: edge-by-id rows: %w", err)
	}

	return nil
}

// readBackEdgesByTriple fetches every edge in keys from graphID, in batches
// of at most edgeBatchSize triples (hydrate.go's own VALUES-list cap, reused
// here since it's the same query shape and the same PostgreSQL bound-
// parameter budget applies), keyed by (start, end, kind). Unlike
// hydrateEdges, a triple absent from the result is not an error -- the
// caller reports it via absentTriples instead.
func readBackEdgesByTriple(ctx context.Context, pool *pgxpool.Pool, graphID int32, keys []edgeKey) (map[edgeKey]edgeState, error) {
	out := make(map[edgeKey]edgeState, len(keys))

	for lo := 0; lo < len(keys); lo += edgeBatchSize {
		hi := lo + edgeBatchSize
		if hi > len(keys) {
			hi = len(keys)
		}
		if err := readBackEdgeTripleBatch(ctx, pool, graphID, keys[lo:hi], out); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// readBackEdgeTripleBatch runs one batch of edgeBatchQuery (hydrate.go) --
// the same (start_id, end_id, kind_id) IN (VALUES ...) shape hydrateEdges
// itself queries with, reused as-is rather than duplicated -- discarding the
// properties column it also selects (read-back's edgeState carries no
// properties; see its own doc for why).
func readBackEdgeTripleBatch(ctx context.Context, pool *pgxpool.Pool, graphID int32, batch []edgeKey, out map[edgeKey]edgeState) error {
	sql, args := edgeBatchQuery(graphID, batch)

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("engine: readBack: query edges by triple: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, start, end int64
			kind           snapshot.KindID
			properties     map[string]any // discarded: readBack needs no edge properties
		)
		if err := rows.Scan(&id, &start, &end, &kind, &properties); err != nil {
			return fmt.Errorf("engine: readBack: scan edge by triple: %w", err)
		}

		key := edgeKey{start: uint64(start), end: uint64(end), kind: kind}
		out[key] = edgeState{id: uint64(id), start: uint64(start), end: uint64(end), kindID: int16(kind)}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("engine: readBack: edge-by-triple rows: %w", err)
	}

	return nil
}

// sortedNodeStates returns m's values ordered ascending by id -- readBack's
// own result ordering is not load-bearing for correctness (its doc says so
// explicitly), but a deterministic order makes both this package's tests
// and any future caller's own diffing/logging simpler.
func sortedNodeStates(m map[uint64]nodeState) []nodeState {
	if len(m) == 0 {
		return nil
	}
	out := make([]nodeState, 0, len(m))
	for _, ns := range m {
		out = append(out, ns)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// sortedEdgeStates is sortedNodeStates' edge equivalent.
func sortedEdgeStates(m map[uint64]edgeState) []edgeState {
	if len(m) == 0 {
		return nil
	}
	out := make([]edgeState, 0, len(m))
	for _, es := range m {
		out = append(out, es)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// sortedTripleKeys returns m's keys ordered ascending by (start, end,
// kindID), the same determinism rationale sortedNodeStates' doc gives.
func sortedTripleKeys(m map[tripleKey]struct{}) []tripleKey {
	if len(m) == 0 {
		return nil
	}
	out := make([]tripleKey, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].start != out[j].start {
			return out[i].start < out[j].start
		}
		if out[i].end != out[j].end {
			return out[i].end < out[j].end
		}
		return out[i].kindID < out[j].kindID
	})
	return out
}
