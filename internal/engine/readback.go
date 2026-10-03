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
// commit) of every row a ChangeSet's write could have touched, split into
// what was found and what wasn't. The applier (buildApplySegment) stages
// nodes/edges into a delta segment as upserts and every absent* entry as a
// tombstone -- the row is gone, or (for a triple) was never produced.
//
// absentNodeIDs and absentEdgeIDs cover the ids the ChangeSet named and the
// View candidates readBack re-read (viewCandidates) alike; an objectid that
// matched no row has no entry of its own, since what it means for the
// replica is decided by re-reading the nodes the View knew under it.
//
// resolvedKinds carries the name of every kind id this read-back
// encountered (in a node's kindIDs, or an edge's kindID) that the View
// didn't already know about -- typically a kind created or asserted after
// that snapshot was built -- plus every kind a kind-scoped delete criteria
// names that the View didn't know either (resolveCriteriaKinds). The
// applier feeds these straight into SegmentBuilder.AddKind so the delta
// segment it builds from this result can name the kind at all.
type readbackResult struct {
	nodes         []nodeState
	absentNodeIDs []uint64

	edges         []edgeState
	absentEdgeIDs []uint64
	absentTriples []tripleKey

	resolvedKinds map[int16]string
}

// readBack queries PostgreSQL, on e.writePool (writePathPool; post-commit
// visibility -- a pool of its own, not any specific
// transaction, so it always sees a write that has already committed), for
// the current state of every row cs's write could have touched, as far as
// view -- the View the result is going to be layered onto -- can tell. It
// never retries and performs no I/O beyond what's described below; a caller
// that gets a transient error back is expected to decide whether/how to
// retry on its own.
//
// Five independent lookups of the keys cs recorded, each described in more
// detail on its own helper, then one re-read of the View's candidates:
//
//  1. cs.NodeIDs() by database id (readBackNodesByID) -- an id absent from
//     the result is reported via absentNodeIDs.
//  2. cs.NodeObjectIDs() by the `objectid` property (readBackNodesByObjectID)
//     -- an objectid can legitimately match more than one row (PostgreSQL
//     enforces no uniqueness constraint on it), so every match is kept. Every
//     match is also merged into the same node result set (1) uses, keyed by
//     its own database id -- a node named by both its id and (separately) by
//     an objectid it happens to carry is reported exactly once. An objectid
//     matching zero rows makes the nodes view knows under it candidates for
//     the re-read below.
//  3. cs.EdgeIDs() by database id (readBackEdgesByID), mirroring (1).
//  4. cs.EdgeTriples() by (start, end, kind name) -- resolveKindIDs
//     resolves each distinct kind name to its current KindID; a name that
//     ends up unresolved (genuinely never asserted, per resolveKindIDs' own
//     doc -- which also covers the residual ambiguity a name can be
//     unresolved for) means the triple is reported absent (kindID
//     unresolvedTripleKind) without ever being queried. A hard failure
//     resolving kind ids (ctx cancellation or deadline) aborts readBack
//     entirely instead of guessing -- see resolveKindIDs' own doc.
//     Every triple whose kind does resolve is queried in one shared batch
//     pass together with (5) below (readBackEdgesByTriple, adapted from
//     hydrate.go's own edgeBatchQuery); a triple not found in the result is
//     reported via absentTriples with its real, resolved kindID.
//  5. cs.EdgeTriplesByObjectID() -- each endpoint objectid is resolved
//     against (2)'s own result rather than queried again. A pair where both
//     endpoints resolve is expanded into every (start id, end id)
//     combination across both endpoints' matches (deduplicated, and bounded
//     by how many rows (2) actually returned for those two objectids) and
//     folded into the same batch pass (4) runs. A pair where either
//     endpoint's objectid matched zero rows in (2) is held back instead (the
//     write either failed, or the endpoint is gone or re-keyed since) and
//     resolved in a second pass after the candidate re-read below, which is
//     what tells those cases apart: a re-keyed endpoint re-reads PRESENT
//     under its own (unchanged) id, which is the id the upsert resolved its
//     objectid to, so the triple is queried for those ids
//     (rekeyedTripleKeys) rather than left to a later write or a rebuild;
//     an endpoint that really is gone takes the edge with it through its own
//     tombstone's cascade.
//
// Then the candidates (viewCandidates): the View rows cs's write may have
// removed without naming them by key -- every node or edge a kind-scoped
// delete criteria (NodeKindCriteria, EdgeKindCriteria) matches in view, and
// every node view still knows under an objectid (2) found on no row. They
// are re-read by id exactly like (1) and (3) (rereadByID), in the same
// chunks, skipping any id the lookups above already answered: a present
// candidate joins the node or edge results -- restaged as PostgreSQL holds
// it now -- and an absent one joins absentNodeIDs or absentEdgeIDs. This is
// what makes a kind-scoped delete a read-back like every other write rather
// than an instruction replayed over whatever view holds by the time the
// delete is applied: rows another writer committed after the DELETE's
// snapshot, and applied first, come back present and stay. The criteria's
// kind names resolve against view's kind table, then by name in PostgreSQL
// for any view doesn't know (resolveCriteriaKinds, whose results also join
// resolvedKinds); an exclusion that resolves nowhere is an error (see
// viewCandidates.addNodeKindCriteria). A nil view -- only ever a test's --
// has no candidates, and so leaves (5)'s held-back triples unresolved.
//
// Finally, every kind id encountered in a returned node or edge row is
// checked against view (a nil view treats every kind id as unknown). Any id
// the view doesn't recognize is resolved, in one batched call, via
// e.pgDriver.KindMapper().MapKindIDs and recorded in resolvedKinds; a
// mapper failure here is returned as an error rather than silently
// producing an incomplete resolvedKinds map, since the applier has no safe
// way to build a delta segment naming a kind it can't resolve -- its only
// sound response is to fall back to a full resync, exactly as ChangeSet's
// own RecordFallback path already does for other unrepresentable writes.
func (e *Engine) readBack(ctx context.Context, view *snapshot.View, cs *ChangeSet) (*readbackResult, error) {
	graphModel, ok := e.pgDriver.DefaultGraph()
	if !ok {
		return nil, fmt.Errorf("engine: readBack: no default graph is set")
	}
	graphID := graphModel.ID
	kindMapper := e.pgDriver.KindMapper()

	result := &readbackResult{}
	nodesByID := make(map[uint64]nodeState)

	// Every read-back query runs on the write path's own pool: Apply can be
	// running while its caller still holds one of e.pool's connections (a
	// mid-batch Commit), and a second connection from that pool is what
	// saturated writers waited on each other for (writePathPool).
	pool := e.writePool.get(e.pool)

	nodeIDs := cs.NodeIDs()
	foundByID, err := readBackNodesByID(ctx, pool, graphID, nodeIDs)
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
	oidRows, oidToIDs, err := readBackNodesByObjectID(ctx, pool, graphID, objectIDs)
	if err != nil {
		return nil, err
	}
	for _, ns := range oidRows {
		nodesByID[ns.id] = ns
	}

	edgesByID := make(map[uint64]edgeState)

	edgeIDs := cs.EdgeIDs()
	foundEdgesByID, err := readBackEdgesByID(ctx, pool, graphID, edgeIDs)
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

	triples := cs.EdgeTriples()
	oidTriples := cs.EdgeTriplesByObjectID()

	kindNames := make([]graph.Kind, 0, len(triples)+len(oidTriples))
	for _, t := range triples {
		kindNames = append(kindNames, t.Kind)
	}
	for _, t := range oidTriples {
		kindNames = append(kindNames, t.Kind)
	}
	resolvedTripleKinds, err := resolveKindIDs(ctx, kindMapper, kindNames)
	if err != nil {
		return nil, err
	}

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

	// Triples with an endpoint objectid that matched no row at all, held
	// back until the View's own candidates have been re-read below: what
	// such a triple means depends on whether that endpoint is actually gone
	// or merely re-keyed, which only the re-read can tell (rekeyedTripleKeys).
	var deferredOIDTriples []EdgeTripleOIDRef

	for _, t := range oidTriples {
		startIDs, endIDs := oidToIDs[t.StartOID], oidToIDs[t.EndOID]
		if len(startIDs) == 0 || len(endIDs) == 0 {
			// Unresolvable endpoint: the write failed, or the node is gone
			// or re-keyed since. Deferred to the second pass below, which
			// resolves it against the candidate re-read's own answer.
			deferredOIDTriples = append(deferredOIDTriples, t)
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

	foundTriples, err := readBackEdgesByTriple(ctx, pool, graphID, pendingKeys)
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

	criteriaKinds, err := resolveCriteriaKinds(ctx, kindMapper, view, cs)
	if err != nil {
		return nil, err
	}

	if view != nil {
		var absentObjectIDs []string
		for _, oid := range objectIDs {
			if len(oidToIDs[oid]) == 0 {
				absentObjectIDs = append(absentObjectIDs, oid)
			}
		}
		candidates, err := collectViewCandidates(view, criteriaKinds, cs, absentObjectIDs)
		if err != nil {
			return nil, err
		}

		result.absentNodeIDs, err = rereadByID(candidates.nodeIDs(), nodesByID, result.absentNodeIDs,
			func(ids []uint64) (map[uint64]nodeState, error) { return readBackNodesByID(ctx, pool, graphID, ids) })
		if err != nil {
			return nil, err
		}
		result.absentEdgeIDs, err = rereadByID(candidates.edges, edgesByID, result.absentEdgeIDs,
			func(ids []uint64) (map[uint64]edgeState, error) { return readBackEdgesByID(ctx, pool, graphID, ids) })
		if err != nil {
			return nil, err
		}

		// Step 5's second pass: the triples held back above, now that the
		// candidate re-read has settled which of their endpoints were only
		// re-keyed. One more batch pass, only ever reached by a write that
		// raced a re-key of one of its own endpoints.
		rekeyed := rekeyedTripleKeys(view, nodesByID, oidToIDs, resolvedTripleKinds, deferredOIDTriples, pending, absentTriples)
		foundRekeyed, err := readBackEdgesByTriple(ctx, pool, graphID, rekeyed)
		if err != nil {
			return nil, err
		}
		for _, k := range rekeyed {
			if es, ok := foundRekeyed[k]; ok {
				edgesByID[es.id] = es
			} else {
				absentTriples[tripleKey{start: k.start, end: k.end, kindID: k.kind}] = struct{}{}
			}
		}
	}

	result.nodes = sortedNodeStates(nodesByID)
	sort.Slice(result.absentNodeIDs, func(i, j int) bool { return result.absentNodeIDs[i] < result.absentNodeIDs[j] })
	result.edges = sortedEdgeStates(edgesByID)
	sort.Slice(result.absentEdgeIDs, func(i, j int) bool { return result.absentEdgeIDs[i] < result.absentEdgeIDs[j] })
	result.absentTriples = sortedTripleKeys(absentTriples)

	resolvedKinds, err := resolveUnknownKinds(ctx, kindMapper, view, result.nodes, result.edges)
	if err != nil {
		return nil, err
	}
	for id, name := range criteriaKinds {
		if resolvedKinds == nil {
			resolvedKinds = make(map[int16]string, len(criteriaKinds))
		}
		resolvedKinds[id] = name
	}
	result.resolvedKinds = resolvedKinds

	return result, nil
}

// rekeyedTripleKeys resolves the objectid-keyed triples readBack's step 5
// held back -- those with an endpoint objectid that matched no row -- into
// the (start, end, kind) keys still worth querying, once the candidate
// re-read has answered what became of the nodes view knew under each of
// those objectids.
//
// An objectid matching no row does not mean its node is gone. A node whose
// objectid was rewritten keeps its id, its row and its edges, so it
// re-reads PRESENT -- and that settles the NODE, not the edge an
// UpdateRelationshipBy upsert created through it: the upsert resolved that
// objectid to a node id while it still carried it, and a re-key changes no
// node id, so the edge's endpoint is exactly the node view knew under the
// old objectid. Those ids are what this resolves such an endpoint to
// (rekeyedEndpointIDs), which is what closes the re-key race's staleness
// window -- without them the committed edge stays out of the replica until
// a later write names it or a rebuild loads it.
//
// An endpoint whose nodes all re-read ABSENT needs nothing: deleting a node
// deletes its edges, and the cascade of its own tombstone (buildApplySegment)
// takes the edge with it. So does an objectid view never knew anything
// under, which is what a write that never landed looks like from here -- and
// also the one residual this leaves: an endpoint node the upsert itself
// created and another writer re-keyed within the same window is unknown to
// both pg's objectid lookup and the View, so neither it nor its edge can be
// resolved here. That costs a false negative (a missing row), never a wrong
// one, and needs a writer to re-key a node it has never seen.
//
// Keys already queried in the first pass (pending) are skipped; a triple
// whose kind never resolved to a KindID joins absentTriples under the
// unresolvedTripleKind sentinel, exactly as the first pass records one.
// Both maps are read and written in place.
func rekeyedTripleKeys(view *snapshot.View, nodesByID map[uint64]nodeState, oidToIDs map[string][]uint64,
	resolvedTripleKinds map[string]int16, deferred []EdgeTripleOIDRef,
	pending map[edgeKey]struct{}, absentTriples map[tripleKey]struct{}) []edgeKey {
	if len(deferred) == 0 {
		return nil
	}

	resolve := func(objectID string) []uint64 {
		if ids := oidToIDs[objectID]; len(ids) > 0 {
			return ids
		}
		return rekeyedEndpointIDs(view, nodesByID, objectID)
	}

	var keys []edgeKey
	seen := make(map[edgeKey]struct{})
	for _, t := range deferred {
		startIDs, endIDs := resolve(t.StartOID), resolve(t.EndOID)
		if len(startIDs) == 0 || len(endIDs) == 0 {
			continue
		}

		kindID, ok := resolvedTripleKinds[kindName(t.Kind)]
		for _, start := range startIDs {
			for _, end := range endIDs {
				if !ok {
					absentTriples[tripleKey{start: start, end: end, kindID: unresolvedTripleKind}] = struct{}{}
					continue
				}
				key := edgeKey{start: start, end: end, kind: kindID}
				if _, queried := pending[key]; queried {
					continue
				}
				if _, dup := seen[key]; dup {
					continue
				}
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// rekeyedEndpointIDs returns the database ids of the nodes view knows under
// objectID that the candidate re-read found PRESENT in PostgreSQL -- the
// re-keyed endpoints of rekeyedTripleKeys' doc. A node view knows under
// objectID but nodesByID has no row for was re-read absent (rereadByID put
// it in absentNodeIDs), so it is genuinely gone and contributes nothing.
func rekeyedEndpointIDs(view *snapshot.View, nodesByID map[uint64]nodeState, objectID string) []uint64 {
	dense, ok := view.NodesByObjectID(objectID)
	if !ok {
		return nil
	}

	var ids []uint64
	for _, n := range dense {
		id := view.GraphID(n)
		if _, present := nodesByID[id]; present {
			ids = append(ids, id)
		}
	}
	return ids
}

// rereadByID re-reads, through read, every candidate id the ChangeSet's own
// keyed lookups have not already answered -- neither found (a key of found)
// nor reported absent (an element of absent) -- adding each present row to
// found and returning absent extended by each id read found no row for.
// read is readBackNodesByID or readBackEdgesByID, which chunk the ids
// (readbackNodeIDChunk per round trip), so even a delete criteria whose
// kind covers millions of View rows is re-read in bounded queries.
func rereadByID[S any](candidates []uint64, found map[uint64]S, absent []uint64, read func([]uint64) (map[uint64]S, error)) ([]uint64, error) {
	if len(candidates) == 0 {
		return absent, nil
	}

	answered := make(map[uint64]struct{}, len(absent))
	for _, id := range absent {
		answered[id] = struct{}{}
	}
	pending := make([]uint64, 0, len(candidates))
	for _, id := range candidates {
		if _, ok := found[id]; ok {
			continue
		}
		if _, ok := answered[id]; ok {
			continue
		}
		pending = append(pending, id)
	}
	if len(pending) == 0 {
		return absent, nil
	}

	rows, err := read(pending)
	if err != nil {
		return nil, err
	}
	for _, id := range pending {
		if row, ok := rows[id]; ok {
			found[id] = row
		} else {
			absent = append(absent, id)
		}
	}
	return absent, nil
}

// resolveUnknownKinds collects every kind id referenced by nodes/edges that
// view doesn't already recognize, resolves them in one batched call via
// kindMapper.MapKindIDs, and returns the result as an id->name map (nil if
// every kind id was already known). See readBack's own doc for why a nil
// view treats every kind id as unknown, and why a mapper failure here is a
// hard error rather than a partial map.
func resolveUnknownKinds(ctx context.Context, kindMapper pg.KindMapper, view *snapshot.View, nodes []nodeState, edges []edgeState) (map[int16]string, error) {
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

// resolveCriteriaKinds resolves, by name, every kind cs's kind-scoped delete
// criteria name (both halves of each NodeKindCriteria pair, and every
// EdgeKindCriteria set) that view's own kind table doesn't know -- all of
// them for a nil view, the same reading resolveUnknownKinds gives one -- and
// returns those that resolve as an id->name map ready to join
// resolvedKinds, or nil when there is nothing to add.
//
// readBack matches a criteria's kinds against the View's rows to choose
// which rows to re-read (viewCandidates), and without this the View's kind
// table can lack a kind PostgreSQL has had all along. A View learns kinds
// from its last full load's scan of the `kind` table and from the rows
// read-back meets, so a kind registered since then that no row carries stays
// unknown to it -- an OpenGraph source kind whose upload failed after
// registering it, then named by BloodHound's "delete sourceless data"
// exclusion list, is the shape that matters. PostgreSQL's own delete
// resolved that kind (dawgs refuses a node delete with an exclusion it
// cannot resolve), so the match lacks only the id, which this supplies;
// matching then happens by id against every node's own kind ids, so an
// exclusion no node carries excludes nothing, exactly as it does in
// PostgreSQL.
//
// A name that does not resolve is left out rather than reported:
// viewCandidates decides what that means (an include kind matches nothing,
// an exclusion fails closed). The one hard error is resolveKindIDs' own --
// a cancelled or expired ctx.
func resolveCriteriaKinds(ctx context.Context, kindMapper pg.KindMapper, view *snapshot.View, cs *ChangeSet) (map[int16]string, error) {
	var unknown graph.Kinds
	collect := func(kinds graph.Kinds) {
		for _, kind := range kinds {
			if view != nil {
				if _, ok := view.Kinds().ID(kindName(kind)); ok {
					continue
				}
			}
			unknown = append(unknown, kind)
		}
	}
	for _, criteria := range cs.NodeKindCriteria() {
		collect(criteria.Include)
		collect(criteria.Exclude)
	}
	for _, kinds := range cs.EdgeKindCriteria() {
		collect(kinds)
	}
	if len(unknown) == 0 {
		return nil, nil
	}

	ids, err := resolveKindIDs(ctx, kindMapper, unknown)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	resolved := make(map[int16]string, len(ids))
	for name, id := range ids {
		resolved[id] = name
	}
	return resolved, nil
}

// resolveKindIDs resolves every distinct kind name among kinds to its
// current PostgreSQL KindID, deduplicating by name so a kind shared by many
// entries costs one lookup, not one per entry. A name that ends up
// unresolved is simply absent from the result, and each caller decides what
// that means rather than treating it as an error: readBack reports a triple
// of that kind absent without ever querying it ("this triple's kind was
// never asserted, so the edge cannot exist"), and viewCandidates lets an
// unresolved include kind match nothing while an unresolved exclusion fails
// closed. A nil kind is absent from the result too, and never reaches the
// mapper at all: it has no name to resolve, and dawgs formats a mapping
// error by calling String() on every kind it was handed. The exception is a
// hard error (see below), which resolveKindIDs itself returns rather than
// silently folding into "unresolved".
//
// dawgs' pg.KindMapper (v0.8.0, drivers/pg/manager.go) gives MapKind and
// MapKinds a single opaque error return that conflates two very different
// situations: a kind name PostgreSQL has genuinely never heard of (fetched
// the current kind table and it just isn't in there), and a Fetch failure
// while refreshing that kind table (context cancellation, connection pool
// exhaustion, any other transport error) -- there is no sentinel or typed
// error to tell them apart. Treating both the same way, as earlier versions
// of this function did, is wrong: if the write's own ctx has already been
// cancelled or hit its deadline, that failure has nothing to do with
// whether the kind was ever asserted, and reporting the affected entries
// unresolved would let readBack return a confidently-wrong result instead
// of the error the applier needs in order to fall back to a full resync
// (see readBack's own doc).
//
// So every MapKind/MapKinds error is checked against ctx.Err() before it is
// allowed to mean "unresolved": a non-nil ctx.Err() means the error IS the
// cancellation, and resolveKindIDs aborts immediately, returning it as a
// hard error. Only once ctx is confirmed still live does a mapper error get
// treated as "this kind name doesn't exist".
//
// The resolution itself tries kindMapper.MapKinds first, as one batched,
// all-or-nothing round trip covering every deduplicated kind at once: when
// every one of them resolves (by far the common case -- a write's own kinds
// were necessarily asserted before anything could reference them), that
// single call is all this function ever does. MapKinds has no partial
// result to return on error (mapKinds' own all-or-nothing contract, dawgs
// manager.go), so a batch failure -- once ruled out as ctx cancellation --
// falls back to resolving each deduplicated kind with its own MapKind call,
// confining one bad or failing name's damage to that name's own entries
// rather than the whole batch, exactly as a pure per-kind loop always has.
//
// Residual ambiguity, accepted as a dawgs v0.8.0 API limitation: a non-ctx
// infrastructure failure mid-Fetch (ctx still live, e.g. a transient
// connection-pool exhaustion) still can't be told apart from a genuinely
// unasserted kind, so the per-kind fallback still misclassifies it as
// "unresolved" rather than retrying or erroring. The blast radius is
// bounded, not eliminated, by the per-kind fallback above: only that one
// kind name's entries are affected. A delete criteria kind misclassified
// this way costs no more than an unknown kind always did (an include kind
// matches nothing, an exclusion fails closed into a fallback). A triple's
// kind is folded into absentTriples under the unresolvedTripleKind sentinel
// (-1) rather than a real KindID. That sentinel can never match a real
// edge's kind id in the engine's current View -- kindKnownToView's own doc:
// "a KindTable never registers a negative id" -- and the only way a future
// applier can turn an absentTriples entry into a concrete tombstone is by
// resolving it to a real edge id via the View's own adjacency first
// (SegmentBuilder.TombstoneEdge, snapshot/segment.go, takes an edge id, not
// a triple), so a sentinel-kinded entry can never resolve to one. The
// observable failure mode of this residual ambiguity for a triple is
// therefore a false negative -- a just-created edge of that kind silently
// missing from the resulting View -- rather than a wrong tombstone of a
// pre-existing, unrelated View edge.
func resolveKindIDs(ctx context.Context, kindMapper pg.KindMapper, kinds []graph.Kind) (map[string]int16, error) {
	names := make([]string, 0, len(kinds))
	deduped := make(graph.Kinds, 0, len(kinds))
	seen := make(map[string]struct{}, len(kinds))

	for _, kind := range kinds {
		if kind == nil {
			continue
		}
		name := kindName(kind)
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
		deduped = append(deduped, kind)
	}

	resolved := make(map[string]int16, len(deduped))
	if len(deduped) == 0 {
		return resolved, nil
	}

	if ids, err := kindMapper.MapKinds(ctx, deduped); err == nil {
		for i, id := range ids {
			resolved[names[i]] = id
		}
		return resolved, nil
	} else if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("engine: readBack: resolve kind ids: %w", ctxErr)
	}

	// The batch call failed for a reason other than ctx cancellation --
	// most likely at least one deduplicated name doesn't exist, but per
	// this function's own doc that can't be told apart from an infra
	// failure. Fall back to resolving each name on its own so only that
	// name's entries are affected.
	for i, kind := range deduped {
		id, err := kindMapper.MapKind(ctx, kind)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, fmt.Errorf("engine: readBack: resolve kind ids: %w", ctxErr)
			}
			continue
		}
		resolved[names[i]] = id
	}

	return resolved, nil
}

// readBackNodesByID fetches every node in ids from graphID, in batches of at
// most readbackNodeIDChunk ids, keyed by database id. Unlike hydrateNodes
// (hydrate.go), a missing id is not an error: it's reported by the caller as
// an absentNodeIDs entry, since read-back's whole job is to tell present
// apart from absent, not to assume every named key still exists. The result
// is sized for one chunk up front, not for every id: re-reading a mass
// delete's candidates (rereadByID) names many ids of which few still exist.
func readBackNodesByID(ctx context.Context, pool *pgxpool.Pool, graphID int32, ids []uint64) (map[uint64]nodeState, error) {
	out := make(map[uint64]nodeState, min(len(ids), readbackNodeIDChunk))

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
	out := make(map[uint64]edgeState, min(len(ids), readbackNodeIDChunk))

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
