// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Decline reasons specific to the builder-serving path (this file and Task
// 7's rel-query siblings), alongside the reason* consts declared in
// engine.go, which TryAllShortestPaths/TryCypher continue to use unchanged.
const (
	// reasonNoKindConstraint fires when a recognize.NodeSpec (or RelSpec)
	// carries zero kind constraints -- including one that constrains only
	// by id(): a spec's kind-scoped staleness proof (see resolveNodeSpec's
	// doc) is built entirely from ConstraintKinds(), so a spec with no
	// constraints at all has nothing to check freshness against and is
	// declined outright rather than served on a freshness guarantee that
	// cannot be established. An id-only node query is expected to be rare
	// enough in practice (BloodHound's builder queries always pair id()
	// filters with a kind filter) that delegating it to PostgreSQL costs
	// little.
	reasonNoKindConstraint = "no_kind_constraint"

	// reasonKindStale fires when a spec's own constrained kinds are not
	// clean against the serving snapshot's generation (allNodesClean &&
	// nodeKindsClean, or their edge equivalents for Task 7): some write
	// landed, after the snapshot was built, that touched a kind this spec's
	// answer depends on.
	reasonKindStale = "kind_stale"
)

// Operation names for the builder-serving path's "op" attr, shared by both
// servedOp's success log line and declineOp's failure one.
const (
	opNodeCount = "node_count"
	opNodeIDs   = "node_ids"
	opNodeKinds = "node_kinds"
)

// declineOp is decline's builder-serving counterpart: TryNodeCount,
// TryNodeFetchIDs, and TryNodeFetchKinds (this file), and Task 7's
// rel-query siblings, each log one Debug "bloodtrail: builder engine
// declined" event carrying which operation declined (op) alongside the
// reason and, when available, the underlying error. This is a separate
// method from decline, not an added parameter to it, precisely so that
// TryAllShortestPaths/TryCypher's existing decline call sites -- and the
// "bloodtrail: path engine declined" message they log -- are left
// completely untouched: the two serving pipelines' declines stay
// distinguishable in the log by message alone, the same way their two
// "served" messages already are.
func (e *Engine) declineOp(ctx context.Context, op, reason string, err error) {
	attrs := make([]any, 0, 3)
	attrs = append(attrs, slog.String("op", op), slog.String("reason", reason))
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	e.cfg.Log.DebugContext(ctx, "bloodtrail: builder engine declined", attrs...)
}

// servedOp logs one Debug "bloodtrail: builder engine served" event -- the
// builder-serving path's counterpart to TryAllShortestPaths/TryCypher's Info
// "bloodtrail: path engine served" line, deliberately one level quieter
// (Debug, not Info): a structural node/relationship count or id/kind
// listing query is expected to run far more often than a shortest-path
// query, so logging every one at Info would be excessive volume for a
// comparatively low-value event. extra carries whatever per-op attrs the
// caller wants beyond op and duration (e.g. the resulting count), which
// every call logs.
func (e *Engine) servedOp(ctx context.Context, op string, start time.Time, extra ...any) {
	attrs := append([]any{slog.String("op", op), slog.Duration("duration", time.Since(start))}, extra...)
	e.cfg.Log.DebugContext(ctx, "bloodtrail: builder engine served", attrs...)
}

// serveGate is the first two checks shared by every builder-serving entry
// point: TryNodeCount/TryNodeFetchIDs/TryNodeFetchKinds here, and Task 7's
// rel-query siblings. It reports cfg.Enabled (reasonDisabled) and a non-nil
// current snapshot (reasonNoSnapshot) -- exactly the same two reasons, in
// the same order, as servePathQuery's own step 1.
//
// Freshness is deliberately NOT checked here, unlike servePathQuery's
// combined Fresh() call: whether a snapshot is fresh enough to serve a
// structural query depends on which specific kinds that query constrains
// (spec.ConstraintKinds()), which only the caller knows. A snapshot can be
// stale for one spec (its constrained kind was just written) while still
// perfectly servable for another (an unrelated kind elsewhere in the graph
// changed) -- collapsing that down to Fresh()'s single blanket bool would
// force declining specs that are, in fact, safe to serve. Every caller must
// therefore run its own kind-scoped freshness check (allNodesClean +
// nodeKindsClean, or the edge equivalents) after this gate passes and
// before executing.
//
// Design note on staleness, for this gate and every caller built on it:
// unlike servePathQuery, none of TryNodeCount/TryNodeFetchIDs/
// TryNodeFetchKinds re-validates the snapshot after computing its answer.
// servePathQuery's step-7 recheck exists because that pipeline's own work
// (graph traversal, then a PostgreSQL round trip to hydrate the resulting
// dense paths) can run long enough for a concurrent write to land
// mid-computation, and a result that mixes two generations would be
// silently wrong. The builder-serving path has no equivalent window: every
// value it returns -- a bitset built purely from NodesOfKind/KindOffsets/
// NodeKinds, a count of its set bits, the GraphIDs each set bit maps to --
// is derived exclusively from fields already frozen on the one immutable
// snapshot instance in hand, with no live external round trip in between
// that a concurrent write could invalidate partway through. A write that
// lands after the freshness check below can only ever be observed on the
// *next* call (via the marks it stamps), never corrupt the answer already
// in flight, so a post-execution recheck here would catch nothing a
// pre-execution one hasn't already ruled out.
func (e *Engine) serveGate(ctx context.Context, op string) (*snapshot.Snapshot, bool) {
	if !e.cfg.Enabled {
		e.declineOp(ctx, op, reasonDisabled, nil)
		return nil, false
	}

	snap := e.snap.Load()
	if snap == nil {
		e.declineOp(ctx, op, reasonNoSnapshot, nil)
		return nil, false
	}

	return snap, true
}

// resolveNodeSpec runs the shared gate (serveGate) and matching-set
// computation behind TryNodeCount, TryNodeFetchIDs, and TryNodeFetchKinds:
//
//  1. serveGate: cfg.Enabled, then a non-nil snapshot.
//  2. spec.Constraints non-empty (reasonNoKindConstraint otherwise).
//  3. Every constraint's kinds mapped to a KindID via e.mapKind
//     (reasonError on the first failure -- consistent with milestone 2's
//     MapKind stance in resolveKindsEndpoint/buildKindMask: a mapping
//     failure is ambiguous between "kind genuinely doesn't exist" and "the
//     lookup itself failed", so it is never silently treated as "matches
//     nothing").
//  4. spec.ConstraintKinds()' freshness against snap.Generation
//     (allNodesClean && nodeKindsClean; reasonKindStale otherwise) -- run
//     only after kind mapping succeeds, so a spec naming an unknown kind is
//     always declined reasonError rather than reasonKindStale, even if the
//     snapshot also happens to be stale.
//  5. The matching dense-NodeID bitset itself: each KindConstraint's
//     any-of union or all-of intersection of NodesOfKind bitmaps
//     (matchConstraint), every constraint's result intersected with the
//     next (intersectBitmaps), and -- if spec.IDs is non-nil -- intersected
//     again with the Dense-mapped id set (denseIDBitmap): a non-nil
//     zero-length IDs slice is a well-formed "matches nothing" spec (e.g.
//     id(n)=1 AND id(n)=2); nil IDs means unconstrained. An id absent from
//     the snapshot drops silently, the same "narrow towards zero matches"
//     contract resolveIDEndpoint already documents for the path-query side.
//
// op names the specific entry point, threaded through to serveGate/
// declineOp so every decline's "op" attr says which of TryNodeCount/
// TryNodeFetchIDs/TryNodeFetchKinds (or a Task 7 rel-query sibling) made the
// call.
func (e *Engine) resolveNodeSpec(ctx context.Context, op string, spec recognize.NodeSpec) (*snapshot.Bitset, *snapshot.Snapshot, bool) {
	snap, ok := e.serveGate(ctx, op)
	if !ok {
		return nil, nil, false
	}

	if len(spec.Constraints) == 0 {
		e.declineOp(ctx, op, reasonNoKindConstraint, nil)
		return nil, nil, false
	}

	constraintBitmaps := make([]*snapshot.Bitset, 0, len(spec.Constraints))
	for _, constraint := range spec.Constraints {
		bm, err := matchConstraint(ctx, e.mapKind, snap, constraint)
		if err != nil {
			e.declineOp(ctx, op, reasonError, err)
			return nil, nil, false
		}
		constraintBitmaps = append(constraintBitmaps, bm)
	}

	if !e.allNodesClean(snap.Generation) || !e.nodeKindsClean(snap.Generation, spec.ConstraintKinds()) {
		e.declineOp(ctx, op, reasonKindStale, nil)
		return nil, nil, false
	}

	matches := constraintBitmaps[0]
	if len(constraintBitmaps) > 1 {
		matches = intersectBitmaps(snap.NodeCount(), constraintBitmaps)
	}

	if spec.IDs != nil {
		matches = intersectBitmaps(snap.NodeCount(), []*snapshot.Bitset{matches, denseIDBitmap(snap, spec.IDs)})
	}

	return matches, snap, true
}

// matchConstraint maps every kind in constraint.Kinds to its KindID via
// mapKind and combines their NodesOfKind bitmaps according to constraint.
// AllOf: false (any-of, e.g. a KindMatcher built from query.KindIn) unions
// them -- "carries at least one of these kinds" -- while true (all-of, e.g.
// a Cypher `:A:B` label list) intersects them -- "carries every one of
// these kinds" -- matching recognize.KindConstraint's own doc. A KindID
// that maps successfully but has no bitmap of its own (NodesOfKind returns
// an empty bitset for it) folds in as "matches nothing" exactly like every
// other bitmap, correctly narrowing an any-of union or collapsing an all-of
// intersection to empty -- both consistent with what the same kind filter
// would find in PostgreSQL.
func matchConstraint(ctx context.Context, mapKind func(context.Context, graph.Kind) (int16, error), snap *snapshot.Snapshot, constraint recognize.KindConstraint) (*snapshot.Bitset, error) {
	bitmaps := make([]*snapshot.Bitset, 0, len(constraint.Kinds))
	for _, kind := range constraint.Kinds {
		kindID, err := mapKind(ctx, kind)
		if err != nil {
			return nil, fmt.Errorf("engine: matchConstraint: map kind %s: %w", kind, err)
		}
		bitmaps = append(bitmaps, snap.NodesOfKind(kindID))
	}

	switch {
	case len(bitmaps) == 0:
		// recognize.FromNodeCriteria/FromRelCriteria never produce a
		// KindConstraint with empty Kinds (see KindConstraint's doc), but a
		// hand-built NodeSpec could; treat it the same way an empty-Kinds
		// KindMatcher would have been rejected upstream -- matches nothing,
		// rather than panicking on bitmaps[0] below.
		return snapshot.NewBitset(snap.NodeCount()), nil
	case len(bitmaps) == 1:
		return bitmaps[0], nil
	case constraint.AllOf:
		return intersectBitmaps(snap.NodeCount(), bitmaps), nil
	default:
		return unionBitmaps(snap.NodeCount(), bitmaps), nil
	}
}

// denseIDBitmap builds a bitset of the dense NodeIDs corresponding to ids,
// dropping any id absent from snap -- the same "narrow towards zero
// matches" contract resolveIDEndpoint documents for the path-query side,
// adapted to a Bitset (rather than a sorted slice) since the caller is
// about to intersect it with kind-constraint bitmaps.
func denseIDBitmap(snap *snapshot.Snapshot, ids []graph.ID) *snapshot.Bitset {
	bm := snapshot.NewBitset(snap.NodeCount())
	for _, id := range ids {
		if dense, ok := snap.Dense(uint64(id)); ok {
			bm.Set(dense)
		}
	}
	return bm
}

// unionBitmaps returns a fresh Bitset (sized for n dense NodeIDs) holding
// the union of every bitmap in bitmaps -- matchConstraint's any-of case,
// where a node need only carry one of several kinds to match. Bitset.words
// is unexported outside the snapshot package, so there is no word-level OR
// available here the way intersectBitmaps' smallest-bitmap optimization
// might suggest; this iterates every input bitmap's set bits once via the
// public Iterate/Set, so cost is proportional to the total number of set
// bits across all of them.
func unionBitmaps(n int, bitmaps []*snapshot.Bitset) *snapshot.Bitset {
	result := snapshot.NewBitset(n)
	for _, bm := range bitmaps {
		bm.Iterate(func(id snapshot.NodeID) bool {
			result.Set(id)
			return true
		})
	}
	return result
}

// TryNodeCount attempts to serve spec's matching node count entirely from
// the engine's current snapshot, returning (count, true) on success. It
// returns (0, false) whenever the engine cannot, or chooses not to, serve
// the query itself (see resolveNodeSpec's doc for the full gate), in which
// case the caller must delegate to PostgreSQL.
//
// Execution runs inline, with no feeder goroutine: counting a bitset's set
// bits is already O(words), via Bitset.Count, so there is nothing to stream.
func (e *Engine) TryNodeCount(ctx context.Context, spec recognize.NodeSpec) (int64, bool) {
	start := time.Now()

	matches, _, ok := e.resolveNodeSpec(ctx, opNodeCount, spec)
	if !ok {
		return 0, false
	}

	count := int64(matches.Count())
	e.servedOp(ctx, opNodeCount, start, slog.Int64("count", count))
	return count, true
}

// TryNodeFetchIDs attempts to serve spec's matching node ids entirely from
// the engine's current snapshot, returning (cursor, true) on success. It
// returns (nil, false) under the same conditions as TryNodeCount (see
// resolveNodeSpec's doc).
//
// The matching bitset is fully computed (resolveNodeSpec has already run to
// completion) before this returns; the cursor it hands back streams
// already-known, error-free values through newFeedCursor exactly the way
// pathCursor streams an already-computed graph.PathSet -- there is nothing
// left that can fail mid-drain.
//
// Values are emitted in ascending dense-NodeID order via Bitset.Iterate,
// mapped through GraphIDs back to database ids -- equivalently, ascending
// database-id order, since dense NodeIDs are assigned in ascending
// database-id order at snapshot build time (snapshot.Builder's doc).
// PostgreSQL's own FetchNodeIDsByKind gives no ordering guarantee at all,
// so this is a superset guarantee over what a caller can already rely on;
// differential tests comparing engine output against PostgreSQL compare as
// sets, not sequences.
func (e *Engine) TryNodeFetchIDs(ctx context.Context, spec recognize.NodeSpec) (graph.Cursor[graph.ID], bool) {
	start := time.Now()

	matches, snap, ok := e.resolveNodeSpec(ctx, opNodeIDs, spec)
	if !ok {
		return nil, false
	}

	count := matches.Count()
	cursor := newFeedCursor(ctx, func(yield func(graph.ID) bool) {
		matches.Iterate(func(dense snapshot.NodeID) bool {
			return yield(graph.ID(snap.GraphIDs[dense]))
		})
	})

	e.servedOp(ctx, opNodeIDs, start, slog.Int("count", count))
	return cursor, true
}

// TryNodeFetchKinds attempts to serve spec's matching nodes' ids and kinds
// entirely from the engine's current snapshot, returning (cursor, true) on
// success. It returns (nil, false) under the same conditions as
// TryNodeCount (see resolveNodeSpec's doc), plus one more: failing to
// resolve the KindIDs actually carried by the matching nodes back to their
// graph.Kind names declines reasonError.
//
// Every distinct KindID carried by any matching node is resolved to its
// graph.Kind name via one batched e.mapKindNames call (resolveMatchingKindNames),
// made eagerly -- before the cursor is even constructed, not lazily as it is
// drained. This keeps the same guarantee TryNodeFetchIDs already has: a
// caller that gets (cursor, true) back is holding a fully-determined,
// already error-free answer, with no possibility of a resolution failure
// surfacing mid-stream that the caller would have no way to distinguish
// from a legitimately exhausted cursor.
func (e *Engine) TryNodeFetchKinds(ctx context.Context, spec recognize.NodeSpec) (graph.Cursor[graph.KindsResult], bool) {
	start := time.Now()

	matches, snap, ok := e.resolveNodeSpec(ctx, opNodeKinds, spec)
	if !ok {
		return nil, false
	}

	kindNames, err := resolveMatchingKindNames(snap, matches, e.mapKindNames)
	if err != nil {
		e.declineOp(ctx, opNodeKinds, reasonError, err)
		return nil, false
	}

	count := matches.Count()
	cursor := newFeedCursor(ctx, func(yield func(graph.KindsResult) bool) {
		matches.Iterate(func(dense snapshot.NodeID) bool {
			lo, hi := snap.KindOffsets[dense], snap.KindOffsets[dense+1]
			kinds := make(graph.Kinds, 0, hi-lo)
			for _, kindID := range snap.NodeKinds[lo:hi] {
				kinds = append(kinds, kindNames[kindID])
			}
			return yield(graph.KindsResult{ID: graph.ID(snap.GraphIDs[dense]), Kinds: kinds})
		})
	})

	e.servedOp(ctx, opNodeKinds, start, slog.Int("count", count))
	return cursor, true
}

// resolveMatchingKindNames collects every distinct snapshot.KindID carried
// by any node set in matches (via snap.KindOffsets/NodeKinds) and resolves
// them all in a single batched call to resolve (e.mapKindNames in
// production, wrapping e.kindNamesByID), returning a map from KindID to its
// resolved graph.Kind name that TryNodeFetchKinds' cursor can then index per
// node with no further resolution or error handling once streaming begins.
//
// Resolving up front, once, for the distinct set rather than once per node
// (or once per node's kind list) both bounds the number of resolve calls to
// one regardless of how many nodes match, and is what lets
// TryNodeFetchKinds decline reasonError before starting to emit instead of
// discovering a resolution failure mid-stream.
func resolveMatchingKindNames(snap *snapshot.Snapshot, matches *snapshot.Bitset, resolve func([]snapshot.KindID) (graph.Kinds, error)) (map[snapshot.KindID]graph.Kind, error) {
	seen := make(map[snapshot.KindID]struct{})
	var ids []snapshot.KindID

	matches.Iterate(func(dense snapshot.NodeID) bool {
		lo, hi := snap.KindOffsets[dense], snap.KindOffsets[dense+1]
		for _, kindID := range snap.NodeKinds[lo:hi] {
			if _, ok := seen[kindID]; !ok {
				seen[kindID] = struct{}{}
				ids = append(ids, kindID)
			}
		}
		return true
	})

	if len(ids) == 0 {
		return map[snapshot.KindID]graph.Kind{}, nil
	}

	kinds, err := resolve(ids)
	if err != nil {
		return nil, fmt.Errorf("engine: resolveMatchingKindNames: %w", err)
	}
	if len(kinds) != len(ids) {
		return nil, fmt.Errorf("engine: resolveMatchingKindNames: resolve returned %d kinds for %d ids", len(kinds), len(ids))
	}

	byID := make(map[snapshot.KindID]graph.Kind, len(ids))
	for i, id := range ids {
		byID[id] = kinds[i]
	}
	return byID, nil
}
