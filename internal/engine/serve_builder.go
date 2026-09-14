// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Decline reasons specific to the builder-serving path (this file and Task
// 7's rel-query siblings), alongside the reason* consts declared in
// engine.go, which TryAllShortestPaths/TryCypher continue to use unchanged.
const (
	// reasonNoKindConstraint fires when a recognize.NodeSpec carries zero
	// kind constraints -- including one that constrains only by id().
	// resolveNodeSpec builds its match set by intersecting one bitmap per
	// kind constraint, so a spec naming none has no bitmap to start from at
	// all (resolveConstraintBitmaps answers "unconstrained", which is not a
	// match set): it is declined outright rather than given a fabricated
	// everything-matches bitmap. An id-only node query is expected to be
	// rare enough in practice (BloodHound's builder queries always pair
	// id() filters with a kind filter) that delegating it to PostgreSQL
	// costs little.
	//
	// recognize.RelSpec has no equivalent minimum (see resolveRelSpec's
	// doc): a relationship scan walks the adjacency itself, with the kind
	// mask and endpoint bitmaps as optional filters, so a fully
	// unconstrained RelSpec is a well-defined full scan rather than a
	// missing match set.
	reasonNoKindConstraint = "no_kind_constraint"

	// reasonUnsupportedOrder is TryRelQueryRows-only: orderByEdgeID was
	// requested but spec anchors neither endpoint (spec.StartIDs and
	// spec.EndIDs both nil), so there is no anchored scan to gather and sort
	// -- see resolveRelSpec/TryRelQueryRows' doc for why ordering is only
	// offered for an anchored scan (a full scan's match set can be the
	// entire edge set, which upstream never asks to sort this way; see
	// recognize.OrderIsEdgeIDAscending's callers).
	reasonUnsupportedOrder = "unsupported_order"

	// reasonProjectionMismatch is TryRelQueryRows-only: proj names a
	// direction (recognize.ProjectionStepOutbound/StepInbound) whose
	// anchored side isn't actually anchored by spec -- ProjectionStepOutbound
	// requires spec.StartIDs non-nil (the far/varying side is the end), and
	// ProjectionStepInbound requires spec.EndIDs non-nil (the far side is
	// the start). Both real upstream callers (shallowFetchRelationships)
	// only ever build a step projection from a single known segment.Node,
	// which always compiles to exactly one endpoint's id() being fixed, so
	// this should never fire against a genuinely upstream-shaped query --
	// it exists as a defensive decline for a RelSpec that doesn't match that
	// assumption, rather than emitting rows keyed off the wrong endpoint.
	reasonProjectionMismatch = "projection_mismatch"
)

// Operation names for the builder-serving path's "op" attr, shared by both
// servedOp's success log line and declineOp's failure one.
const (
	opNodeCount  = "node_count"
	opNodeIDs    = "node_ids"
	opNodeKinds  = "node_kinds"
	opRelCount   = "rel_count"
	opRelIDs     = "rel_ids"
	opRelTriples = "rel_triples"
	opRelKinds   = "rel_kinds"
	opRelRows    = "rel_rows"
)

// declineOp is decline's builder-serving counterpart: TryNodeCount,
// TryNodeFetchIDs, and TryNodeFetchKinds (this file), and the
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
// builder-serving path's counterpart to TryAllShortestPaths' Info
// "bloodtrail: path engine served" line, deliberately one level quieter
// (Debug, not Info): a structural node/relationship count or id/kind
// listing query is expected to run far more often than a shortest-path
// query, so logging every one at Info would be excessive volume for a
// comparatively low-value event. TryCypher's own served line
// (cypherServedLogMessage, engine.go) is Debug for the identical reason,
// not Info -- the two Debug-level lines are peers, not one mirroring the
// other's level. extra carries whatever per-op attrs the caller wants
// beyond op and duration (e.g. the resulting count), which every call logs.
func (e *Engine) servedOp(ctx context.Context, op string, start time.Time, extra ...any) {
	attrs := append([]any{slog.String("op", op), slog.Duration("duration", time.Since(start))}, extra...)
	e.cfg.Log.DebugContext(ctx, "bloodtrail: builder engine served", attrs...)
}

// serveGate is the gate shared by every builder-serving entry point:
// TryNodeCount/TryNodeFetchIDs/TryNodeFetchKinds here, and the rel-query
// siblings below. It reports cfg.Enabled (reasonDisabled), a non-nil current
// View (reasonNoSnapshot), and the engine being in stateServing rather than
// fallback (reasonFallback) -- exactly the same three reasons, in the same
// order, as servePathQuery's own step 1, via the same serveState helper.
//
// No freshness check follows it in any caller, and none is needed: with
// write-through (apply.go), the View this returns already reflects every
// write committed before this call -- each one published its own delta
// before its writing call returned -- so there is no "is this snapshot
// behind" question left to ask, per spec or otherwise. The kind-scoped
// marks gates every caller used to run after this gate are gone with it.
//
// Nor does any caller re-validate after computing its answer: every value
// the builder-serving path returns -- a bitset built from NodesOfKind/
// KindIDsOf, a count of its set bits, the GraphIDs each set bit maps to --
// is derived exclusively from the one immutable View in hand, so a write
// landing mid-computation publishes a new View for the NEXT query rather
// than corrupting this one.
func (e *Engine) serveGate(ctx context.Context, op string) (*snapshot.View, bool) {
	if !e.cfg.Enabled {
		e.declineOp(ctx, op, reasonDisabled, nil)
		return nil, false
	}

	snap, serving := e.serveState()
	if snap == nil {
		e.declineOp(ctx, op, reasonNoSnapshot, nil)
		return nil, false
	}
	if !serving {
		e.declineOp(ctx, op, reasonFallback, nil)
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
//  4. The matching dense-NodeID bitset itself: each KindConstraint's
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
// TryNodeFetchIDs/TryNodeFetchKinds (or a rel-query sibling) made the
// call.
func (e *Engine) resolveNodeSpec(ctx context.Context, op string, spec recognize.NodeSpec) (*snapshot.Bitset, *snapshot.View, bool) {
	snap, ok := e.serveGate(ctx, op)
	if !ok {
		return nil, nil, false
	}

	if len(spec.Constraints) == 0 {
		e.declineOp(ctx, op, reasonNoKindConstraint, nil)
		return nil, nil, false
	}

	// spec.Constraints is already known non-empty (checked above), so
	// resolveConstraintBitmaps' nil-means-unconstrained result never applies
	// here -- matches is always a real bitmap, never "everything passes".
	matches, err := e.resolveConstraintBitmaps(ctx, snap, spec.Constraints)
	if err != nil {
		e.declineOp(ctx, op, reasonError, err)
		return nil, nil, false
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
func matchConstraint(ctx context.Context, mapKind func(context.Context, graph.Kind) (int16, error), snap *snapshot.View, constraint recognize.KindConstraint) (*snapshot.Bitset, error) {
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

// resolveConstraintBitmaps maps every recognize.KindConstraint in constraints
// to its own bitmap via matchConstraint and intersects them together --
// Cypher's AND semantics for multiple conjuncts naming the same variable
// (resolveNodeSpec's step 5 doc, generalized). It is the shared
// constraint-resolution step behind resolveNodeSpec (one endpoint:
// the bare node variable "n") and resolveRelSpec (two independent
// endpoints: "s" and "e", each calling this once with its own Start/
// EndConstraints).
//
// A nil result (with nil error) means constraints itself was empty -- "this
// endpoint carries no kind constraint at all". Every caller must treat that
// as "everything passes" when combining it with other filters, never as
// "matches nothing" the way an actual zero-bit bitmap would; resolveNodeSpec
// never sees this case (it declines reasonNoKindConstraint before calling in
// first place when spec.Constraints is empty), but resolveRelSpec relies on
// it directly, since RelSpec's Start/EndConstraints are allowed to be empty.
func (e *Engine) resolveConstraintBitmaps(ctx context.Context, snap *snapshot.View, constraints []recognize.KindConstraint) (*snapshot.Bitset, error) {
	if len(constraints) == 0 {
		return nil, nil
	}

	bitmaps := make([]*snapshot.Bitset, 0, len(constraints))
	for _, constraint := range constraints {
		bm, err := matchConstraint(ctx, e.mapKind, snap, constraint)
		if err != nil {
			return nil, err
		}
		bitmaps = append(bitmaps, bm)
	}

	if len(bitmaps) == 1 {
		return bitmaps[0], nil
	}
	return intersectBitmaps(snap.NodeCount(), bitmaps), nil
}

// denseIDBitmap builds a bitset of the dense NodeIDs corresponding to ids,
// dropping any id absent from snap -- the same "narrow towards zero
// matches" contract resolveIDEndpoint documents for the path-query side,
// adapted to a Bitset (rather than a sorted slice) since the caller is
// about to intersect it with kind-constraint bitmaps.
func denseIDBitmap(snap *snapshot.View, ids []graph.ID) *snapshot.Bitset {
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
			return yield(graph.ID(snap.GraphID(dense)))
		})
	})

	e.servedOp(ctx, opNodeIDs, start, slog.Int("count", count))
	return cursor, true
}

// TryNodeFetchKinds attempts to serve spec's matching nodes' ids and kinds
// entirely from the engine's current snapshot, returning (cursor, true) on
// success. It returns (nil, false) under the same conditions as
// TryNodeCount (see resolveNodeSpec's doc), plus two more:
//
//   - failing to resolve the KindIDs actually carried by the matching nodes
//     back to their graph.Kind names declines reasonError.
//
// It once additionally required every node kind in the whole snapshot to be
// mark-clean, because a kind LISTING exposes kinds the spec never constrains
// by, and a write touching one of those would have gone uncaught by
// resolveNodeSpec's own spec-scoped freshness check. Write-through removes
// the premise entirely: the View this serves from already carries every
// committed kind change, named by the spec or not.
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

	kindNames, err := resolveMatchingKindNames(ctx, snap, matches, e.mapKindNames)
	if err != nil {
		e.declineOp(ctx, opNodeKinds, reasonError, err)
		return nil, false
	}

	count := matches.Count()
	cursor := newFeedCursor(ctx, func(yield func(graph.KindsResult) bool) {
		matches.Iterate(func(dense snapshot.NodeID) bool {
			nodeKindIDs := snap.KindIDsOf(dense)
			kinds := make(graph.Kinds, 0, len(nodeKindIDs))
			for _, kindID := range nodeKindIDs {
				kinds = append(kinds, kindNames[kindID])
			}
			return yield(graph.KindsResult{ID: graph.ID(snap.GraphID(dense)), Kinds: kinds})
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
func resolveMatchingKindNames(ctx context.Context, snap *snapshot.View, matches *snapshot.Bitset, resolve func(context.Context, []snapshot.KindID) (graph.Kinds, error)) (map[snapshot.KindID]graph.Kind, error) {
	seen := make(map[snapshot.KindID]struct{})
	var ids []snapshot.KindID

	matches.Iterate(func(dense snapshot.NodeID) bool {
		for _, kindID := range snap.KindIDsOf(dense) {
			if _, ok := seen[kindID]; !ok {
				seen[kindID] = struct{}{}
				ids = append(ids, kindID)
			}
		}
		return true
	})

	return resolveKindNameMap(ctx, ids, resolve)
}

// resolveKindNameMap is resolveMatchingKindNames'/TryRelFetchKinds'/
// TryRelQueryRows' shared resolve+validate+map-build tail: it calls resolve
// once with the full ids batch, checks the result is the same length back
// (a length mismatch means resolve itself is misbehaving -- not a case any
// production KindMapper is expected to hit, but cheap to guard against
// rather than silently misaligning ids[i] with kinds[j]), and builds the
// KindID->graph.Kind map every caller then indexes per row with no further
// resolution or error handling once streaming begins.
//
// A nil or empty ids returns an empty, non-nil map without calling resolve
// at all -- there is nothing to look up, and a production KindMapper is
// under no obligation to handle an empty batch gracefully.
func resolveKindNameMap(ctx context.Context, ids []snapshot.KindID, resolve func(context.Context, []snapshot.KindID) (graph.Kinds, error)) (map[snapshot.KindID]graph.Kind, error) {
	if len(ids) == 0 {
		return map[snapshot.KindID]graph.Kind{}, nil
	}

	kinds, err := resolve(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("engine: resolveKindNameMap: %w", err)
	}
	if len(kinds) != len(ids) {
		return nil, fmt.Errorf("engine: resolveKindNameMap: resolve returned %d kinds for %d ids", len(kinds), len(ids))
	}

	byID := make(map[snapshot.KindID]graph.Kind, len(ids))
	for i, id := range ids {
		byID[id] = kinds[i]
	}
	return byID, nil
}

// ---------------------------------------------------------------------------
// Relationship-query serving.
//
// TryRelCount, TryRelFetchIDs, TryRelFetchTriples, TryRelFetchKinds, and
// TryRelQueryRows all serve a recognize.RelSpec entirely from the current
// View's adjacency, sharing one gate (resolveRelSpec) and one scan
// (relScanIter) the same way TryNodeCount/TryNodeFetchIDs/TryNodeFetchKinds
// share resolveNodeSpec above. Unlike the node-spec side, a RelSpec needs no
// constraint minimum: a fully unconstrained relationship query (nil
// StartIDs/EndIDs, empty EdgeKinds, no endpoint kind constraints) is a
// well-defined full scan, so there is no analogue to resolveNodeSpec's
// reasonNoKindConstraint decline here.
// ---------------------------------------------------------------------------

// relEdge is one matching relationship produced by relScanIter/sliceRelIter:
// dense start/end NodeIDs, the relationship's own database id, and its
// KindID -- everything every TryRel* entry point needs to render its own
// row shape (a bare id, a triple, a kind-annotated triple, or a
// recognize.RowProjection row) without re-deriving anything from the
// snapshot's CSR arrays a second time.
type relEdge struct {
	start, end snapshot.NodeID
	edgeID     uint64
	kind       snapshot.KindID
}

// relIterator is the pull-style contract every TryRel* entry point drains,
// implemented by relScanIter (a live scan over the snapshot's CSR arrays)
// and sliceRelIter (a pre-sorted []relEdge, for TryRelQueryRows'
// orderByEdgeID case). next returns ok=false once exhausted, exactly like a
// Go 1.23 range-over-func iterator's pull adapter, but written out by hand
// here since rowResult (rowresult.go) needs to drive it from its own Next()
// method with no goroutine in between.
type relIterator interface {
	next() (relEdge, bool)
}

// relPlan is resolveRelSpec's result: everything a relIterator needs to
// walk the snapshot's edges according to spec, computed once, after the
// freshness gate has already passed. Every TryRel* entry point builds
// exactly one relIterator from a relPlan via newRelScanIter.
type relPlan struct {
	snap *snapshot.View

	// kindMask is spec.EdgeKinds mapped to a snapshot.KindMask via
	// buildKindMaskSeam -- SetAll when spec.EdgeKinds is empty ("every kind
	// allowed"), matching buildKindMask's own contract on the path-query
	// side (engine.go).
	kindMask *snapshot.KindMask

	// startBits/endBits are spec.StartConstraints/EndConstraints resolved
	// via resolveConstraintBitmaps: nil means that endpoint carries no kind
	// constraint at all ("everything passes"), never "matches nothing".
	startBits, endBits *snapshot.Bitset

	// startAnchor/endAnchor are non-nil iff spec.StartIDs/EndIDs is non-nil
	// -- the Dense-mapped id() bitmap that endpoint's id constraint narrows
	// the scan to (denseIDBitmap), which may itself be the empty bitset
	// ("matches nothing", from a non-nil empty StartIDs/EndIDs), but is
	// never nil unless the corresponding spec field itself was nil
	// ("unconstrained by id"). This nil-vs-empty distinction is exactly
	// RelSpec.StartIDs/EndIDs' own documented contract, preserved all the
	// way through to scan-strategy selection (newRelScanIter).
	startAnchor, endAnchor *snapshot.Bitset
}

// resolveRelSpec runs the shared gate and per-query setup behind every
// TryRel* entry point:
//
//  1. serveGate: cfg.Enabled, then a non-nil snapshot (same as
//     resolveNodeSpec's step 1).
//  2. spec.EdgeKinds mapped to a snapshot.KindMask via buildKindMaskSeam;
//     spec.StartConstraints and spec.EndConstraints each mapped to a bitmap
//     via resolveConstraintBitmaps (shared with resolveNodeSpec). Any
//     mapKind failure along the way declines reasonError, exactly like
//     resolveNodeSpec's step 3 -- an unmappable kind is never silently
//     treated as "matches nothing" (see matchConstraint's doc).
//  3. spec.StartIDs/EndIDs Dense-mapped into startAnchor/endAnchor via
//     denseIDBitmap, preserving the nil ("unconstrained")-vs-non-nil
//     ("anchored, possibly to zero ids") distinction (relPlan's own doc).
//
// op names the specific entry point, threaded through to serveGate/
// declineOp exactly like resolveNodeSpec's op parameter.
func (e *Engine) resolveRelSpec(ctx context.Context, op string, spec recognize.RelSpec) (*relPlan, bool) {
	snap, ok := e.serveGate(ctx, op)
	if !ok {
		return nil, false
	}

	kindMask, err := buildKindMaskSeam(ctx, e.mapKind, snap.MaxKindID(), spec.EdgeKinds)
	if err != nil {
		e.declineOp(ctx, op, reasonError, err)
		return nil, false
	}

	startBits, err := e.resolveConstraintBitmaps(ctx, snap, spec.StartConstraints)
	if err != nil {
		e.declineOp(ctx, op, reasonError, err)
		return nil, false
	}
	endBits, err := e.resolveConstraintBitmaps(ctx, snap, spec.EndConstraints)
	if err != nil {
		e.declineOp(ctx, op, reasonError, err)
		return nil, false
	}

	var startAnchor, endAnchor *snapshot.Bitset
	if spec.StartIDs != nil {
		startAnchor = denseIDBitmap(snap, spec.StartIDs)
	}
	if spec.EndIDs != nil {
		endAnchor = denseIDBitmap(snap, spec.EndIDs)
	}

	return &relPlan{
		snap:        snap,
		kindMask:    kindMask,
		startBits:   startBits,
		endBits:     endBits,
		startAnchor: startAnchor,
		endAnchor:   endAnchor,
	}, true
}

// buildKindMaskSeam is buildKindMask (engine.go) adapted to the mapKind seam
// (a plain function value) instead of a live pg.KindMapper, for the same
// reason resolveNodeSpec/matchConstraint go through e.mapKind rather than
// e.pgDriver.KindMapper() directly: it lets this file's unit tests fake kind
// resolution without a live PostgreSQL connection. Semantics are unchanged
// from buildKindMask: an empty edgeKinds means "every kind allowed"
// (SetAll); otherwise every kind must map successfully via mapKind, or the
// whole call fails -- servePathQuery's own buildKindMask call site is left
// untouched, exactly like every other seam this file introduces.
func buildKindMaskSeam(ctx context.Context, mapKind func(context.Context, graph.Kind) (int16, error), maxKindID snapshot.KindID, edgeKinds graph.Kinds) (*snapshot.KindMask, error) {
	mask := snapshot.NewKindMask(maxKindID)

	if len(edgeKinds) == 0 {
		mask.SetAll()
		return mask, nil
	}

	for _, kind := range edgeKinds {
		kindID, err := mapKind(ctx, kind)
		if err != nil {
			return nil, fmt.Errorf("engine: buildKindMaskSeam: map kind %s: %w", kind, err)
		}
		mask.Set(kindID)
	}

	return mask, nil
}

// relScanIter is the one live-scan relIterator every TryRel* entry point's
// default (non-ordered) path builds from a relPlan, via newRelScanIter. It
// unifies all three scan strategies resolveRelSpec's doc enumerates
// (start-anchored, end-anchored, and full scan) into a single walk over two
// nested cursors:
//
//   - An outer sequence of "near" dense node ids to visit, ascending: either
//     an anchor bitmap's own members (materialized once via materializeBitset
//     -- bounded by the anchor set's size, not the edge count), or, for a
//     full scan, implicitly every dense NodeID 0..NodeCount()-1 (outer nil,
//     nodeCount driving the loop directly instead, so a full scan never
//     materializes anything edge- or even node-count sized up front).
//   - For each near node, a forward (Out) or reverse (In) CSR segment,
//     walked slot by slot.
//
// forward selects Out-based iteration for a start-anchored or full scan (the
// near node is the start, the far node read off OutTargets is the end);
// false selects In-based iteration for an end-anchored scan (the near node
// is the end, the far node read off InTargets is the start, and the edge id
// is looked up one indirection away via InEdgeIdx into OutEdgeIDs, exactly
// as Snapshot.InEdgeIdx's own doc describes).
//
// nearBits is the near side's own kind-constraint bitmap, tested once per
// near node in advanceNear rather than once per slot -- correct because
// every edge in one near node's segment shares that same near node, so a
// per-node test is exactly equivalent to (but cheaper than) testing it on
// every one of that node's slots individually. farBits is the opposite
// side's kind-constraint bitmap, tested per slot in next, since the far
// node varies slot to slot. farAnchor is the opposite side's id() anchor
// bitmap (nil unless both endpoints are anchored, i.e. the "both anchors
// non-nil" case in resolveRelSpec's doc), also tested per slot. nearBits,
// farBits, and farAnchor are all nil-means-"unconstrained", never
// nil-means-"matches nothing" -- see relPlan's own doc for why.
type relScanIter struct {
	outer     []snapshot.NodeID // nil for a full scan
	nodeCount int               // used only when outer == nil
	outerPos  int               // next unread index into outer, or next full-scan node id

	forward bool
	snap    *snapshot.View

	kindMask  *snapshot.KindMask
	nearBits  *snapshot.Bitset
	farBits   *snapshot.Bitset
	farAnchor *snapshot.Bitset

	// curNear/slot/hi drive the !snap.Overlay() walk: a raw forward/reverse
	// CSR slot range over curNear's own segment. ovEdges/ovPos are their
	// Overlay() counterpart -- curNear's edges as yielded by OutEdges/
	// InEdges, since a delta edge (or a base slot the delta overrides) has
	// no CSR range to point slot/hi at -- see snapshot.View.OutEdges/InEdges'
	// own doc. advanceNear populates whichever pair applies once per near
	// node; next drains it exactly the way it always drained slot/hi.
	curNear  snapshot.NodeID
	slot, hi uint64

	ovEdges []relScanOverlayEdge
	ovPos   int
}

// relScanOverlayEdge is one (far, kind, edgeID) triple advanceNear buffers
// for curNear when snap.Overlay() -- see relScanIter's own doc.
type relScanOverlayEdge struct {
	far    snapshot.NodeID
	kind   snapshot.KindID
	edgeID uint64
}

// newRelScanIter builds the relScanIter for plan, choosing among the four
// scan strategies resolveRelSpec's doc documents:
//
//   - Both startAnchor and endAnchor non-nil: anchor on whichever bitmap has
//     fewer set bits (Bitset.Count()), membership-testing the other side per
//     slot via farAnchor -- the cheaper of the two anchored scans is always
//     preferred, since both would produce the same match set.
//   - Only startAnchor non-nil: start-anchored (forward/Out) scan.
//   - Only endAnchor non-nil: end-anchored (reverse/In) scan.
//   - Neither: full forward scan over every dense node id.
func newRelScanIter(plan *relPlan) *relScanIter {
	switch {
	case plan.startAnchor != nil && plan.endAnchor != nil:
		if plan.startAnchor.Count() <= plan.endAnchor.Count() {
			return &relScanIter{
				outer: materializeBitset(plan.startAnchor), forward: true, snap: plan.snap,
				kindMask: plan.kindMask, nearBits: plan.startBits, farBits: plan.endBits, farAnchor: plan.endAnchor,
			}
		}
		return &relScanIter{
			outer: materializeBitset(plan.endAnchor), forward: false, snap: plan.snap,
			kindMask: plan.kindMask, nearBits: plan.endBits, farBits: plan.startBits, farAnchor: plan.startAnchor,
		}

	case plan.startAnchor != nil:
		return &relScanIter{
			outer: materializeBitset(plan.startAnchor), forward: true, snap: plan.snap,
			kindMask: plan.kindMask, nearBits: plan.startBits, farBits: plan.endBits,
		}

	case plan.endAnchor != nil:
		return &relScanIter{
			outer: materializeBitset(plan.endAnchor), forward: false, snap: plan.snap,
			kindMask: plan.kindMask, nearBits: plan.endBits, farBits: plan.startBits,
		}

	default:
		return &relScanIter{
			nodeCount: plan.snap.NodeCount(), forward: true, snap: plan.snap,
			kindMask: plan.kindMask, nearBits: plan.startBits, farBits: plan.endBits,
		}
	}
}

// next advances the scan by one matching edge, applying kindMask, farAnchor,
// and farBits to every candidate (see relScanIter's doc for why nearBits is
// instead applied once per near node, in advanceNear). ok is false once
// every near node's edges have been exhausted.
func (it *relScanIter) next() (relEdge, bool) {
	for {
		if !it.snap.Overlay() {
			for it.slot < it.hi {
				slot := it.slot
				it.slot++

				var far snapshot.NodeID
				var kind snapshot.KindID
				var edgeID uint64
				if it.forward {
					far = it.snap.Base().OutTargets[slot]
					kind = it.snap.Base().OutKinds[slot]
					edgeID = it.snap.Base().OutEdgeIDs[slot]
				} else {
					far = it.snap.Base().InTargets[slot]
					kind = it.snap.Base().InKinds[slot]
					edgeID = it.snap.Base().OutEdgeIDs[it.snap.Base().InEdgeIdx[slot]]
				}

				if !it.kindMask.Has(kind) {
					continue
				}
				if it.farAnchor != nil && !it.farAnchor.Has(far) {
					continue
				}
				if it.farBits != nil && !it.farBits.Has(far) {
					continue
				}

				if it.forward {
					return relEdge{start: it.curNear, end: far, edgeID: edgeID, kind: kind}, true
				}
				return relEdge{start: far, end: it.curNear, edgeID: edgeID, kind: kind}, true
			}
		} else {
			for it.ovPos < len(it.ovEdges) {
				e := it.ovEdges[it.ovPos]
				it.ovPos++

				if !it.kindMask.Has(e.kind) {
					continue
				}
				if it.farAnchor != nil && !it.farAnchor.Has(e.far) {
					continue
				}
				if it.farBits != nil && !it.farBits.Has(e.far) {
					continue
				}

				if it.forward {
					return relEdge{start: it.curNear, end: e.far, edgeID: e.edgeID, kind: e.kind}, true
				}
				return relEdge{start: e.far, end: it.curNear, edgeID: e.edgeID, kind: e.kind}, true
			}
		}

		if !it.advanceNear() {
			return relEdge{}, false
		}
	}
}

// advanceNear moves to the next near node with a non-empty, nearBits-passing
// set of edges, setting curNear (and, per snap.Overlay(), either slot/hi or
// ovEdges/ovPos) to it and reporting true, or reports false once outer (or,
// for a full scan, 0..nodeCount-1) is exhausted.
//
// A near node this method visits under Overlay() is never explicitly
// Alive-checked here: OutEdges/InEdges themselves yield nothing at all for a
// non-Alive node (snapshot.View.OutEdges/InEdges's own doc), so a dead near
// node -- reached via outer (a stale id() anchor bitmap; denseIDBitmap does
// not itself filter liveness) or via the full-scan default (every dense id
// in [0, NodeCount()), tombstoned ones included) -- naturally produces an
// empty ovEdges and falls through to the same "continue to the next near
// node" branch a zero-degree node already takes.
func (it *relScanIter) advanceNear() bool {
	for {
		var node snapshot.NodeID
		if it.outer != nil {
			if it.outerPos >= len(it.outer) {
				return false
			}
			node = it.outer[it.outerPos]
			it.outerPos++
		} else {
			if it.outerPos >= it.nodeCount {
				return false
			}
			node = snapshot.NodeID(it.outerPos)
			it.outerPos++
		}

		if it.nearBits != nil && !it.nearBits.Has(node) {
			continue
		}

		if !it.snap.Overlay() {
			var lo, hi uint64
			if it.forward {
				lo, hi = it.snap.Base().OutOffsets[node], it.snap.Base().OutOffsets[node+1]
			} else {
				lo, hi = it.snap.Base().InOffsets[node], it.snap.Base().InOffsets[node+1]
			}
			if lo == hi {
				continue
			}

			it.curNear, it.slot, it.hi = node, lo, hi
			return true
		}

		it.ovEdges = it.ovEdges[:0]
		collect := func(far snapshot.NodeID, kind snapshot.KindID, edgeID uint64) bool {
			it.ovEdges = append(it.ovEdges, relScanOverlayEdge{far: far, kind: kind, edgeID: edgeID})
			return true
		}
		if it.forward {
			it.snap.OutEdges(node, collect)
		} else {
			it.snap.InEdges(node, collect)
		}
		if len(it.ovEdges) == 0 {
			continue
		}

		it.curNear, it.ovPos = node, 0
		return true
	}
}

// materializeBitset collects bm's set bits into an ascending []snapshot.
// NodeID slice, once -- the anchor-set materialization newRelScanIter uses
// to turn a Bitset's push-style Iterate into the ascending, resumable outer
// sequence relScanIter.advanceNear needs. Cost is bounded by the anchor
// set's own size (how many ids a query's id() constraint named), never by
// the snapshot's total node or edge count.
func materializeBitset(bm *snapshot.Bitset) []snapshot.NodeID {
	ids := make([]snapshot.NodeID, 0, bm.Count())
	bm.Iterate(func(id snapshot.NodeID) bool {
		ids = append(ids, id)
		return true
	})
	return ids
}

// sliceRelIter is relIterator's other implementation: a plain, already-
// materialized []relEdge walked in order. TryRelQueryRows builds one from
// drainRelIter's output, after sorting it ascending by edge id, to serve
// orderByEdgeID -- the one case where every TryRel* entry point's shared
// relScanIter isn't enough on its own, since sorting requires the full match
// set in hand before the first row can be emitted.
type sliceRelIter struct {
	edges []relEdge
	pos   int
}

func (it *sliceRelIter) next() (relEdge, bool) {
	if it.pos >= len(it.edges) {
		return relEdge{}, false
	}
	edge := it.edges[it.pos]
	it.pos++
	return edge, true
}

// drainRelIter pulls it to exhaustion into a plain []relEdge, in scan order.
// Used only by TryRelQueryRows' orderByEdgeID path (gather-then-sort-then-
// emit); every other TryRel* entry point streams relScanIter's output
// directly instead of gathering it first.
func drainRelIter(it relIterator) []relEdge {
	var edges []relEdge
	for {
		edge, ok := it.next()
		if !ok {
			return edges
		}
		edges = append(edges, edge)
	}
}

// selectKindIDs returns every KindID in [1, maxKindID] for which allow
// reports true, in ascending order -- a candidate list bounded by the
// schema's own (typically small) total kind count, never by how much data
// carries any given kind. TryRelFetchKinds passes kindMask.Has, resolving
// names only for the kinds spec.EdgeKinds actually allows; TryRelQueryRows'
// step projections pass an always-true predicate, since a far node's
// carried kinds are never filtered by spec.EdgeKinds (that mask constrains
// only the traversed relationship's own kind). Either way, resolving the
// whole allowed set eagerly, once, up front costs one bounded batch resolve
// call regardless of how many rows the scan itself goes on to produce --
// see TryRelFetchKinds' doc for why this is preferred over collecting the
// distinct kinds actually encountered mid-scan, which would need a second
// pass over the same data.
//
// The range starts at 1, not 0: dawgs' pg.SchemaManager/InMemoryKindMapper
// both hand out KindIDs starting at 1 (nextKindID: int16(1)) and never
// assign 0 to any real kind, so 0 can never appear in a snapshot's OutKinds/
// InKinds/NodeKinds either. A snapshot.KindMask's SetAll (buildKindMaskSeam,
// for an empty spec.EdgeKinds) sets bit 0 anyway -- it is a plain 0-based bit
// vector with no notion of which indices are real KindIDs -- so this
// function, not kindMask.Has, is what keeps kind id 0 out of the batch and
// out of a real e.mapKindNames call, which would otherwise fail resolving an
// id no KindMapper ever assigned.
func selectKindIDs(maxKindID snapshot.KindID, allow func(snapshot.KindID) bool) []snapshot.KindID {
	var ids []snapshot.KindID
	for k := snapshot.KindID(1); k <= maxKindID; k++ {
		if allow(k) {
			ids = append(ids, k)
		}
	}
	return ids
}

// TryRelCount attempts to serve spec's matching relationship count entirely
// from the engine's current snapshot, returning (count, true) on success. It
// returns (0, false) whenever the engine cannot, or chooses not to, serve
// the query itself (see resolveRelSpec's doc for the full gate).
//
// Unlike TryNodeCount (a Bitset.Count() over an already-computed bitmap, so
// O(words)), there is no equivalent precomputed structure to count for a
// relationship query: relScanIter must actually visit every candidate slot
// the scan strategy selects, so this runs in time proportional to the
// matching (or anchor-adjacent, for an anchored scan) edges, not to the
// answer alone. It still runs inline, with no feeder goroutine -- there is
// nothing to stream, only a running total.
func (e *Engine) TryRelCount(ctx context.Context, spec recognize.RelSpec) (int64, bool) {
	start := time.Now()

	plan, ok := e.resolveRelSpec(ctx, opRelCount, spec)
	if !ok {
		return 0, false
	}

	it := newRelScanIter(plan)
	var count int64
	for {
		if _, ok := it.next(); !ok {
			break
		}
		count++
	}

	e.servedOp(ctx, opRelCount, start, slog.Int64("count", count))
	return count, true
}

// TryRelFetchIDs attempts to serve spec's matching relationships' own
// database ids entirely from the engine's current snapshot, returning
// (cursor, true) on success. It returns (nil, false) under the same
// conditions as TryRelCount (see resolveRelSpec's doc).
//
// Unlike TryNodeFetchIDs (which fully computes its bitset before streaming,
// since a Bitset is cheap to materialize in full), this streams relScanIter's
// output directly through newFeedCursor's feeder goroutine as the scan
// itself runs: gathering every matching edge into a slice first, purely to
// mirror TryNodeFetchIDs' "answer fully known before the cursor exists"
// shape, would cost an extra full materialization for no benefit here, since
// nothing about rendering a bare edge id can fail mid-scan the way
// TryRelFetchKinds' kind-name resolution can. Emission order is scan order,
// documented as unspecified (see relScanIter's doc) except when the caller
// needs otherwise, which is what TryRelQueryRows' orderByEdgeID is for.
func (e *Engine) TryRelFetchIDs(ctx context.Context, spec recognize.RelSpec) (graph.Cursor[graph.ID], bool) {
	start := time.Now()

	plan, ok := e.resolveRelSpec(ctx, opRelIDs, spec)
	if !ok {
		return nil, false
	}

	it := newRelScanIter(plan)
	cursor := newFeedCursor(ctx, func(yield func(graph.ID) bool) {
		for {
			edge, ok := it.next()
			if !ok {
				return
			}
			if !yield(graph.ID(edge.edgeID)) {
				return
			}
		}
	})

	e.servedOp(ctx, opRelIDs, start)
	return cursor, true
}

// TryRelFetchTriples attempts to serve spec's matching relationships' own
// id plus their endpoints' database ids (graph.RelationshipTripleResult)
// entirely from the engine's current snapshot, returning (cursor, true) on
// success. It returns (nil, false) under the same conditions as TryRelCount.
// See TryRelFetchIDs' doc for why this streams directly rather than fully
// materializing first.
func (e *Engine) TryRelFetchTriples(ctx context.Context, spec recognize.RelSpec) (graph.Cursor[graph.RelationshipTripleResult], bool) {
	start := time.Now()

	plan, ok := e.resolveRelSpec(ctx, opRelTriples, spec)
	if !ok {
		return nil, false
	}

	snap := plan.snap
	it := newRelScanIter(plan)
	cursor := newFeedCursor(ctx, func(yield func(graph.RelationshipTripleResult) bool) {
		for {
			edge, ok := it.next()
			if !ok {
				return
			}
			row := graph.RelationshipTripleResult{
				ID:      graph.ID(edge.edgeID),
				StartID: graph.ID(snap.GraphID(edge.start)),
				EndID:   graph.ID(snap.GraphID(edge.end)),
			}
			if !yield(row) {
				return
			}
		}
	})

	e.servedOp(ctx, opRelTriples, start)
	return cursor, true
}

// TryRelFetchKinds attempts to serve spec's matching relationships' triples
// plus each one's own graph.Kind entirely from the engine's current
// snapshot, returning (cursor, true) on success. It returns (nil, false)
// under the same conditions as TryRelCount, plus one more: failing to
// resolve the KindIDs spec.EdgeKinds' mask allows back to their graph.Kind
// names declines reasonError.
//
// Every KindID plan.kindMask allows (selectKindIDs(plan.snap.MaxKindID,
// plan.kindMask.Has)) is resolved to its graph.Kind name via one batched
// e.mapKindNames call, made eagerly before the cursor is even constructed --
// not lazily as it streams, and not by collecting the distinct kinds
// actually encountered mid-scan either, which would need a second full pass
// over the same edges (selectKindIDs' doc). This keeps the same guarantee
// TryNodeFetchKinds already has: a caller that gets (cursor, true) back is
// holding a fully-determined, already error-free answer, with no possibility
// of a resolution failure surfacing mid-stream.
func (e *Engine) TryRelFetchKinds(ctx context.Context, spec recognize.RelSpec) (graph.Cursor[graph.RelationshipKindsResult], bool) {
	start := time.Now()

	plan, ok := e.resolveRelSpec(ctx, opRelKinds, spec)
	if !ok {
		return nil, false
	}

	kindNames, err := resolveKindNameMap(ctx, selectKindIDs(plan.snap.MaxKindID(), plan.kindMask.Has), e.mapKindNames)
	if err != nil {
		e.declineOp(ctx, opRelKinds, reasonError, err)
		return nil, false
	}

	snap := plan.snap
	it := newRelScanIter(plan)
	cursor := newFeedCursor(ctx, func(yield func(graph.RelationshipKindsResult) bool) {
		for {
			edge, ok := it.next()
			if !ok {
				return
			}
			row := graph.RelationshipKindsResult{
				RelationshipTripleResult: graph.RelationshipTripleResult{
					ID:      graph.ID(edge.edgeID),
					StartID: graph.ID(snap.GraphID(edge.start)),
					EndID:   graph.ID(snap.GraphID(edge.end)),
				},
				Kind: kindNames[edge.kind],
			}
			if !yield(row) {
				return
			}
		}
	})

	e.servedOp(ctx, opRelKinds, start)
	return cursor, true
}

// TryRelQueryRows attempts to serve spec's matching relationships as rows
// shaped by proj (recognize.RowProjection) entirely from the engine's
// current snapshot, returning a pull-based graph.Result (rowResult,
// rowresult.go) on success. It returns (nil, false) under the same
// conditions as TryRelCount, plus three more, all checked only after
// resolveRelSpec's gate has already passed (so cfg.Enabled/no-snapshot/
// unmappable-kind/kind-stale declines always take priority, mirroring every
// other TryRel* entry point's decline ordering):
//
//   - Projection/anchor direction mismatch (reasonProjectionMismatch):
//     recognize.ProjectionStepOutbound requires spec.StartIDs non-nil (the
//     row's far/varying side is the end), and ProjectionStepInbound requires
//     spec.EndIDs non-nil (the far side is the start) -- see
//     reasonProjectionMismatch's doc for why a real upstream caller should
//     never actually hit this.
//   - orderByEdgeID requested with neither endpoint anchored
//     (reasonUnsupportedOrder): ordering needs a bounded match set to gather
//     and sort ahead of emitting the first row, which only an anchored scan
//     (start- or end-anchored, or both) guarantees; a full scan's match set
//     can be the entire edge set, which the real upstream orderByEdgeID
//     callers (traversal's paging order) never ask a full-scan query for
//     anyway.
//
// A step projection once additionally required every node kind in the
// snapshot to be mark-clean, since its row carries the far node's own kinds
// column (TryNodeFetchKinds' identical, now equally retired, concern):
// write-through removes the premise, because the View this serves from
// already carries every committed kind change.
//
// When orderByEdgeID is honored, newRelScanIter's relIterator is fully
// drained (drainRelIter) and sorted ascending by edge id before rowResult is
// constructed, exactly mirroring resolveRelSpec's "gather matching slots,
// sort by edge id, then emit" doc; otherwise rowResult drives the live
// relScanIter directly, one relEdge per Next() call, with no goroutine and
// no prior materialization -- see TryRelFetchIDs' doc for why streaming
// directly is preferred whenever nothing about rendering a row can fail
// mid-scan.
//
// For a step projection (ProjectionStepOutbound/StepInbound), the far
// node's kinds column needs every possible node KindID resolved to its
// graph.Kind name up front, for the same reason TryRelFetchKinds resolves
// its edge-kind names eagerly: so a resolution failure declines reasonError
// here, before rowResult is ever constructed, rather than surfacing from
// inside a Values() call with no clean way to report it (rowResult.Error()
// always returns nil; see its own doc). Unlike TryRelFetchKinds, every
// KindID 1..snap.MaxKindID is resolved regardless of plan.kindMask, since a
// far node's own carried kinds are never filtered by spec.EdgeKinds; for
// ProjectionStartEnd, no kind names are needed at all (the row is a bare id
// pair), so this resolution is skipped entirely.
func (e *Engine) TryRelQueryRows(ctx context.Context, spec recognize.RelSpec, proj recognize.RowProjection, orderByEdgeID bool) (graph.Result, bool) {
	start := time.Now()

	plan, ok := e.resolveRelSpec(ctx, opRelRows, spec)
	if !ok {
		return nil, false
	}

	switch proj {
	case recognize.ProjectionStepOutbound:
		if spec.StartIDs == nil {
			e.declineOp(ctx, opRelRows, reasonProjectionMismatch, nil)
			return nil, false
		}
	case recognize.ProjectionStepInbound:
		if spec.EndIDs == nil {
			e.declineOp(ctx, opRelRows, reasonProjectionMismatch, nil)
			return nil, false
		}
	}

	if orderByEdgeID && spec.StartIDs == nil && spec.EndIDs == nil {
		e.declineOp(ctx, opRelRows, reasonUnsupportedOrder, nil)
		return nil, false
	}

	var kindNames map[snapshot.KindID]graph.Kind
	if proj != recognize.ProjectionStartEnd {
		resolved, err := resolveKindNameMap(ctx, selectKindIDs(plan.snap.MaxKindID(), func(snapshot.KindID) bool { return true }), e.mapKindNames)
		if err != nil {
			e.declineOp(ctx, opRelRows, reasonError, err)
			return nil, false
		}
		kindNames = resolved
	}

	var it relIterator = newRelScanIter(plan)
	if orderByEdgeID {
		edges := drainRelIter(it)
		sort.Slice(edges, func(i, j int) bool { return edges[i].edgeID < edges[j].edgeID })
		it = &sliceRelIter{edges: edges}
	}

	result := newRowResult(plan.snap, it, proj, kindNames)

	e.servedOp(ctx, opRelRows, start)
	return result, true
}
