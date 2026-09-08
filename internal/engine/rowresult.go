// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// rowCursorBuffer sizes the buffered channel newFeedCursor's feeder
// goroutine submits into. Unlike pathCursor's unbuffered channel (result.go)
// -- fine there because a graph.PathSet is small enough to fully compute
// before streaming even starts -- the builder-serving queries this backs
// (TryNodeFetchIDs, TryNodeFetchKinds, and Task 7's rel-query siblings) can
// iterate a snapshot bitset with many thousands of set bits, and a caller
// that reads in small batches (or with per-row work between reads) would
// otherwise force the feeder to resume and block on every single row. A
// modest buffer lets the feeder run ahead of a slower consumer without
// growing unbounded: 256 rows costs nothing measurable next to a snapshot's
// own size, and is large enough to smooth out ordinary per-row consumer
// latency.
const rowCursorBuffer = 256

// feedCursor implements graph.Cursor[T] over a generic feeder function,
// generalizing pathCursor (result.go) from a fixed pre-computed graph.
// PathSet to any producer shaped like Bitset.Iterate: a function that calls
// a yield callback once per value and stops early if yield returns false.
//
// The zero value is not useful; construct with newFeedCursor.
type feedCursor[T any] struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	valueC     chan T

	// err is written only by the feeder goroutine, and only before it closes
	// valueC; see pathCursor's identical field for the happens-before
	// argument that makes this safe to read from Error() without a lock,
	// provided the caller only does so after fully draining Chan() (the
	// documented Cursor contract).
	err error
}

// newFeedCursor wraps feed as a graph.Cursor[T]: a feeder goroutine calls
// feed once, passing it a yield closure that submits each value onto a
// buffered channel (rowCursorBuffer deep) via channels.Submit, which honors
// ctx cancellation. yield itself returns false the instant ctx is done, so a
// feed implementation built on top of an early-exit iterator (e.g. Bitset.
// Iterate) naturally stops pulling more values the moment the caller stops
// reading or calls Close() -- exactly the same shape pathCursor.feed gives
// its fixed graph.PathSet, generalized to any push-style producer.
func newFeedCursor[T any](ctx context.Context, feed func(yield func(T) bool)) graph.Cursor[T] {
	cursorCtx, cancel := context.WithCancel(ctx)

	cursor := &feedCursor[T]{
		ctx:        cursorCtx,
		cancelFunc: cancel,
		valueC:     make(chan T, rowCursorBuffer),
	}

	go cursor.feed(feed)

	return cursor
}

// feed runs feed to completion, submitting every yielded value onto
// c.valueC and closing it when done -- either because feed returned on its
// own (the producer is exhausted) or because a submit failed, meaning c.ctx
// was canceled (by the caller passing an already-canceled/expiring ctx to
// newFeedCursor, or by a subsequent Close() call) before the value could be
// delivered.
func (c *feedCursor[T]) feed(feed func(yield func(T) bool)) {
	defer close(c.valueC)

	feed(func(v T) bool {
		if !channels.Submit(c.ctx, c.valueC, v) {
			c.err = graph.ErrContextTimedOut
			return false
		}
		return true
	})
}

// Error returns the error captured while feeding, if any. See err's doc for
// the concurrency contract callers must observe.
func (c *feedCursor[T]) Error() error {
	return c.err
}

// Close cancels the cursor's context, unblocking a feeder goroutine that is
// waiting to submit into a full, unread channel.
func (c *feedCursor[T]) Close() {
	c.cancelFunc()
}

// Chan returns the channel values are fed through.
func (c *feedCursor[T]) Chan() chan T {
	return c.valueC
}

// rowResult implements graph.Result over a relIterator (serve_builder.go's
// shared rel-query scan), rendering each matching relationship as a row
// shaped by proj (recognize.RowProjection). It is TryRelQueryRows'
// pull-based counterpart to pathResult (result.go): Next() advances it
// directly, in place, with no feeder goroutine and no channel -- unlike
// TryAllShortestPaths' already-computed graph.PathSet, a row here is read
// live off the snapshot's CSR arrays (via it) exactly as the caller pulls,
// which is the same synchronous, no-buffering shape a real driver.Result
// implementation gives ops.FetchByQuery's own `for results.Next() {
// results.Scan(...) }` loop and, one level up, dawgs' own container.
// FetchDirectedGraph and traversal.shallowFetchRelationships (see this
// type's tests, which drive it exactly the way those two do).
//
// The zero value is not useful; construct with newRowResult.
type rowResult struct {
	snap *snapshot.View
	it   relIterator
	proj recognize.RowProjection

	// kindNames resolves a snapshot.KindID (a far node's own carried kind,
	// for ProjectionStepOutbound/StepInbound, or the traversed
	// relationship's kind, same two projections) to its graph.Kind name.
	// TryRelQueryRows resolves this fully before ever calling newRowResult
	// (see its own doc), so Values() never has anything left that could
	// fail -- a missing entry (never expected, since every KindID up to
	// snap.MaxKindID is resolved regardless of proj) reads as a nil
	// graph.Kind rather than panicking. Left nil for ProjectionStartEnd,
	// which needs no kind names at all.
	kindNames map[snapshot.KindID]graph.Kind

	// cur/valid hold the most recent relEdge Next() advanced to, and
	// whether one exists -- Values() returns nil unless valid, mirroring
	// pathResult's identical "before the first Next() or past the last one"
	// contract.
	cur   relEdge
	valid bool
}

// newRowResult wraps it as a graph.Result, projecting each relEdge it
// produces into the row shape proj names. See rowResult's doc for kindNames'
// contract; snap resolves a relEdge's dense start/end NodeIDs and a far
// node's own kinds back to database ids and graph.Kind names.
func newRowResult(snap *snapshot.View, it relIterator, proj recognize.RowProjection, kindNames map[snapshot.KindID]graph.Kind) graph.Result {
	return &rowResult{snap: snap, it: it, proj: proj, kindNames: kindNames}
}

// Next advances to the next matching relationship, reporting whether one
// exists.
func (r *rowResult) Next() bool {
	edge, ok := r.it.next()
	r.cur, r.valid = edge, ok
	return ok
}

// Keys names each projection's columns. Consumers of a rowResult (dawgs'
// container.FetchDirectedGraph and traversal.shallowFetchRelationships)
// drive Scan()/Values() positionally and never call Keys() at all -- these
// names exist purely so a rowResult remains a well-formed graph.Result on
// its own terms, matching the column count and rough shape query.Returning
// itself would have produced (see recognize.RowProjection's doc for the
// exact FunctionInvocation each column mirrors).
func (r *rowResult) Keys() []string {
	switch r.proj {
	case recognize.ProjectionStepOutbound:
		return []string{"end_id", "end_kinds", "rel_id", "rel_kind"}
	case recognize.ProjectionStepInbound:
		return []string{"start_id", "start_kinds", "rel_id", "rel_kind"}
	default:
		return []string{"start_id", "end_id"}
	}
}

// Values returns the current row's values, shaped by proj (see
// TryRelQueryRows' doc for the exact contract): []any{graph.ID(start),
// graph.ID(end)} for ProjectionStartEnd; []any{graph.ID(farID),
// graph.Kinds(farKinds), graph.ID(edgeID), graph.Kind(edgeKind)} for a step
// projection, far being the end for StepOutbound and the start for
// StepInbound. Calling Values() before any Next() call, or after Next() has
// returned false, returns nil, matching pathResult's identical convention.
func (r *rowResult) Values() []any {
	if !r.valid {
		return nil
	}

	switch r.proj {
	case recognize.ProjectionStartEnd:
		return []any{graph.ID(r.snap.GraphID(r.cur.start)), graph.ID(r.snap.GraphID(r.cur.end))}

	case recognize.ProjectionStepOutbound:
		return []any{
			graph.ID(r.snap.GraphID(r.cur.end)),
			r.nodeKinds(r.cur.end),
			graph.ID(r.cur.edgeID),
			r.kindNames[r.cur.kind],
		}

	case recognize.ProjectionStepInbound:
		return []any{
			graph.ID(r.snap.GraphID(r.cur.start)),
			r.nodeKinds(r.cur.start),
			graph.ID(r.cur.edgeID),
			r.kindNames[r.cur.kind],
		}

	default:
		return nil
	}
}

// nodeKinds resolves dense's own carried kinds (snap.KindOffsets/NodeKinds)
// to their graph.Kind names via r.kindNames, for a step projection's far-
// node kinds column.
func (r *rowResult) nodeKinds(dense snapshot.NodeID) graph.Kinds {
	nodeKindIDs := r.snap.KindIDsOf(dense)
	kinds := make(graph.Kinds, 0, len(nodeKindIDs))
	for _, kindID := range nodeKindIDs {
		kinds = append(kinds, r.kindNames[kindID])
	}
	return kinds
}

// Mapper returns a graph.ValueMapper built from mapRowIDValue,
// mapRowKindsValue, and mapRowKindValue -- see those functions' docs for why
// dawgs' own defaultMapValue (graph/mapper.go), despite nominally handling
// *graph.ID/*graph.Kinds/*graph.Kind targets, does not actually recognize an
// already-typed graph.ID/graph.Kinds/graph.Kind raw value the way Values()
// above produces one, and so cannot be relied on alone here.
func (r *rowResult) Mapper() graph.ValueMapper {
	return graph.NewValueMapper(mapRowIDValue, mapRowKindsValue, mapRowKindValue)
}

// Scan is graph.Result's deprecated convenience method, implemented via
// graph.ScanNextResult -- the same shape pathResult.Scan uses, and the one
// every real consumer (container.FetchDirectedGraph, traversal.
// shallowFetchRelationships) actually calls.
func (r *rowResult) Scan(targets ...any) error {
	return graph.ScanNextResult(r, targets...)
}

// Error always returns nil: TryRelQueryRows resolves every kind name a
// rowResult could ever need (kindNames) before constructing it at all (see
// rowResult's own doc), so nothing that could fail is left to happen during
// iteration -- unlike, say, a live PostgreSQL cursor, whose Result
// implementation surfaces a mid-stream network or decode error here.
func (r *rowResult) Error() error {
	return nil
}

// Close is a no-op: a rowResult holds no external resource (cursor,
// connection, file, goroutine) to release -- Next() reads directly off the
// snapshot's already-resident CSR arrays via r.it.
func (r *rowResult) Close() {}

// mapRowIDValue is rowResult's explicit MapFunc for a *graph.ID target.
// dawgs' own defaultMapValue (graph/mapper.go) has a *ID case, but it goes
// through AsNumeric[ID], which type-switches on rawValue's own dynamic type
// against the driver's raw scalar types (uint64, int64, string, ...) --
// never against graph.ID itself, since a live database driver never hands
// back an already-boxed graph.ID. A rowResult row, by contrast, is built
// entirely from already-typed Go values (Values() above literally
// constructs a graph.ID), so rawValue's dynamic type here is graph.ID
// itself, which AsNumeric's switch does not match; defaultMapValue would
// therefore silently decline every row's id column without this -- see
// rowresult_test.go's TestDefaultValueMapperDoesNotHandleEngineNativeTypes,
// which pins this exact failure mode down directly against graph.
// NewValueMapper() with no MapFuncs added, so a future dawgs upgrade that
// changes defaultMapValue's behavior is caught rather than silently
// relied upon.
func mapRowIDValue(rawValue, target any) bool {
	id, isID := rawValue.(graph.ID)
	if !isID {
		return false
	}
	idTarget, isIDTarget := target.(*graph.ID)
	if !isIDTarget {
		return false
	}
	*idTarget = id
	return true
}

// mapRowKindsValue is rowResult's explicit MapFunc for a *graph.Kinds
// target. dawgs' own defaultMapValue has a *Kinds case, but it goes through
// AsKinds, which requires rawValue's dynamic type to be exactly []any (a
// raw, not-yet-typed driver value) via SliceOf[string] -- never a
// graph.Kinds ([]graph.Kind) value already boxed as one, which is exactly
// what Values() above produces (r.nodeKinds returns a graph.Kinds). See
// mapRowIDValue's doc for the same "already-typed value, not a raw driver
// value" root cause.
func mapRowKindsValue(rawValue, target any) bool {
	kinds, isKinds := rawValue.(graph.Kinds)
	if !isKinds {
		return false
	}
	kindsTarget, isKindsTarget := target.(*graph.Kinds)
	if !isKindsTarget {
		return false
	}
	*kindsTarget = kinds
	return true
}

// mapRowKindValue is rowResult's explicit MapFunc for a *graph.Kind target.
// dawgs' own defaultMapValue has a *Kind case, but it requires rawValue's
// dynamic type to be exactly string (a raw driver value it then wraps via
// graph.StringKind) -- never a value that already implements the graph.Kind
// interface, which is exactly what r.kindNames stores and Values() above
// returns directly. The type assertion here is against the graph.Kind
// interface itself, so it succeeds for any concrete Kind implementation
// (graph.StringKind or otherwise), unlike defaultMapValue's exact-string
// check. See mapRowIDValue's doc for the same root cause.
func mapRowKindValue(rawValue, target any) bool {
	kind, isKind := rawValue.(graph.Kind)
	if !isKind {
		return false
	}
	kindTarget, isKindTarget := target.(*graph.Kind)
	if !isKindTarget {
		return false
	}
	*kindTarget = kind
	return true
}
