// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
)

// pathResult implements graph.Result over a graph.PathSet computed by the
// in-memory engine: one row per path, projecting a single "p" column whose
// value is the graph.Path itself. It is the shape ops.FetchByQuery (the real
// BloodHound cypher-endpoint consumer) expects back from
// graph.Transaction.Query -- see graph/result.go's Result interface and
// ops/ops.go:190's Next/Values/Mapper/Close usage.
//
// The zero value is not useful; construct with newPathResult.
type pathResult struct {
	paths graph.PathSet

	// idx is the current row. It starts one before the first path (-1);
	// Next() advances it before checking bounds, mirroring the
	// database/sql Rows convention FetchByQuery's `for queryResult.Next()
	// { ... }` loop assumes -- Values() is only ever called after a Next()
	// that returned true.
	idx int
}

// newPathResult wraps paths as a graph.Result: one row per path, Keys()
// []string{"p"}, Values() []any{path}, and a Mapper() that recognizes only
// *graph.Path targets, declining every other target type so
// ops.FetchByQuery's relationship/node/path type-switch (in that order; see
// ops/ops.go:190) reaches the path branch for these rows instead of
// misclassifying them.
func newPathResult(paths graph.PathSet) graph.Result {
	return &pathResult{paths: paths, idx: -1}
}

// Next advances to the next path, reporting whether one exists.
func (r *pathResult) Next() bool {
	r.idx++
	return r.idx < len(r.paths)
}

// Keys names the single projected column, matching a Cypher query that
// returns a lone path variable (e.g. "RETURN p").
func (r *pathResult) Keys() []string {
	return []string{"p"}
}

// Values returns the current row's single value: the graph.Path most
// recently advanced to by Next(). Calling Values() before any Next() call,
// or after Next() has returned false, returns nil.
func (r *pathResult) Values() []any {
	if r.idx < 0 || r.idx >= len(r.paths) {
		return nil
	}
	return []any{r.paths[r.idx]}
}

// Mapper returns a graph.ValueMapper built from mapPathValue.
func (r *pathResult) Mapper() graph.ValueMapper {
	return graph.NewValueMapper(mapPathValue)
}

// Scan is graph.Result's deprecated convenience method, implemented via
// graph.ScanNextResult for interface completeness; the intended consumer
// (ops.FetchByQuery) drives Values()/Mapper() directly rather than calling
// Scan.
func (r *pathResult) Scan(targets ...any) error {
	return graph.ScanNextResult(r, targets...)
}

// Error always returns nil: a pathResult is built from an already-computed
// PathSet, so nothing can fail during iteration.
func (r *pathResult) Error() error {
	return nil
}

// Close is a no-op: a pathResult holds no external resource (cursor,
// connection, file) to release.
func (r *pathResult) Close() {}

// mapPathValue is pathResult's ValueMapper function. It maps a graph.Path
// rawValue into a *graph.Path target and returns false for every other
// combination -- notably *graph.Relationship and *graph.Node targets, the
// two ops.FetchByQuery's Values() loop tries before *graph.Path (see
// ops/ops.go:190's if/else chain: relationship first, node second, path
// third) -- so a pathResult row is never misclassified as a relationship or
// node value and always reaches FetchByQuery's path branch.
func mapPathValue(rawValue, target any) bool {
	path, isPath := rawValue.(graph.Path)
	if !isPath {
		return false
	}

	pathTarget, isPathTarget := target.(*graph.Path)
	if !isPathTarget {
		return false
	}

	*pathTarget = path
	return true
}

// pathCursor implements graph.Cursor[graph.Path] over a pre-computed
// graph.PathSet, modeled directly on dawgs's own graph.ResultIterator
// (graph/result.go): a feeder goroutine pushes every path onto Chan() via
// channels.Submit, which honors ctx cancellation, so a caller that never
// finishes draining (or calls Close() early, which cancels ctx via
// cancelFunc) doesn't leak the feeder goroutine blocked on a full,
// unread channel.
//
// The zero value is not useful; construct with NewPathCursor.
type pathCursor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	valueC     chan graph.Path

	// err is written only by the feeder goroutine, and only before it closes
	// valueC; a caller that reads Error() only after Chan() has been fully
	// drained (the documented Cursor contract, e.g. "for v := range
	// cursor.Chan() { ... }; return cursor.Error()") observes it safely via
	// the channel-close happens-before edge, matching graph.ResultIterator's
	// identical convention.
	err error
}

// NewPathCursor wraps paths as a graph.Cursor[graph.Path], feeding them
// through a channel exactly the way a live database cursor would -- so
// recordingRelationshipQuery.FetchAllShortestPaths (the root package's
// relationship_query.go) can hand an engine-computed graph.PathSet to a
// delegate written against the
// FetchAllShortestPaths(delegate func(cursor graph.Cursor[Path]) error)
// contract unchanged, whether the paths came from PostgreSQL or from the
// in-memory engine.
func NewPathCursor(ctx context.Context, paths graph.PathSet) graph.Cursor[graph.Path] {
	cursorCtx, cancel := context.WithCancel(ctx)

	cursor := &pathCursor{
		ctx:        cursorCtx,
		cancelFunc: cancel,
		valueC:     make(chan graph.Path),
	}

	go cursor.feed(paths)

	return cursor
}

// feed pushes every path in paths onto c.valueC, stopping early (and
// recording graph.ErrContextTimedOut) if c.ctx is canceled -- by the caller
// passing an already-canceled/expiring ctx to NewPathCursor, or by a
// subsequent Close() call -- before the set is fully drained.
func (c *pathCursor) feed(paths graph.PathSet) {
	defer close(c.valueC)

	for _, path := range paths {
		if !channels.Submit(c.ctx, c.valueC, path) {
			c.err = graph.ErrContextTimedOut
			return
		}
	}
}

// Error returns the error captured while feeding, if any. See err's doc for
// the concurrency contract callers must observe.
func (c *pathCursor) Error() error {
	return c.err
}

// Close cancels the cursor's context, unblocking a feeder goroutine that is
// waiting to submit into an unread channel.
func (c *pathCursor) Close() {
	c.cancelFunc()
}

// Chan returns the channel paths are fed through.
func (c *pathCursor) Chan() chan graph.Path {
	return c.valueC
}
