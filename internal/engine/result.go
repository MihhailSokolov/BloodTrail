// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
)

// mapPathValue is a graph.ValueMapper function mapping a graph.Path
// rawValue into a *graph.Path target and returning false for every other
// combination -- notably *graph.Relationship and *graph.Node targets, the
// two ops.FetchByQuery's Values() loop tries before *graph.Path (see
// ops/ops.go:190's if/else chain: relationship first, node second, path
// third) -- so a path-valued row is never misclassified as a relationship
// or node value and always reaches FetchByQuery's path branch.
//
// Originally written for the now-retired pathResult (TryCypher's own
// pre-interpreter result type, deleted once TryCypher was rewired to the
// Cypher interpreter's cypherRowsResult in serve_cypher.go); this function
// stayed, since cypherRowsResult.Mapper still needs exactly the same path
// classification for its own OutPath columns.
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
