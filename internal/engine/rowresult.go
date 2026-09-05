// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/channels"
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
