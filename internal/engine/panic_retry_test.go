// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// panicRetryLogCounter is a slog.Handler that counts the records carrying
// one message. Safe for concurrent use: the recovery rebuild keeps logging
// from its own goroutine while the test reads.
type panicRetryLogCounter struct {
	message string
	seen    atomic.Int64
}

func (c *panicRetryLogCounter) Enabled(context.Context, slog.Level) bool { return true }

func (c *panicRetryLogCounter) Handle(_ context.Context, rec slog.Record) error {
	if rec.Message == c.message {
		c.seen.Add(1)
	}
	return nil
}

func (c *panicRetryLogCounter) WithAttrs([]slog.Attr) slog.Handler { return c }

func (c *panicRetryLogCounter) WithGroup(string) slog.Handler { return c }

func (c *panicRetryLogCounter) count() int64 { return c.seen.Load() }

// TestADeterministicRebuildPanicLeavesTheFastRetrySchedule: a panic that
// depends on the data -- the shape the rebuild's recover exists for, since
// one that does not is gone on the next attempt -- recurs on every attempt.
// On the ordinary doubling backoff that means a full snapshot load, and an
// ERROR with a full stack, every 30 s for the life of the process, while
// the engine is already serving correctly from PostgreSQL. The recovery
// loop must back such a rebuild off to the long interval instead.
//
// An engine with no PostgreSQL driver panics inside the load itself, which
// stands in for a snapshot build that panics on data it cannot represent
// (the same fixture TestRebuildPanicEntersFallback uses).
func TestADeterministicRebuildPanicLeavesTheFastRetrySchedule(t *testing.T) {
	counter := &panicRetryLogCounter{message: "bloodtrail: snapshot rebuild panicked"}
	e := New(nil, nil, Config{Enabled: true, Log: slog.New(counter)})
	t.Cleanup(e.Stop)
	e.snap.Store(buildApplyView(t))

	// The first panic enters fallback and starts the recovery loop, whose
	// own attempts panic the same way.
	if err := e.RebuildNow(context.Background(), "manual"); err == nil {
		t.Fatalf("RebuildNow returned nil, want the recovered panic as an error")
	}

	// Long enough for the doubling schedule (from fallbackRetryInterval,
	// 100ms) to have retried several times over.
	time.Sleep(800 * time.Millisecond)

	n := counter.count()
	if n == 0 {
		t.Fatalf("the rebuild panic was never logged; this fixture is meant to panic")
	}
	// The manual call and the recovery loop's own first attempt: after that
	// the loop waits fallbackBudgetRetryInterval, not a few hundred ms.
	if n > 2 {
		t.Fatalf("a deterministic rebuild panic logged %d stacks in 800ms; want the recovery loop on the long interval after its first attempt", n)
	}
}
