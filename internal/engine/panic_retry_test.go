// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
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
// The load is made to panic through loadSnapshotFn (load.go), standing in
// for a snapshot build that panics on data it cannot represent.
func TestADeterministicRebuildPanicLeavesTheFastRetrySchedule(t *testing.T) {
	previousLoad := loadSnapshotFn
	loadSnapshotFn = func(context.Context, *pg.Driver, *pgxpool.Pool, bool, bool) (*snapshot.Snapshot, loadedWatermark, error) {
		panic("load panic injected by the test")
	}
	t.Cleanup(func() { loadSnapshotFn = previousLoad })

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

// TestARecoveredLoadPanicAlsoLeavesTheFastRetrySchedule is the same claim
// for the OTHER half of a panicking load: a panic on one of the load's own
// errgroup goroutines (loadNodes' row scan, ParseProps, AddParsedNode) is
// recovered where it happens and returned as an ordinary load error
// (goRecovered, load.go), so it never reaches recoverRebuildPanic and
// nothing marks the engine. That error is the most likely instance of a
// data-dependent panic -- the whole class goRecovered was written for -- and
// it must get the same long interval, or the recovery loop re-runs the
// entire load, stack log and all, every 30 s for the life of the process.
func TestARecoveredLoadPanicAlsoLeavesTheFastRetrySchedule(t *testing.T) {
	previousLoad := loadSnapshotFn
	loadSnapshotFn = func(context.Context, *pg.Driver, *pgxpool.Pool, bool, bool) (*snapshot.Snapshot, loadedWatermark, error) {
		// Exactly what loadNodes returns once goRecovered has turned a
		// panic on one of its goroutines into this load's error.
		return nil, loadedWatermark{}, panicLoadError("staging nodes", "AddParsedNode blew up")
	}
	t.Cleanup(func() { loadSnapshotFn = previousLoad })

	counter := &panicRetryLogCounter{message: "bloodtrail: fallback rebuild failed"}
	e := New(nil, nil, Config{Enabled: true, Log: slog.New(counter)})
	t.Cleanup(e.Stop)
	e.snap.Store(buildApplyView(t))

	e.startFallbackRebuild()

	// Long enough for the doubling schedule (from fallbackRetryInterval,
	// 100ms) to have retried several times over.
	time.Sleep(800 * time.Millisecond)

	n := counter.count()
	if n == 0 {
		t.Fatalf("the load never failed; this fixture is meant to return a recovered panic")
	}
	if n > 2 {
		t.Fatalf("a recovered load panic was retried %d times in 800ms; want the recovery loop on the long interval after its first attempt", n)
	}
}
