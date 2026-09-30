// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// panicLogRecorder is a slog.Handler that keeps every record's level and
// message. Safe for concurrent use: the recovery rebuild a fallback starts
// keeps logging from its own goroutine while a test reads.
type panicLogRecorder struct {
	mu      sync.Mutex
	entries []panicLogEntry
}

type panicLogEntry struct {
	level   slog.Level
	message string
}

func (r *panicLogRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *panicLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, panicLogEntry{level: rec.Level, message: rec.Message})
	return nil
}

func (r *panicLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }

func (r *panicLogRecorder) WithGroup(string) slog.Handler { return r }

// logged reports whether a record with exactly this level and message was
// handled.
func (r *panicLogRecorder) logged(level slog.Level, message string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.entries {
		if e.level == level && e.message == message {
			return true
		}
	}
	return false
}

// callCatchingPanic runs fn and returns what it panicked with, if anything.
func callCatchingPanic(fn func()) (panicked any) {
	defer func() { panicked = recover() }()
	fn()
	return nil
}

// requireApplyMuFree fails the test unless applyMu can be taken within a
// few seconds: a recovered panic must not leave the lock every write takes
// held.
func requireApplyMuFree(t *testing.T, e *Engine) {
	t.Helper()
	acquired := make(chan struct{})
	go func() {
		e.applyMu.Lock()
		close(acquired)
		e.applyMu.Unlock()
	}()
	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("applyMu still held 5s after the recovered panic; every write would block forever")
	}
}

// TestRebuildPanicEntersFallback pins the rebuild that boot load, fallback
// recovery and RebuildNow all share: a panic inside it must come back as an
// error and a fallback, never as a dead process, and never leave the
// replica serving after a load that did not finish. An engine without a
// PostgreSQL driver panics inside the load itself, which stands in for a
// snapshot build that panics on data it cannot represent.
func TestRebuildPanicEntersFallback(t *testing.T) {
	logs := &panicLogRecorder{}
	e := New(nil, nil, Config{Enabled: true, Log: slog.New(logs)})
	t.Cleanup(e.Stop)
	e.snap.Store(buildApplyView(t))
	if _, serving := e.serveState(); !serving {
		t.Fatal("fixture engine is not serving (fixture assumption)")
	}
	loopsBefore := e.rebuildLoopStarts.Load()

	var err error
	if p := callCatchingPanic(func() { err = e.RebuildNow(context.Background(), "manual") }); p != nil {
		t.Fatalf("RebuildNow panicked: %v", p)
	}
	if err == nil {
		t.Fatal("RebuildNow returned nil, want the recovered panic as an error")
	}
	if _, serving := e.serveState(); serving {
		t.Fatal("engine still serving after its rebuild panicked, want fallback")
	}
	if e.rebuildLoopStarts.Load() == loopsBefore {
		t.Fatal("no recovery rebuild was started for the fallback")
	}
	if !logs.logged(slog.LevelError, "bloodtrail: snapshot rebuild panicked") {
		t.Fatal(`no ERROR "bloodtrail: snapshot rebuild panicked" line was logged`)
	}
	requireApplyMuFree(t, e)
}

// TestCompactionPanicEntersFallback is the compaction goroutine's
// counterpart: a panic while folding must not kill the process, must leave
// the compaction gate released, and must take the engine out of serving.
// A base Fold cannot read -- no kind table, no packed arrays -- stands in
// for a fold that panics on data it cannot represent.
func TestCompactionPanicEntersFallback(t *testing.T) {
	logs := &panicLogRecorder{}
	e := New(nil, nil, Config{Enabled: true, Log: slog.New(logs)})
	t.Cleanup(e.Stop)
	e.snap.Store(buildApplyView(t))
	loopsBefore := e.rebuildLoopStarts.Load()

	unreadable := &snapshot.Snapshot{GraphIDs: []uint64{1}}
	e.compacting.Store(true) // claimed, as maybeStartCompaction's CAS would have
	if p := callCatchingPanic(func() { e.runCompaction(unreadable, nil) }); p != nil {
		t.Fatalf("runCompaction panicked: %v", p)
	}

	if e.compacting.Load() {
		t.Fatal("compaction gate still held after the recovered panic; no compaction could ever start again")
	}
	if got := e.CompactionCount(); got != 0 {
		t.Fatalf("CompactionCount() = %d, want 0", got)
	}
	if _, serving := e.serveState(); serving {
		t.Fatal("engine still serving after its compaction panicked, want fallback")
	}
	if e.rebuildLoopStarts.Load() == loopsBefore {
		t.Fatal("no recovery rebuild was started for the fallback")
	}
	if !logs.logged(slog.LevelError, "bloodtrail: compaction panicked") {
		t.Fatal(`no ERROR "bloodtrail: compaction panicked" line was logged`)
	}
	requireApplyMuFree(t, e)
}
