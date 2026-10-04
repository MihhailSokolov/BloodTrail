// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// panicLogRecorder is a slog.Handler that keeps every record's level, message
// and recovered-panic attr, and can be armed to PANIC the next time a chosen
// message is logged. Safe for concurrent use: the recovery rebuild a fallback
// starts keeps logging from its own goroutine while a test reads.
//
// The arming is this file's panic injection, and it needs no production seam:
// Config.Log is the engine's own, operator-supplied handler, so a handler that
// panics is a hazard production can genuinely present -- and the panic lands
// inside the recovered extent of whatever background work emitted that line,
// with a value the test chose. That is what lets these tests stop depending on
// a nil dereference happening to be reachable.
type panicLogRecorder struct {
	mu      sync.Mutex
	entries []panicLogEntry
	pending map[string]any
}

type panicLogEntry struct {
	level      slog.Level
	message    string
	panicValue any
	panicSeen  bool
}

func (r *panicLogRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *panicLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	entry := panicLogEntry{level: rec.Level, message: rec.Message}
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == "panic" {
			entry.panicValue, entry.panicSeen = a.Value.Any(), true
			return false
		}
		return true
	})
	r.entries = append(r.entries, entry)
	injected, armed := r.pending[rec.Message]
	if armed {
		delete(r.pending, rec.Message) // one shot: the recovery's own lines log normally
	}
	r.mu.Unlock()

	if armed {
		// Deliberately after the unlock: this panic unwinds out of the
		// engine's goroutine, and a mutex left held here would deadlock every
		// later read of this recorder -- including the assertions that prove
		// the panic was recovered.
		panic(injected)
	}
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

// panicOn arms the recorder to panic with value the next time message is
// logged. See the type's own doc for why this is the injection point.
func (r *panicLogRecorder) panicOn(message string, value any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending == nil {
		r.pending = map[string]any{}
	}
	r.pending[message] = value
}

// stillArmed reports whether an armed panic is still waiting -- that is,
// whether the line it was meant to ride in on was never logged, so no panic
// was injected at all and whatever the test went on to observe proves nothing.
func (r *panicLogRecorder) stillArmed(message string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, armed := r.pending[message]
	return armed
}

// recoveredPanic returns the value the recovery logged under its "panic" attr
// for message, so a test can assert the panic that was recovered is the panic
// it caused.
func (r *panicLogRecorder) recoveredPanic(message string) (any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.entries {
		if e.message == message && e.panicSeen {
			return e.panicValue, true
		}
	}
	return nil, false
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
// replica serving after a load that did not finish.
//
// The panic itself comes from an engine with no PostgreSQL driver, whose load
// dereferences it -- which stands in for a snapshot build that panics on data
// it cannot represent. That trigger is incidental to the behaviour under test,
// so it is PROVED here rather than assumed: a nil guard added to loadSnapshot
// would otherwise turn this into a test that quietly exercises nothing and
// then fails about fallback, which is not where the reader would look. The
// probe below fails first, and says exactly what happened.
//
// (A panic injected into the rebuild the way TestCompactionPanicEntersFallback
// injects one is not reachable from a unit test: nothing inside rebuildOnce's
// recovered extent is under a test's control before the load, and the load
// takes no logger. Doing it properly needs either a database or a seam on
// loadSnapshot of the kind boot.go's readSnapshotFile already is.)
func TestRebuildPanicEntersFallback(t *testing.T) {
	triggerPanic := callCatchingPanic(func() {
		_, _, _ = loadSnapshot(context.Background(), nil, nil, true, false)
	})
	if triggerPanic == nil {
		t.Fatal("loadSnapshot with no driver no longer panics -- it returns an error now, so this test's panic trigger is gone and rebuildOnce's recover is not being exercised at all. Replace the trigger (a seam on loadSnapshot, or a database-backed injection) rather than deleting this check.")
	}

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
	// The panic that was recovered is the trigger's, not some other failure
	// that happened to produce an error on the way.
	got, ok := requireRecoveredPanicValue(t, logs, "bloodtrail: snapshot rebuild panicked")
	if ok && fmt.Sprint(got) != fmt.Sprint(triggerPanic) {
		t.Errorf("the rebuild recovered %v, want the load's own panic %v", got, triggerPanic)
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

// requireRecoveredPanicValue reads the panic value the recovery logged under
// message, failing the test when the line carried none -- the recovery is
// supposed to name what it caught.
func requireRecoveredPanicValue(t *testing.T, logs *panicLogRecorder, message string) (any, bool) {
	t.Helper()
	got, ok := logs.recoveredPanic(message)
	if !ok {
		t.Errorf("no %q line carried a panic value; the recovery did not name what it caught", message)
	}
	return got, ok
}

// compactionPanicInjected is the value TestCompactionPanicEntersFallback
// panics with, so the recovery can be held to having caught that exact panic
// rather than anything else the fold might have raised on its own.
const compactionPanicInjected = "compaction panic injected by the test"

// TestCompactionPanicEntersFallback is the compaction goroutine's
// counterpart: a panic while folding must not kill the process, must leave
// the compaction gate released, and must take the engine out of serving.
//
// The panic is injected rather than provoked. An earlier version handed the
// fold a snapshot it could not read -- no kind table, no packed arrays -- and
// relied on Fold dereferencing its way into a panic, which is both incidental
// to what is being tested and silently undone the day Fold grows a guard and
// returns an error instead. Here the inputs are a perfectly foldable base
// that simply is not the one the engine is serving, so adoptCompaction
// discards it and logs that it did -- and panicLogRecorder is armed to panic
// on exactly that line, with a value this test owns, inside
// foldAndAdoptCompaction's recovered extent.
func TestCompactionPanicEntersFallback(t *testing.T) {
	logs := &panicLogRecorder{}
	e := New(nil, nil, Config{Enabled: true, Log: slog.New(logs)})
	t.Cleanup(e.Stop)
	e.snap.Store(buildApplyView(t))
	loopsBefore := e.rebuildLoopStarts.Load()

	// A second, independently built base: foldable on its own, but not the
	// base the engine is serving, which is what makes adoptCompaction discard
	// the result (its first check is that pointer).
	const discarded = "bloodtrail: compaction discarded"
	other := buildApplyView(t).Base()
	logs.panicOn(discarded, compactionPanicInjected)

	e.compacting.Store(true) // claimed, as maybeStartCompaction's CAS would have
	if p := callCatchingPanic(func() { e.runCompaction(other, nil) }); p != nil {
		t.Fatalf("runCompaction panicked: %v", p)
	}

	if logs.stillArmed(discarded) {
		t.Fatalf("the compaction never logged %q, so no panic was injected and nothing below proves anything; the discard path this test rides on has changed", discarded)
	}
	if got, ok := requireRecoveredPanicValue(t, logs, "bloodtrail: compaction panicked"); ok && got != compactionPanicInjected {
		t.Errorf("the compaction recovered %v, want the injected %q", got, compactionPanicInjected)
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
