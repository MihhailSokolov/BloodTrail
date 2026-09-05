// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestPollerDrivesRebuilds is the poller's integration evidence: Start
// against a live datapipe_status row builds the first snapshot (rule (a):
// no snapshot exists yet), then updating the row's last_complete_analysis_at
// to a newer value (rule (b)) drives a second rebuild that picks up the new
// stamp -- and Stop terminates the goroutine promptly.
func TestPollerDrivesRebuilds(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	createDatapipeStatusTable(t, pool)

	stamp1 := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx,
		`INSERT INTO datapipe_status (singleton, status, updated_at, last_complete_analysis_at) VALUES (true, 'idle', now(), $1)`,
		stamp1,
	); err != nil {
		t.Fatalf("insert datapipe_status: %v", err)
	}

	eng := New(pgDriver, pool, Config{
		Enabled:      true,
		PollInterval: 50 * time.Millisecond,
		Log:          testEngineLogger(),
	})

	eng.Start(ctx)
	defer eng.Stop()

	snap1 := waitForFreshSnapshot(t, eng, 2*time.Second)
	if !snap1.AnalysisStamp.Equal(stamp1) {
		t.Fatalf("first build's AnalysisStamp = %v, want %v", snap1.AnalysisStamp, stamp1)
	}

	// A write outside the poller's own rebuild invalidates the snapshot the
	// poller just adopted.
	eng.NoteWrite(nil)

	stamp2 := stamp1.Add(time.Hour)
	if _, err := pool.Exec(ctx,
		`UPDATE datapipe_status SET last_complete_analysis_at = $1, updated_at = now() WHERE singleton`,
		stamp2,
	); err != nil {
		t.Fatalf("update datapipe_status: %v", err)
	}

	// The poller must pick up the newer stamp (rule (b)) and rebuild again,
	// which also happens to clear the staleness NoteWrite introduced.
	snap2 := waitForFreshSnapshot(t, eng, 2*time.Second)
	if !snap2.AnalysisStamp.Equal(stamp2) {
		t.Fatalf("second build's AnalysisStamp = %v, want %v", snap2.AnalysisStamp, stamp2)
	}

	stopped := make(chan struct{})
	go func() {
		eng.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(1 * time.Second):
		t.Fatalf("Stop did not return within 1s")
	}
}

// TestPollerAnalyzingPhaseRebuild is rule (d)'s integration evidence: once a
// snapshot exists and a write goes stale while the datapipe is currently
// analyzing -- as opposed to idle, which is rule (c)'s territory -- the
// poller rebuilds anyway, and the trigger it logs is triggerAnalyzing, not
// some other rule's label. last_complete_analysis_at is left unchanged
// across the status flip to "analyzing" specifically so rule (b) (a newer
// stamp) cannot also explain the rebuild: this is the analysis-just-started
// moment, before any completed-run stamp update, so only rule (d) can be
// responsible for what gets observed here.
func TestPollerAnalyzingPhaseRebuild(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	createDatapipeStatusTable(t, pool)

	stamp := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx,
		`INSERT INTO datapipe_status (singleton, status, updated_at, last_complete_analysis_at) VALUES (true, 'idle', now(), $1)`,
		stamp,
	); err != nil {
		t.Fatalf("insert datapipe_status: %v", err)
	}

	handler := newCountingHandler()
	eng := New(pgDriver, pool, Config{
		Enabled:      true,
		PollInterval: 50 * time.Millisecond,
		Log:          slog.New(handler),
	})

	eng.Start(ctx)
	defer eng.Stop()

	// rule (a): no snapshot exists yet, so the first tick builds one.
	// Counting from 0 (rather than reading rebuildAttempts only after
	// waitForFreshSnapshot) sidesteps any question of whether Fresh()
	// can observe the adopted snapshot a moment before rebuildAttempts'
	// deferred increment runs (see RebuildNow's doc): waiting on the
	// attempt count itself is unambiguous either way.
	waitForAttempts(t, eng, 1, 2*time.Second)
	if _, fresh := eng.Fresh(); !fresh {
		t.Fatalf("Fresh() reports stale right after the startup build")
	}

	// A write invalidates the snapshot the poller just adopted, and the
	// datapipe status flips to analyzing.
	eng.NoteWrite(nil)
	if _, err := pool.Exec(ctx,
		`UPDATE datapipe_status SET status = 'analyzing', updated_at = now() WHERE singleton`,
	); err != nil {
		t.Fatalf("update datapipe_status: %v", err)
	}

	// Rule (d) must fire even though the datapipe is analyzing rather than
	// idle: wait for a second real LoadSnapshot attempt, then confirm both
	// that the snapshot is fresh again and that the rebuild which got us
	// there was actually labeled triggerAnalyzing.
	waitForAttempts(t, eng, 2, 2*time.Second)
	if _, fresh := eng.Fresh(); !fresh {
		t.Fatalf("Fresh() reports stale after rule (d) should have rebuilt during analyzing")
	}
	if n := handler.triggerCount(triggerAnalyzing); n != 1 {
		t.Fatalf("snapshot-rebuilt records with trigger=%q = %d, want 1", triggerAnalyzing, n)
	}

	stopped := make(chan struct{})
	go func() {
		eng.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(1 * time.Second):
		t.Fatalf("Stop did not return within 1s")
	}
}

// TestPollerAnalyzingCapAndReset drives rule (d)'s cap and reset (see
// maxAnalyzingRebuilds, resetAnalyzingRebuilds, recordAnalyzingRebuild in
// poller.go) through the real poll loop, rather than through decideRebuild
// or the counter functions directly: TestPollerAnalyzingPhaseRebuild already
// proves rule (d) fires once during an analyzing episode, but neither it nor
// any unit test exercises tick's own wiring of
// resetAnalyzingRebuilds/recordAnalyzingRebuild -- deleting either call from
// tick leaves every other test in the suite green.
//
// The scenario: reach the cap (two rebuilds in one analyzing episode, then a
// third write that must NOT rebuild), then prove the cap is per-episode by
// leaving analyzing and coming back.
//
// Rule (c) (stale + idle) is territory this test cannot avoid touching: by
// the time the cap is reached, the snapshot is deliberately left stale (an
// uncounted write beyond the cap), and leaving "analyzing" for "idle" from
// there hits rule (c) immediately. Rather than contort the write timing to
// dodge it, this test accounts for that extra idle_stale rebuild explicitly
// (see the comment at that step) -- it is also what resets
// st.analyzingRebuilds for the final step to observe.
func TestPollerAnalyzingCapAndReset(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	createDatapipeStatusTable(t, pool)

	// last_complete_analysis_at is never touched again after this insert:
	// keeping it fixed for the whole test means stamp.After(snap.AnalysisStamp)
	// (rule (b)) never becomes true, so every rebuild this test observes is
	// unambiguously rule (c) or rule (d), never rule (b).
	stamp := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx,
		`INSERT INTO datapipe_status (singleton, status, updated_at, last_complete_analysis_at) VALUES (true, 'idle', now(), $1)`,
		stamp,
	); err != nil {
		t.Fatalf("insert datapipe_status: %v", err)
	}

	const pollInterval = 25 * time.Millisecond
	handler := newCountingHandler()
	eng := New(pgDriver, pool, Config{
		Enabled:      true,
		PollInterval: pollInterval,
		Log:          slog.New(handler),
	})

	eng.Start(ctx)
	defer eng.Stop()

	// rule (a): no snapshot exists yet, so the first tick builds one.
	waitForAttempts(t, eng, 1, 2*time.Second)
	if _, fresh := eng.Fresh(); !fresh {
		t.Fatalf("Fresh() reports stale right after the startup build")
	}

	// --- Drive the cap: two analyzing-triggered rebuilds, then a third
	// write that must be refused by the cap. ---

	// First write invalidates the snapshot, then the datapipe flips to
	// analyzing: rule (d) must fire (1st rebuild of the episode).
	eng.NoteWrite(nil)
	if _, err := pool.Exec(ctx, `UPDATE datapipe_status SET status = 'analyzing', updated_at = now() WHERE singleton`); err != nil {
		t.Fatalf("flip to analyzing: %v", err)
	}
	waitForAttempts(t, eng, 2, 2*time.Second)
	if _, fresh := eng.Fresh(); !fresh {
		t.Fatalf("Fresh() reports stale after the first analyzing rebuild")
	}
	if n := handler.triggerCount(triggerAnalyzing); n != 1 {
		t.Fatalf("triggerAnalyzing count = %d, want 1 after the first analyzing rebuild", n)
	}

	// A second write, still analyzing: rule (d) fires again (2nd rebuild,
	// cap now at 2/2 for this episode).
	eng.NoteWrite(nil)
	waitForAttempts(t, eng, 3, 2*time.Second)
	if _, fresh := eng.Fresh(); !fresh {
		t.Fatalf("Fresh() reports stale after the second analyzing rebuild")
	}
	if n := handler.triggerCount(triggerAnalyzing); n != 2 {
		t.Fatalf("triggerAnalyzing count = %d, want 2 after the second analyzing rebuild (cap reached)", n)
	}

	// A third write, still analyzing and already at 2/2: rule (d) must NOT
	// fire again. Bounded negative wait (a handful of poll intervals, not
	// the 2s positive-wait budget used elsewhere) rather than an unbounded
	// one -- generous relative to pollInterval to avoid flaking on scheduler
	// jitter, but still short enough that this assertion resolves quickly
	// when the cap is (correctly) holding, and fails promptly when it isn't.
	eng.NoteWrite(nil)
	time.Sleep(6 * pollInterval)
	if n := eng.rebuildAttempts.Load(); n != 3 {
		t.Fatalf("rebuildAttempts = %d after a third write while analyzing and capped, want exactly 3 (rule (d) must not exceed maxAnalyzingRebuilds)", n)
	}
	if n := handler.triggerCount(triggerAnalyzing); n != 2 {
		t.Fatalf("triggerAnalyzing count = %d after the capped third write, want still 2", n)
	}
	if _, fresh := eng.Fresh(); fresh {
		t.Fatalf("Fresh() reports fresh after the capped third write; it must still be stale since rule (d) refused to rebuild")
	}

	// --- Prove the cap is per-episode: leave analyzing and come back. ---

	// The snapshot is still stale from the capped write above. Flipping to
	// idle from here also satisfies rule (c) (stale + idle), so this step
	// deliberately expects one more rebuild of its own -- an idle_stale one,
	// not a bug and not what this section is testing for. It is also what
	// resets st.analyzingRebuilds (tick calls resetAnalyzingRebuilds on
	// every non-analyzing status, unconditionally on whether a rebuild
	// happens), which the final step below depends on.
	if _, err := pool.Exec(ctx, `UPDATE datapipe_status SET status = 'idle', updated_at = now() WHERE singleton`); err != nil {
		t.Fatalf("flip to idle: %v", err)
	}
	waitForAttempts(t, eng, 4, 2*time.Second)
	if _, fresh := eng.Fresh(); !fresh {
		t.Fatalf("Fresh() reports stale after rule (c)'s idle rebuild")
	}
	if n := handler.triggerCount(triggerIdleStale); n != 1 {
		t.Fatalf("triggerIdleStale count = %d, want 1 (rule (c) firing once while idle)", n)
	}

	// A fresh write plus flipping back to analyzing, with the snapshot
	// stale: if resetAnalyzingRebuilds had not cleared st.analyzingRebuilds
	// while status read "idle" above, the counter would still read 2 (last
	// episode's cap) and rule (d) would refuse to fire here, leaving the
	// snapshot stale and this wait timing out. Observing a rebuild (and a
	// fresh snapshot, and a third triggerAnalyzing record) proves the reset
	// actually ran and this analyzing episode gets its own fresh budget.
	eng.NoteWrite(nil)
	if _, err := pool.Exec(ctx, `UPDATE datapipe_status SET status = 'analyzing', updated_at = now() WHERE singleton`); err != nil {
		t.Fatalf("flip back to analyzing: %v", err)
	}
	waitForAttempts(t, eng, 5, 2*time.Second)
	if _, fresh := eng.Fresh(); !fresh {
		t.Fatalf("Fresh() reports stale after the reset episode's rebuild; resetAnalyzingRebuilds may not have cleared the counter")
	}
	if n := handler.triggerCount(triggerAnalyzing); n != 3 {
		t.Fatalf("triggerAnalyzing count = %d, want 3 (a fresh episode's first rebuild after the reset)", n)
	}

	stopped := make(chan struct{})
	go func() {
		eng.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(1 * time.Second):
		t.Fatalf("Stop did not return within 1s")
	}
}

// createDatapipeStatusTable creates the datapipe_status table with the
// column names and constraint verified against upstream BloodHound
// v9.6.0's migrations (cmd/api/src/database/migration/migrations/
// 00000000000001_init.sql for singleton/status/updated_at/
// last_complete_analysis_at + the singleton_uni CHECK, and
// 20260514224346_v9_add_analysis_timestamps_bed_8136.sql /
// 20260805120000_v9_default_enable_graph_storage_optimization.sql /
// 20260722120000_v9_add_graph_storage_optimization_parameter.sql for the
// three columns added afterwards), and registers a t.Cleanup to drop it so
// other packages sharing this test database aren't left with a stray table
// the poller doesn't otherwise expect.
func createDatapipeStatusTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	const ddl = `CREATE TABLE IF NOT EXISTS datapipe_status (
		singleton boolean DEFAULT true NOT NULL,
		status text NOT NULL,
		updated_at timestamp with time zone NOT NULL,
		last_complete_analysis_at timestamp with time zone,
		last_analysis_run_at timestamp with time zone,
		last_complete_optimize_at timestamp with time zone,
		next_scheduled_analysis_at timestamp with time zone,
		CONSTRAINT singleton_uni CHECK (singleton)
	)`

	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("create datapipe_status: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS datapipe_status"); err != nil {
			t.Errorf("drop datapipe_status: %v", err)
		}
	})
}

// waitForFreshSnapshot polls eng.Fresh() until it reports a fresh snapshot
// or timeout elapses, failing the test on timeout.
func waitForFreshSnapshot(t *testing.T, eng *Engine, timeout time.Duration) *snapshot.Snapshot {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if snap, fresh := eng.Fresh(); fresh {
			return snap
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for a fresh snapshot", timeout)
	return nil
}

// refusalWarnMsg is the exact message RebuildNow logs (engine.go) when a
// freshly loaded snapshot exceeds cfg.MemoryLimit -- duplicated here (rather
// than exported from engine.go) since only this test needs to recognize it.
const refusalWarnMsg = "bloodtrail: snapshot rebuild refused: exceeds memory limit"

// countingHandler is a minimal slog.Handler that counts log records by
// message, so a test can assert how many times a specific event was logged
// without parsing text output. It also tracks, per distinct "trigger" attr
// value seen on a "bloodtrail: snapshot rebuilt" record, how many times that
// trigger was logged -- so a test can tell which decideRebuild rule actually
// drove an observed rebuild, not just that some rebuild happened. Safe for
// concurrent use (the poller goroutine logs while the test goroutine reads
// counts).
type countingHandler struct {
	mu       sync.Mutex
	counts   map[string]int
	triggers map[string]int
}

func newCountingHandler() *countingHandler {
	return &countingHandler{counts: make(map[string]int), triggers: make(map[string]int)}
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[r.Message]++

	if r.Message == "bloodtrail: snapshot rebuilt" {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "trigger" {
				h.triggers[a.Value.String()]++
			}
			return true
		})
	}
	return nil
}

func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *countingHandler) WithGroup(string) slog.Handler      { return h }

func (h *countingHandler) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[msg]
}

// triggerCount returns how many "bloodtrail: snapshot rebuilt" records
// carried the given trigger attr value (see Handle).
func (h *countingHandler) triggerCount(trigger string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.triggers[trigger]
}

// waitForAttempts polls eng.rebuildAttempts until it reaches at least want,
// or fails the test on timeout. Deliberately used instead of polling a log
// message count directly: rebuildAttempts is incremented via a defer at the
// very end of RebuildNow (see its doc), strictly after that call's own
// logging decision has already run, so once this returns, any log line the
// just-completed attempt was going to emit is already guaranteed recorded
// -- a caller can check a log count immediately afterward without a
// separate wait or race.
func waitForAttempts(t *testing.T, eng *Engine, want uint64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if eng.rebuildAttempts.Load() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for rebuildAttempts to reach %d; got %d", timeout, want, eng.rebuildAttempts.Load())
}

// TestPollerMemoryLimitRefusalDoesNotSpamRetries is the regression test for
// the finding this fix addresses: a rebuild refused for exceeding
// cfg.MemoryLimit while rule (c) applies (a write landed while the datapipe
// is idle) must not be retried -- neither a fresh LoadSnapshot attempt
// (rebuildAttempts) nor a repeated warning log (refusalWarnMsg) -- on every
// subsequent tick indefinitely; only a genuinely new write should reopen it,
// and doing so is itself observable as a second LoadSnapshot attempt even
// though the warning for it stays rate-limited (refusalLogInterval) so
// close after the first -- the two independent halves of the fix
// (decideRebuild's generation-based retry gate, and RebuildNow's log rate
// limit) each verified doing exactly their own job.
//
// cfg.MemoryLimit is set to a real, fixture-size-derived threshold (strictly
// between a small graph's and a bigger graph's ApproxBytes(), both measured
// directly via LoadSnapshot rather than hardcoded) so the poller's first
// build (small graph) succeeds and adopts, and a later build (small + big
// graph, after a write grows it) is refused -- reproducing the finding's
// exact scenario against a live snapshot loaded from PostgreSQL rather than
// a synthetic *snapshot.Snapshot.
func TestPollerMemoryLimitRefusalDoesNotSpamRetries(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	// Measure ApproxBytes() for the small graph (fixture 1 alone) and the
	// bigger graph (both fixtures loaded additively, same composition
	// TestTryAllShortestPathsDifferential uses) to derive a MemoryLimit
	// strictly between them.
	graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)
	smallSnap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot (small): %v", err)
	}
	smallBytes := smallSnap.ApproxBytes()

	graphtest.LoadDataset(t, pgDriver, adcsFixturePath)
	bigSnap, err := LoadSnapshot(ctx, pgDriver, pool)
	if err != nil {
		t.Fatalf("LoadSnapshot (big): %v", err)
	}
	bigBytes := bigSnap.ApproxBytes()

	if bigBytes <= smallBytes {
		t.Fatalf("test fixtures too similar in size to build a MemoryLimit threshold between them: small=%d big=%d", smallBytes, bigBytes)
	}
	memoryLimit := size.Size(smallBytes + (bigBytes-smallBytes)/2)

	// Reset to just the small graph: the poller's first build must succeed
	// under memoryLimit. adcsFixturePath is loaded again later, after Start,
	// to simulate the write that grows the graph past the limit.
	graphtest.WipeGraph(t, pgDriver)
	graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	createDatapipeStatusTable(t, pool)

	stamp := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx,
		`INSERT INTO datapipe_status (singleton, status, updated_at, last_complete_analysis_at) VALUES (true, 'idle', now(), $1)`,
		stamp,
	); err != nil {
		t.Fatalf("insert datapipe_status: %v", err)
	}

	const pollInterval = 20 * time.Millisecond
	handler := newCountingHandler()
	eng := New(pgDriver, pool, Config{
		Enabled:      true,
		PollInterval: pollInterval,
		MemoryLimit:  memoryLimit,
		Log:          slog.New(handler),
	})

	eng.Start(ctx)
	defer eng.Stop()

	// First build: the small graph fits under memoryLimit (rule (a)), so it
	// is adopted without any refusal. baseline is the one RebuildNow call
	// that took (rule (a) only ever fires once); every assertion below
	// counts rebuildAttempts relative to it rather than assuming an
	// absolute value, since RebuildNow is also called for that first,
	// successful build.
	waitForFreshSnapshot(t, eng, 2*time.Second)
	if n := handler.count(refusalWarnMsg); n != 0 {
		t.Fatalf("refusal warning logged %d time(s) before any refusal happened", n)
	}
	baseline := eng.rebuildAttempts.Load()

	// A write grows the graph past memoryLimit while the datapipe stays
	// idle: rule (c) fires, RebuildNow loads the bigger graph, and refuses
	// to adopt it -- the first refusal, so it must be logged. Waiting on
	// rebuildAttempts rather than the log count directly matters here:
	// rebuildAttempts is incremented via defer, strictly after RebuildNow's
	// own logging decision has already run (see its doc), so once the test
	// observes the count advance, the corresponding log line (if any) is
	// already guaranteed recorded -- checking the log count immediately
	// afterward, with no separate wait, is then race-free.
	graphtest.LoadDataset(t, pgDriver, adcsFixturePath)
	eng.NoteWrite(nil)

	waitForAttempts(t, eng, baseline+1, 2*time.Second)
	if n := handler.count(refusalWarnMsg); n != 1 {
		t.Fatalf("refusal warning logged %d time(s) right after the first refusal, want exactly 1", n)
	}

	// The core of the fix: with no further write and the same refused
	// reading, the poller must not retry for many more ticks -- neither a
	// fresh LoadSnapshot attempt (rebuildAttempts) nor the warning log may
	// climb, even though every one of these ticks sees the exact same
	// "!fresh && idle" condition that used to retry unconditionally.
	time.Sleep(15 * pollInterval)
	if n := eng.rebuildAttempts.Load(); n != baseline+1 {
		t.Fatalf("rebuildAttempts = %d after 15 idle ticks with no new write, want exactly %d (no LoadSnapshot retry until state changes)", n, baseline+1)
	}
	if n := handler.count(refusalWarnMsg); n != 1 {
		t.Fatalf("refusal warning logged %d time(s) after 15 idle ticks with no new write, want exactly 1", n)
	}

	// The over-budget snapshot must never have been adopted.
	if _, fresh := eng.Fresh(); fresh {
		t.Fatalf("Fresh() reports fresh after a memory-limit refusal; the over-budget snapshot must not have been adopted")
	}

	// A second write while still over budget (the graph itself is
	// unchanged, still bigger than memoryLimit) is a genuinely new trigger
	// condition: the write-generation counter advances past
	// refusedGeneration, so rule (c) must retry -- a second real
	// LoadSnapshot attempt, refused again. refusalLogInterval (10 minutes)
	// is nowhere near elapsed since the first refusal a few ticks ago, so
	// this second refusal's warning must stay rate-limited: rebuildAttempts
	// climbs to baseline+2, but the log count stays at 1 -- the two halves
	// of the fix (decideRebuild's gate vs. RebuildNow's log rate limit)
	// each doing exactly their own job.
	eng.NoteWrite(nil)

	waitForAttempts(t, eng, baseline+2, 2*time.Second)
	if n := handler.count(refusalWarnMsg); n != 1 {
		t.Fatalf("refusal warning logged %d time(s) after the second refusal, want exactly 1 (rate-limited: refusalLogInterval has not elapsed since the first)", n)
	}

	stopped := make(chan struct{})
	go func() {
		eng.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(1 * time.Second):
		t.Fatalf("Stop did not return within 1s")
	}
}
