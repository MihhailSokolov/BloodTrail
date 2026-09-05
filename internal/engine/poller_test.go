// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestDecideRebuild is the poller's truth table: every rule from the task
// brief ((a) no snapshot yet, (b) a newer analysis stamp, (c) stale +
// idle, (d) stale + analyzing, capped per episode), the no-op case (fresh
// snapshot, unchanged stamp), a case where the snapshot is stale but the
// datapipe is still busy (so (c) must not fire), the no-ingest re-analysis
// case (analyzing but still fresh, so (d) must not fire), rule (d)'s
// per-episode cap, and the memory-limit-refusal-remembered interaction: a
// remembered refusal suppresses retrying rules (a)/(b) against the same
// stamp, retrying resumes once the stamp itself advances, and -- the fix for
// the "retries every tick under a remembered refusal" finding -- a
// remembered refusal *also* suppresses rules (c) and (d) until either the
// stamp advances or a new write lands (the live write-generation counter
// advances past the generation the refusal was recorded at): for rule (c),
// (i) same generation, no rebuild; (ii) generation advanced, rebuild; (iii)
// stamp advanced, rebuild (generation unchanged); rule (d) mirrors the same
// three cases as (iv)-(vi).
func TestDecideRebuild(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	t2 := t1.Add(time.Hour)

	cases := []struct {
		name       string
		st         *pollState
		status     string
		stamp      time.Time
		snap       *snapshot.Snapshot
		fresh      bool
		generation uint64
		want       bool
	}{
		{
			name:   "rule (a): no snapshot yet",
			st:     &pollState{},
			status: "idle",
			stamp:  t0,
			snap:   nil,
			fresh:  false,
			want:   true,
		},
		{
			name:   "rule (b): newer stamp on an existing snapshot",
			st:     &pollState{},
			status: "analyzing",
			stamp:  t1,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:  true,
			want:   true,
		},
		{
			name:   "rule (c): stale snapshot, datapipe idle",
			st:     &pollState{},
			status: "idle",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:  false,
			want:   true,
		},
		{
			name:   "no-op: fresh snapshot, unchanged stamp",
			st:     &pollState{},
			status: "idle",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:  true,
			want:   false,
		},
		{
			// status is "ingesting" here rather than "analyzing" precisely
			// because "analyzing" is no longer a generic stand-in for "any
			// non-idle status" now that rule (d) exists for it -- this case
			// is about a status that is neither rule (c)'s idle nor rule
			// (d)'s analyzing, so neither may fire; see "rule (d): stale
			// snapshot, datapipe analyzing" below for the analyzing case,
			// which now legitimately does rebuild.
			name:   "stale but not idle or analyzing: datapipe still busy, neither (c) nor (d) fires",
			st:     &pollState{},
			status: "ingesting",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:  false,
			want:   false,
		},
		{
			name:   "memory-limit refusal remembered: same stamp, no snapshot yet",
			st:     &pollState{lastStamp: t1, refused: true},
			status: "idle",
			stamp:  t1,
			snap:   nil,
			fresh:  false,
			want:   false,
		},
		{
			name:   "memory-limit refusal remembered but stamp advanced: retry",
			st:     &pollState{lastStamp: t1, refused: true},
			status: "idle",
			stamp:  t2,
			snap:   nil,
			fresh:  false,
			want:   true,
		},
		{
			// (i): the finding's exact scenario -- a rebuild was refused for
			// exceeding MemoryLimit while rule (c) applied (a write landed
			// while idle), and nothing has changed since: same stamp, no
			// further writes (generation unchanged from refusedGeneration).
			// Both (b) (via remembered) and (c) (via refusalLifted) must stay
			// suppressed -- retrying would just repeat the same refused
			// LoadSnapshot attempt every tick.
			name:       "(i) remembered refusal, no new writes: rule (c) suppressed too",
			st:         &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status:     "idle",
			stamp:      t1,
			snap:       &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:      false,
			generation: 7,
			want:       false,
		},
		{
			// (ii): same remembered refusal as (i), but a *new* write landed
			// since (generation advanced past refusedGeneration). That is a
			// genuinely new trigger condition, not a repeat of the refused
			// one, so rule (c) must retry.
			name:       "(ii) remembered refusal, generation advanced: rule (c) retries",
			st:         &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status:     "idle",
			stamp:      t1,
			snap:       &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:      false,
			generation: 8,
			want:       true,
		},
		{
			// (iii): same remembered refusal, generation unchanged, but the
			// stamp itself advanced (a completed analysis run landed).
			// snap.AnalysisStamp is set to the same new stamp so rule (b)'s
			// own condition (stamp.After(snap.AnalysisStamp)) is false --
			// isolating that it is rule (c), not rule (b), unblocked by the
			// stamp advance here.
			name:       "(iii) remembered refusal, stamp advanced: rule (c) retries",
			st:         &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status:     "idle",
			stamp:      t2,
			snap:       &snapshot.Snapshot{AnalysisStamp: t2},
			fresh:      false,
			generation: 7,
			want:       true,
		},
		{
			// rule (d): the snapshot went stale while the datapipe is
			// analyzing (not idle, so (c) does not apply). st is a fresh
			// pollState -- analyzingRebuilds at its zero value, exactly what
			// tick's resetAnalyzingRebuilds would leave behind either at the
			// very start of a new analyzing episode, or once a prior episode
			// has ended and a new one begun.
			name:   "rule (d): stale snapshot, datapipe analyzing",
			st:     &pollState{},
			status: "analyzing",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:  false,
			want:   true,
		},
		{
			// The no-ingest re-analysis case: analysis re-ran without any
			// ingest happening first, so nothing wrote and the snapshot
			// stayed generation-fresh throughout. Rule (d) must not fire --
			// there is nothing to catch up on -- and the snapshot already
			// being served just keeps serving.
			name:   "no-op: datapipe analyzing but snapshot still fresh (no-ingest re-analysis)",
			st:     &pollState{},
			status: "analyzing",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:  true,
			want:   false,
		},
		{
			// The cap: this episode has already used up both of its rule
			// (d) rebuilds (maxAnalyzingRebuilds == 2), so a third stale
			// reading during the same analyzing episode must not trigger
			// another one -- otherwise derived-kind writes advancing the
			// generation every tick for the rest of analysis would rebuild
			// in a loop.
			name:   "rule (d) capped: episode already used its two rebuilds",
			st:     &pollState{analyzingRebuilds: maxAnalyzingRebuilds},
			status: "analyzing",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:  false,
			want:   false,
		},
		{
			// (iv): rule (d)'s own memory-refusal interplay, mirroring (i)
			// above but for status "analyzing" instead of "idle": a refusal
			// recorded while rule (d) applied, with nothing changed since
			// (same stamp, same generation), must stay suppressed.
			// snap.AnalysisStamp == t0 < stamp keeps rule (b) textually
			// true, but remembered(st, stamp) suppresses it too, isolating
			// that this case is purely about rule (d).
			name:       "(iv) remembered refusal during analyzing, no new writes: rule (d) suppressed",
			st:         &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status:     "analyzing",
			stamp:      t1,
			snap:       &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:      false,
			generation: 7,
			want:       false,
		},
		{
			// (v): same remembered refusal as (iv), but a new write landed
			// (generation advanced past refusedGeneration) -- rule (d)
			// retries, mirroring rule (c)'s (ii).
			name:       "(v) remembered refusal during analyzing, generation advanced: rule (d) retries",
			st:         &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status:     "analyzing",
			stamp:      t1,
			snap:       &snapshot.Snapshot{AnalysisStamp: t0},
			fresh:      false,
			generation: 8,
			want:       true,
		},
		{
			// (vi): same remembered refusal, generation unchanged, but the
			// stamp itself advanced -- rule (d) retries via the
			// stamp-advance half of refusalLifted, mirroring rule (c)'s
			// (iii). snap.AnalysisStamp is set to the same new stamp so rule
			// (b)'s own bare condition is false too, isolating that it is
			// rule (d) unblocking this, not rule (b).
			name:       "(vi) remembered refusal during analyzing, stamp advanced: rule (d) retries",
			st:         &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status:     "analyzing",
			stamp:      t2,
			snap:       &snapshot.Snapshot{AnalysisStamp: t2},
			fresh:      false,
			generation: 7,
			want:       true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideRebuild(tc.st, tc.status, tc.stamp, tc.snap, tc.fresh, tc.generation)
			if got != tc.want {
				t.Fatalf("decideRebuild() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPickTrigger checks the trigger label the poller attributes to a tick
// it has already decided (via decideRebuild) to rebuild for, including
// remembered-refusal cases mirroring TestDecideRebuild: rule (b)'s textual
// condition can hold even when a remembered refusal means rule (c) or (d)
// is actually what justified rebuilding, and the label must reflect that --
// both when the refusal is lifted by a generation advance and when it is
// lifted by a stamp advance. It also checks rule (d)'s own label
// (triggerAnalyzing) and that rule (b)'s case still takes priority over it:
// a stamp advance while status happens to read "analyzing" must still be
// labeled analysis, not analyzing, since it was a completed analysis run
// that justified the rebuild, not the analyzing-phase catch-up rule.
func TestPickTrigger(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	t2 := t1.Add(time.Hour)

	cases := []struct {
		name   string
		st     *pollState
		status string
		stamp  time.Time
		snap   *snapshot.Snapshot
		want   string
	}{
		{
			name:   "no snapshot yet => startup",
			st:     &pollState{},
			status: "idle",
			stamp:  t0,
			snap:   nil,
			want:   triggerStartup,
		},
		{
			name:   "newer stamp => analysis",
			st:     &pollState{},
			status: "analyzing",
			stamp:  t1,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			want:   triggerAnalysis,
		},
		{
			name:   "stale + idle, stamp unchanged => idle_stale",
			st:     &pollState{},
			status: "idle",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			want:   triggerIdleStale,
		},
		{
			name:   "remembered refusal: rule (b) textually true but suppressed => idle_stale, not analysis",
			st:     &pollState{lastStamp: t1, refused: true},
			status: "idle",
			stamp:  t1,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			want:   triggerIdleStale,
		},
		{
			// Mirrors TestDecideRebuild's case (iii): a remembered refusal
			// whose stamp has since advanced (lifting the suppression for
			// rule (c)) but whose new stamp equals snap.AnalysisStamp, so
			// rule (b)'s own bare condition is false too -- the label must
			// still default to idle_stale, not misattribute the rebuild to
			// rule (b).
			name:   "remembered refusal, stamp advanced to match AnalysisStamp => idle_stale",
			st:     &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status: "idle",
			stamp:  t2,
			snap:   &snapshot.Snapshot{AnalysisStamp: t2},
			want:   triggerIdleStale,
		},
		{
			name:   "stale + analyzing, stamp unchanged => analyzing",
			st:     &pollState{},
			status: "analyzing",
			stamp:  t0,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			want:   triggerAnalyzing,
		},
		{
			// Mirrors the idle-status "remembered refusal ... => idle_stale"
			// case above, but for status "analyzing": rule (b)'s bare
			// condition holds textually (stamp t1 is after AnalysisStamp
			// t0), but the remembered refusal suppresses it, so the label
			// must fall through to rule (d)'s triggerAnalyzing, not
			// misattribute the rebuild to rule (b).
			name:   "remembered refusal during analyzing: rule (b) textually true but suppressed => analyzing, not analysis",
			st:     &pollState{lastStamp: t1, refused: true, refusedGeneration: 7},
			status: "analyzing",
			stamp:  t1,
			snap:   &snapshot.Snapshot{AnalysisStamp: t0},
			want:   triggerAnalyzing,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickTrigger(tc.st, tc.stamp, tc.snap, tc.status); got != tc.want {
				t.Fatalf("pickTrigger() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestAnalyzingRebuildsBookkeeping unit-tests the two small pollState
// mutations tick performs around rule (d) (see tick's doc comment):
// resetAnalyzingRebuilds clears analyzingRebuilds the moment status stops
// reading "analyzing" (so the next analyzing episode gets its own fresh
// budget of maxAnalyzingRebuilds instead of inheriting whatever the
// previous episode used up), and recordAnalyzingRebuild increments it
// whenever a rebuild actually ran labeled triggerAnalyzing -- unconditionally
// on the outcome, so a rebuild RebuildNow went on to refuse for exceeding
// cfg.MemoryLimit still counts against the cap, same as an adopted one
// would, and ignoring any other trigger label entirely.
func TestAnalyzingRebuildsBookkeeping(t *testing.T) {
	t.Run("resets when status leaves analyzing", func(t *testing.T) {
		st := &pollState{analyzingRebuilds: 2}
		resetAnalyzingRebuilds(st, "idle")
		if st.analyzingRebuilds != 0 {
			t.Fatalf("analyzingRebuilds = %d, want 0 after a non-analyzing tick", st.analyzingRebuilds)
		}
	})

	t.Run("stays put while status is still analyzing", func(t *testing.T) {
		st := &pollState{analyzingRebuilds: 1}
		resetAnalyzingRebuilds(st, "analyzing")
		if st.analyzingRebuilds != 1 {
			t.Fatalf("analyzingRebuilds = %d, want unchanged 1 while status is still analyzing", st.analyzingRebuilds)
		}
	})

	t.Run("increments on an analyzing-triggered rebuild", func(t *testing.T) {
		st := &pollState{analyzingRebuilds: 0}
		recordAnalyzingRebuild(st, triggerAnalyzing)
		if st.analyzingRebuilds != 1 {
			t.Fatalf("analyzingRebuilds = %d, want 1 after an analyzing-triggered rebuild", st.analyzingRebuilds)
		}
	})

	t.Run("increments regardless of a subsequent memory-limit refusal", func(t *testing.T) {
		// recordAnalyzingRebuild has no overBudget parameter at all: tick
		// calls it whenever trigger == triggerAnalyzing, before it ever
		// looks at e.overBudget.Load(), so a refused rebuild counts against
		// the cap exactly like an adopted one -- this test just documents
		// that recordAnalyzingRebuild's contract does not distinguish the
		// two by construction.
		st := &pollState{analyzingRebuilds: 1}
		recordAnalyzingRebuild(st, triggerAnalyzing)
		if st.analyzingRebuilds != 2 {
			t.Fatalf("analyzingRebuilds = %d, want 2", st.analyzingRebuilds)
		}
	})

	t.Run("ignores rebuilds triggered by other rules", func(t *testing.T) {
		st := &pollState{analyzingRebuilds: 0}
		recordAnalyzingRebuild(st, triggerIdleStale)
		if st.analyzingRebuilds != 0 {
			t.Fatalf("analyzingRebuilds = %d, want unchanged 0 for a non-analyzing trigger", st.analyzingRebuilds)
		}
	})
}

// The tests below exercise Start/Stop's goroutine lifecycle without a
// database: every Engine here has PollInterval: time.Hour, so the poller's
// ticker never fires within these tests' short lifetime and e.tick (which
// would dereference the nil pool) never actually runs -- what's under test
// is purely the signal-driven shutdown path (pollStop/ctx.Done), not the
// tick logic itself (covered by TestDecideRebuild and the DB-backed
// TestPollerDrivesRebuilds).

// TestStopWithoutStart checks Stop is safe to call when Start never ran --
// e.pollStop is still its nil zero value, and Stop must recognize that
// rather than panic trying to close a nil channel.
func TestStopWithoutStart(t *testing.T) {
	e := New(nil, nil, Config{})
	e.Stop()
}

// TestStartDisabledIsNoop checks Start launches no goroutine when
// !cfg.Enabled: e.pollStop stays nil, and a subsequent Stop is still safe.
func TestStartDisabledIsNoop(t *testing.T) {
	e := New(nil, nil, Config{Enabled: false})
	e.Start(context.Background())

	if e.pollStop != nil {
		t.Fatalf("Start launched a poller goroutine despite !cfg.Enabled")
	}

	e.Stop()
}

// TestStopIdempotent checks a second Stop call is a safe no-op rather than
// blocking (waiting on an already-closed pollDone would be fine, but
// double-closing pollStop would panic without pollStopOnce).
func TestStopIdempotent(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true, PollInterval: time.Hour})
	e.Start(context.Background())

	e.Stop()
	e.Stop()
}

// TestStartStopExitsPromptly checks Stop's "waits for the goroutine" promise
// is driven by the pollStop signal, not by waiting out the ticker: with
// PollInterval an hour, Stop returning quickly can only mean the goroutine
// noticed pollStop closing.
func TestStartStopExitsPromptly(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true, PollInterval: time.Hour})
	e.Start(context.Background())

	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("Stop did not return promptly; poller goroutine did not exit on signal")
	}
}

// TestStartExitsOnContextCancel checks the poller goroutine also exits when
// ctx is done, not just on an explicit Stop -- again distinguishable from
// "waited out the ticker" only because PollInterval is an hour.
func TestStartExitsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := New(nil, nil, Config{Enabled: true, PollInterval: time.Hour})
	e.Start(ctx)
	cancel()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case <-e.pollDone:
			return
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	t.Fatalf("poller goroutine did not exit after ctx cancellation")
}
