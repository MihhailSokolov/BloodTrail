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
// idle), the no-op case (fresh snapshot, unchanged stamp), a case where the
// snapshot is stale but the datapipe is still busy (so (c) must not fire),
// and the memory-limit-refusal-remembered interaction: a remembered refusal
// suppresses retrying rules (a)/(b) against the same stamp, retrying resumes
// once the stamp itself advances, and -- the fix for the "retries every tick
// under a remembered refusal" finding -- a remembered refusal *also*
// suppresses rule (c) until either the stamp advances or a new write lands
// (the live write-generation counter advances past the generation the
// refusal was recorded at): (i) same generation, no rebuild; (ii) generation
// advanced, rebuild; (iii) stamp advanced, rebuild (generation unchanged).
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
			name:   "stale but not idle: datapipe still busy, (c) must not fire",
			st:     &pollState{},
			status: "analyzing",
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
// it has already decided (via decideRebuild) to rebuild for, including two
// remembered-refusal cases mirroring TestDecideRebuild: rule (b)'s textual
// condition can hold even when a remembered refusal means rule (c) is
// actually what justified rebuilding, and the label must reflect that --
// both when the refusal is lifted by a generation advance and when it is
// lifted by a stamp advance.
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pickTrigger(tc.st, tc.stamp, tc.snap); got != tc.want {
				t.Fatalf("pickTrigger() = %q, want %q", got, tc.want)
			}
		})
	}
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
