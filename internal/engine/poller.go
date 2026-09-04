// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Trigger labels a RebuildNow caller supplies, logged in the "trigger" attr
// on both the "bloodtrail: snapshot rebuilt" event and a memory-limit
// refusal warning. triggerManual is what every call outside the poller uses
// (this milestone's tests, and any future admin-triggered rebuild); the
// other three name which decideRebuild rule the poller fired for.
const (
	triggerStartup   = "startup"    // rule (a): no snapshot exists yet.
	triggerAnalysis  = "analysis"   // rule (b): a completed analysis run stamped a newer last_complete_analysis_at.
	triggerIdleStale = "idle_stale" // rule (c): a write invalidated the snapshot and the datapipe is idle.
	triggerManual    = "manual"
)

// queryErrorLogInterval rate-limits the poller's "query datapipe_status
// failed" warning: a database outage that outlasts a few ticks should not
// spam the log once per PollInterval.
const queryErrorLogInterval = 10 * time.Minute

// pollState is the poller's memory carried between ticks. The zero value is
// ready to use.
type pollState struct {
	// lastStamp and refused together remember a RebuildNow attempt that was
	// refused for exceeding cfg.MemoryLimit (Engine.overBudget): lastStamp is
	// the last_complete_analysis_at reading that attempt was made for, and
	// refused records that the refusal happened. decideRebuild consults both
	// (never lastStamp alone -- a genuinely NULL last_complete_analysis_at
	// reads back as the zero time.Time, which must not be confused with "no
	// refusal has ever happened") to avoid retrying rule (a) or (b) every
	// tick against an unchanged reading: nothing has changed since the
	// refusal, so retrying would just repeat it, and its warning log, every
	// PollInterval forever. Both are cleared -- refused back to false -- the
	// moment a rebuild actually succeeds, and the gate itself lifts as soon
	// as a genuinely newer stamp is observed (see remembered).
	//
	// Rule (c) is deliberately not gated by this memory: it is driven by
	// staleness + idle status, not by stamp, and this milestone does not
	// track enough history (prior status, prior generation) to recognize
	// when *that* trigger condition has "changed" versus merely persisted.
	// A memory-limit refusal while rule (c) applies therefore does retry
	// every tick -- a known, deliberate simplification (see the task
	// report); this is not the common case (rule (c) fires only right after
	// a write while the pipeline happens to be idle) and each retry is still
	// cheap relative to a full poll cycle.
	lastStamp time.Time
	refused   bool

	// lastFailureLogged is the last time the poller logged a
	// "query datapipe_status failed" warning, rate-limited to
	// queryErrorLogInterval.
	lastFailureLogged time.Time
}

// remembered reports whether st holds a memory-limit refusal that still
// applies to stamp: a refusal was recorded, and stamp has not advanced past
// the reading it was recorded for.
func remembered(st *pollState, stamp time.Time) bool {
	return st.refused && !stamp.After(st.lastStamp)
}

// decideRebuild is the poller's tick decision, extracted as a pure function
// for unit testing (side-effect-free: it only reads st, never writes it).
// status and stamp are this tick's datapipe_status.status and
// .last_complete_analysis_at reading; snap and fresh are the engine's
// current Fresh() result.
//
// Rebuild when:
//
//	(a) no snapshot exists yet (snap == nil);
//	(b) stamp is newer than the current snapshot's AnalysisStamp -- a
//	    completed analysis run the engine hasn't picked up yet;
//	(c) the snapshot is stale (a write landed since it was built, so
//	    !fresh) and the datapipe is currently idle -- catch up now rather
//	    than wait for the next analysis to complete.
//
// Rules (a) and (b) are both driven by the same stamp reading, so a
// remembered memory-limit refusal (see pollState) suppresses both until
// stamp itself advances past it. Rule (c) is independent of both and is
// never suppressed by a remembered refusal (see pollState's doc).
func decideRebuild(st *pollState, status string, stamp time.Time, snap *snapshot.Snapshot, fresh bool) bool {
	if snap == nil {
		return !remembered(st, stamp)
	}

	ruleB := stamp.After(snap.AnalysisStamp) && !remembered(st, stamp)
	ruleC := !fresh && status == "idle"

	return ruleB || ruleC
}

// pickTrigger names which decideRebuild rule justified a rebuild, for a
// tick that has already decided (via decideRebuild) to rebuild. It mirrors
// decideRebuild's own rule order, including the remembered-refusal
// suppression of rule (b): if a remembered refusal means (b)'s bare
// condition no longer counts, a rebuild can still be happening (via (c)),
// and the label must say idle_stale in that case, not analysis.
func pickTrigger(st *pollState, stamp time.Time, snap *snapshot.Snapshot) string {
	switch {
	case snap == nil:
		return triggerStartup
	case stamp.After(snap.AnalysisStamp) && !remembered(st, stamp):
		return triggerAnalysis
	default:
		return triggerIdleStale
	}
}

// Start launches the poller goroutine, which ticks every cfg.PollInterval
// and rebuilds the snapshot whenever decideRebuild says to. A no-op when
// !cfg.Enabled -- no goroutine is started, and Stop is then also a no-op.
//
// Start is meant to be called once, followed by exactly one Stop; it is not
// itself idempotent (calling it twice starts two poller goroutines).
func (e *Engine) Start(ctx context.Context) {
	if !e.cfg.Enabled {
		return
	}

	e.pollStop = make(chan struct{})
	e.pollDone = make(chan struct{})

	go e.runPoller(ctx)
}

// Stop signals the poller goroutine to exit and waits for it to actually
// exit before returning. Idempotent, and safe to call even if Start was
// never called (or was a no-op because !cfg.Enabled).
func (e *Engine) Stop() {
	e.pollStopOnce.Do(func() {
		if e.pollStop == nil {
			// Start either was never called, or was a no-op: no goroutine
			// exists to signal or wait for.
			return
		}
		close(e.pollStop)
		<-e.pollDone
	})
}

// runPoller is the poller goroutine body launched by Start: it ticks every
// cfg.PollInterval, running one tick per tick, until Stop closes e.pollStop
// or ctx is done. It closes e.pollDone on exit so Stop can wait for it.
func (e *Engine) runPoller(ctx context.Context) {
	defer close(e.pollDone)

	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	st := &pollState{}

	for {
		select {
		case <-e.pollStop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tick(ctx, st)
		}
	}
}

// tick runs one poll iteration: read datapipe_status, ask decideRebuild
// whether to rebuild, and if so call RebuildNow with the trigger it picked.
//
// A query error is logged at most once per queryErrorLogInterval and the
// poller keeps ticking (st is otherwise untouched, so the next tick tries
// again immediately). A RebuildNow error is logged every time it happens and
// the poller retries next tick -- unlike a memory-limit refusal (returned as
// success, e.overBudget = true), which tick remembers in st so decideRebuild
// stops retrying the same reading.
func (e *Engine) tick(ctx context.Context, st *pollState) {
	var (
		status   string
		rawStamp pgtype.Timestamptz
	)

	if err := e.pool.QueryRow(ctx, "SELECT status, last_complete_analysis_at FROM datapipe_status LIMIT 1").Scan(&status, &rawStamp); err != nil {
		if st.lastFailureLogged.IsZero() || time.Since(st.lastFailureLogged) >= queryErrorLogInterval {
			e.cfg.Log.WarnContext(ctx, "bloodtrail: poller: query datapipe_status failed", slog.Any("error", err))
			st.lastFailureLogged = time.Now()
		}
		return
	}

	var stamp time.Time
	if rawStamp.Valid {
		stamp = rawStamp.Time
	}

	snap, fresh := e.Fresh()
	if !decideRebuild(st, status, stamp, snap, fresh) {
		return
	}

	trigger := pickTrigger(st, stamp, snap)

	if err := e.RebuildNow(ctx, trigger, stamp); err != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: poller: rebuild failed", slog.String("trigger", trigger), slog.Any("error", err))
		return
	}

	if e.overBudget.Load() {
		st.lastStamp = stamp
		st.refused = true
	} else {
		st.refused = false
	}
}
