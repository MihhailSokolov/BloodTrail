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
// other four name which decideRebuild rule the poller fired for.
const (
	triggerStartup   = "startup"    // rule (a): no snapshot exists yet.
	triggerAnalysis  = "analysis"   // rule (b): a completed analysis run stamped a newer last_complete_analysis_at.
	triggerIdleStale = "idle_stale" // rule (c): a write invalidated the snapshot and the datapipe is idle.
	triggerAnalyzing = "analyzing"  // rule (d): a write invalidated the snapshot and the datapipe is analyzing (capped, see maxAnalyzingRebuilds).
	triggerManual    = "manual"
)

// maxAnalyzingRebuilds caps how many rebuilds rule (d) (see decideRebuild)
// may drive per analyzing episode (see pollState.analyzingRebuilds).
// Analysis's own writes during the episode -- mostly derived edge kinds and
// tag node kinds -- keep the write-generation counter advancing
// continuously for as long as analysis runs, so an uncapped rule (d) would
// rebuild on every single tick for the run's entire (potentially long)
// duration. Two is enough to restore kind-clean serving for the long
// post-processing tail that follows: once when analysis starts (picking up
// the source-kind state analysis is about to read) and once more if that
// first rebuild itself went stale again from analysis's own early
// node/kind writes.
const maxAnalyzingRebuilds = 2

// queryErrorLogInterval rate-limits the poller's "query datapipe_status
// failed" warning: a database outage that outlasts a few ticks should not
// spam the log once per PollInterval.
const queryErrorLogInterval = 10 * time.Minute

// pollState is the poller's memory carried between ticks. The zero value is
// ready to use.
type pollState struct {
	// lastStamp, refusedGeneration and refused together remember a
	// RebuildNow attempt that was refused for exceeding cfg.MemoryLimit
	// (Engine.overBudget): lastStamp is the last_complete_analysis_at
	// reading that attempt was made for, refusedGeneration is the engine's
	// write-generation counter as observed at that same tick, and refused
	// records that the refusal happened. decideRebuild never reads lastStamp
	// or refusedGeneration alone -- a genuinely NULL last_complete_analysis_at
	// reads back as the zero time.Time, and generation 0 is a real value a
	// brand-new Engine starts at, so either alone is indistinguishable from
	// "no refusal has ever happened" without the refused flag.
	//
	// Rules (a) and (b) are both driven by the stamp reading, so a
	// remembered refusal suppresses both until stamp itself advances past
	// lastStamp (see remembered): nothing about *that* trigger condition has
	// changed since the refusal, so retrying would just repeat it, and its
	// warning log, every PollInterval forever.
	//
	// Rule (c) is driven by staleness (write-generation, not stamp) + idle
	// status instead, so it is gated on refusedGeneration rather than
	// lastStamp: a remembered refusal suppresses rule (c) only until a *new*
	// write lands (the live write-generation counter advances past
	// refusedGeneration) or stamp itself advances -- either means the
	// condition that would justify retrying has actually changed, rather
	// than the poller just observing the same still-refused state again
	// (see refusalLifted).
	//
	// All three fields are cleared -- refused back to false -- the moment a
	// rebuild actually succeeds.
	lastStamp         time.Time
	refusedGeneration uint64
	refused           bool

	// analyzingRebuilds counts how many rebuilds rule (d) (see
	// decideRebuild) has driven during the current analyzing episode. It
	// starts at 0, increments (via recordAnalyzingRebuild) each time tick
	// actually runs a rebuild labeled triggerAnalyzing -- regardless of
	// whether that rebuild was then refused for exceeding cfg.MemoryLimit,
	// since a refused attempt must also count against maxAnalyzingRebuilds
	// or a persistently over-budget graph during analysis would retry every
	// tick for the rest of the episode instead of backing off the way every
	// other rule does -- and resets to 0 (via resetAnalyzingRebuilds) the
	// moment a tick observes a status other than "analyzing", so the next
	// analyzing episode starts with its own fresh budget rather than
	// inheriting whatever the previous episode used up.
	analyzingRebuilds int

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

// refusalLifted reports whether a remembered memory-limit refusal (see
// pollState) no longer blocks rule (c) from retrying at generation: either
// no refusal is remembered (or stamp has already advanced past it -- the
// same condition that lifts rules (a)/(b), see remembered), or a new write
// has landed since the refusal was recorded (generation has advanced past
// the value observed at refusal time). A refusal that still applies to both
// stamp and generation means nothing rule (c) cares about has changed since
// the refusal, so retrying would just repeat it.
func refusalLifted(st *pollState, stamp time.Time, generation uint64) bool {
	return !remembered(st, stamp) || generation > st.refusedGeneration
}

// decideRebuild is the poller's tick decision, extracted as a pure function
// for unit testing (side-effect-free: it only reads st, never writes it).
// status and stamp are this tick's datapipe_status.status and
// .last_complete_analysis_at reading; snap and fresh are the engine's
// current Fresh() result; generation is the engine's live write-generation
// counter (Engine.generation) as observed this tick.
//
// Rebuild when:
//
//	(a) no snapshot exists yet (snap == nil);
//	(b) stamp is newer than the current snapshot's AnalysisStamp -- a
//	    completed analysis run the engine hasn't picked up yet;
//	(c) the snapshot is stale (a write landed since it was built, so
//	    !fresh) and the datapipe is currently idle -- catch up now rather
//	    than wait for the next analysis to complete;
//	(d) the snapshot is stale and the datapipe is currently analyzing,
//	    capped at maxAnalyzingRebuilds rebuilds per analyzing episode (see
//	    pollState.analyzingRebuilds).
//
// Rule (d) exists because analysis's post-processing tail can run long
// after it starts, and unlike ingest it does not advance stamp (rule (b)'s
// trigger) until the entire run completes. During that tail, analysis's own
// writes touch mostly derived edge kinds and tag node kinds, while the
// heavy path-finding reads that matter care about source kinds -- so
// rebuilding once analysis starts (picking up the source-kind state
// analysis is about to run against) and once more if that first rebuild
// itself goes stale again from analysis's own early node/kind writes
// restores kind-clean serving for the rest of the tail, rather than leaving
// it stale until rule (b) finally fires at the end. An analysis re-run with
// no ingest beforehand leaves the snapshot generation-fresh throughout
// (fresh stays true, since nothing wrote), so rule (d) never fires in that
// case and the snapshot already loaded just keeps serving. The cap exists
// because analysis's derived-kind writes keep the write-generation counter
// advancing continuously for the whole analyzing phase: without
// maxAnalyzingRebuilds, an uncapped rule (d) would rebuild on every tick for
// the run's entire duration.
//
// Rules (a) and (b) are both driven by the same stamp reading, so a
// remembered memory-limit refusal (see pollState) suppresses both until
// stamp itself advances past it. Rules (c) and (d) are both driven by
// write-generation staleness instead, so a remembered refusal suppresses
// each of them separately, until either stamp advances or a new write lands
// (refusalLifted) -- without this gate, a refusal recorded while rule (c) or
// (d) applies would otherwise retry (and, absent the RebuildNow-side rate
// limit, re-warn) every tick forever, since !fresh stays true until a
// rebuild actually succeeds.
func decideRebuild(st *pollState, status string, stamp time.Time, snap *snapshot.Snapshot, fresh bool, generation uint64) bool {
	if snap == nil {
		return !remembered(st, stamp)
	}

	ruleB := stamp.After(snap.AnalysisStamp) && !remembered(st, stamp)
	ruleC := !fresh && status == "idle" && refusalLifted(st, stamp, generation)
	ruleD := !fresh && status == "analyzing" && st.analyzingRebuilds < maxAnalyzingRebuilds && refusalLifted(st, stamp, generation)

	return ruleB || ruleC || ruleD
}

// pickTrigger names which decideRebuild rule justified a rebuild, for a
// tick that has already decided (via decideRebuild) to rebuild. It mirrors
// decideRebuild's own rule order, including the remembered-refusal
// suppression of rule (b): if a remembered refusal means (b)'s bare
// condition no longer counts, a rebuild can still be happening (via (c) or
// (d)), and the label must reflect whichever one it actually was, not
// misattribute it to analysis. Rule (b)'s case comes before rule (d)'s for
// the same reason: a completed-analysis stamp advance must still be labeled
// analysis even on a tick where status happens to still read "analyzing".
//
// It does not need generation or st.analyzingRebuilds (unlike decideRebuild):
// once (a) and (b) are ruled out, decideRebuild only ever returns true for
// (c) or (d), which -- by decideRebuild's own construction -- apply to
// mutually exclusive status values ("idle" and "analyzing" respectively).
// So status alone disambiguates the two: status == "analyzing" means it was
// (d), and anything else falls through to (c)'s idle_stale label,
// regardless of which part of refusalLifted's gate let either one through.
func pickTrigger(st *pollState, stamp time.Time, snap *snapshot.Snapshot, status string) string {
	switch {
	case snap == nil:
		return triggerStartup
	case stamp.After(snap.AnalysisStamp) && !remembered(st, stamp):
		return triggerAnalysis
	case status == "analyzing":
		return triggerAnalyzing
	default:
		return triggerIdleStale
	}
}

// resetAnalyzingRebuilds clears st.analyzingRebuilds (rule (d)'s per-episode
// counter, see pollState) whenever this tick's status is not "analyzing":
// the analyzing episode rule (d) was counting rebuilds against has ended
// (or one has simply never started), so the next episode that begins gets
// its own fresh budget of maxAnalyzingRebuilds rather than inheriting
// whatever count the previous episode left behind. Extracted from tick as
// its own small pure-ish mutation, the same way decideRebuild and
// pickTrigger were extracted, so it can be unit tested without a database.
func resetAnalyzingRebuilds(st *pollState, status string) {
	if status != "analyzing" {
		st.analyzingRebuilds = 0
	}
}

// recordAnalyzingRebuild increments st.analyzingRebuilds (see pollState)
// whenever tick actually ran a rebuild labeled triggerAnalyzing.
// Deliberately unconditional on the rebuild's outcome: a refused attempt
// (RebuildNow returning success with e.overBudget = true) must count
// against maxAnalyzingRebuilds exactly like an adopted one does, or a
// persistently over-budget graph during analysis would keep retrying every
// tick for the rest of the episode instead of backing off the cap the same
// way every other rebuild does. A trigger other than triggerAnalyzing is a
// no-op.
func recordAnalyzingRebuild(st *pollState, trigger string) {
	if trigger == triggerAnalyzing {
		st.analyzingRebuilds++
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
// Before consulting decideRebuild, tick also maintains st.analyzingRebuilds
// (rule (d)'s per-episode counter -- see pollState, decideRebuild,
// resetAnalyzingRebuilds): a status reading other than "analyzing" resets it
// to 0 so the next analyzing episode starts with a fresh budget. After a
// rebuild actually runs (RebuildNow returned no error) labeled
// triggerAnalyzing, tick increments it via recordAnalyzingRebuild --
// including when that rebuild was refused for exceeding cfg.MemoryLimit, so
// a refused attempt also stops retrying against the cap rather than
// retrying every tick for the rest of the episode.
//
// A query error is logged at most once per queryErrorLogInterval and the
// poller keeps ticking (st is otherwise untouched, so the next tick tries
// again immediately). A RebuildNow error is logged every time it happens and
// the poller retries next tick -- unlike a memory-limit refusal (returned as
// success, e.overBudget = true), which tick remembers in st (both the stamp
// and the write-generation counter as observed this tick) so decideRebuild
// stops retrying rules (a)/(b) against the same stamp and rules (c)/(d)
// against the same write-generation, until either actually moves.
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

	resetAnalyzingRebuilds(st, status)

	snap, fresh := e.Fresh()
	generation := e.generation.Load()
	if !decideRebuild(st, status, stamp, snap, fresh, generation) {
		return
	}

	trigger := pickTrigger(st, stamp, snap, status)

	if err := e.RebuildNow(ctx, trigger, stamp); err != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: poller: rebuild failed", slog.String("trigger", trigger), slog.Any("error", err))
		return
	}

	recordAnalyzingRebuild(st, trigger)

	if e.overBudget.Load() {
		st.lastStamp = stamp
		st.refusedGeneration = generation
		st.refused = true
	} else {
		st.refused = false
	}
}
