// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"time"
)

// triggerStartup labels the RebuildNow calls Start's boot-load goroutine
// makes (runBootLoad, below), alongside apply.go's triggerFallback and
// triggerManual, so a rebuild driven by the initial load is distinguishable
// in the log from a fallback-recovery or test/admin-driven one.
const triggerStartup = "startup"

// triggerManual is what every RebuildNow caller outside Start's boot-load
// goroutine and apply.go's fallback recovery goroutine uses: every test in
// this package that drives a rebuild directly, and any future admin-
// triggered rebuild.
const triggerManual = "manual"

// Start launches the engine's boot-load goroutine, which loads the first
// snapshot from PostgreSQL so the engine can begin serving without waiting
// on any external signal. A no-op when !cfg.Enabled: a disabled engine never
// serves, so there is nothing worth loading, and Stop is then also
// effectively a no-op (nothing was started for it to quiesce).
//
// This replaces the retired poller, whose job was to keep rebuilding a
// snapshot that could fall behind PostgreSQL as writes landed. Write-through
// (apply.go) means a published View already reflects every committed write,
// so the only rebuild left worth doing on a timer is... none: the single
// remaining reason to (re)load from PostgreSQL is to obtain the FIRST
// snapshot at all (this method) or to recover one that Apply gave up
// replaying into (enterFallback's recovery goroutine, apply.go). Both are
// one-shot, retry-until-adopted loads with the identical body
// (runBootLoad/runFallbackRebuild), and -- unlike an earlier version of this
// doc claimed -- they are NOT kept apart by construction: Apply's nil-scope
// and HasFallback branches (apply.go, steps 3-4) call enterFallback BEFORE
// Apply ever checks whether a snapshot has been adopted (step 7), so a
// Run()/WipeGraph()/unrecognized write landing before boot load's first
// adoption calls enterFallback while runBootLoad is still retrying. What
// actually keeps this from becoming two concurrent rebuild loops is
// claimRebuildLoop's shared CAS on fallbackRebuilding (engine.go), which
// Start goes through exactly as startFallbackRebuild does: whichever of the
// two reaches it first runs the one loop that exists; the other finds the
// flag already held and does not start a second one, correctly relying on
// the loop that IS running -- whichever trigger started it -- to adopt a
// snapshot and end both jobs at once (adoptRebuiltView adopts, and clears
// fallback, regardless of which trigger asked for the rebuild that
// succeeds). A genuine pre-boot fallback trip still logs "fallback entered"/
// "fallback exited" honestly around that shared loop's eventual adoption --
// that pairing is a true event (a write really could not be replayed), not
// noise; ordinary startup, with no such write, never calls enterFallback at
// all, so it never logs either line.
//
// Start is meant to be called once, followed by exactly one Stop; it is not
// itself idempotent (a second call's own claimRebuildLoop attempt loses the
// CAS to the first and simply does nothing -- harmless, but still not
// something a caller should do).
//
// ensureWatermarkTable (watermark.go) runs first, unconditionally -- even
// when !cfg.Enabled: the watermark protocol tracks every mutating write
// PostgreSQL ever sees regardless of whether THIS engine ever serves a
// query from an in-memory replica, since a future snapshot-file consumer
// (possibly a different BloodTrail instance) still needs the counter to be
// trustworthy. It is a no-op when e.pool is nil (ensureWatermarkTable's own
// doc), which is what keeps this safe to call from a unit test built with
// no database behind it at all.
func (e *Engine) Start(ctx context.Context) {
	e.ensureWatermarkTable(ctx)

	if !e.cfg.Enabled {
		return
	}
	if !e.claimRebuildLoop() {
		// Lost the race for the single rebuild-loop gate to a write that
		// already tripped enterFallback (see this method's own doc) -- that
		// goroutine is already retrying the exact load boot load itself
		// would otherwise start, so there is nothing left for boot load to
		// do.
		return
	}
	go e.runBootLoad(ctx)
}

// Stop quiesces the engine's background work: it cancels the engine's own
// background context (bgCtx, engine.go), which is what makes both the
// boot-load goroutine below and the fallback recovery goroutine (apply.go)
// exit promptly -- whichever of the two happens to be running, at whatever
// point in its own retry backoff it is currently waiting. Safe to call more
// than once (a context.CancelFunc is itself idempotent) and safe even if
// Start was never called, or was a no-op because !cfg.Enabled.
//
// Deliberately does not wait for either goroutine to actually exit, the same
// choice the fallback recovery goroutine's own doc already makes: an
// in-flight LoadSnapshot can be blocked on a slow PostgreSQL round trip for
// longer than any caller of Stop should have to wait, and neither goroutine
// holds anything a caller needs back. Their own context checks -- at the top
// of each retry loop, and while waiting out a backoff -- are what make exit
// "prompt" without Stop needing to block on it.
func (e *Engine) Stop() {
	if e.bgCancel != nil {
		e.bgCancel()
	}
}

// runBootLoad is the boot-load goroutine body launched by Start, once
// Start's own claimRebuildLoop call has won the single rebuild-loop gate
// (fallbackRebuilding): it keeps calling rebuildOnce, labeled
// triggerStartup, until one is actually adopted, then hands off to
// finishFallbackRebuild exactly as runFallbackRebuild's own adopted exit
// does -- both loops share that flag now, so the same race
// finishFallbackRebuild closes for fallback recovery (a fresh write tripping
// enterFallback in the narrow window between adoption and the flag actually
// clearing) applies here too. Retried on the same doubling backoff
// runFallbackRebuild uses (fallbackRetryInterval..fallbackRetryMax, with a
// refusal for exceeding cfg.MemoryLimit backed off to
// fallbackBudgetRetryInterval instead via fallbackRetryDelay): boot load and
// fallback recovery are the same kind of operation (retry a snapshot load
// until it sticks), just triggered differently.
//
// Runs on the engine's own background context (bgCtx, cancelled by Stop),
// not ctx: ctx's cancellation is honored too (a caller-supplied way to stop
// boot load without going through Stop), but bgCtx is what Stop actually
// cancels, so only checking ctx would leave this goroutine unable to be
// quiesced by Stop the way the fallback recovery goroutine already is. Both
// early-return paths below clear fallbackRebuilding directly, the same way
// runFallbackRebuild's own context-cancelled returns do (and for the same
// reason: Stop cancelling bgCtx means the engine is shutting down, so there
// is no reason to run finishFallbackRebuild's relaunch-if-raced recheck).
func (e *Engine) runBootLoad(ctx context.Context) {
	backoff := fallbackRetryInterval
	for {
		if ctx.Err() != nil || e.bgCtx.Err() != nil {
			e.fallbackRebuilding.Store(false)
			return
		}

		adopted, err := e.rebuildOnce(e.bgCtx, triggerStartup)
		switch {
		case err != nil:
			e.cfg.Log.WarnContext(e.bgCtx, "bloodtrail: boot load failed", slog.Any("error", err))
		case adopted:
			e.finishFallbackRebuild()
			return
		}

		wait, next := fallbackRetryDelay(err == nil && e.overBudget.Load(), backoff)
		backoff = next

		select {
		case <-ctx.Done():
			e.fallbackRebuilding.Store(false)
			return
		case <-e.bgCtx.Done():
			e.fallbackRebuilding.Store(false)
			return
		case <-time.After(wait):
		}
	}
}
