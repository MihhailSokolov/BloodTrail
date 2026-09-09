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
// one-shot, retry-until-adopted loads, which is exactly what runBootLoad
// shares with runFallbackRebuild below.
//
// Start is meant to be called once, followed by exactly one Stop; it is not
// itself idempotent (calling it twice launches two boot-load goroutines,
// which race harmlessly against each other -- adoptRebuiltView's epoch
// check admits only one outcome either way -- but is still not something a
// caller should do).
func (e *Engine) Start(ctx context.Context) {
	if !e.cfg.Enabled {
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

// runBootLoad is the boot-load goroutine body launched by Start: it keeps
// calling rebuildOnce, labeled triggerStartup, until one is actually
// adopted, then returns. Retried on the same doubling backoff
// runFallbackRebuild uses (fallbackRetryInterval..fallbackRetryMax, with a
// refusal for exceeding cfg.MemoryLimit backed off to
// fallbackBudgetRetryInterval instead via fallbackRetryDelay) -- boot load
// and fallback recovery are the same kind of operation (retry a snapshot
// load until it sticks), just triggered differently and, by construction,
// never running at the same time: Apply never calls enterFallback while no
// snapshot has ever been adopted (it returns early instead, apply.go), so
// there is no snapshot for this goroutine to be racing the fallback
// recovery goroutine over.
//
// Runs on the engine's own background context (bgCtx, cancelled by Stop),
// not ctx: ctx's cancellation is honored too (a caller-supplied way to stop
// boot load without going through Stop), but bgCtx is what Stop actually
// cancels, so only checking ctx would leave this goroutine unable to be
// quiesced by Stop the way the fallback recovery goroutine already is.
func (e *Engine) runBootLoad(ctx context.Context) {
	backoff := fallbackRetryInterval
	for {
		if ctx.Err() != nil || e.bgCtx.Err() != nil {
			return
		}

		adopted, err := e.rebuildOnce(e.bgCtx, triggerStartup)
		switch {
		case err != nil:
			e.cfg.Log.WarnContext(e.bgCtx, "bloodtrail: boot load failed", slog.Any("error", err))
		case adopted:
			return
		}

		wait, next := fallbackRetryDelay(err == nil && e.overBudget.Load(), backoff)
		backoff = next

		select {
		case <-ctx.Done():
			return
		case <-e.bgCtx.Done():
			return
		case <-time.After(wait):
		}
	}
}
