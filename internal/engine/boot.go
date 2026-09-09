// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
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
// Before any of that, it makes exactly one attempt to short-circuit the
// whole retry-against-PostgreSQL loop by loading a local snapshot file
// instead (tryLoadSnapshotFile) -- cheap, and only ever worth retrying by
// falling through to the ordinary pg rebuild loop below on any failure, so
// there is no separate retry schedule for it. A successful, adopted file
// load hands off to finishFallbackRebuild exactly as an adopted pg rebuild
// would, and returns without the loop below ever running at all (see
// RebuildCount's own doc: this is the whole reason a file-boot integration
// test can assert RebuildCount() == 0).
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
	if e.tryLoadSnapshotFile(e.bgCtx) {
		e.finishFallbackRebuild()
		return
	}

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

// snapshotFilePath returns the path this engine's boot-load attempt
// (tryLoadSnapshotFile, below) reads from and SaveSnapshot (persist.go)
// writes to, and whether the snapshot-file feature is enabled at all:
// false, with an empty path, exactly when cfg.SnapshotDir == "" -- the
// documented meaning of an empty SnapshotDir (Config's own doc) -- checked
// FIRST and unconditionally, before ever touching e.pgDriver, so that this
// is a genuine no-op (no default-graph lookup, no filesystem I/O) for every
// engine that never set it, including one built with a nil pgDriver in a
// test that has no reason to care about this feature at all.
//
// The filename embeds the default graph's id ("graph-<id>.btsnap"), read
// through the identical pgDriver.DefaultGraph() call LoadSnapshot itself
// uses to stamp Snapshot.GraphID -- so a graph swap (SetDefaultGraph) never
// risks loading one graph's file as though it were another's: the swapped-
// to graph simply names a different file, one that either doesn't exist
// yet (a quiet miss, tryLoadSnapshotFile's own doc) or was itself written
// by a previous SaveSnapshot against that same graph.
func (e *Engine) snapshotFilePath() (string, bool) {
	if e.cfg.SnapshotDir == "" {
		return "", false
	}
	graphModel, ok := e.pgDriver.DefaultGraph()
	if !ok {
		return "", false
	}
	return filepath.Join(e.cfg.SnapshotDir, fmt.Sprintf("graph-%d.btsnap", graphModel.ID)), true
}

// snapshotFileTrustedAtBoot reports whether a snapshot file whose embedded
// watermark is fileWatermark may be trusted at boot, given PostgreSQL's
// current watermark counter pgWatermark: exactly fileWatermark ==
// pgWatermark. Extracted as a pure function so this one-line decision has
// a direct unit test, mirroring watermarkTrustedFor's identical treatment
// (watermark.go).
//
// This is deliberately NOT WatermarkTrusted (watermark.go), and cannot be:
// that predicate requires this engine's own appliedWatermark bookkeeping to
// equal pg's counter, but a freshly constructed Engine starts with
// appliedWatermark == 0 regardless of what pg's counter actually is -- it
// has applied nothing yet, in memory or otherwise -- so watermarkConverged
// would read false for almost any real, previously-running database's
// nonzero counter, at exactly the moment (boot) a file is most useful to
// trust. WatermarkTrusted's generation pair (dirtyGen == resolvedGen) is
// equally beside the point here: both are legitimately zero at boot
// regardless of the file's own trustworthiness, since nothing has had a
// chance to fail yet.
//
// The boot question is narrower than WatermarkTrusted's, and answerable
// without any of that bookkeeping. SaveSnapshot (persist.go) only ever
// writes a file while stamping it with the exact pg watermark counter that
// was true, live, at the moment it decided to proceed -- so the file's own
// counter is, by construction, PostgreSQL's committed counter as of some
// past instant. If pg's counter right now is still that same value, then
// no mutating write has committed since: BumpWatermark's UPDATE is the
// only thing that ever advances the counter, and every mutating write
// bumps it eagerly, before its own effect ever reaches PostgreSQL
// (watermark.go's own doc) -- so an unchanged counter proves an empty set
// of writes landed in between, and the file's contents are therefore
// exactly PostgreSQL's current committed state for this graph.
//
// The comparison is plain equality, not "file <= pg", deliberately: a file
// behind pg's counter is missing at least one committed write and must
// never be trusted (this whole feature exists to make that refusal
// automatic), and a file AHEAD of pg's counter should never happen at all
// (the counter only advances), so treating that case as anything other
// than "something is wrong, don't trust it" would be guessing rather than
// reasoning from evidence. Equality is also exactly what NEVER trusting on
// a mismatch requires: there is no direction in which this check is
// lenient.
func snapshotFileTrustedAtBoot(fileWatermark, pgWatermark uint64) bool {
	return fileWatermark == pgWatermark
}

// tryLoadSnapshotFile makes one attempt to boot this engine straight from
// its own snapshot file (snapshotFilePath) instead of PostgreSQL, reporting
// whether it actually adopted one. Called once, at the very top of
// runBootLoad, before the ordinary pg rebuild loop -- see that method's own
// doc for why a single attempt, with no retry schedule of its own, is
// enough: any failure here simply falls through to the retry loop that
// already exists for the pg path.
//
// epoch and settledGen are read BEFORE ReadSnapshotFile even opens the
// file, mirroring rebuildOnce's identical ordering and for the identical
// reason (that method's own doc): an unchanged applyEpoch at adoption time
// proves no write was applied while this load ran, so the file's contents
// (had they been trusted) cannot be missing one, and settledGen is the
// watermark trust generation this adoption is entitled to resolve.
//
// Every rejection is logged at Info as "bloodtrail: snapshot file
// rejected" -- an e2e/observability grep target -- naming why, with one
// exception: a missing file (the ordinary first-ever-boot case, or any
// boot after SnapshotDir was pointed at a fresh directory) logs the
// quieter Debug "bloodtrail: no snapshot file" instead, since there is
// nothing to reject, only nothing found. Trust, once granted, is never
// given twice: every return point below that found something wrong
// returns false and lets the caller fall back to PostgreSQL -- there is no
// path from a rejected file to a "trust it anyway" outcome.
//
// Adoption goes through adoptRebuiltView, the exact same publish path
// rebuildOnce itself uses -- reused rather than duplicated so a
// file-sourced snapshot is subject to the identical epoch check, the
// identical stateFallback-clearing behavior, and (via the memory-limit
// check just before it) the identical cfg.MemoryLimit budget a pg-sourced
// one is. An over-limit file is refused exactly like an over-limit
// pg-loaded snapshot (rebuildOnce's own doc): the file is not adopted, and
// the caller falls through to the pg rebuild loop, whose own budget
// backoff (fallbackRetryDelay) takes over from there -- there is
// deliberately no separate over-limit retry schedule for the file path
// itself.
func (e *Engine) tryLoadSnapshotFile(ctx context.Context) bool {
	path, ok := e.snapshotFilePath()
	if !ok {
		return false
	}

	epoch := e.applyEpoch.Load()
	settledGen := e.settledDirtyGen.Load()

	snap, fileWatermark, err := snapshot.ReadSnapshotFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			e.cfg.Log.DebugContext(ctx, "bloodtrail: no snapshot file", slog.String("path", path))
		} else {
			e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file rejected",
				slog.String("path", path),
				slog.Any("error", err),
			)
		}
		return false
	}

	pgWatermark, err := e.ReadWatermark(ctx)
	if err != nil {
		e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file rejected",
			slog.String("path", path),
			slog.String("reason", "read pg watermark failed"),
			slog.Any("error", err),
		)
		return false
	}

	if !snapshotFileTrustedAtBoot(fileWatermark, pgWatermark) {
		e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file rejected",
			slog.String("path", path),
			slog.String("reason", "watermark mismatch"),
			slog.Uint64("file_watermark", fileWatermark),
			slog.Uint64("pg_watermark", pgWatermark),
		)
		return false
	}

	approxBytes := snap.ApproxBytes()
	if e.cfg.MemoryLimit > 0 && approxBytes > uint64(e.cfg.MemoryLimit) {
		e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file rejected",
			slog.String("path", path),
			slog.String("reason", "exceeds memory limit"),
			slog.Uint64("bytes", approxBytes),
			slog.Uint64("limit", uint64(e.cfg.MemoryLimit)),
		)
		return false
	}

	if !e.adoptRebuiltView(ctx, snapshot.NewView(snap), epoch, settledGen) {
		e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file rejected",
			slog.String("path", path),
			slog.String("reason", "a write was applied while the file was loading"),
		)
		return false
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file loaded",
		slog.String("path", path),
		slog.Uint64("watermark", fileWatermark),
		slog.Int("nodes", snap.NodeCount()),
		slog.Int("edges", snap.EdgeCount()),
	)
	return true
}
