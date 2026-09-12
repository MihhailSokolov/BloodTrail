// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// SaveSnapshot folds this engine's current View -- its base snapshot plus
// every write-through delta segment layered onto it (View.Segments) -- into
// one flat snapshot.Snapshot, and writes it to this engine's own snapshot
// file (snapshotFilePath, boot.go), stamped with the pg watermark counter
// that was live and converged at the moment this call decided to proceed.
// A later boot's tryLoadSnapshotFile compares that stamped counter against
// pg's own counter at boot time, and trusts the file only when the gap
// between the two is exactly covered by that boot's own buffered writes
// (adoptSnapshotFileView's bootGapCoveredAt check; a quiet boot's empty gap
// is the degenerate cover) -- so this is the write side of the same
// watermark-gated contract boot.go enforces on the read side.
//
// This is the shutdown caller's entry point: Driver.Close calls this,
// best-effort, strictly AFTER engine.Stop() has already run, while the pg
// pool is still open. That ordering is why the save preconditions below are
// not simply WatermarkTrusted(ctx): see saveSnapshotPreconditionsFor's own
// doc for exactly what changes, and what does not, once Stop has already
// run -- and see saveSnapshotCommit's own doc for the epoch guard that
// closes the specific TOCTOU window that ordering alone does NOT rule out
// (an Apply call already in flight when Stop ran, racing this method's own
// view/counter sampling).
//
// There is a SECOND caller of this same save machinery -- runCompaction
// (compact.go), which runs while the engine is very much still serving live
// traffic, Stop's own guarantees notwithstanding -- reached through
// saveSnapshotAfterCompaction, below, rather than through this exported
// method: the two share every step here except one, gated by the
// requireEmptyDelta parameter saveSnapshotCommit's own doc explains in
// full. This method always passes false (fold whatever delta is there, the
// original shutdown-caller behavior, unchanged).
//
// The work is split into saveSnapshotProbe (read-only: sample applyEpoch,
// then run the pg watermark round trip) and saveSnapshotCommit (locked:
// verify the epoch, then fold and write) purely so a whitebox test can
// interleave a real Apply call between the two deterministically -- the
// same "drive the sequence manually" technique
// TestAdoptionRefusedByEpochResolvesNothing (watermark_test.go) already
// uses for adoptRebuiltView's own epoch check -- without either method
// needing a production-only synchronization hook.
//
// Three cases are a no-op, returning nil rather than an error, because
// there is genuinely nothing to save, not because anything went wrong:
//
//   - The snapshot-file feature is disabled (cfg.SnapshotDir == "");
//     snapshotFilePath reports this without ever touching the filesystem.
//   - No snapshot has ever been adopted (e.snap.Load() == nil).
//   - The save preconditions don't hold, epoch included -- logged (Debug
//     for an ordinary precondition miss, Warn for an epoch mismatch) purely
//     for an operator's observability; every caller of this save machinery
//     treats its outcome as best-effort regardless, so neither is ever
//     surfaced as something the caller has to react to. What an epoch
//     mismatch actually costs differs by caller -- see the Warn log's own
//     doc in saveSnapshotCommit for why this is worded caller-neutrally
//     rather than assuming the shutdown caller's stakes.
//
// A failure past that point -- Fold, or WriteSnapshotFile itself -- is
// logged at Warn and returned as an error. Driver.Close logs nothing
// further and continues closing the embedded PostgreSQL driver regardless
// (see its own doc): SaveSnapshot must never be the reason a graceful
// shutdown blocks or aborts.
func (e *Engine) SaveSnapshot(ctx context.Context) error {
	return e.saveSnapshot(ctx, false)
}

// saveSnapshotAfterCompaction is runCompaction's own entry point into this
// file's save machinery (compact.go, right after a successful
// adoptCompaction): identical to the exported SaveSnapshot except it passes
// requireEmptyDelta=true through to saveSnapshotCommit, whose own doc
// explains exactly what that changes and why a caller running against live
// traffic -- rather than after Stop, SaveSnapshot's own guarantee -- needs
// it.
func (e *Engine) saveSnapshotAfterCompaction(ctx context.Context) error {
	return e.saveSnapshot(ctx, true)
}

// saveSnapshot is SaveSnapshot's and saveSnapshotAfterCompaction's shared
// body -- see either exported/semi-exported wrapper's own doc for what
// requireEmptyDelta means to saveSnapshotCommit.
func (e *Engine) saveSnapshot(ctx context.Context, requireEmptyDelta bool) error {
	path, ok := e.snapshotFilePath()
	if !ok {
		return nil
	}

	if e.snap.Load() == nil {
		return nil
	}

	epoch, pgCounter, converged := e.saveSnapshotProbe(ctx)
	return e.saveSnapshotCommit(ctx, path, epoch, pgCounter, converged, requireEmptyDelta)
}

// saveSnapshotProbe is SaveSnapshot's read-only preparation, run BEFORE
// applyMu is ever taken: it samples e.applyEpoch first, then runs the pg
// watermark round trip (watermarkConverged) -- in that order, and
// deliberately, mirroring rebuildOnce's own "epoch, then the pg round trip"
// ordering (engine.go): reading the epoch after the round trip would let an
// Apply that ran during the round trip slip in unnoticed by the later
// recheck, exactly the gap saveSnapshotCommit's epoch guard exists to close.
func (e *Engine) saveSnapshotProbe(ctx context.Context) (epoch, pgCounter uint64, converged bool) {
	epoch = e.applyEpoch.Load()
	pgCounter, converged = e.watermarkConverged(ctx)
	return epoch, pgCounter, converged
}

// saveSnapshotCommit is SaveSnapshot's locked, epoch-verified fold-and-write
// step: the fix for the TOCTOU a review of this method once missed.
//
// The bug it closes: the previous version sampled e.snap.Load() (the View)
// and the watermark preconditions (state, the trust generations, the
// converged pg counter) as two separate, unlocked reads with an unbounded
// gap between them. Apply (apply.go) runs its ENTIRE sequence -- including
// the read-back's own pg round trip -- under applyMu, bumping applyEpoch at
// its very top (apply.go line ~136, "Bump applyEpoch... before anything else
// can decide to return early") and publishing its new View at the very
// bottom (apply.go line ~226, e.snap.Store(newView)), both still under that
// same lock. Because the old SaveSnapshot never took applyMu at all, an
// Apply already in flight when Driver.Close called Stop() (Stop does not
// wait for one -- see its own doc) could run its ENTIRE sequence in the gap
// between the old code's two unlocked reads: this method would load the OLD
// View (before the Apply's Store), then sample a watermark that has already
// resolved past the Apply's write (converged, pgCounter == C+1) -- folding
// stale data and stamping it with a counter that promises it is not stale.
// A later boot (adoptSnapshotFileView's empty-gap case, boot.go) would
// then trust that file and silently serve a replica missing the write.
//
// The fix mirrors adoptRebuiltView's own proof (engine.go), applied to the
// opposite direction: rebuildOnce samples epoch/settledGen BEFORE its load
// begins and adoptRebuiltView rechecks epoch AFTER the load, under applyMu,
// before publishing; here, saveSnapshotProbe samples epoch BEFORE the pg
// round trip, and this method rechecks it AFTER, under the same applyMu,
// before folding. An unchanged epoch across that whole window -- probe
// through lock acquisition -- proves no Apply call ran (started or
// finished) anywhere in it, by the identical argument adoptRebuiltView's own
// doc makes: Apply bumps the epoch unconditionally before any early return,
// so ANY Apply overlapping the window would have moved it. A caller
// blocked waiting for applyMu because an Apply is CURRENTLY holding it is
// covered the same way: by the time Lock() returns, that Apply has already
// bumped the epoch, so the recheck still catches it. A changed epoch is
// therefore treated as a flat refusal -- logged at Warn (see that log call's
// own doc, below, for exactly what it does and does not claim) -- with no
// file written, exactly like every other "nothing to save this time"
// outcome SaveSnapshot's own doc lists.
//
// Once the epoch is confirmed unchanged, the View, state and the trust
// generations are all RE-READ here, fresh, under the same lock -- not
// reused from any pre-lock sample -- so the final precondition check
// (saveSnapshotPreconditionsFor) is evaluated against a single consistent
// instant the epoch guard has just proven contains no missed Apply. The pg
// counter and converged flag are the one exception: they come from
// saveSnapshotProbe's own pg round trip, taken before the lock, because
// re-reading them here would mean a second live pg call while holding
// applyMu -- exactly the kind of I/O this lock must never gate (see below).
// That is sound precisely because the epoch stayed unchanged: no Apply
// resolved any bump in the window, so nothing could have moved pgCounter or
// converged in a way an Apply-driven write would need this call to notice.
// (A bump whose OWN write has not yet reached Apply -- BumpWatermark run,
// but the write's Apply call not yet made -- cannot make converged flip
// from false to true either, so a converged=true sample never understates
// what has actually landed.)
//
// requireEmptyDelta is where this method's two callers (SaveSnapshot and
// saveSnapshotAfterCompaction, above) actually diverge, and it exists
// because they no longer share one premise the ORIGINAL version of this
// method -- written when SaveSnapshot had exactly one caller -- was built
// on: "Stop has already run, so there is no serving traffic contending for
// applyMu here" (the reasoning that used to justify folding unconditionally
// inside this lock). That is still exactly true for SaveSnapshot's own
// caller, Driver.Close, which calls it strictly after engine.Stop() -- but
// saveSnapshotAfterCompaction's caller, runCompaction (compact.go), calls
// this WHILE the engine is still serving live Apply traffic, by design: a
// compaction runs entirely in the background, concurrently with whatever
// writes keep arriving. A View can legitimately have a large delta layered
// on it at that exact moment -- not the just-adopted compaction's own tiny
// rebased tail (adoptCompaction's own doc), but whatever ordinary Apply
// traffic has piled onto it again in the time since -- and folding THAT
// under applyMu would block every one of those Applies for as long as the
// fold takes, exactly the live-traffic cost this whole background-compactor
// feature exists to avoid paying inline.
//
// So: when requireEmptyDelta is true, this method folds ONLY when
// view.Segments() is already empty at this exact instant (checked below,
// under this same lock, immediately after the precondition check) --
// nothing layered on since whatever last cleared the delta -- in which case
// snap is already just view.Base() and no Fold call happens AT ALL, empty
// or not (see the code below); a non-empty delta is left entirely alone,
// logged at Debug, and NOT written this time -- the next compaction's own
// save attempt gets another chance once ITS OWN adoption has cleared the
// delta again. When requireEmptyDelta is false (SaveSnapshot, the shutdown
// caller), the original behavior is unchanged: Fold runs inside the lock
// whenever the delta is non-empty, because for THAT caller specifically the
// "no live traffic" premise above still holds, and a shutdown gets exactly
// one attempt to persist whatever delta exists, empty or not.
//
// The file WRITE, once a snap is decided (folded or bare base), never runs
// inside the lock either way: applyMu is released before WriteSnapshotFile's
// own I/O, capturing the folded snapshot and pgCounter as one pair first.
// That release reopens a narrow window -- an Apply that lands between the
// unlock and the write completing -- but it cannot reintroduce the bug this
// method exists to fix: the captured (snapshot, pgCounter) pair is still
// mutually consistent (the data folded is exactly what pgCounter described
// when this call verified it), and the racing Apply's own AdvanceWatermark
// moves pg's watermark counter PAST pgCounter -- so the file this call is
// about to write will be REJECTED by the very next boot's watermark-gap
// check (bootGapCoveredAt): the racing write's counter belongs to THIS
// process, so no later boot's own buffer can ever account for it, and the
// gap can never be covered -- never trusted as complete when it is not.
// Rejecting a file is always safe (boot.go's own fallback is a genuine
// PostgreSQL rebuild); silently trusting a wrong one is the only outcome
// this whole feature exists to rule out.
func (e *Engine) saveSnapshotCommit(ctx context.Context, path string, epoch, pgCounter uint64, converged bool, requireEmptyDelta bool) error {
	start := time.Now()

	e.applyMu.Lock()

	if e.applyEpoch.Load() != epoch {
		e.applyMu.Unlock()
		// Worded caller-neutrally, deliberately: an epoch mismatch means an
		// Apply call raced this method's own probe (saveSnapshotProbe,
		// above), so THIS save attempt cannot trust the (pgCounter,
		// converged) pair it sampled and refuses rather than risk stamping
		// a file with a watermark that overstates what it actually
		// captured. Whether that write gets another chance depends
		// entirely on which caller reached here, and this log line does
		// not know or assume: SaveSnapshot's own shutdown caller
		// (Driver.Close) calls this once, right before closing the pg pool
		// for good, so a race here really can mean this file save's one
		// and only chance to capture that write is gone (never that the
		// write itself is "lost" -- it already landed in the live View and
		// PostgreSQL regardless of what this file save does; only the FILE
		// misses it, and the next boot falls back to a PostgreSQL rebuild
		// exactly as it would with no file at all).
		// saveSnapshotAfterCompaction's caller (runCompaction) calls this
		// again after every future compaction, so the identical race there
		// is simply retried next time, not a near-miss of anything. Either
		// way, every caller of this save machinery treats its return value
		// as best-effort (SaveSnapshot's own doc), so neither reacts to
		// this beyond the log line.
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file not written",
			slog.String("reason", "an Apply call landed while probing the watermark"),
			slog.Uint64("epoch_at_probe", epoch),
			slog.Uint64("epoch_at_commit", e.applyEpoch.Load()),
		)
		return nil
	}

	state := e.state.Load()
	dirtyGen, resolvedGen := e.watermarkGens()
	view := e.snap.Load()

	if view == nil || !saveSnapshotPreconditionsFor(state, dirtyGen, resolvedGen, converged) {
		e.applyMu.Unlock()
		e.cfg.Log.DebugContext(ctx, "bloodtrail: snapshot file not written",
			slog.Bool("serving", state == stateServing),
			slog.Bool("gens_equal", dirtyGen == resolvedGen),
			slog.Bool("converged", converged),
		)
		return nil
	}

	segments := view.Segments()
	if requireEmptyDelta && len(segments) > 0 {
		e.applyMu.Unlock()
		e.cfg.Log.DebugContext(ctx, "bloodtrail: snapshot file skipped",
			slog.String("reason", "segments pending since adoption; the next compaction's own save covers it"),
			slog.Int("segments", len(segments)),
		)
		return nil
	}

	foldStart := time.Now()
	base := view.Base()
	snap := base
	var foldErr error
	if len(segments) > 0 {
		snap, foldErr = snapshot.Fold(base, segments)
	}
	foldDuration := time.Since(foldStart)

	e.applyMu.Unlock()

	if foldErr != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file write failed", slog.String("step", "fold"), slog.Any("error", foldErr))
		return fmt.Errorf("engine: SaveSnapshot: fold: %w", foldErr)
	}

	writeStart := time.Now()
	if err := snapshot.WriteSnapshotFile(path, snap, pgCounter); err != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file write failed", slog.String("step", "write"), slog.Any("error", err))
		return fmt.Errorf("engine: SaveSnapshot: %w", err)
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file written",
		slog.String("path", path),
		slog.Uint64("watermark", pgCounter),
		slog.Int("nodes", snap.NodeCount()),
		slog.Int("edges", snap.EdgeCount()),
		slog.Duration("fold_duration", foldDuration),
		slog.Duration("write_duration", time.Since(writeStart)),
		slog.Duration("duration", time.Since(start)),
	)
	return nil
}

// saveSnapshotPreconditionsFor is SaveSnapshot's own decision, pulled out
// as a pure function for unit testing without a live pg connection --
// mirroring watermarkTrustedFor's identical treatment (watermark.go): true
// iff state == stateServing, dirtyGen == resolvedGen, and converged.
//
// This is WatermarkTrusted's own two structural conditions, unmodified --
// but reached without WatermarkTrusted's live double-sampling dance (its
// own doc's "Condition 1 is actually sampled TWICE" section), and that
// omission is deliberate, not an oversight. That double sample exists to
// defend a caller who is about to act on live serving traffic against a
// CONCURRENT write racing WatermarkTrusted's own pg round trip -- a
// failure noted, or a bump resolved, while the round trip is in flight,
// which a single sample could miss.
//
// SaveSnapshot runs strictly after Stop() (Driver.Close's own calling
// contract: engine.Stop() first, this second, while the pool is still
// open), and Stop() does end this engine's OWN background write-through
// activity for good: it cancels bgCtx, which is what makes the boot-load
// and fallback-recovery goroutines exit (boot.go, apply.go) and therefore
// stops either of them from calling Apply again on this engine's behalf.
// "No live traffic left to race" for THAT category of caller is a fact
// this package can actually verify, not an assumption.
//
// It is NOT, however, a fact about every Apply call this Engine could ever
// receive: Apply is also called directly by the embedding Driver's own
// write path (driver.go's WriteTransaction/BatchOperation/Run, via
// write_observer.go), for ordinary application writes that have nothing to
// do with this engine's background goroutines and that Stop() neither
// cancels nor waits for (Stop's own doc: "Deliberately does not wait for
// either goroutine to actually exit" -- and it never claims to wait for a
// caller's in-flight write either). "The application has stopped issuing
// writes before calling Driver.Close" is therefore an EXTERNAL precondition
// on the caller, not something this package can observe or enforce -- a
// write already past its own pg commit and mid-Apply when Close runs is
// entirely possible, and previously raced this exact method's own unlocked
// view/counter sampling (see saveSnapshotCommit's doc for the bug that let
// through, and its fix). That fix is what makes the single sample below
// sound regardless of whether the external precondition actually holds:
// saveSnapshotCommit's epoch guard proves, from data internal to this
// engine, that no Apply call started or finished across the whole window
// from this method's own pg round trip to the moment it commits -- which is
// the same guarantee the double-sampling dance buys WatermarkTrusted's
// caller, reached by a different mechanism (an epoch check under applyMu,
// rather than a second pair of atomic loads) because SaveSnapshot's shape
// -- fold a View, not just answer a question -- has a lock available that a
// pure predicate does not. "No in-flight applies" -- the thing
// WatermarkTrusted's second sample exists to catch -- is covered here by
// that guard FIRST, and only backed up by converged's own inflight == 0
// half (watermarkConverged's own doc) for the bumped-but-not-yet-applied
// writes the epoch mechanism has no way to see at all (Apply has not run
// for them yet, so applyEpoch has not moved either).
//
// state == stateServing is NOT dropped, and cannot be: unlike the double-
// sampling dance, it is not merely insurance against a race, but the one
// condition that answers a question the generations and the counter both
// stay silent on. An engine in stateFallback carries a View that is not
// known to match PostgreSQL (apply.go's enterFallback: some write could
// not be replayed into it) -- and critically, that can be true while
// dirtyGen == resolvedGen and watermarkConverged both hold, because they
// track a DIFFERENT thing. dirtyGen only advances on a genuine watermark
// BUMP failure (NoteWatermarkBumpFailure); an ordinary ChangeSet fallback
// record -- what Run, WipeGraph, and SetDefaultGraph record unconditionally
// on every successful call (driver.go), and what a read-back or segment-
// build failure records inside Apply itself -- trips stateFallback via
// Apply's cs.HasFallback() branch while that SAME write's own watermark
// bump, a completely ordinary success, still resolves cleanly through
// AdvanceWatermark at the very top of Apply. So a graceful shutdown that
// catches the engine mid-recovery from one of those everyday fallback
// trips (Stop cancels the recovery goroutine without waiting for it to
// finish, per Stop's own doc) would see fully "clean" generations and a
// fully converged counter, and -- without this check -- would fold and
// persist the STALE View that tripped fallback in the first place, stamped
// with a watermark that a later boot's tryLoadSnapshotFile would consider
// a perfect match and load without question: exactly the wrong-trust
// outcome this whole feature exists to make impossible on the read side
// (bootGapCoveredAt's own doc), reintroduced from the write side
// instead. Keeping this check is what closes that: a shutdown caught mid-
// fallback simply skips writing a file at all, leaving whatever file an
// earlier, successful save already left behind (or no file at all)
// untouched for the next boot to fall back to its own pg rebuild -- always
// correct, if sometimes slower.
//
// saveSnapshotAfterCompaction's caller (runCompaction, compact.go) is
// gated by this exact same check too, unmodified: state == stateServing
// rules out a fallback entered while that compaction's own fold was
// running (the same reasoning adoptCompaction's own state recheck already
// applies to publishing the fold itself, just now applied a second time to
// persisting it), and the generation/converged half is just as meaningful
// for that caller as for the shutdown one. What differs by caller is
// requireEmptyDelta (saveSnapshotCommit's own doc), a check layered AFTER
// this one, not a reason to weaken this one.
func saveSnapshotPreconditionsFor(state int32, dirtyGen, resolvedGen uint64, converged bool) bool {
	return state == stateServing && dirtyGen == resolvedGen && converged
}
