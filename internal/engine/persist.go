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
// pg's own counter at boot time, and trusts the file only on an exact
// match (snapshotFileTrustedAtBoot) -- so this is the write side of the
// same watermark-gated contract boot.go enforces on the read side.
//
// Driver.Close calls this, best-effort, strictly AFTER engine.Stop() has
// already run, while the pg pool is still open. That ordering is why the
// save preconditions below are not simply WatermarkTrusted(ctx): see
// saveSnapshotPreconditionsFor's own doc for exactly what changes, and
// what does not, once Stop has already run -- and see saveSnapshotCommit's
// own doc for the epoch guard that closes the specific TOCTOU window that
// ordering alone does NOT rule out (an Apply call already in flight when
// Stop ran, racing this method's own view/counter sampling).
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
//     for an ordinary precondition miss, Warn for an epoch mismatch, since
//     that specifically means a write was almost lost) purely for an
//     operator's observability; Driver.Close treats every SaveSnapshot
//     outcome as best-effort regardless, so neither is ever surfaced as
//     something the caller has to react to.
//
// A failure past that point -- Fold, or WriteSnapshotFile itself -- is
// logged at Warn and returned as an error. Driver.Close logs nothing
// further and continues closing the embedded PostgreSQL driver regardless
// (see its own doc): SaveSnapshot must never be the reason a graceful
// shutdown blocks or aborts.
func (e *Engine) SaveSnapshot(ctx context.Context) error {
	path, ok := e.snapshotFilePath()
	if !ok {
		return nil
	}

	if e.snap.Load() == nil {
		return nil
	}

	epoch, pgCounter, converged := e.saveSnapshotProbe(ctx)
	return e.saveSnapshotCommit(ctx, path, epoch, pgCounter, converged)
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
// A later boot (snapshotFileTrustedAtBoot, boot.go) would then trust that
// file on an exact-match basis and silently serve a replica missing the
// write.
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
// therefore treated as a flat refusal -- logged at Warn, since (unlike an
// ordinary precondition miss) this specifically means a write was almost
// folded into a file that would have claimed not to be missing it -- with
// no file written, exactly like every other "nothing to save this time"
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
// Fold runs INSIDE the lock, deliberately: folding a View this size costs
// nothing shutdown cares about (Stop has already run, so there is no
// serving traffic contending for applyMu here, and any write racing this
// call would fail the epoch check above regardless of how long the fold
// takes). The file WRITE does not: applyMu is released before
// WriteSnapshotFile's own I/O, capturing the folded snapshot and pgCounter
// as one pair first. That release reopens a narrow window -- an Apply that
// lands between the unlock and the write completing -- but it cannot
// reintroduce the bug this method exists to fix: the captured (snapshot,
// pgCounter) pair is still mutually consistent (the data folded is exactly
// what pgCounter described when this call verified it), and the racing
// Apply's own AdvanceWatermark moves pg's watermark counter PAST pgCounter
// -- so the file this call is about to write will be REJECTED by the very
// next boot's exact-match check (snapshotFileTrustedAtBoot), never trusted
// as complete when it is not. Rejecting a file is always safe (boot.go's
// own fallback is a genuine PostgreSQL rebuild); silently trusting a wrong
// one is the only outcome this whole feature exists to rule out.
func (e *Engine) saveSnapshotCommit(ctx context.Context, path string, epoch, pgCounter uint64, converged bool) error {
	start := time.Now()

	e.applyMu.Lock()

	if e.applyEpoch.Load() != epoch {
		e.applyMu.Unlock()
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

	foldStart := time.Now()
	base := view.Base()
	snap := base
	var foldErr error
	if segments := view.Segments(); len(segments) > 0 {
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
// (snapshotFileTrustedAtBoot's own doc), reintroduced from the write side
// instead. Keeping this check is what closes that: a shutdown caught mid-
// fallback simply skips writing a file at all, leaving whatever file an
// earlier, successful save already left behind (or no file at all)
// untouched for the next boot to fall back to its own pg rebuild -- always
// correct, if sometimes slower.
func saveSnapshotPreconditionsFor(state int32, dirtyGen, resolvedGen uint64, converged bool) bool {
	return state == stateServing && dirtyGen == resolvedGen && converged
}
