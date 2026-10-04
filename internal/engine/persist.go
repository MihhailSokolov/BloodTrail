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
// that was live and converged at the moment this call decided to proceed
// and with where the node and edge id sequences stood right after (the
// snapshot.Stamp; watermark.go's insertedSinceFile says what a later boot
// compares them against), and with the watermark lineage the base was
// loaded in (the snapshot's own WatermarkLineage, never a fresh read: see
// watermark.go's watermarkLineageDDL). A base whose lineage is unknown is
// not written.
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

	epoch, stamp, converged := e.saveSnapshotProbe(ctx)
	return e.saveSnapshotCommit(ctx, path, epoch, stamp, converged, requireEmptyDelta)
}

// reasonUnresolvedWatermark is the "reason" a save refused over a counter
// value this process never resolved logs (saveSnapshotProbe).
const reasonUnresolvedWatermark = "the watermark counter holds values this process never resolved: " +
	"another BloodTrail server may be writing this database, or a bump's outcome was lost"

// saveSnapshotProbe is SaveSnapshot's read-only preparation, run BEFORE
// applyMu is ever taken: it samples e.applyEpoch first, then runs the pg
// watermark round trip (readWatermarkConvergence, the reading
// watermarkConverged decides on) -- in that order, and
// deliberately, mirroring rebuildOnce's own "epoch, then the pg round trip"
// ordering (engine.go): reading the epoch after the round trip would let an
// Apply that ran during the round trip slip in unnoticed by the later
// recheck, exactly the gap saveSnapshotCommit's epoch guard exists to close.
//
// The id sequence positions the stamp carries are read only once the
// counter has proven converged, and after it: every write the counter
// counts has then resolved, so its inserts are already behind the positions
// read, and a write that bumps after the counter read leaves the stamp's
// counter behind PostgreSQL's for good, which is all a later boot needs to
// know not to compare positions at all (insertedSinceFile). Read the other
// way round, a write landing between the two reads could put its own
// inserts past the stamped positions under a counter that already counts
// it, and the next boot would refuse the file for rows this process wrote.
// A failed read reports not converged: a file without its positions would
// only ever be refused.
//
// A counter holding a value no write of this process resolved or still
// carries is refused like any other miss, but said out loud (Warn): it is
// what another BloodTrail server writing the same database looks like from
// here, a deployment the snapshot file cannot be trusted in (watermarkReading's
// unaccounted). Only a rebuild that loads such a writer's writes lets this
// process save again.
func (e *Engine) saveSnapshotProbe(ctx context.Context) (epoch uint64, stamp snapshot.Stamp, converged bool) {
	epoch = e.applyEpoch.Load()
	reading, err := e.readWatermarkConvergence(ctx)
	if err != nil {
		return epoch, stamp, false
	}
	stamp.Watermark = reading.pgCounter
	if !reading.converged() {
		if reading.unaccounted() {
			e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file not written",
				slog.String("reason", reasonUnresolvedWatermark),
				slog.Uint64("pg_watermark", reading.pgCounter),
				slog.Uint64("resolved_through", reading.through),
				slog.Bool("resolved_exactly", reading.exact),
			)
		}
		return epoch, stamp, false
	}
	nodeSeq, edgeSeq, err := e.readSequencePositions(ctx)
	if err != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file not written",
			slog.String("reason", "could not read the id sequence positions to stamp it with"),
			slog.Any("error", err))
		return epoch, stamp, false
	}
	stamp.NodeIDSeq, stamp.EdgeIDSeq = nodeSeq, edgeSeq
	return epoch, stamp, true
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
// resolved past the Apply's write (converged, counter == C+1) -- folding
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
// resolved any bump in the window, so nothing could have moved the counter or
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
// own I/O, capturing the folded snapshot and the stamp as one pair first.
// That release reopens a narrow window -- an Apply that lands between the
// unlock and the write completing -- but it cannot reintroduce the bug this
// method exists to fix: the captured (snapshot, stamp) pair is still
// mutually consistent (the data folded is exactly what the stamped counter
// described when this call verified it), and the racing Apply's own
// AdvanceWatermark moves pg's watermark counter PAST the stamped one -- so
// the file this call is about to write will be REJECTED by the very next
// boot's watermark-gap check (bootGapCoveredAt): the racing write's counter
// belongs to THIS process, so no later boot's own buffer can ever account
// for it, and the gap can never be covered -- never trusted as complete
// when it is not. Rejecting a file is always safe (boot.go's own fallback
// is a genuine PostgreSQL rebuild); silently trusting a wrong one is the
// only outcome this whole feature exists to rule out.
//
// The same window is where a racing write's watermark bump can FAIL, and
// that write, unlike the one above, moves no counter at all: it reaches
// PostgreSQL uncounted, so nothing about the file this call writes would
// ever look stale to a later boot. NoteWatermarkBumpFailure removes the
// snapshot file for exactly that reason (watermark.go), but its removal can
// land before this call's file does, which would leave this file in place
// -- stamped with the counter the uncounted write never moved, missing that
// write's rows -- for the next boot after a hard stop to adopt as current.
// saveSnapshotWrite therefore checks, once its file is in place, that no
// bump has failed since the preconditions were checked, and removes the
// file itself if one has. One of the two removals always comes last:
// either the failure was noted before that check, which then sees it, or
// after it, in which case the failure's own removal runs after this file
// landed.
//
// The work is split into saveSnapshotPrepare (the locked decision and the
// fold) and saveSnapshotWrite (the file) for the same reason SaveSnapshot's
// own work is split into probe and commit: so a whitebox test can land a
// failed bump between the two.
func (e *Engine) saveSnapshotCommit(ctx context.Context, path string, epoch uint64, stamp snapshot.Stamp, converged bool, requireEmptyDelta bool) error {
	start := time.Now()
	pending, err := e.saveSnapshotPrepare(ctx, epoch, converged, requireEmptyDelta)
	if pending == nil {
		return err
	}
	return e.saveSnapshotWrite(ctx, path, pending, stamp, start)
}

// pendingSnapshotSave is a snapshot saveSnapshotPrepare has decided to
// write: the folded (or bare base) snapshot, the watermark generation its
// preconditions were checked against, and how long the fold took.
type pendingSnapshotSave struct {
	snap         *snapshot.Snapshot
	dirtyGen     uint64
	foldDuration time.Duration
}

// saveSnapshotPrepare is saveSnapshotCommit's locked half: the epoch guard,
// the preconditions and the fold, all as that method's doc describes.
// Returns nil and no error for a save it refused (already logged), nil and
// the error for a fold that failed (already logged), or what to write.
func (e *Engine) saveSnapshotPrepare(ctx context.Context, epoch uint64, converged bool, requireEmptyDelta bool) (*pendingSnapshotSave, error) {
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
		return nil, nil
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
		return nil, nil
	}

	// A file is only as good as the lineage it names, and the boot refuses
	// one that names none -- so there is nothing worth writing for a base
	// whose load could not read it (loadSnapshot's doc).
	if view.Base().WatermarkLineage.IsZero() {
		e.applyMu.Unlock()
		e.cfg.Log.DebugContext(ctx, "bloodtrail: snapshot file not written",
			slog.String("reason", "the replica's watermark lineage is unknown"))
		return nil, nil
	}

	segments := view.Segments()
	if requireEmptyDelta && len(segments) > 0 {
		e.applyMu.Unlock()
		e.cfg.Log.DebugContext(ctx, "bloodtrail: snapshot file skipped",
			slog.String("reason", "segments pending since adoption; the next compaction's own save covers it"),
			slog.Int("segments", len(segments)),
		)
		return nil, nil
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
		return nil, fmt.Errorf("engine: SaveSnapshot: fold: %w", foldErr)
	}
	return &pendingSnapshotSave{snap: snap, dirtyGen: dirtyGen, foldDuration: foldDuration}, nil
}

// writeSnapshotFile is the file write saveSnapshotWrite runs:
// snapshot.WriteSnapshotFile, held in a variable only so a test can make a
// write report an error after its file is already in place.
var writeSnapshotFile = snapshot.WriteSnapshotFile

// saveSnapshotWrite is saveSnapshotCommit's unlocked half: it writes what
// saveSnapshotPrepare decided on, then takes the file back out of play if a
// watermark bump failed while it was being written (saveSnapshotCommit's
// doc says why that cannot be left to NoteWatermarkBumpFailure's own
// removal alone).
//
// That check runs whether or not the write reported an error: a write that
// failed only in syncing the directory after its rename has left a complete
// file in place all the same (snapshot.WriteSnapshotFile's doc), and a file
// written across a failed bump has to go whichever way its write ended.
func (e *Engine) saveSnapshotWrite(ctx context.Context, path string, pending *pendingSnapshotSave, stamp snapshot.Stamp, start time.Time) error {
	snap := pending.snap

	writeStart := time.Now()
	writeErr := writeSnapshotFile(path, snap, stamp)
	if writeErr != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file write failed", slog.String("step", "write"), slog.Any("error", writeErr))
		writeErr = fmt.Errorf("engine: SaveSnapshot: %w", writeErr)
	}

	if e.dirtyGen.Load() != pending.dirtyGen {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file not written",
			slog.String("reason", "a watermark bump failed while the file was being written"))
		e.removeSnapshotFile(ctx, path, invalidateUncountedWrite)
		return writeErr
	}
	if writeErr != nil {
		return writeErr
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file written",
		slog.String("path", path),
		slog.Uint64("watermark", stamp.Watermark),
		slog.String("lineage", snap.WatermarkLineage.String()),
		slog.Int64("node_id_seq", stamp.NodeIDSeq),
		slog.Int64("edge_id_seq", stamp.EdgeIDSeq),
		slog.Int("nodes", snap.NodeCount()),
		slog.Int("edges", snap.EdgeCount()),
		slog.Duration("fold_duration", pending.foldDuration),
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

// Why a snapshot file is taken out of play, as the "snapshot file
// invalidated" line states it.
const (
	// invalidateUncountedWrite: a write reached PostgreSQL without advancing
	// the watermark counter, so the file's stamp no longer proves anything.
	invalidateUncountedWrite = "a write reached PostgreSQL without advancing the watermark"
	// invalidateBootPanicked: loading the file panicked, and a panic that
	// depends on the file would recur at every boot.
	invalidateBootPanicked = "booting from it panicked"
)

// invalidateSnapshotFile removes the saved snapshot file for this graph, so
// no later boot can adopt it, and logs reason as the cause. Called when
// something has made the file unprovable or unusable rather than merely out
// of date -- a failed eager watermark bump (NoteWatermarkBumpFailure,
// watermark.go), whose write reaches PostgreSQL without advancing the counter
// the boot's gap check compares against, or a panic while booting from it
// (recoverSnapshotFileBootPanic, background_panic.go).
//
// Best effort by design, and quiet about a file that was never there: the
// point is that no adoptable file is left behind, not that one was found. A
// removal that genuinely fails is logged at Warn, since the next boot may
// then adopt a file it should not -- there is nothing else this process can
// do about it from here.
func (e *Engine) invalidateSnapshotFile(ctx context.Context, reason string) {
	path, ok := e.snapshotFilePath()
	if !ok {
		if e.cfg.SnapshotDir != "" {
			e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file not invalidated",
				slog.String("reason", "no snapshot file path resolved yet"))
		}
		return
	}
	e.removeSnapshotFile(ctx, path, reason)
}

// removeSnapshotFile is invalidateSnapshotFile's second half, split out so it
// can be exercised against a path of the caller's choosing rather than only
// the one a resolved default graph produces.
//
// The removal is made durable (snapshot.RemoveSnapshotFile syncs the
// directory after the unlink, and even when there was nothing to unlink):
// an unlink lost to a power loss brings back exactly the file this call
// exists to take out of play -- one stamped with a counter an uncounted
// write never moved, which the next boot would find current. A sync that
// fails is logged like a removal that fails: the file is gone for now, but
// may not stay gone.
func (e *Engine) removeSnapshotFile(ctx context.Context, path, reason string) {
	removed, err := snapshot.RemoveSnapshotFile(path)
	switch {
	case err != nil:
		e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file invalidation failed",
			slog.String("path", path), slog.Bool("removed", removed), slog.Any("error", err))
	case removed:
		e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file invalidated",
			slog.String("path", path),
			slog.String("reason", reason))
	default:
		// Nothing saved yet, or already gone. Either way there is no file
		// a later boot could adopt.
	}
}
