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
// what does not, once Stop has already run.
//
// Three cases are a no-op, returning nil rather than an error, because
// there is genuinely nothing to save, not because anything went wrong:
//
//   - The snapshot-file feature is disabled (cfg.SnapshotDir == "");
//     snapshotFilePath reports this without ever touching the filesystem.
//   - No snapshot has ever been adopted (e.snap.Load() == nil).
//   - The save preconditions don't hold -- logged at Debug, naming which
//     of the three conditions failed, purely for an operator's
//     observability; Driver.Close treats every SaveSnapshot outcome as
//     best-effort regardless, so this is never surfaced as something the
//     caller has to react to.
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

	view := e.snap.Load()
	if view == nil {
		return nil
	}

	state := e.state.Load()
	dirtyGen, resolvedGen := e.watermarkGens()
	pgCounter, converged := e.watermarkConverged(ctx)

	if !saveSnapshotPreconditionsFor(state, dirtyGen, resolvedGen, converged) {
		e.cfg.Log.DebugContext(ctx, "bloodtrail: snapshot file not written",
			slog.Bool("serving", state == stateServing),
			slog.Bool("gens_equal", dirtyGen == resolvedGen),
			slog.Bool("converged", converged),
		)
		return nil
	}

	start := time.Now()

	base := view.Base()
	snap := base
	if segments := view.Segments(); len(segments) > 0 {
		folded, err := snapshot.Fold(base, segments)
		if err != nil {
			e.cfg.Log.WarnContext(ctx, "bloodtrail: snapshot file write failed", slog.String("step", "fold"), slog.Any("error", err))
			return fmt.Errorf("engine: SaveSnapshot: fold: %w", err)
		}
		snap = folded
	}
	foldDuration := time.Since(start)

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
// which a single sample could miss. SaveSnapshot runs strictly after
// Stop() (Driver.Close's own calling contract: engine.Stop() first, this
// second, while the pool is still open), so no new background rebuild can
// ever start again, and a graceful shutdown is the one moment this engine
// can assume no concurrent call is still driving BumpWatermark/Apply --
// there is no live traffic left to race. One sample is therefore enough,
// and "no in-flight applies" -- the thing a second sample would otherwise
// exist to catch -- is exactly what converged's own inflight == 0 half
// already proves for the single sample taken here (watermarkConverged's
// own doc); nothing further needs checking for it.
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
