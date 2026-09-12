// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// triggerStartup labels the RebuildNow calls Start's boot-load goroutine
// makes (runBootLoad, below), alongside apply.go's triggerFallback, so a
// rebuild driven by the initial load is distinguishable in the log from a
// fallback-recovery or test/admin-driven one.
//
// There is deliberately no equivalent named constant for a manual or
// test-driven rebuild: every such caller is either an integration test in
// this package (which passes the literal "manual" directly -- there is
// nothing to share the label with under the plain, tag-less build every
// other caller in this package is compiled under) or code outside this
// package calling the exported RebuildNow directly (bench/pathbench, for
// one), which cannot see an unexported constant here regardless and chooses
// its own literal by the same "manual" convention.
const triggerStartup = "startup"

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
// sweepStaleSnapshotTempFiles and ensureWatermarkTable (watermark.go) both
// run first, unconditionally -- even when !cfg.Enabled: the watermark
// protocol tracks every mutating write PostgreSQL ever sees regardless of
// whether THIS engine ever serves a query from an in-memory replica, since
// a future snapshot-file consumer (possibly a different BloodTrail
// instance) still needs the counter to be trustworthy, and a stale temp
// file left by an earlier process is worth reaping even for an engine that
// will not itself write another one this run. ensureWatermarkTable is a
// no-op when e.pool is nil (its own doc), and sweepStaleSnapshotTempFiles is
// a no-op when cfg.SnapshotDir == "" (its own doc), which is what keeps
// both safe to call from a unit test built with no database and no
// snapshot directory at all.
func (e *Engine) Start(ctx context.Context) {
	e.sweepStaleSnapshotTempFiles()
	e.ensureWatermarkTable(ctx)

	if !e.cfg.Enabled {
		return
	}

	// Arm the boot gap buffer before the boot-load goroutine below could
	// possibly race a write into Apply: from here until the file attempt
	// concludes (runBootLoad) or any View is adopted, every committed
	// write's ChangeSet is buffered so the snapshot file can be adopted
	// despite it (bootgap.go). Only worth arming when there is a file that
	// could ever benefit.
	if e.cfg.SnapshotDir != "" {
		e.bootGap.activate()
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
// (fallbackRebuilding): it makes one attempt to load a local snapshot file
// (tryLoadSnapshotFile) and otherwise keeps calling rebuildOnce, labeled
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
// Each iteration is gated on the default graph having resolved
// (defaultGraphResolved, below) -- because NEITHER load can run before it
// has. This is not a defensive nicety; it is the whole reason this loop is
// shaped the way it is, and an earlier version of this method got it wrong
// in a way that made the snapshot-file feature unreachable in production:
//
//   - pg.NewDriverWithOptions does no database I/O, so a freshly opened
//     pg.Driver's SchemaManager has hasDefaultGraph == false (dawgs
//     drivers/pg/manager.go). Only SetDefaultGraph/AssertDefaultGraph -- i.e.
//     only a caller's own AssertSchema -- ever sets it.
//   - bloodtrail.Open (driver.go) calls Start BEFORE it returns the driver
//     the caller needs in order to call AssertSchema at all. So this
//     goroutine ALWAYS starts with the default graph unresolved, and the
//     caller cannot possibly have fixed that yet.
//   - The earlier version made its file attempt as a single statement before
//     this loop. snapshotFilePath's DefaultGraph() lookup therefore returned
//     ("", false) every time, so tryLoadSnapshotFile returned false without
//     ever reading -- or logging anything at all about -- the file, and the
//     attempt was spent, never retried. Measured in production ordering: 0
//     "snapshot file loaded", 0 "rejected", 0 "no snapshot file", every open,
//     with a valid matching-watermark file sitting unread on disk.
//     (internal/engine's own file-boot tests missed this because
//     internal/graphtest.OpenPG asserts the schema before the engine exists,
//     an ordering production can never produce.)
//
// Treating "no default graph yet" as a RETRYABLE condition -- rather than as
// a terminal false for the file, and rather than as the failed rebuildOnce
// call it used to produce -- is what fixes it. Both loads need the graph id:
// snapshotFilePath names the file after it, and LoadSnapshot (load.go)
// errors "no default graph is set" without it, so an iteration that runs
// before AssertSchema lands has nothing useful to attempt and is skipped
// entirely, waiting out the same backoff any other unproductive iteration
// does. That the skip also stops rebuildAttempts from counting a rebuild
// that never reached PostgreSQL is what lets a file boot honestly report
// RebuildCount() == 0 (see RebuildCount's own doc), and it stops the
// "boot load failed: no default graph is set" WARN that every driver open
// used to log exactly once -- an ordinary startup wait was never an error.
//
// The file attempt itself keeps its one-shot semantics per successful boot:
// fileTried is set the first time an iteration is actually ABLE to attempt
// it, so the attempt happens exactly once, and only once the graph id is
// knowable -- never repeatedly, and never before the pg rebuild that a
// failed attempt falls through to. Trying it before rebuildOnce in that same
// iteration is deliberate: a file load that works saves the entire pg
// rebuild, so it must not be spent first.
//
// The e.snap.Load() == nil guard is the other half of that ordering
// question: a View already adopted by anyone (a concurrent manual
// RebuildNow, say) makes a file load pointless -- the replica is already
// current -- and it would be wrong to publish an older file over it. The
// loop's own adopted-rebuild exits return before ever reaching another
// attempt, so this guard is only about adoptions from OUTSIDE this loop; it
// is a cheap, explicit statement of an invariant rather than a race to win,
// since adoptSnapshotFileView re-checks it under applyMu and independently
// refuses a file whose watermark gap its buffered writes cannot cover
// (bootGapCoveredAt), so a file that would lose a write is refused either
// way.
//
// A successful, adopted file load hands off to finishFallbackRebuild exactly
// as an adopted pg rebuild would, and returns without rebuildOnce ever being
// called at all.
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
// The cancellation check runs FIRST, before the default-graph probe and
// before any load attempt, so a Stop during startup still exits promptly.
func (e *Engine) runBootLoad(ctx context.Context) {
	backoff := fallbackRetryInterval
	fileTried := false

	for {
		if ctx.Err() != nil || e.bgCtx.Err() != nil {
			e.fallbackRebuilding.Store(false)
			return
		}

		var err error
		if !e.defaultGraphResolved() {
			e.cfg.Log.DebugContext(e.bgCtx, "bloodtrail: boot load waiting for the default graph")
		} else {
			if !fileTried {
				fileTried = true
				if e.snap.Load() == nil && e.tryLoadSnapshotFile(e.bgCtx) {
					e.finishFallbackRebuild()
					return
				}
				// The one file attempt is spent: nothing can ever consume
				// the boot gap buffer now (adoption through the pg rebuild
				// path reads post-write state and needs no replay), so stop
				// paying for it. Idempotent when the attempt already
				// consumed the buffer itself (adoptSnapshotFileView's take).
				e.bootGap.deactivate()
			}

			var adopted bool
			if adopted, err = e.rebuildOnce(e.bgCtx, triggerStartup); err != nil {
				e.cfg.Log.WarnContext(e.bgCtx, "bloodtrail: boot load failed", slog.Any("error", err))
			} else if adopted {
				e.finishFallbackRebuild()
				return
			}
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

// defaultGraphResolved reports whether the embedded PostgreSQL driver's
// default graph is known yet -- the precondition BOTH of boot load's two
// loads share (snapshotFilePath names its file after the graph id;
// LoadSnapshot errors without it), and the one thing a freshly opened
// pg.Driver is guaranteed NOT to have (see runBootLoad's doc).
//
// A nil pgDriver reports false rather than panicking, matching
// snapshotFilePath's own care about the same field: unit tests in this
// package routinely build an Engine with no driver at all, and an engine
// that has no driver genuinely has no default graph -- there is nothing for
// boot load to load from, so waiting is the honest answer rather than a
// nil-pointer dereference inside a background goroutine.
func (e *Engine) defaultGraphResolved() bool {
	if e.pgDriver == nil {
		return false
	}
	_, ok := e.pgDriver.DefaultGraph()
	return ok
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
//
// The ("", false) an unresolved default graph produces here is a genuine
// "cannot answer yet", not "no file": boot load must never spend its one
// file attempt on it, which is why runBootLoad gates that attempt on
// defaultGraphResolved instead of letting this return stand in for a miss
// (see runBootLoad's doc). SaveSnapshot's own caller (persist.go) is under
// no such constraint -- by the time anything is worth saving, a snapshot has
// been adopted, which cannot have happened without the graph resolving.
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

// snapshotTempFilePattern is snapshot.WriteSnapshotFile's own os.CreateTemp
// pattern (internal/engine/snapshot/file.go), repeated here so
// sweepStaleSnapshotTempFiles recognizes -- and only ever recognizes --
// exactly the files that pattern can produce, never anything else that
// happens to live alongside them in cfg.SnapshotDir (a real graph-<id>.btsnap
// file included).
const snapshotTempFilePattern = ".snapshot-*.tmp"

// sweepStaleSnapshotTempFiles removes every leftover WriteSnapshotFile temp
// file (snapshotTempFilePattern) sitting directly in cfg.SnapshotDir -- the
// one thing nothing else in this codebase ever reaps. WriteSnapshotFile
// (snapshot/file.go) writes that pattern's directory argument as
// filepath.Dir(path), and path is always cfg.SnapshotDir joined with a
// graph-<id>.btsnap leaf (snapshotFilePath, above), so every temp file this
// engine could ever create lives directly in cfg.SnapshotDir regardless of
// which graph it was for -- this sweep does not need the graph id to
// resolve first, unlike snapshotFilePath's own file.
//
// WriteSnapshotFile's own defer already removes its temp file on any
// ordinary error return (its own doc), so the only way one survives is a
// SIGKILL landing between os.CreateTemp and the rename -- exactly the case
// driver.go's snapshotSaveTimeout doc calls out: the fold and the write
// past the watermark read take no context at all, so a database that has
// stopped answering is not the only way a shutdown save can be running when
// the container runtime's own grace period expires. Left alone, each such
// file is full-graph-sized (WriteSnapshotFile's payload is a complete copy
// of the graph), and repeated hard stops accumulate them indefinitely: the
// boot loader only ever opens the final graph-<id>.btsnap name
// (snapshotFilePath), never a *.tmp, so nothing else would ever notice or
// remove one.
//
// Called exactly once, from Start, before this engine's own boot-load
// goroutine, any background compaction, or Driver.Close's own shutdown save
// could possibly attempt a write of its own -- which is what makes this
// safe under this feature's existing single-writer assumption (at most one
// BloodTrail process owns a given SnapshotDir at a time -- the same
// assumption adoptSnapshotFileView's watermark-gap trust (bootGapCoveredAt)
// already depends on: one BloodHound container, one bind-mounted
// directory). Under
// that assumption, sweeping HERE -- and only here -- is provably safe: by
// construction this process has not attempted a single write of its own
// yet, so every matching temp file already in the directory can only be a
// leftover from an EARLIER process's interrupted write, never one a peer is
// still writing right now. Sweeping again later in this same process's life
// (e.g. immediately before this process's own next write) would not have
// that guarantee -- a concurrent compaction save and a concurrent shutdown
// save could, in principle, both be genuinely mid-write at that point -- so
// this package deliberately sweeps once, at boot, and never again.
//
// A no-op, without ever touching the filesystem, when cfg.SnapshotDir == ""
// (the feature disabled) -- matching snapshotFilePath's identical
// short-circuit, and safe to call unconditionally from Start for every
// engine that never set it, including one built with a nil pgDriver in a
// test that has no reason to care about this feature at all.
func (e *Engine) sweepStaleSnapshotTempFiles() {
	if e.cfg.SnapshotDir == "" {
		return
	}

	matches, err := filepath.Glob(filepath.Join(e.cfg.SnapshotDir, snapshotTempFilePattern))
	if err != nil {
		// filepath.Glob's only possible error is ErrBadPattern -- unreachable
		// here since snapshotTempFilePattern is a fixed, valid, compile-time
		// constant -- so there is nothing an operator could act on; treated
		// as "nothing found" rather than logged.
		return
	}

	for _, path := range matches {
		if err := os.Remove(path); err != nil {
			e.cfg.Log.WarnContext(context.Background(), "bloodtrail: failed to remove stale snapshot temp file",
				slog.String("path", path),
				slog.Any("error", err),
			)
			continue
		}
		e.cfg.Log.InfoContext(context.Background(), "bloodtrail: removed stale snapshot temp file",
			slog.String("path", path),
		)
	}
}

// tryLoadSnapshotFile makes one attempt to boot this engine straight from
// its own snapshot file (snapshotFilePath) instead of PostgreSQL, reporting
// whether it actually adopted one. Called once per successful boot, from
// inside runBootLoad's retry loop -- in the first iteration whose
// defaultGraphResolved check passes, before that same iteration's
// rebuildOnce call. See runBootLoad's own doc for why the attempt has to be
// made from inside the loop rather than before it, and why a single attempt,
// with no retry schedule of its own, is still enough once it is: any failure
// here simply falls through to the retry loop that already exists for the pg
// path.
//
// settledGen is read BEFORE ReadSnapshotFile even opens the file, mirroring
// rebuildOnce's identical ordering and for the identical reason (that
// method's own doc): it is the watermark trust generation this adoption is
// entitled to resolve. There is no applyEpoch read to pair it with anymore:
// a write applied while the file loads is no longer a reason to reject it --
// the boot gap buffer captured it, and adoptSnapshotFileView replays it --
// so the epoch check's job is done instead by that adoption's own
// counter-coverage proof (bootGapCoveredAt) plus its settledDirtyGen re-check
// under applyMu, which together refuse exactly the writes the replay cannot
// account for.
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
// Adoption goes through adoptSnapshotFileView (below), which publishes
// under the same applyMu serialization adoptRebuiltView does and applies
// the identical cfg.MemoryLimit budget to the view it actually publishes
// (base plus replayed segments). The base snapshot's own over-limit check
// still runs here first, before any lock is taken: a file that is too big
// on its own cannot become smaller by replaying writes onto it, and
// refusing it early keeps the applyMu hold time for genuine candidates
// only. An over-limit file is refused exactly like an over-limit pg-loaded
// snapshot (rebuildOnce's own doc): the caller falls through to the pg
// rebuild loop, whose own budget backoff (fallbackRetryDelay) takes over
// from there.
func (e *Engine) tryLoadSnapshotFile(ctx context.Context) bool {
	path, ok := e.snapshotFilePath()
	if !ok {
		return false
	}

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

	replayed, adopted := e.adoptSnapshotFileView(ctx, path, snap, fileWatermark, settledGen)
	if !adopted {
		return false
	}

	e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file loaded",
		slog.String("path", path),
		slog.Uint64("watermark", fileWatermark),
		slog.Int("nodes", snap.NodeCount()),
		slog.Int("edges", snap.EdgeCount()),
		slog.Int("replayed_writes", replayed),
	)
	return true
}

// bootGapSettleTimeout and bootGapSettleRetryInterval pace the settle-wait
// in adoptSnapshotFileView: how long an adoption may wait, in how fine a
// step, for the boot gap buffer to cover the frozen watermark target before
// giving the file up to the pg rebuild. The wait a covered adoption
// actually needs is the remaining lifetime of the slowest write in flight
// at freeze time -- a batch chunk's bump commits at its first buffered
// operation, its Apply at the flush that commits the chunk, and
// bench/applybench measured its largest chunks (20k-op flushes at 5M)
// taking ~1.5-2s wall between the two; ordinary BloodHound ingest flushes
// are smaller. 5s is ~2.5x that worst case, and the cost of exhausting it
// is bounded and honest: 5s plus exactly the rebuild that would have run
// anyway. Vars rather than consts so the integration tests can exercise
// the timeout path without a five-second stall; deliberately NOT Config --
// there is no operator judgment to invite here.
var (
	bootGapSettleTimeout       = 5 * time.Second
	bootGapSettleRetryInterval = 50 * time.Millisecond
)

// adoptAttempt is one adoption attempt's verdict. Adopted and rejected are
// terminal (a rejected attempt has already logged its reason); notYetCovered
// is the one verdict the settle-wait loop sleeps on and retries -- the gap
// to the frozen target has a hole that an in-flight write's own Apply may
// yet fill.
type adoptAttempt int

const (
	adoptAttemptAdopted adoptAttempt = iota
	adoptAttemptRejected
	adoptAttemptNotYetCovered
)

// adoptSnapshotFileView is the snapshot-file boot's publish step: it
// decides whether the file-loaded snapshot plus a replay of the boot gap
// buffer's writes reproduces PostgreSQL's state at a FROZEN watermark
// target, and publishes the combined View if so -- waiting, bounded, for
// in-flight writes to settle rather than sampling a single instant.
// Reports how many buffered writes it replayed, and whether it adopted at
// all; every rejection logs its own "bloodtrail: snapshot file rejected"
// line naming why, so the caller has nothing left to log on the false path.
//
// The settle-wait exists because the one-instant decision this replaces
// could not survive concurrent writes at all: a batch write's bump commits
// eagerly at the chunk's first buffered operation while the Apply the
// buffer observes runs only at the flush that commits it, so under any
// sustained write stream there is an unaccounted in-flight bump at
// essentially every instant, and sampling one instant rejects essentially
// every time (bench/applybench measured a flat-out writer losing 3/3).
// Freezing the target once and WAITING for the buffer -- still armed, still
// observing -- to cover it converges under any write pattern in which
// individual writes finish inside bootGapSettleTimeout: every bump at or
// below the frozen target belongs to a committed write of THIS process,
// whose Apply (or ResolveAbandonedWrite) arrives within that write's own
// remaining lifetime. Bumps ABOVE the target need no accounting at all,
// by cases: a post-freeze write whose Apply already ran (a no-op against
// the nil snapshot) is in the buffer and rides the replay below; one whose
// Apply is parked on applyMu lands after publish as an ordinary delta.
// Both stage read-back truth -- pg's current committed state per key,
// never the write's own payload -- so replay order cannot matter, the
// same argument the replay paragraph below already makes for same-key
// rewrites. The freeze itself therefore needs no lock: earlier bumps are
// waited for, later ones are tolerated by construction.
//
// The proof each attempt demands, evaluated under applyMu so no Apply can
// move anything mid-attempt:
//
//   - No View was adopted while the file was loading or the wait was
//     waiting (a concurrent manual RebuildNow, say) -- publishing an older
//     file over one would lose whatever that adoption contained.
//   - The engine is still stateServing: a write that tripped fallback
//     could not be replayed then and cannot be now.
//   - settledDirtyGen has not moved since the pre-load read: a watermark
//     failure that settled mid-load or mid-wait belongs to a write whose
//     bump never produced a counter, which the coverage check below is
//     therefore blind to -- rejecting is the only sound answer, exactly as
//     the retired epoch check would have. (Apply-side settles cannot race
//     this -- Apply holds applyMu -- and a ResolveAbandonedWrite settle
//     slipping in after this check changed nothing in PostgreSQL, so the
//     file is still faithful; its trust generation stays unresolved until
//     finishFallbackRebuild's recheck relaunches a rebuild for it, the
//     same handling that race already has everywhere else.)
//   - bootGapCoveredAt: the buffered counters at or below the frozen
//     target are exactly the file's counter through the target, with no
//     hole and nothing the buffer could not faithfully replay (a poisoned
//     buffer rejects immediately -- poison never heals, so there is
//     nothing to wait for). The attempt PEEKS for this check and only the
//     covered attempt take()s, so the buffer keeps observing the very
//     Applies the wait is waiting for.
//
// The replay itself is Apply's own machinery, reused verbatim per buffered
// write -- readBack for pg's post-commit truth on every key the ChangeSet
// names, buildApplySegment, WithSegment -- onto an UNPUBLISHED view, in
// ascending counter order, with nothing stored until every segment landed:
// a reader either sees no snapshot at all (declining to pg, exactly as it
// did all boot) or the fully replayed result, never a half-replayed file.
// Read-backs running here rather than at each write's own commit change
// nothing: read-back always returns pg's current committed truth for a
// key, so a later write to the same key simply means both replays stage
// that same final truth (and that write's own delta lands on top once
// applyMu releases, exactly as it would against any published View).
//
// cfg.MemoryLimit is applied to the view actually being published --
// base plus replay segments -- mirroring Apply's identical check on the
// view IT publishes; maintainAfterPublish then runs for the same reason
// Apply runs it (a replay-heavy adoption can arrive with an overgrown
// segment stack, and the compaction trigger owns that judgment).
//
// resolvedDirtyGen advances to the pre-load settledGen exactly as
// adoptRebuiltView's identical line does, and for the identical reason
// (its own doc): the settledDirtyGen re-check above proves no failure
// settled between that read and this publish, so every failure counted in
// settledGen belongs to a write whose committed rows are either in the
// file or in the replay.
func (e *Engine) adoptSnapshotFileView(ctx context.Context, path string, snap *snapshot.Snapshot, fileWatermark, settledGen uint64) (replayed int, adopted bool) {
	reject := func(reason string, attrs ...any) {
		e.cfg.Log.InfoContext(ctx, "bloodtrail: snapshot file rejected",
			append([]any{slog.String("path", path), slog.String("reason", reason)}, attrs...)...)
	}

	// The frozen target (see the doc above for why no lock is needed here):
	// everything at or below it must be accounted for before adoption,
	// everything above it is safe by construction.
	pgSnapshot, err := e.ReadWatermark(ctx)
	if err != nil {
		reject("read pg watermark failed", slog.Any("error", err))
		return 0, false
	}

	start := time.Now()
	deadline := start.Add(bootGapSettleTimeout)
	for {
		replayed, verdict, buffered := e.adoptSnapshotFileAttempt(ctx, snap, fileWatermark, pgSnapshot, settledGen, reject)
		switch verdict {
		case adoptAttemptAdopted:
			return replayed, true
		case adoptAttemptRejected:
			return 0, false
		}

		if time.Now().After(deadline) {
			reject("boot gap not covered by buffered writes",
				slog.Uint64("file_watermark", fileWatermark),
				slog.Uint64("pg_watermark", pgSnapshot),
				slog.Int("buffered_writes", buffered),
				slog.Duration("waited", time.Since(start)),
			)
			return 0, false
		}
		select {
		case <-ctx.Done():
			reject("boot cancelled while waiting for in-flight writes to settle",
				slog.Duration("waited", time.Since(start)))
			return 0, false
		case <-time.After(bootGapSettleRetryInterval):
		}
	}
}

// adoptSnapshotFileAttempt is one locked attempt of the settle-wait loop
// above: prechecks, coverage against the frozen target, and -- only when
// covered -- the take, replay and publish. Terminal rejections log through
// reject before returning adoptAttemptRejected; adoptAttemptNotYetCovered
// logs nothing (the loop owns the eventual timeout line) and reports how
// many writes were buffered at the time, for that line's use.
func (e *Engine) adoptSnapshotFileAttempt(ctx context.Context, snap *snapshot.Snapshot, fileWatermark, pgSnapshot, settledGen uint64, reject func(string, ...any)) (replayed int, verdict adoptAttempt, buffered int) {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()

	if e.snap.Load() != nil {
		reject("a view was already adopted while the file was loading")
		return 0, adoptAttemptRejected, 0
	}
	if e.state.Load() != stateServing {
		reject("the engine entered fallback while the file was loading")
		return 0, adoptAttemptRejected, 0
	}
	if e.settledDirtyGen.Load() != settledGen {
		reject("a watermark failure settled while the file was loading")
		return 0, adoptAttemptRejected, 0
	}

	peeked, poisoned := e.bootGap.peek()
	if poisoned != "" {
		reject("boot write buffer poisoned: " + poisoned)
		return 0, adoptAttemptRejected, 0
	}

	counters := make([]uint64, len(peeked))
	for i, entry := range peeked {
		counters[i] = entry.counter
	}
	if !bootGapCoveredAt(fileWatermark, pgSnapshot, counters) {
		return 0, adoptAttemptNotYetCovered, len(peeked)
	}

	// Covered: consume. A ResolveAbandonedWrite observe landing between the
	// peek and this take rides along harmlessly -- counter-only entries are
	// skipped by the replay -- but a poison landing in that window must
	// still reject, so the take's own poison answer is checked again.
	entries, poisoned := e.bootGap.take()
	if poisoned != "" {
		reject("boot write buffer poisoned: " + poisoned)
		return 0, adoptAttemptRejected, 0
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].counter < entries[j].counter })

	view := snapshot.NewView(snap)
	for _, entry := range entries {
		if entry.cs == nil {
			continue
		}
		rb, err := e.readBack(ctx, entry.cs)
		if err != nil {
			reject("boot write replay failed", slog.Any("error", err))
			return 0, adoptAttemptRejected, 0
		}
		seg, err := buildApplySegment(view, rb, entry.cs)
		if err != nil {
			reject("boot write replay failed", slog.Any("error", err))
			return 0, adoptAttemptRejected, 0
		}
		view = view.WithSegment(seg)
		replayed++
	}

	if e.cfg.MemoryLimit > 0 {
		if viewBytes := view.ApproxBytes(); viewBytes > uint64(e.cfg.MemoryLimit) {
			reject("exceeds memory limit",
				slog.Uint64("bytes", viewBytes),
				slog.Uint64("limit", uint64(e.cfg.MemoryLimit)),
			)
			return 0, adoptAttemptRejected, 0
		}
	}

	e.snap.Store(view)
	e.resolvedDirtyGen.Store(maxWatermark(e.resolvedDirtyGen.Load(), settledGen))
	e.maintainAfterPublish(ctx, view)
	return replayed, adoptAttemptAdopted, 0
}
