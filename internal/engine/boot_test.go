// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// -----------------------------------------------------------------------
// Start's boot-load goroutine and the fallback recovery goroutine must
// never run concurrently. Before this fix, boot.go
// claimed they never could -- "Apply only ever calls enterFallback when
// e.snap.Load() != nil" -- but apply.go's nil-scope and HasFallback
// branches call enterFallback BEFORE Apply's own nil-snapshot check runs
// (see apply.go's numbered steps 3-4 vs. step 7), so a Run()/WipeGraph()/
// unrecognized write landing before boot load's first adoption could start
// a second, redundant rebuild loop racing the first. Both tests below drive
// claimRebuildLoop's shared CAS on fallbackRebuilding directly, the same
// way apply_test.go's F4 tests drive finishFallbackRebuild/startFallbackRebuild
// -- deterministic, no goroutine scheduling involved, and no database.
// -----------------------------------------------------------------------

// TestStartDoesNotDoubleARebuildLoopAgainstAPreBootFallback simulates the
// race's first ordering: some write's enterFallback call wins the shared
// CAS before Start ever runs (standing in for fallbackRebuilding.Store(true)
// -- exactly what startFallbackRebuild's own CAS success would have left
// behind). Start's own claimRebuildLoop attempt must then lose and return
// without spawning runBootLoad, leaving the flag exactly as the winning
// caller left it.
func TestStartDoesNotDoubleARebuildLoopAgainstAPreBootFallback(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.fallbackRebuilding.Store(true) // stand-in for a fallback trip that won the race first

	e.Start(context.Background())

	if !e.fallbackRebuilding.Load() {
		t.Fatalf("fallbackRebuilding = false after Start lost the CAS, want it to stay true (still held by the pre-boot fallback loop)")
	}
	if got := e.state.Load(); got != stateServing {
		t.Fatalf("state after Start lost the CAS = %d, want it untouched at stateServing (%d) -- Start must not itself flip fallback state", got, stateServing)
	}
}

// TestEnterFallbackDoesNotDoubleARebuildLoopDuringBoot simulates the race's
// other ordering: Start's boot-load goroutine wins the shared CAS first --
// the common case, since Start normally runs before any write could
// possibly fail -- and a write then trips enterFallback while boot load is
// still retrying. enterFallback's own state transition still fires
// (a write really could not be replayed, so "fallback entered" is a true
// event here, not noise -- see boot.go's Start doc), but its
// startFallbackRebuild call must lose the CAS to the already-running boot
// loop rather than start a second one.
func TestEnterFallbackDoesNotDoubleARebuildLoopDuringBoot(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.fallbackRebuilding.Store(true) // stand-in for Start's boot-load goroutine already claiming the gate

	e.enterFallback(context.Background(), "write during boot")

	if got := e.state.Load(); got != stateFallback {
		t.Fatalf("state after enterFallback = %d, want stateFallback (%d): a write that cannot be replayed is a genuine fallback trip even before boot load has adopted anything", got, stateFallback)
	}
	if !e.fallbackRebuilding.Load() {
		t.Fatalf("fallbackRebuilding = false after enterFallback, want it to stay true -- still held by the boot-load loop, not reclaimed for a second one")
	}
}

// TestRunBootLoadNeverFlipsToFallbackOnContextCancel pins the other half of
// F1: ordinary startup -- no write ever calling enterFallback -- must never
// flip state to stateFallback and so must never log "fallback entered"
// (that line is only ever reachable through enterFallback's own state CAS).
// bgCancel makes runBootLoad's very first check return immediately, before
// it would ever reach rebuildOnce/LoadSnapshot, so this runs synchronously
// with no database and no goroutine.
func TestRunBootLoadNeverFlipsToFallbackOnContextCancel(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.bgCancel()
	e.fallbackRebuilding.Store(true) // stand-in for Start's own claimRebuildLoop call having already run

	e.runBootLoad(context.Background())

	if got := e.state.Load(); got != stateServing {
		t.Fatalf("state after runBootLoad's context-cancelled return = %d, want stateServing (%d): plain startup must never call enterFallback", got, stateServing)
	}
	if e.fallbackRebuilding.Load() {
		t.Fatalf("fallbackRebuilding still true after runBootLoad's context-cancelled return, want it cleared")
	}
}

// TestDefaultGraphResolvedIsNilSafe pins the nil guard runBootLoad's
// per-iteration gate depends on: every unit test in this package builds an
// Engine with a nil pgDriver, and runBootLoad now probes the default graph
// on every iteration -- so a missing guard would turn an ordinary
// database-free unit test (or, worse, a production Engine constructed
// before its driver) into a nil-pointer panic inside a background
// goroutine, where nothing would ever recover it.
func TestDefaultGraphResolvedIsNilSafe(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})

	if e.defaultGraphResolved() {
		t.Fatalf("defaultGraphResolved() = true with a nil pgDriver, want false")
	}
}

// -----------------------------------------------------------------------
// Snapshot-file boot load. The test below pins snapshotFilePath's disabled
// short-circuit; the trust predicate's own pure tests live in
// bootgap_test.go (bootGapCoveredAt), and the full read-compare-adopt
// sequence (tryLoadSnapshotFile/adoptSnapshotFileView) is exercised end to
// end, against a live PostgreSQL watermark table and real snapshot files,
// by file_boot_integration_test.go.
// -----------------------------------------------------------------------

// TestSnapshotFilePathDisabledWhenDirEmpty pins the order snapshotFilePath
// must check things in: cfg.SnapshotDir == "" has to return before ever
// calling e.pgDriver.DefaultGraph(), so that the snapshot-file feature is a
// genuine no-op -- no default-graph lookup, no filesystem I/O -- for every
// engine that never set it, including this one, built with a nil pgDriver
// on purpose: reordering the checks would turn this test into a nil-pointer
// panic instead of a clean assertion.
func TestSnapshotFilePathDisabledWhenDirEmpty(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})

	if path, ok := e.snapshotFilePath(); ok || path != "" {
		t.Fatalf("snapshotFilePath() = (%q, %v) with SnapshotDir empty, want (\"\", false)", path, ok)
	}
}

// -----------------------------------------------------------------------
// Stale temp-file sweep: a process SIGKILLed between WriteSnapshotFile's
// os.CreateTemp and its own rename leaves a ".snapshot-*.tmp" file behind
// that nothing else in the codebase ever reaps -- the boot loader only ever
// opens the final graph-<id>.btsnap name. sweepStaleSnapshotTempFiles closes
// that: called once from Start, before this process could possibly have
// written a temp file of its own, so anything matching the pattern already
// in cfg.SnapshotDir can only be a leftover from an earlier process's
// interrupted write.
// -----------------------------------------------------------------------

// TestSweepStaleSnapshotTempFilesRemovesOnlyTheTempPattern pins the sweep's
// own selectivity: a leftover ".snapshot-*.tmp" file (WriteSnapshotFile's
// exact os.CreateTemp pattern, snapshot/file.go) must be removed, while a
// real .btsnap file and an unrelated file that merely happens to end in
// ".tmp" -- neither of which WriteSnapshotFile could ever have produced --
// are left completely untouched.
func TestSweepStaleSnapshotTempFilesRemovesOnlyTheTempPattern(t *testing.T) {
	dir := t.TempDir()

	stale := filepath.Join(dir, ".snapshot-123456789.tmp")
	if err := os.WriteFile(stale, []byte("stale temp file"), 0o600); err != nil {
		t.Fatalf("write stale temp file: %v", err)
	}

	realSnapshot := filepath.Join(dir, "graph-1.btsnap")
	if err := os.WriteFile(realSnapshot, []byte("not a temp file"), 0o600); err != nil {
		t.Fatalf("write real snapshot file: %v", err)
	}

	unrelatedTmp := filepath.Join(dir, "unrelated.tmp")
	if err := os.WriteFile(unrelatedTmp, []byte("not the snapshot pattern"), 0o600); err != nil {
		t.Fatalf("write unrelated .tmp file: %v", err)
	}

	e := New(nil, nil, Config{Enabled: true, SnapshotDir: dir})
	e.sweepStaleSnapshotTempFiles()

	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat stale temp file after sweep = %v, want fs.ErrNotExist (the sweep should have removed it)", err)
	}
	if _, err := os.Stat(realSnapshot); err != nil {
		t.Fatalf("stat real snapshot file after sweep = %v, want nil -- the sweep must never touch a real .btsnap file", err)
	}
	if _, err := os.Stat(unrelatedTmp); err != nil {
		t.Fatalf("stat unrelated .tmp file after sweep = %v, want nil -- the sweep must match snapshotTempFilePattern exactly, not any *.tmp file", err)
	}
}

// TestSweepStaleSnapshotTempFilesNoopWhenDirEmpty pins the same
// disabled-feature short-circuit snapshotFilePath already documents: no
// filesystem I/O at all when cfg.SnapshotDir == "", so this stays safe to
// call unconditionally from Start for every engine that never set it --
// including this one, built with a nil pgDriver on purpose, which a
// filepath.Glob against an empty/relative pattern could otherwise turn into
// a surprising directory scan.
func TestSweepStaleSnapshotTempFilesNoopWhenDirEmpty(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.sweepStaleSnapshotTempFiles() // must return immediately without panicking
}
