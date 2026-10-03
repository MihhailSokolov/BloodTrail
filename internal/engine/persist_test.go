// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestSaveSnapshotPreconditionsForRequiresEveryCondition pins
// saveSnapshotPreconditionsFor's own predicate, mirroring
// TestWatermarkTrustedForRequiresEveryCondition's identical table shape
// (watermark_test.go) for WatermarkTrusted's own pure decision.
//
// The "fallback, otherwise clean and converged" case is the one worth
// reading twice: it pins the exact scenario saveSnapshotPreconditionsFor's
// own doc walks through in full -- an engine caught in stateFallback by an
// everyday ChangeSet fallback record (Run/WipeGraph/SetDefaultGraph, or a
// read-back/segment-build failure inside Apply) can have fully "clean"
// watermark generations and a fully converged counter at the same time,
// because dirtyGen/resolvedGen and watermarkConverged track a write's
// COUNTER bookkeeping, never whether its DATA effect could be replayed.
// Without the state==stateServing half of this predicate, SaveSnapshot
// would fold and persist that stale View anyway, stamped with a watermark
// a later boot would consider a perfect, trustworthy match.
func TestSaveSnapshotPreconditionsForRequiresEveryCondition(t *testing.T) {
	cases := []struct {
		name      string
		state     int32
		dirty     uint64
		resolved  uint64
		converged bool
		want      bool
	}{
		{"serving, no failure ever noted, converged", stateServing, 0, 0, true, true},
		{"serving, every failure resolved, converged", stateServing, 3, 3, true, true},
		{"serving, a failure not yet resolved by any adoption", stateServing, 3, 2, true, false},
		{"serving, resolved but the counters have not converged", stateServing, 3, 3, false, false},
		{"fallback, otherwise clean and converged", stateFallback, 3, 3, true, false},
		{"fallback, nothing holds", stateFallback, 3, 1, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := saveSnapshotPreconditionsFor(tc.state, tc.dirty, tc.resolved, tc.converged); got != tc.want {
				t.Fatalf("saveSnapshotPreconditionsFor(%d, %d, %d, %v) = %v, want %v",
					tc.state, tc.dirty, tc.resolved, tc.converged, got, tc.want)
			}
		})
	}
}

// TestSaveSnapshotWriteTakesBackAFileWhoseWriteReportedAnError pins that a
// save runs its post-write check even when writing reported an error: an
// error from syncing the directory after the rename leaves a complete file
// in place (snapshot.WriteSnapshotFile's doc). If a watermark bump failed
// while that file was being written -- its own removal having run before
// the file landed -- the file vouches for a counter an uncounted write never
// moved, and the save has to take it back exactly as it does after a write
// that reported nothing.
func TestSaveSnapshotWriteTakesBackAFileWhoseWriteReportedAnError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "graph-1.btsnap")

	var logged bytes.Buffer
	e := New(nil, nil, Config{Enabled: true, SnapshotDir: dir, Log: slog.New(slog.NewTextHandler(&logged, nil))})
	snap, err := snapshot.NewBuilder(1).Build()
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	pending := &pendingSnapshotSave{snap: snap, dirtyGen: e.dirtyGen.Load()}

	syncFailure := errors.New("simulated directory sync failure")
	prev := writeSnapshotFile
	writeSnapshotFile = func(path string, s *snapshot.Snapshot, stamp snapshot.Stamp) error {
		// The bump fails before the file lands, so its own removal finds
		// nothing to take away.
		e.NoteWatermarkBumpFailure(ctx, NewWriteScope(), errors.New("simulated bump failure"))
		if err := prev(path, s, stamp); err != nil {
			return err
		}
		return syncFailure
	}
	t.Cleanup(func() { writeSnapshotFile = prev })

	if err := e.saveSnapshotWrite(ctx, path, pending, snapshot.Stamp{Watermark: 5}, time.Now()); !errors.Is(err, syncFailure) {
		t.Fatalf("saveSnapshotWrite = %v, want the write's own error reported", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a file written across a failed bump was left in place because its write reported an error (stat: %v):\n%s", err, logged.String())
	}
	if !strings.Contains(logged.String(), "a watermark bump failed while the file was being written") {
		t.Fatalf("the save did not say why it took its file back:\n%s", logged.String())
	}
}
