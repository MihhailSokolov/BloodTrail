// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestFileBootPanicDiscardsTheFileAndRebuilds pins the file-boot attempt's
// panic recovery. The attempt runs on the boot-load goroutine, which no
// request can recover a panic for, so a panic while reading, replaying or
// warming the file used to end the process -- and, when the panic depended
// on the file, end it again on every restart until someone deleted the
// file. A panic there must instead be logged at Error with its stack, the
// file discarded (never adopted, and deleted so the next boot does not trip
// over it again), and the boot must fall through to the ordinary rebuild
// from PostgreSQL. A panic injected into an otherwise valid file stands in
// for a file whose decoding, replay or warm-up panics on data it cannot
// represent.
//
// The rebuild that must follow is awaited on the counter, not read off it
// after waitForFresh: rebuildOnce increments the counter from a deferred
// call that runs only after the View is published, so reading it the moment
// Fresh() flips races that increment (waitForRebuildCounted's own doc in
// file_boot_integration_test.go has the measurement).
//
// The attempt has two windows and each is covered. The read comes before
// applyMu is taken. The replay, the warm-up and the publish run with it held,
// where a recovery that tried to take it again would deadlock, so the
// injection there also checks that the lock really is held at that point and
// that the engine can take it afterwards.
func TestFileBootPanicDiscardsTheFileAndRebuilds(t *testing.T) {
	for _, window := range []struct {
		name string
		// inject makes the file-boot attempt panic in this window and undoes
		// that when the test ends. eng is the engine that will boot from the
		// file.
		inject func(t *testing.T, eng *Engine)
	}{
		{"before applyMu is taken: reading the file", func(t *testing.T, _ *Engine) {
			read := readSnapshotFile
			readSnapshotFile = func(string) (*snapshot.Snapshot, snapshot.Stamp, error) {
				panic("file boot panic injected by the test")
			}
			t.Cleanup(func() { readSnapshotFile = read })
		}},
		{"with applyMu held: warming the replayed view", func(t *testing.T, eng *Engine) {
			warm := warmSnapshotFile
			warmSnapshotFile = func(*snapshot.Snapshot) {
				if eng.applyMu.TryLock() {
					eng.applyMu.Unlock()
					t.Error("applyMu is free where the attempt is meant to hold it: the injection is not in the locked window")
				}
				panic("file boot panic injected by the test")
			}
			t.Cleanup(func() { warmSnapshotFile = warm })
		}},
	} {
		t.Run(window.name, func(t *testing.T) {
			dsn := graphtest.PGAvailable(t)
			ctx := context.Background()

			pgDriver, pool := graphtest.OpenPG(t, dsn)
			graphtest.WipeGraph(t, pgDriver)

			dir := t.TempDir()
			nodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)
			path := snapshotFilePathFor(t, pgDriver, dir)

			eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
			window.inject(t, eng)
			eng.Start(ctx)
			defer stopEngineAndCloseWritePool(eng)

			waitForFresh(t, eng)
			waitForRebuildCounted(t, eng, "after the file boot panicked")

			view, serving := eng.Fresh()
			if !serving {
				t.Fatal("engine not serving after the file boot panicked")
			}
			if _, ok := view.Dense(uint64(nodeID)); !ok {
				t.Fatal("the rebuilt replica is missing a node PostgreSQL holds")
			}
			logged := buf.String()
			var panicLine, invalidatedLine string
			for _, line := range strings.Split(logged, "\n") {
				switch {
				case strings.Contains(line, "level=ERROR") && strings.Contains(line, `msg="bloodtrail: snapshot file boot panicked"`):
					panicLine = line
				case strings.Contains(line, `msg="bloodtrail: snapshot file invalidated"`):
					invalidatedLine = line
				}
			}
			if panicLine == "" {
				t.Fatalf("no ERROR \"bloodtrail: snapshot file boot panicked\" line was logged:\n%s", logged)
			}
			if !strings.Contains(panicLine, "path="+path) {
				t.Errorf("the panic line does not name the file %s:\n%s", path, panicLine)
			}
			if invalidatedLine == "" {
				t.Fatalf("no \"bloodtrail: snapshot file invalidated\" line was logged:\n%s", logged)
			}
			if !strings.Contains(invalidatedLine, "path="+path) || !strings.Contains(invalidatedLine, `reason="booting from it panicked"`) {
				t.Errorf("the invalidation line does not name the file %s and the reason:\n%s", path, invalidatedLine)
			}
			if strings.Contains(logged, "bloodtrail: snapshot file loaded") {
				t.Fatalf("the file whose boot panicked was adopted:\n%s", logged)
			}
			if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("the snapshot file whose boot panicked is still on disk (stat: %v); the next boot would trip over it again", err)
			}
			requireApplyMuFree(t, eng)
		})
	}
}
