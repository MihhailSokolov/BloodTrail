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
// from PostgreSQL. A read that panics on a valid file stands in for a file
// whose decoding or replay panics on data it cannot represent.
func TestFileBootPanicDiscardsTheFileAndRebuilds(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	nodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)
	path := snapshotFilePathFor(t, pgDriver, dir)

	read := readSnapshotFile
	readSnapshotFile = func(string) (*snapshot.Snapshot, snapshot.Stamp, error) {
		panic("file boot panic injected by the test")
	}
	t.Cleanup(func() { readSnapshotFile = read })

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.Start(ctx)
	defer eng.Stop()

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
	panicLogged := false
	for _, line := range strings.Split(logged, "\n") {
		if strings.Contains(line, "level=ERROR") && strings.Contains(line, `msg="bloodtrail: snapshot file boot panicked"`) {
			panicLogged = true
		}
	}
	if !panicLogged {
		t.Fatalf("no ERROR \"bloodtrail: snapshot file boot panicked\" line was logged:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file loaded") {
		t.Fatalf("the file whose boot panicked was adopted:\n%s", logged)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the snapshot file whose boot panicked is still on disk (stat: %v); the next boot would trip over it again", err)
	}
	requireApplyMuFree(t, eng)
}
