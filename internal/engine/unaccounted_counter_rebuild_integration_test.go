// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestUnaccountedCounterSchedulesItsOwnRebuild: a counter value this process
// never resolved -- a second BloodTrail server's bump, a lost bump response,
// the installer's lineage end -- makes `unaccounted()` true, and nothing
// this process does on its own ever accounts for it again. Only a rebuild's
// ledger rebase can, so the refused save has to ask for one; without that,
// every save for the rest of the process's life is refused with the same
// Warn and no snapshot file is ever written again.
func TestUnaccountedCounterSchedulesItsOwnRebuild(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{persistRaceKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	dir := t.TempDir()
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	resetWatermarkTable(t, ctx, eng)
	defer eng.Stop()
	adoptOneRebuild(t, ctx, eng)
	path := snapshotFilePathFor(t, pgDriver, dir)

	// A counter nobody here will ever resolve: a second server's own bump.
	other := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if _, err := other.BumpWatermark(ctx); err != nil {
		t.Fatalf("the other server's bump: %v", err)
	}

	loopsBefore := eng.rebuildLoopStarts.Load()
	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	requireNoSnapshotFile(t, path, buf.String(), "over a counter this process never resolved")
	if logged := buf.String(); !strings.Contains(logged, "never resolved") {
		t.Fatalf("the refused save did not say the counter holds values this process never resolved:\n%s", logged)
	}

	// The refusal must schedule the rebuild that can account for it.
	deadline := time.Now().Add(20 * time.Second)
	for eng.rebuildLoopStarts.Load() == loopsBefore {
		if time.Now().After(deadline) {
			t.Fatalf("no rebuild was ever scheduled after a save refused over an unaccounted counter:\n%s", buf.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// And once that rebuild has been adopted, the engine saves again by
	// itself -- the same save call that was refused above.
	for {
		if err := eng.SaveSnapshot(ctx); err != nil {
			t.Fatalf("SaveSnapshot after the scheduled rebuild: %v", err)
		}
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trust never recovered: no snapshot file after the scheduled rebuild:\n%s", buf.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
