// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file pin the id-sequence half of a snapshot file's
// stamp (watermark.go's insertedSinceFile) against a live PostgreSQL: rows
// that a writer which never bumps the counter inserts after the file was
// saved must cost the file its adoption even when nothing ended the
// lineage, while BloodTrail's own writes at boot, which move the very same
// sequences, must not.

// TestFileBootRejectsAFileWithRowsInsertedBehindTheWatermark is the
// reported reproduction as stated: save a file, write through the plain
// dawgs pg driver, reopen BloodTrail. Nothing ends the lineage in between --
// no installer ran; the stock image was started by hand, or BloodHound's
// tool API switched to the plain driver -- so the counter and the lineage
// both read exactly what the file was stamped with, and only the id
// sequence the stock write drew from shows that PostgreSQL moved on.
func TestFileBootRejectsAFileWithRowsInsertedBehindTheWatermark(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	strayID := stockImageCreateNode(t, ctx, pgDriver, "plain-driver-write")

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.Start(ctx)
	defer eng.Stop()

	waitForFresh(t, eng)
	waitForBootMarker(t, buf, "bloodtrail: snapshot file rejected")

	logged := buf.String()
	if !strings.Contains(logged, reasonInsertedBehindCounter) {
		t.Fatalf("the boot did not refuse the file for the rows inserted behind the watermark:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file loaded") {
		t.Fatalf("the boot adopted a file missing a row PostgreSQL holds:\n%s", logged)
	}
	waitForRebuildCounted(t, eng, "after refusing the snapshot file")
	view, serving := eng.Fresh()
	if !serving {
		t.Fatalf("engine not serving after boot")
	}
	if _, ok := view.Dense(uint64(strayID)); !ok {
		t.Fatalf("node %d, inserted through the plain driver, is missing from the served graph", strayID)
	}
}

// TestFileBootAdoptsDespiteItsOwnInsertsAtBoot is the other side of the
// same check: BloodHound writes the graph within moments of starting, and
// those writes draw from the same id sequences the stock image's would. They
// bump the counter first, and Start records where PostgreSQL stood before
// any of them could run, so the file must still be adopted with them
// replayed onto it -- driven here in Start's own order (captureStartState,
// then the boot gap buffer, then writes) up to the file attempt.
func TestFileBootAdoptsDespiteItsOwnInsertsAtBoot(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.captureStartState(ctx)
	if eng.atStart.Load() == nil {
		t.Fatalf("captureStartState recorded nothing:\n%s", buf.String())
	}
	eng.bootGap.activate()
	bootNodeID := bootGapWrite(t, ctx, eng, "boot-write")

	if !eng.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile refused a file whose only newer rows are this boot's own counted writes:\n%s", buf.String())
	}
	logged := buf.String()
	if !strings.Contains(logged, "replayed_writes=1") {
		t.Fatalf("the loaded marker did not report the boot write replayed:\n%s", logged)
	}
	view, _ := eng.Fresh()
	for _, id := range []graph.ID{savedNodeID, bootNodeID} {
		if _, ok := view.Dense(uint64(id)); !ok {
			t.Fatalf("node %d missing from the adopted view:\n%s", id, logged)
		}
	}
}

// TestFileBootRefusesOnTheHeaderAlone pins that a file the lineage already
// rules out is refused before its body is read: every boot after an install
// or a rollback refuses its file, and reading gigabytes first to reach the
// same answer would only delay the rebuild that has to run anyway. The body
// is corrupted here, so a boot that read it would report the corruption
// rather than the lineage.
func TestFileBootRefusesOnTheHeaderAlone(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)
	endLineage(t, ctx, pool)
	corruptFile(t, snapshotFilePathFor(t, pgDriver, dir))

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.bootGap.activate()
	if eng.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile adopted a corrupt file from an ended lineage:\n%s", buf.String())
	}
	logged := buf.String()
	requireLineageRejection(t, logged)
	if strings.Contains(logged, "corrupt") {
		t.Fatalf("the body of a file the header already ruled out was read:\n%s", logged)
	}
}
