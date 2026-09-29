// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// persistRaceKind is the node kind the tests below write through -- a
// distinct StringKind from file_boot_integration_test.go's fileBootKind,
// purely so a failure's log output names which suite's write actually ran.
var persistRaceKind = graph.StringKind("PersistRaceNode")

// applyOneWrite performs exactly the bump-then-Apply sequence
// write_observer.go's ensureBumped/Apply pairing performs in production for
// one committed write (mirroring seedFileBootSnapshot's identical
// sequence, file_boot_integration_test.go): bump the pg watermark counter
// eagerly, create one node, then hand Apply the ChangeSet naming it. Used
// here to drive a real Apply call at an exact point in a test's own
// sequence -- the "drive the sequence manually" technique this package's
// own watermark_integration_test.go already uses for adoptRebuiltView's
// epoch check, applied to SaveSnapshot's epoch guard instead.
func applyOneWrite(t *testing.T, ctx context.Context, eng *Engine) graph.ID {
	t.Helper()

	counter, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("applyOneWrite: BumpWatermark: %v", err)
	}

	var nodeID graph.ID
	if err := eng.pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties(), persistRaceKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("applyOneWrite: create node: %v", err)
	}

	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordNodeID(nodeID)
	eng.Apply(ctx, scope)

	return nodeID
}

// TestSaveSnapshotRefusesWhenApplyRacesTheProbe is C1's regression: an Apply
// call that lands strictly between SaveSnapshot's own watermark probe and
// its locked, epoch-verified commit step must make the save refuse outright
// -- no file written, nothing silently stamped with a counter that has
// already moved past the View it describes.
//
// The interleaving is driven manually, exactly the way
// TestAdoptionRefusedByEpochResolvesNothing (watermark_test.go) drives
// adoptRebuiltView's own identical epoch check without needing real
// goroutines or timing: saveSnapshotProbe and saveSnapshotCommit are called
// directly (both same-package, unexported), with a full, real Apply run in
// between. This is a faithful reproduction of the race, not merely an
// analogous one -- Apply holds applyMu for its entire body (apply.go), so
// from saveSnapshotCommit's perspective a single sequential call to
// applyOneWrite here is indistinguishable from a concurrent Apply that
// happened to hold the lock for the same duration: either way,
// saveSnapshotCommit's own Lock() call blocks until it is done, and by then
// applyEpoch has already moved past the value saveSnapshotProbe captured.
//
// Before this fix, SaveSnapshot sampled the View and the watermark
// preconditions as two separate, unlocked reads with no epoch check at
// all -- so this exact interleaving would have folded the OLD View (missing
// applyOneWrite's node) and stamped it with the NEW, already-advanced
// counter, producing a file a later boot's exact-match check would have
// wrongly trusted as complete.
func TestSaveSnapshotRefusesWhenApplyRacesTheProbe(t *testing.T) {
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
	parkRebuildLoop(eng)
	defer eng.Stop()

	adoptOneRebuild(t, ctx, eng)

	path := snapshotFilePathFor(t, pgDriver, dir)

	// Capture SaveSnapshot's own probe -- epoch, then the pg watermark round
	// trip -- exactly as SaveSnapshot itself does (persist.go).
	epoch, stamp, converged := eng.saveSnapshotProbe(ctx)
	if !converged {
		t.Fatalf("saveSnapshotProbe: converged = false before the racing write, want true (the test needs a real converged sample to prove the epoch guard, not convergence, is what refuses the save)")
	}

	// The race: a write's Apply call lands entirely between the probe above
	// and the commit below.
	racedNodeID := applyOneWrite(t, ctx, eng)

	if err := eng.saveSnapshotCommit(ctx, path, epoch, stamp, converged, false); err != nil {
		t.Fatalf("saveSnapshotCommit after a racing Apply returned an error, want nil (a refusal, not a failure): %v", err)
	}

	if _, err := os.Stat(path); err == nil {
		t.Fatalf("snapshot file %s exists after a refused save, want no file written", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("os.Stat(%s): %v", path, err)
	}

	logged := buf.String()
	if !strings.Contains(logged, "bloodtrail: snapshot file not written") {
		t.Fatalf("did not log \"snapshot file not written\" for a save refused by the epoch guard:\n%s", logged)
	}
	if !strings.Contains(logged, "an Apply call landed while probing the watermark") {
		t.Fatalf("did not log the epoch-guard's own reason:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file written") {
		t.Fatalf("logged \"snapshot file written\" despite the epoch guard refusing the save:\n%s", logged)
	}

	// Ground truth: the raced write genuinely landed, and this engine's own
	// (unrelated to the refused save) View reflects it -- the refusal above
	// was about the STALE probe, not about anything actually being wrong
	// with the engine.
	view, serving := eng.Fresh()
	if !serving {
		t.Fatalf("engine not serving after the racing write, want serving")
	}
	if _, ok := view.Dense(uint64(racedNodeID)); !ok {
		t.Fatalf("the raced write's own node is missing from the engine's current View -- Apply itself did not do its job")
	}
}

// TestSaveSnapshotHappyPathStillWrites is C1's paired non-regression case:
// with no racing Apply at all, an ordinary SaveSnapshot call must still
// succeed exactly as it always has -- the epoch guard added above must add
// zero cost or behavior change to the case where nothing raced it.
func TestSaveSnapshotHappyPathStillWrites(t *testing.T) {
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
	parkRebuildLoop(eng)
	defer eng.Stop()

	adoptOneRebuild(t, ctx, eng)
	nodeID := applyOneWrite(t, ctx, eng)

	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot (happy path): %v", err)
	}

	path := snapshotFilePathFor(t, pgDriver, dir)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file %s missing after a happy-path save: %v", path, err)
	}

	logged := buf.String()
	if !strings.Contains(logged, "bloodtrail: snapshot file written") {
		t.Fatalf("did not log \"snapshot file written\" for a happy-path save:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file not written") {
		t.Fatalf("logged \"snapshot file not written\" for a happy-path save:\n%s", logged)
	}

	// Reload straight from the file (bypassing tryLoadSnapshotFile's own
	// watermark gate, which is exercised end to end by
	// file_boot_integration_test.go) and confirm the write this test applied
	// is actually in it.
	snap, stamp, err := snapshot.ReadSnapshotFile(path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	pgCounter, err := eng.ReadWatermark(ctx)
	if err != nil {
		t.Fatalf("ReadWatermark: %v", err)
	}
	if stamp.Watermark != pgCounter {
		t.Fatalf("file watermark = %d, want %d (the current, converged pg counter)", stamp.Watermark, pgCounter)
	}
	nodeSeq, edgeSeq, err := eng.readSequencePositions(ctx)
	if err != nil {
		t.Fatalf("readSequencePositions: %v", err)
	}
	if stamp.NodeIDSeq != nodeSeq || stamp.EdgeIDSeq != edgeSeq {
		t.Fatalf("file id sequence positions = (%d, %d), want (%d, %d) (where PostgreSQL's stood when the file was saved)",
			stamp.NodeIDSeq, stamp.EdgeIDSeq, nodeSeq, edgeSeq)
	}
	if nodeSeq < int64(nodeID) {
		t.Fatalf("node id sequence at %d, behind node %d this test created: the position read is not the sequence's", nodeSeq, nodeID)
	}
	view := snapshot.NewView(snap)
	if _, ok := view.Dense(uint64(nodeID)); !ok {
		t.Fatalf("the written-through node is missing from the saved file's own snapshot")
	}
}

// TestSaveSnapshotRemovesAFileWrittenAcrossAFailedBump is a save racing a
// write whose watermark bump fails. The failure removes the snapshot file
// (NoteWatermarkBumpFailure) because its write reaches PostgreSQL without
// moving the counter -- but a save that had already decided to write lands
// its own file after that removal, stamped with the counter the write never
// moved. After a hard stop the next boot finds that file exactly current by
// every check it has and serves a graph without the write. The write here
// is an update, which leaves the id sequences where they were, so the
// file's stamp cannot give it away either: the save itself has to take the
// file back.
//
// The interleaving is driven the way TestSaveSnapshotRefusesWhenApplyRacesTheProbe
// drives its own: saveSnapshotPrepare and saveSnapshotWrite are called
// directly, with the failure and the write in between.
func TestSaveSnapshotRemovesAFileWrittenAcrossAFailedBump(t *testing.T) {
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
	parkRebuildLoop(eng)
	defer eng.Stop()

	adoptOneRebuild(t, ctx, eng)
	nodeID := applyOneWrite(t, ctx, eng)
	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	path := snapshotFilePathFor(t, pgDriver, dir)

	epoch, stamp, converged := eng.saveSnapshotProbe(ctx)
	if !converged {
		t.Fatalf("saveSnapshotProbe: converged = false, want true (the save has to get as far as writing for the race to exist)")
	}
	pending, err := eng.saveSnapshotPrepare(ctx, epoch, converged, false)
	if err != nil || pending == nil {
		t.Fatalf("saveSnapshotPrepare = (%v, %v), want a snapshot to write:\n%s", pending, err, buf.String())
	}

	// The race: a write's bump fails, its removal takes the earlier file
	// away, and the write commits uncounted -- all before the save's file
	// lands.
	eng.NoteWatermarkBumpFailure(ctx, NewWriteScope(), errors.New("simulated bump failure"))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the failed bump left the earlier file in place (stat: %v)", err)
	}
	stockImageSetName(t, ctx, pgDriver, nodeID, "written-uncounted")

	if err := eng.saveSnapshotWrite(ctx, path, pending, stamp, time.Now()); err != nil {
		t.Fatalf("saveSnapshotWrite: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("a file written across a failed bump was left in place (stat: %v):\n%s", err, buf.String())
	}
	logged := buf.String()
	if !strings.Contains(logged, "a watermark bump failed while the file was being written") {
		t.Fatalf("the save did not say why it took its file back:\n%s", logged)
	}

	// What the file would have cost: the next process serves the write.
	engB, bufB := newLogCapturingEngine(pgDriver, pool, dir)
	engB.Start(ctx)
	defer engB.Stop()
	waitForFresh(t, engB)
	if strings.Contains(bufB.String(), "bloodtrail: snapshot file loaded") {
		t.Fatalf("the next boot adopted a snapshot file:\n%s", bufB.String())
	}
	view, _ := engB.Fresh()
	dense, ok := view.Dense(uint64(nodeID))
	if !ok {
		t.Fatalf("node %d is missing from the served graph", nodeID)
	}
	if name, _ := view.PropValueByName(dense, "name"); name != "written-uncounted" {
		t.Fatalf("node %d serves name %v, want the uncounted write's", nodeID, name)
	}
}
