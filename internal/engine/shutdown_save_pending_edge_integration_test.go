// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The two tests in this file are a pair, and neither means anything without
// the other. The shutdown save (SaveSnapshot, persist.go) folds whatever
// delta the View carries and writes the result; a live BloodHound always has
// a delta, so suppressing that save over a non-empty delta would mean no
// shutdown file ever again. What it may NOT do is write a file the fold has
// silently shortened: a fold drops a delta edge whose endpoint is in neither
// the base nor any segment, and the file's stamp covers that edge's write, so
// the next boot adopts it and serves a replica missing a row PostgreSQL holds
// -- persistently, across restarts, because every later save writes the same
// short file again.
//
// So: a non-empty delta with nothing pending must still be written
// (TestShutdownSaveWritesANonEmptyDeltaWithNothingPending), and only the
// presence of a pending edge may suppress it
// (TestShutdownSaveRefusesToWriteAFileTheFoldWouldShorten).

// TestShutdownSaveWritesANonEmptyDeltaWithNothingPending is the half that
// guards against over-refusing. The engine carries a real write-through
// delta -- asserted, not assumed, since a test whose View happened to have no
// segments would pass while proving nothing -- every edge in it resolves, and
// the file must be written with that write's rows in it.
func TestShutdownSaveWritesANonEmptyDeltaWithNothingPending(t *testing.T) {
	ctx := context.Background()
	fx := seedShutdownSaveEngine(t, ctx)

	nodeID := applyOneWrite(t, ctx, fx.eng)

	view, serving := fx.eng.Fresh()
	if !serving {
		t.Fatalf("engine not serving after the write applied")
	}
	if view.SegmentCount() == 0 {
		t.Fatalf("the View carries no delta segment, so this test does not exercise the fold the shutdown save runs")
	}
	if err := fx.eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot over a non-empty delta with nothing pending: %v", err)
	}

	if _, err := os.Stat(fx.path); err != nil {
		t.Fatalf("no snapshot file at %s after a shutdown save over an ordinary non-empty delta: %v\n%s", fx.path, err, fx.buf.String())
	}
	if logged := fx.buf.String(); !strings.Contains(logged, "bloodtrail: snapshot file written") {
		t.Fatalf("the shutdown save never reported writing a file:\n%s", logged)
	}

	// The delta's own rows are in the file, so the fold really did run and
	// really was folded in.
	snap, _, err := snapshot.ReadSnapshotFile(fx.path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	if _, ok := snapshot.NewView(snap).Dense(uint64(nodeID)); !ok {
		t.Fatalf("node %d was in the folded delta but is missing from the saved file", nodeID)
	}
}

// TestShutdownSaveRefusesToWriteAFileTheFoldWouldShorten is the other half.
// The delta holds an edge whose endpoint PostgreSQL has no node row for, so
// the shutdown fold drops it. Dropping is very probably right -- PostgreSQL
// matches no pattern through that edge either -- but the file cannot say so:
// its stamp is the converged counter, which already counts that edge's write,
// so a boot finds the file exactly current and adopts a graph the fold
// shortened. Where the endpoint is instead a row PostgreSQL does hold that an
// applied write's change set never named, that is a row missing from every
// future boot. The save has to refuse and let the next boot rebuild.
func TestShutdownSaveRefusesToWriteAFileTheFoldWouldShorten(t *testing.T) {
	ctx := context.Background()
	fx := seedCarriedEdgeEngine(t, ctx)
	eng := fx.eng

	// Nothing may rebuild underneath this test: an adoption would clear the
	// pending edge and the save would have nothing to refuse over.
	parkRebuildLoop(eng)

	// The premise: the save's own preconditions hold, so a refusal below can
	// only be about the fold.
	if _, _, converged := eng.saveSnapshotProbe(ctx); !converged {
		t.Fatalf("saveSnapshotProbe: converged = false; this test needs a save refused only for its pending edges")
	}

	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot returned an error, want nil (a refusal, not a failure): %v", err)
	}

	if _, err := os.Stat(fx.path); err == nil {
		// Spell out the harm rather than just "a file exists": the file is
		// short, and its stamp says it is not.
		snap, stamp, rerr := snapshot.ReadSnapshotFile(fx.path)
		if rerr != nil {
			t.Fatalf("a snapshot file was written although the fold dropped pending edges, and it cannot be read back: %v", rerr)
		}
		pgCounter, cerr := eng.ReadWatermark(ctx)
		if cerr != nil {
			t.Fatalf("ReadWatermark: %v", cerr)
		}
		var absent []uint64
		for _, id := range fx.edgeIDs {
			if _, ok := snap.EdgeByID(uint64(id)); !ok {
				absent = append(absent, uint64(id))
			}
		}
		t.Fatalf("a snapshot file was written although the fold dropped the pending edges: edges %v are absent from it, "+
			"its stamp is watermark %d against PostgreSQL's %d (so the next boot finds it exactly current and adopts it)\n%s",
			absent, stamp.Watermark, pgCounter, fx.buf.String())
	} else if !os.IsNotExist(err) {
		t.Fatalf("os.Stat(%s): %v", fx.path, err)
	}

	logged := fx.buf.String()
	if !strings.Contains(logged, "bloodtrail: snapshot file not written") {
		t.Fatalf("the refused shutdown save logged no \"snapshot file not written\" line:\n%s", logged)
	}
	if !strings.Contains(logged, reasonFoldWouldDropPendingEdges) {
		t.Fatalf("the refused shutdown save never named the condition it refused over:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file written") {
		t.Fatalf("logged \"snapshot file written\" although the save was refused:\n%s", logged)
	}

	// The replica itself is untouched by the refusal: it still serves
	// PostgreSQL's own answers, and the pending records stay in its delta.
	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine left serving; the comparison below would be vacuous")
	}
	if got := eng.snap.Load().SegmentCount(); got == 0 {
		t.Fatalf("the engine's delta emptied during the refused save; the pending records are gone")
	}
	const (
		edgeQuery = `MATCH (s:CarriedSaveNode)-[:CarriedSaveEdge]->(e:CarriedSaveNode) RETURN s, e`
		nodeQuery = `MATCH (n:CarriedSaveNode) RETURN n.name`
	)
	requireOracleRowTotal(t, fx.pgDriver, edgeQuery, 1)
	requireOracleRowTotal(t, fx.pgDriver, nodeQuery, fx.nodeCount)
	assertTypedCasesMatchOracle(t, fx.pgDriver, eng, []typedCase{
		{edgeQuery, true},
		{nodeQuery, true},
	})
}

// shutdownSaveFixture is a snapshot-file-enabled engine over a small graph,
// adopted and converged, with nothing pending.
type shutdownSaveFixture struct {
	eng  *Engine
	buf  *lockedBuffer
	path string
}

func seedShutdownSaveEngine(t *testing.T, ctx context.Context) shutdownSaveFixture {
	t.Helper()

	dsn := graphtest.PGAvailable(t)
	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{persistRaceKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	dir := t.TempDir()
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	t.Cleanup(func() { stopEngineAndCloseWritePool(eng) })
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)
	adoptOneRebuild(t, ctx, eng)

	return shutdownSaveFixture{eng: eng, buf: buf, path: snapshotFilePathFor(t, pgDriver, dir)}
}
