// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file cover the delta edge whose endpoint NEVER arrives.
//
// A delta edge whose endpoint the replica has not applied yet is carried, not
// dropped, through every compaction fold (snapshot.FoldWithPendingEdges,
// adoptCompaction) -- the endpoint may still be on its way, and the edge has
// to be there for it to meet. An edge whose endpoint never arrives is carried
// by every compaction all the same, so the adopted View always has a segment
// and the post-compaction save, which requires an empty delta, is skipped
// every time. Nothing used to clear that: no snapshot file was written again
// for the rest of the process's life, and every restart rebuilt the whole
// replica from PostgreSQL.
//
// PostgreSQL permits exactly this shape: its edge table names its endpoints
// by id with no foreign key to node (dawgs v0.8.0's schema_up.sql -- the
// cascade on node delete is a statement trigger, not a constraint), so an
// edge whose endpoint row does not exist is an ordinary row PostgreSQL
// accepts, matches no pattern through, and never heals.

var (
	danglingSaveNodeKind = graph.StringKind("CarriedSaveNode")
	danglingSaveEdgeKind = graph.StringKind("CarriedSaveEdge")
)

// carriedSaveCompactEntries is the compaction entry threshold the engines
// below are built with, so that the two carried edges alone exceed it: that
// is the 65,536-carried-edge shape at production scale, where the carry by
// itself re-trips the trigger on every single Apply.
const carriedSaveCompactEntries = 1

// carriedEdgeFixture is what seedCarriedEdgeEngine hands its callers.
type carriedEdgeFixture struct {
	eng       *Engine
	buf       *lockedBuffer
	pgDriver  *pg.Driver
	pool      *pgxpool.Pool
	dir       string
	path      string
	nodeCount int
	// missing holds the endpoint ids the dangling edges name: ids no node row
	// has, and far enough above the node id sequence that nothing can ever
	// allocate them.
	missing []graph.ID
	// edgeIDs holds the dangling edges' own database ids.
	edgeIDs []graph.ID
}

// seedCarriedEdgeEngine builds the permanent case end to end: a wiped graph
// with two nodes and one ordinary edge between them, adopted by a
// snapshot-file-enabled engine, and then one write that commits two edges
// whose end_id names no node row at all, applied. The engine is left
// serving and converged, with those edges sitting in its delta as records it
// cannot show.
func seedCarriedEdgeEngine(t *testing.T, ctx context.Context) carriedEdgeFixture {
	t.Helper()

	dsn := graphtest.PGAvailable(t)
	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{danglingSaveNodeKind, danglingSaveEdgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	var a, b *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		if a, err = tx.CreateNode(graph.NewProperties().Set("name", "a"), danglingSaveNodeKind); err != nil {
			return err
		}
		if b, err = tx.CreateNode(graph.NewProperties().Set("name", "b"), danglingSaveNodeKind); err != nil {
			return err
		}
		_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, danglingSaveEdgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	dir := t.TempDir()
	buf := &lockedBuffer{}
	eng := New(pgDriver, pool, Config{
		Enabled:        true,
		SnapshotDir:    dir,
		CompactEntries: carriedSaveCompactEntries,
		Log:            slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	t.Cleanup(func() { stopEngineAndCloseWritePool(eng) })
	resetWatermarkTable(t, ctx, eng)
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// Ids the node sequence has not reached and nothing here will ever push
	// it to: whatever happens next, no node row will carry them, so the edges
	// below can never resolve.
	nodeSeq, _, err := eng.readSequencePositions(ctx)
	if err != nil {
		t.Fatalf("readSequencePositions: %v", err)
	}
	missing := []graph.ID{graph.ID(uint64(nodeSeq) + 1_000_000), graph.ID(uint64(nodeSeq) + 2_000_000)}

	counter, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}
	var dangling []graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, end := range missing {
			rel, err := tx.CreateRelationshipByIDs(a.ID, end, danglingSaveEdgeKind, graph.NewProperties())
			if err != nil {
				return err
			}
			dangling = append(dangling, rel.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("create the dangling edges: %v", err)
	}
	scope := NewWriteScope()
	scope.SetWatermark(counter)
	for _, id := range dangling {
		scope.Changes().RecordEdgeID(id)
	}
	// Two carried entries already exceed cfg.CompactEntries here, so this
	// Apply's own tail would spawn a background compaction of its own and
	// race every fold the tests below drive explicitly. Claiming the trigger
	// gate is how maybeStartCompaction is told one is already running.
	eng.compacting.Store(true)
	eng.Apply(ctx, scope)
	eng.compacting.Store(false)

	view, serving := eng.Fresh()
	if !serving {
		t.Fatalf("engine not serving after the dangling edges' write applied")
	}
	// The premise, asserted rather than assumed: the records are in the delta,
	// and the View cannot show them.
	if view.SegmentCount() == 0 {
		t.Fatalf("no delta segment after the write applied; the edge records are not where this test needs them")
	}
	for i, end := range missing {
		if _, known := view.Dense(uint64(end)); known {
			t.Fatalf("endpoint %d is in the replica; edge %d is not dangling", end, dangling[i])
		}
		if _, _, _, visible := view.EdgeStateByID(uint64(dangling[i])); visible {
			t.Fatalf("edge %d is visible although its endpoint %d has no node row at all", dangling[i], end)
		}
	}

	return carriedEdgeFixture{
		eng:       eng,
		buf:       buf,
		pgDriver:  pgDriver,
		pool:      pool,
		dir:       dir,
		path:      snapshotFilePathFor(t, pgDriver, dir),
		nodeCount: 2,
		missing:   missing,
		edgeIDs:   dangling,
	}
}

// waitForEmptyDelta polls until the engine's published View carries no delta
// segment at all, which is what an adopted rebuild publishes.
func waitForEmptyDelta(t *testing.T, eng *Engine, why string) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if view, serving := eng.Fresh(); serving && view.SegmentCount() == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the engine's delta never emptied within 30s: %s (segments = %d, rebuild attempts = %d)",
				why, eng.snap.Load().SegmentCount(), eng.RebuildCount())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestPermanentlyPendingDeltaEdgesAreStillCarriedByEveryFold pins what the
// fix does NOT change: a fold still carries an unresolvable delta edge
// forward rather than dropping it, every time, and its save is still skipped
// rather than writing a file without it. The rebuild loop is parked here, so
// nothing can clear the carry and the repetition is the only thing on show.
func TestPermanentlyPendingDeltaEdgesAreStillCarriedByEveryFold(t *testing.T) {
	ctx := context.Background()
	fx := seedCarriedEdgeEngine(t, ctx)
	eng := fx.eng

	// Nothing may rebuild: the carry has to survive both folds below for
	// this test to be about the carry at all.
	parkRebuildLoop(eng)

	for i := 1; i <= 2; i++ {
		view := eng.snap.Load()
		eng.runCompaction(view.Base(), view.Segments())
		if got := eng.CompactionCount(); got != uint64(i) {
			t.Fatalf("CompactionCount() = %d after compaction %d, want %d", got, i, i)
		}
		if got := eng.snap.Load().SegmentCount(); got != 1 {
			t.Fatalf("SegmentCount() = %d after compaction %d, want 1 (the pending edges carried forward again)", got, i)
		}
		for j, id := range fx.edgeIDs {
			if _, _, _, visible := eng.snap.Load().EdgeStateByID(uint64(id)); visible {
				t.Fatalf("edge %d became visible after compaction %d although its endpoint %d has no node row", id, i, fx.missing[j])
			}
		}
		requireNoSnapshotFile(t, fx.path, fx.buf.String(), "after a compaction that carried permanently pending edges")
	}

	logged := fx.buf.String()
	if got := strings.Count(logged, "carried_edges=2"); got != 2 {
		t.Fatalf("both compactions should have reported carrying 2 edges (saw %d such lines):\n%s", got, logged)
	}
	if got := strings.Count(logged, reasonUnresolvableDelta); got != 2 {
		t.Fatalf("both skipped saves should have named the delta that can never empty (saw %d such lines):\n%s", got, logged)
	}
}

// TestPermanentlyPendingDeltaEdgeStopsBlockingSnapshotSaves is the defect:
// edges whose endpoints never arrive are re-carried by every compaction, so
// the post-compaction save -- which requires an empty delta -- is skipped
// every time, and no snapshot file is ever written again. The engine has to
// notice that the delta it carries can never empty on its own and ask
// PostgreSQL for a fresh base, which is the only thing that can tell a
// never-arriving endpoint from a late one. After that rebuild the delta is
// empty, saves resume, and what gets written is adoptable by the next boot.
func TestPermanentlyPendingDeltaEdgeStopsBlockingSnapshotSaves(t *testing.T) {
	ctx := context.Background()
	fx := seedCarriedEdgeEngine(t, ctx)
	eng := fx.eng

	// The save's own preconditions hold: it is not convergence or the trust
	// generations refusing this save, it is the carried delta.
	if _, _, converged := eng.saveSnapshotProbe(ctx); !converged {
		t.Fatalf("saveSnapshotProbe: converged = false; this test needs a save refused only for its delta")
	}

	// One compaction, exactly as the background compactor runs it: it carries
	// both edges and its save writes nothing.
	view := eng.snap.Load()
	eng.runCompaction(view.Base(), view.Segments())
	if got := eng.CompactionCount(); got != 1 {
		t.Fatalf("CompactionCount() = %d, want 1", got)
	}
	if logged := fx.buf.String(); !strings.Contains(logged, "carried_edges=2") {
		t.Fatalf("the compaction never reported carrying the pending edges:\n%s", logged)
	}
	requireNoSnapshotFile(t, fx.path, fx.buf.String(), "after a compaction that carried permanently pending edges")

	// The fix: a save that can never succeed on its own asks for the rebuild
	// that reconciles the replica with PostgreSQL. Without it nothing ever
	// clears the delta and no file is written again.
	waitForEmptyDelta(t, eng,
		"a post-compaction save skipped over a delta that can never empty must ask for a rebuild")

	if logged := fx.buf.String(); !strings.Contains(logged, reasonUnresolvableDelta) {
		t.Fatalf("the skipped save never named the delta that can never empty:\n%s", logged)
	}

	// PostgreSQL's base does not carry the dangling edges at all -- they have
	// no endpoint to attach to -- so the rebuilt replica has nothing pending.
	rebuilt, serving := eng.Fresh()
	if !serving {
		t.Fatalf("engine not serving after the rebuild")
	}
	for i, id := range fx.edgeIDs {
		if _, ok := rebuilt.Base().EdgeByID(uint64(id)); ok {
			t.Fatalf("edge %d is in the rebuilt base although its endpoint %d has no node row", id, fx.missing[i])
		}
	}

	// Saves resume: the next compaction's save writes a file.
	view = eng.snap.Load()
	eng.runCompaction(view.Base(), view.Segments())
	if _, err := os.Stat(fx.path); err != nil {
		t.Fatalf("snapshot file %s still missing after the rebuild cleared the delta: %v", fx.path, err)
	}
	if logged := fx.buf.String(); !strings.Contains(logged, "bloodtrail: snapshot file written") {
		t.Fatalf("no save ever reported writing a file:\n%s", logged)
	}

	// ...and what was written is adoptable: a fresh engine over the same
	// directory and pool boots from the file with no PostgreSQL rebuild, and
	// serves PostgreSQL's own answers.
	engB, bufB := newLogCapturingEngine(fx.pgDriver, fx.pool, fx.dir)
	engB.Start(ctx)
	defer stopEngineAndCloseWritePool(engB)

	waitForFresh(t, engB)
	waitForBootMarker(t, bufB, "bloodtrail: snapshot file loaded")
	if got := engB.RebuildCount(); got != 0 {
		t.Fatalf("RebuildCount = %d for the engine that booted from the saved file, want 0:\n%s", got, bufB.String())
	}
	if _, serving := engB.serveState(); !serving {
		t.Fatalf("engine B left serving; the comparisons below would be vacuous")
	}

	const (
		edgeQuery = `MATCH (s:CarriedSaveNode)-[:CarriedSaveEdge]->(e:CarriedSaveNode) RETURN s, e`
		nodeQuery = `MATCH (n:CarriedSaveNode) RETURN n.name`
	)
	// PostgreSQL's own totals first, so neither comparison can pass by both
	// sides being empty: the one real edge survives, the dangling ones match
	// no pattern because they have no endpoint row, and both nodes are there.
	requireOracleRowTotal(t, fx.pgDriver, edgeQuery, 1)
	requireOracleRowTotal(t, fx.pgDriver, nodeQuery, fx.nodeCount)
	assertTypedCasesMatchOracle(t, fx.pgDriver, engB, []typedCase{
		{edgeQuery, true},
		{nodeQuery, true},
	})
}

// TestPermanentlyPendingDeltaEdgesStopRetriggeringTheFold is the second half
// of the cost: the carried edges are themselves delta entries, so once there
// are more of them than cfg.CompactEntries every single Apply trips the
// compaction trigger again and runs another full fold over the whole base.
// The trigger has to stop firing, and the only thing that stops it is the
// delta actually emptying.
//
// The assertion is on the trigger's own decision
// (compactionThresholdExceeded over the published delta), not on a timing:
// spawning the real background compaction would prove nothing more and would
// race this test's own folds.
func TestPermanentlyPendingDeltaEdgesStopRetriggeringTheFold(t *testing.T) {
	ctx := context.Background()
	fx := seedCarriedEdgeEngine(t, ctx)
	eng := fx.eng

	tripped := func(view *snapshot.View) bool {
		return compactionThresholdExceeded(eng.cfg.CompactEntries, 0, deltaEntries(view.Segments()), 0)
	}

	// Fold and adopt without the save that follows it (runCompaction's own
	// two halves, compact.go), so the carried View below is observed with
	// nothing yet able to have cleared it.
	view := eng.snap.Load()
	if _, _, adopted := eng.foldAndAdoptCompaction(view.Base(), view.Segments(), time.Now()); !adopted {
		t.Fatalf("the fold was not adopted; there is no carried delta to observe")
	}

	// Before: the carried delta alone trips the trigger, so the next write
	// folds the whole base again -- and so would every write after it.
	carried := eng.snap.Load()
	if !tripped(carried) {
		t.Fatalf("the carried delta (%d entries) does not trip cfg.CompactEntries = %d, so this test is not about anything",
			deltaEntries(carried.Segments()), eng.cfg.CompactEntries)
	}

	// The save is what notices, so run the whole compaction now.
	view = eng.snap.Load()
	eng.runCompaction(view.Base(), view.Segments())
	waitForEmptyDelta(t, eng, "the carried delta must be cleared, or the fold keeps re-triggering forever")

	// After: nothing is carried, so there is no delta to trip the trigger
	// with at all.
	if cleared := eng.snap.Load(); tripped(cleared) {
		t.Fatalf("the trigger still fires over a delta of %d entries after the carry should have been cleared",
			deltaEntries(cleared.Segments()))
	}
}

// TestPendingDeltaEdgeWhoseEndpointArrivesLaterIsStillCarried is the
// regression guard: the deliberate carry must survive. W1 commits a node and
// does NOT apply; W2 commits an edge into it and applies, so the delta holds
// an edge the replica cannot show. A compaction in that window must carry the
// edge -- and nothing may conclude that its endpoint is never coming, because
// W1's own watermark bump is still in flight and the save's convergence
// precondition says so. W1 then applies, the edge resolves, and the next
// compaction folds it in and saves.
func TestPendingDeltaEdgeWhoseEndpointArrivesLaterIsStillCarried(t *testing.T) {
	ctx := context.Background()
	dsn := graphtest.PGAvailable(t)

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{danglingSaveNodeKind, danglingSaveEdgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	var anchor *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		anchor, err = tx.CreateNode(graph.NewProperties().Set("name", "anchor"), danglingSaveNodeKind)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	dir := t.TempDir()
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	t.Cleanup(func() { stopEngineAndCloseWritePool(eng) })
	resetWatermarkTable(t, ctx, eng)
	// Nothing in this test may rebuild: every adoption here would clear the
	// carry this test exists to watch survive.
	parkRebuildLoop(eng)
	adoptOneRebuild(t, ctx, eng)
	path := snapshotFilePathFor(t, pgDriver, dir)

	// W1: the endpoint's write commits, bumps, and does not apply.
	counterLate, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("W1 BumpWatermark: %v", err)
	}
	var late *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		late, err = tx.CreateNode(graph.NewProperties().Set("name", "late"), danglingSaveNodeKind)
		return err
	}); err != nil {
		t.Fatalf("W1 create: %v", err)
	}
	lateScope := NewWriteScope()
	lateScope.SetWatermark(counterLate)
	lateScope.Changes().RecordNodeID(late.ID)

	// W2: the edge into W1's node commits and applies first.
	counterEdge, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("W2 BumpWatermark: %v", err)
	}
	var rel *graph.Relationship
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		rel, err = tx.CreateRelationshipByIDs(anchor.ID, late.ID, danglingSaveEdgeKind, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("W2 create: %v", err)
	}
	edgeScope := NewWriteScope()
	edgeScope.SetWatermark(counterEdge)
	edgeScope.Changes().RecordEdgeID(rel.ID)
	eng.Apply(ctx, edgeScope)

	if _, _, _, visible := eng.snap.Load().EdgeStateByID(uint64(rel.ID)); visible {
		t.Fatalf("edge %d is visible before its endpoint's write applied; there is no pending edge here", rel.ID)
	}

	rebuildsBefore := eng.RebuildCount()
	trustBefore := eng.lastTrustRebuildNano.Load()

	// The compaction carries it, and the save is skipped -- but nothing may
	// conclude the endpoint is never coming: W1's bump is still in flight.
	view := eng.snap.Load()
	eng.runCompaction(view.Base(), view.Segments())
	if _, _, _, visible := eng.snap.Load().EdgeStateByID(uint64(rel.ID)); visible {
		t.Fatalf("edge %d became visible although its endpoint still has not applied", rel.ID)
	}
	requireNoSnapshotFile(t, path, buf.String(), "while a write's own bump is still in flight")
	if got := eng.lastTrustRebuildNano.Load(); got != trustBefore {
		t.Fatalf("a rebuild was requested for a delta edge whose endpoint is still on its way: the deliberate carry was mistaken for a permanent one")
	}
	if got := eng.RebuildCount(); got != rebuildsBefore {
		t.Fatalf("RebuildCount moved from %d to %d while a write was still in flight", rebuildsBefore, got)
	}

	// W1 applies: the endpoint lands on top of the compacted base and the
	// carried edge resolves, exactly as the carry exists to allow.
	eng.Apply(ctx, lateScope)
	if _, _, _, visible := eng.snap.Load().EdgeStateByID(uint64(rel.ID)); !visible {
		t.Fatalf("edge %d did not resolve once its endpoint arrived: the compaction dropped the carried record", rel.ID)
	}

	// The next compaction has nothing left pending, folds it into the base,
	// and its save writes a file.
	view = eng.snap.Load()
	eng.runCompaction(view.Base(), view.Segments())
	if got := eng.snap.Load().SegmentCount(); got != 0 {
		t.Fatalf("SegmentCount() = %d after the second compaction, want 0", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot file %s missing after a compaction with nothing pending: %v", path, err)
	}
	if logged := buf.String(); !strings.Contains(logged, "bloodtrail: snapshot file written") {
		t.Fatalf("the save after the endpoint arrived never wrote a file:\n%s", logged)
	}

	if _, serving := eng.serveState(); !serving {
		t.Fatalf("engine left serving; the comparison below would be vacuous")
	}
	const edgeQuery = `MATCH (s:CarriedSaveNode)-[:CarriedSaveEdge]->(e:CarriedSaveNode) RETURN s, e`
	requireOracleRowTotal(t, pgDriver, edgeQuery, 1)
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{{edgeQuery, true}})
}
