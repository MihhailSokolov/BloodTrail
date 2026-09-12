// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file pin adoptSnapshotFileView's replay path against a
// live PostgreSQL: a snapshot file plus the boot gap buffer's writes must
// adopt -- with the buffered writes visible in the served view and zero pg
// rebuilds -- and every way the gap proof can fail must reject. Each test
// drives tryLoadSnapshotFile directly rather than through Start, the same
// deterministic choice watermark_integration_test.go makes: the boot-load
// goroutine's own retry/ordering behavior is file_boot_integration_test.go's
// business, and driving the attempt synchronously is what lets a test place
// a write EXACTLY inside the load window instead of racing for it.

// bootGapWrite commits one real node write through pgDriver and hands its
// scope to eng.Apply, mirroring write_observer.go's ensureBumped/Apply
// pairing exactly as seedFileBootSnapshot does (its own doc explains why
// the wiring is rebuilt from engine-level primitives here). With no View
// adopted yet, Apply's no-snapshot branch drops the delta -- and the armed
// boot gap buffer is what remembers it. Returns the created node's id.
func bootGapWrite(t *testing.T, ctx context.Context, eng *Engine, objectID string) graph.ID {
	t.Helper()

	counter, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("bootGapWrite: BumpWatermark: %v", err)
	}

	var nodeID graph.ID
	if err := eng.pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("objectid", objectID), fileBootKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("bootGapWrite: create node: %v", err)
	}

	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordNodeID(nodeID)
	eng.Apply(ctx, scope)

	return nodeID
}

// TestFileBootReplaysBootWritesAndAdopts is the reason the boot gap buffer
// exists: writes landing while the snapshot file loads -- BloodHound's own
// startup analysis does this on every boot -- must no longer cost the file
// its adoption. Two real writes land after the file was saved (one while
// "loading": before the attempt runs), and the boot must still adopt the
// file with zero pg rebuilds, serve BOTH the saved node and the boot-time
// nodes, and say so in the "snapshot file loaded" marker's replayed_writes.
func TestFileBootReplaysBootWritesAndAdopts(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate() // what Start does when SnapshotDir is set

	bootNode1 := bootGapWrite(t, ctx, engB, "boot-write-1")
	bootNode2 := bootGapWrite(t, ctx, engB, "boot-write-2")

	if !engB.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile did not adopt despite a fully covered boot gap:\n%s", buf.String())
	}

	if got := engB.RebuildCount(); got != 0 {
		t.Fatalf("RebuildCount = %d after a replayed file boot, want 0 (no pg rebuild should ever have run)", got)
	}

	view, serving := engB.Fresh()
	if !serving {
		t.Fatalf("engine B not serving after a replayed file boot")
	}
	for _, id := range []graph.ID{savedNodeID, bootNode1, bootNode2} {
		if _, ok := view.Dense(uint64(id)); !ok {
			t.Fatalf("node %d missing from the adopted view (saved %d, boot writes %d/%d):\n%s",
				id, savedNodeID, bootNode1, bootNode2, buf.String())
		}
	}

	logged := buf.String()
	if !strings.Contains(logged, "bloodtrail: snapshot file loaded") {
		t.Fatalf("boot did not log \"snapshot file loaded\":\n%s", logged)
	}
	if !strings.Contains(logged, "replayed_writes=2") {
		t.Fatalf("the loaded marker did not report replayed_writes=2:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file rejected") {
		t.Fatalf("boot logged a rejection despite adopting:\n%s", logged)
	}
}

// TestFileBootReplaysDeletionOntoTheFile pins the tombstone half of the
// replay: a boot-time write that DELETES a node the file still contains
// must leave that node absent from the adopted view -- read-back reporting
// the key absent is what turns the replayed ChangeSet into a tombstone, so
// a bug here would resurrect deleted rows on every file boot.
func TestFileBootReplaysDeletionOntoTheFile(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate()

	// Delete the saved node, with the same bump-then-write-then-Apply
	// ordering production performs. The delete itself goes straight to SQL:
	// which driver call performed it is irrelevant here, since the scope is
	// hand-built either way, and the replay only ever consults the
	// ChangeSet plus PostgreSQL's own post-commit truth.
	counter, err := engB.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}
	if _, err := pool.Exec(ctx, "delete from edge where start_id = $1 or end_id = $1", uint64(savedNodeID)); err != nil {
		t.Fatalf("delete node's edges: %v", err)
	}
	if _, err := pool.Exec(ctx, "delete from node where id = $1", uint64(savedNodeID)); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordNodeID(savedNodeID)
	engB.Apply(ctx, scope)

	if !engB.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile did not adopt despite a fully covered boot gap:\n%s", buf.String())
	}

	view, serving := engB.Fresh()
	if !serving {
		t.Fatalf("engine B not serving after a replayed file boot")
	}
	if dense, ok := view.Dense(uint64(savedNodeID)); ok && view.Alive(dense) {
		t.Fatalf("node %d was deleted during boot but is alive in the adopted view -- the file resurrected it", savedNodeID)
	}
}

// TestFileBootReplaysAbandonedWriteCounter pins the counter-only entry: a
// write whose eager bump committed but which then failed (nothing landed
// in PostgreSQL) advances the counter without ever reaching Apply --
// ResolveAbandonedWrite is what accounts for it, and without that the gap
// would have a hole and the file would be rejected for a write that
// changed nothing.
func TestFileBootReplaysAbandonedWriteCounter(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate()

	// A bumped write that produced no committed effect: exactly what
	// driver.go's WriteTransaction error branch hands to
	// resolveAbandonedWrite.
	counter, err := engB.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}
	abandoned := NewWriteScope()
	abandoned.SetWatermark(counter)
	engB.ResolveAbandonedWrite(ctx, abandoned)

	// And one ordinary boot write on top, so the gap holds both shapes.
	bootNode := bootGapWrite(t, ctx, engB, "boot-write-after-abandoned")

	if !engB.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile did not adopt despite the abandoned write's counter being accounted:\n%s", buf.String())
	}

	view, serving := engB.Fresh()
	if !serving {
		t.Fatalf("engine B not serving after a replayed file boot")
	}
	for _, id := range []graph.ID{savedNodeID, bootNode} {
		if _, ok := view.Dense(uint64(id)); !ok {
			t.Fatalf("node %d missing from the adopted view:\n%s", id, buf.String())
		}
	}
	if !strings.Contains(buf.String(), "replayed_writes=1") {
		t.Fatalf("the loaded marker should report exactly the one replayable write (the abandoned one is counter-only):\n%s", buf.String())
	}
}

// TestFileBootRejectsUncoveredGap pins the conservative half: a counter
// advance nothing in this process will EVER account for (an out-of-band
// bump here; in production a previous process's crash-window write or
// another writer) must still reject the file with the gap-specific reason,
// and the engine must still come up correctly via the pg rebuild. Since
// the settle-wait, "still" means after waiting out bootGapSettleTimeout --
// an in-flight write's Apply could yet fill such a hole, and only the
// deadline distinguishes "in flight" from "never coming" -- so the test
// shortens the window rather than stalling the suite for the real 5s, and
// asserts the rejection line reports how long it waited.
func TestFileBootRejectsUncoveredGap(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	oldTimeout := bootGapSettleTimeout
	bootGapSettleTimeout = 250 * time.Millisecond
	t.Cleanup(func() { bootGapSettleTimeout = oldTimeout })

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	if _, err := pool.Exec(ctx, "update bloodtrail_watermark set counter = counter + 1"); err != nil {
		t.Fatalf("bump watermark out of band: %v", err)
	}

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate()

	start := time.Now()
	if engB.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile adopted a file whose watermark gap nothing covered")
	}
	if waited := time.Since(start); waited < bootGapSettleTimeout {
		t.Fatalf("rejection came after %v, want at least the %v settle window (the wait must precede the give-up)", waited, bootGapSettleTimeout)
	}

	logged := buf.String()
	if !strings.Contains(logged, "boot gap not covered by buffered writes") {
		t.Fatalf("rejection did not name the uncovered gap:\n%s", logged)
	}
	if !strings.Contains(logged, "waited=") {
		t.Fatalf("the uncovered-gap rejection did not report how long it waited:\n%s", logged)
	}
	if !strings.Contains(logged, "bloodtrail: snapshot file rejected") {
		t.Fatalf("no \"snapshot file rejected\" marker logged:\n%s", logged)
	}
}

// TestFileBootWaitsForInFlightWriteAndAdopts is milestone 7's headline
// case: a write whose eager bump has committed but whose Apply has not yet
// run when the file attempt starts -- the shape that made a one-instant
// decision reject under any concurrent writer (a batch's bump lands at its
// first buffered operation, its Apply only at the flush). The attempt must
// WAIT, not reject: once the write's Apply lands, mid-wait, the very next
// coverage check adopts the file with the write replayed.
func TestFileBootWaitsForInFlightWriteAndAdopts(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate()

	// The in-flight write: bumped now, applied only after the attempt is
	// already waiting on it.
	counter, err := engB.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}

	adopted := make(chan bool, 1)
	go func() { adopted <- engB.tryLoadSnapshotFile(ctx) }()

	// Give the attempt a few retry intervals to observe the hole and start
	// waiting. Not load-bearing for correctness -- if the attempt were
	// somehow slower than this, the Apply below simply lands before its
	// first check and the test degrades to the plain replay case -- but in
	// practice this orders "waiting" before "filled".
	time.Sleep(4 * bootGapSettleRetryInterval)

	var nodeID graph.ID
	if err := engB.pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("objectid", "in-flight-write"), fileBootKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordNodeID(nodeID)
	engB.Apply(ctx, scope)

	if !<-adopted {
		t.Fatalf("tryLoadSnapshotFile rejected instead of waiting for the in-flight write to settle:\n%s", buf.String())
	}
	if got := engB.RebuildCount(); got != 0 {
		t.Fatalf("RebuildCount = %d after a settled file boot, want 0", got)
	}

	view, serving := engB.Fresh()
	if !serving {
		t.Fatalf("engine B not serving after the settled adoption")
	}
	for _, id := range []graph.ID{savedNodeID, nodeID} {
		if _, ok := view.Dense(uint64(id)); !ok {
			t.Fatalf("node %d missing from the adopted view:\n%s", id, buf.String())
		}
	}
	if logged := buf.String(); !strings.Contains(logged, "replayed_writes=1") {
		t.Fatalf("the loaded marker did not report the settled write as replayed:\n%s", logged)
	}
}

// TestFileBootReplaysPostFreezeWriteAndAdopts pins the frozen target's
// other tolerance: a whole write landing AFTER the attempt froze its pg
// snapshot -- bump, commit and Apply all mid-wait -- needs no accounting
// against the target (bootGapCoveredAt ignores its counter) but was
// observed by the still-armed buffer, so it must ride the replay rather
// than be lost. The wait itself is still resolved by the pre-freeze
// write's late Apply, exactly as in the test above.
//
// The 4-interval sleep before the post-freeze write orders it after the
// attempt's freeze in practice; if the freeze were somehow slower, the
// write's counter lands inside the target and the test degrades to two
// late-applied writes -- still a pass, merely less discriminating.
func TestFileBootReplaysPostFreezeWriteAndAdopts(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate()

	counter, err := engB.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}

	adopted := make(chan bool, 1)
	go func() { adopted <- engB.tryLoadSnapshotFile(ctx) }()

	time.Sleep(4 * bootGapSettleRetryInterval)

	// The post-freeze write: a complete bump+commit+Apply landing while the
	// attempt is waiting on the pre-freeze hole.
	postFreezeNode := bootGapWrite(t, ctx, engB, "post-freeze-write")

	// Now fill the pre-freeze hole and let the attempt adopt.
	var inFlightNode graph.ID
	if err := engB.pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("objectid", "pre-freeze-write"), fileBootKind)
		if err != nil {
			return err
		}
		inFlightNode = n.ID
		return nil
	}); err != nil {
		t.Fatalf("create node: %v", err)
	}
	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordNodeID(inFlightNode)
	engB.Apply(ctx, scope)

	if !<-adopted {
		t.Fatalf("tryLoadSnapshotFile rejected despite the target being covered once the in-flight write settled:\n%s", buf.String())
	}

	view, serving := engB.Fresh()
	if !serving {
		t.Fatalf("engine B not serving after the settled adoption")
	}
	for _, id := range []graph.ID{savedNodeID, inFlightNode, postFreezeNode} {
		if _, ok := view.Dense(uint64(id)); !ok {
			t.Fatalf("node %d missing from the adopted view (saved %d, pre-freeze %d, post-freeze %d):\n%s",
				id, savedNodeID, inFlightNode, postFreezeNode, buf.String())
		}
	}
	if logged := buf.String(); !strings.Contains(logged, "replayed_writes=2") {
		t.Fatalf("the loaded marker did not report both writes as replayed:\n%s", logged)
	}
}

// TestFileBootRejectsAfterFallbackWrite pins the poison-adjacent path a
// real deployment hits when a fallback-shaped write (raw Cypher, a wipe)
// lands during boot: Apply trips the engine into fallback before any View
// exists, and the file attempt must then refuse to adopt -- the buffered
// ChangeSets cannot express that write, and the pending fallback rebuild
// reads post-write state anyway.
func TestFileBootRejectsAfterFallbackWrite(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate()
	// Stop() up front cancels bgCtx, so the recovery goroutine the
	// fallback-shaped Apply below launches exits at its first context
	// check instead of racing this test's own synchronous file attempt
	// with a pg rebuild adoption of its own -- the deterministic-attempt
	// choice this file's header describes.
	engB.Stop()

	counter, err := engB.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}
	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordFallback("raw cypher during boot")
	engB.Apply(ctx, scope)

	if engB.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile adopted despite a fallback-shaped write during boot")
	}

	logged := buf.String()
	if !strings.Contains(logged, "the engine entered fallback while the file was loading") {
		t.Fatalf("rejection did not name the fallback:\n%s", logged)
	}
}
