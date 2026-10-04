// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file pin what a boot does with a snapshot file stamped
// AHEAD of PostgreSQL's watermark counter: a database restored from a backup
// taken before the file's last write, with nothing ending the lineage. No
// sequence of BloodTrail writes produces that; the file holds writes the
// restored database no longer has, and nothing this boot can observe will
// ever make it right.

// restoreBackupFromBeforeTheFile plays a backup, taken before the snapshot
// file's last write, back into PostgreSQL: that write's row and its counter
// value are gone, and the lineage is left as it was.
func restoreBackupFromBeforeTheFile(t *testing.T, ctx context.Context, pool *pgxpool.Pool, lostID graph.ID) {
	t.Helper()
	if _, err := pool.Exec(ctx, "delete from node where id = $1", int64(lostID)); err != nil {
		t.Fatalf("restore: drop the row: %v", err)
	}
	if _, err := pool.Exec(ctx, "update bloodtrail_watermark set counter = 0"); err != nil {
		t.Fatalf("restore: roll the counter back: %v", err)
	}
}

// TestFileBootRefusesAFileStampedAheadOfTheCounterAtStart is the restore
// seen from a boot that recorded where PostgreSQL stood at start: the
// counter was behind the file's stamp. A boot write then bumps the counter
// back up to the stamp and has not reached its Apply when the file attempt
// freezes its target -- so the target equals the stamp with nothing
// buffered, the degenerate "quiet restart" cover. The boot must refuse the
// file at once instead of serving the row PostgreSQL lost.
func TestFileBootRefusesAFileStampedAheadOfTheCounterAtStart(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	lostID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir) // stamped 1, holds lostID
	restoreBackupFromBeforeTheFile(t, ctx, pool, lostID)

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.captureStartState(ctx) // what Start does, in its order
	if at := eng.atStart.Load(); at == nil || at.counter != 0 {
		t.Fatalf("start state = %+v, want the restored counter 0 captured:\n%s", at, buf.String())
	}
	eng.bootGap.activate()
	if _, err := eng.BumpWatermark(ctx); err != nil { // a boot write, not yet applied
		t.Fatalf("BumpWatermark: %v", err)
	}

	adopted := eng.tryLoadSnapshotFile(ctx)
	logged := buf.String()
	if adopted {
		t.Fatalf("adopted a file stamped ahead of the counter at start, serving node %d PostgreSQL no longer holds:\n%s", lostID, logged)
	}
	if !strings.Contains(logged, "behind the file's stamp") {
		t.Fatalf("the refusal did not say the counter was behind the file's stamp:\n%s", logged)
	}
}

// TestFileBootRejectsAtOnceABootCounterTheFileAlreadyClaims is the same
// restore, played back after this boot already recorded where PostgreSQL
// stood -- so the start state cannot show it, and this boot's own write
// draws a counter the file already claims instead. Waiting cannot fill that
// in -- the buffered counter will never stop contradicting the file -- so
// the attempt must reject at once rather than sit out the settle timeout
// before its PostgreSQL rebuild.
func TestFileBootRejectsAtOnceABootCounterTheFileAlreadyClaims(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	lostID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir) // stamped 1

	const settle = 20 * time.Second
	prevTimeout := bootGapSettleTimeout
	bootGapSettleTimeout = settle
	t.Cleanup(func() { bootGapSettleTimeout = prevTimeout })

	// The start state is captured first, while the counter still stands
	// level with the file's stamp; the restore lands after it, and a boot
	// write then draws counter 1, which the file already claims, and is
	// buffered.
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.captureStartState(ctx)
	restoreBackupFromBeforeTheFile(t, ctx, pool, lostID)
	eng.bootGap.activate()
	bootGapWrite(t, ctx, eng, "boot-write-under-the-stamp")

	start := time.Now()
	adopted := eng.tryLoadSnapshotFile(ctx)
	elapsed := time.Since(start)
	logged := buf.String()
	if adopted {
		t.Fatalf("adopted a file whose stamp a boot write's own counter contradicts:\n%s", logged)
	}
	if elapsed >= settle/2 {
		t.Fatalf("the attempt took %v to reject a contradiction no write can resolve; want an immediate rejection, not the %v settle wait:\n%s", elapsed, settle, logged)
	}
	if !strings.Contains(logged, "contradicts the file") {
		t.Fatalf("the rejection did not say the boot writes contradict the file:\n%s", logged)
	}
}
