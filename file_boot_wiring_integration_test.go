// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file exists because every snapshot-file boot test before it
// (internal/engine/file_boot_integration_test.go) proved the FEATURE works
// while silently assuming an ordering production can never produce.
//
// # The ordering that matters
//
// internal/graphtest.OpenPG asserts the schema on its pg.Driver (graphtest.go)
// BEFORE any engine is constructed over that driver, so by the time those
// tests call Start, pgDriver.DefaultGraph() already answers -- and
// snapshotFilePath (internal/engine/boot.go) can name the file to read.
//
// Production is the other way round, and cannot be anything else:
//
//	dawgs.Open(ctx, bloodtrail.DriverName, cfg)   // Open -> eng.Start(...)
//	db.AssertSchema(ctx, schema)                  // ... only possible AFTER Open returns
//
// pg.NewDriverWithOptions does no database I/O at all, so a freshly opened
// pg.Driver's SchemaManager has hasDefaultGraph == false (dawgs
// drivers/pg/manager.go); only SetDefaultGraph/AssertDefaultGraph -- i.e.
// only a caller's own AssertSchema -- ever sets it. bloodtrail.Open calls
// eng.Start before it returns the driver the caller needs in order to call
// AssertSchema at all, so the boot-load goroutine necessarily starts while
// the default graph is still unresolved.
//
// This file pins the consequence: the snapshot-file attempt must still
// happen, once the default graph resolves, rather than being spent (and
// lost) on the one instant at which it is guaranteed to be impossible.
//
// Lives in package bloodtrail (not bloodtrail_test) for the same two reasons
// apply_integration_test.go does: only the root package can drive the real
// Open path, and only an in-package test can read Driver's unexported engine
// field for RebuildCount.

package bloodtrail

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The engine's own snapshot-file log markers, as an outside observer (an
// operator grepping logs, or this test) sees them -- see
// internal/engine/boot.go's tryLoadSnapshotFile for each one's meaning.
const (
	snapshotFileLoadedMarker   = "bloodtrail: snapshot file loaded"
	snapshotFileRejectedMarker = "bloodtrail: snapshot file rejected"
	noSnapshotFileMarker       = "bloodtrail: no snapshot file"
)

// fileBootWiringKind is this file's own fixture kind, named distinctly from
// every other integration test's in this shared, long-lived database (see
// stalenessNodeKindA's doc for why that matters).
var fileBootWiringKind = graph.StringKind("FileBootWiringNode")

// newBenchPool opens a pgxpool the way BloodHound's own bootstrap does --
// one pool per driver -- closed by t.Cleanup.
//
// Each phase below needs its OWN pool because pg.Driver.Close closes the
// pool it was given (dawgs drivers/pg/driver.go), so the seeding driver's
// Close -- the very call that writes the snapshot file -- also takes its
// pool with it. pgxpool.Pool.Close is itself idempotent, so a t.Cleanup
// close after that is harmless.
func newFileBootPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	pool, err := pg.NewPool(cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// openProductionOrdered opens the BloodTrail driver exactly the way
// production does -- dawgs.Open FIRST, AssertSchema AFTER -- and returns it
// without waiting for boot load, so a caller can observe boot load's own
// behavior across that ordering.
func openProductionOrdered(t *testing.T, ctx context.Context, dsn string, pool *pgxpool.Pool) (*Driver, graph.Database) {
	t.Helper()

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	// Only now -- Open has returned, which is the earliest instant any
	// caller could possibly reach this call.
	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	return d, bt
}

// seedSnapshotFileThroughRealClose drives one full production-shaped
// lifecycle -- open, assert schema, write a node, Close -- so that Close's
// own SaveSnapshot leaves behind a real snapshot file stamped with
// PostgreSQL's current watermark counter. Nothing writes after it returns,
// so that file's watermark still equals PostgreSQL's, which is exactly the
// condition snapshotFileTrustedAtBoot (internal/engine/boot.go) requires.
//
// Returns the written file's path, having already failed the test if no file
// was produced: a seeding failure must never be mistaken for the boot-side
// defect this file is about.
func seedSnapshotFileThroughRealClose(t *testing.T, ctx context.Context, dsn, dir string) string {
	t.Helper()

	pool := newFileBootPool(t, dsn)
	pgDriver := pg.NewDriver(size.Gibibyte, pool)
	if err := pgDriver.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("seed: assert schema: %v", err)
	}
	graphtest.WipeGraph(t, pgDriver)

	d, bt := openProductionOrdered(t, ctx, dsn, pool)
	waitForBootLoad(t, d)

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("objectid", "FILE-BOOT-WIRING-1"), fileBootWiringKind)
		return err
	}); err != nil {
		t.Fatalf("seed: write node: %v", err)
	}

	// Close stops the engine and then saves -- the production path that
	// produces a snapshot file at all (driver.go's Close).
	if err := bt.Close(ctx); err != nil {
		t.Fatalf("seed: close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.btsnap"))
	if err != nil {
		t.Fatalf("seed: glob %s: %v", dir, err)
	}
	if len(matches) != 1 {
		entries, _ := os.ReadDir(dir)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("seed: want exactly one .btsnap file in %s, found %d (%v) -- the seeding half failed, before anything this test is actually about", dir, len(matches), names)
	}

	return matches[0]
}

// TestOpenLoadsSnapshotFileWithProductionAssertSchemaOrdering is this file's
// crux, and the regression test for the defect described at the top: with a
// valid, matching-watermark snapshot file present, a driver opened the way
// production opens one -- dawgs.Open first, AssertSchema after -- must
// actually load that file, and must therefore never spend a full PostgreSQL
// rebuild (RebuildCount() == 0).
//
// Before the fix this failed on both counts at once: the one-shot file
// attempt ran inside Start's goroutine while the default graph was still
// unresolved, so snapshotFilePath returned ("", false) and
// tryLoadSnapshotFile returned false without reading -- or even logging
// anything about -- the file, and boot load then rebuilt from PostgreSQL
// while a perfectly good file sat unread on disk.
func TestOpenLoadsSnapshotFileWithProductionAssertSchemaOrdering(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	dir := t.TempDir()
	t.Setenv(EnvSnapshotDir, dir)

	buf := installLogCapture(t)

	path := seedSnapshotFileThroughRealClose(t, ctx, dsn, dir)

	// Everything above was setup; the measurement window starts here.
	loadedBefore := markerCount(buf, snapshotFileLoadedMarker)
	rejectedBefore := markerCount(buf, snapshotFileRejectedMarker)

	pool := newFileBootPool(t, dsn)
	d, _ := openProductionOrdered(t, ctx, dsn, pool)
	t.Cleanup(func() { _ = d.Close(ctx) })

	waitForBootLoad(t, d)

	if got := markerCount(buf, snapshotFileLoadedMarker) - loadedBefore; got != 1 {
		t.Fatalf("%q fired %d time(s) booting with a valid matching-watermark file at %s, want exactly 1 -- the file attempt never became reachable through the production Open/AssertSchema ordering\ncaptured log:\n%s",
			snapshotFileLoadedMarker, got, path, buf.String())
	}
	if got := markerCount(buf, snapshotFileRejectedMarker) - rejectedBefore; got != 0 {
		t.Fatalf("%q fired %d time(s) for a valid matching-watermark file, want 0\ncaptured log:\n%s", snapshotFileRejectedMarker, got, buf.String())
	}
	if got := d.engine.RebuildCount(); got != 0 {
		t.Fatalf("RebuildCount = %d after booting from a valid matching-watermark snapshot file, want 0 -- a full PostgreSQL rebuild was spent while the file sat unread", got)
	}

	view, serving := d.engine.Fresh()
	if !serving {
		t.Fatalf("engine not serving after a file boot")
	}
	if view.NodeCount() == 0 {
		t.Fatalf("the loaded snapshot is empty, want the node the seeding driver wrote through and saved")
	}
}

// TestOpenWithNoSnapshotFileStillRebuildsThroughProductionOrdering is the
// negative half of the same wiring: making the file attempt survive the
// production ordering must not make a missing file survive it too. With the
// feature enabled but the directory empty, the same production-ordered open
// must log the quiet "no snapshot file" miss ONCE (proving the attempt was
// genuinely reached and genuinely answered, rather than skipped as it was
// before the fix) and then fall through to a real PostgreSQL rebuild.
func TestOpenWithNoSnapshotFileStillRebuildsThroughProductionOrdering(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	dir := t.TempDir() // real directory, deliberately left empty
	t.Setenv(EnvSnapshotDir, dir)

	buf := installLogCapture(t)

	setupPool := newFileBootPool(t, dsn)
	setupDriver := pg.NewDriver(size.Gibibyte, setupPool)
	if err := setupDriver.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("setup: assert schema: %v", err)
	}
	graphtest.WipeGraph(t, setupDriver)

	missBefore := markerCount(buf, noSnapshotFileMarker)

	pool := newFileBootPool(t, dsn)
	d, _ := openProductionOrdered(t, ctx, dsn, pool)
	t.Cleanup(func() { _ = d.Close(ctx) })

	waitForBootLoad(t, d)

	if got := markerCount(buf, noSnapshotFileMarker) - missBefore; got != 1 {
		t.Fatalf("%q fired %d time(s) booting against an empty snapshot directory, want exactly 1 (reached once, answered once -- never retried after a definitive answer)\ncaptured log:\n%s",
			noSnapshotFileMarker, got, buf.String())
	}
	if got := markerCount(buf, snapshotFileLoadedMarker); got != 0 {
		t.Fatalf("%q fired with no file ever present\ncaptured log:\n%s", snapshotFileLoadedMarker, buf.String())
	}
	if got := d.engine.RebuildCount(); got == 0 {
		t.Fatalf("RebuildCount = 0 with no snapshot file to load, want at least one PostgreSQL rebuild")
	}
	if strings.Contains(buf.String(), "bloodtrail: boot load failed") {
		t.Fatalf("boot load logged a failure while merely waiting for the default graph to resolve -- that is an ordinary startup wait, not an error\ncaptured log:\n%s", buf.String())
	}
}
