// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/dbswitch"
	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file pin the watermark lineage (watermark.go's
// watermarkLineageDDL) against a live PostgreSQL. The counter only sees
// writes that bump it, and the stock BloodHound image -- running between a
// `bloodtrail rollback` and the next install, or refilling the graph after
// `install --replace-postgres-graph` -- writes through the plain dawgs pg
// driver, which never does. Each test saves a snapshot file the way a
// running BloodTrail does, lets such a writer change PostgreSQL behind it,
// and requires the next boot to refuse the file rather than serve a graph
// PostgreSQL no longer holds.
//
// Where the installer takes part, these tests run its own statements
// (dbswitch.Store, through graphtest.PSQLRunner), not a re-typed copy of
// them.

// lineageRejection is the reason adoptSnapshotFileView logs for a file from
// another lineage.
const lineageRejection = "watermark lineage changed since the file was written"

// installerStore is the dbswitch.Store `bloodtrail install` and `rollback`
// run their statements through, pointed at the test database.
func installerStore(pool *pgxpool.Pool) dbswitch.Store {
	return dbswitch.Store{
		Compose:  dockerx.Compose{Runner: graphtest.PSQLRunner{Pool: pool}, File: "docker-compose.yml", ProjectDir: "."},
		Service:  "app-db",
		User:     "bloodhound",
		Database: "bloodhound",
	}
}

// endLineage runs the installer's own EndWatermarkLineage.
func endLineage(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := installerStore(pool).EndWatermarkLineage(ctx); err != nil {
		t.Fatalf("EndWatermarkLineage: %v", err)
	}
}

// stockImageCreateNode commits a node the way the stock BloodHound image
// does: through the plain dawgs pg driver, with no watermark bump and no
// Apply.
func stockImageCreateNode(t *testing.T, ctx context.Context, pgDriver *pg.Driver, objectID string) graph.ID {
	t.Helper()
	var id graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("objectid", objectID), fileBootKind)
		if err != nil {
			return err
		}
		id = n.ID
		return nil
	}); err != nil {
		t.Fatalf("stock image write: %v", err)
	}
	return id
}

// stockImageSetName updates an existing node's "name" property the same
// way: an UPDATE, which no row count or maximum id would ever reveal.
func stockImageSetName(t *testing.T, ctx context.Context, pgDriver *pg.Driver, id graph.ID, name string) {
	t.Helper()
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(&graph.Node{ID: id, Properties: graph.NewProperties().Set("name", name)})
	}); err != nil {
		t.Fatalf("stock image update: %v", err)
	}
}

// pgLineage reads the lineage bloodtrail_watermark is in.
func pgLineage(t *testing.T, ctx context.Context, pool *pgxpool.Pool) snapshot.Lineage {
	t.Helper()
	var lineage [16]byte
	if err := pool.QueryRow(ctx, selectWatermarkLineageSQL).Scan(&lineage); err != nil {
		t.Fatalf("read lineage: %v", err)
	}
	return snapshot.Lineage(lineage)
}

// requireLineageRejection asserts the boot refused its file because of the
// lineage, and for no other reason: in particular not by waiting out an
// uncovered counter gap, which is how the installer's own counter advance
// alone would have ended.
func requireLineageRejection(t *testing.T, logged string) {
	t.Helper()
	if !strings.Contains(logged, lineageRejection) {
		t.Fatalf("the boot did not refuse the file for its lineage:\n%s", logged)
	}
	for _, marker := range []string{"bloodtrail: snapshot file loaded", "boot gap not covered"} {
		if strings.Contains(logged, marker) {
			t.Fatalf("logged %q; the file should have been refused on its lineage alone:\n%s", marker, logged)
		}
	}
}

// TestFileBootRejectsAFileTheStockImageWroteBehind is the rollback/reinstall
// cycle: BloodTrail saves its file and stops, `bloodtrail rollback` puts the
// stock image back, the stock image creates one node and renames another
// through the plain pg driver, and `bloodtrail install` starts BloodTrail
// again -- with the counter exactly where the file left it. Without the
// lineage the file is adopted and the engine serves a graph missing the
// new node and holding the old name, indefinitely; with it, the boot
// refuses the file at once and rebuilds from PostgreSQL.
func TestFileBootRejectsAFileTheStockImageWroteBehind(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	endLineage(t, ctx, pool) // rollback, once the stock image is back
	strayID := stockImageCreateNode(t, ctx, pgDriver, "stock-image-write")
	stockImageSetName(t, ctx, pgDriver, savedNodeID, "renamed-by-the-stock-image")
	endLineage(t, ctx, pool) // install, right before BloodTrail starts

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.Start(ctx)
	defer eng.Stop()

	waitForFresh(t, eng)
	waitForBootMarker(t, buf, "bloodtrail: snapshot file rejected")
	requireLineageRejection(t, buf.String())

	if got := eng.RebuildCount(); got == 0 {
		t.Fatalf("RebuildCount = 0 after refusing the snapshot file, want the PostgreSQL rebuild")
	}
	view, serving := eng.Fresh()
	if !serving {
		t.Fatalf("engine not serving after boot")
	}
	if _, ok := view.Dense(uint64(strayID)); !ok {
		t.Fatalf("node %d, committed by the stock image, is missing from the served graph", strayID)
	}
	dense, ok := view.Dense(uint64(savedNodeID))
	if !ok {
		t.Fatalf("node %d is missing from the served graph", savedNodeID)
	}
	if name, _ := view.PropValueByName(dense, "name"); name != "renamed-by-the-stock-image" {
		t.Fatalf("node %d serves name %v, want the stock image's rename", savedNodeID, name)
	}
}

// TestFileBootRejectsAFileFromBeforeTheGraphWasReplaced is `bloodtrail
// install --replace-postgres-graph` after a rollback: the installer's own
// ClearGraph truncates the graph, BloodHound's migrator refills it through
// the plain pg driver, and neither moves the counter. The install ends the
// lineage again before starting BloodTrail; this test deliberately leaves
// that out, pinning that the clear already ended it by itself.
func TestFileBootRejectsAFileFromBeforeTheGraphWasReplaced(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	savedNodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	if err := installerStore(pool).ClearGraph(ctx); err != nil {
		t.Fatalf("ClearGraph: %v", err)
	}
	migratedID := stockImageCreateNode(t, ctx, pgDriver, "migrated-from-neo4j")

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.Start(ctx)
	defer eng.Stop()

	waitForFresh(t, eng)
	waitForBootMarker(t, buf, "bloodtrail: snapshot file rejected")
	requireLineageRejection(t, buf.String())

	view, _ := eng.Fresh()
	if _, ok := view.Dense(uint64(migratedID)); !ok {
		t.Fatalf("the migrated node %d is missing from the served graph", migratedID)
	}
	if dense, ok := view.Dense(uint64(savedNodeID)); ok && view.Alive(dense) {
		t.Fatalf("node %d, truncated away with the replaced graph, is still served", savedNodeID)
	}
}

// TestFileBootRejectsAFileFromAnEndedLineageDespiteBootWrites is the same
// cycle on the boot a real deployment has: BloodHound writes the graph
// within milliseconds of starting, so the file attempt never sees a quiet
// counter -- it sees buffered writes of its own, whose replay would happily
// cover their own counters. The lineage has to refuse the file anyway,
// before the gap is ever weighed. The settle window is shortened only so a
// regression fails quickly instead of stalling for the real 5s.
func TestFileBootRejectsAFileFromAnEndedLineageDespiteBootWrites(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	oldTimeout := bootGapSettleTimeout
	bootGapSettleTimeout = 250 * time.Millisecond
	t.Cleanup(func() { bootGapSettleTimeout = oldTimeout })

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	endLineage(t, ctx, pool)
	stockImageCreateNode(t, ctx, pgDriver, "stock-image-write")
	endLineage(t, ctx, pool)

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.bootGap.activate() // what Start does when SnapshotDir is set
	bootGapWrite(t, ctx, eng, "boot-analysis-write")

	if eng.tryLoadSnapshotFile(ctx) {
		t.Fatalf("tryLoadSnapshotFile adopted a file from an ended lineage:\n%s", buf.String())
	}
	requireLineageRejection(t, buf.String())
}

// TestFileBootRejectsAFileFromAnotherLineageWithAMatchingCounter isolates
// the engine's half: a lineage that changed while the counter did not -- a
// different or reset database behind the same snapshot directory, whose
// fresh counter happens to read what the file was stamped with. Every
// counter check passes, so only the lineage stands between that file and
// adoption.
func TestFileBootRejectsAFileFromAnotherLineageWithAMatchingCounter(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	if _, err := pool.Exec(ctx, "update bloodtrail_watermark set lineage = gen_random_uuid()"); err != nil {
		t.Fatalf("replace lineage: %v", err)
	}

	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	eng.Start(ctx)
	defer eng.Stop()

	waitForFresh(t, eng)
	waitForBootMarker(t, buf, "bloodtrail: snapshot file rejected")
	requireLineageRejection(t, buf.String())
	if got := eng.RebuildCount(); got == 0 {
		t.Fatalf("RebuildCount = 0 after refusing the snapshot file, want the PostgreSQL rebuild")
	}
}

// TestSnapshotFileNamesTheLineageItsReplicaWasLoadedIn pins where a file's
// lineage comes from: the load its replica descends from, never PostgreSQL
// at save time. A BloodTrail still running when the lineage ends (the
// rollback's end racing a container that has not stopped yet) keeps
// writing, and its own writes bring the counter back into agreement with
// what it has applied -- so it can still save. If that save stamped the
// lineage PostgreSQL holds by then, its file would vouch for writes it never
// saw; stamped with its own, the file is refused.
func TestSnapshotFileNamesTheLineageItsReplicaWasLoadedIn(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{persistRaceKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	dir := t.TempDir()
	eng, _ := newLogCapturingEngine(pgDriver, pool, dir)
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)
	defer eng.Stop()

	adoptOneRebuild(t, ctx, eng)
	loaded := pgLineage(t, ctx, pool)
	view, _ := eng.Fresh()
	if got := view.Base().WatermarkLineage; got != loaded {
		t.Fatalf("rebuilt snapshot carries lineage %s, want the %s it was loaded in", got, loaded)
	}
	var loadedText string
	if err := pool.QueryRow(ctx, "select lineage::text from bloodtrail_watermark where id = 1").Scan(&loadedText); err != nil {
		t.Fatalf("read lineage text: %v", err)
	}
	if loaded.String() != loadedText {
		t.Fatalf("Lineage.String() = %q, want PostgreSQL's own spelling %q", loaded.String(), loadedText)
	}

	endLineage(t, ctx, pool)
	applyOneWrite(t, ctx, eng)
	if _, converged := eng.WatermarkConverged(ctx); !converged {
		t.Fatalf("the engine's own write did not bring the counter back into agreement; the save below would prove nothing")
	}

	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	path := snapshotFilePathFor(t, pgDriver, dir)
	saved, _, err := snapshot.ReadSnapshotFile(path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	if saved.WatermarkLineage != loaded {
		t.Fatalf("file names lineage %s, want %s, the one its replica was loaded in (PostgreSQL is in %s now)",
			saved.WatermarkLineage, loaded, pgLineage(t, ctx, pool))
	}

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.bootGap.activate()
	if engB.tryLoadSnapshotFile(ctx) {
		t.Fatalf("a file saved after its lineage ended was adopted:\n%s", buf.String())
	}
	requireLineageRejection(t, buf.String())
}

// holdWatermarkTable opens a transaction that reads bloodtrail_watermark and
// keeps it open -- holding the ACCESS SHARE lock every reader takes, the way
// a pg_dump running across the whole database does for its entire length --
// until the returned release is called (at the latest, when the test ends).
func holdWatermarkTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (release func()) {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, "select counter from bloodtrail_watermark"); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		t.Fatalf("read bloodtrail_watermark: %v", err)
	}
	released := false
	release = func() {
		if !released {
			released = true
			_ = tx.Rollback(context.Background())
			conn.Release()
		}
	}
	t.Cleanup(release)
	return release
}

// TestEnsureWatermarkTableDoesNotWaitOnAReader pins that Start's DDL stays
// out of any reader's way once the lineage column exists. ALTER TABLE takes
// an ACCESS EXCLUSIVE lock before it ever checks IF NOT EXISTS, so running it
// on every start would queue BloodHound's startup behind whatever holds the
// table -- a nightly pg_dump holds it for as long as the dump runs.
func TestEnsureWatermarkTableDoesNotWaitOnAReader(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng, buf := newLogCapturingEngine(pgDriver, pool, t.TempDir())
	eng.ensureWatermarkTable(ctx) // the table, and its lineage column, exist from here on

	holdWatermarkTable(t, ctx, pool)

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	eng.ensureWatermarkTable(callCtx)
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("ensureWatermarkTable took %v with a reader holding the table, want it to return without waiting on that reader", waited)
	}
	if logged := buf.String(); strings.Contains(logged, "DDL failed") {
		t.Fatalf("a start with every column already in place reported a DDL failure:\n%s", logged)
	}
}

// TestEnsureWatermarkTableGivesUpOnALockedUpgrade is the one start that has
// to take that lock -- the upgrade that adds the column -- finding the table
// held. It must give up within a bounded wait rather than hold startup
// hostage, leave the counter every write bumps working, and add the column
// at a later start once nothing holds the table.
func TestEnsureWatermarkTableGivesUpOnALockedUpgrade(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	if _, err := pool.Exec(ctx, `drop table if exists bloodtrail_watermark;
create table bloodtrail_watermark (
	id smallint primary key default 1 check (id = 1),
	counter bigint not null default 0,
	updated_at timestamptz not null default now()
);
insert into bloodtrail_watermark (id, counter) values (1, 7);`); err != nil {
		t.Fatalf("create a pre-lineage table: %v", err)
	}

	eng, buf := newLogCapturingEngine(pgDriver, pool, t.TempDir())
	release := holdWatermarkTable(t, ctx, pool)

	callCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	start := time.Now()
	eng.ensureWatermarkTable(callCtx)
	if waited := time.Since(start); waited > 8*time.Second {
		t.Fatalf("ensureWatermarkTable waited %v on a reader to add the lineage column, want it to give up promptly", waited)
	}
	if logged := buf.String(); !strings.Contains(logged, "watermark lineage DDL failed") {
		t.Fatalf("giving up on the lineage column was not reported:\n%s", logged)
	}
	release()

	if _, err := eng.BumpWatermark(ctx); err != nil {
		t.Fatalf("BumpWatermark after the lineage DDL gave up: %v -- the counter protocol must not depend on it", err)
	}

	eng.ensureWatermarkTable(ctx)
	var (
		counter uint64
		lineage [16]byte
	)
	if err := pool.QueryRow(ctx, "select counter, lineage from bloodtrail_watermark where id = 1").Scan(&counter, &lineage); err != nil {
		t.Fatalf("the next start did not add the lineage column: %v", err)
	}
	if counter != 8 {
		t.Fatalf("counter = %d, want 8 (7, plus the one bump above)", counter)
	}
}

// TestEnsureWatermarkTableGivesAnOlderTableALineage is the upgrade: a
// database whose bloodtrail_watermark an engine from before lineages
// created. Start must add the column, give the row a lineage, and leave the
// counter every earlier write advanced exactly where it was.
func TestEnsureWatermarkTableGivesAnOlderTableALineage(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	if _, err := pool.Exec(ctx, `drop table if exists bloodtrail_watermark;
create table bloodtrail_watermark (
	id smallint primary key default 1 check (id = 1),
	counter bigint not null default 0,
	updated_at timestamptz not null default now()
);
insert into bloodtrail_watermark (id, counter) values (1, 7);`); err != nil {
		t.Fatalf("create a pre-lineage table: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	eng.ensureWatermarkTable(ctx)

	var (
		counter uint64
		lineage [16]byte
	)
	if err := pool.QueryRow(ctx, "select counter, lineage from bloodtrail_watermark where id = 1").Scan(&counter, &lineage); err != nil {
		t.Fatalf("read counter and lineage: %v", err)
	}
	if counter != 7 {
		t.Fatalf("counter = %d after the upgrade, want 7", counter)
	}
	if snapshot.Lineage(lineage).IsZero() {
		t.Fatal("the upgraded row has no lineage")
	}
}

// TestRebuildSaysWhenItCannotReadTheLineage pins that a rebuild whose
// lineage read fails says so. The load itself is sound and still adopted,
// but a snapshot without a lineage is never saved, and the only other trace
// of that is SaveSnapshot's Debug line -- an operator would see snapshot
// files stop appearing and nothing to say why.
func TestRebuildSaysWhenItCannotReadTheLineage(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)
	defer eng.Stop()

	if _, err := pool.Exec(ctx, "alter table bloodtrail_watermark drop column lineage"); err != nil {
		t.Fatalf("drop the lineage column: %v", err)
	}
	t.Cleanup(func() { eng.ensureWatermarkTable(context.Background()) }) // give it back to the tests after this one

	adoptOneRebuild(t, ctx, eng)
	view, _ := eng.Fresh()
	if lineage := view.Base().WatermarkLineage; !lineage.IsZero() {
		t.Fatalf("a rebuild that could not read the lineage stamped %s", lineage)
	}
	if logged := buf.String(); !strings.Contains(logged, "could not read the watermark lineage") {
		t.Fatalf("the rebuild did not say it could not read the lineage:\n%s", logged)
	}

	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if _, err := os.Stat(snapshotFilePathFor(t, pgDriver, dir)); !os.IsNotExist(err) {
		t.Fatalf("a snapshot without a lineage was saved (stat: %v)", err)
	}
}
