// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// fileBootKind is the node kind seedFileBootSnapshot writes -- suffixed
// nowhere, unlike readback_integration_test.go's runtimeKind, since this
// file never asserts anything about the kind table being fresh; it only
// needs a real, resolvable kind for CreateNode.
var fileBootKind = graph.StringKind("FileBootNode")

// seedFileBootSnapshot builds one engine ("A") directly against pgDriver/
// pool, rebuilds its base snapshot from PostgreSQL, then replays one write
// through it via the exact bump+Apply sequence write_observer.go's
// ensureBumped/Apply pairing performs in production -- built here from the
// engine-level primitives directly, the same way watermark_integration_test.go's
// own suites do, since this package cannot import the root package's
// observer wiring without an import cycle (engine.go's own doc, e.g.
// ApplyCount, makes the identical point). Finally calls SaveSnapshot, which
// folds engine A's base+segment View into one flat snapshot and writes it
// to dir/graph-<id>.btsnap, stamped with the pg watermark counter that is
// now exactly caught up (nothing else touched the watermark table in
// between).
//
// Returns the created node's database id, so a caller booting a later
// engine from the resulting file can assert that node is actually present
// in what got loaded.
func seedFileBootSnapshot(t *testing.T, ctx context.Context, pgDriver *pg.Driver, pool *pgxpool.Pool, dir string) graph.ID {
	t.Helper()

	engA := New(pgDriver, pool, Config{Enabled: true, SnapshotDir: dir, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, engA)

	if err := engA.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("seedFileBootSnapshot: initial RebuildNow: %v", err)
	}

	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{fileBootKind}); err != nil {
		t.Fatalf("seedFileBootSnapshot: assert kinds: %v", err)
	}

	// Mirrors write_observer.go's ensureBumped: bump the pg watermark
	// counter eagerly, before the write's own pg effect.
	counter, err := engA.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("seedFileBootSnapshot: BumpWatermark: %v", err)
	}

	var nodeID graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("objectid", "file-boot-node"), fileBootKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("seedFileBootSnapshot: create node: %v", err)
	}

	// Mirrors driver.go's WriteTransaction success branch: hand the
	// committed write's ChangeSet (naming the one node it touched) and its
	// bumped counter to Apply, which reads it back from PostgreSQL and
	// publishes the resulting delta segment.
	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordNodeID(nodeID)
	engA.Apply(ctx, scope)

	view, serving := engA.Fresh()
	if !serving {
		t.Fatalf("seedFileBootSnapshot: engine A not serving after Apply")
	}
	if _, ok := view.Dense(uint64(nodeID)); !ok {
		t.Fatalf("seedFileBootSnapshot: applied node missing from engine A's own view")
	}

	if err := engA.SaveSnapshot(ctx); err != nil {
		t.Fatalf("seedFileBootSnapshot: SaveSnapshot: %v", err)
	}

	return nodeID
}

// snapshotFilePathFor mirrors snapshotFilePath's own filename convention
// (graph-<id>.btsnap) so a test can locate and manipulate the file
// directly, without reaching into the unexported method on an *Engine it
// may not even want to construct.
func snapshotFilePathFor(t *testing.T, pgDriver *pg.Driver, dir string) string {
	t.Helper()

	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatalf("snapshotFilePathFor: no default graph set")
	}
	return filepath.Join(dir, fmt.Sprintf("graph-%d.btsnap", graphModel.ID))
}

// waitForBootMarker blocks until buf's captured log contains marker, as a
// companion to waitForFresh, not a replacement. The two observe different
// instants: Fresh() flips true inside the adoption's own applyMu critical
// section, while the "snapshot file loaded" line a test goes on to assert
// is printed by tryLoadSnapshotFile only after that adoption returns -- so
// a test that reads the buffer the moment Fresh() flips can see a served
// engine and a still-missing marker in the same breath. CI measured
// exactly that once, in this file's own default-graph test (its failure
// dump shows no rebuild and no rejection: the file was genuinely adopted,
// its line simply not yet printed). The root package's wiring tests carry
// the identical wait (waitForFileAttemptOutcome) for the identical reason.
func waitForBootMarker(t *testing.T, buf *lockedBuffer, marker string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if strings.Contains(buf.String(), marker) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("boot never logged %q within 5s of serving:\n%s", marker, buf.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newLogCapturingEngine builds an *Engine wired to dir, over pgDriver/pool,
// whose log output is captured into the returned *lockedBuffer (defined in
// engine_integration_test.go, this package's own log-capture helper) at
// Debug level -- Debug because tryLoadSnapshotFile's one quiet "no
// snapshot file" line, and Apply's own "write-through applied" line, are
// both logged there, and a test asserting the ABSENCE of a marker needs
// every level captured, not just Info and above.
func newLogCapturingEngine(pgDriver *pg.Driver, pool *pgxpool.Pool, dir string) (*Engine, *lockedBuffer) {
	buf := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return New(pgDriver, pool, Config{Enabled: true, SnapshotDir: dir, Log: logger}), buf
}

// TestFileBootLoadsMatchingWatermarkSnapshot is the core
// case: engine B, booting fresh over the same pool and SnapshotDir engine A
// just saved into, must adopt the file directly -- no PostgreSQL rebuild at
// all (RebuildCount() == 0) -- and serve the node engine A wrote through
// and saved, having logged the "snapshot file loaded" marker and nothing
// under "snapshot file rejected".
func TestFileBootLoadsMatchingWatermarkSnapshot(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	nodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	engB, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engB.Start(ctx)
	defer engB.Stop()

	waitForFresh(t, engB)
	waitForBootMarker(t, buf, "bloodtrail: snapshot file loaded")

	if got := engB.RebuildCount(); got != 0 {
		t.Fatalf("RebuildCount = %d after booting from a matching-watermark snapshot file, want 0 (no pg rebuild should ever have run)", got)
	}

	view, serving := engB.Fresh()
	if !serving {
		t.Fatalf("engine B not serving after boot")
	}
	if _, ok := view.Dense(uint64(nodeID)); !ok {
		t.Fatalf("engine B's loaded snapshot is missing the node engine A wrote through and saved")
	}

	if logged := buf.String(); strings.Contains(logged, "bloodtrail: snapshot file rejected") {
		t.Fatalf("boot logged \"snapshot file rejected\" for a matching-watermark file:\n%s", logged)
	}
}

// TestFileBootWaitsForTheDefaultGraphToResolve pins the mechanism the
// production-ordering defect turned on (see runBootLoad's own doc, and the
// root package's file_boot_wiring_integration_test.go for the end-to-end
// version through dawgs.Open): a boot load that starts BEFORE the default
// graph resolves must still make its file attempt afterwards, not spend it
// on the one instant at which it is guaranteed to be impossible.
//
// Every other test in this file boots over graphtest.OpenPG's driver, whose
// schema was asserted before the engine existed -- so DefaultGraph() answers
// immediately, and none of them can distinguish "attempted once, up front"
// from "attempted once the graph was knowable". This one deliberately builds
// a SECOND, fresh pg.Driver over the same pool and never asserts a schema on
// it until the engine has already been running for a while, reproducing
// exactly what bloodtrail.Open (driver.go) forces on every real deployment:
// Start runs before any caller can possibly have called AssertSchema.
func TestFileBootWaitsForTheDefaultGraphToResolve(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	nodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	// A driver whose SchemaManager has never resolved a default graph --
	// pg.NewDriver does no database I/O at all, so this is exactly the state
	// dawgs.Open hands bloodtrail.Open.
	unresolved := pg.NewDriver(size.Gibibyte, pool)
	if _, ok := unresolved.DefaultGraph(); ok {
		t.Fatalf("a freshly constructed pg.Driver already reports a default graph; this test's premise no longer holds")
	}

	engB, buf := newLogCapturingEngine(unresolved, pool, dir)
	engB.Start(ctx)
	defer engB.Stop()

	// Long enough for several boot-load iterations to run and find nothing
	// they can do yet (the loop's first wait is fallbackRetryInterval, 100ms).
	time.Sleep(250 * time.Millisecond)

	if got := engB.RebuildCount(); got != 0 {
		t.Fatalf("RebuildCount = %d while the default graph was still unresolved, want 0: a rebuild that cannot name a graph must not be attempted, let alone counted", got)
	}
	if !strings.Contains(buf.String(), "bloodtrail: boot load waiting for the default graph") {
		t.Fatalf("boot load never logged that it was waiting for the default graph:\n%s", buf.String())
	}

	// The instant a real caller's AssertSchema could first land.
	if err := unresolved.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	waitForFresh(t, engB)
	waitForBootMarker(t, buf, "bloodtrail: snapshot file loaded")

	if got := engB.RebuildCount(); got != 0 {
		t.Fatalf("RebuildCount = %d after the deferred file attempt should have adopted, want 0 (no pg rebuild should ever have run)", got)
	}

	view, serving := engB.Fresh()
	if !serving {
		t.Fatalf("engine B not serving after boot")
	}
	if _, ok := view.Dense(uint64(nodeID)); !ok {
		t.Fatalf("engine B's loaded snapshot is missing the node engine A wrote through and saved")
	}

	if logged := buf.String(); strings.Contains(logged, "bloodtrail: boot load failed") {
		t.Fatalf("boot load logged a failure while merely waiting for the default graph -- an ordinary startup wait is not an error:\n%s", logged)
	}
}

// TestFileBootRejectsStaleWatermarkAndRebuilds is the
// out-of-band-write case: after engine A saves its file, the pg watermark
// counter is bumped directly (simulating a write that landed -- and
// advanced the counter -- without ever being folded into a new save,
// exactly the gap a crash between commit and the next SaveSnapshot would
// leave behind). A fresh engine C must then refuse the now-stale file, log
// "snapshot file rejected" naming both counters, fall back to a genuine
// PostgreSQL rebuild (RebuildCount() > 0), and still serve correctly --
// PostgreSQL itself was never behind, only the file was.
func TestFileBootRejectsStaleWatermarkAndRebuilds(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	nodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	if _, err := pool.Exec(ctx, "update bloodtrail_watermark set counter = counter + 1"); err != nil {
		t.Fatalf("bump watermark out of band: %v", err)
	}

	engC, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engC.Start(ctx)
	defer engC.Stop()

	waitForFresh(t, engC)

	if got := engC.RebuildCount(); got == 0 {
		t.Fatalf("RebuildCount = 0 after booting from a stale-watermark snapshot file, want at least one pg rebuild")
	}

	view, serving := engC.Fresh()
	if !serving {
		t.Fatalf("engine C not serving after boot")
	}
	if _, ok := view.Dense(uint64(nodeID)); !ok {
		t.Fatalf("engine C's rebuilt snapshot is missing a node that is genuinely present in PostgreSQL")
	}

	logged := buf.String()
	if !strings.Contains(logged, "bloodtrail: snapshot file rejected") {
		t.Fatalf("boot did not log \"snapshot file rejected\" for a stale-watermark file:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file loaded") {
		t.Fatalf("boot logged \"snapshot file loaded\" for a stale-watermark file:\n%s", logged)
	}
}

// TestFileBootRejectsCorruptFileAndRebuilds is the "corrupt the
// file" case: a byte flipped inside an otherwise-valid, matching-watermark
// file must still be refused (ReadSnapshotFile's own CRC32 check, or
// possibly a field it drives an allocation from, catching it before this
// package ever sees a *Snapshot) -- logged as "snapshot file rejected"
// naming the read error -- with the engine falling back to a genuine
// PostgreSQL rebuild and still serving correctly.
func TestFileBootRejectsCorruptFileAndRebuilds(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	nodeID := seedFileBootSnapshot(t, ctx, pgDriver, pool, dir)

	corruptFile(t, snapshotFilePathFor(t, pgDriver, dir))

	engC, buf := newLogCapturingEngine(pgDriver, pool, dir)
	engC.Start(ctx)
	defer engC.Stop()

	waitForFresh(t, engC)

	if got := engC.RebuildCount(); got == 0 {
		t.Fatalf("RebuildCount = 0 after booting from a corrupt snapshot file, want at least one pg rebuild")
	}

	view, serving := engC.Fresh()
	if !serving {
		t.Fatalf("engine C not serving after boot")
	}
	if _, ok := view.Dense(uint64(nodeID)); !ok {
		t.Fatalf("engine C's rebuilt snapshot is missing a node that is genuinely present in PostgreSQL")
	}

	logged := buf.String()
	if !strings.Contains(logged, "bloodtrail: snapshot file rejected") {
		t.Fatalf("boot did not log \"snapshot file rejected\" for a corrupt file:\n%s", logged)
	}
}

// corruptFile flips one byte at the midpoint of the file at path,
// simulating on-disk corruption -- the same idea as the snapshot package's
// own TestSnapshotFileCorruptByte (snapshot/file_test.go), reimplemented
// here rather than reused since that helper is unexported to its own
// package. The midpoint of a real, non-trivial fixture file (a full
// snapshot body: CSR arrays, a kind table, a PropStore) lands well clear of
// both the 7-byte magic and the trailing 4-byte CRC, so this reliably
// exercises the CRC/structure check rather than the magic/version guards a
// smaller, synthetic file might land in instead.
func corruptFile(t *testing.T, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("corruptFile: read %s: %v", path, err)
	}
	if len(data) < 32 {
		t.Fatalf("corruptFile: %s is only %d bytes, too short to corrupt safely away from the magic/CRC", path, len(data))
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("corruptFile: write %s: %v", path, err)
	}
}

// TestFileBootDisabledWhenSnapshotDirEmpty is the "SnapshotDir
// empty -> no file ever written/read" case: with the feature left off
// (the zero value of Config.SnapshotDir), a normal boot must fall straight
// to the pg rebuild loop, SaveSnapshot must be a silent no-op, and NONE of
// the feature's log markers -- loaded, rejected, written, or even the
// quiet "no snapshot file" miss -- may ever fire, since snapshotFilePath's
// own empty-string check is meant to make this a genuine no-op before
// touching the filesystem at all.
func TestFileBootDisabledWhenSnapshotDirEmpty(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	buf := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	eng := New(pgDriver, pool, Config{Enabled: true, SnapshotDir: "", Log: logger})
	resetWatermarkTable(t, ctx, eng)

	eng.Start(ctx)
	waitForFresh(t, eng)

	if got := eng.RebuildCount(); got == 0 {
		t.Fatalf("RebuildCount = 0 with the snapshot-file feature disabled, want at least one pg rebuild (there is no file to load instead)")
	}

	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot with SnapshotDir empty returned an error, want a silent no-op: %v", err)
	}

	eng.Stop()

	logged := buf.String()
	for _, marker := range []string{"snapshot file loaded", "snapshot file rejected", "snapshot file written", "no snapshot file"} {
		if strings.Contains(logged, marker) {
			t.Fatalf("logged %q with the snapshot-file feature disabled (SnapshotDir empty), want none of these markers to ever fire:\n%s", marker, logged)
		}
	}
}

// TestFileBootQuietlyMissesWithNoSnapshotFileYet is I1's regression: a real,
// non-empty SnapshotDir (the feature genuinely enabled) whose directory
// simply has no graph-<id>.btsnap file in it yet -- the ordinary shape of
// the very first boot ever against a given SnapshotDir, before any
// Driver.Close has had a chance to call SaveSnapshot even once -- must fall
// straight through to the pg rebuild loop exactly like the SnapshotDir-empty
// case above, but is NOT the same code path: tryLoadSnapshotFile's own
// ReadSnapshotFile call actually runs and gets a real fs.ErrNotExist, which
// its own doc says logs the quiet Debug "bloodtrail: no snapshot file"
// marker rather than the noisier Info "snapshot file rejected" one a
// present-but-untrustworthy file would earn. No other test exercises that
// specific branch: TestFileBootDisabledWhenSnapshotDirEmpty
// never reaches ReadSnapshotFile at all (snapshotFilePath's own empty-string
// check returns false first), and every other file_boot test in this file
// seeds a real file before booting from it.
func TestFileBootQuietlyMissesWithNoSnapshotFileYet(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	dir := t.TempDir() // real directory, deliberately left empty: no .btsnap file
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	resetWatermarkTable(t, ctx, eng)

	eng.Start(ctx)
	defer eng.Stop()

	waitForFresh(t, eng)

	if got := eng.RebuildCount(); got == 0 {
		t.Fatalf("RebuildCount = 0 booting against an empty SnapshotDir with no file yet, want at least one pg rebuild")
	}

	view, serving := eng.Fresh()
	if !serving {
		t.Fatalf("engine not serving after boot with no snapshot file present")
	}
	if view.NodeCount() == 0 {
		t.Fatalf("boot's own pg rebuild adopted an empty view, want the hydrated fixture's nodes")
	}

	logged := buf.String()
	if !strings.Contains(logged, "bloodtrail: no snapshot file") {
		t.Fatalf("did not log the quiet \"no snapshot file\" marker for a first boot with no file present:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file rejected") {
		t.Fatalf("logged \"snapshot file rejected\" noise for a merely-missing file, want the quiet miss only:\n%s", logged)
	}
	if strings.Contains(logged, "bloodtrail: snapshot file loaded") {
		t.Fatalf("logged \"snapshot file loaded\" with no file ever present:\n%s", logged)
	}
}
