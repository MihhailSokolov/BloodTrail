// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bloodtrail_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// servedMarker is the exact message engine.TryAllShortestPaths/TryCypher log
// (at Info) whenever they serve a query from the in-memory snapshot --
// duplicated here (rather than exported from internal/engine) since only
// this test needs to recognize it in captured log output.
const servedMarker = "bloodtrail: path engine served"

// builderServedMarker is the exact message engine.TryNodeCount/
// TryNodeFetchIDs/TryNodeFetchKinds (internal/engine/serve_builder.go's
// servedOp) log at Debug whenever they serve a structural node query from
// the in-memory snapshot -- duplicated here for the same reason servedMarker
// is (only this test needs to recognize it in captured log output). It is
// deliberately Debug, not Info, unlike servedMarker (servedOp's own doc), so
// installLogCapture must enable Debug-level capture for this marker to ever
// appear in buf.
const builderServedMarker = "bloodtrail: builder engine served"

// lockedBuffer is a bytes.Buffer safe for concurrent writes from the
// engine's poller/serving goroutines and reads from the test goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// installLogCapture installs a slog default logger that writes into a
// returned buffer, restoring the previous default via t.Cleanup. It must be
// called before dawgs.Open constructs the bloodtrail driver: driver.go's
// Open reads slog.Default() exactly once, at construction time, to build
// the engine's own Config.Log, so installing the capture afterward would
// miss every line the engine itself logs.
//
// The handler is configured at Debug level (rather than the text handler's
// own Info default) so builderServedMarker -- logged via DebugContext by the
// structural node/relationship query path (servedOp's doc) -- is captured
// alongside servedMarker's Info-level line; every existing caller that only
// ever greps for servedMarker is unaffected by the extra Debug output.
func installLogCapture(t *testing.T) *lockedBuffer {
	t.Helper()

	buf := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return buf
}

// createDatapipeStatusTable creates the datapipe_status table with the
// column names and constraint verified against upstream BloodHound
// v9.6.0's migrations -- see internal/engine/poller_integration_test.go's
// identically named helper for the full provenance note; duplicated here
// since that one is unexported in a different package.
//
// Unlike that helper, this one does not register its own t.Cleanup to drop
// the table: pg.Driver.Close (drivers/pg/driver.go) closes the shared
// *pgxpool.Pool it was constructed from, and the bt/oracle drivers built on
// that same pool in these tests are closed via plain defer in the test
// body -- which always runs before any t.Cleanup callback -- so a
// t.Cleanup-based drop here would run against an already-closed pool.
// Callers must instead defer the drop themselves, placed after the
// bt/oracle Close defers so LIFO ordering runs it first, while the pool is
// still open.
func createDatapipeStatusTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()

	const ddl = `CREATE TABLE IF NOT EXISTS datapipe_status (
		singleton boolean DEFAULT true NOT NULL,
		status text NOT NULL,
		updated_at timestamp with time zone NOT NULL,
		last_complete_analysis_at timestamp with time zone,
		last_analysis_run_at timestamp with time zone,
		last_complete_optimize_at timestamp with time zone,
		next_scheduled_analysis_at timestamp with time zone,
		CONSTRAINT singleton_uni CHECK (singleton)
	)`

	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("create datapipe_status: %v", err)
	}
}

// dropDatapipeStatusTable drops the table created by
// createDatapipeStatusTable. See that function's doc for why callers defer
// this explicitly instead of relying on t.Cleanup.
func dropDatapipeStatusTable(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS datapipe_status"); err != nil {
		t.Errorf("drop datapipe_status: %v", err)
	}
}

// insertDatapipeStatus inserts the table's single row. status is
// deliberately a parameter rather than hardcoded "idle": decideRebuild's
// rule (c) (internal/engine/poller.go) opportunistically rebuilds on any
// stale snapshot whenever status == "idle", which would make phase 3 below
// (a plain write must not itself trigger a rebuild) vacuous. Tests that
// exercise that phase pass a non-idle status.
func insertDatapipeStatus(t *testing.T, pool *pgxpool.Pool, status string, stamp time.Time) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO datapipe_status (singleton, status, updated_at, last_complete_analysis_at) VALUES (true, $1, now(), $2)`,
		status, stamp,
	); err != nil {
		t.Fatalf("insert datapipe_status: %v", err)
	}
}

// updateDatapipeStamp advances the row's last_complete_analysis_at, the
// stamp decideRebuild's rule (b) compares against the current snapshot's
// AnalysisStamp to decide whether a completed analysis run justifies a
// rebuild.
func updateDatapipeStamp(t *testing.T, pool *pgxpool.Pool, stamp time.Time) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE datapipe_status SET last_complete_analysis_at = $1, updated_at = now() WHERE singleton`,
		stamp,
	); err != nil {
		t.Fatalf("update datapipe_status: %v", err)
	}
}

// shortestPathsViaCriteria runs the same graph.Criteria shape BloodHound's
// API builds for FetchAllShortestPaths -- query.And(query.Equals(StartID),
// query.Equals(EndID)) -- directly through db's RelationshipQuery (the path
// recordingRelationshipQuery.FetchAllShortestPaths, not tx.Query, serves),
// and renders the result the same way driver_integration_test.go's
// pathSignatures does for the cypher-text path, so the two are directly
// comparable.
func shortestPathsViaCriteria(t *testing.T, ctx context.Context, db graph.Database, startID, endID graph.ID) []string {
	t.Helper()

	var sigs []string
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		criteria := query.And(
			query.Equals(query.StartID(), startID),
			query.Equals(query.EndID(), endID),
		)
		return tx.Relationships().Filter(criteria).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for p := range cursor.Chan() {
				parts := make([]string, 0, len(p.Nodes))
				for _, n := range p.Nodes {
					parts = append(parts, n.ID.String())
				}
				sigs = append(sigs, strings.Join(parts, ">"))
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("shortestPathsViaCriteria(%d, %d): %v", startID, endID, err)
	}
	sort.Strings(sigs)
	return sigs
}

// waitForEngineServe repeatedly calls query -- one FetchAllShortestPaths
// lookup through the wrapped driver, rendered the same way
// shortestPathsViaCriteria does -- until buf's captured log shows more
// servedMarker lines than baseline, up to deadline. Each call actively
// drives a query through the driver: the served marker is logged only from
// inside TryAllShortestPaths/TryCypher when a caller's query actually
// reaches them, never by the poller's background rebuild alone (which logs
// its own, differently worded "snapshot rebuilt" line), so a caller that
// only waited passively without issuing calls here would wait forever. It
// returns the result from the exact call that observed a new marker, so a
// caller gets both "the engine built/rebuilt" and a live result in one
// step, with no separate race between the two.
func waitForEngineServe(t *testing.T, buf *lockedBuffer, baseline int, deadline time.Duration, query func() []string) []string {
	t.Helper()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		got := query()
		if strings.Count(buf.String(), servedMarker) > baseline {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for a new %q log line (count stayed at or below %d)\nlog:\n%s", deadline, servedMarker, baseline, buf.String())
	return nil
}

// waitForBuilderServe is waitForEngineServe's counterpart for the
// builder-serving path (Task 8's Nodes() wiring): it repeatedly calls query
// until buf's captured log shows more builderServedMarker lines than
// baseline, up to deadline, returning the result from the exact call that
// observed a new marker. Generic over query's return type so it serves both
// nodeCountByKind (int64) and nodeIDsByKind ([]graph.ID) callers without a
// separate helper for each.
func waitForBuilderServe[T any](t *testing.T, buf *lockedBuffer, baseline int, deadline time.Duration, query func() T) T {
	t.Helper()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		got := query()
		if strings.Count(buf.String(), builderServedMarker) > baseline {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	var zero T
	t.Fatalf("timed out after %s waiting for a new %q log line (count stayed at or below %d)\nlog:\n%s", deadline, builderServedMarker, baseline, buf.String())
	return zero
}

// nodeCountByKind runs tx.Nodes().Filter(query.Kind(query.Node(), kind)).
// Count() -- the exact shape recordingNodeQuery.Count (node_query.go)
// recognizes and may serve from the engine -- through db, returning the
// result.
func nodeCountByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) int64 {
	t.Helper()

	var count int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Nodes().Filter(query.Kind(query.Node(), kind)).Count()
		count = n
		return err
	})
	if err != nil {
		t.Fatalf("Nodes().Filter(Kind(%s)).Count(): %v", kind, err)
	}
	return count
}

// nodeIDsByKind runs tx.Nodes().Filter(query.Kind(query.Node(), kind)).
// FetchIDs() through db, returning the matching ids sorted ascending so two
// calls' results (e.g. bt vs. the pg oracle) are directly comparable as
// sets, matching TryNodeFetchIDs' own documented "compare as sets, not
// sequences" contract (internal/engine/serve_builder.go).
func nodeIDsByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) []graph.ID {
	t.Helper()

	var ids []graph.ID
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Filter(query.Kind(query.Node(), kind)).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				ids = append(ids, id)
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("Nodes().Filter(Kind(%s)).FetchIDs(): %v", kind, err)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestEngineServesFromLiveDriver is Task 13's core evidence: opened through
// dawgs.Open(ctx, bloodtrail.DriverName, cfg) exactly as BloodHound would,
// with a live datapipe_status row driving the poller, the driver must serve
// both the Criteria/FetchAllShortestPaths API and cypher-text queries from
// the in-memory engine once it has built a snapshot, keep answering
// correctly (now delegated to PostgreSQL) the instant a write makes that
// snapshot stale, and resume serving once the datapipe stamp advances.
func TestEngineServesFromLiveDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	// Both must be set before dawgs.Open: SettingsFromEnv and the engine's
	// captured Config.Log are both read exactly once, at Open() time.
	t.Setenv(bloodtrail.EnvEnginePollInterval, "50ms")
	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	// A plain pg driver on the same pool and data is the oracle.
	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	createDatapipeStatusTable(t, pool)
	// Registered after the bt/oracle Close defers above, so LIFO ordering
	// runs this drop first -- while the pool they share is still open. See
	// createDatapipeStatusTable's doc.
	defer dropDatapipeStatusTable(t, pool)

	stamp1 := time.Now().UTC().Truncate(time.Microsecond)
	insertDatapipeStatus(t, pool, "running", stamp1)

	// --- Phase 1: FetchAllShortestPaths via the Criteria API, waiting for
	// the engine's first build.
	got := waitForEngineServe(t, buf, 0, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})
	want := shortestPathsViaCriteria(t, ctx, oracle, ids["c0"], ids["c10"])
	if len(want) == 0 {
		t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("FetchAllShortestPaths paths differ\n got: %v\nwant: %v", got, want)
	}

	// --- Phase 2: the three cypher equivalence texts through tx.Query.
	for _, eq := range equivalenceQueries {
		cypher := eq.cypher(ids)
		t.Run(eq.name, func(t *testing.T) {
			got := pathSignatures(t, ctx, bt, cypher)
			want := pathSignatures(t, ctx, oracle, cypher)
			if len(want) == 0 {
				t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("paths differ\n got: %v\nwant: %v", got, want)
			}
		})
	}

	// --- Phase 3: a write through the driver invalidates the snapshot, but
	// must not itself trigger a rebuild (status stays "running", so
	// decideRebuild's idle-only rule (c) never fires) -- results stay
	// correct via the PostgreSQL fallback until the datapipe stamp advances,
	// which does trigger a rebuild and a fresh served line.
	servedBefore := strings.Count(buf.String(), servedMarker)

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("ExtraNode"))
		return err
	}); err != nil {
		t.Fatalf("WriteTransaction (CreateNode): %v", err)
	}

	// Give the poller several ticks' worth of headroom to (incorrectly)
	// rebuild if the taint/idle-gating logic were broken, then assert it
	// did not, and that the query is still answered correctly (now
	// delegated).
	time.Sleep(10 * 50 * time.Millisecond)
	if got := shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"]); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("post-write (delegated) paths differ\n got: %v\nwant: %v", got, want)
	}
	if servedAfter := strings.Count(buf.String(), servedMarker); servedAfter != servedBefore {
		t.Fatalf("served line count changed from %d to %d after a write with no datapipe stamp advance", servedBefore, servedAfter)
	}

	stamp2 := stamp1.Add(time.Hour)
	updateDatapipeStamp(t, pool, stamp2)

	got = waitForEngineServe(t, buf, servedBefore, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("post-rebuild paths differ\n got: %v\nwant: %v", got, want)
	}
}

// TestEngineOffDelegatesEverythingAndNeverServes is phase 4: with
// BLOODTRAIL_ENGINE=off, the driver must answer every query correctly by
// delegating straight to PostgreSQL, and the engine must never log a served
// line -- Start is a no-op when disabled (internal/engine/poller.go), so no
// poller goroutine even runs, and every TryAllShortestPaths/TryCypher call
// declines immediately (reason "disabled") before ever touching a snapshot.
func TestEngineOffDelegatesEverythingAndNeverServes(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	t.Setenv(bloodtrail.EnvEngine, "off")
	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	got := shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	want := shortestPathsViaCriteria(t, ctx, oracle, ids["c0"], ids["c10"])
	if len(want) == 0 {
		t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("FetchAllShortestPaths paths differ\n got: %v\nwant: %v", got, want)
	}

	for _, eq := range equivalenceQueries {
		cypher := eq.cypher(ids)
		t.Run(eq.name, func(t *testing.T) {
			got := pathSignatures(t, ctx, bt, cypher)
			want := pathSignatures(t, ctx, oracle, cypher)
			if len(want) == 0 {
				t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("paths differ\n got: %v\nwant: %v", got, want)
			}
		})
	}

	if n := strings.Count(buf.String(), servedMarker); n != 0 {
		t.Fatalf("engine served %d time(s) with %s=off, want 0\nlog:\n%s", n, bloodtrail.EnvEngine, buf.String())
	}
}

// TestWipeGraphInvalidatesEngineSnapshot is Finding 1's integration
// evidence: WipeGraph -- BloodHound's "clear database" action -- reaches
// PostgreSQL through the embedded *pg.Driver's own internal WriteTransaction
// call, never through Driver's own WriteTransaction override (embedding has
// no virtual dispatch), so without Driver's own WipeGraph override
// (driver.go) the engine would keep serving the pre-wipe snapshot
// indefinitely. This mirrors TestEngineServesFromLiveDriver's phase 3 (a
// plain WriteTransaction write) but for WipeGraph specifically: a query the
// engine was serving before the wipe must not serve again (no new
// servedMarker line) until the datapipe stamp advances and the poller
// rebuilds, and the post-wipe result must be correct (empty) throughout.
func TestWipeGraphInvalidatesEngineSnapshot(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	t.Setenv(bloodtrail.EnvEnginePollInterval, "50ms")
	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	driver, ok := bt.(*bloodtrail.Driver)
	if !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", bt)
	}

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	createDatapipeStatusTable(t, pool)
	// Registered after the bt Close defer above, so LIFO ordering runs this
	// drop first -- while the pool is still open. See
	// createDatapipeStatusTable's doc.
	defer dropDatapipeStatusTable(t, pool)

	stamp1 := time.Now().UTC().Truncate(time.Microsecond)
	// "running", not "idle": decideRebuild's rule (c) opportunistically
	// rebuilds any stale snapshot whenever status == "idle", which would
	// make the "no premature rebuild" assertion below vacuous.
	insertDatapipeStatus(t, pool, "running", stamp1)

	// Build the initial snapshot and confirm it actually serves before the
	// wipe -- otherwise this test would prove nothing about invalidation.
	waitForEngineServe(t, buf, 0, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})

	servedBefore := strings.Count(buf.String(), servedMarker)

	if err := driver.WipeGraph(ctx, nil); err != nil {
		t.Fatalf("WipeGraph: %v", err)
	}

	// Several poll intervals' worth of headroom for the poller to
	// (incorrectly) rebuild if WipeGraph's NoteWrite override were missing
	// or broken; status stays "running" so rule (c) must not fire on its
	// own, and no query has been issued yet to reach TryAllShortestPaths at
	// all -- this alone must not produce a new served line.
	time.Sleep(10 * 50 * time.Millisecond)
	if servedAfter := strings.Count(buf.String(), servedMarker); servedAfter != servedBefore {
		t.Fatalf("served line count changed from %d to %d after WipeGraph with no datapipe stamp advance", servedBefore, servedAfter)
	}

	// A query issued now must fall through to PostgreSQL -- correctly empty,
	// the graph having just been wiped -- rather than serve the stale
	// pre-wipe snapshot, and must not itself log a new served line.
	if got := shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"]); len(got) != 0 {
		t.Fatalf("post-wipe query returned paths from a wiped graph: %v", got)
	}
	if servedAfter := strings.Count(buf.String(), servedMarker); servedAfter != servedBefore {
		t.Fatalf("served line count changed from %d to %d after querying a post-wipe, still-stale snapshot", servedBefore, servedAfter)
	}

	// Advancing the stamp lets the poller rebuild against the now-empty
	// graph; the engine resumes serving, correctly reporting zero paths.
	stamp2 := stamp1.Add(time.Hour)
	updateDatapipeStamp(t, pool, stamp2)

	got := waitForEngineServe(t, buf, servedBefore, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})
	if len(got) != 0 {
		t.Fatalf("post-rebuild query on a wiped graph returned paths: %v", got)
	}
}

// TestFetchAllShortestPathsClosesCursorWhenDelegateReturnsEarly is Finding
// 2's regression test: recordingRelationshipQuery.FetchAllShortestPaths
// (relationship_query.go) must close the graph.Cursor[graph.Path] it hands
// to delegate itself -- provider-closes, the same convention the pg
// driver's own FetchAllShortestPaths follows (drivers/pg/relationship.go:
// `cursor := ...; defer cursor.Close(); return delegate(cursor)`) -- rather
// than leaving the cursor for delegate to close. Before the fix, a delegate
// that returned without draining Chan() at all left the cursor's feeder
// goroutine (internal/engine/result.go's pathCursor.feed) blocked forever on
// an unbuffered channel send: only Close() cancels the context that
// unblocks it.
//
// This needs a live, actually-serving engine (not a mock): the served
// branch inside FetchAllShortestPaths -- the only branch that constructs an
// engine.NewPathCursor at all -- is only reached once
// engine.TryAllShortestPaths itself succeeds, which requires a real
// snapshot built from PostgreSQL. A query that instead fell through to the
// pg driver's own FetchAllShortestPaths would exercise that driver's
// already-correct cursor and prove nothing about this fix.
func TestFetchAllShortestPathsClosesCursorWhenDelegateReturnsEarly(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	t.Setenv(bloodtrail.EnvEnginePollInterval, "50ms")
	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	createDatapipeStatusTable(t, pool)
	defer dropDatapipeStatusTable(t, pool)

	insertDatapipeStatus(t, pool, "running", time.Now().UTC().Truncate(time.Microsecond))

	// Warm the engine: this both builds the initial snapshot and confirms
	// the query is actually served (not delegated) before moving on.
	waitForEngineServe(t, buf, 0, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})

	runtime.GC()
	before := runtime.NumGoroutine()

	// Repeat the served call several times, each with a delegate that
	// returns immediately without draining Chan() at all -- the exact shape
	// that leaked one feeder goroutine per call before the fix. Repeating
	// amplifies a real leak into an unmistakable signal well above any
	// incidental goroutine-count jitter (e.g. pool housekeeping).
	const attempts = 20
	for i := 0; i < attempts; i++ {
		err := bt.ReadTransaction(ctx, func(tx graph.Transaction) error {
			criteria := query.And(
				query.Equals(query.StartID(), ids["c0"]),
				query.Equals(query.EndID(), ids["c10"]),
			)
			return tx.Relationships().Filter(criteria).FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error {
				return nil
			})
		})
		if err != nil {
			t.Fatalf("FetchAllShortestPaths (attempt %d): %v", i, err)
		}
	}

	if !strings.Contains(buf.String(), servedMarker) {
		t.Fatalf("expected at least one of these queries to have been served by the engine\nlog:\n%s", buf.String())
	}

	// The feeder goroutines should unblock and exit promptly once Close()
	// cancels their context; poll with a generous deadline rather than
	// asserting immediately; allow a small amount of slack over the
	// baseline for incidental, unrelated goroutine churn.
	const slack = 3
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		after := runtime.NumGoroutine()
		if after <= before+slack {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("feeder goroutines leaked: goroutines before=%d after=%d (delta %d over %d attempts)", before, after, after-before, attempts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestNodeQueryServesFromLiveDriver is Task 8's core evidence: opened
// through dawgs.Open exactly like TestEngineServesFromLiveDriver, a
// structural Nodes() query -- the shape BloodHound's builder queries use for
// e.g. FetchNodeIDsByKind, Filter(query.Kind(query.Node(), kind)) -- must be
// served from the in-memory engine (recordingNodeQuery, node_query.go) once
// a snapshot exists, agree with the pg-driver oracle on both Count() and the
// FetchIDs() set, and a query recognize.FromNodeCriteria rejects outright
// (a property lookup, never a shape FromNodeCriteria models -- see its own
// doc) must still delegate transparently to PostgreSQL and answer correctly.
func TestNodeQueryServesFromLiveDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	// Must be set before dawgs.Open: SettingsFromEnv and the engine's
	// captured Config.Log are both read exactly once, at Open() time.
	t.Setenv(bloodtrail.EnvEnginePollInterval, "50ms")
	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	loadDatasets(t, ctx, bt)

	// A plain pg driver on the same pool and data is the oracle.
	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	createDatapipeStatusTable(t, pool)
	// Registered after the bt/oracle Close defers above, so LIFO ordering
	// runs this drop first -- while the pool they share is still open. See
	// createDatapipeStatusTable's doc.
	defer dropDatapipeStatusTable(t, pool)

	stamp1 := time.Now().UTC().Truncate(time.Microsecond)
	insertDatapipeStatus(t, pool, "running", stamp1)

	// TraversalNode is carried by every one of traversal_shapes.json's 45
	// nodes (testdata/dawgs/traversal_shapes.json), so a bare kind filter
	// against it gives an unambiguous, easy-to-verify answer.
	kind := graph.StringKind("TraversalNode")

	// --- Phase 1: Count(), waiting for the engine's first build.
	gotCount := waitForBuilderServe(t, buf, 0, 5*time.Second, func() int64 {
		return nodeCountByKind(t, ctx, bt, kind)
	})
	wantCount := nodeCountByKind(t, ctx, oracle, kind)
	if wantCount == 0 {
		t.Fatalf("oracle returned zero nodes for kind %s; the fixture or kind name is wrong", kind)
	}
	if gotCount != wantCount {
		t.Fatalf("Count() = %d, want %d (oracle)", gotCount, wantCount)
	}

	// --- Phase 2: FetchIDs() set-equality (order is not a shared contract;
	// see TryNodeFetchIDs' doc).
	gotIDs := nodeIDsByKind(t, ctx, bt, kind)
	wantIDs := nodeIDsByKind(t, ctx, oracle, kind)
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("FetchIDs() returned %d ids, want %d\n got: %v\nwant: %v", len(gotIDs), len(wantIDs), gotIDs, wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("FetchIDs() sets differ\n got: %v\nwant: %v", gotIDs, wantIDs)
		}
	}

	// --- Phase 3: a property-filtered query -- recognize.FromNodeCriteria
	// always rejects a PropertyLookup conjunct (its doc) -- must still
	// delegate transparently through recordingNodeQuery.Count to PostgreSQL
	// and answer correctly, proving Filter's recording never interferes with
	// an unrecognized shape. adcs_fanout.json's Group node "n" is the only
	// node in either fixture carrying an "objectid" property.
	const wantObjectID = "S-1-5-21-2643190041-1319121918-239771340-513"
	var gotObjectIDCount int64
	if err := bt.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Nodes().Filter(query.Equals(query.NodeProperty("objectid"), wantObjectID)).Count()
		gotObjectIDCount = n
		return err
	}); err != nil {
		t.Fatalf("property-filtered Count(): %v", err)
	}
	if gotObjectIDCount != 1 {
		t.Fatalf("property-filtered Count() = %d, want 1", gotObjectIDCount)
	}
}
