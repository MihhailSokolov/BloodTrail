// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bloodtrail_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
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
func installLogCapture(t *testing.T) *lockedBuffer {
	t.Helper()

	buf := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
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
