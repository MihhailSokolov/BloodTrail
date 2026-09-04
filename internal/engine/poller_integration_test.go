// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestPollerDrivesRebuilds is the poller's integration evidence: Start
// against a live datapipe_status row builds the first snapshot (rule (a):
// no snapshot exists yet), then updating the row's last_complete_analysis_at
// to a newer value (rule (b)) drives a second rebuild that picks up the new
// stamp -- and Stop terminates the goroutine promptly.
func TestPollerDrivesRebuilds(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	createDatapipeStatusTable(t, pool)

	stamp1 := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := pool.Exec(ctx,
		`INSERT INTO datapipe_status (singleton, status, updated_at, last_complete_analysis_at) VALUES (true, 'idle', now(), $1)`,
		stamp1,
	); err != nil {
		t.Fatalf("insert datapipe_status: %v", err)
	}

	eng := New(pgDriver, pool, Config{
		Enabled:      true,
		PollInterval: 50 * time.Millisecond,
		Log:          testEngineLogger(),
	})

	eng.Start(ctx)
	defer eng.Stop()

	snap1 := waitForFreshSnapshot(t, eng, 2*time.Second)
	if !snap1.AnalysisStamp.Equal(stamp1) {
		t.Fatalf("first build's AnalysisStamp = %v, want %v", snap1.AnalysisStamp, stamp1)
	}

	// A write outside the poller's own rebuild invalidates the snapshot the
	// poller just adopted.
	eng.NoteWrite()

	stamp2 := stamp1.Add(time.Hour)
	if _, err := pool.Exec(ctx,
		`UPDATE datapipe_status SET last_complete_analysis_at = $1, updated_at = now() WHERE singleton`,
		stamp2,
	); err != nil {
		t.Fatalf("update datapipe_status: %v", err)
	}

	// The poller must pick up the newer stamp (rule (b)) and rebuild again,
	// which also happens to clear the staleness NoteWrite introduced.
	snap2 := waitForFreshSnapshot(t, eng, 2*time.Second)
	if !snap2.AnalysisStamp.Equal(stamp2) {
		t.Fatalf("second build's AnalysisStamp = %v, want %v", snap2.AnalysisStamp, stamp2)
	}

	stopped := make(chan struct{})
	go func() {
		eng.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(1 * time.Second):
		t.Fatalf("Stop did not return within 1s")
	}
}

// createDatapipeStatusTable creates the datapipe_status table with the
// column names and constraint verified against upstream BloodHound
// v9.6.0's migrations (cmd/api/src/database/migration/migrations/
// 00000000000001_init.sql for singleton/status/updated_at/
// last_complete_analysis_at + the singleton_uni CHECK, and
// 20260514224346_v9_add_analysis_timestamps_bed_8136.sql /
// 20260805120000_v9_default_enable_graph_storage_optimization.sql /
// 20260722120000_v9_add_graph_storage_optimization_parameter.sql for the
// three columns added afterwards), and registers a t.Cleanup to drop it so
// other packages sharing this test database aren't left with a stray table
// the poller doesn't otherwise expect.
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
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP TABLE IF EXISTS datapipe_status"); err != nil {
			t.Errorf("drop datapipe_status: %v", err)
		}
	})
}

// waitForFreshSnapshot polls eng.Fresh() until it reports a fresh snapshot
// or timeout elapses, failing the test on timeout.
func waitForFreshSnapshot(t *testing.T, eng *Engine, timeout time.Duration) *snapshot.Snapshot {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if snap, fresh := eng.Fresh(); fresh {
			return snap
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for a fresh snapshot", timeout)
	return nil
}
