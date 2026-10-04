// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestWritePathPoolOutlivesStopUntilTheShutdownSave pins the shutdown
// order Driver.Close relies on: Stop, then the shutdown save, then the write
// path's own pool closes. Stop used to close that pool itself, so a
// read-back already holding it when the shutdown began could fail its next
// query there, enter fallback, and make the save that follows Stop skip
// writing the file. The pool must stay usable through Stop and the save,
// and close only when CloseWritePool is called.
func TestWritePathPoolOutlivesStopUntilTheShutdownSave(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	eng := New(pgDriver, pool, Config{Enabled: true, SnapshotDir: dir, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, eng)
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// One write through the write path, so its pool exists, as it does on
	// any server that has ingested anything.
	kind := graph.StringKind("WritePoolShutdownNode")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{kind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}
	counter, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}
	var nodeID graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("objectid", "write-pool-shutdown"), kind)
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
	eng.Apply(ctx, scope)
	if _, serving := eng.Fresh(); !serving {
		t.Fatal("engine not serving after the write (fixture assumption)")
	}

	// A read-back under way when the shutdown begins holds the pool get
	// handed it.
	inFlight := eng.writePool.get(pool, eng.cfg.Log)
	if inFlight == pool {
		t.Fatal("the write path ran on the main pool (fixture assumption)")
	}

	eng.Stop()

	var one int
	if err := inFlight.QueryRow(ctx, "select 1").Scan(&one); err != nil {
		t.Fatalf("the write path's pool failed a query after Stop, before the shutdown save: %v", err)
	}
	if got := eng.writePool.get(pool, eng.cfg.Log); got != inFlight {
		t.Fatal("after Stop the write path no longer gets its own pool, before the shutdown save")
	}

	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot after Stop: %v", err)
	}
	path, ok := eng.snapshotFilePath()
	if !ok {
		t.Fatal("no snapshot file path")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the shutdown save after Stop wrote no file: %v", err)
	}

	eng.CloseWritePool()
	if got := eng.writePool.get(pool, eng.cfg.Log); got != pool {
		t.Fatal("after CloseWritePool the write path still gets its own pool, want the main pool")
	}
	deadline := time.Now().Add(5 * time.Second)
	for inFlight.Ping(ctx) == nil {
		if time.Now().After(deadline) {
			t.Fatal("the write path's pool still answers 5s after CloseWritePool")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// nodeTableLock is a held ACCESS EXCLUSIVE lock on the `node` table, taken
// on a connection of its own: nothing can read that table until release is
// called, which is what parks a read-back's first query mid-flight for as
// long as a test needs it there.
type nodeTableLock struct {
	release func()
}

// holdNodeTableLock takes the lock and returns it. Released by the returned
// release, and again at t's end, so a failing test cannot leave the table
// locked for the tests that follow.
func holdNodeTableLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool) nodeTableLock {
	t.Helper()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire a connection for the node lock: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		t.Fatalf("begin the node lock's transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "lock table node in access exclusive mode"); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		t.Fatalf("lock table node: %v", err)
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			_ = tx.Rollback(ctx)
			conn.Release()
		})
	}
	t.Cleanup(release)
	return nodeTableLock{release: release}
}

// waitForNodeReadBlockedOnLock waits until PostgreSQL itself reports a
// statement of its own waiting on the `node` table lock: the read-back is
// then genuinely in flight -- past get, holding the write path's pool,
// mid-query -- rather than merely about to start, which is the only state
// in which this file's claim about Stop means anything.
//
// It waits for a condition PostgreSQL publishes, never for a duration, and
// fails rather than continuing if the wait never appears: a test that called
// Stop before the read-back reached the database would pass while pinning
// nothing.
func waitForNodeReadBlockedOnLock(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	const waitingQuery = `select count(*) from pg_stat_activity
		where datname = current_database() and pid <> pg_backend_pid()
		  and wait_event_type = 'Lock' and query ilike '%from node%'`

	deadline := time.Now().Add(30 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, waitingQuery).Scan(&waiting); err != nil {
			t.Fatalf("read pg_stat_activity: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no statement is waiting on the node table lock 30s after the Apply started: its read-back never reached PostgreSQL, so nothing below pins anything")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestStopLeavesAnInFlightReadBackItsPoolAndTheSaveKeepsItsFile is the
// shutdown ordering itself, driven by the race that made the ordering a bug
// fix in the first place -- which
// TestWritePathPoolOutlivesStopUntilTheShutdownSave above does not reach: it
// checks that the pool get hands out still answers after Stop, but no Apply
// is running while Stop is called, so nothing there would notice a Stop that
// closed the pool out from under a read-back's LATER query. That is the
// failure the ordering exists to prevent: Stop closing the pool did not
// break the query already on the wire, it broke the next one, which failed
// to acquire, failed the read-back, put the engine in fallback -- and so
// cost the shutdown save that follows Stop its file.
//
// So this test stops the engine with a read-back genuinely parked
// mid-sequence: the write it applies names a node AND an edge, so readBack
// queries `node` and then `edge` on the same pool, and an ACCESS EXCLUSIVE
// lock on `node` holds it between get and that second query while Stop runs.
// Then, in Driver.Close's order:
//
//   - the pool the read-back is holding is still the write path's own, after
//     Stop (not the main pool);
//   - the read-back's remaining queries succeed once the lock goes, so the
//     write is replayed into the replica rather than costing a fallback:
//     the engine still serves, and what it serves is still PostgreSQL's own
//     answer (assertTypedCasesMatchOracle, guarded on the engine serving so
//     the comparison cannot pass vacuously);
//   - the shutdown save that follows writes its file;
//   - and only then does CloseWritePool close the pool.
func TestStopLeavesAnInFlightReadBackItsPoolAndTheSaveKeepsItsFile(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	nodeKind := graph.StringKind("StopReadBackNode")
	edgeKind := graph.StringKind("StopReadBackEdge")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	dir := t.TempDir()
	eng := New(pgDriver, pool, Config{Enabled: true, SnapshotDir: dir, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, eng)
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// The write whose Apply is caught mid-read-back: two nodes and an edge
	// between them, committed, with the watermark bumped first exactly as a
	// driver-level write does.
	counter, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}
	var start, end *graph.Node
	var edgeID graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		if start, err = tx.CreateNode(graph.NewProperties().Set("name", "start").Set("objectid", "stop-readback-start"), nodeKind); err != nil {
			return err
		}
		if end, err = tx.CreateNode(graph.NewProperties().Set("name", "end").Set("objectid", "stop-readback-end"), nodeKind); err != nil {
			return err
		}
		rel, err := tx.CreateRelationshipByIDs(start.ID, end.ID, edgeKind, graph.NewProperties())
		if err != nil {
			return err
		}
		edgeID = rel.ID
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	scope := NewWriteScope()
	scope.SetWatermark(counter)
	scope.Changes().RecordNodeID(start.ID)
	scope.Changes().RecordNodeID(end.ID)
	scope.Changes().RecordEdgeID(edgeID)

	lock := holdNodeTableLock(t, ctx, pool)

	applyDone := make(chan struct{})
	go func() {
		defer close(applyDone)
		eng.Apply(ctx, scope)
	}()
	waitForNodeReadBlockedOnLock(t, ctx, pool)

	// The shutdown begins here, with that read-back holding the write path's
	// pool and more queries left to run on it.
	eng.Stop()
	if got := eng.writePool.get(pool, eng.cfg.Log); got == pool {
		t.Fatal("after Stop the write path gets the main pool: Stop closed the pool the in-flight read-back is holding")
	}

	lock.release()
	select {
	case <-applyDone:
	case <-time.After(60 * time.Second):
		t.Fatal("the Apply whose read-back was parked never finished after the lock went")
	}

	if _, serving := eng.serveState(); !serving {
		t.Fatal("the engine is in fallback after an Apply whose read-back spanned Stop: its read-back failed, and the comparison below would be vacuous")
	}
	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (s:StopReadBackNode) RETURN s`, true},
		{`MATCH (s:StopReadBackNode)-[:StopReadBackEdge]->(e:StopReadBackNode) RETURN s, e`, true},
	})

	// The save the shutdown runs after Stop still writes its file.
	if err := eng.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot after Stop: %v", err)
	}
	path, ok := eng.snapshotFilePath()
	if !ok {
		t.Fatal("no snapshot file path")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the shutdown save after Stop wrote no file: %v", err)
	}

	// Last, as Driver.Close does it.
	eng.CloseWritePool()
	if got := eng.writePool.get(pool, eng.cfg.Log); got != pool {
		t.Fatal("after CloseWritePool the write path still gets its own pool, want the main pool")
	}
}
