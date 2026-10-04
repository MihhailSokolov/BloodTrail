// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"os"
	"testing"
	"time"

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
