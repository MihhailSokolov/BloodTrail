// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bloodtrail

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestDriverLevelWritePanicSettlesItsWatermarkBump pins what this file's unit
// twin (TestDriverCapabilityWritePanicsSettleByWhereThePanicArose,
// driver_write_path_test.go) cannot observe at all: every driver-level write
// bumps the pg watermark counter at its top (ensureBumped), and a bump whose
// write never settles leaves an entry in flight for the life of the process.
// The watermark then never converges again, so no snapshot file can be
// written -- not by a compaction, not by the shutdown save -- however many
// good writes follow.
//
// The panic comes from a stand-in backend (Driver.pgOverride, the only way to
// make the embedded driver panic on demand) around an otherwise real driver,
// engine and database. Driver.Run is the path under test because its panic
// resolves as abandoned: the statement panicked and the embedded driver's
// deferred Close rolled it back, so nothing is durable, the engine keeps
// serving, and the snapshot save that follows has nothing but the stranded
// bump to refuse over.
func TestDriverLevelWritePanicSettlesItsWatermarkBump(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	dir := t.TempDir()
	t.Setenv(EnvSnapshotDir, dir)

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })
	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, serving := d.engine.Fresh(); serving {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("boot load did not produce a serving snapshot within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, converged := d.engine.WatermarkConverged(ctx); !converged {
		t.Fatal("the watermark had not converged before the panicking write (fixture assumption)")
	}

	d.pgOverride = &panickingPGBackend{tx: rawPanicTransaction{}, panicAfterDelegate: true}
	recovered := writePathPanicValueOf(func() { _ = d.Run(ctx, "MATCH (n) DELETE n", nil) })
	if recovered != driverPanicValue {
		t.Fatalf("recovered %v, want the embedded driver's own panic %q unchanged", recovered, driverPanicValue)
	}
	d.pgOverride = nil

	if _, converged := d.engine.WatermarkConverged(ctx); !converged {
		t.Fatal("WatermarkConverged = false after a panicking Run: its eager bump is still in flight, and no snapshot file can ever be written again")
	}
	if _, serving := d.engine.Fresh(); !serving {
		t.Fatal("the engine stopped serving after a panicking Run that rolled back")
	}

	if err := d.engine.SaveSnapshot(ctx); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the snapshot directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("the save after a panicking Run wrote no snapshot file")
	}
}
