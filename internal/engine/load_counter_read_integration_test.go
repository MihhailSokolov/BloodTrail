// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestRebuildSaysWhenItCannotReadTheWatermarkCounter pins that a rebuild
// whose counter read fails says exactly that. The load itself is sound and
// still adopted, but without the counter the ledger is not rebased, so the
// counter stays unaccounted and the next save is refused with the
// "another BloodTrail server may be writing this database" Warn. The real
// cause has to be in the log: before this, the counter failure was either
// silent or reported as a lineage failure, which it is not.
func TestRebuildSaysWhenItCannotReadTheWatermarkCounter(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	dir := t.TempDir()
	eng, buf := newLogCapturingEngine(pgDriver, pool, dir)
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)
	defer eng.Stop()

	if _, err := pool.Exec(ctx, "drop table bloodtrail_watermark"); err != nil {
		t.Fatalf("drop the watermark table: %v", err)
	}
	t.Cleanup(func() { eng.ensureWatermarkTable(context.Background()) }) // give it back to the tests after this one

	adoptOneRebuild(t, ctx, eng)

	logged := buf.String()
	if !strings.Contains(logged, "could not read the watermark counter") {
		t.Fatalf("the rebuild did not say it could not read the watermark counter:\n%s", logged)
	}
	if strings.Contains(logged, "could not read the watermark lineage") {
		t.Fatalf("the rebuild blamed the lineage for a counter read that failed before it:\n%s", logged)
	}
}
