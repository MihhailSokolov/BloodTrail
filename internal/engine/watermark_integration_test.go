// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// resetWatermarkTable ensures bloodtrail_watermark exists (via eng's own
// ensureWatermarkTable) and resets its one row to counter = 0, so every
// test in this file starts from a known baseline regardless of what any
// earlier test in this same run left behind -- the table is process/
// database-wide, not reset by graphtest.WipeGraph (which only truncates
// graph data).
func resetWatermarkTable(t *testing.T, ctx context.Context, eng *Engine) {
	t.Helper()

	eng.ensureWatermarkTable(ctx)
	if _, err := eng.pool.Exec(ctx, "update bloodtrail_watermark set counter = 0"); err != nil {
		t.Fatalf("resetWatermarkTable: %v", err)
	}
	eng.appliedWatermark.Store(0)
	eng.inflightBumps.Store(0)
	eng.dirtyGen.Store(0)
	eng.settledDirtyGen.Store(0)
	eng.resolvedDirtyGen.Store(0)
}

// parkRebuildLoop claims the engine's single rebuild-loop gate
// (fallbackRebuilding, engine.go) for the test itself, so that nothing any
// test below does -- entering fallback, settling a watermark failure --
// launches a background rebuild goroutine that could adopt a snapshot at an
// unpredictable moment. Every adoption in these tests is then driven
// explicitly through adoptOneRebuild, which is what makes assertions about
// exactly WHEN trust returns deterministic.
func parkRebuildLoop(eng *Engine) {
	eng.fallbackRebuilding.Store(true)
}

// adoptOneRebuild runs one real rebuild (rebuildOnce: a live LoadSnapshot
// against PostgreSQL, the epoch check, and the adoption) and fails the test
// unless the snapshot was actually adopted -- adoption being the only thing
// that ever advances resolvedDirtyGen (adoptRebuiltView).
func adoptOneRebuild(t *testing.T, ctx context.Context, eng *Engine) {
	t.Helper()

	adopted, err := eng.rebuildOnce(ctx, "manual")
	if err != nil {
		t.Fatalf("rebuildOnce: %v", err)
	}
	if !adopted {
		t.Fatalf("rebuildOnce did not adopt its snapshot, want adopted")
	}
}

// wantTrusted asserts WatermarkTrusted, reporting the three generations and
// the engine state alongside a failure so a break is diagnosable without a
// debugger.
func wantTrusted(t *testing.T, ctx context.Context, eng *Engine, want bool, why string) {
	t.Helper()

	if got := eng.WatermarkTrusted(ctx); got != want {
		t.Fatalf("WatermarkTrusted = %v, want %v (%s); generations = (dirty %d, settled %d, resolved %d), state = %d",
			got, want, why, eng.dirtyGen.Load(), eng.settledDirtyGen.Load(), eng.resolvedDirtyGen.Load(), eng.state.Load())
	}
}

// TestWatermarkTwoWritesConverge is the brief's Step 1(a): two writes bump
// the pg counter to N and N+1 in turn; after both have been applied,
// appliedWatermark must equal the pg counter exactly, and watermarkConverged
// must report true.
//
// Each "write" here is exactly the sequence write_observer.go's ensureBumped
// and driver.go's own call sites perform against the real engine primitives
// -- BumpWatermark, then Apply carrying the bumped counter on its scope --
// without needing the root package's *Driver (which would be an import
// cycle from this package): this test exercises the engine-side half of the
// protocol directly, and the root package's own integration suites
// (apply_integration_test.go, staleness_integration_test.go) exercise the
// observer/driver wiring end to end.
func TestWatermarkTwoWritesConverge(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, eng)

	var lastCounter uint64
	for i := 0; i < 2; i++ {
		counter, err := eng.BumpWatermark(ctx)
		if err != nil {
			t.Fatalf("BumpWatermark #%d: %v", i+1, err)
		}
		if want := uint64(i + 1); counter != want {
			t.Fatalf("BumpWatermark #%d = %d, want %d", i+1, counter, want)
		}
		lastCounter = counter

		scope := NewWriteScope()
		scope.SetWatermark(counter)
		eng.Apply(ctx, scope)
	}

	if got := eng.appliedWatermark.Load(); got != lastCounter {
		t.Fatalf("appliedWatermark = %d after two writes, want %d", got, lastCounter)
	}

	pgCounter, err := eng.ReadWatermark(ctx)
	if err != nil {
		t.Fatalf("ReadWatermark: %v", err)
	}
	if pgCounter != lastCounter {
		t.Fatalf("pg counter = %d, want %d", pgCounter, lastCounter)
	}

	if counter, converged := eng.watermarkConverged(ctx); !converged || counter != lastCounter {
		t.Fatalf("watermarkConverged = (%d, %v), want (%d, true)", counter, converged, lastCounter)
	}
}

// TestWatermarkRolledBackWriteTransactionStillConverges is the brief's Step
// 1(b): a WriteTransaction whose delegate writes a node and then returns an
// error rolls back (pg data unchanged), but its eager bump still landed --
// so the pg counter still advances, and the engine still reaches
// convergence once that bump is resolved via AdvanceWatermark (the "empty
// Apply" driver.go's own WriteTransaction error branch performs -- see its
// doc), with no read-back needed at all.
func TestWatermarkRolledBackWriteTransactionStillConverges(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, eng)

	countBefore, err := nodeCount(ctx, pgDriver)
	if err != nil {
		t.Fatalf("count before: %v", err)
	}

	// Simulates write_observer.go's ensureBumped: the observer's first
	// mutating call bumps the counter eagerly, before the transaction's own
	// pg effect.
	counter, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}

	rollbackErr := errors.New("intentional rollback")
	txErr := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("RolledBack")); err != nil {
			return err
		}
		return rollbackErr
	})
	if !errors.Is(txErr, rollbackErr) {
		t.Fatalf("WriteTransaction error = %v, want %v", txErr, rollbackErr)
	}

	// The transaction rolled back: nothing for a read-back to replay, so
	// the error path resolves the bump directly -- exactly what driver.go's
	// own WriteTransaction error branch does via resolveAbandonedWrite.
	eng.AdvanceWatermark(counter)

	countAfter, err := nodeCount(ctx, pgDriver)
	if err != nil {
		t.Fatalf("count after: %v", err)
	}
	if countAfter != countBefore {
		t.Fatalf("node count changed after a rolled-back transaction: before=%d after=%d", countBefore, countAfter)
	}

	pgCounter, err := eng.ReadWatermark(ctx)
	if err != nil {
		t.Fatalf("ReadWatermark: %v", err)
	}
	if pgCounter != counter {
		t.Fatalf("pg counter = %d, want %d (the eager bump still landed despite the rollback)", pgCounter, counter)
	}

	if got, converged := eng.watermarkConverged(ctx); !converged || got != counter {
		t.Fatalf("watermarkConverged = (%d, %v), want (%d, true)", got, converged, counter)
	}
}

// nodeCount returns the total node count in d's default graph, through a
// plain read transaction -- ground truth for
// TestWatermarkRolledBackWriteTransactionStillConverges' "pg data
// unchanged" assertion.
func nodeCount(ctx context.Context, d interface {
	ReadTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error
}) (int64, error) {
	var count int64
	err := d.ReadTransaction(ctx, func(tx graph.Transaction) error {
		c, err := tx.Nodes().Count()
		count = c
		return err
	})
	return count, err
}

// TestWatermarkConcurrentBumpsConverge is the brief's Step 1(c): 10
// goroutines each performing 10 bump+apply cycles concurrently must still
// converge once every goroutine finishes -- the monotonic max-advance
// (AdvanceWatermark) and the inflight counter together are what make
// out-of-order completion safe (see their own docs).
func TestWatermarkConcurrentBumpsConverge(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, eng)

	const (
		goroutines   = 10
		perGoroutine = 10
	)

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perGoroutine)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				counter, err := eng.BumpWatermark(ctx)
				if err != nil {
					errs <- err
					return
				}

				scope := NewWriteScope()
				scope.SetWatermark(counter)
				eng.Apply(ctx, scope)
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("BumpWatermark: %v", err)
	}

	pgCounter, err := eng.ReadWatermark(ctx)
	if err != nil {
		t.Fatalf("ReadWatermark: %v", err)
	}
	if want := uint64(goroutines * perGoroutine); pgCounter != want {
		t.Fatalf("pg counter = %d, want %d", pgCounter, want)
	}

	if got, converged := eng.watermarkConverged(ctx); !converged || got != pgCounter {
		t.Fatalf("watermarkConverged = (%d, %v), want (%d, true)", got, converged, pgCounter)
	}
	if got := eng.inflightBumps.Load(); got != 0 {
		t.Fatalf("inflightBumps = %d after every goroutine finished, want 0", got)
	}
}

// TestWatermarkTrustReturnsOnlyAfterFailureSettlesAndSnapshotAdopts walks
// one watermark failure through its entire life against a real pg watermark
// table, asserting WatermarkTrusted (watermark.go) at each of the four
// moments that matter. Trust is withdrawn the instant the failure is noted,
// and comes back only when BOTH of the things the generations track have
// happened: the failing write's outcome became final in PostgreSQL, and a
// snapshot whose load began after that was actually adopted.
//
// The third step is the one worth reading twice: an adoption that happens
// while the failing write is still in flight resolves nothing, because
// rebuildOnce captures the settled generation BEFORE its load begins, and at
// that moment the failure had not settled.
func TestWatermarkTrustReturnsOnlyAfterFailureSettlesAndSnapshotAdopts(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)

	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, true, "converged and serving, with no watermark failure ever noted")

	// W's eager bump fails; W itself has not even reached PostgreSQL yet.
	scopeW := NewWriteScope()
	eng.NoteWatermarkBumpFailure(ctx, scopeW, errors.New("simulated bump failure"))
	wantTrusted(t, ctx, eng, false, "a bump failure was just noted; its write is still in flight")

	// An adoption while W is still in flight cannot resolve W.
	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, false, "this snapshot's load began before W's failure settled")

	// W lands, and its Apply settles the failure -- carrying the same
	// ChangeSet fallback record ensureBumped (write_observer.go) pairs with
	// every genuine bump failure, so this also trips the engine into
	// fallback.
	scopeW.Changes().RecordFallback("watermark: bump failed: simulated bump failure")
	eng.Apply(ctx, scopeW)
	if got := eng.state.Load(); got != stateFallback {
		t.Fatalf("state = %d after W's Apply saw a ChangeSet fallback record, want stateFallback (%d)", got, stateFallback)
	}
	wantTrusted(t, ctx, eng, false, "W settled, but no adopted snapshot contains it yet")

	// One adoption now does both jobs at once: it ends the fallback and
	// resolves the generation W's failure opened.
	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, true, "an adopted snapshot's load began after W settled")
}

// TestWatermarkTrustWithheldWhileInFallback pins the third condition of the
// predicate on its own: generations fully resolved and counters converged is
// not enough while the replica itself is not trustworthy. state is driven
// directly here (a same-package whitebox test) so the assertion is about the
// predicate rather than about how some write happened to trip fallback.
func TestWatermarkTrustWithheldWhileInFallback(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)

	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, true, "converged and serving")

	eng.state.Store(stateFallback)
	wantTrusted(t, ctx, eng, false, "the replica is in fallback, whatever the generations say")

	eng.state.Store(stateServing)
	wantTrusted(t, ctx, eng, true, "serving again")
}

// TestWatermarkRepeatedBumpFailuresOnOneScopeCountAsOne is F1's regression,
// exercised against WatermarkTrusted's full, live-pg predicate (rather than
// watermark_test.go's TestNoteWatermarkBumpFailureCountsOncePerScopeAcrossRepeatedRetries,
// which pins the same generation algebra without a database): a write scope
// whose eager bump fails on TWO of its own mutating calls in a row -- the
// shape a transaction produces once its first bump fails, since every later
// call on that same scope retries the identical failing bump (ensureBumped's
// own doc) -- must open exactly one generation, settle with exactly one
// Apply, and become trusted again with exactly one adoption. Before this
// fix, the second call's extra dirtyGen increment could never be matched by
// a second settle (the scope's mark only ever consumes once), so
// WatermarkTrusted would stay false no matter how many snapshots were
// adopted afterwards.
func TestWatermarkRepeatedBumpFailuresOnOneScopeCountAsOne(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)

	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, true, "converged and serving before the write starts")

	// Two failing mutating calls against the same scope.
	scope := NewWriteScope()
	eng.NoteWatermarkBumpFailure(ctx, scope, errors.New("simulated bump failure 1"))
	eng.NoteWatermarkBumpFailure(ctx, scope, errors.New("simulated bump failure 2"))

	if got, settled := eng.dirtyGen.Load(), eng.settledDirtyGen.Load(); got != 1 || settled != 0 {
		t.Fatalf("generations = (dirty %d, settled %d) after two failing calls on one scope, want (1, 0): the second call must not open a second generation", got, settled)
	}
	wantTrusted(t, ctx, eng, false, "an unsettled failure was noted")

	// The write lands anyway, carrying the one fallback record ensureBumped
	// records only for the scope's FIRST failure (write_observer.go's own
	// doc).
	scope.Changes().RecordFallback("watermark: bump failed: simulated bump failure 1")
	eng.Apply(ctx, scope)
	if got, settled := eng.dirtyGen.Load(), eng.settledDirtyGen.Load(); got != 1 || settled != 1 {
		t.Fatalf("generations = (dirty %d, settled %d) after Apply, want (1, 1): one Apply must settle both failing calls' shared generation", got, settled)
	}
	wantTrusted(t, ctx, eng, false, "settled, but no adopted snapshot contains it yet")

	// One adoption resolves it -- recoverable, exactly as a single bump
	// failure would be.
	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, true, "the shared generation settled once, and one adoption resolved it")
}

// TestWatermarkTrustNotRestoredByAConcurrentWriteResolving is the direct
// regression test for the wrong-trust race that a clearable dirty flag could
// not close, whatever it was gated on.
//
// The race: write W's eager bump fails, which is noted BEFORE W's own pg
// effect is even attempted -- so at that instant W has left no trace anywhere
// else in the engine (no counter, nothing in flight, and its Apply, the thing
// that would trip fallback, has not run). A concurrent write C, whose own
// bump succeeded, then resolves completely: it observes a fully converged
// engine in stateServing and, under the old design, cleared the dirty flag on
// exactly that evidence -- while W was still in flight.
//
// This test reproduces that interleaving deterministically (C's whole
// lifecycle runs between W's failure and W's Apply) and asserts that both
// conditions the old rule checked really do hold at that moment, and that
// WatermarkTrusted still reports false anyway -- because trust is computed
// from the generations, and W's generation is not settled, so there is
// nothing for C to clear.
func TestWatermarkTrustNotRestoredByAConcurrentWriteResolving(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()
	resetWatermarkTable(t, ctx, eng)
	parkRebuildLoop(eng)

	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, true, "converged and serving before either write starts")

	// W: the eager bump fails. W's own write and Apply are still to come.
	scopeW := NewWriteScope()
	eng.NoteWatermarkBumpFailure(ctx, scopeW, errors.New("simulated bump failure"))

	// C: a concurrent write whose bump succeeded, resolved end to end.
	counterC, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark (C): %v", err)
	}
	scopeC := NewWriteScope()
	scopeC.SetWatermark(counterC)
	eng.Apply(ctx, scopeC)

	// Exactly the evidence the old clear-on-(converged && serving) rule
	// accepted, asserted here so this test would fail loudly if the scenario
	// ever stopped reproducing the race rather than silently passing.
	if _, converged := eng.watermarkConverged(ctx); !converged {
		t.Fatalf("watermarkConverged = false after C resolved, want true: this test only reproduces the race if C sees a converged engine")
	}
	if got := eng.state.Load(); got != stateServing {
		t.Fatalf("state = %d after C resolved, want stateServing (%d): W's Apply must not have run yet for this test to reproduce the race", got, stateServing)
	}

	wantTrusted(t, ctx, eng, false, "C1: W's bump failure has not settled, so C's own resolution proves nothing about W")

	// W's Apply finally runs, and the recovery it triggers restores trust --
	// the failure's generation settles, then an adopted snapshot resolves it.
	scopeW.Changes().RecordFallback("watermark: bump failed: simulated bump failure")
	eng.Apply(ctx, scopeW)
	wantTrusted(t, ctx, eng, false, "W settled, but no adopted snapshot contains it yet")

	adoptOneRebuild(t, ctx, eng)
	wantTrusted(t, ctx, eng, true, "an adopted snapshot's load began after W settled")
}

// unreachableEnginePool returns a *pgxpool.Pool pointed at 127.0.0.1 on a low
// port nothing listens on, mirroring the root package's own
// unreachablePGDriver (wrapper_test.go) but scoped to just the pool:
// BumpWatermark/ensureWatermarkTable/LoadSnapshot all reach PostgreSQL
// through e.pool alone, so handing an Engine one of these while its pgDriver
// stays real and reachable (TestWatermarkGenuineBumpFailureSetsDirtyAndFallsBack,
// below) is what lets a test drive a GENUINE bump failure -- a real network
// error from e.pool.QueryRow -- rather than the nil-pool
// ErrWatermarkUnavailable short-circuit every other bump-failure-adjacent
// test in this file exercises.
func unreachableEnginePool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), "postgres://bloodtrail:bloodtrail@127.0.0.1:1/bloodtrail")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// TestWatermarkGenuineBumpFailureOpensAGenerationAndFallsBack is the real,
// live-pool counterpart to watermark_test.go's
// TestNoteWatermarkBumpFailureOpensGenerationAndMarksScope (which pins
// NoteWatermarkBumpFailure in isolation) and to TestDriverMutatingCapabilityMethodsDoNotCallApplyOnError
// (wrapper_test.go), whose own disabledEngine() gives the ENGINE a nil pool
// -- so every bump attempt there returns ErrWatermarkUnavailable without
// ever reaching a network at all, never the genuine-failure path this test
// exercises.
//
// This engine's own pool is a real, live *pgxpool.Pool pointed at an address
// nothing listens on (unreachableEnginePool), while pgDriver/pool (from
// graphtest.OpenPG) are real and reachable -- the same split production
// always has (the engine's watermark pool and the driver's graph-write pool
// are never the same connection by construction), which is what lets
// BumpWatermark's own UPDATE genuinely fail here while the write it guards
// still lands.
//
// Asserts, in order, exactly what ensureBumped's genuine-failure branch
// (write_observer.go) and Apply's own cs.HasFallback() branch (apply.go)
// together promise: the bump failure never blocks the write (the node is
// created), the failure opens a watermark trust generation that stays
// unsettled while the write is in flight, and Apply -- seeing the ChangeSet
// fallback record ensureBumped itself would have recorded for this same
// error -- settles that generation and trips the engine into fallback.
func TestWatermarkGenuineBumpFailureOpensAGenerationAndFallsBack(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, _ := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	eng := New(pgDriver, unreachableEnginePool(t), Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()
	parkRebuildLoop(eng)

	countBefore, err := nodeCount(ctx, pgDriver)
	if err != nil {
		t.Fatalf("count before: %v", err)
	}

	// Simulates write_observer.go's ensureBumped: attempt the eager bump
	// first, exactly as every real mutating call does.
	_, bumpErr := eng.BumpWatermark(ctx)
	if bumpErr == nil {
		t.Fatalf("BumpWatermark against an unreachable pool succeeded, want a genuine error")
	}
	if errors.Is(bumpErr, ErrWatermarkUnavailable) {
		t.Fatalf("BumpWatermark = %v, want a real network error (this engine's own pool is non-nil, just unreachable), not ErrWatermarkUnavailable", bumpErr)
	}
	// The scope this failure is recorded against is the write's own, exactly
	// as ensureBumped (write_observer.go) hands it its own scope: the mark it
	// leaves there is the only thing that can ever settle the generation the
	// failure opens.
	scope := NewWriteScope()
	eng.NoteWatermarkBumpFailure(ctx, scope, bumpErr)

	if got, settled := eng.dirtyGen.Load(), eng.settledDirtyGen.Load(); got != 1 || settled != 0 {
		t.Fatalf("generations = (dirty %d, settled %d) after a genuine bump failure, want (1, 0)", got, settled)
	}
	if eng.WatermarkTrusted(ctx) {
		t.Fatalf("WatermarkTrusted = true after a genuine bump failure, want false")
	}

	// The write proceeds regardless -- through the real, reachable pgDriver
	// -- carrying the same fallback record ensureBumped itself records for
	// this exact error (its literal text is reproduced here since this file
	// cannot import the root package's write_observer.go without an import
	// cycle -- see this package's own doc on that constraint, e.g.
	// engine.go's ApplyCount doc).
	scope.Changes().RecordFallback(fmt.Sprintf("watermark: bump failed: %v", bumpErr))

	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("BumpFailureProbe"))
		if err != nil {
			return err
		}
		scope.Changes().RecordNodeID(n.ID)
		return nil
	}); err != nil {
		t.Fatalf("WriteTransaction (a bump failure must never block the write it guards): %v", err)
	}

	countAfter, err := nodeCount(ctx, pgDriver)
	if err != nil {
		t.Fatalf("count after: %v", err)
	}
	if countAfter != countBefore+1 {
		t.Fatalf("node count = %d after the write, want %d -- the write must have proceeded despite the bump failure", countAfter, countBefore+1)
	}

	eng.Apply(ctx, scope)

	if got, settled := eng.dirtyGen.Load(), eng.settledDirtyGen.Load(); got != 1 || settled != 1 {
		t.Fatalf("generations = (dirty %d, settled %d) after the failing write's Apply, want (1, 1): the write has landed, so its failure has settled", got, settled)
	}
	if eng.resolvedDirtyGen.Load() != 0 {
		t.Fatalf("resolvedDirtyGen = %d with no snapshot adopted since the failure settled, want 0", eng.resolvedDirtyGen.Load())
	}
	if eng.WatermarkTrusted(ctx) {
		t.Fatalf("WatermarkTrusted = true after the failing write landed, want false: no adopted snapshot contains it yet")
	}
	if got := eng.state.Load(); got != stateFallback {
		t.Fatalf("state = %d after Apply saw a ChangeSet fallback record, want stateFallback (%d)", got, stateFallback)
	}
	if _, serving := eng.Fresh(); serving {
		t.Fatalf("Fresh() reports serving after a genuine bump failure's write, want fallback")
	}
}

// TestEnsureWatermarkTableSelfSettlesOnLiveFailure is
// TestEnsureWatermarkTableIsNoOpWithNoPool's (watermark_test.go) live-pool
// counterpart: a nil pool is a deliberate no-op (ensureWatermarkTable's own
// doc), but a real, non-nil pool that genuinely fails the DDL exec -- here,
// because nothing is listening on the other end -- must still withdraw trust.
// It is the one failure shape that settles itself immediately
// (noteSelfSettlingWatermarkFailure: a failed DDL exec guarded no write, so
// no write's settling could ever retire it), leaving the next adopted
// snapshot to resolve it -- at boot, Start's own boot load. No live database
// is actually required for this one: pgxpool.New itself never blocks on a
// connection, so the unreachable pool is enough on its own, without
// graphtest.PGAvailable's own live-DB skip guard.
func TestEnsureWatermarkTableSelfSettlesOnLiveFailure(t *testing.T) {
	ctx := context.Background()
	eng := New(nil, unreachableEnginePool(t), Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()

	eng.ensureWatermarkTable(ctx)

	if dirty, settled, resolved := eng.dirtyGen.Load(), eng.settledDirtyGen.Load(), eng.resolvedDirtyGen.Load(); dirty != 1 || settled != 1 || resolved != 0 {
		t.Fatalf("generations = (dirty %d, settled %d, resolved %d) after a failed DDL exec, want (1, 1, 0)", dirty, settled, resolved)
	}
	if eng.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = true with no snapshot adopted since the DDL failure, want false")
	}
	if eng.WatermarkTrusted(ctx) {
		t.Fatalf("WatermarkTrusted = true after a failed watermark-table DDL exec, want false")
	}
}
