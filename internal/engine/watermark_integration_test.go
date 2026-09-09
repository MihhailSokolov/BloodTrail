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
	eng.watermarkDirty.Store(false)
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
	// own WriteTransaction error branch does via advanceIfBumped.
	eng.AdvanceWatermark(ctx, counter)

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

// TestWatermarkDirtyClearsOnLaterConvergedBump exercises watermarkDirty's
// own recovery rule against a real pg watermark table: once set (standing
// in for an earlier bump that genuinely failed -- NoteWatermarkBumpFailure,
// unit-tested against a nil pool in watermark_test.go), it must stay set
// through a bump+apply pair that does NOT reach convergence, and clear the
// moment one does -- AdvanceWatermark's own documented "later successful
// bump+apply pair" recovery.
func TestWatermarkDirtyClearsOnLaterConvergedBump(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, eng)

	eng.watermarkDirty.Store(true)

	// A bump whose own AdvanceWatermark call is deliberately NOT made yet:
	// inflightBumps stays at 1, so watermarkConverged can't report true,
	// and dirty must stay set.
	counter1, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark #1: %v", err)
	}
	if _, converged := eng.watermarkConverged(ctx); converged {
		t.Fatalf("watermarkConverged = true with an unresolved bump in flight, want false")
	}

	// A second bump, resolved immediately: AdvanceWatermark's own re-check
	// still sees inflightBumps == 1 (counter1's own bump is still
	// unresolved), so dirty must still not clear.
	counter2, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark #2: %v", err)
	}
	eng.AdvanceWatermark(ctx, counter2)
	if !eng.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty cleared while counter1's own bump is still unresolved, want it to stay set")
	}

	// Resolving counter1 now retires the last unresolved bump: convergence
	// is reachable, so this AdvanceWatermark call must clear dirty.
	eng.AdvanceWatermark(ctx, counter1)
	if eng.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty stayed set after every bumped scope resolved and the engine reached convergence, want it cleared")
	}

	if got, converged := eng.watermarkConverged(ctx); !converged || got != counter2 {
		t.Fatalf("watermarkConverged = (%d, %v), want (%d, true)", got, converged, counter2)
	}
}

// TestWatermarkDirtyStaysSetUntilStateReturnsToServing pins C1's own fix
// directly against a real pg watermark table: recheckWatermarkDirty
// (watermark.go) must not clear watermarkDirty on watermarkConverged alone
// while the engine is still in stateFallback -- see that method's own doc
// for the full soundness argument this exercises. e.state is manipulated
// directly here (a same-package whitebox test, mirroring
// TestWatermarkDirtyClearsOnLaterConvergedBump's own direct
// watermarkDirty.Store(true) setup, above) rather than through a real
// enterFallback trip, so the scenario is deterministic and does not race a
// real background rebuild goroutine --
// TestWatermarkGenuineBumpFailureSetsDirtyAndFallsBack, below, already
// proves enterFallback itself flips state correctly end to end; this test's
// own job is just the recheck's gating logic once state is whatever it is.
func TestWatermarkDirtyStaysSetUntilStateReturnsToServing(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	resetWatermarkTable(t, ctx, eng)

	eng.watermarkDirty.Store(true)
	eng.state.Store(stateFallback) // standing in for an unresolved fallback trip

	// A bump+resolve cycle that reaches convergence on the counters alone:
	// pg's counter and appliedWatermark agree, and nothing is left in
	// flight -- exactly the state a write with zero counter trace (C1's own
	// scenario) leaves behind for some LATER, unrelated write to walk into.
	counter, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark: %v", err)
	}
	eng.AdvanceWatermark(ctx, counter)

	if _, converged := eng.watermarkConverged(ctx); !converged {
		t.Fatalf("watermarkConverged = false, want true: this bump was the only one outstanding")
	}
	if !eng.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty cleared while state is still stateFallback, want it to stay set (C1: convergence on the counters alone does not prove the fallback rebuild that resolves the missed write has been adopted)")
	}

	// The rebuild that resolved this fallback trip has now adopted.
	eng.state.Store(stateServing)

	// The next bump+resolve cycle's recheck now sees both halves hold.
	counter2, err := eng.BumpWatermark(ctx)
	if err != nil {
		t.Fatalf("BumpWatermark #2: %v", err)
	}
	eng.AdvanceWatermark(ctx, counter2)

	if eng.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty stayed set once state returned to stateServing and the engine converged, want it cleared")
	}
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

// TestWatermarkGenuineBumpFailureSetsDirtyAndFallsBack is the real,
// live-pool counterpart to watermark_test.go's
// TestNoteWatermarkBumpFailureSetsDirty (which pins NoteWatermarkBumpFailure
// in isolation) and to TestDriverMutatingCapabilityMethodsDoNotCallApplyOnError
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
// created), watermarkDirty is set, and Apply -- seeing the ChangeSet
// fallback record ensureBumped itself would have recorded for this same
// error -- trips the engine into fallback.
func TestWatermarkGenuineBumpFailureSetsDirtyAndFallsBack(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, _ := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	eng := New(pgDriver, unreachableEnginePool(t), Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()

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
	eng.NoteWatermarkBumpFailure(ctx, bumpErr)

	if !eng.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty = false after a genuine bump failure, want true")
	}

	// The write proceeds regardless -- through the real, reachable pgDriver
	// -- carrying the same fallback record ensureBumped itself records for
	// this exact error (its literal text is reproduced here since this file
	// cannot import the root package's write_observer.go without an import
	// cycle -- see this package's own doc on that constraint, e.g.
	// engine.go's ApplyCount doc).
	scope := NewWriteScope()
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

	if !eng.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty cleared, want it to stay set: this scope never bumped, so its own Apply call cannot resolve it")
	}
	if got := eng.state.Load(); got != stateFallback {
		t.Fatalf("state = %d after Apply saw a ChangeSet fallback record, want stateFallback (%d)", got, stateFallback)
	}
	if _, serving := eng.Fresh(); serving {
		t.Fatalf("Fresh() reports serving after a genuine bump failure's write, want fallback")
	}
}

// TestEnsureWatermarkTableSetsDirtyOnLiveFailure is
// TestEnsureWatermarkTableIsNoOpWithNoPool's (watermark_test.go) live-pool
// counterpart: a nil pool is a deliberate no-op (ensureWatermarkTable's own
// doc), but a real, non-nil pool that genuinely fails the DDL exec -- here,
// because nothing is listening on the other end -- must still set
// watermarkDirty, exactly as any other genuine watermark failure does. No
// live database is actually required for this one: pgxpool.New itself never
// blocks on a connection, so the unreachable pool is enough on its own,
// without graphtest.PGAvailable's own live-DB skip guard.
func TestEnsureWatermarkTableSetsDirtyOnLiveFailure(t *testing.T) {
	eng := New(nil, unreachableEnginePool(t), Config{Enabled: true, Log: testEngineLogger()})
	defer eng.Stop()

	eng.ensureWatermarkTable(context.Background())

	if !eng.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty = false after ensureWatermarkTable failed against an unreachable pool, want true")
	}
}
