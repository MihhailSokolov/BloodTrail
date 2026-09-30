// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file drives the driver's write path -- the observing wrappers
// (write_observer.go), the read-transaction write accounting
// (transaction.go) and the Driver entry points themselves (driver.go) --
// through shapes whose effect on PostgreSQL is easy to get wrong: criteria
// the change log must record exactly, writes hidden in a query's final
// criteria, flushes that fail and are retried, and delegates that panic.
// Every test compares the replica's answer with PostgreSQL's own, read
// through the plain pg driver.

package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// openWritePathDriver is openApplyDriver plus the plain pg driver (the
// oracle every test here compares against) and the shared pool (for
// installing test triggers).
func openWritePathDriver(t *testing.T) (*bloodtrail.Driver, graph.Database, *pg.Driver, *pgxpool.Pool, *lockedBuffer, context.Context) {
	t.Helper()

	dsn := graphtest.PGAvailable(t)
	buf := installLogCapture(t)
	ctx := context.Background()

	oracle, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, oracle)

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })

	d, ok := bt.(*bloodtrail.Driver)
	if !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", bt)
	}
	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	waitForBootLoad(t, d)

	return d, bt, oracle, pool, buf, ctx
}

// TestRelationshipDeleteByConjoinedKindMatchersMatchesPostgres covers a
// relationship Delete whose criteria ANDs KindMatchers over the
// relationship. dawgs translates an edge KindMatcher as
// `kind_id = any(<ids>)`, so a conjunction deletes only the edges whose
// kind is in EVERY list -- the intersection, empty whenever two lists are
// disjoint or one is empty. The change log used to record the UNION, and
// the applier then tombstoned every edge of every listed kind while
// PostgreSQL kept them.
func TestRelationshipDeleteByConjoinedKindMatchersMatchesPostgres(t *testing.T) {
	var (
		nodeKind = graph.StringKind("WritePathConjDeleteNode")
		kindA    = graph.StringKind("WritePathConjDeleteA")
		kindB    = graph.StringKind("WritePathConjDeleteB")
	)

	cases := []struct {
		name         string
		criteria     graph.Criteria
		wantA, wantB int64
	}{
		{
			name:     "disjoint kinds delete nothing",
			criteria: query.And(query.Kind(query.Relationship(), kindA), query.Kind(query.Relationship(), kindB)),
			wantA:    1, wantB: 1,
		},
		{
			name:     "overlapping kind lists delete only the shared kind",
			criteria: query.And(query.KindIn(query.Relationship(), kindA, kindB), query.KindIn(query.Relationship(), kindB)),
			wantA:    1, wantB: 0,
		},
		{
			name:     "an empty kind list in the conjunction deletes nothing",
			criteria: query.And(query.KindIn(query.Relationship(), kindA), query.KindIn(query.Relationship())),
			wantA:    1, wantB: 1,
		},
		{
			name:     "a lone empty kind list deletes nothing",
			criteria: query.KindIn(query.Relationship()),
			wantA:    1, wantB: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, bt, oracle, _, buf, ctx := openWritePathDriver(t)

			if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
				a, err := tx.CreateNode(graph.NewProperties(), nodeKind)
				if err != nil {
					return err
				}
				b, err := tx.CreateNode(graph.NewProperties(), nodeKind)
				if err != nil {
					return err
				}
				if _, err := tx.CreateRelationshipByIDs(a.ID, b.ID, kindA, graph.NewProperties()); err != nil {
					return err
				}
				_, err = tx.CreateRelationshipByIDs(b.ID, a.ID, kindB, graph.NewProperties())
				return err
			}); err != nil {
				t.Fatalf("fixture: %v", err)
			}
			requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the kind A edge serves",
				func() int64 { return relCountByKind(t, ctx, bt, kindA) }, 1)

			if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
				return tx.Relationships().Filter(tc.criteria).Delete()
			}); err != nil {
				t.Fatalf("delete: %v", err)
			}

			if gotA, gotB := relCountByKind(t, ctx, oracle, kindA), relCountByKind(t, ctx, oracle, kindB); gotA != tc.wantA || gotB != tc.wantB {
				t.Fatalf("postgresql after the delete: A=%d B=%d, want A=%d B=%d", gotA, gotB, tc.wantA, tc.wantB)
			}
			requireMarkerDelta(t, buf, builderServedMarker, 1, "served kind A count matches postgresql",
				func() int64 { return relCountByKind(t, ctx, bt, kindA) }, tc.wantA)
			requireMarkerDelta(t, buf, builderServedMarker, 1, "served kind B count matches postgresql",
				func() int64 { return relCountByKind(t, ctx, bt, kindB) }, tc.wantB)
			assertNoFallback(t, buf)
		})
	}
}

// writePathQuerySource is what a graph.Transaction and a graph.Batch both
// offer: query builders bound to the write in progress.
type writePathQuerySource interface {
	Nodes() graph.NodeQuery
	Relationships() graph.RelationshipQuery
}

// TestUpdatingClauseInFinalCriteriaReachesTheReplica covers
// NodeQuery.Query/Fetch and RelationshipQuery.Query given an updating
// clause in finalCriteria -- exactly how the pg driver's own Delete and
// Update run (liveQuery.exec) -- through every entry point that can write.
// Nothing tells the replica which rows such a call touched, so it must
// bump the watermark and fall back: the answer afterwards must match
// PostgreSQL's, and a ReadTransaction that wrote this way must answer its
// own later reads from PostgreSQL, not from the pre-write replica.
func TestUpdatingClauseInFinalCriteriaReachesTheReplica(t *testing.T) {
	var (
		nodeKind = graph.StringKind("WritePathFinalCriteriaNode")
		edgeKind = graph.StringKind("WritePathFinalCriteriaEdge")
	)

	type method struct {
		name string
		// write runs the updating call against src.
		write func(src writePathQuerySource) error
		// inTxCount reads the count the write changes, through the
		// transaction that wrote it.
		inTxCount func(src writePathQuerySource) (int64, error)
		// count reads the same count through db.
		count func(t *testing.T, ctx context.Context, db graph.Database) int64
	}
	methods := []method{
		{
			name: "NodeQuery.Query",
			write: func(src writePathQuerySource) error {
				return src.Nodes().Filter(query.Kind(query.Node(), nodeKind)).Query(func(result graph.Result) error {
					return result.Error()
				}, query.Delete(query.Node()))
			},
			inTxCount: func(src writePathQuerySource) (int64, error) {
				return src.Nodes().Filter(query.Kind(query.Node(), nodeKind)).Count()
			},
			count: func(t *testing.T, ctx context.Context, db graph.Database) int64 {
				return nodeCountByKind(t, ctx, db, nodeKind)
			},
		},
		{
			name: "NodeQuery.Fetch",
			write: func(src writePathQuerySource) error {
				return src.Nodes().Filter(query.Kind(query.Node(), nodeKind)).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
					for range cursor.Chan() {
					}
					return cursor.Error()
				}, query.Delete(query.Node()))
			},
			inTxCount: func(src writePathQuerySource) (int64, error) {
				return src.Nodes().Filter(query.Kind(query.Node(), nodeKind)).Count()
			},
			count: func(t *testing.T, ctx context.Context, db graph.Database) int64 {
				return nodeCountByKind(t, ctx, db, nodeKind)
			},
		},
		{
			name: "RelationshipQuery.Query",
			write: func(src writePathQuerySource) error {
				return src.Relationships().Filter(query.Kind(query.Relationship(), edgeKind)).Query(func(result graph.Result) error {
					return result.Error()
				}, query.Delete(query.Relationship()))
			},
			inTxCount: func(src writePathQuerySource) (int64, error) {
				return src.Relationships().Filter(query.Kind(query.Relationship(), edgeKind)).Count()
			},
			count: func(t *testing.T, ctx context.Context, db graph.Database) int64 {
				return relCountByKind(t, ctx, db, edgeKind)
			},
		},
	}

	type entryPoint struct {
		name string
		// run performs m.write and, for a ReadTransaction, returns the count
		// m.inTxCount read inside the same call afterwards (-1 otherwise).
		run func(ctx context.Context, bt graph.Database, m method) (int64, error)
	}
	entryPoints := []entryPoint{
		{
			name: "WriteTransaction",
			run: func(ctx context.Context, bt graph.Database, m method) (int64, error) {
				return -1, bt.WriteTransaction(ctx, func(tx graph.Transaction) error { return m.write(tx) })
			},
		},
		{
			name: "ReadTransaction",
			run: func(ctx context.Context, bt graph.Database, m method) (int64, error) {
				inTx := int64(-1)
				err := bt.ReadTransaction(ctx, func(tx graph.Transaction) error {
					if err := m.write(tx); err != nil {
						return err
					}
					n, err := m.inTxCount(tx)
					inTx = n
					return err
				})
				return inTx, err
			},
		},
		{
			name: "BatchOperation",
			run: func(ctx context.Context, bt graph.Database, m method) (int64, error) {
				return -1, bt.BatchOperation(ctx, func(b graph.Batch) error { return m.write(b) })
			},
		},
	}

	for _, ep := range entryPoints {
		for _, m := range methods {
			t.Run(ep.name+"/"+m.name, func(t *testing.T) {
				d, bt, oracle, _, buf, ctx := openWritePathDriver(t)

				if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
					a, err := tx.CreateNode(graph.NewProperties(), nodeKind)
					if err != nil {
						return err
					}
					b, err := tx.CreateNode(graph.NewProperties(), nodeKind)
					if err != nil {
						return err
					}
					_, err = tx.CreateRelationshipByIDs(a.ID, b.ID, edgeKind, graph.NewProperties())
					return err
				}); err != nil {
					t.Fatalf("fixture: %v", err)
				}
				requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the nodes serve",
					func() int64 { return nodeCountByKind(t, ctx, bt, nodeKind) }, 2)

				counterBefore, err := bloodtrail.TestingEngine(d).ReadWatermark(ctx)
				if err != nil {
					t.Fatalf("ReadWatermark: %v", err)
				}
				fallbacksBefore := markerCount(buf, fallbackEnteredMarker)

				inTx, err := ep.run(ctx, bt, m)
				if err != nil {
					t.Fatalf("write: %v", err)
				}

				if pgCount := m.count(t, ctx, oracle); pgCount != 0 {
					t.Fatalf("postgresql still counts %d: the updating clause did not run", pgCount)
				}
				if inTx > 0 {
					t.Fatalf("the writing ReadTransaction read back %d, postgresql holds 0: its own read was served from the pre-write replica", inTx)
				}
				wantNodes, wantEdges := nodeCountByKind(t, ctx, oracle, nodeKind), relCountByKind(t, ctx, oracle, edgeKind)
				if gotNodes, gotEdges := nodeCountByKind(t, ctx, bt, nodeKind), relCountByKind(t, ctx, bt, edgeKind); gotNodes != wantNodes || gotEdges != wantEdges {
					t.Fatalf("WRONG ANSWER after the write: bloodtrail nodes=%d edges=%d, postgresql nodes=%d edges=%d", gotNodes, gotEdges, wantNodes, wantEdges)
				}
				if delta := markerCount(buf, fallbackEnteredMarker) - fallbacksBefore; delta != 1 {
					t.Fatalf("fallback entered %d time(s) for the write, want 1", delta)
				}
				counterAfter, err := bloodtrail.TestingEngine(d).ReadWatermark(ctx)
				if err != nil {
					t.Fatalf("ReadWatermark: %v", err)
				}
				if counterAfter != counterBefore+1 {
					t.Fatalf("watermark counter %d -> %d across the write, want exactly one bump", counterBefore, counterAfter)
				}
			})
		}
	}
}

// writePathFailProperty is the property key installWritePathInsertFailure's
// trigger looks for.
const writePathFailProperty = "writepath_fail"

// installWritePathInsertFailure makes every node INSERT whose properties
// carry writePathFailProperty fail until the returned function is called:
// a stand-in for a transient flush failure (a deadlock, a lock or statement
// timeout, a dropped connection) that the next attempt survives. The
// trigger, its function and its flag table are dropped when the test ends.
func installWritePathInsertFailure(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (pass func()) {
	t.Helper()

	statements := []string{
		`create table if not exists writepath_fail_flag (id int primary key, fail boolean not null)`,
		`insert into writepath_fail_flag values (1, true) on conflict (id) do update set fail = true`,
		`create or replace function writepath_fail() returns trigger language plpgsql as $$
begin
  if new.properties ? '` + writePathFailProperty + `' and exists (select 1 from writepath_fail_flag where id = 1 and fail) then
    raise exception 'writepath: injected insert failure';
  end if;
  return new;
end $$`,
		`drop trigger if exists writepath_fail_trg on node`,
		`create trigger writepath_fail_trg before insert on node for each row execute function writepath_fail()`,
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		for _, statement := range []string{
			`drop trigger if exists writepath_fail_trg on node`,
			`drop function if exists writepath_fail()`,
			`drop table if exists writepath_fail_flag`,
		} {
			if _, err := pool.Exec(cleanupCtx, statement); err != nil {
				t.Errorf("drop injected insert failure: %s: %v", statement, err)
			}
		}
	})
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("install injected insert failure: %s: %v", statement, err)
		}
	}

	return func() {
		if _, err := pool.Exec(ctx, `update writepath_fail_flag set fail = false where id = 1`); err != nil {
			t.Fatalf("let inserts pass again: %v", err)
		}
	}
}

// writePathFailingNode is a batch-create node, identified by objectID the
// way BloodHound's ingest identifies nodes, whose INSERT
// installWritePathInsertFailure's trigger fails.
func writePathFailingNode(objectID string, kind graph.Kind) *graph.Node {
	return graph.PrepareNode(graph.NewProperties().Set("objectid", objectID).Set(writePathFailProperty, true), kind)
}

// seedWritePathAnchor creates one node of kind through bt, so the kind
// exists and serves before a test's own writes.
func seedWritePathAnchor(t *testing.T, ctx context.Context, bt graph.Database, kind graph.Kind) {
	t.Helper()
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("objectid", "WRITEPATH-ANCHOR-"+kind.String()), kind)
		return err
	}); err != nil {
		t.Fatalf("seed anchor: %v", err)
	}
}

// TestBatchCommitFailureKeepsTheBufferedWrites covers a mid-batch Commit
// whose flush fails. The pg batch keeps the failed buffer, and every later
// one, and its final Commit flushes them once the delegate returns nil --
// so the keys recorded for those writes must survive the failed Commit for
// the final Apply to read them back.
func TestBatchCommitFailureKeepsTheBufferedWrites(t *testing.T) {
	_, bt, oracle, pool, buf, ctx := openWritePathDriver(t)
	kind := graph.StringKind("WritePathCommitRetryNode")

	seedWritePathAnchor(t, ctx, bt, kind)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the anchor serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, 1)
	passInserts := installWritePathInsertFailure(t, ctx, pool)

	var commitErr error
	if err := bt.BatchOperation(ctx, func(b graph.Batch) error {
		if err := b.CreateNode(writePathFailingNode("WRITEPATH-COMMIT-RETRY", kind)); err != nil {
			return err
		}
		commitErr = b.Commit()
		passInserts()
		return nil
	}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if commitErr == nil {
		t.Fatalf("the mid-batch Commit succeeded: the injected failure did not fire")
	}

	if got := nodeCountByKind(t, ctx, oracle, kind); got != 2 {
		t.Fatalf("postgresql holds %d node(s), want 2: the final Commit did not flush the retried create", got)
	}
	requireMarkerDelta(t, buf, builderServedMarker, 1, "the create the final Commit flushed serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, 2)
	assertNoFallback(t, buf)
}

// TestBatchCreateNodeFlushFailureStillServesTheNode covers a batch
// CreateNode whose flush fails. The pg batch appends the node to its buffer
// before flushing -- and the failure can come from any buffer, not only
// this node's -- so the node stays buffered and a later flush (here the
// batch's final Commit) creates it: it must have been recorded all the
// same.
func TestBatchCreateNodeFlushFailureStillServesTheNode(t *testing.T) {
	_, bt, oracle, pool, buf, ctx := openWritePathDriver(t)
	kind := graph.StringKind("WritePathCreateRetryNode")

	seedWritePathAnchor(t, ctx, bt, kind)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the anchor serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, 1)
	passInserts := installWritePathInsertFailure(t, ctx, pool)

	var createErr error
	if err := bt.BatchOperation(ctx, func(b graph.Batch) error {
		if err := b.CreateNode(writePathFailingNode("WRITEPATH-CREATE-RETRY-A", kind)); err != nil {
			return err
		}
		// The second create overflows the batch size of 1, so this call
		// flushes both creates, and the flush fails.
		createErr = b.CreateNode(writePathFailingNode("WRITEPATH-CREATE-RETRY-B", kind))
		passInserts()
		return nil
	}, graph.WithBatchSize(1)); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if createErr == nil {
		t.Fatalf("the overflowing CreateNode succeeded: the injected failure did not fire")
	}

	if got := nodeCountByKind(t, ctx, oracle, kind); got != 3 {
		t.Fatalf("postgresql holds %d node(s), want 3: the final Commit did not flush both creates", got)
	}
	requireMarkerDelta(t, buf, builderServedMarker, 1, "both creates the final Commit flushed serve",
		func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, 3)
	assertNoFallback(t, buf)
}

// writePathDelegatePanic is the value the panic tests' delegates panic
// with; it must reach the caller unchanged.
const writePathDelegatePanic = "writepath: delegate bug"

// recoverWritePathPanic runs run and returns what it panicked with (nil if
// it did not), standing in for the recovery middleware upstream code runs
// under.
func recoverWritePathPanic(run func()) (recovered any) {
	defer func() { recovered = recover() }()
	run()
	return nil
}

// requireWritePathConverged requires that the engine's watermark
// bookkeeping has caught up with PostgreSQL's counter: every bumped write
// resolved, so a snapshot could be saved.
func requireWritePathConverged(t *testing.T, ctx context.Context, d *bloodtrail.Driver, label string) {
	t.Helper()
	if counter, converged := bloodtrail.TestingEngine(d).WatermarkConverged(ctx); !converged {
		t.Fatalf("%s: watermark not converged at counter %d: a bumped write was never resolved", label, counter)
	}
}

// TestBatchOperationPanicAfterAFlushStillReachesTheReplica covers a batch
// delegate that panics after a flush: the flushed rows are durable (the pg
// batch autocommits each flush), so the replica must still learn of them,
// and the batch's watermark bump must still be resolved, while the panic
// itself reaches the caller unchanged.
func TestBatchOperationPanicAfterAFlushStillReachesTheReplica(t *testing.T) {
	d, bt, oracle, _, buf, ctx := openWritePathDriver(t)
	kind := graph.StringKind("WritePathBatchPanicNode")

	seedWritePathAnchor(t, ctx, bt, kind)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the anchor serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, 1)

	recovered := recoverWritePathPanic(func() {
		_ = bt.BatchOperation(ctx, func(b graph.Batch) error {
			if err := b.CreateNode(graph.PrepareNode(graph.NewProperties().Set("objectid", "WRITEPATH-BATCH-PANIC-A"), kind)); err != nil {
				return err
			}
			// Overflows the batch size of 1: both creates flush, durably.
			if err := b.CreateNode(graph.PrepareNode(graph.NewProperties().Set("objectid", "WRITEPATH-BATCH-PANIC-B"), kind)); err != nil {
				return err
			}
			panic(writePathDelegatePanic)
		}, graph.WithBatchSize(1))
	})
	if recovered != writePathDelegatePanic {
		t.Fatalf("recovered %v, want the delegate's own panic", recovered)
	}

	if got := nodeCountByKind(t, ctx, oracle, kind); got != 3 {
		t.Fatalf("postgresql holds %d node(s), want 3: the flush before the panic did not land", got)
	}
	if got := nodeCountByKind(t, ctx, bt, kind); got != 3 {
		t.Fatalf("WRONG ANSWER: bloodtrail counts %d node(s) after the panic, postgresql 3", got)
	}
	requireWritePathConverged(t, ctx, d, "after the panicked batch")
}

// TestReadTransactionPanicAfterAWriteStillReachesTheReplica is the same for
// a ReadTransaction: a write through it autocommits, so it must reach the
// replica even when the delegate then panics.
func TestReadTransactionPanicAfterAWriteStillReachesTheReplica(t *testing.T) {
	d, bt, oracle, _, buf, ctx := openWritePathDriver(t)
	kind := graph.StringKind("WritePathReadPanicNode")

	seedWritePathAnchor(t, ctx, bt, kind)
	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: the anchor serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, 1)

	recovered := recoverWritePathPanic(func() {
		_ = bt.ReadTransaction(ctx, func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties().Set("objectid", "WRITEPATH-READ-PANIC"), kind); err != nil {
				return err
			}
			panic(writePathDelegatePanic)
		})
	})
	if recovered != writePathDelegatePanic {
		t.Fatalf("recovered %v, want the delegate's own panic", recovered)
	}

	if got := nodeCountByKind(t, ctx, oracle, kind); got != 2 {
		t.Fatalf("postgresql holds %d node(s), want 2: the write before the panic did not autocommit", got)
	}
	if got := nodeCountByKind(t, ctx, bt, kind); got != 2 {
		t.Fatalf("WRONG ANSWER: bloodtrail counts %d node(s) after the panic, postgresql 2", got)
	}
	requireWritePathConverged(t, ctx, d, "after the panicked read transaction")
}

// TestWriteTransactionPanicResolvesItsWatermarkBump covers a
// WriteTransaction delegate that panics after its first write: PostgreSQL
// rolls the transaction back, but the eager watermark bump it made must
// still be resolved, or no later write could ever let the watermark
// converge again (and no snapshot could be saved for the rest of the
// process).
func TestWriteTransactionPanicResolvesItsWatermarkBump(t *testing.T) {
	d, bt, oracle, _, buf, ctx := openWritePathDriver(t)
	kind := graph.StringKind("WritePathWritePanicNode")

	seedWritePathAnchor(t, ctx, bt, kind)
	requireWritePathConverged(t, ctx, d, "before the panic")

	recovered := recoverWritePathPanic(func() {
		_ = bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties().Set("objectid", "WRITEPATH-WRITE-PANIC"), kind); err != nil {
				return err
			}
			panic(writePathDelegatePanic)
		})
	})
	if recovered != writePathDelegatePanic {
		t.Fatalf("recovered %v, want the delegate's own panic", recovered)
	}

	if got := nodeCountByKind(t, ctx, oracle, kind); got != 1 {
		t.Fatalf("postgresql holds %d node(s), want 1: the panicked transaction was not rolled back", got)
	}
	requireMarkerDelta(t, buf, builderServedMarker, 1, "the rolled-back create does not serve",
		func() int64 { return nodeCountByKind(t, ctx, bt, kind) }, 1)
	requireWritePathConverged(t, ctx, d, "after the panicked transaction")

	// A later, ordinary write.
	seedWritePathAnchor(t, ctx, bt, graph.StringKind("WritePathWritePanicLater"))
	requireWritePathConverged(t, ctx, d, "after a later write")
	assertNoFallback(t, buf)
}
