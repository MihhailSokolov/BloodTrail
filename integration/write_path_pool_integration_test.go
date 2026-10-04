// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// The tests in this file pin that BloodTrail's write path never needs a
// second connection from the pool its caller's own write already holds one
// of. The eager watermark bump runs at a transaction's or batch's first
// mutating call, inside the delegate, and a mid-batch Commit applies --
// reads back -- while the batch still holds its connection; drawing either
// from BloodHound's pool makes writers wait on each other's connections
// once the pool is saturated. With a deadline the writes fail and the
// watermark stops being trusted; without one (a daemon's context) they hang
// for good. The stock PostgreSQL driver needs one connection per write, so
// the same load runs through it untouched.

var (
	saturatedPoolTxKind         = graph.StringKind("SaturatedPoolTxNode")
	saturatedPoolBatchKind      = graph.StringKind("SaturatedPoolBatchNode")
	saturatedPoolDeleteNodeKind = graph.StringKind("SaturatedPoolDeleteNode")

	// saturatedPoolDeleteEdgeKinds holds one edge kind per writer, so each
	// writer's kind delete has rows of its own to re-read however the two
	// Applies are ordered.
	saturatedPoolDeleteEdgeKinds = [saturatedPoolWriters]graph.Kind{
		graph.StringKind("SaturatedPoolDeleteEdge0"),
		graph.StringKind("SaturatedPoolDeleteEdge1"),
	}
)

// saturatedPoolWriters is how many writers each test runs at once: exactly
// as many as the pool has connections, so every connection is held by a
// writer's own transaction or batch when the writers make their mutating
// calls.
const saturatedPoolWriters = 2

// openSaturatablePool opens a bloodtrail driver on a pool of exactly
// saturatedPoolWriters connections, with kinds asserted up front so no
// write needs the pg driver's own kind-cache fetch. It also returns the
// captured log.
func openSaturatablePool(t *testing.T, engineSetting string) (*bloodtrail.Driver, *lockedBuffer, context.Context) {
	t.Helper()
	dsn := graphtest.PGAvailable(t)
	buf := installLogCapture(t)
	t.Setenv(bloodtrail.EnvEngine, engineSetting)
	ctx := context.Background()

	pgDriver, _ := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	kinds := graph.Kinds{saturatedPoolTxKind, saturatedPoolBatchKind, saturatedPoolDeleteNodeKind}
	kinds = append(kinds, saturatedPoolDeleteEdgeKinds[:]...)
	if _, err := pgDriver.AssertKinds(ctx, kinds); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = saturatedPoolWriters
	cfg.MinConns = 0
	cfg.AfterConnect = pg.AfterPooledConnectionEstablished
	cfg.AfterRelease = pg.AfterPooledConnectionRelease
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	db, err := dawgs.Open(ctx, bloodtrail.DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = db.Close(ctx) })
	if err := db.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	d := db.(*bloodtrail.Driver)
	if engineSetting == "on" {
		waitForBootLoad(t, d)
	}
	return d, buf, ctx
}

// runSaturatingWriters runs write once per writer, concurrently, and returns
// how long they took and what each returned. Each write gets writerDeadline:
// a writer that waits on another's connection runs into it.
func runSaturatingWriters(ctx context.Context, write func(ctx context.Context, i int, holding *sync.WaitGroup) error) (time.Duration, []error) {
	const writerDeadline = 10 * time.Second

	var (
		wg      sync.WaitGroup
		holding sync.WaitGroup
		errs    = make([]error, saturatedPoolWriters)
	)
	holding.Add(saturatedPoolWriters)
	start := time.Now()
	for i := 0; i < saturatedPoolWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			wctx, cancel := context.WithTimeout(ctx, writerDeadline)
			defer cancel()
			errs[i] = write(wctx, i, &holding)
		}(i)
	}
	wg.Wait()
	return time.Since(start), errs
}

// requireWritersFinished fails unless every writer succeeded, well inside
// its deadline.
func requireWritersFinished(t *testing.T, elapsed time.Duration, errs []error) {
	t.Helper()
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("concurrent writers on a saturated pool failed after %v: %v", elapsed.Round(time.Millisecond), err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("concurrent writers on a saturated pool took %v, want them not to wait on each other", elapsed.Round(time.Millisecond))
	}
}

// TestWriteTransactionsDoNotWaitOnEachOthersConnection is the reported
// shape: as many single-write transactions as the pool has connections,
// each already holding its own connection when it makes its first mutating
// call -- the eager watermark bump's moment.
func TestWriteTransactionsDoNotWaitOnEachOthersConnection(t *testing.T) {
	for _, engineSetting := range []string{"on", "off"} {
		t.Run("engine "+engineSetting, func(t *testing.T) {
			d, _, ctx := openSaturatablePool(t, engineSetting)

			elapsed, errs := runSaturatingWriters(ctx, func(wctx context.Context, i int, holding *sync.WaitGroup) error {
				return d.WriteTransaction(wctx, func(tx graph.Transaction) error {
					holding.Done()
					holding.Wait() // every writer holds its pooled connection now
					_, err := tx.CreateNode(graph.NewProperties().Set("objectid", fmt.Sprintf("saturated-tx-%d", i)), saturatedPoolTxKind)
					return err
				})
			})
			requireWritersFinished(t, elapsed, errs)

			if engineSetting == "on" && !bloodtrail.TestingEngine(d).WatermarkTrusted(ctx) {
				t.Fatalf("WatermarkTrusted = false after the writers finished, want true")
			}
		})
	}
}

// TestMidBatchCommitsDoNotWaitOnEachOthersConnection is the batch shape: a
// batch holds its connection for its whole delegate, and a mid-batch Commit
// applies what it flushed -- reading it back from PostgreSQL -- before the
// batch goes on. Every read-back runs while the batch's own connection is
// still held.
func TestMidBatchCommitsDoNotWaitOnEachOthersConnection(t *testing.T) {
	d, _, ctx := openSaturatablePool(t, "on")

	elapsed, errs := runSaturatingWriters(ctx, func(wctx context.Context, i int, holding *sync.WaitGroup) error {
		return d.BatchOperation(wctx, func(batch graph.Batch) error {
			holding.Done()
			holding.Wait() // every writer holds its pooled connection now
			if err := batch.CreateNode(graph.PrepareNode(graph.NewProperties().Set("objectid", fmt.Sprintf("saturated-batch-%d", i)), saturatedPoolBatchKind)); err != nil {
				return err
			}
			return batch.Commit()
		})
	})
	requireWritersFinished(t, elapsed, errs)

	if !bloodtrail.TestingEngine(d).WatermarkTrusted(ctx) {
		t.Fatalf("WatermarkTrusted = false after the batches finished, want true")
	}
	if got := nodeCountByKind(t, ctx, d, saturatedPoolBatchKind); got != saturatedPoolWriters {
		t.Fatalf("served node count = %d after both batches committed, want %d", got, saturatedPoolWriters)
	}
}

// TestMidBatchKindDeletesDoNotWaitOnEachOthersConnection is the batch shape
// with a kind-scoped delete: a kind delete names no row by key, so its
// Apply re-reads by id every row of the kind the View holds (readBack's
// candidate re-read) -- one more read-back query that runs while the
// batch's own connection is still held, and so has to run on the write
// path's pool like the keyed lookups do.
func TestMidBatchKindDeletesDoNotWaitOnEachOthersConnection(t *testing.T) {
	d, buf, ctx := openSaturatablePool(t, "on")

	// One edge per writer's kind, applied, so each delete has a View
	// candidate to re-read.
	if err := d.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i, kind := range saturatedPoolDeleteEdgeKinds {
			start, err := tx.CreateNode(graph.NewProperties().Set("objectid", fmt.Sprintf("saturated-delete-%d-start", i)), saturatedPoolDeleteNodeKind)
			if err != nil {
				return err
			}
			end, err := tx.CreateNode(graph.NewProperties().Set("objectid", fmt.Sprintf("saturated-delete-%d-end", i)), saturatedPoolDeleteNodeKind)
			if err != nil {
				return err
			}
			if _, err := tx.CreateRelationshipByIDs(start.ID, end.ID, kind, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, kind := range saturatedPoolDeleteEdgeKinds {
		if got := relCountByKind(t, ctx, d, kind); got != 1 {
			t.Fatalf("served %s count = %d after the seed, want 1", kind, got)
		}
	}

	elapsed, errs := runSaturatingWriters(ctx, func(wctx context.Context, i int, holding *sync.WaitGroup) error {
		return d.BatchOperation(wctx, func(batch graph.Batch) error {
			holding.Done()
			holding.Wait() // every writer holds its pooled connection now
			if err := batch.Relationships().Filter(query.Kind(query.Relationship(), saturatedPoolDeleteEdgeKinds[i])).Delete(); err != nil {
				return err
			}
			return batch.Commit()
		})
	})
	requireWritersFinished(t, elapsed, errs)
	// A re-read that waited on the other batch's connection fails at the
	// writer's deadline and Apply falls back, which the writer never sees.
	assertNoFallback(t, buf)

	if !bloodtrail.TestingEngine(d).WatermarkTrusted(ctx) {
		t.Fatalf("WatermarkTrusted = false after the batches finished, want true")
	}
	for _, kind := range saturatedPoolDeleteEdgeKinds {
		if got := relCountByKind(t, ctx, d, kind); got != 0 {
			t.Fatalf("served %s count = %d after both kind deletes committed, want 0", kind, got)
		}
	}
}
