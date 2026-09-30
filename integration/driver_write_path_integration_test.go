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
