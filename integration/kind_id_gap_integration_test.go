// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// kindIDGapBurnSQL is dawgs' own insert_or_get_kind statement. Re-run for a
// kind that already exists, its data-modifying CTE still draws a value from
// the kind table's smallserial before ON CONFLICT DO NOTHING discards the
// row, which leaves a hole in the ids (a rolled-back transaction does the
// same).
const kindIDGapBurnSQL = `with existing as (select id from kind where kind.name = $1), inserted as (insert into kind (name) values ($1) on conflict (name) do nothing returning id) select * from existing union select * from inserted`

// TestRelationshipKindListingsServeAcrossKindIDGap: the kind table's ids are
// not contiguous once a sequence value has been burned. The listings that
// resolve kind names up front -- a relationship kinds listing with no kind
// filter, and a step projection whose far node carries kinds -- resolved
// every id from 1 to the highest carried one, so a hole made the resolution
// fail and the query decline (and refetch the kind table under the driver's
// lock every time). They must resolve only the ids the kind table holds, and
// serve the same rows PostgreSQL returns.
func TestRelationshipKindListingsServeAcrossKindIDGap(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)
	dsn := graphtest.PGAvailable(t)
	oracle, pool := graphtest.OpenPG(t, dsn)

	// Fresh kind names every run, so the burned id always lies below the
	// fixture's kinds whatever an earlier run left in the database.
	//
	// That costs four of the kind table's smallserial ids per run -- three
	// kinds plus the one deliberately burned below -- and the residue is
	// deliberately NOT cleaned up, because cleaning it up would not help:
	// deleting a kind row does not rewind the sequence, so the ids are spent
	// either way, and the names have to differ per run for the gap to sit
	// below the fixture at all. What bounds it is smallserial's own ceiling of
	// 32767, i.e. roughly eight thousand runs of this one test against the
	// same database. Past that it is PostgreSQL that refuses the next kind,
	// not BloodTrail: the engine itself serves a kind table that has reached
	// 32767 (internal/engine's TestRebuildServesTheLargestSmallserialKindID
	// pins exactly that). The remedy is recreating the test database, which
	// is also what CI does every run -- so only a long-lived local database
	// ever gets there.
	suffix := time.Now().UnixNano()
	anchorKind := graph.StringKind(fmt.Sprintf("KindIDGapAnchor%d", suffix))
	nodeKind := graph.StringKind(fmt.Sprintf("KindIDGapNode%d", suffix))
	edgeKind := graph.StringKind(fmt.Sprintf("KindIDGapEdge%d", suffix))

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties(), anchorKind)
		return err
	}); err != nil {
		t.Fatalf("create the anchor kind: %v", err)
	}
	if _, err := pool.Exec(ctx, kindIDGapBurnSQL, anchorKind.String()); err != nil {
		t.Fatalf("burn a kind id: %v", err)
	}

	var hub graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		h, err := tx.CreateNode(graph.NewProperties(), nodeKind)
		if err != nil {
			return err
		}
		hub = h.ID
		for i := 0; i < 3; i++ {
			far, err := tx.CreateNode(graph.NewProperties(), nodeKind, anchorKind)
			if err != nil {
				return err
			}
			if _, err := tx.CreateRelationshipByIDs(h.ID, far.ID, edgeKind, graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	requireKindIDGapBelow(ctx, t, pool, edgeKind)

	criteria := query.Equals(query.StartID(), hub)

	// Both closures take the subtest's own *testing.T rather than closing
	// over this one: they run inside t.Run below, on the subtest's goroutine,
	// and a Fatalf on the PARENT t from there calls runtime.Goexit on the
	// wrong goroutine -- it ends the subtest while the parent believes it is
	// still running, so the failure is reported against the wrong test and
	// the run can stall instead of failing.
	kindsRows := func(t *testing.T, db graph.Database) []string {
		t.Helper()
		var rows []string
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Relationships().Filter(criteria).FetchKinds(func(cursor graph.Cursor[graph.RelationshipKindsResult]) error {
				for row := range cursor.Chan() {
					rows = append(rows, fmt.Sprintf("%d/%d->%d/%s", row.ID, row.StartID, row.EndID, row.Kind))
				}
				return cursor.Error()
			})
		}); err != nil {
			t.Fatalf("FetchKinds: %v", err)
		}
		sort.Strings(rows)
		return rows
	}
	stepRows := func(t *testing.T, db graph.Database) []string {
		t.Helper()
		var rows []string
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			return tx.Relationships().Filter(criteria).OrderBy(query.Order(query.Identity(query.Relationship()), query.Ascending())).Query(func(results graph.Result) error {
				var (
					farID    graph.ID
					farKinds graph.Kinds
					relID    graph.ID
					relKind  graph.Kind
				)
				for results.Next() {
					if err := results.Scan(&farID, &farKinds, &relID, &relKind); err != nil {
						return err
					}
					rows = append(rows, fmt.Sprintf("%d/%v/%d/%v", farID, farKinds, relID, relKind))
				}
				return results.Error()
			}, query.Returning(query.EndID(), query.KindsOf(query.End()), query.RelationshipID(), query.KindsOf(query.Relationship())))
		}); err != nil {
			t.Fatalf("step projection: %v", err)
		}
		return rows
	}

	for _, tc := range []struct {
		name string
		rows func(*testing.T, graph.Database) []string
	}{
		{"kinds listing without a kind filter", kindsRows},
		{"step projection with far-node kinds", stepRows},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.rows(t, oracle)
			if len(want) != 3 {
				t.Fatalf("PostgreSQL rows = %v, want 3", want)
			}
			before := markerCount(buf, builderServedMarker)
			got := tc.rows(t, bt)
			served := markerCount(buf, builderServedMarker) - before
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("bloodtrail rows %v, postgresql %v", got, want)
			}
			if served != 1 {
				t.Fatalf("%q log count changed by %d, want 1: the engine declined across the kind id gap\ncaptured log:\n%s", builderServedMarker, served, buf.String())
			}
		})
	}
}

// requireKindIDGapBelow fails the test unless the kind table has an unused id
// below kind's own: without one the test proves nothing about a gap.
func requireKindIDGapBelow(ctx context.Context, t *testing.T, pool *pgxpool.Pool, kind graph.Kind) {
	t.Helper()

	var id, used int
	if err := pool.QueryRow(ctx, `select id from kind where name = $1`, kind.String()).Scan(&id); err != nil {
		t.Fatalf("read the id of kind %s: %v", kind, err)
	}
	if err := pool.QueryRow(ctx, `select count(*) from kind where id <= $1`, id).Scan(&used); err != nil {
		t.Fatalf("count the kinds up to id %d: %v", id, err)
	}
	if used == id {
		t.Fatalf("kind ids 1..%d are all in use: the fixture has no gap to exercise", id)
	}
}
