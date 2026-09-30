// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// TestFetchAllShortestPathsEmptyEdgeKindsMatchesPostgreSQL: a relationship
// kind filter that names no kinds -- query.KindIn(query.Relationship()) --
// is rendered by dawgs as kind_id = any('{}'), which matches no edge, so
// PostgreSQL finds no path. An empty EdgeKinds means "every kind" to the
// engine, so serving it would have returned the shortest path; the shape
// must go to PostgreSQL instead. (BloodHound's pathfinding rejects an empty
// kind list itself, so this is a latent difference, not a reachable one.)
func TestFetchAllShortestPathsEmptyEdgeKindsMatchesPostgreSQL(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)
	oracle, _ := graphtest.OpenPG(t, graphtest.PGAvailable(t))

	ids := loadLevelFixture(t, ctx, oracle)
	names := make(map[graph.ID]string, len(ids))
	for name, id := range ids {
		names[id] = name
	}
	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	for _, tc := range []struct {
		name       string
		edgeKinds  graph.Kinds
		wantPaths  []string
		wantServed int
	}{
		{"no kinds matches nothing", nil, []string{}, 0},
		{"one kind serves the shortest path", graph.Kinds{levelEdgeKind}, []string{"r0->i1->i2->t9"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			criteria := query.And(
				query.Equals(query.StartID(), ids["r0"]),
				query.Equals(query.EndID(), ids["t9"]),
				query.KindIn(query.Relationship(), tc.edgeKinds...),
			)

			want := builderLevelPaths(t, ctx, oracle, names, criteria)
			if !reflect.DeepEqual(want, tc.wantPaths) {
				t.Fatalf("PostgreSQL paths = %v, want %v: the fixture no longer pins the difference", want, tc.wantPaths)
			}

			before := markerCount(buf, servedMarker)
			got := builderLevelPaths(t, ctx, bt, names, criteria)
			served := markerCount(buf, servedMarker) - before
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("bloodtrail paths = %v (served %d), postgresql %v", got, served, want)
			}
			if served != tc.wantServed {
				t.Fatalf("%q log count changed by %d, want %d", servedMarker, served, tc.wantServed)
			}
		})
	}
}
