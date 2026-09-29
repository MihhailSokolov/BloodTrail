// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// Kinds for TestShortestPathLevelParity's fixture, private to this file so
// no other suite's data can land in its answers.
var (
	levelNodeKind     = graph.StringKind("SPLevelNode")
	levelRootKind     = graph.StringKind("SPLevelRoot")
	levelTerminalKind = graph.StringKind("SPLevelTerminal")
	levelEdgeKind     = graph.StringKind("SPLevelEdge")
)

// levelFixtureEdges puts four roots at three different distances from one
// terminal, created farthest root first so the lowest ids belong to the
// longest pairs:
//
//	r0 -> i1 -> i2 -> t9   (3 hops)
//	r4 -> i5 -> t9         (2 hops)
//	r6 -> t9               (1 hop)
//	r7 -> t9               (1 hop)
var levelFixtureEdges = [][2]string{
	{"r0", "i1"}, {"i1", "i2"}, {"i2", "t9"},
	{"r4", "i5"}, {"i5", "t9"},
	{"r6", "t9"},
	{"r7", "t9"},
}

// loadLevelFixture writes the fixture through the raw pg driver and returns
// each node's database id by name. Every node is an SPLevelNode named after
// itself; the r* nodes are also SPLevelRoot and t9 is also SPLevelTerminal.
func loadLevelFixture(t *testing.T, ctx context.Context, db graph.Database) map[string]graph.ID {
	t.Helper()

	names := []string{"r0", "i1", "i2", "r4", "i5", "r6", "r7", "t9"}
	ids := make(map[string]graph.ID, len(names))
	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, name := range names {
			kinds := graph.Kinds{levelNodeKind}
			switch {
			case strings.HasPrefix(name, "r"):
				kinds = append(kinds, levelRootKind)
			case strings.HasPrefix(name, "t"):
				kinds = append(kinds, levelTerminalKind)
			}
			n, err := tx.CreateNode(graph.NewProperties().Set("name", name), kinds...)
			if err != nil {
				return fmt.Errorf("create %s: %w", name, err)
			}
			ids[name] = n.ID
		}
		for _, e := range levelFixtureEdges {
			if _, err := tx.CreateRelationshipByIDs(ids[e[0]], ids[e[1]], levelEdgeKind, graph.NewProperties()); err != nil {
				return fmt.Errorf("create %s->%s: %w", e[0], e[1], err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("load level fixture: %v", err)
	}
	return ids
}

// renderLevelPaths renders each path as its node names joined by "->",
// sorted, naming nodes through names (database id -> name) so the rendering
// does not depend on which node properties a path carries.
func renderLevelPaths(t *testing.T, paths []graph.Path, names map[graph.ID]string) []string {
	t.Helper()

	out := make([]string, 0, len(paths))
	for _, p := range paths {
		hops := make([]string, len(p.Nodes))
		for i, n := range p.Nodes {
			name, ok := names[n.ID]
			if !ok {
				t.Fatalf("path node %d is not a fixture node", n.ID)
			}
			hops[i] = name
		}
		out = append(out, strings.Join(hops, "->"))
	}
	sort.Strings(out)
	return out
}

// cypherLevelPaths runs text through db and renders its paths.
func cypherLevelPaths(t *testing.T, ctx context.Context, db graph.Database, names map[graph.ID]string, text string) []string {
	t.Helper()

	var paths []graph.Path
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		qr, err := ops.FetchByQuery(tx, text)
		if err != nil {
			return err
		}
		paths = qr.Paths
		return nil
	}); err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	return renderLevelPaths(t, paths, names)
}

// builderLevelPaths runs criteria through db's FetchAllShortestPaths, the
// builder call BloodHound's pathfinding endpoint makes, and renders its
// paths.
func builderLevelPaths(t *testing.T, ctx context.Context, db graph.Database, names map[graph.ID]string, criteria graph.Criteria) []string {
	t.Helper()

	var paths []graph.Path
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(criteria).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for p := range cursor.Chan() {
				paths = append(paths, p)
			}
			return cursor.Error()
		})
	}); err != nil {
		t.Fatalf("FetchAllShortestPaths: %v", err)
	}
	return renderLevelPaths(t, paths, names)
}

// TestShortestPathLevelParity checks BloodTrail against PostgreSQL where
// allShortestPaths has two answers: a query whose root/terminal pairs sit at
// different distances. dawgs resolves pairs one by one when its translation
// hands bidirectional_asp_harness a pair filter (property or id constraints
// on both endpoints), and otherwise stops at the first depth where any root
// reaches any terminal; shortestPath() always answers per pair. Every case
// must match PostgreSQL. Cases marked served must also come from memory:
// those are the shapes where the engine knows which answer PostgreSQL gives.
func TestShortestPathLevelParity(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	buf := installLogCapture(t)
	ctx := context.Background()
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("captured log:\n%s", buf.String())
		}
	})

	oracle, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, oracle)

	rawBT, err := dawgs.Open(ctx, bloodtrail.DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = rawBT.Close(ctx) })
	d, ok := rawBT.(*bloodtrail.Driver)
	if !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", rawBT)
	}
	// Each driver instance keeps its own schema state, and boot load waits
	// for this one's default graph.
	if err := d.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema on bloodtrail driver: %v", err)
	}
	waitForBootLoad(t, d)

	ids := loadLevelFixture(t, ctx, oracle)
	names := make(map[graph.ID]string, len(ids))
	for name, id := range ids {
		names[id] = name
	}
	if err := bloodtrail.TestingEngine(d).RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := bloodtrail.TestingEngine(d).RebuildCount()

	for _, tc := range []struct {
		name, cypher string
		served       bool
		want         []string
	}{
		{
			"asp, kind-constrained roots and terminal (overall shortest)",
			`MATCH p = allShortestPaths((s:SPLevelRoot)-[:SPLevelEdge*1..]->(e:SPLevelTerminal)) RETURN p`,
			true, []string{"r6->t9", "r7->t9"},
		},
		{
			"asp, property-constrained roots and terminal (per pair)",
			`MATCH p = allShortestPaths((s:SPLevelNode)-[:SPLevelEdge*1..]->(e:SPLevelNode)) WHERE s.name IN ['r0', 'r4', 'r6', 'r7'] AND e.name = 't9' RETURN p`,
			true, []string{"r0->i1->i2->t9", "r4->i5->t9", "r6->t9", "r7->t9"},
		},
		{
			"asp, property-constrained roots, kind-constrained terminal (overall shortest)",
			`MATCH p = allShortestPaths((s:SPLevelNode)-[:SPLevelEdge*1..]->(e:SPLevelTerminal)) WHERE s.name IN ['r0', 'r4'] RETURN p`,
			true, []string{"r4->i5->t9"},
		},
		{
			"asp, kind-constrained roots, property-constrained terminal (overall shortest)",
			`MATCH p = allShortestPaths((s:SPLevelRoot)-[:SPLevelEdge*1..]->(e:SPLevelNode)) WHERE e.name = 't9' RETURN p`,
			true, []string{"r6->t9", "r7->t9"},
		},
		{
			"asp, one root",
			`MATCH p = allShortestPaths((s:SPLevelNode)-[:SPLevelEdge*1..]->(e:SPLevelTerminal)) WHERE s.name = 'r0' RETURN p`,
			true, []string{"r0->i1->i2->t9"},
		},
		{
			"asp, kind-constrained, LIMIT",
			`MATCH p = allShortestPaths((s:SPLevelRoot)-[:SPLevelEdge*1..]->(e:SPLevelTerminal)) RETURN p LIMIT 5`,
			true, []string{"r6->t9", "r7->t9"},
		},
		{
			"sp, kind-constrained roots and terminal (per pair)",
			`MATCH p = shortestPath((s:SPLevelRoot)-[:SPLevelEdge*1..]->(e:SPLevelTerminal)) RETURN p`,
			true, []string{"r0->i1->i2->t9", "r4->i5->t9", "r6->t9", "r7->t9"},
		},
		{
			"asp, unconstrained roots",
			`MATCH p = allShortestPaths((s)-[:SPLevelEdge*1..]->(e:SPLevelTerminal)) RETURN p`,
			false, nil,
		},
		{
			"asp, unconstrained terminals",
			`MATCH p = allShortestPaths((s:SPLevelRoot)-[:SPLevelEdge*1..]->(e)) RETURN p`,
			false, nil,
		},
	} {
		t.Run("cypher/"+tc.name, func(t *testing.T) {
			want := cypherLevelPaths(t, ctx, oracle, names, tc.cypher)
			if tc.want != nil && strings.Join(want, " ") != strings.Join(tc.want, " ") {
				t.Fatalf("postgresql returned %v, want %v: the fixture no longer shows the case this test is about", want, tc.want)
			}

			before := markerCount(buf, cypherServedMarker)
			got := cypherLevelPaths(t, ctx, d, names, tc.cypher)
			served := markerCount(buf, cypherServedMarker) - before

			if strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("paths differ:\n  bloodtrail: %v\n  postgresql: %v", got, want)
			}
			if tc.served && served != 1 {
				t.Fatalf("%q log count changed by %d, want 1", cypherServedMarker, served)
			}
		})
	}

	// Builder calls: recognize.FromCriteria serves only the single-pair
	// shape BloodHound's pathfinding endpoint sends, where both answers
	// agree; anything wider is left to PostgreSQL. (The builder cases stay
	// off list-valued criteria such as query.In: dawgs' pg driver cannot put
	// one into its shortest-path harness and fails the call itself.)
	edges := query.KindIn(query.Relationship(), levelEdgeKind)
	for _, tc := range []struct {
		name     string
		criteria graph.Criteria
		served   bool
	}{
		{
			"one pair",
			query.And(query.Equals(query.StartID(), ids["r0"]), query.Equals(query.EndID(), ids["t9"]), edges),
			true,
		},
		{
			"kind-constrained roots at different distances",
			query.And(query.KindIn(query.Start(), levelRootKind), query.Equals(query.EndID(), ids["t9"]), edges),
			false,
		},
	} {
		t.Run("builder/"+tc.name, func(t *testing.T) {
			want := builderLevelPaths(t, ctx, oracle, names, tc.criteria)

			before := markerCount(buf, servedMarker)
			got := builderLevelPaths(t, ctx, d, names, tc.criteria)
			served := markerCount(buf, servedMarker) - before

			if strings.Join(got, " ") != strings.Join(want, " ") {
				t.Fatalf("paths differ:\n  bloodtrail: %v\n  postgresql: %v", got, want)
			}
			if wantServed := map[bool]int{true: 1, false: 0}[tc.served]; served != wantServed {
				t.Fatalf("%q log count changed by %d, want %d", servedMarker, served, wantServed)
			}
		})
	}

	assertRebuildCountUnchanged(t, d, rebuilds, t.Name())
}
