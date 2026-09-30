// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// pathParityNode and pathParityEdge describe a small named fixture: every node
// carries its name as the "name" property, so both sides of a comparison can
// be rendered by name instead of by database id.
type pathParityNode struct {
	name  string
	kinds []string
}

type pathParityEdge struct {
	from, to, kind string
}

// pathParityFixture is one fixture's nodes and edges.
type pathParityFixture struct {
	nodes []pathParityNode
	edges []pathParityEdge
}

// pathParityGraph is one seeded fixture with the plain pg driver (the oracle)
// and an engine serving a snapshot rebuilt from exactly that data.
type pathParityGraph struct {
	pg    *pg.Driver
	eng   *Engine
	names map[graph.ID]string
}

// seedPathParityGraph wipes the graph, writes the fixture through the plain
// pg driver and rebuilds an engine snapshot from it.
func seedPathParityGraph(t *testing.T, fixture pathParityFixture) *pathParityGraph {
	t.Helper()
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := map[string]graph.ID{}
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, n := range fixture.nodes {
			var kinds graph.Kinds
			for _, k := range n.kinds {
				kinds = append(kinds, graph.StringKind(k))
			}
			created, err := tx.CreateNode(graph.NewProperties().Set("name", n.name), kinds...)
			if err != nil {
				return err
			}
			ids[n.name] = created.ID
		}
		for _, e := range fixture.edges {
			if _, err := tx.CreateRelationshipByIDs(ids[e.from], ids[e.to], graph.StringKind(e.kind), graph.NewProperties()); err != nil {
				return fmt.Errorf("edge %s-%s->%s: %w", e.from, e.kind, e.to, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	names := make(map[graph.ID]string, len(ids))
	for name, id := range ids {
		names[id] = name
	}
	return &pathParityGraph{pg: pgDriver, eng: eng, names: names}
}

// engineRows asks the engine to serve query, reporting whether it did and,
// if so, its rendered rows.
func (g *pathParityGraph) engineRows(t *testing.T, query string) (served bool, rows []string) {
	t.Helper()
	ctx := context.Background()
	var renderErr error
	if err := g.pg.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result, ok := g.eng.TryCypher(ctx, tx, query, nil)
		if !ok {
			return nil
		}
		served = true
		rows, renderErr = g.render(result)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction (engine): %v", err)
	}
	if renderErr != nil {
		t.Fatalf("served result failed: %v\nquery: %s", renderErr, query)
	}
	return served, rows
}

// oracleRows runs query through the plain pg driver.
func (g *pathParityGraph) oracleRows(t *testing.T, query string) (rows []string, oracleErr error) {
	t.Helper()
	ctx := context.Background()
	if err := g.pg.ReadTransaction(ctx, func(tx graph.Transaction) error {
		rows, oracleErr = g.render(tx.Query(query, map[string]any{}))
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction (oracle): %v", err)
	}
	return rows, oracleErr
}

// render drains result into sorted rows, one column per value joined by " | ".
func (g *pathParityGraph) render(result graph.Result) ([]string, error) {
	defer result.Close()
	var rows []string
	for result.Next() {
		var cols []string
		for _, v := range result.Values() {
			cols = append(cols, g.renderValue(result.Mapper(), v))
		}
		rows = append(rows, strings.Join(cols, " | "))
	}
	if err := result.Error(); err != nil {
		return nil, err
	}
	sort.Strings(rows)
	return rows, nil
}

// renderValue renders a path as its node names and edge kinds in order (a
// path with no nodes at all renders as a bare "path:"), a node by name and
// anything else with its Go type.
func (g *pathParityGraph) renderValue(mapper graph.ValueMapper, v any) string {
	switch typed := v.(type) {
	case nil:
		return "null"
	case graph.Path:
		return g.renderPath(typed)
	case *graph.Path:
		if typed != nil {
			return g.renderPath(*typed)
		}
	case string, bool, int, int32, int64, float64:
		return fmt.Sprintf("%T:%v", v, v)
	}
	var path graph.Path
	if mapper.Map(v, &path) && len(path.Nodes) > 0 {
		return g.renderPath(path)
	}
	var node graph.Node
	if mapper.Map(v, &node) && node.ID != 0 {
		return "node:" + g.names[node.ID]
	}
	return fmt.Sprintf("%T:%v", v, v)
}

func (g *pathParityGraph) renderPath(p graph.Path) string {
	var b strings.Builder
	b.WriteString("path:")
	for i, n := range p.Nodes {
		if i > 0 {
			kind := "?"
			if i-1 < len(p.Edges) && p.Edges[i-1] != nil && p.Edges[i-1].Kind != nil {
				kind = p.Edges[i-1].Kind.String()
			}
			b.WriteString("-" + kind + "->")
		}
		if n != nil {
			b.WriteString(g.names[n.ID])
		}
	}
	return b.String()
}

// assertNeverWrong holds the engine to its contract for query: it may decline,
// but whatever it serves must equal PostgreSQL's rows, and it must not serve at
// all where PostgreSQL raises an error.
func (g *pathParityGraph) assertNeverWrong(t *testing.T, query string) {
	t.Helper()
	served, got := g.engineRows(t, query)
	if !served {
		return
	}
	want, oracleErr := g.oracleRows(t, query)
	if oracleErr != nil {
		t.Errorf("engine served %v where PostgreSQL raises %v\nquery: %s", got, oracleErr, query)
		return
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("engine served %v, PostgreSQL returns %v\nquery: %s", got, want, query)
	}
}

// assertServesOracle requires the engine to serve query, with PostgreSQL's
// exact rows.
func (g *pathParityGraph) assertServesOracle(t *testing.T, query string) {
	t.Helper()
	served, got := g.engineRows(t, query)
	if !served {
		t.Errorf("engine declined a shape it must serve\nquery: %s", query)
		return
	}
	want, oracleErr := g.oracleRows(t, query)
	if oracleErr != nil {
		t.Errorf("PostgreSQL raises %v; the fixture no longer shows this case\nquery: %s", oracleErr, query)
		return
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("engine served %v, PostgreSQL returns %v\nquery: %s", got, want, query)
	}
}

// varLengthCycleFixture is a 3-cycle with a node that carries both endpoint
// kinds, a parallel edge of another kind and a tail:
//
//	a(ZA) -ZE-> b(ZB) -ZE-> c(ZA,ZB) -ZE-> a
//	a -ZF-> b
//	c -ZE-> d(ZB)
func varLengthCycleFixture() pathParityFixture {
	return pathParityFixture{
		nodes: []pathParityNode{
			{name: "a", kinds: []string{"ZA"}},
			{name: "b", kinds: []string{"ZB"}},
			{name: "c", kinds: []string{"ZA", "ZB"}},
			{name: "d", kinds: []string{"ZB"}},
		},
		edges: []pathParityEdge{
			{"a", "b", "ZE"}, {"b", "c", "ZE"}, {"c", "a", "ZE"},
			{"a", "b", "ZF"},
			{"c", "d", "ZE"},
		},
	}
}

// TestVarLengthZeroLengthPathMatchesOracle: a `*0..` row bound to a path
// variable is the one-node path of its start node in PostgreSQL (dawgs'
// zero-length arm hands ordered_edges_to_path the root and no edges), not an
// empty path. Every shape must serve, forward and inbound, alone and with a
// deeper upper bound.
func TestVarLengthZeroLengthPathMatchesOracle(t *testing.T) {
	g := seedPathParityGraph(t, varLengthCycleFixture())
	for _, query := range []string{
		`MATCH p = (x:ZA)-[:ZE*0..1]->(y) RETURN p`,
		`MATCH p = (y:ZB)<-[:ZE*0..1]-(x:ZA) RETURN p`,
		`MATCH p = (x:ZA)-[:ZE*0..]->(y:ZB) RETURN p`,
		`MATCH p = (x:ZA)-[:ZE*0..2]->(y:ZA) RETURN p`,
		`MATCH p = (x)-[:ZE*0..]->(y:ZA) WHERE y.name = 'a' RETURN p`,
		`MATCH p = (x:ZA)-[:ZE*0..1]->(m)-[:ZE]->(z) RETURN p`,
		`MATCH p = (x:ZA)-[:ZE]->(m)-[:ZE*0..1]->(z) RETURN p`,
	} {
		t.Run(query, func(t *testing.T) { g.assertServesOracle(t, query) })
	}
}

// TestVarLengthZeroUpperBoundMatchesOracle: an explicit upper bound of zero
// (`*..0`, `*1..0`, `*0..0`) does not stop dawgs' primer, which emits every
// depth-1 row regardless of the bound, while the shortest-path harness loop
// never runs at all. The engine may decline these shapes but must never serve
// an answer PostgreSQL does not give.
func TestVarLengthZeroUpperBoundMatchesOracle(t *testing.T) {
	g := seedPathParityGraph(t, varLengthCycleFixture())
	for _, query := range []string{
		`MATCH p = (x:ZA)-[:ZE*1..0]->(y) RETURN p`,
		`MATCH p = (x:ZA)-[:ZE*..0]->(y) RETURN p`,
		`MATCH p = (x:ZA)-[:ZE*0..0]->(y) RETURN p`,
		`MATCH (x:ZA)-[:ZE*0..0]->(y) RETURN x, y`,
		`MATCH p = shortestPath((x:ZA)-[:ZE*1..0]->(y:ZB)) WHERE x.name = 'a' RETURN p`,
		`MATCH p = allShortestPaths((x:ZA)-[:ZE*1..0]->(y:ZB)) WHERE x.name = 'a' RETURN p`,
		`MATCH p = shortestPath((x:ZA)-[:ZE*..0]->(y:ZB)) WHERE x.name = 'a' RETURN p`,
	} {
		t.Run(query, func(t *testing.T) { g.assertNeverWrong(t, query) })
	}
}
