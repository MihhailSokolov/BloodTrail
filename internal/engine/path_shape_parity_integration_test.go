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
	ids   map[string]graph.ID
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
	return &pathParityGraph{pg: pgDriver, eng: eng, ids: ids, names: names}
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

// shortestPathLevelFixture puts four roots at three distances from one
// terminal; no root has an incoming edge of the traversed kind:
//
//	r0 -> i1 -> i2 -> t9   (3 hops)
//	r4 -> i5 -> t9         (2 hops)
//	r6 -> t9               (1 hop)
//	r7 -> t9               (1 hop)
//
// Every node is a ZNode; the r* nodes are also ZRoot and t9 is ZTerm.
func shortestPathLevelFixture() pathParityFixture {
	var f pathParityFixture
	for _, name := range []string{"r0", "i1", "i2", "r4", "i5", "r6", "r7", "t9"} {
		kinds := []string{"ZNode"}
		switch name[0] {
		case 'r':
			kinds = append(kinds, "ZRoot")
		case 't':
			kinds = append(kinds, "ZTerm")
		}
		f.nodes = append(f.nodes, pathParityNode{name: name, kinds: kinds})
	}
	f.edges = []pathParityEdge{
		{"r0", "i1", "ZEdge"}, {"i1", "i2", "ZEdge"}, {"i2", "t9", "ZEdge"},
		{"r4", "i5", "ZEdge"}, {"i5", "t9", "ZEdge"},
		{"r6", "t9", "ZEdge"},
		{"r7", "t9", "ZEdge"},
	}
	return f
}

// TestShortestPathUnconstrainedSecondEndpointMatchesOracle: when the
// pattern's second-written endpoint carries no constraint, dawgs' harness
// has no terminal filter and marks a hop satisfied by a continuation test on
// the seed instead -- `exists (select 1 from edge where end_id =
// e0.start_id)` for a forward pattern, `start_id = e0.end_id` for a backward
// one. At depth 1 that asks whether the SEED has an incoming (resp.
// outgoing) edge of any kind, so pg drops the one-hop paths of a seed
// without one and reports the next level instead. Here only r6 has an
// incoming edge (x0 -ZOther-> r6), and t9 has no outgoing edge at all.
//
// When the FIRST-written endpoint is the unconstrained one, dawgs seeds from
// the other side and the same test degenerates to always-true, so those
// spellings must keep serving.
func TestShortestPathUnconstrainedSecondEndpointMatchesOracle(t *testing.T) {
	fixture := shortestPathLevelFixture()
	fixture.nodes = append(fixture.nodes, pathParityNode{name: "x0", kinds: []string{"ZNode"}})
	fixture.edges = append(fixture.edges, pathParityEdge{"x0", "r6", "ZOther"})
	g := seedPathParityGraph(t, fixture)

	for _, query := range []string{
		`MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(e)) WHERE s <> e RETURN p`,
		`MATCH p = allShortestPaths((s:ZRoot)-[:ZEdge*1..]->(e)) WHERE s <> e RETURN p`,
		`MATCH p = shortestPath((s:ZNode)-[:ZEdge*1..]->(e)) WHERE s.name = 'r4' AND s <> e RETURN p`,
		fmt.Sprintf(`MATCH p = shortestPath((s)-[:ZEdge*1..]->(e)) WHERE id(s) = %d AND s <> e RETURN p`, g.ids["r4"]),
		`MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(e)) WHERE s <> e RETURN p LIMIT 2`,
		`MATCH p = shortestPath((e:ZTerm)<-[:ZEdge*1..]-(s)) WHERE s <> e RETURN p`,
		`MATCH p = allShortestPaths((e:ZTerm)<-[:ZEdge*1..]-(s)) WHERE s <> e RETURN p`,
	} {
		t.Run(query, func(t *testing.T) { g.assertNeverWrong(t, query) })
	}

	for _, query := range []string{
		`MATCH p = shortestPath((e)<-[:ZEdge*1..]-(s:ZRoot)) WHERE s <> e RETURN p`,
		`MATCH p = allShortestPaths((e)<-[:ZEdge*1..]-(s:ZRoot)) WHERE s <> e RETURN p`,
		`MATCH p = shortestPath((s)-[:ZEdge*1..]->(e:ZTerm)) WHERE s <> e RETURN p`,
		`MATCH p = allShortestPaths((s)-[:ZEdge*1..]->(e:ZTerm)) WHERE s <> e RETURN p`,
		`MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(e:ZNode)) WHERE s <> e RETURN p`,
	} {
		t.Run(query, func(t *testing.T) { g.assertServesOracle(t, query) })
	}
}

// selfCycleFixture puts group g1 on a 2-cycle (g1 -> u -> g1) while the only
// other group, g2, is three hops away:
//
//	g1 -> u -> g1
//	g1 -> x -> y -> g2
func selfCycleFixture() pathParityFixture {
	return pathParityFixture{
		nodes: []pathParityNode{
			{name: "g1", kinds: []string{"ZG"}},
			{name: "u", kinds: []string{"ZU"}},
			{name: "x", kinds: []string{"ZU"}},
			{name: "y", kinds: []string{"ZU"}},
			{name: "g2", kinds: []string{"ZG"}},
		},
		edges: []pathParityEdge{
			{"g1", "u", "ZEdge"}, {"u", "g1", "ZEdge"},
			{"g1", "x", "ZEdge"}, {"x", "y", "ZEdge"}, {"y", "g2", "ZEdge"},
		},
	}
}

// TestAllShortestPathsSharedEndpointInequalityMatchesOracle: in the
// overall-shortest answer (unidirectional_asp_harness) PostgreSQL picks the
// first depth at which ANY root reaches ANY terminal, and a cycle back to a
// root that is also a terminal counts -- the harness has no visited set and
// the endpoint inequality is only applied afterwards. So with g1 on a
// 2-cycle, depth 2 wins, the inequality then drops g1's own paths and pg
// returns nothing, while excluding the self pair before choosing the length
// served g1 -> x -> y -> g2. The per-pair answer (both endpoints carrying a
// property filter) and shortestPath are unaffected and must keep serving.
func TestAllShortestPathsSharedEndpointInequalityMatchesOracle(t *testing.T) {
	t.Run("two-cycle", func(t *testing.T) {
		g := seedPathParityGraph(t, selfCycleFixture())
		for _, query := range []string{
			`MATCH p = allShortestPaths((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a <> b RETURN p`,
			`MATCH p = allShortestPaths((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE id(a) <> id(b) RETURN p`,
			`MATCH p = allShortestPaths((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a <> b AND a.name = 'g1' RETURN p`,
			`MATCH p = allShortestPaths((a:ZG)-[:ZEdge*1..]->(b)) WHERE a <> b AND b.name IN ['g1', 'g2'] RETURN p`,
		} {
			t.Run(query, func(t *testing.T) { g.assertNeverWrong(t, query) })
		}
		for _, query := range []string{
			`MATCH p = allShortestPaths((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a <> b AND a.name IN ['g1', 'g2'] AND b.name IN ['g1', 'g2'] RETURN p`,
			`MATCH p = shortestPath((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a <> b RETURN p`,
			`MATCH p = allShortestPaths((a:ZG)-[:ZEdge*1..]->(b:ZU)) WHERE a <> b RETURN p`,
		} {
			t.Run(query, func(t *testing.T) { g.assertServesOracle(t, query) })
		}
	})

	t.Run("self-loop", func(t *testing.T) {
		g := seedPathParityGraph(t, pathParityFixture{
			nodes: []pathParityNode{
				{name: "g1", kinds: []string{"ZG"}},
				{name: "x", kinds: []string{"ZU"}},
				{name: "g2", kinds: []string{"ZG"}},
			},
			edges: []pathParityEdge{{"g1", "g1", "ZEdge"}, {"g1", "x", "ZEdge"}, {"x", "g2", "ZEdge"}},
		})
		g.assertNeverWrong(t, `MATCH p = allShortestPaths((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a <> b RETURN p`)
		g.assertServesOracle(t, `MATCH p = shortestPath((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a <> b RETURN p`)
	})

	// A node carrying both endpoint kinds: a is a User and a Group on a
	// 2-cycle; the only other pair, c -> b, is three hops long.
	t.Run("node with both endpoint kinds", func(t *testing.T) {
		g := seedPathParityGraph(t, pathParityFixture{
			nodes: []pathParityNode{
				{name: "a", kinds: []string{"ZNode", "ZUser", "ZGroup"}},
				{name: "m", kinds: []string{"ZNode"}},
				{name: "c", kinds: []string{"ZNode", "ZUser"}},
				{name: "m2", kinds: []string{"ZNode"}},
				{name: "m3", kinds: []string{"ZNode"}},
				{name: "b", kinds: []string{"ZNode", "ZGroup"}},
			},
			edges: []pathParityEdge{
				{"a", "m", "ZEdge"}, {"m", "a", "ZEdge"},
				{"c", "m2", "ZEdge"}, {"m2", "m3", "ZEdge"}, {"m3", "b", "ZEdge"},
			},
		})
		g.assertNeverWrong(t, `MATCH p = allShortestPaths((s:ZUser)-[:ZEdge*1..]->(e:ZGroup)) WHERE s <> e RETURN p`)
		g.assertNeverWrong(t, `MATCH p = allShortestPaths((s:ZUser)-[:ZEdge*1..]->(e:ZGroup)) WHERE s.name = 'a' AND s <> e RETURN p`)
	})
}
