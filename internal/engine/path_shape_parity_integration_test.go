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

// TestShortestPathPairFilterLimitOverSharedEndpointMatchesOracle: when both
// endpoints carry a property or id condition, dawgs searches pair by pair
// (bidirectional_sp_harness with a pair filter) and pushes a bare LIMIT into
// that search. The pair filter is the plain product of the two endpoint sets,
// so it includes (g1, g1); the harness resolves that pair around g1's cycle
// and counts it toward the LIMIT, and `a <> b` drops it only afterwards --
// PostgreSQL returns fewer rows than LIMIT. Without the LIMIT, with kind-only
// endpoints (the unidirectional harness never revisits a root) or with
// DISTINCT (no pushdown), the answers agree and must keep serving.
func TestShortestPathPairFilterLimitOverSharedEndpointMatchesOracle(t *testing.T) {
	g := seedPathParityGraph(t, selfCycleFixture())
	for _, query := range []string{
		`MATCH p = shortestPath((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a.name IN ['g1', 'g2'] AND b.name IN ['g1', 'g2'] AND a <> b RETURN p LIMIT 1`,
		`MATCH p = shortestPath((a)-[:ZEdge*1..]->(b)) WHERE a.name = 'g1' AND b.name IN ['g1', 'g2'] AND a <> b RETURN p LIMIT 1`,
	} {
		t.Run(query, func(t *testing.T) { g.assertNeverWrong(t, query) })
	}
	for _, query := range []string{
		`MATCH p = shortestPath((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a.name IN ['g1', 'g2'] AND b.name IN ['g1', 'g2'] AND a <> b RETURN p`,
		`MATCH p = shortestPath((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a.name IN ['g1', 'g2'] AND b.name IN ['g1', 'g2'] AND a <> b RETURN DISTINCT p LIMIT 1`,
		`MATCH p = shortestPath((a:ZG)-[:ZEdge*1..]->(b:ZG)) WHERE a <> b RETURN p LIMIT 1`,
	} {
		t.Run(query, func(t *testing.T) { g.assertServesOracle(t, query) })
	}
}

// TestShortestPathAfterEarlierBindingMatchesOracle: dawgs compiles a
// shortestPath/allShortestPaths pattern into a harness call whose frame
// projects every earlier frame's bindings but joins an earlier frame only in
// two cases, and whose endpoint filters run as SQL text inside plpgsql
// EXECUTE, where no outer CTE is visible. So an earlier pattern in the same
// query part -- a separate MATCH clause or a comma-separated pattern, bound
// to the endpoints or not -- or an endpoint carried through a WITH is 42P01
// ("missing FROM-clause entry" / "relation does not exist"). After a WITH
// the harness frame joins the carried frame only when a condition on it
// lands on the side dawgs' selectivity model picks as the seed, so pg
// answers some spellings and rejects near-identical ones (all below).
func TestShortestPathAfterEarlierBindingMatchesOracle(t *testing.T) {
	g := seedPathParityGraph(t, shortestPathLevelFixture())

	for _, query := range []string{
		// An earlier pattern in the same query part.
		`MATCH (x:ZTerm) MATCH p = shortestPath((a:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) RETURN p, x`,
		`MATCH (a:ZRoot) WHERE a.name = 'r0' MATCH p = shortestPath((a)-[:ZEdge*1..]->(b:ZTerm)) RETURN p`,
		`MATCH (a:ZRoot), (b:ZTerm) MATCH p = shortestPath((a)-[:ZEdge*1..]->(b)) RETURN p`,
		`MATCH (a:ZRoot), (b:ZTerm) MATCH p = allShortestPaths((a)-[:ZEdge*1..]->(b)) RETURN p`,
		`MATCH (a:ZRoot) MATCH p = allShortestPaths((a)-[:ZEdge*1..]->(b:ZTerm)) RETURN p`,
		`MATCH (s:ZRoot), p = shortestPath((s)-[:ZEdge*1..]->(t:ZTerm)) RETURN p`,
		`MATCH (a:ZRoot)-[:ZEdge]->(b), p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(t:ZTerm)) WHERE s <> t RETURN a`,
		// After a WITH.
		`MATCH (x:ZRoot) WITH x MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) RETURN p`,
		`MATCH (x:ZRoot) WITH count(x) AS c MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) RETURN p`,
		`WITH 1 AS one MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) RETURN p`,
		`MATCH (x:ZRoot) WHERE x.name = 'r0' WITH x MATCH p = shortestPath((x)-[:ZEdge*1..]->(b:ZTerm)) RETURN p`,
		`MATCH (x:ZRoot) WITH x MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) WHERE b <> x RETURN p`,
		`MATCH (x:ZRoot) WITH x MATCH p = shortestPath((b:ZTerm)<-[:ZEdge*1..]-(s:ZRoot)) WHERE s <> x RETURN p`,
		`MATCH (x:ZRoot) WITH x MATCH p = allShortestPaths((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) WHERE s.name = x.name AND b.name = 't9' RETURN p`,
		`MATCH (x:ZRoot) WITH x MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) WHERE s <> x AND (s.foo = 1 OR s.bar = 2) RETURN p`,
		// ...and spellings pg happens to answer, declined with the rest.
		`MATCH (x:ZRoot) WITH x MATCH p = shortestPath((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) WHERE s.name = x.name RETURN p`,
		`MATCH (a:ZRoot) WHERE a.name IN ['r0', 'r4'] WITH a MATCH p = allShortestPaths((s:ZRoot)-[:ZEdge*1..]->(b:ZTerm)) WHERE s.name = a.name RETURN p`,
	} {
		t.Run(query, func(t *testing.T) { g.assertNeverWrong(t, query) })
	}

	g.assertServesOracle(t, `MATCH p = shortestPath((s:ZRoot {name: 'r0'})-[:ZEdge*1..]->(b:ZTerm)) RETURN p`)
}

// TestShortestPathLaterPatternMatchesOracle: dawgs hands a shortest-path
// harness only the conditions of the pattern's own MATCH clause, and applies
// a later clause's WHERE, labels and inline map after the harness has picked
// its paths; a later pattern re-mentioning an endpoint is a further cross
// join. The engine pooled every clause's WHERE into the endpoint
// constraints, so a later `t.name = 'far'` let allShortestPaths skip the
// one-hop pair (pg: the harness answers at depth 1, the filter then leaves
// nothing), a later `s <> t` hid pg's 22023, and a later unrelated filter is
// itself 42P01 in pg. The same condition in the pattern's own clause must
// keep serving.
//
//	a1 -ZR-> near(ZB)            (1 hop)
//	a1 -ZR-> mid -ZR-> far(ZB,ZT) (2 hops)
//	both(ZA,ZB) -ZQ-> far -ZQ-> both
func TestShortestPathLaterPatternMatchesOracle(t *testing.T) {
	g := seedPathParityGraph(t, pathParityFixture{
		nodes: []pathParityNode{
			{name: "a1", kinds: []string{"ZA"}},
			{name: "near", kinds: []string{"ZB"}},
			{name: "mid", kinds: []string{"ZM"}},
			{name: "far", kinds: []string{"ZB", "ZT"}},
			{name: "x", kinds: []string{"ZX"}},
			{name: "both", kinds: []string{"ZA", "ZB"}},
		},
		edges: []pathParityEdge{
			{"a1", "near", "ZR"}, {"a1", "mid", "ZR"}, {"mid", "far", "ZR"},
			{"both", "far", "ZQ"}, {"far", "both", "ZQ"},
		},
	})

	for _, query := range []string{
		`MATCH p = allShortestPaths((s:ZA)-[:ZR*1..]->(t:ZB)) MATCH (x:ZX) WHERE t.name = 'far' RETURN p`,
		`MATCH p = allShortestPaths((s:ZA)-[:ZR*1..]->(t:ZB)) MATCH (x:ZX) WHERE x.name = 'x' AND t.name = 'far' RETURN p`,
		`MATCH p = allShortestPaths((s:ZA)-[:ZR*1..]->(t:ZB)) MATCH (t:ZT) RETURN p`,
		`MATCH p = allShortestPaths((s:ZA)-[:ZR*1..]->(t:ZB)) MATCH (t {name: 'far'}) RETURN p`,
		`MATCH p = allShortestPaths((s:ZA)-[:ZR*1..]->(t:ZB)), (t:ZT) RETURN p`,
		`MATCH p = shortestPath((s:ZA)-[:ZR*1..]->(t:ZB)), (s {name: 'a1'}) RETURN p`,
		`MATCH p = shortestPath((s:ZA)-[:ZQ*1..]->(t:ZB)) MATCH (x:ZX) WHERE s <> t RETURN p`,
		`MATCH p = shortestPath((s:ZA)-[:ZR*1..]->(t:ZB)) MATCH (x:ZX) WHERE x.name = 'x' RETURN p, x`,
	} {
		t.Run(query, func(t *testing.T) { g.assertNeverWrong(t, query) })
	}

	g.assertServesOracle(t, `MATCH p = allShortestPaths((s:ZA)-[:ZR*1..]->(t:ZB)) WHERE t.name = 'far' RETURN p`)
}
