// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherOrderByNameResolutionMatchesOracle compares ORDER BY names
// that are both a RETURN alias and something else -- a MATCH variable, a
// carried COUNT or constant, or another alias once PostgreSQL folds it to
// lower case -- with PostgreSQL. dawgs sorts a bound name by the binding
// (`RETURN g.v AS g ORDER BY g` is `order by s0.n0`) and emits any other
// name as the unquoted alias, so the engine, which sorted by the alias,
// served other rows (`... ORDER BY g LIMIT 1` returned [null] where
// PostgreSQL returns [5]) and served shapes PostgreSQL rejects (42803,
// 42P10, 42702).
//
// Row order is compared exactly: every query sorts. A shape the engine can
// not reproduce may decline, but if it is served it must be PostgreSQL's
// answer in PostgreSQL's order, and never where PostgreSQL raises an error;
// the shapes where the alias and the binding agree must stay served (their
// sort keys are unique, so PostgreSQL's order is fully determined).
func TestTryCypherOrderByNameResolutionMatchesOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	user, group, memberOf := graph.StringKind("OBUser"), graph.StringKind("OBGroup"), graph.StringKind("OBMemberOf")
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		nodes := map[string]*graph.Node{}
		for _, n := range []struct {
			name  string
			kind  graph.Kind
			props map[string]any
		}{
			{"u1", user, nil}, {"u2", user, nil}, {"u3", user, nil}, {"u4", user, nil},
			{"g1", group, map[string]any{"v": 5}},
			{"g2", group, map[string]any{"v": nil}},
			{"g3", group, nil},
			{"g4", group, map[string]any{"v": 1}},
		} {
			props := graph.NewProperties().Set("name", n.name)
			for k, v := range n.props {
				props.Set(k, v)
			}
			node, err := tx.CreateNode(props, n.kind)
			if err != nil {
				return err
			}
			nodes[n.name] = node
		}
		for _, e := range [][2]string{
			{"u1", "g1"},
			{"u1", "g2"}, {"u2", "g2"},
			{"u1", "g3"}, {"u2", "g3"}, {"u3", "g3"},
			{"u1", "g4"}, {"u2", "g4"}, {"u3", "g4"}, {"u4", "g4"},
		} {
			if _, err := tx.CreateRelationshipByIDs(nodes[e[0]].ID, nodes[e[1]].ID, memberOf, graph.NewProperties()); err != nil {
				return err
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

	// render reads every row in the order the result yields it: a node as
	// its name, a scalar as its Go type and value.
	render := func(result graph.Result) ([]string, error) {
		defer result.Close()
		var rows []string
		for result.Next() {
			row := ""
			for _, v := range result.Values() {
				var node graph.Node
				switch {
				case v == nil:
					row += "null|"
				case isScalarValue(v):
					row += fmt.Sprintf("%T:%v|", v, v)
				case result.Mapper().Map(v, &node) && node.Properties != nil && node.Properties.Exists("name"):
					name, _ := node.Properties.Get("name").String()
					row += "node:" + name + "|"
				default:
					row += fmt.Sprintf("%T:%v|", v, v)
				}
			}
			rows = append(rows, row)
		}
		return rows, result.Error()
	}

	const chain = `MATCH (u:OBUser)-[:OBMemberOf]->(g:OBGroup) `
	const counted = chain + `WITH g, count(u) AS c `
	for _, tc := range []struct {
		query     string
		mustServe bool
	}{
		// The alias shadows a MATCH variable.
		{`MATCH (g:OBGroup) RETURN g.v AS g ORDER BY g`, false},
		{`MATCH (g:OBGroup) RETURN g.v AS g ORDER BY g LIMIT 1`, false},
		{`MATCH (g:OBGroup) RETURN g.v AS g ORDER BY g DESC`, false},
		{`MATCH (g:OBGroup) RETURN DISTINCT g.v AS g ORDER BY g`, false},
		{chain + `RETURN g.name AS name, id(u) AS g ORDER BY g DESC LIMIT 2`, false},
		{chain + `RETURN count(u) AS u ORDER BY u`, false},
		{chain + `RETURN g, count(u) AS u ORDER BY u DESC`, false},
		// The alias shadows a carried COUNT or constant.
		{counted + `RETURN g.name AS name, g.v AS c ORDER BY c DESC LIMIT 1`, false},
		{counted + `RETURN g.name AS name, g.v AS c ORDER BY c LIMIT 2`, false},
		{counted + `RETURN DISTINCT g.v AS c ORDER BY c`, false},
		{counted + `RETURN g.v AS g ORDER BY g DESC LIMIT 2`, false},
		{`MATCH (g:OBGroup) WITH g, 1 AS k RETURN g.v AS k ORDER BY k`, false},
		// Two aliases PostgreSQL folds to one column name.
		{`MATCH (g:OBGroup) RETURN g.v AS x, id(g) AS X ORDER BY X DESC`, false},
		{chain + `RETURN g, count(u) AS c, count(DISTINCT u) AS C ORDER BY c DESC`, false},
		// A parenthesised name: dawgs emits `order by (i0)` (42703).
		{`MATCH (g:OBGroup) RETURN id(g) AS i ORDER BY (i)`, false},
		{`MATCH (g:OBGroup) RETURN g, id(g) AS i ORDER BY (i) LIMIT 1`, false},
		{chain + `RETURN g, count(u) AS c ORDER BY (c)`, false},
		{`MATCH (g:OBGroup) RETURN count(g) AS c ORDER BY (c)`, false},
		// A name PostgreSQL reserves, emitted unquoted: 42601, or -- user is
		// current_user there -- a constant sort key.
		{`MATCH (g:OBGroup) RETURN id(g) AS select ORDER BY select`, false},
		{`MATCH (g:OBGroup) RETURN id(g) AS table ORDER BY table`, false},
		{`MATCH (g:OBGroup) RETURN id(g) AS group ORDER BY group DESC`, false},
		{`MATCH (g:OBGroup) RETURN count(g) AS select ORDER BY select`, false},
		{`MATCH (g:OBGroup) RETURN g.v AS left ORDER BY left LIMIT 2`, false},
		{`MATCH (g:OBGroup) RETURN g.v AS user ORDER BY user`, false},
		// Alias and binding agree: still served.
		{counted + `RETURN g, c ORDER BY c`, true},
		{counted + `RETURN g, c ORDER BY c DESC LIMIT 2`, true},
		{counted + `RETURN g ORDER BY c DESC`, true},
		{counted + `RETURN g.name AS g ORDER BY c`, true},
		{chain + `RETURN g.name AS name, count(u) AS c ORDER BY c DESC`, true},
		{chain + `RETURN g, count(u) AS g2 ORDER BY g2 DESC`, true},
		{`MATCH (g:OBGroup) RETURN g.v AS gv ORDER BY gv`, true},
		{`MATCH (g:OBGroup) RETURN g.v AS g ORDER BY g.v`, true},
		{`MATCH (g:OBGroup) RETURN id(g) AS i ORDER BY i DESC`, true},
		{`MATCH (g:OBGroup) RETURN id(g) AS name ORDER BY name`, true},
		{`MATCH (g:OBGroup) RETURN id(g) AS value ORDER BY value DESC`, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			var (
				served     bool
				engineRows []string
				engineErr  error
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				var result graph.Result
				if result, served = eng.TryCypher(ctx, tx, tc.query, nil); served {
					engineRows, engineErr = render(result)
				}
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (engine): %v", err)
			}
			if !served {
				if tc.mustServe {
					t.Fatalf("TryCypher declined a shape where the alias and the binding agree")
				}
				return
			}
			if engineErr != nil {
				t.Fatalf("served result failed: %v", engineErr)
			}

			var (
				oracleRows []string
				oracleErr  error
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				oracleRows, oracleErr = render(tx.Query(tc.query, map[string]any{}))
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (oracle): %v", err)
			}
			if oracleErr != nil {
				t.Fatalf("engine served %v; PostgreSQL raises an error: %v", engineRows, oracleErr)
			}
			if !reflect.DeepEqual(engineRows, oracleRows) {
				t.Fatalf("engine served %v, PostgreSQL returns %v", engineRows, oracleRows)
			}
		})
	}
}
