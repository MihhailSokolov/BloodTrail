// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherReturnAggregatesMatchOracle compares served RETURN-position
// aggregates with PostgreSQL row for row, column TYPES included. It pins
// three things the other differentials could not see: COUNT(sym) over an
// OPTIONAL MATCH skips null-padded rows (the engine used to count them),
// a bare node next to an aggregate groups by node identity and projects the
// node (it used to project the property map, merging two nodes whose
// properties are identical), and a RETURN-position count comes back as
// pg's int8 (it used to come back as float64). The random differential
// compares values through a type-blind rendering, so it passed all three.
func TestTryCypherReturnAggregatesMatchOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	user, group, computer := graph.StringKind("AggUser"), graph.StringKind("AggGroup"), graph.StringKind("AggComputer")
	memberOf, session := graph.StringKind("AggMemberOf"), graph.StringKind("AggHasSession")

	// The OPTIONAL MATCH fixture: u0 fans out to two groups, u1 to one, u2
	// has the wrong edge kind, u3 an edge to a non-group, u4/u5 nothing.
	// u4 and u5 carry IDENTICAL properties, so grouping by property map
	// instead of identity is visible.
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var users []*graph.Node
		for i := 0; i < 6; i++ {
			name := fmt.Sprintf("U%d", i)
			if i >= 4 {
				name = "twin"
			}
			n, err := tx.CreateNode(graph.NewProperties().Set("name", name), user)
			if err != nil {
				return err
			}
			users = append(users, n)
		}
		ga, err := tx.CreateNode(graph.NewProperties().Set("name", "GA"), group)
		if err != nil {
			return err
		}
		gb, err := tx.CreateNode(graph.NewProperties().Set("name", "GB"), group)
		if err != nil {
			return err
		}
		c0, err := tx.CreateNode(graph.NewProperties().Set("name", "C0"), computer)
		if err != nil {
			return err
		}
		for _, e := range []struct {
			from, to *graph.Node
			kind     graph.Kind
		}{
			{users[0], ga, memberOf}, {users[0], gb, memberOf}, {users[1], ga, memberOf},
			{users[2], ga, session}, {users[3], c0, memberOf},
		} {
			if _, err := tx.CreateRelationshipByIDs(e.from.ID, e.to.ID, e.kind, graph.NewProperties()); err != nil {
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

	// renderRows reads every row into a sorted list of strings: a node as
	// its id (the engine hands back *graph.Node, pg a composite, so both go
	// through the result's own Mapper), anything else as its Go type and
	// value -- the type is the point for count's int8.
	renderRows := func(t *testing.T, result graph.Result) []string {
		t.Helper()
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
				case result.Mapper().Map(v, &node):
					row += fmt.Sprintf("node:%d|", node.ID)
				default:
					row += fmt.Sprintf("%T:%v|", v, v)
				}
			}
			rows = append(rows, row)
		}
		if err := result.Error(); err != nil {
			t.Fatalf("result.Error(): %v", err)
		}
		sort.Strings(rows)
		return rows
	}

	for _, query := range []string{
		`MATCH (u:AggUser) OPTIONAL MATCH (u)-[:AggMemberOf]->(g:AggGroup) RETURN count(g)`,
		`MATCH (u:AggUser) OPTIONAL MATCH (u)-[:AggMemberOf]->(g:AggGroup) RETURN count(*)`,
		`MATCH (u:AggUser) OPTIONAL MATCH (u)-[:AggMemberOf]->(g:AggGroup) RETURN count(DISTINCT g)`,
		`MATCH (u:AggUser) OPTIONAL MATCH (u)-[:AggMemberOf]->(g:AggGroup) RETURN u, count(g)`,
		`MATCH (u:AggUser) OPTIONAL MATCH (u)-[:AggMemberOf]->(g:AggGroup) RETURN g, count(u)`,
		`MATCH (u:AggUser) OPTIONAL MATCH (u)-[:AggMemberOf]->(g:AggGroup) RETURN u.name, count(g)`,
		`MATCH (u:AggUser) OPTIONAL MATCH (u)-[:AggMemberOf]->(g:AggGroup) WITH count(g) AS c RETURN c`,
		`MATCH (u:AggUser) RETURN count(u)`,
		`MATCH (u:AggUser) RETURN u, count(u)`,
		`MATCH (u:AggUser) RETURN u.name, count(u)`,
		`MATCH (u:AggUser)-[:AggMemberOf]->(g:AggGroup) RETURN g, count(u) AS members`,
	} {
		t.Run(query, func(t *testing.T) {
			var engineRows []string
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				result, served := eng.TryCypher(ctx, tx, query, nil)
				if !served {
					t.Fatalf("TryCypher declined a shape this test exists to pin as served: %s", query)
				}
				engineRows = renderRows(t, result)
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (engine): %v", err)
			}

			var oracleRows []string
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				oracleRows = renderRows(t, tx.Query(query, map[string]any{}))
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (oracle): %v", err)
			}

			if !reflect.DeepEqual(engineRows, oracleRows) {
				t.Fatalf("engine rows:\n  %v\nPostgreSQL rows:\n  %v", engineRows, oracleRows)
			}
		})
	}
}

func isScalarValue(v any) bool {
	switch v.(type) {
	case bool, string, int, int8, int16, int32, int64, float32, float64:
		return true
	}
	return false
}
