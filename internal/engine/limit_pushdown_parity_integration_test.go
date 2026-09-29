// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherLimitPushdownMatchesOracle compares LIMIT queries the engine
// answers by stopping a traversal early -- the reverse var-length walk and
// shortestPath -- with PostgreSQL. Both used to count rows before the WHERE
// that then dropped some of them, and served short answers: a nested group
// counted toward `(a:User OR a:Computer) ... LIMIT k` (k-1 rows back), a
// shortestPath terminal outside `(t:Group OR t:Domain)` counted toward its
// LIMIT, and a trail cycling back to its own seed counted toward `a <> t`.
//
// An unordered LIMIT may return any rows, so each served answer must have
// exactly as many rows as PostgreSQL's own answer to the same query, every
// one of them a row of PostgreSQL's unlimited answer. Each query must be
// SERVED: a decline would silently drop the comparison.
func TestTryCypherLimitPushdownMatchesOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	var (
		user, group    = graph.StringKind("LimUser"), graph.StringKind("LimGroup")
		domain, other  = graph.StringKind("LimDomain"), graph.StringKind("LimOther")
		computer       = graph.StringKind("LimComputer")
		memberOf, owns = graph.StringKind("LimMemberOf"), graph.StringKind("LimOwns")
	)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		node := func(kind graph.Kind, name, objectid string) (*graph.Node, error) {
			props := graph.NewProperties().Set("name", name)
			if objectid != "" {
				props.Set("objectid", objectid)
			}
			return tx.CreateNode(props, kind)
		}
		edge := func(from, to *graph.Node, kind graph.Kind) error {
			_, err := tx.CreateRelationshipByIDs(from.ID, to.ID, kind, graph.NewProperties())
			return err
		}

		// DA <- NG <- 20 users over LimMemberOf: the "All Domain Admins"
		// prebuilt's shape, where the walk reaches NG before any user.
		da, err := node(group, "DA", "S-1-5-21-9-512")
		if err != nil {
			return err
		}
		ng, err := node(group, "NG", "S-1-5-21-9-1105")
		if err != nil {
			return err
		}
		if err := edge(ng, da, memberOf); err != nil {
			return err
		}
		for i := 0; i < 20; i++ {
			u, err := node(user, fmt.Sprintf("U%d", i), fmt.Sprintf("S-1-5-21-9-%d", 2000+i))
			if err != nil {
				return err
			}
			if err := edge(u, ng, memberOf); err != nil {
				return err
			}
		}

		// X <-> T over LimOwns, 20 computers -> X: the walk from T reaches
		// T again through X. T is created last so the walk visits it first.
		x, err := node(group, "X", "S-1-5-21-9-1106")
		if err != nil {
			return err
		}
		for i := 0; i < 20; i++ {
			c, err := node(computer, fmt.Sprintf("C%d", i), "")
			if err != nil {
				return err
			}
			if err := edge(c, x, owns); err != nil {
				return err
			}
		}
		tier, err := node(group, "T", "S-1-5-21-9-519")
		if err != nil {
			return err
		}
		if err := edge(tier, x, owns); err != nil {
			return err
		}
		if err := edge(x, tier, owns); err != nil {
			return err
		}

		// Ten more users, each LimMemberOf the group G and of an LimOther
		// node named X*: every user has a shortest path to both.
		g, err := node(group, "G", "")
		if err != nil {
			return err
		}
		if _, err := node(domain, "D", ""); err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			u, err := node(user, fmt.Sprintf("SU%d", i), "")
			if err != nil {
				return err
			}
			o, err := node(other, fmt.Sprintf("XO%d", i), "")
			if err != nil {
				return err
			}
			if err := edge(u, o, memberOf); err != nil {
				return err
			}
			if err := edge(u, g, memberOf); err != nil {
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

	// render reads a single-column result as sorted strings: a node as its
	// id, a path as its node-id sequence, both through the result's own
	// Mapper (the engine and pg hand back different raw shapes). The column
	// kind comes from the query, not from probing: pg's mapper also "maps" a
	// node into a Path, as an empty one.
	render := func(result graph.Result, paths bool) ([]string, error) {
		defer result.Close()
		var out []string
		for result.Next() {
			v := result.Values()[0]
			if paths {
				var path graph.Path
				if !result.Mapper().Map(v, &path) || len(path.Nodes) == 0 {
					return nil, fmt.Errorf("column did not map to a path: %#v", v)
				}
				ids := make([]string, len(path.Nodes))
				for i, n := range path.Nodes {
					ids[i] = n.ID.String()
				}
				out = append(out, "path:"+strings.Join(ids, ","))
				continue
			}
			var node graph.Node
			if !result.Mapper().Map(v, &node) {
				return nil, fmt.Errorf("column did not map to a node: %#v", v)
			}
			out = append(out, "node:"+node.ID.String())
		}
		if err := result.Error(); err != nil {
			return nil, err
		}
		sort.Strings(out)
		return out, nil
	}
	returnsPath := func(query string) bool {
		return strings.HasSuffix(query, "RETURN p") || strings.Contains(query, "RETURN p LIMIT")
	}
	oracle := func(query string) []string {
		t.Helper()
		var rows []string
		if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			var err error
			rows, err = render(tx.Query(query, map[string]any{}), returnsPath(query))
			return err
		}); err != nil {
			t.Fatalf("PostgreSQL %s: %v", query, err)
		}
		return rows
	}

	for _, unlimited := range []string{
		`MATCH (t:LimGroup)<-[:LimMemberOf*1..]-(a) WHERE (a:LimUser OR a:LimComputer) AND t.objectid ENDS WITH '-512' RETURN a`,
		`MATCH p = (t:LimGroup)<-[:LimMemberOf*1..]-(a) WHERE (a:LimUser OR a:LimComputer) AND t.objectid ENDS WITH '-512' RETURN p`,
		`MATCH (t:LimGroup)<-[:LimMemberOf*1..]-(a) WHERE NOT a.objectid ENDS WITH '-1105' AND t.objectid ENDS WITH '-512' RETURN a`,
		`MATCH (t:LimGroup)<-[:LimOwns*1..]-(a) WHERE t.objectid ENDS WITH '-519' AND a <> t RETURN a`,
		`MATCH p = shortestPath((s:LimUser)-[:LimMemberOf*1..]->(t)) WHERE (t:LimGroup OR t:LimDomain) AND s <> t RETURN p`,
		`MATCH p = shortestPath((s:LimUser)-[:LimMemberOf*1..]->(t)) WHERE NOT t.name STARTS WITH 'X' AND s <> t RETURN p`,
		`MATCH p = shortestPath((s:LimUser)-[:LimMemberOf*1..]->(t)) WHERE (t:LimGroup) AND s <> t RETURN p`,
	} {
		t.Run(unlimited, func(t *testing.T) {
			all := oracle(unlimited)
			if len(all) < 3 {
				t.Fatalf("fixture too small to observe a short answer: PostgreSQL returns %d rows", len(all))
			}
			valid := map[string]bool{}
			for _, r := range all {
				valid[r] = true
			}
			for _, limit := range []int{1, 2, 3, 5, 8, len(all) + 1} {
				query := fmt.Sprintf("%s LIMIT %d", unlimited, limit)
				var (
					served     bool
					engineRows []string
				)
				if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
					var result graph.Result
					if result, served = eng.TryCypher(ctx, tx, query, nil); served {
						var err error
						engineRows, err = render(result, returnsPath(query))
						return err
					}
					return nil
				}); err != nil {
					t.Fatalf("engine %s: %v", query, err)
				}
				if !served {
					t.Fatalf("TryCypher declined a shape this test exists to compare: %s", query)
				}
				if want := len(oracle(query)); len(engineRows) != want {
					t.Fatalf("%s: engine served %d rows, PostgreSQL returns %d", query, len(engineRows), want)
				}
				for _, r := range engineRows {
					if !valid[r] {
						t.Fatalf("%s: engine served %s, which is not a row of PostgreSQL's unlimited answer", query, r)
					}
				}
			}
		})
	}
}
