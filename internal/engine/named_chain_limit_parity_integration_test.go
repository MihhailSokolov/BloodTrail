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

// TestTryCypherNamedBranchingChainLimitMatchesOracle compares named paths
// over fixed chains whose arrows do not all point one way -- BloodHound's
// co-membership shape `(a)-[:MemberOf]->(g)<-[:MemberOf]-(b)` and its
// "members' admin rights" shape `(g)<-[:MemberOf]-(u)-[:AdminTo]->(c)` --
// with PostgreSQL once a LIMIT or RETURN DISTINCT is added. Those clauses
// route the query through the chunked LIMIT driver or the DISTINCT
// streamer, which scanned one end of the chain while the walk they handed
// the rows to started from another, and the engine served an empty answer
// where PostgreSQL returns every path.
//
// Every query must be SERVED (a decline would silently drop the
// comparison). An unordered LIMIT may return any rows, so each answer must
// have exactly as many rows as PostgreSQL's, every one a row of
// PostgreSQL's unlimited answer.
func TestTryCypherNamedBranchingChainLimitMatchesOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	var (
		user, group, computer = graph.StringKind("BCUser"), graph.StringKind("BCGroup"), graph.StringKind("BCComputer")
		memberOf, adminTo     = graph.StringKind("BCMemberOf"), graph.StringKind("BCAdminTo")
	)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		nodes := map[string]*graph.Node{}
		for _, n := range []struct {
			name string
			kind graph.Kind
		}{
			// C0 is created first, so it is the replica's dense node 0: the
			// node an expansion from an unbound symbol used to start from.
			{"C0", computer},
			{"G0", group}, {"G1", group},
			{"U1", user}, {"U2", user}, {"U3", user},
			{"C1", computer}, {"C2", computer},
		} {
			node, err := tx.CreateNode(graph.NewProperties().Set("name", n.name).Set("objectid", "S-1-5-21-7-"+n.name), n.kind)
			if err != nil {
				return err
			}
			nodes[n.name] = node
		}
		for _, e := range []struct {
			from, to string
			kind     graph.Kind
		}{
			{"U1", "G1", memberOf}, {"U2", "G1", memberOf}, {"U3", "G1", memberOf}, {"U2", "G0", memberOf},
			{"U1", "C1", adminTo}, {"U2", "C2", adminTo},
		} {
			if _, err := tx.CreateRelationshipByIDs(nodes[e.from].ID, nodes[e.to].ID, e.kind, graph.NewProperties()); err != nil {
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

	// render reads a single path column as sorted node-name sequences.
	render := func(result graph.Result) ([]string, error) {
		defer result.Close()
		var out []string
		for result.Next() {
			var path graph.Path
			if !result.Mapper().Map(result.Values()[0], &path) || len(path.Nodes) == 0 {
				return nil, fmt.Errorf("column did not map to a path: %#v", result.Values()[0])
			}
			names := make([]string, len(path.Nodes))
			for i, n := range path.Nodes {
				names[i], _ = n.Properties.Get("name").String()
			}
			out = append(out, fmt.Sprintf("%s/%d", strings.Join(names, ">"), len(path.Edges)))
		}
		if err := result.Error(); err != nil {
			return nil, err
		}
		sort.Strings(out)
		return out, nil
	}
	oracle := func(t *testing.T, query string) []string {
		t.Helper()
		var rows []string
		if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			var err error
			rows, err = render(tx.Query(query, map[string]any{}))
			return err
		}); err != nil {
			t.Fatalf("PostgreSQL %s: %v", query, err)
		}
		return rows
	}

	for _, tc := range []struct {
		unlimited string
		distinct  bool
	}{
		{`MATCH p = (a:BCUser)-[:BCMemberOf]->(g:BCGroup)<-[:BCMemberOf]-(b:BCUser) WHERE b.objectid = 'S-1-5-21-7-U2' RETURN p`, true},
		{`MATCH p = (a:BCUser)-[:BCMemberOf]->(g:BCGroup)<-[:BCMemberOf]-(b:BCUser {objectid: 'S-1-5-21-7-U2'}) RETURN p`, true},
		{`MATCH p = (g:BCGroup {objectid: 'S-1-5-21-7-G1'})<-[:BCMemberOf]-(u:BCUser)-[:BCAdminTo]->(c:BCComputer) RETURN p`, true},
		// After a WITH boundary each path repeats once per seed, so DISTINCT
		// is not the same answer; LIMIT exercises the carried driver.
		{`MATCH (x:BCComputer) WITH x MATCH p = (g:BCGroup {objectid: 'S-1-5-21-7-G1'})<-[:BCMemberOf]-(u:BCUser)-[:BCAdminTo]->(c:BCComputer) RETURN p`, false},
	} {
		t.Run(tc.unlimited, func(t *testing.T) {
			all := oracle(t, tc.unlimited)
			if len(all) < 2 {
				t.Fatalf("fixture too small to observe a short answer: PostgreSQL returns %d rows", len(all))
			}
			valid := map[string]bool{}
			for _, r := range all {
				valid[r] = true
			}
			variants := []string{tc.unlimited + " LIMIT 1", tc.unlimited + " LIMIT 100"}
			if tc.distinct {
				distinct := strings.Replace(tc.unlimited, "RETURN p", "RETURN DISTINCT p", 1)
				variants = append(variants, distinct, distinct+" LIMIT 100")
			}
			for _, query := range variants {
				var (
					served     bool
					engineRows []string
				)
				if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
					var result graph.Result
					if result, served = eng.TryCypher(ctx, tx, query, nil); served {
						var err error
						engineRows, err = render(result)
						return err
					}
					return nil
				}); err != nil {
					t.Fatalf("engine %s: %v", query, err)
				}
				if !served {
					t.Fatalf("TryCypher declined a shape this test exists to compare: %s", query)
				}
				if want := oracle(t, query); len(engineRows) != len(want) {
					t.Fatalf("%s: engine served %v, PostgreSQL returns %v", query, engineRows, want)
				}
				for _, r := range engineRows {
					if !valid[r] {
						t.Fatalf("%s: engine served %s, which is not a row of PostgreSQL's unlimited answer %v", query, r, all)
					}
				}
			}
		})
	}
}
