// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// stubResultTx is a minimal graph.Transaction whose Query returns a
// pre-built graph.Result and whose GraphQueryMemoryLimit returns a real
// limit -- the only two methods ops.FetchByQuery's own code path reaches
// (see ops/ops.go:190: it calls tx.Query once, then tx.GraphQueryMemoryLimit
// per row). Every other method panics if called: reaching one would mean
// this test is exercising more of graph.Transaction than FetchByQuery
// actually needs, which should fail loudly rather than silently no-op.
type stubResultTx struct {
	result graph.Result
	limit  size.Size
}

func (s stubResultTx) WithGraph(graph.Graph) graph.Transaction {
	panic("stubResultTx: WithGraph not implemented")
}

func (s stubResultTx) CreateNode(*graph.Properties, ...graph.Kind) (*graph.Node, error) {
	panic("stubResultTx: CreateNode not implemented")
}

func (s stubResultTx) UpdateNode(*graph.Node) error {
	panic("stubResultTx: UpdateNode not implemented")
}

func (s stubResultTx) Nodes() graph.NodeQuery {
	panic("stubResultTx: Nodes not implemented")
}

func (s stubResultTx) CreateRelationshipByIDs(graph.ID, graph.ID, graph.Kind, *graph.Properties) (*graph.Relationship, error) {
	panic("stubResultTx: CreateRelationshipByIDs not implemented")
}

func (s stubResultTx) UpdateRelationship(*graph.Relationship) error {
	panic("stubResultTx: UpdateRelationship not implemented")
}

func (s stubResultTx) Relationships() graph.RelationshipQuery {
	panic("stubResultTx: Relationships not implemented")
}

func (s stubResultTx) Raw(string, map[string]any) graph.Result {
	panic("stubResultTx: Raw not implemented")
}

func (s stubResultTx) Query(string, map[string]any) graph.Result {
	return s.result
}

func (s stubResultTx) Commit() error {
	panic("stubResultTx: Commit not implemented")
}

func (s stubResultTx) GraphQueryMemoryLimit() size.Size {
	return s.limit
}

var _ graph.Transaction = stubResultTx{}

// drainEngineResult feeds result -- what TryCypher handed back -- through
// the real ops.FetchByQuery via stubResultTx, proving newPathResult /
// mapPathValue satisfy FetchByQuery's mapper protocol (relationship, then
// node, then path -- see ops/ops.go:190) rather than merely resembling it.
func drainEngineResult(t *testing.T, result graph.Result, text string) graph.PathSet {
	t.Helper()

	qr, err := ops.FetchByQuery(stubResultTx{result: result, limit: size.Gibibyte}, text)
	if err != nil {
		t.Fatalf("ops.FetchByQuery(engine result): %v", err)
	}
	return qr.Paths
}

// drainOracleResult runs text through the real ops.FetchByQuery against tx
// (a live transaction on the plain pg driver) -- the oracle side of the
// differential comparison.
func drainOracleResult(t *testing.T, tx graph.Transaction, text string) graph.PathSet {
	t.Helper()

	qr, err := ops.FetchByQuery(tx, text)
	if err != nil {
		t.Fatalf("ops.FetchByQuery(oracle): %v", err)
	}
	return qr.Paths
}

// cypherDiffCase is one TestTryCypherDifferential table entry: a Cypher text
// TryCypher must recognize and serve, compared canonically (assertSameSet)
// against ops.FetchByQuery run on the plain pg driver. Every case here is
// engineered to have a single deterministic shortest-path set (no ties in
// path length among candidate routes), so an exact canonical comparison is
// meaningful for both shortestPath (ModeOne) and allShortestPaths (ModeAll)
// texts alike.
type cypherDiffCase struct {
	name string
	text string
}

// TestTryCypherDifferential is Task 11's core evidence: for Cypher texts
// covering a chain shortestPath, a diamond allShortestPaths, an edge-kind-
// filtered shortestPath, and two WHERE-predicate shapes exercising the
// endpoint-predicate-lifting extension (NOT+COALESCE, regex =~), it drives
// TryCypher against a live pg transaction, drains the returned graph.Result
// through the real ops.FetchByQuery (via stubResultTx), and compares the
// result against ops.FetchByQuery run directly against the plain pg driver
// (never touching the engine).
func TestTryCypherDifferential(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	// The base fixture's nodes carry no properties at all (see
	// hydrateFixturePath's own doc), so the NOT/COALESCE and regex
	// differential cases below need their own small property-bearing
	// subgraph to actually discriminate on. propEdgeKind links two
	// disjoint 2-candidate fan-outs: one exercising a lifted NOT+COALESCE
	// predicate on the *start* endpoint, the other a lifted regex (=~)
	// predicate on the *end* endpoint -- deliberately covering both
	// endpoint positions.
	propEdgeKind := graph.StringKind("PropEdge")
	propNodeKind := graph.StringKind("PropNode")

	// Critical finding 1c's required differential coverage: a multi-label
	// pattern endpoint ((s:MultiA:MultiB)) combined with a lifted predicate
	// (forcing the Criteria path rather than the already-correct bitmap
	// path) must AND its labels together, matching Cypher's real semantics,
	// which the pg oracle enforces natively. multiKindEdge links two
	// candidate starts to the same end at equal (one-hop) distance:
	// multiMatchStart carries BOTH labels and must be the only start the
	// engine considers; multiDecoyStart carries only one of the two labels
	// and must be excluded. Before the fix, the engine's criteria() merged
	// both labels into a single array-overlap KindMatcher (OR semantics),
	// which would have wrongly admitted multiDecoyStart too -- since its
	// path to the end ties multiMatchStart's in length, that bug would have
	// produced a second, spurious allShortestPaths result the pg oracle
	// (real Cypher AND semantics) never returns.
	multiKindA := graph.StringKind("MultiKindA")
	multiKindB := graph.StringKind("MultiKindB")
	multiKindEdge := graph.StringKind("MultiKindEdge")

	var (
		notCoalesceEndID graph.ID
		regexStartID     graph.ID
		multiKindEndID   graph.ID
	)
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		// NOT + COALESCE case: two candidate starts, one filtered out.
		matchNode, err := tx.CreateNode(graph.NewProperties().Set("hasspn", true).Set("silent", false), propNodeKind)
		if err != nil {
			return err
		}
		excludedNode, err := tx.CreateNode(graph.NewProperties().Set("hasspn", true).Set("silent", true), propNodeKind)
		if err != nil {
			return err
		}
		endNode, err := tx.CreateNode(graph.NewProperties(), propNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(matchNode.ID, endNode.ID, propEdgeKind, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(excludedNode.ID, endNode.ID, propEdgeKind, graph.NewProperties()); err != nil {
			return err
		}
		notCoalesceEndID = endNode.ID

		// Regex case: two candidate ends, one filtered out.
		startNode, err := tx.CreateNode(graph.NewProperties(), propNodeKind)
		if err != nil {
			return err
		}
		adminNode, err := tx.CreateNode(graph.NewProperties().Set("name", "Administrator"), propNodeKind)
		if err != nil {
			return err
		}
		operatorNode, err := tx.CreateNode(graph.NewProperties().Set("name", "operator"), propNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(startNode.ID, adminNode.ID, propEdgeKind, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(startNode.ID, operatorNode.ID, propEdgeKind, graph.NewProperties()); err != nil {
			return err
		}
		regexStartID = startNode.ID

		// Multi-label AND-semantics case: two candidate starts tied at
		// distance 1 from the end, differing only in which labels they
		// carry.
		multiMatchStart, err := tx.CreateNode(graph.NewProperties().Set("name", "alice"), multiKindA, multiKindB)
		if err != nil {
			return err
		}
		multiDecoyStart, err := tx.CreateNode(graph.NewProperties().Set("name", "alice"), multiKindA)
		if err != nil {
			return err
		}
		multiEndNode, err := tx.CreateNode(graph.NewProperties(), propNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(multiMatchStart.ID, multiEndNode.ID, multiKindEdge, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(multiDecoyStart.ID, multiEndNode.ID, multiKindEdge, graph.NewProperties()); err != nil {
			return err
		}
		multiKindEndID = multiEndNode.ID

		return nil
	}); err != nil {
		t.Fatalf("seed property fixture: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, triggerManual, time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	cases := []cypherDiffCase{
		{
			name: "chain shortestPath with id() endpoints",
			text: fmt.Sprintf(`MATCH p = shortestPath((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p`, ids["c0"], ids["c10"]),
		},
		{
			name: "diamond allShortestPaths with id() endpoints",
			text: fmt.Sprintf(`MATCH p = allShortestPaths((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p`, ids["d0"], ids["d4"]),
		},
		{
			// A kind-filtered variant: restricting the relationship kind to
			// "Allowed" reaches s3 (mirrors engine_integration_test.go's
			// "edge-kind restricted to Allowed reaches s3" Criteria case,
			// here recognized from Cypher text instead).
			name: "kind-filtered variant restricted to Allowed edges",
			text: fmt.Sprintf(`MATCH p = shortestPath((s)-[:Allowed*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p`, ids["s0"], ids["s3"]),
		},
		{
			// Endpoint-predicate lifting: NOT + COALESCE scoped to the
			// start variable (modeled on the "Shortest paths to Domain
			// Admins from Kerberoastable users" prebuilt shape). Only
			// matchNode passes hasspn=true AND NOT COALESCE(silent,
			// false)=true; excludedNode (silent=true) must not appear as a
			// start.
			name: "NOT + COALESCE endpoint predicate",
			text: fmt.Sprintf(`MATCH p = shortestPath((s)-[:PropEdge*1..]->(e)) WHERE id(e) = %d AND s.hasspn = true AND NOT COALESCE(s.silent, false) = true RETURN p`, notCoalesceEndID),
		},
		{
			// Endpoint-predicate lifting: a regex (=~) match scoped to the
			// end variable (modeled on the "Shortest paths to privileged
			// roles" prebuilt shape). Only adminNode's name matches;
			// operatorNode must not appear as an end.
			name: "regex endpoint predicate",
			text: fmt.Sprintf(`MATCH p = shortestPath((s)-[:PropEdge*1..]->(e)) WHERE id(s) = %d AND e.name =~ '(?i)^admin.*$' RETURN p`, regexStartID),
		},
		{
			// Critical finding 1c: a multi-label endpoint ((s:MultiKindA:
			// MultiKindB)) whose Criteria path is exercised via a lifted
			// predicate (s.name = 'alice') must require BOTH labels (AND),
			// matching the pg oracle exactly -- excluding multiDecoyStart,
			// which carries only MultiKindA.
			name: "multi-label endpoint ANDs its kinds via the Criteria path",
			text: fmt.Sprintf(`MATCH p = allShortestPaths((s:MultiKindA:MultiKindB)-[:MultiKindEdge*1..]->(e)) WHERE id(e) = %d AND s.name = 'alice' RETURN p`, multiKindEndID),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var (
				engineResult graph.Result
				served       bool
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				engineResult, served = eng.TryCypher(ctx, tx, tc.text, nil)
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction: %v", err)
			}
			if !served {
				t.Fatalf("TryCypher declined, want served (query: %s)", tc.text)
			}

			engineOut := drainEngineResult(t, engineResult, tc.text)

			var oracleOut graph.PathSet
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				oracleOut = drainOracleResult(t, tx, tc.text)
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (oracle): %v", err)
			}

			assertSameSet(t, engineOut, oracleOut, false)
		})
	}
}

// TestTryCypherRejectsNonShortestPath is Task 11's required reject case: a
// query recognize.FromCypher cannot recognize (no shortestPath /
// allShortestPaths pattern at all) must decline rather than being served.
func TestTryCypherRejectsNonShortestPath(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, triggerManual, time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	var served bool
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		_, served = eng.TryCypher(ctx, tx, `MATCH (n) RETURN n`, nil)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if served {
		t.Fatalf("TryCypher served a non-shortestPath query, want declined")
	}
}

// TestTryCypherParams exercises the params check's exact contract: nil and
// an empty (but non-nil) map must both be treated as "no params" and served
// -- matching ops.FetchByQuery, the real upstream consumer, which always
// calls tx.Query(query, map[string]any{}) -- while any non-empty map must
// decline.
func TestTryCypherParams(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	ids := graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, triggerManual, time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	text := fmt.Sprintf(`MATCH p = shortestPath((s)-[*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p`, ids["c0"], ids["c10"])

	tryCypher := func(t *testing.T, params map[string]any) bool {
		t.Helper()
		var served bool
		if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, served = eng.TryCypher(ctx, tx, text, params)
			return nil
		}); err != nil {
			t.Fatalf("ReadTransaction: %v", err)
		}
		return served
	}

	t.Run("nil params served", func(t *testing.T) {
		if !tryCypher(t, nil) {
			t.Fatalf("TryCypher declined with nil params, want served")
		}
	})

	t.Run("empty params served", func(t *testing.T) {
		if !tryCypher(t, map[string]any{}) {
			t.Fatalf("TryCypher declined with an empty (non-nil) params map, want served")
		}
	})

	t.Run("non-empty params declined", func(t *testing.T) {
		if tryCypher(t, map[string]any{"x": 1}) {
			t.Fatalf("TryCypher served with a non-empty params map, want declined")
		}
	})
}
