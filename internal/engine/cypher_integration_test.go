// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

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
// the real ops.FetchByQuery via stubResultTx, proving cypherRowsResult
// (serve_cypher.go) satisfies FetchByQuery's mapper protocol (relationship,
// then node, then path -- see ops/ops.go:190) rather than merely resembling
// it.
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

// TestTryCypherDifferential is TryCypher's core evidence: for Cypher texts
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

	// Regression coverage for the reversed-single-step named-path fix
	// (assembleChainPathVal, exec.go): a named path whose one-step pattern
	// is written with a backward arrow, mirroring the corpus's own common
	// `(g:Group)<-[:MemberOf]-(u:User)` shape. groupKind/userKind/
	// memberOfKind are unique to this fixture (no collision with
	// hydrateFixturePath's own kind names), so the differential case below
	// matches exactly this one pair -- letting assertSameSet's canonicalized
	// per-path node ORDER comparison catch a regression directly, not just
	// the node/edge set.
	groupKind := graph.StringKind("Group")
	userKind := graph.StringKind("User")
	memberOfKind := graph.StringKind("MemberOf")

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

	// hydratedEdgeKind links two candidate starts to the same end, exactly
	// like multiKindEdge above, but carries a real, non-empty property bag
	// (unlike every other edge kind seeded in this fixture, which uses
	// graph.NewProperties() -- empty) -- deliberate differential
	// coverage: a path query whose edges actually need the
	// hydrateEdgePropsByID round trip, not just the CSR-only shape
	// hydration-less edges would already get right for free. assertSameSet
	// (via canonicalize) compares edge Properties byte for byte, so a
	// hydration bug (wrong edge id batched, wrong property decoded, or no
	// hydration attempted at all) would show up here as a property mismatch
	// even though the path's own node/edge shape matched.
	hydratedEdgeKind := graph.StringKind("HydratedPropEdge")

	var (
		notCoalesceEndID    graph.ID
		regexStartID        graph.ID
		multiKindEndID      graph.ID
		hydratedEdgeStartID graph.ID
		hydratedEdgeEndID   graph.ID
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

		// Hydrated-edge-properties case: a shortestPath whose one edge
		// carries a real property bag, so the differential comparison below
		// (assertSameSet, via canonicalize) actually exercises the
		// hydrateEdgePropsByID round trip -- every other edge kind in this
		// fixture is seeded with graph.NewProperties() (empty), which a
		// completely unhydrated edge (a zero-value *graph.Properties) would
		// satisfy just as well, so none of them alone would catch a
		// hydration bug.
		hydratedStart, err := tx.CreateNode(graph.NewProperties(), propNodeKind)
		if err != nil {
			return err
		}
		hydratedEnd, err := tx.CreateNode(graph.NewProperties(), propNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(hydratedStart.ID, hydratedEnd.ID, hydratedEdgeKind,
			graph.NewProperties().Set("since", "2020-01-01").Set("weight", float64(7)),
		); err != nil {
			return err
		}
		hydratedEdgeEndID = hydratedEnd.ID
		hydratedEdgeStartID = hydratedStart.ID

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

		// Duplicate-objectid differential coverage: PostgreSQL enforces no
		// uniqueness constraint on objectid, so real data can (and does)
		// contain more than one node sharing the same value. An objectid
		// anchor that silently picked just one match would under-serve
		// relative to the pg oracle, which naturally returns both rows for
		// this query.
		if _, err := tx.CreateNode(graph.NewProperties().Set("objectid", "DUP-OID"), propNodeKind); err != nil {
			return err
		}
		if _, err := tx.CreateNode(graph.NewProperties().Set("objectid", "DUP-OID"), propNodeKind); err != nil {
			return err
		}

		// Reversed-single-step named-path case: one User MemberOf one
		// Group, queried as `(g:Group)<-[:MemberOf]-(u:User)` below.
		reversedGroupNode, err := tx.CreateNode(graph.NewProperties(), groupKind)
		if err != nil {
			return err
		}
		reversedUserNode, err := tx.CreateNode(graph.NewProperties(), userKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(reversedUserNode.ID, reversedGroupNode.ID, memberOfKind, graph.NewProperties()); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("seed property fixture: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
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
		{
			// Plain-property-MATCH coverage: a bare
			// MATCH/WHERE/RETURN with no shortestPath/allShortestPaths
			// pattern at all -- exactly the general-purpose shape the
			// retired milestone-2 recognizer could never serve, and the
			// interpreter now can. adminNode's name is unique across this
			// entire fixture, so the match (and therefore the aggregated
			// single-node "path" ops.FetchByQuery builds from a plain node
			// RETURN -- see FetchByQuery's own doc) is exactly one row on
			// both sides.
			name: "plain property MATCH (no shortestPath at all)",
			text: `MATCH (n:PropNode) WHERE n.name = 'Administrator' RETURN n`,
		},
		{
			// Two PropNode instances share objectid "DUP-OID" (seeded
			// above); both must come back, matching the pg oracle's row
			// count exactly.
			name: "duplicate objectid anchor returns every match",
			text: `MATCH (n:PropNode) WHERE n.objectid = 'DUP-OID' RETURN n`,
		},
		{
			// Regression coverage for the assembleChainPathVal fix: a
			// one-step named path written with a backward arrow
			// (buildStep's inbound-arrow swap sets Step.Reversed=true) must
			// still render its PathVal in the pattern's WRITTEN order (g,
			// then u), matching the pg oracle -- not the executor's own
			// internal TRAVERSAL order (u, then g), which is what this bug
			// produced before the fix. assertSameSet's canonicalize keeps
			// each path's own node order intact (only the SET of paths is
			// order-independent), so a regression here would show up as a
			// path-set mismatch even though both sides agree on which
			// node/edge instances are involved.
			name: "reversed single-step named path renders pattern-written order",
			text: `MATCH p = (g:Group)<-[:MemberOf]-(u:User) RETURN p LIMIT 5`,
		},
		{
			// Coverage for a path query asserting that hydrated edge
			// properties equal pg's: hydratedEdgeKind's one edge
			// carries a real property bag (since/weight), so this
			// differential comparison actually exercises the
			// hydrateEdgePropsByID round trip -- assertSameSet's
			// canonicalize compares edge Properties byte for byte, so a
			// hydration bug would show up here as a property mismatch even
			// though the path's own node/edge shape matched.
			name: "shortestPath with a real hydrated edge property bag",
			text: fmt.Sprintf(`MATCH p = shortestPath((s)-[:HydratedPropEdge*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p`, hydratedEdgeStartID, hydratedEdgeEndID),
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

// TestTryCypherRejectsNonShortestPath was originally the reject case
// for the retired milestone-2 recognizer (recognize.FromCypher), which
// only ever recognized shortestPath/allShortestPaths shapes -- so a plain
// `MATCH (n) RETURN n` was, at the time, the simplest possible "declined"
// example. The interpreter-backed TryCypher serves that exact shape
// directly (see TestTryCypherDifferential's own "plain property MATCH (no
// shortestPath at all)" case above), so this case is renamed and re-pointed
// at a shape the interpreter itself still declines:
// interpret.Plan's default-deny posture rejects every UpdatingClause
// (CREATE/MERGE/SET/DELETE/REMOVE) outright, regardless of what RETURN item
// follows it (see interpret/plan.go's planStages) -- a stable, permanent
// reject case, unlike a mere "not yet implemented" gap that a future
// interpreter feature could close out from under this test.
func TestTryCypherRejectsUpdatingClause(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)
	graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	var served bool
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		_, served = eng.TryCypher(ctx, tx, `CREATE (n:NewNode) RETURN n`, nil)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if served {
		t.Fatalf("TryCypher served a CREATE query, want declined")
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
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
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

// TestTryCypherAggregationQuery is the "corpus aggregation
// query" coverage: interpret/plan_test.go's unexported corpusAggregationQuery
// (migrated verbatim from BloodHound's real analysis-query corpus,
// "Kerberoastable users with most admin privileges" -- duplicated here byte
// for byte, the same way gate_test.go's corpusAggregationQueryText already
// does, since it is unexported and defined in a _test.go file, so invisible
// outside package interpret) -- a WITH DISTINCT/COUNT/ORDER BY/LIMIT query
// with no shortestPath/allShortestPaths pattern at all -- served against a
// small property-bearing User/Computer subgraph engineered for exactly one
// qualifying user (two decoys, one per negative WHERE conjunct, confirm the
// filter is not vacuously true), so the RETURN u result -- and therefore the
// single aggregated "path" ops.FetchByQuery's own node/path mapping builds
// from a plain node RETURN (see FetchByQuery's own doc) -- is unambiguous on
// both the engine and pg oracle sides.
func TestTryCypherAggregationQuery(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	kindUser := graph.StringKind("User")
	kindComputer := graph.StringKind("Computer")
	kindAdminTo := graph.StringKind("AdminTo")

	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		computer, err := tx.CreateNode(graph.NewProperties(), kindComputer)
		if err != nil {
			return err
		}

		// goodUser satisfies every WHERE conjunct and has an AdminTo edge to
		// computer, so it is the query's one and only result.
		goodUser, err := tx.CreateNode(graph.NewProperties().
			Set("hasspn", true).Set("enabled", true).Set("objectid", "OBJ-GOOD-1"), kindUser)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(goodUser.ID, computer.ID, kindAdminTo, graph.NewProperties()); err != nil {
			return err
		}

		// badUserGMSA fails NOT COALESCE(u.gmsa, false) = true.
		badUserGMSA, err := tx.CreateNode(graph.NewProperties().
			Set("hasspn", true).Set("enabled", true).Set("objectid", "OBJ-BAD-GMSA").Set("gmsa", true), kindUser)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(badUserGMSA.ID, computer.ID, kindAdminTo, graph.NewProperties()); err != nil {
			return err
		}

		// badUserDisabled fails u.enabled = true.
		badUserDisabled, err := tx.CreateNode(graph.NewProperties().
			Set("hasspn", true).Set("enabled", false).Set("objectid", "OBJ-BAD-DISABLED"), kindUser)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(badUserDisabled.ID, computer.ID, kindAdminTo, graph.NewProperties()); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("seed aggregation fixture: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	const text = `MATCH (u:User)
WHERE u.hasspn = true
  AND u.enabled = true
  AND NOT u.objectid ENDS WITH '-502'
  AND NOT COALESCE(u.gmsa, false) = true
  AND NOT COALESCE(u.msa, false) = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100`

	var (
		engineResult graph.Result
		served       bool
	)
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		engineResult, served = eng.TryCypher(ctx, tx, text, nil)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if !served {
		t.Fatalf("TryCypher declined, want served (query: %s)", text)
	}

	engineOut := drainEngineResult(t, engineResult, text)

	var oracleOut graph.PathSet
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		oracleOut = drainOracleResult(t, tx, text)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction (oracle): %v", err)
	}

	assertSameSet(t, engineOut, oracleOut, false)
}

// TestTryCypherCollectAntiJoinQuery is the "*0.. COLLECT
// anti-join" coverage: interpret/plan_test.go's unexported
// corpusCollectAntiJoinQuery (migrated verbatim from BloodHound's real
// analysis-query corpus, "Domain Admins logons to non-Domain Controllers" --
// duplicated here for the same reason TestTryCypherAggregationQuery's own
// doc explains) -- a two-Part query joined by a WITH COLLECT(...) boundary,
// whose second Part excludes every node the first collected via
// `NOT c IN exclude` -- served against a small Computer/User/Group subgraph
// engineered for exactly one non-excluded path.
func TestTryCypherCollectAntiJoinQuery(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	kindUser := graph.StringKind("User")
	kindComputer := graph.StringKind("Computer")
	kindGroup := graph.StringKind("Group")
	kindMemberOf := graph.StringKind("MemberOf")
	kindHasSession := graph.StringKind("HasSession")

	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		group516, err := tx.CreateNode(graph.NewProperties().Set("objectid", "DOMAIN-516"), kindGroup)
		if err != nil {
			return err
		}
		group512, err := tx.CreateNode(graph.NewProperties().Set("objectid", "DOMAIN-512"), kindGroup)
		if err != nil {
			return err
		}

		// compExcluded reaches group516 via one MemberOf hop, so it lands in
		// the first MATCH's `exclude` := COLLECT(s) set.
		compExcluded, err := tx.CreateNode(graph.NewProperties(), kindComputer)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(compExcluded.ID, group516.ID, kindMemberOf, graph.NewProperties()); err != nil {
			return err
		}
		userExcluded, err := tx.CreateNode(graph.NewProperties(), kindUser)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(compExcluded.ID, userExcluded.ID, kindHasSession, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(userExcluded.ID, group512.ID, kindMemberOf, graph.NewProperties()); err != nil {
			return err
		}

		// compGood never reaches group516, so it must survive the
		// `NOT c IN exclude` filter and be the query's one served path.
		compGood, err := tx.CreateNode(graph.NewProperties(), kindComputer)
		if err != nil {
			return err
		}
		userGood, err := tx.CreateNode(graph.NewProperties(), kindUser)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(compGood.ID, userGood.ID, kindHasSession, graph.NewProperties()); err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(userGood.ID, group512.ID, kindMemberOf, graph.NewProperties()); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatalf("seed anti-join fixture: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	const text = `MATCH (s)-[:MemberOf*0..]->(g:Group)
WHERE g.objectid ENDS WITH '-516'
WITH COLLECT(s) AS exclude
MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group)
WHERE g.objectid ENDS WITH '-512' AND NOT c IN exclude
RETURN p
LIMIT 1000`

	var (
		engineResult graph.Result
		served       bool
	)
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		engineResult, served = eng.TryCypher(ctx, tx, text, nil)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if !served {
		t.Fatalf("TryCypher declined, want served (query: %s)", text)
	}

	engineOut := drainEngineResult(t, engineResult, text)

	var oracleOut graph.PathSet
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		oracleOut = drainOracleResult(t, tx, text)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction (oracle): %v", err)
	}

	assertSameSet(t, engineOut, oracleOut, false)
}

// TestTryCypherReturnPropertyLiteralMatchesOracleType is the
// "RETURN n.prop literal projection" coverage: TryCypher's own result and a
// direct PostgreSQL round trip through the plain pg driver -- deliberately
// bypassing ops.FetchByQuery's own node/edge/path mapping, which launders
// every scalar column through graph.Literal{Value: ...} and would hide a
// Go-type mismatch behind an `any` comparison that only ever inspects the
// wrapped value, never the value's own concrete type -- must agree on both
// the exact value and the exact Go type materializeScalar/decodeScalarString
// (serve_cypher.go) produce for a plain, non-amended (no id()/size()/
// datetime() epoch component, no WITH COUNT alias) property projection.
func TestTryCypherReturnPropertyLiteralMatchesOracleType(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	propNodeKind := graph.StringKind("PropNode")
	var nodeID graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("name", "Administrator"), propNodeKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	text := fmt.Sprintf(`MATCH (n:PropNode) WHERE id(n) = %d RETURN n.name`, nodeID)

	readOneValue := func(t *testing.T, result graph.Result) any {
		t.Helper()
		defer result.Close()
		if !result.Next() {
			t.Fatalf("result: no rows (query: %s)", text)
		}
		values := result.Values()
		if len(values) != 1 {
			t.Fatalf("result: %d columns, want 1 (query: %s)", len(values), text)
		}
		if result.Next() {
			t.Fatalf("result: more than one row (query: %s)", text)
		}
		if err := result.Error(); err != nil {
			t.Fatalf("result.Error(): %v", err)
		}
		return values[0]
	}

	var engineValue any
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result, served := eng.TryCypher(ctx, tx, text, nil)
		if !served {
			t.Fatalf("TryCypher declined, want served (query: %s)", text)
		}
		engineValue = readOneValue(t, result)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction (engine): %v", err)
	}

	var oracleValue any
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		oracleValue = readOneValue(t, tx.Query(text, map[string]any{}))
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction (oracle): %v", err)
	}

	if engineValue != oracleValue {
		t.Fatalf("engine value = %#v, oracle value = %#v: want equal", engineValue, oracleValue)
	}
	if gotType, wantType := reflect.TypeOf(engineValue), reflect.TypeOf(oracleValue); gotType != wantType {
		t.Fatalf("engine value type = %v, oracle value type = %v: want equal", gotType, wantType)
	}
	if engineValue != "Administrator" {
		t.Fatalf("engine value = %#v, want %q", engineValue, "Administrator")
	}
}
