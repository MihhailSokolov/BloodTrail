// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// kindIDByName reads one kind's current id straight out of PostgreSQL's
// global `kind` table -- the same table pgKindCatalog reads -- so the bulk
// seed below can write `kind_ids` and `kind_id` columns itself without
// guessing at ids dawgs assigned.
func kindIDByName(t *testing.T, pool *pgxpool.Pool, name string) int16 {
	t.Helper()

	var id int16
	if err := pool.QueryRow(context.Background(), "SELECT id FROM kind WHERE name = $1", name).Scan(&id); err != nil {
		t.Fatalf("read the id of kind %q: %v", name, err)
	}
	return id
}

// seedHubSpokes bulk-creates spokes nodes, each carrying a distinct objectid
// built from oidPrefix, and one edge of kindID from each of them into hubID.
//
// It writes the `node` and `edge` tables directly, in one data-modifying CTE,
// rather than going through dawgs: the fan-out cap is counted in thousands of
// edges, and one statement that PostgreSQL routes to the graph's own
// partitions seeds them in a single round trip. Nothing about the shape under
// test depends on how the spokes got there -- read-back reads whatever
// PostgreSQL holds.
func seedHubSpokes(t *testing.T, pool *pgxpool.Pool, graphID int32, nodeKindID, kindID int16, hubID uint64, oidPrefix string, spokes int) {
	t.Helper()

	if _, err := pool.Exec(context.Background(), `
		WITH spokes AS (
			INSERT INTO node (graph_id, kind_ids, properties)
			SELECT $1, ARRAY[$2::smallint], jsonb_build_object('objectid', $3::text || g)
			FROM generate_series(1, $4) AS g
			RETURNING id
		)
		INSERT INTO edge (graph_id, start_id, end_id, kind_id, properties)
		SELECT $1, spokes.id, $5, $6::smallint, '{}'::jsonb FROM spokes`,
		graphID, nodeKindID, oidPrefix, spokes, int64(hubID), kindID); err != nil {
		t.Fatalf("seed %d hub spokes: %v", spokes, err)
	}
}

// TestEdgeFanoutCapFallsBackRatherThanStagingAPartialSet pins both sides of
// readbackEdgeFanoutCap, the bound on the endpoint-keyed fan-out read-back
// uses to find the edge of a deferred objectid-keyed triple whose other
// endpoint nobody can name (readBackEdgesByEndpoint).
//
// The fan-out's only anchors are one endpoint and one kind, and that pair's
// degree is unbounded in BloodHound's data model -- every user in a domain is
// `MemberOf` the same "Domain Users" group. So the query carries a LIMIT, and
// a fan-out above the cap stages NOTHING and records a fallback instead of a
// partial set: an incomplete answer that the engine kept serving would be a
// missing row, while the fallback reloads exactly what PostgreSQL holds.
//
// Both arms run the same race -- an objectid-keyed upsert into a hub endpoint
// commits cleanly, then another writer re-keys the upsert's own new start
// node, so only the hub end is nameable -- and differ only in the hub's
// degree for that kind once the upsert's own edge has landed:
//
//   - exactly readbackEdgeFanoutCap edges: the fan-out fits, the committed
//     edge is staged, and the engine keeps serving without a reload;
//   - one more: the fan-out is over the cap, read-back fails closed, and the
//     rebuild brings in what PostgreSQL holds.
//
// Each arm compares against the PostgreSQL oracle while the engine is
// serving, and names the raced edge's own row explicitly, so neither arm can
// pass on an empty answer or a decline.
//
// The rebuild loop is parked (parkRebuildLoop) so no recovery goroutine can
// adopt a snapshot at an unpredictable moment -- which is also what makes
// "state is still stateServing" mean "no fallback was entered", since nothing
// else can leave fallback. Every rebuild here is driven explicitly.
func TestEdgeFanoutCapFallsBackRatherThanStagingAPartialSet(t *testing.T) {
	cases := []struct {
		name string
		// seeded spokes; the upsert under test adds one more edge.
		spokes        int
		wantState     int32
		wantFallsBack bool
	}{
		{
			name:      "a fan-out of exactly the cap is read, and the edge is staged",
			spokes:    readbackEdgeFanoutCap - 1,
			wantState: stateServing,
		},
		{
			name:          "one edge past the cap falls back instead of staging a partial set",
			spokes:        readbackEdgeFanoutCap,
			wantState:     stateFallback,
			wantFallsBack: true,
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := graphtest.PGAvailable(t)
			ctx := context.Background()

			pgDriver, pool := graphtest.OpenPG(t, dsn)
			graphtest.WipeGraph(t, pgDriver)

			withObjectIDUniqueIndex(t, pgDriver, pool)

			nodeKind := graph.StringKind("FanoutCapNode")
			edgeKind := graph.StringKind("FanoutCapEdge")
			if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind, edgeKind}); err != nil {
				t.Fatalf("assert kinds: %v", err)
			}

			graphModel, ok := pgDriver.DefaultGraph()
			if !ok {
				t.Fatalf("no default graph is set")
			}

			// Distinct objectids per arm, so neither arm's unique index can
			// collide with rows the other left behind.
			var (
				hubOID      = fmt.Sprintf("S-1-5-21-15-%d-hub", i)
				racedOID    = fmt.Sprintf("S-1-5-21-15-%d-raced", i)
				rekeyedToID = fmt.Sprintf("S-1-5-21-15-%d-rekeyed", i)
				spokePrefix = fmt.Sprintf("S-1-5-21-15-%d-spoke-", i)
			)

			var hub graph.ID
			if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
				node, err := tx.CreateNode(graph.NewProperties().Set("objectid", hubOID), nodeKind)
				if err != nil {
					return err
				}
				hub = node.ID
				return nil
			}); err != nil {
				t.Fatalf("create the hub endpoint: %v", err)
			}

			seedHubSpokes(t, pool, graphModel.ID,
				kindIDByName(t, pool, nodeKind.String()), kindIDByName(t, pool, edgeKind.String()),
				uint64(hub), spokePrefix, tc.spokes)

			eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
			defer stopEngineAndCloseWritePool(eng)
			parkRebuildLoop(eng)

			// The View holds the hub and every seeded spoke, but nothing
			// under the raced objectid: that endpoint is the upsert's own.
			adoptOneRebuild(t, ctx, eng)

			// The write under test: one objectid-keyed upsert into the hub,
			// which commits cleanly and takes the hub's degree for this kind
			// to tc.spokes + 1.
			if err := pgDriver.BatchOperation(ctx, func(batch graph.Batch) error {
				return batch.UpdateRelationshipBy(rekeyedEndpointUpdate(racedOID, hubOID, nodeKind, edgeKind))
			}); err != nil {
				t.Fatalf("upsert relationship: %v", err)
			}
			assertEdgeCount(t, pgDriver, edgeKind, tc.spokes+1)

			// The race: the upsert's own new start node is re-keyed after the
			// edge committed, so only the hub end stays nameable.
			raced := fetchNodeByObjectID(t, pgDriver, racedOID)
			raced.Properties.Set("objectid", rekeyedToID)
			if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
				return tx.UpdateNode(raced)
			}); err != nil {
				t.Fatalf("rewrite objectid: %v", err)
			}

			scope := NewWriteScope()
			scope.Changes().RecordEdgeTripleByObjectID(racedOID, hubOID, edgeKind)
			scope.Changes().RecordNodeObjectID(racedOID)
			scope.Changes().RecordNodeObjectID(hubOID)
			eng.Apply(ctx, scope)

			if got := eng.state.Load(); got != tc.wantState {
				t.Fatalf("state = %d after a clean commit whose fan-out covers %d edges (cap %d), want %d",
					got, tc.spokes+1, readbackEdgeFanoutCap, tc.wantState)
			}

			if tc.wantFallsBack {
				// Convergence: the fallback's rebuild reloads what
				// PostgreSQL holds, including the raced edge the fan-out
				// refused to read.
				adoptOneRebuild(t, ctx, eng)
			}

			if _, serving := eng.serveState(); !serving {
				t.Fatalf("engine not serving; the comparisons below would be vacuous")
			}

			// The raced edge's own row, named rather than merely compared:
			// "the two answers agree" would also hold if neither held it.
			edgeQuery := `MATCH (s:FanoutCapNode)-[:FanoutCapEdge]->(e:FanoutCapNode) WHERE s.objectid = '` + rekeyedToID + `' RETURN s.objectid, e.objectid`
			servedRows, oracleRows := servedAndOracleRows(t, pgDriver, eng, edgeQuery)
			want := []string{"string:" + rekeyedToID + "|string:" + hubOID + "|"}
			if !reflect.DeepEqual(servedRows, want) || !reflect.DeepEqual(oracleRows, want) {
				t.Fatalf("engine served %v, PostgreSQL returns %v, want both %v", servedRows, oracleRows, want)
			}

			assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
				{edgeQuery, true},
				{`MATCH (s:FanoutCapNode) WHERE s.objectid = '` + rekeyedToID + `' RETURN s.objectid`, true},
				{`MATCH (s:FanoutCapNode)-[:FanoutCapEdge]->(e:FanoutCapNode) WHERE e.objectid = '` + hubOID + `' RETURN count(s)`, true},
			})
		})
	}
}
