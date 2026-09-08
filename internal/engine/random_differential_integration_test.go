// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// randomDifferentialSeeds is the seed range TestRandomDifferential sweeps.
// Each seed reproduces the exact same graph (graphtest.LoadRandom) and the
// exact same 10 queries (randomPathQuery, seeded identically): rerunning
// this test, or a single "seed=N/query=M" subtest, reproduces a failure
// deterministically.
const randomDifferentialSeeds = 20

// randomDifferentialQueriesPerSeed is how many random TryAllShortestPaths
// queries TestRandomDifferential issues against each seed's graph.
const randomDifferentialQueriesPerSeed = 10

// TestRandomDifferential is Task 14's randomized differential suite: for
// seeds 1..randomDifferentialSeeds, it wipes the graph, generates a random
// one via graphtest.LoadRandom (60 nodes across 5 kinds, 180 edges across 6
// kinds, seeded rand, self-loops and same/different-kind parallel edges all
// possible), rebuilds the engine's snapshot from it, and issues
// randomDifferentialQueriesPerSeed random TryAllShortestPaths queries --
// random explicit-id endpoint pair (sometimes the same node), a random 1-4
// edge-kind subset, and a random Mode -- checked against the same
// pg-driver-only oracle TestTryAllShortestPathsDifferential (Task 10) uses:
// assertSameSet for ModeAll, assertModeOne for ModeOne, both called with
// allowEmpty true since a uniformly random id pair is frequently
// disconnected and that is not itself a bug.
//
// Same-node pairs (start == end) are handled separately, not compared
// against the oracle: PostgreSQL's own shortest-path implementation cannot
// serve that request once the shared node has an outgoing edge -- it raises
// SQLSTATE 22023 ("shortest path endpoints must not resolve to the same
// node") from the very first BFS hop, aborting the whole query rather than
// just skipping that pair (see traverse.SelfEndpointConflict's doc for the
// underlying SQL). Task 10's oraclePathsByPair issues exactly the query
// shape that hits this, so calling it for such a pair fails the test on an
// oracle error, not a real engine/oracle mismatch. The engine now declines
// outright whenever the resolved roots and terminals share a node with an
// outgoing edge (see engine.go's reasonSelfEndpoint), so this suite instead
// checks that contract directly for same-node queries and skips the oracle
// call entirely -- see assertSelfEndpointQuery, which also covers the
// complementary case (the shared node has no outgoing edge, so PostgreSQL
// itself never reaches the guard and the engine must still serve, trivially
// empty).
//
// Every query runs in its own "seed=N/query=M" subtest so a failure
// reproduces by rerunning just that subtest (-run
// 'TestRandomDifferential/seed=7/query=3'); each subtest also logs its full
// query (endpoints, edge kinds, mode) on failure via a deferred t.Failed()
// check, since the oracle/engine mismatch assertions themselves only know
// the paths, not the query that produced them.
func TestRandomDifferential(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})

	for seed := int64(1); seed <= randomDifferentialSeeds; seed++ {
		graphtest.WipeGraph(t, pgDriver)
		ids := graphtest.LoadRandom(t, pgDriver, seed)

		if err := eng.RebuildNow(ctx, triggerManual, time.Time{}); err != nil {
			t.Fatalf("seed=%d: RebuildNow: %v", seed, err)
		}

		// Queries are generated from their own rand.Rand, independent of the
		// one graphtest.LoadRandom already consumed generating the graph, so
		// query generation never has to know how many random draws graph
		// generation happened to make.
		rng := rand.New(rand.NewSource(seed))

		for q := 0; q < randomDifferentialQueriesPerSeed; q++ {
			pq := randomPathQuery(rng, ids)

			t.Run(fmt.Sprintf("seed=%d/query=%d", seed, q), func(t *testing.T) {
				defer func() {
					if t.Failed() {
						t.Logf("reproduce with: seed=%d query=%d %s", seed, q, describePathQuery(pq))
					}
				}()

				if pq.Start.IDs[0] == pq.End.IDs[0] {
					assertSelfEndpointQuery(t, ctx, pgDriver, eng, pq)
					return
				}

				startIDs := oracleEndpointIDs(t, ctx, pgDriver, pq.Start)
				endIDs := oracleEndpointIDs(t, ctx, pgDriver, pq.End)
				oracleByPair := oraclePathsByPair(t, ctx, pgDriver, startIDs, endIDs, pq.EdgeKinds)

				var (
					engineOut graph.PathSet
					served    bool
				)
				if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
					engineOut, served = eng.TryAllShortestPaths(ctx, tx, pq)
					return nil
				}); err != nil {
					t.Fatalf("ReadTransaction: %v", err)
				}
				if !served {
					t.Fatalf("TryAllShortestPaths declined, want served")
				}

				switch pq.Mode {
				case recognize.ModeOne:
					assertModeOne(t, oracleByPair, engineOut, true)
				default:
					var oracleAll graph.PathSet
					for _, paths := range oracleByPair {
						oracleAll = append(oracleAll, paths...)
					}
					assertSameSet(t, engineOut, oracleAll, true)
				}
			})
		}
	}
}

// randomPathQuery builds one random explicit-id PathQuery drawn from ids:
// a start and end node chosen independently and uniformly (so the two are
// sometimes the same node), a random 1-4 element subset of
// graphtest.RandomEdgeKinds, and a random Mode.
func randomPathQuery(rng *rand.Rand, ids []graph.ID) recognize.PathQuery {
	start := ids[rng.Intn(len(ids))]
	end := ids[rng.Intn(len(ids))]

	mode := recognize.ModeAll
	if rng.Intn(2) == 1 {
		mode = recognize.ModeOne
	}

	return recognize.PathQuery{
		Start:     recognize.Endpoint{IDs: []graph.ID{start}},
		End:       recognize.Endpoint{IDs: []graph.ID{end}},
		EdgeKinds: randomEdgeKindSubset(rng),
		Mode:      mode,
	}
}

// randomEdgeKindSubset returns a random, duplicate-free subset of
// graphtest.RandomEdgeKinds sized 1-4 (out of its 6 kinds).
func randomEdgeKindSubset(rng *rand.Rand) graph.Kinds {
	n := 1 + rng.Intn(4)

	perm := rng.Perm(len(graphtest.RandomEdgeKinds))[:n]
	kinds := make(graph.Kinds, n)
	for i, idx := range perm {
		kinds[i] = graphtest.RandomEdgeKinds[idx]
	}
	return kinds
}

// assertSelfEndpointQuery checks pq -- a same-node query (pq.Start.IDs[0] ==
// pq.End.IDs[0]) -- against reasonSelfEndpoint's contract directly, instead
// of the pg-driver oracle (which errors for the has-an-edge case; see
// TestRandomDifferential's doc). Whether TryAllShortestPaths must decline
// depends on whether the shared node has an outgoing edge (any kind --
// see traverse.SelfEndpointConflict's doc): if it does, PostgreSQL's own
// shortest-path query cannot serve the request either, so the engine must
// decline (served == false); if it does not, no path -- trivial or
// otherwise -- can exist from the node back to itself, so the engine must
// still serve, with zero paths (and PostgreSQL, asked the same question,
// would silently agree, since the guard's join never produces a row to
// check in the first place).
//
// Whether the node has an outgoing edge is read directly off the engine's
// own current snapshot (Engine.Fresh -> Snapshot.Out) rather than a second
// live query: the same data TryAllShortestPaths itself just consulted, so
// this is authoritative and free of any oracle-side ambiguity.
func assertSelfEndpointQuery(t *testing.T, ctx context.Context, pgDriver *pg.Driver, eng *Engine, pq recognize.PathQuery) {
	t.Helper()

	id := pq.Start.IDs[0]

	snap, fresh := eng.Fresh()
	if !fresh {
		t.Fatalf("Engine.Fresh() = (_, false) mid-test, want a current snapshot")
	}
	dense, ok := snap.Dense(uint64(id))
	if !ok {
		t.Fatalf("snapshot has no dense id for database id %d, which LoadRandom just created", id)
	}
	targets, _, _ := snap.Out(dense)
	hasOutgoingEdge := len(targets) > 0

	var (
		engineOut graph.PathSet
		served    bool
	)
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		engineOut, served = eng.TryAllShortestPaths(ctx, tx, pq)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}

	if hasOutgoingEdge {
		if served {
			t.Fatalf("TryAllShortestPaths served a same-node query (start=end=%d) whose node has an outgoing edge, want declined", id)
		}
		return
	}

	if !served {
		t.Fatalf("TryAllShortestPaths declined a same-node query (start=end=%d) whose node has no outgoing edge, want served (no path can exist either way)", id)
	}
	if len(engineOut) != 0 {
		t.Fatalf("TryAllShortestPaths(start=end=%d, no outgoing edge) returned %d paths, want 0", id, len(engineOut))
	}
}

// describePathQuery renders pq's endpoints, edge kinds, and mode for a
// failure message. TestRandomDifferential only ever builds pq via
// randomPathQuery, so Start/End are always single explicit ids.
func describePathQuery(pq recognize.PathQuery) string {
	modeName := "ModeAll"
	if pq.Mode == recognize.ModeOne {
		modeName = "ModeOne"
	}
	return fmt.Sprintf("start=%d end=%d edgeKinds=%v mode=%s", pq.Start.IDs[0], pq.End.IDs[0], pq.EdgeKinds, modeName)
}
