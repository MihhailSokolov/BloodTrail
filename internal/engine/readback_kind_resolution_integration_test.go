// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// The tests in this file close the last hole in the write path's own rule
// (writepool.go): a statement the write path runs while its caller may be
// holding one of the main pool's connections must never draw a second one
// from that pool. Read-back's kind resolution used to break it -- dawgs'
// KindMapper answers from its in-process cache, but a MISS refetches the
// whole kind table through SchemaManager.Fetch, which acquires a MAIN-pool
// connection while applyMu is held. On a saturated pool that waits for a
// connection the write itself is holding: with a deadline the read-back
// fails and the write silently costs a fallback and a full rebuild, without
// one it waits for good.
//
// Both directions resolve that way -- a kind id a read-back row carries
// that the View does not name, and a kind name a kind-scoped delete criteria
// names that it does not either -- so there is one test per direction. Each
// one makes its kind after the engine's snapshot was built AND outside the
// pg driver (direct SQL), which is what makes the View and dawgs' cache miss
// it together, exactly as they do for a kind another BloodHound server (or
// an OpenGraph source registration) created since this replica loaded.

// kindResolutionMainPoolConns is how many connections the main pool gets:
// exactly as many as the test holds while the write under test runs, so a
// read-back reaching for one of its own waits for a connection nobody is
// going to release.
const kindResolutionMainPoolConns = 2

// kindResolutionApplyTimeout bounds the Apply under test, so the hazard
// shows up as a failing assertion rather than a test that never returns (a
// daemon's context has no deadline at all).
const kindResolutionApplyTimeout = 3 * time.Second

// kindResolutionSeedKind is asserted -- through the pg driver, before the
// engine's snapshot is built -- so every test here has one kind the View and
// dawgs' cache both know, to tell resolution of a kind they do not know
// apart from resolution in general.
var kindResolutionSeedKind = graph.StringKind("KindResolutionSeed")

// openKindResolutionEngine opens a pg driver on a main pool of exactly
// kindResolutionMainPoolConns connections, wipes the graph, writes one node
// of kindResolutionSeedKind and returns an engine already serving a snapshot
// of it, that node's id and the default graph's id.
//
// The snapshot is taken here, before the caller inserts a kind of its own:
// LoadSnapshot scans the whole kind table (load.go's loadKinds), so a kind
// that exists by now is one the View names. laterKinds are the names the
// caller is going to insert afterwards -- dropped from the kind table before
// the snapshot is built, and again on cleanup, so that a previous run of
// this test (whose kinds WipeGraph leaves behind) cannot make the View name
// them from the start.
func openKindResolutionEngine(t *testing.T, ctx context.Context, laterKinds ...string) (*Engine, *pgxpool.Pool, graph.ID, int32) {
	t.Helper()

	dsn := graphtest.PGAvailable(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.MaxConns = kindResolutionMainPoolConns
	cfg.MinConns = 0
	cfg.AfterConnect = pg.AfterPooledConnectionEstablished
	cfg.AfterRelease = pg.AfterPooledConnectionRelease

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	pgDriver := pg.NewDriver(size.Gibibyte, pool)
	if err := pgDriver.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	graphtest.WipeGraph(t, pgDriver)
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{kindResolutionSeedKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}

	dropKinds := func() {
		if _, err := pool.Exec(ctx, "DELETE FROM kind WHERE name = ANY($1::text[])", laterKinds); err != nil {
			t.Errorf("drop kinds %v: %v", laterKinds, err)
		}
	}
	if len(laterKinds) > 0 {
		dropKinds()
		t.Cleanup(dropKinds)
	}

	var seed *graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		seed, err = tx.CreateNode(graph.NewProperties().Set("objectid", "kind-resolution-seed"), kindResolutionSeedKind)
		return err
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}

	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatal("no default graph is set")
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	t.Cleanup(eng.CloseWritePool)
	t.Cleanup(eng.Stop)
	resetWatermarkTable(t, ctx, eng)
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	return eng, pool, seed.ID, graphModel.ID
}

// insertKindOutsideTheDriver inserts one kind row with direct SQL, so the pg
// driver's own SchemaManager cache never learns it the way AssertKinds
// would. openKindResolutionEngine has already dropped the name, so a row
// left behind by an interrupted run fails here rather than silently.
func insertKindOutsideTheDriver(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) int16 {
	t.Helper()

	var id int16
	if err := pool.QueryRow(ctx, "INSERT INTO kind (name) VALUES ($1) RETURNING id", name).Scan(&id); err != nil {
		t.Fatalf("insert kind %s: %v", name, err)
	}
	return id
}

// insertNodeOutsideTheDriver writes one node row with direct SQL, for a kind
// the pg driver cannot map: going through the driver would both define the
// kind in its cache and need a connection of the main pool.
func insertNodeOutsideTheDriver(t *testing.T, ctx context.Context, pool *pgxpool.Pool, graphID int32, kindIDs []int16, objectID string) graph.ID {
	t.Helper()

	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO node (graph_id, kind_ids, properties) VALUES ($1, $2, $3::jsonb) RETURNING id",
		graphID, kindIDs, `{"objectid":"`+objectID+`"}`).Scan(&id); err != nil {
		t.Fatalf("insert node %s: %v", objectID, err)
	}
	return graph.ID(id)
}

// holdEveryMainPoolConnection acquires every connection of the main pool and
// returns the release function, so that anything reaching for one while the
// write under test runs waits exactly as it does on a saturated pool.
func holdEveryMainPoolConnection(t *testing.T, ctx context.Context, pool *pgxpool.Pool) func() {
	t.Helper()

	conns := make([]*pgxpool.Conn, 0, kindResolutionMainPoolConns)
	release := func() {
		for _, conn := range conns {
			conn.Release()
		}
	}
	for i := 0; i < kindResolutionMainPoolConns; i++ {
		acquireCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		conn, err := pool.Acquire(acquireCtx)
		cancel()
		if err != nil {
			release()
			t.Fatalf("hold main pool connection %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	return release
}

// TestReadBackResolvesAnUnknownKindIDOnASaturatedMainPool is the id->name
// direction: a node whose kind id neither the View nor dawgs' cache knows,
// read back while every main-pool connection is held. The read-back has to
// name that kind to stage the node at all, and resolving it must not need a
// connection of the pool the write's own caller holds.
func TestReadBackResolvesAnUnknownKindIDOnASaturatedMainPool(t *testing.T) {
	ctx := context.Background()
	const kindName = "KindResolutionUnknownNodeKind"
	eng, pool, _, graphID := openKindResolutionEngine(t, ctx, kindName)

	kindID := insertKindOutsideTheDriver(t, ctx, pool, kindName)
	nodeID := insertNodeOutsideTheDriver(t, ctx, pool, graphID, []int16{kindID}, "kind-resolution-unknown-kind")

	if _, known := eng.snap.Load().Kinds().Name(kindID); known {
		t.Fatalf("the View already names kind %d (fixture assumption)", kindID)
	}

	release := holdEveryMainPoolConnection(t, ctx, pool)
	defer release()

	scope := NewWriteScope()
	scope.Changes().RecordNodeID(nodeID)

	applyCtx, cancel := context.WithTimeout(ctx, kindResolutionApplyTimeout)
	defer cancel()
	eng.Apply(applyCtx, scope)

	view, serving := eng.serveState()
	if !serving {
		t.Fatal("the engine stopped serving: the read-back could not resolve the node's kind without a main-pool connection")
	}
	if name, ok := view.Kinds().Name(kindID); !ok || name != kindName {
		t.Fatalf("view kind %d = (%q, %v), want (%q, true)", kindID, name, ok, kindName)
	}
	dense, ok := view.Dense(uint64(nodeID))
	if !ok || !view.Alive(dense) {
		t.Fatalf("node %d in view: dense=%v alive=%v, want it staged and alive", nodeID, ok, ok && view.Alive(dense))
	}
	if kinds := view.KindIDsOf(dense); len(kinds) != 1 || kinds[0] != kindID {
		t.Fatalf("node %d kind ids = %v, want [%d]", nodeID, kinds, kindID)
	}
}

// TestReadBackResolvesAnUnknownCriteriaKindOnASaturatedMainPool is the
// name->id direction: a kind-scoped node delete whose EXCLUSION names a kind
// neither the View nor dawgs' cache knows, applied while every main-pool
// connection is held. An exclusion that does not resolve fails closed (the
// View cannot tell which rows it spares), so this resolution is not
// optional -- and it must not need a main-pool connection either.
func TestReadBackResolvesAnUnknownCriteriaKindOnASaturatedMainPool(t *testing.T) {
	ctx := context.Background()
	const kindName = "KindResolutionUnknownExcludedKind"
	eng, pool, seedID, _ := openKindResolutionEngine(t, ctx, kindName)

	kindID := insertKindOutsideTheDriver(t, ctx, pool, kindName)
	if _, known := eng.snap.Load().Kinds().Name(kindID); known {
		t.Fatalf("the View already names kind %d (fixture assumption)", kindID)
	}

	release := holdEveryMainPoolConnection(t, ctx, pool)
	defer release()

	scope := NewWriteScope()
	scope.Changes().RecordDeleteNodesByKinds(
		graph.Kinds{kindResolutionSeedKind},
		graph.Kinds{graph.StringKind(kindName)},
	)

	applyCtx, cancel := context.WithTimeout(ctx, kindResolutionApplyTimeout)
	defer cancel()
	eng.Apply(applyCtx, scope)

	view, serving := eng.serveState()
	if !serving {
		t.Fatal("the engine stopped serving: the read-back could not resolve the criteria's excluded kind without a main-pool connection")
	}
	if name, ok := view.Kinds().Name(kindID); !ok || name != kindName {
		t.Fatalf("view kind %d = (%q, %v), want (%q, true)", kindID, name, ok, kindName)
	}
	// PostgreSQL still holds the seed node (nothing was deleted there), so
	// the candidate re-read has to find it present and keep it.
	dense, ok := view.Dense(uint64(seedID))
	if !ok || !view.Alive(dense) {
		t.Fatalf("seed node %d in view: dense=%v alive=%v, want it kept", seedID, ok, ok && view.Alive(dense))
	}
}
