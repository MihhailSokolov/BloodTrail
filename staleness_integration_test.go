// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file is an end-to-end integration suite for BloodTrail's write-through
// model, driven through the REAL driver write paths (WriteTransaction,
// BatchOperation) rather than through internal/engine's own white-box unit
// tests, which call the engine's applier directly and never exercise
// observingTransaction/observingBatch (write_observer.go) at all.
//
// # What it proves
//
// Every write this package recognizes is replayed into the in-memory replica
// before the writing call returns, so the very next query is SERVED from the
// replica -- and served correctly. The suite grew up asserting the opposite
// (a write invalidated the kinds it touched, and those queries delegated to
// PostgreSQL until the next rebuild); those assertions were inverted, not
// deleted, so each write shape still has a test naming it, now asserting the
// stronger property. Two things are asserted for every shape: the served-log
// marker fired for that exact call, and the value returned equals what the
// write actually produced. A third, cross-cutting one runs per test: the
// engine's RebuildCount must not move, since a rebuild would make a served
// answer prove nothing about write-through.
//
// # File placement: root package, not internal/engine
//
// The write paths under test -- observingTransaction, observingBatch, and
// Driver.WriteTransaction/BatchOperation themselves -- are defined in the
// root package (driver.go, write_observer.go), and the root package imports
// internal/engine (for engine.Engine, engine.WriteScope, engine.New).
// internal/engine therefore cannot import the root package to drive writes
// through Driver.WriteTransaction/BatchOperation -- that would be an import
// cycle. Putting this file in the root package instead resolves it: a test
// here can freely construct a *bloodtrail.Driver via dawgs.Open and drive
// real writes through it.
//
// # White-box access, and its actual limits
//
// Being in package bloodtrail (not bloodtrail_test) gives this file
// unexported access to Driver's own `engine *engine.Engine` field, which
// bloodtrail_test-package tests (engine_serving_integration_test.go) cannot
// reach directly. That access is used below for three exported-method calls:
// d.engine.RebuildNow (a deterministic, on-demand rebuild -- no need to wire
// up a datapipe_status table and wait on the poller), d.engine.Fresh (is the
// engine serving at all), and d.engine.RebuildCount (has any rebuild
// happened).
//
// Everything else stays a black-box proof: every flow issues a real query
// through the real driver and observes, by exactly how much (0 or 1), the
// exact Debug/Info log line the engine emits precisely when it serves a query
// from its in-memory replica -- "bloodtrail: builder engine served" for
// structural node/relationship queries (serve_builder.go's servedOp),
// "bloodtrail: path engine served" for shortest-path queries (engine.go's
// TryAllShortestPaths), and "bloodtrail: cypher engine served" for Cypher
// text (engine.go's cypherServedLogMessage) -- alongside the returned value's
// own correctness. A marker delta of 1 plus a correct result means "served
// from the replica, and the replica was right to serve it"; a delta of 0 plus
// a correct result means "declined and fell through to PostgreSQL, and
// PostgreSQL was consulted correctly".
package bloodtrail

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// servedMarker and builderServedMarker are the exact messages
// TryAllShortestPaths (engine.go) and TryNodeCount/TryRelCount/etc.
// (serve_builder.go's servedOp) log whenever they actually serve a query
// from the in-memory snapshot -- duplicated here from
// engine_serving_integration_test.go (a different package, bloodtrail_test,
// so its unexported consts are not visible from here) for the same reason
// that file gives: only this test needs to recognize them in captured log
// output. TryCypher logs its own, separate marker (engine.go's
// cypherServedLogMessage) since being rewired to the Cypher interpreter;
// this file never issues a raw Cypher-text query, so it has no reason to
// watch for that one.
const (
	servedMarker        = "bloodtrail: path engine served"
	builderServedMarker = "bloodtrail: builder engine served"
)

// staleness{Node,Edge}Kind* are this file's own fixture kinds, named
// distinctly from every other integration test's fixture (traversal_shapes,
// adcs_fanout, LoadRandom's A-E/R1-R6, etc.) so this test's counts are never
// at risk of colliding with -- or being polluted by -- data any other test
// in this shared, long-lived integration database happens to leave behind.
// Node kinds A/B/C plus a fourth, Tag, standing in for a label a later
// analysis pass tags onto an existing node (flow 4); edge kinds A/B/C
// mirroring the node side for the relationship-count flows.
var (
	stalenessNodeKindA   = graph.StringKind("StalenessNodeA")
	stalenessNodeKindB   = graph.StringKind("StalenessNodeB")
	stalenessNodeKindC   = graph.StringKind("StalenessNodeC")
	stalenessNodeKindTag = graph.StringKind("StalenessNodeTag")

	stalenessEdgeKindA = graph.StringKind("StalenessEdgeA")
	stalenessEdgeKindB = graph.StringKind("StalenessEdgeB")
	stalenessEdgeKindC = graph.StringKind("StalenessEdgeC")
)

// lockedBuffer is a bytes/strings-backed log sink safe for concurrent writes
// from the engine's own goroutines and reads from the test goroutine --
// duplicated from engine_serving_integration_test.go's identically-named,
// identically-shaped type for the same cross-package reason servedMarker is.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// installLogCapture installs a Debug-level slog default logger writing into
// a returned buffer, restored via t.Cleanup. It must run before dawgs.Open
// constructs the driver: Open reads slog.Default() exactly once, to build
// the engine's own Config.Log, so installing the capture afterward would
// miss every line the engine itself logs -- see
// engine_serving_integration_test.go's identically-purposed helper.
func installLogCapture(t *testing.T) *lockedBuffer {
	t.Helper()

	buf := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return buf
}

// markerCount returns how many times marker (servedMarker or
// builderServedMarker) appears in buf's captured log so far.
func markerCount(buf *lockedBuffer, marker string) int {
	return strings.Count(buf.String(), marker)
}

// requireMarkerDelta is this file's one core assertion: it runs query,
// then requires that marker's count in buf advanced by exactly wantDelta
// (1 if the engine actually served this specific call, 0 if it declined and
// the call fell through to PostgreSQL instead) and that the value query
// produced equals want.
//
// Because before/after are captured immediately around one query call, this
// stays correct with no shared, running baseline to manage across flows --
// nothing else is logging either marker while this test runs (the poller is
// configured with an hour-long interval that never fires during the test;
// see TestKindScopedStalenessEndToEnd's setup), so any change in the count
// across exactly this call can only be attributed to this call.
func requireMarkerDelta[T comparable](t *testing.T, buf *lockedBuffer, marker string, wantDelta int, label string, query func() T, want T) {
	t.Helper()

	before := markerCount(buf, marker)
	got := query()
	after := markerCount(buf, marker)

	if delta := after - before; delta != wantDelta {
		t.Fatalf("%s: %q log count changed by %d, want %d (got value = %v)\ncaptured log:\n%s", label, marker, delta, wantDelta, got, buf.String())
	}
	if got != want {
		t.Fatalf("%s: got %v, want %v", label, got, want)
	}
}

// nodeCountByKind runs tx.Nodes().Filter(query.Kind(query.Node(), kind)).
// Count() through db -- the exact shape recordingNodeQuery.Count
// (node_query.go) recognizes and may serve from the engine.
func nodeCountByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) int64 {
	t.Helper()

	var count int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Nodes().Filter(query.Kind(query.Node(), kind)).Count()
		count = n
		return err
	})
	if err != nil {
		t.Fatalf("Nodes().Filter(Kind(%s)).Count(): %v", kind, err)
	}
	return count
}

// relCountByKind runs tx.Relationships().Filter(query.Kind(query.
// Relationship(), kind)).Count() through db -- the exact structural shape
// recordingRelationshipQuery.Count (relationship_query.go) recognizes and
// may serve from the engine.
func relCountByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) int64 {
	t.Helper()

	var count int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Relationships().Filter(query.Kind(query.Relationship(), kind)).Count()
		count = n
		return err
	})
	if err != nil {
		t.Fatalf("Relationships().Filter(Kind(%s)).Count(): %v", kind, err)
	}
	return count
}

// shortestPathCount runs the same graph.Criteria shape BloodHound's API
// builds for FetchAllShortestPaths -- query.And(query.Equals(StartID),
// query.Equals(EndID)) -- through db and returns how many paths came back.
// This is the path-serving equivalent of nodeCountByKind/relCountByKind
// above: recordingRelationshipQuery.FetchAllShortestPaths
// (relationship_query.go) is the shape that may be served by
// engine.TryAllShortestPaths, logging servedMarker when it is.
func shortestPathCount(t *testing.T, ctx context.Context, db graph.Database, startID, endID graph.ID) int {
	t.Helper()

	count := 0
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		criteria := query.And(
			query.Equals(query.StartID(), startID),
			query.Equals(query.EndID(), endID),
		)
		return tx.Relationships().Filter(criteria).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for range cursor.Chan() {
				count++
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("FetchAllShortestPaths(%d, %d): %v", startID, endID, err)
	}
	return count
}

// TestWriteThroughEndToEnd is this file's central deliverable: five
// sequential flows proving that a write through the real driver is visible
// to the very next query, served from the in-memory replica, with no rebuild
// anywhere in between.
//
// This test used to assert the opposite -- that a write invalidated the kinds
// it touched and forced those queries to delegate to PostgreSQL until the
// next rebuild. Write-through (internal/engine/apply.go) removed the premise:
// every committed write is read back from PostgreSQL and published into the
// replica before the writing call returns, so a query issued immediately
// afterward is served, and served correctly. The evidence is the same
// black-box kind this file has always used (log marker deltas plus result
// correctness), now asserting a delta of 1 where it once asserted 0.
//
// Fixture shape (all built once, up front, through bt.WriteTransaction, so
// every kind's baseline state is exactly known rather than inferred):
//
//   - StalenessNodeA: a1, a2, joined by one StalenessEdgeA edge.
//   - StalenessNodeB: b1, b2, with NO edge yet -- flow 2 creates one.
//   - StalenessNodeC: c1, c2, joined by one StalenessEdgeC edge.
//
// The five flows:
//
//  1. One rebuild (the only one this test ever performs), then kind-A
//     relationship Count and an a1->a2 shortest-path query both serve.
//  2. A WriteTransaction creates a kind-B edge. The kind-B Count -- the
//     query whose own kind was just written -- serves immediately, and
//     returns 1; kind-A's Count and the a1/a2 path query, untouched by the
//     write, keep serving too.
//  3. A BatchOperation deletes the one kind-A edge. Kind-A's Count now serves
//     0, and the a1->a2 shortest path serves 0 paths -- the delete reached
//     the replica's adjacency, not just its counters -- while kind-B and
//     kind-C keep serving 1 each.
//  4. A BatchOperation tags c1 with an added node kind. The Tag node Count
//     serves 1 immediately, the unrelated kind-A node Count still serves 2,
//     and kind-C's relationship Count is unaffected.
//  5. No rebuild happened anywhere in flows 2-4 (RebuildCount unchanged since
//     flow 1). A manual rebuild is then run purely as a differential check:
//     every count above must be identical when answered from a snapshot
//     loaded from scratch, proving the deltas the replica accumulated agree
//     with PostgreSQL's own state.
func TestWriteThroughEndToEnd(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	// An hour-long poll interval means the poller's own ticker will not
	// fire even once during this test's lifetime, so every rebuild observed
	// below is the direct, deterministic result of this test's own
	// d.engine.RebuildNow calls. Must be set before dawgs.Open: Settings are
	// read from the environment exactly once, at Open time.
	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	// graphtest.OpenPG both asserts the "bloodtrail_test" default graph this
	// test's own dawgs.Open(DriverName, ...) call below needs (a physical
	// database fact, not tied to any one driver instance, so asserting it
	// once via this raw pg.Driver is enough) and hands back the pool the
	// wrapped driver is opened against. graphtest.WipeGraph then clears
	// every node and edge across all graphs, so this test's absolute count
	// assertions below hold regardless of what any earlier test run (in this
	// process or a previous one, against this same long-lived integration
	// database) left behind.
	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	// AssertSchema's default-graph target is tracked per driver instance
	// (not auto-discovered from the database by a freshly opened one), so
	// this wrapped driver needs its own call even though graphtest.OpenPG
	// already asserted the same graph, by name, for pgDriver above.
	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	// --- Fixture: nodes and one edge each for kinds A and C, two bare nodes
	// (no edge yet) for kind B, all created through one real WriteTransaction
	// so their ids are known and captured for the flows below.
	var (
		a1ID, a2ID graph.ID
		b1ID, b2ID graph.ID
		c1ID       graph.ID
		edgeAID    graph.ID
	)
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		a1, err := tx.CreateNode(graph.NewProperties(), stalenessNodeKindA)
		if err != nil {
			return err
		}
		a2, err := tx.CreateNode(graph.NewProperties(), stalenessNodeKindA)
		if err != nil {
			return err
		}
		edgeA, err := tx.CreateRelationshipByIDs(a1.ID, a2.ID, stalenessEdgeKindA, graph.NewProperties())
		if err != nil {
			return err
		}

		b1, err := tx.CreateNode(graph.NewProperties(), stalenessNodeKindB)
		if err != nil {
			return err
		}
		b2, err := tx.CreateNode(graph.NewProperties(), stalenessNodeKindB)
		if err != nil {
			return err
		}

		c1, err := tx.CreateNode(graph.NewProperties(), stalenessNodeKindC)
		if err != nil {
			return err
		}
		c2, err := tx.CreateNode(graph.NewProperties(), stalenessNodeKindC)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(c1.ID, c2.ID, stalenessEdgeKindC, graph.NewProperties()); err != nil {
			return err
		}

		a1ID, a2ID = a1.ID, a2.ID
		b1ID, b2ID = b1.ID, b2.ID
		c1ID = c1.ID
		edgeAID = edgeA.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	// === Flow 1: one rebuild, then kind-A relationship Count and a
	// shortest-path query both serve. ===

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (flow 1): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("flow 1: engine is not serving immediately after RebuildNow")
	}
	rebuilds := d.engine.RebuildCount()

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 1: kind-A relationship count serves",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 1)

	requireMarkerDelta(t, buf, servedMarker, 1, "flow 1: a1->a2 shortest-path query serves",
		func() int { return shortestPathCount(t, ctx, bt, a1ID, a2ID) }, 1)

	// === Flow 2: a WriteTransaction creates one kind-B edge. The kind-B
	// count -- the query whose own kind was just written -- serves it
	// immediately; kind A and the path query are unaffected. ===

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(b1ID, b2ID, stalenessEdgeKindB, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("WriteTransaction (create kind-B edge, flow 2): %v", err)
	}

	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("flow 2: engine stopped serving after a write; write-through must keep it serving")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 2: kind-B relationship count serves the just-written edge",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindB) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 2: kind-A relationship count still serves",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 1)

	requireMarkerDelta(t, buf, servedMarker, 1, "flow 2: shortest-path query still serves",
		func() int { return shortestPathCount(t, ctx, bt, a1ID, a2ID) }, 1)

	// === Flow 3: a BatchOperation deletes the one kind-A edge. Both the
	// kind-A count and the a1->a2 path query must reflect the deletion
	// immediately -- the tombstone reached the replica's adjacency, not just
	// its per-kind counters. ===

	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteRelationship(edgeAID)
	}); err != nil {
		t.Fatalf("BatchOperation (delete kind-A edge, flow 3): %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 3: kind-A relationship count serves 0 (its only edge was just deleted)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 0)

	requireMarkerDelta(t, buf, servedMarker, 1, "flow 3: a1->a2 shortest path serves 0 paths (the edge is gone from the replica's adjacency)",
		func() int { return shortestPathCount(t, ctx, bt, a1ID, a2ID) }, 0)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 3: kind-B relationship count still serves",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindB) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 3: kind-C relationship count still serves (never touched)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindC) }, 1)

	// === Flow 4: a BatchOperation runs UpdateNodes on c1 with
	// AddedKinds=[Tag] -- simulating an analysis pass tagging one existing
	// node. Kinds is set alongside AddedKinds so the write actually lands
	// (dawgs' pg driver batch path reads Kinds, not AddedKinds, as the set of
	// kinds to union in -- see NodeUpdateParameters.Append in
	// drivers/pg/batch.go). The Tag node count must serve the new kind
	// immediately. ===

	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateNodes([]*graph.Node{{
			ID:         c1ID,
			Kinds:      graph.Kinds{stalenessNodeKindTag},
			AddedKinds: graph.Kinds{stalenessNodeKindTag},
			Properties: graph.NewProperties(),
		}})
	}); err != nil {
		t.Fatalf("BatchOperation (UpdateNodes AddedKinds, flow 4): %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 4: added-tag node count serves the just-added kind",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessNodeKindTag) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 4: unrelated node kind (A) still serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessNodeKindA) }, 2)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 4: kind-C relationship count still serves",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindC) }, 1)

	// === Flow 5: not one rebuild happened across flows 2-4 -- every answer
	// above came from the replica the writes themselves updated. A rebuild
	// now serves as the differential check: a snapshot loaded from scratch
	// must agree with every one of those answers. ===

	if got := d.engine.RebuildCount(); got != rebuilds {
		t.Fatalf("flow 5: RebuildCount = %d, want %d -- no rebuild may happen for a written-through write", got, rebuilds)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (flow 5): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("flow 5: engine is not serving immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 5: kind-A relationship count agrees post-rebuild",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 0)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 5: kind-B relationship count agrees post-rebuild",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindB) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 5: added-tag node count agrees post-rebuild",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessNodeKindTag) }, 1)

	requireMarkerDelta(t, buf, servedMarker, 1, "flow 5: a1->a2 shortest path agrees post-rebuild",
		func() int { return shortestPathCount(t, ctx, bt, a1ID, a2ID) }, 0)
}

// stalenessUpsertBaseKind / stalenessUpsertNovelKind are this file's own
// fixture kinds for TestBatchUpdateNodesKindsOnlyUpsertServesImmediately
// below, distinct from stalenessNodeKind*/stalenessEdgeKind* above for the
// same isolation reason those give: this test builds and rebuilds its own
// driver instance, independent of TestWriteThroughEndToEnd, so its counts
// must never be able to collide with that test's fixture.
var (
	stalenessUpsertBaseKind  = graph.StringKind("StalenessUpsertBase")
	stalenessUpsertNovelKind = graph.StringKind("StalenessUpsertNovel")
)

// TestBatchUpdateNodesKindsOnlyUpsertServesImmediately is the write-through
// form of a real staleness-tracking regression: a BatchOperation's
// UpdateNodes call that sets a node's Kinds field to include a novel kind --
// WITHOUT also setting AddedKinds, the one detail every existing caller in
// this codebase happens to always pair together, but nothing in graph.Batch's
// documented contract requires -- must still be seen as a write to that
// novel kind.
//
// dawgs' pg batch driver unions the full Kinds field into the database row
// regardless (NodeUpdateParameters.Append/FormatNodesUpdate, see
// engine.WriteScope.UpsertNodeKinds' doc for the verified SQL), so the node
// genuinely becomes StalenessUpsertNovel. The original failure this test was
// written for was subtle and silent: a node Count on the novel kind matched
// that kind's (empty) bitmap in a snapshot where the kind never existed and
// served 0 instead of 1 -- served, but wrong.
//
// Under write-through the correct answer is stronger than the "declines
// until a rebuild" the test used to assert: the batch's UpdateNodes records
// the node id in its ChangeSet, the applier reads that row back -- kind_ids
// and all -- and republishes it, so the novel-kind Count serves 1
// immediately. The base kind must keep serving 1 throughout, and no rebuild
// may happen; a manual rebuild at the end is the differential check that the
// replica's answer matches a from-scratch load.
func TestBatchUpdateNodesKindsOnlyUpsertServesImmediately(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	var baseID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties(), stalenessUpsertBaseKind)
		if err != nil {
			return err
		}
		baseID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (baseline): %v", err)
	}
	rebuilds := d.engine.RebuildCount()

	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: base-kind node count serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertBaseKind) }, 1)

	// The write under test: Kinds gains a novel kind with AddedKinds left
	// empty.
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateNodes([]*graph.Node{{
			ID:         baseID,
			Kinds:      graph.Kinds{stalenessUpsertBaseKind, stalenessUpsertNovelKind},
			Properties: graph.NewProperties(),
		}})
	}); err != nil {
		t.Fatalf("BatchOperation (Kinds-only upsert): %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "novel-kind node count serves the Kinds-only upsert immediately, and answers correctly",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertNovelKind) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "base-kind node count still serves (the node kept its original kind too)",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertBaseKind) }, 1)

	if got := d.engine.RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- the upsert must be served without any rebuild", got, rebuilds)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (post-upsert): %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "novel-kind node count agrees post-rebuild",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertNovelKind) }, 1)
}

// --- Cypher-serving integration tests ------------------------------------
//
// The four tests below extend this file's write-through narrative to
// engine.TryCypher. Three of them assert the same "the next query serves the
// write" property the builder-serving tests above do, across the shapes that
// used to be the hardest for the retired freshness model to get right: a
// pure property write (which touched no kind at all, so nothing kind-scoped
// ever noticed it), an edge write under a query that also hydrates edge
// properties from PostgreSQL, and a stream of writes landing while queries
// run concurrently. TestCypherMultiGraphGuard covers the one TryCypher-only
// decline that has nothing to do with writes at all: Snapshot.MultiGraph,
// set by LoadSnapshot's global probeMultiGraph, so it gets its own dedicated
// database state (a second, unrelated graph) rather than reusing any
// write-based flow above.

// cypherStringValue runs text -- a Cypher query returning exactly one row
// with exactly one string-valued column -- through db and returns that
// value. It works unchanged whether TryCypher served text from the engine
// or wrappedTransaction.Query fell through to PostgreSQL: both paths
// return an ordinary graph.Result over the same Values()/Next() contract.
func cypherStringValue(t *testing.T, ctx context.Context, db graph.Database, text string) string {
	t.Helper()

	var value string
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(text, nil)
		defer result.Close()

		if !result.Next() {
			return fmt.Errorf("no rows")
		}
		values := result.Values()
		if len(values) != 1 {
			return fmt.Errorf("%d columns, want 1", len(values))
		}
		s, ok := values[0].(string)
		if !ok {
			return fmt.Errorf("value %#v is not a string", values[0])
		}
		value = s

		if result.Next() {
			return fmt.Errorf("more than one row")
		}
		return result.Error()
	}); err != nil {
		t.Fatalf("cypherStringValue(%q): %v", text, err)
	}
	return value
}

// cypherPathCount runs text -- a Cypher shortestPath/allShortestPaths query
// -- through db via dawgs' own ops.FetchByQuery and returns how many paths
// came back. FetchByQuery's mapper protocol is exactly what
// internal/engine/serve_cypher.go's cypherRowsResult satisfies (see
// internal/engine/cypher_integration_test.go's drainEngineResult, which
// proves this against the real type), so -- like cypherStringValue above --
// this works unchanged whether the query was served by the engine or
// delegated to PostgreSQL.
func cypherPathCount(t *testing.T, ctx context.Context, db graph.Database, text string) int {
	t.Helper()

	var count int
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		qr, err := ops.FetchByQuery(tx, text)
		if err != nil {
			return err
		}
		count = len(qr.Paths)
		return nil
	}); err != nil {
		t.Fatalf("cypherPathCount(%q): %v", text, err)
	}
	return count
}

// requireDecline runs query and requires that engine.decline's shared
// "bloodtrail: path engine declined" event -- the same method
// TryAllShortestPaths/servePathQuery and TryCypher both log through,
// distinguished only by their "reason" attr -- fired with exactly
// wantReason since the call began, and that the value query produced
// (necessarily PostgreSQL's own answer: a declined TryCypher call never
// returns a result at all) still equals want. declineReason
// (prebuilt_corpus_integration_test.go, this same package) does the actual
// log-tail parsing, the identical helper that file's own corpus
// differential suite already relies on to report *why* a corpus query
// unexpectedly delegated.
func requireDecline[T comparable](t *testing.T, buf *lockedBuffer, wantReason string, label string, query func() T, want T) {
	t.Helper()

	before := buf.String()
	got := query()
	tail := buf.String()[len(before):]

	if reason := declineReason(tail); reason != wantReason {
		t.Fatalf("%s: decline reason = %q, want %q\ncaptured log:\n%s", label, reason, wantReason, tail)
	}
	if got != want {
		t.Fatalf("%s: got %v, want %v", label, got, want)
	}
}

// cypherStalenessNodeKind is TestCypherPropertyOnlyWriteServesImmediately's
// own fixture kind, distinctly named for the same collision-avoidance reason
// stalenessNodeKind*/stalenessUpsert* above are.
var cypherStalenessNodeKind = graph.StringKind("CypherStalenessNode")

// TestCypherPropertyOnlyWriteServesImmediately covers the narrowest write
// shape there is: graph.Transaction.UpdateNode with neither AddedKinds nor
// DeletedKinds set changes only a property value, touching no kind anywhere.
//
// That shape used to be the sharpest illustration of the old two-model
// freshness split -- a kind-scoped builder query kept serving straight
// through it, while a Cypher read of that very property had to delegate,
// because TryCypher could only ask the coarse whole-generation freshness
// bit. Write-through collapses both models into one: the transaction records
// the node id, the applier reads the row back and republishes its property
// bag, and the Cypher read serves the NEW value immediately.
//
// Flow: seed one CypherStalenessNode with name="before", rebuild, confirm
// both a Cypher property read and a kind-scoped node Count serve. The
// property-only write sets name="after". The Cypher read must now serve
// "after" -- from the replica, not from PostgreSQL -- with no rebuild in
// between, while the kind-scoped node Count keeps serving its unchanged
// count.
func TestCypherPropertyOnlyWriteServesImmediately(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	var nodeID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("name", "before"), cypherStalenessNodeKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (baseline): %v", err)
	}
	rebuilds := d.engine.RebuildCount()

	text := fmt.Sprintf(`MATCH (n:CypherStalenessNode) WHERE id(n) = %d RETURN n.name`, nodeID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: cypher property read serves",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "before")

	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: kind-scoped node count serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, cypherStalenessNodeKind) }, 1)

	// The write under test: properties only, no AddedKinds/DeletedKinds.
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(&graph.Node{ID: nodeID, Properties: graph.NewProperties().Set("name", "after")})
	}); err != nil {
		t.Fatalf("WriteTransaction (property-only update): %v", err)
	}

	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("engine stopped serving after a property-only write; write-through must keep it serving")
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "after the property-only write: the cypher read serves the NEW value from the replica",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "after")

	requireMarkerDelta(t, buf, builderServedMarker, 1, "after the property-only write: kind-scoped node count still serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, cypherStalenessNodeKind) }, 1)

	if got := d.engine.RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- a property-only write must be served without any rebuild", got, rebuilds)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (post-write): %v", err)
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "post-rebuild: the cypher read agrees with the replica's own answer",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "after")
}

// cypherHydrationNodeKind and cypherHydrationEdgeKind are
// TestCypherShortestPathServesEdgeWritesImmediately's own fixture kinds.
var (
	cypherHydrationNodeKind = graph.StringKind("CypherHydrationNode")
	cypherHydrationEdgeKind = graph.StringKind("CypherHydrationEdge")
)

// TestCypherShortestPathServesEdgeWritesImmediately is the edge-side
// counterpart to the property-write test above, and covers the one serving
// path that still performs a PostgreSQL round trip after executing:
// TryCypher's edge-property hydration (engine.go's step 10). Edges are the
// one thing the replica holds structurally but not fully -- their property
// bags are read from PostgreSQL on demand -- so a path query over a
// written-through edge exercises the delta and that round trip together.
//
// This test used to force a write into the window between execution and
// hydration, through a test-only seam, and assert that TryCypher noticed and
// declined. That recheck is gone with the freshness model it belonged to: a
// View is immutable and was current when the query captured it, and a write
// landing mid-query publishes a NEW View for the next query rather than
// making this one wrong. What is asserted instead is the property that
// recheck was standing in for -- every write is visible to the next query:
//
//  1. baseline: the s->e shortestPath serves one path;
//  2. the edge is deleted through a batch: the same query serves ZERO paths,
//     immediately;
//  3. a fresh edge is created between the same endpoints: the query serves
//     one path again -- and this time the edge exists only in the replica's
//     delta, so hydrating its properties by its brand-new database id is
//     part of what is being proven.
//
// No rebuild happens across any of it.
func TestCypherShortestPathServesEdgeWritesImmediately(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	var (
		startID, endID graph.ID
		edgeID         graph.ID
	)
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		s, err := tx.CreateNode(graph.NewProperties(), cypherHydrationNodeKind)
		if err != nil {
			return err
		}
		e, err := tx.CreateNode(graph.NewProperties(), cypherHydrationNodeKind)
		if err != nil {
			return err
		}
		rel, err := tx.CreateRelationshipByIDs(s.ID, e.ID, cypherHydrationEdgeKind, graph.NewProperties().Set("weight", "one"))
		if err != nil {
			return err
		}
		startID, endID, edgeID = s.ID, e.ID, rel.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := d.engine.RebuildCount()

	text := fmt.Sprintf(`MATCH p = shortestPath((s)-[:CypherHydrationEdge*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p`, startID, endID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: shortestPath cypher query serves",
		func() int { return cypherPathCount(t, ctx, bt, text) }, 1)

	// (2) Delete the only edge: the path must vanish from the replica at once.
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteRelationship(edgeID)
	}); err != nil {
		t.Fatalf("BatchOperation (delete the edge): %v", err)
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "after deleting the edge: the shortestPath query serves zero paths",
		func() int { return cypherPathCount(t, ctx, bt, text) }, 0)

	// (3) Re-create it: the path is back, this time through an edge that
	// exists only in the replica's delta -- hydrating its properties by its
	// brand-new database id is part of what serving it requires.
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(startID, endID, cypherHydrationEdgeKind, graph.NewProperties().Set("weight", "two"))
		return err
	}); err != nil {
		t.Fatalf("WriteTransaction (re-create the edge): %v", err)
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "after re-creating the edge: the shortestPath query serves one path again, over a delta-only edge",
		func() int { return cypherPathCount(t, ctx, bt, text) }, 1)

	if got := d.engine.RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- edge writes must be served without any rebuild", got, rebuilds)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (post-write): %v", err)
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "post-rebuild: the shortestPath query agrees with the replica's own answer",
		func() int { return cypherPathCount(t, ctx, bt, text) }, 1)
}

// cypherStaleRecheckNodeKind is
// TestCypherServesConsistentlyDuringConcurrentWrites' own fixture kind.
var cypherStaleRecheckNodeKind = graph.StringKind("CypherStaleRecheckNode")

// TestCypherServesConsistentlyDuringConcurrentWrites is the concurrency
// half of the same claim, for a pure-snapshot query (a property read with no
// edge or path column, so nothing to hydrate).
//
// It replaces a test that used a seam to force a write into the window
// between execution and the result being returned, and asserted TryCypher
// declined. Write-through removes that recheck along with the model it
// belonged to, so what matters now is what the recheck was protecting
// against: a query running while writes land must never return a torn or
// invented answer. Each query executes against whichever immutable View it
// captured, so its answer must be one of the values actually written -- and
// once the writer has finished, the next query must see the final one.
//
// The writer goroutine walks a known sequence of property values while the
// reader issues the same Cypher property read over and over; every read must
// be SERVED from the replica (never delegated) and must return a value from
// that known set. A final read after the writer joins must return the last
// value written, and no rebuild may have happened anywhere.
func TestCypherServesConsistentlyDuringConcurrentWrites(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	var nodeID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("name", "before"), cypherStaleRecheckNodeKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := d.engine.RebuildCount()

	text := fmt.Sprintf(`MATCH (n:CypherStaleRecheckNode) WHERE id(n) = %d RETURN n.name`, nodeID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: plain property-read cypher query serves",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "before")

	const writes = 12

	allowed := map[string]struct{}{"before": {}}
	for i := 1; i <= writes; i++ {
		allowed[fmt.Sprintf("v%d", i)] = struct{}{}
	}
	last := fmt.Sprintf("v%d", writes)

	writerErr := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= writes; i++ {
			value := fmt.Sprintf("v%d", i)
			if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
				return tx.UpdateNode(&graph.Node{ID: nodeID, Properties: graph.NewProperties().Set("name", value)})
			}); err != nil {
				writerErr <- fmt.Errorf("concurrent write %q: %w", value, err)
				return
			}
		}
		writerErr <- nil
	}()

	for i := 0; i < writes*2; i++ {
		before := markerCount(buf, cypherServedMarker)
		got := cypherStringValue(t, ctx, bt, text)
		if delta := markerCount(buf, cypherServedMarker) - before; delta != 1 {
			t.Fatalf("concurrent read %d: cypher served marker delta %d, want 1 -- a write landing mid-flight must not stop the engine serving", i, delta)
		}
		if _, ok := allowed[got]; !ok {
			t.Fatalf("concurrent read %d returned %q, which was never written", i, got)
		}
	}

	wg.Wait()
	if err := <-writerErr; err != nil {
		t.Fatalf("%v", err)
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "after the concurrent writer finished: the next read serves the final value",
		func() string { return cypherStringValue(t, ctx, bt, text) }, last)

	if got := d.engine.RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- concurrent writes must be served without any rebuild", got, rebuilds)
	}
}

// cypherMultiGraphNodeKind/cypherMultiGraphSecondKind are
// TestCypherMultiGraphGuard's own fixture kinds -- one for the node living
// in the driver's default graph, one for the node in the second, unrelated
// graph this test creates purely to trip probeMultiGraph.
// cypherMultiGraphSecondGraphName names that second graph.
var (
	cypherMultiGraphNodeKind        = graph.StringKind("CypherMultiGraphNode")
	cypherMultiGraphSecondKind      = graph.StringKind("CypherMultiGraphSecondNode")
	cypherMultiGraphSecondGraphName = "bloodtrail_test_second_graph"
)

// TestCypherMultiGraphGuard: TryCypher must decline reasonMultiGraph the
// instant LoadSnapshot's probeMultiGraph
// (internal/engine/load.go) finds a second graph holding at least one node
// anywhere in the database -- regardless of whether that second graph has
// anything to do with the query being asked, since the interpreter has no
// notion of which graph a query is scoped to at all (engine.go's own doc
// for reasonMultiGraph). The builder-serving path carries no such guard
// (serve_builder.go never inspects Snapshot.MultiGraph), so a builder query
// on the very same default-graph fixture must keep serving, completely
// unaffected -- the one place the two serving paths still differ.
//
// The second graph is created directly through the raw pg driver (not the
// wrapped bloodtrail one) via WithGraph, which dawgs' own SchemaManager.
// AssertGraph lazily creates the moment a node is written to it -- exactly
// the same lazy-creation path CreateNode always takes for kinds. Both
// drivers share the one PostgreSQL connection pool graphtest.OpenPG hands
// out (dawgs.Config.Pool, passed through unchanged to bt below), so the
// second graph's row and node are visible to whichever driver's own
// LoadSnapshot later runs probeMultiGraph's global, cross-graph query,
// regardless of which driver object created them.
//
// t.Cleanup wipes every node and edge across all graphs (WipeGraph's
// documented scope: "truncates the partitioned node and edge tables ...
// across all graphs", driver.go's own WipeGraph doc in the dawgs pg
// driver) before this test returns, so the second graph's node cannot
// leave Snapshot.MultiGraph permanently true for whatever test in this
// shared, long-lived integration database happens to run next -- it only
// left the *graph* catalog row behind, which probeMultiGraph's own
// EXISTS-a-node join ignores.
//
// This wipe is registered via t.Cleanup, not deferred inline, and
// deliberately registered AFTER bt's own Close -- also moved to t.Cleanup
// rather than a plain defer -- so that t.Cleanup's documented last-added-
// first-called order runs the wipe *before* bt.Close: bt.Driver (the
// wrapped driver's own embedded pg.Driver) closes the exact pgxpool.Pool
// this test shares with pgDriver (both opened against dawgs.Config.Pool /
// graphtest.OpenPG's same pool), so a wipe ordered after it would find the
// pool already closed.
func TestCypherMultiGraphGuard(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })
	t.Cleanup(func() { graphtest.WipeGraph(t, pgDriver) })

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	var nodeID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.CreateNode(graph.NewProperties().Set("name", "solo"), cypherMultiGraphNodeKind)
		if err != nil {
			return err
		}
		nodeID = n.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	// A second, unrelated graph with exactly one node -- enough for
	// probeMultiGraph to flag Snapshot.MultiGraph true on the very next
	// rebuild, no matter what the query below asks about.
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		tx = tx.WithGraph(graph.Graph{Name: cypherMultiGraphSecondGraphName})
		_, err := tx.CreateNode(graph.NewProperties(), cypherMultiGraphSecondKind)
		return err
	}); err != nil {
		t.Fatalf("create second graph's node: %v", err)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("engine reports stale immediately after RebuildNow")
	}

	text := fmt.Sprintf(`MATCH (n:CypherMultiGraphNode) WHERE id(n) = %d RETURN n.name`, nodeID)

	requireDecline(t, buf, "multi_graph", "cypher query declines with reasonMultiGraph while a second populated graph exists, and still returns the correct (PostgreSQL-delegated) value",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "solo")

	requireMarkerDelta(t, buf, builderServedMarker, 1, "kind-scoped builder query still serves; the MultiGraph guard is TryCypher-only",
		func() int64 { return nodeCountByKind(t, ctx, bt, cypherMultiGraphNodeKind) }, 1)
}

// writeTxReadNodeKind and writeTxReadObjectID are
// TestWriteTransactionReadAfterWriteDelegatesToPG's own fixture, named
// distinctly from every other integration test's fixture in this shared,
// long-lived database (see stalenessNodeKindA's doc for why that matters).
var writeTxReadNodeKind = graph.StringKind("WriteTxReadNode")

const writeTxReadObjectID = "WriteTxRead-1"

// TestWriteTransactionReadAfterWriteDelegatesToPG pins the correctness rule
// a WriteTransaction must uphold once it has performed any write: every
// subsequent read run against that same transaction must see PostgreSQL's
// own view -- including the transaction's own uncommitted write -- and must
// never be served from the in-memory engine, which only ever learns about a
// write at commit (Driver.WriteTransaction's own engine.NoteWrite call,
// driver.go). Reading a stale, pre-write snapshot back from inside the very
// transaction that just wrote would silently hide the caller's own write
// from itself.
//
// The Cypher read below (`MATCH (n:WriteTxReadNode) WHERE n.objectid = ...
// RETURN n.objectid`) is deliberately the same WHERE-objectid-anchor, RETURN
// -property shape TestCypherStalenessPropertyOnlyWrite above and
// internal/engine/cypher_integration_test.go's "duplicate objectid anchor"
// case already prove TryCypher recognizes and serves once the engine is
// fresh and idle -- confirmed again below, by rerunning the identical query
// after this test's own transaction commits and the engine rebuilds. That
// makes the marker delta of 0 asserted for the in-transaction read
// meaningful evidence of a real decline, not merely "an unrecognized query
// fell through anyway".
//
// Composition finding backing this pin (write_observer.go's own doc on
// observingTransaction has the full argument): Driver.WriteTransaction
// (driver.go) wraps the delegate's tx directly in an observingTransaction,
// never in a wrappedTransaction (transaction.go) -- so
// observingTransaction.Query/.Nodes()/.Relationships() never reach
// engine.TryCypher/TryNodeCount/TryRelCount/etc. at all, whether or not a
// write has already happened on this transaction. This test's core
// assertion therefore already held, unconditionally, before this change;
// see observingTransaction.wrote()'s doc for why the explicit wrote() guard
// is still added to write_observer.go as defense against a future change to
// that composition, rather than skipped because this pin currently passes
// either way.
func TestWriteTransactionReadAfterWriteDelegatesToPG(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })
	t.Cleanup(func() { graphtest.WipeGraph(t, pgDriver) })

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	if err := bt.AssertSchema(ctx, graph.Schema{DefaultGraph: graph.Graph{Name: graphtest.GraphName}}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	text := fmt.Sprintf(`MATCH (n:WriteTxReadNode) WHERE n.objectid = '%s' RETURN n.objectid`, writeTxReadObjectID)

	// === The pin: create, then read, inside the SAME WriteTransaction. ===
	before := markerCount(buf, cypherServedMarker)
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateNode(graph.NewProperties().Set("objectid", writeTxReadObjectID), writeTxReadNodeKind); err != nil {
			return err
		}

		result := tx.Query(text, nil)
		defer result.Close()

		if !result.Next() {
			return fmt.Errorf("read-after-write: no rows (query: %s)", text)
		}
		values := result.Values()
		if len(values) != 1 {
			return fmt.Errorf("read-after-write: %d columns, want 1", len(values))
		}
		got, ok := values[0].(string)
		if !ok {
			return fmt.Errorf("read-after-write: value %#v is not a string", values[0])
		}
		if got != writeTxReadObjectID {
			return fmt.Errorf("read-after-write: got objectid %q, want %q -- the read did not see this transaction's own write", got, writeTxReadObjectID)
		}
		if result.Next() {
			return fmt.Errorf("read-after-write: more than one row")
		}
		return result.Error()
	}); err != nil {
		t.Fatalf("WriteTransaction (create then read): %v", err)
	}
	if delta := markerCount(buf, cypherServedMarker) - before; delta != 0 {
		t.Fatalf("cypher read inside the write transaction was served by the engine (marker delta %d, want 0)\ncaptured log:\n%s", delta, buf.String())
	}

	// === Sanity check on the premise: the identical query, run once the
	// transaction has committed and the engine has rebuilt, DOES serve --
	// proving the marker delta of 0 above was a real decline, not a query
	// shape TryCypher was never going to recognize in the first place. ===
	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "post-commit, post-rebuild: the identical query now serves from the engine",
		func() string { return cypherStringValue(t, ctx, bt, text) }, writeTxReadObjectID)
}

// batchReadNodeKind and batchReadObjectID are
// TestBatchReadAfterWriteDelegatesToPG's own fixture, named distinctly from
// every other integration test's fixture in this shared, long-lived
// database (see stalenessNodeKindA's doc for why that matters).
var batchReadNodeKind = graph.StringKind("BatchReadNode")

const batchReadObjectID = "BatchRead-1"

// TestBatchReadAfterWriteDelegatesToPG is
// TestWriteTransactionReadAfterWriteDelegatesToPG's batch analogue: once a
// BatchOperation has performed a write, a subsequent read through that same
// batch's Nodes()/Relationships() wrappers must reflect PostgreSQL's own
// view of what was just flushed, never a stale in-memory engine answer.
// UpdateNodeBy is an upsert (graph.Batch's own doc: "in the case where the
// node does not yet exist, created") -- since no BatchReadNode exists yet in
// this test's freshly wiped graph, the call below creates one. UpdateNodeBy
// buffers, though (dawgs' own nodeUpdateByBuffer): dawgs documents that
// batch.Nodes()/Relationships() "delegate straight to the inner transaction
// and execute immediately... they do not see Go-side buffered writes, but
// do see flushed (committed) chunks" -- so this test calls batch.Commit()
// (graph.Batch's own doc: "calls to commit this batch transaction right
// away") between the two to flush that chunk before Count() runs. That
// Commit call is itself the mid-batch-commit path observingBatch.Commit
// (write_observer.go) exists for: it also flushes this batch's WriteScope
// to the engine immediately, rather than waiting for Driver.BatchOperation's
// own final NoteWrite -- so a read run right after it, through a batch that
// has now written, has the engine's own bookkeeping already caught up too.
//
// See TestWriteTransactionReadAfterWriteDelegatesToPG's doc for the
// composition finding this pin shares: observingBatch.Nodes() (write_
// observer.go) wraps the inner batch's raw NodeQuery directly, never a
// recordingNodeQuery (node_query.go), so engine.TryNodeCount is never
// reachable from a batch's Nodes() at all, written-to or not. The marker
// delta of 0 asserted below is still meaningful evidence, not a vacuous
// pass, because the second half of this test proves the identical
// structural count IS a servable shape once the batch has committed and the
// engine has rebuilt.
func TestBatchReadAfterWriteDelegatesToPG(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	t.Setenv(EnvEnginePollInterval, "1h")
	buf := installLogCapture(t)

	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	bt, err := dawgs.Open(ctx, DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	t.Cleanup(func() { _ = bt.Close(ctx) })
	t.Cleanup(func() { graphtest.WipeGraph(t, pgDriver) })

	d, ok := bt.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", bt)
	}

	// BatchReadNode is asserted up front, via schema.Graphs (not
	// DefaultGraph.Nodes, which assertSchema never reads) -- unlike
	// WriteTransaction's CreateNode, which registers a brand new kind
	// synchronously on the call that first uses it, a batch's UpdateNodeBy
	// only registers the kinds it names when its buffered write actually
	// flushes (dawgs' flushNodeUpsertBatch -> AssertKinds,
	// drivers/pg/batch.go), which has not happened yet by the time this
	// test's Count() call below needs to translate the SAME kind name into
	// a kind id. Asserting it here sidesteps that ordering pitfall
	// entirely, rather than being what this test is trying to prove.
	if err := bt.AssertSchema(ctx, graph.Schema{
		Graphs:       []graph.Graph{{Name: graphtest.GraphName, Nodes: graph.Kinds{batchReadNodeKind}}},
		DefaultGraph: graph.Graph{Name: graphtest.GraphName},
	}); err != nil {
		t.Fatalf("assert schema: %v", err)
	}

	// === The pin: UpdateNodeBy (an upsert -- no BatchReadNode exists yet,
	// so this creates one), then Count, inside the SAME BatchOperation. ===
	var gotCount int64
	before := markerCount(buf, builderServedMarker)
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		// No IdentityKind/IdentityProperties: formatConflictMatcher's empty
		// case targets the always-present (id, graph_id) constraint rather
		// than an objectid uniqueness index this test's schema never
		// declares (BloodHound's own schema declares one in production;
		// asserting it here would be testing dawgs' index machinery, not
		// this task's read/write delegation rule). Node.ID is left at its
		// zero value, so there is no existing (0, graph_id) row to
		// conflict with -- this upsert genuinely creates a new node.
		update := graph.NodeUpdate{
			Node: &graph.Node{
				Properties: graph.NewProperties().Set("objectid", batchReadObjectID),
				Kinds:      graph.Kinds{batchReadNodeKind},
			},
		}
		if err := batch.UpdateNodeBy(update); err != nil {
			return err
		}

		// UpdateNodeBy buffers into dawgs' own nodeUpdateByBuffer -- it is
		// not visible to PostgreSQL until flushed: dawgs documents that
		// batch.Nodes()/Relationships() "do not see Go-side buffered
		// writes, but do see flushed (committed) chunks". Commit is the
		// documented way to flush a chunk mid-batch and keep going
		// (graph.Batch's own doc: "calls to commit this batch transaction
		// right away") -- observingBatch.Commit (write_observer.go)
		// additionally flushes scope to the engine right here, so a read
		// run right after it, through a batch that has now written, has
		// the engine's own bookkeeping already caught up too.
		if err := batch.Commit(); err != nil {
			return err
		}

		n, err := batch.Nodes().Filter(query.Kind(query.Node(), batchReadNodeKind)).Count()
		gotCount = n
		return err
	}); err != nil {
		t.Fatalf("BatchOperation (UpdateNodeBy then Count): %v", err)
	}
	if delta := markerCount(buf, builderServedMarker) - before; delta != 0 {
		t.Fatalf("node count inside the batch was served by the engine (marker delta %d, want 0)\ncaptured log:\n%s", delta, buf.String())
	}
	if gotCount != 1 {
		t.Fatalf("Count inside the batch = %d, want 1 (the node UpdateNodeBy just upserted, seen via PostgreSQL)", gotCount)
	}

	// === Sanity check on the premise: the identical structural count,
	// once the batch has committed and the engine has rebuilt, DOES serve. ===
	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "post-commit, post-rebuild: the identical node count now serves from the engine",
		func() int64 { return nodeCountByKind(t, ctx, bt, batchReadNodeKind) }, 1)
}
