// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file is Task 11 of the milestone-3 plan: an end-to-end integration
// suite proving kind-scoped staleness through the REAL driver write paths
// (WriteTransaction, BatchOperation), not through internal/engine's own
// white-box unit tests, which call Engine.NoteWrite directly and never
// exercise observingTransaction/observingBatch (write_observer.go) at all.
//
// # File placement: root package, not internal/engine
//
// The task brief called for this file to live at
// internal/engine/staleness_integration_test.go. It lives here instead, at
// the repository root as part of package bloodtrail, for an unavoidable
// reason: the write paths under test -- observingTransaction,
// observingBatch, and Driver.WriteTransaction/BatchOperation themselves --
// are defined in the root package (driver.go, write_observer.go), and the
// root package imports internal/engine (for engine.Engine, engine.WriteScope,
// engine.New). internal/engine therefore cannot import the root package to
// drive writes through Driver.WriteTransaction/BatchOperation -- doing so
// would be an import cycle. Putting this file in the root package instead
// resolves that: the root package already imports internal/engine, so a test
// here can freely construct a *bloodtrail.Driver via dawgs.Open and drive
// real writes through it.
//
// # White-box access, and its actual limits
//
// Being in package bloodtrail (not bloodtrail_test) gives this file
// unexported access to Driver's own `engine *engine.Engine` field, which
// bloodtrail_test-package tests (engine_serving_integration_test.go) cannot
// reach directly. That access is used below for two genuinely useful,
// exported-method calls: d.engine.RebuildNow (a deterministic, on-demand
// rebuild -- no need to wire up a datapipe_status table and wait on the
// poller) and d.engine.Fresh (the plain whole-generation freshness bit).
//
// It does NOT, however, unlock the kind-scoped predicates that actually
// decide staleness (nodeKindsClean, edgeKindsClean, allNodesClean,
// allEdgesClean, in internal/engine/marks.go): those are unexported
// identifiers of package engine, and Go's visibility rules make them
// inaccessible from ANY other package, including this one -- "same package"
// for an unexported identifier means internal/engine itself, not "any
// package that happens to hold a value of an exported type from it". So the
// proof this file builds is deliberately a black-box one: every flow issues
// a real query through the real driver and observes, by exactly how much (0
// or 1), the exact Debug/Info log line the engine emits precisely when it
// serves a query from its in-memory snapshot --
// "bloodtrail: builder engine served" for structural node/relationship
// queries (serve_builder.go's servedOp) and "bloodtrail: path engine served"
// for shortest-path queries (engine.go's TryAllShortestPaths) -- alongside
// the returned value's own correctness. A marker delta of 1 plus a correct
// result means "served from the snapshot, and the snapshot was right to
// serve it"; a delta of 0 plus a correct result means "declined and fell
// through to PostgreSQL, and PostgreSQL was consulted correctly". That is
// the same style of evidence engine_serving_integration_test.go's existing
// TestNodeQueryServesFromLiveDriver/TestEngineServesFromLiveDriver already
// rely on, just assembled here into one continuous, kind-scoped narrative.
package bloodtrail

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// servedMarker and builderServedMarker are the exact messages
// TryAllShortestPaths/TryCypher (engine.go) and TryNodeCount/TryRelCount/etc.
// (serve_builder.go's servedOp) log whenever they actually serve a query
// from the in-memory snapshot -- duplicated here from
// engine_serving_integration_test.go (a different package, bloodtrail_test,
// so its unexported consts are not visible from here) for the same reason
// that file gives: only this test needs to recognize them in captured log
// output.
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

// TestKindScopedStalenessEndToEnd is Task 11's deliverable: five sequential
// flows, each building on the previous one's dirt, proving that a write
// through the real driver invalidates exactly the kinds it touched --
// leaving every other kind free to keep serving from the in-memory
// snapshot -- while a shortest-path query (whose freshness check is the
// coarser, whole-snapshot Fresh(), not the kind-scoped marks) goes stale on
// ANY write at all. See this file's own top-of-file doc for why the evidence
// below is a black-box one (log marker deltas plus result correctness)
// rather than calls into the engine's own unexported freshness predicates.
//
// Fixture shape (all built once, up front, through bt.WriteTransaction, so
// every kind's baseline state is exactly known rather than inferred):
//
//   - StalenessNodeA: a1, a2, joined by one StalenessEdgeA edge.
//   - StalenessNodeB: b1, b2, with NO edge yet -- flow 2 creates one.
//   - StalenessNodeC: c1, c2, joined by one StalenessEdgeC edge -- this pair
//     stays completely untouched by every write below, making it this
//     test's "nothing here changed" witness for both the relationship-count
//     flows (3, 4) and, via c1, the node-tagging flow (4).
//
// The five flows:
//
//  1. Rebuild, then kind-A relationship Count serves (and a shortest-path
//     query between a1 and a2 also serves, establishing the path-serving
//     baseline flow 2 contrasts against).
//  2. A WriteTransaction creates a kind-B edge. Kind-A Count still serves
//     (kind-A untouched); kind-B Count now delegates (just written) but
//     still answers correctly; the SAME a1/a2 shortest-path query -- whose
//     kind (A) was not touched by this write -- now ALSO delegates, because
//     path-serving freshness (Fresh(), the plain write-generation counter)
//     has no kind-scoped exception the way the builder-serving path does.
//  3. A BatchOperation deletes the one kind-A edge. Kind-A now delegates too
//     (its only edge just vanished); kind-B still delegates (dirty since
//     flow 2, untouched by this write); kind-C, never touched, still serves.
//  4. A BatchOperation runs UpdateNodes on c1 with AddedKinds=[Tag] --
//     simulating an analysis pass tagging one existing node. Only the Tag
//     node kind dirties: a node Count on Tag delegates, a node Count on the
//     wholly unrelated kind A still serves, and kind-C's relationship Count
//     is unaffected (a node-kind write has nothing to say about edge marks).
//  5. A manual rebuild. Every one of kind-A/kind-B/Tag's counts serves
//     again, each agreeing with the exact value this test's own writes
//     produced.
func TestKindScopedStalenessEndToEnd(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	// An hour-long poll interval means the poller's own ticker will not
	// fire even once during this test's lifetime, so every rebuild observed
	// below is the direct, deterministic result of this test's own
	// d.engine.RebuildNow calls -- no datapipe_status table, and no race
	// against a background rebuild, needed at all. Must be set before
	// dawgs.Open: Settings are read from the environment exactly once, at
	// Open time.
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

	// === Flow 1: rebuild, then kind-A relationship Count and a shortest-path
	// query both serve from the fresh snapshot. ===

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (flow 1): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("flow 1: engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 1: kind-A relationship count serves",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 1)

	requireMarkerDelta(t, buf, servedMarker, 1, "flow 1: a1->a2 shortest-path query serves (baseline for flow 2's contrast)",
		func() int { return shortestPathCount(t, ctx, bt, a1ID, a2ID) }, 1)

	// === Flow 2: a WriteTransaction creates one kind-B edge. Kind-A keeps
	// serving (untouched); kind-B now delegates (just written) but still
	// answers correctly; the SAME shortest-path query that just served in
	// flow 1 now ALSO delegates, since Fresh() (whole-generation) has no
	// kind-scoped carve-out the way the builder-serving path does -- the
	// documented difference this flow exists to demonstrate. ===

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(b1ID, b2ID, stalenessEdgeKindB, graph.NewProperties())
		return err
	}); err != nil {
		t.Fatalf("WriteTransaction (create kind-B edge, flow 2): %v", err)
	}

	if _, fresh := d.engine.Fresh(); fresh {
		t.Fatalf("flow 2: engine still reports fresh after a write; Fresh() must go stale on any write (whole-generation)")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 2: kind-A relationship count still serves (kind-A untouched by the kind-B write)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 0, "flow 2: kind-B relationship count delegates (kind-B just written)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindB) }, 1)

	requireMarkerDelta(t, buf, servedMarker, 0, "flow 2: shortest-path query now delegates too (whole-generation staleness, unlike the kind-scoped builder path above)",
		func() int { return shortestPathCount(t, ctx, bt, a1ID, a2ID) }, 1)

	// === Flow 3: a BatchOperation deletes the one kind-A edge (present in
	// the snapshot flow 1 built). Kind-A now delegates too; kind-B still
	// delegates (dirty since flow 2, untouched by this write); kind-C,
	// never touched by anything so far, still serves. ===

	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteRelationship(edgeAID)
	}); err != nil {
		t.Fatalf("BatchOperation (delete kind-A edge, flow 3): %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 0, "flow 3: kind-A relationship count now delegates (its only edge was just deleted)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 0)

	requireMarkerDelta(t, buf, builderServedMarker, 0, "flow 3: kind-B relationship count still delegates (dirty since flow 2)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindB) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 3: kind-C relationship count still serves (never touched)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindC) }, 1)

	// === Flow 4: a BatchOperation runs UpdateNodes on c1 with
	// AddedKinds=[Tag] -- simulating an analysis pass tagging one existing
	// node. Kinds is set alongside AddedKinds so the write actually lands
	// (dawgs' pg driver batch path reads Kinds, not AddedKinds, as the set of
	// kinds to union in -- see NodeUpdateParameters.Append in
	// drivers/pg/batch.go -- while bloodtrail's own kind-scoped dirty-marking
	// reads AddedKinds/DeletedKinds, see touchNodeKindDelta in
	// write_observer.go; setting both keeps this test correct regardless of
	// that library-internal asymmetry). Only the Tag node kind dirties: a
	// node Count on Tag delegates, a node Count on the wholly unrelated kind
	// A still serves, and kind-C's relationship Count is unaffected -- a
	// node-kind write has nothing to say about any edge mark. ===

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

	requireMarkerDelta(t, buf, builderServedMarker, 0, "flow 4: added-tag node count delegates (c1 was just tagged)",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessNodeKindTag) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 4: unrelated node kind (A) still serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessNodeKindA) }, 2)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 4: kind-C relationship count still serves (edge queries unaffected by a node-kind write)",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindC) }, 1)

	// === Flow 5: a manual rebuild picks up every write from flows 2-4 at
	// once. Kind-A, kind-B, and the Tag node kind all serve again, each
	// agreeing with the exact value this test's own writes produced. ===

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (flow 5): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("flow 5: engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 5: kind-A relationship count serves again post-rebuild",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindA) }, 0)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 5: kind-B relationship count serves again post-rebuild",
		func() int64 { return relCountByKind(t, ctx, bt, stalenessEdgeKindB) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "flow 5: added-tag node count serves again post-rebuild",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessNodeKindTag) }, 1)
}
