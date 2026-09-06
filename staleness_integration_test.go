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

// stalenessUpsertBaseKind / stalenessUpsertNovelKind are this file's own
// fixture kinds for TestBatchUpdateNodesKindsOnlyUpsertDirtiesExactKind
// below, distinct from stalenessNodeKind*/stalenessEdgeKind* above for the
// same isolation reason those give: this test builds and rebuilds its own
// driver instance, independent of TestKindScopedStalenessEndToEnd, so its
// counts must never be able to collide with that test's fixture.
var (
	stalenessUpsertBaseKind  = graph.StringKind("StalenessUpsertBase")
	stalenessUpsertNovelKind = graph.StringKind("StalenessUpsertNovel")
)

// TestBatchUpdateNodesKindsOnlyUpsertDirtiesExactKind is the end-to-end
// regression test for a real staleness-tracking gap: a BatchOperation's
// UpdateNodes call that sets a node's
// Kinds field to include a novel kind -- WITHOUT also setting AddedKinds,
// the one detail every existing caller in this codebase happens to always
// pair together, but nothing in graph.Batch's documented contract requires
// -- must still be seen as a write to that novel kind.
//
// Before this fix, observingBatch.UpdateNodes only ever looked at
// AddedKinds/DeletedKinds (touchNodeKindDelta), so a Kinds-only change like
// this recorded an Empty() scope: NoteWrite saw nothing to mark, even though
// dawgs' pg batch driver actually unions the full Kinds field into the
// database row regardless (NodeUpdateParameters.Append/FormatNodesUpdate,
// see engine.WriteScope.UpsertNodeKinds' doc for the verified SQL). A node
// Count on the novel kind would then incorrectly report itself "clean"
// against the pre-write snapshot generation, match this kind's bitmap in a
// snapshot where the kind never even existed (empty bitmap, since
// NodesOfKind is nil-safe -- serve_builder.go), and serve 0 instead of the
// correct 1: served, but silently wrong. This test's central assertion is
// therefore not just "declines" but "declines AND returns the right value",
// via requireMarkerDelta's combined check.
//
// This test proves the fix end-to-end through the real driver, independent
// of and in addition to TestKindScopedStalenessEndToEnd's flow 4 (which
// exercises the AddedKinds-paired shape every caller uses today and so never
// would have caught this gap): a node created with StalenessUpsertBase is
// later updated via BatchOperation.UpdateNodes with Kinds = [Base, Novel]
// and AddedKinds left nil/empty. A node Count on the novel kind must
// delegate to PostgreSQL immediately after (the builder-serving marker must
// NOT fire) and must still answer correctly (1); a node Count on the
// already-present base kind must keep serving from the snapshot the whole
// time, since UpsertNodeKinds' snapshot set-difference must not dirty a kind
// the node already carried before this write (marks_test.go's
// TestNoteWriteUpsertNodeKindsNovelKindDirtiesOnlyThatKind is this same
// claim's white-box unit-test counterpart). A manual rebuild afterward must
// make the novel kind's count serve too.
func TestBatchUpdateNodesKindsOnlyUpsertDirtiesExactKind(t *testing.T) {
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
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("baseline: engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: base-kind node count serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertBaseKind) }, 1)

	// The write this test guards against: Kinds gains a novel kind with
	// AddedKinds left empty. dawgs' pg batch driver still unions Kinds into
	// the database row (NodeUpdateParameters.Append), so this node genuinely
	// becomes StalenessUpsertNovel too -- the engine must not miss that.
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateNodes([]*graph.Node{{
			ID:         baseID,
			Kinds:      graph.Kinds{stalenessUpsertBaseKind, stalenessUpsertNovelKind},
			Properties: graph.NewProperties(),
		}})
	}); err != nil {
		t.Fatalf("BatchOperation (Kinds-only upsert): %v", err)
	}

	requireMarkerDelta(t, buf, builderServedMarker, 0, "novel-kind node count delegates immediately after the Kinds-only upsert, and still answers correctly",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertNovelKind) }, 1)

	requireMarkerDelta(t, buf, builderServedMarker, 1, "base-kind node count still serves (Base was already present in the snapshot; the upsert's set-difference must not dirty it)",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertBaseKind) }, 1)

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (post-upsert): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("post-upsert: engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "novel-kind node count serves again post-rebuild",
		func() int64 { return nodeCountByKind(t, ctx, bt, stalenessUpsertNovelKind) }, 1)
}

// --- Task 18: Cypher-serving staleness/guard integration tests -----------
//
// The three tests below extend this file's kind-scoped staleness narrative
// to engine.TryCypher, whose own freshness model is deliberately coarser
// than the builder-serving path's (see engine.go's own doc: TryCypher's
// interpreter has no notion of which kinds a query touches, so it can only
// ever ask the plain, whole-generation Fresh() bit -- the same bit
// TestKindScopedStalenessEndToEnd's flow 2 already showed a shortest-path
// query is bound by too). TestCypherStalenessPropertyOnlyWrite goes one
// step further than that flow: a pure property write never touches any
// kind mark at all (write_observer.go's touchNodeKindDelta), so it is
// exactly the write class that leaves a kind-scoped builder query serving
// right through it while a Cypher read of that same property must not.
// TestCypherHydrationRecheck exercises TryCypher's own step-10
// post-hydration recheck, forced deterministically via a small test-only
// seam added to internal/engine/engine.go (SetCypherHydrationRaceHookForTest)
// -- no existing seam already forced this exact race for either serving
// path, so this task adds the minimal one TryCypher needs, mirroring the
// same "capture a snapshot, then check it's still current after I/O"
// pattern servePathQuery's own step 7 already establishes.
// TestCypherMultiGraphGuard covers the one TryCypher-only decline this file
// had not yet exercised: Snapshot.MultiGraph, set by LoadSnapshot's global
// probeMultiGraph, has nothing to do with kind-scoped or whole-generation
// staleness at all, so it gets its own dedicated database state (a second,
// unrelated graph) rather than reusing any write-based flow above.

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

// cypherStalenessNodeKind is TestCypherStalenessPropertyOnlyWrite's own
// fixture kind, distinctly named for the same collision-avoidance reason
// stalenessNodeKind*/stalenessUpsert* above are.
var cypherStalenessNodeKind = graph.StringKind("CypherStalenessNode")

// TestCypherStalenessPropertyOnlyWrite is Task 18's first deliverable: a
// pure property write -- graph.Transaction.UpdateNode with neither
// AddedKinds nor DeletedKinds set -- records nothing at all onto the
// WriteScope write_observer.go's touchNodeKindDelta builds
// (TestObservingTransactionUpdateNodePropertyOnlyLeavesScopeEmpty pins this
// down at the unit level; this test proves the end-to-end consequence). A
// kind-scoped builder query is therefore still "clean" against it and keeps
// serving right through the write -- but engine.NoteWrite bumps the
// write-generation counter unconditionally, before it even looks at
// whether scope is empty (marks.go's noteResolved, step 1 of its own doc),
// so TryCypher's coarser, whole-generation Fresh() check goes stale on this
// exact write anyway. This is a strictly narrower trigger than
// TestKindScopedStalenessEndToEnd's flow 2 (which at least dirtied kind-B's
// own mark): a pure property write dirties no kind's mark whatsoever, yet
// still must flip Cypher serving to delegation.
//
// Flow: seed one CypherStalenessNode with name="before", rebuild, confirm
// both a Cypher property read and a kind-scoped node Count serve. The
// property-only write sets name="after". The Cypher read must now delegate
// to PostgreSQL -- and must return the NEW value, proving the fallback
// actually consults live data rather than any cached answer -- while the
// kind-scoped node Count keeps serving the unchanged count throughout, the
// two-freshness-models contrast this test exists to pin down. A manual
// rebuild afterward restores Cypher serving, now reflecting "after".
func TestCypherStalenessPropertyOnlyWrite(t *testing.T) {
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
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("baseline: engine reports stale immediately after RebuildNow")
	}

	text := fmt.Sprintf(`MATCH (n:CypherStalenessNode) WHERE id(n) = %d RETURN n.name`, nodeID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: cypher property read serves",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "before")

	requireMarkerDelta(t, buf, builderServedMarker, 1, "baseline: kind-scoped node count serves",
		func() int64 { return nodeCountByKind(t, ctx, bt, cypherStalenessNodeKind) }, 1)

	// The write under test: properties only, no AddedKinds/DeletedKinds --
	// touchNodeKindDelta records nothing onto scope for this call, so the
	// CypherStalenessNode kind mark never dirties.
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		return tx.UpdateNode(&graph.Node{ID: nodeID, Properties: graph.NewProperties().Set("name", "after")})
	}); err != nil {
		t.Fatalf("WriteTransaction (property-only update): %v", err)
	}

	if _, fresh := d.engine.Fresh(); fresh {
		t.Fatalf("engine still reports fresh after a property-only write; Fresh() must go stale on any write (whole-generation)")
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 0, "after property-only write: cypher property read delegates, and still returns the live (new) value",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "after")

	requireMarkerDelta(t, buf, builderServedMarker, 1, "after property-only write: kind-scoped node count still serves (no kind mark was ever touched)",
		func() int64 { return nodeCountByKind(t, ctx, bt, cypherStalenessNodeKind) }, 1)

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (post-write): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("post-write: engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "post-rebuild: cypher property read serves again, reflecting the new value",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "after")
}

// cypherHydrationNodeKind and cypherHydrationEdgeKind are
// TestCypherHydrationRecheck's own fixture kinds.
var (
	cypherHydrationNodeKind = graph.StringKind("CypherHydrationNode")
	cypherHydrationEdgeKind = graph.StringKind("CypherHydrationEdge")
)

// TestCypherHydrationRecheck is Task 18's second deliverable: proving
// TryCypher's step-10 post-hydration snapshotStillCurrent recheck
// (engine.go's own pipeline doc) actually declines when a write lands in
// the narrow window between interpret.Execute completing and
// hydrateEdgePropsByID's own PostgreSQL round trip -- the same race
// servePathQuery's step 7 exists to catch on the shortest-path side, here
// exercised on the Cypher side instead.
//
// A single synchronous TryCypher call has no externally observable point
// between those two steps for a caller outside package engine to
// intervene at, short of a timing-dependent goroutine race against a real
// concurrent write -- so this test forces it deterministically instead, via
// a minimal seam this task adds to internal/engine/engine.go,
// SetCypherHydrationRaceHookForTest (see that method's own doc): installed
// just before the one call under test, it runs synchronously the moment
// TryCypher determines this query's result needs edge-property hydration,
// immediately before hydrateEdgePropsByID's own query -- calling
// engine.NoteWrite(nil) right there reproduces, deterministically, exactly
// what a genuinely concurrent write landing in that instant would do to
// the write-generation counter.
//
// The query itself (a one-edge shortestPath) is planned and executed
// entirely correctly against a still-fresh snapshot -- Plan, the translate
// gate, and Execute all run, and complete, before the hook ever fires --
// so the only thing under test is step 10's own recheck: TryCypher must
// decline (reasonStale) rather than hand back a result computed against a
// snapshot a write has since invalidated, and wrappedTransaction.Query must
// fall through to PostgreSQL, which -- since nothing about the database
// itself actually changed; NoteWrite(nil) only advances the engine's own
// in-memory counter -- must still find the exact same one path.
func TestCypherHydrationRecheck(t *testing.T) {
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

	var startID, endID graph.ID
	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		s, err := tx.CreateNode(graph.NewProperties(), cypherHydrationNodeKind)
		if err != nil {
			return err
		}
		e, err := tx.CreateNode(graph.NewProperties(), cypherHydrationNodeKind)
		if err != nil {
			return err
		}
		if _, err := tx.CreateRelationshipByIDs(s.ID, e.ID, cypherHydrationEdgeKind, graph.NewProperties()); err != nil {
			return err
		}
		startID, endID = s.ID, e.ID
		return nil
	}); err != nil {
		t.Fatalf("fixture setup WriteTransaction: %v", err)
	}

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("engine reports stale immediately after RebuildNow")
	}

	text := fmt.Sprintf(`MATCH p = shortestPath((s)-[:CypherHydrationEdge*1..]->(e)) WHERE id(s) = %d AND id(e) = %d RETURN p`, startID, endID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: shortestPath cypher query serves",
		func() int { return cypherPathCount(t, ctx, bt, text) }, 1)

	// Arm the race hook for exactly the one call below: the moment TryCypher
	// determines this query's result needs edge-property hydration, force a
	// write to land first, deterministically hitting step 10's recheck.
	d.engine.SetCypherHydrationRaceHookForTest(func() { d.engine.NoteWrite(nil) })
	t.Cleanup(func() { d.engine.SetCypherHydrationRaceHookForTest(nil) })

	requireDecline(t, buf, "stale", "hydration recheck: a write lands between Execute and hydration, so the query declines and falls back to PostgreSQL, still returning the correct path",
		func() int { return cypherPathCount(t, ctx, bt, text) }, 1)

	// Disarm before rebuilding: RebuildNow itself never reaches
	// TryCypher's hydration branch, but leaving the hook armed past its one
	// intended call would silently force every future hydrating query in
	// this test (there are none below) to decline too.
	d.engine.SetCypherHydrationRaceHookForTest(nil)

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (post-race): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("post-race: engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "post-rebuild: shortestPath cypher query serves again",
		func() int { return cypherPathCount(t, ctx, bt, text) }, 1)
}

// cypherStaleRecheckNodeKind is TestCypherStaleRecheckNoHydration's own
// fixture kind.
var cypherStaleRecheckNodeKind = graph.StringKind("CypherStaleRecheckNode")

// TestCypherStaleRecheckNoHydration is the final review's I1 regression:
// TryCypher's step-11 UNCONDITIONAL post-execution snapshotStillCurrent
// recheck (engine.go's own pipeline doc) must fire even for a
// pure-snapshot result -- one with no edge/path column at all, so
// collectEdgeIDs never finds anything to hydrate and step 10's own
// hydration-branch recheck never runs. Before this fix, TryCypher declined
// staleness only for the pre-execution Fresh() check (step 8) and the
// hydration-branch recheck (step 10) -- a query with neither shape could
// silently serve a result computed against a snapshot a write had already
// invalidated, if that write landed in the window between Execute
// completing and the result being handed back, however narrow that window
// is in practice.
//
// Mirrors TestCypherHydrationRecheck's own approach exactly, but for the
// no-hydration path: since a single synchronous TryCypher call has no
// externally observable point in that window for a test to intervene at
// short of a genuinely racing concurrent write, this reuses the identical
// cypherHydrationRaceHook seam, which -- per its own doc, extended by this
// same fix -- now also fires immediately before step 11's check on the
// no-hydration path specifically (there being no hydration branch to fire
// it from instead). The query itself (a plain property read, no
// edge/path/path column) is planned and executed entirely correctly
// against a still-fresh snapshot before the hook ever fires, so only step
// 11's own recheck is under test: TryCypher must decline (reasonStale)
// rather than hand back a result computed against a since-invalidated
// snapshot, and the fallback must still return the correct (post-write)
// value from PostgreSQL directly.
func TestCypherStaleRecheckNoHydration(t *testing.T) {
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
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("engine reports stale immediately after RebuildNow")
	}

	text := fmt.Sprintf(`MATCH (n:CypherStaleRecheckNode) WHERE id(n) = %d RETURN n.name`, nodeID)

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "baseline: plain property-read cypher query serves",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "before")

	// The write under test: lands (via the race hook) after Execute has
	// already run against a fresh snapshot, but before TryCypher hands the
	// result back -- exactly the window step 11 exists to catch, here
	// reached via the no-hydration branch since this query has no
	// edge/path column at all. The write ALSO changes name to "after", so
	// the fallback's answer is independently verifiable as PostgreSQL's own
	// live read (not some cached pre-write value).
	d.engine.SetCypherHydrationRaceHookForTest(func() {
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			return tx.UpdateNode(&graph.Node{ID: nodeID, Properties: graph.NewProperties().Set("name", "after")})
		}); err != nil {
			t.Fatalf("race-hook WriteTransaction: %v", err)
		}
	})
	t.Cleanup(func() { d.engine.SetCypherHydrationRaceHookForTest(nil) })

	requireDecline(t, buf, "stale", "no-hydration recheck: a write lands between Execute and the result being returned, so the query declines and falls back to PostgreSQL, returning the live (new) value",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "after")

	// Disarm before rebuilding: RebuildNow itself never reaches TryCypher's
	// serving pipeline at all, but leaving the hook armed past its one
	// intended call would silently force every future cypher query in this
	// test (there are none below) to decline too.
	d.engine.SetCypherHydrationRaceHookForTest(nil)

	if err := d.engine.RebuildNow(ctx, "manual_test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow (post-race): %v", err)
	}
	if _, fresh := d.engine.Fresh(); !fresh {
		t.Fatalf("post-race: engine reports stale immediately after RebuildNow")
	}

	requireMarkerDelta(t, buf, cypherServedMarker, 1, "post-rebuild: plain property-read cypher query serves again, reflecting the new value",
		func() string { return cypherStringValue(t, ctx, bt, text) }, "after")
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

// TestCypherMultiGraphGuard is Task 18's third deliverable: TryCypher must
// decline reasonMultiGraph the instant LoadSnapshot's probeMultiGraph
// (internal/engine/load.go) finds a second graph holding at least one node
// anywhere in the database -- regardless of whether that second graph has
// anything to do with the query being asked, since the interpreter has no
// notion of which graph a query is scoped to at all (engine.go's own doc
// for reasonMultiGraph). The kind-scoped builder-serving path carries no
// such guard (serve_builder.go never inspects Snapshot.MultiGraph), so a
// builder query on the very same default-graph fixture must keep serving,
// completely unaffected -- this milestone's now-familiar contrast between
// the two serving paths, drawn here along the MultiGraph axis instead of a
// freshness one.
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
