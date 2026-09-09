// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bloodtrail_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/graphcache"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/traversal"
	"github.com/specterops/dawgs/util/size"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
)

// servedMarker is the exact message engine.TryAllShortestPaths logs (at
// Info) whenever it serves a query from the in-memory snapshot --
// duplicated here (rather than exported from internal/engine) since only
// this test needs to recognize it in captured log output. engine.TryCypher
// no longer shares this marker: since being rewired to the general-purpose
// Cypher interpreter (internal/engine/interpret), it logs its own, Debug-
// level "bloodtrail: cypher engine served" instead (engine.go's
// cypherServedLogMessage) -- this test never issues a raw Cypher-text
// query through the wrapped driver, so it has no reason to watch for that
// marker.
const servedMarker = "bloodtrail: path engine served"

// builderServedMarker is the exact message engine.TryNodeCount/
// TryNodeFetchIDs/TryNodeFetchKinds (internal/engine/serve_builder.go's
// servedOp) log at Debug whenever they serve a structural node query from
// the in-memory snapshot -- duplicated here for the same reason servedMarker
// is (only this test needs to recognize it in captured log output). It is
// deliberately Debug, not Info, unlike servedMarker (servedOp's own doc), so
// installLogCapture must enable Debug-level capture for this marker to ever
// appear in buf.
const builderServedMarker = "bloodtrail: builder engine served"

// lockedBuffer is a bytes.Buffer safe for concurrent writes from the
// engine's background/serving goroutines and reads from the test goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
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

// installLogCapture installs a slog default logger that writes into a
// returned buffer, restoring the previous default via t.Cleanup. It must be
// called before dawgs.Open constructs the bloodtrail driver: driver.go's
// Open reads slog.Default() exactly once, at construction time, to build
// the engine's own Config.Log, so installing the capture afterward would
// miss every line the engine itself logs.
//
// The handler is configured at Debug level (rather than the text handler's
// own Info default) so builderServedMarker -- logged via DebugContext by the
// structural node/relationship query path (servedOp's doc) -- is captured
// alongside servedMarker's Info-level line; every existing caller that only
// ever greps for servedMarker is unaffected by the extra Debug output.
func installLogCapture(t *testing.T) *lockedBuffer {
	t.Helper()

	buf := &lockedBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return buf
}

// shortestPathsViaCriteria runs the same graph.Criteria shape BloodHound's
// API builds for FetchAllShortestPaths -- query.And(query.Equals(StartID),
// query.Equals(EndID)) -- directly through db's RelationshipQuery (the path
// recordingRelationshipQuery.FetchAllShortestPaths, not tx.Query, serves),
// and renders the result the same way driver_integration_test.go's
// pathSignatures does for the cypher-text path, so the two are directly
// comparable.
func shortestPathsViaCriteria(t *testing.T, ctx context.Context, db graph.Database, startID, endID graph.ID) []string {
	t.Helper()

	var sigs []string
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		criteria := query.And(
			query.Equals(query.StartID(), startID),
			query.Equals(query.EndID(), endID),
		)
		return tx.Relationships().Filter(criteria).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for p := range cursor.Chan() {
				parts := make([]string, 0, len(p.Nodes))
				for _, n := range p.Nodes {
					parts = append(parts, n.ID.String())
				}
				sigs = append(sigs, strings.Join(parts, ">"))
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("shortestPathsViaCriteria(%d, %d): %v", startID, endID, err)
	}
	sort.Strings(sigs)
	return sigs
}

// waitForEngineServe repeatedly calls query -- one FetchAllShortestPaths
// lookup through the wrapped driver, rendered the same way
// shortestPathsViaCriteria does -- until buf's captured log shows more
// servedMarker lines than baseline, up to deadline. Each call actively
// drives a query through the driver: the served marker is logged only from
// inside TryAllShortestPaths/TryCypher when a caller's query actually
// reaches them, never by a background rebuild alone (which logs its own,
// differently worded "snapshot rebuilt" line), so a caller that only
// waited passively without issuing calls here would wait forever -- this
// is also what makes the wait correct with no dependency on Start's
// boot-load goroutine having already adopted a snapshot by the time this
// is first called: the very first call may simply decline (no snapshot
// yet) or serve the pre-existing one, and this loop keeps retrying either
// way until a NEW serve is observed. It returns the result from the exact
// call that observed a new marker, so a caller gets both "the engine
// built/rebuilt" and a live result in one step, with no separate race
// between the two.
func waitForEngineServe(t *testing.T, buf *lockedBuffer, baseline int, deadline time.Duration, query func() []string) []string {
	t.Helper()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		got := query()
		if strings.Count(buf.String(), servedMarker) > baseline {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for a new %q log line (count stayed at or below %d)\nlog:\n%s", deadline, servedMarker, baseline, buf.String())
	return nil
}

// waitForBuilderServe is waitForEngineServe's counterpart for the
// builder-serving path (Task 8's Nodes() wiring): it repeatedly calls query
// until buf's captured log shows more builderServedMarker lines than
// baseline, up to deadline, returning the result from the exact call that
// observed a new marker. Generic over query's return type so it serves both
// nodeCountByKind (int64) and nodeIDsByKind ([]graph.ID) callers without a
// separate helper for each.
func waitForBuilderServe[T any](t *testing.T, buf *lockedBuffer, baseline int, deadline time.Duration, query func() T) T {
	t.Helper()

	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		got := query()
		if strings.Count(buf.String(), builderServedMarker) > baseline {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	var zero T
	t.Fatalf("timed out after %s waiting for a new %q log line (count stayed at or below %d)\nlog:\n%s", deadline, builderServedMarker, baseline, buf.String())
	return zero
}

// nodeCountByKind runs tx.Nodes().Filter(query.Kind(query.Node(), kind)).
// Count() -- the exact shape recordingNodeQuery.Count (node_query.go)
// recognizes and may serve from the engine -- through db, returning the
// result.
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

// nodeIDsByKind runs tx.Nodes().Filter(query.Kind(query.Node(), kind)).
// FetchIDs() through db, returning the matching ids sorted ascending so two
// calls' results (e.g. bt vs. the pg oracle) are directly comparable as
// sets, matching TryNodeFetchIDs' own documented "compare as sets, not
// sequences" contract (internal/engine/serve_builder.go).
func nodeIDsByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) []graph.ID {
	t.Helper()

	var ids []graph.ID
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Filter(query.Kind(query.Node(), kind)).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				ids = append(ids, id)
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("Nodes().Filter(Kind(%s)).FetchIDs(): %v", kind, err)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// relCountByKind runs tx.Relationships().Filter(query.Kind(query.
// Relationship(), kind)).Count() -- the exact structural shape
// recordingRelationshipQuery.Count (relationship_query.go) recognizes and may
// serve from the engine -- through db, returning the result.
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

// relCountByKindOrdered is relCountByKind with an added OrderBy that
// recognize.OrderIsEdgeIDAscending accepts -- exactly the paging order
// dawgs' own traversal.LightweightDriver issues before every Query call (see
// bfsCollectNodeIDs' doc) -- proving Count's own !orderByEdgeID guard
// component: this must fall through to PostgreSQL rather than serve from the
// engine, since an order has no meaning for a bare count
// (relationship_query.go's Count doc).
//
// Unlike relCountByKind, this returns the error rather than failing the test
// on one: ordering an aggregated Count() query this way is rejected by
// PostgreSQL itself (a plain "ORDER BY column must appear in an aggregate"
// error, reproducible identically against the pg driver alone, independent
// of bloodtrail entirely) -- so "falls through to PostgreSQL" here means
// "fails exactly the way calling this against the raw pg driver already
// does," not "succeeds with a count." The caller compares bt's and oracle's
// results (including whether each errored) rather than assuming success.
func relCountByKindOrdered(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) (int64, error) {
	t.Helper()

	var count int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Relationships().
			Filter(query.Kind(query.Relationship(), kind)).
			OrderBy(query.Order(query.Identity(query.Relationship()), query.Ascending())).
			Count()
		count = n
		return err
	})
	return count, err
}

// relCountByKindLimited is relCountByKind with an added Limit -- one of the
// calls that taints a recordingRelationshipQuery (relationship_query.go's
// Limit override) -- proving Count's own !tainted guard component: this must
// fall through to PostgreSQL rather than serve from the engine.
func relCountByKindLimited(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind, limit int) int64 {
	t.Helper()

	var count int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Relationships().
			Filter(query.Kind(query.Relationship(), kind)).
			Limit(limit).
			Count()
		count = n
		return err
	})
	if err != nil {
		t.Fatalf("Relationships().Filter(Kind(%s)).Limit(%d).Count(): %v", kind, limit, err)
	}
	return count
}

// relIDsByKind runs tx.Relationships().Filter(query.Kind(query.
// Relationship(), kind)).FetchIDs() through db, returning the matching
// relationship ids sorted ascending so two calls' results (e.g. bt vs. the pg
// oracle) are directly comparable as sequences -- scan order itself is
// unspecified (TryRelFetchIDs' doc), but each edge id is unique, so sorting
// both sides the same way canonicalizes any two equal sets to the same
// sequence.
func relIDsByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) []graph.ID {
	t.Helper()

	var ids []graph.ID
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(query.Kind(query.Relationship(), kind)).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				ids = append(ids, id)
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("Relationships().Filter(Kind(%s)).FetchIDs(): %v", kind, err)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// relTriplesByKind is relIDsByKind's FetchTriples counterpart, sorted
// ascending by each triple's own relationship id for the same reason.
func relTriplesByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) []graph.RelationshipTripleResult {
	t.Helper()

	var triples []graph.RelationshipTripleResult
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(query.Kind(query.Relationship(), kind)).FetchTriples(func(cursor graph.Cursor[graph.RelationshipTripleResult]) error {
			for triple := range cursor.Chan() {
				triples = append(triples, triple)
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("Relationships().Filter(Kind(%s)).FetchTriples(): %v", kind, err)
	}
	sort.Slice(triples, func(i, j int) bool { return triples[i].ID < triples[j].ID })
	return triples
}

// relKindsByKind is relIDsByKind's FetchKinds counterpart, sorted ascending
// by each row's own relationship id for the same reason.
func relKindsByKind(t *testing.T, ctx context.Context, db graph.Database, kind graph.Kind) []graph.RelationshipKindsResult {
	t.Helper()

	var rows []graph.RelationshipKindsResult
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(query.Kind(query.Relationship(), kind)).FetchKinds(func(cursor graph.Cursor[graph.RelationshipKindsResult]) error {
			for row := range cursor.Chan() {
				rows = append(rows, row)
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("Relationships().Filter(Kind(%s)).FetchKinds(): %v", kind, err)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// builderServedCount returns the number of builderServedMarker lines
// currently captured in buf -- a plain wrapper around the
// strings.Count(buf.String(), builderServedMarker) expression repeated around
// every call in TestRelationshipStructuralFetchesServeFromLiveDriver, so a
// before/after pair around one call reads as a single marker-delta check.
func builderServedCount(buf *lockedBuffer) int {
	return strings.Count(buf.String(), builderServedMarker)
}

// digraphEdgeSet collects every (start, end) database-id edge pair reachable
// via digraph's own EachNode/EachAdjacentNode(..., graph.DirectionOutbound)
// walk into a plain set, so two independently built container.DirectedGraph
// values (e.g. bt vs. the pg oracle, run over the same underlying
// PostgreSQL data and therefore the same database ids) can be compared for
// exact edge-set equality via edgeSetsEqual, ignoring container's own
// internal dense-index bookkeeping entirely (csr.go's EachNode/
// EachAdjacentNode both yield external database ids, never the dense CSR
// indices AddEdge assigns internally).
func digraphEdgeSet(digraph container.DirectedGraph) map[[2]uint64]struct{} {
	edges := make(map[[2]uint64]struct{})
	digraph.EachNode(func(node uint64) bool {
		digraph.EachAdjacentNode(node, graph.DirectionOutbound, func(adjacent uint64) bool {
			edges[[2]uint64{node, adjacent}] = struct{}{}
			return true
		})
		return true
	})
	return edges
}

// edgeSetsEqual reports whether a and b contain exactly the same edges.
func edgeSetsEqual(a, b map[[2]uint64]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for edge := range a {
		if _, ok := b[edge]; !ok {
			return false
		}
	}
	return true
}

// fetchNodeByID fetches the single *graph.Node carrying id through db --
// used to build the *graph.Node traversal.Plan.Root wants, which
// loadDatasets/opengraph.IDMap only ever hands back as a bare graph.ID.
// tx.Nodes().Filter(...).First() is not one of recordingNodeQuery's own
// overrides (node_query.go's doc: First is promoted straight through
// unchanged), so this always reaches PostgreSQL directly regardless of
// whether the engine has a snapshot -- exactly as a real caller resolving a
// traversal root would.
func fetchNodeByID(t *testing.T, ctx context.Context, db graph.Database, id graph.ID) *graph.Node {
	t.Helper()

	var node *graph.Node
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Nodes().Filter(query.Equals(query.NodeID(), id)).First()
		node = n
		return err
	})
	if err != nil {
		t.Fatalf("fetch node %d: %v", id, err)
	}
	return node
}

// bfsCollectNodeIDs drives traversal.New(db, 1).BreadthFirst from root using
// traversal.LightweightDriver(direction, graphcache.New(), query.Kind(query.
// Relationship(), kind), filter) -- the exact upstream pattern
// recordingRelationshipQuery.Query (relationship_query.go) and its
// orderByEdgeID plumbing exist to serve: LightweightDriver's own
// shallowFetchRelationships (traversal/traversal.go) issues, for every
// segment it descends into, a fresh tx.Relationships().Filter(...).
// OrderBy(query.Order(query.Identity(query.Relationship()),
// query.Ascending())).Query(...) call -- OrderBy with exactly the first of
// recognize.OrderIsEdgeIDAscending's two accepted spellings, then Query with
// one of the two step RowProjection shapes (recognize.
// ProjectionStepOutbound for graph.DirectionOutbound, ProjectionStepInbound
// for graph.DirectionInbound).
//
// numParallelWorkers is deliberately 1, not dawgs' own multi-worker-capable
// default: graph.PathSegment.Descend (graph/path.go) accumulates a path
// tree's size by walking Trunk pointers up to the root and mutating each
// ancestor's unexported size field with a plain, unsynchronized += -- with
// more than one worker, two goroutines can both be descending from segments
// that share an ancestor (any two children of the same node, which this
// fixture's very first BFS level already produces) and race on that
// ancestor's size, a data race in dawgs itself with nothing to do with this
// package's own driver wrapping. -race would (correctly) flag that race, so
// this helper avoids it by never running more than one traversal worker; a
// single worker still drives every one of shallowFetchRelationships'
// Filter/OrderBy/Query calls through the wrapped driver exactly as multiple
// workers would, just serially, which is all this needs to exercise.
//
// The traversal.UniquePathSegmentFilter wrapper collects every segment's
// node into a traversal.NodeCollector and unconditionally allows further
// descent (bounded, for a real graph, by that same filter's own dedup-by-
// edge-id cycle guard), so the returned ids are every node reachable from
// root via a kind-filtered walk in direction, sorted ascending for direct
// set comparison against a second call (e.g. bt vs. the pg oracle) the same
// way nodeIDsByKind's callers already compare FetchIDs() output.
func bfsCollectNodeIDs(t *testing.T, ctx context.Context, db graph.Database, root *graph.Node, direction graph.Direction, kind graph.Kind) []graph.ID {
	t.Helper()

	collector := traversal.NewNodeCollector()
	filter := traversal.UniquePathSegmentFilter(func(next *graph.PathSegment) bool {
		collector.Collect(next)
		return true
	})
	driver := traversal.LightweightDriver(direction, graphcache.New(), query.Kind(query.Relationship(), kind), filter)

	if err := (traversal.New(db, 1)).BreadthFirst(ctx, traversal.Plan{Root: root, Driver: driver}); err != nil {
		t.Fatalf("BreadthFirst: %v", err)
	}

	ids := make([]graph.ID, 0, len(collector.Nodes))
	for id := range collector.Nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// TestEngineServesFromLiveDriver is the driver's core serving evidence:
// opened through dawgs.Open(ctx, bloodtrail.DriverName, cfg) exactly as
// BloodHound would, the driver must serve both the Criteria/
// FetchAllShortestPaths API and
// cypher-text queries from the in-memory engine once it has built a
// snapshot, and keep serving them correctly straight through a write (which
// write-through publishes into the replica rather than invalidating it).
func TestEngineServesFromLiveDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	// A plain pg driver on the same pool and data is the oracle.
	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	// --- Phase 1: FetchAllShortestPaths via the Criteria API, waiting for
	// the engine's first build.
	got := waitForEngineServe(t, buf, 0, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})
	want := shortestPathsViaCriteria(t, ctx, oracle, ids["c0"], ids["c10"])
	if len(want) == 0 {
		t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("FetchAllShortestPaths paths differ\n got: %v\nwant: %v", got, want)
	}

	// --- Phase 2: the three cypher equivalence texts through tx.Query.
	for _, eq := range equivalenceQueries {
		cypher := eq.cypher(ids)
		t.Run(eq.name, func(t *testing.T) {
			got := pathSignatures(t, ctx, bt, cypher)
			want := pathSignatures(t, ctx, oracle, cypher)
			if len(want) == 0 {
				t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("paths differ\n got: %v\nwant: %v", got, want)
			}
		})
	}

	// --- Phase 3: a write through the driver is replayed into the replica
	// rather than invalidating it, so the same query keeps being SERVED
	// (not delegated) and keeps answering correctly -- and the write still
	// must not itself trigger a rebuild: write-through replays it directly,
	// so there is nothing left for a rebuild to fix.
	servedBefore := strings.Count(buf.String(), servedMarker)
	rebuiltBefore := strings.Count(buf.String(), rebuiltMarker)

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("ExtraNode"))
		return err
	}); err != nil {
		t.Fatalf("WriteTransaction (CreateNode): %v", err)
	}

	if got := shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"]); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("post-write paths differ\n got: %v\nwant: %v", got, want)
	}
	if servedAfter := strings.Count(buf.String(), servedMarker); servedAfter <= servedBefore {
		t.Fatalf("served line count stayed at %d after a write; the write-through replica must keep serving", servedBefore)
	}
	if rebuiltAfter := strings.Count(buf.String(), rebuiltMarker); rebuiltAfter != rebuiltBefore {
		t.Fatalf("%q log count changed from %d to %d after a plain write; a replayable write must not cost a rebuild", rebuiltMarker, rebuiltBefore, rebuiltAfter)
	}
}

// rebuiltMarker is the exact message the engine logs (at Info) whenever it
// actually adopts a freshly loaded snapshot -- duplicated here for the same
// reason servedMarker is, so a test can assert that a write was served
// WITHOUT one, which is the whole claim write-through makes.
const rebuiltMarker = "bloodtrail: snapshot rebuilt"

// TestEngineOffDelegatesEverythingAndNeverServes covers BLOODTRAIL_ENGINE=off:
// the driver must answer every query correctly by delegating straight to
// PostgreSQL, and the engine must never log a served line -- Start is a
// no-op when disabled (internal/engine/boot.go), so no boot-load goroutine
// even runs, and every TryAllShortestPaths/TryCypher call declines
// immediately (reason "disabled") before ever touching a snapshot.
func TestEngineOffDelegatesEverythingAndNeverServes(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	t.Setenv(bloodtrail.EnvEngine, "off")
	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	got := shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	want := shortestPathsViaCriteria(t, ctx, oracle, ids["c0"], ids["c10"])
	if len(want) == 0 {
		t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("FetchAllShortestPaths paths differ\n got: %v\nwant: %v", got, want)
	}

	for _, eq := range equivalenceQueries {
		cypher := eq.cypher(ids)
		t.Run(eq.name, func(t *testing.T) {
			got := pathSignatures(t, ctx, bt, cypher)
			want := pathSignatures(t, ctx, oracle, cypher)
			if len(want) == 0 {
				t.Fatalf("oracle returned no paths; the query or dataset names are wrong")
			}
			if strings.Join(got, "|") != strings.Join(want, "|") {
				t.Fatalf("paths differ\n got: %v\nwant: %v", got, want)
			}
		})
	}

	if n := strings.Count(buf.String(), servedMarker); n != 0 {
		t.Fatalf("engine served %d time(s) with %s=off, want 0\nlog:\n%s", n, bloodtrail.EnvEngine, buf.String())
	}
}

// TestWipeGraphInvalidatesEngineSnapshot is the integration evidence for
// Driver's WipeGraph override: WipeGraph -- BloodHound's "clear database"
// action -- reaches PostgreSQL through the embedded *pg.Driver's own
// internal WriteTransaction call, never through Driver's own
// WriteTransaction override (embedding has no virtual dispatch), so without
// that override (driver.go) the engine would keep serving the pre-wipe
// snapshot indefinitely.
//
// A truncation is not expressible as a delta -- nothing enumerates what it
// removed -- so the override records a ChangeSet fallback, which is the one
// path that still invalidates the whole replica: the engine enters fallback
// (declining every query, so results come from PostgreSQL and are correct),
// rebuilds once in the background against the now-empty graph, and resumes
// serving. What must never happen, at any point in that sequence, is a query
// answered from the pre-wipe snapshot.
func TestWipeGraphInvalidatesEngineSnapshot(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	driver, ok := bt.(*bloodtrail.Driver)
	if !ok {
		t.Fatalf("expected *bloodtrail.Driver, got %T", bt)
	}

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	// Build the initial snapshot and confirm it actually serves before the
	// wipe -- otherwise this test would prove nothing about invalidation.
	waitForEngineServe(t, buf, 0, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})

	servedBefore := strings.Count(buf.String(), servedMarker)

	if err := driver.WipeGraph(ctx, nil); err != nil {
		t.Fatalf("WipeGraph: %v", err)
	}

	// Whatever the engine's recovery has or has not finished by now, a query
	// must never report the pre-wipe graph's paths: while it is in fallback
	// the query is delegated to PostgreSQL (correctly empty), and once the
	// background rebuild lands it is served from the empty replica (also
	// empty).
	if got := shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"]); len(got) != 0 {
		t.Fatalf("post-wipe query returned paths from a wiped graph: %v", got)
	}

	// The recovery rebuild restores serving on its own.
	got := waitForEngineServe(t, buf, servedBefore, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})
	if len(got) != 0 {
		t.Fatalf("post-recovery query on a wiped graph returned paths: %v", got)
	}
}

// TestFetchAllShortestPathsClosesCursorWhenDelegateReturnsEarly is Finding
// 2's regression test: recordingRelationshipQuery.FetchAllShortestPaths
// (relationship_query.go) must close the graph.Cursor[graph.Path] it hands
// to delegate itself -- provider-closes, the same convention the pg
// driver's own FetchAllShortestPaths follows (drivers/pg/relationship.go:
// `cursor := ...; defer cursor.Close(); return delegate(cursor)`) -- rather
// than leaving the cursor for delegate to close. Before the fix, a delegate
// that returned without draining Chan() at all left the cursor's feeder
// goroutine (internal/engine/result.go's pathCursor.feed) blocked forever on
// an unbuffered channel send: only Close() cancels the context that
// unblocks it.
//
// This needs a live, actually-serving engine (not a mock): the served
// branch inside FetchAllShortestPaths -- the only branch that constructs an
// engine.NewPathCursor at all -- is only reached once
// engine.TryAllShortestPaths itself succeeds, which requires a real
// snapshot built from PostgreSQL. A query that instead fell through to the
// pg driver's own FetchAllShortestPaths would exercise that driver's
// already-correct cursor and prove nothing about this fix.
func TestFetchAllShortestPathsClosesCursorWhenDelegateReturnsEarly(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	// Warm the engine: this both builds the initial snapshot and confirms
	// the query is actually served (not delegated) before moving on.
	waitForEngineServe(t, buf, 0, 5*time.Second, func() []string {
		return shortestPathsViaCriteria(t, ctx, bt, ids["c0"], ids["c10"])
	})

	runtime.GC()
	before := runtime.NumGoroutine()

	// Repeat the served call several times, each with a delegate that
	// returns immediately without draining Chan() at all -- the exact shape
	// that leaked one feeder goroutine per call before the fix. Repeating
	// amplifies a real leak into an unmistakable signal well above any
	// incidental goroutine-count jitter (e.g. pool housekeeping).
	const attempts = 20
	for i := 0; i < attempts; i++ {
		err := bt.ReadTransaction(ctx, func(tx graph.Transaction) error {
			criteria := query.And(
				query.Equals(query.StartID(), ids["c0"]),
				query.Equals(query.EndID(), ids["c10"]),
			)
			return tx.Relationships().Filter(criteria).FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error {
				return nil
			})
		})
		if err != nil {
			t.Fatalf("FetchAllShortestPaths (attempt %d): %v", i, err)
		}
	}

	if !strings.Contains(buf.String(), servedMarker) {
		t.Fatalf("expected at least one of these queries to have been served by the engine\nlog:\n%s", buf.String())
	}

	// The feeder goroutines should unblock and exit promptly once Close()
	// cancels their context; poll with a generous deadline rather than
	// asserting immediately; allow a small amount of slack over the
	// baseline for incidental, unrelated goroutine churn.
	const slack = 3
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		after := runtime.NumGoroutine()
		if after <= before+slack {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("feeder goroutines leaked: goroutines before=%d after=%d (delta %d over %d attempts)", before, after, after-before, attempts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestNodeQueryServesFromLiveDriver is Task 8's core evidence: opened
// through dawgs.Open exactly like TestEngineServesFromLiveDriver, a
// structural Nodes() query -- the shape BloodHound's builder queries use for
// e.g. FetchNodeIDsByKind, Filter(query.Kind(query.Node(), kind)) -- must be
// served from the in-memory engine (recordingNodeQuery, node_query.go) once
// a snapshot exists, agree with the pg-driver oracle on both Count() and the
// FetchIDs() set, and a query recognize.FromNodeCriteria rejects outright
// (a property lookup, never a shape FromNodeCriteria models -- see its own
// doc) must still delegate transparently to PostgreSQL and answer correctly.
func TestNodeQueryServesFromLiveDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	loadDatasets(t, ctx, bt)

	// A plain pg driver on the same pool and data is the oracle.
	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	// TraversalNode is carried by every one of traversal_shapes.json's 45
	// nodes (testdata/dawgs/traversal_shapes.json), so a bare kind filter
	// against it gives an unambiguous, easy-to-verify answer.
	kind := graph.StringKind("TraversalNode")

	// --- Phase 1: Count(), waiting for the engine's first build.
	gotCount := waitForBuilderServe(t, buf, 0, 5*time.Second, func() int64 {
		return nodeCountByKind(t, ctx, bt, kind)
	})
	wantCount := nodeCountByKind(t, ctx, oracle, kind)
	if wantCount == 0 {
		t.Fatalf("oracle returned zero nodes for kind %s; the fixture or kind name is wrong", kind)
	}
	if gotCount != wantCount {
		t.Fatalf("Count() = %d, want %d (oracle)", gotCount, wantCount)
	}

	// --- Phase 2: FetchIDs() set-equality (order is not a shared contract;
	// see TryNodeFetchIDs' doc).
	gotIDs := nodeIDsByKind(t, ctx, bt, kind)
	wantIDs := nodeIDsByKind(t, ctx, oracle, kind)
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("FetchIDs() returned %d ids, want %d\n got: %v\nwant: %v", len(gotIDs), len(wantIDs), gotIDs, wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("FetchIDs() sets differ\n got: %v\nwant: %v", gotIDs, wantIDs)
		}
	}

	// --- Phase 3: a property-filtered query -- recognize.FromNodeCriteria
	// always rejects a PropertyLookup conjunct (its doc) -- must still
	// delegate transparently through recordingNodeQuery.Count to PostgreSQL
	// and answer correctly, proving Filter's recording never interferes with
	// an unrecognized shape. adcs_fanout.json's Group node "n" is the only
	// node in either fixture carrying an "objectid" property.
	const wantObjectID = "S-1-5-21-2643190041-1319121918-239771340-513"
	var gotObjectIDCount int64
	if err := bt.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Nodes().Filter(query.Equals(query.NodeProperty("objectid"), wantObjectID)).Count()
		gotObjectIDCount = n
		return err
	}); err != nil {
		t.Fatalf("property-filtered Count(): %v", err)
	}
	if gotObjectIDCount != 1 {
		t.Fatalf("property-filtered Count() = %d, want 1", gotObjectIDCount)
	}
}

// TestContainerFetchDirectedGraphServesFromLiveDriver is Task 9's evidence
// for the Count/FetchIDs/FetchTriples/FetchKinds/Query interceptions added
// to recordingRelationshipQuery (relationship_query.go): dawgs' own
// container.FetchDirectedGraph -- a real upstream consumer, imported here
// rather than reimplemented, per this task's own point (see relationship_
// query.go's Query doc) -- issues exactly
// tx.Relationships().Filter(criteria).Query(delegate,
// query.Returning(query.StartID(), query.EndID())), the single-criteria,
// single-finalCriteria, ProjectionStartEnd shape Query recognizes with no
// direction requirement at all (projectionDirectionConsistent's doc).
// Opened through dawgs.Open exactly as TestNodeQueryServesFromLiveDriver is,
// the resulting container.DirectedGraph's edge set must agree exactly with
// the same call against the pg driver oracle, and the builder-serving path
// must actually have been used (builderServedMarker).
func TestContainerFetchDirectedGraphServesFromLiveDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	loadDatasets(t, ctx, bt)

	// A plain pg driver on the same pool and data is the oracle.
	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	// traversal_shapes.json's ChainEdge kind: a straight 10-hop chain
	// c0->c1->...->c10, giving an unambiguous 10-edge answer.
	kind := graph.StringKind("ChainEdge")
	criteria := query.KindIn(query.Relationship(), kind)

	gotEdges := waitForBuilderServe(t, buf, 0, 5*time.Second, func() map[[2]uint64]struct{} {
		digraph, err := container.FetchDirectedGraph(ctx, bt, criteria)
		if err != nil {
			t.Fatalf("FetchDirectedGraph (bt): %v", err)
		}
		return digraphEdgeSet(digraph)
	})

	wantDigraph, err := container.FetchDirectedGraph(ctx, oracle, criteria)
	if err != nil {
		t.Fatalf("FetchDirectedGraph (oracle): %v", err)
	}
	wantEdges := digraphEdgeSet(wantDigraph)
	if len(wantEdges) == 0 {
		t.Fatalf("oracle returned no edges; the query or dataset names are wrong")
	}
	if !edgeSetsEqual(gotEdges, wantEdges) {
		t.Fatalf("FetchDirectedGraph edge sets differ\n got: %v\nwant: %v", gotEdges, wantEdges)
	}
}

// TestTraversalLightweightDriverBreadthFirstServesFromLiveDriver is Task 9's
// evidence for orderByEdgeID: dawgs' own
// traversal.New(db, 1).BreadthFirst(ctx, traversal.Plan{Root: ..., Driver:
// traversal.LightweightDriver(...)}) (see bfsCollectNodeIDs' doc for why the
// worker count is 1, not dawgs' own traversal.New(db, 2) as sketched in the
// task brief) -- again a real upstream consumer, not reimplemented --
// drives shallowFetchRelationships (traversal/traversal.go), which OrderBys
// every relationship query by ascending relationship id before calling
// Query with a step RowProjection. This is
// the one upstream shape that sets recordingRelationshipQuery.orderByEdgeID
// and requires Query (not Count/FetchIDs/FetchTriples/FetchKinds, which all
// decline once it's set) to pass it through to engine.TryRelQueryRows.
//
// Opened through dawgs.Open exactly like the container test above, the set
// of nodes traversal.NodeCollector reaches from traversal_shapes.json's
// FanoutEdge tree root (f0) must agree exactly with the same walk against
// the pg driver oracle, and the builder-serving path must actually have been
// used (builderServedMarker).
func TestTraversalLightweightDriverBreadthFirstServesFromLiveDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	ids := loadDatasets(t, ctx, bt)

	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	// traversal_shapes.json's FanoutEdge tree: f0 fans out through f1..f3 and
	// f1a..f3b to a third level (f1a1..f3b1) -- 15 reachable descendants,
	// unambiguous and acyclic.
	kind := graph.StringKind("FanoutEdge")
	btRoot := fetchNodeByID(t, ctx, bt, ids["f0"])

	gotIDs := waitForBuilderServe(t, buf, 0, 5*time.Second, func() []graph.ID {
		return bfsCollectNodeIDs(t, ctx, bt, btRoot, graph.DirectionOutbound, kind)
	})

	oracleRoot := fetchNodeByID(t, ctx, oracle, ids["f0"])
	wantIDs := bfsCollectNodeIDs(t, ctx, oracle, oracleRoot, graph.DirectionOutbound, kind)
	if len(wantIDs) == 0 {
		t.Fatalf("oracle traversal reached no nodes; the query or dataset names are wrong")
	}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("BreadthFirst reached %d nodes, want %d\n got: %v\nwant: %v", len(gotIDs), len(wantIDs), gotIDs, wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("BreadthFirst node sets differ\n got: %v\nwant: %v", gotIDs, wantIDs)
		}
	}
}

// TestRelationshipStructuralFetchesServeFromLiveDriver is this task's
// evidence for the !orderByEdgeID and !tainted guard components shared by
// recordingRelationshipQuery's Count/FetchIDs/FetchTriples/FetchKinds
// (relationship_query.go): unlike TestContainerFetchDirectedGraphServesFrom
// LiveDriver and TestTraversalLightweightDriverBreadthFirstServesFromLive
// Driver above -- which only ever exercise Query, through real upstream
// consumers that never trip either guard clause -- this test calls the four
// structural fetch methods directly, both on their own (proving each is
// actually served, not merely recognized) and combined with an OrderBy or
// Limit that must disqualify them from being served at all.
//
// Opened through dawgs.Open exactly like TestNodeQueryServesFromLiveDriver:
//
//   - Phase 1 proves the served path: a kind-filtered Count/FetchIDs/
//     FetchTriples/FetchKinds call must each agree with the pg-driver oracle
//     AND increment builderServedMarker's count by at least one on that
//     exact call. Checking each call's own delta (not just that the marker
//     appears somewhere in the whole test's log) is what makes phase 2 below
//     meaningful: without this, an always-true guard would look identical to
//     a working one.
//   - Phase 2 proves the two guard components themselves: the identical
//     filter, with an OrderBy that recognize.OrderIsEdgeIDAscending accepts
//     (2a) or a Limit (2b) added before Count(), must still agree with the
//     oracle (falling through to PostgreSQL and answering correctly) but
//     must add NO new builderServedMarker line. Stripping either guard's
//     clause (!orderByEdgeID or !tainted) from Count's condition would
//     instead serve these two calls from the engine, appearing here as an
//     unwanted marker increment -- exactly the failure this phase exists to
//     catch.
//
// traversal_shapes.json's ChainEdge kind (a straight 10-hop chain
// c0->c1->...->c10, the same kind TestContainerFetchDirectedGraphServesFrom
// LiveDriver uses) gives an unambiguous, easy-to-verify answer.
func TestRelationshipStructuralFetchesServeFromLiveDriver(t *testing.T) {
	dsn := os.Getenv(testPGEnv)
	if dsn == "" {
		t.Skipf("%s not set", testPGEnv)
	}

	buf := installLogCapture(t)

	ctx := context.Background()
	pool := openPool(t, ctx, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = bt.Close(ctx) }()

	schema := schemaFromDatasets(t)
	if err := bt.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema: %v", err)
	}
	loadDatasets(t, ctx, bt)

	// A plain pg driver on the same pool and data is the oracle.
	oracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg: %v", err)
	}
	defer func() { _ = oracle.Close(ctx) }()
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("assert schema on pg: %v", err)
	}

	kind := graph.StringKind("ChainEdge")

	// --- Phase 1a: Count(), waiting for the engine's first build.
	gotCount := waitForBuilderServe(t, buf, 0, 5*time.Second, func() int64 {
		return relCountByKind(t, ctx, bt, kind)
	})
	wantCount := relCountByKind(t, ctx, oracle, kind)
	if wantCount == 0 {
		t.Fatalf("oracle returned zero relationships for kind %s; the fixture or kind name is wrong", kind)
	}
	if gotCount != wantCount {
		t.Fatalf("Count() = %d, want %d (oracle)", gotCount, wantCount)
	}

	// The snapshot is now warm and clean, with no write landing between here
	// and the end of the test. Every call below is checked against its own
	// before/after marker delta, so each of the four methods must reach the
	// served branch on its own merits rather than merely piggyback on the
	// build the call above already triggered.

	// --- Phase 1b: Count() again, this time asserting its own marker delta.
	before := builderServedCount(buf)
	gotCount = relCountByKind(t, ctx, bt, kind)
	if delta := builderServedCount(buf) - before; delta < 1 {
		t.Fatalf("Count(): builder-served marker delta = %d, want >= 1", delta)
	}
	if gotCount != wantCount {
		t.Fatalf("Count() = %d, want %d (oracle)", gotCount, wantCount)
	}

	// --- Phase 1c: FetchIDs().
	before = builderServedCount(buf)
	gotIDs := relIDsByKind(t, ctx, bt, kind)
	if delta := builderServedCount(buf) - before; delta < 1 {
		t.Fatalf("FetchIDs(): builder-served marker delta = %d, want >= 1", delta)
	}
	wantIDs := relIDsByKind(t, ctx, oracle, kind)
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("FetchIDs() returned %d ids, want %d\n got: %v\nwant: %v", len(gotIDs), len(wantIDs), gotIDs, wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("FetchIDs() sets differ\n got: %v\nwant: %v", gotIDs, wantIDs)
		}
	}

	// --- Phase 1d: FetchTriples().
	before = builderServedCount(buf)
	gotTriples := relTriplesByKind(t, ctx, bt, kind)
	if delta := builderServedCount(buf) - before; delta < 1 {
		t.Fatalf("FetchTriples(): builder-served marker delta = %d, want >= 1", delta)
	}
	wantTriples := relTriplesByKind(t, ctx, oracle, kind)
	if len(gotTriples) != len(wantTriples) {
		t.Fatalf("FetchTriples() returned %d triples, want %d\n got: %v\nwant: %v", len(gotTriples), len(wantTriples), gotTriples, wantTriples)
	}
	for i := range wantTriples {
		if gotTriples[i] != wantTriples[i] {
			t.Fatalf("FetchTriples() sets differ\n got: %v\nwant: %v", gotTriples, wantTriples)
		}
	}

	// --- Phase 1e: FetchKinds().
	before = builderServedCount(buf)
	gotKinds := relKindsByKind(t, ctx, bt, kind)
	if delta := builderServedCount(buf) - before; delta < 1 {
		t.Fatalf("FetchKinds(): builder-served marker delta = %d, want >= 1", delta)
	}
	wantKinds := relKindsByKind(t, ctx, oracle, kind)
	if len(gotKinds) != len(wantKinds) {
		t.Fatalf("FetchKinds() returned %d rows, want %d\n got: %v\nwant: %v", len(gotKinds), len(wantKinds), gotKinds, wantKinds)
	}
	for i := range wantKinds {
		if gotKinds[i] != wantKinds[i] {
			t.Fatalf("FetchKinds() sets differ\n got: %v\nwant: %v", gotKinds, wantKinds)
		}
	}

	// --- Phase 2a: the !orderByEdgeID guard. The identical filter, with an
	// OrderBy recognize.OrderIsEdgeIDAscending accepts, must behave exactly
	// like the same call against the raw pg driver -- which, empirically,
	// PostgreSQL itself rejects (an aggregate Count() ordered by a plain
	// column, "ORDER BY column must appear in an aggregate" -- see
	// relCountByKindOrdered's doc) -- but must add NO new marker either way:
	// an order has no meaning for a bare count, so Count declines rather than
	// silently dropping it (relationship_query.go's Count doc) or serving an
	// answer PostgreSQL itself would refuse to compute this way.
	before = builderServedCount(buf)
	gotOrdered, gotOrderedErr := relCountByKindOrdered(t, ctx, bt, kind)
	delta := builderServedCount(buf) - before
	wantOrdered, wantOrderedErr := relCountByKindOrdered(t, ctx, oracle, kind)
	if (gotOrderedErr == nil) != (wantOrderedErr == nil) {
		t.Fatalf("Count() with OrderBy(edge id asc): error parity differs\n bt: count=%d err=%v\noracle: count=%d err=%v", gotOrdered, gotOrderedErr, wantOrdered, wantOrderedErr)
	}
	if gotOrderedErr == nil && gotOrdered != wantOrdered {
		t.Fatalf("Count() with OrderBy(edge id asc) = %d, want %d (oracle)", gotOrdered, wantOrdered)
	}
	if delta != 0 {
		t.Fatalf("Count() with OrderBy(edge id asc): builder-served marker delta = %d, want 0 (the !orderByEdgeID guard should have declined and fallen through to PostgreSQL)", delta)
	}

	// --- Phase 2b: the !tainted guard. The identical filter, with a Limit
	// (one of the calls that taints a recordingRelationshipQuery -- see
	// relationship_query.go's tainted doc) added before Count(), must
	// likewise add NO new marker: a taint changes what Count would need to
	// mean, so the engine must not be consulted at all.
	before = builderServedCount(buf)
	gotLimited := relCountByKindLimited(t, ctx, bt, kind, 1)
	delta = builderServedCount(buf) - before
	wantLimited := relCountByKindLimited(t, ctx, oracle, kind, 1)
	if gotLimited != wantLimited {
		t.Fatalf("Count() with Limit(1) = %d, want %d (oracle)", gotLimited, wantLimited)
	}
	if delta != 0 {
		t.Fatalf("Count() with Limit(1): builder-served marker delta = %d, want 0 (the !tainted guard should have declined and fallen through to PostgreSQL)", delta)
	}
}
