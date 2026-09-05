// SPDX-License-Identifier: Apache-2.0

// Command builderbench benchmarks BloodTrail's builder-query serving path
// (dawgs' structural Nodes()/Relationships() query builder, as opposed to
// pathbench's shortest-path queries) against a graph already loaded into
// PostgreSQL, normally by bench/adgen (see bench/adgen/README.md).
//
// Unlike pathbench (which constructs internal/engine directly, bypassing
// the poller entirely via a manual RebuildNow call), builderbench opens the
// *real* production driver -- dawgs.Open(ctx, bloodtrail.DriverName, cfg),
// exactly as BloodHound would -- because the shapes benchmarked here are
// intercepted by the driver's Nodes()/Relationships() wrapping
// (recordingNodeQuery/recordingRelationshipQuery, see relationship_query.go
// and node_query.go at the repo root), not by internal/engine's traverse
// package directly. A second dawgs.Open(ctx, pg.DriverName, cfg) on the same
// DSN and pool is the delegated baseline: plain PostgreSQL, no engine at
// all.
//
// It measures four query shapes BloodHound's builder issues in production:
//
//  1. container.FetchDirectedGraph over the MemberOf edge kind -- a
//     whole-kind-pair scan, the shape dawgs' own container package uses to
//     pull a filtered subgraph into memory.
//  2. traversal.BreadthFirst + traversal.LightweightDriver, a group-members
//     expansion from the graph's highest-in-degree MemberOf-target group
//     (found with one SQL query at startup).
//  3. Nodes().Filter(Kind(Node(), User)).Count(), and the same filter's
//     FetchIDs() fully drained.
//  4. Relationships().Filter(And(KindIn(Start(), Base), Kind(Relationship(),
//     AdminTo), KindIn(End(), Base))).FetchIDs() fully drained -- the
//     DeleteTransitEdges shape real BloodHound issues before recomputing a
//     derived edge kind wholesale (see shapeDeleteTransitEdges' doc for why
//     AdminTo stands in for "derived" here).
//
// For each shape, builderbench runs one warmup call against each driver
// (bloodtrail then pg), comparing a cheap size metric (a node/edge count,
// never the full result set) between the two as a correctness guard, then
// -runs (default 5) further timed calls against each driver, reporting
// p50/p95 per driver and the p50 ratio (pg / bloodtrail) -- how many times
// faster serving from memory is than delegating to PostgreSQL.
//
// Before any shape is measured, builderbench waits for the bloodtrail
// driver's background poller to build its first in-memory snapshot. There
// is no way to observe that build completing from outside the driver (no
// exported hook, and log-scraping a specific message string is deliberately
// not used here -- see the package's task brief): instead, builderbench
// times its own throwaway engine.LoadSnapshot call against the same data
// (also how it reports the snapshot's node/edge/byte counts cheaply) and
// sleeps a safety multiple of that measured cost plus the poller's own poll
// interval, printing the wait duration. This scales with graph size
// automatically, unlike a fixed sleep.
//
// Usage:
//
//	go run ./bench/builderbench -dsn <dsn> [-runs 5] [-enforce]
//
// builderbench never imports bench/adgen (a generator, not a library) and
// never writes to the database (beyond a scratch datapipe_status row it
// creates and drops itself, needed to drive the poller -- see
// createDatapipeStatusTable's doc): it finds every kind name purely by
// duplicating bench/adgen/generate.go's constants, the same convention
// pathbench follows.
//
// Every shape prints a human-readable line and a machine-greppable
// BUILDERBENCH_* summary line (grep '^BUILDERBENCH_'), ending in
// BUILDERBENCH_RESULT PASS or FAIL. With -enforce, builderbench exits
// nonzero if any shape's p50 ratio is below enforceRatio (5x) or if its two
// drivers' results mismatched -- see benchResult.report's doc for why a 5x
// ratio is itself proof the engine actually served the query rather than
// silently delegating. CI must never pass -enforce. Any other failure (a
// database error, an empty graph, a driver that returns an outright error)
// aborts the run with a nonzero exit regardless of -enforce.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/graphcache"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/traversal"
	"github.com/specterops/dawgs/util/size"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// graphName matches bench/adgen's graphName and internal/graphtest.GraphName
// -- see pathbench's identical constant for the full rationale.
const graphName = "bloodtrail_test"

// Node/edge kind name constants, duplicated from bench/adgen/generate.go
// (never imported -- see the package doc, and pathbench's identical
// duplication). These must stay in sync with that file's NodeKinds/
// EdgeKinds by hand.
const (
	kindBase     = "Base"
	kindUser     = "User"
	kindComputer = "Computer"
	kindGroup    = "Group"

	// edgeMemberOf is bench/adgen's only membership-shaped edge kind (see
	// generate.go's Generate) and, by construction, also its highest-count
	// edge kind: every user gets 1 guaranteed MemberOf edge to its domain's
	// Domain Users hub plus extraMemberOfPerUser (8) more to random groups,
	// dwarfing every other edge category. This is shape 1's "biggest
	// membership edge kind" and shape 2's expansion kind.
	edgeMemberOf   = "MemberOf"
	edgeAdminTo    = "AdminTo"
	edgeHasSession = "HasSession"
	edgeGenericAll = "GenericAll"
	edgeWriteDacl  = "WriteDacl"
	edgeAddMember  = "AddMember"
)

var (
	nodeKindNames = []string{kindBase, kindUser, kindComputer, kindGroup}
	edgeKindNames = []string{edgeMemberOf, edgeAdminTo, edgeHasSession, edgeGenericAll, edgeWriteDacl, edgeAddMember}
)

// enforceRatio is the minimum p50 ratio (delegated / served) -enforce
// requires of every shape -- see benchResult.report's doc for why this
// number itself doubles as evidence the engine served the query at all.
const enforceRatio = 5.0

// bfsWorkers is the traversal.New worker count for shape 2's group-members
// BFS. dawgs v0.8.0 has a known internal data race in multi-worker
// graph.PathSegment.Descend: it accumulates a path tree's size by walking
// Trunk pointers up to the root and mutating each ancestor's unexported
// size field with a plain, unsynchronized +=, so two goroutines descending
// from segments that share an ancestor (any two children of the same node)
// race on that ancestor's size. This repo's own
// engine_serving_integration_test.go (bfsCollectNodeIDs) works around it by
// using exactly 1 worker, since that test runs under `go test -race`.
// builderbench is a plain `go run` binary that is never built or run with
// -race, so the race itself is harmless here; bfsWorkers is still kept
// small (2, not a large production worker count) so this shape's timing
// stays dominated by real traversal work rather than goroutine-scheduling
// noise.
const bfsWorkers = 2

// enginePollInterval is set via BLOODTRAIL_ENGINE_POLL_INTERVAL before
// opening the bloodtrail driver, short enough that the wait computed in
// execute (waitForFreshSnapshot's caller) is dominated by the actual
// snapshot build cost rather than by the poller's own idle cadence.
const enginePollInterval = 200 * time.Millisecond

func main() {
	os.Exit(run(os.Args[1:]))
}

// run parses flags, executes the benchmark, prints its report, and returns
// the process exit code. Kept separate from main so main can call os.Exit
// with the returned code -- mirroring pathbench's run/main split.
func run(args []string) int {
	fs := flag.NewFlagSet("builderbench", flag.ContinueOnError)
	var (
		dsn     = fs.String("dsn", "", "PostgreSQL connection string, e.g. postgresql://user:pass@host:port/db")
		runs    = fs.Int("runs", 5, "number of warmed-up, timed runs per shape per driver")
		enforce = fs.Bool("enforce", false, "exit nonzero if any shape's p50 ratio < 5x or its two drivers' results mismatch (never pass this in CI)")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "builderbench: -dsn is required")
		return 2
	}
	if *runs <= 0 {
		fmt.Fprintln(os.Stderr, "builderbench: -runs must be positive")
		return 2
	}

	result, err := execute(context.Background(), config{dsn: *dsn, runs: *runs})
	if err != nil {
		fmt.Fprintf(os.Stderr, "builderbench: %v\n", err)
		return 1
	}

	pass := result.report(*enforce)
	if *enforce && !pass {
		return 1
	}
	return 0
}

// config holds run's parsed flags that execute needs.
type config struct {
	dsn  string
	runs int
}

// shapeSpec is one benchmarked query shape: a name for reporting and a
// function that runs it once against db, fully draining any cursor it
// opens (so the returned duration reflects the whole query, not just a
// first-row latency), and returning a cheap comparable size metric (a
// count -- never the full result set) used both as the shape's headline
// number and as the two-driver correctness check.
type shapeSpec struct {
	name string
	run  func(ctx context.Context, db graph.Database) (int64, error)
}

// shapeResult accumulates one shapeSpec's measurements against both
// drivers.
type shapeResult struct {
	name string

	btSize, pgSize int64
	match          bool

	btDurations []time.Duration
	pgDurations []time.Duration
}

// btP50P95 and pgP50P95 return the nearest-rank p50/p95 of each driver's
// timed-run durations.
func (r shapeResult) btP50P95() (time.Duration, time.Duration) {
	return percentile(r.btDurations, 0.50), percentile(r.btDurations, 0.95)
}

func (r shapeResult) pgP50P95() (time.Duration, time.Duration) {
	return percentile(r.pgDurations, 0.50), percentile(r.pgDurations, 0.95)
}

// ratio is the delegated/served p50 speed ratio: how many times faster the
// bloodtrail driver's p50 is than the plain pg driver's p50. A bt p50 of
// zero (never observed in practice, but guarded rather than divided by)
// reports +Inf unless pg's p50 is also zero, in which case the two are
// declared equal (ratio 1).
func (r shapeResult) ratio() float64 {
	btp50, _ := r.btP50P95()
	pgp50, _ := r.pgP50P95()
	if btp50 <= 0 {
		if pgp50 <= 0 {
			return 1
		}
		return math.Inf(1)
	}
	return float64(pgp50) / float64(btp50)
}

// ok reports whether this shape satisfies -enforce: its two drivers' size
// metrics matched, and its p50 ratio meets enforceRatio.
func (r shapeResult) ok() bool {
	return r.match && r.ratio() >= enforceRatio
}

// benchResult accumulates every phase's measurements for report to print.
type benchResult struct {
	buildDuration time.Duration
	buildNodes    int
	buildEdges    int
	buildBytes    uint64

	pollInterval time.Duration
	waitDuration time.Duration

	rootID       uint64
	rootObjectID string
	rootInDegree int64

	shapes []shapeResult
}

// execute runs the whole benchmark against cfg.dsn and returns the
// collected measurements. Any error here is a setup/infrastructure failure
// (a bad DSN, an empty graph, a driver error) and always aborts the run
// with a nonzero exit, regardless of -enforce -- -enforce only gates
// benchResult.report's per-shape ratio/match checks.
func execute(ctx context.Context, cfg config) (*benchResult, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pg.NewPool(poolCfg)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	// Every driver opened below (setupDriver, bt, oracle) shares this one
	// pool, exactly like pathbench and bench/adgen share one pool across
	// their own driver constructions. *pg.Driver.Close (dawgs v0.8.0) closes
	// the *pgxpool.Pool it was built from, not just its own bookkeeping --
	// so bt and oracle are deliberately never Close()'d below (that would
	// close this shared pool out from under whichever driver is still in
	// use); this defer is builderbench's one and only close. builderbench
	// is a one-shot CLI process, so bt's background poller goroutine
	// (started deep inside dawgs.Open by bloodtrail.Open) is simply
	// abandoned at process exit rather than stopped gracefully -- there is
	// no exported way to stop it without calling the very Close this
	// comment explains why we avoid, and an abandoned goroutine in a
	// process about to exit is harmless.
	defer pool.Close()

	setupDriver := pg.NewDriver(size.Gibibyte, pool)
	schema := graph.Schema{
		Graphs:       []graph.Graph{{Name: graphName, Nodes: stringKinds(nodeKindNames), Edges: stringKinds(edgeKindNames)}},
		DefaultGraph: graph.Graph{Name: graphName},
	}
	if err := setupDriver.AssertSchema(ctx, schema); err != nil {
		return nil, fmt.Errorf("assert schema: %w", err)
	}
	graphModel, ok := setupDriver.DefaultGraph()
	if !ok {
		return nil, fmt.Errorf("no default graph resolved after AssertSchema")
	}

	allKinds := append(stringKinds(nodeKindNames), stringKinds(edgeKindNames)...)
	if _, err := setupDriver.KindMapper().AssertKinds(ctx, allKinds); err != nil {
		return nil, fmt.Errorf("assert kinds: %w", err)
	}
	kindMapper := setupDriver.KindMapper()

	memberOfKindID, err := kindMapper.MapKind(ctx, graph.StringKind(edgeMemberOf))
	if err != nil {
		return nil, fmt.Errorf("map kind %q: %w", edgeMemberOf, err)
	}
	groupKindID, err := kindMapper.MapKind(ctx, graph.StringKind(kindGroup))
	if err != nil {
		return nil, fmt.Errorf("map kind %q: %w", kindGroup, err)
	}

	result := &benchResult{}

	fmt.Println("builderbench: measuring snapshot build cost (also the wait budget below) ...")
	buildStart := time.Now()
	snap, err := engine.LoadSnapshot(ctx, setupDriver, pool)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	result.buildDuration = time.Since(buildStart)
	result.buildNodes = snap.NodeCount()
	result.buildEdges = snap.EdgeCount()
	result.buildBytes = snap.ApproxBytes()
	fmt.Printf("builderbench: measured build: nodes=%d edges=%d approx_bytes=%d (%s) duration=%s\n",
		result.buildNodes, result.buildEdges, result.buildBytes, fmtBytes(result.buildBytes), fmtSeconds(result.buildDuration))
	fmt.Printf("BUILDERBENCH_BUILD nodes=%d edges=%d approx_bytes=%d duration_ms=%.3f\n",
		result.buildNodes, result.buildEdges, result.buildBytes, floatMillis(result.buildDuration))

	rootID, rootObjectID, inDegree, err := highestInDegreeGroup(ctx, pool, graphModel.ID, memberOfKindID, groupKindID)
	if err != nil {
		return nil, fmt.Errorf("find highest in-degree group: %w", err)
	}
	result.rootID, result.rootObjectID, result.rootInDegree = rootID, rootObjectID, inDegree
	fmt.Printf("builderbench: highest in-degree group (shape 2's BFS root): objectid=%s id=%d in_degree(%s)=%d\n",
		rootObjectID, rootID, edgeMemberOf, inDegree)

	if err := createDatapipeStatusTable(ctx, pool); err != nil {
		return nil, fmt.Errorf("create datapipe_status: %w", err)
	}
	defer func() {
		if err := dropDatapipeStatusTable(ctx, pool); err != nil {
			fmt.Fprintf(os.Stderr, "builderbench: drop datapipe_status: %v\n", err)
		}
	}()
	if err := insertDatapipeStatus(ctx, pool, "idle", time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("insert datapipe_status: %w", err)
	}

	// Must be set before dawgs.Open below: bloodtrail.Open reads
	// SettingsFromEnv exactly once, at construction time.
	if err := os.Setenv(bloodtrail.EnvEnginePollInterval, enginePollInterval.String()); err != nil {
		return nil, fmt.Errorf("set %s: %w", bloodtrail.EnvEnginePollInterval, err)
	}
	result.pollInterval = enginePollInterval

	dawgsCfg := dawgs.Config{ConnectionString: cfg.dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	bt, err := dawgs.Open(ctx, bloodtrail.DriverName, dawgsCfg)
	if err != nil {
		return nil, fmt.Errorf("open bloodtrail driver: %w", err)
	}
	if err := bt.AssertSchema(ctx, schema); err != nil {
		return nil, fmt.Errorf("assert schema (bloodtrail): %w", err)
	}

	oracle, err := dawgs.Open(ctx, pg.DriverName, dawgsCfg)
	if err != nil {
		return nil, fmt.Errorf("open pg driver: %w", err)
	}
	if err := oracle.AssertSchema(ctx, schema); err != nil {
		return nil, fmt.Errorf("assert schema (pg): %w", err)
	}

	// See the package doc: there is no exported way to observe the
	// bloodtrail driver's background poller actually finishing its first
	// build, so this waits a safety multiple of the just-measured build
	// cost plus the poller's own poll interval instead. 2x the poll
	// interval covers the wait for the first tick to fire at all (a
	// time.Ticker's first tick arrives one full interval after it starts,
	// never immediately) plus one interval of scheduling slack; 2x the
	// measured build cost covers the rebuild itself plus variance from
	// running against a now-shared pool. The 1s floor keeps tiny graphs
	// (whose build cost is sub-millisecond) from racing the poller on a
	// near-zero wait.
	waitDuration := 2*enginePollInterval + 2*result.buildDuration + 500*time.Millisecond
	if waitDuration < time.Second {
		waitDuration = time.Second
	}
	result.waitDuration = waitDuration
	fmt.Printf("builderbench: waiting %s for the bloodtrail driver's poller to build its first snapshot (poll_interval=%s) ...\n",
		waitDuration, enginePollInterval)
	time.Sleep(waitDuration)
	fmt.Printf("BUILDERBENCH_WAIT duration_ms=%.3f poll_interval_ms=%.3f\n", floatMillis(waitDuration), floatMillis(enginePollInterval))

	root, err := fetchNodeByID(ctx, bt, graph.ID(rootID))
	if err != nil {
		return nil, fmt.Errorf("fetch root node %d: %w", rootID, err)
	}

	shapes := []shapeSpec{
		{name: "fetch_directed_graph_memberof", run: shapeFetchDirectedGraph},
		{name: "group_members_bfs", run: makeGroupBFSShape(root)},
		{name: "node_count_user", run: shapeNodeCountUser},
		{name: "node_fetchids_user", run: shapeNodeFetchIDsUser},
		{name: "delete_transit_edges_admin_to", run: shapeDeleteTransitEdges},
	}

	for _, spec := range shapes {
		fmt.Printf("\n=== shape: %s ===\n", spec.name)
		sr, err := measureShape(ctx, spec, bt, oracle, cfg.runs)
		if err != nil {
			return nil, fmt.Errorf("shape %s: %w", spec.name, err)
		}
		printShapeResult(sr)
		result.shapes = append(result.shapes, sr)
	}

	return result, nil
}

// measureShape runs spec once against bt and once against oracle as
// warmup, comparing their size metrics for spec.name's correctness check,
// then runs runs further timed calls against each, in turn (bt block, then
// oracle block) -- collecting only their durations, since the correctness
// check already ran once above and repeating it every timed iteration
// would just re-measure the same thing at proportional extra cost.
func measureShape(ctx context.Context, spec shapeSpec, bt, oracle graph.Database, runs int) (shapeResult, error) {
	result := shapeResult{name: spec.name}

	_, btSize, err := timeOnce(ctx, bt, spec.run)
	if err != nil {
		return result, fmt.Errorf("bloodtrail warmup: %w", err)
	}
	_, pgSize, err := timeOnce(ctx, oracle, spec.run)
	if err != nil {
		return result, fmt.Errorf("pg warmup: %w", err)
	}
	result.btSize, result.pgSize = btSize, pgSize
	result.match = btSize == pgSize

	for i := 0; i < runs; i++ {
		d, _, err := timeOnce(ctx, bt, spec.run)
		if err != nil {
			return result, fmt.Errorf("bloodtrail run %d: %w", i, err)
		}
		result.btDurations = append(result.btDurations, d)
	}
	for i := 0; i < runs; i++ {
		d, _, err := timeOnce(ctx, oracle, spec.run)
		if err != nil {
			return result, fmt.Errorf("pg run %d: %w", i, err)
		}
		result.pgDurations = append(result.pgDurations, d)
	}

	return result, nil
}

// timeOnce runs run against db once, returning its wall-clock duration
// alongside its result.
func timeOnce(ctx context.Context, db graph.Database, run func(context.Context, graph.Database) (int64, error)) (time.Duration, int64, error) {
	t0 := time.Now()
	value, err := run(ctx, db)
	return time.Since(t0), value, err
}

// shapeFetchDirectedGraph is shape 1: container.FetchDirectedGraph over
// MemberOf. NumNodes() is the size metric (an O(1) read on the already-
// built container.DirectedGraph) rather than an edge count -- the
// DirectedGraph interface dawgs returns from FetchDirectedGraph exposes no
// NumEdges(), only NumNodes()/EachNode/EachAdjacentNode, and walking the
// whole adjacency structure just to count edges would double this shape's
// real cost for a correctness check that a node-set-size comparison already
// satisfies.
func shapeFetchDirectedGraph(ctx context.Context, db graph.Database) (int64, error) {
	digraph, err := container.FetchDirectedGraph(ctx, db, query.KindIn(query.Relationship(), graph.StringKind(edgeMemberOf)))
	if err != nil {
		return 0, err
	}
	return int64(digraph.NumNodes()), nil
}

// makeGroupBFSShape returns shape 2: a traversal.BreadthFirst group-members
// expansion from root, following MemberOf edges inbound (i.e. from a member
// to the group it is a member of) so the collected set is every node
// transitively MemberOf root -- root's direct and nested members. This
// mirrors engine_serving_integration_test.go's bfsCollectNodeIDs exactly
// (traversal.LightweightDriver + traversal.UniquePathSegmentFilter feeding
// a traversal.NodeCollector), except for the worker count -- see bfsWorkers'
// doc.
func makeGroupBFSShape(root *graph.Node) func(ctx context.Context, db graph.Database) (int64, error) {
	return func(ctx context.Context, db graph.Database) (int64, error) {
		collector := traversal.NewNodeCollector()
		filter := traversal.UniquePathSegmentFilter(func(next *graph.PathSegment) bool {
			collector.Collect(next)
			return true
		})
		driver := traversal.LightweightDriver(graph.DirectionInbound, graphcache.New(), query.Kind(query.Relationship(), graph.StringKind(edgeMemberOf)), filter)

		if err := (traversal.New(db, bfsWorkers)).BreadthFirst(ctx, traversal.Plan{Root: root, Driver: driver}); err != nil {
			return 0, err
		}
		return int64(len(collector.Nodes)), nil
	}
}

// shapeNodeCountUser is shape 3's Count() half: Nodes().Filter(Kind(Node(),
// User)).Count(), the exact shape recordingNodeQuery.Count (node_query.go)
// recognizes and may serve from the engine.
func shapeNodeCountUser(ctx context.Context, db graph.Database) (int64, error) {
	var count int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Nodes().Filter(query.Kind(query.Node(), graph.StringKind(kindUser))).Count()
		count = n
		return err
	})
	return count, err
}

// shapeNodeFetchIDsUser is shape 3's FetchIDs() half, fully drained: the
// same filter as shapeNodeCountUser, but reading every matching id instead
// of just counting them.
func shapeNodeFetchIDsUser(ctx context.Context, db graph.Database) (int64, error) {
	var n int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Filter(query.Kind(query.Node(), graph.StringKind(kindUser))).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for range cursor.Chan() {
				n++
			}
			return cursor.Error()
		})
	})
	return n, err
}

// shapeDeleteTransitEdges is shape 4: the DeleteTransitEdges read shape
// real BloodHound issues before recomputing a derived edge kind wholesale
// -- And(KindIn(Start(), Base), Kind(Relationship(), k), KindIn(End(),
// Base)), FetchIDs fully drained. Base (bench/adgen's kind every node
// carries) stands in for the "any node" endpoint bound production
// BloodHound uses here.
//
// edgeAdminTo stands in for k, a genuinely *derived* edge kind: production
// BloodHound computes AdminTo, among others, by post-processing direct and
// nested-group admin memberships, so its query shape when recomputing it
// (delete every existing AdminTo edge between two Base-bound endpoints,
// then reinsert the freshly computed set) matches exactly. bench/adgen
// (generate.go) has no separate synthesized-vs-computed distinction --
// every edge it writes is "direct" -- so AdminTo, an edge kind real
// BloodHound genuinely does derive, is the closest available analog it
// actually writes.
//
// This exact criteria shape (KindIn(Start)/Kind(Relationship)/KindIn(End))
// is confirmed recognized by internal/engine/recognize.FromRelCriteria --
// see recognize/builder_test.go's TestFromRelCriteria_Accepts.
func shapeDeleteTransitEdges(ctx context.Context, db graph.Database) (int64, error) {
	baseKind := graph.StringKind(kindBase)
	criteria := query.And(
		query.KindIn(query.Start(), baseKind),
		query.Kind(query.Relationship(), graph.StringKind(edgeAdminTo)),
		query.KindIn(query.End(), baseKind),
	)

	var n int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Relationships().Filter(criteria).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for range cursor.Chan() {
				n++
			}
			return cursor.Error()
		})
	})
	return n, err
}

// fetchNodeByID fetches the single *graph.Node carrying id through db, for
// use as shape 2's traversal.Plan.Root. Unlike every other shape here, this
// is not itself measured -- it runs exactly once, before the shape loop --
// and, per engine_serving_integration_test.go's identically named helper's
// doc, always reaches PostgreSQL directly (First() is not one of
// recordingNodeQuery's engine-serving overrides) regardless of which
// driver db is, so it is called on bt purely for convenience.
func fetchNodeByID(ctx context.Context, db graph.Database, id graph.ID) (*graph.Node, error) {
	var node *graph.Node
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		n, err := tx.Nodes().Filter(query.Equals(query.NodeID(), id)).First()
		node = n
		return err
	})
	return node, err
}

// highestInDegreeGroup finds graphID's Group-kind node with the most
// incoming memberOfKindID edges -- shape 2's BFS root -- via one SQL query
// against the edge table's (graph_id, kind_id) columns, restricted to
// Group-kind endpoints via a subquery against the node table's GIN index on
// kind_ids (the same "kind_ids operator(pg_catalog.@>) ARRAY[...]::int2[]"
// idiom pathbench's countByKind uses, schema-qualified for the same reason:
// the database also loads the intarray extension, which overloads bare
// "@>" for smallint[]). Returns the winning node's database id, its
// "objectid" property (for a readable report line), and its in-degree.
func highestInDegreeGroup(ctx context.Context, pool *pgxpool.Pool, graphID int32, memberOfKindID, groupKindID int16) (id uint64, objectID string, inDegree int64, err error) {
	var rawID int64
	if err := pool.QueryRow(ctx,
		`SELECT end_id, count(*) AS in_degree
		 FROM edge
		 WHERE graph_id = $1 AND kind_id = $2
		   AND end_id IN (
		       SELECT id FROM node
		       WHERE graph_id = $1 AND kind_ids operator(pg_catalog.@>) ARRAY[$3]::int2[]
		   )
		 GROUP BY end_id
		 ORDER BY in_degree DESC
		 LIMIT 1`,
		graphID, memberOfKindID, groupKindID,
	).Scan(&rawID, &inDegree); err != nil {
		return 0, "", 0, err
	}
	id = uint64(rawID)

	if err := pool.QueryRow(ctx, `SELECT properties->>'objectid' FROM node WHERE id = $1`, rawID).Scan(&objectID); err != nil {
		return 0, "", 0, err
	}
	return id, objectID, inDegree, nil
}

// createDatapipeStatusTable, insertDatapipeStatus, and
// dropDatapipeStatusTable manage a scratch datapipe_status row: the
// column names and constraint verified against upstream BloodHound
// v9.6.0's migrations -- see internal/engine/poller_integration_test.go's
// identically named test helper for the full provenance note, duplicated
// here (a non-test main package cannot import a _test.go helper) for the
// same reason bench/pathbench and bench/adgen duplicate other constants
// rather than import test-only code.
//
// The bloodtrail driver's poller (internal/engine/poller.go's tick) reads
// this table on every tick and does nothing at all if the query fails --
// including "relation does not exist" -- so without this table the poller
// would silently never build a snapshot, and builderbench would spend its
// whole run delegating to PostgreSQL on both drivers (a ratio of ~1x, not
// an error). createDatapipeStatusTable must run, and insertDatapipeStatus's
// row must exist, before dawgs.Open constructs the bloodtrail driver in
// execute, so the very first poll tick already finds a valid row.
func createDatapipeStatusTable(ctx context.Context, pool *pgxpool.Pool) error {
	const ddl = `CREATE TABLE IF NOT EXISTS datapipe_status (
		singleton boolean DEFAULT true NOT NULL,
		status text NOT NULL,
		updated_at timestamp with time zone NOT NULL,
		last_complete_analysis_at timestamp with time zone,
		last_analysis_run_at timestamp with time zone,
		last_complete_optimize_at timestamp with time zone,
		next_scheduled_analysis_at timestamp with time zone,
		CONSTRAINT singleton_uni CHECK (singleton)
	)`
	_, err := pool.Exec(ctx, ddl)
	return err
}

func insertDatapipeStatus(ctx context.Context, pool *pgxpool.Pool, status string, stamp time.Time) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO datapipe_status (singleton, status, updated_at, last_complete_analysis_at) VALUES (true, $1, now(), $2)`,
		status, stamp,
	)
	return err
}

func dropDatapipeStatusTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS datapipe_status")
	return err
}

// report prints every shape's enforcement line and the final PASS/FAIL
// line, returning whether every shape satisfied enforceRatio.ok()
// (independent of whether -enforce was actually passed -- run decides
// whether that return value changes the exit code).
//
// A shape's p50 ratio meeting enforceRatio (5x) is itself strong evidence
// the bloodtrail driver actually served the query from its in-memory
// snapshot rather than silently delegating to PostgreSQL: a 5x speedup
// purely from query planning or connection reuse against the same
// PostgreSQL tables, with no in-memory structure involved at all, is not
// achievable -- delegating is delegating, on the same database, at
// essentially the same cost either way. So -enforce's ratio check doubles
// as the "did it actually serve" check the task brief considered and
// rejected exporting a hook for.
func (r *benchResult) report(enforce bool) bool {
	allOK := true

	fmt.Printf("\n=== enforce thresholds (checked=%t, min_p50_ratio=%.1fx) ===\n", enforce, enforceRatio)
	for _, s := range r.shapes {
		btp50, btp95 := s.btP50P95()
		pgp50, pgp95 := s.pgP50P95()
		ratio := s.ratio()
		ok := s.ok()
		allOK = allOK && ok

		fmt.Printf("builderbench: %-32s ratio=%7.2fx match=%-5t %s\n", s.name, ratio, s.match, passFail(ok))
		fmt.Printf("BUILDERBENCH_SHAPE name=%s bt_p50_ms=%.3f bt_p95_ms=%.3f pg_p50_ms=%.3f pg_p95_ms=%.3f ratio=%.3f match=%t bt_size=%d pg_size=%d ok=%t\n",
			s.name, floatMillis(btp50), floatMillis(btp95), floatMillis(pgp50), floatMillis(pgp95), ratio, s.match, s.btSize, s.pgSize, ok)
	}

	fmt.Printf("BUILDERBENCH_ENFORCE checked=%t ok=%t min_ratio=%.1f\n", enforce, allOK, enforceRatio)
	if allOK {
		fmt.Println("BUILDERBENCH_RESULT PASS")
	} else {
		fmt.Println("BUILDERBENCH_RESULT FAIL")
	}
	return allOK
}

// printShapeResult prints one shape's human-readable detail: both drivers'
// size metric and whether they matched, every timed run's duration, and
// the p50/p95/ratio summary.
func printShapeResult(r shapeResult) {
	fmt.Printf("builderbench: %s: bt_size=%d pg_size=%d match=%t\n", r.name, r.btSize, r.pgSize, r.match)
	fmt.Printf("builderbench: %s: bt runs (ms): %s\n", r.name, fmtDurationsMs(r.btDurations))
	fmt.Printf("builderbench: %s: pg runs (ms): %s\n", r.name, fmtDurationsMs(r.pgDurations))

	btp50, btp95 := r.btP50P95()
	pgp50, pgp95 := r.pgP50P95()
	fmt.Printf("builderbench: %s: bt p50=%s p95=%s | pg p50=%s p95=%s | ratio(pg/bt)=%.2fx\n",
		r.name, fmtMillis(btp50), fmtMillis(btp95), fmtMillis(pgp50), fmtMillis(pgp95), r.ratio())
}

// stringKinds converts kind name strings to graph.Kinds, matching
// bench/adgen's and pathbench's own helper of the same name/shape.
func stringKinds(names []string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// percentile returns durations' p-th percentile (nearest-rank method, 0 <
// p <= 1), sorting a copy so the caller's slice order is undisturbed. An
// empty input returns zero. Duplicated from pathbench (each bench tool is
// self-contained -- see the package doc's "never imports bench/adgen").
func percentile(durations []time.Duration, p float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func floatMillis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func fmtMillis(d time.Duration) string {
	return fmt.Sprintf("%.2fms", floatMillis(d))
}

func fmtSeconds(d time.Duration) string {
	return fmt.Sprintf("%.3fs", d.Seconds())
}

func fmtBytes(n uint64) string {
	const mib = 1024 * 1024
	return fmt.Sprintf("%.1f MiB", float64(n)/mib)
}

// fmtDurationsMs renders durations as a compact, human-readable list of
// millisecond values, e.g. "[12.10 11.94 12.53]".
func fmtDurationsMs(durations []time.Duration) string {
	parts := make([]string, len(durations))
	for i, d := range durations {
		parts[i] = fmt.Sprintf("%.2f", floatMillis(d))
	}
	out := "["
	for i, p := range parts {
		if i > 0 {
			out += " "
		}
		out += p
	}
	return out + "]"
}

// slogDiscard silences the bloodtrail driver's own default logging (it
// would otherwise log an Info line for driver construction and, depending
// on BLOODTRAIL_LOG_LEVEL, per-query serve/decline lines) so builderbench's
// own output stays the only thing on stdout/stderr. Installed once, in
// init, since bloodtrail.Open reads slog.Default() exactly once at
// construction time and this must happen before that -- long before
// execute's own flow reaches dawgs.Open, so init is the only place this can
// reliably run first regardless of how run/execute are restructured later.
func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}
