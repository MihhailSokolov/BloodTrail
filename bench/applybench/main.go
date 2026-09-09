// SPDX-License-Identifier: Apache-2.0

// Command applybench benchmarks BloodTrail's write-through path
// (Engine.Apply, internal/engine/apply.go, run synchronously inside every
// mutating driver call -- see the root package's driver.go) against a graph
// already loaded into PostgreSQL, normally by bench/adgen (see
// bench/adgen/README.md).
//
// Unlike bench/pathbench (which constructs internal/engine directly),
// applybench opens the *real* production driver -- dawgs.Open(ctx,
// bloodtrail.DriverName, cfg) -- for every measurement that drives writes or
// reads through it, because write-through and its serving path are only
// reachable through the real driver's BatchOperation/ReadTransaction
// wiring, the same reason bench/cypherbench and bench/builderbench open the
// real driver instead of internal/engine directly. Each measurement below
// opens its own driver instance (its own *pgxpool.Pool, closed by that
// instance's own Close call) with BLOODTRAIL_ENGINE/BLOODTRAIL_COMPACT_*/
// BLOODTRAIL_SNAPSHOT_DIR set however that measurement needs -- these are
// process-wide environment variables read once at dawgs.Open time
// (settings.go's SettingsFromEnv), so applybench never runs two driver
// instances that need different settings concurrently; every phase below
// runs its writes/opens sequentially for exactly this reason.
//
// It measures four things from the milestone's write-through spec:
//
//   - (a) sustained apply throughput: ingest-shaped writes (batch.
//     UpdateNodeBy objectid upserts for new User nodes, batch.
//     UpdateRelationshipBy upserts for their MemberOf edge to an existing
//     hub Group node, flushed via an explicit batch.Commit() every -flush
//     operations -- matching production's write-flush cadence, see
//     write_observer.go's observingBatch.Commit doc for why an explicit
//     mid-delegate Commit is what actually makes write-through Apply run at
//     that boundary rather than only once at the very end of the whole
//     BatchOperation call) into the loaded graph, -repeats times with the
//     engine on and -repeats times with it off (BLOODTRAIL_ENGINE), report
//     total write wall time for both and the engine's own apply+read-back
//     overhead as a percentage of the disabled baseline
//     ((on-off)/off*100).
//   - (b) query latency during active ingest: a pathbench-style two-node
//     FetchAllShortestPaths query, sampled continuously for as long as each
//     engine-on repeat's ingest runs concurrently, compared against an idle
//     baseline (the same query shape, sampled -queries times with no
//     concurrent writes) -- both p50/p95.
//   - (c) compaction duration at scale: BLOODTRAIL_COMPACT_ENTRIES set low
//     enough to force several background compactions, each one's own
//     engine-measured duration (internal/engine/compact.go's runCompaction
//     logs "bloodtrail: compaction finished" with the exact fold+publish
//     duration) captured via a temporary slog.Handler -- see logCapture's
//     doc for why log capture, not CompactionCount, is what this package
//     uses: CompactionCount is a method on the internal *engine.Engine,
//     which the root package's Driver embeds unexported, so nothing outside
//     package bloodtrail (applybench included) can reach it through the
//     real driver at all.
//   - (d) snapshot file write and load duration at scale:
//     BLOODTRAIL_SNAPSHOT_DIR pointed at a scratch directory, Close's own
//     Stop-then-SaveSnapshot call timed end to end, then a fresh driver
//     open against the same directory timed from Open to the
//     engine-logged "bloodtrail: snapshot file loaded" line (again via
//     logCapture -- boot load runs on its own background goroutine with no
//     other way to observe adoption from outside the driver).
//
// Every measurement prints a human-readable line and a machine-greppable
// APPLYBENCH_<NAME> summary line (grep '^APPLYBENCH_'), ending in
// APPLYBENCH_RESULT PASS or FAIL. With -enforce, applybench exits nonzero if
// (a)'s overhead exceeds applyOverheadMaxPct or (b)'s during-ingest p95
// exceeds idleP95Multiplier times the idle p95 -- see both constants' docs
// for why they are PROVISIONAL. (c) and (d) are reported only, never
// enforced: this task's brief calls for evidence-based caps on those to be
// set once they have been measured at production scale, which is a later
// task's job (see the README's cap-table convention, matching
// bench/builderbench's/bench/cypherbench's measured-physics caps). CI must
// never pass -enforce. Any other failure (a database error, a missing base
// graph, a phase exceeding its own -cap watchdog) aborts the run with a
// nonzero exit regardless of -enforce.
//
// # Watchdog
//
// Every phase that drives writes or waits on an asynchronous engine event
// (a background compaction adopting, a boot-load goroutine loading a
// snapshot file) is bounded by -cap (default 10m), applied per operation via
// context.WithTimeout wrapped directly around that operation -- the same
// fail-fast convention bench/cypherbench's -bt-cap and bench/builderbench's
// -bt-cap use for their own engine-side calls: exceeding it always aborts
// the whole run (a hard error, never a graceful "capped" data point the way
// -pg-cap's pg_capped is for those two siblings, since there is no
// PostgreSQL-baseline half here to fall back on judging). A long-running
// write loop also prints progress every -flush operations, and the
// compaction/snapshot-load waits print progress every 2s while polling, so
// a genuinely slow (not hung) run is visibly making progress rather than
// looking indistinguishable from a wedged one -- see the 2026-09 5M-scale
// bench incident bench/cypherbench's defaultBTCap doc describes for why
// "wait and hope" is never an acceptable substitute for a hard cap here.
//
// Usage:
//
//	go run ./bench/applybench -dsn <dsn> [-ingest 12000] [-flush 20000] [-repeats 3] [-queries 50] [-compact-entries 2000] [-cap 10m] [-enforce] [-cpuprofile <file>]
//
// applybench never imports bench/adgen (a generator, not a library -- and,
// being package main, not importable at all): it requires a graph already
// loaded (normally via `go run ./bench/adgen -users N -wipe`, which the
// bench-apply Makefile target runs first) and finds every kind name purely
// by duplicating bench/adgen/generate.go's constants, the same convention
// pathbench/builderbench/cypherbench follow.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/drivers/pg/model"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
	"github.com/MihhailSokolov/BloodTrail/internal/engine"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// graphName matches bench/adgen's graphName and internal/graphtest.GraphName
// -- see bench/pathbench's and bench/builderbench's identical constant for
// the full rationale.
const graphName = "bloodtrail_test"

// Node/edge kind name constants, duplicated from bench/adgen/generate.go
// (never imported -- see the package doc). These must stay in sync with
// that file's NodeKinds/EdgeKinds by hand.
const (
	kindBase     = "Base"
	kindUser     = "User"
	kindComputer = "Computer"
	kindGroup    = "Group"

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

// Engine log message literals this package matches on via logCapture (see
// its own doc for why matching on the engine's own log lines, rather than a
// counter, is how this package observes compaction/snapshot-save events
// that happen on the driver's own background goroutines/Close call).
// Duplicated verbatim from internal/engine/compact.go and persist.go --
// TestLogMessagesMatchEngineSource (main_test.go) guards against these
// silently drifting out of sync with that package's own literals.
const (
	msgCompactionFinished = "bloodtrail: compaction finished"
	msgSnapshotWritten    = "bloodtrail: snapshot file written"
)

// applyOverheadMaxPct is -enforce's bar for (a): the engine-enabled write
// wall time must not exceed the engine-disabled baseline by more than this
// percentage. PROVISIONAL, per the task brief: this number was chosen
// before any measurement at production scale existed at all, so it is a
// placeholder bar, not evidence. A later task must replace it with an
// evidence-based cap (measured worst overhead x 1.75, per the m4.5
// measured-physics convention -- see bench/builderbench's/
// bench/cypherbench's shapeThresholds docs for that convention applied
// elsewhere) once a 5M-scale run has actually measured this number, and
// document the measurement in this package's README cap table the same
// way.
const applyOverheadMaxPct = 25.0

// idleP95Multiplier is -enforce's bar for (b): the during-ingest p95 must
// not exceed this multiple of the idle p95. PROVISIONAL for the identical
// reason applyOverheadMaxPct is -- see its doc, which applies here
// unchanged.
const idleP95Multiplier = 3.0

// defaultCap is -cap's default: see the package doc's "Watchdog" section.
const defaultCap = 10 * time.Minute

// progressEvery controls how often the ingest loop reports progress to
// stderr, mirroring bench/adgen's identically named constant and the
// package doc's "Watchdog" section (a long write loop must look visibly
// alive, not merely be alive).
const progressEvery = 5_000

func main() {
	os.Exit(run(os.Args[1:]))
}

// run parses flags, executes the benchmark, prints its report, and returns
// the process exit code -- mirroring bench/pathbench's/bench/builderbench's/
// bench/cypherbench's identical run/main split.
func run(args []string) int {
	fs := flag.NewFlagSet("applybench", flag.ContinueOnError)
	var (
		dsn            = fs.String("dsn", "", "PostgreSQL connection string, e.g. postgresql://user:pass@host:port/db")
		ingest         = fs.Int("ingest", 12_000, "ingest units written per repeat (each unit is 1 new User node upsert + 1 new MemberOf edge upsert to an existing hub Group -- M in the task's 'M nodes+edges' sense is 2x this)")
		flush          = fs.Int("flush", 20_000, "batch flush size in operations (an explicit batch.Commit() every this many UpdateNodeBy/UpdateRelationshipBy calls), matching production's write flush size")
		repeats        = fs.Int("repeats", 3, "number of repeats per measurement (p50 is taken over these); must be >= 3")
		queries        = fs.Int("queries", 50, "number of two-node shortest-path queries sampled for the idle latency baseline (the during-ingest sample runs continuously for as long as ingest does, unbounded by this flag)")
		compactEntries = fs.Int("compact-entries", 2_000, "BLOODTRAIL_COMPACT_ENTRIES used only for measurement (c); deliberately low so several compactions actually fire")
		seed           = fs.Int64("seed", 1, "seed for deterministic query-pair sampling")
		cap            = fs.Duration("cap", defaultCap, "per-operation wall-clock watchdog; any single operation exceeding this ABORTS THE WHOLE RUN (nonzero exit, never a data point) -- see README")
		enforce        = fs.Bool("enforce", false, "exit nonzero if apply overhead or during-ingest p95 miss their (PROVISIONAL) bars (never pass this in CI)")
		cpuprofile     = fs.String("cpuprofile", "", "write a pprof CPU profile to this file")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "applybench: -dsn is required")
		return 2
	}
	if *ingest <= 0 {
		fmt.Fprintln(os.Stderr, "applybench: -ingest must be positive")
		return 2
	}
	if *flush <= 0 {
		fmt.Fprintln(os.Stderr, "applybench: -flush must be positive")
		return 2
	}
	if *repeats < 3 {
		fmt.Fprintln(os.Stderr, "applybench: -repeats must be >= 3 (each measurement's p50 is taken over >= 3 repeats)")
		return 2
	}
	if *queries <= 0 {
		fmt.Fprintln(os.Stderr, "applybench: -queries must be positive")
		return 2
	}
	if *compactEntries <= 0 {
		fmt.Fprintln(os.Stderr, "applybench: -compact-entries must be positive")
		return 2
	}
	if *cap <= 0 {
		fmt.Fprintln(os.Stderr, "applybench: -cap must be positive")
		return 2
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "applybench: create cpuprofile: %v\n", err)
			return 1
		}
		defer func() {
			if cerr := f.Close(); cerr != nil {
				fmt.Fprintf(os.Stderr, "applybench: close cpuprofile: %v\n", cerr)
			}
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "applybench: start cpuprofile: %v\n", err)
			return 1
		}
		defer pprof.StopCPUProfile()
	}

	cfg := config{
		dsn:            *dsn,
		ingest:         *ingest,
		flush:          *flush,
		repeats:        *repeats,
		queries:        *queries,
		compactEntries: *compactEntries,
		seed:           *seed,
		cap:            *cap,
	}
	result, err := execute(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "applybench: %v\n", err)
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
	dsn            string
	ingest         int
	flush          int
	repeats        int
	queries        int
	compactEntries int
	seed           int64
	cap            time.Duration
}

// pair is one (User, Computer) sample for the two-node latency queries,
// identified by database id -- unlike bench/pathbench (which drives
// internal/engine's traverse package directly and so needs dense
// snapshot.NodeID values too), applybench only ever queries through the
// real driver's graph.RelationshipQuery.FetchAllShortestPaths, which takes
// plain graph.ID values.
type pair struct {
	startID, endID             graph.ID
	startObjectID, endObjectID string
}

// benchResult accumulates every measurement for report to print.
type benchResult struct {
	buildDuration time.Duration
	buildNodes    int
	buildEdges    int
	buildBytes    uint64

	onDurations  []time.Duration
	offDurations []time.Duration

	idleDurations   []time.Duration
	duringDurations []time.Duration

	compactionDurations []time.Duration
	compactionCount     int

	saveDurations []time.Duration
	loadDurations []time.Duration
}

// execute runs every measurement against cfg.dsn and returns the collected
// results. Any error here is a setup/infrastructure failure or a watchdog
// trip and always aborts the run with a nonzero exit, regardless of
// -enforce -- -enforce only gates benchResult.report's two named bars.
func execute(ctx context.Context, cfg config) (*benchResult, error) {
	base, cleanupIndex, err := discoverBaseGraph(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("discover base graph (run bench/adgen first): %w", err)
	}
	defer cleanupIndex()
	fmt.Printf("applybench: base graph: nodes=%d edges=%d approx_bytes=%d (%s) build_duration=%s\n",
		base.buildNodes, base.buildEdges, base.buildBytes, fmtBytes(base.buildBytes), fmtSeconds(base.buildDuration))
	fmt.Printf("APPLYBENCH_BUILD nodes=%d edges=%d approx_bytes=%d duration_ms=%.3f\n",
		base.buildNodes, base.buildEdges, base.buildBytes, floatMillis(base.buildDuration))

	result := &benchResult{buildDuration: base.buildDuration, buildNodes: base.buildNodes, buildEdges: base.buildEdges, buildBytes: base.buildBytes}

	fmt.Println("\n=== (a)+(b) sustained apply throughput and query latency during ingest ===")
	if err := measureThroughputAndLatency(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("throughput/latency: %w", err)
	}

	fmt.Println("\n=== (c) compaction duration at scale ===")
	if err := measureCompaction(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("compaction: %w", err)
	}

	fmt.Println("\n=== (d) snapshot file write/load duration at scale ===")
	if err := measureSnapshotIO(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("snapshot file I/O: %w", err)
	}

	return result, nil
}

// baseGraph is what discoverBaseGraph finds in the already-loaded graph:
// enough to drive every later measurement without ever touching
// bench/adgen's own (unimportable) generator code.
type baseGraph struct {
	graphID     int32
	hubObjectID string

	pairs []pair

	buildDuration time.Duration
	buildNodes    int
	buildEdges    int
	buildBytes    uint64
}

// discoverBaseGraph asserts the schema/kinds every bench in this repo
// duplicates (see the package doc), then finds this run's fixed inputs
// purely through SQL: an existing Group node to serve as every ingested
// User's MemberOf hub, and a pool of (User, Computer) id pairs to sample
// for the latency measurements. It also times one throwaway
// engine.LoadSnapshot call, exactly as bench/builderbench/bench/cypherbench
// do, both to report the base graph's size and as the safety-multiple input
// waitForBoot uses before any later phase starts issuing writes/queries
// against a freshly opened driver.
//
// The returned cleanup func drops the scratch objectid-upsert unique
// constraint benchSchema's own AssertSchema call creates (see its doc for
// why applybench needs that constraint at all, and objectIDConstraintName's
// doc for why its actual database name has to be computed, not guessed) --
// always non-nil, so the caller can defer it unconditionally, even when
// this function itself returns an error before AssertSchema ever runs (a
// no-op cleanup in that case, since nothing was created).
func discoverBaseGraph(ctx context.Context, cfg config) (*baseGraph, func(), error) {
	noopCleanup := func() {}

	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return nil, noopCleanup, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pg.NewPool(poolCfg)
	if err != nil {
		return nil, noopCleanup, fmt.Errorf("new pool: %w", err)
	}
	defer pool.Close()

	driver := pg.NewDriver(size.Gibibyte, pool)
	if err := driver.AssertSchema(ctx, benchSchema()); err != nil {
		return nil, noopCleanup, fmt.Errorf("assert schema: %w", err)
	}

	graphModel, ok := driver.DefaultGraph()
	if !ok {
		return nil, noopCleanup, fmt.Errorf("no default graph resolved after AssertSchema")
	}
	// Only known now that graphModel.ID has resolved -- see
	// dropObjectIDUpsertIndex's own doc for why the constraint's actual
	// database name depends on it.
	cleanup := dropObjectIDUpsertIndex(cfg.dsn, graphModel.ID)

	allKinds := append(stringKinds(nodeKindNames), stringKinds(edgeKindNames)...)
	if _, err := driver.KindMapper().AssertKinds(ctx, allKinds); err != nil {
		cleanup()
		return nil, noopCleanup, fmt.Errorf("assert kinds: %w", err)
	}
	kindMapper := driver.KindMapper()

	groupKindID, err := kindMapper.MapKind(ctx, graph.StringKind(kindGroup))
	if err != nil {
		cleanup()
		return nil, noopCleanup, fmt.Errorf("map kind %q: %w", kindGroup, err)
	}
	userKindID, err := kindMapper.MapKind(ctx, graph.StringKind(kindUser))
	if err != nil {
		cleanup()
		return nil, noopCleanup, fmt.Errorf("map kind %q: %w", kindUser, err)
	}
	compKindID, err := kindMapper.MapKind(ctx, graph.StringKind(kindComputer))
	if err != nil {
		cleanup()
		return nil, noopCleanup, fmt.Errorf("map kind %q: %w", kindComputer, err)
	}

	buildStart := time.Now()
	snap, err := engine.LoadSnapshot(ctx, driver, pool)
	if err != nil {
		cleanup()
		return nil, noopCleanup, fmt.Errorf("load snapshot: %w", err)
	}
	buildDuration := time.Since(buildStart)

	_, hubObjectID, err := nodeByKindOffset(ctx, pool, graphModel.ID, groupKindID, 0)
	if err != nil {
		cleanup()
		return nil, noopCleanup, fmt.Errorf("select a hub Group node: %w", err)
	}

	pairCount := cfg.queries
	if pairCount < 20 {
		pairCount = 20
	}
	pairs, err := selectPairs(ctx, pool, graphModel.ID, userKindID, compKindID, pairCount, cfg.seed)
	if err != nil {
		cleanup()
		return nil, noopCleanup, fmt.Errorf("select query pairs: %w", err)
	}

	return &baseGraph{
		graphID:       graphModel.ID,
		hubObjectID:   hubObjectID,
		pairs:         pairs,
		buildDuration: buildDuration,
		buildNodes:    snap.NodeCount(),
		buildEdges:    snap.EdgeCount(),
		buildBytes:    snap.ApproxBytes(),
	}, cleanup, nil
}

// objectIDConstraintField is the property benchSchema declares a required
// unique constraint on (see its own doc). Passed to graph.Constraint's
// Field, not Name: dawgs' own schema-reconciliation machinery
// (drivers/pg/model's ConstraintName) computes the constraint's ACTUAL
// database object name itself, from the partition table name and this
// field, ignoring whatever Name a caller supplies entirely -- see
// objectIDConstraintName's own doc, which is what this package must call
// to know what to actually DROP later.
const objectIDConstraintField = "objectid"

// benchSchema is the one graph.Schema value every phase in this package
// asserts (discoverBaseGraph, openPhaseDriver) -- factored out so it is
// impossible for two call sites to declare it inconsistently.
//
// Its DefaultGraph.NodeConstraints declares objectIDConstraintField as a
// required unique property constraint. This is not decorative: dawgs' pg
// driver compiles
// batch.UpdateNodeBy/UpdateRelationshipBy -- keyed by a bare "objectid"
// identity property, exactly the shape write_observer.go's
// recordNodeUpsertIdentity/recordRelationshipUpsertIdentity recognize and
// production BloodHound's own graphify ingestion uses -- to `insert ...
// on conflict ((properties->>'objectid')) do update ...`. PostgreSQL
// refuses that clause outright ("no unique or exclusion constraint
// matching the ON CONFLICT specification") unless a matching unique index
// already exists on the target partition. Production BloodHound's own
// schema declares one; the schema this repo's benches and integration
// tests share (schema_up.sql, from the pinned dawgs module) deliberately
// does not -- see apply_integration_test.go's own
// TestApplyObjectIDUpsertServesNewNode doc for exactly why NOT adding one
// to that long-lived, widely shared database is the right call there:
// every integration suite in this repo writes into the SAME graph
// ("bloodtrail_test", graphName's own doc), and some of their fixtures
// intentionally create duplicate objectid values, which a permanent index
// here would then reject.
//
// applybench still needs the real UpdateNodeBy/UpdateRelationshipBy shape
// to work at all (see the package doc's (a)), so it declares this
// constraint through dawgs' own schema-reconciliation mechanism
// (drivers/pg's SchemaManager.AssertGraph, reached through every
// AssertSchema call here) rather than issuing a bare CREATE UNIQUE INDEX by
// hand -- that reconciliation DROPS any constraint present on the
// partition that the schema it was just called with does not also
// declare as required (see AssertGraph's own comparison of "present" vs
// "required" constraints), so a hand-issued index that this package's own
// LATER phases' own AssertSchema calls didn't also know to ask for would
// simply be deleted out from under the very first phase that opens a new
// driver after it -- exactly the bug this comment exists to explain, not
// a hypothetical: an earlier version of this file created the index by
// hand and watched openPhaseDriver's own very next AssertSchema call
// silently drop it.
//
// dropObjectIDUpsertIndex (discoverBaseGraph's returned cleanup, deferred
// by execute) still explicitly drops it once this run finishes, success
// or failure alike -- declaring it as "required" here is what keeps
// applybench's OWN phases from ever dropping it prematurely, not a promise
// that it survives forever. If applybench is killed before that deferred
// cleanup can run (a SIGKILL, not a panic -- Go's defer still runs on a
// panic), the index is left behind; the very next AssertSchema call ANY
// tool in this repo makes against this same graph (none of which declare
// this constraint) will drop it as an incidental side effect of its own
// ordinary reconciliation -- a real safety net, not a guarantee -- but if
// a duplicate objectid write needs to succeed before that happens, drop it
// by hand (objectIDConstraintName's own doc gives the exact name to use
// for a given graph id).
//
// See the README's own callout of this same risk.
func benchSchema() graph.Schema {
	return graph.Schema{
		Graphs: []graph.Graph{{Name: graphName, Nodes: stringKinds(nodeKindNames), Edges: stringKinds(edgeKindNames)}},
		DefaultGraph: graph.Graph{
			Name: graphName,
			NodeConstraints: []graph.Constraint{
				{Field: objectIDConstraintField, Type: graph.BTreeIndex},
			},
		},
	}
}

// objectIDConstraintName returns the ACTUAL database name of the unique
// constraint benchSchema declares on graphID's node partition:
// dawgs' own naming convention (drivers/pg/model.ConstraintName, called
// from model.NewGraphPartitionFromSchema during AssertGraph's
// reconciliation) derives it from the partition table name and the
// constraint's Field alone -- "<table>_<field>_constraint" -- and ignores
// whatever a caller puts in graph.Constraint.Name entirely. Calling that
// same dawgs function here, rather than hand-formatting the equivalent
// string, is what keeps this package's drop target correct even if dawgs'
// own naming convention ever changes: an earlier version of this file
// hand-picked a name of its own (graph.Constraint.Name = "applybench_..."),
// which the driver silently ignored, so this package's own DROP INDEX
// cleanup at the end of every run was targeting an index that was never
// actually created under that name -- a real bug this run into, not a
// hypothetical: the constraint dawgs actually created
// ("node_<id>_objectid_constraint") was left behind, undetected, after
// every prior run, and this function is the fix.
func objectIDConstraintName(graphID int32) string {
	return model.ConstraintName(model.NodePartitionTableName(graphID), graph.Constraint{Field: objectIDConstraintField})
}

// dropObjectIDUpsertIndex returns discoverBaseGraph's cleanup function:
// drop objectIDConstraintName(graphID) via a fresh, short-lived connection
// (the pool it was created through, benchSchema's own AssertSchema call,
// is long closed by the time any caller here defers this). See
// benchSchema's own doc for the full rationale.
func dropObjectIDUpsertIndex(dsn string, graphID int32) func() {
	name := objectIDConstraintName(graphID)
	return func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, err := pgx.Connect(dropCtx, dsn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "applybench: WARNING: could not connect to drop %s -- drop it by hand before running other suites against this database: %v\n", name, err)
			return
		}
		defer func() { _ = conn.Close(dropCtx) }()

		if _, err := conn.Exec(dropCtx, "DROP INDEX IF EXISTS "+name); err != nil {
			fmt.Fprintf(os.Stderr, "applybench: WARNING: failed to drop %s -- drop it by hand before running other suites against this database: %v\n", name, err)
			return
		}
		fmt.Printf("applybench: dropped scratch unique constraint %s\n", name)
	}
}

// selectPairs draws count seeded-random (User, Computer) pairs, mirroring
// bench/pathbench's selectPairs -- minus its dense-id resolution, which
// applybench has no use for (see the pair type's own doc).
func selectPairs(ctx context.Context, pool *pgxpool.Pool, graphID int32, userKindID, compKindID int16, count int, seed int64) ([]pair, error) {
	userCount, err := countByKind(ctx, pool, graphID, userKindID)
	if err != nil {
		return nil, fmt.Errorf("count %s nodes: %w", kindUser, err)
	}
	if userCount == 0 {
		return nil, fmt.Errorf("no %s nodes found in graph_id=%d", kindUser, graphID)
	}
	compCount, err := countByKind(ctx, pool, graphID, compKindID)
	if err != nil {
		return nil, fmt.Errorf("count %s nodes: %w", kindComputer, err)
	}
	if compCount == 0 {
		return nil, fmt.Errorf("no %s nodes found in graph_id=%d", kindComputer, graphID)
	}

	rng := rand.New(rand.NewSource(seed))
	pairs := make([]pair, 0, count)
	for i := 0; i < count; i++ {
		userID, userObjectID, err := nodeByKindOffset(ctx, pool, graphID, userKindID, rng.Intn(userCount))
		if err != nil {
			return nil, fmt.Errorf("pick %s: %w", kindUser, err)
		}
		compID, compObjectID, err := nodeByKindOffset(ctx, pool, graphID, compKindID, rng.Intn(compCount))
		if err != nil {
			return nil, fmt.Errorf("pick %s: %w", kindComputer, err)
		}
		pairs = append(pairs, pair{
			startID: graph.ID(userID), endID: graph.ID(compID),
			startObjectID: userObjectID, endObjectID: compObjectID,
		})
	}
	return pairs, nil
}

// countByKind returns the number of graphID's nodes carrying kindID, via
// the node table's GIN index on kind_ids -- duplicated from
// bench/pathbench's identically named helper; see its own doc for exactly
// why the operator must be schema-qualified.
func countByKind(ctx context.Context, pool *pgxpool.Pool, graphID int32, kindID int16) (int, error) {
	var n int
	err := pool.QueryRow(ctx,
		"SELECT count(*) FROM node WHERE graph_id = $1 AND kind_ids operator(pg_catalog.@>) ARRAY[$2]::int2[]",
		graphID, kindID,
	).Scan(&n)
	return n, err
}

// nodeByKindOffset returns the id and objectid of the offset'th node (0
// ordered ascending by id) of graphID carrying kindID -- duplicated from
// bench/pathbench's identically named helper.
func nodeByKindOffset(ctx context.Context, pool *pgxpool.Pool, graphID int32, kindID int16, offset int) (uint64, string, error) {
	var (
		id  int64
		obj string
	)
	err := pool.QueryRow(ctx,
		"SELECT id, properties->>'objectid' FROM node WHERE graph_id = $1 AND kind_ids operator(pg_catalog.@>) ARRAY[$2]::int2[] ORDER BY id OFFSET $3 LIMIT 1",
		graphID, kindID, offset,
	).Scan(&id, &obj)
	return uint64(id), obj, err
}

// waitForBoot sleeps a safety multiple of buildDuration -- see
// bench/builderbench's/bench/cypherbench's identical wait for the full
// rationale: there is no exported hook to observe the bloodtrail driver's
// Start-launched boot-load goroutine actually adopting its first snapshot,
// so this waits a safety multiple of the throwaway build cost
// discoverBaseGraph already measured instead. A 1s floor keeps a
// near-instant build (a tiny smoke-scale graph) from racing it.
func waitForBoot(buildDuration time.Duration) {
	wait := 2*buildDuration + 500*time.Millisecond
	if wait < time.Second {
		wait = time.Second
	}
	time.Sleep(wait)
}

// phaseEnv is one dawgs.Open call's worth of BloodTrail environment
// settings -- see the package doc for why these are process-wide and
// therefore why every phase below opens, uses, and closes its own driver
// sequentially rather than ever running two phases with different phaseEnv
// values concurrently.
type phaseEnv struct {
	engineOn       bool
	compactEntries int
	snapshotDir    string
}

func setPhaseEnv(env phaseEnv) {
	toggle := "off"
	if env.engineOn {
		toggle = "on"
	}
	// Setenv on a fixed, always-valid key can only fail in a way this
	// process could not recover from anyway (an exhausted environment);
	// errors are deliberately discarded rather than propagated, matching
	// driver.go's own convention for a call this narrow.
	_ = os.Setenv(bloodtrail.EnvEngine, toggle)
	_ = os.Setenv(bloodtrail.EnvCompactEntries, strconv.Itoa(env.compactEntries))
	_ = os.Setenv(bloodtrail.EnvCompactBytes, "0")
	_ = os.Setenv(bloodtrail.EnvSnapshotDir, env.snapshotDir)
}

// openPhaseDriver sets env (see phaseEnv's own doc) and opens a fresh
// production bloodtrail driver on its own dedicated *pgxpool.Pool. It calls
// AssertSchema on the freshly opened driver, exactly as
// bench/cypherbench's and bench/builderbench's own bt/oracle opens do --
// each *pg.Driver-backed instance resolves its own default-graph/kind-map
// state at construction time, so a fresh instance connecting to a database
// another driver already schema-asserted still needs its own
// AssertSchema call before DefaultGraph()/KindMapper() (and therefore
// every read/write this package issues against it) resolve at all.
//
// The returned cleanup calls Close on a fresh context.Background(), never
// the ctx this phase's own watchdog wraps -- Close's own
// Stop-then-SaveSnapshot sequence (driver.go) must not be cut off by a
// caller's timeout that was budgeting the WRITE this phase just finished,
// not the shutdown that follows it.
func openPhaseDriver(ctx context.Context, dsn string, env phaseEnv) (graph.Database, func(), error) {
	setPhaseEnv(env)

	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pg.NewPool(poolCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("new pool: %w", err)
	}

	db, err := dawgs.Open(ctx, bloodtrail.DriverName, dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool})
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("open bloodtrail driver: %w", err)
	}

	// benchSchema, not a bare inline literal: every phase's AssertSchema
	// call must declare the SAME required NodeConstraints (the
	// objectid-upsert unique index discoverBaseGraph's own AssertSchema
	// call created), or dawgs' own schema reconciliation would drop it the
	// moment this call runs -- see benchSchema's own doc.
	if err := db.AssertSchema(ctx, benchSchema()); err != nil {
		_ = db.Close(context.Background())
		return nil, nil, fmt.Errorf("assert schema: %w", err)
	}

	cleanup := func() { _ = db.Close(context.Background()) }
	return db, cleanup, nil
}

// ============================================================================
// (a)+(b): sustained apply throughput, and query latency during active ingest
// ============================================================================

// measureThroughputAndLatency runs cfg.repeats engine-on ingest repeats
// (each with continuous concurrent query sampling, feeding both (a)'s
// onDurations and (b)'s duringDurations), cfg.repeats engine-off ingest
// repeats (feeding (a)'s offDurations), and one idle-baseline sample of
// cfg.queries queries with no concurrent writes at all (feeding (b)'s
// idleDurations) -- filling in result in place.
func measureThroughputAndLatency(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	onEnv := phaseEnv{engineOn: true, compactEntries: 1 << 30} // effectively unbounded: (a)/(b) must not see a mid-run compaction (see the package doc; (c) measures that separately with its own low threshold).
	offEnv := phaseEnv{engineOn: false}

	fmt.Println("applybench: idle latency baseline (engine on, no concurrent writes) ...")
	idleDB, idleCleanup, err := openPhaseDriver(ctx, cfg.dsn, onEnv)
	if err != nil {
		return fmt.Errorf("open idle-baseline driver: %w", err)
	}
	waitForBoot(base.buildDuration)
	idleDurations, err := sampleQueryLatenciesCapped(ctx, idleDB, base.pairs, cfg.queries, cfg.cap)
	idleCleanup()
	if err != nil {
		return fmt.Errorf("idle-baseline queries: %w", err)
	}
	result.idleDurations = idleDurations
	idleP50, idleP95 := percentile(idleDurations, 0.50), percentile(idleDurations, 0.95)
	fmt.Printf("applybench: idle baseline: n=%d p50=%s p95=%s\n", len(idleDurations), fmtMillis(idleP50), fmtMillis(idleP95))

	for i := 0; i < cfg.repeats; i++ {
		fmt.Printf("applybench: engine-on repeat %d/%d (ingest %d units, concurrent query sampling) ...\n", i+1, cfg.repeats, cfg.ingest)
		db, cleanup, err := openPhaseDriver(ctx, cfg.dsn, onEnv)
		if err != nil {
			return fmt.Errorf("open engine-on driver (repeat %d): %w", i, err)
		}
		waitForBoot(base.buildDuration)

		onDur, during, err := runIngestWithConcurrentQueries(ctx, db, base, cfg, "on", i)
		cleanup()
		if err != nil {
			return fmt.Errorf("engine-on ingest (repeat %d): %w", i, err)
		}
		result.onDurations = append(result.onDurations, onDur)
		result.duringDurations = append(result.duringDurations, during...)
		fmt.Printf("applybench: engine-on repeat %d: write_wall_time=%s during_ingest_queries=%d\n", i+1, fmtSeconds(onDur), len(during))
	}

	for i := 0; i < cfg.repeats; i++ {
		fmt.Printf("applybench: engine-off repeat %d/%d (ingest %d units) ...\n", i+1, cfg.repeats, cfg.ingest)
		db, cleanup, err := openPhaseDriver(ctx, cfg.dsn, offEnv)
		if err != nil {
			return fmt.Errorf("open engine-off driver (repeat %d): %w", i, err)
		}
		// No waitForBoot: Start is a no-op when the engine is disabled
		// (internal/engine/boot.go's Start), so there is no boot-load
		// goroutine to wait for at all.

		offDur, err := runIngestCapped(ctx, db, base.hubObjectID, cfg.ingest, cfg.flush, "off", i, cfg.cap)
		cleanup()
		if err != nil {
			return fmt.Errorf("engine-off ingest (repeat %d): %w", i, err)
		}
		result.offDurations = append(result.offDurations, offDur)
		fmt.Printf("applybench: engine-off repeat %d: write_wall_time=%s\n", i+1, fmtSeconds(offDur))
	}

	return nil
}

// runIngestWithConcurrentQueries runs one ingest repeat on its own goroutine
// while the calling goroutine samples pair queries against the very same db
// continuously, for as long as ingest takes -- see the package doc's (b) for
// why continuous sampling, not a fixed -queries count, is what "concurrently
// with (a)'s write load" means here. Returns the ingest wall time and every
// sampled query's duration.
func runIngestWithConcurrentQueries(ctx context.Context, db graph.Database, base *baseGraph, cfg config, tag string, repeat int) (time.Duration, []time.Duration, error) {
	capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
	defer cancel()

	done := make(chan struct{})
	var (
		ingestDur time.Duration
		ingestErr error
	)
	go func() {
		defer close(done)
		ingestDur, ingestErr = writeIngestBatch(capCtx, db, base.hubObjectID, cfg.ingest, cfg.flush, tag, repeat)
	}()

	rng := rand.New(rand.NewSource(cfg.seed + int64(repeat) + 1))
	var (
		durations []time.Duration
		queryErr  error
	)
sampleLoop:
	for {
		select {
		case <-done:
			break sampleLoop
		default:
		}
		p := base.pairs[rng.Intn(len(base.pairs))]
		d, err := runPairQuery(capCtx, db, p.startID, p.endID)
		if err != nil {
			queryErr = err
			break sampleLoop
		}
		durations = append(durations, d)
	}
	<-done

	if ingestErr != nil {
		if errors.Is(capCtx.Err(), context.DeadlineExceeded) {
			return 0, nil, fmt.Errorf("engine-on ingest exceeded -cap=%s: %w", cfg.cap, ingestErr)
		}
		return 0, nil, ingestErr
	}
	if queryErr != nil && !errors.Is(queryErr, context.Canceled) {
		return 0, nil, fmt.Errorf("concurrent query sampling: %w", queryErr)
	}
	return ingestDur, durations, nil
}

// runIngestCapped runs writeIngestBatch bounded to cap via
// context.WithTimeout wrapped directly around the call -- see the package
// doc's "Watchdog" section: exceeding it always aborts the whole run.
func runIngestCapped(ctx context.Context, db graph.Database, hubObjectID string, count, flushSize int, tag string, repeat int, cap time.Duration) (time.Duration, error) {
	capCtx, cancel := context.WithTimeout(ctx, cap)
	defer cancel()

	d, err := writeIngestBatch(capCtx, db, hubObjectID, count, flushSize, tag, repeat)
	if err != nil && errors.Is(capCtx.Err(), context.DeadlineExceeded) {
		return 0, fmt.Errorf("ingest exceeded -cap=%s: %w", cap, err)
	}
	return d, err
}

// writeIngestBatch performs one repeat's worth of ingest-shaped writes: for
// i in [0, count), a new User node upserted by objectid
// (batch.UpdateNodeBy) and a new MemberOf edge from that user to
// hubObjectID upserted by both endpoints' objectid (batch.
// UpdateRelationshipBy) -- exactly the shape write_observer.go's
// recordNodeUpsertIdentity/recordRelationshipUpsertIdentity recognize (a
// bare "objectid" identity), matching production's own graphify ingestion
// path (cmd/api/src/services/graphify/ingestnodes.go/ingestrelationships.go
// in the upstream BloodHound source this driver plugs into). Every
// flushSize operations, an explicit batch.Commit() flushes the buffer and
// -- via observingBatch.Commit, write_observer.go -- runs write-through
// Apply for everything flushed so far, exactly the production write-flush
// cadence this package's -flush flag matches. objectids are namespaced by
// tag and repeat so concurrent/sequential phases and repeats never collide.
// Returns the whole BatchOperation call's wall-clock time.
func writeIngestBatch(ctx context.Context, db graph.Database, hubObjectID string, count, flushSize int, tag string, repeat int) (time.Duration, error) {
	baseKind := graph.StringKind(kindBase)
	userKind := graph.StringKind(kindUser)
	groupKind := graph.StringKind(kindGroup)
	memberOfKind := graph.StringKind(edgeMemberOf)

	t0 := time.Now()
	err := db.BatchOperation(ctx, func(batch graph.Batch) error {
		ops := 0
		for i := 0; i < count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}

			userObjectID := fmt.Sprintf("APPLYBENCH-%s-%d-%08d", tag, repeat, i)
			userNode := graph.PrepareNode(graph.AsProperties(map[string]any{"objectid": userObjectID, "name": userObjectID}), baseKind, userKind)
			if err := batch.UpdateNodeBy(graph.NodeUpdate{
				Node:               userNode,
				IdentityKind:       baseKind,
				IdentityProperties: []string{"objectid"},
			}); err != nil {
				return fmt.Errorf("UpdateNodeBy user %d: %w", i, err)
			}
			ops++

			startNode := graph.PrepareNode(graph.AsProperties(map[string]any{"objectid": userObjectID}), baseKind, userKind)
			endNode := graph.PrepareNode(graph.AsProperties(map[string]any{"objectid": hubObjectID}), baseKind, groupKind)
			if err := batch.UpdateRelationshipBy(graph.RelationshipUpdate{
				Relationship:            graph.PrepareRelationship(graph.NewProperties(), memberOfKind),
				Start:                   startNode,
				StartIdentityKind:       baseKind,
				StartIdentityProperties: []string{"objectid"},
				End:                     endNode,
				EndIdentityKind:         baseKind,
				EndIdentityProperties:   []string{"objectid"},
			}); err != nil {
				return fmt.Errorf("UpdateRelationshipBy edge %d: %w", i, err)
			}
			ops++

			if ops >= flushSize {
				if err := batch.Commit(); err != nil {
					return fmt.Errorf("flush commit at i=%d: %w", i, err)
				}
				ops = 0
			}
			if (i+1)%progressEvery == 0 {
				fmt.Fprintf(os.Stderr, "applybench: ... %s repeat %d: %d/%d ingest units\n", tag, repeat, i+1, count)
			}
		}
		return nil
	})
	return time.Since(t0), err
}

// sampleQueryLatenciesCapped samples n pair queries against db, bounded to
// cap via context.WithTimeout wrapped around the whole sampling loop.
func sampleQueryLatenciesCapped(ctx context.Context, db graph.Database, pairs []pair, n int, cap time.Duration) ([]time.Duration, error) {
	capCtx, cancel := context.WithTimeout(ctx, cap)
	defer cancel()

	rng := rand.New(rand.NewSource(1))
	durations := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		p := pairs[rng.Intn(len(pairs))]
		d, err := runPairQuery(capCtx, db, p.startID, p.endID)
		if err != nil {
			if errors.Is(capCtx.Err(), context.DeadlineExceeded) {
				return durations, fmt.Errorf("query sampling exceeded -cap=%s: %w", cap, err)
			}
			return durations, err
		}
		durations = append(durations, d)
	}
	return durations, nil
}

// runPairQuery runs the same graph.Criteria shape BloodHound's own
// FetchAllShortestPaths API builds -- query.And(query.Equals(StartID),
// query.Equals(EndID)) -- through db's RelationshipQuery, the exact path
// engine_serving_integration_test.go's shortestPathsViaCriteria exercises
// (recordingRelationshipQuery.FetchAllShortestPaths, reached through
// Engine.TryAllShortestPaths when the engine can serve it). Fully drains
// the returned cursor so the timed duration reflects the whole query, not
// just a first-row latency; whether it finds any path at all is irrelevant
// to a latency sample.
func runPairQuery(ctx context.Context, db graph.Database, startID, endID graph.ID) (time.Duration, error) {
	t0 := time.Now()
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		criteria := query.And(
			query.Equals(query.StartID(), startID),
			query.Equals(query.EndID(), endID),
		)
		return tx.Relationships().Filter(criteria).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
			for range cursor.Chan() {
			}
			return cursor.Error()
		})
	})
	return time.Since(t0), err
}

// ============================================================================
// (c): compaction duration at scale
// ============================================================================

// measureCompaction opens a driver with BLOODTRAIL_COMPACT_ENTRIES set to
// cfg.compactEntries (deliberately low), captures the engine's own logging
// via logCapture (see its doc for why: CompactionCount is unreachable from
// outside package bloodtrail through the real driver), drives enough
// ingest-shaped writes -- chunked so each flush's delta comfortably exceeds
// the threshold -- to trigger at least cfg.repeats background compactions,
// then waits (watchdog-capped, polling with progress) for that many
// "bloodtrail: compaction finished" lines to have been logged, reading
// each one's own engine-measured duration attribute straight off the log
// record.
func measureCompaction(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	capture := newLogCapture()
	previous := slog.Default()
	slog.SetDefault(slog.New(capture))
	defer slog.SetDefault(previous)

	capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
	defer cancel()

	db, cleanup, err := openPhaseDriver(capCtx, cfg.dsn, phaseEnv{engineOn: true, compactEntries: cfg.compactEntries})
	if err != nil {
		return fmt.Errorf("open compaction-forcing driver: %w", err)
	}
	defer cleanup()
	waitForBoot(base.buildDuration)

	// Each flush's own delta should comfortably exceed compactEntries (1
	// ingest unit = 2 entries: 1 node + 1 edge), and enough flushes should
	// run to observe several triggers even if some overlap while a prior
	// compaction is still folding (maybeStartCompaction, compact.go, skips
	// re-triggering while one is already running).
	unitsPerFlush := cfg.compactEntries/2 + 1
	totalUnits := unitsPerFlush * (cfg.repeats*3 + 1)
	fmt.Printf("applybench: forcing compaction: compact_entries=%d units_per_flush=%d total_units=%d\n", cfg.compactEntries, unitsPerFlush, totalUnits)
	if _, err := writeIngestBatch(capCtx, db, base.hubObjectID, totalUnits, unitsPerFlush, "compact", 0); err != nil {
		if errors.Is(capCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("compaction-forcing ingest exceeded -cap=%s: %w", cfg.cap, err)
		}
		return fmt.Errorf("compaction-forcing ingest: %w", err)
	}

	target := cfg.repeats
	for {
		n := capture.count(msgCompactionFinished)
		if n >= target {
			break
		}
		if capCtx.Err() != nil {
			return fmt.Errorf("timed out waiting for %d compactions (-cap=%s); observed %d so far", target, cfg.cap, n)
		}
		fmt.Printf("applybench: ... waiting for compaction %d/%d\n", n, target)
		select {
		case <-time.After(2 * time.Second):
		case <-capCtx.Done():
		}
	}

	result.compactionDurations = capture.durations(msgCompactionFinished)
	result.compactionCount = capture.count(msgCompactionFinished)
	fmt.Printf("applybench: observed %d adopted compactions\n", result.compactionCount)
	return nil
}

// ============================================================================
// (d): snapshot file write and load duration at scale
// ============================================================================

// measureSnapshotIO runs cfg.repeats save/load cycles against one scratch
// directory. Each cycle:
//
//  1. Opens a real production driver with BLOODTRAIL_SNAPSHOT_DIR set,
//     waits for boot, and times Close() (Stop then the engine's own
//     SaveSnapshot -- driver.go) end to end. This is the real write path,
//     unavoidably exercised through the real driver (see the package doc).
//  2. Reads the file back directly via internal/engine/snapshot.
//     ReadSnapshotFile -- the exact function boot.go's own
//     tryLoadSnapshotFile calls -- timed directly, mirroring how
//     bench/builderbench/bench/cypherbench measure "build duration" via a
//     direct engine.LoadSnapshot call rather than trying to observe the
//     driver's own boot goroutine adopting.
//
// The load half deliberately does NOT open a fresh driver and wait for its
// boot-load goroutine to adopt the file the way an earlier version of this
// function did: Start (boot.go) launches that goroutine, and its OWN first
// (and only -- tryLoadSnapshotFile has no retry schedule of its own)
// attempt to resolve e.pgDriver.DefaultGraph() races this package's own
// AssertSchema call, made from a separate goroutine only after dawgs.Open
// itself returns -- a race the boot-load goroutine, which has no I/O of
// its own to do first, wins essentially every time in practice (observed:
// every single driver open in this package's other phases logs exactly
// one "bloodtrail: boot load failed: no default graph is set" before
// succeeding on retry). For the ordinary pg-rebuild path that retry is
// harmless -- runBootLoad's own loop retries it a moment later -- but
// tryLoadSnapshotFile gets no such second chance, so a fresh driver open
// essentially never actually exercises the file-load path at all, and
// waiting for its log line hangs until -cap's own watchdog trips (this is
// exactly what an earlier version of this function did, confirmed by
// running it). Reading the file directly sidesteps that race entirely
// while still measuring the one operation that actually scales with file
// size: parsing it.
func measureSnapshotIO(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	dir, err := os.MkdirTemp("", "applybench-snapshot-*")
	if err != nil {
		return fmt.Errorf("create scratch snapshot dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	for i := 0; i < cfg.repeats; i++ {
		capture := newLogCapture()
		previous := slog.Default()
		slog.SetDefault(slog.New(capture))

		saveDur, err := func() (time.Duration, error) {
			capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
			defer cancel()

			db, cleanup, err := openPhaseDriver(capCtx, cfg.dsn, phaseEnv{engineOn: true, compactEntries: 1 << 30, snapshotDir: dir})
			if err != nil {
				return 0, fmt.Errorf("open save driver (repeat %d): %w", i, err)
			}
			waitForBoot(base.buildDuration)

			// A tiny write gives every repeat's file a fresh, distinguishable
			// watermark; small enough not to matter to the timing.
			if _, err := writeIngestBatch(capCtx, db, base.hubObjectID, 10, 10, "snapshot", i); err != nil {
				return 0, fmt.Errorf("pre-save ingest (repeat %d): %w", i, err)
			}

			t0 := time.Now()
			cleanup()
			d := time.Since(t0)
			if capture.count(msgSnapshotWritten) == 0 {
				return 0, fmt.Errorf("repeat %d: Close did not log %q -- SaveSnapshot's own preconditions were not met (see internal/engine/persist.go)", i, msgSnapshotWritten)
			}
			return d, nil
		}()
		slog.SetDefault(previous)
		if err != nil {
			return err
		}
		result.saveDurations = append(result.saveDurations, saveDur)
		fmt.Printf("applybench: repeat %d: save (Close) duration=%s\n", i+1, fmtMillis(saveDur))

		path := filepath.Join(dir, fmt.Sprintf("graph-%d.btsnap", base.graphID))
		t0 := time.Now()
		snap, watermark, err := snapshot.ReadSnapshotFile(path)
		loadDur := time.Since(t0)
		if err != nil {
			return fmt.Errorf("repeat %d: read snapshot file %s: %w", i, path, err)
		}
		result.loadDurations = append(result.loadDurations, loadDur)
		fmt.Printf("applybench: repeat %d: load (ReadSnapshotFile) duration=%s nodes=%d edges=%d watermark=%d\n",
			i+1, fmtMillis(loadDur), snap.NodeCount(), snap.EdgeCount(), watermark)
	}

	return nil
}

// ============================================================================
// logCapture
// ============================================================================

// logCapture is a slog.Handler that records every log record's message,
// Time, and (if present) a "duration" attribute -- installed as
// slog.Default() for exactly as long as a measurement needs it (measureCompaction,
// measureSnapshotIO), then restored.
//
// This exists because internal/engine.Engine's own test-observability
// counters (CompactionCount, ApplyCount, RebuildCount) are methods on a
// type the root package's Driver embeds as an UNEXPORTED field (driver.go's
// `engine *engine.Engine`) -- reachable from white-box tests inside package
// bloodtrail itself (see e.g. apply_integration_test.go's own doc for why
// those tests live in that package rather than internal/engine or a fully
// external one), but not from applybench, which is a separate `main`
// package outside bloodtrail entirely. What IS reachable from outside is
// exactly what a real operator would have in production too: the engine's
// own structured log lines (internal/engine/compact.go's runCompaction,
// persist.go's saveSnapshotCommit, boot.go's tryLoadSnapshotFile), several
// of which already carry the exact engine-measured duration this package
// wants as a slog.Duration attribute. Matching on the literal message
// string is the same technique this repo's own test helpers
// (installLogCapture, engine_serving_integration_test.go and siblings) use
// to observe engine behavior from outside the engine package; the message
// literals themselves are duplicated as constants above, guarded by
// TestLogMessagesMatchEngineSource (main_test.go) against silent drift.
type logCapture struct {
	mu     sync.Mutex
	events map[string][]logEvent
}

type logEvent struct {
	at       time.Time
	duration time.Duration
	hasDur   bool
}

func newLogCapture() *logCapture {
	return &logCapture{events: make(map[string][]logEvent)}
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	ev := logEvent{at: r.Time}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "duration" && a.Value.Kind() == slog.KindDuration {
			ev.duration = a.Value.Duration()
			ev.hasDur = true
		}
		return true
	})
	c.mu.Lock()
	c.events[r.Message] = append(c.events[r.Message], ev)
	c.mu.Unlock()
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

// count returns how many records matching message have been captured.
func (c *logCapture) count(message string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events[message])
}

// durations returns the "duration" attribute of every captured record
// matching message that carried one, in capture order.
func (c *logCapture) durations(message string) []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, 0, len(c.events[message]))
	for _, e := range c.events[message] {
		if e.hasDur {
			out = append(out, e.duration)
		}
	}
	return out
}

// latest returns the most recently captured record matching message, if
// any.
func (c *logCapture) latest(message string) (logEvent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	evs := c.events[message]
	if len(evs) == 0 {
		return logEvent{}, false
	}
	return evs[len(evs)-1], true
}

// ============================================================================
// report
// ============================================================================

// report prints every measurement's summary and the final PASS/FAIL line,
// returning whether both -enforce bars ((a)'s overhead, (b)'s during-ingest
// p95) were met, independent of whether -enforce was actually passed (run
// decides whether that return value changes the exit code). (c) and (d) are
// printed but never scored -- see the package doc for why.
func (r *benchResult) report(enforce bool) bool {
	onP50 := percentile(r.onDurations, 0.50)
	offP50 := percentile(r.offDurations, 0.50)
	overheadPct := overheadPercent(onP50, offP50)

	idleP50, idleP95 := percentile(r.idleDurations, 0.50), percentile(r.idleDurations, 0.95)
	duringP50, duringP95 := percentile(r.duringDurations, 0.50), percentile(r.duringDurations, 0.95)

	overheadOK := overheadPct <= applyOverheadMaxPct
	latencyOK := latencyWithinMultiplier(duringP95, idleP95, idleP95Multiplier)

	fmt.Printf("\n=== (a) sustained apply throughput ===\n")
	fmt.Printf("applybench: engine-on  write wall time p50=%s (n=%d): %s\n", fmtSeconds(onP50), len(r.onDurations), fmtDurationsSeconds(r.onDurations))
	fmt.Printf("applybench: engine-off write wall time p50=%s (n=%d): %s\n", fmtSeconds(offP50), len(r.offDurations), fmtDurationsSeconds(r.offDurations))
	fmt.Printf("applybench: apply+read-back overhead = %.2f%% (max %.2f%%, PROVISIONAL -- see README): %s\n", overheadPct, applyOverheadMaxPct, passFail(overheadOK))
	fmt.Printf("APPLYBENCH_APPLY_THROUGHPUT on_p50_ms=%.3f off_p50_ms=%.3f overhead_pct=%.3f max_pct=%.3f ok=%t\n",
		floatMillis(onP50), floatMillis(offP50), overheadPct, applyOverheadMaxPct, overheadOK)

	fmt.Printf("\n=== (b) query latency during active ingest ===\n")
	fmt.Printf("applybench: idle    p50=%s p95=%s (n=%d)\n", fmtMillis(idleP50), fmtMillis(idleP95), len(r.idleDurations))
	fmt.Printf("applybench: ingest  p50=%s p95=%s (n=%d)\n", fmtMillis(duringP50), fmtMillis(duringP95), len(r.duringDurations))
	fmt.Printf("applybench: during-ingest p95 within %.1fx idle p95 (PROVISIONAL -- see README): %s\n", idleP95Multiplier, passFail(latencyOK))
	fmt.Printf("APPLYBENCH_LATENCY_DURING_INGEST idle_p50_ms=%.3f idle_p95_ms=%.3f during_p50_ms=%.3f during_p95_ms=%.3f multiplier=%.3f ok=%t\n",
		floatMillis(idleP50), floatMillis(idleP95), floatMillis(duringP50), floatMillis(duringP95), idleP95Multiplier, latencyOK)

	compactionP50 := percentile(r.compactionDurations, 0.50)
	fmt.Printf("\n=== (c) compaction duration (reported only, not enforced) ===\n")
	fmt.Printf("applybench: compactions observed=%d p50=%s: %s\n", r.compactionCount, fmtMillis(compactionP50), fmtDurationsMs(r.compactionDurations))
	fmt.Printf("APPLYBENCH_COMPACTION count=%d p50_ms=%.3f\n", r.compactionCount, floatMillis(compactionP50))

	saveP50, loadP50 := percentile(r.saveDurations, 0.50), percentile(r.loadDurations, 0.50)
	fmt.Printf("\n=== (d) snapshot file write/load duration (reported only, not enforced) ===\n")
	fmt.Printf("applybench: save p50=%s: %s\n", fmtMillis(saveP50), fmtDurationsMs(r.saveDurations))
	fmt.Printf("applybench: load p50=%s: %s\n", fmtMillis(loadP50), fmtDurationsMs(r.loadDurations))
	fmt.Printf("APPLYBENCH_SNAPSHOT_IO save_p50_ms=%.3f load_p50_ms=%.3f\n", floatMillis(saveP50), floatMillis(loadP50))

	allOK := overheadOK && latencyOK
	fmt.Printf("\nAPPLYBENCH_ENFORCE checked=%t ok=%t\n", enforce, allOK)
	if allOK {
		fmt.Println("APPLYBENCH_RESULT PASS")
	} else {
		fmt.Println("APPLYBENCH_RESULT FAIL")
	}
	return allOK
}

// overheadPercent is (a)'s pure decision input: the percentage by which
// onP50 exceeds offP50. A zero or negative offP50 (never observed in
// practice, but guarded rather than divided by) reports 0 when onP50 is
// also non-positive, or +Inf otherwise -- an engine-off baseline of zero
// with any positive engine-on cost is an undefined ratio, not a passing
// one.
func overheadPercent(onP50, offP50 time.Duration) float64 {
	if offP50 <= 0 {
		if onP50 <= 0 {
			return 0
		}
		return math.Inf(1)
	}
	return float64(onP50-offP50) / float64(offP50) * 100
}

// latencyWithinMultiplier is (b)'s pure decision input: whether duringP95
// is at most multiplier times idleP95. An idleP95 of zero (a degenerate
// idle sample, e.g. a near-instant smoke-scale query) reports true
// unconditionally -- there is nothing meaningful to bound duringP95
// against, and failing a smoke-scale run on that technicality would be
// exactly the kind of false failure the m4.5 measured-physics convention
// this package's caps are meant to eventually replace already warns
// against.
func latencyWithinMultiplier(duringP95, idleP95 time.Duration, multiplier float64) bool {
	if idleP95 <= 0 {
		return true
	}
	return float64(duringP95) <= float64(idleP95)*multiplier
}

// ============================================================================
// small helpers
// ============================================================================

func stringKinds(names []string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// percentile returns durations' p-th percentile (nearest-rank method, 0 <
// p <= 1), sorting a copy so the caller's slice order is undisturbed. An
// empty input returns zero. Duplicated from the sibling benches -- see the
// package doc's "self-contained" convention.
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

func fmtDurationsMs(durations []time.Duration) string {
	parts := make([]string, len(durations))
	for i, d := range durations {
		parts[i] = fmt.Sprintf("%.2f", floatMillis(d))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func fmtDurationsSeconds(durations []time.Duration) string {
	parts := make([]string, len(durations))
	for i, d := range durations {
		parts[i] = fmt.Sprintf("%.3f", d.Seconds())
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// init silences the bloodtrail driver's own default logging, matching
// bench/builderbench's/bench/cypherbench's identical init -- see either's
// doc for why this must run before main. measureCompaction and
// measureSnapshotIO temporarily swap slog.Default() again, on top of this,
// for exactly as long as they need logCapture installed, then restore
// whatever was current (this handler, for either of those methods being the
// first to run) before returning.
func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}
