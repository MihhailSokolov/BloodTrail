// SPDX-License-Identifier: Apache-2.0

// Command pathbench benchmarks the in-memory path engine (internal/engine)
// against a graph already loaded into PostgreSQL, normally by bench/adgen
// (see bench/adgen/README.md).
//
// Usage:
//
//	go run ./bench/pathbench -dsn <dsn> [-runs 20] [-seed 1] [-enforce] [-cpuprofile <file>]
//
// pathbench never imports bench/adgen (a generator, not a library) and
// never writes to the database: it opens the pg driver + pool exactly the
// way bench/adgen does (same schema/default-graph assertion, so the
// "bloodtrail_test" default graph resolves the same way), then finds its
// benchmark inputs purely through SQL against the node table's kind_ids and
// properties->>'objectid' columns -- the same conventions bench/adgen's
// generated graphs follow, without depending on its Go types.
//
// It runs, in order:
//
//   - a snapshot build (engine.LoadSnapshot), reporting nodes/edges/bytes;
//   - (a) -runs seeded-random User->Computer pairs, each a pure
//     traverse.AllShortestPaths (ModeAll) call over every edge kind
//     bench/adgen emits, reporting p50/p95 latency;
//   - (a, hydrated) the same pairs served through the real production path
//     (engine.Engine.TryAllShortestPaths, which additionally fetches full
//     node/edge properties from PostgreSQL), to show the hydration cost on
//     top of (a)'s pure in-memory latency;
//   - (b) "shortest paths to Domain Admins": every -512 group as an
//     unconstrained-root, ModeOne, Limit-1000 traverse.AllShortestPaths
//     call, reporting total wall time;
//   - (c) a second, separately timed engine.LoadSnapshot call, reporting
//     rebuild time.
//
// Every section prints a human-readable line and a machine-greppable
// PATHBENCH_* summary line (grep '^PATHBENCH_'). With -enforce, pathbench
// exits nonzero if (a)'s p95 exceeds pairP95Threshold or (b)'s total time
// exceeds domainAdminsThreshold; CI must never pass -enforce, since these
// thresholds are only meaningful against a realistically sized graph (see
// the task-16 brief). Any other failure (a database error, an empty graph,
// an engine decline that should not happen for a single-ID pair query)
// aborts the run with a nonzero exit regardless of -enforce.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"runtime/pprof"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/traverse"
)

// graphName matches bench/adgen's graphName and internal/graphtest.GraphName:
// the "bloodtrail_test" graph the rest of the project resolves as its
// default graph. Duplicated here (not imported) for the same reason the
// kind names below are -- see the package doc.
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

// Enforcement thresholds from the task-16 brief.
const (
	pairP95Threshold       = 100 * time.Millisecond
	domainAdminsThreshold  = 5 * time.Second
	domainAdminsPathsLimit = 1000
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// run parses flags, executes the benchmark, prints its report, and returns
// the process exit code. Kept separate from main so cpuprofile's defers
// (StopCPUProfile) always run before the process exits: os.Exit in main
// never runs deferred calls, so every defer that must fire lives in run's
// frame, not main's.
func run(args []string) int {
	fs := flag.NewFlagSet("pathbench", flag.ContinueOnError)
	var (
		dsn        = fs.String("dsn", "", "PostgreSQL connection string, e.g. postgresql://user:pass@host:port/db")
		runs       = fs.Int("runs", 20, "number of seeded-random User->Computer pairs to sample for section (a)")
		seed       = fs.Int64("seed", 1, "seed for deterministic pair selection (same -runs/-seed reproduces the same pairs)")
		enforce    = fs.Bool("enforce", false, "exit nonzero if pairs p95 > 100ms or the Domain Admins query > 5s (never pass this in CI)")
		cpuprofile = fs.String("cpuprofile", "", "write a pprof CPU profile to this file")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "pathbench: -dsn is required")
		return 2
	}
	if *runs <= 0 {
		fmt.Fprintln(os.Stderr, "pathbench: -runs must be positive")
		return 2
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pathbench: create cpuprofile: %v\n", err)
			return 1
		}
		defer func() {
			if cerr := f.Close(); cerr != nil {
				fmt.Fprintf(os.Stderr, "pathbench: close cpuprofile: %v\n", cerr)
			}
		}()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "pathbench: start cpuprofile: %v\n", err)
			return 1
		}
		defer pprof.StopCPUProfile()
	}

	result, err := execute(context.Background(), config{dsn: *dsn, runs: *runs, seed: *seed})
	if err != nil {
		fmt.Fprintf(os.Stderr, "pathbench: %v\n", err)
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
	seed int64
}

// snapshotStats is engine.LoadSnapshot's result, timed.
type snapshotStats struct {
	duration    time.Duration
	nodes       int
	edges       int
	approxBytes uint64
}

// pair is one seeded-random (User, Computer) sample: the same node
// identified both ways traverse.AllShortestPaths and
// engine.TryAllShortestPaths each need it (dense snapshot.NodeID for the
// former, database graph.ID for the latter).
type pair struct {
	rootDB, termDB             uint64
	rootDense, termDense       snapshot.NodeID
	rootObjectID, termObjectID string
}

// benchResult accumulates every section's measurements for report to print.
type benchResult struct {
	build   snapshotStats
	rebuild snapshotStats

	pairs         []pair
	pairDurations []time.Duration

	hydratedDurations []time.Duration

	domainAdminsGroups   int
	domainAdminsPaths    int
	domainAdminsDuration time.Duration
}

// execute runs every benchmark section against cfg.dsn and returns the
// collected measurements. Any error here is a setup/infrastructure failure
// (bad DSN, empty graph, an engine decline that should never happen for a
// single-ID pair query) and always aborts the run with a nonzero exit,
// regardless of -enforce -- -enforce only gates the two named latency
// thresholds inside benchResult.report.
func execute(ctx context.Context, cfg config) (*benchResult, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pg.NewPool(poolCfg)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	defer pool.Close()

	driver := pg.NewDriver(size.Gibibyte, pool)

	schema := graph.Schema{
		Graphs:       []graph.Graph{{Name: graphName, Nodes: stringKinds(nodeKindNames), Edges: stringKinds(edgeKindNames)}},
		DefaultGraph: graph.Graph{Name: graphName},
	}
	if err := driver.AssertSchema(ctx, schema); err != nil {
		return nil, fmt.Errorf("assert schema: %w", err)
	}

	graphModel, ok := driver.DefaultGraph()
	if !ok {
		return nil, fmt.Errorf("no default graph resolved after AssertSchema")
	}

	allKinds := append(stringKinds(nodeKindNames), stringKinds(edgeKindNames)...)
	if _, err := driver.KindMapper().AssertKinds(ctx, allKinds); err != nil {
		return nil, fmt.Errorf("assert kinds: %w", err)
	}
	kindMapper := driver.KindMapper()

	userKindID, err := kindMapper.MapKind(ctx, graph.StringKind(kindUser))
	if err != nil {
		return nil, fmt.Errorf("map kind %q: %w", kindUser, err)
	}
	compKindID, err := kindMapper.MapKind(ctx, graph.StringKind(kindComputer))
	if err != nil {
		return nil, fmt.Errorf("map kind %q: %w", kindComputer, err)
	}

	result := &benchResult{}

	fmt.Println("pathbench: loading initial snapshot ...")
	buildStart := time.Now()
	snap, err := engine.LoadSnapshot(ctx, driver, pool)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	result.build = snapshotStats{duration: time.Since(buildStart), nodes: snap.NodeCount(), edges: snap.EdgeCount(), approxBytes: snap.ApproxBytes()}
	printSnapshotStats("build", result.build)

	kindMask, err := buildKindMask(ctx, kindMapper, snap.MaxKindID, edgeKindNames)
	if err != nil {
		return nil, fmt.Errorf("build kind mask: %w", err)
	}

	pairs, err := selectPairs(ctx, pool, graphModel.ID, userKindID, compKindID, snap, cfg.runs, cfg.seed)
	if err != nil {
		return nil, fmt.Errorf("select pairs: %w", err)
	}
	result.pairs = pairs

	view := snapshot.NewView(snap)

	fmt.Printf("\n=== (a) %d random User->Computer pairs (seed=%d, mode=all) ===\n", cfg.runs, cfg.seed)
	for i, p := range pairs {
		q := traverse.Query{
			Roots:     traverse.Endpoint{IDs: []snapshot.NodeID{p.rootDense}},
			Terminals: traverse.Endpoint{IDs: []snapshot.NodeID{p.termDense}},
			Kinds:     kindMask,
			Mode:      traverse.ModeAll,
		}
		t0 := time.Now()
		paths, err := traverse.AllShortestPaths(view, q)
		d := time.Since(t0)
		if err != nil {
			return nil, fmt.Errorf("pair %d (%s -> %s): %w", i, p.rootObjectID, p.termObjectID, err)
		}
		result.pairDurations = append(result.pairDurations, d)
		fmt.Printf("pathbench: pair %d: %s -> %s: paths=%d duration=%s\n", i, p.rootObjectID, p.termObjectID, len(paths), fmtMillis(d))
	}
	printLatencySummary("PATHBENCH_PAIRS", "pairs", result.pairDurations)

	fmt.Printf("\n=== (a, hydrated) same pairs via engine.TryAllShortestPaths ===\n")
	eng := engine.New(driver, pool, engine.Config{
		Enabled: true,
		// Warn and above only: TryAllShortestPaths logs an Info line per
		// served query, which would otherwise print once per pair below.
		Log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		return nil, fmt.Errorf("engine rebuild: %w", err)
	}
	hydratedEdgeKinds := stringKinds(edgeKindNames)
	for i, p := range pairs {
		pq := recognize.PathQuery{
			Start:     recognize.Endpoint{IDs: []graph.ID{graph.ID(p.rootDB)}},
			End:       recognize.Endpoint{IDs: []graph.ID{graph.ID(p.termDB)}},
			EdgeKinds: hydratedEdgeKinds,
			Mode:      recognize.ModeAll,
		}

		var (
			paths  graph.PathSet
			served bool
		)
		t0 := time.Now()
		// A real graph.Transaction is required: servePathQuery unconditionally
		// reads tx.GraphQueryMemoryLimit() to size the query's MemoryLimit,
		// even for a single-ID Endpoint pair that otherwise never touches tx
		// (see internal/engine's resolveEndpoint). Opening one read
		// transaction per pair, exactly as a real caller (recordingRelationship
		// Query.FetchAllShortestPaths) would, also folds the transaction's own
		// begin/commit round trip into the timing -- part of the real serving
		// path's latency, not an artifact to strip out.
		txErr := driver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			paths, served = eng.TryAllShortestPaths(ctx, tx, pq)
			return nil
		})
		d := time.Since(t0)
		if txErr != nil {
			return nil, fmt.Errorf("hydrated pair %d (%s -> %s): open read transaction: %w", i, p.rootObjectID, p.termObjectID, txErr)
		}
		if !served {
			return nil, fmt.Errorf("hydrated pair %d (%s -> %s): engine declined to serve a single-ID pair query -- see stderr for the decline reason", i, p.rootObjectID, p.termObjectID)
		}
		result.hydratedDurations = append(result.hydratedDurations, d)
		fmt.Printf("pathbench: hydrated pair %d: %s -> %s: paths=%d duration=%s\n", i, p.rootObjectID, p.termObjectID, len(paths), fmtMillis(d))
	}
	printLatencySummary("PATHBENCH_PAIRS_HYDRATED", "hydrated pairs", result.hydratedDurations)
	printHydrationDelta(result.pairDurations, result.hydratedDurations)

	fmt.Printf("\n=== (b) shortest paths to Domain Admins ===\n")
	daIDs, err := domainAdminsGroupIDs(ctx, pool, graphModel.ID)
	if err != nil {
		return nil, fmt.Errorf("domain admins lookup: %w", err)
	}
	result.domainAdminsGroups = len(daIDs)
	if len(daIDs) == 0 {
		return nil, fmt.Errorf("no -512 (Domain Admins) groups found in graph_id=%d", graphModel.ID)
	}
	termDense := make([]snapshot.NodeID, 0, len(daIDs))
	for _, id := range daIDs {
		d, ok := snap.Dense(id)
		if !ok {
			return nil, fmt.Errorf("domain admins group id %d not present in snapshot", id)
		}
		termDense = append(termDense, d)
	}
	sort.Slice(termDense, func(i, j int) bool { return termDense[i] < termDense[j] })

	daQuery := traverse.Query{
		Terminals: traverse.Endpoint{IDs: termDense}, // Roots left zero-valued: unconstrained.
		Kinds:     kindMask,
		Mode:      traverse.ModeOne,
		Limit:     domainAdminsPathsLimit,
	}
	t0 := time.Now()
	daPaths, err := traverse.AllShortestPaths(view, daQuery)
	result.domainAdminsDuration = time.Since(t0)
	if err != nil {
		return nil, fmt.Errorf("domain admins query: %w", err)
	}
	result.domainAdminsPaths = len(daPaths)
	fmt.Printf("pathbench: domain admins groups=%d paths=%d duration=%s\n", result.domainAdminsGroups, result.domainAdminsPaths, fmtSeconds(result.domainAdminsDuration))
	fmt.Printf("PATHBENCH_DOMAIN_ADMINS groups=%d paths=%d duration_ms=%.3f\n", result.domainAdminsGroups, result.domainAdminsPaths, floatMillis(result.domainAdminsDuration))

	fmt.Printf("\n=== (c) rebuild time ===\n")
	rebuildStart := time.Now()
	snap2, err := engine.LoadSnapshot(ctx, driver, pool)
	if err != nil {
		return nil, fmt.Errorf("rebuild snapshot: %w", err)
	}
	result.rebuild = snapshotStats{duration: time.Since(rebuildStart), nodes: snap2.NodeCount(), edges: snap2.EdgeCount(), approxBytes: snap2.ApproxBytes()}
	printSnapshotStats("rebuild", result.rebuild)

	return result, nil
}

// report prints the enforcement section and the final PASS/FAIL line,
// returning whether both named thresholds were met (independent of whether
// -enforce was actually passed -- run decides whether that return value
// changes the exit code).
func (r *benchResult) report(enforce bool) bool {
	pairsP95 := percentile(r.pairDurations, 0.95)
	pairsOK := pairsP95 <= pairP95Threshold
	domainAdminsOK := r.domainAdminsDuration <= domainAdminsThreshold

	fmt.Printf("\n=== enforce thresholds (checked=%t) ===\n", enforce)
	fmt.Printf("pathbench: pairs p95 %s <= %s: %s\n", fmtMillis(pairsP95), fmtMillis(pairP95Threshold), passFail(pairsOK))
	fmt.Printf("pathbench: domain admins total %s <= %s: %s\n", fmtSeconds(r.domainAdminsDuration), fmtSeconds(domainAdminsThreshold), passFail(domainAdminsOK))
	fmt.Printf("PATHBENCH_ENFORCE checked=%t pairs_p95_ms=%.3f pairs_p95_threshold_ms=%.3f pairs_ok=%t domain_admins_ms=%.3f domain_admins_threshold_ms=%.3f domain_admins_ok=%t\n",
		enforce, floatMillis(pairsP95), floatMillis(pairP95Threshold), pairsOK, floatMillis(r.domainAdminsDuration), floatMillis(domainAdminsThreshold), domainAdminsOK)

	pass := pairsOK && domainAdminsOK
	if pass {
		fmt.Println("PATHBENCH_RESULT PASS")
	} else {
		fmt.Println("PATHBENCH_RESULT FAIL")
	}
	return pass
}

// selectPairs draws cfg.runs seeded-random (User, Computer) pairs. Node
// selection is entirely SQL-driven -- a count-then-offset draw over the
// node table's GIN kind_ids index -- so pathbench never has to reproduce
// bench/adgen's own id-assignment formula (which depends on its RNG-derived
// domain SIDs, not just -users and -seed).
func selectPairs(ctx context.Context, pool *pgxpool.Pool, graphID int32, userKindID, compKindID snapshot.KindID, snap *snapshot.Snapshot, runs int, seed int64) ([]pair, error) {
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
	pairs := make([]pair, 0, runs)
	for i := 0; i < runs; i++ {
		userOffset := rng.Intn(userCount)
		compOffset := rng.Intn(compCount)

		userID, userObjectID, err := nodeByKindOffset(ctx, pool, graphID, userKindID, userOffset)
		if err != nil {
			return nil, fmt.Errorf("pick %s at offset %d: %w", kindUser, userOffset, err)
		}
		compID, compObjectID, err := nodeByKindOffset(ctx, pool, graphID, compKindID, compOffset)
		if err != nil {
			return nil, fmt.Errorf("pick %s at offset %d: %w", kindComputer, compOffset, err)
		}

		userDense, ok := snap.Dense(userID)
		if !ok {
			return nil, fmt.Errorf("%s %d (objectid %s) not present in the loaded snapshot", kindUser, userID, userObjectID)
		}
		compDense, ok := snap.Dense(compID)
		if !ok {
			return nil, fmt.Errorf("%s %d (objectid %s) not present in the loaded snapshot", kindComputer, compID, compObjectID)
		}

		pairs = append(pairs, pair{
			rootDB: userID, termDB: compID,
			rootDense: userDense, termDense: compDense,
			rootObjectID: userObjectID, termObjectID: compObjectID,
		})
	}
	return pairs, nil
}

// countByKind returns the number of graphID's nodes carrying kindID, via
// the node table's GIN index on kind_ids (see schema_up.sql:
// "create index ... on node using gin (kind_ids)"), using the same
// "kind_ids operator(pg_catalog.@>) ARRAY[...]::int2[]" containment idiom
// the pg driver's own Cypher-to-SQL translator emits for a single label
// match. The operator must be schema-qualified: the database also loads
// the intarray extension (schema_up.sql), whose own @> overload for
// smallint[] makes the bare "@>" spelling ambiguous (SQLSTATE 42725).
func countByKind(ctx context.Context, pool *pgxpool.Pool, graphID int32, kindID snapshot.KindID) (int, error) {
	var n int
	err := pool.QueryRow(ctx,
		"SELECT count(*) FROM node WHERE graph_id = $1 AND kind_ids operator(pg_catalog.@>) ARRAY[$2]::int2[]",
		graphID, kindID,
	).Scan(&n)
	return n, err
}

// nodeByKindOffset returns the id and objectid of the offset'th node (0
// ordered ascending by id) of graphID carrying kindID. Combined with a
// seeded random offset in [0, countByKind's result), this is
// selectPairs's uniform sampling primitive.
func nodeByKindOffset(ctx context.Context, pool *pgxpool.Pool, graphID int32, kindID snapshot.KindID, offset int) (uint64, string, error) {
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

// domainAdminsGroupIDs returns the database id of every Domain Admins group
// in graphID, identified by bench/adgen's objectid convention (every
// generated domain's Domain Admins group has objectid suffix "-512", the
// real AD well-known RID -- see generate.go's doc comment). Ordered
// ascending by id, matching traverse.Endpoint.IDs' documented ascending
// contract (selectPairs's caller still re-sorts by dense id, since database
// id order and dense id order need not coincide for an arbitrary graph).
func domainAdminsGroupIDs(ctx context.Context, pool *pgxpool.Pool, graphID int32) ([]uint64, error) {
	rows, err := pool.Query(ctx,
		"SELECT id FROM node WHERE graph_id = $1 AND properties->>'objectid' LIKE '%-512' ORDER BY id",
		graphID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uint64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, uint64(id))
	}
	return ids, rows.Err()
}

// buildKindMask builds the traverse.KindMask allowing exactly the edge
// kinds named, mirroring internal/engine's own (unexported) buildKindMask.
func buildKindMask(ctx context.Context, kindMapper pg.KindMapper, maxKindID snapshot.KindID, names []string) (*snapshot.KindMask, error) {
	mask := snapshot.NewKindMask(maxKindID)
	for _, name := range names {
		id, err := kindMapper.MapKind(ctx, graph.StringKind(name))
		if err != nil {
			return nil, fmt.Errorf("map kind %q: %w", name, err)
		}
		mask.Set(id)
	}
	return mask, nil
}

// stringKinds converts kind name strings to graph.Kinds, matching
// bench/adgen's own helper of the same name/shape.
func stringKinds(names []string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// percentile returns durations' p-th percentile (nearest-rank method, 0 <
// p <= 1), sorting a copy so the caller's slice order is undisturbed. An
// empty input returns zero.
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

// meanMax returns the arithmetic mean and maximum of durations (both zero
// for an empty input).
func meanMax(durations []time.Duration) (mean, max time.Duration) {
	if len(durations) == 0 {
		return 0, 0
	}
	var total time.Duration
	for _, d := range durations {
		total += d
		if d > max {
			max = d
		}
	}
	return total / time.Duration(len(durations)), max
}

func printSnapshotStats(label string, s snapshotStats) {
	fmt.Printf("pathbench: %s: nodes=%d edges=%d approx_bytes=%d (%s) duration=%s\n",
		label, s.nodes, s.edges, s.approxBytes, fmtBytes(s.approxBytes), fmtSeconds(s.duration))
	fmt.Printf("PATHBENCH_%s nodes=%d edges=%d approx_bytes=%d duration_ms=%.3f\n",
		toUpperTag(label), s.nodes, s.edges, s.approxBytes, floatMillis(s.duration))
}

func printLatencySummary(tag, label string, durations []time.Duration) {
	p50 := percentile(durations, 0.50)
	p95 := percentile(durations, 0.95)
	mean, max := meanMax(durations)
	fmt.Printf("pathbench: %s summary: n=%d p50=%s p95=%s mean=%s max=%s\n",
		label, len(durations), fmtMillis(p50), fmtMillis(p95), fmtMillis(mean), fmtMillis(max))
	fmt.Printf("%s n=%d p50_ms=%.3f p95_ms=%.3f mean_ms=%.3f max_ms=%.3f\n",
		tag, len(durations), floatMillis(p50), floatMillis(p95), floatMillis(mean), floatMillis(max))
}

// printHydrationDelta reports the extra latency engine.TryAllShortestPaths'
// property hydration adds on top of the pure in-memory traverse.
// AllShortestPaths call (a), pair-for-pair-index (both slices come from the
// same pairs in the same order, per execute's single loop over pairs for
// each), at p50 and p95.
func printHydrationDelta(pure, hydrated []time.Duration) {
	purep50, purep95 := percentile(pure, 0.50), percentile(pure, 0.95)
	hydp50, hydp95 := percentile(hydrated, 0.50), percentile(hydrated, 0.95)
	deltap50, deltap95 := hydp50-purep50, hydp95-purep95
	fmt.Printf("pathbench: hydration delta: p50=+%s p95=+%s\n", fmtMillis(deltap50), fmtMillis(deltap95))
	fmt.Printf("PATHBENCH_HYDRATION_DELTA p50_ms=%.3f p95_ms=%.3f\n", floatMillis(deltap50), floatMillis(deltap95))
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

// toUpperTag renders a lower-case section label ("build", "rebuild") as the
// upper-case token printSnapshotStats' PATHBENCH_* line uses.
func toUpperTag(label string) string {
	out := make([]byte, len(label))
	for i := 0; i < len(label); i++ {
		c := label[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}
