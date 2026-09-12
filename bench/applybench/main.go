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
// It measures four things about the write-through path:
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
//     ((on-off)/off*100). The two arms are SYMMETRIC -- identical work,
//     neither with a concurrent reader -- and INTERLEAVED (on, off, on,
//     off, ...); measureApplyThroughput's own doc records what each of
//     those was worth in measured percentage points when it was missing.
//   - (b) query latency during active ingest: a pathbench-style two-node
//     FetchAllShortestPaths query, sampled continuously while one engine-on
//     ingest runs concurrently (flushing every -latency-flush operations, so
//     many write-through Apply calls land INSIDE the sampled window rather
//     than one near the end of it), compared against an idle baseline of the
//     same query shape on the same warm driver over a comparable wall-clock
//     window -- p50/p95 for the full window, for the delta-populated subset
//     of it (samples taken after the window's first Apply, which is what
//     -enforce scores), and for idle.
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
//   - (d) snapshot file save, load and boot duration at scale:
//     BLOODTRAIL_SNAPSHOT_DIR pointed at a scratch directory; Close's own
//     Stop-then-SaveSnapshot call timed end to end AND the engine's own
//     logged fold+write duration; the file's parse cost measured directly
//     via internal/engine/snapshot.ReadSnapshotFile; and the whole real
//     boot path timed by opening a fresh production driver against the same
//     directory and waiting for the engine's own
//     "bloodtrail: snapshot file loaded" line (again via logCapture -- boot
//     load runs on its own background goroutine with no other way to
//     observe adoption from outside the driver). See measureSnapshotIO for
//     what each of the three includes and excludes.
//   - (e) snapshot-file boot adoption under concurrent writes: (d)'s save,
//     then a fresh production driver opened against the same directory with
//     (a)'s ingest shape hammering it from the instant the open returns, so
//     writes land inside the boot-load window -- the bench-side stand-in
//     for BloodHound's own startup-analysis writes, which land within
//     milliseconds of every real boot. Asserts the boot gap buffer's whole
//     point end to end: the file is still ADOPTED, with the loaded marker's
//     replayed_writes counting the buffered writes folded in (the one
//     legitimate alternative is the documented conservative gap-rejection
//     race; anything else fails). See measureBootReplay.
//
// (a) and (b) additionally assert, via the engine's own log markers, that
// the engine really applied ("bloodtrail: write-through applied") and, for
// (b), really served ("bloodtrail: path engine served") during the measured
// window, and abort if it did not. An engine that never adopted a snapshot
// returns early from Apply and declines every query, which would otherwise
// be reported as near-zero apply overhead and PostgreSQL's own latency --
// a passing run that measured nothing.
//
// Every objectid this bench writes is namespaced by a per-invocation run id
// (-run-id, defaulting to a timestamp): the write shape being measured is an
// upsert, so a second run against a database that was not re-wiped in
// between would otherwise silently update the previous run's rows instead of
// inserting new ones. See resolveRunID.
//
// Every measurement prints a human-readable line and a machine-greppable
// APPLYBENCH_<NAME> summary line (grep '^APPLYBENCH_'), ending in
// APPLYBENCH_RESULT PASS or FAIL. With -enforce, applybench exits nonzero if
// (a)'s overhead exceeds applyOverheadMaxPct or (b)'s during-ingest p95
// exceeds idleP95Multiplier times the idle p95 -- see both constants' docs
// for the 5M-scale evidence behind each. (c) and (d) are reported only,
// never enforced: both are background/operational costs a served query
// never waits on, not costs with the "a user is waiting on this" property
// (a)/(b) have -- see the README's own "Why (c) and (d) have no bar at all
// yet" section, which revisits that question now that real 5M-scale
// numbers exist for both (matching bench/builderbench's/
// bench/cypherbench's measured-physics caps convention). CI must never
// pass -enforce. Any other failure (a database error, a missing base
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
// write loop also prints progress every progressEvery units, and the
// compaction/snapshot-boot waits print progress every 2s while polling, so
// a genuinely slow (not hung) run is visibly making progress rather than
// looking indistinguishable from a wedged one -- see the 2026-09 5M-scale
// bench incident bench/cypherbench's defaultBTCap doc describes for why
// "wait and hope" is never an acceptable substitute for a hard cap here.
//
// Usage:
//
//	go run ./bench/applybench -dsn <dsn> [-ingest 12000] [-flush 20000] [-latency-flush 2000] [-repeats 3] [-queries 200] [-compact-entries 2000] [-run-id <id>] [-cap 10m] [-enforce] [-cpuprofile <file>]
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
	"sync/atomic"
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
// counter, is how this package observes engine-internal events that happen
// on the driver's own background goroutines, inside a write call, or during
// Close). Duplicated verbatim from internal/engine's compact.go, persist.go,
// boot.go, apply.go and engine.go -- TestLogMessagesMatchEngineSource
// (main_test.go) guards against these silently drifting out of sync with
// that package's own literals.
//
// msgWriteThroughApplied and msgPathEngineServed are the two liveness
// markers (a) and (b) assert on: without them, an engine that never adopted
// a snapshot would make Apply return early (zero read-back cost) and every
// query delegate straight to PostgreSQL, and this bench would cheerfully
// report ~0% apply overhead and pg's own latency as though it had measured
// write-through at all. Both measurements abort rather than report that --
// the same "prove the engine actually did the work" check
// bench/cypherbench's own served-marker assertion makes.
const (
	msgCompactionFinished   = "bloodtrail: compaction finished"
	msgSnapshotRebuilt      = "bloodtrail: snapshot rebuilt"
	msgSnapshotWritten      = "bloodtrail: snapshot file written"
	msgSnapshotFileLoaded   = "bloodtrail: snapshot file loaded"
	msgSnapshotFileRejected = "bloodtrail: snapshot file rejected"
	msgWriteThroughApplied  = "bloodtrail: write-through applied"
	msgPathEngineServed     = "bloodtrail: path engine served"
)

// applyOverheadMaxPct is -enforce's bar for (a): the engine-enabled write
// wall time must not exceed the engine-disabled baseline by more than this
// percentage. Evidence-based, following the same measured-physics convention
// used elsewhere in this repo (see bench/builderbench's and
// bench/cypherbench's shapeThresholds docs): three 5M-scale runs (2026-09, see this
// package's README cap-rationale table) measured 33.52% / 25.22% / 26.05%;
// 60.0 is the worst of those (33.52%) x ~1.75, rounded up to a clean number.
// Replacing this value requires fresh 5M-scale evidence recorded alongside
// the change, in the README table and in TestMeasuredEngineOverheadCaps
// (main_test.go) together -- the exact discipline
// bench/builderbench's/bench/cypherbench's own absolute-cap pins already
// enforce for their shapes.
const applyOverheadMaxPct = 60.0

// idleP95Multiplier is -enforce's bar for (b): the during-ingest,
// delta-populated p95 must not exceed this multiple of the idle p95.
// Evidence-based for the identical reason applyOverheadMaxPct is -- see its
// doc. The same three 5M-scale runs measured a delta-p95/idle-p95 ratio of
// 0.44 / 0.48 / 0.98 (idle p95 itself carries a wide tail on this shared,
// loaded machine, from 349ms to 846ms across the three runs -- see the
// README table); 1.75 is the worst of those (0.98) x ~1.75, rounded up.
const idleP95Multiplier = 1.75

// defaultCap is -cap's default: see the package doc's "Watchdog" section.
const defaultCap = 10 * time.Minute

// progressEvery controls how often the ingest loop reports progress to
// stderr, mirroring bench/adgen's identically named constant and the
// package doc's "Watchdog" section (a long write loop must look visibly
// alive, not merely be alive).
const progressEvery = 5_000

// warmupQueries is how many pair queries (b) runs and DISCARDS before it
// samples anything, on the same driver it then measures -- the same warmup
// bench/builderbench's measureShape does before its own timed runs.
//
// Not optional bookkeeping: a first-50-samples idle baseline taken without
// it measured p50 7.85ms / p95 19.18ms on a driver whose own later
// 200-sample window measured p50 0.17ms / p95 10.46ms. Everything above the
// second reading was one-time cost -- pool connections opening, the pg
// driver's translation cache filling, the engine's traversal buffers
// growing -- charged entirely to the idle arm because it happened to run
// first. Enforcing "during-ingest p95 within 3x idle p95" against that
// inflated denominator makes the bar meaningless in the lenient direction,
// and makes "queries are FASTER under load" look like a finding rather than
// the ordering artifact it is.
const warmupQueries = 30

// maxIdleWindow bounds (b)'s idle baseline sampling window. The idle arm
// samples for as long as the during-ingest arm did (see
// sampleQueryLatenciesForWindow) so the two p95s come from comparable
// wall-clock windows rather than from 50 samples vs several hundred -- but
// a production-scale ingest can run for many minutes, and doubling that to
// re-measure a baseline that has long since stabilized buys nothing. Both
// windows are reported, so a reader can always see whether they actually
// matched.
const maxIdleWindow = 60 * time.Second

// maxPairPool bounds how many distinct (User, Computer) pairs
// discoverBaseGraph resolves for the latency loops to draw from -- see its
// own pairCount comment for why this is capped independently of -queries.
const maxPairPool = 50

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
		ingest         = fs.Int("ingest", 12_000, "ingest units written per repeat (each unit is 1 new User node upsert + 1 new MemberOf edge upsert to an existing hub Group -- so M in the 'M nodes+edges' sense is 2x this)")
		flush          = fs.Int("flush", 20_000, "(a)'s batch flush size in operations (an explicit batch.Commit() every this many UpdateNodeBy/UpdateRelationshipBy calls), matching production's write flush size")
		latencyFlush   = fs.Int("latency-flush", 2_000, "(b)'s batch flush size in operations -- deliberately smaller than -flush so several write-through Apply calls land INSIDE the sampled window (see the package doc's (b))")
		repeats        = fs.Int("repeats", 3, "number of repeats per measurement (p50 is taken over these); must be >= 3")
		queries        = fs.Int("queries", 200, "MINIMUM number of two-node shortest-path queries sampled for (b)'s idle baseline; that baseline also samples for as long as the during-ingest window ran (capped, see maxIdleWindow), so more are usually taken")
		compactEntries = fs.Int("compact-entries", 2_000, "BLOODTRAIL_COMPACT_ENTRIES used only for measurement (c); deliberately low so several compactions actually fire")
		seed           = fs.Int64("seed", 1, "seed for deterministic query-pair sampling")
		runID          = fs.String("run-id", "", "objectid namespace for every node/edge this run writes; defaults to a timestamp so a re-run against an un-wiped database inserts rather than silently updating (see README)")
		cap            = fs.Duration("cap", defaultCap, "per-operation wall-clock watchdog; any single operation exceeding this ABORTS THE WHOLE RUN (nonzero exit, never a data point) -- see README")
		enforce        = fs.Bool("enforce", false, "exit nonzero if apply overhead or during-ingest p95 miss their evidence-based bars (never pass this in CI)")
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
	if *latencyFlush <= 0 {
		fmt.Fprintln(os.Stderr, "applybench: -latency-flush must be positive")
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
		latencyFlush:   *latencyFlush,
		repeats:        *repeats,
		queries:        *queries,
		compactEntries: *compactEntries,
		seed:           *seed,
		runID:          resolveRunID(*runID),
		cap:            *cap,
	}
	fmt.Printf("applybench: run id %s (every objectid this run writes is namespaced by it)\n", cfg.runID)
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
	latencyFlush   int
	repeats        int
	queries        int
	compactEntries int
	seed           int64
	runID          string
	cap            time.Duration
}

// resolveRunID returns explicit if the operator gave one, else a fresh
// timestamp-derived id.
//
// Every objectid this bench writes is namespaced by it (writeIngestBatch),
// and that namespacing is load-bearing, not cosmetic: the write shape (a)
// and (b) measure is batch.UpdateNodeBy/UpdateRelationshipBy, an UPSERT.
// Before this existed, objectids were "APPLYBENCH-<tag>-<repeat>-<i>" --
// fully deterministic across invocations -- so a second run against a
// database that had not been re-wiped in between silently turned every
// single insert into an update of the row the previous run left behind.
// That is a materially different (and cheaper: no new rows, no index
// growth, a read-back that finds an existing id rather than a new one)
// workload than the ingest this bench claims to measure, and nothing in the
// output would have said so.
func resolveRunID(explicit string) string {
	if explicit != "" {
		return explicit
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
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
	onApplies    int

	idleDurations []time.Duration
	idleWindow    time.Duration

	// duringDurations is every sample taken while ingest ran;
	// duringDeltaDurations is the subset taken after the window's FIRST
	// write-through Apply, i.e. the only ones that actually queried a
	// populated, pre-compaction delta -- what (b) claims to measure. See
	// splitByFirstApply.
	duringDurations      []time.Duration
	duringDeltaDurations []time.Duration
	duringWindow         time.Duration
	duringApplies        int
	duringServed         int

	compactionDurations []time.Duration
	compactionCount     int

	saveDurations       []time.Duration
	saveLoggedDurations []time.Duration
	loadDurations       []time.Duration
	bootDurations       []time.Duration

	// (e)'s outcomes, one set per writer scenario: how many boots adopted
	// the file (with how many buffered writes replayed onto it, and how
	// long the open->loaded path took) versus how many were superseded by a
	// gap-rejection, plus how many ingest units the concurrent writer
	// landed per repeat. replay* is the burst scenario (BloodHound's
	// ordinary startup-analysis shape); sustainedReplay* is the flat-out
	// writer (a restart landing mid-ingest), the case the adoption path's
	// settle-wait exists for.
	replayAdopted     int
	replayGapRejected int
	replayOverflowed  int
	replayCounts      []int64
	replayBootDurs    []time.Duration
	replayWritten     []int

	sustainedReplayAdopted     int
	sustainedReplayGapRejected int
	sustainedReplayOverflowed  int
	sustainedReplayCounts      []int64
	sustainedReplayBootDurs    []time.Duration
	sustainedReplayWritten     []int
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

	fmt.Println("\n=== running (a) sustained apply throughput ===")
	if err := measureApplyThroughput(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("throughput: %w", err)
	}

	fmt.Println("\n=== running (b) query latency during active ingest ===")
	if err := measureLatencyUnderIngest(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("latency: %w", err)
	}

	fmt.Println("\n=== running (c) compaction duration at scale ===")
	if err := measureCompaction(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("compaction: %w", err)
	}

	fmt.Println("\n=== running (d) snapshot file save/load/boot duration at scale ===")
	if err := measureSnapshotIO(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("snapshot file I/O: %w", err)
	}

	fmt.Println("\n=== running (e) snapshot-file boot adoption under concurrent writes ===")
	if err := measureBootReplay(ctx, cfg, base, result); err != nil {
		return nil, fmt.Errorf("boot replay: %w", err)
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
// do, to report the base graph's size and build cost. (An earlier version
// also used that duration as the safety multiple behind a fixed
// wait-for-boot sleep; every phase now waits on the engine's own adoption
// marker instead -- waitForAdoption's doc has the failure that forced
// this.)
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

	// The pair POOL is what the latency loops draw from at random; it is not
	// the number of samples they take (both loops are duration-bounded, and
	// (b)'s idle arm alone takes at least cfg.queries). Kept small and
	// clamped deliberately: selectPairs costs two OFFSET-scanning queries per
	// pair, which at 5M nodes is not free, and a few dozen distinct pairs
	// already give the sampling loops all the variety they need.
	pairCount := cfg.queries
	if pairCount < 20 {
		pairCount = 20
	}
	if pairCount > maxPairPool {
		pairCount = maxPairPool
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

// waitForAdoption blocks until the engine whose logs capture is recording
// announces its first adopted snapshot -- a PostgreSQL boot rebuild
// ("bloodtrail: snapshot rebuilt") or a snapshot-file boot ("bloodtrail:
// snapshot file loaded") -- and replaces the fixed sleep this package used
// to take here. The sleep (2x a build duration measured once, at run
// start) undershot exactly once, on a run whose own earlier phases had
// left the database's I/O slower than the measurement's: (d)'s save driver
// then reached Close before any View existed, SaveSnapshot declined on its
// preconditions, and the phase failed on a race the harness itself
// manufactured. The engine says, in its own log, when it has adopted;
// waiting for that statement instead of predicting it is the same
// check-progress-not-liveness discipline every other wait in this package
// already follows (runSnapshotBoot, runBootWithConcurrentWrites).
//
// cap bounds the wait through its own context: a boot that never adopts is
// a real failure the caller must report, not sleep through.
func waitForAdoption(ctx context.Context, capture *logCapture, cap time.Duration) error {
	deadline, cancel := context.WithTimeout(ctx, cap)
	defer cancel()

	start := time.Now()
	lastProgress := start
	for {
		if capture.count(msgSnapshotRebuilt) > 0 || capture.count(msgSnapshotFileLoaded) > 0 {
			return nil
		}
		if deadline.Err() != nil {
			return fmt.Errorf("timed out waiting for the engine's first adoption (%q or %q) after %s (-cap=%s)", msgSnapshotRebuilt, msgSnapshotFileLoaded, fmtSeconds(time.Since(start)), cap)
		}
		if time.Since(lastProgress) >= 10*time.Second {
			fmt.Printf("applybench: ... waiting for the engine's first adoption (%s elapsed)\n", fmtSeconds(time.Since(start)))
			lastProgress = time.Now()
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-deadline.Done():
		}
	}
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
// (a): sustained apply throughput
// ============================================================================

// measureApplyThroughput runs cfg.repeats engine-on and cfg.repeats
// engine-off ingest repeats, INTERLEAVED (on, off, on, off, ...), each arm
// doing exactly the same work: writeIngestBatch with cfg.flush, and nothing
// else running against the same pool or CPU.
//
// Both properties are corrections to a first version of this measurement
// that had neither, and whose numbers were consequently not comparable:
//
//   - Asymmetric work. The engine-on arm ran through
//     runIngestWithConcurrentQueries -- with a reader goroutine hammering the
//     same driver, pool and CPU throughout -- while the engine-off arm ran a
//     bare runIngestCapped with no reader at all. The "overhead" that
//     difference alone produced was measured directly, by running the on arm
//     both ways against the same off arm: 29.05% quiet vs 18.64% noisy. Ten
//     points of spread on a 25% bar, from the harness rather than from the
//     engine. (b) is where a concurrent reader belongs, and (b) now owns it
//     exclusively.
//   - Block ordering. All on repeats ran before all off repeats, so every
//     off repeat wrote into a graph the on repeats had already grown by
//     cfg.repeats * cfg.ingest rows -- charging the off arm for index and
//     table growth the on arm never saw, in the direction that makes the
//     engine look better. Interleaving does not eliminate growth (nothing
//     can, short of restoring a snapshot between arms), but it spreads it
//     evenly across both arms instead of concentrating it in one.
//
// Each arm's own engine-on repeat additionally asserts the engine actually
// applied (msgWriteThroughApplied): an engine that never adopted a snapshot
// makes Apply return early with no read-back at all, which would report as
// near-zero overhead -- a passing bar measuring nothing.
func measureApplyThroughput(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	// compactEntries effectively unbounded: (a) must not see a mid-run
	// compaction (see the package doc; (c) measures that separately with its
	// own low threshold).
	onEnv := phaseEnv{engineOn: true, compactEntries: 1 << 30}
	offEnv := phaseEnv{engineOn: false}

	for i := 0; i < cfg.repeats; i++ {
		fmt.Printf("applybench: engine-on repeat %d/%d (ingest %d units, no concurrent reader) ...\n", i+1, cfg.repeats, cfg.ingest)
		onDur, applies, err := runArm(ctx, cfg, base, onEnv, "on", i)
		if err != nil {
			return fmt.Errorf("engine-on ingest (repeat %d): %w", i, err)
		}
		if applies == 0 {
			return fmt.Errorf("engine-on repeat %d logged no %q: the engine never applied anything, so this arm measured delegation, not write-through (was a snapshot ever adopted? try a longer -cap or check BLOODTRAIL_ENGINE)", i, msgWriteThroughApplied)
		}
		result.onDurations = append(result.onDurations, onDur)
		result.onApplies += applies
		fmt.Printf("applybench: engine-on repeat %d: write_wall_time=%s applies=%d\n", i+1, fmtSeconds(onDur), applies)

		fmt.Printf("applybench: engine-off repeat %d/%d (ingest %d units, no concurrent reader) ...\n", i+1, cfg.repeats, cfg.ingest)
		offDur, _, err := runArm(ctx, cfg, base, offEnv, "off", i)
		if err != nil {
			return fmt.Errorf("engine-off ingest (repeat %d): %w", i, err)
		}
		result.offDurations = append(result.offDurations, offDur)
		fmt.Printf("applybench: engine-off repeat %d: write_wall_time=%s\n", i+1, fmtSeconds(offDur))
	}

	return nil
}

// runArm is one of (a)'s two arms, for one repeat: open a driver under env,
// wait for boot if the engine is on at all, write cfg.ingest units, close.
// Returns the write wall time and how many write-through Apply calls the
// engine logged while it ran (always 0 for the engine-off arm, which has no
// engine to log any).
//
// The log capture is installed BEFORE the driver opens, deliberately:
// dawgs.Open reads slog.Default() exactly once, to build the engine's own
// Config.Log (driver.go), so a capture installed afterwards would see
// nothing the engine logs at all.
func runArm(ctx context.Context, cfg config, base *baseGraph, env phaseEnv, tag string, repeat int) (time.Duration, int, error) {
	capture, restore := installLogCapture()
	defer restore()

	db, cleanup, err := openPhaseDriver(ctx, cfg.dsn, env)
	if err != nil {
		return 0, 0, fmt.Errorf("open driver: %w", err)
	}
	defer cleanup()

	if env.engineOn {
		if err := waitForAdoption(ctx, capture, cfg.cap); err != nil {
			return 0, 0, fmt.Errorf("%s arm (repeat %d): %w", tag, repeat, err)
		}
	}
	// No adoption wait when the engine is off: Start is a no-op for a
	// disabled engine (internal/engine/boot.go's Start), so there is no
	// boot-load goroutine to wait for -- and waiting anyway would only
	// lengthen the wall clock this arm is not being timed on.

	d, err := runIngestCapped(ctx, db, base.hubObjectID, cfg.ingest, cfg.flush, cfg.runID, tag, repeat, cfg.cap)
	if err != nil {
		return 0, 0, err
	}
	return d, capture.count(msgWriteThroughApplied), nil
}

// ============================================================================
// (b): query latency during active ingest
// ============================================================================

// measureLatencyUnderIngest samples the two-node FetchAllShortestPaths query
// continuously while an engine-on ingest runs against the same driver, then
// samples the same query on the same (still warm) driver with no writes at
// all as the idle baseline -- both from one driver, one warm-up, and
// comparable wall-clock windows.
//
// Four things here are deliberate, each fixing a way a first version of this
// measurement reported a number that did not mean what it said:
//
//  1. cfg.latencyFlush, not cfg.flush. Write-through Apply only runs at a
//     Commit boundary (write_observer.go's observingBatch.Commit), and with
//     the defaults -ingest 12000 (= 24,000 operations) and -flush 20,000,
//     exactly ONE mid-delegate Commit ever happened -- around unit 10,000 of
//     12,000. So for roughly the first 83% of the sampled window there was
//     no published delta to query at all, and (b) mostly measured the base
//     snapshot. A smaller (b)-only flush size puts many Apply calls inside
//     the window; the default 2,000 gives 12 of them.
//  2. The samples are split by the window's first Apply (splitByFirstApply),
//     and the delta-populated subset is what -enforce actually scores. The
//     full-window numbers are still printed, so the difference between the
//     two is visible rather than assumed away.
//  3. A warm-up (warmupQueries) runs and is discarded before anything is
//     sampled -- see that constant's doc for the measured size of the
//     artifact this removes.
//  4. The idle baseline runs on the SAME driver, AFTER the ingest, over a
//     window matching the ingest's own (sampleQueryLatenciesForWindow). It
//     used to be the first thing the whole bench did, on its own
//     freshly-opened driver, for a fixed 50 samples -- so it absorbed every
//     one-time cost in the process and became an inflated denominator for
//     the "within 3x idle p95" bar it anchors.
//
// It also asserts the engine actually applied and actually served during the
// window: without both, Apply returns early and every query delegates to
// PostgreSQL, and this measurement would silently report pg's latency under
// pg's own write load as though it were the engine's.
func measureLatencyUnderIngest(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	capture, restore := installLogCapture()
	defer restore()

	db, cleanup, err := openPhaseDriver(ctx, cfg.dsn, phaseEnv{engineOn: true, compactEntries: 1 << 30})
	if err != nil {
		return fmt.Errorf("open latency driver: %w", err)
	}
	defer cleanup()
	if err := waitForAdoption(ctx, capture, cfg.cap); err != nil {
		return fmt.Errorf("latency driver boot: %w", err)
	}

	fmt.Printf("applybench: warming up (%d discarded queries) ...\n", warmupQueries)
	if _, err := sampleQueryLatenciesCapped(ctx, db, base.pairs, warmupQueries, cfg.cap); err != nil {
		return fmt.Errorf("warm-up queries: %w", err)
	}

	appliedBefore := capture.count(msgWriteThroughApplied)
	servedBefore := capture.count(msgPathEngineServed)

	fmt.Printf("applybench: during-ingest sampling (ingest %d units, flush %d ops, concurrent query sampling) ...\n", cfg.ingest, cfg.latencyFlush)
	windowStart := time.Now()
	ingestDur, samples, err := runIngestWithConcurrentQueries(ctx, db, base, cfg, "lat", 0)
	if err != nil {
		return err
	}

	result.duringApplies = capture.count(msgWriteThroughApplied) - appliedBefore
	result.duringServed = capture.count(msgPathEngineServed) - servedBefore
	if result.duringApplies == 0 {
		return fmt.Errorf("no %q logged during the sampled window: write-through never ran, so these samples are not 'during ingest with a populated delta' at all", msgWriteThroughApplied)
	}
	if result.duringServed == 0 {
		return fmt.Errorf("no %q logged during the sampled window: every query delegated to PostgreSQL, so these samples measure pg's latency, not the engine's", msgPathEngineServed)
	}

	firstApply, haveFirstApply := capture.firstAfter(msgWriteThroughApplied, windowStart)
	emptyDelta, populated := splitByFirstApply(samples, firstApply, haveFirstApply)
	result.duringDurations = allDurations(samples)
	result.duringDeltaDurations = populated
	result.duringWindow = ingestDur
	fmt.Printf("applybench: during-ingest: window=%s samples=%d (empty_delta=%d delta_populated=%d) applies=%d engine_served=%d\n",
		fmtSeconds(ingestDur), len(samples), len(emptyDelta), len(populated), result.duringApplies, result.duringServed)

	idleWindow := ingestDur
	if idleWindow > maxIdleWindow {
		idleWindow = maxIdleWindow
	}
	fmt.Printf("applybench: idle baseline (same warm driver, no concurrent writes, window=%s, min %d samples) ...\n", fmtSeconds(idleWindow), cfg.queries)
	idleDurations, idleActual, err := sampleQueryLatenciesForWindow(ctx, db, base.pairs, idleWindow, cfg.queries, cfg.cap)
	if err != nil {
		return fmt.Errorf("idle-baseline queries: %w", err)
	}
	result.idleDurations = idleDurations
	result.idleWindow = idleActual
	fmt.Printf("applybench: idle baseline: window=%s n=%d p50=%s p95=%s\n",
		fmtSeconds(idleActual), len(idleDurations), fmtMillis(percentile(idleDurations, 0.50)), fmtMillis(percentile(idleDurations, 0.95)))

	return nil
}

// latencySample is one timed query, tagged with the wall-clock instant it
// STARTED -- which is what splitByFirstApply compares against the engine's
// own first "write-through applied" log record to decide whether that
// sample ran against a populated delta or an empty one.
type latencySample struct {
	at time.Time
	d  time.Duration
}

// splitByFirstApply partitions samples into those taken before the window's
// first write-through Apply (an empty delta: the engine was serving the
// base snapshot, exactly as it would with no ingest running at all) and
// those taken at or after it (a populated, pre-compaction delta, which is
// the state (b) exists to measure).
//
// haveFirstApply false -- no Apply was observed in the window at all --
// puts every sample in the empty-delta half, which is the honest answer:
// nothing published a delta, so nothing queried one. (measureLatencyUnderIngest
// treats that case as a hard error before ever calling this, but the
// function must still be total.)
//
// Compaction would end the "populated delta" state by folding the delta back
// into the base, which is why (b) forces compaction effectively off; (c)
// measures compaction separately.
func splitByFirstApply(samples []latencySample, firstApply time.Time, haveFirstApply bool) (emptyDelta, populated []time.Duration) {
	for _, s := range samples {
		if haveFirstApply && !s.at.Before(firstApply) {
			populated = append(populated, s.d)
		} else {
			emptyDelta = append(emptyDelta, s.d)
		}
	}
	return emptyDelta, populated
}

// allDurations drops samples' timestamps, keeping their durations in order.
func allDurations(samples []latencySample) []time.Duration {
	out := make([]time.Duration, len(samples))
	for i, s := range samples {
		out[i] = s.d
	}
	return out
}

// runIngestWithConcurrentQueries runs one ingest repeat on its own goroutine
// while the calling goroutine samples pair queries against the very same db
// continuously, for as long as ingest takes -- see the package doc's (b) for
// why continuous sampling, not a fixed -queries count, is what "concurrently
// with a write load" means here. Returns the ingest wall time and every
// sampled query, timestamped (see latencySample).
//
// Used by (b) only: (a)'s arms deliberately run no concurrent reader at all
// (measureApplyThroughput's own doc explains what that asymmetry cost when
// (a) used this).
func runIngestWithConcurrentQueries(ctx context.Context, db graph.Database, base *baseGraph, cfg config, tag string, repeat int) (time.Duration, []latencySample, error) {
	capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
	defer cancel()

	done := make(chan struct{})
	var (
		ingestDur time.Duration
		ingestErr error
	)
	go func() {
		defer close(done)
		ingestDur, ingestErr = writeIngestBatch(capCtx, db, base.hubObjectID, cfg.ingest, cfg.latencyFlush, cfg.runID, tag, repeat)
	}()

	rng := rand.New(rand.NewSource(cfg.seed + int64(repeat) + 1))
	var (
		samples  []latencySample
		queryErr error
	)
sampleLoop:
	for {
		select {
		case <-done:
			break sampleLoop
		default:
		}
		p := base.pairs[rng.Intn(len(base.pairs))]
		at := time.Now()
		d, err := runPairQuery(capCtx, db, p.startID, p.endID)
		if err != nil {
			queryErr = err
			break sampleLoop
		}
		samples = append(samples, latencySample{at: at, d: d})
	}
	<-done

	if ingestErr != nil {
		if errors.Is(capCtx.Err(), context.DeadlineExceeded) {
			return 0, nil, fmt.Errorf("during-ingest write load exceeded -cap=%s: %w", cfg.cap, ingestErr)
		}
		return 0, nil, ingestErr
	}
	if queryErr != nil && !errors.Is(queryErr, context.Canceled) {
		return 0, nil, fmt.Errorf("concurrent query sampling: %w", queryErr)
	}
	return ingestDur, samples, nil
}

// runIngestCapped runs writeIngestBatch bounded to cap via
// context.WithTimeout wrapped directly around the call -- see the package
// doc's "Watchdog" section: exceeding it always aborts the whole run.
func runIngestCapped(ctx context.Context, db graph.Database, hubObjectID string, count, flushSize int, runID, tag string, repeat int, cap time.Duration) (time.Duration, error) {
	capCtx, cancel := context.WithTimeout(ctx, cap)
	defer cancel()

	d, err := writeIngestBatch(capCtx, db, hubObjectID, count, flushSize, runID, tag, repeat)
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
// runID (see resolveRunID: this is what keeps a re-run against an un-wiped
// database from silently turning every insert into an update), then by tag
// and repeat so phases and repeats within one run never collide either.
// Returns the whole BatchOperation call's wall-clock time.
func writeIngestBatch(ctx context.Context, db graph.Database, hubObjectID string, count, flushSize int, runID, tag string, repeat int) (time.Duration, error) {
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

			userObjectID := fmt.Sprintf("APPLYBENCH-%s-%s-%d-%08d", runID, tag, repeat, i)
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
// cap via context.WithTimeout wrapped around the whole sampling loop. Used
// for (b)'s warm-up, whose results are discarded.
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

// sampleQueryLatenciesForWindow samples pair queries against db until BOTH
// window has elapsed and at least minSamples have been taken, bounded to cap
// the same way sampleQueryLatenciesCapped is. Returns the samples and the
// window that was actually covered.
//
// A duration-bounded loop, rather than a fixed count, is what makes (b)'s
// idle arm comparable to its during-ingest arm: the during-ingest arm is
// inherently duration-bounded (it samples for exactly as long as its write
// load runs), so a fixed-count idle arm compares two windows of arbitrarily
// different lengths -- and a p95 over a short window is dominated by
// whatever one-time cost happened to fall in it. minSamples is still
// enforced so a very short ingest cannot leave the baseline with too few
// samples for a p95 to mean anything.
func sampleQueryLatenciesForWindow(ctx context.Context, db graph.Database, pairs []pair, window time.Duration, minSamples int, cap time.Duration) ([]time.Duration, time.Duration, error) {
	capCtx, cancel := context.WithTimeout(ctx, cap)
	defer cancel()

	rng := rand.New(rand.NewSource(2))
	start := time.Now()
	var durations []time.Duration
	for {
		elapsed := time.Since(start)
		if elapsed >= window && len(durations) >= minSamples {
			return durations, elapsed, nil
		}
		p := pairs[rng.Intn(len(pairs))]
		d, err := runPairQuery(capCtx, db, p.startID, p.endID)
		if err != nil {
			if errors.Is(capCtx.Err(), context.DeadlineExceeded) {
				return durations, time.Since(start), fmt.Errorf("idle query sampling exceeded -cap=%s: %w", cap, err)
			}
			return durations, time.Since(start), err
		}
		durations = append(durations, d)
	}
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
// outside package bloodtrail through the real driver), and drives
// cfg.repeats separate flushes -- each one, on its own, comfortably over
// the threshold -- waiting after EACH for that flush's own compaction to be
// adopted before issuing the next one, so every trigger gets an isolated
// chance to fold and finish (see the "one flush at a time" note below for
// why a single burst cannot do this at production node counts).
func measureCompaction(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	capture, restore := installLogCapture()
	defer restore()

	capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
	defer cancel()

	db, cleanup, err := openPhaseDriver(capCtx, cfg.dsn, phaseEnv{engineOn: true, compactEntries: cfg.compactEntries})
	if err != nil {
		return fmt.Errorf("open compaction-forcing driver: %w", err)
	}
	defer cleanup()
	if err := waitForAdoption(capCtx, capture, cfg.cap); err != nil {
		return fmt.Errorf("compaction driver boot: %w", err)
	}

	// Each flush's own delta should comfortably exceed compactEntries.
	//
	// The two units are NOT interchangeable, and conflating them was a real
	// bug here: one ingest unit is 2 delta entries (1 node + 1 edge) AND 2
	// batch operations (one UpdateNodeBy, one UpdateRelationshipBy), while
	// writeIngestBatch's flushSize parameter counts OPERATIONS. An earlier
	// version passed unitsPerFlush straight through as flushSize, so it
	// actually flushed every unitsPerFlush/2 units -- roughly
	// compactEntries/2 entries per flush, i.e. under rather than over the
	// threshold this measurement exists to cross.
	unitsPerFlush := cfg.compactEntries/2 + 1
	opsPerFlush := 2 * unitsPerFlush

	// One flush at a time, not one burst of cfg.repeats*3+1 flushes: an
	// earlier version wrote every flush in a single writeIngestBatch call
	// before ever checking capture.count, on the theory that "enough
	// flushes should run to observe several triggers even if some overlap
	// while a prior compaction is still folding". That theory fails at
	// production node counts specifically because maybeStartCompaction
	// (compact.go) is only ever invoked from Apply's own tail -- it is not
	// a periodic background check -- so it only gets a chance to trigger
	// compaction 2 (or 3) if ANOTHER write lands after compaction 1 has
	// adopted. A whole burst of flushes issued back-to-back, with nothing
	// pausing between them, finishes writing (a few seconds, per (a)'s own
	// measurement) long before a single fold over a multi-million-node base
	// snapshot can complete; every flush after the first lands while
	// `compacting` is still true, gets skipped by maybeStartCompaction's own
	// CAS guard, and piles up as one large tail segment with nothing left
	// to write afterward that could ever re-check the threshold. The result
	// measured at 5M (2026-09, see this package's README cap-rationale
	// table): exactly 1 compaction observed, then an unconditional 10-minute
	// stall with 0% CPU and static RSS -- not a hang, just genuinely nothing
	// left to trigger a second one, correctly reported as an honest timeout
	// by the loop below rather than a silent hang. Spacing the flushes out,
	// one at a time, and waiting for each one's own compaction to land
	// before issuing the next, is what lets every trigger actually get a
	// turn.
	target := cfg.repeats
	fmt.Printf("applybench: forcing compaction: compact_entries=%d units_per_flush=%d ops_per_flush=%d target_compactions=%d\n",
		cfg.compactEntries, unitsPerFlush, opsPerFlush, target)
	for i := 0; i < target; i++ {
		if _, err := writeIngestBatch(capCtx, db, base.hubObjectID, unitsPerFlush, opsPerFlush, cfg.runID, "compact", i); err != nil {
			if errors.Is(capCtx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("compaction-forcing ingest exceeded -cap=%s: %w", cfg.cap, err)
			}
			return fmt.Errorf("compaction-forcing ingest (flush %d/%d): %w", i+1, target, err)
		}

		want := i + 1
		for {
			n := capture.count(msgCompactionFinished)
			if n >= want {
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
// directory. Each cycle measures three distinct things, deliberately kept
// apart rather than collapsed into one "save" and one "load" number:
//
//  1. save: Close() (Stop, then the engine's own SaveSnapshot -- driver.go)
//     timed end to end from the caller's side, AND the engine's own
//     fold+write duration read straight off its "snapshot file written" log
//     record. The two differ by Stop's quiescing and the pg driver's own
//     Close, so reporting only the wall time (as an earlier version did)
//     silently attributes that to the save.
//  2. load: internal/engine/snapshot.ReadSnapshotFile, timed directly. This
//     is the parse cost alone -- the one part that scales with file size --
//     and it deliberately EXCLUDES the boot path's own ReadWatermark round
//     trip and adoption, which measurement 3 covers.
//  3. boot: a fresh production driver open (dawgs.Open, then AssertSchema,
//     exactly as production does it) timed until the engine logs
//     "snapshot file loaded". This is what an operator actually waits for,
//     and it includes everything measurement 2 leaves out plus the driver
//     open itself and up to one boot-load retry interval
//     (fallbackRetryInterval, 100ms) of the loop's own backoff -- so it is
//     an upper bound on the real thing, not a like-for-like parse timing.
//
// Measurement 3 only became possible with the fix in
// internal/engine/boot.go: the file attempt used to be made once, before
// runBootLoad's retry loop, at an instant where the default graph could not
// possibly have resolved yet (bloodtrail.Open calls Start before it returns
// the driver a caller needs to call AssertSchema on). It therefore never
// read the file at all through a fresh driver open, and an earlier version
// of this function that waited for the "snapshot file loaded" line hung
// until -cap's watchdog tripped, every time. The wait below now doubles as
// the harness-side regression check for that: if the boot path ever stops
// reaching the file again, this measurement fails loudly rather than
// silently reporting a parse timing in its place.
func measureSnapshotIO(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	dir, err := os.MkdirTemp("", "applybench-snapshot-*")
	if err != nil {
		return fmt.Errorf("create scratch snapshot dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	path := filepath.Join(dir, fmt.Sprintf("graph-%d.btsnap", base.graphID))

	for i := 0; i < cfg.repeats; i++ {
		saveDur, loggedDur, err := runSnapshotSave(ctx, cfg, base, dir, i)
		if err != nil {
			return err
		}
		result.saveDurations = append(result.saveDurations, saveDur)
		if loggedDur > 0 {
			result.saveLoggedDurations = append(result.saveLoggedDurations, loggedDur)
		}
		fmt.Printf("applybench: repeat %d: save close_wall_time=%s engine_logged_duration=%s\n", i+1, fmtMillis(saveDur), fmtMillis(loggedDur))

		t0 := time.Now()
		snap, watermark, err := snapshot.ReadSnapshotFile(path)
		loadDur := time.Since(t0)
		if err != nil {
			return fmt.Errorf("repeat %d: read snapshot file %s: %w", i, path, err)
		}
		result.loadDurations = append(result.loadDurations, loadDur)
		fmt.Printf("applybench: repeat %d: load (ReadSnapshotFile, parse only) duration=%s nodes=%d edges=%d watermark=%d\n",
			i+1, fmtMillis(loadDur), snap.NodeCount(), snap.EdgeCount(), watermark)

		bootDur, err := runSnapshotBoot(ctx, cfg, dir, i)
		if err != nil {
			return err
		}
		result.bootDurations = append(result.bootDurations, bootDur)
		fmt.Printf("applybench: repeat %d: boot (driver open -> %q) duration=%s\n", i+1, msgSnapshotFileLoaded, fmtMillis(bootDur))
	}

	return nil
}

// runSnapshotSave is (d)'s save half for one repeat: open a driver with
// BLOODTRAIL_SNAPSHOT_DIR set, write a little so this repeat's file carries
// a fresh watermark, then time Close end to end. Returns both that wall time
// and the engine's own logged fold+write duration (0 if the record carried
// none).
func runSnapshotSave(ctx context.Context, cfg config, base *baseGraph, dir string, repeat int) (time.Duration, time.Duration, error) {
	capture, restore := installLogCapture()
	defer restore()

	capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
	defer cancel()

	db, cleanup, err := openPhaseDriver(capCtx, cfg.dsn, phaseEnv{engineOn: true, compactEntries: 1 << 30, snapshotDir: dir})
	if err != nil {
		return 0, 0, fmt.Errorf("open save driver (repeat %d): %w", repeat, err)
	}
	if err := waitForAdoption(capCtx, capture, cfg.cap); err != nil {
		// cleanup here, not deferred: the ordinary path times cleanup()
		// itself as the measured Close, so only this early exit owns it.
		cleanup()
		return 0, 0, fmt.Errorf("save driver boot (repeat %d): %w", repeat, err)
	}

	// A tiny write gives every repeat's file a fresh, distinguishable
	// watermark; small enough not to matter to the timing. It is also what
	// makes SaveSnapshot's own convergence precondition hold on a repeat
	// whose driver booted from the PREVIOUS repeat's file (which it now
	// does, since the file attempt is reachable): a file-booted engine
	// starts with appliedWatermark 0 against a nonzero pg counter, and it is
	// this write's own resolved bump that brings the two back level.
	if _, err := writeIngestBatch(capCtx, db, base.hubObjectID, 10, 10, cfg.runID, "snapshot", repeat); err != nil {
		return 0, 0, fmt.Errorf("pre-save ingest (repeat %d): %w", repeat, err)
	}

	t0 := time.Now()
	cleanup()
	wall := time.Since(t0)

	if capture.count(msgSnapshotWritten) == 0 {
		return 0, 0, fmt.Errorf("repeat %d: Close did not log %q -- SaveSnapshot's own preconditions were not met (see internal/engine/persist.go)", repeat, msgSnapshotWritten)
	}
	var logged time.Duration
	if durs := capture.durations(msgSnapshotWritten); len(durs) > 0 {
		logged = durs[len(durs)-1]
	}
	return wall, logged, nil
}

// runSnapshotBoot is (d)'s boot half for one repeat: open a fresh production
// driver against the same snapshot directory and time it until the engine
// logs that it loaded the file. See measureSnapshotIO's own doc for exactly
// what this number includes (and why it is an upper bound rather than a
// parse timing), and for why it was unmeasurable before the engine's own
// boot-load fix.
func runSnapshotBoot(ctx context.Context, cfg config, dir string, repeat int) (time.Duration, error) {
	capture, restore := installLogCapture()
	defer restore()

	capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
	defer cancel()

	t0 := time.Now()
	_, cleanup, err := openPhaseDriver(capCtx, cfg.dsn, phaseEnv{engineOn: true, compactEntries: 1 << 30, snapshotDir: dir})
	if err != nil {
		return 0, fmt.Errorf("open boot driver (repeat %d): %w", repeat, err)
	}
	defer cleanup()

	lastProgress := t0
	for {
		if capture.count(msgSnapshotFileLoaded) > 0 {
			return time.Since(t0), nil
		}
		if rejected := capture.count(msgSnapshotFileRejected); rejected > 0 {
			return 0, fmt.Errorf("repeat %d: boot logged %q instead of loading the file -- the file this run just wrote was not trusted (see internal/engine/boot.go's tryLoadSnapshotFile for the reasons it names)", repeat, msgSnapshotFileRejected)
		}
		if capCtx.Err() != nil {
			return 0, fmt.Errorf("repeat %d: timed out waiting for %q (-cap=%s)", repeat, msgSnapshotFileLoaded, cfg.cap)
		}
		if time.Since(lastProgress) >= 2*time.Second {
			fmt.Printf("applybench: ... waiting for %q (%s elapsed)\n", msgSnapshotFileLoaded, fmtSeconds(time.Since(t0)))
			lastProgress = time.Now()
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-capCtx.Done():
		}
	}
}

// ============================================================================
// (e): snapshot-file boot adoption under concurrent writes
// ============================================================================

// measureBootReplay proves, at whatever scale the loaded graph provides, the
// property the boot gap buffer exists for (internal/engine/bootgap.go):
// recognized writes landing while the snapshot file loads must not cost the
// file its adoption -- they are buffered, proven complete against the
// watermark gap, and replayed onto the loaded snapshot before it is
// published. BloodHound does this to every real boot (its startup analysis
// writes within milliseconds of the API coming up, then goes quiet), so this
// phase is the bench-side stand-in for a production restart: per repeat,
// save a file through a real driver Close exactly as (d) does, then reopen a
// fresh production driver and fire a BURST of (a)'s own ingest write shape
// at it from the moment the open returns, so the writes land inside the
// boot-load window and are done before it ends.
//
// Two writer scenarios run cfg.repeats boots each, distinguished by what
// they prove:
//
//   - BURST (4x25 units, then silence): BloodHound's ordinary boot shape --
//     the startup analysis writes within milliseconds of the API coming up,
//     then goes quiet. The original phase, kept as the baseline.
//   - SUSTAINED (flat out until the attempt concludes): a restart landing
//     mid-ingest. A batch write's watermark bump commits when its first
//     operation is buffered (eager, write_observer.go's ensureBumped) while
//     its Apply -- the call the boot gap buffer observes -- only runs at
//     the flush that commits the chunk, so a writer that never stops keeps
//     an unaccounted in-flight bump alive at essentially every instant. A
//     one-instant adoption decision therefore lost this scenario 3/3 when
//     it was first measured; the settle-wait (adoptSnapshotFileView's
//     frozen-target wait, internal/engine/boot.go) exists precisely to win
//     it, and this scenario is its regression proof at scale.
//
// Two outcomes are legitimate per boot, and both are reported rather than
// barred (like (c) and (d), this phase measures; only an outcome neither
// explains fails the run):
//
//   - ADOPTED: "snapshot file loaded", with the marker's replayed_writes
//     attr counting the buffered writes folded in. The expected outcome in
//     BOTH scenarios now; each scenario must land it at least once, or the
//     phase proved nothing about that scenario. (On a graph small enough
//     that the load outruns the burst's first commit, replayed_writes can
//     legitimately be 0 -- the writes simply landed after adoption, as
//     ordinary deltas; only a large graph makes the burst scenario bite.)
//   - GAP-REJECTED: "snapshot file rejected" with reason "boot gap not
//     covered by buffered writes": a bump was still unaccounted when the
//     settle window closed -- a write outliving the wait. Rare in either
//     scenario now, and the boot's fallback to a pg rebuild is correct
//     when it happens.
//   - OVERFLOW-REJECTED: "snapshot file rejected" for a buffer-cap poison
//     ("boot write buffer poisoned: overflow: ..."): the writer
//     outproduced the boot gap buffer's caps across the whole boot window
//     -- the bounded-memory design handing the boot to the rebuild path
//     honestly, exactly as documented. Counted and reported so a run at a
//     scale beyond the caps' envelope says so instead of aborting; the
//     caps themselves are sized from this scenario's own measurement
//     (maxBootGapEntries' doc, internal/engine/bootgap.go), so at current
//     caps and scale the expected count is zero.
//
// Any other rejection reason, or a boot that never reaches either marker
// inside -cap, is a real defect and aborts the run.
func measureBootReplay(ctx context.Context, cfg config, base *baseGraph, result *benchResult) error {
	dir, err := os.MkdirTemp("", "applybench-bootreplay-*")
	if err != nil {
		return fmt.Errorf("create scratch snapshot dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	scenarios := []struct {
		name       string
		sustained  bool
		adopted    *int
		rejected   *int
		overflowed *int
		counts     *[]int64
		bootDurs   *[]time.Duration
		written    *[]int
	}{
		{"burst", false, &result.replayAdopted, &result.replayGapRejected, &result.replayOverflowed, &result.replayCounts, &result.replayBootDurs, &result.replayWritten},
		{"sustained", true, &result.sustainedReplayAdopted, &result.sustainedReplayGapRejected, &result.sustainedReplayOverflowed, &result.sustainedReplayCounts, &result.sustainedReplayBootDurs, &result.sustainedReplayWritten},
	}

	for _, sc := range scenarios {
		for i := 0; i < cfg.repeats; i++ {
			if _, _, err := runSnapshotSave(ctx, cfg, base, dir, i); err != nil {
				return fmt.Errorf("boot-replay save (%s, repeat %d): %w", sc.name, i, err)
			}

			outcome, replayed, bootDur, written, err := runBootWithConcurrentWrites(ctx, cfg, base, dir, i, sc.sustained)
			if err != nil {
				return err
			}
			*sc.written = append(*sc.written, written)
			switch outcome {
			case bootReplayAdopted:
				*sc.adopted++
				*sc.counts = append(*sc.counts, replayed)
				*sc.bootDurs = append(*sc.bootDurs, bootDur)
				fmt.Printf("applybench: %s repeat %d: boot ADOPTED the file, replayed_writes=%d open->loaded=%s (writer landed %d units meanwhile)\n",
					sc.name, i+1, replayed, fmtMillis(bootDur), written)
			case bootReplayGapRejected:
				*sc.rejected++
				fmt.Printf("applybench: %s repeat %d: boot GAP-REJECTED the file (a write outlived the settle window; writer landed %d units)\n", sc.name, i+1, written)
			case bootReplayOverflowRejected:
				*sc.overflowed++
				fmt.Printf("applybench: %s repeat %d: boot OVERFLOW-REJECTED the file (the writer outproduced the boot gap buffer's caps across the whole window; writer landed %d units)\n", sc.name, i+1, written)
			}
		}

		if *sc.adopted == 0 {
			return fmt.Errorf("no %s-writer repeat ever adopted the snapshot file (%d/%d gap-rejected, %d/%d overflowed): losing every time is indistinguishable from the replay (or its settle-wait) never working -- investigate before trusting this build's restart path", sc.name, *sc.rejected, cfg.repeats, *sc.overflowed, cfg.repeats)
		}
	}
	return nil
}

// bootReplayGapReason is adoptSnapshotFileView's uncovered-gap rejection
// reason (internal/engine/boot.go): a write outlived the settle window.
// bootReplayOverflowPrefix matches the buffer-poisoned rejection for a cap
// overflow ("boot write buffer poisoned: " from the adoption path,
// "overflow: ..." from the buffer's own poison reasons) -- the bounded-
// memory design working as documented when a writer outproduces the caps
// across the whole boot window. These are the two rejections (e)'s own
// writers can legitimately drive the boot into; both pieces are pinned
// against the engine source by TestLogMessagesMatchEngineSource alongside
// the message literals.
const (
	bootReplayGapReason      = "boot gap not covered by buffered writes"
	bootReplayOverflowPrefix = "boot write buffer poisoned: overflow: too many buffered"
)

// bootReplayOutcome is one (e) boot's result.
type bootReplayOutcome int

const (
	bootReplayAdopted bootReplayOutcome = iota
	bootReplayGapRejected
	bootReplayOverflowRejected
)

// bootReplayBurstCalls and bootReplayBurstUnits shape (e)'s write burst:
// bootReplayBurstCalls back-to-back writeIngestBatch calls of
// bootReplayBurstUnits units each, fired the moment the driver open
// returns and then STOPPED -- the burst-then-quiet shape
// measureBootReplay's own doc explains is load-bearing. 4x25 units lands
// ~100 writes (200 upsert operations) across a handful of batch commits:
// comfortably inside a multi-second load window at 5M scale, done
// committing long before the adoption decision runs, and an order of
// magnitude below the boot gap buffer's own entry cap.
const (
	bootReplayBurstCalls = 4
	bootReplayBurstUnits = 25
)

// runBootWithConcurrentWrites is (e)'s boot half for one repeat: open a
// fresh production driver against dir and, from the moment the open
// returns, write through that same driver -- so the writes land inside the
// load window and exercise the boot gap buffer, not after adoption. The
// writer is the scenario: a finite burst (sustained=false), or flat out
// until the attempt concludes (sustained=true; stopped by the outcome
// poll's cancel below). The writer's own error fails the repeat: a write
// refused during boot would silently turn this phase into a quiet-restart
// measurement.
func runBootWithConcurrentWrites(ctx context.Context, cfg config, base *baseGraph, dir string, repeat int, sustained bool) (outcome bootReplayOutcome, replayed int64, bootDur time.Duration, written int, err error) {
	capture, restore := installLogCapture()
	defer restore()

	capCtx, cancel := context.WithTimeout(ctx, cfg.cap)
	defer cancel()

	t0 := time.Now()
	db, cleanup, err := openPhaseDriver(capCtx, cfg.dsn, phaseEnv{engineOn: true, compactEntries: 1 << 30, snapshotDir: dir})
	if err != nil {
		return bootReplayGapRejected, 0, 0, 0, fmt.Errorf("open boot-replay driver (repeat %d): %w", repeat, err)
	}
	defer cleanup()

	writerCtx, stopWriter := context.WithCancel(capCtx)
	defer stopWriter()

	// writerUnits is atomic because the outcome poll below prints it live
	// while the writer goroutine is still advancing it; writerErr needs no
	// synchronization beyond writerDone, which is always received before it
	// is read.
	var (
		writerDone  = make(chan struct{})
		writerUnits atomic.Int64
		writerErr   error
	)
	go func() {
		defer close(writerDone)
		tag := fmt.Sprintf("bootreplay%d", repeat)
		if sustained {
			tag = fmt.Sprintf("bootreplaysus%d", repeat)
		}
		for call := 0; sustained || call < bootReplayBurstCalls; call++ {
			if writerCtx.Err() != nil {
				return
			}
			if _, err := writeIngestBatch(writerCtx, db, base.hubObjectID, bootReplayBurstUnits, 2*bootReplayBurstUnits, cfg.runID, tag, call); err != nil {
				if writerCtx.Err() == nil {
					writerErr = err
				}
				return
			}
			writerUnits.Add(bootReplayBurstUnits)
		}
	}()

	decide := func() (bootReplayOutcome, error) {
		lastProgress := t0
		for {
			if capture.count(msgSnapshotFileLoaded) > 0 {
				return bootReplayAdopted, nil
			}
			if capture.count(msgSnapshotFileRejected) > 0 {
				reasons := capture.reasons(msgSnapshotFileRejected)
				for _, r := range reasons {
					if r != bootReplayGapReason && !strings.HasPrefix(r, bootReplayOverflowPrefix) {
						return bootReplayGapRejected, fmt.Errorf("repeat %d: boot rejected the file for %q -- only %q or a %q cap overflow is a legitimate outcome of this phase's own writers", repeat, r, bootReplayGapReason, bootReplayOverflowPrefix)
					}
				}
				for _, r := range reasons {
					if strings.HasPrefix(r, bootReplayOverflowPrefix) {
						return bootReplayOverflowRejected, nil
					}
				}
				return bootReplayGapRejected, nil
			}
			if capCtx.Err() != nil {
				return bootReplayGapRejected, fmt.Errorf("repeat %d: timed out waiting for the boot's file attempt to conclude (-cap=%s)", repeat, cfg.cap)
			}
			if time.Since(lastProgress) >= 2*time.Second {
				fmt.Printf("applybench: ... boot-replay repeat %d waiting for the file attempt (%s elapsed, writer at %d units)\n", repeat+1, fmtSeconds(time.Since(t0)), writerUnits.Load())
				lastProgress = time.Now()
			}
			select {
			case <-time.After(10 * time.Millisecond):
			case <-capCtx.Done():
			}
		}
	}

	outcome, err = decide()
	bootDur = time.Since(t0)
	stopWriter()
	<-writerDone
	written = int(writerUnits.Load())
	if err != nil {
		return outcome, 0, 0, written, err
	}
	if writerErr != nil {
		return outcome, 0, 0, written, fmt.Errorf("repeat %d: the writer failed mid-boot: %w", repeat, writerErr)
	}

	if outcome == bootReplayAdopted {
		counts := capture.replayedCounts(msgSnapshotFileLoaded)
		if len(counts) == 0 {
			return outcome, 0, 0, written, fmt.Errorf("repeat %d: %q carried no replayed_writes attribute -- the marker contract changed under this bench", repeat, msgSnapshotFileLoaded)
		}
		replayed = counts[len(counts)-1]
	}
	return outcome, replayed, bootDur, written, nil
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

	// next, when set, is the handler that was installed before this capture
	// took over, and every record is forwarded to it as well (subject to its
	// own Enabled). Without that forwarding, installing a capture would make
	// the driver's ordinary operator-facing warnings vanish for exactly as
	// long as a measurement is running -- which is most of a run now that
	// (a), (b), (c) and (d) all capture.
	next slog.Handler
}

type logEvent struct {
	at          time.Time
	duration    time.Duration
	hasDur      bool
	replayed    int64
	hasReplayed bool
	reason      string
}

func newLogCapture() *logCapture {
	return &logCapture{events: make(map[string][]logEvent)}
}

// installLogCapture swaps slog.Default() for a fresh logCapture teed at
// whatever handler was installed before, returning that capture and the
// restore func a caller must defer.
//
// MUST be called before the driver whose logging is being observed is
// opened: dawgs.Open reads slog.Default() exactly once, to build the
// engine's own Config.Log (driver.go), so a capture installed afterwards
// sees nothing that engine logs.
func installLogCapture() (*logCapture, func()) {
	previous := slog.Default()
	capture := newLogCapture()
	capture.next = previous.Handler()
	slog.SetDefault(slog.New(capture))
	return capture, func() { slog.SetDefault(previous) }
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(ctx context.Context, r slog.Record) error {
	ev := logEvent{at: r.Time}
	r.Attrs(func(a slog.Attr) bool {
		switch {
		case a.Key == "duration" && a.Value.Kind() == slog.KindDuration:
			ev.duration = a.Value.Duration()
			ev.hasDur = true
		case a.Key == "replayed_writes" && a.Value.Kind() == slog.KindInt64:
			ev.replayed = a.Value.Int64()
			ev.hasReplayed = true
		case a.Key == "reason" && a.Value.Kind() == slog.KindString:
			ev.reason = a.Value.String()
		}
		return true
	})
	c.mu.Lock()
	c.events[r.Message] = append(c.events[r.Message], ev)
	c.mu.Unlock()

	if c.next != nil && c.next.Enabled(ctx, r.Level) {
		return c.next.Handle(ctx, r)
	}
	return nil
}

// WithAttrs and WithGroup return the capture unchanged: this handler keys
// everything off the record's own message, never off attributes a derived
// logger would carry, and every consumer of it (the engine's cfg.Log) logs
// through the handler directly rather than deriving a child logger.
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

// replayedCounts returns the "replayed_writes" attribute of every captured
// record matching message that carried one, in capture order -- how (e)
// reads the loaded marker's replay count (internal/engine/boot.go's
// tryLoadSnapshotFile stamps it on "bloodtrail: snapshot file loaded").
func (c *logCapture) replayedCounts(message string) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int64, 0, len(c.events[message]))
	for _, e := range c.events[message] {
		if e.hasReplayed {
			out = append(out, e.replayed)
		}
	}
	return out
}

// reasons returns the "reason" attribute of every captured record matching
// message that carried one, in capture order -- how (e) discriminates the
// one legitimate rejection its own writer can race the boot into from every
// rejection that would be a real defect.
func (c *logCapture) reasons(message string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.events[message]))
	for _, e := range c.events[message] {
		if e.reason != "" {
			out = append(out, e.reason)
		}
	}
	return out
}

// firstAfter returns the time of the earliest captured record matching
// message whose own log timestamp is at or after t -- how (b) finds the
// first write-through Apply that landed inside its sampled window,
// ignoring any that happened during the warm-up before it (splitByFirstApply).
func (c *logCapture) firstAfter(message string, t time.Time) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.events[message] {
		if !e.at.Before(t) {
			return e.at, true
		}
	}
	return time.Time{}, false
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
	deltaP50, deltaP95 := percentile(r.duringDeltaDurations, 0.50), percentile(r.duringDeltaDurations, 0.95)

	overheadOK := overheadPct <= applyOverheadMaxPct
	// Scored on the delta-populated subset, not the whole window: a sample
	// taken before the window's first Apply queried the base snapshot with
	// no delta at all, which is the idle state, not the state (b) is about.
	latencyOK := latencyWithinMultiplier(deltaP95, idleP95, idleP95Multiplier)

	fmt.Printf("\n=== (a) sustained apply throughput ===\n")
	fmt.Printf("applybench: arms are symmetric (neither runs a concurrent reader) and interleaved on/off; engine applied %d time(s) across the on arm\n", r.onApplies)
	fmt.Printf("applybench: engine-on  write wall time p50=%s (n=%d): %s\n", fmtSeconds(onP50), len(r.onDurations), fmtDurationsSeconds(r.onDurations))
	fmt.Printf("applybench: engine-off write wall time p50=%s (n=%d): %s\n", fmtSeconds(offP50), len(r.offDurations), fmtDurationsSeconds(r.offDurations))
	fmt.Printf("applybench: apply+read-back overhead = %.2f%% (max %.2f%%, evidence-based -- see README): %s\n", overheadPct, applyOverheadMaxPct, passFail(overheadOK))
	fmt.Printf("APPLYBENCH_APPLY_THROUGHPUT on_p50_ms=%.3f off_p50_ms=%.3f overhead_pct=%.3f max_pct=%.3f applies=%d ok=%t\n",
		floatMillis(onP50), floatMillis(offP50), overheadPct, applyOverheadMaxPct, r.onApplies, overheadOK)

	fmt.Printf("\n=== (b) query latency during active ingest ===\n")
	fmt.Printf("applybench: idle            p50=%s p95=%s (n=%d, window=%s)\n", fmtMillis(idleP50), fmtMillis(idleP95), len(r.idleDurations), fmtSeconds(r.idleWindow))
	fmt.Printf("applybench: ingest, all     p50=%s p95=%s (n=%d, window=%s)\n", fmtMillis(duringP50), fmtMillis(duringP95), len(r.duringDurations), fmtSeconds(r.duringWindow))
	fmt.Printf("applybench: ingest, delta   p50=%s p95=%s (n=%d of %d samples were taken after the window's first Apply; %d applies, %d engine-served)\n",
		fmtMillis(deltaP50), fmtMillis(deltaP95), len(r.duringDeltaDurations), len(r.duringDurations), r.duringApplies, r.duringServed)
	fmt.Printf("applybench: delta-populated p95 within %.1fx idle p95 (evidence-based -- see README): %s\n", idleP95Multiplier, passFail(latencyOK))
	fmt.Printf("APPLYBENCH_LATENCY_DURING_INGEST idle_p50_ms=%.3f idle_p95_ms=%.3f idle_n=%d idle_window_ms=%.3f during_p50_ms=%.3f during_p95_ms=%.3f during_n=%d during_window_ms=%.3f delta_p50_ms=%.3f delta_p95_ms=%.3f delta_n=%d applies=%d served=%d multiplier=%.3f ok=%t\n",
		floatMillis(idleP50), floatMillis(idleP95), len(r.idleDurations), floatMillis(r.idleWindow),
		floatMillis(duringP50), floatMillis(duringP95), len(r.duringDurations), floatMillis(r.duringWindow),
		floatMillis(deltaP50), floatMillis(deltaP95), len(r.duringDeltaDurations),
		r.duringApplies, r.duringServed, idleP95Multiplier, latencyOK)

	compactionP50 := percentile(r.compactionDurations, 0.50)
	fmt.Printf("\n=== (c) compaction duration (reported only, not enforced) ===\n")
	fmt.Printf("applybench: compactions observed=%d p50=%s: %s\n", r.compactionCount, fmtMillis(compactionP50), fmtDurationsMs(r.compactionDurations))
	fmt.Printf("APPLYBENCH_COMPACTION count=%d p50_ms=%.3f\n", r.compactionCount, floatMillis(compactionP50))

	saveP50 := percentile(r.saveDurations, 0.50)
	saveLoggedP50 := percentile(r.saveLoggedDurations, 0.50)
	loadP50, bootP50 := percentile(r.loadDurations, 0.50), percentile(r.bootDurations, 0.50)
	fmt.Printf("\n=== (d) snapshot file save/load/boot duration (reported only, not enforced) ===\n")
	fmt.Printf("applybench: save, Close wall time      p50=%s: %s\n", fmtMillis(saveP50), fmtDurationsMs(r.saveDurations))
	fmt.Printf("applybench: save, engine fold+write    p50=%s: %s\n", fmtMillis(saveLoggedP50), fmtDurationsMs(r.saveLoggedDurations))
	fmt.Printf("applybench: load, ReadSnapshotFile     p50=%s: %s  (parse only -- excludes the boot path's watermark read and adoption)\n", fmtMillis(loadP50), fmtDurationsMs(r.loadDurations))
	fmt.Printf("applybench: boot, open -> file loaded  p50=%s: %s  (whole real boot path; includes the driver open and up to one 100ms boot-load retry interval)\n", fmtMillis(bootP50), fmtDurationsMs(r.bootDurations))
	fmt.Printf("APPLYBENCH_SNAPSHOT_IO save_p50_ms=%.3f save_logged_p50_ms=%.3f load_p50_ms=%.3f boot_p50_ms=%.3f\n",
		floatMillis(saveP50), floatMillis(saveLoggedP50), floatMillis(loadP50), floatMillis(bootP50))

	replayBootP50 := percentile(r.replayBootDurs, 0.50)
	sustainedBootP50 := percentile(r.sustainedReplayBootDurs, 0.50)
	fmt.Printf("\n=== (e) boot adoption under concurrent writes (reported only, not enforced) ===\n")
	fmt.Printf("applybench: burst:     adopted=%d gap_rejected=%d overflowed=%d of %d boots; replayed_writes per adoption: %v; writer units per boot: %v\n",
		r.replayAdopted, r.replayGapRejected, r.replayOverflowed, r.replayAdopted+r.replayGapRejected+r.replayOverflowed, r.replayCounts, r.replayWritten)
	fmt.Printf("applybench: sustained: adopted=%d gap_rejected=%d overflowed=%d of %d boots; replayed_writes per adoption: %v; writer units per boot: %v\n",
		r.sustainedReplayAdopted, r.sustainedReplayGapRejected, r.sustainedReplayOverflowed, r.sustainedReplayAdopted+r.sustainedReplayGapRejected+r.sustainedReplayOverflowed, r.sustainedReplayCounts, r.sustainedReplayWritten)
	fmt.Printf("applybench: boot under burst writes,     open -> file loaded  p50=%s: %s\n", fmtMillis(replayBootP50), fmtDurationsMs(r.replayBootDurs))
	fmt.Printf("applybench: boot under sustained writes, open -> file loaded  p50=%s: %s\n", fmtMillis(sustainedBootP50), fmtDurationsMs(r.sustainedReplayBootDurs))
	fmt.Printf("APPLYBENCH_BOOT_REPLAY adopted=%d gap_rejected=%d overflowed=%d boot_p50_ms=%.3f max_replayed=%d\n",
		r.replayAdopted, r.replayGapRejected, r.replayOverflowed, floatMillis(replayBootP50), maxInt64(r.replayCounts))
	fmt.Printf("APPLYBENCH_BOOT_REPLAY_SUSTAINED adopted=%d gap_rejected=%d overflowed=%d boot_p50_ms=%.3f max_replayed=%d\n",
		r.sustainedReplayAdopted, r.sustainedReplayGapRejected, r.sustainedReplayOverflowed, floatMillis(sustainedBootP50), maxInt64(r.sustainedReplayCounts))

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
// exactly the kind of false failure a bar anchored to measured physics,
// rather than to an invented number, is meant to avoid.
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

// maxInt64 returns the largest value in vs, or 0 for an empty slice -- (e)'s
// summary line reports the largest per-boot replay count observed, the
// number to compare against the boot gap buffer's own entry cap.
func maxInt64(vs []int64) int64 {
	var m int64
	for _, v := range vs {
		if v > m {
			m = v
		}
	}
	return m
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
