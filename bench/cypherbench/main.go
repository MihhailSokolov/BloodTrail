// SPDX-License-Identifier: Apache-2.0

// Command cypherbench benchmarks BloodTrail's Cypher-interpreter serving
// path (internal/engine.TryCypher, reached through the real driver's
// wrappedTransaction.Query -- see transaction.go) against a graph already
// loaded into PostgreSQL, normally by bench/adgen (see bench/adgen/README.md).
//
// Like bench/builderbench (and unlike bench/pathbench, which constructs
// internal/engine directly), cypherbench opens the *real* production
// driver -- dawgs.Open(ctx, bloodtrail.DriverName, cfg) -- because the only
// way to exercise TryCypher exactly as BloodHound's own cypher endpoint does
// is through graph.Transaction.Query. A second dawgs.Open(ctx, pg.DriverName,
// cfg) on the same DSN and pool is the delegated baseline: plain PostgreSQL,
// no engine at all.
//
// It measures five Cypher shapes from the milestone's spec:
//
//  1. rid_suffix_scan: a RID-suffix property scan, MATCH (n:Group) WHERE
//     n.objectid ENDS WITH '-512' RETURN n -- the same shape BloodHound's
//     own "Domain Admins" selector uses.
//  2. flag_scan: a two-boolean-property AND scan, MATCH (u:User) WHERE
//     u.hasspn = true AND u.enabled = true RETURN u LIMIT 1000.
//  3. objectid_point_lookup: a single-property equality lookup against a
//     real objectid selected from the loaded graph at run time, MATCH (n)
//     WHERE n.objectid = '<sid>' RETURN n -- exercises the snapshot's
//     objectid index rather than a scan.
//  4. shortest_path_prebuilt: BloodHound's own "Shortest paths to Domain
//     Admins" pre-built query, copied verbatim from
//     testdata/prebuilt/agt.json -- a shortestPath alternated over its full
//     ~64-member Active Directory pathfinding edge-kind list (see
//     shape4Text and extractRelKinds).
//  5. collect_antijoin_prebuilt: BloodHound's own "Domain Admins logons to
//     non-Domain Controllers" pre-built query, copied verbatim from the same
//     corpus -- a WITH COLLECT(...) anti-join (NOT c IN exclude) feeding a
//     second MATCH.
//
// For each shape, cypherbench runs one warmup call against each driver
// (bloodtrail then pg), comparing the two drivers' result *row count* as a
// correctness guard, then -runs (default 5) further timed calls against
// each, reporting p50/p95 per driver and the p50 ratio (pg / bloodtrail) --
// how many times faster serving from memory is than delegating to
// PostgreSQL.
//
// Before any shape is measured, cypherbench waits for the bloodtrail
// driver's background poller to build its first in-memory snapshot, exactly
// as builderbench does: see execute's own wait-duration computation for why
// this is measured rather than a fixed sleep.
//
// Usage:
//
//	go run ./bench/cypherbench -dsn <dsn> [-runs 5] [-enforce]
//
// cypherbench never imports bench/adgen (a generator, not a library) and
// never writes to the database (beyond a scratch datapipe_status row it
// creates and drops itself, needed to drive the poller -- see
// createDatapipeStatusTable's doc, duplicated from builderbench for the
// same reason): it finds every adgen kind name purely by duplicating
// bench/adgen/generate.go's constants, the same convention pathbench and
// builderbench follow. shape4Text's own ~64-member edge-kind list is a
// separate matter -- see extractRelKinds' doc for why every kind a query
// alternates over must be asserted into the schema before the query can run
// at all, on *either* driver.
//
// Every shape prints a human-readable line and a machine-greppable
// CYPHERBENCH_<SHAPE> summary line (grep '^CYPHERBENCH_'), ending in
// CYPHERBENCH_RESULT PASS or FAIL. With -enforce, cypherbench exits nonzero
// if any shape fails evaluateShape's decision: shapes 1, 2, 4, and 5 need at
// least enforceRatio (5x); shape 3 (the point lookup) needs only
// pointLookupMinRatio (1x), since a lookup is already fast against an
// indexed jsonb property on the pg side and 5x is not a meaningful bar for
// it. Every shape also requires the two drivers' result row counts to
// match, regardless of ratio. CI must never pass -enforce. Any other
// failure (a database error, an empty graph, a driver that returns an
// outright error) aborts the run with a nonzero exit regardless of
// -enforce.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	bloodtrail "github.com/MihhailSokolov/BloodTrail"
	"github.com/MihhailSokolov/BloodTrail/internal/engine"
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

// enforceRatio is the minimum p50 ratio (delegated pg / served bt) -enforce
// requires of every shape except objectid_point_lookup -- see
// pointLookupMinRatio's doc for that exception.
const enforceRatio = 5.0

// pointLookupMinRatio is objectid_point_lookup's own, weaker, p50 ratio bar.
// A single-row equality lookup against jsonb's own GIN/expression indexing
// on properties->>'objectid' (see schema_up.sql) is already fast on the pg
// side -- there is no full scan or traversal for the engine's in-memory
// objectid index to out-run the way there is for the other four shapes -- so
// the spec only requires the engine not be *slower*, not 5x faster.
const pointLookupMinRatio = 1.0

// Shape names, used both for human-readable reporting and (upper-cased) for
// each shape's CYPHERBENCH_<SHAPE> summary-line tag.
const (
	shapeRIDSuffixScan           = "rid_suffix_scan"
	shapeFlagScan                = "flag_scan"
	shapeObjectIDPointLookup     = "objectid_point_lookup"
	shapeShortestPathPrebuilt    = "shortest_path_prebuilt"
	shapeCollectAntiJoinPrebuilt = "collect_antijoin_prebuilt"
)

// shapeMinRatio holds every shape's -enforce p50 ratio bar, keyed by shape
// name. Every shape execute benchmarks must have an entry here --
// TestMinRatioForKnownShapesHaveEntries (main_test.go) catches a shapeSpec
// added without one; minRatioFor falls back to enforceRatio (the stricter,
// more common bar) and warns on stderr rather than panicking mid-report if
// that maintenance invariant is ever violated at run time.
var shapeMinRatio = map[string]float64{
	shapeRIDSuffixScan:           enforceRatio,
	shapeFlagScan:                enforceRatio,
	shapeObjectIDPointLookup:     pointLookupMinRatio,
	shapeShortestPathPrebuilt:    enforceRatio,
	shapeCollectAntiJoinPrebuilt: enforceRatio,
}

// minRatioFor returns name's shapeMinRatio, warning on stderr and falling
// back to enforceRatio if name has none.
func minRatioFor(name string) float64 {
	if r, ok := shapeMinRatio[name]; ok {
		return r
	}
	fmt.Fprintf(os.Stderr, "cypherbench: WARNING: shape %q has no shapeMinRatio entry; using the default %.1fx bar\n", name, enforceRatio)
	return enforceRatio
}

// shape4Text is BloodHound's own pre-built "Shortest paths to Domain
// Admins" query, copied verbatim (byte-for-byte, including its exact
// newlines) from testdata/prebuilt/agt.json -- see
// TestShape4And5TextsAreVerbatim for the regression test proving it stays
// that way. It alternates over graphSchema.ts's full ~64-member Active
// Directory pathfinding edge-kind list; extractRelKinds pulls that list
// back out of this same constant (see its own doc) rather than having it
// hand-transcribed a second time.
const shape4Text = `MATCH p=shortestPath((t:Group)<-[:Owns|GenericAll|GenericWrite|WriteOwner|WriteDacl|MemberOf|ForceChangePassword|AllExtendedRights|AddMember|HasSession|GPLink|AllowedToDelegate|CoerceToTGT|AllowedToAct|AdminTo|CanPSRemote|CanRDP|ExecuteDCOM|HasSIDHistory|AddSelf|DCSync|ReadLAPSPassword|ReadGMSAPassword|DumpSMSAPassword|SQLAdmin|AddAllowedToAct|WriteSPN|AddKeyCredentialLink|SyncLAPSPassword|WriteAccountRestrictions|WriteGPLink|GoldenCert|ADCSESC1|ADCSESC3|ADCSESC4|ADCSESC6a|ADCSESC6b|ADCSESC9a|ADCSESC9b|ADCSESC10a|ADCSESC10b|ADCSESC13|SyncedToADUser|CoerceAndRelayNTLMToSMB|CoerceAndRelayNTLMToADCS|WriteOwnerLimitedRights|OwnsLimitedRights|ClaimSpecialIdentity|CoerceAndRelayNTLMToLDAP|CoerceAndRelayNTLMToLDAPS|ContainsIdentity|PropagatesACEsTo|GPOAppliesTo|CanApplyGPO|HasTrustKeys|WriteAltSecurityIdentities|WritePublicInformation|ManageCA|ManageCertificates|Contains|DCFor|SameForestTrust|SpoofSIDHistory|AbuseTGTDelegation*1..]-(s:Base))
WHERE t.objectid ENDS WITH '-512' AND s<>t
RETURN p
LIMIT 1000`

// shape5Text is BloodHound's own pre-built "Domain Admins logons to
// non-Domain Controllers" query, copied verbatim from
// testdata/prebuilt/agt.json -- the milestone's required "COLLECT
// anti-join" shape: a WITH COLLECT(...) result feeding a second MATCH's NOT
// ... IN exclusion. Both its edge kinds (MemberOf, HasSession) are already
// in edgeKindNames, so unlike shape4Text it needs no extra kind assertion.
const shape5Text = `MATCH (s)-[:MemberOf*0..]->(g:Group)
WHERE g.objectid ENDS WITH '-516'
WITH COLLECT(s) AS exclude
MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group)
WHERE g.objectid ENDS WITH '-512' AND NOT c IN exclude
RETURN p
LIMIT 1000`

// relKindsPattern matches a Cypher relationship-type alternation
// immediately followed by a variable-length range's opening ("*"), e.g.
// "[:Owns|GenericAll|...|AbuseTGTDelegation*1..]" -- capturing group 1 is
// the pipe-separated kind list.
var relKindsPattern = regexp.MustCompile(`\[:([A-Za-z0-9|]+)\*`)

// extractRelKinds returns the pipe-separated relationship-kind names inside
// text's first "[:A|B|C*" alternation, in order, or nil if text has none.
//
// Every kind a Cypher MATCH pattern references -- including one alternated
// over inside a variable-length relationship, as shape4Text does -- must be
// asserted into the schema before the query is run on *either* driver:
// dawgs' pgsql translator fails the entire query ("unable to map kinds") the
// instant even one alternative was never declared, degrading only to
// "no rows" for a declared-but-unused kind, never for an undeclared one --
// see internal/graphtest/corpusfixture.go's identical rationale for its own
// corpusEdgeKindNames, which this same ~64-member list is drawn from.
// Deriving the list from shape4Text itself, rather than transcribing it a
// second time by hand, means the embedded query text and the kinds
// execute asserts into the schema can never silently drift apart.
func extractRelKinds(text string) []string {
	m := relKindsPattern.FindStringSubmatch(text)
	if m == nil {
		return nil
	}
	return strings.Split(m[1], "|")
}

// enginePollInterval is set via BLOODTRAIL_ENGINE_POLL_INTERVAL before
// opening the bloodtrail driver -- identical to builderbench's own constant
// and rationale.
const enginePollInterval = 200 * time.Millisecond

func main() {
	os.Exit(run(os.Args[1:]))
}

// run parses flags, executes the benchmark, prints its report, and returns
// the process exit code -- mirroring pathbench's and builderbench's
// identical run/main split.
func run(args []string) int {
	fs := flag.NewFlagSet("cypherbench", flag.ContinueOnError)
	var (
		dsn     = fs.String("dsn", "", "PostgreSQL connection string, e.g. postgresql://user:pass@host:port/db")
		runs    = fs.Int("runs", 5, "number of warmed-up, timed runs per shape per driver")
		enforce = fs.Bool("enforce", false, "exit nonzero if any shape fails its per-shape enforce threshold (never pass this in CI)")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "cypherbench: -dsn is required")
		return 2
	}
	if *runs <= 0 {
		fmt.Fprintln(os.Stderr, "cypherbench: -runs must be positive")
		return 2
	}

	result, err := execute(context.Background(), config{dsn: *dsn, runs: *runs})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cypherbench: %v\n", err)
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

// shapeSpec is one benchmarked Cypher shape: a name and the query text to
// run against both drivers.
type shapeSpec struct {
	name string
	text string
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

func (r shapeResult) btP50P95() (time.Duration, time.Duration) {
	return percentile(r.btDurations, 0.50), percentile(r.btDurations, 0.95)
}

func (r shapeResult) pgP50P95() (time.Duration, time.Duration) {
	return percentile(r.pgDurations, 0.50), percentile(r.pgDurations, 0.95)
}

// ratioOf is the delegated/served p50 speed ratio: how many times faster
// btP50 is than pgP50. A btP50 of zero (never observed in practice, but
// guarded rather than divided by) reports +Inf unless pgP50 is also zero,
// in which case the two are declared equal (ratio 1). Pure: no I/O, no
// globals -- duplicated from bench/builderbench's identical helper (each
// bench tool is self-contained, per the package doc).
func ratioOf(btP50, pgP50 time.Duration) float64 {
	if btP50 <= 0 {
		if pgP50 <= 0 {
			return 1
		}
		return math.Inf(1)
	}
	return float64(pgP50) / float64(btP50)
}

// evaluateShape is -enforce's pure per-shape decision function: given one
// shape's measured p50 durations, whether the two drivers' result row
// counts matched, and the minRatio it must clear, it returns whether the
// shape passes and, when it does not, the specific reasons why. Pure: no
// I/O, no globals -- table-tested directly against synthetic durations in
// main_test.go's TestEvaluateShape, mirroring bench/builderbench's
// evaluateShape one level simpler (no pg-capping concept here: none of
// cypherbench's five shapes are expected to run long enough at any
// realistic scale to need one -- see the package doc).
func evaluateShape(btP50, pgP50 time.Duration, match bool, minRatio float64) (ok bool, reasons []string) {
	if !match {
		reasons = append(reasons, "bt/pg result row count mismatch")
	}
	if ratio := ratioOf(btP50, pgP50); ratio < minRatio {
		reasons = append(reasons, fmt.Sprintf("p50 ratio %.2fx below required %.2fx", ratio, minRatio))
	}
	return len(reasons) == 0, reasons
}

// benchResult accumulates every phase's measurements for report to print.
type benchResult struct {
	buildDuration time.Duration
	buildNodes    int
	buildEdges    int
	buildBytes    uint64

	pollInterval time.Duration
	waitDuration time.Duration

	shapes []shapeResult

	rebuildDuration time.Duration
	rebuildNodes    int
	rebuildEdges    int
	rebuildBytes    uint64
}

// execute runs the whole benchmark against cfg.dsn and returns the
// collected measurements. Any error here is a setup/infrastructure failure
// and always aborts the run with a nonzero exit, regardless of -enforce --
// -enforce only gates benchResult.report's per-shape ratio/match checks.
func execute(ctx context.Context, cfg config) (*benchResult, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pg.NewPool(poolCfg)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}
	// bt and oracle (opened below via dawgs.Open) share this one pool and are
	// deliberately never Close()'d themselves -- see builderbench's identical
	// defer for the full rationale (*pg.Driver.Close in dawgs v0.8.0 closes
	// the whole shared pool, not just its own bookkeeping).
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

	// Every kind name any benchmarked shape's Cypher text references must be
	// asserted into the (global, not per-graph) kind table before either
	// driver runs a single query -- see extractRelKinds' doc. allKinds is
	// the union of adgen's own node/edge kinds (already used by shapes 1-3
	// and shape5) and shape4Text's ~64-member edge-kind alternation (shape4);
	// AssertKinds is idempotent for names already present.
	allKinds := append(stringKinds(nodeKindNames), stringKinds(edgeKindNames)...)
	allKinds = append(allKinds, stringKinds(extractRelKinds(shape4Text))...)
	if _, err := setupDriver.KindMapper().AssertKinds(ctx, allKinds); err != nil {
		return nil, fmt.Errorf("assert kinds: %w", err)
	}

	result := &benchResult{}

	fmt.Println("cypherbench: measuring snapshot build cost (also the wait budget below) ...")
	buildStart := time.Now()
	snap, err := engine.LoadSnapshot(ctx, setupDriver, pool)
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	result.buildDuration = time.Since(buildStart)
	result.buildNodes = snap.NodeCount()
	result.buildEdges = snap.EdgeCount()
	result.buildBytes = snap.ApproxBytes()
	fmt.Printf("cypherbench: measured build: nodes=%d edges=%d approx_bytes=%d (%s) duration=%s\n",
		result.buildNodes, result.buildEdges, result.buildBytes, fmtBytes(result.buildBytes), fmtSeconds(result.buildDuration))
	fmt.Printf("CYPHERBENCH_BUILD nodes=%d edges=%d approx_bytes=%d duration_ms=%.3f\n",
		result.buildNodes, result.buildEdges, result.buildBytes, floatMillis(result.buildDuration))

	if err := createDatapipeStatusTable(ctx, pool); err != nil {
		return nil, fmt.Errorf("create datapipe_status: %w", err)
	}
	defer func() {
		if err := dropDatapipeStatusTable(ctx, pool); err != nil {
			fmt.Fprintf(os.Stderr, "cypherbench: drop datapipe_status: %v\n", err)
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

	// See builderbench's identical wait: there is no exported hook to
	// observe the bloodtrail driver's poller finishing its first build, so
	// this waits a safety multiple of the just-measured build cost plus the
	// poller's own poll interval instead.
	waitDuration := 2*enginePollInterval + 2*result.buildDuration + 500*time.Millisecond
	if waitDuration < time.Second {
		waitDuration = time.Second
	}
	result.waitDuration = waitDuration
	fmt.Printf("cypherbench: waiting %s for the bloodtrail driver's poller to build its first snapshot (poll_interval=%s) ...\n",
		waitDuration, enginePollInterval)
	time.Sleep(waitDuration)
	fmt.Printf("CYPHERBENCH_WAIT duration_ms=%.3f poll_interval_ms=%.3f\n", floatMillis(waitDuration), floatMillis(enginePollInterval))

	pointLookupText, pointLookupObjectID, err := buildPointLookupText(ctx, pool, graphModel.ID)
	if err != nil {
		return nil, fmt.Errorf("select point-lookup objectid: %w", err)
	}
	fmt.Printf("cypherbench: objectid_point_lookup target: objectid=%s\n", pointLookupObjectID)

	shapes := []shapeSpec{
		{name: shapeRIDSuffixScan, text: `MATCH (n:Group) WHERE n.objectid ENDS WITH '-512' RETURN n`},
		{name: shapeFlagScan, text: `MATCH (u:User) WHERE u.hasspn = true AND u.enabled = true RETURN u LIMIT 1000`},
		{name: shapeObjectIDPointLookup, text: pointLookupText},
		{name: shapeShortestPathPrebuilt, text: shape4Text},
		{name: shapeCollectAntiJoinPrebuilt, text: shape5Text},
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

	fmt.Printf("\n=== rebuild time ===\n")
	rebuildStart := time.Now()
	snap2, err := engine.LoadSnapshot(ctx, setupDriver, pool)
	if err != nil {
		return nil, fmt.Errorf("rebuild snapshot: %w", err)
	}
	result.rebuildDuration = time.Since(rebuildStart)
	result.rebuildNodes = snap2.NodeCount()
	result.rebuildEdges = snap2.EdgeCount()
	result.rebuildBytes = snap2.ApproxBytes()
	fmt.Printf("cypherbench: rebuild: nodes=%d edges=%d approx_bytes=%d (%s) duration=%s\n",
		result.rebuildNodes, result.rebuildEdges, result.rebuildBytes, fmtBytes(result.rebuildBytes), fmtSeconds(result.rebuildDuration))
	fmt.Printf("CYPHERBENCH_REBUILD nodes=%d edges=%d approx_bytes=%d duration_ms=%.3f\n",
		result.rebuildNodes, result.rebuildEdges, result.rebuildBytes, floatMillis(result.rebuildDuration))

	return result, nil
}

// buildPointLookupText picks a real User node's objectid from graphID (the
// one nearest the middle of the id range, an arbitrary but stable choice --
// any ordinary node works equally well for a point lookup) and returns the
// shape 3 query text with that objectid spelled in as a literal, plus the
// objectid itself for the report line.
func buildPointLookupText(ctx context.Context, pool *pgxpool.Pool, graphID int32) (text string, objectID string, err error) {
	var count int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM node WHERE graph_id = $1 AND properties ? 'objectid'",
		graphID,
	).Scan(&count); err != nil {
		return "", "", fmt.Errorf("count nodes with objectid: %w", err)
	}
	if count == 0 {
		return "", "", fmt.Errorf("no nodes with an objectid property found in graph_id=%d", graphID)
	}

	if err := pool.QueryRow(ctx,
		"SELECT properties->>'objectid' FROM node WHERE graph_id = $1 AND properties ? 'objectid' ORDER BY id OFFSET $2 LIMIT 1",
		graphID, count/2,
	).Scan(&objectID); err != nil {
		return "", "", fmt.Errorf("select objectid at offset %d: %w", count/2, err)
	}

	return fmt.Sprintf(`MATCH (n) WHERE n.objectid = '%s' RETURN n`, objectID), objectID, nil
}

// measureShape runs spec once against bt and once against oracle as warmup,
// comparing their result row counts for spec.name's correctness check, then
// runs runs further timed calls against each in turn (bt block, then oracle
// block) -- collecting only their durations, since the correctness check
// already ran once above.
func measureShape(ctx context.Context, spec shapeSpec, bt, oracle graph.Database, runs int) (shapeResult, error) {
	result := shapeResult{name: spec.name}

	_, btSize, err := runCypherOnce(ctx, bt, spec.text)
	if err != nil {
		return result, fmt.Errorf("bloodtrail warmup: %w", err)
	}
	result.btSize = btSize

	_, pgSize, err := runCypherOnce(ctx, oracle, spec.text)
	if err != nil {
		return result, fmt.Errorf("pg warmup: %w", err)
	}
	result.pgSize = pgSize
	result.match = btSize == pgSize

	for i := 0; i < runs; i++ {
		d, _, err := runCypherOnce(ctx, bt, spec.text)
		if err != nil {
			return result, fmt.Errorf("bloodtrail run %d: %w", i, err)
		}
		result.btDurations = append(result.btDurations, d)
	}

	for i := 0; i < runs; i++ {
		d, _, err := runCypherOnce(ctx, oracle, spec.text)
		if err != nil {
			return result, fmt.Errorf("pg run %d: %w", i, err)
		}
		result.pgDurations = append(result.pgDurations, d)
	}

	return result, nil
}

// runCypherOnce runs text against db once via graph.Transaction.Query
// (exactly BloodHound's own cypher endpoint's call shape -- see
// wrappedTransaction.Query), fully draining the returned graph.Result and
// counting its rows as the shape's cheap, comparable size metric. Returns
// the wall-clock duration of the whole read transaction (open, query,
// drain, commit), not just a first-row latency.
func runCypherOnce(ctx context.Context, db graph.Database, text string) (time.Duration, int64, error) {
	var count int64
	t0 := time.Now()
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(text, nil)
		defer result.Close()
		for result.Next() {
			count++
		}
		return result.Error()
	})
	return time.Since(t0), count, err
}

// report prints every shape's enforcement line and the final PASS/FAIL
// line, returning whether every shape satisfied evaluateShape (independent
// of whether -enforce was actually passed -- run decides whether that
// return value changes the exit code).
func (r *benchResult) report(enforce bool) bool {
	allOK := true

	fmt.Printf("\n=== enforce thresholds (checked=%t) ===\n", enforce)
	for _, s := range r.shapes {
		minRatio := minRatioFor(s.name)
		btp50, btp95 := s.btP50P95()
		pgp50, pgp95 := s.pgP50P95()
		ratio := ratioOf(btp50, pgp50)
		ok, reasons := evaluateShape(btp50, pgp50, s.match, minRatio)
		allOK = allOK && ok

		fmt.Printf("cypherbench: %-26s ratio=%7.2fx match=%-5t min_ratio=%.1fx %s\n",
			s.name, ratio, s.match, minRatio, passFail(ok))
		for _, reason := range reasons {
			fmt.Printf("cypherbench:   - %s\n", reason)
		}
		fmt.Printf("CYPHERBENCH_%s bt_p50_ms=%.3f bt_p95_ms=%.3f pg_p50_ms=%.3f pg_p95_ms=%.3f ratio=%.3f min_ratio=%.2f match=%t bt_size=%d pg_size=%d ok=%t\n",
			strings.ToUpper(s.name), floatMillis(btp50), floatMillis(btp95), floatMillis(pgp50), floatMillis(pgp95), ratio, minRatio, s.match, s.btSize, s.pgSize, ok)
	}

	fmt.Printf("CYPHERBENCH_ENFORCE checked=%t ok=%t\n", enforce, allOK)
	if allOK {
		fmt.Println("CYPHERBENCH_RESULT PASS")
	} else {
		fmt.Println("CYPHERBENCH_RESULT FAIL")
	}
	return allOK
}

// printShapeResult prints one shape's human-readable detail: both drivers'
// result row count and whether they matched, every timed run's duration,
// and the p50/p95/ratio summary.
func printShapeResult(r shapeResult) {
	fmt.Printf("cypherbench: %s: bt_size=%d pg_size=%d match=%t\n", r.name, r.btSize, r.pgSize, r.match)
	fmt.Printf("cypherbench: %s: bt runs (ms): %s\n", r.name, fmtDurationsMs(r.btDurations))
	fmt.Printf("cypherbench: %s: pg runs (ms): %s\n", r.name, fmtDurationsMs(r.pgDurations))

	btp50, btp95 := r.btP50P95()
	pgp50, pgp95 := r.pgP50P95()
	fmt.Printf("cypherbench: %s: bt p50=%s p95=%s | pg p50=%s p95=%s | ratio(pg/bt)=%.2fx\n",
		r.name, fmtMillis(btp50), fmtMillis(btp95), fmtMillis(pgp50), fmtMillis(pgp95), ratioOf(btp50, pgp50))
}

// createDatapipeStatusTable, insertDatapipeStatus, and
// dropDatapipeStatusTable manage a scratch datapipe_status row the
// bloodtrail driver's poller reads on every tick -- duplicated verbatim
// from bench/builderbench (a non-test main package cannot import another
// main package or a _test.go helper); see builderbench's identical
// functions for the full provenance note and the "dedicated benchmark
// database only" warning.
func createDatapipeStatusTable(ctx context.Context, pool *pgxpool.Pool) error {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('datapipe_status') IS NOT NULL").Scan(&exists); err != nil {
		return fmt.Errorf("check datapipe_status pre-existence: %w", err)
	}
	if exists {
		return fmt.Errorf("datapipe_status table already exists; cypherbench requires a dedicated benchmark database loaded by adgen, never a real BloodHound installation (the deferred DROP would destroy live pipeline state)")
	}

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

// stringKinds converts kind name strings to graph.Kinds, matching
// bench/adgen's, bench/pathbench's, and bench/builderbench's own helper of
// the same name/shape.
func stringKinds(names []string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// percentile returns durations' p-th percentile (nearest-rank method, 0 <
// p <= 1), sorting a copy so the caller's slice order is undisturbed. An
// empty input returns zero. Duplicated from pathbench/builderbench (each
// bench tool is self-contained -- see the package doc).
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
	return "[" + strings.Join(parts, " ") + "]"
}

// init silences the bloodtrail driver's own default logging, matching
// builderbench's identical init (see its doc for why this must run before
// main).
func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
}
