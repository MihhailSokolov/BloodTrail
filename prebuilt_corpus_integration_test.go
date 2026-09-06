// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file is Task 14 of the milestone-4 plan: count/shape assertions over
// the pre-built Cypher query corpus extracted from an upstream BloodHound CE
// checkout (scripts/extract-prebuilt-queries.go) into testdata/prebuilt/
// {agt,agi,selectors}.json -- see testdata/prebuilt/NOTICE for provenance
// and that script's package doc for exactly what each file holds and why.
//
// The loader helpers below (loadCommonSearches/loadSelectors and the
// commonSearchEntry/selectorEntry types) are deliberately factored out for
// Task 16's differential suite to build on top of: this file only asserts
// the corpus' *shape* -- exact counts, the disabled/probe markers, the
// absence of Cypher bind parameters (this is a fixed, canned corpus of demo
// queries, never caller-parameterized), and syntactic validity via the same
// frontend.ParseCypher the engine's own interpreter uses (see
// write_observer.go's parseCypherFrontend). Task 16 will additionally run
// every enabled query against a live engine/oracle pair.
//
// File placement mirrors dawgs_corpus_integration_test.go and
// builder_differential_matrix_integration_test.go: package bloodtrail, not
// bloodtrail_test, even though this file itself never touches Driver's
// unexported engine field -- Task 16's differential suite almost certainly
// will (to force a deterministic snapshot rebuild the way those two files
// do), and there is no benefit to churning the package declaration once
// this file already has line-item history under the other name.
package bloodtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// commonSearchEntry mirrors one entry of testdata/prebuilt/agt.json or
// agi.json -- see scripts/extract-prebuilt-queries.go's identically shaped
// type for the extraction side.
type commonSearchEntry struct {
	Subheader string `json:"subheader"`
	Name      string `json:"name"`
	Query     string `json:"query"`
	Disabled  bool   `json:"disabled"`
	Probe     bool   `json:"probe,omitempty"`
}

// selectorEntry mirrors one entry of testdata/prebuilt/selectors.json.
type selectorEntry struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// loadCommonSearches loads one of testdata/prebuilt/{agt,agi}.json.
func loadCommonSearches(t *testing.T, path string) []commonSearchEntry {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var entries []commonSearchEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return entries
}

// loadSelectors loads testdata/prebuilt/selectors.json.
func loadSelectors(t *testing.T, path string) []selectorEntry {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var entries []selectorEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return entries
}

// cypherParamPattern matches genuine Cypher bind-parameter syntax ("$" then
// an identifier character), as opposed to a bare "$" appearing incidentally
// inside a string literal. Two corpus queries end a regex pattern with a
// literal "$" end-anchor (e.g. an AGT/AGI query matching high-privileged
// role display names via '(?i)(...|Privileged Role Administrator).*$'), and
// one selector's cypher matches the literal AD computer-account suffix
// 'AZUREADSSOACC$' -- neither is a parameter reference, and a naive
// substring check for "$" would misflag both.
var cypherParamPattern = regexp.MustCompile(`\$[A-Za-z_]`)

// assertNoParams fails the test if any query in queries references a Cypher
// bind parameter -- this corpus is a fixed, canned set of demo queries with
// no caller-supplied parameters, by design.
func assertNoParams(t *testing.T, label string, queries []string) {
	t.Helper()

	for i, q := range queries {
		if cypherParamPattern.MatchString(q) {
			t.Errorf("%s[%d]: query contains a Cypher parameter reference: %s", label, i, q)
		}
	}
}

// assertParses fails the test if query does not parse as valid Cypher.
func assertParses(t *testing.T, label, query string) {
	t.Helper()

	if _, err := frontend.ParseCypher(frontend.DefaultCypherContext(), query); err != nil {
		t.Errorf("%s: expected query to parse, got error: %v\nquery: %s", label, err, query)
	}
}

// assertFailsToParse fails the test if query parses successfully -- used
// for the one deliberately malformed "probe" entry (UncommonSearches'
// "Query Parse Error", carried in agt.json with Probe: true).
func assertFailsToParse(t *testing.T, label, query string) {
	t.Helper()

	if _, err := frontend.ParseCypher(frontend.DefaultCypherContext(), query); err == nil {
		t.Errorf("%s: expected query to fail to parse, but it parsed: %s", label, query)
	}
}

// TestPrebuiltCorpusCounts is milestone 4's task-14 exit criterion: the
// pre-built query corpus extracted from upstream BloodHound CE has the
// exact shape task-14's brief documents --
//
//   - agt.json: 92 entries total -- the 91-entry CommonSearches export (one
//     disabled, the many-to-many "Shortest paths from Owned objects to Tier
//     Zero" query) plus the single UncommonSearches probe entry folded in
//     alongside them;
//   - agi.json: 91 entries, one disabled (the same many-to-many query);
//   - selectors.json: 42 seeded cypher tier-zero selectors --
//
// no query anywhere in the corpus references a Cypher bind parameter, and
// every non-disabled query parses as valid Cypher via frontend.ParseCypher
// except the probe, which must fail to parse.
func TestPrebuiltCorpusCounts(t *testing.T) {
	agt := loadCommonSearches(t, "testdata/prebuilt/agt.json")
	agi := loadCommonSearches(t, "testdata/prebuilt/agi.json")
	selectors := loadSelectors(t, "testdata/prebuilt/selectors.json")

	t.Run("agt", func(t *testing.T) {
		const wantTotal = 92 // 91 CommonSearches (1 disabled) + 1 probe
		if len(agt) != wantTotal {
			t.Fatalf("got %d entries, want %d", len(agt), wantTotal)
		}

		var (
			disabled, probes int
			queries          []string
		)
		for _, e := range agt {
			queries = append(queries, e.Query)
			if e.Disabled {
				disabled++
			}
			if e.Probe {
				probes++
			}
		}
		if disabled != 1 {
			t.Errorf("got %d disabled entries, want 1", disabled)
		}
		if probes != 1 {
			t.Errorf("got %d probe entries, want 1", probes)
		}

		assertNoParams(t, "agt", queries)

		for _, e := range agt {
			switch {
			case e.Probe:
				assertFailsToParse(t, "agt probe "+e.Name, e.Query)
			case e.Disabled:
				// Extracted for the record, never executed or parse-checked:
				// its query text is a block of "// ..." comment lines, not
				// valid Cypher on its own (see extract-prebuilt-queries.go's
				// isCommentedOut).
			default:
				assertParses(t, "agt "+e.Name, e.Query)
			}
		}
	})

	t.Run("agi", func(t *testing.T) {
		const wantTotal = 91
		if len(agi) != wantTotal {
			t.Fatalf("got %d entries, want %d", len(agi), wantTotal)
		}

		var (
			disabled int
			queries  []string
		)
		for _, e := range agi {
			queries = append(queries, e.Query)
			if e.Disabled {
				disabled++
			}
			if e.Probe {
				t.Errorf("agi.json entry %q unexpectedly marked probe", e.Name)
			}
		}
		if disabled != 1 {
			t.Errorf("got %d disabled entries, want 1", disabled)
		}

		assertNoParams(t, "agi", queries)

		for _, e := range agi {
			if e.Disabled {
				continue
			}
			assertParses(t, "agi "+e.Name, e.Query)
		}
	})

	t.Run("selectors", func(t *testing.T) {
		const want = 42
		if len(selectors) != want {
			t.Fatalf("got %d selectors, want %d", len(selectors), want)
		}

		var queries []string
		for _, s := range selectors {
			queries = append(queries, s.Query)
		}
		assertNoParams(t, "selectors", queries)

		for _, s := range selectors {
			assertParses(t, "selector "+s.Name, s.Query)
		}
	})
}

// --- Task 16: the pre-built corpus differential suite ---
//
// This is milestone 4's exit criterion: every active (non-disabled,
// non-probe) query across agt.json, agi.json, and selectors.json -- 222
// entries -- runs against both the bloodtrail driver (served, if at all,
// from a snapshot forced fresh over Task 15's corpus fixture) and a raw pg
// driver oracle on the same database, comparing results and asserting the
// bloodtrail engine actually served the query rather than silently
// delegating. The one deliberately malformed "probe" entry gets its own
// subtest, asserting both drivers fail identically.
//
// # File placement: package bloodtrail, not bloodtrail_test
//
// Task 14's own report already flagged this file for exactly this reason:
// this suite needs a deterministic, on-demand snapshot rebuild
// (d.engine.RebuildNow) after seeding the fixture, the same unexported
// access dawgs_corpus_integration_test.go, builder_differential_matrix_
// integration_test.go, and staleness_integration_test.go already rely on --
// which requires living in package bloodtrail, where Driver's own
// `engine *engine.Engine` field is visible.
//
// Being in this package also means the log-capture plumbing
// (lockedBuffer/installLogCapture/markerCount, and the servedMarker/
// builderServedMarker constants) staleness_integration_test.go already
// defines here is directly reusable, with no need to re-duplicate it a
// third time the way engine_serving_integration_test.go (a different
// package, bloodtrail_test) had to.

// cypherServedMarker is the exact Debug-level message engine.TryCypher
// (internal/engine/engine.go's cypherServedLogMessage) logs whenever it
// serves a Cypher-text query entirely from the interpreter/snapshot --
// duplicated here (rather than exported from internal/engine) for the same
// cross-package reason servedMarker/builderServedMarker are duplicated in
// staleness_integration_test.go. This is the first suite in package
// bloodtrail to issue raw Cypher-text queries through a live driver, so the
// first to need it.
const cypherServedMarker = "bloodtrail: cypher engine served"

// declineReasonPattern extracts the "reason=<token>" attribute
// engine.decline's shared "bloodtrail: path engine declined" log line
// carries (internal/engine/engine.go) -- every reason constant in that
// package is a single space-free token (e.g. "translate_gate"), so a plain
// slog text-handler render always prints it unquoted.
var declineReasonPattern = regexp.MustCompile(`reason=(\S+)`)

// declineReason returns the reason attribute of the most recent
// "bloodtrail: path engine declined" line within logTail -- the slice of
// the captured log written during exactly one query call (see
// TestPrebuiltCorpusDifferential's before/after buffer slicing) -- for a
// failure message that names *why* the engine declined without a reader
// having to go dig through the full captured log by hand.
func declineReason(logTail string) string {
	matches := declineReasonPattern.FindAllStringSubmatch(logTail, -1)
	if len(matches) == 0 {
		return fmt.Sprintf("(no decline reason found in log tail: %q)", logTail)
	}
	return matches[len(matches)-1][1]
}

// expectedDelegations lists corpus queries allowed to delegate to
// PostgreSQL (decline bloodtrail's in-memory Cypher interpreter) instead of
// being served, keyed by "<source>/<name>" (source is "agt", "agi", or
// "selector"; name is the entry's own "name" field). Every entry must carry
// a one-line reason. The milestone target is an empty map -- see
// task-16-report.md for the investigation behind each entry below; none of
// these is a trivial fix (this suite's one budgeted inline-fix round went
// to the two genuine bugs task-16-report.md documents fixing elsewhere
// instead), so each is a real, structural gap flagged for the controller
// rather than silently patched.
var expectedDelegations = map[string]string{
	// interpret's Plan/Execute (internal/engine/interpret) has no handling
	// anywhere for cypher.PatternPredicate at all (confirmed by grep: zero
	// references in the package) -- a bare relationship pattern used as a
	// WHERE-clause boolean predicate, e.g. "WHERE (n)-[:Kind]-(m)", is an
	// entire unimplemented Cypher feature, not a bug to patch inline.
	"agt/Cross-forest trusts with abusable configuration": "WHERE-clause pattern predicate (cypher.PatternPredicate) has no interpret support at all",
	"agi/Cross-forest trusts with abusable configuration": "WHERE-clause pattern predicate (cypher.PatternPredicate) has no interpret support at all",

	// eval.go's applyArithmetic has a doc comment stating plainly: "Cypher
	// also defines `+` for string/list concatenation, but the brief scopes
	// this evaluator to numeric arithmetic only" -- a deliberate, already-
	// documented scope boundary from an earlier milestone-4 task, not
	// something this suite should silently widen. plan.go's checkArithmetic
	// does not type-check the '+' operator's operands (mirrors eval.go's
	// own folding, per its doc), so this query plans successfully and fails
	// at runtime on its first WHERE evaluation
	// ('CN=ADMINSDHOLDER,...' + n.distinguishedname), which
	// applyArithmetic reports as ErrUnsupported -> reasonUnsupported.
	"selector/AdminSDHolder": "WHERE clause concatenates strings via '+'; eval.go's applyArithmetic is deliberately numeric-only",

	// MATCH p=shortestPath((s)-[:<~64 AD pathfinding kinds>*1..]->(t:Tag_Tier_Zero))
	// WHERE s<>t -- s is completely unconstrained (every node in the graph
	// is a candidate source), the path length is unbounded, and the
	// alternation spans nearly every AD relationship kind BloodHound
	// defines. Even against this suite's ~300-node/~950-edge fixture,
	// interpret.Execute's fixed MaxWork budget (internal/engine/
	// serve_cypher.go's maxCypherWork, 1<<28) is exceeded -- a genuine
	// multi-source, unbounded-hop pathfinding scale limit of the current
	// executor, not a fixture or correctness bug. Raising the constant
	// blindly to paper over one worst-case corpus query, with no broader
	// perf investigation, was judged out of scope for this suite's one
	// budgeted inline-fix round.
	"agt/Shortest paths to Tier Zero / High Value targets": "unconstrained multi-source unbounded-length shortestPath exceeds interpret.Execute's MaxWork budget",
	"agi/Shortest paths to Tier Zero / High Value targets": "unconstrained multi-source unbounded-length shortestPath exceeds interpret.Execute's MaxWork budget",
}

// knownAmbiguousQueries lists corpus queries whose shortestPath(...) this
// fixture happens to make genuinely ambiguous for at least one qualifying
// (s, t) pair: more than one equal-length path exists, so which one either
// engine returns is implementation-defined by Cypher's own semantics, not a
// correctness question. Both "Shortest paths to privileged roles" and
// "Shortest paths to Azure Subscriptions" search from every AZBase node,
// unconstrained, and this fixture's azTenant1 (an AZBase node in its own
// right, so a valid `s`) has an AZContains edge to *both* azUser1 and
// azgroup1 -- each exactly one more hop from the query's target
// (azrole:ga, the fixture's only reachable privileged AZRole / its only
// AZSubscription route) via AZHasRole/AZRoleEligible/AZContributor. Both
// two-hop routes are equally short, so bloodtrail and the pg oracle are
// each free to (and empirically do) pick a different one of the tied
// first hops -- confirmed by inspecting the actual edge id mismatch against
// the seeded fixture data (see task-16-report.md) rather than assumed.
//
// Queries in this map skip the edge-id-set and literal comparisons
// (assertCorpusResultsMatch) and are instead compared by node id set only
// -- still a meaningful check (every node either engine's path collection
// reaches must be reachable to the other, tie or not), just not one that
// depends on which of several equally-valid edges either engine happened
// to choose. This is a second, narrower allowlist than expectedDelegations
// on purpose: these queries *are* served correctly by the engine (no
// decline, no serving bug) -- only the suite's default exact-edge-identity
// comparison is too strict for a query whose own semantics permit more
// than one right answer.
var knownAmbiguousQueries = map[string]string{
	"agt/Shortest paths to privileged roles":    "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
	"agi/Shortest paths to privileged roles":    "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
	"agt/Shortest paths to Azure Subscriptions": "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
	"agi/Shortest paths to Azure Subscriptions": "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
}

// corpusQuery is one entry TestPrebuiltCorpusDifferential runs, pulled from
// agt.json/agi.json (commonSearchEntry) or selectors.json (selectorEntry)
// and flattened to a common shape.
type corpusQuery struct {
	source string
	name   string
	query  string
}

// activeCorpusQueries returns every non-disabled, non-probe query across
// agt.json, agi.json, and selectors.json, plus the one probe entry found
// along the way (agt.json's "Query Parse Error" -- task-14-brief.md).
func activeCorpusQueries(t *testing.T) (queries []corpusQuery, probe commonSearchEntry) {
	t.Helper()

	agt := loadCommonSearches(t, "testdata/prebuilt/agt.json")
	agi := loadCommonSearches(t, "testdata/prebuilt/agi.json")
	selectors := loadSelectors(t, "testdata/prebuilt/selectors.json")

	var probeFound bool
	for _, e := range agt {
		if e.Probe {
			if probeFound {
				t.Fatalf("agt.json: more than one probe entry")
			}
			probe = e
			probeFound = true
			continue
		}
		if e.Disabled {
			continue
		}
		queries = append(queries, corpusQuery{source: "agt", name: e.Name, query: e.Query})
	}
	if !probeFound {
		t.Fatalf("agt.json: no probe entry found")
	}

	for _, e := range agi {
		if e.Disabled {
			continue
		}
		queries = append(queries, corpusQuery{source: "agi", name: e.Name, query: e.Query})
	}

	for _, s := range selectors {
		queries = append(queries, corpusQuery{source: "selector", name: s.Name, query: s.Query})
	}

	return queries, probe
}

// hasOrderBy reports whether query's text contains an ORDER BY clause,
// case-insensitively (matching Cypher's own keyword case-insensitivity).
// Exactly one corpus query has one -- "Kerberoastable users with most admin
// privileges", identical text in both agt.json and agi.json -- whose result
// order (by a derived adminCount, descending) is semantically meaningful, so
// that query's comparisons run as ordered sequences instead of the default
// set/multiset ones every other query gets.
func hasOrderBy(query string) bool {
	return strings.Contains(strings.ToUpper(query), "ORDER BY")
}

// extractNodeIDs, extractEdgeIDs, and extractLiteralSignatures flatten an
// ops.QueryResult into the three comparable projections this suite's brief
// calls for: node ids, edge ids, and literal signatures (Key plus a
// type-tolerant rendering of Value via scalarSignature --
// dawgs_corpus_integration_test.go's own helper, reused here since it
// already normalizes exactly the numeric-representation differences (int32
// vs int64 vs float64, etc.) two independently implemented query engines
// are liable to disagree on despite being otherwise correct).
//
// ops.FetchByQuery accumulates every row's node/edge values into one
// shared, running graph.Path rather than resetting per row (see its own
// doc) -- harmless here, since every comparison below is by id set (or, for
// the one ordered query, by encounter order across the whole result) rather
// than by path shape.
func extractNodeIDs(qr ops.QueryResult) []graph.ID {
	var ids []graph.ID
	for _, p := range qr.Paths {
		for _, n := range p.Nodes {
			if n != nil {
				ids = append(ids, n.ID)
			}
		}
	}
	return ids
}

func extractEdgeIDs(qr ops.QueryResult) []graph.ID {
	var ids []graph.ID
	for _, p := range qr.Paths {
		for _, e := range p.Edges {
			if e != nil {
				ids = append(ids, e.ID)
			}
		}
	}
	return ids
}

func extractLiteralSignatures(qr ops.QueryResult) []string {
	sigs := make([]string, len(qr.Literals))
	for i, lit := range qr.Literals {
		sigs[i] = lit.Key + "=" + scalarSignature(lit.Value)
	}
	return sigs
}

// idSequence and idSet render a []graph.ID as strings for
// assertStringSequence/assertStringMultiset (both already defined in
// dawgs_corpus_integration_test.go, this same package). idSet deduplicates
// first, so handing its output to assertStringMultiset (a count-sensitive
// comparison) still yields plain set equality, matching the brief's "node
// sets by id, edge sets by id" wording; idSequence preserves encounter
// order, for the one query compared as an ordered sequence instead (see
// hasOrderBy).
func idSequence(ids []graph.ID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func idSet(ids []graph.ID) []string {
	seen := make(map[graph.ID]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id.String())
	}
	return out
}

// assertStringSequence is assertStringMultiset's ordered counterpart: exact
// element-by-element equality, order included.
func assertStringSequence(t *testing.T, label string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Errorf("%s: ordered sequence mismatch\n got:  %v\nwant: %v", label, got, want)
	}
}

// assertCorpusResultsMatch compares got (the bloodtrail-side result)
// against want (the pg oracle's) per this suite's brief: node sets by id,
// edge sets by id, literals by (scalarSignature-normalized) deep equality --
// ordered sequences instead of set/multiset comparisons when ordered is
// true (see hasOrderBy).
func assertCorpusResultsMatch(t *testing.T, ordered bool, got, want ops.QueryResult) {
	t.Helper()

	gotNodes, wantNodes := extractNodeIDs(got), extractNodeIDs(want)
	gotEdges, wantEdges := extractEdgeIDs(got), extractEdgeIDs(want)
	gotLits, wantLits := extractLiteralSignatures(got), extractLiteralSignatures(want)

	if ordered {
		assertStringSequence(t, "nodes", idSequence(gotNodes), idSequence(wantNodes))
		assertStringSequence(t, "edges", idSequence(gotEdges), idSequence(wantEdges))
		assertStringSequence(t, "literals", gotLits, wantLits)
	} else {
		assertStringMultiset(t, idSet(gotNodes), idSet(wantNodes), "node id set")
		assertStringMultiset(t, idSet(gotEdges), idSet(wantEdges), "edge id set")
		assertStringMultiset(t, gotLits, wantLits, "literals")
	}
}

// runCorpusQuery runs query as a plain read-only Cypher-text query against
// db via ops.FetchByQuery, returning the query's own result/error separately
// from the enclosing read transaction's (expected to stay nil even for the
// deliberately-erroring probe entry: ops.FetchByQuery only ever reports
// tx.Query's own result.Error(), wrapped, as its own return value -- it
// never fails the transaction it runs in).
func runCorpusQuery(t *testing.T, ctx context.Context, db graph.Database, query string) (ops.QueryResult, error) {
	t.Helper()

	var (
		result ops.QueryResult
		qerr   error
	)
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result, qerr = ops.FetchByQuery(tx, query)
		return nil
	}); err != nil {
		t.Fatalf("read transaction: %v", err)
	}
	return result, qerr
}

// TestPrebuiltCorpusDifferential is milestone 4's Task 16 exit criterion --
// see this file's Task 16 section doc above for the full design. Per
// non-disabled, non-probe query across agt.json, agi.json, and
// selectors.json (222 entries), this:
//
//  1. runs the query through the bloodtrail driver (served, if at all, from
//     a snapshot forced fresh via a manual engine.RebuildNow over Task 15's
//     internal/graphtest.LoadCorpusFixture) and through a raw pg driver
//     oracle on the same database, both via ops.FetchByQuery;
//  2. compares node id sets, edge id sets, and literal values (ordered
//     sequences instead, for the one corpus query with an ORDER BY --
//     hasOrderBy);
//  3. asserts the bloodtrail side actually served the query (a
//     cypherServedMarker log delta), unless the query is named in
//     expectedDelegations;
//
// then runs the probe entry ("Query Parse Error") separately, asserting
// both drivers fail with the identical error string, and finally asserts
// two suite-level anti-vacuity tallies: at least 60% of the 222 queries
// returned at least one row against the fixture, and every non-allowlisted
// query was served.
func TestPrebuiltCorpusDifferential(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	// Must be installed before dawgs.Open constructs the bloodtrail driver:
	// its Config.Log is built from slog.Default() exactly once, at
	// construction time (staleness_integration_test.go's installLogCapture
	// doc, reused here directly since this file shares its package).
	buf := installLogCapture(t)

	ctx := context.Background()

	// A throwaway pg.Driver purely to obtain a sized/configured
	// *pgxpool.Pool, matching dawgs_corpus_integration_test.go's identical
	// precedent -- the driver return itself is unused.
	_, pool := graphtest.OpenPG(t, dsn)
	cfg := dawgs.Config{ConnectionString: dsn, GraphQueryMemoryLimit: size.Gibibyte, Pool: pool}

	rawBT, err := dawgs.Open(ctx, DriverName, cfg)
	if err != nil {
		t.Fatalf("open bloodtrail: %v", err)
	}
	defer func() { _ = rawBT.Close(ctx) }()

	d, ok := rawBT.(*Driver)
	if !ok {
		t.Fatalf("expected *Driver, got %T", rawBT)
	}

	rawOracle, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		t.Fatalf("open pg oracle: %v", err)
	}
	defer func() { _ = rawOracle.Close(ctx) }()

	oracle, ok := rawOracle.(*pg.Driver)
	if !ok {
		t.Fatalf("expected *pg.Driver, got %T", rawOracle)
	}

	// Wipe first (this suite's own brief), then seed the corpus fixture
	// through the oracle instance -- LoadCorpusFixture wants a *pg.Driver,
	// and either instance would physically write the same rows to the same
	// database, so the choice is arbitrary; using the oracle keeps the
	// bloodtrail driver's own write path (engine.NoteWrite et al.) out of
	// the picture entirely, since the very next step (engine.RebuildNow)
	// reads straight from PostgreSQL regardless of which driver wrote it or
	// what its generation counter thinks.
	graphtest.WipeGraph(t, oracle)

	// Asserted on the bloodtrail driver's own embedded *pg.Driver instance
	// too, not just the oracle: each instance keeps its own in-memory
	// kind-id cache (see graphtest.CorpusSchema's own doc), and
	// LoadCorpusFixture below only asserts it through the oracle.
	if err := d.AssertSchema(ctx, graphtest.CorpusSchema()); err != nil {
		t.Fatalf("assert corpus schema on bloodtrail driver: %v", err)
	}

	graphtest.LoadCorpusFixture(t, oracle)

	// A deterministic, on-demand rebuild -- no datapipe_status table, no
	// poller cadence to race -- matching dawgs_corpus_integration_test.go/
	// builder_differential_matrix_integration_test.go's identical use of
	// d.engine.RebuildNow.
	if err := d.engine.RebuildNow(ctx, "manual", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	bt := graph.Database(d)
	oracleDB := graph.Database(oracle)

	queries, probe := activeCorpusQueries(t)

	const wantActive = 222
	if len(queries) != wantActive {
		t.Fatalf("active corpus query count = %d, want %d (see task-14-brief.md's counts)", len(queries), wantActive)
	}

	var (
		nonEmptyCount int
		undelegated   int // active, non-allowlisted queries that declined
	)

	for _, q := range queries {
		q := q
		key := q.source + "/" + q.name

		t.Run(key, func(t *testing.T) {
			ordered := hasOrderBy(q.query)

			before := buf.String()
			baseline := markerCount(buf, cypherServedMarker)

			gotResult, gotErr := runCorpusQuery(t, ctx, bt, q.query)
			if gotErr != nil {
				t.Fatalf("bloodtrail query error: %v\nquery: %s", gotErr, q.query)
			}

			if delta := markerCount(buf, cypherServedMarker) - baseline; delta == 0 {
				if reason, allowed := expectedDelegations[key]; allowed {
					t.Logf("delegated as expected (%s)", reason)
				} else {
					undelegated++
					tail := buf.String()[len(before):]
					t.Errorf("query %q was not served by the bloodtrail engine (delegated to PostgreSQL); decline reason: %s", q.name, declineReason(tail))
				}
			}

			wantResult, wantErr := runCorpusQuery(t, ctx, oracleDB, q.query)
			if wantErr != nil {
				t.Fatalf("oracle query error: %v\nquery: %s", wantErr, q.query)
			}

			if reason, ambiguous := knownAmbiguousQueries[key]; ambiguous {
				t.Logf("comparing node ids only: known shortestPath tie (%s)", reason)
				assertStringMultiset(t, idSet(extractNodeIDs(gotResult)), idSet(extractNodeIDs(wantResult)), "node id set")
			} else {
				assertCorpusResultsMatch(t, ordered, gotResult, wantResult)
			}

			if len(wantResult.Paths) > 0 || len(wantResult.Literals) > 0 {
				nonEmptyCount++
			}
		})
	}

	if pct := nonEmptyCount * 100 / len(queries); pct < 60 {
		t.Errorf("anti-vacuity: only %d%% (%d/%d) of active queries returned >=1 row against the fixture, want >=60%%", pct, nonEmptyCount, len(queries))
	}
	if undelegated > 0 {
		t.Errorf("anti-vacuity: %d of %d active, non-allowlisted queries were not served by the engine (see the per-query failures above), want 0", undelegated, len(queries))
	}

	// The probe entry: both drivers must fail, and identically.
	t.Run("probe/"+probe.Name, func(t *testing.T) {
		_, gotErr := runCorpusQuery(t, ctx, bt, probe.Query)
		_, wantErr := runCorpusQuery(t, ctx, oracleDB, probe.Query)

		if gotErr == nil || wantErr == nil {
			t.Fatalf("expected both drivers to error on the probe query; bloodtrail err=%v, oracle err=%v", gotErr, wantErr)
		}
		if gotErr.Error() != wantErr.Error() {
			t.Fatalf("probe error strings differ:\n bloodtrail: %s\n oracle:     %s", gotErr.Error(), wantErr.Error())
		}
	})
}
