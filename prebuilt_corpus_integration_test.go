// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file holds count/shape assertions over the pre-built Cypher query
// corpus extracted from an upstream BloodHound CE checkout (scripts/
// extract-prebuilt-queries.go) into testdata/prebuilt/{agt,agi,selectors}.
// json -- see testdata/prebuilt/NOTICE for provenance and that script's
// package doc for exactly what each file holds and why -- plus
// TestPrebuiltCorpusDifferential, which runs every enabled query against a
// live engine/oracle pair (see its own doc for the full design).
//
// The loader helpers (loadCommonSearches/loadSelectors and the
// commonSearchEntry/selectorEntry types) are shared by both: the shape
// assertions above -- exact counts, the disabled/probe markers, the
// absence of Cypher bind parameters (this is a fixed, canned corpus of demo
// queries, never caller-parameterized), and syntactic validity via the same
// frontend.ParseCypher the engine's own interpreter uses (see
// write_observer.go's parseCypherFrontend) -- and the differential suite.
//
// File placement mirrors dawgs_corpus_integration_test.go and
// builder_differential_matrix_integration_test.go: package bloodtrail, not
// bloodtrail_test, since the differential suite forces a deterministic
// snapshot rebuild through Driver's own unexported engine field, the same
// way those two files do.
package bloodtrail

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

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

// TestPrebuiltCorpusCounts asserts the pre-built query corpus extracted
// from upstream BloodHound CE has the exact shape scripts/extract-prebuilt-
// queries.go's own doc comment documents --
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
// being served, keyed by corpusQueryKey's "<source>/<name>#<index>" form
// (source is "agt", "agi", or "selector"; name is the entry's own "name"
// field; index disambiguates two or more corpus entries that happen to
// share the same source+name -- e.g. agt.json's and agi.json's two,
// differently-queried "Disabled Tier Zero / High Value principals" entries
// -- since name alone is not always unique within one file). Every entry
// must carry a one-line reason, and assertAllowlistKeysUnambiguous asserts
// every key here matches exactly one corpus query -- a guard that keeps
// working correctly over an empty map too (nothing to be ambiguous about).
//
// Empty: this corpus previously surfaced five genuine, investigated
// interpreter scope gaps here (not one-line bugs), all since closed:
// WHERE-clause pattern predicates (closed by checkPatternPredicate/
// evalPatternPredicate, plan.go/eval.go), `+` string concatenation (closed
// by applyAdd, eval.go), and an unconstrained, unbounded shortestPath to
// Tag_Tier_Zero declining ErrBudget (root-caused to
// traverse.AllShortestPaths' own PairBudget/SideBudget strategy dispatch
// genuinely refusing the shape on this fixture -- not a budget-unit bug --
// and closed via traverse.Query's new per-call SideBudget/PairBudget
// overrides, plus a planner fix for a predicate-only shortestPath endpoint
// the agi variant of the same query also needed).
var expectedDelegations = map[string]string{}

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
// the seeded fixture data rather than assumed.
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
//
// Keyed the same way as expectedDelegations (corpusQueryKey's
// "<source>/<name>#<index>"), and covered by the same
// assertAllowlistKeysUnambiguous guard.
var knownAmbiguousQueries = map[string]string{
	"agt/Shortest paths to privileged roles#0":    "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
	"agi/Shortest paths to privileged roles#0":    "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
	"agt/Shortest paths to Azure Subscriptions#0": "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
	"agi/Shortest paths to Azure Subscriptions#0": "azTenant1 AZContains both azUser1 and azgroup1, each one hop from the query's only reachable target",
}

// corpusQuery is one entry TestPrebuiltCorpusDifferential runs, pulled from
// agt.json/agi.json (commonSearchEntry) or selectors.json (selectorEntry)
// and flattened to a common shape.
//
// index is the 0-based occurrence count of this entry among every corpus
// entry sharing the same source+name seen so far (assigned in file order by
// activeCorpusQueries' newCorpusQuery helper) -- name alone is not always
// unique within one file: both agt.json and agi.json each carry two,
// differently-queried entries named "Disabled Tier Zero / High Value
// principals" (one against `Base`/AD, one against `AZBase`/Azure,
// distinguished only by their "subheader" field, which corpusQuery does not
// otherwise carry). index exists purely so corpusQueryKey can build an
// unambiguous allowlist key for every entry, including these.
type corpusQuery struct {
	source string
	name   string
	query  string
	index  int
}

// corpusQueryKey returns q's key into expectedDelegations and
// knownAmbiguousQueries: "<source>/<name>#<index>". Also used as q's t.Run
// subtest name, so that two same-named corpus entries (see corpusQuery's own
// doc) get distinct, stable subtest names instead of relying on go test's
// own automatic "#01" disambiguation suffix -- which would otherwise be
// visually indistinguishable from this key's own "#0"/"#1" suffix.
func corpusQueryKey(q corpusQuery) string {
	return fmt.Sprintf("%s/%s#%d", q.source, q.name, q.index)
}

// activeCorpusQueries returns every non-disabled, non-probe query across
// agt.json, agi.json, and selectors.json, plus the one probe entry found
// along the way (agt.json's deliberately malformed "Query Parse Error"
// entry -- see scripts/extract-prebuilt-queries.go's own doc comment).
func activeCorpusQueries(t *testing.T) (queries []corpusQuery, probe commonSearchEntry) {
	t.Helper()

	agt := loadCommonSearches(t, "testdata/prebuilt/agt.json")
	agi := loadCommonSearches(t, "testdata/prebuilt/agi.json")
	selectors := loadSelectors(t, "testdata/prebuilt/selectors.json")

	// nameCounts tracks, per "<source>/<name>", how many corpusQuery entries
	// have already been assigned that pair -- corpusQuery.index below.
	nameCounts := make(map[string]int)
	newCorpusQuery := func(source, name, query string) corpusQuery {
		base := source + "/" + name
		index := nameCounts[base]
		nameCounts[base]++
		return corpusQuery{source: source, name: name, query: query, index: index}
	}

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
		queries = append(queries, newCorpusQuery("agt", e.Name, e.Query))
	}
	if !probeFound {
		t.Fatalf("agt.json: no probe entry found")
	}

	for _, e := range agi {
		if e.Disabled {
			continue
		}
		queries = append(queries, newCorpusQuery("agi", e.Name, e.Query))
	}

	for _, s := range selectors {
		queries = append(queries, newCorpusQuery("selector", s.Name, s.Query))
	}

	return queries, probe
}

// assertAllowlistKeysUnambiguous fails the suite if any key in allowlist
// (expectedDelegations or knownAmbiguousQueries, named by label purely for
// the failure message) does not identify exactly one entry of queries under
// corpusQueryKey -- either zero (a stale or mistyped key, silently inert)
// or, the bug this specifically guards against, more than one (two or more
// corpus entries sharing a source+name -- see corpusQuery's own doc --
// colliding on one allowlist slot without corpusQueryKey's index
// disambiguator).
func assertAllowlistKeysUnambiguous(t *testing.T, queries []corpusQuery, allowlist map[string]string, label string) {
	t.Helper()

	counts := make(map[string]int, len(queries))
	for _, q := range queries {
		counts[corpusQueryKey(q)]++
	}

	for key := range allowlist {
		switch n := counts[key]; {
		case n == 0:
			t.Errorf("%s[%q]: matches no corpus query -- stale or mistyped key", label, key)
		case n > 1:
			t.Errorf("%s[%q]: matches %d corpus queries, want exactly 1 -- allowlist keys must be unambiguous", label, key, n)
		}
	}
}

// hasOrderBy reports whether query's text contains an ORDER BY clause,
// case-insensitively (matching Cypher's own keyword case-insensitivity).
// Exactly one corpus query has one today -- orderedCorpusQueryName,
// identical text in both agt.json and agi.json -- whose result order (by a
// derived adminCount, descending) is semantically meaningful, so that
// query's comparisons run as ordered sequences instead of the default
// set/multiset ones every other query gets. See orderedCorpusQueryName's own
// doc for how ties in that sort key are handled, and
// TestPrebuiltCorpusDifferential's inventory check for what happens if a
// second ORDER BY query ever joins the corpus.
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

// canonNode/canonEdge/canonPath and canonicalizePath/renderPathSignatures
// port internal/engine/engine_integration_test.go's identical canonNode/
// canonEdge/canonPath/canonicalize/renderSet comparator into this package:
// the original assertCorpusResultsMatch (git history) compared node id SETS
// and edge id SETS only, flattened
// across every row of a result -- dropping node/edge PROPERTIES entirely,
// and (since a set collapses duplicates) any PATH MULTIPLICITY difference
// too (e.g. the same node reached by three distinct paths on one side but
// two on the other could render an identical id set). Cannot import
// engine_integration_test.go's own versions directly: package engine's
// canonicalize/renderSet/canonNode/canonEdge/canonPath are all unexported,
// invisible outside that package even via Go's internal/ import-path rules
// (which gate import paths, not identifier visibility) -- so this is a
// deliberate duplicate, kept in sync by eye, of the exact same node id+
// props / edge kind+props / path-ordered (not sorted within a path)
// rendering, over graph.Path/graph.PathSet, a public dawgs type both files
// operate on identically.
type canonNode struct {
	ID    uint64         `json:"id"`
	Props map[string]any `json:"props"`
}
type canonEdge struct {
	Kind  string         `json:"kind"`
	Props map[string]any `json:"props"`
}
type canonPath struct {
	Nodes []canonNode `json:"nodes"`
	Edges []canonEdge `json:"edges"`
}

func canonicalizePath(p graph.Path) canonPath {
	cp := canonPath{Nodes: make([]canonNode, len(p.Nodes)), Edges: make([]canonEdge, len(p.Edges))}
	for i, n := range p.Nodes {
		cp.Nodes[i] = canonNode{ID: uint64(n.ID), Props: n.Properties.MapOrEmpty()}
	}
	for i, e := range p.Edges {
		cp.Edges[i] = canonEdge{Kind: e.Kind.String(), Props: e.Properties.MapOrEmpty()}
	}
	return cp
}

// renderPathSignatures canonicalizes and JSON-marshals every path in ps
// (encoding/json sorts map keys, so this is deterministic per path), one
// signature string per path, in ENCOUNTER order. Unlike
// engine_integration_test.go's own renderSet, this deliberately does NOT
// additionally sort the resulting slice: assertStringSequence (the ordered
// comparison below) needs exact encounter order preserved, while
// assertStringMultiset (the unordered one) already sorts its own copies of
// whatever it is handed internally -- so one unsorted rendering correctly
// serves both callers, and a duplicate path (multiplicity) is preserved as
// a duplicate string in both cases rather than silently collapsed.
func renderPathSignatures(ps graph.PathSet) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		b, err := json.Marshal(canonicalizePath(p))
		if err != nil {
			panic(fmt.Sprintf("bloodtrail: renderPathSignatures: marshal canonical path: %v", err))
		}
		out[i] = string(b)
	}
	return out
}

// assertCorpusResultsMatch compares got (the bloodtrail-side result)
// against want (the pg oracle's): paths by canonical node-id+properties/
// edge-kind+properties rendering (renderPathSignatures/canonicalizePath --
// see their own doc for why this replaced the original id-set-only
// comparison), plus an explicit path-COUNT assertion (a
// friendlier, more specific failure message than relying solely on the
// rendered-signature comparison happening to also catch a count mismatch),
// and literals by (scalarSignature-normalized) deep equality -- ordered
// sequences instead of set/multiset comparisons when ordered is true (see
// hasOrderBy).
func assertCorpusResultsMatch(t *testing.T, ordered bool, got, want ops.QueryResult) {
	t.Helper()

	if len(got.Paths) != len(want.Paths) {
		t.Errorf("path count mismatch: got %d paths, want %d", len(got.Paths), len(want.Paths))
	}

	gotPaths, wantPaths := renderPathSignatures(got.Paths), renderPathSignatures(want.Paths)
	gotLits, wantLits := extractLiteralSignatures(got), extractLiteralSignatures(want)

	if ordered {
		assertStringSequence(t, "paths", gotPaths, wantPaths)
		assertStringSequence(t, "literals", gotLits, wantLits)
	} else {
		assertStringMultiset(t, gotPaths, wantPaths, "path set (node id+props, edge kind+props, path-ordered)")
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

// orderedCorpusQueryName is the corpus's one query today whose ORDER BY
// makes its result order semantically meaningful: "Kerberoastable users
// with most admin privileges" (identical text in agt.json and agi.json --
// see hasOrderBy). Its own "RETURN u ORDER BY adminCount DESC LIMIT 100"
// never projects adminCount itself, and this fixture ties 5 of its 6
// qualifying users at adminCount=10 -- so plain exact-sequence comparison
// between two independently implemented engines would depend on each
// engine's own incidental same-key iteration order among the tied rows, not
// on anything ORDER BY itself guarantees. assertOrderedCorpusResult routes
// exactly this query through tie-aware, sort-key-group comparison instead
// (assertOrderedByAdminCountGroups); any other ORDER BY query (there are
// none today) falls back to assertCorpusResultsMatch's plain exact-sequence
// comparison -- but TestPrebuiltCorpusDifferential separately asserts, as a
// suite-level inventory check, that no such other query exists yet. That
// inventory check is a deliberate tripwire: a future corpus update
// introducing a second ORDER BY query would otherwise silently inherit the
// exact-sequence fallback -- which may be exactly as tie-fragile as this one
// was -- without anyone deciding whether it needs its own group-aware
// treatment too.
const orderedCorpusQueryName = "Kerberoastable users with most admin privileges"

// adminCountQuery reproduces orderedCorpusQueryName's own MATCH/WITH clause
// (see testdata/prebuilt/agt.json / agi.json) with "RETURN u, adminCount" in
// place of "RETURN u ... ORDER BY adminCount DESC LIMIT 100": the same
// computation, with its sort key exposed as a second projected value so
// adminCountSignatures can determine, per qualifying user, the adminCount
// that put them where the corpus query put them. Kept in sync with the
// corpus JSON by eye (there is exactly one call site); orderedCorpusQueryName
// names the exact corpus entries to diff this against by hand if either
// ever changes.
const adminCountQuery = `
MATCH (u:User)
WHERE u.hasspn = true
  AND u.enabled = true
  AND NOT u.objectid ENDS WITH '-502'
  AND NOT COALESCE(u.gmsa, false) = true
  AND NOT COALESCE(u.msa, false) = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u, adminCount
`

// adminCountSignatures runs adminCountQuery against db (the pg oracle, in
// practice -- an independent source of truth for the sort key
// orderedCorpusQueryName's own "RETURN u" never exposes) and returns a map
// from each qualifying user's graph.ID (string form, matching
// idSequence/idSet) to a scalarSignature-normalized rendering of that user's
// adminCount.
//
// Correlating each row's node (u) with that same row's literal (adminCount)
// relies on adminCountQuery returning exactly one node and one literal per
// row: ops.FetchByQuery accumulates nodes across *all* rows of a result into
// one shared graph.Path rather than resetting per row (see extractNodeIDs'
// own doc), so node and literal slices are not row-aligned for an arbitrary
// query -- but they are here, because every row of this specific query
// contributes exactly one of each, appended in the same order
// queryResult.Next() visits rows, so the i-th extracted node and the i-th
// extracted literal always come from the same row.
func adminCountSignatures(t *testing.T, ctx context.Context, db graph.Database) map[string]string {
	t.Helper()

	result, err := runCorpusQuery(t, ctx, db, adminCountQuery)
	if err != nil {
		t.Fatalf("adminCountQuery: %v", err)
	}

	ids := idSequence(extractNodeIDs(result))
	sigs := extractLiteralSignatures(result)
	if len(ids) != len(sigs) {
		t.Fatalf("adminCountQuery: got %d user ids but %d adminCount literals (want exactly one of each per row, in lockstep)", len(ids), len(sigs))
	}

	out := make(map[string]string, len(ids))
	for i, id := range ids {
		out[id] = sigs[i]
	}
	return out
}

// sortKeyGroup is one consecutive run of equal sort-key values within an
// ordered corpus result's node-id sequence -- e.g. every user tied at
// adminCount=10, in whatever relative order one engine happened to emit
// them.
type sortKeyGroup struct {
	key string
	ids []string // set semantics within the group; encounter order is not meaningful
}

// groupConsecutiveBySortKey partitions ids (in encounter order) into
// sortKeyGroup runs, looking up each id's sort-key value in keyOf. This is
// the tie-aware comparison unit ORDER BY actually promises: the *sequence*
// of groups -- which key, how many members, in what order relative to other
// groups -- is significant, but *within* one tied group, membership is a
// set: which specific id landed in which position inside the tie is not
// something ORDER BY (or this suite) can hold either engine to.
func groupConsecutiveBySortKey(t *testing.T, label string, ids []string, keyOf map[string]string) []sortKeyGroup {
	t.Helper()

	var groups []sortKeyGroup
	for _, id := range ids {
		key, ok := keyOf[id]
		if !ok {
			t.Fatalf("%s: returned user id %s has no adminCount from the independent oracle query -- fixture/query mismatch", label, id)
		}
		if n := len(groups); n > 0 && groups[n-1].key == key {
			groups[n-1].ids = append(groups[n-1].ids, id)
		} else {
			groups = append(groups, sortKeyGroup{key: key, ids: []string{id}})
		}
	}
	return groups
}

// assertOrderedByAdminCountGroups is orderedCorpusQueryName's tie-aware
// replacement for plain exact-sequence comparison: it derives each returned
// user's adminCount independently (adminCountSignatures), partitions both
// sides' returned node-id sequences into consecutive equal-adminCount runs
// (groupConsecutiveBySortKey), and asserts the *sequence of groups* matches
// -- same key, same size, same relative order -- while each group's own
// membership is compared as a set. That is exactly what "ORDER BY adminCount
// DESC" guarantees when the fixture ties 5 of its 6 qualifying users at
// adminCount=10: which of the tied users is emitted first is unspecified,
// but the tied block as a whole must appear together, in the right position
// relative to any untied rows -- unlike this suite's previous plain
// exact-sequence comparison, which happened to pass only because both
// engines' incidental iteration order over the tied group agreed.
//
// orderedCorpusQueryName's own "RETURN u" projects neither edges nor
// literals, so both are asserted empty here rather than silently skipped --
// a future edit widening that RETURN clause should make this function fail
// loudly instead of quietly stop checking something it once checked.
func assertOrderedByAdminCountGroups(t *testing.T, ctx context.Context, oracleDB graph.Database, got, want ops.QueryResult) {
	t.Helper()

	keyOf := adminCountSignatures(t, ctx, oracleDB)

	gotGroups := groupConsecutiveBySortKey(t, "bloodtrail", idSequence(extractNodeIDs(got)), keyOf)
	wantGroups := groupConsecutiveBySortKey(t, "oracle", idSequence(extractNodeIDs(want)), keyOf)

	if len(gotGroups) != len(wantGroups) {
		t.Fatalf("adminCount group-sequence mismatch: got %d groups, want %d\n got:  %+v\nwant: %+v", len(gotGroups), len(wantGroups), gotGroups, wantGroups)
	}
	for i := range gotGroups {
		if gotGroups[i].key != wantGroups[i].key {
			t.Errorf("group %d: adminCount key mismatch: got %s, want %s", i, gotGroups[i].key, wantGroups[i].key)
			continue
		}
		assertStringMultiset(t, gotGroups[i].ids, wantGroups[i].ids, fmt.Sprintf("group %d (adminCount %s) membership", i, gotGroups[i].key))
	}

	if gotEdges, wantEdges := extractEdgeIDs(got), extractEdgeIDs(want); len(gotEdges) != 0 || len(wantEdges) != 0 {
		t.Errorf("orderedCorpusQueryName unexpectedly returned edges (got %d, want %d); this comparison only handles its current RETURN u shape", len(gotEdges), len(wantEdges))
	}
	if len(got.Literals) != 0 || len(want.Literals) != 0 {
		t.Errorf("orderedCorpusQueryName unexpectedly returned literals (got %d, want %d); this comparison only handles its current RETURN u shape", len(got.Literals), len(want.Literals))
	}
}

// assertOrderedCorpusResult compares got against want for a corpus query
// whose ORDER BY makes its result sequence semantically meaningful,
// dispatching by name: see orderedCorpusQueryName's own doc for why only
// that one name gets tie-aware group comparison, with every other name
// falling back to assertCorpusResultsMatch's plain exact-sequence
// comparison.
func assertOrderedCorpusResult(t *testing.T, ctx context.Context, oracleDB graph.Database, name string, got, want ops.QueryResult) {
	t.Helper()

	if name == orderedCorpusQueryName {
		assertOrderedByAdminCountGroups(t, ctx, oracleDB, got, want)
		return
	}

	assertCorpusResultsMatch(t, true, got, want)
}

// TestPrebuiltCorpusDifferential is milestone 4's Task 16 exit criterion --
// see this file's Task 16 section doc above for the full design. Before
// running anything, it guards both allowlists (assertAllowlistKeysUnambiguous
// on expectedDelegations and knownAmbiguousQueries) and the ORDER BY
// inventory (orderedCorpusQueryName's own doc). Then, per non-disabled,
// non-probe query across agt.json, agi.json, and selectors.json (222
// entries), this:
//
//  1. runs the query through the bloodtrail driver (served, if at all, from
//     a snapshot forced fresh via a manual engine.RebuildNow over Task 15's
//     internal/graphtest.LoadCorpusFixture) and through a raw pg driver
//     oracle on the same database, both via ops.FetchByQuery;
//  2. compares node id sets, edge id sets, and literal values -- tie-aware
//     sort-key-group comparison instead, for the one corpus query with an
//     ORDER BY (hasOrderBy, assertOrderedCorpusResult);
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
	// bloodtrail driver's own write path (engine.Apply et al.) out of the
	// picture entirely, since the very next step (engine.RebuildNow) reads
	// straight from PostgreSQL regardless of which driver wrote it.
	graphtest.WipeGraph(t, oracle)

	// Asserted on the bloodtrail driver's own embedded *pg.Driver instance
	// too, not just the oracle: each instance keeps its own in-memory
	// kind-id cache (see graphtest.CorpusSchema's own doc), and
	// LoadCorpusFixture below only asserts it through the oracle.
	if err := d.AssertSchema(ctx, graphtest.CorpusSchema()); err != nil {
		t.Fatalf("assert corpus schema on bloodtrail driver: %v", err)
	}

	graphtest.LoadCorpusFixture(t, oracle)

	// A deterministic, on-demand rebuild, independent of Start's own
	// one-shot boot-load goroutine (engine/boot.go) -- matching
	// dawgs_corpus_integration_test.go/builder_differential_matrix_
	// integration_test.go's identical use of d.engine.RebuildNow.
	if err := d.engine.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	bt := graph.Database(d)
	oracleDB := graph.Database(oracle)

	queries, probe := activeCorpusQueries(t)

	const wantActive = 222
	if len(queries) != wantActive {
		t.Fatalf("active corpus query count = %d, want %d (see TestPrebuiltCorpusCounts for the corpus' full shape)", len(queries), wantActive)
	}

	// Guard both allowlists before running anything: every key in either map
	// must identify exactly one corpus query under corpusQueryKey (see
	// assertAllowlistKeysUnambiguous's own doc) -- catching, for instance, a
	// key that collides across two corpus entries sharing a source+name.
	assertAllowlistKeysUnambiguous(t, queries, expectedDelegations, "expectedDelegations")
	assertAllowlistKeysUnambiguous(t, queries, knownAmbiguousQueries, "knownAmbiguousQueries")

	// Inventory tripwire for orderedCorpusQueryName (see its own doc): every
	// ORDER BY query in the corpus today must be that one, named query. If a
	// future corpus update adds a second, genuinely different ORDER BY
	// query, this must fail loudly here rather than let that query silently
	// fall back to assertOrderedCorpusResult's plain exact-sequence
	// comparison unnoticed.
	var orderedQueryCount int
	for _, q := range queries {
		if !hasOrderBy(q.query) {
			continue
		}
		orderedQueryCount++
		if q.name != orderedCorpusQueryName {
			t.Fatalf("corpus query %q (%s) has an ORDER BY clause but is not orderedCorpusQueryName (%q); a new ordered corpus query needs a deliberate decision on tie-safety (see orderedCorpusQueryName's doc) before this suite can trust it to either group-aware or exact-sequence comparison", q.name, corpusQueryKey(q), orderedCorpusQueryName)
		}
	}
	const wantOrderedQueryCount = 2 // orderedCorpusQueryName, once each in agt.json and agi.json
	if orderedQueryCount != wantOrderedQueryCount {
		t.Fatalf("found %d corpus queries with ORDER BY named %q, want %d (agt + agi) -- corpus shape changed; re-check whether assertOrderedByAdminCountGroups still applies", orderedQueryCount, orderedCorpusQueryName, wantOrderedQueryCount)
	}

	var (
		nonEmptyCount int
		undelegated   int // active, non-allowlisted queries that declined
	)

	for _, q := range queries {
		q := q
		key := corpusQueryKey(q)

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
			} else if ordered {
				assertOrderedCorpusResult(t, ctx, oracleDB, q.name, gotResult, wantResult)
			} else {
				assertCorpusResultsMatch(t, false, gotResult, wantResult)
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
