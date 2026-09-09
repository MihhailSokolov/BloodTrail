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
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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

// --- The pre-built corpus differential suite ---
//
// This is the corpus-wide differential: every active (non-disabled,
// non-probe) query across agt.json, agi.json, and selectors.json -- 222
// entries -- runs against both the bloodtrail driver (served, if at all,
// from a snapshot forced fresh over the corpus fixture) and a raw pg
// driver oracle on the same database, comparing results and asserting the
// bloodtrail engine actually served the query rather than silently
// delegating. The one deliberately malformed "probe" entry gets its own
// subtest, asserting both drivers fail identically.
//
// # File placement: package bloodtrail, not bloodtrail_test
//
// The reason is the same one that pins its siblings here:
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

// pathVariableBindingPattern matches a Cypher MATCH clause that binds a
// PATH-TYPED variable: "MATCH <name> = (" (a plain path pattern) or
// "MATCH <name> = shortestPath(" / "MATCH <name> = allShortestPaths("
// (Cypher's two path-returning functions) -- case-insensitively, mirroring
// hasOrderBy's own keyword-case handling. Anchored to the MATCH keyword
// specifically (not a bare "<name> = (" anywhere in the query text) so an
// unrelated parenthesized WHERE-clause sub-expression is never mistaken for
// a path binding.
var pathVariableBindingPattern = regexp.MustCompile(`(?i)MATCH\s+([A-Za-z_]\w*)\s*=\s*(?:shortestPath|allShortestPaths)?\s*\(`)

// returnClauseTextPattern extracts everything after a query's own RETURN
// keyword, case-insensitively -- projectsPathVariable's own helper.
var returnClauseTextPattern = regexp.MustCompile(`(?i)RETURN\s+([\s\S]*)`)

// projectsPathVariable reports whether query's RETURN clause returns a
// genuine Cypher path-typed value -- a variable bound via "<name> = (...)"
// or "<name> = shortestPath(...)/allShortestPaths(...)" in an earlier MATCH
// clause -- as opposed to bare node/relationship/scalar values.
//
// This, not len(ops.QueryResult.Paths), is the correct gate for whether
// "unordered" comparison may reorder a packed pseudo-path's own contents
// (sortFlatPathNodesAndEdges): a path variable's internal node/edge
// SEQUENCE is traversal-order-significant -- it IS the actual route the
// query matched -- so it must never be sorted, even when the query happens
// to return exactly one such path against a given fixture. That "exactly
// one path" case is exactly where len(Paths) becomes ambiguous: it is also
// what a genuinely bare "RETURN n" projection produces when its own result
// happens to be exactly one row (ops.FetchByQuery packs every row of a bare
// projection into ONE shared, accumulated graph.Path -- see
// sortFlatPathNodesAndEdges' own doc). Only the query's own projection
// SHAPE -- fixed by its text, independent of how many rows any particular
// fixture happens to produce -- can distinguish the two.
//
// Verified against every active query in testdata/prebuilt/{agt,agi,
// selectors}.json (2026-09-09): 129 of the corpus' 222 active queries
// project a path variable this way (all named "p", always returned bare,
// e.g. "MATCH p = (:Domain)-[:SameForestTrust|CrossForestTrust]->(:Domain)
// RETURN p LIMIT 1000" or "MATCH p=shortestPath((s)-[:...]->(t)) ... RETURN
// p LIMIT 1000") -- of those, 82 return exactly one path with two or more
// nodes against this fixture, the exact shape a naive len(Paths)==1 guard
// cannot tell apart from a bare packed pseudo-path. Reversing one of those
// 82 queries' own returned path and re-comparing demonstrates the
// difference directly: with sortFlatPathNodesAndEdges gated on this
// predicate (sortPackedPathAllowed=false for these), the reversed path
// compares UNEQUAL to the original, as it must; gated on len(Paths)!=1
// alone (this suite's previous, buggy state), it wrongly compared EQUAL --
// see this file's own write-through-preamble report appendix for the
// measured evidence.
func projectsPathVariable(query string) bool {
	bindings := pathVariableBindingPattern.FindAllStringSubmatch(query, -1)
	if len(bindings) == 0 {
		return false
	}

	retMatch := returnClauseTextPattern.FindStringSubmatch(query)
	if retMatch == nil {
		return false
	}
	returnClause := retMatch[1]

	for _, b := range bindings {
		name := b[1]
		if regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(name) + `\b`).MatchString(returnClause) {
			return true
		}
	}
	return false
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
// comparison) still yields plain set equality, matching this suite's own
// "node sets by id, edge sets by id" rule; idSequence preserves encounter
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

// sortFlatPathNodesAndEdges sorts, IN PLACE, the Nodes and Edges of paths'
// one accumulated pseudo-path -- by each node's/edge's own canonical JSON
// signature (canonicalizePath's own rendering, so this sort key agrees
// exactly with what renderPathSignatures compares) -- discovered necessary
// (2026-09-09) when interleaving live mutations into
// random_cypher_differential_integration_test.go's own differential sweep
// started making bt's and PostgreSQL's respective (unspecified, since
// neither side's Cypher text carries an ORDER BY) row-return order for a
// bare "RETURN n"/"RETURN n, r" projection disagree: ops.FetchByQuery
// (dawgs' ops/ops.go) accumulates every row's bare node/relationship
// values into ONE shared, running graph.Path across the WHOLE result
// (never resetting per row -- see nodeSignaturesByCypher's own doc,
// writethrough_differential_integration_test.go, for the same behavior
// documented from a different caller), so for exactly this common query
// shape, "unordered" comparison (assertCorpusResultsMatch's ordered=false
// branch) previously degenerated into comparing one giant, row-encounter-
// order-sensitive blob per side instead of a genuine node/edge SET --
// harmless for TestPrebuiltCorpusDifferential's static, never-mutated
// fixture (both engines' default scan order happened to coincide there)
// but a real, previously-latent gap this suite's own interleaved mutations
// were the first to expose. A same-set, different-order pair of "RETURN
// n" results is not a correctness divergence; it is exactly what
// "unordered" was always supposed to tolerate.
//
// Also a no-op when paths does not hold EXACTLY one entry -- a secondary,
// defensive guard only, not this function's primary safety mechanism: the
// PRIMARY gate is now the caller's own sortPackedPathAllowed argument to
// assertCorpusResultsMatch (see projectsPathVariable's own doc for why
// len(Paths) alone cannot reliably tell a genuine single-row path apart
// from a bare-projection query that merely happens to return one row
// against a given fixture). A correctly gated caller never reaches this
// function at all for a path-projecting query, so this length check should
// never actually fire in practice; it stays as a last-resort safety net
// against a future caller mistake, since sorting only ever makes sense for
// the single-packed-blob shape regardless of how that shape was
// determined.
func sortFlatPathNodesAndEdges(paths graph.PathSet) {
	if len(paths) != 1 {
		return
	}

	signature := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			panic(fmt.Sprintf("bloodtrail: sortFlatPathNodesAndEdges: marshal signature: %v", err))
		}
		return string(b)
	}

	p := &paths[0]
	sort.Slice(p.Nodes, func(i, j int) bool {
		return signature(canonNode{ID: uint64(p.Nodes[i].ID), Props: p.Nodes[i].Properties.MapOrEmpty()}) <
			signature(canonNode{ID: uint64(p.Nodes[j].ID), Props: p.Nodes[j].Properties.MapOrEmpty()})
	})
	sort.Slice(p.Edges, func(i, j int) bool {
		return signature(canonEdge{Kind: p.Edges[i].Kind.String(), Props: p.Edges[i].Properties.MapOrEmpty()}) <
			signature(canonEdge{Kind: p.Edges[j].Kind.String(), Props: p.Edges[j].Properties.MapOrEmpty()})
	})
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
//
// sortPackedPathAllowed must be true only when the query's own RETURN
// clause is known NOT to project a genuine path-typed value (see
// projectsPathVariable's own doc) -- callers compute it once, from the
// query's own text, and must never derive it from len(got.Paths)/
// len(want.Paths): both a bare-projection query's packed pseudo-path and a
// genuine single-row path-projection query produce len(Paths)==1 against a
// given fixture, and only the query's own projection shape (fixed by its
// text) can tell them apart. Ignored when ordered is true, since that
// branch never calls sortFlatPathNodesAndEdges at all.
func assertCorpusResultsMatch(t *testing.T, ordered, sortPackedPathAllowed bool, got, want ops.QueryResult) {
	t.Helper()

	if len(got.Paths) != len(want.Paths) {
		t.Errorf("path count mismatch: got %d paths, want %d", len(got.Paths), len(want.Paths))
	}

	if !ordered && sortPackedPathAllowed {
		// See sortFlatPathNodesAndEdges's own doc: a bare-node/relationship
		// projection ("RETURN n", never a genuine path-typed value) packs
		// every row into ONE shared pseudo-path, in row-ENCOUNTER order --
		// not a meaningful order at all, since neither the underlying
		// Cypher query (no ORDER BY) nor this comparison's own "unordered"
		// contract makes any promise about it. Sorting each side's own
		// pseudo-path contents before rendering makes what follows
		// insensitive to that meaningless order, restoring genuine SET
		// semantics for exactly the queries "unordered" was always meant to
		// cover. sortPackedPathAllowed being false skips this entirely for
		// a query whose RETURN clause projects a genuine path variable
		// (projectsPathVariable): that path's own internal node/edge
		// sequence remains traversal-order-significant and must stay
		// untouched, whether the query returns one such path against this
		// fixture or several.
		sortFlatPathNodesAndEdges(got.Paths)
		sortFlatPathNodesAndEdges(want.Paths)
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

	// sortPackedPathAllowed's value is inert here: ordered=true never
	// reaches assertCorpusResultsMatch's sort branch. Passed as false
	// (the more conservative value) purely so this call site does not need
	// its own query text to compute the real answer -- this fallback path
	// has no query text on hand (only name), and today it is unreachable in
	// practice: TestPrebuiltCorpusDifferential's own inventory tripwire
	// guarantees every ORDER BY query in the corpus is orderedCorpusQueryName,
	// which returns above instead of reaching here.
	assertCorpusResultsMatch(t, true, false, got, want)
}

// TestPrebuiltCorpusDifferential is the corpus differential itself --
// see this file's section doc above for the full design. Before
// running anything, it guards both allowlists (assertAllowlistKeysUnambiguous
// on expectedDelegations and knownAmbiguousQueries) and the ORDER BY
// inventory (orderedCorpusQueryName's own doc). Then, per non-disabled,
// non-probe query across agt.json, agi.json, and selectors.json (222
// entries), this:
//
//  1. runs the query through the bloodtrail driver (served, if at all, from
//     a snapshot forced fresh via a manual engine.RebuildNow over
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

	// Start's boot-load goroutine (driver.go) races the fixture load and
	// this test's own RebuildNow below otherwise: LoadCorpusFixture writes
	// through oracle, the raw pg driver, so nothing bumps applyEpoch, and a
	// still-in-flight boot-load LoadSnapshot could adopt AFTER RebuildNow
	// and silently overwrite the freshly seeded fixture with whatever state
	// existed before it.
	waitForBootLoad(t, d)

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
			servedOrAllowed, nonEmpty := runOneCorpusQueryComparison(t, ctx, buf, bt, oracleDB, q, key)
			if !servedOrAllowed {
				undelegated++
			}
			if nonEmpty {
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

	// --- Write-through rerun: the identical 222-query comparison, now
	// against write-through-derived (not rebuild-derived) replica state --
	// see this file's own "write-through rerun" section doc, below
	// TestPrebuiltCorpusDifferential, for the full design and why it is
	// safe against this specific fixture.
	if writeThroughPreambleEnabled() {
		t.Run("writethrough", func(t *testing.T) {
			rebuilds := d.engine.RebuildCount()
			fallbacks := markerCount(buf, fallbackEnteredMarker)

			runCorpusWriteThroughPreamble(t, ctx, bt, oracleDB, pool)

			var (
				wtNonEmptyCount int
				wtUndelegated   int
			)
			for _, q := range queries {
				q := q
				key := corpusQueryKey(q)

				t.Run(key, func(t *testing.T) {
					servedOrAllowed, nonEmpty := runOneCorpusQueryComparison(t, ctx, buf, bt, oracleDB, q, key)
					if !servedOrAllowed {
						wtUndelegated++
					}
					if nonEmpty {
						wtNonEmptyCount++
					}
				})
			}

			if pct := wtNonEmptyCount * 100 / len(queries); pct < 60 {
				t.Errorf("anti-vacuity (write-through rerun): only %d%% (%d/%d) of active queries returned >=1 row against the mutated fixture, want >=60%%", pct, wtNonEmptyCount, len(queries))
			}
			if wtUndelegated > 0 {
				t.Errorf("anti-vacuity (write-through rerun): %d of %d active, non-allowlisted queries were not served by the engine after the mutation preamble, want 0", wtUndelegated, len(queries))
			}

			assertRebuildCountUnchanged(t, d, rebuilds, t.Name())
			assertNoNewFallback(t, buf, fallbacks, t.Name())
		})
	}
}

// runOneCorpusQueryComparison runs q's own query text against bt (checking
// the served-marker delta against expectedDelegations, exactly as the
// original inline loop body did) and oracleDB, then compares the two
// results via the corpus suite's usual ambiguous/ordered/default dispatch.
// Returns whether the bloodtrail side either served the query or was
// allowed to delegate (expectedDelegations), and whether the oracle's own
// result was non-empty -- the two per-query facts TestPrebuiltCorpusDifferential's
// own anti-vacuity tallies need. Extracted so both the original
// (rebuild-derived) comparison pass and the write-through-derived rerun
// (see runCorpusWriteThroughPreamble, and this file's own "write-through
// rerun" section doc below) share one comparison body instead of two copies
// drifting apart by hand.
func runOneCorpusQueryComparison(t *testing.T, ctx context.Context, buf *lockedBuffer, bt, oracleDB graph.Database, q corpusQuery, key string) (servedOrAllowed, nonEmpty bool) {
	t.Helper()

	ordered := hasOrderBy(q.query)

	before := buf.String()
	baseline := markerCount(buf, cypherServedMarker)

	gotResult, gotErr := runCorpusQuery(t, ctx, bt, q.query)
	if gotErr != nil {
		t.Fatalf("bloodtrail query error: %v\nquery: %s", gotErr, q.query)
	}

	servedOrAllowed = true
	if delta := markerCount(buf, cypherServedMarker) - baseline; delta == 0 {
		if reason, allowed := expectedDelegations[key]; allowed {
			t.Logf("delegated as expected (%s)", reason)
		} else {
			servedOrAllowed = false
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
		// sortPackedPathAllowed must come from the query's own projection
		// SHAPE (projectsPathVariable), never from len(Paths): both a bare
		// projection's packed pseudo-path and a genuine single-row path
		// projection produce len(Paths)==1 against this fixture, and only
		// the query's own text can tell them apart -- see
		// projectsPathVariable's own doc for the measured evidence.
		assertCorpusResultsMatch(t, false, !projectsPathVariable(q.query), gotResult, wantResult)
	}

	nonEmpty = len(wantResult.Paths) > 0 || len(wantResult.Literals) > 0
	return servedOrAllowed, nonEmpty
}

// --- Write-through rerun (re-basing this suite on write-through) ----------
//
// The comparison above (TestPrebuiltCorpusDifferential's main body) proves
// the corpus's 222 active queries agree between bt and oracleDB against
// REBUILD-derived replica state: d.engine.RebuildNow, called once, right
// after graphtest.LoadCorpusFixture, loads the whole fixture from
// PostgreSQL in one shot. That never exercises the write-through path at
// all -- every one of write_observer.go's recognized write shapes could be
// silently broken and this suite would still pass, since RebuildNow always
// starts from PostgreSQL's own ground truth regardless of how the replica
// got there before the rebuild.
//
// The "writethrough" subtest above closes that gap: it runs a small,
// standard mutation batch through bt (runCorpusWriteThroughPreamble, four
// of writethrough_differential_integration_test.go's own already-proven
// mutation classes -- 1, 2, 5, and 7) with RebuildCount pinned around it,
// then reruns the IDENTICAL 222-query comparison (runOneCorpusQueryComparison,
// shared with the original pass) against whatever state the write-through
// path alone produced. Because the mutation touches real corpus data (a
// real :User node's own objectid; a real :CrossForestTrust edge) rather
// than an inert namespace of its own, the rerun's queries see genuinely
// different results than the first pass did -- not a no-op rerun of the
// same comparison against unchanged data, the shape this package's own
// recent history (TestDAWGSCorpus's redundant "delegating" pass, since
// removed) warns against reintroducing.
//
// # Measured blast radius
//
// Exactly 9 of the 222 active queries return different content in the
// rerun (measured 2026-09-09 by diffing each query's own rendered result,
// oracle-side only, before vs. after runCorpusWriteThroughPreamble against
// a freshly loaded fixture -- independent of bt/write-through fidelity,
// which the rerun itself separately checks): "All Kerberoastable users"
// and "Kerberoastable members of Tier Zero / High Value groups" (agt+agi
// each, from class 1/2's own hasspn/admincount properties -- see
// runCorpusWriteThroughPreamble's own doc), "Map domain trusts" and
// "Cross-forest trusts with abusable configuration" (agt+agi each, from
// class 7's own CrossForestTrust deletion), and the "Administrator"
// selector (from class 2 merging directly onto that well-known principal,
// whose own objectid happens to be this fixture's first :User by scan
// order). This replaces an earlier version of this preamble that moved
// only 7 of 222 (a narrower class-1/2 property set, and a class-7 deletion
// targeting a low-salience filler :MemberOf edge instead) -- narrower than
// this doc previously (inaccurately) described as "dozens". This corpus's
// own :User-scanning queries that DON'T also filter on hasspn/
// admincount/enabled (the properties this preamble's class 1/2 nodes
// actually carry) never notice the new/merged node at all: seeing it show
// up in exactly the queries whose predicates it satisfies, and nowhere
// else, is the correct outcome of a targeted mutation, not evidence of a
// weak one -- see runCorpusWriteThroughPreamble's own doc for why this
// preamble's class-7 target changed for a correctness reason (I3-shaped:
// provably outside every shortestPath(...) traversal's own relationship-
// kind alternation) independent of this blast-radius question, and why
// widening class 1/2 further (e.g. a MemberOf edge into a real corpus
// group) was deliberately left for a future pass rather than risking
// interaction with orderedCorpusQueryName's own tie-aware comparison
// without dedicated validation.
//
// writeThroughPreambleEnabled gates this whole phase (shared with
// builder_differential_matrix_integration_test.go's own "fixtures_writethrough"
// subtest): unset (the default) runs it, since it is cheap -- a handful of
// extra writes against an already-open connection, plus one more pass over
// an already-loaded 222-query set (roughly doubling this one test's own
// ~2s runtime, not the whole suite's). Set writeThroughPreambleSkipEnv to
// any non-empty value to skip it, mirroring this package's only other
// test-file env knob, graphtest.PGAvailable's own BLOODTRAIL_TEST_PG (which
// gates whether these suites run at all).
const writeThroughPreambleSkipEnv = "BLOODTRAIL_TEST_SKIP_WRITETHROUGH_PREAMBLE"

func writeThroughPreambleEnabled() bool {
	return os.Getenv(writeThroughPreambleSkipEnv) == ""
}

// runCorpusWriteThroughPreamble performs a small, standard mutation batch
// through bt (the driver under test) after the corpus fixture's initial,
// rebuild-derived comparison pass has already completed -- see this file's
// "write-through rerun" section doc, above, for why and how the caller uses
// this.
//
// Mirrors four of writethrough_differential_integration_test.go's own
// mutation classes -- 1 (objectid upsert creating a node), 2 (objectid
// upsert merging properties/kinds onto an existing node), 5 (batch.
// UpdateNodes carrying AddedKinds/DeletedKinds and a deleted property), and
// 7 (batch.DeleteRelationship by id) -- chosen because all four are already
// proven write-observer-recognized shapes in that file's own passing suite,
// so this preamble carries no risk of tripping fallback by accident; a
// shape that unexpectedly did would be a genuine finding, not something to
// route around (see TestPrebuiltCorpusDifferential's own assertNoNewFallback
// call on the "writethrough" subtest).
//
// Every write here targets real corpus-fixture data -- a real :User node's
// own objectid (class 2's target, read live from oracleDB, never
// hardcoded, matching builder_differential_matrix_integration_test.go's own
// firstEdgeAnchor/firstNodeKindOf precedent for deriving anchors from real
// data rather than fixture-internal literals), and a real :CrossForestTrust
// edge (class 7's target) -- rather than an inert namespace of its own, so
// the mutations are visible to, not invisible to, the 222-query rerun that
// follows. Because both bt and oracleDB read the identical post-mutation
// PostgreSQL state, the rerun's pass/fail verdict never depends on knowing
// the "right" answer in advance -- only on whether the two sides still
// agree.
//
// # Class 1/2 properties: hasspn/admincount, not just a bare :User
//
// The class-1/class-2 nodes carry hasspn=true and admincount=true (on top
// of enabled=true), not just a kind and a throwaway marker property --
// these are properties several real corpus predicate families actually
// filter on (the Kerberoastable-user family's own `u.hasspn = true AND
// u.enabled = true AND ...`; the "admincount" hygiene/selector family).
// Without them, an earlier version of this preamble's class-1/2 nodes were
// only ever visible to a query that scans :User with NO further predicate
// at all -- vanishingly few of the corpus' actual :User-touching queries,
// since almost every one of them filters on something. See this file's own
// "write-through rerun" section doc, above, for the measured before/after
// blast radius this widened property set (together with the class-7
// retarget below) produces.
//
// # Class 7 target: CrossForestTrust, not a :MemberOf edge
//
// An earlier version of this preamble deleted the highest-id :MemberOf
// edge (seedFillerPopulation runs last in LoadCorpusFixture's own seeding
// order, internal/graphtest/corpusfixture.go, so that edge is very likely
// one of its own low-salience filler group memberships). Measured
// (2026-09-09): that deletion moved 6 of the corpus' 22 shortestPath(...)
// queries -- entirely legitimate, deterministic content changes in
// themselves (removing a filler group's only edge removes that group's own
// row from every unconstrained-source shortestPath query it used to
// qualify for), but landing squarely in the one corpus family
// (knownAmbiguousQueries, above) whose own tie-allowlist was calibrated
// against the PRE-mutation graph and whose comparison is by exact path
// signature: a shortestPath tie is implementation-defined by construction
// (either engine may pick a different one of several equally-short
// candidates), so widening this preamble's edge-deletion footprint inside
// that specific family is exactly the wrong place to add mutation risk --
// a future edit disturbing the graph's tie structure near this edge could
// make bt and the oracle pick different tied paths and fail spuriously,
// with no actual write-through bug behind it.
//
// CrossForestTrust carries no such risk: it is not a member of ANY
// shortestPath(...) query's own relationship-kind alternation list
// anywhere in the corpus (verified 2026-09-09 by parsing every
// shortestPath(...) pattern in testdata/prebuilt/{agt,agi,selectors}.json
// and checking CrossForestTrust's absence from all 106 relationship kinds
// those alternations collectively reference) -- deleting it is therefore
// PROVABLY, not just empirically, outside every shortestPath traversal's
// reach, by the query text alone, independent of this fixture's own
// current tie structure. It is still a real, visible mutation: "Map domain
// trusts" and "Cross-forest trusts with abusable configuration" (agt+agi
// each) both match CrossForestTrust directly in a plain (non-shortestPath)
// MATCH, so deleting the fixture's one such edge (domain1 -> domain2,
// seedTrustsAndRelay) removes exactly one row from each, a deterministic
// change both bt and the oracle must agree on.
//
// The objectid unique index classes 1/2 need (UpdateNodeBy's ON CONFLICT
// target) is asserted here, against this fixture specifically -- verified
// (2026-09-09, against a freshly loaded corpus fixture) to carry no
// duplicate objectid across its ~292 objectid-bearing nodes, so creating it
// this late (after the fixture, not before, unlike writethrough_differential_
// integration_test.go's own wipe-first ordering) cannot conflict.
//
// # Why the constraint is asserted through a THROWAWAY driver, not bt itself
//
// bt's own *pg.Driver.SchemaManager caches, per graph name, whatever
// model.Graph its FIRST AssertGraph call for that name resolved to
// (dawgs' drivers/pg/manager.go, SchemaManager.AssertGraph: "if
// graphInstance, isDefined := s.graphs[schema.Name]; isDefined { ... return
// graphInstance, nil }" -- a fast-path that skips calling query.On(tx).
// AssertGraph entirely, so it never re-diffs or re-syncs constraints once a
// name is cached). TestPrebuiltCorpusDifferential's own setup already
// called bt.AssertSchema(graphtest.CorpusSchema()) (no NodeConstraints)
// on this exact bt instance before this preamble ever runs, so a second
// bt.AssertSchema call naming the SAME graph name (even with different
// NodeConstraints) is a complete no-op: it returns the already-cached
// definition without ever issuing the underlying CREATE UNIQUE INDEX SQL.
// This was discovered empirically (not assumed): the first version of this
// preamble called bt.AssertSchema directly here, and the very next
// UpdateNodeBy failed with PostgreSQL error 42P10 ("there is no unique or
// exclusion constraint matching the ON CONFLICT specification") --
// confirming the index was genuinely never created.
//
// The fix is a throwaway *pg.Driver sharing bt's own connection pool,
// asserted exactly once, whose SchemaManager cache starts empty for this
// graph name -- its one AssertGraph call therefore reaches the real
// diff-and-sync path and issues the CREATE UNIQUE INDEX for real. This
// works because the constraint is a physical PostgreSQL object, not
// per-driver-instance state: dawgs' node-upsert SQL (drivers/pg/query/
// format.go's FormatNodeUpsert) builds its "on conflict (...)" clause
// purely from the NodeUpdate's own IdentityProperties at write time, with
// no dependency on whatever bt's own SchemaManager cache happens to
// believe about constraints -- so bt (whose cache never learns about this
// index at all) can still issue UpdateNodeBy calls against it successfully,
// the moment it physically exists.
func runCorpusWriteThroughPreamble(t *testing.T, ctx context.Context, bt, oracleDB graph.Database, pool *pgxpool.Pool) {
	t.Helper()

	// Read BEFORE any write below: class 2 merges onto whichever :User the
	// oracle's own current data names here, so this must be a genuinely
	// pre-existing fixture principal, not (by scan-order accident) the
	// brand-new node class 1 is about to create. ORDER BY id(u) makes
	// "first" deterministic (id order) rather than an unordered scan's own
	// incidental, unspecified row order.
	existingUserOID := cypherStringValue(t, ctx, oracleDB, `MATCH (u:User) WHERE u.objectid IS NOT NULL RETURN u.objectid ORDER BY id(u) LIMIT 1`)

	// Never explicitly closed: pg.Driver.Close closes the *pgxpool.Pool it
	// was given -- here, bt's own shared pool -- and closing that out from
	// under a driver instance every other part of this suite keeps using
	// (bt, oracleDB) would break them, not just this throwaway driver. A
	// one-off *pg.Driver value sharing pool solely to reach the real
	// AssertSchema diff-and-sync path once (see "Why the constraint is
	// asserted through a THROWAWAY driver" below) is worth that small,
	// bounded leak of one Go value for this test process' lifetime; it is
	// not worth (or safe to) "fix" by adding a Close call here.
	schema := graph.Schema{DefaultGraph: graph.Graph{
		Name:            graphtest.GraphName,
		NodeConstraints: []graph.Constraint{{Field: "objectid", Type: graph.BTreeIndex}},
	}}
	if err := pg.NewDriver(size.Gibibyte, pool).AssertSchema(ctx, schema); err != nil {
		t.Fatalf("write-through preamble: assert schema with objectid constraint: %v", err)
	}

	userKind := graph.StringKind("User")
	tempKind := graph.StringKind("WriteThroughPreambleTemp")
	taggedKind := graph.StringKind("WriteThroughPreambleTagged")
	mergedKind := graph.StringKind("WriteThroughPreambleMerged")

	// Class 1: objectid upsert creating a brand-new User node -- visible to
	// every corpus query that scans :User AND filters on hasspn/admincount/
	// enabled (see this function's own "Class 1/2 properties" doc for why
	// those specific properties, not just enabled, and this file's "write-
	// through rerun" section doc for the measured resulting blast radius).
	const newObjectID = "WriteThroughPreambleUser"
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateNodeBy(objectIDUpdate(newObjectID,
			graph.NewProperties().Set("name", "Write-Through Preamble User").Set("enabled", true).
				Set("hasspn", true).Set("admincount", true).Set("gmsa", false).Set("msa", false).
				Set("writethroughtemp", "gone-soon"),
			userKind, tempKind))
	}); err != nil {
		t.Fatalf("write-through preamble: objectid upsert create: %v", err)
	}

	newNodeResult, err := runCorpusQuery(t, ctx, bt, fmt.Sprintf(`MATCH (n) WHERE n.objectid = %s RETURN n`, cypherStringLiteral(newObjectID)))
	if err != nil {
		t.Fatalf("write-through preamble: locate new node: %v", err)
	}
	newIDs := extractNodeIDs(newNodeResult)
	if len(newIDs) != 1 {
		t.Fatalf("write-through preamble: locate new node: got %d nodes, want 1", len(newIDs))
	}
	newNodeID := newIDs[0]

	// Class 2: objectid upsert merging properties and unioning a kind onto
	// an EXISTING corpus :User -- the same hasspn/admincount pair class 1
	// carries, so this genuinely pre-existing principal also becomes
	// visible to the same predicate families, not just the merge marker
	// itself.
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateNodeBy(objectIDUpdate(existingUserOID,
			graph.NewProperties().Set("writethroughmerged", "yes").Set("hasspn", true).Set("admincount", true),
			mergedKind))
	}); err != nil {
		t.Fatalf("write-through preamble: objectid upsert merge: %v", err)
	}

	// Class 5: batch.UpdateNodes carrying AddedKinds/DeletedKinds and a
	// deleted property, targeting the class-1 node above by id (seeded with
	// a throwaway second kind and property for exactly this step to
	// remove).
	update := &graph.Node{ID: newNodeID, Kinds: graph.Kinds{taggedKind}, DeletedKinds: graph.Kinds{tempKind}, Properties: graph.NewProperties()}
	update.Properties.Delete("writethroughtemp")
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateNodes([]*graph.Node{update})
	}); err != nil {
		t.Fatalf("write-through preamble: batch UpdateNodes: %v", err)
	}

	// Class 7: batch.DeleteRelationship by id -- this fixture's one
	// CrossForestTrust edge (domain1 -> domain2, seedTrustsAndRelay), never
	// a :MemberOf edge -- see this function's own "Class 7 target" doc for
	// why: CrossForestTrust is provably outside every shortestPath(...)
	// query's own relationship-kind alternation in the corpus, so deleting
	// it can never perturb a shortestPath tie, unlike a :MemberOf edge.
	crossForestTrust, err := runCorpusQuery(t, ctx, oracleDB, `MATCH ()-[r:CrossForestTrust]->() RETURN r LIMIT 1`)
	if err != nil {
		t.Fatalf("write-through preamble: locate CrossForestTrust edge to delete: %v", err)
	}
	relIDs := extractEdgeIDs(crossForestTrust)
	if len(relIDs) != 1 {
		t.Fatalf("write-through preamble: locate CrossForestTrust edge to delete: got %d, want 1", len(relIDs))
	}
	if err := bt.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.DeleteRelationship(relIDs[0])
	}); err != nil {
		t.Fatalf("write-through preamble: DeleteRelationship: %v", err)
	}
}
