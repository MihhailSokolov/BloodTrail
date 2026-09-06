// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// prebuiltShortestPathQuery mirrors one entry of
// testdata/prebuilt_shortest_path.json (the same JSON shape as
// recognize/testdata/prebuilt_shortest_path.json, copied rather than moved
// -- see the task report for why recognize/cypher.go and its test could not
// yet be deleted).
type prebuiltShortestPathQuery struct {
	Name   string `json:"name"`
	Cypher string `json:"cypher"`
}

func loadPrebuiltShortestPathQueries(t *testing.T) []prebuiltShortestPathQuery {
	t.Helper()
	data, err := os.ReadFile("testdata/prebuilt_shortest_path.json")
	if err != nil {
		t.Fatalf("reading testdata: %v", err)
	}
	var queries []prebuiltShortestPathQuery
	if err := json.Unmarshal(data, &queries); err != nil {
		t.Fatalf("unmarshalling testdata: %v", err)
	}
	return queries
}

// bareKindPattern finds every `identifier:KindName` occurrence in raw
// Cypher text that syntactically denotes a kind: a node pattern label
// (`(s:Group)`), an inline-map-bearing node pattern (`(n:User {...})`), or a
// WHERE-position kind matcher, which is written with the exact same
// `(var:Kind)` shape a node pattern uses (the frontend distinguishes them by
// grammar *position*, not spelling). It intentionally does not need to
// distinguish those cases: this helper only builds a superset kind table for
// the test snapshot, so over-registering a name pg's driver would also
// accept is harmless (registering a kind Plan never actually needs to
// resolve for a given query has no effect on that query's outcome).
var bareKindPattern = regexp.MustCompile(`\(\s*\w*\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*[){]`)

// edgeKindListPattern extracts a relationship pattern's "|"-joined kind
// disjunction, e.g. "Owns|GenericAll|...|AbuseTGTDelegation" out of
// `[:Owns|GenericAll|...|AbuseTGTDelegation*1..]` or a fixed `[:HasSession]`
// -- capturing everything between `[:` and whichever of `*` (a var-length
// range) or `]` comes first.
var edgeKindListPattern = regexp.MustCompile(`\[:([^\]*]+)`)

// discoverKinds extracts every kind name referenced anywhere in texts (both
// node/kind-matcher labels and relationship type disjunctions) via the two
// patterns above, and returns a snapshot.KindTable-ready id/name map
// covering all of them plus any names in extra. This is a test-only
// convenience: writing out the ~90 distinct relationship kind names the
// prebuilt corpus's patterns embed (see testdata/prebuilt_shortest_path.json)
// by hand would be both tedious and a likely source of a silent, hard-to-spot
// transcription gap that would make an entry spuriously reject on an unknown
// kind name -- extracting them mechanically from the same query text the
// golden table already carries is more reliable.
func discoverKinds(texts []string, extra ...string) map[snapshot.KindID]string {
	names := map[string]bool{}
	for _, text := range texts {
		for _, m := range bareKindPattern.FindAllStringSubmatch(text, -1) {
			names[m[1]] = true
		}
		for _, m := range edgeKindListPattern.FindAllStringSubmatch(text, -1) {
			for _, kind := range splitPipe(m[1]) {
				names[kind] = true
			}
		}
	}
	for _, e := range extra {
		names[e] = true
	}

	pairs := make(map[snapshot.KindID]string, len(names))
	var id snapshot.KindID
	for name := range names {
		pairs[id] = name
		id++
	}
	return pairs
}

func splitPipe(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '|' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

// planTestCase is one row of the golden serve/delegate table.
type planTestCase struct {
	name   string
	cypher string
	want   bool
}

// runPlanGolden parses each case's cypher text exactly as the engine would
// (frontend.ParseCypher against a zero-filter context -- the same context
// the pg driver itself uses) and asserts Plan's ok result against want. A
// parse failure is treated as "not served" directly, without ever calling
// Plan -- exactly mirroring production, where a parse error is caught and
// delegated before Plan ever runs.
func runPlanGolden(t *testing.T, snap *snapshot.Snapshot, cases []planTestCase) {
	t.Helper()
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			rq, err := frontend.ParseCypher(frontend.NewContext(), tc.cypher)
			if err != nil || rq == nil {
				if tc.want {
					t.Fatalf("query failed to parse (want served): %v\nquery: %s", err, tc.cypher)
				}
				return
			}
			_, ok := Plan(rq, snap)
			if ok != tc.want {
				t.Fatalf("Plan() ok = %v, want %v\nquery: %s", ok, tc.want, tc.cypher)
			}
		})
	}
}

// corpusAggregationQuery and corpusCollectAntiJoinQuery are the two
// pre-built queries the brief specifically names as required serve=true
// cases beyond the shortestPath corpus (verbatim from upstream's
// commonSearchesAGT.ts, "Kerberoastable users with most admin privileges"
// and "Domain Admins logons to non-Domain Controllers" respectively -- see
// the task report for the extraction).
const (
	corpusAggregationQuery = `MATCH (u:User)
WHERE u.hasspn = true
  AND u.enabled = true
  AND NOT u.objectid ENDS WITH '-502'
  AND NOT COALESCE(u.gmsa, false) = true
  AND NOT COALESCE(u.msa, false) = true
MATCH (u)-[:MemberOf|AdminTo*1..]->(c:Computer)
WITH DISTINCT u, COUNT(c) AS adminCount
RETURN u
ORDER BY adminCount DESC
LIMIT 100`

	corpusCollectAntiJoinQuery = `MATCH (s)-[:MemberOf*0..]->(g:Group)
WHERE g.objectid ENDS WITH '-516'
WITH COLLECT(s) AS exclude
MATCH p = (c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group)
WHERE g.objectid ENDS WITH '-512' AND NOT c IN exclude
RETURN p
LIMIT 1000`

	// corpusMixedChainPathQuery and corpusADCSMixedVarChainQuery are Task 8b's
	// own gap-defining corpus shapes (see the task's own brief): a named path
	// over a fixed-then-var chain, and an ADCS-shaped chain with TWO
	// var-length steps ("*0.." leading, a fixed "*1"-implicit trailing hop)
	// separated by fixed hops. Both must plan+serve now that exec.go's
	// expandChainComponent exists; TestExecChain* (exec_test.go) exercises
	// their actual execution/PathVal content.
	corpusMixedChainPathQuery = `MATCH p = (c:Computer)-[:HasSession]->(u:User)-[:MemberOf*1..]->(g:Group)
RETURN p
LIMIT 1000`

	corpusADCSMixedVarChainQuery = `MATCH p = (u)-[:MemberOf*0..]->(g)-[:Enroll]->(ct:CertTemplate)-[:PublishedTo]->(ca:CA)
RETURN p`
)

func testSnapshot(t *testing.T, corpusTexts []string) *snapshot.Snapshot {
	t.Helper()
	kinds := discoverKinds(corpusTexts,
		// Extra kinds this file's own hand-written cases need beyond the
		// corpus/aggregation/collect queries above (deliberately NOT
		// including "NoSuchKind" -- that omission is the point of the
		// unknown-kind-name reject case).
		"X", "Y", "User", "Computer", "Group",
	)
	b := snapshot.NewBuilder(1)
	b.SetKinds(kinds)
	snap, err := b.Build()
	if err != nil {
		t.Fatalf("building test snapshot: %v", err)
	}
	return snap
}

// TestPlanServeDelegateMatrix is the brief's Step 1 golden table: every
// pre-built shortestPath entry from the migrated JSON corpus (all served
// except the one upstream ships fully commented out, which fails to parse
// at all -- see loadPrebuiltShortestPathQueries's caller below), the corpus
// aggregation query, the COLLECT anti-join query, a standalone `*0..`
// pattern, an inline property map, and the full delegate list named in the
// brief.
func TestPlanServeDelegateMatrix(t *testing.T) {
	corpus := loadPrebuiltShortestPathQueries(t)
	if len(corpus) != 12 {
		t.Fatalf("testdata entry count = %d, want 12", len(corpus))
	}

	texts := make([]string, 0, len(corpus)+2)
	for _, q := range corpus {
		texts = append(texts, q.Cypher)
	}
	texts = append(texts, corpusAggregationQuery, corpusCollectAntiJoinQuery, corpusMixedChainPathQuery, corpusADCSMixedVarChainQuery)

	snap := testSnapshot(t, texts)

	cases := make([]planTestCase, 0, len(corpus)+16)
	for _, q := range corpus {
		want := q.Name != "Shortest paths from Owned objects to Tier Zero" // the disabled, comment-only entry
		cases = append(cases, planTestCase{name: "corpus/" + q.Name, cypher: q.Cypher, want: want})
	}

	cases = append(cases,
		planTestCase{name: "corpus aggregation query", cypher: corpusAggregationQuery, want: true},
		planTestCase{name: "COLLECT anti-join query", cypher: corpusCollectAntiJoinQuery, want: true},
		planTestCase{name: "Task 8b: mixed fixed+var named-path chain", cypher: corpusMixedChainPathQuery, want: true},
		planTestCase{name: "Task 8b: ADCS-shaped multi-var-length chain", cypher: corpusADCSMixedVarChainQuery, want: true},
		planTestCase{name: "standalone *0..", cypher: `MATCH (n)-[:X*0..]->(m) RETURN n`, want: true},
		planTestCase{name: "inline property map", cypher: `MATCH (n:User {name:'X'}) RETURN n`, want: true},

		planTestCase{name: "OPTIONAL MATCH", cypher: `OPTIONAL MATCH (n:User) RETURN n`, want: false},
		planTestCase{name: "RETURN *", cypher: `MATCH (n:User) RETURN *`, want: false},
		planTestCase{name: "MERGE", cypher: `MERGE (n:User {name:'X'}) RETURN n`, want: false},
		planTestCase{name: "RETURN sum(n.x)", cypher: `MATCH (n:User) RETURN sum(n.x)`, want: false},
		planTestCase{name: "RETURN collect(n)", cypher: `MATCH (n:User) RETURN collect(n)`, want: false},
		planTestCase{name: "invalid regex", cypher: `MATCH (n:User) WHERE n.x =~ '[invalid' RETURN n`, want: false},
		planTestCase{name: "ORDER BY property", cypher: `MATCH (n:User) RETURN n ORDER BY n.name`, want: false},
		planTestCase{name: "unknown kind", cypher: `MATCH (n:NoSuchKind) RETURN n`, want: false},
		planTestCase{name: "undirected var-length", cypher: `MATCH (a)-[:X*1..]-(b) RETURN a`, want: false},
		planTestCase{name: "parameter in WHERE", cypher: `MATCH (n:User) WHERE n.x = $param RETURN n`, want: false},
		planTestCase{name: "SKIP with parameter", cypher: `MATCH (n:User) RETURN n SKIP $n`, want: false},
		planTestCase{name: "edge property access", cypher: `MATCH (a)-[r:X]->(b) WHERE r.isacl = true RETURN a`, want: false},
	)

	runPlanGolden(t, snap, cases)
}

// TestPlanRejectMatrix covers additional out-of-matrix constructs the brief
// names but that are not already exercised by TestPlanServeDelegateMatrix's
// required corpus/delegate rows.
func TestPlanRejectMatrix(t *testing.T) {
	snap := testSnapshot(t, nil)

	cases := []planTestCase{
		{name: "duplicate conflicting id anchors", cypher: `MATCH (s),(t) WHERE id(s) = 1 AND id(s) = 2 RETURN s`, want: false},
		{name: "duplicate identical id anchors dedup", cypher: `MATCH (s) WHERE id(s) = 1 AND id(s) = 1 RETURN s`, want: true},
		{name: "unwind", cypher: `UNWIND [1,2,3] AS x RETURN x`, want: false},
		{name: "quantifier", cypher: `MATCH (n:User) WHERE ANY(x IN n.spns WHERE x = 'a') RETURN n`, want: false},
		{name: "pattern predicate", cypher: `MATCH (n:User) WHERE (n)-[:X]->() RETURN n`, want: false},
		{name: "list comprehension", cypher: `MATCH (n:User) RETURN [x IN n.spns] AS s`, want: false},
		{name: "unknown function", cypher: `MATCH (n:User) RETURN keys(n)`, want: false},
		{name: "min", cypher: `MATCH (n:User) RETURN min(n.x)`, want: false},
		{name: "max", cypher: `MATCH (n:User) RETURN max(n.x)`, want: false},
		{name: "avg", cypher: `MATCH (n:User) RETURN avg(n.x)`, want: false},
		{name: "size on non-property-lookup", cypher: `MATCH (n:User) WHERE size(n) > 1 RETURN n`, want: false},
		{name: "size on property lookup ok", cypher: `MATCH (n:User) WHERE size(n.spns) > 1 RETURN n`, want: true},
		{name: "parameter as node property value", cypher: `MATCH (n:User {name:$x}) RETURN n`, want: false},
		{name: "multi-part with beyond one boundary", cypher: `MATCH (a:User) WITH a MATCH (b:Computer) WITH b MATCH (c:Group) RETURN c`, want: false},
		{name: "with-level where", cypher: `MATCH (a:User) WITH a WHERE a.enabled = true RETURN a`, want: false},
		{name: "aliased plain variable in with", cypher: `MATCH (a:User) WITH a AS b RETURN b`, want: false},
		{name: "with constant literal", cypher: `MATCH (a:User) WITH a, 1 AS one RETURN a, one`, want: true},
		{name: "collect used as projected value", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs RETURN xs`, want: false},
		{name: "collect alias used outside membership", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (b:User) WHERE xs = b RETURN b`, want: false},
		{name: "collect alias positive IN", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (b:User) WHERE b IN xs RETURN b`, want: true},

		// Every other position a CollectMembership alias's own IN-comparison
		// can legally appear in, alongside the bare form above: Plan's
		// checkExpr threads a "predicatePosition" flag through exactly the
		// AST shapes the executor's own evalWhereWithMembership recursion
		// also passes through unmodified (Parenthetical/Negation/
		// Conjunction/Disjunction), so the membership bypass stays legal
		// through every one of them.
		{name: "collect alias IN parenthesized", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (b:User) WHERE (b IN xs) RETURN b`, want: true},
		{name: "collect alias IN inside conjunction", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (b:User) WHERE b.name = 'x' AND b IN xs RETURN b`, want: true},
		{name: "collect alias IN inside disjunction", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (b:User) WHERE b.name = 'x' OR b IN xs RETURN b`, want: true},
		{name: "collect alias NOT IN (negation)", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (b:User) WHERE NOT b IN xs RETURN b`, want: true},
		{name: "collect alias NOT (IN) parenthesized negation", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (b:User) WHERE NOT (b IN xs) RETURN b`, want: true},

		// Finding 2 (2026-09-05 review): checkInOperands used to accept a
		// CollectMembership alias appearing in ANY partial of a comparison
		// chain, but the executor's tryMembershipComparison only ever
		// recognizes a single-Partial *cypher.Comparison reachable directly
		// from evalWhereWithMembership's own boolean-structural recursion --
		// a served-but-unexecutable mismatch (a safe ErrUnsupported abort at
		// runtime, but not a clean plan-time reject). Both shapes below
		// parse as one Comparison with the membership IN nested as a mere
		// *value* operand of another Comparison (Cypher's grammar binds IN
		// tighter than the relational operators, so `u = c IN xs` is `u =
		// (c IN xs)`, not a flat two-Partial chain) -- eval.go's EvalValue
		// has no case for a *cypher.Comparison at all, so neither shape's
		// nested membership check is ever reachable by the executor's
		// structural recursion, regardless of how it parses.
		{name: "collect alias IN chained after equality rejected", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (u:User),(c:User) WHERE u = c IN xs RETURN u`, want: false},
		{name: "collect alias IN chained before equality rejected", cypher: `MATCH (a:User) WITH COLLECT(a) AS xs MATCH (c:User) WHERE c IN xs = true RETURN c`, want: false},
		{name: "plain named path projected", cypher: `MATCH p = (a:User)-[:X]->(b:User) RETURN p`, want: true},
		{name: "named path used in WHERE rejected", cypher: `MATCH p = (a:User)-[:X]->(b:User) WHERE p = a RETURN a`, want: false},

		// Task 8b: a named path symbol must not cross a WITH boundary --
		// planWith's plain-variable-carry-over branch already rejects a
		// symPath kind (checked here, not merely reasoned about).
		{name: "named path symbol carried across WITH boundary rejected", cypher: `MATCH p = (a:User)-[:X]->(b:User) WITH p MATCH (c:User) RETURN c`, want: false},
		{name: "id nested in arithmetic rejected", cypher: `MATCH (n:User) RETURN id(n) + 1 AS x`, want: false},
		{name: "bare id call ok", cypher: `MATCH (n:User) RETURN id(n) AS x`, want: true},
		{name: "size nested in function rejected", cypher: `MATCH (n:User) RETURN toString(size(n.spns)) AS x`, want: false},
		{name: "shortestPath both endpoints unconstrained rejected", cypher: `MATCH p = shortestPath((s)-[:X*1..]->(t)) WHERE s<>t RETURN p`, want: false},
		{name: "shortestPath one endpoint constrained by kind ok", cypher: `MATCH p = shortestPath((s)-[:X*1..]->(t:User)) WHERE s<>t RETURN p`, want: true},
		{name: "shortestPath one endpoint constrained by id ok", cypher: `MATCH p = shortestPath((s)-[:X*1..]->(t)) WHERE id(t) = 1 AND s<>t RETURN p`, want: true},
		{name: "objectid anchor", cypher: `MATCH (n:User) WHERE n.objectid = 'S-1-1' RETURN n`, want: true},
		{name: "datetime with argument rejected", cypher: `MATCH (n:User) WHERE n.x < datetime('2024-01-01').epochseconds RETURN n`, want: false},
		{name: "params anywhere in return", cypher: `MATCH (n:User) RETURN $x`, want: false},
		{name: "bare comparison as return value rejected", cypher: `MATCH (n:User) RETURN n.x = 5 AS flag`, want: false},

		// ORDER BY on a node/edge/path-valued projected alias rejects
		// (finding 2): eval.go has no ordering over a node's/edge's/path's
		// value, only over the plain scalars a property lookup/function
		// call/arithmetic/literal produces.
		{name: "order by node alias rejected", cypher: `MATCH (n:User) RETURN n ORDER BY n`, want: false},
		{name: "order by edge alias rejected", cypher: `MATCH (a:User)-[r:X]->(b:User) RETURN r ORDER BY r`, want: false},
		{name: "order by path alias rejected", cypher: `MATCH p = (a:User)-[:X]->(b:User) RETURN p ORDER BY p`, want: false},
		{name: "order by scalar alias ok", cypher: `MATCH (n:User) RETURN n.name AS nm ORDER BY nm`, want: true},

		// shortestPath range restrictions (finding 3): a conservative
		// tightening over plain var-length, which keeps its existing,
		// unrestricted Min/Max acceptance.
		{name: "shortestPath range min>1 rejected", cypher: `MATCH p = shortestPath((s)-[:X*3..5]->(t:User)) RETURN p`, want: false},
		{name: "shortestPath zero-length range rejected", cypher: `MATCH p = shortestPath((s)-[:X*0..]->(t:User)) RETURN p`, want: false},
		{name: "plain var-length min>1 still accepted", cypher: `MATCH (n:User)-[:X*3..5]->(m:User) RETURN n`, want: true},
		{
			name:   "more than one shortestPath pattern part per query rejected",
			cypher: `MATCH p = shortestPath((s)-[:X*1..]->(t:User)), q = shortestPath((a)-[:X*1..]->(b:User)) RETURN p`,
			want:   false,
		},
		{
			name:   "more than one shortestPath across a WITH boundary rejected",
			cypher: `MATCH p = shortestPath((s)-[:X*1..]->(t:User)) WITH p MATCH q = shortestPath((a)-[:X*1..]->(b:User)) RETURN q`,
			want:   false,
		},

		// Edge symbol reuse across steps/pattern parts rejects (finding 4):
		// this package implements no relationship-uniqueness/per-row
		// edge-identity tracking, unlike node variable reuse (which stays
		// supported, joining by identity).
		{name: "edge symbol reused across steps in one chain rejected", cypher: `MATCH (a:User)-[r:X]->(b:User)-[r:X]->(c:User) RETURN a`, want: false},
		{name: "edge symbol reused across pattern parts rejected", cypher: `MATCH (a:User)-[r:X]->(b:User) MATCH (c:User)-[r:X]->(d:User) RETURN a`, want: false},

		// Task 8b: mixed fixed/var-length chains within one component are
		// now servable (see plan_test.go's TestPlanServeDelegateMatrix and
		// exec_test.go/expand_test.go for the executed shapes), but
		// shortestPath must stay isolated -- sharing a symbol with ANY other
		// Step in the same Part, even one from a separate comma-joined
		// PatternPart, rejects (shortestStepsAreIsolated).
		{
			name:   "shortestPath step shares symbol with a longer chain rejected",
			cypher: `MATCH (a:X)-[:X]->(b:X), p = shortestPath((b)-[:X*1..]->(c:X)) RETURN a`,
			want:   false,
		},
		{
			name:   "shortestPath isolated in its own component still ok",
			cypher: `MATCH (a:X)-[:X]->(b:X), p = shortestPath((s:X)-[:X*1..]->(t:X)) WHERE s<>t RETURN a`,
			want:   true,
		},
	}

	runPlanGolden(t, snap, cases)
}

// TestPlanInlineMapDesugarsIntoWhere is the golden test for finding 1
// (fail-unsafe IR contract): an inline node-pattern property map must
// desugar into a real equality conjunct on Part.Where itself, not merely
// into NodeConstraint.Predicates -- Part.Where must be complete and
// sufficient on its own, per Part's doc. This pins that contract by
// evaluating the compiled Where expression directly via EvalPredicate
// against both a matching and a non-matching node, exactly as an executor
// that ignores NodeConstraint.Predicates entirely (using Where alone) would.
func TestPlanInlineMapDesugarsIntoWhere(t *testing.T) {
	snap := testSnapshot(t, nil)

	rq, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n:User {name:'X'}) RETURN n`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q, ok := Plan(rq, snap)
	if !ok {
		t.Fatalf("Plan() ok = false, want true")
	}
	if len(q.Parts) != 1 {
		t.Fatalf("len(q.Parts) = %d, want 1", len(q.Parts))
	}
	part := q.Parts[0]
	if part.Where == nil {
		t.Fatal("Part.Where is nil, want the desugared `n.name = 'X'` equality")
	}

	// NodeConstraint.Predicates should also carry the same equality
	// redundantly (an executor optimization) -- but Part.Where alone must
	// already be sufficient, which is what this test actually exercises.
	nc, ok := part.Nodes["n"]
	if !ok || len(nc.Predicates) == 0 {
		t.Fatal("NodeConstraint.Predicates is empty, want the redundant pushed equality")
	}

	b := snapshot.NewBuilder(1)
	b.SetKinds(map[snapshot.KindID]string{0: "User"})
	if err := b.AddNode(1, []snapshot.KindID{0}, []byte(`{"name":"X"}`)); err != nil {
		t.Fatalf("AddNode(1): %v", err)
	}
	if err := b.AddNode(2, []snapshot.KindID{0}, []byte(`{"name":"Y"}`)); err != nil {
		t.Fatalf("AddNode(2): %v", err)
	}
	evalSnap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	env := &Env{Snap: evalSnap}

	matchID, ok := evalSnap.Dense(1)
	if !ok {
		t.Fatal("Dense(1) not found")
	}
	nonMatchID, ok := evalSnap.Dense(2)
	if !ok {
		t.Fatal("Dense(2) not found")
	}

	matchRow := NewRow()
	matchRow.SetNode("n", matchID)
	got, err := EvalPredicate(env, matchRow, part.Where)
	if err != nil {
		t.Fatalf("EvalPredicate(matching node): %v", err)
	}
	if got != TriTrue {
		t.Fatalf("EvalPredicate(matching node) = %s, want %s", got, TriTrue)
	}

	nonMatchRow := NewRow()
	nonMatchRow.SetNode("n", nonMatchID)
	got, err = EvalPredicate(env, nonMatchRow, part.Where)
	if err != nil {
		t.Fatalf("EvalPredicate(non-matching node): %v", err)
	}
	if got != TriFalse {
		t.Fatalf("EvalPredicate(non-matching node) = %s, want %s", got, TriFalse)
	}
}

// TestPlanNeverPanics feeds Plan a handful of nil/degenerate inputs to
// confirm the recover() backstop and defensive nil checks hold, per the
// brief's "never panics" requirement.
func TestPlanNeverPanics(t *testing.T) {
	snap := testSnapshot(t, nil)

	if _, ok := Plan(nil, snap); ok {
		t.Fatal("Plan(nil, snap) ok = true, want false")
	}
	if _, ok := Plan(&cypher.RegularQuery{}, snap); ok {
		t.Fatal("Plan(&RegularQuery{}, snap) ok = true, want false")
	}
	if _, ok := Plan(&cypher.RegularQuery{SingleQuery: &cypher.SingleQuery{}}, snap); ok {
		t.Fatal("Plan with empty SingleQuery ok = true, want false")
	}

	rq, err := frontend.ParseCypher(frontend.NewContext(), `MATCH (n:User) RETURN n`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, ok := Plan(rq, nil); ok {
		t.Fatal("Plan(rq, nil) ok = true, want false")
	}
	if _, ok := Plan(rq, &snapshot.Snapshot{}); ok {
		t.Fatal("Plan(rq, &Snapshot{}) ok = true, want false (nil Kinds)")
	}
}
