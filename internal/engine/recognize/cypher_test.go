// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// prebuiltShortestPathQuery mirrors one entry of testdata/prebuilt_shortest_path.json.
type prebuiltShortestPathQuery struct {
	Name   string `json:"name"`
	Cypher string `json:"cypher"`
}

// wantPrebuilt describes what FromCypher must report for one prebuilt
// query, keyed by its "name" field in the testdata.
type wantPrebuilt struct {
	ok            bool
	limit         int
	startKind     string // "" means the pattern's start node carries no kind label
	endKind       string // "" means the pattern's end node carries no kind label
	startCriteria bool   // want Start.Criteria non-nil
	endCriteria   bool   // want End.Criteria non-nil
	excludeSelf   bool
}

// wantPrebuiltQueries is this task's cross-check against the upstream
// extraction: exactly 12 entries, matching the "research says 12
// shortestPath queries" count in the task brief (verified directly against
// commonSearchesAGT.ts: grep for "shortestPath(" across its "queries"
// entries yields exactly 12, one of which -- "Shortest paths from Owned
// objects to Tier Zero" -- is entirely commented out in the source as a
// disabled many-to-many example).
//
// Of the 12, 11 match FromCypher's accepted shape and 1 does not:
//   - "Shortest paths from Owned objects to Tier Zero" is the commented-out
//     placeholder above; its "cypher" text is Cypher line-comments only, so
//     it fails to parse into a queryable statement at all.
//
// Two entries only became accepts with the endpoint-predicate-lifting
// extension (see cypher.go's FromCypher doc, case 4) -- before it, both were
// principled rejects, since the original grammar's WHERE clause recognized
// only a closed set of shapes:
//   - "Shortest paths to Domain Admins from Kerberoastable users" uses
//     "AND NOT s.objectid ENDS WITH ..." and "AND NOT COALESCE(...) = true"
//     in its WHERE; each NOT/COALESCE conjunct is scoped to the single
//     variable s alone, so it lifts onto Start.Criteria. It has no s<>t
//     conjunct at all, so ExcludeSelf is false for this entry (unlike every
//     other accepted prebuilt query).
//   - "Shortest paths to privileged roles" matches its target's name with
//     "=~" (regex match); that Comparison is scoped to t alone and lifts
//     onto End.Criteria.
//
// This is a deliberate, principled split rather than a blanket "every entry
// recognizes": the accepted-shape paragraph is normative, and the one
// excluded prebuilt query genuinely falls outside it (it isn't parseable
// Cypher at all). Extraction fidelity (all 12 present, verbatim apart from
// interpolation expansion) is what's exhaustive here, not recognizability.
var wantPrebuiltQueries = map[string]wantPrebuilt{
	"Paths from Domain Users to Tier Zero / High Value targets": {
		ok: true, limit: 1000,
		startKind: "Group", endKind: "Tag_Tier_Zero",
		startCriteria: true, endCriteria: false,
		excludeSelf: true,
	},
	"Shortest paths to systems trusted for unconstrained delegation": {
		ok: true, limit: 1000,
		startKind: "", endKind: "Computer",
		startCriteria: false, endCriteria: true,
		excludeSelf: true,
	},
	"Shortest paths to Domain Admins from Kerberoastable users": {
		// WHERE has no s<>t conjunct at all -- ExcludeSelf is false, unlike
		// every other accepted prebuilt query. s carries four lifted
		// conjuncts (hasspn=true, enabled=true, NOT ... ENDS WITH '-502',
		// two NOT COALESCE(...) = true); t carries one (objectid ENDS WITH
		// '-512').
		ok: true, limit: 1000,
		startKind: "User", endKind: "Group",
		startCriteria: true, endCriteria: true,
		excludeSelf: false,
	},
	"Shortest paths to Tier Zero / High Value targets": {
		ok: true, limit: 1000,
		startKind: "", endKind: "Tag_Tier_Zero",
		startCriteria: false, endCriteria: false,
		excludeSelf: true,
	},
	"Shortest paths from Domain Users to Tier Zero / High Value targets": {
		ok: true, limit: 1000,
		startKind: "Group", endKind: "Tag_Tier_Zero",
		startCriteria: true, endCriteria: false,
		excludeSelf: true,
	},
	"Shortest paths to Domain Admins": {
		// (t:Group)<-[...]-(s:Base): direction is inbound, so FromCypher
		// must swap -- Start becomes s:Base (the arrow's tail), End becomes
		// t:Group (the arrow's head). This is the brief's required
		// spot-check entry; TestFromCypher_PrebuiltShortestPaths_DomainAdminsSpotCheck
		// re-asserts it explicitly.
		ok: true, limit: 1000,
		startKind: "Base", endKind: "Group",
		startCriteria: false, endCriteria: true,
		excludeSelf: true,
	},
	"Shortest paths from Owned objects to Tier Zero": {
		ok: false,
	},
	"Shortest paths from Owned objects": {
		ok: true, limit: 1000,
		startKind: "Base", endKind: "Base",
		startCriteria: true, endCriteria: false,
		excludeSelf: true,
	},
	"Shortest paths from Entra Users to Tier Zero / High Value targets": {
		ok: true, limit: 1000,
		startKind: "AZUser", endKind: "AZBase",
		startCriteria: false, endCriteria: true,
		excludeSelf: true,
	},
	"Shortest paths to privileged roles": {
		// t.name =~ '(?i)...' lifts onto End.Criteria; s only appears in
		// s<>t.
		ok: true, limit: 1000,
		startKind: "AZBase", endKind: "AZRole",
		startCriteria: false, endCriteria: true,
		excludeSelf: true,
	},
	"Shortest paths from Azure Applications to Tier Zero / High Value targets": {
		ok: true, limit: 1000,
		startKind: "AZApp", endKind: "AZBase",
		startCriteria: false, endCriteria: true,
		excludeSelf: true,
	},
	"Shortest paths to Azure Subscriptions": {
		ok: true, limit: 1000,
		startKind: "AZBase", endKind: "AZSubscription",
		startCriteria: false, endCriteria: false,
		excludeSelf: true,
	},
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

// edgeKindListPattern extracts the "|"-joined relationship kind list a
// prebuilt query's pattern embeds directly (e.g. "Owns|GenericAll|...") so
// the test can check FromCypher's EdgeKinds against the query text itself
// rather than re-transcribing the full ActiveDirectoryPathfindingEdges /
// AzurePathfindingEdges lists a second time.
var edgeKindListPattern = regexp.MustCompile(`\[:([^\]]+?)\*1\.\.\]`)

func expectedEdgeKinds(t *testing.T, cypherText string) graph.Kinds {
	t.Helper()

	match := edgeKindListPattern.FindStringSubmatch(cypherText)
	if match == nil {
		t.Fatalf("could not find a relationship kind list in query text: %s", cypherText)
	}

	names := strings.Split(match[1], "|")
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}

	return kinds
}

func containsKind(kinds graph.Kinds, name string) bool {
	for _, k := range kinds {
		if k.String() == name {
			return true
		}
	}
	return false
}

// TestFromCypher_PrebuiltShortestPaths exercises FromCypher against every
// entry of testdata/prebuilt_shortest_path.json -- the actual shortestPath
// query texts BloodHound's UI ships (see that file's extraction notes).
func TestFromCypher_PrebuiltShortestPaths(t *testing.T) {
	queries := loadPrebuiltShortestPathQueries(t)

	if len(queries) != 12 {
		t.Fatalf("testdata entry count = %d, want 12 (see wantPrebuiltQueries doc)", len(queries))
	}

	seen := make(map[string]bool, len(queries))

	for _, q := range queries {
		q := q
		t.Run(q.Name, func(t *testing.T) {
			want, known := wantPrebuiltQueries[q.Name]
			if !known {
				t.Fatalf("no expectation recorded for testdata entry %q -- add one to wantPrebuiltQueries", q.Name)
			}
			seen[q.Name] = true

			got, ok := FromCypher(q.Cypher)
			if ok != want.ok {
				t.Fatalf("FromCypher() ok = %v, want %v (query: %s)", ok, want.ok, q.Cypher)
			}
			if !want.ok {
				return
			}

			if got.Mode != ModeOne {
				t.Errorf("Mode = %v, want ModeOne", got.Mode)
			}
			if got.Limit != want.limit {
				t.Errorf("Limit = %d, want %d", got.Limit, want.limit)
			}
			if got.ExcludeSelf != want.excludeSelf {
				t.Errorf("ExcludeSelf = %v, want %v", got.ExcludeSelf, want.excludeSelf)
			}
			if want.startKind != "" && !containsKind(got.Start.Kinds, want.startKind) {
				t.Errorf("Start.Kinds = %v, want to contain %q", got.Start.Kinds, want.startKind)
			}
			if want.startKind == "" && len(got.Start.Kinds) != 0 {
				t.Errorf("Start.Kinds = %v, want empty", got.Start.Kinds)
			}
			if want.endKind != "" && !containsKind(got.End.Kinds, want.endKind) {
				t.Errorf("End.Kinds = %v, want to contain %q", got.End.Kinds, want.endKind)
			}
			if want.endKind == "" && len(got.End.Kinds) != 0 {
				t.Errorf("End.Kinds = %v, want empty", got.End.Kinds)
			}
			if (got.Start.Criteria != nil) != want.startCriteria {
				t.Errorf("Start.Criteria non-nil = %v, want %v", got.Start.Criteria != nil, want.startCriteria)
			}
			if (got.End.Criteria != nil) != want.endCriteria {
				t.Errorf("End.Criteria non-nil = %v, want %v", got.End.Criteria != nil, want.endCriteria)
			}

			wantEdgeKinds := expectedEdgeKinds(t, q.Cypher)
			if len(got.EdgeKinds) != len(wantEdgeKinds) {
				t.Errorf("len(EdgeKinds) = %d, want %d", len(got.EdgeKinds), len(wantEdgeKinds))
			}
			for _, k := range wantEdgeKinds {
				if !containsKind(got.EdgeKinds, k.String()) {
					t.Errorf("EdgeKinds = %v missing %q", got.EdgeKinds, k.String())
				}
			}
		})
	}

	for name := range wantPrebuiltQueries {
		if !seen[name] {
			t.Errorf("wantPrebuiltQueries has a stale entry not present in testdata: %q", name)
		}
	}
}

// TestFromCypher_PrebuiltShortestPaths_DomainAdminsSpotCheck is the brief's
// explicit spot-check for the "Shortest paths to Domain Admins" entry: its
// pattern is written with a "<-" arrow, so Start/End must come out swapped
// relative to pattern-element order.
func TestFromCypher_PrebuiltShortestPaths_DomainAdminsSpotCheck(t *testing.T) {
	queries := loadPrebuiltShortestPathQueries(t)

	var target *prebuiltShortestPathQuery
	for i := range queries {
		if queries[i].Name == "Shortest paths to Domain Admins" {
			target = &queries[i]
			break
		}
	}
	if target == nil {
		t.Fatal(`testdata is missing the "Shortest paths to Domain Admins" entry`)
	}

	got, ok := FromCypher(target.Cypher)
	if !ok {
		t.Fatalf("FromCypher() ok = false, want true (query: %s)", target.Cypher)
	}

	if !containsKind(got.Start.Kinds, "Base") {
		t.Errorf("Start.Kinds = %v, want to contain Base", got.Start.Kinds)
	}
	if !containsKind(got.End.Kinds, "Group") {
		t.Errorf("End.Kinds = %v, want to contain Group", got.End.Kinds)
	}
	if got.End.Criteria == nil {
		t.Error("End.Criteria = nil, want non-nil (t.objectid ENDS WITH '-512')")
	}
	if !got.ExcludeSelf {
		t.Error("ExcludeSelf = false, want true")
	}
}

// TestFromCypher_Accepts covers hand-written shapes beyond the prebuilt
// testdata: the integration-fixture id(<var>) = <literal> endpoint form and
// allShortestPaths -> ModeAll.
func TestFromCypher_Accepts(t *testing.T) {
	got, ok := FromCypher(`MATCH p = allShortestPaths((s)-[:A|B*1..]->(t)) WHERE id(s) = 1 AND id(t) = 2 RETURN p`)
	if !ok {
		t.Fatal("FromCypher() ok = false, want true")
	}

	if got.Mode != ModeAll {
		t.Errorf("Mode = %v, want ModeAll", got.Mode)
	}
	if len(got.Start.IDs) != 1 || got.Start.IDs[0] != 1 {
		t.Errorf("Start.IDs = %v, want [1]", got.Start.IDs)
	}
	if len(got.End.IDs) != 1 || got.End.IDs[0] != 2 {
		t.Errorf("End.IDs = %v, want [2]", got.End.IDs)
	}
	wantEdgeKinds := graph.Kinds{graph.StringKind("A"), graph.StringKind("B")}
	if len(got.EdgeKinds) != len(wantEdgeKinds) {
		t.Errorf("EdgeKinds = %v, want %v", got.EdgeKinds, wantEdgeKinds)
	}
	for _, k := range wantEdgeKinds {
		if !containsKind(got.EdgeKinds, k.String()) {
			t.Errorf("EdgeKinds = %v missing %q", got.EdgeKinds, k.String())
		}
	}
	if got.Limit != 0 {
		t.Errorf("Limit = %d, want 0 (no LIMIT clause)", got.Limit)
	}
}

// conjunctionCriteria asserts criteria is a non-nil *cypher.Conjunction --
// the shape endpointAccumulator.criteria always builds via query.And -- and
// returns it, failing the test otherwise.
func conjunctionCriteria(t *testing.T, criteria graph.Criteria) *cypher.Conjunction {
	t.Helper()

	conjunction, isConjunction := criteria.(*cypher.Conjunction)
	if !isConjunction || conjunction == nil {
		t.Fatalf("Criteria = %#v (%T), want *cypher.Conjunction", criteria, criteria)
	}
	return conjunction
}

// TestFromCypher_LiftsNegatedPredicate asserts the shape of a lifted NOT
// conjunct (the endpoint-predicate-lifting extension's case 4): a
// WHERE NOT <var>.prop ENDS WITH <string> conjunct scoped to a single
// endpoint variable lifts as a *cypher.Negation wrapping the original
// *cypher.Comparison, with the PropertyLookup's variable rewritten from the
// endpoint's parse-time symbol ("s") to the dawgs query-builder's node
// symbol (query.NodeSymbol, "n") -- exactly what query.Node() returns.
func TestFromCypher_LiftsNegatedPredicate(t *testing.T) {
	got, ok := FromCypher(`MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE NOT s.objectid ENDS WITH '-502' RETURN p`)
	if !ok {
		t.Fatal("FromCypher() ok = false, want true")
	}
	if got.End.Criteria != nil {
		t.Errorf("End.Criteria = %#v, want nil (WHERE only touches s)", got.End.Criteria)
	}

	conjunction := conjunctionCriteria(t, got.Start.Criteria)
	exprs := conjunction.GetAll()
	if len(exprs) == 0 {
		t.Fatal("Start.Criteria conjunction has no expressions")
	}

	negation, isNegation := exprs[len(exprs)-1].(*cypher.Negation)
	if !isNegation || negation == nil {
		t.Fatalf("lifted expression = %#v (%T), want *cypher.Negation", exprs[len(exprs)-1], exprs[len(exprs)-1])
	}

	cmp, isComparison := negation.Expression.(*cypher.Comparison)
	if !isComparison || cmp == nil {
		t.Fatalf("Negation.Expression = %#v (%T), want *cypher.Comparison", negation.Expression, negation.Expression)
	}
	if len(cmp.Partials) != 1 || cmp.Partials[0] == nil || cmp.Partials[0].Operator != cypher.OperatorEndsWith {
		t.Fatalf("Comparison.Partials = %#v, want a single ENDS WITH partial", cmp.Partials)
	}

	lookup, isLookup := cmp.Left.(*cypher.PropertyLookup)
	if !isLookup || lookup == nil {
		t.Fatalf("Comparison.Left = %#v (%T), want *cypher.PropertyLookup", cmp.Left, cmp.Left)
	}
	if lookup.Symbol != "objectid" {
		t.Errorf("PropertyLookup.Symbol = %q, want %q", lookup.Symbol, "objectid")
	}

	variable, isVariable := lookup.Atom.(*cypher.Variable)
	if !isVariable || variable == nil {
		t.Fatalf("PropertyLookup.Atom = %#v (%T), want *cypher.Variable", lookup.Atom, lookup.Atom)
	}
	if variable.Symbol != query.NodeSymbol {
		t.Errorf("PropertyLookup.Atom.Symbol = %q, want %q (rewritten to the query-builder node symbol)", variable.Symbol, query.NodeSymbol)
	}
}

// TestFromCypher_LiftsRegexPredicate asserts the shape of a lifted regex
// (=~) conjunct: a *cypher.Comparison whose operator is unchanged
// (OperatorRegexMatch) and whose PropertyLookup variable is rewritten to
// the query-builder node symbol, same as any other lifted comparison.
func TestFromCypher_LiftsRegexPredicate(t *testing.T) {
	got, ok := FromCypher(`MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE t.name =~ '(?i)^admin.*$' RETURN p`)
	if !ok {
		t.Fatal("FromCypher() ok = false, want true")
	}
	if got.Start.Criteria != nil {
		t.Errorf("Start.Criteria = %#v, want nil (WHERE only touches t)", got.Start.Criteria)
	}

	conjunction := conjunctionCriteria(t, got.End.Criteria)
	exprs := conjunction.GetAll()
	if len(exprs) == 0 {
		t.Fatal("End.Criteria conjunction has no expressions")
	}

	cmp, isComparison := exprs[len(exprs)-1].(*cypher.Comparison)
	if !isComparison || cmp == nil {
		t.Fatalf("lifted expression = %#v (%T), want *cypher.Comparison", exprs[len(exprs)-1], exprs[len(exprs)-1])
	}
	if len(cmp.Partials) != 1 || cmp.Partials[0] == nil || cmp.Partials[0].Operator != cypher.OperatorRegexMatch {
		t.Fatalf("Comparison.Partials = %#v, want a single =~ partial", cmp.Partials)
	}

	lookup, isLookup := cmp.Left.(*cypher.PropertyLookup)
	if !isLookup || lookup == nil {
		t.Fatalf("Comparison.Left = %#v (%T), want *cypher.PropertyLookup", cmp.Left, cmp.Left)
	}
	if lookup.Symbol != "name" {
		t.Errorf("PropertyLookup.Symbol = %q, want %q", lookup.Symbol, "name")
	}

	variable, isVariable := lookup.Atom.(*cypher.Variable)
	if !isVariable || variable == nil {
		t.Fatalf("PropertyLookup.Atom = %#v (%T), want *cypher.Variable", lookup.Atom, lookup.Atom)
	}
	if variable.Symbol != query.NodeSymbol {
		t.Errorf("PropertyLookup.Atom.Symbol = %q, want %q (rewritten to the query-builder node symbol)", variable.Symbol, query.NodeSymbol)
	}
}

// TestFromCypher_LiftsEndpointScopedOr covers the re-expressed "WHERE with
// OR" case: a parenthesized Disjunction whose variable references are
// scoped to a single endpoint variable (here, both sides of the OR touch s
// alone) is a single conjunct under classifyConjunct's flattening rule, and
// since it isn't one of the three special-cased shapes it lifts like any
// other single-variable expression -- unlike the pre-extension grammar,
// where OR was an unconditional reject. The Parenthetical wrapper from the
// source is preserved in the lifted copy (flattenConjuncts only unwraps
// Parenthetical-wrapping-Conjunction, never Parenthetical-wrapping-
// Disjunction) so the OR's grouping survives being embedded as one conjunct
// among others in Endpoint.Criteria's own query.And(...).
func TestFromCypher_LiftsEndpointScopedOr(t *testing.T) {
	got, ok := FromCypher(`MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE (s.a = 1 OR s.b = 2) RETURN p`)
	if !ok {
		t.Fatal("FromCypher() ok = false, want true")
	}
	if got.End.Criteria != nil {
		t.Errorf("End.Criteria = %#v, want nil (WHERE only touches s)", got.End.Criteria)
	}

	conjunction := conjunctionCriteria(t, got.Start.Criteria)
	exprs := conjunction.GetAll()
	if len(exprs) == 0 {
		t.Fatal("Start.Criteria conjunction has no expressions")
	}

	paren, isParenthetical := exprs[len(exprs)-1].(*cypher.Parenthetical)
	if !isParenthetical || paren == nil {
		t.Fatalf("lifted expression = %#v (%T), want *cypher.Parenthetical", exprs[len(exprs)-1], exprs[len(exprs)-1])
	}

	disjunction, isDisjunction := paren.Expression.(*cypher.Disjunction)
	if !isDisjunction || disjunction == nil {
		t.Fatalf("Parenthetical.Expression = %#v (%T), want *cypher.Disjunction", paren.Expression, paren.Expression)
	}
	if got := disjunction.GetAll(); len(got) != 2 {
		t.Fatalf("Disjunction has %d expressions, want 2", len(got))
	}
}

// TestFromCypher_LiftsArithmeticAndListPredicates exercises two lifted
// shapes beyond the brief's two required examples: an arithmetic expression
// on the left of a comparison (s.a + 1 = 2, *cypher.ArithmeticExpression)
// and a list membership test (s.b IN [1,2,3], *cypher.ListLiteral on the
// right of the comparison) -- both still scoped to the single variable s,
// so both lift onto Start.Criteria as two separate conjuncts.
func TestFromCypher_LiftsArithmeticAndListPredicates(t *testing.T) {
	got, ok := FromCypher(`MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s.a + 1 = 2 AND s.b IN [1,2,3] RETURN p`)
	if !ok {
		t.Fatal("FromCypher() ok = false, want true")
	}
	if got.End.Criteria != nil {
		t.Errorf("End.Criteria = %#v, want nil (WHERE only touches s)", got.End.Criteria)
	}

	conjunction := conjunctionCriteria(t, got.Start.Criteria)
	exprs := conjunction.GetAll()
	if len(exprs) != 2 {
		t.Fatalf("Start.Criteria has %d lifted expressions, want 2", len(exprs))
	}

	for _, expr := range exprs {
		if _, isComparison := expr.(*cypher.Comparison); !isComparison {
			t.Errorf("lifted expression = %#v (%T), want *cypher.Comparison", expr, expr)
		}
	}
}

// TestFromCypher_DedupsIdenticalIDConjunct is the accepting half of Critical
// finding 1b: a second id() conjunct for the same endpoint repeating the
// exact same value as the first is a harmless duplicate (Cypher's AND of two
// identical point-equalities matches exactly what either one alone would),
// so it must be deduped rather than rejected outright, and Start.IDs must
// end up with the single value once, not twice.
func TestFromCypher_DedupsIdenticalIDConjunct(t *testing.T) {
	got, ok := FromCypher(`MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE id(s) = 1 AND id(s) = 1 RETURN p`)
	if !ok {
		t.Fatal("FromCypher() ok = false, want true (duplicate id() conjuncts with the SAME value should dedup, not reject)")
	}
	if len(got.Start.IDs) != 1 || got.Start.IDs[0] != 1 {
		t.Errorf("Start.IDs = %v, want [1] (deduped, not doubled)", got.Start.IDs)
	}
}

// TestFromCypher_MultiKindCriteriaIsConjunctive is Critical finding 1c's
// required coverage: a multi-label pattern endpoint ((s:A:B)) whose Criteria
// path is exercised (WHERE also touches s with a lifted predicate, forcing
// endpointAccumulator.touched()) must AND its kind labels together -- one
// query.Kind(query.Node(), k) conjunct per kind, each carrying exactly one
// kind -- rather than a single multi-kind KindMatcher, which dawgs' pg
// translator would compile as an array-overlap ("matches ANY of A, B") test
// instead of Cypher's actual "matches ALL of A, B" semantics for (s:A:B).
func TestFromCypher_MultiKindCriteriaIsConjunctive(t *testing.T) {
	got, ok := FromCypher(`MATCH p=shortestPath((s:A:B)-[:R*1..]->(t)) WHERE s.name = 'x' RETURN p`)
	if !ok {
		t.Fatal("FromCypher() ok = false, want true")
	}

	conjunction := conjunctionCriteria(t, got.Start.Criteria)
	exprs := conjunction.GetAll()

	var kindMatchers []*cypher.KindMatcher
	for _, expr := range exprs {
		if km, isKindMatcher := expr.(*cypher.KindMatcher); isKindMatcher {
			kindMatchers = append(kindMatchers, km)
		}
	}

	if len(kindMatchers) != 2 {
		t.Fatalf("Start.Criteria has %d *cypher.KindMatcher conjuncts, want 2 (one per label, AND'd together); got exprs = %#v", len(kindMatchers), exprs)
	}

	seen := map[string]bool{}
	for _, km := range kindMatchers {
		if len(km.Kinds) != 1 {
			t.Errorf("KindMatcher.Kinds = %v, want exactly 1 kind per conjunct (conjunctive, not a combined array-overlap matcher)", km.Kinds)
			continue
		}
		seen[km.Kinds[0].String()] = true
	}
	if !seen["A"] || !seen["B"] {
		t.Errorf("KindMatcher kinds = %v, want both %q and %q as separate conjuncts", seen, "A", "B")
	}
}

// TestFromCypher_Rejects covers hand-written shapes FromCypher must refuse
// without panicking, each falling outside the accepted shape for a distinct
// reason. Two entries -- "WHERE with OR" and "WHERE with NOT" in the
// original (pre-endpoint-predicate-lifting) version of this test -- no
// longer hold as blanket rejects: an OR/NOT conjunct scoped to a single
// endpoint variable is now liftable (see TestFromCypher_LiftsEndpointScopedOr
// and TestFromCypher_LiftsNegatedPredicate). They're replaced here with the
// shapes that do still reject under the wider grammar: a conjunct spanning
// both endpoint variables (whether via OR or a bare comparison), one scoped
// to the relationship variable, one scoped to the path variable, and one
// containing a $parameter.
func TestFromCypher_Rejects(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{
			name: "query with WITH",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s<>t WITH p RETURN p`,
		},
		{
			name: "bounded range *1..3",
			text: `MATCH p=shortestPath((s)-[:A*1..3]->(t)) WHERE s<>t RETURN p`,
		},
		{
			name: "zero lower bound *0..",
			text: `MATCH p=shortestPath((s)-[:A*0..]->(t)) WHERE s<>t RETURN p`,
		},
		{
			name: "plain node pattern, no shortestPath",
			text: `MATCH (n) RETURN n`,
		},
		{
			name: "mutation clause",
			text: `MATCH (n) SET n.x = 1 RETURN n`,
		},
		{
			name: "RETURN with ORDER BY",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s<>t RETURN p ORDER BY p`,
		},
		{
			name: "shortestPath with a second MATCH clause",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) MATCH (x) WHERE s<>t RETURN p`,
		},
		{
			name: "WHERE with cross-variable OR",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s.a = 1 OR t.b = 2 RETURN p`,
		},
		{
			name: "WHERE predicate referencing both endpoint variables",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s.a = t.b RETURN p`,
		},
		{
			name: "WHERE predicate on the relationship variable",
			text: `MATCH p=shortestPath((s)-[r:A*1..]->(t)) WHERE r.prop = 1 RETURN p`,
		},
		{
			name: "WHERE predicate containing a $parameter",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s.a = $x RETURN p`,
		},
		{
			name: "WHERE predicate on the path variable",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE length(p) > 2 RETURN p`,
		},
		{
			name: "unparseable text",
			text: `this is not cypher at all {{{`,
		},
		{
			// Critical finding 1a: id(s)=1 together with the node pattern's
			// own kind label (s:User). resolveEndpoint (engine.go) resolves
			// an endpoint with IDs set purely by looking those ids up,
			// silently ignoring Kinds -- serving this would drop the :User
			// constraint the query actually asked for -- so FromCypher must
			// decline the whole query instead of guessing.
			name: "id() endpoint mixed with a pattern kind label",
			text: `MATCH p=shortestPath((s:User)-[:A*1..]->(t)) WHERE id(s) = 1 RETURN p`,
		},
		{
			// Critical finding 1a's other shape: id(s)=1 together with a
			// lifted property predicate (s.name = 'x') on the same
			// endpoint. Same reasoning as above, but via Endpoint.Criteria
			// (case 4) rather than a pattern-declared kind label.
			name: "id() endpoint mixed with a lifted predicate",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE id(s) = 1 AND s.name = 'x' RETURN p`,
		},
		{
			// Critical finding 1b: two id() conjuncts for the same endpoint
			// with DIFFERENT values. Cypher ANDs id(s)=1 with id(s)=2, which
			// can never match anything; the old behavior appended both ids
			// into Start.IDs, which resolveIDEndpoint then treats as
			// "matches id 1 OR id 2" -- backwards from AND semantics -- so
			// FromCypher must reject rather than serve that wrong answer.
			name: "duplicate id() conjuncts with different values",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE id(s) = 1 AND id(s) = 2 RETURN p`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("FromCypher() panicked: %v", r)
				}
			}()

			if got, ok := FromCypher(tc.text); ok {
				t.Fatalf("FromCypher() ok = true, want false (got %#v)", got)
			}
		})
	}
}

// TestFromCypher_RecoversFromWalkerPanic guards the recover() backstop
// itself: FromCypher must report ok=false, never propagate a panic, no
// matter how malformed the input is.
func TestFromCypher_RecoversFromWalkerPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("FromCypher() panicked: %v", r)
		}
	}()

	if _, ok := FromCypher(""); ok {
		t.Fatal("FromCypher(\"\") ok = true, want false")
	}
}
