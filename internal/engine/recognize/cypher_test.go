// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
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
// Of the 12, 9 match FromCypher's accepted shape and 3 do not:
//   - "Shortest paths to Domain Admins from Kerberoastable users" uses
//     "AND NOT ... ENDS WITH" and "NOT COALESCE(...) = true" in its WHERE,
//     both outside the accepted grammar (NOT is explicitly a reject case).
//   - "Shortest paths from Owned objects to Tier Zero" is the commented-out
//     placeholder above; its "cypher" text is Cypher line-comments only, so
//     it fails to parse into a queryable statement at all.
//   - "Shortest paths to privileged roles" matches its target's name with
//     "=~" (regex match), an operator outside the accepted WHERE grammar
//     (only "=" and "ENDS WITH" are accepted).
//
// This is a deliberate, principled split rather than a blanket "every entry
// recognizes": the accepted-shape paragraph is normative, and each of these
// three prebuilt queries genuinely falls outside it. Extraction fidelity
// (all 12 present, verbatim apart from interpolation expansion) is what's
// exhaustive here, not recognizability.
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
		ok: false,
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
		ok: false,
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

// TestFromCypher_Rejects covers the ten hand-written shapes FromCypher must
// refuse without panicking, each falling outside the accepted shape for a
// distinct reason.
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
			name: "WHERE with OR",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s<>t OR s.name = 'x' RETURN p`,
		},
		{
			name: "WHERE with NOT",
			text: `MATCH p=shortestPath((s)-[:A*1..]->(t)) WHERE s<>t AND NOT s.name = 'x' RETURN p`,
		},
		{
			name: "unparseable text",
			text: `this is not cypher at all {{{`,
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
