// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// corpusAggregationQueryText mirrors interpret/plan_test.go's unexported
// corpusAggregationQuery constant byte for byte (that file's own doc names
// it as a query migrated from BloodHound's real analysis-query corpus): it
// cannot be imported directly (unexported, and defined in a _test.go file,
// so invisible outside package interpret even if exported), so the literal
// is duplicated here. This is the brief's own "corpus aggregation query"
// fixture -- WITH DISTINCT/COUNT/ORDER BY/LIMIT over a MemberOf|AdminTo
// traversal -- and is expected to pass the gate (dawgs' own pg translator
// accepts it, since it is real BloodHound query shape, not a shape this
// milestone's interpreter merely happens to accept).
const corpusAggregationQueryText = `MATCH (u:User)
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

// gateTestKinds registers every kind name this file's test corpus
// references: User/Group/Computer as node labels, MemberOf/AdminTo as
// relationship types. "Missing" is deliberately never registered here, so
// the unknown-kind test case below has something real to fail on.
var gateTestKinds = map[snapshot.KindID]string{
	1: "User",
	2: "Group",
	3: "Computer",
	4: "MemberOf",
	5: "AdminTo",
}

// gateTestSnapshot builds the shared snapshot translateGateOK's tests run
// against. translate.Translate itself never reads node/edge data -- only
// kind *names*, via the KindMapper -- so a couple of nodes and one edge are
// enough to exercise the mapper realistically without needing a graph shape
// tailored to each individual query.
func gateTestSnapshot(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	return buildCypherTestSnapshot(t, gateTestKinds,
		[]cypherTestNode{
			{id: 10, kinds: []snapshot.KindID{1}, props: map[string]any{"name": "alice"}},
			{id: 20, kinds: []snapshot.KindID{2}},
		},
		[]cypherTestEdge{
			{id: 100, start: 10, end: 20, kind: 4},
		},
	)
}

// --- translateGateOK ---------------------------------------------------

func TestTranslateGateOK(t *testing.T) {
	snap := gateTestSnapshot(t)

	tests := []struct {
		name string
		text string
		want bool
	}{
		// MERGE is a write clause dawgs' own translator has no read-serving
		// case for (its cypher.Merge branch is create-only plumbing) --
		// unable to translate cypher type *cypher.Merge.
		{"merge unsupported", `MERGE (n) RETURN n`, false},
		// keys() has no pgsql translation -- unknown function.
		{"unknown function", `MATCH (n) RETURN keys(n)`, false},
		// "Missing" is not in gateTestKinds -- our own snapshotKindMapper
		// fails MapKinds before the query ever reaches pg's own type
		// checker.
		{"unknown kind", `MATCH (n:Missing) RETURN n`, false},
		// COALESCE(n.a, false) types as boolean; comparing it to the string
		// literal 'x' is a type mismatch pgsql's own type-checker rejects.
		{"coalesce type mismatch", `MATCH (n) WHERE COALESCE(n.a, false) = 'x' RETURN n`, false},
		// RETURN * has no pgsql translation -- unsupported projection shape.
		{"return star", `MATCH (n) RETURN *`, false},

		{"simple predicate", `MATCH (n:User) WHERE n.name = 'x' RETURN n`, true},
		{"shortest path", `MATCH p=shortestPath((s:User)-[:MemberOf*1..]->(t:Group)) WHERE s<>t RETURN p`, true},
		{"corpus aggregation", corpusAggregationQueryText, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rq, err := frontend.ParseCypher(frontend.NewContext(), tc.text)
			if err != nil {
				t.Fatalf("ParseCypher(%q): %v", tc.text, err)
			}
			if got := translateGateOK(context.Background(), rq, snap); got != tc.want {
				t.Fatalf("translateGateOK(%q) = %v, want %v", tc.text, got, tc.want)
			}
		})
	}
}

// --- snapshotKindMapper --------------------------------------------------

func TestSnapshotKindMapperMapKindsHit(t *testing.T) {
	snap := gateTestSnapshot(t)
	mapper := snapshotKindMapper{kinds: snap.Kinds}

	got, err := mapper.MapKinds(context.Background(), graph.Kinds{graph.StringKind("User"), graph.StringKind("Group")})
	if err != nil {
		t.Fatalf("MapKinds(User, Group): unexpected error: %v", err)
	}

	want := []int16{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MapKinds(User, Group) = %v, want %v", got, want)
	}
}

func TestSnapshotKindMapperMapKindsMiss(t *testing.T) {
	snap := gateTestSnapshot(t)
	mapper := snapshotKindMapper{kinds: snap.Kinds}

	got, err := mapper.MapKinds(context.Background(), graph.Kinds{graph.StringKind("User"), graph.StringKind("Nonexistent")})
	if err == nil {
		t.Fatalf("MapKinds(User, Nonexistent) = (%v, nil), want a non-nil error", got)
	}
	if got != nil {
		t.Fatalf("MapKinds(User, Nonexistent) ids = %v, want nil on error (all-or-nothing)", got)
	}
}

func TestSnapshotKindMapperAssertKindsAlwaysErrors(t *testing.T) {
	snap := gateTestSnapshot(t)
	mapper := snapshotKindMapper{kinds: snap.Kinds}

	got, err := mapper.AssertKinds(context.Background(), graph.Kinds{graph.StringKind("User")})
	if err == nil {
		t.Fatalf("AssertKinds(User) = (%v, nil), want a non-nil error", got)
	}
	if got != nil {
		t.Fatalf("AssertKinds(User) ids = %v, want nil", got)
	}
	const want = "bloodtrail: kind assertion during read serving"
	if err.Error() != want {
		t.Fatalf("AssertKinds(User) error = %q, want %q", err.Error(), want)
	}
}
