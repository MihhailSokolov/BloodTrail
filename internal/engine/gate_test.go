// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/interpret"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// corpusAggregationQueryText mirrors interpret/plan_test.go's unexported
// corpusAggregationQuery constant byte for byte (that file's own doc names
// it as a query migrated from BloodHound's real analysis-query corpus): it
// cannot be imported directly (unexported, and defined in a _test.go file,
// so invisible outside package interpret even if exported), so the literal
// is duplicated here. This is the "corpus aggregation query"
// fixture -- WITH DISTINCT/COUNT/ORDER BY/LIMIT over a MemberOf|AdminTo
// traversal -- and is expected to pass the gate (dawgs' own pg translator
// accepts it, since it is real BloodHound query shape, not a shape
// BloodTrail's interpreter merely happens to accept).
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
			if got := translateGateOK(context.Background(), rq, snapshot.NewView(snap)); got != tc.want {
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

// --- gate mutation cannot leak into a served result ------------------------

// TestTranslateGateOKCopyLeavesOriginalASTUntouched is the regression test
// for a critical finding on TryCypher's pipeline
// (engine.go's TryCypher doc, step 7): dawgs' translate.Translate runs a
// real optimizer (cypher/models/pgsql/optimize) over whatever
// *cypher.RegularQuery it is given, and that optimizer is free to mutate
// its input in place -- so TryCypher must call translateGateOK with a fresh
// cypher.Copy of the parsed query, never the same object interpret.Plan
// already built its Query IR from, because that Query IR holds direct
// pointers into the original AST's own WHERE expression nodes for
// interpret.Execute to evaluate afterwards. Handing the gate the original
// object instead -- an easy-to-introduce regression a future refactor could
// make without any type system or compiler help to catch it -- would risk
// silently corrupting those nodes before Execute ever runs, potentially
// serving a subtly wrong result from a query the gate itself rewrote
// in-place.
//
// This test reproduces TryCypher's exact call sequence directly (Plan, then
// a copy to the gate, then Execute against the *original* query) against a
// hand-built snapshot -- no live database needed, mirroring engine_test.go's
// own white-box style -- over a multi-hop variable-length chain with a WHERE
// predicate: the same general shape (a leading unbounded variable-length
// step feeding a further hop, plus endpoint predicates) dawgs' own
// InboundTraversalReversalRule/PredicateAttachmentRule optimizer rules
// document themselves as rewriting (cypher/models/pgsql/optimize/
// direction.go's own doc comment names almost this exact pattern), so the
// gate call here is not a no-op translation for dawgs' optimizer to ignore.
// It asserts both halves of "gate mutation cannot leak": (1) the parsed
// query object itself is still byte-for-byte identical (reflect.DeepEqual
// against a cypher.Copy taken immediately before the gate call) after the
// gate has run, and (2) interpret.Execute against that same, still-original
// query still produces exactly the one correct row -- not an empty result,
// not a differently-shaped one -- proving the served answer reflects the
// query as written, never whatever the gate's own copy may have been
// rewritten into.
func TestTranslateGateOKCopyLeavesOriginalASTUntouched(t *testing.T) {
	// gateTestKinds (gate_test.go): User(1)/Group(2)/Computer(3)/
	// MemberOf(4)/AdminTo(5) -- reused here so the query below's
	// MemberOf|AdminTo variable-length chain has something concrete to
	// traverse alongside its WHERE predicate on the User endpoint.
	snap := buildCypherTestSnapshot(t, gateTestKinds,
		[]cypherTestNode{
			{id: 10, kinds: []snapshot.KindID{1}, props: map[string]any{"name": "alice"}},
			{id: 20, kinds: []snapshot.KindID{2}},
			{id: 30, kinds: []snapshot.KindID{3}},
		},
		[]cypherTestEdge{
			{id: 100, start: 10, end: 20, kind: 4}, // alice -MemberOf-> group
			{id: 101, start: 10, end: 30, kind: 5}, // alice -AdminTo-> computer
		},
	)

	const text = `MATCH (u:User)-[:MemberOf|AdminTo*1..]->(c:Computer) WHERE u.name = 'alice' RETURN u`

	view := snapshot.NewView(snap)

	rq, err := frontend.ParseCypher(frontend.NewContext(), text)
	if err != nil {
		t.Fatalf("ParseCypher(%q): %v", text, err)
	}

	q, ok := interpret.Plan(rq, view)
	if !ok {
		t.Fatalf("Plan(%q): not served", text)
	}

	pristine := cypher.Copy[*cypher.RegularQuery](rq)

	// Exactly TryCypher's own step 7: a copy goes to the gate, never rq
	// itself.
	if !translateGateOK(context.Background(), cypher.Copy[*cypher.RegularQuery](rq), view) {
		t.Fatalf("translateGateOK(%q) = false, want true", text)
	}

	if !reflect.DeepEqual(pristine, rq) {
		t.Fatalf("rq was mutated by translateGateOK despite being handed only a copy:\nbefore: %#v\nafter:  %#v", pristine, rq)
	}

	rs, err := interpret.Execute(&interpret.Env{Snap: view}, q, generousBudgetForGateTest)
	if err != nil {
		t.Fatalf("Execute(%q): %v", text, err)
	}

	alice, _ := snap.Dense(10)
	if len(rs.Rows) != 1 || len(rs.Rows[0]) != 1 || rs.Rows[0][0].Kind != interpret.OutNode || rs.Rows[0][0].Node != alice {
		t.Fatalf("rows = %+v, want exactly one row: [{OutNode %d}]", rs.Rows, alice)
	}
}

// generousBudgetForGateTest mirrors interpret's own generousBudget test
// helper (unexported, package interpret, so not directly reusable here): a
// budget large enough to never trip for this file's tiny fixture.
var generousBudgetForGateTest = interpret.Budgets{MaxRows: 10_000, MaxWork: 10_000_000}

// TestQueryIRIndependentOfASTCopyMutation is
// TestTranslateGateOKCopyLeavesOriginalASTUntouched's deterministic,
// dawgs-version-independent companion: that test proves the *real*
// translateGateOK call leaves rq alone, which is airtight evidence against
// today's dawgs@v0.8.0 (translate.Translate's non-borrowed
// optimize.Optimize path happens to copy its input before any rule ever
// runs -- see optimize/optimizer.go -- so it cannot currently demonstrate a
// rewrite actually landing on a *mutated copy* while rq itself survives).
// This test instead performs that mutation directly and deterministically,
// with no dependency on which optimizer rules dawgs happens to fire for any
// particular query text: it plans q from rq, takes a cypher.Copy of rq, and
// then aggressively mutates that copy in the most destructive way available
// short of a nil pointer -- discarding its entire ReadingClauses list,
// exactly mirroring the dawgs optimizer package's own
// analysisMutatingTestRule fixture (cypher/models/pgsql/optimize/
// optimizer_test.go) -- then asserts rq itself is completely unaffected and
// interpret.Execute(q) against rq still returns the correct row. This is
// the property TryCypher's step 7 actually depends on -- not "dawgs'
// optimizer happens not to mutate its input today" (which this package has
// no control over and cannot pin against a future dawgs upgrade), but "a
// mutation to a cypher.Copy of rq, however severe, can never reach rq
// itself, because interpret.Plan already extracted everything Execute
// needs from the original before any copy was ever made."
func TestQueryIRIndependentOfASTCopyMutation(t *testing.T) {
	snap := buildCypherTestSnapshot(t, gateTestKinds,
		[]cypherTestNode{
			{id: 10, kinds: []snapshot.KindID{1}, props: map[string]any{"name": "alice"}},
			{id: 20, kinds: []snapshot.KindID{2}},
			{id: 30, kinds: []snapshot.KindID{3}},
		},
		[]cypherTestEdge{
			{id: 100, start: 10, end: 20, kind: 4}, // alice -MemberOf-> group
			{id: 101, start: 10, end: 30, kind: 5}, // alice -AdminTo-> computer
		},
	)

	const text = `MATCH (u:User)-[:MemberOf|AdminTo*1..]->(c:Computer) WHERE u.name = 'alice' RETURN u`

	view := snapshot.NewView(snap)

	rq, err := frontend.ParseCypher(frontend.NewContext(), text)
	if err != nil {
		t.Fatalf("ParseCypher(%q): %v", text, err)
	}

	q, ok := interpret.Plan(rq, view)
	if !ok {
		t.Fatalf("Plan(%q): not served", text)
	}

	mutatedCopy := cypher.Copy[*cypher.RegularQuery](rq)
	mutatedCopy.SingleQuery.SinglePartQuery.ReadingClauses = nil // as destructive as dawgs' own analysisMutatingTestRule fixture

	if rq.SingleQuery.SinglePartQuery.ReadingClauses == nil {
		t.Fatalf("rq.ReadingClauses is nil after mutating an unrelated copy, want the original list untouched")
	}

	rs, err := interpret.Execute(&interpret.Env{Snap: view}, q, generousBudgetForGateTest)
	if err != nil {
		t.Fatalf("Execute(%q): %v", text, err)
	}

	alice, _ := snap.Dense(10)
	if len(rs.Rows) != 1 || len(rs.Rows[0]) != 1 || rs.Rows[0][0].Kind != interpret.OutNode || rs.Rows[0][0].Node != alice {
		t.Fatalf("rows = %+v, want exactly one row: [{OutNode %d}]", rs.Rows, alice)
	}
}
