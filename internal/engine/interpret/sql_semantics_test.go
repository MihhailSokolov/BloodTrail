// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"reflect"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// assertServedIDs plans and executes q and fails unless it is served with
// exactly the nodes want (graph ids, the first column).
func assertServedIDs(t *testing.T, snap *snapshot.View, q string, want ...uint64) {
	t.Helper()
	got, ok := servedIDs(t, snap, q)
	if !ok {
		t.Fatalf("%s: declined, want served", q)
	}
	if want == nil {
		want = []uint64{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: served %v, want %v", q, got, want)
	}
}

// assertDeclined fails unless q is declined, by Plan or at execution.
func assertDeclined(t *testing.T, snap *snapshot.View, q string) {
	t.Helper()
	if got, ok := servedIDs(t, snap, q); ok {
		t.Fatalf("%s: served %v, want declined", q, got)
	}
}

const (
	semUser  snapshot.KindID = 1
	semGroup snapshot.KindID = 2
	semEdge  snapshot.KindID = 10
)

var semKinds = map[snapshot.KindID]string{semUser: "User", semGroup: "Group", semEdge: "MemberOf"}

// A null literal is SQL NULL on either side of a comparison. `null <> 0`
// reached the literal comparison with the null as the non-literal side,
// where it evaluated as a stored JSON null -- unequal to 0, so TRUE.
func TestNullLiteralComparisonIsNull(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, map[string]any{"name": "a"}},
	}, nil)

	assertServedIDs(t, snap, `MATCH (n:User) WHERE null <> 0 RETURN n`)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT null = 0 RETURN n`)
}

// `n.v = []` is `(p -> 'v') = '[]' or (p -> 'v') = 'null' and null` in
// dawgs, so a stored JSON null is NULL under both `=` and `<>`, not the
// definite FALSE/TRUE jsonb equality gives it.
func TestEmptyListComparisonOverStoredNull(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, map[string]any{"v": nil}},
		{2, []snapshot.KindID{semUser}, map[string]any{"v": []any{}}},
		{3, []snapshot.KindID{semUser}, map[string]any{"v": []any{1}}},
		{4, []snapshot.KindID{semUser}, map[string]any{"name": "absent"}},
	}, nil)

	assertServedIDs(t, snap, `MATCH (n:User) WHERE n.v <> [] RETURN n`, 3)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT n.v = [] RETURN n`, 3)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE [] = n.v RETURN n`, 2)
}

// A parenthesised property in a string IN list is `(p ->> 'v') = any(...)`,
// the same text comparison a plain one gets: an object's text equals none of
// the strings, so it is FALSE -- kept under NOT -- where In() said NULL.
func TestParenthesisedPropertyInStringList(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, map[string]any{"v": map[string]any{"k": "a"}}},
		{2, []snapshot.KindID{semUser}, map[string]any{"v": "a"}},
		{3, []snapshot.KindID{semUser}, map[string]any{"v": 5}},
	}, nil)

	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT (n.v) IN ['a'] RETURN n`, 1, 3)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE (n.v) IN ['5'] RETURN n`, 3)
}

// dawgs escapes a literal needle's LIKE metacharacters only for a plain
// property; against anything else the needle goes into LIKE as written, so
// _ and % are wildcards. A parenthesised needle goes through
// cypher_starts_with() and is literal again.
func TestLikeWildcardsInUnescapedNeedles(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, map[string]any{"name": "abc"}},
		{2, []snapshot.KindID{semUser}, map[string]any{"name": "a_c"}},
		{3, []snapshot.KindID{semUser}, map[string]any{"name": "axc"}},
		{4, []snapshot.KindID{semUser}, map[string]any{"name": "ac"}},
	}, nil)

	assertServedIDs(t, snap, `MATCH (n:User) WHERE toLower(n.name) CONTAINS 'a_c' RETURN n`, 1, 2, 3)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE n.name CONTAINS 'a_c' RETURN n`, 2)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE toLower(n.name) STARTS WITH 'a%c' RETURN n`, 1, 2, 3, 4)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE toLower(n.name) STARTS WITH ('a_c') RETURN n`, 2)
	// '%_c' matches all four -- ac included, _ taking the a -- where a
	// literal suffix test would keep abc, axc and ac.
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT toLower(n.name) ENDS WITH '_c' RETURN n`)
}

// dawgs rewrites a negated STARTS WITH/ENDS WITH/CONTAINS through
// coalesce(..., "") for a plain property only, and never a regex, so a
// missing value satisfies `NOT n.name STARTS WITH 'a'` and nothing else here.
func TestNegatedStringPredicateCoalescesOnlyPlainProperties(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, map[string]any{"other": 1}},
		{2, []snapshot.KindID{semUser}, map[string]any{"name": "b"}},
	}, nil)

	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT n.name STARTS WITH 'a' RETURN n`, 1, 2)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT toLower(n.name) STARTS WITH 'a' RETURN n`, 2)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT (n.name) CONTAINS 'a' RETURN n`, 2)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT n.name =~ 'a.*' RETURN n`, 2)
}

// The coalesce anchor answered `COALESCE(t.system_tags, "") CONTAINS
// 'admin_tier_0'` from a substring search for the needle itself, but pg's
// LIKE reads each _ as any one character, so 'admin-tier-0' matches too.
// The anchor now narrows on the pattern's longest literal run -- still an
// index lookup, now a superset -- and the per-row LIKE decides.
func TestCoalesceAnchorKeepsLikeWildcardMatches(t *testing.T) {
	var nodes []execNodeSpec
	for i := uint64(1); i <= 30; i++ {
		nodes = append(nodes, execNodeSpec{i, []snapshot.KindID{semGroup}, map[string]any{"name": "plain"}})
	}
	nodes = append(nodes,
		execNodeSpec{31, []snapshot.KindID{semGroup}, map[string]any{"system_tags": "admin_tier_0"}},
		execNodeSpec{32, []snapshot.KindID{semGroup}, map[string]any{"system_tags": "admin-tier-0 owned"}},
		execNodeSpec{33, []snapshot.KindID{semGroup}, map[string]any{"system_tags": "owned"}},
	)
	snap := buildExecSnapshot(t, semKinds, nodes, nil)

	q := `MATCH (t:Group) WHERE COALESCE(t.system_tags, '') CONTAINS 'admin_tier_0' RETURN t`
	pq, ok := planNoFail(t, snap, q)
	if !ok {
		t.Fatalf("%s: declined", q)
	}
	if nc := pq.Parts[0].Nodes["t"]; nc == nil || !nc.PropIndexed {
		t.Fatalf("%s: the coalesce anchor was not adopted", q)
	}
	assertServedIDs(t, snap, q, 31, 32)
}

// Relationship uniqueness holds among one pattern's steps only: dawgs emits
// `e1.id != e0.id` inside a pattern and nothing across comma-separated
// patterns or MATCH clauses, so those pair an edge with itself.
func TestRelationshipUniquenessIsPerPattern(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, nil},
		{2, []snapshot.KindID{semGroup}, nil},
	}, []execEdgeSpec{
		{10, 1, 2, semEdge},
	})

	assertServedIDs(t, snap, `MATCH (x:User)-[:MemberOf]->(g), (y:User)-[:MemberOf]->(g) RETURN x`, 1)
	assertServedIDs(t, snap, `MATCH (x:User)-[:MemberOf]->(g) MATCH (y:User)-[:MemberOf]->(g) RETURN x`, 1)
	assertServedIDs(t, snap, `MATCH (x:User)-[:MemberOf]->(g)<-[:MemberOf]-(y:User) RETURN x`)
}

// `n.v = ['1']` is `jsonb_to_text_array(p -> 'v')::text[] = array ['1']`: a
// list compares its elements' text, so [1] matches too; a JSON null or a
// missing value is NULL. Against a numeric list every element is cast
// first, so one that cannot be is an error for the whole query.
func TestListLiteralEqualityComparesText(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, map[string]any{"v": []any{"1"}}},
		{2, []snapshot.KindID{semUser}, map[string]any{"v": []any{1}}},
		{3, []snapshot.KindID{semUser}, map[string]any{"v": []any{1.5}}},
		{4, []snapshot.KindID{semUser}, map[string]any{"v": []any{}}},
		{5, []snapshot.KindID{semUser}, map[string]any{"v": nil}},
		{6, []snapshot.KindID{semUser}, map[string]any{"name": "absent"}},
		{7, []snapshot.KindID{semUser}, map[string]any{"v": []any{"1", "2"}}},
		{8, []snapshot.KindID{semGroup}, map[string]any{"v": "1"}},
	}, nil)

	assertServedIDs(t, snap, `MATCH (n:User) WHERE n.v = ['1'] RETURN n`, 1, 2)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT n.v = ['1'] RETURN n`, 3, 4, 7)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE n.v <> ['1', '2'] RETURN n`, 1, 2, 3, 4)
	assertDeclined(t, snap, `MATCH (n:User) WHERE n.v = [1] RETURN n`)
	assertDeclined(t, snap, `MATCH (n:Group) WHERE n.v = ['1'] RETURN n`)
}

// `n.v = 'x' + n.w` is `(p ->> 'v') = 'x' || (p ->> 'w')`: plain text, so
// the number 5 equals the concatenation '5', and a JSON null on either side
// is NULL.
func TestPropertyEqualsConcatenationComparesText(t *testing.T) {
	snap := buildExecSnapshot(t, semKinds, []execNodeSpec{
		{1, []snapshot.KindID{semUser}, map[string]any{"v": 5, "w": "5"}},
		{2, []snapshot.KindID{semUser}, map[string]any{"v": "x5", "w": "5"}},
		{3, []snapshot.KindID{semUser}, map[string]any{"v": "5", "w": nil}},
		{4, []snapshot.KindID{semUser}, map[string]any{"w": "5"}},
	}, nil)

	assertServedIDs(t, snap, `MATCH (n:User) WHERE n.v = '' + n.w RETURN n`, 1)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE n.v = 'x' + n.w RETURN n`, 2)
	assertServedIDs(t, snap, `MATCH (n:User) WHERE NOT n.v = 'x' + n.w RETURN n`, 1)
}
