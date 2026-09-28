// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"sort"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildMixedTypeFixture is a graph large enough that every property index
// anchor is worth taking, in which each tested property carries values of
// MORE THAN ONE JSON type. BloodHound's schema puts no type constraint on
// `properties`, and dawgs translates the string predicates through `->>`,
// which renders a number, boolean or list as its JSON text -- so pg matches
// those nodes, and an index that only records string values does not.
//
// Node ids of interest (database ids): 5 tag=12345, 6 tag="12999",
// 7 os=["Windows 2000"], 8 os="Windows 2003", 9 arr=[1,2], 10 arr=["1","x"],
// 11 flag=true, 12 flag="false", 13 arr2=[true], 14 arr3=["a","b"],
// 15 num=7, 16 num="7", 17 numbad="abc", 18 frac=7.5.
func buildMixedTypeFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const kiUser snapshot.KindID = 1
	var nodes []execNodeSpec
	for i := 0; i < 2000; i++ {
		props := map[string]any{"name": fmt.Sprintf("U%d", i)}
		switch i {
		case 5:
			props["tag"] = float64(12345)
		case 6:
			props["tag"] = "12999"
		case 7:
			props["os"] = []any{"Windows 2000"}
		case 8:
			props["os"] = "Windows 2003"
		case 9:
			props["arr"] = []any{float64(1), float64(2)}
		case 10:
			props["arr"] = []any{"1", "x"}
		case 11:
			props["flag"] = true
		case 12:
			props["flag"] = "false"
		case 13:
			props["arr2"] = []any{true}
		case 14:
			props["arr3"] = []any{"a", "b"}
		case 15:
			props["num"] = float64(7)
		case 16:
			props["num"] = "7"
		case 17:
			props["numbad"] = "abc"
		case 18:
			props["frac"] = 7.5
		}
		nodes = append(nodes, execNodeSpec{uint64(i), []snapshot.KindID{kiUser}, props})
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{kiUser: "User"}, nodes, nil)
}

// servedIDs plans and executes q, returning the database ids of the single
// node column and whether the engine served it at all (a plan-time decline
// or an Execute error both delegate to PostgreSQL).
func servedIDs(t *testing.T, snap *snapshot.View, q string) ([]uint64, bool) {
	t.Helper()
	pq, ok := planNoFail(t, snap, q)
	if !ok {
		return nil, false
	}
	rs, err := Execute(&Env{Snap: snap}, pq, generousBudget)
	if err != nil {
		return nil, false
	}
	ids := make([]uint64, 0, len(rs.Rows))
	for _, row := range rs.Rows {
		if row[0].Kind != OutNode {
			t.Fatalf("%s: column projected as %v", q, row[0].Kind)
		}
		ids = append(ids, snap.GraphID(row[0].Node))
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	return ids, true
}

// TestMixedTypeTextPredicatesNeverServeShort: each query's pg answer is
// derived from dawgs@v0.8.0's own translation (quoted per case). The engine
// may decline any of them, but whatever it serves must be exactly pg's rows.
func TestMixedTypeTextPredicatesNeverServeShort(t *testing.T) {
	snap := buildMixedTypeFixture(t)
	for _, tc := range []struct {
		query string
		want  []uint64
	}{
		// (properties ->> 'tag') like '12%'
		{`MATCH (u:User) WHERE u.tag STARTS WITH '12' RETURN u`, []uint64{5, 6}},
		{`MATCH (u:User) WHERE u.tag ENDS WITH '45' RETURN u`, []uint64{5}},
		{`MATCH (u:User) WHERE u.tag CONTAINS '234' RETURN u`, []uint64{5}},
		// (properties ->> 'os') ~ '...': a list renders as its JSON text.
		{`MATCH (u:User) WHERE u.os =~ '.*2000.*' RETURN u`, []uint64{7}},
		// (properties ->> 'tag') = any(array['12345']::text[])
		{`MATCH (u:User) WHERE u.tag IN ['12345'] RETURN u`, []uint64{5}},
		{`MATCH (u:User) WHERE u.flag IN ['true'] RETURN u`, []uint64{11}},
		// coalesce(properties ->> 'tag', '')::text = '12345'
		{`MATCH (u:User) WHERE coalesce(u.tag, '') = '12345' RETURN u`, []uint64{5}},
		{`MATCH (u:User) WHERE coalesce(u.tag, '') STARTS WITH '1234' RETURN u`, []uint64{5}},
		// '1' = any(jsonb_to_text_array(properties -> 'arr')::text[])
		{`MATCH (u:User) WHERE '1' IN u.arr RETURN u`, []uint64{9, 10}},
		{`MATCH (u:User) WHERE 'true' IN u.arr2 RETURN u`, []uint64{13}},
		{`MATCH (u:User) WHERE '1.0' IN u.arr RETURN u`, []uint64{}},
		{`MATCH (u:User) WHERE 'a' IN u.arr3 RETURN u`, []uint64{14}},
		// coalesce() casts the `->>` text to its literal's type:
		// coalesce((properties ->> 'num')::int8, 0)::int8 = 7
		{`MATCH (u:User) WHERE coalesce(u.num, 0) = 7 RETURN u`, []uint64{15, 16}},
		// coalesce(properties ->> 'num', '')::text = '7'
		{`MATCH (u:User) WHERE coalesce(u.num, '') = '7' RETURN u`, []uint64{15, 16}},
		{`MATCH (u:User) WHERE coalesce(u.num, 0) <> 7 AND u.name = 'U15' RETURN u`, []uint64{}},
		// A relational comparison CASTS the text: (properties ->> 'num')::int8 > 5
		{`MATCH (u:User) WHERE u.num > 5 RETURN u`, []uint64{15, 16}},
		{`MATCH (u:User) WHERE u.tag >= 12999 RETURN u`, []uint64{6}},
		// ...::float8 > 5.0
		{`MATCH (u:User) WHERE u.frac > 5.0 RETURN u`, []uint64{18}},
		// Type-strict in pg (jsonb_typeof / jsonb equality), and so here.
		{`MATCH (u:User) WHERE u.tag = '12345' RETURN u`, []uint64{}},
		{`MATCH (u:User) WHERE u.tag = 12345 RETURN u`, []uint64{5}},
	} {
		got, served := servedIDs(t, snap, tc.query)
		if !served {
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s: served %v, pg returns %v", tc.query, got, tc.want)
		}
	}
}

// TestMixedTypeInShapesPgRejects: shapes PostgreSQL answers with an ERROR --
// either dawgs cannot translate them at all, or the int8 cast of a
// non-numeric list element fails at runtime -- must never be served.
func TestMixedTypeInShapesPgRejects(t *testing.T) {
	snap := buildMixedTypeFixture(t)
	for _, q := range []string{
		// 1 = any(jsonb_to_text_array(properties -> 'arr')::int8[]):
		// node 10's 'x' fails the cast.
		`MATCH (u:User) WHERE 1 IN u.arr RETURN u`,
		// "data type has no direct array representation"
		`MATCH (u:User) WHERE true IN u.arr2 RETURN u`,
		`MATCH (u:User) WHERE u.name IN u.arr3 RETURN u`,
		// (properties ->> 'numbad')::int8 fails on 'abc'.
		`MATCH (u:User) WHERE coalesce(u.numbad, 0) = 7 RETURN u`,
		// (properties ->> 'numbad')::int8 and '7.5'::int8 both fail.
		`MATCH (u:User) WHERE u.numbad > 5 RETURN u`,
		`MATCH (u:User) WHERE u.frac > 5 RETURN u`,
		// "coalesce has type bool but is being compared against type text"
		`MATCH (u:User) WHERE coalesce(u.flag, false) CONTAINS 'x' RETURN u`,
	} {
		if got, served := servedIDs(t, snap, q); served {
			t.Errorf("%s: served %v, pg raises an error", q, got)
		}
	}
}

// TestRegexMatchesWhatPostgreSQLMatches pins the two ways Go's regexp and
// PostgreSQL's `~` disagree on the same pattern text. dawgs doubles every
// backslash in a property-side regex literal (rewriteStringWildCardLiteral,
// meant for LIKE), so `\.` reaches pg as "literal backslash, then any
// character" -- served here as Go's "literal dot", the answer was wrong for
// any pattern with an escape. And pg's `.` matches a newline, Go's does not.
func TestRegexMatchesWhatPostgreSQLMatches(t *testing.T) {
	const kiUser snapshot.KindID = 1
	snap := buildExecSnapshot(t, map[snapshot.KindID]string{kiUser: "User"}, []execNodeSpec{
		{1, []snapshot.KindID{kiUser}, map[string]any{"name": "host.corp"}},
		{2, []snapshot.KindID{kiUser}, map[string]any{"name": "a\nb"}},
		{3, []snapshot.KindID{kiUser}, map[string]any{"name": "ADMIN x"}},
	}, nil)

	for _, tc := range []struct {
		query string
		want  []uint64
	}{
		{`MATCH (u:User) WHERE u.name =~ 'a.b' RETURN u`, []uint64{2}},
		{`MATCH (u:User) WHERE u.name =~ '(?i)admin.*' RETURN u`, []uint64{3}},
		{`MATCH (u:User) WHERE u.name =~ '(?:host|x)[.]corp' RETURN u`, []uint64{1}},
	} {
		got, served := servedIDs(t, snap, tc.query)
		if !served {
			t.Errorf("%s: declined, want served", tc.query)
			continue
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("%s: served %v, pg returns %v", tc.query, got, tc.want)
		}
	}

	for _, q := range []string{
		// pg sees '.*\\.corp': a literal backslash, which no name has.
		`MATCH (u:User) WHERE u.name =~ '.*\\.corp' RETURN u`,
		// pg accepts embedded options only at the start of a pattern.
		`MATCH (u:User) WHERE u.name =~ 'x(?i)admin' RETURN u`,
		`MATCH (u:User) WHERE u.name =~ '(?i:admin).*' RETURN u`,
		// `m` is newline-sensitivity in pg, multi-line in Go.
		`MATCH (u:User) WHERE u.name =~ '(?m)^b' RETURN u`,
	} {
		if got, served := servedIDs(t, snap, q); served {
			t.Errorf("%s: served %v; its meaning differs in pg", q, got)
		}
	}
}
