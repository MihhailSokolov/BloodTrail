// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildCoalesceFixture: many Groups, a handful carrying system_tags -- the
// shape of the shipped "Nested groups within Tier Zero / High Value"
// prebuilt, which asks for `COALESCE(t.system_tags, ”) CONTAINS
// 'admin_tier_0'`.
func buildCoalesceFixture(t *testing.T, groups, tagged int) *snapshot.View {
	t.Helper()
	const kiGroup snapshot.KindID = 1
	var nodes []execNodeSpec
	for i := 0; i < groups; i++ {
		props := map[string]any{"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 1000+i)}
		if i < tagged {
			props["system_tags"] = "admin_tier_0"
		}
		nodes = append(nodes, execNodeSpec{uint64(i + 1), []snapshot.KindID{kiGroup}, props})
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{kiGroup: "Group"}, nodes, nil)
}

// TestCoalesceAnchorsThroughTheWrapper pins the idiom BloodHound actually
// writes. Twenty-one of the 185 corpus queries wrap a property in COALESCE,
// seventeen of them as `COALESCE(x, ”) CONTAINS ...`, and the wrapper hid
// the property from the index completely -- the predicate is a function call,
// not a property lookup, so the symbol was priced and enumerated as its bare
// kind bitmap.
//
// The budget is far too small to have walked the Group bitmap, so a passing
// run is proof the anchor came from the index.
func TestCoalesceAnchorsThroughTheWrapper(t *testing.T) {
	const groups, tagged = 4000, 3
	snap := buildCoalesceFixture(t, groups, tagged)
	tight := Budgets{MaxRows: 100, MaxWork: 200, MaxLiveRows: 1000}

	rs := mustExec(t, snap,
		`MATCH (t:Group) WHERE COALESCE(t.system_tags, '') CONTAINS 'admin_tier_0' RETURN t`, tight)
	if len(rs.Rows) != tagged {
		t.Fatalf("got %d rows, want %d", len(rs.Rows), tagged)
	}
}

// TestCoalesceAnchorOnlyWhenTheDefaultCannotMatch is the correctness half.
// COALESCE yields the default exactly when the property is ABSENT, so the
// index population (nodes carrying the property) is a superset of the matches
// only while the default fails the predicate. If it passes, absent nodes
// match too and anchoring would return a SHORT answer -- rows missing, not
// merely found slowly.
func TestCoalesceAnchorOnlyWhenTheDefaultCannotMatch(t *testing.T) {
	const groups, tagged = 40, 3
	snap := buildCoalesceFixture(t, groups, tagged)
	loose := Budgets{MaxRows: 100000, MaxWork: 10000000, MaxLiveRows: 100000}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			// '' does not contain the operand: absent nodes cannot match.
			name:  "default that cannot match anchors",
			query: `MATCH (t:Group) WHERE COALESCE(t.system_tags, '') CONTAINS 'admin_tier_0' RETURN t`,
			want:  tagged,
		},
		{
			// The default CONTAINS the operand, so every node with the
			// property absent matches too -- all of them.
			name:  "default that matches must not anchor",
			query: `MATCH (t:Group) WHERE COALESCE(t.system_tags, 'admin_tier_0') CONTAINS 'admin_tier_0' RETURN t`,
			want:  groups,
		},
		{
			// Equality against the default's exact value: same hazard.
			name:  "equality default that matches must not anchor",
			query: `MATCH (t:Group) WHERE COALESCE(t.system_tags, 'admin_tier_0') = 'admin_tier_0' RETURN t`,
			want:  groups,
		},
		{
			name:  "equality default that cannot match anchors",
			query: `MATCH (t:Group) WHERE COALESCE(t.system_tags, '') = 'admin_tier_0' RETURN t`,
			want:  tagged,
		},
		{
			// A non-string default satisfies no string predicate under pg's
			// jsonb comparison, so it is safe to anchor.
			name:  "non-string default anchors",
			query: `MATCH (t:Group) WHERE COALESCE(t.system_tags, false) CONTAINS 'admin_tier_0' RETURN t`,
			want:  tagged,
		},
		{
			name:  "prefix default that matches must not anchor",
			query: `MATCH (t:Group) WHERE COALESCE(t.system_tags, 'admin_tier_0_x') STARTS WITH 'admin' RETURN t`,
			want:  groups,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, loose)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}
