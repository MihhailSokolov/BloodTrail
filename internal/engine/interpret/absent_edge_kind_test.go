// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildAbsentKindFixture builds a graph whose kind TABLE registers a
// relationship kind that no edge actually uses -- which is what a real
// deployment looks like, since BloodHound registers every kind it knows about
// whether or not the collected data exercises it. On AD-only data that covers
// every ADCS, Entra and NTLM-relay kind its shipped prebuilts query.
//
// The graph is deliberately wide (nodes nodes, each with a MemberOf edge) so
// that scanning it costs real work: the tests below assert the absent-kind
// answer comes back under a work budget far too small to have walked it.
func buildAbsentKindFixture(t *testing.T, nodes int) *snapshot.View {
	t.Helper()
	const (
		akUser     snapshot.KindID = 1
		akGroup    snapshot.KindID = 2
		akMemberOf snapshot.KindID = 10
		akProtects snapshot.KindID = 11 // registered, never written
	)
	kinds := map[snapshot.KindID]string{
		akUser: "User", akGroup: "Group", akMemberOf: "MemberOf", akProtects: "ProtectAdminGroups",
	}

	specs := []execNodeSpec{
		{1, []snapshot.KindID{akGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-512"}},
	}
	var edges []execEdgeSpec
	edgeID := uint64(1000)
	for i := 0; i < nodes; i++ {
		id := uint64(1000 + i)
		specs = append(specs, execNodeSpec{id, []snapshot.KindID{akUser}, map[string]any{
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 10000+i),
		}})
		edges = append(edges, execEdgeSpec{edgeID, id, 1, akMemberOf})
		edgeID++
	}
	return buildExecSnapshot(t, kinds, specs, edges)
}

// TestAbsentEdgeKindAnsweredWithoutWalking pins the fix for the ~224ms floor
// the 500k adversarial sweep found under every shipped prebuilt for a feature
// the deployment does not use. Such a query names a relationship kind with no
// edges; the engine used to scan the whole graph to discover that, where
// PostgreSQL answers from an index in ~18ms. An absent NODE kind was already
// fast (a kind bitmap answers it directly) -- only relationships lacked the
// equivalent.
//
// The assertion is deliberately a WORK BUDGET rather than a timing: the query
// must succeed with a budget of one work unit, which is proof it walked
// nothing at all. Scanning the fixture would blow that budget immediately.
func TestAbsentEdgeKindAnsweredWithoutWalking(t *testing.T) {
	snap := buildAbsentKindFixture(t, 2000)
	tight := Budgets{MaxRows: 1000, MaxWork: 1}

	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			name:  "fixed-length hop on an absent kind",
			query: `MATCH p = (n)-[:ProtectAdminGroups]->(m) RETURN p LIMIT 1000`,
		},
		{
			// The LIMIT path matters on its own: the chunked early-termination
			// driver is the one pattern-match route that does not run through
			// matchPart, and every BloodHound prebuilt carries a LIMIT.
			name:  "kind-labelled endpoints and a LIMIT",
			query: `MATCH p = (n:User)-[:ProtectAdminGroups]->(m:Group) RETURN p LIMIT 1000`,
		},
		{
			name:  "variable-length on an absent kind",
			query: `MATCH p = (n:User)-[:ProtectAdminGroups*1..]->(m:Group) RETURN p LIMIT 1000`,
		},
		{
			name:  "absent kind anywhere in a chain condemns the whole pattern",
			query: `MATCH p = (u:User)-[:MemberOf]->(g:Group)-[:ProtectAdminGroups]->(x) RETURN p LIMIT 1000`,
		},
		{
			name:  "a kind union is absent only when every kind in it is",
			query: `MATCH p = (n)-[:ProtectAdminGroups|ProtectAdminGroups]->(m) RETURN p LIMIT 1000`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, tight)
			if len(rs.Rows) != 0 {
				t.Fatalf("got %d rows, want 0 (no edge of the named kind exists)", len(rs.Rows))
			}
		})
	}
}

// TestAbsentEdgeKindKeepsPresentKindsWorking is the other half: the
// short-circuit must fire only for kinds that really have no edges. A kind
// union that includes a PRESENT kind, and a pattern on a present kind, both
// have to keep returning their rows.
func TestAbsentEdgeKindKeepsPresentKindsWorking(t *testing.T) {
	snap := buildAbsentKindFixture(t, 50)

	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			name:  "present kind still matches",
			query: `MATCH p = (u:User)-[:MemberOf]->(g:Group) RETURN p LIMIT 1000`,
		},
		{
			name:  "a union mixing an absent kind with a present one still matches",
			query: `MATCH p = (u:User)-[:ProtectAdminGroups|MemberOf]->(g:Group) RETURN p LIMIT 1000`,
		},
		{
			name:  "an anonymous relationship admits every kind and still matches",
			query: `MATCH p = (u:User)-[]->(g:Group) RETURN p LIMIT 1000`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, generousBudget)
			if len(rs.Rows) != 50 {
				t.Fatalf("got %d rows, want 50", len(rs.Rows))
			}
		})
	}
}

// TestAbsentEdgeKindZeroLengthRangeStillMatches guards the one case where an
// absent kind does NOT condemn the pattern: a `*0..` range is satisfied by a
// zero-length match binding both endpoints to the same node, which traverses
// no edge, so the kind's absence says nothing about it. Short-circuiting here
// would turn a correct non-empty answer into an empty one.
func TestAbsentEdgeKindZeroLengthRangeStillMatches(t *testing.T) {
	snap := buildAbsentKindFixture(t, 5)

	rs := mustExec(t, snap, `MATCH (g:Group)-[:ProtectAdminGroups*0..]->(x) RETURN g, x`, generousBudget)
	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1 (the sole Group bound to itself at zero length)", len(rs.Rows))
	}
}

// TestAbsentEdgeKindInACarriedPart covers the short-circuit landing on a part
// AFTER a WITH boundary rather than on the query's first part: the second
// match can never succeed, so the query has no rows, and the preceding part's
// own perfectly matchable pattern must not turn that into an error. This is
// why the guard sits in matchPart (which every part runs through) rather than
// at the top of Execute.
func TestAbsentEdgeKindInACarriedPart(t *testing.T) {
	snap := buildAbsentKindFixture(t, 100)

	// Part[1] binds fresh symbols: reusing the carried `g` as a pattern node
	// is a shape this executor declines for unrelated structural reasons
	// (groupKeysOverlapNodes), which would mask what this test is about.
	const query = `MATCH (u:User)-[:MemberOf]->(g:Group) WITH g MATCH (a)-[:ProtectAdminGroups]->(b) RETURN b LIMIT 100`
	rs := mustExec(t, snap, query, generousBudget)
	if len(rs.Rows) != 0 {
		t.Fatalf("got %d rows, want 0 (the carried part names a kind with no edges)", len(rs.Rows))
	}
}
