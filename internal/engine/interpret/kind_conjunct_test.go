// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildTagKindFixture mirrors a real deployment's Tier Zero shape: every
// node carries Base, a handful carry the Tag_Tier_Zero kind on top, and the
// kind TABLE also registers it when zero nodes do -- BloodHound registers
// its schema kinds up front, so on live data a tag kind always resolves even
// when nothing is tagged yet.
func buildTagKindFixture(t *testing.T, users, tagged int) *snapshot.View {
	t.Helper()
	const (
		tkBase snapshot.KindID = 1
		tkUser snapshot.KindID = 2
		tkTag  snapshot.KindID = 3
	)
	kinds := map[snapshot.KindID]string{tkBase: "Base", tkUser: "User", tkTag: "Tag_Tier_Zero"}
	var nodes []execNodeSpec
	for i := 0; i < users; i++ {
		ks := []snapshot.KindID{tkBase, tkUser}
		if i < tagged {
			ks = append(ks, tkTag)
		}
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), ks, map[string]any{
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 10000+i),
			"enabled":  true,
		}})
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// buildTaggedGroupChainFixture builds the "Nested groups within Tier Zero"
// shape: a large population of Groups joined by MemberOf edges, of which only
// `tagged` also carry the Tag_Tier_Zero kind.
//
// Every group has an outgoing MemberOf edge, so a near side narrowed only by
// "has an admissible out-edge" is still the whole population -- the tag is the
// ONLY thing that tells the two endpoints apart, which is what makes this a
// test of whether the route can see a kind written in WHERE.
func buildTaggedGroupChainFixture(t *testing.T, groups, tagged int) *snapshot.View {
	t.Helper()
	const (
		tgGroup    snapshot.KindID = 1
		tgTag      snapshot.KindID = 2
		tgMemberOf snapshot.KindID = 3
	)
	kinds := map[snapshot.KindID]string{
		tgGroup: "Group", tgTag: "Tag_Tier_Zero", tgMemberOf: "MemberOf",
	}

	// Layout, by index:
	//
	//	[0, tagged)        the tagged target groups
	//	[tagged, 2*tagged) one member each, pointing at its target
	//	[2*tagged, groups) the bulk, a closed MemberOf cycle among themselves
	//
	// Nothing points AT a member, so the backward walk from a target stops
	// after one hop and the answer is exactly `tagged` paths. The bulk carries
	// out-edges that lead nowhere near a tag, so a forward walk has to
	// enumerate all of it to discover that.
	bulk := 2 * tagged
	nodes := make([]execNodeSpec, 0, groups)
	for i := 0; i < groups; i++ {
		ks := []snapshot.KindID{tgGroup}
		if i < tagged {
			ks = append(ks, tgTag)
		}
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), ks, map[string]any{
			"objectid": fmt.Sprintf("S-1-5-21-9-9-9-%d", 20000+i),
		}})
	}

	var edges []execEdgeSpec
	var eid uint64
	add := func(from, to int) {
		eid++
		edges = append(edges, execEdgeSpec{eid, uint64(1000 + from), uint64(1000 + to), tgMemberOf})
	}
	for j := 0; j < tagged; j++ {
		add(tagged+j, j)
	}
	for i := bulk; i < groups; i++ {
		add(i, bulk+(i+1-bulk)%(groups-bulk))
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestWhereKindConjunctAnchorsOnTheKind pins extractKindConjunct end to end:
// BloodHound's Tier Zero prebuilts spell their kind test in WHERE
// (`MATCH (n:Base) WHERE (n:Tag_Tier_Zero) AND ...`), and the anchor must be
// the tag kind's own bitmap, not the graph-covering Base one. The assertion
// is a work budget far too small for a Base scan: with the extraction the
// candidate source is the tagged handful, without it the query walks every
// node and blows the budget.
func TestWhereKindConjunctAnchorsOnTheKind(t *testing.T) {
	snap := buildTagKindFixture(t, 3000, 3)
	tight := Budgets{MaxRows: 100, MaxWork: 20}

	rs := mustExec(t, snap, `MATCH (n:Base) WHERE (n:Tag_Tier_Zero) AND n.enabled = true RETURN n LIMIT 10`, tight)
	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want the 3 tagged nodes", len(rs.Rows))
	}

	// The empty-tag variant is the common production case (nothing tagged
	// yet): the anchor bitmap has no members, so the answer is empty at
	// essentially zero cost.
	empty := buildTagKindFixture(t, 3000, 0)
	rs = mustExec(t, empty, `MATCH (n:Base) WHERE (n:Tag_Tier_Zero) AND n.enabled = true RETURN n LIMIT 10`, Budgets{MaxRows: 100, MaxWork: 1})
	if len(rs.Rows) != 0 {
		t.Fatalf("got %d rows, want 0 (no node carries the tag kind)", len(rs.Rows))
	}
}

// TestWhereKindConjunctExtractionScope pins exactly which WHERE shapes reach
// NodeConstraint.Kinds: a bare (or parenthesised) exclusive kind matcher
// does, a negation or disjunction of them does not, and an endpoint of a
// variable-length step is folded like any other symbol.
//
// That last one was once refused, back when the reverse-routing decision was
// a structural rule rather than a cost comparison and any change to a
// traversal endpoint's Kinds re-routed queries unpredictably. Now that the
// route prices both endpoints, hiding a kind from it is what does the damage.
func TestWhereKindConjunctExtractionScope(t *testing.T) {
	snap := buildDomainAdminsFixture(t, 50)
	kindID := func(name string) snapshot.KindID {
		id, ok := snap.Kinds().ID(name)
		if !ok {
			t.Fatalf("fixture kind %q missing", name)
		}
		return id
	}
	countKind := func(part *Part, sym string, k snapshot.KindID) int {
		n := 0
		for _, id := range part.Nodes[sym].Kinds {
			if id == k {
				n++
			}
		}
		return n
	}

	t.Run("bare kind conjunct is folded into Kinds", func(t *testing.T) {
		q := planQuery(t, snap, `MATCH (a) WHERE (a:User) AND a.objectid ENDS WITH '-512' RETURN a`)
		if got := countKind(&q.Parts[0], "a", kindID("User")); got != 1 {
			t.Fatalf("User appears %d times in a's Kinds, want exactly 1", got)
		}
	})

	t.Run("negated and disjunctive kind tests are not folded", func(t *testing.T) {
		q := planQuery(t, snap, `MATCH (a) WHERE NOT (a:User) RETURN a LIMIT 5`)
		if got := countKind(&q.Parts[0], "a", kindID("User")); got != 0 {
			t.Fatalf("negated kind test reached Kinds (%d entries)", got)
		}
		q = planQuery(t, snap, `MATCH (a) WHERE (a:User or a:Computer) RETURN a LIMIT 5`)
		if got := countKind(&q.Parts[0], "a", kindID("User")); got != 0 {
			t.Fatalf("disjunctive kind test reached Kinds (%d entries)", got)
		}
	})

	t.Run("a var-length endpoint is folded like any other symbol", func(t *testing.T) {
		q := planQuery(t, snap, `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE a:User AND t.objectid ENDS WITH '-512' RETURN p`)
		if got := countKind(&q.Parts[0], "a", kindID("User")); got != 1 {
			t.Fatalf("WHERE kind test on a var-length endpoint appears %d times in Kinds, want exactly 1", got)
		}
	})
}

// TestWhereKindOnTraversalEndpointRoutesToTheTag is the reason the var-length
// refusal in extractKindConjunct had to go, stated as the query that suffered
// from it: BloodHound's shipped "Nested groups within Tier Zero / High Value"
// prebuilt.
//
//	MATCH p=(t:Group)<-[:MemberOf*..]-(s:Group) WHERE (t:Tag_Tier_Zero) ...
//
// Both endpoints are written `:Group`, and the only thing telling them apart
// is a kind test in WHERE. While that test was hidden from NodeConstraint.
// Kinds, the two ends priced identically, the route could not prefer either,
// and the walk seeded from every group in the graph to find the handful
// carrying the tag -- measured at 4.7x PostgreSQL on the benchmark graph.
//
// The budget here is far too small to have enumerated the group kind, so a
// passing run is proof the seed scan never touched it.
func TestWhereKindOnTraversalEndpointRoutesToTheTag(t *testing.T) {
	snap := buildTaggedGroupChainFixture(t, 800, 2)

	tight := Budgets{MaxRows: 100, MaxWork: 400, MaxLiveRows: 1000}
	rs := mustExec(t, snap, `MATCH p=(t:Group)<-[:MemberOf*1..]-(s:Group) WHERE (t:Tag_Tier_Zero) RETURN p LIMIT 100`, tight)
	if len(rs.Rows) != 2 {
		t.Fatalf("got %d rows, want the 2 nested-group paths", len(rs.Rows))
	}

	// Same query, same answer, with a budget large enough for either route --
	// the routing change must not have changed which rows come out.
	loose := Budgets{MaxRows: 100000, MaxWork: 100000000, MaxLiveRows: 100000}
	if got := mustExec(t, snap, `MATCH p=(t:Group)<-[:MemberOf*1..]-(s:Group) WHERE (t:Tag_Tier_Zero) RETURN p LIMIT 100`, loose); len(got.Rows) != 2 {
		t.Fatalf("with a loose budget got %d rows, want 2", len(got.Rows))
	}
}
