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
// does; a negation or disjunction of them does not; and an endpoint of a
// variable-length step is never touched, because the reverse-routing
// contract (endpointNarrows/kindOnlyPredicate, scanEquivalentNearSide) was
// calibrated around WHERE kind tests not counting as anchors there.
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

	t.Run("a var-length endpoint keeps only its pattern-written kinds", func(t *testing.T) {
		q := planQuery(t, snap, `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE a:User AND t.objectid ENDS WITH '-512' RETURN p`)
		if got := countKind(&q.Parts[0], "a", kindID("User")); got != 0 {
			t.Fatalf("WHERE kind test on a var-length endpoint reached Kinds (%d entries) -- this re-routes the reverse-seeding decision", got)
		}
	})
}
