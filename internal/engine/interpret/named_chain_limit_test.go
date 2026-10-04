// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// branchingChainKinds is the kind table of the branching-chain fixtures.
var branchingChainKinds = map[snapshot.KindID]string{
	1: "User", 2: "Group", 3: "Computer", 10: "MemberOf", 11: "AdminTo",
}

// branchingChainViews returns the same graph twice: as one base snapshot,
// and as a base holding only some of it with the rest -- a user and every
// edge -- arriving as a delta segment, the steady state of a live replica.
//
// Database id 1 is a Computer no pattern below reaches, and it is dense node
// 0: an expansion that reads an unbound symbol as node 0 starts there.
func branchingChainViews(t *testing.T) map[string]*snapshot.View {
	t.Helper()
	const (
		kUser, kGroup, kComputer snapshot.KindID = 1, 2, 3
		kMemberOf, kAdminTo      snapshot.KindID = 10, 11
	)
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kComputer}, map[string]any{"objectid": "C0"}},
		{2, []snapshot.KindID{kGroup}, map[string]any{"objectid": "G"}},
		{3, []snapshot.KindID{kUser}, map[string]any{"objectid": "A"}},
		{4, []snapshot.KindID{kUser}, map[string]any{"objectid": "B"}},
		{5, []snapshot.KindID{kUser}, map[string]any{"objectid": "C"}},
		{6, []snapshot.KindID{kComputer}, map[string]any{"objectid": "C1"}},
	}
	edges := []execEdgeSpec{
		{100, 3, 2, kMemberOf}, {101, 4, 2, kMemberOf}, {102, 5, 2, kMemberOf},
		{200, 3, 6, kAdminTo}, {201, 4, 6, kAdminTo},
	}

	views := map[string]*snapshot.View{"base": buildExecSnapshot(t, branchingChainKinds, nodes, edges)}

	// Overlay: user C (id 5) and every edge live only in the delta.
	baseNodes := make([]execNodeSpec, 0, len(nodes))
	for _, n := range nodes {
		if n.id != 5 {
			baseNodes = append(baseNodes, n)
		}
	}
	base := buildExecSnapshot(t, branchingChainKinds, baseNodes, nil)
	var sb snapshot.SegmentBuilder
	for id, name := range branchingChainKinds {
		sb.AddKind(id, name)
	}
	if err := sb.AddNodeState(5, []snapshot.KindID{kUser}, mustJSON(t, map[string]any{"objectid": "C"})); err != nil {
		t.Fatalf("AddNodeState: %v", err)
	}
	for _, e := range edges {
		sb.AddEdgeState(e.id, e.start, e.end, e.kind)
	}
	overlay := base.WithSegment(sb.Build())
	if !overlay.Overlay() {
		t.Fatal("fixture is not an overlay")
	}
	views["overlay"] = overlay
	return views
}

// branchingChainPathSigs renders column col of every row as its path's
// database node and edge ids, sorted -- overlay-aware, since an overlay
// edge has no forward-CSR slot.
func branchingChainPathSigs(t *testing.T, snap *snapshot.View, rs *ResultSet, col int) []string {
	t.Helper()
	sigs := make([]string, len(rs.Rows))
	for i, row := range rs.Rows {
		v := row[col]
		if v.Kind != OutPath || v.Path == nil {
			t.Fatalf("row %d column %d is not a path: %+v", i, col, v)
		}
		var b strings.Builder
		for _, n := range v.Path.Nodes {
			fmt.Fprintf(&b, "n%d,", snap.GraphID(n))
		}
		for _, e := range v.Path.Edges {
			fmt.Fprintf(&b, "e%d,", e.DatabaseID(snap))
		}
		sigs[i] = b.String()
	}
	sort.Strings(sigs)
	return sigs
}

// TestNamedBranchingChainServesSamePathsUnderLimitAndDistinct: a named path
// over a fixed chain whose steps do not all point one way -- converging
// `(a)->(g)<-(b)` or diverging `(g)<-(u)->(c)`, BloodHound's own
// co-membership and "members' admin rights" shapes -- must serve the paths
// its unlimited form serves when a LIMIT or RETURN DISTINCT sends it through
// the chunked driver or the DISTINCT streamer.
//
// Both of those scan a chunk of one symbol's rows and hand it to
// runComponentFrom. They used to scan the chain end chainAnchorSym picks
// while runComponentFrom rooted its tree walk at chooseAnchor's symbol; the
// walk then expanded from a symbol no row bound -- read as dense node 0 --
// and served nothing at all.
func TestNamedBranchingChainServesSamePathsUnderLimitAndDistinct(t *testing.T) {
	const (
		converging = `MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User) WHERE b.objectid = 'B' RETURN p`
		inline     = `MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User {objectid: 'B'}) RETURN p`
		diverging  = `MATCH p = (g:Group {objectid: 'G'})<-[:MemberOf]-(u:User)-[:AdminTo]->(c:Computer) RETURN p`
		carried    = `MATCH (x:Computer) WITH x MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User) WHERE b.objectid = 'B' RETURN p`
	)
	for viewName, snap := range branchingChainViews(t) {
		for _, tc := range []struct {
			unlimited string
			want      int
			distinct  bool
		}{
			{converging, 2, true},
			{inline, 2, true},
			{diverging, 2, true},
			// A WITH boundary's cross join repeats each path once per seed,
			// so DISTINCT would collapse rows the unlimited form keeps.
			{carried, 4, false},
		} {
			t.Run(viewName+"/"+tc.unlimited, func(t *testing.T) {
				want := branchingChainPathSigs(t, snap, mustExec(t, snap, tc.unlimited, generousBudget), 0)
				if len(want) != tc.want {
					t.Fatalf("unlimited form serves %d paths %v, want %d", len(want), want, tc.want)
				}
				valid := map[string]bool{}
				for _, sig := range want {
					valid[sig] = true
				}

				variants := map[string]int{
					tc.unlimited + " LIMIT 100": len(want),
					tc.unlimited + " LIMIT 1":   1,
				}
				if tc.distinct {
					variants[strings.Replace(tc.unlimited, "RETURN p", "RETURN DISTINCT p", 1)] = len(want)
					variants[strings.Replace(tc.unlimited, "RETURN p", "RETURN DISTINCT p", 1)+" LIMIT 100"] = len(want)
				}
				for query, wantRows := range variants {
					got := branchingChainPathSigs(t, snap, mustExec(t, snap, query, generousBudget), 0)
					if len(got) != wantRows {
						t.Errorf("%s\n    served %d paths %v, want %d of %v", query, len(got), got, wantRows, want)
						continue
					}
					for _, sig := range got {
						if !valid[sig] {
							t.Errorf("%s\n    served %s, which the unlimited form does not serve (%v)", query, sig, want)
						}
					}
				}
			})
		}
	}
}

// TestFixedStepExpansionRejectsUnboundSymbol: expanding or verifying a fixed
// step from a symbol the row does not bind is an executor inconsistency --
// the anchor a caller scanned is not the one the walk expects -- and must
// fail the query (which then declines to PostgreSQL) rather than read the
// missing binding as dense node 0 and serve whatever node 0 happens to reach.
func TestFixedStepExpansionRejectsUnboundSymbol(t *testing.T) {
	snap := branchingChainViews(t)["base"]
	env := &Env{Snap: snap}
	part := &planQuery(t, snap, `MATCH (u:User)-[:AdminTo]->(c:Computer) RETURN u, c`).Parts[0]
	step := &part.Chains[0]

	// Bound on the far end only: the expansion's own bound symbol is missing.
	onlyTarget := NewRow()
	onlyTarget.SetNode(step.ToSym, 0)
	if _, err := expandStep(env, &workMeter{budget: generousBudget}, []*Row{onlyTarget}, step,
		step.FromSym, step.ToSym, true, part.Nodes[step.ToSym], ""); !errors.Is(err, errUnboundSymbol) {
		t.Errorf("expandStep from an unbound symbol: err = %v, want errUnboundSymbol", err)
	}

	for name, row := range map[string]*Row{"from": onlyTarget, "to": func() *Row {
		r := NewRow()
		r.SetNode(step.FromSym, 0)
		return r
	}()} {
		if _, err := verifyClosingStep(env, &workMeter{budget: generousBudget}, []*Row{row}, step); !errors.Is(err, errUnboundSymbol) {
			t.Errorf("verifyClosingStep with its %s symbol unbound: err = %v, want errUnboundSymbol", name, err)
		}
	}
}
