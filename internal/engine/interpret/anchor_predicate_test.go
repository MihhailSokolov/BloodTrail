// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildSuffixAnchorFixture: many groups, each AdminTo one computer, exactly
// one group carrying the well-known suffix -- BloodHound's "Domain Users"
// query shape (`(s:Group)-[:AdminTo]->(c:Computer) WHERE s.objectid ENDS
// WITH '-513'`).
func buildSuffixAnchorFixture(t *testing.T, groups int) *snapshot.View {
	t.Helper()
	const (
		saGroup    snapshot.KindID = 1
		saComputer snapshot.KindID = 2
		saAdminTo  snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{saGroup: "Group", saComputer: "Computer", saAdminTo: "AdminTo"}
	// AddNode requires strictly ascending database ids: all groups first,
	// then all computers.
	var nodes []execNodeSpec
	var edges []execEdgeSpec
	for i := 0; i < groups; i++ {
		suffix := fmt.Sprintf("-%d", 20000+i)
		if i == 0 {
			suffix = "-513"
		}
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), []snapshot.KindID{saGroup},
			map[string]any{"objectid": "S-1-5-21-1-1-1" + suffix}})
	}
	for i := 0; i < groups; i++ {
		nodes = append(nodes, execNodeSpec{uint64(500000 + i), []snapshot.KindID{saComputer},
			map[string]any{"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 30000+i)}})
		edges = append(edges, execEdgeSpec{uint64(1 + i), uint64(1000 + i), uint64(500000 + i), saAdminTo})
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestPushedPredicateFiltersBeforeExpansion pins admit-time predicate
// evaluation end to end: the anchor's pushed `ENDS WITH` conjunct must
// collapse the 1,000-group anchor to its single match BEFORE the AdminTo
// expansion runs, not after. The budget admits one visit per group plus a
// little; expanding all 1,000 groups first (the pre-fix pipeline) costs
// roughly three times that and fails.
func TestPushedPredicateFiltersBeforeExpansion(t *testing.T) {
	const groups = 1000
	snap := buildSuffixAnchorFixture(t, groups)

	const query = `MATCH p = (s:Group)-[:AdminTo]->(c:Computer) WHERE s.objectid ENDS WITH '-513' RETURN p LIMIT 100`
	rs := mustExec(t, snap, query, Budgets{MaxRows: 1000, MaxWork: groups + 50})
	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want exactly 1 (one group ends with -513, admin to one computer)", len(rs.Rows))
	}
}

// TestPushedPredicateFiltersExpansionTargets is the mirror image: the
// selective predicate sits on the far endpoint, so it must prune candidates
// during expansion rather than after. The anchor chooser picks the smaller
// Group side; every group then expands to its computer, and the computer's
// own pushed conjunct must drop each non-matching candidate at the step --
// each costs its adjacency visit but never becomes a row to carry.
func TestPushedPredicateFiltersExpansionTargets(t *testing.T) {
	const groups = 400
	snap := buildSuffixAnchorFixture(t, groups)

	const query = `MATCH p = (s:Group)-[:AdminTo]->(c:Computer) WHERE c.objectid ENDS WITH '-30000' RETURN p LIMIT 100`
	rs := mustExec(t, snap, query, Budgets{MaxRows: 1000, MaxWork: 3*groups + 100})
	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want exactly 1 (one computer ends with -30000)", len(rs.Rows))
	}
}
