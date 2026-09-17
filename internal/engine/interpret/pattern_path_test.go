// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

const (
	ppUser     snapshot.KindID = 1
	ppGroup    snapshot.KindID = 2
	ppMemberOf snapshot.KindID = 3
)

// buildVShapeFixture: `users` users, all members of one group, plus a second
// group nobody joins and a wide field of decoy users so the anchor choice
// matters.
func buildVShapeFixture(t *testing.T, members, decoys int) *snapshot.View {
	t.Helper()
	kinds := map[snapshot.KindID]string{ppUser: "User", ppGroup: "Group", ppMemberOf: "MemberOf"}

	const gID, otherID uint64 = 500, 501
	nodes := []execNodeSpec{
		{gID, []snapshot.KindID{ppGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-513"}},
		{otherID, []snapshot.KindID{ppGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-999"}},
	}
	for i := 0; i < members+decoys; i++ {
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), []snapshot.KindID{ppUser},
			map[string]any{"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 2000+i)}})
	}
	var edges []execEdgeSpec
	for i := 0; i < members; i++ {
		edges = append(edges, execEdgeSpec{uint64(9000 + i), uint64(1000 + i), gID, ppMemberOf})
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestVShapeNamedPathIsServed pins the shipped shape that used to be declined
// for its `p =` alone:
//
//	MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User)
//
// Both its steps point INTO g, so the chain is not left-to-right and
// isStrictLinearChain refuses it -- but as written it is the plain three-node
// path a-g-b, and the identical query without `p =` was served all along.
func TestVShapeNamedPathIsServed(t *testing.T) {
	const members = 6
	snap := buildVShapeFixture(t, members, 200)
	loose := Budgets{MaxRows: 100000, MaxWork: 10000000, MaxLiveRows: 100000}

	const q = `MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User) ` +
		`WHERE g.objectid ENDS WITH '-513' RETURN p`
	rs := mustExec(t, snap, q, loose)

	// Every ORDERED pair of DISTINCT members. a and b may not be the same
	// user: both steps would then traverse that user's single MemberOf edge,
	// and Cypher forbids one pattern from using a relationship twice.
	if want := members * (members - 1); len(rs.Rows) != want {
		t.Fatalf("got %d rows, want %d", len(rs.Rows), want)
	}

	// The same pattern without the named path must select exactly as many.
	plain := mustExec(t, snap,
		`MATCH (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User) WHERE g.objectid ENDS WITH '-513' RETURN a, b`,
		loose)
	if len(plain.Rows) != len(rs.Rows) {
		t.Fatalf("named path returned %d rows, the same pattern unnamed returned %d",
			len(rs.Rows), len(plain.Rows))
	}
}

// TestVShapePathContents is the correctness half: the path must read the way
// the pattern was WRITTEN -- a, then the shared group, then b -- with both
// edges, whichever direction the executor happened to walk them.
func TestVShapePathContents(t *testing.T) {
	snap := buildVShapeFixture(t, 3, 0)
	loose := Budgets{MaxRows: 100000, MaxWork: 10000000, MaxLiveRows: 100000}

	rs := mustExec(t, snap,
		`MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User) `+
			`WHERE g.objectid ENDS WITH '-513' RETURN p`, loose)
	if len(rs.Rows) != 6 {
		t.Fatalf("got %d rows, want 6 (ordered pairs of 3 distinct members)", len(rs.Rows))
	}

	for i, row := range rs.Rows {
		if len(row) != 1 {
			t.Fatalf("row %d has %d columns, want 1", i, len(row))
		}
		pv := row[0].Path
		if pv == nil {
			t.Fatalf("row %d did not bind a path", i)
		}
		if len(pv.Nodes) != 3 {
			t.Fatalf("row %d path has %d nodes, want 3 (a, g, b)", i, len(pv.Nodes))
		}
		if len(pv.Edges) != 2 {
			t.Fatalf("row %d path has %d edges, want 2", i, len(pv.Edges))
		}
		// The middle node is the shared group in every row.
		if got := snap.GraphID(pv.Nodes[1]); got != 500 {
			t.Fatalf("row %d path middle node is %d, want the shared group 500", i, got)
		}
		if snap.GraphID(pv.Nodes[0]) == 500 || snap.GraphID(pv.Nodes[2]) == 500 {
			t.Fatalf("row %d path endpoints must be the users, not the group", i)
		}
	}
}

// TestPatternLinearChainScope pins which shapes the reorientable path route
// accepts, so a future change cannot quietly widen it into shapes whose path
// order it has no way to determine.
func TestPatternLinearChainScope(t *testing.T) {
	snap := buildVShapeFixture(t, 2, 0)

	for _, tc := range []struct {
		name  string
		query string
		want  bool
	}{
		{"converging V", `MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User) RETURN p`, true},
		{"left-to-right chain", `MATCH p = (a:User)-[:MemberOf]->(g:Group)-[:MemberOf]->(h:Group) RETURN p`, true},
		{
			// A trail, not a single edge: splicing it depends on which end it
			// was grown from, which this route does not track.
			name:  "a variable-length step is refused",
			query: `MATCH p = (a:User)-[:MemberOf*1..3]->(g:Group)<-[:MemberOf]-(b:User) RETURN p`,
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ok := planNoFail(t, snap, tc.query)
			if !ok {
				t.Skip("planner declined this shape for an unrelated reason")
			}
			part := &q.Parts[0]
			idxs := make([]int, len(part.Chains))
			for i := range part.Chains {
				idxs[i] = i
			}
			_, got := patternLinearChain(part, idxs)
			// A left-to-right chain is handled by the strict route instead;
			// either acceptance is fine, so only the refusal is asserted.
			if !tc.want && got {
				t.Fatalf("patternLinearChain accepted a shape it cannot order")
			}
		})
	}
}

// TestVShapeFanOutDeclinesWithoutExpanding pins the guard that keeps the
// reorientable-path route from being a regression on the shape it was written
// for. `Domain Users` holds every account in a domain, so
// `(a)-[:MemberOf]->(g)<-[:MemberOf]-(b)` over it is quadratic in the group's
// membership -- forty billion paths on the benchmark graph. The engine cannot
// serve that under any budget; what it must not do is spend the expansion
// discovering so.
func TestVShapeFanOutDeclinesWithoutExpanding(t *testing.T) {
	// 400 members: 400*399 = 159,600 paths, comfortably past the row budget.
	snap := buildVShapeFixture(t, 400, 0)

	meter := &workMeter{budget: Budgets{MaxRows: 1000, MaxWork: 100_000_000, MaxLiveRows: 1_000_000}}
	_, err := runQuery(&Env{Snap: snap},
		planQuery(t, snap, `MATCH p = (a:User)-[:MemberOf]->(g:Group)<-[:MemberOf]-(b:User) `+
			`WHERE g.objectid ENDS WITH '-513' RETURN p LIMIT 1000`), meter)
	if err == nil {
		t.Fatal("want a decline: the fan is far past the row budget")
	}
	// The whole point: the decline costs the anchor scan, not the expansion.
	if meter.work > 1000 {
		t.Fatalf("meter.work = %d: the decline expanded the fan instead of "+
			"reading the degrees", meter.work)
	}
}
