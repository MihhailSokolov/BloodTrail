// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestTrailContinuesThroughADeltaOnlyEdge pins a SHORT answer -- the one
// failure mode a candidate source is never allowed to have.
//
// trailCanContinue decides whether a node reached mid-walk can extend the
// trail by binary-searching the step's endpoint set, which makes the set's
// SORTEDNESS a correctness requirement rather than a tidiness one. On an
// overlay, EdgeKindEndpoints built that set as "the base's sorted ids, minus
// the delta's, followed by the delta's" -- sorted in each half and not across
// the join. Searching such a slice for an id in the trailing half walks the
// leading half and reports it absent:
//
//	sort.Search([5 6 7 2], 2) = 0, and ids[0] is 5, so 2 "is not there"
//
// A node whose only admissible outgoing edge arrived in a delta segment, and
// whose dense id is below the base's endpoints, was therefore judged unable to
// continue, and every path through it vanished from the result. That is the
// ordinary state of a live BloodHound between compactions: ingest keeps adding
// edges to nodes the base snapshot already holds.
//
// The fixture forces exactly that arrangement -- the delta-only hop is dense
// id 0, every base endpoint is far above it.
func TestTrailContinuesThroughADeltaOnlyEdge(t *testing.T) {
	const (
		kGroup    snapshot.KindID = 1
		kMemberOf snapshot.KindID = 2
		kUser     snapshot.KindID = 3
	)

	// Dense ids are assigned in insertion order, so `mid` is added FIRST to
	// land at dense 0 -- below every node that carries a base MemberOf edge.
	const (
		midID  uint64 = 10
		dstID  uint64 = 11
		srcID  uint64 = 12
		filler        = 40
	)

	base := snapshot.NewBuilder(filler + 3)
	base.SetKinds(map[snapshot.KindID]string{kGroup: "Group", kMemberOf: "MemberOf", kUser: "User"})
	add := func(id uint64, kind snapshot.KindID, name string) {
		if err := base.AddNode(id, []snapshot.KindID{kind},
			mustJSON(t, map[string]any{"objectid": name})); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
	}
	// MID is a User, so it can carry the trail onward but can never be the
	// pattern's own endpoint. That is what makes the pruning decision load
	// bearing: a node wrongly judged unable to continue, and ineligible as an
	// endpoint, is dropped outright and everything beyond it is lost.
	add(midID, kUser, "MID")
	add(dstID, kGroup, "DST")
	add(srcID, kGroup, "SRC")
	for i := 0; i < filler; i++ {
		add(uint64(100+i), kGroup, "FILL")
	}

	// Base edges: SRC -> MID, plus filler edges among the HIGH dense ids so
	// the base endpoint set is entirely above dense 0. MID itself gets no
	// base outgoing edge -- its only way onward arrives in the delta.
	var eid uint64
	nextEdge := func() uint64 { eid++; return eid }
	base.AddEdge(nextEdge(), srcID, midID, kMemberOf)
	for i := 0; i < filler-1; i++ {
		base.AddEdge(nextEdge(), uint64(100+i), uint64(100+i+1), kMemberOf)
	}
	baseSnap, err := base.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// The delta supplies MID -> DST and nothing else.
	var sb snapshot.SegmentBuilder
	sb.AddKind(kGroup, "Group")
	sb.AddKind(kMemberOf, "MemberOf")
	sb.AddKind(kUser, "User")
	sb.AddEdgeState(900, midID, dstID, kMemberOf)

	view := snapshot.NewView(baseSnap).WithSegment(sb.Build())
	if !view.Overlay() {
		t.Fatal("fixture is not an overlay")
	}

	loose := Budgets{MaxRows: 10000, MaxWork: 10000000, MaxLiveRows: 10000}

	// One hop reaches MID; two hops must reach DST through the delta edge.
	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			// One hop reaches MID, which is a User and so not an answer.
			name:  "one hop reaches only the ineligible intermediate",
			query: `MATCH p = (s:Group)-[:MemberOf*1..1]->(t:Group) WHERE s.objectid = 'SRC' RETURN p`,
			want:  0,
		},
		{
			// SRC -> MID -> DST, the whole answer, and it exists only if the
			// walk is willing to pass THROUGH MID.
			name:  "two hops continue through the delta-only edge",
			query: `MATCH p = (s:Group)-[:MemberOf*1..2]->(t:Group) WHERE s.objectid = 'SRC' RETURN p`,
			want:  1,
		},
		{
			name:  "an unbounded walk finds it too",
			query: `MATCH p = (s:Group)-[:MemberOf*1..]->(t:Group) WHERE s.objectid = 'SRC' RETURN p`,
			want:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, view, tc.query, loose)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d -- a SHORT answer means a candidate"+
					" source dropped a real path", len(rs.Rows), tc.want)
			}
		})
	}
}

// TestEdgeKindEndpointsAreSortedOnAnOverlay states the same contract at the
// level it belongs to, so a future change to the union cannot reintroduce the
// break without a direct failure: trailCanContinue binary-searches this slice.
func TestEdgeKindEndpointsAreSortedOnAnOverlay(t *testing.T) {
	const (
		kGroup    snapshot.KindID = 1
		kMemberOf snapshot.KindID = 2
	)
	base := snapshot.NewBuilder(8)
	base.SetKinds(map[snapshot.KindID]string{kGroup: "Group", kMemberOf: "MemberOf"})
	for i := uint64(10); i < 18; i++ {
		if err := base.AddNode(i, []snapshot.KindID{kGroup},
			mustJSON(t, map[string]any{"objectid": "N"})); err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}
	// Base edges only among the higher dense ids.
	for i := uint64(14); i < 17; i++ {
		base.AddEdge(i, i, i+1, kMemberOf)
	}
	baseSnap, err := base.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Delta edges from the LOWEST dense ids, which a plain concatenation
	// would append after the base's higher ones.
	var sb snapshot.SegmentBuilder
	sb.AddKind(kGroup, "Group")
	sb.AddKind(kMemberOf, "MemberOf")
	sb.AddEdgeState(900, 10, 11, kMemberOf)
	sb.AddEdgeState(901, 11, 12, kMemberOf)

	view := snapshot.NewView(baseSnap).WithSegment(sb.Build())
	for _, outgoing := range []bool{true, false} {
		ids, ok := view.EdgeKindEndpoints([]snapshot.KindID{kMemberOf}, outgoing)
		if !ok {
			t.Fatalf("outgoing=%v: EdgeKindEndpoints declined", outgoing)
		}
		for i := 1; i < len(ids); i++ {
			if ids[i-1] >= ids[i] {
				t.Fatalf("outgoing=%v: not strictly ascending at %d: %v", outgoing, i, ids)
			}
		}
	}
}
