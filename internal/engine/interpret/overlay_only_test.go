// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"encoding/json"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestOverlayOnlyGraphAnswersTheE2EShortestPath reproduces the deployment
// shape build/e2e.sh runs in, which no other test in this package covers:
// the engine boots against an essentially EMPTY PostgreSQL (one node), so
// the base snapshot is one node wide, and the entire graph then arrives
// through write-through as delta segments with no rebuild in between. Every
// candidate source that reads the base snapshot -- the string index most of
// all -- sees almost nothing, and correctness rests entirely on the delta
// union.
//
// The query is the one e2e runs verbatim.
func TestOverlayOnlyGraphAnswersTheE2EShortestPath(t *testing.T) {
	const (
		kiUser      snapshot.KindID = 1
		kiGroup     snapshot.KindID = 2
		kiMigration snapshot.KindID = 4
	)

	const (
		userSID = "S-1-5-21-1-1-1-1105"
		midSID  = "S-1-5-21-1-1-1-1106"
		daSID   = "S-1-5-21-1-1-1-512"
	)

	// Base: the single node the e2e's migrated-from-Neo4j PostgreSQL actually
	// holds at boot -- BloodHound's own MigrationData row, whose properties
	// are three integers. Critically it carries NO objectid, so the base
	// snapshot never interns that name.
	//
	// That detail is the whole regression. Resolving `objectid` to a PropID
	// against the base and reading the miss as "nothing carries this
	// property" produced an EMPTY candidate set for a property every node in
	// the delta carries, and the query below answered zero rows -- a served
	// wrong answer, which reaches the user as an HTTP 404 from
	// /api/v2/graphs/cypher. Giving this node an objectid of its own is
	// enough to hide it completely, so it deliberately has none.
	base := snapshot.NewBuilder(1)
	base.SetKinds(map[snapshot.KindID]string{kiMigration: "MigrationData"})
	if err := base.AddNode(1, []snapshot.KindID{kiMigration},
		mustJSON(t, map[string]any{"Major": 9, "Minor": 6, "Patch": 0})); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	baseSnap, err := base.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Delta: the whole graph, the way write-through delivers it.
	var sb snapshot.SegmentBuilder
	sb.AddKind(kiUser, "User")
	sb.AddKind(kiGroup, "Group")
	for _, n := range []struct {
		id    uint64
		kind  snapshot.KindID
		props map[string]any
	}{
		{100, kiUser, map[string]any{"objectid": userSID, "name": "ALICE@TESTLAB.LOCAL"}},
		{101, kiGroup, map[string]any{"objectid": midSID, "name": "HELPDESK@TESTLAB.LOCAL"}},
		{102, kiGroup, map[string]any{"objectid": daSID, "name": "DOMAIN ADMINS@TESTLAB.LOCAL"}},
	} {
		if err := sb.AddNodeState(n.id, []snapshot.KindID{n.kind}, mustJSON(t, n.props)); err != nil {
			t.Fatalf("AddNodeState(%d): %v", n.id, err)
		}
	}
	memberOf := snapshot.KindID(3)
	sb.AddKind(memberOf, "MemberOf")
	sb.AddEdgeState(900, 100, 101, memberOf)
	sb.AddEdgeState(901, 101, 102, memberOf)

	view := snapshot.NewView(baseSnap).WithSegment(sb.Build())
	if !view.Overlay() {
		t.Fatal("fixture is not an overlay")
	}

	// Every shape below must answer, and a decline is as much a failure as a
	// wrong answer would be: declining is what the engine does correctly
	// when it cannot serve something, and PostgreSQL would then supply the
	// right rows -- which is exactly why this regression stayed invisible to
	// every differential test and surfaced only in the e2e.
	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			// The failing e2e probe, verbatim.
			name: "e2e shortestPath",
			query: `MATCH p=shortestPath((s)-[:MemberOf*1..]->(t:Group)) ` +
				`WHERE s.objectid = '` + userSID + `' AND t.objectid ENDS WITH '-512' AND s<>t ` +
				`RETURN p LIMIT 10`,
			want: 1,
		},
		{
			// The anchor alone -- the actual broken piece, which answered
			// zero against a graph holding exactly one match.
			name:  "suffix anchor alone",
			query: `MATCH (t:Group) WHERE t.objectid ENDS WITH '-512' RETURN t`,
			want:  1,
		},
		{
			name:  "equality anchor",
			query: `MATCH (s) WHERE s.objectid = '` + userSID + `' RETURN s`,
			want:  1,
		},
		{
			name:  "prefix anchor",
			query: `MATCH (t:Group) WHERE t.name STARTS WITH 'DOMAIN ADMINS' RETURN t`,
			want:  1,
		},
		{
			name:  "IN-list anchor",
			query: `MATCH (s) WHERE s.objectid IN ['` + userSID + `', '` + daSID + `'] RETURN s`,
			want:  2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ok := planNoFail(t, view, tc.query)
			if !ok || q == nil {
				t.Fatal("the planner declined a query it serves on a flat snapshot")
			}
			rs, err := Execute(&Env{Snap: view}, q, Budgets{MaxRows: 100000, MaxWork: 10000000, MaxLiveRows: 100000})
			if err != nil {
				t.Fatalf("declined at run time: %v", err)
			}
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d: every match lives in the delta, and a base "+
					"that never interned the property must not be read as proof there are none",
					len(rs.Rows), tc.want)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
