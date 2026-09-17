// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

const (
	kcUser  snapshot.KindID = 1
	kcGroup snapshot.KindID = 2
	kcAdmin snapshot.KindID = 3
)

func buildKindCountFixture(t *testing.T, users, groups, admins int) *snapshot.View {
	t.Helper()
	kinds := map[snapshot.KindID]string{kcUser: "User", kcGroup: "Group", kcAdmin: "Tag_Tier_Zero"}
	var nodes []execNodeSpec
	id := uint64(1000)
	for i := 0; i < users; i++ {
		ks := []snapshot.KindID{kcUser}
		if i < admins {
			ks = append(ks, kcAdmin)
		}
		nodes = append(nodes, execNodeSpec{id, ks, map[string]any{
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 2000+i),
			"enabled":  i%2 == 0,
		}})
		id++
	}
	for i := 0; i < groups; i++ {
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{kcGroup}, map[string]any{
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 9000+i)}})
		id++
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestKindCountAnswersFromTheBitmap pins the shortcut: the count of a kind is
// a number the snapshot already holds, so the query materializes no rows.
//
// The budget is far below the population, which a row-per-node count could
// not survive.
func TestKindCountAnswersFromTheBitmap(t *testing.T) {
	const users, groups = 5000, 300
	snap := buildKindCountFixture(t, users, groups, 7)

	meter := &workMeter{budget: Budgets{MaxRows: 10, MaxWork: 10, MaxLiveRows: 10}}
	rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, `MATCH (u:User) RETURN COUNT(u)`), meter)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if len(rs.Rows) != 1 || len(rs.Rows[0]) != 1 {
		t.Fatalf("got %d rows, want a single one-column row", len(rs.Rows))
	}
	if got, ok := rs.Rows[0][0].Scalar.(float64); !ok || int(got) != users {
		t.Fatalf("COUNT(u) = %v, want %d", rs.Rows[0][0].Scalar, users)
	}
	if meter.work != 0 {
		t.Fatalf("meter.work = %d, want 0: the count came from the bitmap or it did not", meter.work)
	}
}

// TestKindCountMatchesTheCountingPath is the correctness half, including
// every shape the shortcut must REFUSE -- each of which would make the kind's
// raw population the wrong answer.
func TestKindCountMatchesTheCountingPath(t *testing.T) {
	const users, groups, admins = 400, 30, 9
	snap := buildKindCountFixture(t, users, groups, admins)
	generous := Budgets{MaxRows: 1_000_000, MaxWork: 10_000_000, MaxLiveRows: 1_000_000}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{"bare kind count", `MATCH (u:User) RETURN COUNT(u)`, users},
		{"a different kind", `MATCH (g:Group) RETURN COUNT(g)`, groups},
		{"a tag kind", `MATCH (n:Tag_Tier_Zero) RETURN COUNT(n)`, admins},
		// Refusals: a predicate, so the population is not the answer.
		{"a predicate must still be applied", `MATCH (u:User) WHERE u.enabled = true RETURN COUNT(u)`, users / 2},
		{"a WHERE kind test narrows too", `MATCH (u:User) WHERE (u:Tag_Tier_Zero) RETURN COUNT(u)`, admins},
		{"an objectid anchor narrows", `MATCH (u:User) WHERE u.objectid = 'S-1-5-21-1-1-1-2000' RETURN COUNT(u)`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs, err := runQuery(&Env{Snap: snap}, planQuery(t, snap, tc.query), &workMeter{budget: generous})
			if err != nil {
				t.Fatalf("runQuery: %v", err)
			}
			if len(rs.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rs.Rows))
			}
			got, ok := rs.Rows[0][0].Scalar.(float64)
			if !ok || int(got) != tc.want {
				t.Fatalf("got %v, want %d", rs.Rows[0][0].Scalar, tc.want)
			}
		})
	}
}

// TestKindCountCountsTheOverlay pins that the shortcut reads a MERGED
// population: a delta that adds and tombstones nodes changes the answer, and
// NodesOfKind resolves both.
func TestKindCountCountsTheOverlay(t *testing.T) {
	base := buildKindCountFixture(t, 100, 0, 0)
	generous := Budgets{MaxRows: 1_000_000, MaxWork: 10_000_000, MaxLiveRows: 1_000_000}

	var sb snapshot.SegmentBuilder
	sb.AddKind(kcUser, "User")
	// One new user, and one existing user tombstoned.
	if err := sb.AddNodeState(77777, []snapshot.KindID{kcUser}, mustJSON(t, map[string]any{"objectid": "NEW"})); err != nil {
		t.Fatalf("AddNodeState: %v", err)
	}
	sb.TombstoneNode(1000)
	ov := base.WithSegment(sb.Build())

	rs, err := runQuery(&Env{Snap: ov}, planQuery(t, ov, `MATCH (u:User) RETURN COUNT(u)`), &workMeter{budget: generous})
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if got := int(rs.Rows[0][0].Scalar.(float64)); got != 100 {
		t.Fatalf("COUNT(u) = %d over an overlay that added one user and removed one, want 100", got)
	}
}
