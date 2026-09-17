// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildOrderFixture: `score` is numeric on most nodes, absent on one and a
// stored JSON null on another -- the three cases PostgreSQL ranks
// differently. `label` is a string property, which must keep ORDER BY
// delegated because pg orders strings by a collation this package cannot
// know.
func buildOrderFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const k snapshot.KindID = 1
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{k}, map[string]any{"tag": "a", "score": 3, "label": "aa"}},
		{2, []snapshot.KindID{k}, map[string]any{"tag": "b", "score": 1, "label": "bb"}},
		{3, []snapshot.KindID{k}, map[string]any{"tag": "c", "label": "cc"}},               // score absent
		{4, []snapshot.KindID{k}, map[string]any{"tag": "d", "score": nil, "label": "dd"}}, // stored null
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{k: "N"}, nodes, nil)
}

// TestOrderByNumericPropertyMatchesPostgres pins the ordering measured
// against a live PostgreSQL:
//
//	ORDER BY n.score       -> [stored null] [1] [3] [absent]
//	ORDER BY n.score DESC  -> [absent] [3] [1] [stored null]
//
// A stored JSON null ranks below every number, an absent property above
// every number, and DESC is the exact reverse -- which is what lets sortRows
// keep implementing DESC as a plain negation.
func TestOrderByNumericPropertyMatchesPostgres(t *testing.T) {
	snap := buildOrderFixture(t)

	tags := func(q string) []string {
		t.Helper()
		rs := mustExec(t, snap, q, generousBudget)
		var out []string
		for _, row := range rs.Rows {
			out = append(out, fmt.Sprint(row[0].Scalar))
		}
		return out
	}

	got := tags(`MATCH (n:N) RETURN n.tag, n.score ORDER BY n.score`)
	if want := []string{"d", "b", "a", "c"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ASC = %v, want %v (stored null, 1, 3, absent)", got, want)
	}

	got = tags(`MATCH (n:N) RETURN n.tag, n.score ORDER BY n.score DESC`)
	if want := []string{"c", "a", "b", "d"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("DESC = %v, want %v (absent, 3, 1, stored null)", got, want)
	}

	// The top-k shape this exists for.
	rs := mustExec(t, snap, `MATCH (n:N) WHERE n.score IS NOT NULL RETURN n.tag, n.score ORDER BY n.score DESC LIMIT 1`, generousBudget)
	if len(rs.Rows) != 1 || fmt.Sprint(rs.Rows[0][0].Scalar) != "a" {
		t.Fatalf("top-1 by score = %v, want the node scoring 3", rs.Rows)
	}
}

// TestOrderByStringPropertyStaysDelegated: a property that is string-valued
// anywhere in the snapshot must still decline at PLAN time. Discovering it
// mid-sort would mean materializing the whole result before delegating, so
// the property index answers it up front.
func TestOrderByStringPropertyStaysDelegated(t *testing.T) {
	snap := buildOrderFixture(t)
	if _, ok := planNoFail(t, snap, `MATCH (n:N) RETURN n.label ORDER BY n.label`); ok {
		t.Fatal("ORDER BY a string property must stay delegated: pg's collation is not knowable here")
	}
	if _, ok := planNoFail(t, snap, `MATCH (n:N) RETURN n.label AS l ORDER BY l`); ok {
		t.Fatal("aliasing the string property must not change that")
	}
}

// TestOrderByUnprojectedProperty pins the top-k shape an analyst actually
// writes: the RESULT is the node, the SORT is by one of its properties, and
// the projection never outputs that property. PostgreSQL sorts by it
// happily; the key is read off each row instead of resolved to a column.
func TestOrderByUnprojectedProperty(t *testing.T) {
	snap := buildOrderFixture(t)

	rs := mustExec(t, snap, `MATCH (n:N) WHERE n.score IS NOT NULL RETURN n ORDER BY n.score DESC LIMIT 1`, generousBudget)
	if len(rs.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rs.Rows))
	}
	if got := snap.GraphID(rs.Rows[0][0].Node); got != 1 {
		t.Fatalf("top-1 node = %d, want node 1 (score 3)", got)
	}

	// Same never-string gate as the projected case.
	if _, ok := planNoFail(t, snap, `MATCH (n:N) RETURN n ORDER BY n.label`); ok {
		t.Fatal("ORDER BY an unprojected STRING property must stay delegated")
	}

	// PostgreSQL rejects an unprojected sort key under DISTINCT ("for
	// SELECT DISTINCT, ORDER BY expressions must appear in select list"),
	// so that combination must keep declining rather than be served with an
	// order pg could not produce.
	if _, ok := planNoFail(t, snap, `MATCH (n:N) RETURN DISTINCT n ORDER BY n.score`); ok {
		t.Fatal("ORDER BY an unprojected property under DISTINCT must stay delegated")
	}
}
