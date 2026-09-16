// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildPropAnchorFixture: `users` User nodes each carrying name/objectid,
// exactly one of which has the well-known `-513` RID suffix and the `SVC`
// name prefix, plus three carrying a sparse `system_tags` value. This is
// the shape of the prebuilt families that anchor on a string predicate.
func buildPropAnchorFixture(t *testing.T, users int) *snapshot.View {
	t.Helper()
	const (
		piUser  snapshot.KindID = 1
		piGroup snapshot.KindID = 2
	)
	kinds := map[snapshot.KindID]string{piUser: "User", piGroup: "Group"}
	var nodes []execNodeSpec
	for i := 0; i < users; i++ {
		props := map[string]any{
			"name":     fmt.Sprintf("USER%06d@CORP.LOCAL", i),
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 100000+i),
			"enabled":  true,
		}
		switch i {
		case 0:
			props["name"] = "SVC-SQL@CORP.LOCAL"
			props["objectid"] = "S-1-5-21-1-1-1-513"
			props["system_tags"] = "admin_tier_0"
		case 1:
			props["system_tags"] = "admin_tier_0"
		case 2:
			props["system_tags"] = "owned"
		}
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), []snapshot.KindID{piUser}, props})
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestStringAnchorReplacesTheKindScan pins the property index end to end:
// each of these queries anchors on a string predicate whose index resolves
// to a handful of nodes, so the answer must come back under a work budget
// far too small to have enumerated the 5000-node User bitmap. Without the
// index every one of them scanned that bitmap and tested the predicate per
// node -- the shape PostgreSQL answers with a single btree/trigram probe,
// and the last large class where it beat this engine.
func TestStringAnchorReplacesTheKindScan(t *testing.T) {
	const users = 5000
	snap := buildPropAnchorFixture(t, users)
	tight := Budgets{MaxRows: 100, MaxWork: 60}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			name:  "ENDS WITH, the well-known-RID shape",
			query: `MATCH (u:User) WHERE u.objectid ENDS WITH '-513' RETURN u LIMIT 10`,
			want:  1,
		},
		{
			name:  "STARTS WITH, the name-prefix shape",
			query: `MATCH (u:User) WHERE u.name STARTS WITH 'SVC' RETURN u LIMIT 10`,
			want:  1,
		},
		{
			name:  "CONTAINS over a sparse hygiene property",
			query: `MATCH (u:User) WHERE u.system_tags CONTAINS 'admin_tier_0' RETURN u LIMIT 10`,
			want:  2,
		},
		{
			name:  "equality on a non-objectid property",
			query: `MATCH (u:User) WHERE u.name = 'USER000042@CORP.LOCAL' RETURN u LIMIT 10`,
			want:  1,
		},
		{
			name:  "a property no node carries is answered without walking",
			query: `MATCH (u:User) WHERE u.nosuchprop STARTS WITH 'x' RETURN u LIMIT 10`,
			want:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, tight)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}

// TestStringAnchorNeverLosesToItsOwnKindBitmap: an UNSELECTIVE string
// predicate must not hijack the anchor away from a smaller kind bitmap.
// `name CONTAINS 'USER'` resolves to nearly the whole User population, so
// ranking the index as its own better tier would have made this query walk
// 5000 candidates where the Group bitmap offers 1 -- the mis-ranking
// rankOf's same-tier, size-compared treatment exists to prevent.
func TestStringAnchorNeverLosesToItsOwnKindBitmap(t *testing.T) {
	const users = 5000
	snap := buildPropAnchorFixture(t, users)

	env := &Env{Snap: snap}
	q := planQuery(t, snap, `MATCH (u:User) WHERE u.name CONTAINS 'USER' RETURN u LIMIT 5`)
	nc := q.Parts[0].Nodes["u"]
	if !nc.PropIndexed {
		t.Fatal("expected the CONTAINS predicate to resolve through the index")
	}
	if len(nc.PropCandidates) < users-10 {
		t.Fatalf("fixture invariant: CONTAINS 'USER' should match nearly every user, got %d", len(nc.PropCandidates))
	}
	// Same tier as a kind bitmap, and the bitmap here is no larger, so the
	// index must not be preferred on size.
	if propIndexPreferred(env, nc) && len(nc.PropCandidates) > smallestKindBitmap(env, nc.Kinds).Count() {
		t.Fatal("an unselective index result was preferred over a smaller kind bitmap")
	}
}

// TestStringAnchorAnswersMatchTheScan is the correctness pin: for each
// shape, the indexed answer must equal what the same query produces with a
// budget generous enough that the route is irrelevant -- and, critically,
// equal the answer on a snapshot where the predicate is evaluated against
// every node. Row counts alone would not catch an index that returned the
// WRONG handful, so the bound identifiers are compared.
func TestStringAnchorAnswersMatchTheScan(t *testing.T) {
	snap := buildPropAnchorFixture(t, 300)

	ids := func(q string) []uint64 {
		t.Helper()
		rs := mustExec(t, snap, q, generousBudget)
		var out []uint64
		for _, row := range rs.Rows {
			for _, v := range row {
				if v.Kind == OutNode {
					out = append(out, snap.GraphID(v.Node))
				}
			}
		}
		return out
	}

	for _, tc := range []struct{ indexed, scanned string }{
		{
			`MATCH (u:User) WHERE u.objectid ENDS WITH '-513' RETURN u`,
			// Same predicate, but written so no single-symbol conjunct is
			// pushed (the OR keeps it in Part.Where only), forcing the
			// ordinary per-node evaluation.
			`MATCH (u:User) WHERE u.objectid ENDS WITH '-513' OR u.objectid ENDS WITH '-513' RETURN u`,
		},
		{
			`MATCH (u:User) WHERE u.name STARTS WITH 'USER00001' RETURN u`,
			`MATCH (u:User) WHERE u.name STARTS WITH 'USER00001' OR u.name STARTS WITH 'USER00001' RETURN u`,
		},
		{
			`MATCH (u:User) WHERE u.system_tags CONTAINS 'admin_tier_0' RETURN u`,
			`MATCH (u:User) WHERE u.system_tags CONTAINS 'admin_tier_0' OR u.system_tags CONTAINS 'admin_tier_0' RETURN u`,
		},
	} {
		a, b := ids(tc.indexed), ids(tc.scanned)
		if len(a) != len(b) {
			t.Fatalf("indexed route returned %d nodes, scanned route %d\n  %s\n  %s", len(a), len(b), tc.indexed, tc.scanned)
		}
		seen := map[uint64]bool{}
		for _, id := range b {
			seen[id] = true
		}
		for _, id := range a {
			if !seen[id] {
				t.Fatalf("indexed route returned node %d the scanned route did not: %s", id, tc.indexed)
			}
		}
	}
}

// TestStringAnchorInList pins `prop IN [...]` -- a disjunction of
// equalities, indexed as the union of their lookups. BloodHound writes
// domain and tenant filters this way. The indexed answer is cross-checked
// against the same predicate written so it cannot be pushed, so a union
// that dropped an operand would fail here rather than silently lose rows.
func TestStringAnchorInList(t *testing.T) {
	snap := buildPropAnchorFixture(t, 5000)

	const inList = `MATCH (u:User) WHERE u.name IN ['USER000007@CORP.LOCAL','USER000009@CORP.LOCAL','SVC-SQL@CORP.LOCAL'] RETURN u`
	rs := mustExec(t, snap, inList+" LIMIT 10", Budgets{MaxRows: 100, MaxWork: 60})
	if len(rs.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rs.Rows))
	}

	q := planQuery(t, snap, inList)
	if !q.Parts[0].Nodes["u"].PropIndexed {
		t.Fatal("an all-string IN list should resolve through the index")
	}
	if got := len(q.Parts[0].Nodes["u"].PropCandidates); got != 3 {
		t.Fatalf("index resolved %d candidates, want exactly the 3 named", got)
	}

	// Cross-check against the unpushable spelling (the OR keeps the
	// conjunct in Part.Where only, so every node is evaluated).
	const list = `['USER000007@CORP.LOCAL','USER000009@CORP.LOCAL','SVC-SQL@CORP.LOCAL']`
	scanned := mustExec(t, snap,
		`MATCH (u:User) WHERE u.name IN `+list+` OR u.name IN `+list+` RETURN u`,
		generousBudget)
	if len(scanned.Rows) != 3 {
		t.Fatalf("scanned route returned %d rows, want 3", len(scanned.Rows))
	}
}
