// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildHybridJoinFixture: `n` AZUser nodes and `n` User nodes, correlated
// the way a real hybrid tenant is -- every third Entra user carries an
// `onpremid` naming an on-prem user's `objectid`. This is the shape of the
// shipped "On-Prem Users synced to Entra Users" family: two MATCHes with no
// pattern between them, joined only by a property equality in WHERE.
func buildHybridJoinFixture(t *testing.T, n int) *snapshot.View {
	t.Helper()
	const (
		kiUser   snapshot.KindID = 1
		kiAZUser snapshot.KindID = 2
	)
	kinds := map[snapshot.KindID]string{kiUser: "User", kiAZUser: "AZUser"}

	var nodes []execNodeSpec
	for i := 0; i < n; i++ {
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), []snapshot.KindID{kiUser}, map[string]any{
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 100000+i),
			"name":     fmt.Sprintf("USER%06d@CORP.LOCAL", i),
		}})
	}
	for i := 0; i < n; i++ {
		props := map[string]any{
			"objectid": fmt.Sprintf("00000000-0000-0000-0000-%012d", i),
			"name":     fmt.Sprintf("AZUSER%06d@CORP.ONMICROSOFT.COM", i),
		}
		if i%3 == 0 {
			props["onpremid"] = fmt.Sprintf("S-1-5-21-1-1-1-%d", 100000+i)
			props["onpremsyncenabled"] = true
		}
		nodes = append(nodes, execNodeSpec{uint64(500000 + i), []snapshot.KindID{kiAZUser}, props})
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestEquiJoinReplacesTheCartesianProduct is the point of the join: two
// disconnected components correlated by a property equality must be
// answered by hashing one side, not by building the product and filtering
// it. The budget here is far too small to have enumerated n*n pairs, so a
// passing run is proof the product was never built.
//
// On the benchmark graph the same shape is 60k AZBase nodes against 920k
// Base nodes -- 55 billion pairs -- which is why this query delegated to
// PostgreSQL before this existed rather than merely being slow.
func TestEquiJoinReplacesTheCartesianProduct(t *testing.T) {
	const n = 600
	snap := buildHybridJoinFixture(t, n)

	// n*n = 360,000 pairs; this budget buys ~2n row observations plus the
	// matches, and nothing remotely near the product.
	tight := Budgets{MaxRows: 10000, MaxWork: 8000}

	want := 0
	for i := 0; i < n; i++ {
		if i%3 == 0 {
			want++
		}
	}

	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			name:  "entra side first",
			query: `MATCH (e:AZUser) MATCH (a:User) WHERE e.onpremid = a.objectid RETURN e`,
		},
		{
			name:  "on-prem side first",
			query: `MATCH (a:User) MATCH (e:AZUser) WHERE e.onpremid = a.objectid RETURN e`,
		},
		{
			name:  "operands written in the other order",
			query: `MATCH (e:AZUser) MATCH (a:User) WHERE a.objectid = e.onpremid RETURN e`,
		},
		{
			name:  "alongside a single-symbol conjunct",
			query: `MATCH (e:AZUser) MATCH (a:User) WHERE e.onpremsyncenabled = true AND e.onpremid = a.objectid RETURN e`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, tight)
			if len(rs.Rows) != want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), want)
			}
		})
	}
}

// TestEquiJoinMatchesTheFilterItReplaces pins the join's agreement with
// PropEq, the predicate the WHERE that follows it applies. Every row the
// join drops must be a row PropEq would have rejected -- a join that is
// merely "selective" is a wrong answer, not an optimization.
//
// The two cases that actually differ are absence and present JSON null:
// pg's jsonb `=` makes `'null'::jsonb = 'null'::jsonb` TRUE, so two present
// nulls join, while an absent property is PropEq's only source of NULL and
// never joins -- including against another absent property.
func TestEquiJoinMatchesTheFilterItReplaces(t *testing.T) {
	const (
		kiLeft  snapshot.KindID = 1
		kiRight snapshot.KindID = 2
	)
	kinds := map[snapshot.KindID]string{kiLeft: "L", kiRight: "R"}

	nodes := []execNodeSpec{
		// Left side: one of each interesting value shape.
		{1, []snapshot.KindID{kiLeft}, map[string]any{"tag": "match-me"}},
		{2, []snapshot.KindID{kiLeft}, map[string]any{"tag": nil}},
		{3, []snapshot.KindID{kiLeft}, map[string]any{"other": "x"}}, // tag absent
		{4, []snapshot.KindID{kiLeft}, map[string]any{"tag": 7.0}},
		{5, []snapshot.KindID{kiLeft}, map[string]any{"tag": true}},

		// Right side: the same shapes, to be paired against them.
		{101, []snapshot.KindID{kiRight}, map[string]any{"tag": "match-me"}},
		{102, []snapshot.KindID{kiRight}, map[string]any{"tag": nil}},
		{103, []snapshot.KindID{kiRight}, map[string]any{"other": "x"}}, // tag absent
		{104, []snapshot.KindID{kiRight}, map[string]any{"tag": 7.0}},
		{105, []snapshot.KindID{kiRight}, map[string]any{"tag": true}},
	}
	snap := buildExecSnapshot(t, kinds, nodes, nil)

	// 5x5 = 25 pairs, small enough that the product path is affordable --
	// so this compares the join's answer against the answer the filter
	// alone produces, rather than against a hand-written expectation.
	loose := Budgets{MaxRows: 1000, MaxWork: 100000}

	joined := mustExec(t, snap, `MATCH (l:L) MATCH (r:R) WHERE l.tag = r.tag RETURN l, r`, loose)

	// The identical predicate, reached without the join. Written twice under
	// an OR purely to defeat the recognizer: a Disjunction is a single
	// conjunct, so no join key is found and the product is built and
	// filtered instead -- while `X OR X` is X under three-valued logic, so
	// the two queries must agree row for row.
	product := mustExec(t, snap, `MATCH (l:L) MATCH (r:R) WHERE l.tag = r.tag OR l.tag = r.tag RETURN l, r`, loose)

	// string, number, bool, AND present-null-to-present-null. Absent never
	// pairs, not even with another absent.
	if len(joined.Rows) != 4 {
		t.Fatalf("joined %d pairs, want 4 (string, number, bool, null-to-null)", len(joined.Rows))
	}
	if len(product.Rows) != len(joined.Rows) {
		t.Fatalf("the join answered %d pairs but the same predicate over the product answered %d",
			len(joined.Rows), len(product.Rows))
	}
}

// TestEquiJoinPresentNullJoins isolates the jsonb-null rule, because it is
// the one place where "skip nulls" -- the reflex a SQL equi-join is written
// with, and what this code did before it was checked against PropEq -- is
// wrong for this engine. Two nodes carrying `tag: null` are a match under
// pg's jsonb `=`, and dropping them would silently lose rows.
func TestEquiJoinPresentNullJoins(t *testing.T) {
	const (
		kiLeft  snapshot.KindID = 1
		kiRight snapshot.KindID = 2
	)
	kinds := map[snapshot.KindID]string{kiLeft: "L", kiRight: "R"}
	snap := buildExecSnapshot(t, kinds, []execNodeSpec{
		{1, []snapshot.KindID{kiLeft}, map[string]any{"tag": nil}},
		{2, []snapshot.KindID{kiLeft}, map[string]any{"other": "x"}},
		{101, []snapshot.KindID{kiRight}, map[string]any{"tag": nil}},
		{102, []snapshot.KindID{kiRight}, map[string]any{"other": "x"}},
	}, nil)

	rs := mustExec(t, snap, `MATCH (l:L) MATCH (r:R) WHERE l.tag = r.tag RETURN l, r`,
		Budgets{MaxRows: 100, MaxWork: 10000})
	if len(rs.Rows) != 1 {
		t.Fatalf("got %d pairs, want 1: present null joins present null, absent joins nothing", len(rs.Rows))
	}
}

// TestEquiJoinOnlyRecognizesSafeShapes pins what the recognizer refuses.
// Every refusal here is a fall back to the product -- still correct, and
// still budget-refused when it is too big -- so the risk being guarded is a
// WRONG join, not a slow one.
func TestEquiJoinOnlyRecognizesSafeShapes(t *testing.T) {
	const n = 40
	snap := buildHybridJoinFixture(t, n)
	loose := Budgets{MaxRows: 100000, MaxWork: 10000000}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			// Under OR, the equality is not a conjunct that every result row
			// satisfies; hashing on it would drop the rows the other
			// disjunct admits.
			name:  "an equality under OR is not a join key",
			query: `MATCH (e:AZUser) MATCH (a:User) WHERE e.onpremid = a.objectid OR a.objectid ENDS WITH '-100001' RETURN e, a`,
			want:  (n+2)/3 + n,
		},
		{
			// Negated: the rows wanted are exactly the ones a join drops.
			name:  "an inequality is not a join key",
			query: `MATCH (e:AZUser) MATCH (a:User) WHERE e.name = 'AZUSER000000@CORP.ONMICROSOFT.COM' AND e.onpremid <> a.objectid RETURN e, a`,
			want:  n - 1,
		},
		{
			// Both sides in the same component: not a cross-component key,
			// and already handled by the ordinary filter.
			name:  "a same-symbol equality is not a join key",
			query: `MATCH (e:AZUser) MATCH (a:User) WHERE e.onpremid = e.objectid RETURN e, a`,
			want:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, loose)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}

// TestEquiJoinServesTheShippedSyncPrebuilts runs the two prebuilt queries
// this join exists for, verbatim as BloodHound ships them (testdata/prebuilt
// agt.json and agi.json, "Tier Zero AD principals synchronized with Entra
// ID"). They are the only two queries in the shipped corpus whose components
// are correlated by nothing but a property equality, and before this join
// both delegated: their product on the benchmark graph is ~55 billion pairs.
func TestEquiJoinServesTheShippedSyncPrebuilts(t *testing.T) {
	const (
		kiBase    snapshot.KindID = 1
		kiAZBase  snapshot.KindID = 2
		kiTierZro snapshot.KindID = 3
	)
	kinds := map[snapshot.KindID]string{kiBase: "Base", kiAZBase: "AZBase", kiTierZro: "Tag_Tier_Zero"}

	const n = 400
	var nodes []execNodeSpec
	for i := 0; i < n; i++ {
		ks := []snapshot.KindID{kiBase}
		props := map[string]any{"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 100000+i)}
		// Every fifth on-prem principal is Tier Zero, by both the label and
		// the system_tags spelling the two prebuilts use interchangeably.
		if i%5 == 0 {
			ks = append(ks, kiTierZro)
			props["system_tags"] = "admin_tier_0"
		}
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), ks, props})
	}
	for i := 0; i < n; i++ {
		props := map[string]any{"objectid": fmt.Sprintf("00000000-0000-0000-0000-%012d", i)}
		if i%3 == 0 {
			props["onpremid"] = fmt.Sprintf("S-1-5-21-1-1-1-%d", 100000+i)
			props["onpremsyncenabled"] = true
		}
		nodes = append(nodes, execNodeSpec{uint64(500000 + i), []snapshot.KindID{kiAZBase}, props})
	}
	snap := buildExecSnapshot(t, kinds, nodes, nil)

	// Synced AND Tier Zero: i divisible by 3 (carries onpremid) and by 5
	// (its on-prem twin is Tier Zero), i.e. by 15.
	want := 0
	for i := 0; i < n; i++ {
		if i%3 == 0 && i%5 == 0 {
			want++
		}
	}

	// n*n = 160,000 pairs. This budget cannot buy the product.
	tight := Budgets{MaxRows: 10000, MaxWork: 6000}

	for _, tc := range []struct {
		name  string
		query string
	}{
		{
			name: "agt.json, Tier Zero by label",
			query: `MATCH (ENTRA:AZBase)
MATCH (AD:Base)
WHERE (AD:Tag_Tier_Zero)
AND ENTRA.onpremsyncenabled = true
AND ENTRA.onpremid = AD.objectid
RETURN ENTRA
LIMIT 100`,
		},
		{
			name: "agi.json, Tier Zero by system_tags",
			query: `MATCH (ENTRA:AZBase)
MATCH (AD:Base)
WHERE ENTRA.onpremsyncenabled = true
AND ENTRA.onpremid = AD.objectid
AND COALESCE(AD.system_tags, '') CONTAINS 'admin_tier_0'
RETURN ENTRA
LIMIT 100`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, ok := planNoFail(t, snap, tc.query)
			if !ok || q == nil {
				t.Fatal("the planner declined a query the join exists to serve")
			}
			rs := mustExec(t, snap, tc.query, tight)
			if len(rs.Rows) != want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), want)
			}
		})
	}
}
