// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildValueAnchorFixture: many Base nodes, of which a handful carry a rare
// list element and a rare boolean -- the shape of the shipped "Principals
// with weak supported Kerberos encryption types" prebuilt, where a few
// hundred principals out of ~920,000 carry a weak cipher.
func buildValueAnchorFixture(t *testing.T, n int) *snapshot.View {
	t.Helper()
	const kiBase snapshot.KindID = 1
	var nodes []execNodeSpec
	for i := 0; i < n; i++ {
		etypes := []any{"AES256"}
		switch i % 500 {
		case 0:
			etypes = []any{"RC4-HMAC-MD5", "AES256"}
		case 1:
			etypes = []any{"DES-CBC-CRC"}
		}
		nodes = append(nodes, execNodeSpec{uint64(i + 1), []snapshot.KindID{kiBase}, map[string]any{
			"etypes":        etypes,
			"usedeskeyonly": i%700 == 5,
			"enabled":       true,
			"idx":           float64(i),
			"name":          fmt.Sprintf("N%06d", i),
		}})
	}
	return buildExecSnapshot(t, map[snapshot.KindID]string{kiBase: "Base"}, nodes, nil)
}

// TestValueAnchorReplacesTheKindScan pins the access path PostgreSQL has no
// equivalent of. BloodHound's schema carries NO index on `properties`, so a
// list membership or a boolean equality is a sequential scan for pg -- and it
// was a scan here too, which is how this engine came to lose those queries on
// per-row throughput alone.
//
// Each budget below is far too small to have walked the Base bitmap, so a
// passing run is proof the candidate source was the index.
func TestValueAnchorReplacesTheKindScan(t *testing.T) {
	const n = 4000
	snap := buildValueAnchorFixture(t, n)
	tight := Budgets{MaxRows: 200, MaxWork: 300, MaxLiveRows: 1000}

	rare := 0
	des := 0
	deskey := 0
	for i := 0; i < n; i++ {
		if i%500 == 0 {
			rare++
		}
		if i%500 == 1 {
			des++
		}
		if i%700 == 5 {
			deskey++
		}
	}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			name:  "list membership",
			query: `MATCH (u:Base) WHERE 'RC4-HMAC-MD5' IN u.etypes RETURN u`,
			want:  rare,
		},
		{
			name:  "boolean equality",
			query: `MATCH (u:Base) WHERE u.usedeskeyonly = true RETURN u`,
			want:  deskey,
		},
		{
			// The shipped shape: a disjunction of list memberships, whose
			// union bounds the OR exactly because every arm is anchorable.
			name:  "a disjunction of list memberships",
			query: `MATCH (u:Base) WHERE 'RC4-HMAC-MD5' IN u.etypes OR 'DES-CBC-CRC' IN u.etypes RETURN u`,
			want:  rare + des,
		},
		{
			// A selective conjunct alongside an unselective one: the anchor
			// must be the cheap one, not whichever came first.
			name:  "the cheapest conjunct wins",
			query: `MATCH (u:Base) WHERE u.enabled = true AND u.usedeskeyonly = true RETURN u`,
			want:  deskey,
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

// TestValueAnchorKeepsTheSameAnswer is the correctness half: the index may
// only change how many candidates are inspected, never which rows come out,
// and it must refuse every shape where its postings would not be a superset.
func TestValueAnchorKeepsTheSameAnswer(t *testing.T) {
	const n = 60
	snap := buildValueAnchorFixture(t, n)
	loose := Budgets{MaxRows: 100000, MaxWork: 10000000, MaxLiveRows: 100000}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{
			// Unselective: matches everything, so the anchor must NOT be
			// adopted -- but the answer is the same either way.
			name:  "an unselective equality still answers",
			query: `MATCH (u:Base) WHERE u.enabled = true RETURN u`,
			want:  n,
		},
		{
			// One arm of the OR is not anchorable, so the union cannot bound
			// it and the whole disjunction must fall back to a scan.
			name:  "a disjunction with an unanchorable arm still answers",
			query: `MATCH (u:Base) WHERE 'RC4-HMAC-MD5' IN u.etypes OR u.idx > 10 RETURN u`,
			want:  n - 11 + 1,
		},
		{
			// Exactly one node in this fixture carries usedeskeyonly=true.
			name:  "negation still answers",
			query: `MATCH (u:Base) WHERE NOT u.usedeskeyonly = true RETURN u`,
			want:  n - 1,
		},
		{
			// A list property is never exactly equal to one of its elements.
			name:  "equality against a list value matches nothing",
			query: `MATCH (u:Base) WHERE u.etypes = 'AES256' RETURN u`,
			want:  0,
		},
		{
			name:  "a value nothing carries matches nothing",
			query: `MATCH (u:Base) WHERE 'NO-SUCH' IN u.etypes RETURN u`,
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

// TestValueAnchorRefusesMarginalPostings pins valueAnchorMargin, which is the
// difference between an index that pays and one that costs.
//
// `enabled = true` matches almost every node. Adopting it is strictly
// "smaller" than the kind bitmap and almost entirely useless: the engine
// materializes and delta-unions a posting list nearly the size of the bitmap
// to avoid visiting the handful of nodes it excludes. On the benchmark graph
// that is a 900,000-id list to skip 20,000 candidates, and it took two
// shipped prebuilts from 0.85x of PostgreSQL to 2.8x.
func TestValueAnchorRefusesMarginalPostings(t *testing.T) {
	const n = 400
	snap := buildValueAnchorFixture(t, n)

	marginal := planQuery(t, snap, `MATCH (u:Base) WHERE u.enabled = true RETURN u`)
	if nc := marginal.Parts[0].Nodes["u"]; nc.PropIndexed {
		t.Fatalf("a predicate matching %d of %d nodes must not become the candidate source "+
			"(resolved %d)", n, n, len(nc.PropCandidates))
	}

	// The same property shape, but genuinely selective, still anchors.
	selective := planQuery(t, snap, `MATCH (u:Base) WHERE u.usedeskeyonly = true RETURN u`)
	nc := selective.Parts[0].Nodes["u"]
	if !nc.PropIndexed {
		t.Fatal("a selective equality must still anchor")
	}
	if len(nc.PropCandidates)*valueAnchorMargin > n {
		t.Fatalf("anchored on %d candidates against %d nodes, which does not clear the margin",
			len(nc.PropCandidates), n)
	}
}
