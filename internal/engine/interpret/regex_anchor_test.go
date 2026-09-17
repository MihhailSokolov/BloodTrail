// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

const (
	raKindComputer snapshot.KindID = 1
	raKindUser     snapshot.KindID = 2
)

// buildRegexAnchorFixture mirrors the shape that decides this optimization:
// a large population of Computers sharing a HANDFUL of distinct
// `operatingsystem` strings, and the same population carrying a `name` that
// is distinct per node.
//
// Those two properties are what separate the two routes. A regex on the
// low-cardinality one can be answered by testing each distinct value once; a
// regex on the per-node one cannot, and must stay on the scan.
func buildRegexAnchorFixture(t *testing.T, computers int) *snapshot.View {
	t.Helper()
	kinds := map[snapshot.KindID]string{raKindComputer: "Computer", raKindUser: "User"}
	oses := []string{
		"Windows Server 2019 Datacenter",
		"Windows 10 Enterprise",
		"Windows Server 2003 Standard",
		"Windows XP Professional",
		"Windows Server 2016 Standard",
	}
	nodes := make([]execNodeSpec, 0, computers)
	for i := 0; i < computers; i++ {
		nodes = append(nodes, execNodeSpec{uint64(1000 + i), []snapshot.KindID{raKindComputer}, map[string]any{
			"operatingsystem": oses[i%len(oses)],
			"name":            fmt.Sprintf("HOST%05d.CORP.LOCAL", i),
		}})
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestRegexAnchorEvaluatesOncePerDistinctValue pins the mechanism that lets
// this engine beat a sequential scan on the one predicate neither side has an
// index for: the pattern is tested against each distinct VALUE, not against
// each node.
//
// Asserted by counting invocations rather than through a work budget. The
// work meter charges per candidate visited and knows nothing about what a
// predicate costs to evaluate, so both routes spend about the same by its
// reckoning -- which is precisely why the saving is invisible to it and has
// to be stated directly.
func TestRegexAnchorEvaluatesOncePerDistinctValue(t *testing.T) {
	const computers = 5000
	snap := buildRegexAnchorFixture(t, computers)

	calls := 0
	seen := map[string]int{}
	ids, ok := snap.NodesMatchingString("operatingsystem", func(s string) bool {
		calls++
		seen[s]++
		return strings.Contains(s, "XP") || strings.Contains(s, "2003")
	})
	if !ok {
		t.Fatal("NodesMatchingString declined a property the base interned")
	}

	// Five distinct operating systems across five thousand computers.
	if calls != 5 {
		t.Fatalf("pattern evaluated %d times, want 5 -- once per distinct value", calls)
	}
	for value, n := range seen {
		if n != 1 {
			t.Fatalf("value %q evaluated %d times, want exactly 1", value, n)
		}
	}
	if want := computers / 5 * 2; len(ids) != want {
		t.Fatalf("got %d nodes, want %d", len(ids), want)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			t.Fatalf("candidates are not a strictly ascending set at %d", i)
		}
	}
}

// TestRegexAnchorMatchesTheScan is the correctness half: for a range of
// patterns, resolving per distinct value must select exactly the rows a
// per-node scan would have.
func TestRegexAnchorMatchesTheScan(t *testing.T) {
	const computers = 500
	snap := buildRegexAnchorFixture(t, computers)
	loose := Budgets{MaxRows: 100000, MaxWork: 100000000, MaxLiveRows: 100000}

	for _, tc := range []struct {
		name    string
		pattern string
		want    int
	}{
		{"unsupported-OS alternation", `(?i).*Windows.* (2000|2003|2008|2012|xp|vista|7|8|me|nt).*`, computers / 5 * 2},
		{"matches one value", `.*XP Professional.*`, computers / 5},
		{"matches nothing", `.*Solaris.*`, 0},
		{"matches every value", `(?i).*windows.*`, computers},
		{"anchored alternation", `^Windows (10|XP) .*`, computers / 5 * 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap,
				`MATCH (c:Computer) WHERE c.operatingsystem =~ '`+tc.pattern+`' RETURN c`, loose)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}

// TestRegexAnchorRefusesAPerNodeProperty pins the refusal. `name` takes one
// value per node, so testing each distinct value is the same number of regex
// evaluations as scanning, plus a union of one-element posting lists on top
// -- a loss, not a wash. The margin in extractRegexAnchor exists for this.
func TestRegexAnchorRefusesAPerNodeProperty(t *testing.T) {
	const computers = 400
	snap := buildRegexAnchorFixture(t, computers)

	q := planQuery(t, snap, `MATCH (c:Computer) WHERE c.name =~ '(?i).*HOST001.*' RETURN c`)
	nc := q.Parts[0].Nodes["c"]
	if nc.PropIndexed {
		t.Fatalf("a per-node property was resolved into %d candidates; it must stay on the scan",
			len(nc.PropCandidates))
	}

	// And the answer is still right on that route.
	loose := Budgets{MaxRows: 100000, MaxWork: 100000000, MaxLiveRows: 100000}
	rs := mustExec(t, snap, `MATCH (c:Computer) WHERE c.name =~ '.*HOST0001[0-9]\\.CORP.*' RETURN c`, loose)
	if len(rs.Rows) != 10 {
		t.Fatalf("got %d rows, want 10", len(rs.Rows))
	}
}

// TestRegexAnchorLowCardinalityIsResolved is the positive half of the same
// decision, asserted on the plan rather than through a budget.
func TestRegexAnchorLowCardinalityIsResolved(t *testing.T) {
	snap := buildRegexAnchorFixture(t, 400)
	q := planQuery(t, snap, `MATCH (c:Computer) WHERE c.operatingsystem =~ '.*XP.*' RETURN c`)
	nc := q.Parts[0].Nodes["c"]
	if !nc.PropIndexed {
		t.Fatal("a five-value property was not resolved per distinct value")
	}
	if len(nc.PropCandidates) != 80 {
		t.Fatalf("resolved %d candidates, want the 80 nodes carrying the matching value",
			len(nc.PropCandidates))
	}
}

// buildMixedRegexFixture gives `operatingsystem` a NUMERIC value on some
// nodes and a string on the rest, and leaves it absent from a third group --
// the two shapes a per-distinct-value index cannot represent.
func buildMixedRegexFixture(t *testing.T, each int) *snapshot.View {
	t.Helper()
	kinds := map[snapshot.KindID]string{raKindComputer: "Computer"}
	var nodes []execNodeSpec
	id := uint64(1000)
	for i := 0; i < each; i++ {
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{raKindComputer},
			map[string]any{"operatingsystem": "Windows XP Professional"}})
		id++
	}
	for i := 0; i < each; i++ { // numeric value for the same property
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{raKindComputer},
			map[string]any{"operatingsystem": 6.1}})
		id++
	}
	for i := 0; i < each; i++ { // property absent entirely
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{raKindComputer},
			map[string]any{"name": "NOPROP"}})
		id++
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestRegexAnchorRefusesMixedTypes pins the refusal that keeps the index from
// converting a DECLINE into a short answer.
//
// interpret evaluates a string predicate against a non-string value as a
// runtime cast error, which declines the query so PostgreSQL -- whose `->>`
// stringifies the value -- answers it. An index built only from the string
// values would skip those nodes, the error would never fire, and the engine
// would serve a result set missing whatever PostgreSQL would have returned.
func TestRegexAnchorRefusesMixedTypes(t *testing.T) {
	snap := buildMixedRegexFixture(t, 50)

	if _, ok := snap.NodesMatchingString("operatingsystem", func(string) bool { return true }); ok {
		t.Fatal("NodesMatchingString answered for a property with non-string values")
	}
	q := planQuery(t, snap, `MATCH (c:Computer) WHERE c.operatingsystem =~ '.*XP.*' RETURN c`)
	if nc := q.Parts[0].Nodes["c"]; nc.PropIndexed {
		t.Fatalf("a mixed-type property was resolved into %d candidates", len(nc.PropCandidates))
	}
}

// buildPartialPropFixture gives `os` a low-cardinality STRING value on some
// computers and leaves it absent from the rest -- the shape that makes a
// COALESCE default observable. The property is interned and cheap to index,
// so the regex anchor genuinely fires here.
func buildPartialPropFixture(t *testing.T, withProp, without int) *snapshot.View {
	t.Helper()
	kinds := map[snapshot.KindID]string{raKindComputer: "Computer"}
	oses := []string{"Windows 10", "Windows XP", "Windows 7"}
	var nodes []execNodeSpec
	id := uint64(1000)
	for i := 0; i < withProp; i++ {
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{raKindComputer},
			map[string]any{"os": oses[i%len(oses)], "name": fmt.Sprintf("H%d", i)}})
		id++
	}
	for i := 0; i < without; i++ {
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{raKindComputer},
			map[string]any{"name": fmt.Sprintf("N%d", i)}})
		id++
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestRegexAnchorCoalesceDefaultThatMatches pins the other way the anchor can
// go short. Under COALESCE a node WITHOUT the property is tested against the
// DEFAULT rather than skipped, so the candidate set -- which holds only nodes
// that carry the property -- is a valid superset only while the pattern
// REJECTS that default. A pattern like `.*` accepts it, making every node
// without the property a real match the anchor would hide.
func TestRegexAnchorCoalesceDefaultThatMatches(t *testing.T) {
	const withProp, without = 300, 120
	snap := buildPartialPropFixture(t, withProp, without)
	loose := Budgets{MaxRows: 100000, MaxWork: 100000000, MaxLiveRows: 100000}

	// The anchor must fire for a pattern that rejects the default, or this
	// test is not exercising the guard at all.
	q := planQuery(t, snap, `MATCH (c:Computer) WHERE COALESCE(c.os, '') =~ '.*XP.*' RETURN c`)
	if nc := q.Parts[0].Nodes["c"]; !nc.PropIndexed {
		t.Fatal("the anchor did not fire on a low-cardinality COALESCE predicate; the guard below is untested")
	}

	for _, tc := range []struct {
		name    string
		pattern string
		want    int
	}{
		{"matches the default, so every property-less node matches too", `.*`, withProp + without},
		{"empty-anchored, matched only by the default", `^$`, without},
		{"rejects the default", `.*XP.*`, withProp / 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap,
				`MATCH (c:Computer) WHERE COALESCE(c.os, '') =~ '`+tc.pattern+`' RETURN c`, loose)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}
