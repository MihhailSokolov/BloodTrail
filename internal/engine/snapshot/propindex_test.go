// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"
)

// buildPropIndexFixture: nodes 1..n carry `name` and `objectid`; only the
// first three carry `system_tags`, mirroring the sparsity that makes the
// index worth having on real BloodHound data (hygiene properties exist on a
// handful of nodes out of a million).
func buildPropIndexFixture(t *testing.T, n int) *Snapshot {
	t.Helper()
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "User"})
	for i := 0; i < n; i++ {
		props := map[string]any{
			"name":     fmt.Sprintf("USER%04d@CORP.LOCAL", i),
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 1000+i),
		}
		switch i {
		case 0:
			props["system_tags"] = "admin_tier_0 owned"
		case 1:
			props["system_tags"] = "admin_tier_0"
		case 2:
			props["system_tags"] = "owned"
		}
		raw, err := json.Marshal(props)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := b.AddNode(uint64(i+1), []KindID{1}, raw); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

func graphIDs(t *testing.T, v *View, ids []NodeID) []uint64 {
	t.Helper()
	out := make([]uint64, 0, len(ids))
	for _, id := range ids {
		out = append(out, v.GraphID(id))
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

func TestNodesWithStringMatches(t *testing.T) {
	v := NewView(buildPropIndexFixture(t, 50))
	name, ok := v.PropIDByName("name")
	if !ok {
		t.Fatal("name property not interned")
	}
	objID, _ := v.PropIDByName("objectid")
	tags, _ := v.PropIDByName("system_tags")

	for _, tc := range []struct {
		name    string
		prop    PropID
		match   StringMatch
		operand string
		want    []uint64
	}{
		{"exact equality", name, StringEquals, "USER0007@CORP.LOCAL", []uint64{8}},
		{"equality miss", name, StringEquals, "NOBODY", nil},
		{"prefix", name, StringPrefix, "USER004", []uint64{41, 42, 43, 44, 45, 46, 47, 48, 49, 50}},
		{"prefix miss", name, StringPrefix, "ZZZ", nil},
		{"suffix on objectid, the RID shape", objID, StringSuffix, "-1049", []uint64{50}},
		{"suffix matching several", objID, StringSuffix, "9", []uint64{10, 20, 30, 40, 50}},
		{"suffix miss", objID, StringSuffix, "-9999", nil},
		{"contains over a sparse property", tags, StringContains, "admin_tier_0", []uint64{1, 2}},
		{"contains matching one", tags, StringContains, "owned", []uint64{1, 3}},
		{"contains miss", tags, StringContains, "nonexistent", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ids, ok := v.NodesWithString(tc.prop, tc.match, tc.operand)
			if !ok {
				t.Fatal("index reported unavailable")
			}
			got := graphIDs(t, v, ids)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestNodesWithStringSparsePopulation pins the property that makes the
// index worth building at all: it holds only nodes CARRYING the property,
// so a hygiene property present on 3 of 5000 nodes costs three comparisons
// to scan, not five thousand.
func TestNodesWithStringSparsePopulation(t *testing.T) {
	s := buildPropIndexFixture(t, 5000)
	v := NewView(s)
	tags, _ := v.PropIDByName("system_tags")
	if n, ok := v.PropCount(tags); !ok || n != 3 {
		t.Fatalf("system_tags population = %d (ok=%v), want 3", n, ok)
	}
	if n, _ := v.PropCount(mustProp(t, v, "name")); n != 5000 {
		t.Fatalf("name population = %d, want 5000", n)
	}
}

func mustProp(t *testing.T, v *View, name string) PropID {
	t.Helper()
	id, ok := v.PropIDByName(name)
	if !ok {
		t.Fatalf("property %q not interned", name)
	}
	return id
}

// TestNodesWithStringAbsentProperty: a property no node carries is answered
// empty-and-available, not unavailable -- the single most valuable answer
// the index gives, since the alternative is scanning the graph to learn it.
func TestNodesWithStringAbsentProperty(t *testing.T) {
	v := NewView(buildPropIndexFixture(t, 20))
	id, ok := v.PropIDByName("nosuchproperty")
	if ok {
		t.Fatalf("fixture unexpectedly interns nosuchproperty as %d", id)
	}
	// A property that IS interned but that no node carries as a string: the
	// index population is empty and every match shape returns nothing.
	b := NewBuilder(1)
	if err := b.AddNode(1, nil, []byte(`{"n":5}`)); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	nv := NewView(s)
	numeric := mustProp(t, nv, "n")
	for _, m := range []StringMatch{StringEquals, StringPrefix, StringSuffix, StringContains} {
		ids, ok := nv.NodesWithString(numeric, m, "5")
		if !ok {
			t.Fatalf("match %d: index unavailable", m)
		}
		if len(ids) != 0 {
			t.Fatalf("match %d: got %d ids, want 0 (the property is numeric, never string)", m, len(ids))
		}
	}
}

// TestNodesWithStringOverlayCandidates pins the delta contract, which is a
// SUPERSET one -- never short, allowed to be loose -- and pins where the
// looseness is allowed to be.
//
// The delta half used to be "every node any segment wrote a record for",
// whatever it wrote. That is a superset, but a needlessly enormous one: a
// view carrying 260,000 written nodes made 260,000 candidates out of a
// question whose answer is one row, and the caller verified every one. The
// delta now contributes only nodes whose WRITTEN value actually matches.
//
// Base postings are still returned whole, and that is the remaining
// looseness: a segment may have moved a node's value out of the match, and
// the base index cannot know, so it stays a candidate and is re-verified.
func TestNodesWithStringOverlayCandidates(t *testing.T) {
	base := buildPropIndexFixture(t, 10)
	v := NewView(base)

	// Database ids are i+1, so graph id 4 is the node named USER0003.
	sb := &SegmentBuilder{}
	for _, n := range []struct {
		id    uint64
		name  string
		props string
	}{
		{4, "leaves the prefix", `{"name":"RENAMED@CORP.LOCAL"}`},
		{6, "enters the prefix", `{"name":"USER0003-COPY@CORP.LOCAL"}`},
		{8, "touched, matches neither", `{"name":"UNRELATED@CORP.LOCAL"}`},
		{99, "delta-added, matches", `{"name":"USER0003@CORP.LOCAL"}`},
	} {
		if err := sb.AddNodeState(n.id, []KindID{1}, []byte(n.props)); err != nil {
			t.Fatalf("AddNodeState(%d, %s): %v", n.id, n.name, err)
		}
	}
	ov := v.WithSegment(sb.Build())

	ids, ok := ov.NodesWithStringByName("name", StringPrefix, "USER0003")
	if !ok {
		t.Fatal("index unavailable on the overlay view")
	}
	got := map[uint64]bool{}
	for _, id := range ids {
		if got[ov.GraphID(id)] {
			t.Fatalf("node %d yielded twice; a candidate source must be a set", ov.GraphID(id))
		}
		got[ov.GraphID(id)] = true
	}

	// Never short: both real matches must be here.
	if !got[99] {
		t.Fatalf("delta-ADDED node matching the prefix missing from %v", got)
	}
	if !got[6] {
		t.Fatalf("node the delta renamed INTO the prefix missing from %v", got)
	}
	// Allowed looseness: renamed out, but the base index said it matched.
	if !got[4] {
		t.Fatalf("node the delta renamed OUT of the prefix missing from %v -- base"+
			" postings must still be offered for re-verification", got)
	}
	// The tightening: a node the delta touched whose value matches neither
	// the base nor the delta side is not a candidate at all.
	if got[8] {
		t.Fatalf("node %d was written by a segment but matches nothing; it must not"+
			" be a candidate just for having been touched (%v)", 8, got)
	}
}

// TestNodesWithStringOverlayNoDuplicates pins that the overlay union never
// yields the same node twice. A candidate source that repeats a node makes
// the interpreter admit it twice and emit a duplicate result row -- which
// is exactly what a write-through query hit before unionDeltaTouched
// existed: a modified node that ALSO matched the base index was returned
// twice where PostgreSQL returned it once.
func TestNodesWithStringOverlayNoDuplicates(t *testing.T) {
	base := buildPropIndexFixture(t, 10)
	v := NewView(base)
	objID := mustProp(t, v, "objectid")

	// Touch node 4 without changing the property the index is keyed on --
	// the write-through shape: it stays a base-index match AND becomes
	// segment-touched.
	sb := &SegmentBuilder{}
	if err := sb.AddNodeState(4, []KindID{1}, []byte(`{"objectid":"S-1-5-21-1-1-1-1003","touched":"yes"}`)); err != nil {
		t.Fatalf("AddNodeState: %v", err)
	}
	ov := v.WithSegment(sb.Build())

	ids, ok := ov.NodesWithString(objID, StringSuffix, "-1003")
	if !ok {
		t.Fatal("index unavailable on the overlay view")
	}
	seen := map[NodeID]int{}
	for _, id := range ids {
		seen[id]++
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("node %d appears %d times in the candidate set; a candidate source must never repeat a node", id, n)
		}
	}
}

// TestNodesWithStringByNameSeesPastAnUninterestedBase pins the rule that a
// base snapshot's silence about a property name is not evidence about the
// delta. A PropID resolves against the base's intern table only, so on a
// View whose base predates the property entirely -- the state every fresh
// install boots into, where PostgreSQL holds nothing and the whole graph
// arrives by write-through -- a name lookup misses while the delta is full
// of nodes carrying it.
//
// Treating that miss as "nothing carries this property" returned an empty
// candidate set, and an empty candidate set is a wrong answer rather than a
// slow one: it made the engine SERVE zero rows for a query whose match was
// sitting in the delta.
func TestNodesWithStringByNameSeesPastAnUninterestedBase(t *testing.T) {
	// A base that interns "other" and nothing else -- in particular, never
	// the name the queries below look up.
	base := NewBuilder(1)
	base.SetKinds(map[KindID]string{1: "Thing"})
	if err := base.AddNode(1, []KindID{1}, []byte(`{"other":"irrelevant"}`)); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	snap, err := base.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if _, interned := NewView(snap).PropIDByName("objectid"); interned {
		t.Fatal("fixture invalid: the base must not intern objectid")
	}

	t.Run("no delta means the base really does settle it", func(t *testing.T) {
		ids, ok := NewView(snap).NodesWithStringByName("objectid", StringSuffix, "-512")
		if !ok {
			t.Fatal("a flat snapshot can always answer")
		}
		if len(ids) != 0 {
			t.Fatalf("got %d candidates, want 0: with no delta, a property the base "+
				"never interned is carried by nothing", len(ids))
		}
	})

	t.Run("a delta carrying the property must not be missed", func(t *testing.T) {
		var sb SegmentBuilder
		sb.AddKind(1, "Thing")
		for _, n := range []struct {
			id   uint64
			json string
		}{
			{100, `{"objectid":"S-1-5-21-1-1-1-512"}`},
			{101, `{"objectid":"S-1-5-21-1-1-1-1105"}`},
		} {
			if err := sb.AddNodeState(n.id, []KindID{1}, []byte(n.json)); err != nil {
				t.Fatalf("AddNodeState(%d): %v", n.id, err)
			}
		}
		view := NewView(snap).WithSegment(sb.Build())

		ids, ok := view.NodesWithStringByName("objectid", StringSuffix, "-512")
		if !ok {
			t.Fatal("an overlay can always answer")
		}
		// The set is allowed to be loose -- it is the whole delta, and the
		// caller re-verifies -- but it must contain the real match.
		if len(ids) == 0 {
			t.Fatal("empty candidate set: the match lives in the delta, so this is a wrong " +
				"answer the caller cannot recover from, not a missed optimization")
		}
		want, ok := view.Dense(100)
		if !ok {
			t.Fatal("delta node 100 has no dense id")
		}
		found := false
		for _, id := range ids {
			if id == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("candidate set %v omits the one node that matches", ids)
		}
	})
}
