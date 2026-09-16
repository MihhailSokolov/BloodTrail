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

// TestNodesWithStringOverlaySuperset pins the delta contract: a segment can
// give a node the property, change its value, or remove it, and the base
// index cannot know -- so every segment-touched node joins the candidate
// set unconditionally. The result must remain a SUPERSET, which is what
// lets callers re-verify instead of the index having to be exact.
func TestNodesWithStringOverlaySuperset(t *testing.T) {
	base := buildPropIndexFixture(t, 10)
	v := NewView(base)
	name := mustProp(t, v, "name")

	// Node 5 is renamed by a segment to something the base index would
	// never return for this prefix, and a brand-new node 99 is added.
	sb := &SegmentBuilder{}
	if err := sb.AddNodeState(5, []KindID{1}, []byte(`{"name":"RENAMED@CORP.LOCAL"}`)); err != nil {
		t.Fatalf("AddNodeState(5): %v", err)
	}
	if err := sb.AddNodeState(99, []KindID{1}, []byte(`{"name":"USER0003@CORP.LOCAL"}`)); err != nil {
		t.Fatalf("AddNodeState(99): %v", err)
	}
	ov := v.WithSegment(sb.Build())

	ids, ok := ov.NodesWithString(name, StringPrefix, "USER0003")
	if !ok {
		t.Fatal("index unavailable on the overlay view")
	}
	got := map[uint64]bool{}
	for _, id := range ids {
		got[ov.GraphID(id)] = true
	}
	// The delta-added node carrying the matching name must be a candidate.
	if !got[99] {
		t.Fatalf("delta-added node 99 missing from candidates %v", got)
	}
	// The renamed node must also be a candidate even though its NEW value
	// does not match this prefix -- the superset contract is what keeps the
	// opposite case (a rename INTO the prefix) correct too.
	if !got[5] {
		t.Fatalf("segment-touched node 5 missing from candidates %v", got)
	}
}
