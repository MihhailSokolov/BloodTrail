// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"fmt"
	"testing"
)

// buildValueIndexFixture: nodes carrying a boolean, a number, and a list
// property -- the shapes PostgreSQL has no index for and this one does.
func buildValueIndexFixture(t *testing.T, n int) *View {
	t.Helper()
	const kiBase KindID = 1
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{kiBase: "Base"})
	for i := 0; i < n; i++ {
		enabled := "true"
		if i%2 == 1 {
			enabled = "false"
		}
		// Only every hundredth node carries the rare list element, and one
		// carries it twice, so the postings must deduplicate.
		etypes := `["AES256"]`
		switch {
		case i%100 == 0:
			etypes = `["RC4-HMAC-MD5","AES256"]`
		case i%250 == 3:
			etypes = `["RC4-HMAC-MD5","RC4-HMAC-MD5"]`
		}
		props := fmt.Sprintf(`{"enabled":%s,"score":%d,"etypes":%s}`, enabled, i%7, etypes)
		if err := b.AddNode(uint64(i+1), []KindID{kiBase}, []byte(props)); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	return NewView(snap)
}

func TestValueIndexExactMatching(t *testing.T) {
	const n = 1000
	v := buildValueIndexFixture(t, n)

	t.Run("boolean equality", func(t *testing.T) {
		on, ok := v.NodesWithValueByName("enabled", true)
		if !ok {
			t.Fatal("not answered")
		}
		if len(on) != n/2 {
			t.Fatalf("got %d nodes with enabled=true, want %d", len(on), n/2)
		}
		off, _ := v.NodesWithValueByName("enabled", false)
		if len(off) != n/2 {
			t.Fatalf("got %d with enabled=false, want %d", len(off), n/2)
		}
	})

	t.Run("numeric equality", func(t *testing.T) {
		got, ok := v.NodesWithValueByName("score", float64(3))
		if !ok {
			t.Fatal("not answered")
		}
		want := 0
		for i := 0; i < n; i++ {
			if i%7 == 3 {
				want++
			}
		}
		if len(got) != want {
			t.Fatalf("got %d nodes with score=3, want %d", len(got), want)
		}
	})

	t.Run("list membership", func(t *testing.T) {
		got, ok := v.NodesWithArrayElementByName("etypes", "RC4-HMAC-MD5")
		if !ok {
			t.Fatal("not answered")
		}
		want := 0
		for i := 0; i < n; i++ {
			if i%100 == 0 || i%250 == 3 {
				want++
			}
		}
		if len(got) != want {
			t.Fatalf("got %d nodes carrying the element, want %d", len(got), want)
		}
		seen := map[NodeID]bool{}
		for _, id := range got {
			if seen[id] {
				t.Fatalf("node %d repeated: a list holding the same element twice must "+
					"not make its node a repeated candidate", id)
			}
			seen[id] = true
		}
	})

	t.Run("a list value is not also a scalar", func(t *testing.T) {
		got, ok := v.NodesWithValueByName("etypes", "RC4-HMAC-MD5")
		if !ok {
			t.Fatal("not answered")
		}
		if len(got) != 0 {
			t.Fatalf("got %d: a list property is never exactly equal to one of its elements", len(got))
		}
	})

	t.Run("a value nothing carries resolves to nothing", func(t *testing.T) {
		got, ok := v.NodesWithArrayElementByName("etypes", "NO-SUCH-ETYPE")
		if !ok || len(got) != 0 {
			t.Fatalf("got %v (ok=%v), want empty", got, ok)
		}
	})

	t.Run("agrees with the property store it indexes", func(t *testing.T) {
		prop, ok := v.PropIDByName("enabled")
		if !ok {
			t.Fatal("enabled not interned")
		}
		want := map[NodeID]bool{}
		for i := 0; i < v.NodeCount(); i++ {
			if val, ok := v.PropValue(NodeID(i), prop); ok {
				if b, isBool := val.(bool); isBool && b {
					want[NodeID(i)] = true
				}
			}
		}
		got, _ := v.NodesWithValueByName("enabled", true)
		if len(got) != len(want) {
			t.Fatalf("got %d, want %d", len(got), len(want))
		}
		for _, id := range got {
			if !want[id] {
				t.Fatalf("node %d is not enabled=true in the store", id)
			}
		}
	})

	t.Run("counting does not need the postings", func(t *testing.T) {
		n1, ok := v.ValuePostingCount("enabled", true, false)
		if !ok {
			t.Fatal("not answered")
		}
		ids, _ := v.NodesWithValueByName("enabled", true)
		if n1 != len(ids) {
			t.Fatalf("count %d disagrees with postings %d", n1, len(ids))
		}
	})
}

// TestValueIndexOverlayIsASuperset pins the contract every candidate source
// here owes: a delta can give a node the value, so its nodes are unioned in
// rather than reasoned about.
func TestValueIndexOverlayIsASuperset(t *testing.T) {
	const kiBase KindID = 1
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{kiBase: "Base"})
	for i := 0; i < 4; i++ {
		if err := b.AddNode(uint64(i+1), []KindID{kiBase}, []byte(`{"enabled":false}`)); err != nil {
			t.Fatal(err)
		}
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}

	var sb SegmentBuilder
	sb.AddKind(kiBase, "Base")
	if err := sb.AddNodeState(3, []KindID{kiBase}, []byte(`{"enabled":true}`)); err != nil {
		t.Fatal(err)
	}
	v := NewView(snap).WithSegment(sb.Build())

	got, ok := v.NodesWithValueByName("enabled", true)
	if !ok {
		t.Fatal("not answered")
	}
	want, _ := v.Dense(3)
	found := false
	for _, id := range got {
		if id == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("got %v, missing the delta-updated node %d: a SHORT candidate source "+
			"makes the executor serve a wrong answer", got, want)
	}
}
