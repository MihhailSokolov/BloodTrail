// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"encoding/json"
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

// TestFlatArrayScannerAgreesWithEncodingJSON is the safety argument for
// reading list properties straight out of the arena instead of decoding
// them: for every array text, the fast scanner must produce exactly the keys
// encoding/json would, or report that it is not certain and fall back.
//
// A disagreement here is not a slow answer, it is a wrong candidate set --
// `'x' IN n.prop` would look at the wrong nodes.
func TestFlatArrayScannerAgreesWithEncodingJSON(t *testing.T) {
	for _, raw := range []string{
		`[]`,
		`["a"]`,
		`["a","b","c"]`,
		`[ "a" , "b" ]`,
		`["RC4-HMAC-MD5","AES256"]`,
		`[1,2,3]`,
		`[1.5,-2.25,0,-0]`,
		`[true,false,null]`,
		`["a",1,true,null]`,
		`["with space","with-dash","with.dot"]`,
		// Shapes the scanner must REFUSE rather than guess at.
		`["with\"escape"]`,
		`["back\\slash"]`,
		`["tab\there"]`,
		`[["nested"]]`,
		`[{"k":"v"}]`,
		`["unicode é"]`,
		// Genuinely non-ASCII content with no escape is still flat.
		`["café","日本語"]`,
	} {
		t.Run(raw, func(t *testing.T) {
			fast, ok := scanFlatArrayKeys(nil, []byte(raw), nil)

			var v []any
			if err := json.Unmarshal([]byte(raw), &v); err != nil {
				t.Skipf("not valid JSON: %v", err)
			}
			var want []string
			for _, el := range v {
				if key, keyable := valueKey(el); keyable {
					want = append(want, key)
				}
			}

			if !ok {
				// Refusing is always allowed; the caller falls back. But it
				// must not refuse everything, which the cases above check by
				// having plenty that do succeed.
				return
			}
			if len(fast) != len(want) {
				t.Fatalf("scanner produced %d keys, encoding/json %d: %q vs %q", len(fast), len(want), fast, want)
			}
			for i := range want {
				if fast[i] != want[i] {
					t.Fatalf("key %d = %q, want %q", i, fast[i], want[i])
				}
			}
		})
	}
}

// TestFlatArrayScannerRefusesTheHardCases pins that the shapes it cannot
// decode correctly are actually refused, so the agreement above is not
// achieved by never running.
func TestFlatArrayScannerRefusesTheHardCases(t *testing.T) {
	for _, raw := range []string{`["a\"b"]`, `["a\\b"]`, `[["x"]]`, `[{"k":1}]`, `["a"`, `not an array`} {
		if _, ok := scanFlatArrayKeys(nil, []byte(raw), nil); ok {
			t.Fatalf("%q was accepted; it must fall back to encoding/json", raw)
		}
	}
	for _, raw := range []string{`["a","b"]`, `[1,2]`, `[true]`, `[]`} {
		if _, ok := scanFlatArrayKeys(nil, []byte(raw), nil); !ok {
			t.Fatalf("%q was refused; the fast path would never run", raw)
		}
	}
}

func BenchmarkValueIndexBuild(b *testing.B) {
	const n = 50000
	kiBase := KindID(1)
	bld := NewBuilder(1)
	bld.SetKinds(map[KindID]string{kiBase: "Base"})
	for i := 0; i < n; i++ {
		props := `{"etypes":["RC4-HMAC-MD5","AES256","AES128"],"enabled":true}`
		if err := bld.AddNode(uint64(i+1), []KindID{kiBase}, []byte(props)); err != nil {
			b.Fatal(err)
		}
	}
	snap, err := bld.Build()
	if err != nil {
		b.Fatal(err)
	}
	prop, _ := NewView(snap).PropIDByName("etypes")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		snap.valueIdx = nil
		b.StartTimer()
		snap.valueIndexFor(prop)
	}
}
