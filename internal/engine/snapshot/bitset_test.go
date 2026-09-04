// SPDX-License-Identifier: Apache-2.0
package snapshot

import "testing"

func TestBitsetSetHasCountIterate(t *testing.T) {
	b := NewBitset(200)
	for _, i := range []NodeID{0, 63, 64, 199} {
		b.Set(i)
	}
	if b.Has(1) || !b.Has(64) || b.Count() != 4 {
		t.Fatalf("membership or count wrong: count=%d", b.Count())
	}
	var got []NodeID
	b.Iterate(func(i NodeID) bool { got = append(got, i); return true })
	want := []NodeID{0, 63, 64, 199}
	if len(got) != len(want) {
		t.Fatalf("iterate got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iterate got %v want %v", got, want)
		}
	}
	// Iterate stops when fn returns false.
	n := 0
	b.Iterate(func(NodeID) bool { n++; return n < 2 })
	if n != 2 {
		t.Fatalf("early stop visited %d", n)
	}
}

func TestKindMask(t *testing.T) {
	m := NewKindMask(70)
	m.Set(3)
	m.Set(70)
	if !m.Has(3) || !m.Has(70) || m.Has(4) || m.Has(-1) || m.Has(71) {
		t.Fatal("mask membership wrong")
	}
	all := NewKindMask(70)
	all.SetAll()
	if !all.Has(0) || !all.Has(70) {
		t.Fatal("SetAll incomplete")
	}
}
