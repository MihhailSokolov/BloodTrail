// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"math"
	"testing"
)

func TestKindTableNameAndID(t *testing.T) {
	tbl := NewKindTable(map[KindID]string{1: "User", 2: "Group"})

	if got, ok := tbl.Name(1); !ok || got != "User" {
		t.Fatalf("Name(1) = (%q, %v), want (%q, true)", got, ok, "User")
	}
	if got, ok := tbl.ID("Group"); !ok || got != 2 {
		t.Fatalf(`ID("Group") = (%d, %v), want (2, true)`, got, ok)
	}

	if _, ok := tbl.Name(3); ok {
		t.Fatal("Name(3) found, want not found")
	}
	if _, ok := tbl.ID("Computer"); ok {
		t.Fatal(`ID("Computer") found, want not found`)
	}
	if _, ok := tbl.ID(""); ok {
		t.Fatal(`ID("") found, want not found`)
	}
}

func TestKindTableLen(t *testing.T) {
	tbl := NewKindTable(map[KindID]string{1: "User", 2: "Group"})
	if got := tbl.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}

	empty := NewKindTable(nil)
	if got := empty.Len(); got != 0 {
		t.Fatalf("Len() on empty table = %d, want 0", got)
	}
}

func TestKindTableMaxKindID(t *testing.T) {
	const maxID KindID = math.MaxInt16 // 32767, the largest a SMALLSERIAL column holds

	tbl := NewKindTable(map[KindID]string{maxID: "MaxKind"})

	if got, ok := tbl.Name(maxID); !ok || got != "MaxKind" {
		t.Fatalf("Name(%d) = (%q, %v), want (%q, true)", maxID, got, ok, "MaxKind")
	}
	if got, ok := tbl.ID("MaxKind"); !ok || got != maxID {
		t.Fatalf(`ID("MaxKind") = (%d, %v), want (%d, true)`, got, ok, maxID)
	}
}

func TestKindTableApproxBytesGrowsWithEntries(t *testing.T) {
	empty := NewKindTable(nil)
	small := NewKindTable(map[KindID]string{1: "User"})
	bigger := NewKindTable(map[KindID]string{1: "User", 2: "Group", 3: "Computer"})

	if small.ApproxBytes() == 0 {
		t.Fatal("ApproxBytes() = 0, want > 0")
	}
	if small.ApproxBytes() <= empty.ApproxBytes() {
		t.Fatalf("ApproxBytes() with 1 entry (%d) not greater than empty table (%d)", small.ApproxBytes(), empty.ApproxBytes())
	}
	if bigger.ApproxBytes() <= small.ApproxBytes() {
		t.Fatalf("ApproxBytes() with 3 entries (%d) not greater than 1 entry (%d)", bigger.ApproxBytes(), small.ApproxBytes())
	}
}
