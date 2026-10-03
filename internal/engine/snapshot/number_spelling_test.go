// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// TestNumberSpellingCanonical pins which stored spellings the float64 value
// model reproduces: exactly FormatFloat(f, 'f', -1, 64).
func TestNumberSpellingCanonical(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"0", true},
		{"7", true},
		{"-5", true},
		{"123456789012345", true},
		{"1700000000", true},
		{"1.5", true},
		{"0.1", true},
		{"9007199254740992", true},
		{"1000000000000000000000", true},
		{"0.0000001", true},
		{"1.0", false},
		{"2.50", false},
		{"-0", false},
		{"0.0", false},
		{"1e2", false},
		{"1E+21", false},
		{"007", false},
		{"9007199254740993", false},
		{"0.10000000000000001", false},
	} {
		f, err := strconv.ParseFloat(tc.text, 64)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.text, err)
		}
		if got := numberSpellingCanonical([]byte(tc.text), f); got != tc.want {
			t.Errorf("numberSpellingCanonical(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

// numberSpellingBase builds a base of three nodes: n.canon only ever holds
// canonical numbers, n.big holds 9007199254740993 on one of them, and the
// list and map properties hold a 1.0 inside.
func numberSpellingBase(t *testing.T) *Snapshot {
	t.Helper()
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "N"})
	mustAddNodeJSON(t, b, 1, []KindID{1}, `{"canon":1,"big":9007199254740993,"list":[1.0,2],"map":{"k":2.50},"strs":["1.0"]}`)
	mustAddNodeJSON(t, b, 2, []KindID{1}, `{"canon":2.5,"big":9007199254740992,"list":[1,2],"map":{"k":2},"strs":["x"]}`)
	mustAddNodeJSON(t, b, 3, []KindID{1}, `{"canon":-7,"name":"a"}`)
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

func assertNumbersCanonical(t *testing.T, v *View, want map[string]bool) {
	t.Helper()
	for name, canonical := range want {
		if got := v.NumbersCanonical(name); got != canonical {
			t.Errorf("NumbersCanonical(%q) = %v, want %v", name, got, canonical)
		}
	}
}

// TestNumbersCanonicalBase checks the base fact, for a number held as the
// value and inside a list or map, and that the numbers still decode.
func TestNumbersCanonicalBase(t *testing.T) {
	s := numberSpellingBase(t)
	assertNumbersCanonical(t, NewView(s), map[string]bool{
		"canon":   true,
		"big":     false,
		"list":    false,
		"map":     false,
		"strs":    true,
		"name":    true,
		"missing": true,
	})
	prop, _ := s.Props.IDByName("big")
	if got, ok := s.Props.Value(0, prop); !ok || got != float64(9007199254740992) {
		t.Errorf("big decodes to %v, %v; want the nearest float64", got, ok)
	}
	prop, _ = s.Props.IDByName("list")
	if got, _ := s.Props.Value(0, prop); len(got.([]any)) != 2 {
		t.Errorf("list decodes to %v", got)
	}
}

// TestNumbersCanonicalDelta checks a segment marks what it writes -- over a
// base that holds only canonical numbers -- and that merging segments and
// folding them into a new base, as compaction does, keeps the mark.
func TestNumbersCanonicalDelta(t *testing.T) {
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "N"})
	mustAddNodeJSON(t, b, 1, []KindID{1}, `{"x":1,"y":2}`)
	mustAddNodeJSON(t, b, 2, []KindID{1}, `{"x":3,"y":4}`)
	base, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	var first SegmentBuilder
	first.AddKind(1, "N")
	mustAddNodeState(t, &first, 1, []KindID{1}, `{"x":1.0,"y":2}`)
	var second SegmentBuilder
	second.AddKind(1, "N")
	mustAddNodeState(t, &second, 7, []KindID{1}, `{"z":[2.50]}`)
	segs := []*Segment{first.Build(), second.Build()}

	want := map[string]bool{"x": false, "y": true, "z": false}
	assertNumbersCanonical(t, NewView(base).WithSegment(segs[0]).WithSegment(segs[1]), want)
	assertNumbersCanonical(t, NewView(base).WithSegment(MergeSegments(segs)), want)

	folded, err := Fold(base, segs)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	assertNumbersCanonical(t, NewView(folded), want)
}

// TestNumbersCanonicalSurvivesSnapshotFile checks the mark round-trips
// through a snapshot file.
func TestNumbersCanonicalSurvivesSnapshotFile(t *testing.T) {
	s := numberSpellingBase(t)
	s.WatermarkLineage = testLineage
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, Stamp{Watermark: 1}); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}
	read, _, err := ReadSnapshotFile(path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	assertNumbersCanonical(t, NewView(read), map[string]bool{"canon": true, "big": false, "list": false})
}

// TestSnapshotFileFromBeforeNumberSpellingsIsRefused pins the format bump
// that came with propKindNumberNonCanonical: a version 2 file carries every
// number as canonical, so reading one would call a non-canonical number
// canonical. It is refused, and the engine rebuilds.
func TestSnapshotFileFromBeforeNumberSpellingsIsRefused(t *testing.T) {
	s := buildFileFixture(t)
	s.WatermarkLineage = testLineage
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, Stamp{Watermark: 1}); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	versionOff := len(snapshotMagic)
	binary.LittleEndian.PutUint32(data[versionOff:versionOff+4], 2)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadSnapshotFile(path); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("ReadSnapshotFile(version 2) = %v, want ErrVersionMismatch", err)
	}
}
