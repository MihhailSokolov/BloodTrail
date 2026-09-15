// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"path/filepath"
	"testing"
)

// buildEdgeKindFixture returns a base Snapshot with three nodes, a kind-11
// edge, and (when withE is set) a kind-10 edge as well. Kind 5 labels a NODE
// and never an edge, and kind 99 is never used at all -- between them they
// cover both ways View.EdgeKindPresent can answer "absent": a kind inside
// edgeKindSeen's range whose flag is false, and a kind past its end.
func buildEdgeKindFixture(t *testing.T, withE bool) *Snapshot {
	t.Helper()
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{5: "NodeOnly", 10: "E", 11: "F", 99: "Unused"})
	for id := uint64(1); id <= 3; id++ {
		var kinds []KindID
		if id == 1 {
			kinds = []KindID{5}
		}
		if err := b.AddNode(id, kinds, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
	}
	b.AddEdge(100, 1, 2, 11)
	if withE {
		b.AddEdge(101, 2, 3, 10)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

func TestEdgeKindPresentBaseDerivation(t *testing.T) {
	v := NewView(buildEdgeKindFixture(t, true))

	for _, tc := range []struct {
		name  string
		kinds []KindID
		want  bool
	}{
		{"a kind with edges is present", []KindID{11}, true},
		{"the other kind with edges is present", []KindID{10}, true},
		{"a node-only kind labels no edge", []KindID{5}, false},
		{"a kind past the derived range labels no edge", []KindID{99}, false},
		{"an empty list admits every kind", nil, true},
		{"any present kind in the list is enough", []KindID{5, 99, 11}, true},
		{"all-absent list stays absent", []KindID{5, 99}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := v.EdgeKindPresent(tc.kinds); got != tc.want {
				t.Fatalf("EdgeKindPresent(%v) = %v, want %v", tc.kinds, got, tc.want)
			}
		})
	}

	// Kind 10's only edge is gone in this variant, so it must read absent
	// while kind 11 still reads present -- the derivation is per-kind, not a
	// single "has any edges" flag.
	without := NewView(buildEdgeKindFixture(t, false))
	if without.EdgeKindPresent([]KindID{10}) {
		t.Fatal("kind 10 has no edges in this fixture; want absent")
	}
	if !without.EdgeKindPresent([]KindID{11}) {
		t.Fatal("kind 11 still has its edge; want present")
	}
}

// TestEdgeKindPresentEmptyGraph: a snapshot with no edges at all must report
// every kind absent, and must not mistake the "any kind" list for a promise
// that some edge exists -- nil kinds is a property of the PATTERN (it admits
// anything), so it stays true only because the caller then still has to find
// an edge, which is exactly what the interpreter's own zero-row path does.
func TestEdgeKindPresentEmptyGraph(t *testing.T) {
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{10: "E"})
	if err := b.AddNode(1, nil, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	v := NewView(s)
	if v.EdgeKindPresent([]KindID{10}) {
		t.Fatal("no edges exist; want kind 10 absent")
	}
}

// TestEdgeKindPresentFileRoundTrip: the set is derived, not stored, so a
// snapshot reloaded from its file form must report exactly what the original
// did.
func TestEdgeKindPresentFileRoundTrip(t *testing.T) {
	s := buildEdgeKindFixture(t, true)
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, 7); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}
	got, _, err := ReadSnapshotFile(path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	v := NewView(got)
	if !v.EdgeKindPresent([]KindID{10}) || !v.EdgeKindPresent([]KindID{11}) {
		t.Fatal("reloaded snapshot must derive the same edge kinds as present")
	}
	if v.EdgeKindPresent([]KindID{5}) || v.EdgeKindPresent([]KindID{99}) {
		t.Fatal("reloaded snapshot must keep absent kinds absent")
	}
}

// TestEdgeKindPresentSegments mirrors the self-loop set's segment contract: a
// delta-written edge makes its kind present on the overlay view, a tombstone
// within the same segment never makes it present, and a base kind stays
// present past a delta tombstone until a fold re-derives the truth.
func TestEdgeKindPresentSegments(t *testing.T) {
	t.Run("delta-added kind becomes present", func(t *testing.T) {
		v := NewView(buildEdgeKindFixture(t, false))
		sb := &SegmentBuilder{}
		sb.AddEdgeState(500, 2, 3, 10)
		ov := v.WithSegment(sb.Build())

		if !ov.EdgeKindPresent([]KindID{10}) {
			t.Fatal("segment adds a kind-10 edge; want present")
		}
		if ov.EdgeKindPresent([]KindID{99}) {
			t.Fatal("kind 99 still has no edge; want absent")
		}
		if v.EdgeKindPresent([]KindID{10}) {
			t.Fatal("the pre-overlay view must be unaffected")
		}
	})

	t.Run("tombstoned delta edge never becomes present", func(t *testing.T) {
		sb := &SegmentBuilder{}
		sb.AddEdgeState(500, 2, 3, 10)
		sb.TombstoneEdge(500)
		ov := NewView(buildEdgeKindFixture(t, false)).WithSegment(sb.Build())
		if ov.EdgeKindPresent([]KindID{10}) {
			t.Fatal("the only kind-10 edge was tombstoned within the same segment; want absent")
		}
	})

	// MergeSegments is the second Segment construction path (SegmentBuilder.
	// Build is the first), and it derives the set separately -- so a merged
	// segment has to carry the union of what its inputs wrote.
	t.Run("merged segments carry the union of their edge kinds", func(t *testing.T) {
		a := &SegmentBuilder{}
		a.AddEdgeState(500, 2, 3, 10)
		b := &SegmentBuilder{}
		b.AddEdgeState(501, 3, 1, 99)

		ov := NewView(buildEdgeKindFixture(t, false)).
			WithSegment(MergeSegments([]*Segment{a.Build(), b.Build()}))
		if !ov.EdgeKindPresent([]KindID{10}) {
			t.Fatal("merged segment must carry the first input's kind 10")
		}
		if !ov.EdgeKindPresent([]KindID{99}) {
			t.Fatal("merged segment must carry the second input's kind 99")
		}
		if !ov.EdgeKindPresent([]KindID{5, 99}) {
			t.Fatal("a list containing a present kind must report present")
		}
	})

	t.Run("base kind stays present past a delta tombstone, until a fold", func(t *testing.T) {
		base := buildEdgeKindFixture(t, true)
		sb := &SegmentBuilder{}
		sb.TombstoneEdge(101)
		seg := sb.Build()

		ov := NewView(base).WithSegment(seg)
		if !ov.EdgeKindPresent([]KindID{10}) {
			t.Fatal("conservative contract: a delta tombstone must not clear a base kind")
		}

		folded, err := Fold(base, []*Segment{seg})
		if err != nil {
			t.Fatalf("Fold: %v", err)
		}
		if NewView(folded).EdgeKindPresent([]KindID{10}) {
			t.Fatal("the fold dropped the only kind-10 edge; want absent on the folded base")
		}
		if !NewView(folded).EdgeKindPresent([]KindID{11}) {
			t.Fatal("the fold kept the kind-11 edge; want present")
		}
	})
}
