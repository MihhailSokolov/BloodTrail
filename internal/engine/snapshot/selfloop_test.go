// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"path/filepath"
	"testing"
)

// buildSelfLoopFixture returns a base Snapshot with three nodes and, when
// withLoop is set, a kind-10 self-loop on node 2 alongside an ordinary
// kind-11 edge -- the minimal graph View.SelfLoopHazard's kind scoping can
// be exercised against.
func buildSelfLoopFixture(t *testing.T, withLoop bool) *Snapshot {
	t.Helper()
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{10: "E", 11: "F"})
	for id := uint64(1); id <= 3; id++ {
		if err := b.AddNode(id, nil, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
	}
	b.AddEdge(100, 1, 2, 11)
	if withLoop {
		b.AddEdge(101, 2, 2, 10)
	}
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

func TestSelfLoopHazardBaseDerivation(t *testing.T) {
	v := NewView(buildSelfLoopFixture(t, true))

	if !v.SelfLoopHazard([]KindID{10}) {
		t.Fatal("kind 10 has a self-loop; want hazard")
	}
	if v.SelfLoopHazard([]KindID{11}) {
		t.Fatal("kind 11 has no self-loop; want no hazard")
	}
	if !v.SelfLoopHazard(nil) {
		t.Fatal("empty kind list admits every kind; want hazard")
	}
	if !v.SelfLoopHazard([]KindID{11, 10}) {
		t.Fatal("kind list including 10 must report the hazard")
	}

	clean := NewView(buildSelfLoopFixture(t, false))
	if clean.SelfLoopHazard(nil) || clean.SelfLoopHazard([]KindID{10}) {
		t.Fatal("no self-loop exists; want no hazard for any kind list")
	}
}

// TestSelfLoopHazardFileRoundTrip: the hazard set is derived, not stored, so
// a snapshot reloaded from its file form must report exactly what the
// original did.
func TestSelfLoopHazardFileRoundTrip(t *testing.T) {
	s := buildSelfLoopFixture(t, true)
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, 7); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}
	got, _, err := ReadSnapshotFile(path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	v := NewView(got)
	if !v.SelfLoopHazard([]KindID{10}) || v.SelfLoopHazard([]KindID{11}) {
		t.Fatal("reloaded snapshot must derive the same self-loop kinds")
	}
}

// TestSelfLoopHazardSegments: a delta-written self-loop arms the hazard on
// the overlay view; a tombstoned base self-loop does NOT clear it (the
// documented conservative over-approximation), and a fold re-derives the
// truth and clears it.
func TestSelfLoopHazardSegments(t *testing.T) {
	t.Run("delta-added self-loop arms the hazard", func(t *testing.T) {
		v := NewView(buildSelfLoopFixture(t, false))
		sb := &SegmentBuilder{}
		sb.AddEdgeState(500, 3, 3, 10)
		ov := v.WithSegment(sb.Build())

		if !ov.SelfLoopHazard([]KindID{10}) {
			t.Fatal("segment adds a kind-10 self-loop; want hazard")
		}
		if ov.SelfLoopHazard([]KindID{11}) {
			t.Fatal("kind 11 still has no self-loop; want no hazard")
		}
		if v.SelfLoopHazard([]KindID{10}) {
			t.Fatal("the pre-overlay view must be unaffected")
		}
	})

	t.Run("tombstoned delta self-loop never armed", func(t *testing.T) {
		sb := &SegmentBuilder{}
		sb.AddEdgeState(500, 3, 3, 10)
		sb.TombstoneEdge(500)
		ov := NewView(buildSelfLoopFixture(t, false)).WithSegment(sb.Build())
		if ov.SelfLoopHazard(nil) {
			t.Fatal("the only self-loop was tombstoned within the same segment; want no hazard")
		}
	})

	t.Run("base self-loop stays armed past a delta tombstone, until a fold", func(t *testing.T) {
		base := buildSelfLoopFixture(t, true)
		sb := &SegmentBuilder{}
		sb.TombstoneEdge(101)
		seg := sb.Build()

		ov := NewView(base).WithSegment(seg)
		if !ov.SelfLoopHazard([]KindID{10}) {
			t.Fatal("conservative contract: a delta tombstone must not clear the base hazard")
		}

		folded, err := Fold(base, []*Segment{seg})
		if err != nil {
			t.Fatalf("Fold: %v", err)
		}
		if NewView(folded).SelfLoopHazard(nil) {
			t.Fatal("the fold dropped the only self-loop; want no hazard on the folded base")
		}
	})
}
