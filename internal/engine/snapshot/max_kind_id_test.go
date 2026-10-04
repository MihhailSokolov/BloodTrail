// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"math"
	"path/filepath"
	"testing"
	"time"
)

// smallserialMaxKindID is the last id PostgreSQL's `kind.id smallserial`
// column can hand out, and the largest value KindID (an int16) holds: every
// derivation that sizes something by "the largest kind id, plus one" has to
// do that arithmetic in int, or it wraps to -32768 exactly here.
const smallserialMaxKindID KindID = math.MaxInt16

// buildCatchingPanic runs b.Build, turning a panic into a test failure that
// names it instead of a crashed test binary.
func buildCatchingPanic(t *testing.T, b *Builder) *Snapshot {
	t.Helper()
	var (
		s        *Snapshot
		err      error
		panicked any
	)
	func() {
		defer func() { panicked = recover() }()
		s, err = b.Build()
	}()
	if panicked != nil {
		t.Fatalf("Build panicked: %v", panicked)
	}
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s
}

// TestBuildAcceptsTheLargestSmallserialKindID pins Build -- and the
// derivation it shares with ReadSnapshotFile and Fold (finalizeDerived) --
// against a node kind and an edge kind at id 32767: the snapshot must
// build, and every kind-derived fact must name that id.
func TestBuildAcceptsTheLargestSmallserialKindID(t *testing.T) {
	t.Run("node kind", func(t *testing.T) {
		b := NewBuilder(1)
		b.SetKinds(map[KindID]string{1: "User", smallserialMaxKindID: "MaxKind"})
		mustAddNodeJSON(t, b, 1, []KindID{smallserialMaxKindID}, `{}`)
		mustAddNodeJSON(t, b, 2, []KindID{1}, `{}`)
		b.AddEdge(10, 1, 2, 1)

		s := buildCatchingPanic(t, b)
		if s.MaxKindID != smallserialMaxKindID {
			t.Fatalf("MaxKindID = %d, want %d", s.MaxKindID, smallserialMaxKindID)
		}
		v := NewView(s)
		if got := v.NodesOfKind(smallserialMaxKindID).Count(); got != 1 {
			t.Fatalf("NodesOfKind(%d).Count() = %d, want 1", smallserialMaxKindID, got)
		}
		if v.EdgeKindPresent([]KindID{smallserialMaxKindID}) {
			t.Fatalf("EdgeKindPresent(%d) = true, but no edge carries it", smallserialMaxKindID)
		}
		if !v.EdgeKindPresent([]KindID{1}) {
			t.Fatal("EdgeKindPresent(1) = false, but edge 10 carries it")
		}
	})

	t.Run("edge kind", func(t *testing.T) {
		b := NewBuilder(1)
		b.SetKinds(map[KindID]string{1: "User", smallserialMaxKindID: "MaxKind"})
		mustAddNodeJSON(t, b, 1, []KindID{1}, `{}`)
		mustAddNodeJSON(t, b, 2, []KindID{1}, `{}`)
		b.AddEdge(10, 1, 2, smallserialMaxKindID)
		b.AddEdge(11, 2, 2, smallserialMaxKindID) // a self-loop, for the hazard bit

		s := buildCatchingPanic(t, b)
		if s.MaxKindID != smallserialMaxKindID {
			t.Fatalf("MaxKindID = %d, want %d", s.MaxKindID, smallserialMaxKindID)
		}
		v := NewView(s)
		if !v.EdgeKindPresent([]KindID{smallserialMaxKindID}) {
			t.Fatalf("EdgeKindPresent(%d) = false, but edges 10 and 11 carry it", smallserialMaxKindID)
		}
		if !v.SelfLoopHazard([]KindID{smallserialMaxKindID}) {
			t.Fatalf("SelfLoopHazard(%d) = false, but edge 11 is a self-loop of that kind", smallserialMaxKindID)
		}
		if name, ok := v.Kinds().Name(smallserialMaxKindID); !ok || name != "MaxKind" {
			t.Fatalf("Kinds().Name(%d) = (%q, %v), want (%q, true)", smallserialMaxKindID, name, ok, "MaxKind")
		}
		if err := CheckViewConsistent(v); err != nil {
			t.Fatalf("CheckViewConsistent: %v", err)
		}
	})
}

// TestFoldAcceptsTheLargestSmallserialKindID covers the steady-state route
// to the same id: a write-through segment registers kind 32767 and writes a
// node and an edge carrying it. The overlay View serves both; compaction's
// Fold then rebuilds a base from them and must neither panic nor lose them.
func TestFoldAcceptsTheLargestSmallserialKindID(t *testing.T) {
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "User"})
	mustAddNodeJSON(t, b, 1, []KindID{1}, `{}`)
	base := buildCatchingPanic(t, b)

	var sb SegmentBuilder
	sb.AddKind(smallserialMaxKindID, "MaxKind")
	mustAddNodeState(t, &sb, 2, []KindID{smallserialMaxKindID}, `{}`)
	sb.AddEdgeState(20, 1, 2, smallserialMaxKindID)
	seg := sb.Build()

	stacked := NewView(base).WithSegment(seg)
	if got := stacked.NodesOfKind(smallserialMaxKindID).Count(); got != 1 {
		t.Fatalf("overlay NodesOfKind(%d).Count() = %d, want 1", smallserialMaxKindID, got)
	}

	var (
		folded   *Snapshot
		err      error
		panicked any
	)
	func() {
		defer func() { panicked = recover() }()
		folded, err = Fold(base, []*Segment{seg})
	}()
	if panicked != nil {
		t.Fatalf("Fold panicked: %v", panicked)
	}
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}

	if folded.MaxKindID != smallserialMaxKindID {
		t.Fatalf("folded MaxKindID = %d, want %d", folded.MaxKindID, smallserialMaxKindID)
	}
	foldedView := NewView(folded)
	if !foldedView.EdgeKindPresent([]KindID{smallserialMaxKindID}) {
		t.Fatalf("folded EdgeKindPresent(%d) = false, but edge 20 carries it", smallserialMaxKindID)
	}
	compareViewContents(t, foldedView, stacked)
}

// TestSnapshotFileRoundTripsTheLargestSmallserialKindID writes and rereads
// a snapshot whose node and edge kinds reach id 32767: ReadSnapshotFile
// re-derives everything Build derives (finalizeDerived), so a file the
// engine saved must load again rather than be refused as corrupt.
func TestSnapshotFileRoundTripsTheLargestSmallserialKindID(t *testing.T) {
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "User", smallserialMaxKindID: "MaxKind"})
	mustAddNodeJSON(t, b, 1, []KindID{smallserialMaxKindID}, `{"name":"n1"}`)
	mustAddNodeJSON(t, b, 2, []KindID{1}, `{"name":"n2"}`)
	b.AddEdge(10, 1, 2, smallserialMaxKindID)
	s := buildCatchingPanic(t, b)
	s.WatermarkLineage = testLineage

	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, testStamp); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}
	got, _, err := ReadSnapshotFile(path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	assertSnapshotsEqual(t, s, got)
	if name, ok := got.Kinds.Name(smallserialMaxKindID); !ok || name != "MaxKind" {
		t.Fatalf("reread Kinds.Name(%d) = (%q, %v), want (%q, true)", smallserialMaxKindID, name, ok, "MaxKind")
	}
	if !NewView(got).EdgeKindPresent([]KindID{smallserialMaxKindID}) {
		t.Fatalf("reread EdgeKindPresent(%d) = false, want true", smallserialMaxKindID)
	}
}

// TestCheckViewsEquivalentStopsAtTheLargestSmallserialKindID pins the kind
// table walk CheckViewsEquivalent makes up to the Views' kind ceiling: a
// KindID loop counter wraps from 32767 to -32768 and never exits. The
// overlay's ceiling reaches 32767 without any Build involved, so this runs
// the comparison under a deadline rather than let a regression hang the
// package's whole test run.
func TestCheckViewsEquivalentStopsAtTheLargestSmallserialKindID(t *testing.T) {
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "User"})
	mustAddNodeJSON(t, b, 1, []KindID{1}, `{}`)
	base := buildCatchingPanic(t, b)

	var sb SegmentBuilder
	sb.AddKind(smallserialMaxKindID, "MaxKind")
	mustAddNodeState(t, &sb, 2, []KindID{smallserialMaxKindID}, `{}`)
	v := NewView(base).WithSegment(sb.Build())
	if v.MaxKindID() != smallserialMaxKindID {
		t.Fatalf("overlay MaxKindID() = %d, want %d (fixture assumption)", v.MaxKindID(), smallserialMaxKindID)
	}

	done := make(chan error, 1)
	go func() { done <- CheckViewsEquivalent(v, v) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CheckViewsEquivalent(v, v) = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("CheckViewsEquivalent did not return within 10s for a kind ceiling of %d", smallserialMaxKindID)
	}
}
