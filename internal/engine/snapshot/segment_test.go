// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"reflect"
	"testing"
)

// mustAddNodeState stages a node upsert via SegmentBuilder.AddNodeState,
// failing the test immediately on error.
func mustAddNodeState(t *testing.T, b *SegmentBuilder, id uint64, kindIDs []KindID, propsJSON string) {
	t.Helper()
	if err := b.AddNodeState(id, kindIDs, []byte(propsJSON)); err != nil {
		t.Fatalf("AddNodeState(%d): %v", id, err)
	}
}

// TestSegmentNodeUpsertWithPropsAndObjectID covers a node upsert carrying a
// property bag with an objectid, checked via NodeState, PropValueByName, and
// PropMap.
func TestSegmentNodeUpsertWithPropsAndObjectID(t *testing.T) {
	b := &SegmentBuilder{}
	mustAddNodeState(t, b, 100, []KindID{1, 2}, `{"objectid":"S-1-100","name":"Alice","enabled":true}`)

	seg := b.Build()

	st, ok := seg.NodeState(100)
	if !ok {
		t.Fatal("NodeState(100) not found")
	}
	if st.Tombstoned {
		t.Fatal("NodeState(100).Tombstoned = true, want false")
	}
	if !reflect.DeepEqual(st.KindIDs, []KindID{1, 2}) {
		t.Fatalf("NodeState(100).KindIDs = %v, want [1 2]", st.KindIDs)
	}
	if st.ObjectID != "S-1-100" {
		t.Fatalf("NodeState(100).ObjectID = %q, want %q", st.ObjectID, "S-1-100")
	}

	if v, ok := st.PropValueByName("name"); !ok || v != "Alice" {
		t.Fatalf(`PropValueByName("name") = (%v, %v), want ("Alice", true)`, v, ok)
	}
	if v, ok := st.PropValueByName("enabled"); !ok || v != true {
		t.Fatalf(`PropValueByName("enabled") = (%v, %v), want (true, true)`, v, ok)
	}
	if _, ok := st.PropValueByName("missing"); ok {
		t.Fatal(`PropValueByName("missing") found, want absent`)
	}

	want := map[string]any{"objectid": "S-1-100", "name": "Alice", "enabled": true}
	if got := st.PropMap(); !reflect.DeepEqual(got, want) {
		t.Fatalf("PropMap() = %v, want %v", got, want)
	}

	// NodesByObjectID resolves the objectid back to the node id.
	if got := seg.NodesByObjectID("S-1-100"); !reflect.DeepEqual(got, []uint64{100}) {
		t.Fatalf("NodesByObjectID(%q) = %v, want [100]", "S-1-100", got)
	}
}

// TestSegmentKindLessNodeUpsert covers a node upsert with zero kinds, a
// production upsert shape BloodHound can produce.
func TestSegmentKindLessNodeUpsert(t *testing.T) {
	b := &SegmentBuilder{}
	mustAddNodeState(t, b, 200, nil, `{}`)

	seg := b.Build()

	st, ok := seg.NodeState(200)
	if !ok {
		t.Fatal("NodeState(200) not found")
	}
	if st.Tombstoned {
		t.Fatal("NodeState(200).Tombstoned = true, want false")
	}
	if len(st.KindIDs) != 0 {
		t.Fatalf("NodeState(200).KindIDs = %v, want empty", st.KindIDs)
	}
	if st.ObjectID != "" {
		t.Fatalf("NodeState(200).ObjectID = %q, want empty", st.ObjectID)
	}
}

// TestSegmentNodeTombstone covers a plain node tombstone with no prior
// upsert.
func TestSegmentNodeTombstone(t *testing.T) {
	b := &SegmentBuilder{}
	b.TombstoneNode(300)

	seg := b.Build()

	st, ok := seg.NodeState(300)
	if !ok {
		t.Fatal("NodeState(300) not found")
	}
	if !st.Tombstoned {
		t.Fatal("NodeState(300).Tombstoned = false, want true")
	}
	if seg.NodesByObjectID("") != nil {
		t.Fatal(`a tombstone must not be indexed under an empty objectid`)
	}
}

// TestSegmentSameKeyRepeatSecondWins covers repeated writes to the same node
// id within one SegmentBuilder: the later call must win, regardless of
// whether it's an upsert-then-tombstone or a tombstone-then-upsert.
func TestSegmentSameKeyRepeatSecondWins(t *testing.T) {
	t.Run("upsert then tombstone", func(t *testing.T) {
		b := &SegmentBuilder{}
		mustAddNodeState(t, b, 400, []KindID{1}, `{"objectid":"dup"}`)
		b.TombstoneNode(400)

		seg := b.Build()
		st, ok := seg.NodeState(400)
		if !ok || !st.Tombstoned {
			t.Fatalf("NodeState(400) = (%+v, %v), want tombstoned", st, ok)
		}
		if got := seg.NodesByObjectID("dup"); got != nil {
			t.Fatalf("NodesByObjectID(dup) = %v, want nil (tombstoned node must not be indexed)", got)
		}
	})

	t.Run("tombstone then upsert", func(t *testing.T) {
		b := &SegmentBuilder{}
		b.TombstoneNode(400)
		mustAddNodeState(t, b, 400, []KindID{1}, `{"objectid":"dup"}`)

		seg := b.Build()
		st, ok := seg.NodeState(400)
		if !ok || st.Tombstoned {
			t.Fatalf("NodeState(400) = (%+v, %v), want present (not tombstoned)", st, ok)
		}
		if st.ObjectID != "dup" {
			t.Fatalf("NodeState(400).ObjectID = %q, want %q", st.ObjectID, "dup")
		}
		if got := seg.NodesByObjectID("dup"); !reflect.DeepEqual(got, []uint64{400}) {
			t.Fatalf("NodesByObjectID(dup) = %v, want [400]", got)
		}
	})
}

// TestSegmentEdgeUpsertAndTombstone covers an edge upsert and a separate
// edge tombstone within the same segment.
func TestSegmentEdgeUpsertAndTombstone(t *testing.T) {
	b := &SegmentBuilder{}
	b.AddEdgeState(1000, 10, 20, 5)
	b.TombstoneEdge(2000)

	seg := b.Build()

	est, ok := seg.EdgeState(1000)
	if !ok {
		t.Fatal("EdgeState(1000) not found")
	}
	if est.Tombstoned {
		t.Fatal("EdgeState(1000).Tombstoned = true, want false")
	}
	if est.StartID != 10 || est.EndID != 20 || est.Kind != 5 {
		t.Fatalf("EdgeState(1000) = %+v, want {StartID:10 EndID:20 Kind:5}", est)
	}

	est2, ok := seg.EdgeState(2000)
	if !ok {
		t.Fatal("EdgeState(2000) not found")
	}
	if !est2.Tombstoned {
		t.Fatal("EdgeState(2000).Tombstoned = false, want true")
	}
}

// TestSegmentAddedKind covers AddKind and AddedKinds.
func TestSegmentAddedKind(t *testing.T) {
	b := &SegmentBuilder{}
	b.AddKind(7, "NewKind")

	seg := b.Build()

	got := seg.AddedKinds()
	want := map[KindID]string{7: "NewKind"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AddedKinds() = %v, want %v", got, want)
	}

	// The returned map must be a copy, not an alias into the segment's
	// internal state.
	got[8] = "Mutated"
	if again := seg.AddedKinds(); reflect.DeepEqual(again, got) {
		t.Fatal("AddedKinds() mutation leaked into the segment's internal state")
	}
}

// TestSegmentAscendingIteration covers IterNodes and IterEdges visiting keys
// in ascending pg-id order regardless of insertion order.
func TestSegmentAscendingIteration(t *testing.T) {
	b := &SegmentBuilder{}
	mustAddNodeState(t, b, 300, nil, `{}`)
	mustAddNodeState(t, b, 100, nil, `{}`)
	b.TombstoneNode(200)

	b.AddEdgeState(30, 1, 2, 0)
	b.AddEdgeState(10, 1, 2, 0)
	b.TombstoneEdge(20)

	seg := b.Build()

	if got := seg.NodeCount(); got != 3 {
		t.Fatalf("NodeCount() = %d, want 3", got)
	}
	if got := seg.EdgeCount(); got != 3 {
		t.Fatalf("EdgeCount() = %d, want 3", got)
	}

	var nodeIDs []uint64
	seg.IterNodes(func(id uint64, st NodeSegState) bool {
		nodeIDs = append(nodeIDs, id)
		return true
	})
	if !reflect.DeepEqual(nodeIDs, []uint64{100, 200, 300}) {
		t.Fatalf("IterNodes order = %v, want [100 200 300]", nodeIDs)
	}

	var edgeIDs []uint64
	seg.IterEdges(func(id uint64, st EdgeSegState) bool {
		edgeIDs = append(edgeIDs, id)
		return true
	})
	if !reflect.DeepEqual(edgeIDs, []uint64{10, 20, 30}) {
		t.Fatalf("IterEdges order = %v, want [10 20 30]", edgeIDs)
	}

	// Early termination: fn returning false stops iteration.
	var stopped []uint64
	seg.IterNodes(func(id uint64, st NodeSegState) bool {
		stopped = append(stopped, id)
		return false
	})
	if !reflect.DeepEqual(stopped, []uint64{100}) {
		t.Fatalf("IterNodes early-stop visited = %v, want [100]", stopped)
	}
}

// TestSegmentApproxBytesPositive is a light sanity check that ApproxBytes
// grows with content rather than a strict accounting assertion.
func TestSegmentApproxBytesPositive(t *testing.T) {
	empty := (&SegmentBuilder{}).Build()
	if got := empty.ApproxBytes(); got != 0 {
		t.Fatalf("empty segment ApproxBytes() = %d, want 0 (nothing staged)", got)
	}

	b := &SegmentBuilder{}
	mustAddNodeState(t, b, 1, []KindID{1}, `{"objectid":"x","name":"a long string value to inflate the arena"}`)
	full := b.Build()

	if full.ApproxBytes() <= empty.ApproxBytes() {
		t.Fatalf("ApproxBytes() did not grow with content: empty=%d full=%d", empty.ApproxBytes(), full.ApproxBytes())
	}
}

// TestMergeSegmentsNewestWins covers MergeSegments' per-key conflict
// resolution: an upsert in the older segment and a tombstone in the newer
// one (and the reverse), plus AddedKinds unioning.
func TestMergeSegmentsNewestWins(t *testing.T) {
	t.Run("upsert then tombstone -> tombstoned", func(t *testing.T) {
		b1 := &SegmentBuilder{}
		mustAddNodeState(t, b1, 500, []KindID{1}, `{"objectid":"m"}`)
		b1.AddKind(1, "KindA")
		s1 := b1.Build()

		b2 := &SegmentBuilder{}
		b2.TombstoneNode(500)
		b2.AddKind(2, "KindB")
		s2 := b2.Build()

		merged := MergeSegments([]*Segment{s1, s2})

		st, ok := merged.NodeState(500)
		if !ok || !st.Tombstoned {
			t.Fatalf("merged NodeState(500) = (%+v, %v), want tombstoned", st, ok)
		}
		if got := merged.NodesByObjectID("m"); got != nil {
			t.Fatalf("merged NodesByObjectID(m) = %v, want nil", got)
		}

		wantKinds := map[KindID]string{1: "KindA", 2: "KindB"}
		if got := merged.AddedKinds(); !reflect.DeepEqual(got, wantKinds) {
			t.Fatalf("merged AddedKinds() = %v, want %v", got, wantKinds)
		}
	})

	t.Run("tombstone then upsert -> present", func(t *testing.T) {
		b1 := &SegmentBuilder{}
		b1.TombstoneNode(600)
		s1 := b1.Build()

		b2 := &SegmentBuilder{}
		mustAddNodeState(t, b2, 600, []KindID{2}, `{"objectid":"n"}`)
		s2 := b2.Build()

		merged := MergeSegments([]*Segment{s1, s2})

		st, ok := merged.NodeState(600)
		if !ok || st.Tombstoned {
			t.Fatalf("merged NodeState(600) = (%+v, %v), want present", st, ok)
		}
		if st.ObjectID != "n" {
			t.Fatalf("merged NodeState(600).ObjectID = %q, want %q", st.ObjectID, "n")
		}
		if v, ok := st.PropValueByName("objectid"); !ok || v != "n" {
			t.Fatalf(`merged PropValueByName("objectid") = (%v, %v), want ("n", true)`, v, ok)
		}
		if got := merged.NodesByObjectID("n"); !reflect.DeepEqual(got, []uint64{600}) {
			t.Fatalf("merged NodesByObjectID(n) = %v, want [600]", got)
		}
	})

	t.Run("edges follow the same newest-wins rule", func(t *testing.T) {
		b1 := &SegmentBuilder{}
		b1.AddEdgeState(7000, 1, 2, 9)
		s1 := b1.Build()

		b2 := &SegmentBuilder{}
		b2.TombstoneEdge(7000)
		s2 := b2.Build()

		merged := MergeSegments([]*Segment{s1, s2})
		est, ok := merged.EdgeState(7000)
		if !ok || !est.Tombstoned {
			t.Fatalf("merged EdgeState(7000) = (%+v, %v), want tombstoned", est, ok)
		}
	})

	t.Run("result is independent of the inputs", func(t *testing.T) {
		b1 := &SegmentBuilder{}
		mustAddNodeState(t, b1, 1, []KindID{1}, `{}`)
		s1 := b1.Build()

		b2 := &SegmentBuilder{}
		mustAddNodeState(t, b2, 2, []KindID{1}, `{}`)
		s2 := b2.Build()

		merged := MergeSegments([]*Segment{s1, s2})
		if merged.NodeCount() != 2 {
			t.Fatalf("merged.NodeCount() = %d, want 2", merged.NodeCount())
		}
		if _, ok := s1.NodeState(2); ok {
			t.Fatal("MergeSegments mutated s1: node 2 leaked into s1")
		}
		if _, ok := s2.NodeState(1); ok {
			t.Fatal("MergeSegments mutated s2: node 1 leaked into s2")
		}
	})
}

// TestSegmentAddNodeStateParseErrorFailsLoudly covers AddNodeState
// propagating a malformed property bag as an error rather than swallowing
// it.
func TestSegmentAddNodeStateParseErrorFailsLoudly(t *testing.T) {
	b := &SegmentBuilder{}
	if err := b.AddNodeState(1, nil, []byte(`{not valid json`)); err == nil {
		t.Fatal("AddNodeState with malformed propsJSON = nil error, want non-nil")
	}
}
