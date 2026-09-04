// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"reflect"
	"testing"
)

func TestBuilderBuildsSnapshot(t *testing.T) {
	// nodes: 100(kinds 1), 200(kinds 1,2), 300(kinds 2), 400(kinds 1)
	// edges: 100->200 k10, 100->300 k11, 200->300 k10, 400->100 k10, 100->200 k11
	b := NewBuilder(7)
	for _, n := range []struct {
		id    uint64
		kinds []KindID
	}{
		{100, []KindID{1}}, {200, []KindID{1, 2}}, {300, []KindID{2}}, {400, []KindID{1}},
	} {
		if err := b.AddNode(n.id, n.kinds); err != nil {
			t.Fatal(err)
		}
	}
	b.AddEdge(100, 300, 11)
	b.AddEdge(400, 100, 10)
	b.AddEdge(100, 200, 10)
	b.AddEdge(200, 300, 10)
	b.AddEdge(100, 200, 11)
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if got := s.NodeCount(); got != 4 {
		t.Fatalf("NodeCount() = %d, want 4", got)
	}
	if got := s.EdgeCount(); got != 5 {
		t.Fatalf("EdgeCount() = %d, want 5", got)
	}
	wantGraphIDs := []uint64{100, 200, 300, 400}
	if !reflect.DeepEqual(s.GraphIDs, wantGraphIDs) {
		t.Fatalf("GraphIDs = %v, want %v", s.GraphIDs, wantGraphIDs)
	}

	if id, ok := s.Dense(300); !ok || id != 2 {
		t.Fatalf("Dense(300) = (%d, %v), want (2, true)", id, ok)
	}
	if _, ok := s.Dense(150); ok {
		t.Fatalf("Dense(150) found, want not found")
	}

	targets, kinds := s.Out(0)
	if !reflect.DeepEqual(targets, []NodeID{1, 1, 2}) {
		t.Fatalf("Out(0) targets = %v, want [1 1 2]", targets)
	}
	if !reflect.DeepEqual(kinds, []KindID{10, 11, 11}) {
		t.Fatalf("Out(0) kinds = %v, want [10 11 11]", kinds)
	}

	inTargets, inKinds := s.In(2)
	if !reflect.DeepEqual(inTargets, []NodeID{0, 1}) {
		t.Fatalf("In(2) targets = %v, want [0 1]", inTargets)
	}
	if !reflect.DeepEqual(inKinds, []KindID{11, 10}) {
		t.Fatalf("In(2) kinds = %v, want [11 10]", inKinds)
	}

	wantOutOffsets := []uint64{0, 3, 4, 4, 5}
	if !reflect.DeepEqual(s.OutOffsets, wantOutOffsets) {
		t.Fatalf("OutOffsets = %v, want %v", s.OutOffsets, wantOutOffsets)
	}
	wantInOffsets := []uint64{0, 1, 3, 5, 5}
	if !reflect.DeepEqual(s.InOffsets, wantInOffsets) {
		t.Fatalf("InOffsets = %v, want %v", s.InOffsets, wantInOffsets)
	}

	k1 := s.NodesOfKind(1)
	for _, want := range []NodeID{0, 1, 3} {
		if !k1.Has(want) {
			t.Fatalf("NodesOfKind(1) missing dense %d", want)
		}
	}
	if k1.Count() != 3 {
		t.Fatalf("NodesOfKind(1).Count() = %d, want 3", k1.Count())
	}

	k2 := s.NodesOfKind(2)
	for _, want := range []NodeID{1, 2} {
		if !k2.Has(want) {
			t.Fatalf("NodesOfKind(2) missing dense %d", want)
		}
	}
	if k2.Count() != 2 {
		t.Fatalf("NodesOfKind(2).Count() = %d, want 2", k2.Count())
	}

	k99 := s.NodesOfKind(99)
	if k99.Count() != 0 {
		t.Fatalf("NodesOfKind(99).Count() = %d, want 0", k99.Count())
	}

	if s.MaxKindID != 11 {
		t.Fatalf("MaxKindID = %d, want 11", s.MaxKindID)
	}
	if s.ApproxBytes() == 0 {
		t.Fatal("ApproxBytes() = 0, want > 0")
	}
}

func TestAddNodeOutOfOrder(t *testing.T) {
	b := NewBuilder(1)
	if err := b.AddNode(100, []KindID{1}); err != nil {
		t.Fatalf("AddNode(100) unexpected error: %v", err)
	}
	if err := b.AddNode(100, []KindID{1}); err == nil {
		t.Fatal("AddNode(100) again: want error for non-ascending id, got nil")
	}
	if err := b.AddNode(50, []KindID{1}); err == nil {
		t.Fatal("AddNode(50) after 100: want error for non-ascending id, got nil")
	}
}

func TestBuildUnknownEdgeEndpoint(t *testing.T) {
	b := NewBuilder(1)
	if err := b.AddNode(100, []KindID{1}); err != nil {
		t.Fatal(err)
	}
	b.AddEdge(100, 999, 10)
	if _, err := b.Build(); err == nil {
		t.Fatal("Build() with unknown endpoint: want error, got nil")
	}
}
