// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"reflect"
	"testing"
)

// buildViewFixture builds the fixture snapshot these tests share -- 3 nodes,
// 2 edges, with an objectid property on every node -- and wraps it in a View.
func buildViewFixture(t *testing.T) (*Snapshot, *View) {
	t.Helper()

	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "User", 2: "Group"})
	mustAddNodeJSON(t, b, 10, []KindID{1}, `{"objectid":"S-1-A","name":"alice"}`)
	mustAddNodeJSON(t, b, 20, []KindID{1, 2}, `{"objectid":"S-1-B","name":"bob"}`)
	mustAddNodeJSON(t, b, 30, []KindID{2}, `{"objectid":"S-1-C","name":"carol"}`)
	b.AddEdge(1, 10, 20, 5)
	b.AddEdge(2, 20, 30, 6)

	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s, NewView(s)
}

// TestViewMirrorsBase asserts that every View accessor returns exactly what
// the direct Snapshot call returns, over buildViewFixture's snapshot: 3
// nodes, 2 edges, props with objectid.
func TestViewMirrorsBase(t *testing.T) {
	s, v := buildViewFixture(t)

	if v.Base() != s {
		t.Fatalf("Base() = %p, want %p", v.Base(), s)
	}

	if got, want := v.NodeCount(), s.NodeCount(); got != want {
		t.Fatalf("NodeCount() = %d, want %d", got, want)
	}
	if got, want := v.EdgeCount(), s.EdgeCount(); got != want {
		t.Fatalf("EdgeCount() = %d, want %d", got, want)
	}

	for _, databaseID := range []uint64{10, 20, 30, 999} {
		gotID, gotOK := v.Dense(databaseID)
		wantID, wantOK := s.Dense(databaseID)
		if gotID != wantID || gotOK != wantOK {
			t.Fatalf("Dense(%d) = (%d, %v), want (%d, %v)", databaseID, gotID, gotOK, wantID, wantOK)
		}
	}

	for n := NodeID(0); n < NodeID(s.NodeCount()); n++ {
		if got, want := v.GraphID(n), s.GraphIDs[n]; got != want {
			t.Fatalf("GraphID(%d) = %d, want %d", n, got, want)
		}

		lo, hi := s.KindOffsets[n], s.KindOffsets[n+1]
		wantKinds := s.NodeKinds[lo:hi]
		if gotKinds := v.KindIDsOf(n); !reflect.DeepEqual(gotKinds, wantKinds) {
			t.Fatalf("KindIDsOf(%d) = %v, want %v", n, gotKinds, wantKinds)
		}

		gotOutTargets, gotOutKinds, gotOutEdgeIDs := v.Out(n)
		wantOutTargets, wantOutKinds := s.Out(n)
		if !reflect.DeepEqual(gotOutTargets, wantOutTargets) {
			t.Fatalf("Out(%d) targets = %v, want %v", n, gotOutTargets, wantOutTargets)
		}
		if !reflect.DeepEqual(gotOutKinds, wantOutKinds) {
			t.Fatalf("Out(%d) kinds = %v, want %v", n, gotOutKinds, wantOutKinds)
		}
		lo64, hi64 := s.OutOffsets[n], s.OutOffsets[n+1]
		if !reflect.DeepEqual(gotOutEdgeIDs, s.OutEdgeIDs[lo64:hi64]) {
			t.Fatalf("Out(%d) edgeIDs = %v, want %v", n, gotOutEdgeIDs, s.OutEdgeIDs[lo64:hi64])
		}

		gotInTargets, gotInKinds := v.In(n)
		wantInTargets, wantInKinds := s.In(n)
		if !reflect.DeepEqual(gotInTargets, wantInTargets) {
			t.Fatalf("In(%d) targets = %v, want %v", n, gotInTargets, wantInTargets)
		}
		if !reflect.DeepEqual(gotInKinds, wantInKinds) {
			t.Fatalf("In(%d) kinds = %v, want %v", n, gotInKinds, wantInKinds)
		}
	}

	// Out/In aliasing: the View's slices must alias the same backing array
	// as the base Snapshot's, not a copy.
	vOutTargets, _, _ := v.Out(0)
	sOutTargets, _ := s.Out(0)
	if len(vOutTargets) > 0 && &vOutTargets[0] != &sOutTargets[0] {
		t.Fatalf("Out(0) targets does not alias the base snapshot's backing array")
	}

	for _, id := range []uint64{1, 2, 999} {
		gotIdx, gotOK := v.EdgeByID(id)
		wantIdx, wantOK := s.EdgeByID(id)
		if gotIdx != wantIdx || gotOK != wantOK {
			t.Fatalf("EdgeByID(%d) = (%d, %v), want (%d, %v)", id, gotIdx, gotOK, wantIdx, wantOK)
		}
	}

	// NodesOfKind: pointer-equal to the base's own bitmap for a kind that
	// exists (kindBitmaps is keyed and cached at Build time); for an absent
	// kind, Snapshot.NodesOfKind allocates a fresh empty Bitset on every
	// call, so only its emptiness is comparable, not its identity.
	for _, k := range []KindID{1, 2} {
		got := v.NodesOfKind(k)
		want := s.NodesOfKind(k)
		if got != want {
			t.Fatalf("NodesOfKind(%d) = %p, want pointer-equal to %p", k, got, want)
		}
	}
	if got := v.NodesOfKind(99); got.Count() != 0 {
		t.Fatalf("NodesOfKind(99).Count() = %d, want 0", got.Count())
	}

	if v.Kinds() != s.Kinds {
		t.Fatalf("Kinds() = %p, want %p", v.Kinds(), s.Kinds)
	}
	if got, want := v.MultiGraph(), s.MultiGraph; got != want {
		t.Fatalf("MultiGraph() = %v, want %v", got, want)
	}
	if got, want := v.ApproxBytes(), s.ApproxBytes(); got != want {
		t.Fatalf("ApproxBytes() = %d, want %d", got, want)
	}

	if v.Overlay() {
		t.Fatal("Overlay() = true, want false (no delta segments yet)")
	}

	nameID, ok := v.PropIDByName("name")
	if !ok {
		t.Fatal(`PropIDByName("name") not found`)
	}
	wantNameID, wantOK := s.Props.IDByName("name")
	if nameID != wantNameID || ok != wantOK {
		t.Fatalf("PropIDByName(name) = (%d, %v), want (%d, %v)", nameID, ok, wantNameID, wantOK)
	}

	for n := NodeID(0); n < NodeID(s.NodeCount()); n++ {
		gotVal, gotOK := v.PropValue(n, nameID)
		wantVal, wantOK := s.Props.Value(n, nameID)
		if gotVal != wantVal || gotOK != wantOK {
			t.Fatalf("PropValue(%d, name) = (%v, %v), want (%v, %v)", n, gotVal, gotOK, wantVal, wantOK)
		}

		gotMap := v.PropNodeMap(n)
		wantMap := s.Props.NodeMap(n)
		if !reflect.DeepEqual(gotMap, wantMap) {
			t.Fatalf("PropNodeMap(%d) = %v, want %v", n, gotMap, wantMap)
		}
	}

	for _, oid := range []string{"S-1-A", "S-1-B", "S-1-C", "missing"} {
		gotID, gotOK := v.NodeByObjectID(oid)
		wantID, wantOK := s.Props.NodeByObjectID(oid)
		if gotID != wantID || gotOK != wantOK {
			t.Fatalf("NodeByObjectID(%q) = (%d, %v), want (%d, %v)", oid, gotID, gotOK, wantID, wantOK)
		}

		gotIDs, gotOK := v.NodesByObjectID(oid)
		wantIDs, wantOK := s.Props.NodesByObjectID(oid)
		if !reflect.DeepEqual(gotIDs, wantIDs) || gotOK != wantOK {
			t.Fatalf("NodesByObjectID(%q) = (%v, %v), want (%v, %v)", oid, gotIDs, gotOK, wantIDs, wantOK)
		}
	}
}
