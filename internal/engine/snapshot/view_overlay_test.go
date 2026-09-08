// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"reflect"
	"sync"
	"testing"
)

// buildOverlayFixture builds a 6-node/5-edge overlay fixture. Node kinds:
// User=1, Group=2. Nodes 10..60 (step 10) each carry an
// objectid "S-obj-<id>" and a "name" property. Edges chain 10->20->30->40,
// plus 20->50 and 30->60, each carrying kind 5.
func buildOverlayFixture(t *testing.T) (*Snapshot, *View) {
	t.Helper()

	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "User", 2: "Group"})
	mustAddNodeJSON(t, b, 10, []KindID{1}, `{"objectid":"S-obj-10","name":"n10"}`)
	mustAddNodeJSON(t, b, 20, []KindID{1}, `{"objectid":"S-obj-20","name":"n20"}`)
	mustAddNodeJSON(t, b, 30, []KindID{2}, `{"objectid":"S-obj-30","name":"n30"}`)
	mustAddNodeJSON(t, b, 40, []KindID{2}, `{"objectid":"S-obj-40","name":"n40"}`)
	mustAddNodeJSON(t, b, 50, []KindID{1}, `{"objectid":"S-obj-50","name":"n50"}`)
	mustAddNodeJSON(t, b, 60, []KindID{2}, `{"objectid":"S-obj-60","name":"n60"}`)
	b.AddEdge(1001, 10, 20, 5)
	b.AddEdge(1002, 20, 30, 5)
	b.AddEdge(1003, 30, 40, 5)
	b.AddEdge(1004, 20, 50, 5)
	b.AddEdge(1005, 30, 60, 5)

	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s, NewView(s)
}

// (a) property update visible via PropValue; the old View still sees the
// old value.
func TestOverlayPropertyUpdateVisibleOldViewUnaffected(t *testing.T) {
	s, v0 := buildOverlayFixture(t)

	nameID, ok := v0.PropIDByName("name")
	if !ok {
		t.Fatal(`PropIDByName("name") not found`)
	}
	n10, ok := v0.Dense(10)
	if !ok {
		t.Fatal("Dense(10) not found")
	}

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 10, []KindID{1}, `{"objectid":"S-obj-10","name":"updated"}`)
	seg := sb.Build()

	v1 := v0.WithSegment(seg)

	if !v1.Overlay() {
		t.Fatal("v1.Overlay() = false, want true")
	}
	if v0.Overlay() {
		t.Fatal("v0.Overlay() = true, want false (WithSegment must not mutate the receiver)")
	}

	gotNew, ok := v1.PropValue(n10, nameID)
	if !ok || gotNew != "updated" {
		t.Fatalf(`v1.PropValue(10, name) = (%v, %v), want ("updated", true)`, gotNew, ok)
	}
	gotOld, ok := v0.PropValue(n10, nameID)
	if !ok || gotOld != "n10" {
		t.Fatalf(`v0.PropValue(10, name) = (%v, %v), want ("n10", true) -- old View must be unaffected`, gotOld, ok)
	}
	// The base snapshot itself must be untouched by WithSegment/PropValue.
	if baseVal, baseOK := s.Props.Value(n10, nameID); !baseOK || baseVal != "n10" {
		t.Fatalf(`base snapshot's own Props.Value(10, name) = (%v, %v), want ("n10", true)`, baseVal, baseOK)
	}

	// PropValueByName mirrors PropValue.
	if got, ok := v1.PropValueByName(n10, "name"); !ok || got != "updated" {
		t.Fatalf(`v1.PropValueByName(10, "name") = (%v, %v), want ("updated", true)`, got, ok)
	}
	if got, ok := v0.PropValueByName(n10, "name"); !ok || got != "n10" {
		t.Fatalf(`v0.PropValueByName(10, "name") = (%v, %v), want ("n10", true)`, got, ok)
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// (b) new node with new kind: Dense, NodesOfKind, KindIDsOf, objectid
// lookup.
func TestOverlayNewNodeWithNewKind(t *testing.T) {
	_, v0 := buildOverlayFixture(t)

	const newKind KindID = 9
	sb := &SegmentBuilder{}
	sb.AddKind(newKind, "Computer")
	mustAddNodeState(t, sb, 70, []KindID{newKind}, `{"objectid":"S-obj-70","name":"n70"}`)
	seg := sb.Build()

	v1 := v0.WithSegment(seg)

	baseCount := v0.NodeCount()
	if got, want := v1.NodeCount(), baseCount+1; got != want {
		t.Fatalf("NodeCount() = %d, want %d", got, want)
	}

	dense, ok := v1.Dense(70)
	if !ok {
		t.Fatal("Dense(70) not found")
	}
	if int(dense) != baseCount {
		t.Fatalf("Dense(70) = %d, want %d (base.NodeCount())", dense, baseCount)
	}
	if got := v1.GraphID(dense); got != 70 {
		t.Fatalf("GraphID(%d) = %d, want 70", dense, got)
	}
	if !v1.Alive(dense) {
		t.Fatal("Alive(virtual node) = false, want true")
	}

	kinds := v1.KindIDsOf(dense)
	if !reflect.DeepEqual(kinds, []KindID{newKind}) {
		t.Fatalf("KindIDsOf(virtual) = %v, want [%d]", kinds, newKind)
	}

	bm := v1.NodesOfKind(newKind)
	if !bm.Has(dense) {
		t.Fatalf("NodesOfKind(%d).Has(%d) = false, want true", newKind, dense)
	}
	if got := bm.Count(); got != 1 {
		t.Fatalf("NodesOfKind(%d).Count() = %d, want 1", newKind, got)
	}

	kt := v1.Kinds()
	if name, ok := kt.Name(newKind); !ok || name != "Computer" {
		t.Fatalf(`Kinds().Name(%d) = (%q, %v), want ("Computer", true)`, newKind, name, ok)
	}
	// base Kinds table must be untouched.
	if _, ok := v0.Kinds().Name(newKind); ok {
		t.Fatal("base Kinds() leaked the added kind")
	}

	gotID, ok := v1.NodeByObjectID("S-obj-70")
	if !ok || gotID != dense {
		t.Fatalf("NodeByObjectID(S-obj-70) = (%d, %v), want (%d, true)", gotID, ok, dense)
	}
	gotIDs, ok := v1.NodesByObjectID("S-obj-70")
	if !ok || !reflect.DeepEqual(gotIDs, []NodeID{dense}) {
		t.Fatalf("NodesByObjectID(S-obj-70) = (%v, %v), want ([%d], true)", gotIDs, ok, dense)
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// (c) node tombstone: Alive false, adjacency from neighbors skips it, kind
// bitmap cleared.
func TestOverlayNodeTombstoneCascades(t *testing.T) {
	_, v0 := buildOverlayFixture(t)

	n20, ok := v0.Dense(20)
	if !ok {
		t.Fatal("Dense(20) not found")
	}
	n10, _ := v0.Dense(10)
	n30, _ := v0.Dense(30)

	// Sanity: node 20 (dense n20) is a User (kind 1) and currently has
	// neighbors both ways.
	if !v0.NodesOfKind(1).Has(n20) {
		t.Fatal("test setup: node 20 must carry kind 1 before the tombstone")
	}

	sb := &SegmentBuilder{}
	sb.TombstoneNode(20)
	seg := sb.Build()
	v1 := v0.WithSegment(seg)

	if v1.Alive(n20) {
		t.Fatal("Alive(20) = true after tombstone, want false")
	}
	if !v1.Alive(n10) || !v1.Alive(n30) {
		t.Fatal("unrelated nodes must remain Alive")
	}

	// Adjacency cascade: node 10's out-edge to 20 must be skipped now, and
	// node 30's in-edge from 20 must be skipped too.
	var got []NodeID
	v1.OutEdges(n10, func(target NodeID, kind KindID, edgeID uint64) bool {
		got = append(got, target)
		return true
	})
	if len(got) != 0 {
		t.Fatalf("OutEdges(10) after tombstoning target 20 = %v, want empty", got)
	}

	var gotIn []NodeID
	v1.InEdges(n30, func(source NodeID, kind KindID, edgeID uint64) bool {
		gotIn = append(gotIn, source)
		return true
	})
	if len(gotIn) != 0 {
		t.Fatalf("InEdges(30) after tombstoning source 20 = %v, want empty", gotIn)
	}

	// Kind bitmap: node 20 must no longer appear under kind 1.
	if v1.NodesOfKind(1).Has(n20) {
		t.Fatal("NodesOfKind(1) still contains tombstoned node 20")
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// (d) edge tombstone: OutEdges/InEdges skip exactly that slot, EdgeByID
// misses.
func TestOverlayEdgeTombstone(t *testing.T) {
	s, v0 := buildOverlayFixture(t)

	n20, _ := v0.Dense(20)
	n30, _ := v0.Dense(30)
	n50, _ := v0.Dense(50)

	// Sanity: base EdgeByID finds edge 1002 (20->30).
	if _, ok := s.EdgeByID(1002); !ok {
		t.Fatal("test setup: base EdgeByID(1002) not found")
	}

	sb := &SegmentBuilder{}
	sb.TombstoneEdge(1002) // 20 -> 30
	seg := sb.Build()
	v1 := v0.WithSegment(seg)

	var outTargets []NodeID
	v1.OutEdges(n20, func(target NodeID, kind KindID, edgeID uint64) bool {
		outTargets = append(outTargets, target)
		return true
	})
	if !reflect.DeepEqual(sortedNodeIDs(outTargets), []NodeID{n50}) {
		t.Fatalf("OutEdges(20) = %v, want only [%d] (edge to 30 tombstoned)", outTargets, n50)
	}

	var inSources []NodeID
	v1.InEdges(n30, func(source NodeID, kind KindID, edgeID uint64) bool {
		inSources = append(inSources, source)
		return true
	})
	if len(inSources) != 0 {
		t.Fatalf("InEdges(30) = %v, want empty (only in-edge was the tombstoned one)", inSources)
	}

	if _, ok := v1.EdgeByID(1002); ok {
		t.Fatal("EdgeByID(1002) found after tombstone, want miss")
	}
	// The base View is unaffected.
	if _, ok := v0.EdgeByID(1002); !ok {
		t.Fatal("v0.EdgeByID(1002) missing, want the un-overlaid View to still find it")
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// (e) added edge between a base node and a virtual node walks both
// directions.
func TestOverlayAddedEdgeBaseToVirtual(t *testing.T) {
	_, v0 := buildOverlayFixture(t)

	n10, _ := v0.Dense(10)

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 70, []KindID{1}, `{}`)
	sb.AddEdgeState(2001, 10, 70, 7) // base node 10 -> virtual node 70
	seg := sb.Build()
	v1 := v0.WithSegment(seg)

	n70, ok := v1.Dense(70)
	if !ok {
		t.Fatal("Dense(70) not found")
	}

	foundOut := false
	v1.OutEdges(n10, func(target NodeID, kind KindID, edgeID uint64) bool {
		if target == n70 && kind == 7 && edgeID == 2001 {
			foundOut = true
		}
		return true
	})
	if !foundOut {
		t.Fatal("OutEdges(10) did not walk the added edge to the virtual node")
	}

	foundIn := false
	v1.InEdges(n70, func(source NodeID, kind KindID, edgeID uint64) bool {
		if source == n10 && kind == 7 && edgeID == 2001 {
			foundIn = true
		}
		return true
	})
	if !foundIn {
		t.Fatal("InEdges(70) did not walk the added edge from the base node")
	}

	start, end, kind, ok := v1.EdgeStateByID(2001)
	if !ok || start != n10 || end != n70 || kind != 7 {
		t.Fatalf("EdgeStateByID(2001) = (%d, %d, %d, %v), want (%d, %d, 7, true)", start, end, kind, ok, n10, n70)
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// (f) two stacked segments where the second overrides the first.
func TestOverlayStackedSegmentsSecondOverrides(t *testing.T) {
	_, v0 := buildOverlayFixture(t)
	n10, _ := v0.Dense(10)

	sb1 := &SegmentBuilder{}
	mustAddNodeState(t, sb1, 10, []KindID{1}, `{"objectid":"S-obj-10","name":"first"}`)
	seg1 := sb1.Build()

	sb2 := &SegmentBuilder{}
	mustAddNodeState(t, sb2, 10, []KindID{1, 2}, `{"objectid":"S-obj-10","name":"second"}`)
	seg2 := sb2.Build()

	v2 := v0.WithSegment(seg1).WithSegment(seg2)

	nameID, _ := v2.PropIDByName("name")
	got, ok := v2.PropValue(n10, nameID)
	if !ok || got != "second" {
		t.Fatalf(`PropValue(10, name) = (%v, %v), want ("second", true)`, got, ok)
	}

	kinds := v2.KindIDsOf(n10)
	if !reflect.DeepEqual(kinds, []KindID{1, 2}) {
		t.Fatalf("KindIDsOf(10) = %v, want [1 2] (second segment's kinds)", kinds)
	}

	if !v2.NodesOfKind(2).Has(n10) {
		t.Fatal("NodesOfKind(2) missing node 10 after the second segment added kind 2")
	}

	if err := CheckViewConsistent(v2); err != nil {
		t.Fatalf("CheckViewConsistent(v2): %v", err)
	}
}

// (g) MergeSegments result View is equivalent to the stacked View: same
// accessor outputs across a representative set of nodes/edges/kinds.
func TestOverlayMergeSegmentsEquivalentToStacked(t *testing.T) {
	_, v0 := buildOverlayFixture(t)

	sb1 := &SegmentBuilder{}
	mustAddNodeState(t, sb1, 10, []KindID{1}, `{"objectid":"S-obj-10","name":"first"}`)
	sb1.TombstoneNode(40)
	sb1.AddEdgeState(3001, 10, 60, 3)
	seg1 := sb1.Build()

	sb2 := &SegmentBuilder{}
	mustAddNodeState(t, sb2, 10, []KindID{1, 2}, `{"objectid":"S-obj-10","name":"second"}`)
	sb2.AddKind(9, "Computer")
	mustAddNodeState(t, sb2, 80, []KindID{9}, `{"objectid":"S-obj-80"}`)
	sb2.TombstoneEdge(1002) // 20 -> 30
	seg2 := sb2.Build()

	stacked := v0.WithSegment(seg1).WithSegment(seg2)
	merged := v0.WithSegment(MergeSegments([]*Segment{seg1, seg2}))

	assertViewsEquivalent(t, stacked, merged)

	if err := CheckViewConsistent(stacked); err != nil {
		t.Fatalf("CheckViewConsistent(stacked): %v", err)
	}
	if err := CheckViewConsistent(merged); err != nil {
		t.Fatalf("CheckViewConsistent(merged): %v", err)
	}
}

// assertViewsEquivalent compares two Views' accessor outputs across every
// dense node id and a representative set of kinds/objectids/edge ids,
// failing the test on the first mismatch.
func assertViewsEquivalent(t *testing.T, a, b *View) {
	t.Helper()

	if a.NodeCount() != b.NodeCount() {
		t.Fatalf("NodeCount() mismatch: %d vs %d", a.NodeCount(), b.NodeCount())
	}
	nameID, _ := a.PropIDByName("name")

	for n := NodeID(0); int(n) < a.NodeCount(); n++ {
		if a.Alive(n) != b.Alive(n) {
			t.Fatalf("Alive(%d) mismatch: %v vs %v", n, a.Alive(n), b.Alive(n))
		}
		if !a.Alive(n) {
			continue
		}
		if a.GraphID(n) != b.GraphID(n) {
			t.Fatalf("GraphID(%d) mismatch: %d vs %d", n, a.GraphID(n), b.GraphID(n))
		}
		if !reflect.DeepEqual(a.KindIDsOf(n), b.KindIDsOf(n)) {
			t.Fatalf("KindIDsOf(%d) mismatch: %v vs %v", n, a.KindIDsOf(n), b.KindIDsOf(n))
		}
		if !reflect.DeepEqual(a.PropNodeMap(n), b.PropNodeMap(n)) {
			t.Fatalf("PropNodeMap(%d) mismatch: %v vs %v", n, a.PropNodeMap(n), b.PropNodeMap(n))
		}
		av, aok := a.PropValue(n, nameID)
		bv, bok := b.PropValue(n, nameID)
		if av != bv || aok != bok {
			t.Fatalf("PropValue(%d, name) mismatch: (%v,%v) vs (%v,%v)", n, av, aok, bv, bok)
		}

		aOut := collectOutEdges(a, n)
		bOut := collectOutEdges(b, n)
		if !reflect.DeepEqual(aOut, bOut) {
			t.Fatalf("OutEdges(%d) mismatch: %v vs %v", n, aOut, bOut)
		}
		aIn := collectInEdges(a, n)
		bIn := collectInEdges(b, n)
		if !reflect.DeepEqual(aIn, bIn) {
			t.Fatalf("InEdges(%d) mismatch: %v vs %v", n, aIn, bIn)
		}
	}

	for k := KindID(1); k <= 9; k++ {
		abm, bbm := a.NodesOfKind(k), b.NodesOfKind(k)
		if abm.Count() != bbm.Count() {
			t.Fatalf("NodesOfKind(%d) count mismatch: %d vs %d", k, abm.Count(), bbm.Count())
		}
		var aids, bids []NodeID
		abm.Iterate(func(id NodeID) bool { aids = append(aids, id); return true })
		bbm.Iterate(func(id NodeID) bool { bids = append(bids, id); return true })
		if !reflect.DeepEqual(aids, bids) {
			t.Fatalf("NodesOfKind(%d) members mismatch: %v vs %v", k, aids, bids)
		}
	}

	for _, oid := range []string{"S-obj-10", "S-obj-40", "S-obj-60", "S-obj-80", "missing"} {
		aids, aok := a.NodesByObjectID(oid)
		bids, bok := b.NodesByObjectID(oid)
		if aok != bok || !reflect.DeepEqual(aids, bids) {
			t.Fatalf("NodesByObjectID(%q) mismatch: (%v,%v) vs (%v,%v)", oid, aids, aok, bids, bok)
		}
	}

	for _, id := range []uint64{1001, 1002, 1003, 1004, 1005, 3001} {
		_, aok := a.EdgeByID(id)
		_, bok := b.EdgeByID(id)
		if aok != bok {
			t.Fatalf("EdgeByID(%d) ok mismatch: %v vs %v", id, aok, bok)
		}
		as, ae, ak, aok2 := a.EdgeStateByID(id)
		bs, be, bk, bok2 := b.EdgeStateByID(id)
		if aok2 != bok2 || as != bs || ae != be || ak != bk {
			t.Fatalf("EdgeStateByID(%d) mismatch: (%d,%d,%d,%v) vs (%d,%d,%d,%v)", id, as, ae, ak, aok2, bs, be, bk, bok2)
		}
	}
}

func collectOutEdges(v *View, n NodeID) []deltaEdge {
	var out []deltaEdge
	v.OutEdges(n, func(target NodeID, kind KindID, edgeID uint64) bool {
		out = append(out, deltaEdge{other: target, kind: kind, edgeID: edgeID})
		return true
	})
	return out
}

func collectInEdges(v *View, n NodeID) []deltaEdge {
	var out []deltaEdge
	v.InEdges(n, func(source NodeID, kind KindID, edgeID uint64) bool {
		out = append(out, deltaEdge{other: source, kind: kind, edgeID: edgeID})
		return true
	})
	return out
}

func sortedNodeIDs(ids []NodeID) []NodeID {
	out := append([]NodeID(nil), ids...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// TestOverlayOutInPanicOnOverlay covers the pre-overlay slice-returning
// Out/In's debug assertion: calling either while Overlay() is true must
// panic rather than silently returning a stale base-array slice.
func TestOverlayOutInPanicOnOverlay(t *testing.T) {
	_, v0 := buildOverlayFixture(t)
	sb := &SegmentBuilder{}
	sb.TombstoneNode(10)
	v1 := v0.WithSegment(sb.Build())

	n10, _ := v0.Dense(10)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("Out on an overlay View did not panic")
			}
		}()
		v1.Out(n10)
	}()

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("In on an overlay View did not panic")
			}
		}()
		v1.In(n10)
	}()
}

// TestDedupEdgeKeysCatchesDoubleEmission unit-tests dedupEdgeKeys
// (viewcheck.go) directly: it must report an error when the same edgeKey
// appears more than once -- the case a plain set-membership check would
// silently collapse and miss -- and otherwise return every key once. No
// View in this package ever actually double-emits (ensureEdgeTomb's
// filtering rules that out by construction -- see view.go), so this is the
// only way to exercise dedupEdgeKeys' own duplicate-detection path.
func TestDedupEdgeKeysCatchesDoubleEmission(t *testing.T) {
	dup := edgeKey{a: 1, b: 2, kind: 5, edge: 1002}
	other := edgeKey{a: 3, b: 4, kind: 1, edge: 9}

	if _, err := dedupEdgeKeys([]edgeKey{dup, dup, other}, "OutEdges"); err == nil {
		t.Fatal("dedupEdgeKeys did not report the key seen twice, want an error")
	}

	set, err := dedupEdgeKeys([]edgeKey{dup, other}, "OutEdges")
	if err != nil {
		t.Fatalf("dedupEdgeKeys(no duplicates): %v", err)
	}
	if len(set) != 2 {
		t.Fatalf("dedupEdgeKeys(no duplicates) set len = %d, want 2", len(set))
	}
	if _, ok := set[dup]; !ok {
		t.Fatal("dedupEdgeKeys(no duplicates) set missing dup")
	}
	if _, ok := set[other]; !ok {
		t.Fatal("dedupEdgeKeys(no duplicates) set missing other")
	}
}

// TestOverlayEdgeOverrideRepointsSourceExactlyOnce covers upserting an edge
// id that already exists in the base to different endpoints: the OLD
// source/target must no longer see it at all, and the NEW source/target must
// see it EXACTLY once -- counting yields rather than checking set
// membership, since a set collapses a double emission to indistinguishable
// from a single one (see CheckViewConsistent's own strengthened check for
// the same reason).
func TestOverlayEdgeOverrideRepointsSourceExactlyOnce(t *testing.T) {
	s, v0 := buildOverlayFixture(t)
	n20, _ := v0.Dense(20) // old source of edge 1002 (20 -> 30, kind 5)
	n30, _ := v0.Dense(30) // old target
	n40, _ := v0.Dense(40) // new source
	n60, _ := v0.Dense(60) // new target

	// Sanity: base EdgeByID finds edge 1002 at its original endpoints.
	if _, ok := s.EdgeByID(1002); !ok {
		t.Fatal("test setup: base EdgeByID(1002) not found")
	}

	sb := &SegmentBuilder{}
	sb.AddEdgeState(1002, 40, 60, 3) // re-point 20->30 (kind 5) to 40->60 (kind 3)
	v1 := v0.WithSegment(sb.Build())

	oldOutCount := 0
	v1.OutEdges(n20, func(target NodeID, kind KindID, edgeID uint64) bool {
		if edgeID == 1002 {
			oldOutCount++
		}
		return true
	})
	if oldOutCount != 0 {
		t.Fatalf("OutEdges(20) yielded edge 1002 %d times after re-pointing away from 20, want 0", oldOutCount)
	}

	oldInCount := 0
	v1.InEdges(n30, func(source NodeID, kind KindID, edgeID uint64) bool {
		if edgeID == 1002 {
			oldInCount++
		}
		return true
	})
	if oldInCount != 0 {
		t.Fatalf("InEdges(30) yielded edge 1002 %d times after re-pointing away from 30, want 0", oldInCount)
	}

	newOutCount := 0
	var gotTarget NodeID
	var gotKind KindID
	v1.OutEdges(n40, func(target NodeID, kind KindID, edgeID uint64) bool {
		if edgeID == 1002 {
			newOutCount++
			gotTarget, gotKind = target, kind
		}
		return true
	})
	if newOutCount != 1 {
		t.Fatalf("OutEdges(40) yielded edge 1002 %d times after re-pointing to 40, want exactly 1", newOutCount)
	}
	if gotTarget != n60 || gotKind != 3 {
		t.Fatalf("OutEdges(40)'s edge 1002 = (target %d, kind %d), want (%d, 3)", gotTarget, gotKind, n60)
	}

	newInCount := 0
	v1.InEdges(n60, func(source NodeID, kind KindID, edgeID uint64) bool {
		if edgeID == 1002 {
			newInCount++
		}
		return true
	})
	if newInCount != 1 {
		t.Fatalf("InEdges(60) yielded edge 1002 %d times after re-pointing to 60, want exactly 1", newInCount)
	}

	start, end, kind, ok := v1.EdgeStateByID(1002)
	if !ok || start != n40 || end != n60 || kind != 3 {
		t.Fatalf("EdgeStateByID(1002) = (%d, %d, %d, %v), want (%d, %d, 3, true)", start, end, kind, ok, n40, n60)
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// TestOverlayKindRemovalViaUpsert covers a base node whose delta state drops
// one of its kinds while retaining another: the dropped kind's bitmap bit
// must clear, and the retained kind must still match.
func TestOverlayKindRemovalViaUpsert(t *testing.T) {
	_, v0 := buildOverlayFixture(t)
	n10, _ := v0.Dense(10) // base kind [1]

	// First segment: give node 10 a second kind, simulating a prior commit
	// that left it with both kind 1 and kind 2.
	sb1 := &SegmentBuilder{}
	mustAddNodeState(t, sb1, 10, []KindID{1, 2}, `{"objectid":"S-obj-10","name":"n10"}`)
	v1 := v0.WithSegment(sb1.Build())

	if !v1.NodesOfKind(1).Has(n10) || !v1.NodesOfKind(2).Has(n10) {
		t.Fatal("test setup: node 10 must carry both kinds 1 and 2 before the removal")
	}

	// Second segment: drop kind 1, retaining kind 2.
	sb2 := &SegmentBuilder{}
	mustAddNodeState(t, sb2, 10, []KindID{2}, `{"objectid":"S-obj-10","name":"n10"}`)
	v2 := v1.WithSegment(sb2.Build())

	if v2.NodesOfKind(1).Has(n10) {
		t.Fatal("NodesOfKind(1) still contains node 10 after the delta dropped kind 1")
	}
	if !v2.NodesOfKind(2).Has(n10) {
		t.Fatal("NodesOfKind(2) missing node 10 -- the retained kind must still match")
	}
	if kinds := v2.KindIDsOf(n10); !reflect.DeepEqual(kinds, []KindID{2}) {
		t.Fatalf("KindIDsOf(10) = %v, want [2]", kinds)
	}

	if err := CheckViewConsistent(v2); err != nil {
		t.Fatalf("CheckViewConsistent(v2): %v", err)
	}
}

// TestOverlayObjectIDChangeStaleness covers a base node whose delta bag
// changes its objectid: the OLD value must no longer resolve via
// NodeByObjectID/NodesByObjectID, and the NEW value must resolve to the same
// dense id.
func TestOverlayObjectIDChangeStaleness(t *testing.T) {
	_, v0 := buildOverlayFixture(t)
	n10, _ := v0.Dense(10)

	if got, ok := v0.NodeByObjectID("S-obj-10"); !ok || got != n10 {
		t.Fatalf("test setup: v0.NodeByObjectID(S-obj-10) = (%d, %v), want (%d, true)", got, ok, n10)
	}

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 10, []KindID{1}, `{"objectid":"S-obj-10-NEW","name":"n10"}`)
	v1 := v0.WithSegment(sb.Build())

	if got, ok := v1.NodeByObjectID("S-obj-10"); ok {
		t.Fatalf("NodeByObjectID(S-obj-10) = (%d, true) after the delta changed node 10's objectid, want a miss", got)
	}
	if ids, ok := v1.NodesByObjectID("S-obj-10"); ok {
		t.Fatalf("NodesByObjectID(S-obj-10) = (%v, true) after the objectid changed, want a miss", ids)
	}

	got, ok := v1.NodeByObjectID("S-obj-10-NEW")
	if !ok || got != n10 {
		t.Fatalf("NodeByObjectID(S-obj-10-NEW) = (%d, %v), want (%d, true)", got, ok, n10)
	}
	ids, ok := v1.NodesByObjectID("S-obj-10-NEW")
	if !ok || !reflect.DeepEqual(ids, []NodeID{n10}) {
		t.Fatalf("NodesByObjectID(S-obj-10-NEW) = (%v, %v), want ([%d], true)", ids, ok, n10)
	}

	// The old View must be unaffected.
	if got, ok := v0.NodeByObjectID("S-obj-10"); !ok || got != n10 {
		t.Fatalf("v0.NodeByObjectID(S-obj-10) = (%d, %v), want (%d, true) -- old View must be unaffected", got, ok, n10)
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// TestOverlayEdgeBetweenTwoVirtualNodes covers a delta edge whose BOTH
// endpoints are delta-added (virtual) nodes -- neither has a base forward-
// CSR slot at all, so the edge can only ever be walked through the delta
// adjacency index in both directions.
func TestOverlayEdgeBetweenTwoVirtualNodes(t *testing.T) {
	_, v0 := buildOverlayFixture(t)

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 70, []KindID{1}, `{"objectid":"S-obj-70","name":"n70"}`)
	mustAddNodeState(t, sb, 80, []KindID{1}, `{"objectid":"S-obj-80","name":"n80"}`)
	sb.AddEdgeState(5001, 70, 80, 7)
	v1 := v0.WithSegment(sb.Build())

	n70, ok := v1.Dense(70)
	if !ok {
		t.Fatal("Dense(70) not found")
	}
	n80, ok := v1.Dense(80)
	if !ok {
		t.Fatal("Dense(80) not found")
	}

	foundOut := false
	v1.OutEdges(n70, func(target NodeID, kind KindID, edgeID uint64) bool {
		if target == n80 && kind == 7 && edgeID == 5001 {
			foundOut = true
		}
		return true
	})
	if !foundOut {
		t.Fatal("OutEdges(70) did not walk the delta edge between two virtual nodes")
	}

	foundIn := false
	v1.InEdges(n80, func(source NodeID, kind KindID, edgeID uint64) bool {
		if source == n70 && kind == 7 && edgeID == 5001 {
			foundIn = true
		}
		return true
	})
	if !foundIn {
		t.Fatal("InEdges(80) did not walk the delta edge between two virtual nodes")
	}

	if err := CheckViewConsistent(v1); err != nil {
		t.Fatalf("CheckViewConsistent(v1): %v", err)
	}
}

// TestOverlayConcurrentReadersRace exercises every memoized projection
// (kind bitmaps, Kinds table, edge-tombstone set, delta adjacency index)
// from many goroutines at once against one shared overlay View, so `go test
// -race` can catch a missing/incorrect guard around any of them.
func TestOverlayConcurrentReadersRace(t *testing.T) {
	_, v0 := buildOverlayFixture(t)

	sb := &SegmentBuilder{}
	sb.AddKind(9, "Computer")
	mustAddNodeState(t, sb, 70, []KindID{9, 1}, `{"objectid":"S-obj-70","name":"n70"}`)
	sb.TombstoneNode(40)
	sb.AddEdgeState(4001, 10, 70, 3)
	sb.TombstoneEdge(1002)
	seg := sb.Build()

	v := v0.WithSegment(seg)

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for n := NodeID(0); int(n) < v.NodeCount(); n++ {
				_ = v.Alive(n)
				_ = v.KindIDsOf(n)
				v.OutEdges(n, func(NodeID, KindID, uint64) bool { return true })
				v.InEdges(n, func(NodeID, KindID, uint64) bool { return true })
			}
			for k := KindID(1); k <= 9; k++ {
				v.NodesOfKind(k)
			}
			v.Kinds()
			v.ApproxBytes()
			v.NodesByObjectID("S-obj-70")
		}()
	}
	wg.Wait()

	if err := CheckViewConsistent(v); err != nil {
		t.Fatalf("CheckViewConsistent(v): %v", err)
	}
}

// TestOverlayMaxKindIDCoversDeltaCarriedKinds is a regression test for a
// silent, data-dependent wrong answer: a delta edge (or node) whose kind id
// exceeds the base snapshot's own MaxKindID must raise the View's ceiling
// even when that kind is NOT in the segment's AddedKinds -- which is exactly
// what happens whenever the kind was already registered in the base's kind
// table (asserted in PostgreSQL by some earlier write) while no base row
// carried it.
//
// Without this, every KindMask sized from MaxKindID silently drops the kind
// (KindMask.Set/Has no-op above the ceiling), and a relationship query for it
// serves zero rows for an edge that plainly exists in the View.
func TestOverlayMaxKindIDCoversDeltaCarriedKinds(t *testing.T) {
	base, view := buildOverlayFixture(t)

	// The fixture's own rows only ever carry kinds 1, 2 and 5.
	if base.MaxKindID != 5 {
		t.Fatalf("fixture base MaxKindID = %d, want 5", base.MaxKindID)
	}

	var b SegmentBuilder
	// Deliberately no AddKind call: this mirrors an applier that found both
	// kinds already named by the base kind table and therefore had nothing
	// new to register.
	b.AddEdgeState(2001, 10, 30, 9)
	if err := b.AddNodeState(70, []KindID{7}, []byte(`{"name":"n70"}`)); err != nil {
		t.Fatalf("AddNodeState: %v", err)
	}

	overlay := view.WithSegment(b.Build())

	if got := overlay.MaxKindID(); got != 9 {
		t.Fatalf("overlay MaxKindID = %d, want 9 (the delta edge's own kind)", got)
	}

	mask := NewKindMask(overlay.MaxKindID())
	mask.Set(9)
	if !mask.Has(9) {
		t.Fatalf("a KindMask sized from the overlay ceiling cannot hold the delta edge's kind")
	}
}
