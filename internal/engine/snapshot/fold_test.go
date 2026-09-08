// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// ---- randomized fixture generation -----------------------------------

// foldAllKinds is the pool of kind ids the property test's randomized base
// and segments draw from: 1-5 are registered on the base snapshot itself;
// 6 and 7 only ever get registered via a segment's AddKind, so the test also
// exercises Fold's kind-table union.
var foldAllKinds = []KindID{1, 2, 3, 4, 5, 6, 7}

func foldBaseKindTable() map[KindID]string {
	return map[KindID]string{1: "User", 2: "Group", 3: "Computer", 4: "Domain", 5: "OU"}
}

// randKindSubset returns 1 or 2 distinct kinds drawn from pool.
func randKindSubset(rng *rand.Rand, pool []KindID) []KindID {
	n := 1 + rng.Intn(2)
	out := make([]KindID, 0, n)
	seen := make(map[KindID]bool, n)
	for len(out) < n {
		k := pool[rng.Intn(len(pool))]
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}
	return out
}

// randPropsJSON returns a property bag JSON for id: a deterministic
// objectid ("OBJ-<id>") and name (so repeated upserts of the same id keep
// the same objectid unless a test deliberately changes it), plus randomized
// number/bool fields and occasional array/object/null values so every JSON
// value kind propEntry can carry gets exercised.
func randPropsJSON(rng *rand.Rand, id uint64) string {
	return randPropsJSONWithObjectID(rng, id, fmt.Sprintf("OBJ-%d", id))
}

// randPropsJSONWithObjectID is randPropsJSON with the objectid value
// overridden to objectID instead of the default "OBJ-<id>" -- used by
// buildRandomSegments to occasionally converge two different node ids onto
// the same objectid, so the randomized property test also exercises
// PropStore.objectIndexDup / View.NodesByObjectID's multi-match path (see
// compareViewContents' objectid section, which already resolves
// NodesByObjectID as a sorted pg-id SET rather than a single witness, so a
// collision here needs no comparator changes). Deterministic, always-on
// collision coverage lives in TestFoldObjectIDCollisionAcrossDifferentNodes
// below; this is best-effort extra fuzzing on top, not load-bearing.
func randPropsJSONWithObjectID(rng *rand.Rand, id uint64, objectID string) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, `{"objectid":%q,"name":"name-%d","score":%f,"enabled":%v`,
		objectID, id, rng.Float64()*100, rng.Intn(2) == 0)
	if rng.Intn(3) == 0 {
		fmt.Fprintf(&buf, `,"tags":["a","b","t%d"]`, rng.Intn(10))
	}
	if rng.Intn(4) == 0 {
		fmt.Fprintf(&buf, `,"meta":{"k":%d}`, rng.Intn(100))
	}
	if rng.Intn(5) == 0 {
		buf.WriteString(`,"note":null`)
	}
	buf.WriteByte('}')
	return buf.String()
}

// randObjectID returns the default objectid for id, unless it randomly
// decides (roughly 1 in 12) to instead reuse a randomly chosen id's from
// pool -- deliberately colliding two different node ids onto one objectid
// value. pool may be empty, in which case the default is always used.
func randObjectID(rng *rand.Rand, id uint64, pool []uint64) string {
	if len(pool) > 0 && rng.Intn(12) == 0 {
		return fmt.Sprintf("OBJ-%d", pool[rng.Intn(len(pool))])
	}
	return fmt.Sprintf("OBJ-%d", id)
}

// buildRandomBaseSnapshot builds a seeded random base Snapshot with
// nodeCount nodes (strictly ascending ids with realistic bigserial gaps) and
// edgeCount edges among them, returning it alongside its node ids (ascending)
// and its edge ids for the caller's segment generator to reference.
func buildRandomBaseSnapshot(t *testing.T, rng *rand.Rand, nodeCount, edgeCount int) (*Snapshot, []uint64, []uint64) {
	t.Helper()

	b := NewBuilder(1)
	b.SetKinds(foldBaseKindTable())

	ids := make([]uint64, nodeCount)
	id := uint64(1000)
	for i := range ids {
		id += uint64(1 + rng.Intn(4))
		ids[i] = id
		kinds := randKindSubset(rng, []KindID{1, 2, 3, 4, 5})
		if err := b.AddNode(id, kinds, []byte(randPropsJSON(rng, id))); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
	}

	edgeIDs := make([]uint64, edgeCount)
	for i := 0; i < edgeCount; i++ {
		src := ids[rng.Intn(len(ids))]
		dst := ids[rng.Intn(len(ids))]
		kind := KindID(10 + rng.Intn(3))
		edgeID := uint64(i + 1)
		edgeIDs[i] = edgeID
		b.AddEdge(edgeID, src, dst, kind)
	}

	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return s, ids, edgeIDs
}

// buildRandomSegments builds 3 randomized segments layering upserts (with
// changed kinds/props/objectids), tombstones (including nodes with base
// edges, to exercise the adjacency cascade), new nodes, new edges (including
// edges touching the new nodes), edge tombstones, and added kinds, on top of
// a base built by buildRandomBaseSnapshot. Segments share running state
// (which node ids are currently live, the next id to allocate) so later
// segments can override or tombstone what earlier segments just wrote.
func buildRandomSegments(t *testing.T, rng *rand.Rand, baseIDs, baseEdgeIDs []uint64) []*Segment {
	t.Helper()

	nextNewID := baseIDs[len(baseIDs)-1] + 100000 // safely above every base id
	nextEdgeID := uint64(900000)                  // safely above every base edge id
	liveIDs := append([]uint64(nil), baseIDs...)

	segs := make([]*Segment, 3)
	for s := 0; s < 3; s++ {
		sb := &SegmentBuilder{}
		tombstonedThisSeg := make(map[uint64]bool)

		// Upserts: ~15 currently-live ids get a changed kind list and/or
		// property bag (score/enabled/tags/meta vary per call; objectid/name
		// are normally a deterministic function of the id, but randObjectID
		// occasionally converges this upsert's objectid onto another
		// currently-live id's -- two upserts converging on one value, per
		// the objectid-collision finding).
		for i := 0; i < 15 && len(liveIDs) > 0; i++ {
			id := liveIDs[rng.Intn(len(liveIDs))]
			objID := randObjectID(rng, id, liveIDs)
			mustAddNodeState(t, sb, id, randKindSubset(rng, foldAllKinds), randPropsJSONWithObjectID(rng, id, objID))
		}

		// Tombstones: ~5 currently-live ids, deliberately drawn from the same
		// pool as the upserts above so a same-segment upsert-then-tombstone
		// (last write wins) gets exercised too.
		for i := 0; i < 5 && len(liveIDs) > 0; i++ {
			id := liveIDs[rng.Intn(len(liveIDs))]
			sb.TombstoneNode(id)
			tombstonedThisSeg[id] = true
		}

		// One new kind per the first two segments.
		switch s {
		case 0:
			sb.AddKind(6, "GPO")
		case 1:
			sb.AddKind(7, "Container")
		}

		// New nodes: ids all comfortably above every base id. randObjectID
		// occasionally has a brand-new node reuse a currently-live id's
		// objectid -- a delta-added node colliding with a still-live node,
		// per the objectid-collision finding.
		newIDs := make([]uint64, 0, 5)
		for i := 0; i < 5; i++ {
			nextNewID++
			nid := nextNewID
			objID := randObjectID(rng, nid, liveIDs)
			mustAddNodeState(t, sb, nid, randKindSubset(rng, foldAllKinds), randPropsJSONWithObjectID(rng, nid, objID))
			newIDs = append(newIDs, nid)
		}

		// Live pool for the NEXT segment: this segment's survivors (not
		// tombstoned here) plus its new nodes.
		nextLive := make([]uint64, 0, len(liveIDs)+len(newIDs))
		for _, id := range liveIDs {
			if !tombstonedThisSeg[id] {
				nextLive = append(nextLive, id)
			}
		}
		nextLive = append(nextLive, newIDs...)

		// New edges among this segment's live pool (base survivors, prior
		// segments' new nodes, and this segment's own new nodes), so some
		// land on brand-new nodes.
		for i := 0; i < 20; i++ {
			nextEdgeID++
			src := nextLive[rng.Intn(len(nextLive))]
			dst := nextLive[rng.Intn(len(nextLive))]
			sb.AddEdgeState(nextEdgeID, src, dst, KindID(10+rng.Intn(5)))
		}

		// Tombstone a few base edges.
		for i := 0; i < 3 && len(baseEdgeIDs) > 0; i++ {
			sb.TombstoneEdge(baseEdgeIDs[rng.Intn(len(baseEdgeIDs))])
		}

		segs[s] = sb.Build()
		liveIDs = nextLive
	}
	return segs
}

// ---- reusable View-content comparator ---------------------------------

// nodeInfo is one alive node's externally-visible content, keyed by pg id
// rather than dense id so it can be compared across two Views whose
// dense-id spaces differ entirely.
type nodeInfo struct {
	kinds []KindID
	props map[string]any
}

// collectAliveByPgID walks every dense id in v and returns the alive ones'
// content keyed by database id.
func collectAliveByPgID(v *View) map[uint64]nodeInfo {
	out := make(map[uint64]nodeInfo, v.NodeCount())
	for n := NodeID(0); int(n) < v.NodeCount(); n++ {
		if !v.Alive(n) {
			continue
		}
		out[v.GraphID(n)] = nodeInfo{kinds: v.KindIDsOf(n), props: v.PropNodeMap(n)}
	}
	return out
}

func sortedKindIDs(ks []KindID) []KindID {
	out := append([]KindID(nil), ks...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// objectIDPgIDs resolves objectID through v and returns the matching nodes'
// database ids, sorted ascending (nil if there is no match).
func objectIDPgIDs(v *View, objectID string) []uint64 {
	ids, ok := v.NodesByObjectID(objectID)
	if !ok {
		return nil
	}
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = v.GraphID(id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// edgeTriple identifies one edge from one endpoint's perspective, with the
// OTHER endpoint already resolved to its database id.
type edgeTriple struct {
	edgeID uint64
	kind   KindID
	other  uint64
}

func sortEdgeTriples(triples []edgeTriple) {
	sort.Slice(triples, func(i, j int) bool {
		if triples[i].edgeID != triples[j].edgeID {
			return triples[i].edgeID < triples[j].edgeID
		}
		if triples[i].other != triples[j].other {
			return triples[i].other < triples[j].other
		}
		return triples[i].kind < triples[j].kind
	})
}

func collectOutTriples(v *View, n NodeID) []edgeTriple {
	var out []edgeTriple
	v.OutEdges(n, func(target NodeID, kind KindID, edgeID uint64) bool {
		out = append(out, edgeTriple{edgeID: edgeID, kind: kind, other: v.GraphID(target)})
		return true
	})
	sortEdgeTriples(out)
	return out
}

func collectInTriples(v *View, n NodeID) []edgeTriple {
	var out []edgeTriple
	v.InEdges(n, func(source NodeID, kind KindID, edgeID uint64) bool {
		out = append(out, edgeTriple{edgeID: edgeID, kind: kind, other: v.GraphID(source)})
		return true
	})
	sortEdgeTriples(out)
	return out
}

// edgeStateByPgID is View.EdgeStateByID with both endpoints resolved to
// database ids, so its result is comparable across two Views with different
// dense-id spaces.
func edgeStateByPgID(v *View, id uint64) (startPg, endPg uint64, kind KindID, ok bool) {
	s, e, k, ok := v.EdgeStateByID(id)
	if !ok {
		return 0, 0, 0, false
	}
	return v.GraphID(s), v.GraphID(e), k, true
}

// compareViewContents compares two Views' complete externally-visible
// content by DATABASE id rather than dense id: a's and b's dense-id spaces
// may differ entirely (a folded Snapshot renumbers; a stacked overlay View
// keeps tombstone gaps and virtual ids), so every comparison below keys off
// pg ids, never dense ones. Fails the test on the first mismatch found.
// Reusable by any caller wanting a folded snapshot checked content-equivalent
// to an accumulated segment stack.
func compareViewContents(t *testing.T, a, b *View) {
	t.Helper()

	aliveA := collectAliveByPgID(a)
	aliveB := collectAliveByPgID(b)

	if len(aliveA) != len(aliveB) {
		t.Fatalf("alive node count mismatch: a=%d b=%d", len(aliveA), len(aliveB))
	}
	for pgID, want := range aliveB {
		got, ok := aliveA[pgID]
		if !ok {
			t.Fatalf("pg node %d alive in b but missing from a", pgID)
		}
		if !reflect.DeepEqual(sortedKindIDs(got.kinds), sortedKindIDs(want.kinds)) {
			t.Fatalf("pg node %d kinds mismatch: a=%v b=%v", pgID, got.kinds, want.kinds)
		}
		if !reflect.DeepEqual(got.props, want.props) {
			t.Fatalf("pg node %d PropNodeMap mismatch: a=%v b=%v", pgID, got.props, want.props)
		}

		// PropValueByName is a separate lookup path from PropNodeMap (a
		// per-property binary search over the node's entries, rather than a
		// blind full-bag iteration -- see PropStore.Value/decode), so it is
		// checked independently rather than assumed to agree just because
		// the full maps above already matched.
		na, _ := a.Dense(pgID)
		nb, _ := b.Dense(pgID)
		for name := range want.props {
			av, aok := a.PropValueByName(na, name)
			bv, bok := b.PropValueByName(nb, name)
			if aok != bok || !reflect.DeepEqual(av, bv) {
				t.Fatalf("pg node %d PropValueByName(%q) mismatch: a=(%v,%v) b=(%v,%v)", pgID, name, av, aok, bv, bok)
			}
		}
		// And a name absent from the bag must miss on both sides too.
		if av, aok := a.PropValueByName(na, "definitely-not-a-real-property"); aok {
			t.Fatalf("pg node %d PropValueByName(unknown) = (%v, true) on a, want a miss", pgID, av)
		}
		if bv, bok := b.PropValueByName(nb, "definitely-not-a-real-property"); bok {
			t.Fatalf("pg node %d PropValueByName(unknown) = (%v, true) on b, want a miss", pgID, bv)
		}
	}
	for pgID := range aliveA {
		if _, ok := aliveB[pgID]; !ok {
			t.Fatalf("pg node %d alive in a but not in b -- a has an extra node", pgID)
		}
	}

	// objectid resolution, for every objectid value observed on either side.
	objectIDs := make(map[string]struct{})
	for _, info := range aliveA {
		if v, ok := info.props["objectid"].(string); ok && v != "" {
			objectIDs[v] = struct{}{}
		}
	}
	for _, info := range aliveB {
		if v, ok := info.props["objectid"].(string); ok && v != "" {
			objectIDs[v] = struct{}{}
		}
	}
	for oid := range objectIDs {
		gotIDs := objectIDPgIDs(a, oid)
		wantIDs := objectIDPgIDs(b, oid)
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Fatalf("NodesByObjectID(%q) pg-id set mismatch: a=%v b=%v", oid, gotIDs, wantIDs)
		}
	}

	// Full adjacency, per alive pg node id.
	edgeIDs := make(map[uint64]struct{})
	for pgID := range aliveB {
		na, _ := a.Dense(pgID)
		nb, _ := b.Dense(pgID)

		outA, outB := collectOutTriples(a, na), collectOutTriples(b, nb)
		if !reflect.DeepEqual(outA, outB) {
			t.Fatalf("OutEdges(pg %d) mismatch: a=%v b=%v", pgID, outA, outB)
		}
		inA, inB := collectInTriples(a, na), collectInTriples(b, nb)
		if !reflect.DeepEqual(inA, inB) {
			t.Fatalf("InEdges(pg %d) mismatch: a=%v b=%v", pgID, inA, inB)
		}
		for _, tr := range outB {
			edgeIDs[tr.edgeID] = struct{}{}
		}
	}

	// EdgeStateByID for every live edge id observed above.
	for id := range edgeIDs {
		as, ae, ak, aok := edgeStateByPgID(a, id)
		bs, be, bk, bok := edgeStateByPgID(b, id)
		if aok != bok || as != bs || ae != be || ak != bk {
			t.Fatalf("EdgeStateByID(%d) mismatch: a=(%d,%d,%d,%v) b=(%d,%d,%d,%v)", id, as, ae, ak, aok, bs, be, bk, bok)
		}
	}

	// Kinds table equality over every id either side's ceiling reaches.
	maxKind := a.MaxKindID()
	if bmax := b.MaxKindID(); bmax > maxKind {
		maxKind = bmax
	}
	for k := KindID(0); k <= maxKind; k++ {
		an, aok := a.Kinds().Name(k)
		bn, bok := b.Kinds().Name(k)
		if aok != bok || an != bn {
			t.Fatalf("Kinds().Name(%d) mismatch: a=(%q,%v) b=(%q,%v)", k, an, aok, bn, bok)
		}
	}
}

// ---- the property test --------------------------------------------------

// TestFoldMatchesStackedOverlay is the brief's core property test: a
// randomized 200-node/600-edge base with 3 randomized segments layered on
// top (upserts, tombstones incl. cascade-relevant node deletes, new nodes,
// new edges incl. edges to new nodes, added kinds) must fold into a
// Snapshot whose View is content-equivalent, by every accessor, to the
// stacked base+segments View -- even though the two Views' dense-id spaces
// are completely different (folded renumbers everything; stacked keeps
// tombstone gaps and virtual ids).
func TestFoldMatchesStackedOverlay(t *testing.T) {
	rng := rand.New(rand.NewSource(20260908))

	base, baseIDs, baseEdgeIDs := buildRandomBaseSnapshot(t, rng, 200, 600)
	segs := buildRandomSegments(t, rng, baseIDs, baseEdgeIDs)

	folded, err := Fold(base, segs)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}

	stacked := NewView(base)
	for _, seg := range segs {
		stacked = stacked.WithSegment(seg)
	}
	foldedView := NewView(folded)

	if foldedView.Overlay() {
		t.Fatal("NewView(folded).Overlay() = true, want false -- Fold's output must be a plain base snapshot with no segments of its own")
	}

	compareViewContents(t, foldedView, stacked)

	if err := CheckViewConsistent(foldedView); err != nil {
		t.Fatalf("CheckViewConsistent(foldedView): %v", err)
	}
	if err := CheckViewConsistent(stacked); err != nil {
		t.Fatalf("CheckViewConsistent(stacked): %v", err)
	}
}

// ---- targeted unit tests -------------------------------------------------

// TestFoldNoSegmentsProducesEquivalentSnapshot covers Fold(base, nil):
// folding an empty delta must produce a fresh snapshot that is
// content-equivalent to the base itself.
func TestFoldNoSegmentsProducesEquivalentSnapshot(t *testing.T) {
	base, _ := buildOverlayFixture(t)

	folded, err := Fold(base, nil)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}

	compareViewContents(t, NewView(folded), NewView(base))
	if err := CheckViewConsistent(NewView(folded)); err != nil {
		t.Fatalf("CheckViewConsistent: %v", err)
	}
}

// containsUint64 reports whether want appears anywhere in ids.
func containsUint64(ids []uint64, want uint64) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestFoldObjectIDCollisionAcrossDifferentNodes covers what the randomized
// property test's randPropsJSON generator alone can never guarantee (it
// derives every node's default objectid from its own database id, so two
// different ids collide only on randObjectID's roughly-1-in-12 draw): TWO
// alive nodes sharing one objectid value in Fold's output. Fold itself has
// no objectid-specific logic anywhere (see fold.go's doc: every property bag
// it transplants was already parsed once, and addPreparedNode just copies
// propEntry bytes) -- so a collision surviving correctly is really proving
// that PropStore.objectIndexDup (buildPropStore, props.go) gets built
// correctly from FOLDED output, not that Fold "knows" about objectid at all.
//
// Two ways a collision can arise are both covered, in the same delta:
//   - a delta-added node (70) reuses a still-live BASE node's (10) untouched
//     objectid "S-obj-10";
//   - two delta UPSERTS (nodes 20 and 30) both write the same new value
//     "DUP-CONVERGE", converging from two previously-distinct base values.
func TestFoldObjectIDCollisionAcrossDifferentNodes(t *testing.T) {
	base, _ := buildOverlayFixture(t) // nodes 10..60, each carrying "S-obj-<id>"

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 70, []KindID{1}, `{"objectid":"S-obj-10","name":"n70"}`)
	mustAddNodeState(t, sb, 20, []KindID{1}, `{"objectid":"DUP-CONVERGE","name":"n20"}`)
	mustAddNodeState(t, sb, 30, []KindID{2}, `{"objectid":"DUP-CONVERGE","name":"n30"}`)
	seg := sb.Build()

	folded, err := Fold(base, []*Segment{seg})
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	foldedView := NewView(folded)
	stacked := NewView(base).WithSegment(seg)

	if foldedView.Overlay() {
		t.Fatal("NewView(folded).Overlay() = true, want false")
	}

	// The full comparator's own "objectid resolution" section already
	// resolves NodesByObjectID as a SORTED PG-ID SET on both sides (see
	// objectIDPgIDs), never a single witness -- so this call alone
	// re-verifies both collisions agree between the folded and the stacked
	// overlay view.
	compareViewContents(t, foldedView, stacked)

	// Belt-and-suspenders: name the exact expected sets from the finding's
	// own wording, and confirm a multi-match is not itself flagged as an
	// invariant violation by either view.
	cases := []struct {
		objectID  string
		wantPgIDs []uint64
	}{
		{"S-obj-10", []uint64{10, 70}},
		{"DUP-CONVERGE", []uint64{20, 30}},
	}
	for _, tc := range cases {
		gotFolded := objectIDPgIDs(foldedView, tc.objectID)
		if !reflect.DeepEqual(gotFolded, tc.wantPgIDs) {
			t.Fatalf("folded NodesByObjectID(%q) pg ids = %v, want %v", tc.objectID, gotFolded, tc.wantPgIDs)
		}
		gotStacked := objectIDPgIDs(stacked, tc.objectID)
		if !reflect.DeepEqual(gotStacked, tc.wantPgIDs) {
			t.Fatalf("stacked NodesByObjectID(%q) pg ids = %v, want %v", tc.objectID, gotStacked, tc.wantPgIDs)
		}

		// NodeByObjectID (single witness): witness selection differs
		// between a folded plain PropStore's "arbitrary member" (see
		// PropStore.NodeByObjectID's doc) and an overlay View's
		// deterministic-sort witness (see View.NodeByObjectID's doc) by
		// documented design, so this does NOT assert the two witnesses
		// equal EACH OTHER -- only that folded's witness resolves to SOME
		// member of the same pg-id set.
		n, ok := foldedView.NodeByObjectID(tc.objectID)
		if !ok {
			t.Fatalf("folded NodeByObjectID(%q) = (_, false), want a hit", tc.objectID)
		}
		if pg := foldedView.GraphID(n); !containsUint64(tc.wantPgIDs, pg) {
			t.Fatalf("folded NodeByObjectID(%q) resolved to pg %d, want one of %v", tc.objectID, pg, tc.wantPgIDs)
		}
	}

	if err := CheckViewConsistent(foldedView); err != nil {
		t.Fatalf("CheckViewConsistent(foldedView): %v", err)
	}
	if err := CheckViewConsistent(stacked); err != nil {
		t.Fatalf("CheckViewConsistent(stacked): %v", err)
	}
}

// TestFoldObjectIDValueChangeAcrossOverride closes the reviewer's Minor
// finding for Fold specifically (an equivalent check already exists for a
// bare overlay View, not folded output, in
// TestOverlayObjectIDChangeStaleness in view_overlay_test.go): a delta
// upsert that changes a base node's objectid must, after folding, make the
// OLD value resolve to nothing and the NEW value resolve to that node. Fold's
// generic addPreparedNode transplant has no objectid-specific logic, so this
// proves buildPropStore's objectIndex construction (props.go) runs correctly
// against FOLDED output specifically.
func TestFoldObjectIDValueChangeAcrossOverride(t *testing.T) {
	base, _ := buildOverlayFixture(t) // node 10 carries "S-obj-10"

	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 10, []KindID{1}, `{"objectid":"S-obj-10-NEW","name":"n10"}`)
	seg := sb.Build()

	folded, err := Fold(base, []*Segment{seg})
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	foldedView := NewView(folded)
	pg10, ok := foldedView.Dense(10)
	if !ok {
		t.Fatalf("folded Dense(10) = (_, false), want ok")
	}

	if got, ok := foldedView.NodeByObjectID("S-obj-10"); ok {
		t.Fatalf("folded NodeByObjectID(S-obj-10) = (%d, true) after the override changed node 10's objectid, want a miss", got)
	}
	if ids, ok := foldedView.NodesByObjectID("S-obj-10"); ok {
		t.Fatalf("folded NodesByObjectID(S-obj-10) = (%v, true), want a miss", ids)
	}

	got, ok := foldedView.NodeByObjectID("S-obj-10-NEW")
	if !ok || got != pg10 {
		t.Fatalf("folded NodeByObjectID(S-obj-10-NEW) = (%d, %v), want (%d, true)", got, ok, pg10)
	}
	ids, ok := foldedView.NodesByObjectID("S-obj-10-NEW")
	if !ok || !reflect.DeepEqual(ids, []NodeID{pg10}) {
		t.Fatalf("folded NodesByObjectID(S-obj-10-NEW) = (%v, %v), want ([%d], true)", ids, ok, pg10)
	}

	// The base snapshot Fold was given, and a fresh View over it, must be
	// entirely unaffected by folding a segment on top of it -- Fold never
	// mutates its inputs.
	baseView := NewView(base)
	if got, ok := baseView.NodeByObjectID("S-obj-10"); !ok || baseView.GraphID(got) != 10 {
		t.Fatalf("base NodeByObjectID(S-obj-10) = (%d, %v), want it still resolving to pg 10 -- Fold must not mutate its base input", got, ok)
	}

	if err := CheckViewConsistent(foldedView); err != nil {
		t.Fatalf("CheckViewConsistent(foldedView): %v", err)
	}
}

// TestFoldAllNodesTombstoned covers a delta that tombstones every base node:
// the folded snapshot must come out completely empty (nodes AND the edges
// that depended on them), not merely empty of nodes.
func TestFoldAllNodesTombstoned(t *testing.T) {
	base, _ := buildOverlayFixture(t)

	sb := &SegmentBuilder{}
	for _, id := range []uint64{10, 20, 30, 40, 50, 60} {
		sb.TombstoneNode(id)
	}
	seg := sb.Build()

	folded, err := Fold(base, []*Segment{seg})
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}
	if got := folded.NodeCount(); got != 0 {
		t.Fatalf("NodeCount() = %d, want 0", got)
	}
	if got := folded.EdgeCount(); got != 0 {
		t.Fatalf("EdgeCount() = %d, want 0 (every edge's endpoints were tombstoned)", got)
	}
	if err := CheckViewConsistent(NewView(folded)); err != nil {
		t.Fatalf("CheckViewConsistent: %v", err)
	}
}

// TestFoldAscendingIDViolationErrors covers a delta that stages a "new" node
// id that does not exceed the base's own max id -- a bigserial monotonicity
// violation Fold must reject by name rather than silently miscompact.
func TestFoldAscendingIDViolationErrors(t *testing.T) {
	base, _ := buildOverlayFixture(t) // max base id is 60
	sb := &SegmentBuilder{}
	mustAddNodeState(t, sb, 45, []KindID{1}, `{}`) // 45 < 60 and not a base id
	seg := sb.Build()

	_, err := Fold(base, []*Segment{seg})
	if err == nil {
		t.Fatal("Fold: want an error for a delta-added node id not exceeding the base's max id, got nil")
	}
	if !strings.Contains(err.Error(), "45") {
		t.Fatalf("Fold error = %v, want it to name the violating id 45", err)
	}
}

// bigPropsJSON returns a JSON object with n property keys "<prefix>0" ..
// "<prefix><n-1>", each holding its own index as a number value -- used by
// TestFoldInternCapErrorPropagates to build property bags with a large but
// controlled number of distinct property names.
func bigPropsJSON(prefix string, n int) string {
	var buf strings.Builder
	buf.WriteByte('{')
	for i := 0; i < n; i++ {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `"%s%d":%d`, prefix, i, i)
	}
	buf.WriteByte('}')
	return buf.String()
}

// TestFoldInternCapErrorPropagates covers the intern-cap guard firing
// specifically INSIDE Fold's own fresh Builder: base node 10 alone carries
// 40000 distinct property names (comfortably under the 65536 per-Builder
// cap, so base's own Build succeeds), and a segment overrides base node 20
// with 30000 DIFFERENT distinct names (comfortably under the cap for the
// SegmentBuilder that built it too). Neither source Builder ever sees more
// than 65536 names, but Fold's single fresh Builder re-interns both sets
// into ONE shared table -- 70000 distinct names total, over the cap -- and
// must fail loudly rather than let PropID wrap and silently alias two
// different property names.
func TestFoldInternCapErrorPropagates(t *testing.T) {
	b := NewBuilder(1)
	if err := b.AddNode(10, []KindID{1}, []byte(bigPropsJSON("b", 40000))); err != nil {
		t.Fatalf("AddNode(10): %v", err)
	}
	if err := b.AddNode(20, []KindID{1}, nil); err != nil {
		t.Fatalf("AddNode(20): %v", err)
	}
	base, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	sb := &SegmentBuilder{}
	if err := sb.AddNodeState(20, []KindID{1}, []byte(bigPropsJSON("s", 30000))); err != nil {
		t.Fatalf("SegmentBuilder.AddNodeState: %v", err)
	}
	seg := sb.Build()

	if _, err := Fold(base, []*Segment{seg}); err == nil {
		t.Fatal("Fold: want an intern-cap error propagated from addPreparedNode (40000 base names + 30000 new segment names > 65536), got nil")
	}
}
