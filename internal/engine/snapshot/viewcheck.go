// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"fmt"
	"reflect"
	"sort"
)

// CheckViewConsistent checks a set of invariants any View -- overlay or not
// -- must uphold, returning the first violation found, or nil if none is.
//
// It is deliberately dependency-light (no *testing.T, no import of
// "testing"): this lives in a plain, exported, non-test file so tests in
// other packages can call it directly against a View they built, without
// pulling "testing" into this package's non-test build. Callers that want
// the usual t.Fatal-on-error shape wrap this themselves:
//
//	if err := snapshot.CheckViewConsistent(v); err != nil {
//		t.Fatal(err)
//	}
//
// Checked invariants:
//
//   - Virtual dense ids are contiguous and round-trip through Dense/GraphID,
//     and are always Alive (a tombstoned effective state is never assigned
//     one -- see View.ensureDelta).
//   - Every kind bitmap NodesOfKind returns (for every kind the merged
//     Kinds table registers) contains only Alive, in-range node ids.
//   - A tombstoned base node's stale objectid never resolves via
//     NodesByObjectID.
//   - OutEdges/InEdges are symmetric: every edge OutEdges(n) yields has a
//     matching entry in InEdges(target), and vice versa.
//   - Alive is decidable (does not panic) for every id in [0, NodeCount()).
func CheckViewConsistent(v *View) error {
	n := v.NodeCount()
	baseN := v.Base().NodeCount()

	alive := make(map[NodeID]bool, n)
	for id := NodeID(0); int(id) < n; id++ {
		alive[id] = v.Alive(id)
	}

	// Virtual ids: dense, contiguous, round-trip via Dense/GraphID, always
	// alive.
	for id := NodeID(baseN); int(id) < n; id++ {
		pgID := v.GraphID(id)
		dense, ok := v.Dense(pgID)
		if !ok || dense != id {
			return fmt.Errorf("snapshot: CheckViewConsistent: virtual node %d (pg id %d): Dense round-trip = (%d, %v), want (%d, true)", id, pgID, dense, ok, id)
		}
		if !alive[id] {
			return fmt.Errorf("snapshot: CheckViewConsistent: virtual node %d (pg id %d) is not Alive, want a virtual id to always be alive", id, pgID)
		}
	}

	// Kind bitmaps must never contain a non-alive or out-of-range node id.
	kt := v.Kinds()
	for id, name := range kindTableIDs(kt) {
		bm := v.NodesOfKind(id)
		var bad NodeID
		var badFound bool
		bm.Iterate(func(nid NodeID) bool {
			if int(nid) >= n || !alive[nid] {
				bad, badFound = nid, true
				return false
			}
			return true
		})
		if badFound {
			return fmt.Errorf("snapshot: CheckViewConsistent: NodesOfKind(%d %q) contains non-alive/out-of-range node %d", id, name, bad)
		}
	}

	// A tombstoned base node's stale objectid must never resolve.
	if objID, ok := v.Base().Props.IDByName("objectid"); ok {
		for id := NodeID(0); int(id) < baseN; id++ {
			if alive[id] {
				continue
			}
			val, ok := v.Base().Props.Value(id, objID)
			if !ok {
				continue
			}
			s, ok := val.(string)
			if !ok || s == "" {
				continue
			}
			ids, found := v.NodesByObjectID(s)
			if !found {
				continue
			}
			for _, got := range ids {
				if got == id {
					return fmt.Errorf("snapshot: CheckViewConsistent: NodesByObjectID(%q) returned tombstoned node %d", s, id)
				}
			}
		}
	}

	// OutEdges/InEdges symmetry, plus duplicate-emission detection. Keys are
	// collected into slices (not inserted straight into a set) so
	// dedupEdgeKeys can tally occurrences: the same node yielding the exact
	// same (target, kind, edgeID) twice from one OutEdges/InEdges call would
	// insert the same key twice, which a bare `set[key] = struct{}{}` would
	// silently collapse to one entry and never flag as the inconsistency it
	// is.
	var outKeys, inKeys []edgeKey
	for id := NodeID(0); int(id) < n; id++ {
		if !alive[id] {
			continue
		}
		v.OutEdges(id, func(target NodeID, kind KindID, edgeID uint64) bool {
			outKeys = append(outKeys, edgeKey{id, target, kind, edgeID})
			return true
		})
	}
	for id := NodeID(0); int(id) < n; id++ {
		if !alive[id] {
			continue
		}
		v.InEdges(id, func(source NodeID, kind KindID, edgeID uint64) bool {
			inKeys = append(inKeys, edgeKey{source, id, kind, edgeID})
			return true
		})
	}

	outSet, err := dedupEdgeKeys(outKeys, "OutEdges")
	if err != nil {
		return err
	}
	inSet, err := dedupEdgeKeys(inKeys, "InEdges")
	if err != nil {
		return err
	}

	for k := range outSet {
		if _, ok := inSet[k]; !ok {
			return fmt.Errorf("snapshot: CheckViewConsistent: OutEdges(%d) yielded edge %d to %d (kind %d) with no matching InEdges entry", k.a, k.edge, k.b, k.kind)
		}
	}
	for k := range inSet {
		if _, ok := outSet[k]; !ok {
			return fmt.Errorf("snapshot: CheckViewConsistent: InEdges(%d) yielded edge %d from %d (kind %d) with no matching OutEdges entry", k.b, k.edge, k.a, k.kind)
		}
	}

	return nil
}

// edgeKey identifies one edge's appearance in an OutEdges/InEdges emission.
// a and b are always the edge's true source and target respectively,
// regardless of which accessor produced the key (OutEdges(a) yields b as
// the target; InEdges(b) yields a as the source) -- that's what lets
// outSet/inSet be compared directly for symmetry above.
type edgeKey struct {
	a, b NodeID
	kind KindID
	edge uint64
}

// dedupEdgeKeys tallies occurrences of each edgeKey in keys and returns the
// deduplicated set, or an error naming the first key seen more than once.
// See CheckViewConsistent's call sites for why counting occurrences, rather
// than inserting straight into a set, is the point: it is what catches a
// double emission -- the same node yielding the exact same edge to the same
// other endpoint more than once in one OutEdges/InEdges call -- that a plain
// set-membership check would silently collapse and miss entirely. label
// names the direction ("OutEdges" or "InEdges") for the error message.
func dedupEdgeKeys(keys []edgeKey, label string) (map[edgeKey]struct{}, error) {
	counts := make(map[edgeKey]int, len(keys))
	for _, k := range keys {
		counts[k]++
	}
	set := make(map[edgeKey]struct{}, len(counts))
	for k, c := range counts {
		if c > 1 {
			return nil, fmt.Errorf("snapshot: CheckViewConsistent: %s yielded edge %d from %d to %d (kind %d) %d times, want at most once", label, k.edge, k.a, k.b, k.kind, c)
		}
		set[k] = struct{}{}
	}
	return set, nil
}

// kindTableIDs returns every (id, name) pair kt registers. Reaches into
// KindTable's unexported names slice directly rather than adding a public
// iteration method to KindTable purely for this checker's benefit -- both
// live in package snapshot.
func kindTableIDs(kt *KindTable) map[KindID]string {
	out := make(map[KindID]string)
	for id, name := range kt.names {
		if name != "" {
			out[KindID(id)] = name
		}
	}
	return out
}

// nodeInfo is one alive node's externally-visible content, keyed by pg id
// rather than dense id so it can be compared across two Views whose
// dense-id spaces differ entirely -- see CheckViewsEquivalent.
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

// sortedKindIDs returns a sorted copy of ks, so a kind-list comparison does
// not depend on incidental ordering.
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

// CheckViewsEquivalent compares two Views' complete externally-visible
// content by DATABASE id rather than dense id: a's and b's dense-id spaces
// may differ entirely -- most notably, a's base was just folded (Fold
// renumbers every dense id from scratch) while b is still an unfolded
// segment stack (which keeps tombstone gaps and virtual ids) -- so every
// comparison below keys off pg ids, never dense ones. Returns the first
// mismatch found as an error, or nil if the two Views are
// content-equivalent.
//
// Exported so a caller outside this package can check a folded View against
// an unfolded reference without duplicating this comparison. Its first use
// outside this package is the engine's own compactor tests, checking a
// post-compaction View against the never-compacted stack it replaced; this
// package's own Fold property test (fold_test.go's compareViewContents)
// wraps this same function rather than keeping a second copy.
func CheckViewsEquivalent(a, b *View) error {
	aliveA := collectAliveByPgID(a)
	aliveB := collectAliveByPgID(b)

	if len(aliveA) != len(aliveB) {
		return fmt.Errorf("snapshot: CheckViewsEquivalent: alive node count mismatch: a=%d b=%d", len(aliveA), len(aliveB))
	}
	for pgID, want := range aliveB {
		got, ok := aliveA[pgID]
		if !ok {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: pg node %d alive in b but missing from a", pgID)
		}
		if !reflect.DeepEqual(sortedKindIDs(got.kinds), sortedKindIDs(want.kinds)) {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: pg node %d kinds mismatch: a=%v b=%v", pgID, got.kinds, want.kinds)
		}
		if !reflect.DeepEqual(got.props, want.props) {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: pg node %d PropNodeMap mismatch: a=%v b=%v", pgID, got.props, want.props)
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
				return fmt.Errorf("snapshot: CheckViewsEquivalent: pg node %d PropValueByName(%q) mismatch: a=(%v,%v) b=(%v,%v)", pgID, name, av, aok, bv, bok)
			}
		}
		// And a name absent from the bag must miss on both sides too.
		if av, aok := a.PropValueByName(na, "definitely-not-a-real-property"); aok {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: pg node %d PropValueByName(unknown) = (%v, true) on a, want a miss", pgID, av)
		}
		if bv, bok := b.PropValueByName(nb, "definitely-not-a-real-property"); bok {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: pg node %d PropValueByName(unknown) = (%v, true) on b, want a miss", pgID, bv)
		}
	}
	for pgID := range aliveA {
		if _, ok := aliveB[pgID]; !ok {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: pg node %d alive in a but not in b -- a has an extra node", pgID)
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
			return fmt.Errorf("snapshot: CheckViewsEquivalent: NodesByObjectID(%q) pg-id set mismatch: a=%v b=%v", oid, gotIDs, wantIDs)
		}
	}

	// Full adjacency, per alive pg node id.
	edgeIDs := make(map[uint64]struct{})
	for pgID := range aliveB {
		na, _ := a.Dense(pgID)
		nb, _ := b.Dense(pgID)

		outA, outB := collectOutTriples(a, na), collectOutTriples(b, nb)
		if !reflect.DeepEqual(outA, outB) {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: OutEdges(pg %d) mismatch: a=%v b=%v", pgID, outA, outB)
		}
		inA, inB := collectInTriples(a, na), collectInTriples(b, nb)
		if !reflect.DeepEqual(inA, inB) {
			return fmt.Errorf("snapshot: CheckViewsEquivalent: InEdges(pg %d) mismatch: a=%v b=%v", pgID, inA, inB)
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
			return fmt.Errorf("snapshot: CheckViewsEquivalent: EdgeStateByID(%d) mismatch: a=(%d,%d,%d,%v) b=(%d,%d,%d,%v)", id, as, ae, ak, aok, bs, be, bk, bok)
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
			return fmt.Errorf("snapshot: CheckViewsEquivalent: Kinds().Name(%d) mismatch: a=(%q,%v) b=(%q,%v)", k, an, aok, bn, bok)
		}
	}

	return nil
}
