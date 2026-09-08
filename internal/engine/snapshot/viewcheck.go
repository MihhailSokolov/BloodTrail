// SPDX-License-Identifier: Apache-2.0
package snapshot

import "fmt"

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

	// OutEdges/InEdges symmetry.
	type edgeKey struct {
		a, b NodeID
		kind KindID
		edge uint64
	}
	outSet := make(map[edgeKey]struct{})
	for id := NodeID(0); int(id) < n; id++ {
		if !alive[id] {
			continue
		}
		v.OutEdges(id, func(target NodeID, kind KindID, edgeID uint64) bool {
			outSet[edgeKey{id, target, kind, edgeID}] = struct{}{}
			return true
		})
	}
	inSet := make(map[edgeKey]struct{})
	for id := NodeID(0); int(id) < n; id++ {
		if !alive[id] {
			continue
		}
		v.InEdges(id, func(source NodeID, kind KindID, edgeID uint64) bool {
			inSet[edgeKey{source, id, kind, edgeID}] = struct{}{}
			return true
		})
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
