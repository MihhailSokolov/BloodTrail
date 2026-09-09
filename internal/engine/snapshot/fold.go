// SPDX-License-Identifier: Apache-2.0
package snapshot

import "fmt"

// Fold merges base and every delta segment in segments into a completely
// fresh, immutable Snapshot -- the in-memory compaction core that replaces
// an aging base+overlay View with a single flat snapshot again, without
// ever touching PostgreSQL or re-parsing a single byte of JSON: every
// property bag Fold transplants was already parsed and validated once,
// either by the original load (base.Props) or by the commit path that built
// each segment (SegmentBuilder.commitProps) -- see Builder.addPreparedNode,
// the mechanism that makes this possible.
//
// segments is collapsed once via MergeSegments (oldest first, newest wins --
// see its own doc) before folding, so the rest of Fold's logic never has to
// reason about more than one effective state per touched id.
//
// The result satisfies every Builder invariant: fresh backing arrays end to
// end -- no aliasing into base's or any segment's mutable structures, see
// addPreparedNode's doc for how property bags in particular avoid it --
// derived indexes rebuilt from scratch by Build, and nodes staged in
// strictly ascending database-id order. base's own MultiGraph flag is
// carried over (informational metadata about the source database, unrelated
// to this fold).
func Fold(base *Snapshot, segments []*Segment) (*Snapshot, error) {
	merged := MergeSegments(segments)

	b := NewBuilder(base.GraphID)
	b.SetKinds(foldKindPairs(base.Kinds, merged.AddedKinds()))

	if err := foldBaseNodes(b, base, merged); err != nil {
		return nil, err
	}
	if err := foldAddedNodes(b, base, merged); err != nil {
		return nil, err
	}
	foldEdges(b, base, merged)

	folded, err := b.Build()
	if err != nil {
		return nil, err
	}
	folded.MultiGraph = base.MultiGraph
	return folded, nil
}

// foldKindPairs returns base's own kind id<->name pairs plus added (the
// merged delta's AddedKinds), with added taking precedence on a collision --
// mirroring View.Kinds' identical merge exactly, since Fold needs the same
// union to hand Builder.SetKinds.
func foldKindPairs(base *KindTable, added map[KindID]string) map[KindID]string {
	pairs := make(map[KindID]string, len(base.names)+len(added))
	for id, name := range base.names {
		if name != "" {
			pairs[KindID(id)] = name
		}
	}
	for id, name := range added {
		pairs[id] = name
	}
	return pairs
}

// foldBaseNodes stages every base node the merged delta didn't tombstone, in
// base's own ascending dense-id (== ascending database-id) order, which
// satisfies Builder's own ascending-id staging contract for free. A
// delta-overridden node's kinds and property bag come from its effective
// NodeSegState, transplanted via addPreparedNode from the ORIGINATING
// segment's own name table and arena (see NodeSegState's doc on why seg
// keeps pointing there through a merge); an untouched node's kinds and bag
// are transplanted verbatim from base itself, via the same addPreparedNode
// path.
func foldBaseNodes(b *Builder, base *Snapshot, merged *Segment) error {
	for i := 0; i < base.NodeCount(); i++ {
		pgID := base.GraphIDs[i]

		if st, overridden := merged.NodeState(pgID); overridden {
			if st.Tombstoned {
				continue
			}
			props := preparedProps{entries: st.entries, names: st.seg.names, arena: st.seg.arena}
			if err := b.addPreparedNode(pgID, st.KindIDs, props); err != nil {
				return fmt.Errorf("snapshot: Fold: node %d (delta-overridden): %w", pgID, err)
			}
			continue
		}

		lo, hi := base.Props.nodeOffsets[i], base.Props.nodeOffsets[i+1]
		kLo, kHi := base.KindOffsets[i], base.KindOffsets[i+1]
		props := preparedProps{entries: base.Props.entries[lo:hi], names: base.Props.names, arena: base.Props.arena}
		if err := b.addPreparedNode(pgID, base.NodeKinds[kLo:kHi], props); err != nil {
			return fmt.Errorf("snapshot: Fold: node %d (base): %w", pgID, err)
		}
	}
	return nil
}

// foldAddedNodes stages every node the merged delta introduces that base
// never had at all. Their database ids must all be strictly greater than
// every base node's, by PostgreSQL's bigserial monotonicity -- a brand-new
// row can never reuse an id the sequence already handed out, including one
// belonging to a row deleted before the base snapshot was taken. Rather than
// assume this holds, foldAddedNodes asserts it: a violation means something
// upstream already broke the invariant, and Fold must fail loudly, naming
// the offending id, rather than silently produce a corrupt snapshot -- see
// Builder.addParsedNode's own strictly-ascending guard, which this mirrors
// one layer up.
//
// merged.IterNodes already walks in ascending pg-id order, so no separate
// sort is needed here either.
func foldAddedNodes(b *Builder, base *Snapshot, merged *Segment) error {
	var maxBaseID uint64
	if n := base.NodeCount(); n > 0 {
		maxBaseID = base.GraphIDs[n-1]
	}

	var stageErr error
	merged.IterNodes(func(pgID uint64, st NodeSegState) bool {
		if st.Tombstoned {
			return true
		}
		if _, ok := base.Dense(pgID); ok {
			return true // already staged by foldBaseNodes as a delta-overridden base node
		}
		if pgID <= maxBaseID {
			stageErr = fmt.Errorf("snapshot: Fold: delta-added node %d does not exceed the base snapshot's max database id %d (bigserial monotonicity violated)", pgID, maxBaseID)
			return false
		}
		props := preparedProps{entries: st.entries, names: st.seg.names, arena: st.seg.arena}
		if err := b.addPreparedNode(pgID, st.KindIDs, props); err != nil {
			stageErr = fmt.Errorf("snapshot: Fold: node %d (delta-added): %w", pgID, err)
			return false
		}
		return true
	})
	return stageErr
}

// foldEdges stages every base edge the merged delta didn't tombstone or
// override, plus every one of the merged delta's own non-tombstoned edges
// (added or overriding). Builder.AddEdge accepts edges in any order and
// resolves + sorts them at Build time, so foldEdges doesn't need to reason
// about ordering at all -- and Build's own tolerant edge resolution (an edge
// whose endpoint was never staged, e.g. because it was tombstoned, is
// dropped rather than failing the build) is exactly the "dangling delta
// edge" tolerance the loader and View.OutEdges/InEdges already document, so
// foldEdges doesn't need to duplicate that check itself.
func foldEdges(b *Builder, base *Snapshot, merged *Segment) {
	touched := make(map[uint64]struct{}, merged.EdgeCount())
	merged.IterEdges(func(id uint64, _ EdgeSegState) bool {
		touched[id] = struct{}{}
		return true
	})

	for n := 0; n < base.NodeCount(); n++ {
		lo, hi := base.OutOffsets[n], base.OutOffsets[n+1]
		for i := lo; i < hi; i++ {
			id := base.OutEdgeIDs[i]
			if _, skip := touched[id]; skip {
				continue
			}
			b.AddEdge(id, base.GraphIDs[n], base.GraphIDs[base.OutTargets[i]], base.OutKinds[i])
		}
	}

	merged.IterEdges(func(id uint64, st EdgeSegState) bool {
		if st.Tombstoned {
			return true
		}
		b.AddEdge(id, st.StartID, st.EndID, st.Kind)
		return true
	})
}
