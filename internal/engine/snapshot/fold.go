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

	if err := foldNodes(b, base, merged); err != nil {
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

// foldNodes stages every node Fold's output must carry -- base nodes the
// merged delta didn't tombstone (possibly delta-overridden), plus nodes the
// merged delta adds that base never had at all -- in strictly ascending
// database-id order, satisfying Builder's own ascending-id staging contract.
//
// It does this as a two-pointer ascending MERGE of two already-sorted,
// id-disjoint streams (foldOneBaseNode over base.GraphIDs, ascending by
// construction -- Snapshot's own doc; foldOneAddedNode over addedNodeIDs,
// ascending because merged.IterNodes already walks ascending and this only
// filters it), rather than the two SEQUENTIAL passes an earlier version of
// this function used (base nodes first, then every delta-added node
// afterward). That earlier shape silently assumed every delta-added id
// exceeds every base id -- true only if the segments folded were published
// in the same order their underlying database ids were allocated, which
// ordinary concurrency does not guarantee: two commits racing to Apply can
// reach it in either order regardless of which one PostgreSQL's bigserial
// sequence numbered first, and this engine's own compactor can fold a base
// whose max already exceeds a still-pending, lower-numbered write once that
// write's segment lands as a rebased tail (adoptCompaction, compact.go). A
// delta-added id below the CURRENT base's max is therefore an entirely
// ordinary shape, not a corruption signal, and must interleave into its
// correct ascending position rather than be rejected.
//
// The two streams are disjoint by id, not merely assumed to be: a pg id is
// classified as delta-added, rather than delta-overridden (handled inside
// foldOneBaseNode instead), exactly when base.Dense(pgID) reports it absent
// from base -- and a pg id can never be BOTH a base id and something the
// merged delta introduces as new, again by bigserial monotonicity (a
// database id is assigned exactly once, ever, by the sequence that
// generated it; it does not become reusable by being deleted). The one
// scenario that could in principle produce a duplicate across the two
// streams -- a base node tombstoned by an earlier segment, then "readded"
// under the SAME database id by a later one -- cannot happen in this
// engine's own write model: a deleted PostgreSQL row's id is never handed
// back out by the sequence, so a genuinely new row always gets a NEW,
// higher id, which read-back stages as an ordinary delta-added node, not a
// same-id override of the tombstoned one (buildApplySegment,
// tombstoneNodeWithCascade, apply.go). A same-id "readd" within one segment
// (or across segments merged by MergeSegments) is instead just the ordinary
// last-write-wins collapse SegmentBuilder.Build and MergeSegments already
// document -- a single NodeSegState per id, never two.
//
// Because the two streams are provably disjoint, the merged stream this
// produces is provably strictly ascending too, given each input stream is
// -- but this is not merely assumed: Builder.addPreparedNode (called by
// both foldOneBaseNode and foldOneAddedNode) independently re-asserts
// strictly-ascending order on every staged id regardless of which stream it
// came from, and returns a named error rather than silently mis-stage on
// any violation -- belt-and-suspenders against a future change to either
// stream's own ordering assumption, not merely this function's.
func foldNodes(b *Builder, base *Snapshot, merged *Segment) error {
	added := addedNodeIDs(base, merged)
	baseIDs := base.GraphIDs

	i, j := 0, 0
	for i < len(baseIDs) || j < len(added) {
		if j >= len(added) || (i < len(baseIDs) && baseIDs[i] < added[j]) {
			if err := foldOneBaseNode(b, base, merged, i); err != nil {
				return err
			}
			i++
			continue
		}
		if err := foldOneAddedNode(b, merged, added[j]); err != nil {
			return err
		}
		j++
	}
	return nil
}

// foldOneBaseNode stages base's i'th node (in base's own ascending dense-id
// order): a delta-overridden node's kinds and property bag come from its
// effective NodeSegState, transplanted via addPreparedNode from the
// ORIGINATING segment's own name table and arena (see NodeSegState's doc on
// why seg keeps pointing there through a merge); a tombstoned one is
// skipped entirely; an untouched node's kinds and bag are transplanted
// verbatim from base itself, via the same addPreparedNode path.
func foldOneBaseNode(b *Builder, base *Snapshot, merged *Segment, i int) error {
	pgID := base.GraphIDs[i]

	if st, overridden := merged.NodeState(pgID); overridden {
		if st.Tombstoned {
			return nil
		}
		props := preparedProps{entries: st.entries, names: st.seg.names, arena: st.seg.arena}
		if err := b.addPreparedNode(pgID, st.KindIDs, props); err != nil {
			return fmt.Errorf("snapshot: Fold: node %d (delta-overridden): %w", pgID, err)
		}
		return nil
	}

	lo, hi := base.Props.nodeOffsets[i], base.Props.nodeOffsets[i+1]
	kLo, kHi := base.KindOffsets[i], base.KindOffsets[i+1]
	props := preparedProps{entries: base.Props.entries[lo:hi], names: base.Props.names, arena: base.Props.arena}
	if err := b.addPreparedNode(pgID, base.NodeKinds[kLo:kHi], props); err != nil {
		return fmt.Errorf("snapshot: Fold: node %d (base): %w", pgID, err)
	}
	return nil
}

// addedNodeIDs returns, in ascending order, every database id the merged
// delta introduces that base never had at all -- excluding both tombstoned
// ids (nothing to stage: created and deleted within the same uncompacted
// delta) and ids merged.NodeState reports as present in base (those are
// delta-OVERRIDDEN base nodes, staged by foldOneBaseNode instead; see
// foldNodes' doc for why an id can never legitimately be both). Ascending
// because merged.IterNodes already walks in ascending pg-id order and this
// only filters that stream, never reorders it.
func addedNodeIDs(base *Snapshot, merged *Segment) []uint64 {
	var added []uint64
	merged.IterNodes(func(pgID uint64, st NodeSegState) bool {
		if st.Tombstoned {
			return true
		}
		if _, ok := base.Dense(pgID); ok {
			return true
		}
		added = append(added, pgID)
		return true
	})
	return added
}

// foldOneAddedNode stages one delta-added node (a pgID addedNodeIDs
// returned): its kinds and property bag are transplanted from the merged
// segment's own effective NodeSegState via addPreparedNode, exactly as
// foldOneBaseNode does for a delta-overridden base node -- the only
// difference is where the pre-fold state comes from, not how it is staged.
func foldOneAddedNode(b *Builder, merged *Segment, pgID uint64) error {
	st, ok := merged.NodeState(pgID)
	if !ok {
		// addedNodeIDs derives pgID from this exact merged segment's own
		// IterNodes, so this can only mean a caller passed a mismatched
		// (merged, pgID) pair -- a programmer error worth naming loudly,
		// not silently skipping a node Fold's caller expects to see.
		return fmt.Errorf("snapshot: Fold: delta-added node %d missing from merged segment (internal invariant violated)", pgID)
	}
	props := preparedProps{entries: st.entries, names: st.seg.names, arena: st.seg.arena}
	if err := b.addPreparedNode(pgID, st.KindIDs, props); err != nil {
		return fmt.Errorf("snapshot: Fold: node %d (delta-added): %w", pgID, err)
	}
	return nil
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
