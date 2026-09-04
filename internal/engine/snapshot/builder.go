// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"fmt"
	"sort"
	"time"
)

// stagedEdge is a raw, unresolved edge as handed to AddEdge: endpoints are
// still database ids, not dense NodeIDs.
type stagedEdge struct {
	start, end uint64
	kind       KindID
}

// Builder accumulates nodes and edges for a single graph and packs them into
// an immutable Snapshot on Build.
//
// Nodes must be staged via AddNode in strictly ascending database-id order;
// that order becomes the dense NodeID assignment (0, 1, 2, ...), so no
// separate sort of nodes is needed at build time. Edges may be staged via
// AddEdge in any order.
type Builder struct {
	graphID int32

	ids         []uint64
	kindsFlat   []KindID
	kindOffsets []uint32 // len(ids)+1, into kindsFlat

	edges []stagedEdge
}

// NewBuilder creates an empty Builder for the given graph id.
func NewBuilder(graphID int32) *Builder {
	return &Builder{
		graphID:     graphID,
		kindOffsets: []uint32{0},
	}
}

// AddNode stages a node. databaseID must be strictly greater than every
// previously added node's databaseID; otherwise AddNode returns an error and
// the node is not staged.
func (b *Builder) AddNode(databaseID uint64, kinds []KindID) error {
	if n := len(b.ids); n > 0 && databaseID <= b.ids[n-1] {
		return fmt.Errorf("snapshot: AddNode: databaseID %d is not strictly greater than previous %d", databaseID, b.ids[n-1])
	}
	b.ids = append(b.ids, databaseID)
	b.kindsFlat = append(b.kindsFlat, kinds...)
	b.kindOffsets = append(b.kindOffsets, uint32(len(b.kindsFlat)))
	return nil
}

// AddEdge stages an edge between two database node ids. Edges may be added
// in any order and are resolved against staged nodes at Build time.
func (b *Builder) AddEdge(startID, endID uint64, kind KindID) {
	b.edges = append(b.edges, stagedEdge{start: startID, end: endID, kind: kind})
}

// denseEdge is a staged edge with endpoints resolved to dense NodeIDs.
type denseEdge struct {
	src, dst NodeID
	kind     KindID
}

// Build packs the staged nodes and edges into an immutable Snapshot.
//
// It runs in O(N+E): a counting sort places each edge directly into its
// node's segment of the CSR arrays via prefix-summed offsets and per-node
// cursors, followed by a sort of each (typically small) per-node segment by
// (target, kind) — never a global comparison sort over all edges.
func (b *Builder) Build() (*Snapshot, error) {
	n := len(b.ids)

	graphIDs := make([]uint64, n)
	copy(graphIDs, b.ids)

	idIndex := make(map[uint64]NodeID, n)
	for i, id := range graphIDs {
		idIndex[id] = NodeID(i)
	}

	dEdges := make([]denseEdge, len(b.edges))
	for i, e := range b.edges {
		src, ok := idIndex[e.start]
		if !ok {
			return nil, fmt.Errorf("snapshot: Build: edge references unknown start node id %d", e.start)
		}
		dst, ok := idIndex[e.end]
		if !ok {
			return nil, fmt.Errorf("snapshot: Build: edge references unknown end node id %d", e.end)
		}
		dEdges[i] = denseEdge{src: src, dst: dst, kind: e.kind}
	}

	outOffsets, outTargets, outKinds := packEdges(n, dEdges, func(e denseEdge) (NodeID, NodeID, KindID) {
		return e.src, e.dst, e.kind
	})
	inOffsets, inTargets, inKinds := packEdges(n, dEdges, func(e denseEdge) (NodeID, NodeID, KindID) {
		return e.dst, e.src, e.kind
	})

	kindOffsets := make([]uint32, len(b.kindOffsets))
	copy(kindOffsets, b.kindOffsets)
	nodeKinds := make([]KindID, len(b.kindsFlat))
	copy(nodeKinds, b.kindsFlat)

	var maxKind KindID
	kindBitmaps := make(map[KindID]*Bitset)
	for i := 0; i < n; i++ {
		lo, hi := kindOffsets[i], kindOffsets[i+1]
		for _, k := range nodeKinds[lo:hi] {
			if k > maxKind {
				maxKind = k
			}
			bm, ok := kindBitmaps[k]
			if !ok {
				bm = NewBitset(n)
				kindBitmaps[k] = bm
			}
			bm.Set(NodeID(i))
		}
	}
	for _, e := range dEdges {
		if e.kind > maxKind {
			maxKind = e.kind
		}
	}

	return &Snapshot{
		GraphID:     b.graphID,
		GraphIDs:    graphIDs,
		OutOffsets:  outOffsets,
		OutTargets:  outTargets,
		OutKinds:    outKinds,
		InOffsets:   inOffsets,
		InTargets:   inTargets,
		InKinds:     inKinds,
		KindOffsets: kindOffsets,
		NodeKinds:   nodeKinds,
		MaxKindID:   maxKind,
		BuiltAt:     time.Now(),
		idIndex:     idIndex,
		kindBitmaps: kindBitmaps,
	}, nil
}

// packEdges counting-sorts dEdges into CSR form keyed by bucket(e), storing
// (other(e), kind(e)) in each edge's bucket segment, then sorts each
// segment by (target, kind). It is used for both the Out arrays (bucket =
// source, other = target) and the In arrays (bucket = target, other =
// source) by swapping which endpoint plays which role.
func packEdges(n int, dEdges []denseEdge, role func(denseEdge) (bucket, other NodeID, kind KindID)) (offsets []uint64, others []NodeID, kinds []KindID) {
	degree := make([]uint32, n)
	for _, e := range dEdges {
		bucket, _, _ := role(e)
		degree[bucket]++
	}

	offsets = make([]uint64, n+1)
	for i := 0; i < n; i++ {
		offsets[i+1] = offsets[i] + uint64(degree[i])
	}

	others = make([]NodeID, len(dEdges))
	kinds = make([]KindID, len(dEdges))

	cursor := make([]uint64, n)
	copy(cursor, offsets[:n])
	for _, e := range dEdges {
		bucket, other, kind := role(e)
		c := cursor[bucket]
		others[c] = other
		kinds[c] = kind
		cursor[bucket] = c + 1
	}

	for i := 0; i < n; i++ {
		lo, hi := offsets[i], offsets[i+1]
		sortSegment(others[lo:hi], kinds[lo:hi])
	}

	return offsets, others, kinds
}

// segment is a tiny sort.Interface over the parallel (target, kind) slices
// of one node's adjacency segment, ordered by (target, kind).
type segment struct {
	targets []NodeID
	kinds   []KindID
}

func (s segment) Len() int { return len(s.targets) }

func (s segment) Less(i, j int) bool {
	if s.targets[i] != s.targets[j] {
		return s.targets[i] < s.targets[j]
	}
	return s.kinds[i] < s.kinds[j]
}

func (s segment) Swap(i, j int) {
	s.targets[i], s.targets[j] = s.targets[j], s.targets[i]
	s.kinds[i], s.kinds[j] = s.kinds[j], s.kinds[i]
}

func sortSegment(targets []NodeID, kinds []KindID) {
	sort.Sort(segment{targets: targets, kinds: kinds})
}
