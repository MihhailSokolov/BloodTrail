// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"sort"
	"sync"
	"time"
)

// Snapshot is an immutable, point-in-time copy of a graph laid out as
// compressed sparse row (CSR) arrays for cache-friendly traversal.
//
// Nodes are addressed by a dense NodeID (0..NodeCount()-1), assigned in
// ascending database-id order at build time. GraphIDs maps a dense NodeID
// back to the database id it was built from.
type Snapshot struct {
	GraphID int32

	GraphIDs []uint64 // dense id -> database id, ascending

	OutOffsets []uint64 // len N+1
	OutTargets []NodeID
	OutKinds   []KindID
	OutEdgeIDs []uint64 // database edge id per forward-CSR slot, aligned with OutTargets/OutKinds

	InOffsets []uint64 // len N+1
	InTargets []NodeID
	InKinds   []KindID
	InEdgeIdx []uint32 // per reverse-CSR slot, index of the same edge in the forward arrays: InTargets[i]'s edge is OutEdgeIDs[InEdgeIdx[i]], kind OutKinds[InEdgeIdx[i]]

	KindOffsets []uint32 // len N+1, into NodeKinds
	NodeKinds   []KindID

	MaxKindID KindID

	// Kinds is the snapshot-owned id<->name table for every kind registered
	// in the database's global `kind` table (see LoadSnapshot), independent
	// of which kinds this particular graph's nodes and edges actually carry.
	// Set once via Builder.SetKinds before Build; never nil after Build.
	Kinds *KindTable

	// Props holds every node's property bag (see props.go), plus the
	// exact-match `objectid` index Cypher predicate evaluation looks nodes
	// up by. Populated from each Builder.AddNode call's propsJSON; never nil
	// after Build.
	Props *PropStore

	// DroppedEdges counts edges dropped at build time because one or both
	// endpoints did not resolve to a staged node. See Builder.Build.
	DroppedEdges int

	BuiltAt time.Time

	// MultiGraph reports whether the source database holds more than one
	// graph with at least one node, as of LoadSnapshot's multi-graph probe.
	// It is informational only -- the snapshot itself always holds exactly
	// one graph's nodes and edges (GraphID) -- flagging a database shape
	// later Cypher work (e.g. an unscoped query) may need to reason about.
	// Set by LoadSnapshot after Build; false on a Builder-only Snapshot
	// (e.g. in unit tests) that never went through it.
	MultiGraph bool

	idIndex     map[uint64]NodeID
	kindBitmaps map[KindID]*Bitset

	// selfLoopKinds holds every edge kind with at least one self-loop edge
	// (start == end) in this snapshot; nil when there are none (the ordinary
	// case). Derived by finalizeDerived on every construction path -- see its
	// doc -- and read through View.SelfLoopHazard.
	selfLoopKinds map[KindID]struct{}

	// edgeKindSeen[k] reports whether kind k labels at least one edge in this
	// snapshot's forward CSR; a kind at or past its length labels none.
	// Derived by finalizeDerived on every construction path, like
	// selfLoopKinds above, but held as a dense slice rather than a map
	// because EVERY edge contributes to it -- a map would cost one write per
	// edge on a multi-million-edge build, where self-loops are rare enough
	// that theirs stays tiny. Read through View.EdgeKindPresent.
	edgeKindSeen []bool

	// stringIdx memoizes per-property value indexes (propindex.go), built
	// lazily on first use rather than at Build time: a deployment queries a
	// handful of properties out of the dozens BloodHound collects, and
	// sorting every one of them eagerly would cost far more than it saves.
	// Guarded by stringIdxMu because a Snapshot is otherwise immutable and
	// shared across concurrently-served queries.
	stringIdxMu sync.Mutex
	stringIdx   map[PropID]*stringIndex

	// valueIdx memoizes per-property EXACT-match postings (valueindex.go),
	// built lazily like stringIdx and guarded for the same reason.
	valueIdxMu sync.Mutex
	valueIdx   map[PropID]*valueIndex

	// edgeKindIdx memoizes the per-edge-kind endpoint index
	// (edgekindindex.go), built lazily in one pass on first use. Guarded for
	// the same reason stringIdx is: a Snapshot is otherwise immutable and
	// shared across concurrently-served queries.
	edgeKindIdxMu sync.Mutex
	edgeKindIdx   *edgeKindIndex

	// edgeIDPerm holds forward-CSR indices 0..EdgeCount()-1 permuted into
	// ascending OutEdgeIDs order, letting EdgeByID binary-search by database
	// edge id without a separate id->index map.
	edgeIDPerm []uint32
}

// NodeCount returns the number of nodes in the snapshot.
func (s *Snapshot) NodeCount() int {
	return len(s.GraphIDs)
}

// EdgeCount returns the number of edges in the snapshot.
func (s *Snapshot) EdgeCount() int {
	return len(s.OutTargets)
}

// Dense returns the dense NodeID for a database id, and whether it exists.
func (s *Snapshot) Dense(databaseID uint64) (NodeID, bool) {
	id, ok := s.idIndex[databaseID]
	return id, ok
}

// Out returns slice views over n's outgoing edges: aligned target and kind
// slices, sorted by (target, kind).
func (s *Snapshot) Out(n NodeID) ([]NodeID, []KindID) {
	lo, hi := s.OutOffsets[n], s.OutOffsets[n+1]
	return s.OutTargets[lo:hi], s.OutKinds[lo:hi]
}

// hasEdgeKind reports whether k labels at least one edge in s. False is a
// proof of absence for this snapshot alone; callers that must account for
// later writes go through View.EdgeKindPresent, which also consults the
// segment stack.
func (s *Snapshot) hasEdgeKind(k KindID) bool {
	return k >= 0 && int(k) < len(s.edgeKindSeen) && s.edgeKindSeen[k]
}

// In returns slice views over n's incoming edges: aligned source and kind
// slices, sorted by (source, kind).
func (s *Snapshot) In(n NodeID) ([]NodeID, []KindID) {
	lo, hi := s.InOffsets[n], s.InOffsets[n+1]
	return s.InTargets[lo:hi], s.InKinds[lo:hi]
}

// EdgeByID looks up the forward-CSR slot of the edge with the given database
// id via binary search over edgeIDPerm, a permutation of forward indices
// sorted ascending by OutEdgeIDs and built once at Build time. If found, the
// edge's target and kind are OutTargets[fwdIdx] and OutKinds[fwdIdx]
// (OutEdgeIDs[fwdIdx] == id, by construction). ok is false if no edge in the
// snapshot carries id.
func (s *Snapshot) EdgeByID(id uint64) (fwdIdx uint64, ok bool) {
	n := len(s.edgeIDPerm)
	i := sort.Search(n, func(i int) bool {
		return s.OutEdgeIDs[s.edgeIDPerm[i]] >= id
	})
	if i < n && s.OutEdgeIDs[s.edgeIDPerm[i]] == id {
		return uint64(s.edgeIDPerm[i]), true
	}
	return 0, false
}

// NodesOfKind returns the bitset of dense NodeIDs carrying kind k. It is
// nil-safe: an absent kind yields an empty bitset rather than nil.
func (s *Snapshot) NodesOfKind(k KindID) *Bitset {
	if bm, ok := s.kindBitmaps[k]; ok {
		return bm
	}
	return NewBitset(0)
}

// Approximate per-element byte sizes of the packed arrays, used by
// ApproxBytes. These are fixed sizes for the fixed-width element types
// declared on Snapshot, not runtime-measured.
const (
	bytesPerUint64 = 8
	bytesPerNodeID = 4 // uint32
	bytesPerKindID = 2 // int16
	bytesPerUint32 = 4

	// Rough per-entry overhead estimates for the unexported maps, covering
	// bucket/pointer overhead in addition to the key/value payload.
	approxIdIndexEntryBytes    = 24 // uint64 key + NodeID value + map bucket overhead
	approxKindBitmapEntryBytes = 24 // KindID key + *Bitset pointer + map bucket overhead
)

// ApproxBytes estimates the snapshot's resident memory footprint: the sum of
// the packed slices' element sizes plus a rough estimate for the unexported
// index structures.
func (s *Snapshot) ApproxBytes() uint64 {
	var total uint64

	total += uint64(len(s.GraphIDs)) * bytesPerUint64
	total += uint64(len(s.OutOffsets)) * bytesPerUint64
	total += uint64(len(s.OutTargets)) * bytesPerNodeID
	total += uint64(len(s.OutKinds)) * bytesPerKindID
	total += uint64(len(s.OutEdgeIDs)) * bytesPerUint64
	total += uint64(len(s.InOffsets)) * bytesPerUint64
	total += uint64(len(s.InTargets)) * bytesPerNodeID
	total += uint64(len(s.InKinds)) * bytesPerKindID
	total += uint64(len(s.InEdgeIdx)) * bytesPerUint32
	total += uint64(len(s.KindOffsets)) * bytesPerUint32
	total += uint64(len(s.NodeKinds)) * bytesPerKindID
	total += uint64(len(s.edgeIDPerm)) * bytesPerUint32

	total += uint64(len(s.idIndex)) * approxIdIndexEntryBytes

	for _, bm := range s.kindBitmaps {
		total += approxKindBitmapEntryBytes
		total += uint64(len(bm.words)) * bytesPerUint64
	}

	if s.Kinds != nil {
		total += s.Kinds.ApproxBytes()
	}
	if s.Props != nil {
		total += s.Props.ApproxBytes()
	}

	return total
}
