// SPDX-License-Identifier: Apache-2.0
package snapshot

import "time"

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

	InOffsets []uint64 // len N+1
	InTargets []NodeID
	InKinds   []KindID

	KindOffsets []uint32 // len N+1, into NodeKinds
	NodeKinds   []KindID

	MaxKindID KindID

	BuiltAt time.Time

	Generation    uint64    // set by the engine (Task 10)
	AnalysisStamp time.Time // set by the poller (Task 12)

	idIndex     map[uint64]NodeID
	kindBitmaps map[KindID]*Bitset
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

// In returns slice views over n's incoming edges: aligned source and kind
// slices, sorted by (source, kind).
func (s *Snapshot) In(n NodeID) ([]NodeID, []KindID) {
	lo, hi := s.InOffsets[n], s.InOffsets[n+1]
	return s.InTargets[lo:hi], s.InKinds[lo:hi]
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
	total += uint64(len(s.InOffsets)) * bytesPerUint64
	total += uint64(len(s.InTargets)) * bytesPerNodeID
	total += uint64(len(s.InKinds)) * bytesPerKindID
	total += uint64(len(s.KindOffsets)) * bytesPerUint32
	total += uint64(len(s.NodeKinds)) * bytesPerKindID

	total += uint64(len(s.idIndex)) * approxIdIndexEntryBytes

	for _, bm := range s.kindBitmaps {
		total += approxKindBitmapEntryBytes
		total += uint64(len(bm.words)) * bytesPerUint64
	}

	return total
}
