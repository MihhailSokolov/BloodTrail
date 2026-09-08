// SPDX-License-Identifier: Apache-2.0
package snapshot

// View is the read surface every query executor runs against. Today it is a
// pure passthrough over one immutable base Snapshot: every accessor below
// forwards straight to the matching Snapshot (or PropStore) method or field,
// so a View is exactly as immutable as the base it wraps. A later change
// layers delta data over the base; Overlay is the fast-path guard executors
// will branch on once that exists.
type View struct {
	base *Snapshot
}

// NewView wraps base in a View.
func NewView(base *Snapshot) *View {
	return &View{base: base}
}

// Base returns the underlying base Snapshot this View wraps.
func (v *View) Base() *Snapshot {
	return v.base
}

// Overlay reports whether this View has any delta data layered over its
// base snapshot. It is unconditionally false today.
func (v *View) Overlay() bool {
	return false
}

// NodeCount returns the number of nodes visible through this View.
func (v *View) NodeCount() int {
	return v.base.NodeCount()
}

// EdgeCount returns the number of edges visible through this View.
func (v *View) EdgeCount() int {
	return v.base.EdgeCount()
}

// Dense returns the dense NodeID for a database id, and whether it exists.
func (v *View) Dense(databaseID uint64) (NodeID, bool) {
	return v.base.Dense(databaseID)
}

// GraphID returns the database id dense NodeID n was built from -- not to be
// confused with Snapshot.GraphID, the identifier of the one graph this
// snapshot holds.
func (v *View) GraphID(n NodeID) uint64 {
	return v.base.GraphIDs[n]
}

// KindIDsOf returns a slice view over dense NodeID n's kind ids, aliasing
// the base snapshot's NodeKinds array (immutable, so aliasing is safe; see
// Snapshot.Out for the same pattern over edges).
func (v *View) KindIDsOf(n NodeID) []KindID {
	lo, hi := v.base.KindOffsets[n], v.base.KindOffsets[n+1]
	return v.base.NodeKinds[lo:hi]
}

// Out returns slice views over n's outgoing edges: aligned target, kind, and
// database edge id slices, sorted by (target, kind) -- see Snapshot.Out.
func (v *View) Out(n NodeID) (targets []NodeID, kinds []KindID, edgeIDs []uint64) {
	lo, hi := v.base.OutOffsets[n], v.base.OutOffsets[n+1]
	return v.base.OutTargets[lo:hi], v.base.OutKinds[lo:hi], v.base.OutEdgeIDs[lo:hi]
}

// In returns slice views over n's incoming edges: aligned source and kind
// slices, aliasing the base snapshot exactly as Out does, sorted by (source,
// kind) -- see Snapshot.In. The edge id slice cannot alias the same way: the
// reverse CSR stores each slot's forward-array index (Snapshot.InEdgeIdx)
// rather than the database edge id itself, so edgeIDs is resolved through
// OutEdgeIDs and freshly allocated on every call.
func (v *View) In(n NodeID) (sources []NodeID, kinds []KindID, edgeIDs []uint64) {
	lo, hi := v.base.InOffsets[n], v.base.InOffsets[n+1]
	sources = v.base.InTargets[lo:hi]
	kinds = v.base.InKinds[lo:hi]

	fwdIdx := v.base.InEdgeIdx[lo:hi]
	edgeIDs = make([]uint64, len(fwdIdx))
	for i, j := range fwdIdx {
		edgeIDs[i] = v.base.OutEdgeIDs[j]
	}
	return sources, kinds, edgeIDs
}

// EdgeByID looks up the forward-CSR slot of the edge with the given database
// id -- see Snapshot.EdgeByID.
func (v *View) EdgeByID(id uint64) (fwdIdx uint64, ok bool) {
	return v.base.EdgeByID(id)
}

// NodesOfKind returns the base snapshot's bitset of dense NodeIDs carrying
// kind k.
func (v *View) NodesOfKind(k KindID) *Bitset {
	return v.base.NodesOfKind(k)
}

// Kinds returns the base snapshot's kind id<->name table.
func (v *View) Kinds() *KindTable {
	return v.base.Kinds
}

// MultiGraph reports whether the source database held more than one graph
// with at least one node, as of the base snapshot's load.
func (v *View) MultiGraph() bool {
	return v.base.MultiGraph
}

// Generation returns the base snapshot's generation counter.
func (v *View) Generation() uint64 {
	return v.base.Generation
}

// ApproxBytes estimates the base snapshot's resident memory footprint.
func (v *View) ApproxBytes() uint64 {
	return v.base.ApproxBytes()
}

// PropIDByName returns the PropID interned for name, and whether one
// exists -- see PropStore.IDByName.
func (v *View) PropIDByName(name string) (PropID, bool) {
	return v.base.Props.IDByName(name)
}

// PropValue returns node n's value for property id -- see PropStore.Value.
func (v *View) PropValue(n NodeID, id PropID) (any, bool) {
	return v.base.Props.Value(n, id)
}

// PropNodeMap returns node n's full property bag as a fresh map -- see
// PropStore.NodeMap.
func (v *View) PropNodeMap(n NodeID) map[string]any {
	return v.base.Props.NodeMap(n)
}

// NodeByObjectID returns one node whose objectid property has the exact
// string value objectID, and whether any match was found -- see
// PropStore.NodeByObjectID.
func (v *View) NodeByObjectID(objectID string) (NodeID, bool) {
	return v.base.Props.NodeByObjectID(objectID)
}

// NodesByObjectID returns every node whose objectid property has the exact
// string value objectID, and whether any match was found -- see
// PropStore.NodesByObjectID.
func (v *View) NodesByObjectID(objectID string) ([]NodeID, bool) {
	return v.base.Props.NodesByObjectID(objectID)
}
