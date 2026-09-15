// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Segment is an immutable, ordered set of post-write entity states produced
// by applying one commit against a base Snapshot: node upserts and
// tombstones, edge upserts and tombstones, and any newly registered kinds.
// Everything a Segment holds is keyed by pg id (uint64 node/edge ids), never
// by a base snapshot's dense NodeID -- a Segment must survive being layered
// over a different, later-rebuilt base than the one it was recorded against.
//
// A Segment is built once by SegmentBuilder.Build (or produced by
// MergeSegments) and never mutated afterward, which is the entirety of its
// concurrency story: no locks, no atomics, just data nothing ever writes to
// again once a caller holds a *Segment.
//
// A Segment's prop bags are complete replacements, not patches: NodeState's
// PropValueByName/PropMap always reflect the node's full post-write property
// set, exactly as PropStore's Value/NodeMap do for a base Snapshot.
type Segment struct {
	nodeIDs    []uint64 // sorted ascending, unique
	nodeStates map[uint64]NodeSegState

	edgeIDs    []uint64 // sorted ascending, unique
	edgeStates map[uint64]EdgeSegState

	// Segment-local property-name interning and byte arena, in the same
	// spirit as PropStore's names/ids/arena (see props.go) but scoped to
	// just this segment's own node upserts. Populated only by
	// SegmentBuilder.Build; a Segment produced by MergeSegments leaves these
	// nil/empty, since every NodeSegState it carries decodes through the
	// *originating* input segment's own tables instead (see NodeSegState's
	// and MergeSegments' docs).
	names []string
	ids   map[string]PropID
	arena []byte

	addedKinds map[KindID]string

	// objectIndex maps a string-valued objectid to every non-tombstoned node
	// in this segment carrying it. Unlike PropStore's objectIndex (a
	// single-witness fast path plus a rarely-populated dup map), a segment
	// is expected to be small (one commit's worth of writes), so
	// NodesByObjectID's single-slice-return signature is served directly
	// from one map with no fast/slow-path split.
	objectIndex map[string][]uint64

	// selfLoopKinds holds every edge kind this segment writes at least one
	// live self-loop edge for (start == end, not tombstoned); nil when there
	// are none. The Segment half of the base snapshot's identically-named
	// derived field -- see segmentSelfLoopKinds and View.SelfLoopHazard.
	selfLoopKinds map[KindID]struct{}

	// edgeKinds holds every edge kind this segment writes at least one live
	// (non-tombstoned) edge for; nil when it writes none. The Segment half of
	// the base snapshot's edgeKindSeen -- a map here rather than a dense
	// slice because a segment carries one commit's worth of writes, not a
	// whole graph's. See segmentEdgeKinds and View.EdgeKindPresent.
	edgeKinds map[KindID]struct{}

	// approxBytes memoizes ApproxBytes' result, computed once by
	// computeSegmentApproxBytes at construction time (SegmentBuilder.Build
	// or MergeSegments) rather than walked afresh on every call. A Segment
	// is immutable for the whole of its lifetime once built (this type's
	// own doc), so computing this once is exactly as accurate as computing
	// it on every call, and turns ApproxBytes into a plain field read --
	// see ApproxBytes' own doc for why that matters to compact.go's
	// deltaSize, the trigger heuristic this field exists to make cheap.
	approxBytes uint64
}

// NodeSegState is one node's post-write state as recorded in a Segment:
// whether it was tombstoned, its full kind list (possibly empty -- a
// production upsert shape creates kind-less nodes), and its objectid
// property's value ("" if absent or non-string).
//
// PropValueByName and PropMap materialize the node's property bag on
// demand, decoding lazily through the owning segment's arena exactly as
// PropStore.Value/NodeMap do for a base Snapshot (see decode) -- arrays and
// objects are stored as raw JSON and re-decoded on every access; unlike
// PropStore, strings are not returned zero-copy (a plain copy on read),
// trading a little throughput for a much simpler segment representation.
//
// The segment a NodeSegState decodes through (seg) is not necessarily the
// Segment it was retrieved from: MergeSegments carries each winning node's
// NodeSegState value over from whichever input segment produced it, unmodified,
// so seg keeps pointing at that original segment's own names/arena. This is
// a deliberate aliasing choice -- see MergeSegments' doc.
type NodeSegState struct {
	Tombstoned bool
	KindIDs    []KindID
	ObjectID   string
	// HasObjectID says whether ObjectID is a value the node actually
	// carries, which "" alone cannot: the empty string is a perfectly
	// ordinary objectid, and PropStore's own index records it. Keying the
	// segment index off ObjectID != "" instead made a delta-written node
	// whose objectid really is empty invisible to objectid resolution --
	// until a compaction happened to fold it into the base, at which point
	// it appeared. That is an overlay-versus-base divergence in exactly
	// the lookup apply.go resolves written ids through.
	HasObjectID bool

	seg     *Segment
	entries []propEntry // sorted by prop; nil when Tombstoned
}

// PropValueByName returns the node's value for the named property. The
// returned any follows the same post-JSON value model as PropStore.Value:
// float64 | string | bool | []any | map[string]any | nil. ok is false when
// the property is absent (including on a tombstoned node, which carries no
// bag at all).
func (st NodeSegState) PropValueByName(name string) (any, bool) {
	if st.seg == nil {
		return nil, false
	}
	id, ok := st.seg.ids[name]
	if !ok {
		return nil, false
	}
	entries := st.entries
	i := sort.Search(len(entries), func(i int) bool { return entries[i].prop >= id })
	if i >= len(entries) || entries[i].prop != id {
		return nil, false
	}
	return st.seg.decode(entries[i]), true
}

// PropMap returns the node's full property bag as a fresh map on every
// call, mirroring PropStore.NodeMap.
func (st NodeSegState) PropMap() map[string]any {
	m := make(map[string]any, len(st.entries))
	if st.seg == nil {
		return m
	}
	for _, e := range st.entries {
		m[st.seg.names[e.prop]] = st.seg.decode(e)
	}
	return m
}

// EdgeSegState is one edge's post-write state as recorded in a Segment: its
// endpoints and kind, or Tombstoned if the edge was removed. Edges carry no
// property bag in BloodTrail's model, so unlike NodeSegState this is a
// plain value type with no decode machinery.
type EdgeSegState struct {
	Tombstoned bool
	StartID    uint64
	EndID      uint64
	Kind       KindID
}

// decode turns one propEntry into the post-JSON model value
// PropValueByName/PropMap return, reading string/array/object payloads out
// of the segment's own arena. This mirrors PropStore.decode exactly (see
// its doc for why arrays/objects are re-decoded on every access rather than
// cached), except string values are copied rather than aliased -- see
// NodeSegState's doc for why zero-copy strings aren't worth it here.
func (s *Segment) decode(e propEntry) any {
	switch e.kind {
	case propKindNull:
		return nil
	case propKindFalse:
		return false
	case propKindTrue:
		return true
	case propKindNumber:
		return e.num
	case propKindString:
		return string(s.arena[e.ref : e.ref+e.len])
	case propKindArray:
		var v []any
		if err := json.Unmarshal(s.arena[e.ref:e.ref+e.len], &v); err != nil {
			// The arena only ever holds bytes this package itself wrote from
			// already-validated JSON (see SegmentBuilder.commitProps), so a
			// decode failure here means Segment's own invariant was
			// violated, not bad input -- a programmer error worth failing
			// loudly on, exactly as PropStore.decode does.
			panic(fmt.Sprintf("snapshot: Segment: corrupt array bytes for prop %d: %v", e.prop, err))
		}
		return v
	case propKindObject:
		var v map[string]any
		if err := json.Unmarshal(s.arena[e.ref:e.ref+e.len], &v); err != nil {
			panic(fmt.Sprintf("snapshot: Segment: corrupt object bytes for prop %d: %v", e.prop, err))
		}
		return v
	default:
		return nil
	}
}

// NodeState returns node id's recorded state, and whether this segment
// carries any record (upsert or tombstone) for it at all.
func (s *Segment) NodeState(id uint64) (NodeSegState, bool) {
	st, ok := s.nodeStates[id]
	return st, ok
}

// EdgeState returns edge id's recorded state, and whether this segment
// carries any record (upsert or tombstone) for it at all.
func (s *Segment) EdgeState(id uint64) (EdgeSegState, bool) {
	st, ok := s.edgeStates[id]
	return st, ok
}

// AddedKinds returns a fresh copy of the kind id<->name pairs this segment
// registers, safe for the caller to mutate.
func (s *Segment) AddedKinds() map[KindID]string {
	out := make(map[KindID]string, len(s.addedKinds))
	for k, v := range s.addedKinds {
		out[k] = v
	}
	return out
}

// IterNodes calls fn for every node id this segment carries a record for,
// in ascending pg-id order, stopping early if fn returns false.
func (s *Segment) IterNodes(fn func(id uint64, st NodeSegState) bool) {
	for _, id := range s.nodeIDs {
		if !fn(id, s.nodeStates[id]) {
			return
		}
	}
}

// IterEdges calls fn for every edge id this segment carries a record for,
// in ascending pg-id order, stopping early if fn returns false.
func (s *Segment) IterEdges(fn func(id uint64, st EdgeSegState) bool) {
	for _, id := range s.edgeIDs {
		if !fn(id, s.edgeStates[id]) {
			return
		}
	}
}

// NodeCount returns the number of node ids this segment carries a record
// for, tombstones included.
func (s *Segment) NodeCount() int {
	return len(s.nodeIDs)
}

// EdgeCount returns the number of edge ids this segment carries a record
// for, tombstones included.
func (s *Segment) EdgeCount() int {
	return len(s.edgeIDs)
}

// NodesByObjectID returns every non-tombstoned node id in this segment
// whose objectid property has the exact string value objectid, or nil if
// none does.
func (s *Segment) NodesByObjectID(objectid string) []uint64 {
	return s.objectIndex[objectid]
}

// Approximate per-entry byte overheads used by ApproxBytes, in the same
// fixed/estimated spirit as Snapshot's and PropStore's own constants.
const (
	approxSegNodeStateMapEntryBytes = 40 // uint64 key + NodeSegState value + map bucket overhead
	approxSegEdgeStateMapEntryBytes = 40 // uint64 key + EdgeSegState value + map bucket overhead
	approxSegObjectIndexEntryBytes  = 24 // map bucket/pointer overhead for one objectIndex key
)

// ApproxBytes returns this segment's own directly-owned memory footprint,
// as estimated once by computeSegmentApproxBytes at construction time (see
// approxBytes' own doc for why this is a memoized field read, not a fresh
// walk of this segment's maps on every call).
func (s *Segment) ApproxBytes() uint64 {
	return s.approxBytes
}

// computeSegmentApproxBytes computes the estimate ApproxBytes returns: this
// segment's id slices, state maps, added-kinds table, objectid index, and
// (for a segment built directly by SegmentBuilder) its own prop-name table
// and arena. Called exactly once per segment, by SegmentBuilder.Build and
// MergeSegments, right after every other field is already set -- s itself
// is never mutated again after that (Segment's own doc), so there is no
// later point at which recomputing this could ever produce a different
// answer.
//
// This deliberately does not follow a NodeSegState's seg pointer to any
// *other* segment: a Segment produced by MergeSegments carries some
// NodeSegState values that alias data owned by the input segments passed to
// MergeSegments (see its doc) rather than anything reachable from this
// segment's own arena/names fields, which stay empty in that case. Counting
// those would mean either double-counting shared inputs referenced by more
// than one merge, or having to walk and deduplicate every input segment's
// identity -- more machinery than a rough, non-authoritative estimate (in
// the same spirit as Snapshot's and PropStore's own ApproxBytes) is worth.
// The memory those inputs hold is the caller's to account for, by the same
// reasoning that decides how long to keep them reachable at all.
func computeSegmentApproxBytes(s *Segment) uint64 {
	var total uint64

	total += uint64(len(s.nodeIDs)) * bytesPerUint64
	total += uint64(len(s.edgeIDs)) * bytesPerUint64

	total += uint64(len(s.nodeStates)) * approxSegNodeStateMapEntryBytes
	for _, st := range s.nodeStates {
		total += uint64(len(st.KindIDs)) * bytesPerKindID
		total += uint64(len(st.ObjectID))
	}

	total += uint64(len(s.edgeStates)) * approxSegEdgeStateMapEntryBytes

	total += uint64(len(s.arena))
	for _, name := range s.names {
		total += uint64(len(name))
	}
	total += uint64(len(s.ids)) * approxPropNameMapEntryBytes

	total += uint64(len(s.addedKinds)) * approxKindTableMapEntryBytes

	for k, ids := range s.objectIndex {
		total += uint64(len(k))
		total += approxSegObjectIndexEntryBytes
		total += uint64(len(ids)) * bytesPerUint64
	}

	return total
}

// segNodeBuild is one node's staged state inside a SegmentBuilder, before
// Build collapses the (possibly repeated) writes for each id down to a
// single final NodeSegState.
type segNodeBuild struct {
	tombstoned  bool
	kindIDs     []KindID
	objectID    string
	hasObjectID bool
	entries     []propEntry
}

// segEdgeBuild is segNodeBuild's edge counterpart.
type segEdgeBuild struct {
	tombstoned bool
	startID    uint64
	endID      uint64
	kind       KindID
}

// SegmentBuilder accumulates one commit's worth of node and edge state
// changes and packs them into an immutable Segment on Build.
//
// Unlike Builder (which requires nodes staged in strictly ascending
// database-id order, since that order becomes the dense NodeID assignment),
// SegmentBuilder has no such requirement: nodes and edges may be staged in
// any order, and Build sorts ids ascending itself. This also lets the same
// id be staged more than once -- AddNodeState/TombstoneNode (and their edge
// counterparts) simply overwrite whatever was previously staged for that
// id, so the *last* call for a given id determines its final state,
// regardless of how many times, or in what mix of upserts and tombstones,
// that id was staged before.
//
// The zero value is a ready-to-use, empty SegmentBuilder.
type SegmentBuilder struct {
	nodes map[uint64]segNodeBuild
	edges map[uint64]segEdgeBuild

	propNames []string
	propIDs   map[string]PropID
	propArena []byte

	kindTable map[KindID]string
}

// AddNodeState stages node id's post-write state: its full kind list
// (kindIDs may be nil/empty -- kind-less nodes are a legal production
// upsert shape) and its property bag as raw jsonb bytes (propsJSON may be
// nil/empty, meaning no properties), parsed via the package-level
// ParseProps exactly as Builder.AddNode parses a base snapshot's node
// props. A parse error, or internPropName's PropID-wrap guard firing while
// committing the parsed properties, is returned rather than swallowed,
// leaving this id's previously staged state (if any) unchanged.
func (b *SegmentBuilder) AddNodeState(id uint64, kindIDs []KindID, propsJSON []byte) error {
	parsed, err := ParseProps(propsJSON)
	if err != nil {
		return fmt.Errorf("snapshot: SegmentBuilder.AddNodeState: id %d: %w", id, err)
	}

	entries, objectID, hasObjectID, err := b.commitProps(parsed.parsed)
	if err != nil {
		return fmt.Errorf("snapshot: SegmentBuilder.AddNodeState: id %d: %w", id, err)
	}

	if b.nodes == nil {
		b.nodes = make(map[uint64]segNodeBuild)
	}
	b.nodes[id] = segNodeBuild{
		kindIDs:     append([]KindID(nil), kindIDs...),
		objectID:    objectID,
		hasObjectID: hasObjectID,
		entries:     entries,
	}
	return nil
}

// TombstoneNode stages node id as removed, overwriting any previously
// staged state for id.
func (b *SegmentBuilder) TombstoneNode(id uint64) {
	if b.nodes == nil {
		b.nodes = make(map[uint64]segNodeBuild)
	}
	b.nodes[id] = segNodeBuild{tombstoned: true}
}

// AddEdgeState stages edge edgeID's post-write state: its endpoints and
// kind, overwriting any previously staged state for edgeID.
func (b *SegmentBuilder) AddEdgeState(edgeID, startID, endID uint64, kindID KindID) {
	if b.edges == nil {
		b.edges = make(map[uint64]segEdgeBuild)
	}
	b.edges[edgeID] = segEdgeBuild{startID: startID, endID: endID, kind: kindID}
}

// TombstoneEdge stages edge edgeID as removed, overwriting any previously
// staged state for edgeID.
func (b *SegmentBuilder) TombstoneEdge(edgeID uint64) {
	if b.edges == nil {
		b.edges = make(map[uint64]segEdgeBuild)
	}
	b.edges[edgeID] = segEdgeBuild{tombstoned: true}
}

// AddKind stages a newly registered kind id<->name pair, overwriting any
// previously staged name for the same id.
func (b *SegmentBuilder) AddKind(id KindID, name string) {
	if b.kindTable == nil {
		b.kindTable = make(map[KindID]string)
	}
	b.kindTable[id] = name
}

// commitProps interns each parsed property's name into this builder's
// segment-local table, appends its payload (if any) to the builder's shared
// arena, and returns the node's entries sorted by PropID -- the segment
// counterpart of Builder.commitNodeProps, minus the flat-array-plus-offsets
// staging that method uses (not needed here: SegmentBuilder holds each
// node's entries directly in its own segNodeBuild, keyed by sparse pg id
// rather than a dense, sequential index). It also extracts the node's
// objectid property value, if present and string-valued, since NodeSegState
// stores it as a plain field rather than requiring a name lookup through
// the arena on every access.
//
// A node overwritten by a later AddNodeState call leaves its earlier
// entries' bytes behind in propArena, unreferenced -- a small, deliberate
// waste in exchange for not having to reclaim or compact the arena on
// overwrite; a segment holds one commit's worth of writes, not a long-lived
// accumulation, so this is not expected to matter in practice.
//
// Returns an error, refusing to commit any of parsed, the instant
// internPropName's PropID-wrap guard fires for one of them (see its doc);
// entries built so far are discarded rather than returned, matching
// Builder.commitNodeProps's own "nothing partially committed on error"
// contract.
func (b *SegmentBuilder) commitProps(parsed []parsedProp) (entries []propEntry, objectID string, hasObjectID bool, err error) {
	entries = make([]propEntry, len(parsed))
	for i, pp := range parsed {
		propID, err := b.internPropName(pp.name)
		if err != nil {
			return nil, "", false, err
		}
		e := propEntry{prop: propID, kind: pp.kind, num: pp.num}
		switch pp.kind {
		case propKindString, propKindArray, propKindObject:
			ref, length, appendErr := appendPropBytes(&b.propArena, pp.bytes)
			if appendErr != nil {
				return nil, "", false, appendErr
			}
			e.ref, e.len = ref, length
		}
		entries[i] = e

		if pp.name == "objectid" && pp.kind == propKindString {
			objectID, hasObjectID = string(pp.bytes), true
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].prop < entries[j].prop })
	return entries, objectID, hasObjectID, nil
}

// internPropName returns the PropID for name, interning it (assigning the
// next dense id) if this is the first time name has been seen by this
// SegmentBuilder. Mirrors Builder.internProp exactly, reusing the same
// package-level maxPropID constant: PropID is a uint16, so a SegmentBuilder
// that has already interned math.MaxUint16+1 distinct names must refuse to
// intern one more rather than let PropID(len(b.propNames)) silently wrap
// back to an id already in use, which would alias two different property
// names under the same PropID -- a silent correctness corruption, not
// merely a missing feature. OpenGraph's custom property schemas make an
// unusually large distinct-name count within one commit reachable in
// principle, unlike the base Builder's whole-graph case this guard was
// first written for, so SegmentBuilder needs the identical protection.
func (b *SegmentBuilder) internPropName(name string) (PropID, error) {
	if id, ok := b.propIDs[name]; ok {
		return id, nil
	}
	if len(b.propNames) > maxPropID {
		return 0, fmt.Errorf("snapshot: SegmentBuilder.internPropName: more than %d distinct property names (PropID, a uint16, would wrap)", maxPropID+1)
	}
	if b.propIDs == nil {
		b.propIDs = make(map[string]PropID)
	}
	id := PropID(len(b.propNames))
	b.propNames = append(b.propNames, name)
	b.propIDs[name] = id
	return id, nil
}

// Build packs the staged node and edge writes into an immutable Segment.
// Each id's final state is whatever its last AddNodeState/TombstoneNode (or
// AddEdgeState/TombstoneEdge) call staged -- see SegmentBuilder's doc.
func (b *SegmentBuilder) Build() *Segment {
	s := &Segment{
		names:      append([]string(nil), b.propNames...),
		arena:      append([]byte(nil), b.propArena...),
		addedKinds: copyKindNames(b.kindTable),
	}
	s.ids = make(map[string]PropID, len(b.propIDs))
	for k, v := range b.propIDs {
		s.ids[k] = v
	}

	nodeIDs := make([]uint64, 0, len(b.nodes))
	for id := range b.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool { return nodeIDs[i] < nodeIDs[j] })

	nodeStates := make(map[uint64]NodeSegState, len(nodeIDs))
	for _, id := range nodeIDs {
		nb := b.nodes[id]
		nodeStates[id] = NodeSegState{
			Tombstoned:  nb.tombstoned,
			KindIDs:     nb.kindIDs,
			ObjectID:    nb.objectID,
			HasObjectID: nb.hasObjectID,
			seg:         s,
			entries:     nb.entries,
		}
	}

	edgeIDs := make([]uint64, 0, len(b.edges))
	for id := range b.edges {
		edgeIDs = append(edgeIDs, id)
	}
	sort.Slice(edgeIDs, func(i, j int) bool { return edgeIDs[i] < edgeIDs[j] })

	edgeStates := make(map[uint64]EdgeSegState, len(edgeIDs))
	for _, id := range edgeIDs {
		eb := b.edges[id]
		edgeStates[id] = EdgeSegState{
			Tombstoned: eb.tombstoned,
			StartID:    eb.startID,
			EndID:      eb.endID,
			Kind:       eb.kind,
		}
	}

	s.nodeIDs = nodeIDs
	s.nodeStates = nodeStates
	s.edgeIDs = edgeIDs
	s.edgeStates = edgeStates
	s.objectIndex = buildSegmentObjectIndex(nodeIDs, nodeStates)
	s.selfLoopKinds = segmentSelfLoopKinds(edgeStates)
	s.edgeKinds = segmentEdgeKinds(edgeStates)
	s.approxBytes = computeSegmentApproxBytes(s)

	return s
}

// segmentSelfLoopKinds derives which edge kinds carry at least one live
// (non-tombstoned) self-loop edge in edgeStates -- the Segment counterpart
// of finalizeDerived's base-snapshot derivation, shared by
// SegmentBuilder.Build and MergeSegments the same way
// buildSegmentObjectIndex is. nil when there are none (the ordinary case).
func segmentSelfLoopKinds(edgeStates map[uint64]EdgeSegState) map[KindID]struct{} {
	var out map[KindID]struct{}
	for _, st := range edgeStates {
		if st.Tombstoned || st.StartID != st.EndID {
			continue
		}
		if out == nil {
			out = make(map[KindID]struct{})
		}
		out[st.Kind] = struct{}{}
	}
	return out
}

// segmentEdgeKinds returns every edge kind edgeStates writes a live edge
// for, or nil if it writes none. A tombstoned edge contributes nothing: if
// its kind still exists in the base snapshot, the base's own edgeKindSeen
// keeps the kind present for View.EdgeKindPresent, and if it does not, the
// tombstone cannot be the thing that makes it appear.
func segmentEdgeKinds(edgeStates map[uint64]EdgeSegState) map[KindID]struct{} {
	var out map[KindID]struct{}
	for _, st := range edgeStates {
		if st.Tombstoned {
			continue
		}
		if out == nil {
			out = make(map[KindID]struct{})
		}
		out[st.Kind] = struct{}{}
	}
	return out
}

// copyKindNames returns a copy of pairs, or an empty (non-nil) map if pairs
// is nil.
func copyKindNames(pairs map[KindID]string) map[KindID]string {
	out := make(map[KindID]string, len(pairs))
	for k, v := range pairs {
		out[k] = v
	}
	return out
}

// buildSegmentObjectIndex scans nodeIDs (assumed sorted ascending) and
// indexes every non-tombstoned node that carries a string objectid at all --
// HasObjectID, not a non-empty ObjectID, since "" is a value a node can
// genuinely have and PropStore indexes it too -- in the
// same ascending order MergeSegments and SegmentBuilder.Build both produce
// their id lists in -- shared by both, since Build and MergeSegments each
// need to derive an objectIndex from a final, already-collapsed
// id->NodeSegState set.
func buildSegmentObjectIndex(nodeIDs []uint64, nodeStates map[uint64]NodeSegState) map[string][]uint64 {
	index := make(map[string][]uint64)
	for _, id := range nodeIDs {
		st := nodeStates[id]
		if st.Tombstoned || !st.HasObjectID {
			continue
		}
		index[st.ObjectID] = append(index[st.ObjectID], id)
	}
	return index
}

// MergeSegments collapses segs -- ordered oldest first -- into one Segment
// whose per-id state (node or edge) is whichever input segment last wrote
// that id: for any id present in more than one input, the segment latest in
// segs wins, exactly like SegmentBuilder's own same-key-repeat rule. Kinds
// registered across every input's AddedKinds are unioned (a later input's
// name for the same KindID overwrites an earlier one, though in practice a
// KindID's name is expected to be stable once assigned).
//
// The result is independent of the inputs in the sense that mutating it --
// impossible, since Segment has no mutator methods once built -- could
// never reach back into an input's own state; the two are simply distinct
// map/slice headers. It is NOT independent in the deeper sense of owning
// separate bytes for everything a returned NodeSegState can decode: a
// carried-over NodeSegState's seg field still points at whichever input
// segment produced it, so PropValueByName/PropMap on it continue reading
// that input's names/arena rather than anything newly allocated here. This
// is deliberate aliasing, safe only because every Segment is immutable for
// the whole of its lifetime: nothing merged ever needs to invalidate, and
// no input is ever mutated by being merged. Skipping a full re-encode this
// way is why MergeSegments costs O(total node/edge count across segs)
// rather than O(total node/edge count plus total prop-bag size).
func MergeSegments(segs []*Segment) *Segment {
	nodeStates := make(map[uint64]NodeSegState)
	edgeStates := make(map[uint64]EdgeSegState)
	addedKinds := make(map[KindID]string)

	for _, seg := range segs {
		if seg == nil {
			continue
		}
		for _, id := range seg.nodeIDs {
			nodeStates[id] = seg.nodeStates[id]
		}
		for _, id := range seg.edgeIDs {
			edgeStates[id] = seg.edgeStates[id]
		}
		for k, v := range seg.addedKinds {
			addedKinds[k] = v
		}
	}

	nodeIDs := sortedUint64Keys(nodeStates)
	edgeIDs := sortedUint64Keys(edgeStates)

	merged := &Segment{
		nodeIDs:       nodeIDs,
		nodeStates:    nodeStates,
		edgeIDs:       edgeIDs,
		edgeStates:    edgeStates,
		addedKinds:    addedKinds,
		objectIndex:   buildSegmentObjectIndex(nodeIDs, nodeStates),
		selfLoopKinds: segmentSelfLoopKinds(edgeStates),
		edgeKinds:     segmentEdgeKinds(edgeStates),
	}
	merged.approxBytes = computeSegmentApproxBytes(merged)
	return merged
}

// sortedUint64Keys returns m's keys sorted ascending. Used by MergeSegments
// for both the node-id and edge-id sides.
func sortedUint64Keys[V any](m map[uint64]V) []uint64 {
	out := make([]uint64, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
