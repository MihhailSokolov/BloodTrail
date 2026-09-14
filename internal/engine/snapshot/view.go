// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"sort"
	"sync"
)

// View is the read surface every query executor runs against: one immutable
// base Snapshot, plus zero or more immutable Segments layered on top of it
// (oldest first). With no segments, every accessor below is a pure
// passthrough to the base -- exactly as before this file grew an overlay
// story. With segments, "newest wins": a pg id's effective state is
// whichever segment latest in the stack carries a record for it, else the
// base snapshot's own state -- see WithSegment and override.
//
// A View is immutable once constructed (WithSegment returns a new one
// rather than mutating the receiver); the only mutation happening anywhere
// in this file is memoization of a handful of derived projections (merged
// delta, virtual id assignment, kind bitmaps, the merged kind table, the
// edge-tombstone set, the delta adjacency index), each computed at most
// once per View and guarded accordingly (sync.Once, or a mutex-guarded map
// for the per-kind bitmaps) so concurrent readers of one View are safe.
type View struct {
	base *Snapshot

	// segments holds every delta Segment layered onto base, oldest first;
	// empty (nil) exactly when Overlay() is false.
	segments []*Segment

	// deltaOnce guards the one-time computation of merged (the newest-wins
	// collapse of segments via MergeSegments) and the virtual dense id
	// assignment derived from it (virtualPgIDs/pgToVirtual). Every other
	// memoized projection below is derived from merged, so they all call
	// ensureDelta first.
	deltaOnce sync.Once
	merged    *Segment // MergeSegments(segments); nil when segments is empty

	// virtualPgIDs holds the database ids of every delta-added node --
	// present in merged with a non-tombstoned effective state, but with no
	// base dense id -- in ascending pg-id order; virtualPgIDs[i]'s dense id
	// is base.NodeCount()+i. pgToVirtual is its inverse.
	virtualPgIDs []uint64
	pgToVirtual  map[uint64]NodeID

	// kindBitmapMu guards kindBitmaps, memoized lazily per (View, kind) --
	// see NodesOfKind.
	kindBitmapMu sync.Mutex
	kindBitmaps  map[KindID]*Bitset

	// kindsOnce guards kindTable, the merged base+AddedKinds table -- see
	// Kinds.
	kindsOnce sync.Once
	kindTable *KindTable

	// edgeTombOnce guards edgeTomb, the set of edge ids the merged delta
	// carries a record for at all (tombstoned or upserted) -- see
	// ensureEdgeTomb.
	edgeTombOnce sync.Once
	edgeTomb     map[uint64]struct{}

	// maxKindOnce guards maxKindCeil, the merged kind-id ceiling MaxKindID
	// returns -- see its doc.
	maxKindOnce sync.Once
	maxKindCeil KindID

	// deltaAdjOnce guards deltaOut/deltaIn, the per-dense-node index of
	// delta-added/-upserted edges -- see ensureDeltaAdjacency.
	deltaAdjOnce sync.Once
	deltaOut     map[NodeID][]deltaEdge
	deltaIn      map[NodeID][]deltaEdge
}

// NewView wraps base in a View with no segments layered on top (Overlay()
// is false).
func NewView(base *Snapshot) *View {
	return &View{base: base}
}

// Base returns the underlying base Snapshot this View wraps.
func (v *View) Base() *Snapshot {
	return v.base
}

// WithSegment returns a new View sharing v's base and every segment v
// already carries, plus seg layered on top as the newest. v itself is never
// mutated -- any reader still holding v keeps seeing exactly what it saw
// before this call, unaffected by seg.
func (v *View) WithSegment(seg *Segment) *View {
	segments := make([]*Segment, len(v.segments)+1)
	copy(segments, v.segments)
	segments[len(v.segments)] = seg
	return &View{base: v.base, segments: segments}
}

// Overlay reports whether this View has any delta segments layered over its
// base snapshot.
func (v *View) Overlay() bool {
	return len(v.segments) > 0
}

// SegmentCount returns how many delta segments are layered over the base
// snapshot -- 0 exactly when Overlay() is false. It exists for observability
// (the applier logs it per published View, and a compactor decides when to
// fold on it); nothing about a View's semantics depends on it.
func (v *View) SegmentCount() int {
	return len(v.segments)
}

// Segments returns every delta Segment layered onto this View's base
// snapshot, oldest first -- the same slice WithSegment/ensureDelta read
// internally, exposed so a caller outside this package can hand them to
// Fold without reaching into an unexported field. Its callers today are all
// in the engine package: SaveSnapshot (persist.go), folding a View back
// into a single flat Snapshot before writing a snapshot file, and the
// background compactor (compact.go), which reads this slice (alongside
// Base()) to capture what a fold should run against, decide when the stack
// itself needs collapsing or is due for compaction, and identify exactly
// which segments a captured prefix's tail consists of once a fold is ready
// to adopt.
//
// Safe to return the backing slice directly, with no defensive copy: a View
// is immutable once constructed (this file's own package doc), and
// WithSegment always allocates a fresh backing array rather than appending
// to v's own -- so nothing a caller does with the returned slice, short of
// writing through its elements (each a *Segment, and Segment is documented
// immutable in its own right), can ever affect v or any other View sharing
// its tail.
func (v *View) Segments() []*Segment {
	return v.segments
}

// SelfLoopHazard reports whether this View may contain a self-loop edge
// (start == end) whose kind is admitted by kinds -- an empty kinds list, the
// "any relationship type" pattern, admits every kind. It is an
// over-approximation by design: the base snapshot's set is derived at build
// time and a delta tombstone never clears a base kind's bit (only a
// compaction, which re-derives from what actually survives, does), so a
// true return means "a self-loop of an admitted kind may exist", while a
// false return is a proof that none does.
//
// The interpreter's variable-length trail executor declines to serve when
// this returns true: PostgreSQL's recursive-CTE expansion applies its
// self-loop dead-end rule to whichever pattern edge sits on the CTE's own
// SEED side, and dawgs chooses that side per query through
// version-specific optimizer heuristics (pattern reversal, per-step
// direction flips, constraint balancing) this engine deliberately does not
// mirror. With no admissible self-loop in the graph the rule can never
// fire, so the seeding choice is unobservable and serving is provably
// equivalent; with one present, delegating is the only answer that cannot
// silently diverge. See expand.go's package doc in interpret.
func (v *View) SelfLoopHazard(kinds []KindID) bool {
	if setAdmitsSelfLoop(v.base.selfLoopKinds, kinds) {
		return true
	}
	for _, seg := range v.segments {
		if setAdmitsSelfLoop(seg.selfLoopKinds, kinds) {
			return true
		}
	}
	return false
}

// setAdmitsSelfLoop reports whether set (a self-loop kind set, possibly nil)
// contains any kind admitted by kinds (empty = every kind).
func setAdmitsSelfLoop(set map[KindID]struct{}, kinds []KindID) bool {
	if len(set) == 0 {
		return false
	}
	if len(kinds) == 0 {
		return true
	}
	for _, k := range kinds {
		if _, ok := set[k]; ok {
			return true
		}
	}
	return false
}

// ensureDelta lazily computes merged (the newest-wins collapse of v's
// segment stack) and the virtual dense id assignment derived from it, at
// most once per View. Every pg id merged carries a non-tombstoned record
// for, and that has no base dense id, is a delta-added node; it gets a
// virtual dense id base.NodeCount()+i, assigned in the ascending pg-id
// order merged.IterNodes already iterates in -- so no separate sort is
// needed here.
func (v *View) ensureDelta() {
	v.deltaOnce.Do(func() {
		if len(v.segments) == 0 {
			return
		}
		v.merged = MergeSegments(v.segments)

		baseN := v.base.NodeCount()
		var added []uint64
		v.merged.IterNodes(func(id uint64, st NodeSegState) bool {
			if st.Tombstoned {
				return true
			}
			if _, ok := v.base.Dense(id); ok {
				return true
			}
			added = append(added, id)
			return true
		})
		if len(added) == 0 {
			return
		}
		v.virtualPgIDs = added
		v.pgToVirtual = make(map[uint64]NodeID, len(added))
		for i, pgID := range added {
			v.pgToVirtual[pgID] = NodeID(baseN + i)
		}
	})
}

// override returns dense node n's delta-effective NodeSegState -- the
// merged delta's record for n's database id, if any -- and whether one
// exists at all. A virtual node (n >= the base snapshot's own node count)
// always has one, by construction: it exists at all only because ensureDelta
// found a non-tombstoned record for it. A base node has one only when some
// segment in the stack touched its database id, upsert or tombstone.
func (v *View) override(n NodeID) (NodeSegState, bool) {
	if !v.Overlay() {
		return NodeSegState{}, false
	}
	v.ensureDelta()
	if v.merged == nil {
		return NodeSegState{}, false
	}

	baseN := v.base.NodeCount()
	if int(n) >= baseN {
		idx := int(n) - baseN
		if idx < 0 || idx >= len(v.virtualPgIDs) {
			return NodeSegState{}, false
		}
		return v.merged.NodeState(v.virtualPgIDs[idx])
	}
	return v.merged.NodeState(v.base.GraphIDs[n])
}

// deltaOverridden reports whether the merged delta carries a record --
// upsert or tombstone -- for database id pgID at all.
func (v *View) deltaOverridden(pgID uint64) bool {
	if v.merged == nil {
		return false
	}
	_, ok := v.merged.NodeState(pgID)
	return ok
}

// NodeCount returns the number of dense node ids visible through this View:
// the base snapshot's own count, plus every delta-added node (see
// ensureDelta). A tombstoned base node still occupies its dense id -- a
// tombstone hides a node, it does not renumber the ids after it -- so
// NodeCount does not shrink relative to the base; use Alive where a
// tombstoned id must be told apart from a live one.
func (v *View) NodeCount() int {
	v.ensureDelta()
	return v.base.NodeCount() + len(v.virtualPgIDs)
}

// EdgeCount returns the number of edges in the base snapshot. Unlike nodes,
// edges have no dense id an overlay needs to renumber or extend -- delta
// edges are always addressed by database edge id (see EdgeByID/
// EdgeStateByID) -- so this stays a pure base passthrough regardless of
// Overlay().
func (v *View) EdgeCount() int {
	return v.base.EdgeCount()
}

// Alive reports whether dense node n is currently visible through this
// View: false for a base node the merged delta has tombstoned, or for any
// id outside [0, NodeCount()); true for every other id in range --
// including every virtual id, which by construction (see ensureDelta) is
// only ever assigned to a non-tombstoned effective state. Callers that care
// whether a node still exists, rather than just whether its dense id is in
// range, must consult this once Overlay() is true: NodeCount growing to
// keep a tombstoned base node's dense id occupied means range membership
// alone no longer implies existence.
func (v *View) Alive(n NodeID) bool {
	if int(n) < 0 || int(n) >= v.NodeCount() {
		return false
	}
	st, ok := v.override(n)
	return !ok || !st.Tombstoned
}

// Dense returns the dense NodeID for a database id, and whether it exists.
// Checks the base snapshot first, then (if Overlay()) the virtual id
// assigned to a delta-added node -- see ensureDelta. A tombstoned base
// node's database id still resolves here (its dense id is hidden, not
// removed -- see Alive); a pg id the delta tombstoned outright, with no
// prior base or virtual existence, resolves nowhere, exactly like an
// unknown id.
func (v *View) Dense(databaseID uint64) (NodeID, bool) {
	if id, ok := v.base.Dense(databaseID); ok {
		return id, true
	}
	if !v.Overlay() {
		return 0, false
	}
	v.ensureDelta()
	id, ok := v.pgToVirtual[databaseID]
	return id, ok
}

// GraphID returns the database id dense NodeID n was built from -- not to be
// confused with Snapshot.GraphID, the identifier of the one graph this
// snapshot holds. Covers virtual ids exactly as Dense does in reverse.
func (v *View) GraphID(n NodeID) uint64 {
	baseN := v.base.NodeCount()
	if int(n) < baseN {
		return v.base.GraphIDs[n]
	}
	v.ensureDelta()
	return v.virtualPgIDs[int(n)-baseN]
}

// KindIDsOf returns dense NodeID n's kind ids. For a delta-overridden node
// (base or virtual), this is the effective NodeSegState's KindIDs -- a
// complete replacement, never merged with the base list -- or nil if that
// state is a tombstone. Otherwise it aliases the base snapshot's NodeKinds
// array exactly as the pre-overlay passthrough did.
func (v *View) KindIDsOf(n NodeID) []KindID {
	if st, ok := v.override(n); ok {
		if st.Tombstoned {
			return nil
		}
		return st.KindIDs
	}
	lo, hi := v.base.KindOffsets[n], v.base.KindOffsets[n+1]
	return v.base.NodeKinds[lo:hi]
}

// Out returns slice views over n's outgoing edges: aligned target, kind, and
// database edge id slices, sorted by (target, kind) -- see Snapshot.Out.
//
// Valid ONLY when !Overlay(): a base array slice cannot reflect a delta
// segment's added/tombstoned edges or nodes. Panics (a debug assertion)
// if called while Overlay() is true -- use OutEdges instead.
func (v *View) Out(n NodeID) (targets []NodeID, kinds []KindID, edgeIDs []uint64) {
	if v.Overlay() {
		panic("snapshot: View.Out called on an overlay View (Overlay() == true); use OutEdges instead")
	}
	lo, hi := v.base.OutOffsets[n], v.base.OutOffsets[n+1]
	return v.base.OutTargets[lo:hi], v.base.OutKinds[lo:hi], v.base.OutEdgeIDs[lo:hi]
}

// In returns slice views over n's incoming edges: aligned source and kind
// slices, aliasing the base snapshot exactly as Out does, sorted by (source,
// kind) -- see Snapshot.In.
//
// Unlike Out, this deliberately returns no edge id slice, and so is an exact
// mirror of Snapshot.In. It could not alias one anyway: the reverse CSR
// stores each slot's forward-array index (Snapshot.InEdgeIdx) rather than the
// database edge id itself, so edge ids would have to be gathered through
// OutEdgeIDs into a freshly allocated slice on every call -- an allocation
// and a gather sized to n's in-degree, which is multi-megabyte work on a hub
// node and lands on every backward-BFS expansion in this package's hot path.
// No caller ever wanted it: the traversals here read only sources and kinds,
// and the one call site that needs to identify the edges themselves
// (interpret's In-expansion) wants the forward slot INDEX, not the id, and
// reads it straight off Base().InEdgeIdx. A caller that genuinely needs
// resolved edge ids should use InEdges, which streams them without
// materializing a slice at all.
//
// Valid ONLY when !Overlay() -- see Out's doc. Panics (a debug assertion)
// if called while Overlay() is true -- use InEdges instead.
func (v *View) In(n NodeID) (sources []NodeID, kinds []KindID) {
	if v.Overlay() {
		panic("snapshot: View.In called on an overlay View (Overlay() == true); use InEdges instead")
	}
	lo, hi := v.base.InOffsets[n], v.base.InOffsets[n+1]
	return v.base.InTargets[lo:hi], v.base.InKinds[lo:hi]
}

// deltaEdge is one delta-added or delta-upserted edge, from the perspective
// of one of its endpoints, with the OTHER endpoint already resolved to a
// dense NodeID -- the unit ensureDeltaAdjacency indexes per node for
// OutEdges/InEdges to walk.
type deltaEdge struct {
	other  NodeID
	kind   KindID
	edgeID uint64
}

// ensureEdgeTomb lazily computes edgeTomb: the set of every edge id the
// merged delta carries a record for at all, tombstoned or upserted. Any base
// forward-CSR slot naming one of these ids must be skipped by
// OutEdges/InEdges: a tombstoned edge is gone, and an upserted one is
// re-emitted from the delta adjacency index instead (see
// ensureDeltaAdjacency), so leaving the base slot in would double-emit a
// stale copy alongside the authoritative one.
//
// A map[uint64]struct{} keyed by edge id, consulted once per base forward
// slot, is the deliberate v1 choice here. A Bitset over base forward-slot
// indices (rather than a map keyed by database edge id) would avoid the
// hashing, at the cost of needing the slot index rather than the edge id at
// the call site -- a plausible follow-up if this ever shows up hot in
// profiling, but Views are short-lived between commits, so the simpler
// structure was preferred for this first cut.
func (v *View) ensureEdgeTomb() {
	v.edgeTombOnce.Do(func() {
		v.ensureDelta()
		if v.merged == nil {
			return
		}
		m := make(map[uint64]struct{}, v.merged.EdgeCount())
		v.merged.IterEdges(func(id uint64, _ EdgeSegState) bool {
			m[id] = struct{}{}
			return true
		})
		v.edgeTomb = m
	})
}

// ensureDeltaAdjacency lazily computes deltaOut/deltaIn: per dense node id,
// the list of every non-tombstoned edge the merged delta carries a record
// for that touches it, with the OTHER endpoint resolved to a dense id via
// Dense. This covers both brand-new edges (no base slot at all) and
// delta-upserted edges that also exist in base (skipped there via
// ensureEdgeTomb, emitted here instead as the authoritative copy -- see its
// doc). An edge whose endpoint doesn't resolve via Dense at all, or resolves
// to a node that isn't Alive, is dropped: the former mirrors the loader's
// own tolerance for a dangling reference (see Segment's doc), the latter is
// the tombstone-cascade OutEdges/InEdges document.
func (v *View) ensureDeltaAdjacency() {
	v.deltaAdjOnce.Do(func() {
		v.ensureDelta()
		if v.merged == nil {
			return
		}
		out := make(map[NodeID][]deltaEdge)
		in := make(map[NodeID][]deltaEdge)
		v.merged.IterEdges(func(id uint64, st EdgeSegState) bool {
			if st.Tombstoned {
				return true
			}
			s, sOK := v.Dense(st.StartID)
			e, eOK := v.Dense(st.EndID)
			if !sOK || !eOK || !v.Alive(s) || !v.Alive(e) {
				return true
			}
			out[s] = append(out[s], deltaEdge{other: e, kind: st.Kind, edgeID: id})
			in[e] = append(in[e], deltaEdge{other: s, kind: st.Kind, edgeID: id})
			return true
		})
		v.deltaOut = out
		v.deltaIn = in
	})
}

// OutEdges iterates dense node n's outgoing edges through this View's
// overlay: base forward-CSR slots (skipping any whose edge id the merged
// delta carries a record for at all, or whose target is not Alive) followed
// by the delta's own added/upserted out-edges touching n. yield returning
// false stops iteration early. A non-Alive n yields nothing.
//
// This is the overlay-aware counterpart to Out, which remains valid only
// when !Overlay() -- see its doc.
func (v *View) OutEdges(n NodeID, yield func(target NodeID, kind KindID, edgeID uint64) bool) {
	if !v.Alive(n) {
		return
	}
	if int(n) < v.base.NodeCount() {
		v.ensureEdgeTomb()
		lo, hi := v.base.OutOffsets[n], v.base.OutOffsets[n+1]
		for i := lo; i < hi; i++ {
			edgeID := v.base.OutEdgeIDs[i]
			if _, skip := v.edgeTomb[edgeID]; skip {
				continue
			}
			target := v.base.OutTargets[i]
			if !v.Alive(target) {
				continue
			}
			if !yield(target, v.base.OutKinds[i], edgeID) {
				return
			}
		}
	}
	v.ensureDeltaAdjacency()
	for _, de := range v.deltaOut[n] {
		if !yield(de.other, de.kind, de.edgeID) {
			return
		}
	}
}

// InEdges is OutEdges' incoming-edge counterpart -- the overlay-aware
// version of In, which remains valid only when !Overlay().
func (v *View) InEdges(n NodeID, yield func(source NodeID, kind KindID, edgeID uint64) bool) {
	if !v.Alive(n) {
		return
	}
	if int(n) < v.base.NodeCount() {
		v.ensureEdgeTomb()
		lo, hi := v.base.InOffsets[n], v.base.InOffsets[n+1]
		for i := lo; i < hi; i++ {
			fwdIdx := v.base.InEdgeIdx[i]
			edgeID := v.base.OutEdgeIDs[fwdIdx]
			if _, skip := v.edgeTomb[edgeID]; skip {
				continue
			}
			source := v.base.InTargets[i]
			if !v.Alive(source) {
				continue
			}
			if !yield(source, v.base.InKinds[i], edgeID) {
				return
			}
		}
	}
	v.ensureDeltaAdjacency()
	for _, de := range v.deltaIn[n] {
		if !yield(de.other, de.kind, de.edgeID) {
			return
		}
	}
}

// sourceOfSlot returns the dense NodeID whose forward-CSR segment contains
// fwdIdx, via binary search over OutOffsets (length NodeCount()+1,
// monotonically non-decreasing) -- the one piece of information a bare
// forward-CSR slot index doesn't carry on its own (Snapshot.EdgeByID hands
// back only a slot to index OutTargets/OutKinds with, never the source).
func sourceOfSlot(s *Snapshot, fwdIdx uint64) NodeID {
	n := s.NodeCount()
	i := sort.Search(n, func(i int) bool { return s.OutOffsets[i+1] > fwdIdx })
	return NodeID(i)
}

// EdgeByID looks up the forward-CSR slot of the edge with the given database
// id -- see Snapshot.EdgeByID. It misses (ok = false) for any id the merged
// delta carries a record for at all, tombstoned or upserted: a virtual
// (delta-added) edge has no base forward-CSR slot to name at all, and a
// delta-upserted edge's current state is authoritative over whatever the
// stale base slot still holds (pg's edge-property-merge upserts, and any
// hypothetical kind/endpoint change), so returning that stale slot index
// would be actively wrong rather than merely incomplete. Use
// EdgeStateByID for any id this misses on while Overlay() is true.
func (v *View) EdgeByID(id uint64) (fwdIdx uint64, ok bool) {
	if v.Overlay() {
		v.ensureDelta()
		if v.merged != nil {
			if _, isDelta := v.merged.EdgeState(id); isDelta {
				return 0, false
			}
		}
	}
	return v.base.EdgeByID(id)
}

// EdgeStateByID returns edge id's overlay-complete effective state --
// dense-resolved endpoints and kind -- and whether id currently resolves to
// a live edge at all. It misses for an unknown id, an edge the merged delta
// tombstoned, or an edge whose endpoint doesn't resolve to a live node
// (mirroring the same dangling/cascade tolerance ensureDeltaAdjacency
// documents). Unlike EdgeByID, which only ever names a base forward-CSR
// slot, this also answers for delta-added or delta-upserted edges, which
// have no base slot to index into at all -- a caller resolving an edge id
// that might have been introduced or changed by a delta segment must use
// this instead of EdgeByID.
func (v *View) EdgeStateByID(id uint64) (start, end NodeID, kind KindID, ok bool) {
	if v.Overlay() {
		v.ensureDelta()
		if v.merged != nil {
			if st, isDelta := v.merged.EdgeState(id); isDelta {
				if st.Tombstoned {
					return 0, 0, 0, false
				}
				s, sOK := v.Dense(st.StartID)
				e, eOK := v.Dense(st.EndID)
				if !sOK || !eOK || !v.Alive(s) || !v.Alive(e) {
					return 0, 0, 0, false
				}
				return s, e, st.Kind, true
			}
		}
	}

	fwdIdx, ok := v.base.EdgeByID(id)
	if !ok {
		return 0, 0, 0, false
	}
	target := v.base.OutTargets[fwdIdx]
	src := sourceOfSlot(v.base, fwdIdx)
	if !v.Alive(src) || !v.Alive(target) {
		return 0, 0, 0, false
	}
	return src, target, v.base.OutKinds[fwdIdx], true
}

// containsKindID reports whether k appears in kinds -- a node's kind list is
// typically tiny, so a linear scan is preferred over building any auxiliary
// structure per node.
func containsKindID(kinds []KindID, k KindID) bool {
	for _, kk := range kinds {
		if kk == k {
			return true
		}
	}
	return false
}

// NodesOfKind returns the merged bitset of dense NodeIDs carrying kind k,
// sized to NodeCount(). Memoized lazily per (View, kind) under
// kindBitmapMu: Views are short-lived between commits, so this trades a
// small map lookup for never recomputing the same kind's bitmap twice
// against one View.
func (v *View) NodesOfKind(k KindID) *Bitset {
	if !v.Overlay() {
		return v.base.NodesOfKind(k)
	}

	v.kindBitmapMu.Lock()
	defer v.kindBitmapMu.Unlock()
	if bm, ok := v.kindBitmaps[k]; ok {
		return bm
	}
	bm := v.buildKindBitmap(k)
	if v.kindBitmaps == nil {
		v.kindBitmaps = make(map[KindID]*Bitset)
	}
	v.kindBitmaps[k] = bm
	return bm
}

// buildKindBitmap computes NodesOfKind(k)'s merged result: copy the base
// bitmap's bits for every base node the delta didn't override at all, then
// let the merged delta's effective kind membership decide every
// delta-touched node (base-overridden or virtual) -- set if its effective,
// non-tombstoned state carries k, otherwise left clear. This naturally
// covers a base node losing k, gaining it, or a virtual node carrying it
// from the start.
func (v *View) buildKindBitmap(k KindID) *Bitset {
	v.ensureDelta()
	out := NewBitset(v.NodeCount())

	base := v.base.NodesOfKind(k)
	base.Iterate(func(id NodeID) bool {
		if _, overridden := v.override(id); overridden {
			return true // decided entirely by the merged delta below
		}
		out.Set(id)
		return true
	})

	if v.merged != nil {
		v.merged.IterNodes(func(pgID uint64, st NodeSegState) bool {
			if st.Tombstoned || !containsKindID(st.KindIDs, k) {
				return true
			}
			if nid, ok := v.Dense(pgID); ok {
				out.Set(nid)
			}
			return true
		})
	}

	return out
}

// Kinds returns the merged base+AddedKinds id<->name table. Memoized once
// per View: base.Kinds is returned as-is (no allocation) when the merged
// delta registered no new kinds at all, otherwise a brand-new KindTable is
// built (base's own table is never mutated).
func (v *View) Kinds() *KindTable {
	if !v.Overlay() {
		return v.base.Kinds
	}

	v.kindsOnce.Do(func() {
		v.ensureDelta()
		var added map[KindID]string
		if v.merged != nil {
			added = v.merged.AddedKinds()
		}
		if len(added) == 0 {
			v.kindTable = v.base.Kinds
			return
		}

		pairs := make(map[KindID]string, len(v.base.Kinds.names)+len(added))
		for id, name := range v.base.Kinds.names {
			if name != "" {
				pairs[KindID(id)] = name
			}
		}
		for id, name := range added {
			pairs[id] = name
		}
		v.kindTable = NewKindTable(pairs)
	})
	return v.kindTable
}

// MaxKindID returns the largest KindID this View's callers should size a
// snapshot.KindMask to. For a base-only View (Overlay() == false) this is
// exactly base.MaxKindID, unchanged -- the highest kind id any BASE node or
// edge actually carries (Snapshot.MaxKindID's own doc). For an overlay View
// it is raised to also cover every kind id the merged delta introduces:
// those registered via Segment.AddedKinds, AND those carried by a delta
// node's KindIDs or a delta edge's Kind.
//
// The second half matters as much as the first, and is not implied by it.
// AddedKinds only carries kinds the applier had to resolve because the base
// View's kind TABLE did not name them; a kind the table already knows --
// because some earlier write asserted it in PostgreSQL, whether or not any
// node or edge in this base actually carries it -- is absent from AddedKinds
// while still being able to exceed base.MaxKindID, which counts only kinds
// base rows actually carry. A delta edge of such a kind would then be
// filtered out by every KindMask sized from this ceiling (KindMask.Set/Has
// both no-op above it), silently serving zero rows for a relationship that
// demonstrably exists.
//
// This is the ceiling every snapshot.KindMask sizing call site outside this
// package (buildKindMask, buildKindMaskSeam, selectKindIDs, kindMaskFor)
// should read instead of Base().MaxKindID directly: a mask sized off
// Base().MaxKindID alone silently drops every delta-introduced kind
// (KindMask.Set/Has both no-op above the mask's own ceiling), producing
// wrong-but-not-panicking rows -- a resolved-but-unmapped kind id, or an
// edge/node of that kind missing from a kind-filtered result -- rather than
// an error.
//
// Memoized once per View (maxKindOnce): the merged delta's AddedKinds is
// typically empty and never large (at most one commit's worth of newly
// registered kinds), but Segment.AddedKinds allocates a fresh copy on every
// call, so this computes the ceiling at most once regardless of how many
// call sites ask.
func (v *View) MaxKindID() KindID {
	if !v.Overlay() {
		return v.base.MaxKindID
	}

	v.maxKindOnce.Do(func() {
		v.ensureDelta()
		max := v.base.MaxKindID
		if v.merged != nil {
			for id := range v.merged.AddedKinds() {
				if id > max {
					max = id
				}
			}
			v.merged.IterNodes(func(_ uint64, st NodeSegState) bool {
				if st.Tombstoned {
					return true
				}
				for _, id := range st.KindIDs {
					if id > max {
						max = id
					}
				}
				return true
			})
			v.merged.IterEdges(func(_ uint64, st EdgeSegState) bool {
				if !st.Tombstoned && st.Kind > max {
					max = st.Kind
				}
				return true
			})
		}
		v.maxKindCeil = max
	})
	return v.maxKindCeil
}

// MultiGraph reports whether the source database held more than one graph
// with at least one node, as of the base snapshot's load.
func (v *View) MultiGraph() bool {
	return v.base.MultiGraph
}

// bytesPerDeltaEdge is deltaEdge's fixed in-memory size, used by
// ApproxBytes for the delta adjacency index -- computed via a struct
// literal's field sizes rather than guessed, in the same spirit as
// bytesPerPropEntry in props.go.
const bytesPerDeltaEdge = 4 + 2 + 8 // NodeID (uint32) + KindID (int16) + uint64, ignoring struct padding

// ApproxBytes estimates this View's resident memory footprint: the base
// snapshot's own footprint, plus every layered segment's ApproxBytes, plus a
// rough estimate of whatever overlay projections have been memoized so far
// (kind bitmaps always; the merged kind table, edge-tombstone set, and delta
// adjacency index are force-computed here if Overlay() so the estimate
// doesn't depend on which accessors happen to have been called before this
// one).
func (v *View) ApproxBytes() uint64 {
	total := v.base.ApproxBytes()
	for _, seg := range v.segments {
		total += seg.ApproxBytes()
	}
	if !v.Overlay() {
		return total
	}

	v.ensureEdgeTomb()
	v.ensureDeltaAdjacency()

	if kt := v.Kinds(); kt != v.base.Kinds {
		total += kt.ApproxBytes()
	}

	v.kindBitmapMu.Lock()
	for _, bm := range v.kindBitmaps {
		total += approxKindBitmapEntryBytes
		total += uint64(len(bm.words)) * bytesPerUint64
	}
	v.kindBitmapMu.Unlock()

	total += uint64(len(v.edgeTomb)) * bytesPerUint64 // map[uint64]struct{}, key bytes only
	for _, edges := range v.deltaOut {
		total += uint64(len(edges)) * bytesPerDeltaEdge
	}
	for _, edges := range v.deltaIn {
		total += uint64(len(edges)) * bytesPerDeltaEdge
	}

	return total
}

// PropIDByName returns the PropID interned for name, and whether one
// exists -- see PropStore.IDByName. This only ever resolves names the base
// snapshot's own PropStore interned at load time: a property name that
// first appears in a delta segment's upserts, and was never seen by the
// base snapshot, has no PropID here at all, even though a delta-overridden
// node might carry it. PropValueByName resolves by name instead, and
// answers correctly in both cases -- prefer it over the
// PropIDByName+PropValue pair once a View might be an overlay one and might
// need a segment-only property name.
func (v *View) PropIDByName(name string) (PropID, bool) {
	return v.base.Props.IDByName(name)
}

// PropValue returns node n's value for property id -- see PropStore.Value.
// For a delta-overridden node (see override), id (always a base PropID --
// see PropIDByName's doc) is resolved back to its name via the base
// PropStore, and the value is answered from the node's effective
// NodeSegState instead of the stale base value.
func (v *View) PropValue(n NodeID, id PropID) (any, bool) {
	if st, ok := v.override(n); ok {
		if st.Tombstoned {
			return nil, false
		}
		name := v.base.Props.Name(id)
		if name == "" {
			return nil, false
		}
		return st.PropValueByName(name)
	}
	return v.base.Props.Value(n, id)
}

// PropValueByName returns node n's value for the named property, resolving
// by name rather than PropID -- the overlay-safe way to read a
// delta-overridden node's properties, since PropIDByName can miss a name
// that only ever appeared in a delta segment (see its doc). Executors
// currently read node properties via the PropIDByName+PropValue pair;
// migrating a call site to this instead is only needed once it might run
// against an overlay View and might need a segment-only property name.
func (v *View) PropValueByName(n NodeID, name string) (any, bool) {
	if st, ok := v.override(n); ok {
		if st.Tombstoned {
			return nil, false
		}
		return st.PropValueByName(name)
	}
	id, ok := v.base.Props.IDByName(name)
	if !ok {
		return nil, false
	}
	return v.base.Props.Value(n, id)
}

// PropNodeMap returns node n's full property bag as a fresh map -- see
// PropStore.NodeMap. A delta-overridden node's bag comes wholly from its
// effective NodeSegState (bags are complete replacements, never merged with
// the base bag -- see NodeSegState's doc); a tombstoned node's bag is empty.
func (v *View) PropNodeMap(n NodeID) map[string]any {
	if st, ok := v.override(n); ok {
		if st.Tombstoned {
			return map[string]any{}
		}
		return st.PropMap()
	}
	return v.base.Props.NodeMap(n)
}

// NodeByObjectID returns one node whose objectid property has the exact
// string value objectID, and whether any match was found -- see
// PropStore.NodeByObjectID. Delegates to NodesByObjectID's overlay-aware
// result when Overlay() (its first match after a deterministic sort, in
// place of PropStore's own "arbitrary witness" -- see its doc; still just
// one witness among possibly-several, per the same contract).
func (v *View) NodeByObjectID(objectID string) (NodeID, bool) {
	if !v.Overlay() {
		return v.base.Props.NodeByObjectID(objectID)
	}
	ids, ok := v.NodesByObjectID(objectID)
	if !ok {
		return 0, false
	}
	return ids[0], true
}

// NodesByObjectID returns every node whose objectid property has the exact
// string value objectID, and whether any match was found -- see
// PropStore.NodesByObjectID. Overlay-aware: consults the merged delta's own
// NodesByObjectID first (already newest-wins and tombstone-excluded, since
// it is built from the merged Segment's final per-id state -- see
// buildSegmentObjectIndex), then adds every base match whose database id
// the delta didn't touch at all. A base match the delta DID touch is never
// added here, even if its objectid value happens to be unchanged: that
// node's fate under this objectID is entirely decided by the merged lookup
// above, so a delta-changed objectid can never resolve via its stale base
// value.
func (v *View) NodesByObjectID(objectID string) ([]NodeID, bool) {
	if !v.Overlay() {
		return v.base.Props.NodesByObjectID(objectID)
	}
	v.ensureDelta()

	var out []NodeID
	if v.merged != nil {
		for _, pgID := range v.merged.NodesByObjectID(objectID) {
			if id, ok := v.Dense(pgID); ok {
				out = append(out, id)
			}
		}
	}
	if baseIDs, ok := v.base.Props.NodesByObjectID(objectID); ok {
		for _, id := range baseIDs {
			if v.deltaOverridden(v.base.GraphIDs[id]) {
				continue
			}
			out = append(out, id)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, true
}
