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
	id         uint64
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

	// Per-node property bag staging (see props.go): propNames/propIDs intern
	// property names Builder-wide; propEntries/propOffsets mirror
	// kindsFlat/kindOffsets's flat-array-plus-offsets shape, one entry per
	// node property; propArena is the shared byte arena that string and
	// array/object property values are appended to.
	propNames   []string
	propIDs     map[string]PropID
	propEntries []propEntry
	propOffsets []uint32 // len(ids)+1, into propEntries
	propArena   []byte

	edges []stagedEdge

	kindTable map[KindID]string // set via SetKinds; nil until then
}

// NewBuilder creates an empty Builder for the given graph id.
func NewBuilder(graphID int32) *Builder {
	return &Builder{
		graphID:     graphID,
		kindOffsets: []uint32{0},
		propOffsets: []uint32{0},
	}
}

// AddNode stages a node. databaseID must be strictly greater than every
// previously added node's databaseID; otherwise AddNode returns an error and
// the node is not staged.
//
// propsJSON is the node's jsonb property bag as raw bytes (nil or empty,
// i.e. "{}", means no properties); it is parsed and validated before any
// Builder state is mutated, so a decode error (see parseNodeProps) also
// leaves the node unstaged, exactly like the ascending-id check above.
//
// AddNode does both of ParseProps's and AddParsedNode's jobs on one
// goroutine (parse then commit); a caller loading many nodes and wanting to
// parallelize the parse half across a worker pool should call those two
// directly instead -- see AddParsedNode's doc.
func (b *Builder) AddNode(databaseID uint64, kinds []KindID, propsJSON []byte) error {
	parsed, err := parseNodeProps(propsJSON)
	if err != nil {
		return fmt.Errorf("snapshot: AddNode: databaseID %d: %w", databaseID, err)
	}
	if err := b.addParsedNode(databaseID, kinds, parsed); err != nil {
		return fmt.Errorf("snapshot: AddNode: %w", err)
	}
	return nil
}

// AddParsedNode is AddNode's commit-only half: it stages a node whose
// property bag was already parsed and validated by the package-level
// ParseProps, typically off the hot path in a worker pool (see ParseProps's
// doc for why). Like AddNode, databaseID must be strictly greater than
// every previously staged node's databaseID.
//
// AddParsedNode is not safe to call concurrently -- with itself, with
// AddNode, or with any other Builder method -- since it mutates the
// Builder's shared state (the id/kind arrays, the property intern table,
// and the shared arena) exactly as AddNode's commit half does. Concurrency
// belongs entirely on the ParseProps side, ahead of a single, ordered
// stream of AddParsedNode calls.
func (b *Builder) AddParsedNode(databaseID uint64, kinds []KindID, props ParsedProps) error {
	return b.addParsedNode(databaseID, kinds, props.parsed)
}

// addParsedNode is AddNode's and AddParsedNode's shared commit step: the
// ascending-id check, committing already-parsed properties, and staging
// id/kinds. See AddNode's and AddParsedNode's docs for the ordering and
// concurrency contract this relies on.
//
// commitNodeProps runs BEFORE id/kinds are staged, deliberately: its only
// failure mode (internProp's PropID-wrap guard -- see its own doc) must
// leave this node completely unstaged, the same "no partial mutation on
// error" contract AddNode's own doc promises for a parse failure --
// reversing the order would instead leave b.ids/b.kindsFlat/b.kindOffsets
// one node ahead of b.propOffsets, corrupting every subsequent Value/
// NodeMap lookup's row alignment for the rest of this Builder's life.
func (b *Builder) addParsedNode(databaseID uint64, kinds []KindID, parsed []parsedProp) error {
	if n := len(b.ids); n > 0 && databaseID <= b.ids[n-1] {
		return fmt.Errorf("databaseID %d is not strictly greater than previous %d", databaseID, b.ids[n-1])
	}
	if err := b.commitNodeProps(parsed); err != nil {
		return err
	}

	b.ids = append(b.ids, databaseID)
	b.kindsFlat = append(b.kindsFlat, kinds...)
	b.kindOffsets = append(b.kindOffsets, uint32(len(b.kindsFlat)))
	return nil
}

// preparedProps is one node's already-encoded property entries, exactly as
// stored in a base Snapshot's PropStore or in one of a Segment's
// NodeSegStates: entries whose prop field indexes into names -- some OTHER
// source's name table, not yet this Builder's own intern table -- and whose
// string/array/object entries' ref/len fields index into arena -- that same
// source's byte arena, not yet this Builder's own propArena.
// Builder.addPreparedNode is what actually remaps and transplants one of
// these into a fresh Builder; see its doc for why Fold (the only caller)
// needs this at all.
type preparedProps struct {
	entries []propEntry
	names   []string
	arena   []byte
}

// addPreparedNode stages a node whose property bag is already encoded as
// propEntry values from another source (a base Snapshot's PropStore, or one
// of a Segment's NodeSegStates) -- Fold's building block for folding a
// delta into a fresh base without ever re-parsing the original JSON (see
// fold.go). Unlike AddParsedNode, which commits a freshly-parsed bag whose
// property names have never been interned anywhere, this must first REMAP
// every entry's prop id: props.entries' prop fields index props.names, a
// DIFFERENT source's name table (the base Snapshot's PropStore.names, or
// one input Segment's own names -- see NodeSegState's doc on why its seg
// field keeps pointing at its ORIGINAL segment through a MergeSegments
// merge), not this Builder's own intern table. Each entry's name is looked
// up in props.names and re-interned via internProp, and string/array/object
// payloads are copied from props.arena into this Builder's own propArena at
// their new offsets -- so the result aliases neither the source's name
// table nor its arena.
//
// Because interning order can differ between this Builder and whichever
// source built props.entries, the entries' relative order by (remapped)
// prop id is not guaranteed to survive the remap -- entries is re-sorted by
// the new prop ids before being committed, exactly as commitNodeProps sorts
// after its own interning pass.
//
// Like addParsedNode, databaseID must be strictly greater than every
// previously staged node's databaseID, checked before anything else is
// touched. internProp's PropID-wrap guard is this method's only other
// failure mode; on that error, b.ids/b.kindsFlat/b.kindOffsets/
// b.propEntries/b.propOffsets are left exactly as they were before this
// call, the same "no partial mutation on error" contract addParsedNode's
// own doc promises -- modulo the same small, deliberate exception
// commitNodeProps already documents: a property whose bytes were already
// appended to b.propArena before a LATER property in the same bag hit the
// guard leaves those bytes orphaned in the arena rather than reclaimed.
func (b *Builder) addPreparedNode(databaseID uint64, kinds []KindID, props preparedProps) error {
	if n := len(b.ids); n > 0 && databaseID <= b.ids[n-1] {
		return fmt.Errorf("databaseID %d is not strictly greater than previous %d", databaseID, b.ids[n-1])
	}

	entries := make([]propEntry, len(props.entries))
	for i, e := range props.entries {
		var name string
		if int(e.prop) < len(props.names) {
			name = props.names[e.prop]
		}
		newID, err := b.internProp(name)
		if err != nil {
			return err
		}
		ne := propEntry{prop: newID, kind: e.kind, num: e.num}
		switch e.kind {
		case propKindString, propKindArray, propKindObject:
			ne.ref = uint32(len(b.propArena))
			b.propArena = append(b.propArena, props.arena[e.ref:e.ref+e.len]...)
			ne.len = e.len
		}
		entries[i] = ne
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].prop < entries[j].prop })

	b.propEntries = append(b.propEntries, entries...)
	b.propOffsets = append(b.propOffsets, uint32(len(b.propEntries)))

	b.ids = append(b.ids, databaseID)
	b.kindsFlat = append(b.kindsFlat, kinds...)
	b.kindOffsets = append(b.kindOffsets, uint32(len(b.kindsFlat)))
	return nil
}

// AddEdge stages an edge between two database node ids, carrying its own
// database edge id (id). Edges may be added in any order and are resolved
// against staged nodes at Build time; an edge whose start or end id never
// matches a staged node is dropped at Build time rather than rejected here
// (see Build), since AddEdge cannot know at call time whether a
// not-yet-staged id will be added later.
func (b *Builder) AddEdge(id, startID, endID uint64, kind KindID) {
	b.edges = append(b.edges, stagedEdge{id: id, start: startID, end: endID, kind: kind})
}

// SetKinds registers the database's complete kind id<->name table, to be
// exposed on the built Snapshot as Kinds. It is independent of AddNode's
// per-node kind lists: pairs is the global `kind` table's entire contents,
// not just the kinds this graph's nodes and edges happen to use. Call it at
// most once, before Build; if never called, Build produces an empty (but
// non-nil) KindTable.
func (b *Builder) SetKinds(pairs map[KindID]string) {
	b.kindTable = pairs
}

// denseEdge is a staged edge with endpoints resolved to dense NodeIDs.
type denseEdge struct {
	id       uint64
	src, dst NodeID
	kind     KindID
}

// Build packs the staged nodes and edges into an immutable Snapshot.
//
// Packing itself runs in O(N+E): a counting sort places each edge directly
// into its node's segment of the CSR arrays via prefix-summed offsets and
// per-node cursors, followed by a sort of each (typically small) per-node
// segment — never a global comparison sort over all edges. The one
// exception is edgeIDPerm (see buildEdgeIDPerm), a single O(E log E) sort
// over every edge, needed once so Snapshot.EdgeByID can binary-search by
// database edge id in O(log E) afterward.
//
// An edge whose start or end id does not match any staged node is dropped
// rather than failing the build; the returned Snapshot's DroppedEdges
// counts how many. This tolerates a graph that isn't perfectly
// self-consistent at load time (e.g. a node deleted concurrently with the
// edge scan of a repeatable-read transaction, or a caller feeding partial
// data) without discarding an otherwise-usable snapshot over a handful of
// bad edges. Build's error return is retained for a future validation this
// tolerant approach doesn't need today, so it currently always returns a
// nil error.
func (b *Builder) Build() (*Snapshot, error) {
	n := len(b.ids)

	graphIDs := make([]uint64, n)
	copy(graphIDs, b.ids)

	idIndex := make(map[uint64]NodeID, n)
	for i, id := range graphIDs {
		idIndex[id] = NodeID(i)
	}

	dEdges := make([]denseEdge, 0, len(b.edges))
	dropped := 0
	for _, e := range b.edges {
		src, ok := idIndex[e.start]
		if !ok {
			dropped++
			continue
		}
		dst, ok := idIndex[e.end]
		if !ok {
			dropped++
			continue
		}
		dEdges = append(dEdges, denseEdge{id: e.id, src: src, dst: dst, kind: e.kind})
	}

	outOffsets, outTargets, outKinds, outEdgeIDs := packForward(n, dEdges)
	inOffsets, inTargets, inKinds, inEdgeIdx := packReverse(n, outOffsets, outTargets, outKinds)
	edgeIDPerm := buildEdgeIDPerm(outEdgeIDs)

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
		GraphID:      b.graphID,
		GraphIDs:     graphIDs,
		OutOffsets:   outOffsets,
		OutTargets:   outTargets,
		OutKinds:     outKinds,
		OutEdgeIDs:   outEdgeIDs,
		InOffsets:    inOffsets,
		InTargets:    inTargets,
		InKinds:      inKinds,
		InEdgeIdx:    inEdgeIdx,
		KindOffsets:  kindOffsets,
		NodeKinds:    nodeKinds,
		MaxKindID:    maxKind,
		Kinds:        NewKindTable(b.kindTable),
		Props:        b.buildPropStore(n),
		DroppedEdges: dropped,
		BuiltAt:      time.Now(),
		idIndex:      idIndex,
		kindBitmaps:  kindBitmaps,
		edgeIDPerm:   edgeIDPerm,
	}, nil
}

// packForward counting-sorts dEdges into forward CSR form keyed by source
// node, then sorts each node's (typically small) segment by (target, kind).
// The database edge id rides along with each edge through both the
// counting sort and the segment sort, so OutEdgeIDs comes out aligned with
// OutTargets/OutKinds slot for slot.
func packForward(n int, dEdges []denseEdge) (offsets []uint64, targets []NodeID, kinds []KindID, edgeIDs []uint64) {
	degree := make([]uint32, n)
	for _, e := range dEdges {
		degree[e.src]++
	}

	offsets = make([]uint64, n+1)
	for i := 0; i < n; i++ {
		offsets[i+1] = offsets[i] + uint64(degree[i])
	}

	targets = make([]NodeID, len(dEdges))
	kinds = make([]KindID, len(dEdges))
	edgeIDs = make([]uint64, len(dEdges))

	cursor := make([]uint64, n)
	copy(cursor, offsets[:n])
	for _, e := range dEdges {
		c := cursor[e.src]
		targets[c] = e.dst
		kinds[c] = e.kind
		edgeIDs[c] = e.id
		cursor[e.src] = c + 1
	}

	for i := 0; i < n; i++ {
		lo, hi := offsets[i], offsets[i+1]
		sortForwardSegment(targets[lo:hi], kinds[lo:hi], edgeIDs[lo:hi])
	}

	return offsets, targets, kinds, edgeIDs
}

// packReverse counting-sorts the already-packed forward CSR arrays into
// reverse CSR form keyed by target node, then sorts each node's segment by
// (source, kind). It needs no separate source array: it walks the forward
// arrays one source segment at a time (outOffsets), so the source node for
// forward slot j is simply whichever i's segment j falls in. Each reverse
// slot carries the originating forward index (edgeIdx / InEdgeIdx) rather
// than a copy of the edge id or kind, so OutEdgeIDs stays the single source
// of truth for ids and a reverse-to-forward lookup is one indirection away
// (see Snapshot's InEdgeIdx doc).
func packReverse(n int, outOffsets []uint64, outTargets []NodeID, outKinds []KindID) (offsets []uint64, sources []NodeID, kinds []KindID, edgeIdx []uint32) {
	e := len(outTargets)

	degree := make([]uint32, n)
	for _, dst := range outTargets {
		degree[dst]++
	}

	offsets = make([]uint64, n+1)
	for i := 0; i < n; i++ {
		offsets[i+1] = offsets[i] + uint64(degree[i])
	}

	sources = make([]NodeID, e)
	kinds = make([]KindID, e)
	edgeIdx = make([]uint32, e)

	cursor := make([]uint64, n)
	copy(cursor, offsets[:n])
	for i := 0; i < n; i++ {
		lo, hi := outOffsets[i], outOffsets[i+1]
		for j := lo; j < hi; j++ {
			dst := outTargets[j]
			c := cursor[dst]
			sources[c] = NodeID(i)
			kinds[c] = outKinds[j]
			edgeIdx[c] = uint32(j)
			cursor[dst] = c + 1
		}
	}

	for i := 0; i < n; i++ {
		lo, hi := offsets[i], offsets[i+1]
		sortReverseSegment(sources[lo:hi], kinds[lo:hi], edgeIdx[lo:hi])
	}

	return offsets, sources, kinds, edgeIdx
}

// forwardSegment is a tiny sort.Interface over one source node's forward
// adjacency segment: parallel (target, kind, edge id) slices, ordered by
// (target, kind). The edge id is satellite data carried along for Swap; it
// never participates in Less.
type forwardSegment struct {
	targets []NodeID
	kinds   []KindID
	edgeIDs []uint64
}

func (s forwardSegment) Len() int { return len(s.targets) }

func (s forwardSegment) Less(i, j int) bool {
	if s.targets[i] != s.targets[j] {
		return s.targets[i] < s.targets[j]
	}
	return s.kinds[i] < s.kinds[j]
}

func (s forwardSegment) Swap(i, j int) {
	s.targets[i], s.targets[j] = s.targets[j], s.targets[i]
	s.kinds[i], s.kinds[j] = s.kinds[j], s.kinds[i]
	s.edgeIDs[i], s.edgeIDs[j] = s.edgeIDs[j], s.edgeIDs[i]
}

func sortForwardSegment(targets []NodeID, kinds []KindID, edgeIDs []uint64) {
	sort.Sort(forwardSegment{targets: targets, kinds: kinds, edgeIDs: edgeIDs})
}

// reverseSegment is forwardSegment's mirror for one target node's reverse
// adjacency segment: parallel (source, kind, forward index) slices, ordered
// by (source, kind). The forward index is satellite data carried along for
// Swap; it never participates in Less.
type reverseSegment struct {
	sources []NodeID
	kinds   []KindID
	fwdIdx  []uint32
}

func (s reverseSegment) Len() int { return len(s.sources) }

func (s reverseSegment) Less(i, j int) bool {
	if s.sources[i] != s.sources[j] {
		return s.sources[i] < s.sources[j]
	}
	return s.kinds[i] < s.kinds[j]
}

func (s reverseSegment) Swap(i, j int) {
	s.sources[i], s.sources[j] = s.sources[j], s.sources[i]
	s.kinds[i], s.kinds[j] = s.kinds[j], s.kinds[i]
	s.fwdIdx[i], s.fwdIdx[j] = s.fwdIdx[j], s.fwdIdx[i]
}

func sortReverseSegment(sources []NodeID, kinds []KindID, fwdIdx []uint32) {
	sort.Sort(reverseSegment{sources: sources, kinds: kinds, fwdIdx: fwdIdx})
}

// edgeIDPermSort is a tiny sort.Interface over indices 0..len(ids)-1,
// ordered by the database edge id each index names in ids (OutEdgeIDs).
// Sorting the indices rather than the ids directly is what lets
// buildEdgeIDPerm hand back a permutation into forward-CSR slots instead of
// a bare sorted copy of the ids themselves.
type edgeIDPermSort struct {
	perm []uint32
	ids  []uint64
}

func (s edgeIDPermSort) Len() int { return len(s.perm) }

func (s edgeIDPermSort) Less(i, j int) bool {
	return s.ids[s.perm[i]] < s.ids[s.perm[j]]
}

func (s edgeIDPermSort) Swap(i, j int) {
	s.perm[i], s.perm[j] = s.perm[j], s.perm[i]
}

// buildEdgeIDPerm returns forward-CSR indices 0..len(ids)-1 permuted into
// ascending database-edge-id order, for Snapshot.EdgeByID's binary search.
// Unlike the per-node segment sorts in packForward/packReverse above, this
// is a single comparison sort over every edge in the snapshot (ids are not
// grouped by any CSR bucket to sort within), so it costs O(E log E) rather
// than O(N + E) -- the price of an id-keyed lookup path alongside the
// CSR's node-keyed one.
func buildEdgeIDPerm(ids []uint64) []uint32 {
	perm := make([]uint32, len(ids))
	for i := range perm {
		perm[i] = uint32(i)
	}
	sort.Sort(edgeIDPermSort{perm: perm, ids: ids})
	return perm
}
