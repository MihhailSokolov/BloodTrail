// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

// edgeKindIndex answers "which nodes have at least one edge of kind k, in
// this direction" -- the access path PostgreSQL has and this engine did not.
//
// BloodHound's own schema indexes `edge` by kind_id alone
// (edge_kind_id_id_start_id_end_id_index), so PostgreSQL answers a query
// anchored on a rare edge kind with a single btree probe. This engine's
// candidate sources knew about node kinds, ids, objectids and property
// values, but nothing about edge kinds, so the shipped "All Global
// Administrators" prebuilt --
//
//	MATCH p = (:AZBase)-[:AZGlobalAdmin*1..]->(:AZTenant) RETURN p LIMIT 1000
//
// seeded its walk from all 84,481 AZBase nodes to find the ONE AZGlobalAdmin
// edge in the graph, and measured 2.7x slower than the database it replaces.
// A node with no admissible outgoing edge cannot start a match, and this is
// what lets the executor skip it without looking.
//
// Endpoints are stored as sorted, deduplicated dense ids rather than as
// bitmaps. The kinds that matter most here are the sparse ones, where a
// bitmap would cost NodeCount()/8 bytes to represent a handful of nodes; a
// slice costs four bytes per node that actually has such an edge, iterates
// in cache order, and is directly usable as a candidate source.
type edgeKindIndex struct {
	out map[KindID][]NodeID
	in  map[KindID][]NodeID

	// unions memoizes the endpoint set of a kind ALTERNATION, which is what
	// the corpus actually asks for: `[:MemberOf|AdminTo*1..3]` names two
	// kinds and the shipped shortest-path prebuilts name upwards of sixty.
	// Recomputing that union per call meant a map insert and a sort over
	// every endpoint of every named kind, on a hot path the planner and the
	// router both hit several times per query -- hundreds of thousands of
	// nodes' worth of work to answer a question whose answer never changes
	// for an immutable snapshot.
	unionMu sync.Mutex
	unions  map[string][]NodeID
}

// unionKey identifies one alternation-and-direction for the union memo. Kind
// ids are sorted so two spellings of the same alternation share an entry.
func unionKey(kinds []KindID, outgoing bool) string {
	sorted := append([]KindID(nil), kinds...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
	var b strings.Builder
	if outgoing {
		b.WriteByte('o')
	} else {
		b.WriteByte('i')
	}
	for _, k := range sorted {
		b.WriteByte(':')
		b.WriteString(strconv.Itoa(int(k)))
	}
	return b.String()
}

// ensureEdgeKindIndex builds the whole index on first use and memoizes it.
//
// One pass over the forward CSR populates EVERY kind at once, deliberately:
// building per-kind on demand would cost a full CSR pass each time, and the
// queries that need this most are the shipped shortest-path prebuilts whose
// relationship lists name upwards of sixty kinds -- sixty passes where one
// does. Building it eagerly at snapshot construction was the other option and
// is what the string index already rejects for its own reasons: this costs
// one pass over the edges either way, so it is paid once on the first query
// that asks rather than on every snapshot rebuild whether or not anything
// asks.
func (s *Snapshot) ensureEdgeKindIndex() *edgeKindIndex {
	s.edgeKindIdxMu.Lock()
	defer s.edgeKindIdxMu.Unlock()
	if s.edgeKindIdx != nil {
		return s.edgeKindIdx
	}

	idx := &edgeKindIndex{
		out: make(map[KindID][]NodeID),
		in:  make(map[KindID][]NodeID),
	}
	n := s.NodeCount()
	for node := 0; node < n; node++ {
		lo, hi := s.OutOffsets[node], s.OutOffsets[node+1]
		for i := lo; i < hi; i++ {
			k := s.OutKinds[i]
			idx.out[k] = appendDistinctTail(idx.out[k], NodeID(node))
			idx.in[k] = append(idx.in[k], s.OutTargets[i])
		}
	}
	// `out` is already ascending and deduplicated by construction (the outer
	// loop walks nodes in order); `in` is populated in target order, so it
	// needs a sort and a dedupe pass of its own.
	for k, ids := range idx.in {
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		idx.in[k] = dedupeSorted(ids)
	}

	s.edgeKindIdx = idx
	return idx
}

// appendDistinctTail appends id unless it already sits at the tail. The
// caller walks nodes in ascending order and appends the same node once per
// outgoing edge, so a tail check is all the deduplication that shape needs.
func appendDistinctTail(ids []NodeID, id NodeID) []NodeID {
	if len(ids) > 0 && ids[len(ids)-1] == id {
		return ids
	}
	return append(ids, id)
}

func dedupeSorted(ids []NodeID) []NodeID {
	if len(ids) < 2 {
		return ids
	}
	out := ids[:1]
	for _, id := range ids[1:] {
		if id != out[len(out)-1] {
			out = append(out, id)
		}
	}
	return out
}

// EdgeKindEndpoints returns the dense ids of every node carrying at least one
// edge whose kind is among kinds, in the direction outgoing selects, and
// whether the question could be answered at all.
//
// The returned slice is READ-ONLY: for a single kind it aliases the memoized
// index rather than copying it, because every caller is a candidate source
// that only iterates.
//
// ok is false for an EMPTY kinds list -- the "any relationship type" pattern,
// which every node with any edge satisfies and which therefore narrows
// nothing worth the intersection.
//
// The result is a SUPERSET on an overlay: a delta segment can add an edge of
// a kind the base snapshot never carried, so every endpoint of every
// segment-written edge is unioned in without asking whether the base already
// had it. A tombstoned edge is deliberately NOT removed -- a candidate source
// is allowed to be loose and is never allowed to be short, and the caller
// re-verifies every candidate anyway.
func (v *View) EdgeKindEndpoints(kinds []KindID, outgoing bool) ([]NodeID, bool) {
	if len(kinds) == 0 {
		return nil, false
	}
	idx := v.base.ensureEdgeKindIndex()
	side := idx.out
	if !outgoing {
		side = idx.in
	}

	var merged []NodeID
	if len(kinds) == 1 {
		merged = side[kinds[0]]
	} else {
		merged = idx.unionFor(side, kinds, outgoing)
	}

	if !v.Overlay() {
		return merged, true
	}
	return unionDeltaTouched(merged, v.deltaEdgeEndpointsFor(kinds, outgoing)), true
}

// unionFor returns the memoized endpoint union for a kind alternation,
// computing it at most once per alternation per direction.
func (idx *edgeKindIndex) unionFor(side map[KindID][]NodeID, kinds []KindID, outgoing bool) []NodeID {
	key := unionKey(kinds, outgoing)

	idx.unionMu.Lock()
	defer idx.unionMu.Unlock()
	if idx.unions == nil {
		idx.unions = make(map[string][]NodeID)
	}
	if got, ok := idx.unions[key]; ok {
		return got
	}

	seen := make(map[NodeID]struct{})
	var merged []NodeID
	for _, k := range kinds {
		for _, id := range side[k] {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				merged = append(merged, id)
			}
		}
	}
	sort.Slice(merged, func(a, b int) bool { return merged[a] < merged[b] })
	idx.unions[key] = merged
	return merged
}

// EdgeKindEndpointCount is EdgeKindEndpoints' size without materializing the
// union -- what the cost model asks for, once per candidate symbol per query.
func (v *View) EdgeKindEndpointCount(kinds []KindID, outgoing bool) (int, bool) {
	ids, ok := v.EdgeKindEndpoints(kinds, outgoing)
	if !ok {
		return 0, false
	}
	return len(ids), true
}

// deltaEdgeEndpointsFor returns the endpoints, on the requested side, of every
// delta edge of an admissible kind -- the overlay half of EdgeKindEndpoints'
// superset contract.
//
// Served from a per-View index built once, NOT by walking the merged delta
// per call. The walk is O(every edge any segment wrote), and this is asked
// several times per query by the planner and the router; on a live instance
// carrying a few hundred thousand segment entries that turned a query the
// base index answers from one seed into tens of milliseconds of set-building.
// A View is immutable once published, so one pass serves every query against
// it -- the same bargain ensureEdgeKindIndex makes for the base snapshot.
func (v *View) deltaEdgeEndpointsFor(kinds []KindID, outgoing bool) []NodeID {
	idx := v.ensureDeltaEdgeKindIndex()
	if idx == nil {
		return nil
	}
	side := idx.out
	if !outgoing {
		side = idx.in
	}
	if len(kinds) == 1 {
		return side[kinds[0]]
	}
	var merged []NodeID
	seen := make(map[NodeID]struct{})
	for _, k := range kinds {
		for _, id := range side[k] {
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				merged = append(merged, id)
			}
		}
	}
	return merged
}

// ensureDeltaEdgeKindIndex builds this View's delta-side edge-kind endpoint
// index once and memoizes it. nil when there is no delta at all.
func (v *View) ensureDeltaEdgeKindIndex() *edgeKindIndex {
	v.deltaEdgeIdxOnce.Do(func() {
		v.ensureDelta()
		if v.merged == nil {
			return
		}
		idx := &edgeKindIndex{out: make(map[KindID][]NodeID), in: make(map[KindID][]NodeID)}
		dense := func(dbID uint64) (NodeID, bool) {
			if id, ok := v.base.Dense(dbID); ok {
				return id, true
			}
			id, ok := v.pgToVirtual[dbID]
			return id, ok
		}
		v.merged.IterEdges(func(_ uint64, st EdgeSegState) bool {
			if id, ok := dense(st.StartID); ok {
				idx.out[st.Kind] = append(idx.out[st.Kind], id)
			}
			if id, ok := dense(st.EndID); ok {
				idx.in[st.Kind] = append(idx.in[st.Kind], id)
			}
			return true
		})
		for k, ids := range idx.out {
			sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
			idx.out[k] = dedupeSorted(ids)
		}
		for k, ids := range idx.in {
			sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
			idx.in[k] = dedupeSorted(ids)
		}
		v.deltaEdgeIdx = idx
	})
	return v.deltaEdgeIdx
}
