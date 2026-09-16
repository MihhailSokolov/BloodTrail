// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"sort"
	"strings"
	"sync"
)

// StringMatch is the shape of a string-property predicate a caller wants
// candidate nodes for.
type StringMatch uint8

const (
	// StringEquals matches a property value byte-identical to the operand.
	StringEquals StringMatch = iota
	// StringPrefix matches values starting with the operand.
	StringPrefix
	// StringSuffix matches values ending with the operand.
	StringSuffix
	// StringContains matches values containing the operand anywhere.
	StringContains
)

// stringIndex is one property's value index over the base snapshot: dense
// node ids ordered by that property's string value. `fwd` is ascending by
// value, which answers equality and prefix ranges by binary search; `rev`
// is ascending by the value read BACKWARD, which turns a suffix into a
// prefix and answers ENDS WITH the same way. Each direction is built and
// memoized independently, because a query needs exactly one of them and
// sorting is the whole cost.
//
// Both slices hold only nodes that actually carry the property with a
// STRING value. That sparsity is most of the win on real BloodHound data:
// hygiene properties like `system_tags` or `serviceprincipalnames` exist on
// a handful of nodes out of a million, so even the linear match shapes
// (StringContains, which no ordering can answer) walk a handful of entries
// instead of every node in the graph.
type stringIndex struct {
	prop PropID

	fwdOnce sync.Once
	fwd     []NodeID

	revOnce sync.Once
	rev     []NodeID

	// present is every node carrying prop as a string, in ascending node
	// order -- the unsorted population both directions sort, and the answer
	// for StringContains directly.
	present []NodeID
}

// stringIndexFor returns s's index for prop, building the population once.
// nil means the property is not interned in this snapshot at all.
func (s *Snapshot) stringIndexFor(prop PropID) *stringIndex {
	s.stringIdxMu.Lock()
	defer s.stringIdxMu.Unlock()
	if idx, ok := s.stringIdx[prop]; ok {
		return idx
	}
	if int(prop) >= len(s.Props.names) {
		return nil
	}
	idx := &stringIndex{prop: prop}
	n := s.NodeCount()
	for i := 0; i < n; i++ {
		if _, ok := s.Props.stringValue(NodeID(i), prop); ok {
			idx.present = append(idx.present, NodeID(i))
		}
	}
	if s.stringIdx == nil {
		s.stringIdx = make(map[PropID]*stringIndex)
	}
	s.stringIdx[prop] = idx
	return idx
}

// stringValue returns node n's value for prop when it is present AND
// string-valued, without boxing it into an `any` (PropStore.Value's decode
// allocates an interface word per call, which is exactly the per-node cost
// an index exists to avoid). The returned string aliases the shared arena
// and is valid for the lifetime of the immutable PropStore.
func (p *PropStore) stringValue(n NodeID, prop PropID) (string, bool) {
	lo, hi := p.nodeOffsets[n], p.nodeOffsets[n+1]
	entries := p.entries[lo:hi]
	i := sort.Search(len(entries), func(i int) bool { return entries[i].prop >= prop })
	if i >= len(entries) || entries[i].prop != prop || entries[i].kind != propKindString {
		return "", false
	}
	return p.stringAt(entries[i].ref, entries[i].len), true
}

func (idx *stringIndex) forward(p *PropStore) []NodeID {
	idx.fwdOnce.Do(func() {
		idx.fwd = append([]NodeID(nil), idx.present...)
		sort.Slice(idx.fwd, func(a, b int) bool {
			va, _ := p.stringValue(idx.fwd[a], idx.prop)
			vb, _ := p.stringValue(idx.fwd[b], idx.prop)
			return va < vb
		})
	})
	return idx.fwd
}

func (idx *stringIndex) reverse(p *PropStore) []NodeID {
	idx.revOnce.Do(func() {
		idx.rev = append([]NodeID(nil), idx.present...)
		sort.Slice(idx.rev, func(a, b int) bool {
			va, _ := p.stringValue(idx.rev[a], idx.prop)
			vb, _ := p.stringValue(idx.rev[b], idx.prop)
			return lessReversed(va, vb)
		})
	})
	return idx.rev
}

// lessReversed orders a before b by their bytes read back to front, without
// materializing either reversal -- the ordering under which a shared SUFFIX
// becomes a shared prefix.
func lessReversed(a, b string) bool {
	i, j := len(a)-1, len(b)-1
	for i >= 0 && j >= 0 {
		if a[i] != b[j] {
			return a[i] < b[j]
		}
		i--
		j--
	}
	return len(a) < len(b)
}

// hasReversedPrefix reports whether v, read backward, starts with operand
// read backward -- i.e. whether v ends with operand. Spelled this way so
// the scan loop below reads as the mirror of the forward one.
func hasReversedPrefix(v, operand string) bool { return strings.HasSuffix(v, operand) }

// NodesWithString returns dense node ids whose property prop may satisfy
// `prop <match> operand`, and whether an indexed answer was available at
// all (false means the caller must fall back to its ordinary candidate
// source).
//
// The returned set is a SUPERSET of the true matches, never a subset, and
// callers are expected to re-verify -- which every caller in this repository
// already does, because a node also has to satisfy its kind labels and the
// query's remaining predicates. Being a superset is what makes the overlay
// case cheap: a delta segment can add a node, or change an indexed node's
// property out from under the base index, so every node any segment touched
// is added to the candidate set unconditionally rather than reasoned about.
//
// Ordering of the result is unspecified.
func (v *View) NodesWithString(prop PropID, match StringMatch, operand string) ([]NodeID, bool) {
	idx := v.base.stringIndexFor(prop)
	if idx == nil {
		return nil, false
	}
	p := v.base.Props

	var out []NodeID
	switch match {
	case StringContains:
		// No ordering answers "contains"; the win here is the population,
		// which holds only nodes carrying the property at all.
		for _, id := range idx.present {
			if s, ok := p.stringValue(id, prop); ok && strings.Contains(s, operand) {
				out = append(out, id)
			}
		}

	case StringSuffix:
		ids := idx.reverse(p)
		lo := sort.Search(len(ids), func(i int) bool {
			s, _ := p.stringValue(ids[i], prop)
			return !lessReversed(s, operand)
		})
		for i := lo; i < len(ids); i++ {
			s, _ := p.stringValue(ids[i], prop)
			if !hasReversedPrefix(s, operand) {
				break
			}
			out = append(out, ids[i])
		}

	default: // StringEquals, StringPrefix
		ids := idx.forward(p)
		lo := sort.Search(len(ids), func(i int) bool {
			s, _ := p.stringValue(ids[i], prop)
			return s >= operand
		})
		for i := lo; i < len(ids); i++ {
			s, _ := p.stringValue(ids[i], prop)
			if !strings.HasPrefix(s, operand) {
				break
			}
			if match == StringEquals && s != operand {
				break
			}
			out = append(out, ids[i])
		}
	}

	if !v.Overlay() {
		return out, true
	}
	return unionDeltaTouched(out, v.deltaTouchedNodes()), true
}

// unionDeltaTouched appends the segment-touched nodes to an indexed result
// WITHOUT repeating any the index already returned. A candidate source must
// never yield the same node twice: scanAnchorVisit admits each element it
// is handed, so a duplicate becomes a duplicate result row -- which is
// exactly what happened before this existed, on a write-through query whose
// modified node also matched the base index (`MATCH (n:User) WHERE
// n.objectid ENDS WITH '-500' RETURN n` returned the Administrator twice).
//
// The delta side is one commit's worth of writes, so it is the side that
// becomes the set; the indexed side is filtered through it and the delta
// ids are appended whole.
func unionDeltaTouched(indexed, touched []NodeID) []NodeID {
	if len(touched) == 0 {
		return indexed
	}
	inDelta := make(map[NodeID]struct{}, len(touched))
	for _, id := range touched {
		inDelta[id] = struct{}{}
	}
	out := make([]NodeID, 0, len(indexed)+len(touched))
	for _, id := range indexed {
		if _, dup := inDelta[id]; !dup {
			out = append(out, id)
		}
	}
	return append(out, touched...)
}

// deltaTouchedNodes returns every dense node id any segment in the stack
// wrote a record for -- upsert or tombstone, base-resident or delta-added.
// NodesWithString appends these to an indexed result wholesale: a segment
// may have given a node the property, changed its value, or removed it, and
// the base index cannot know. They are re-verified by the caller like every
// other candidate, and a segment is one commit's worth of writes, so the
// unconditional union stays cheap.
func (v *View) deltaTouchedNodes() []NodeID {
	v.ensureDelta()
	if v.merged == nil {
		return nil
	}
	var out []NodeID
	v.merged.IterNodes(func(id uint64, _ NodeSegState) bool {
		if dense, ok := v.base.Dense(id); ok {
			out = append(out, dense)
			return true
		}
		if virtual, ok := v.pgToVirtual[id]; ok {
			out = append(out, virtual)
		}
		return true
	})
	return out
}

// PropCount reports how many nodes carry prop with a string value, and
// whether the property is interned at all. Callers use it to decide whether
// an indexed candidate source is worth preferring over a kind bitmap.
func (v *View) PropCount(prop PropID) (int, bool) {
	idx := v.base.stringIndexFor(prop)
	if idx == nil {
		return 0, false
	}
	n := len(idx.present)
	if v.Overlay() {
		n += len(v.deltaTouchedNodes())
	}
	return n, true
}
