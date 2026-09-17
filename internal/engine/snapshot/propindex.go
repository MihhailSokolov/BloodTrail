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
// BASE-ONLY, and therefore NOT usable on its own against an overlay View: a
// delta segment can give a node the property, change the value the base index
// recorded, or add the node outright, and none of that is reflected here.
// NodesWithStringByName is the entry point that adds the delta's own matches,
// and it is the only caller for exactly that reason. Calling this directly on
// an overlay yields a candidate set missing every node the delta introduced,
// which reaches the user as a served SHORT answer rather than an error -- the
// same trap PropIDByName's doc records.
//
// Within the base the returned set is a SUPERSET of the true matches, never a
// subset, and callers re-verify anyway because a node also has to satisfy its
// kind labels and the query's remaining predicates.
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

	return out, true
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

// NodesWithStringByName is NodesWithString keyed by the property NAME a
// query actually writes, and is the entry point every caller holding a name
// must use.
//
// Resolving the name to a PropID first and reading a miss as "nothing
// carries this property" is WRONG on a View, and was: PropIDByName reads the
// BASE snapshot's intern table, while each delta segment interns its own
// property names. An engine that boots against an empty database and
// receives its entire graph through write-through -- every fresh install,
// and the deployment build/e2e.sh exercises -- has a base that interned
// nothing at all, so `objectid` missed, and the empty candidate set that
// followed made `MATCH (t:Group) WHERE t.objectid ENDS WITH '-512'` answer
// zero rows against a graph that plainly contained that group.
//
// A candidate source is allowed to be loose and is never allowed to be
// short, so a miss resolves to every delta-touched node when there IS a
// delta -- the same superset NodesWithString unions in for a property the
// base does know -- and to the empty set only when there is no delta for a
// node to be hiding in, which is the one case where the base's silence
// really does settle the question.
func (v *View) NodesWithStringByName(name string, match StringMatch, operand string) ([]NodeID, bool) {
	prop, interned := v.PropIDByName(name)
	if !interned && !v.Overlay() {
		// The base never saw the property and there is no delta for a node
		// to be hiding in, which is the one case where the base's silence
		// really does settle the question.
		return nil, true
	}

	var base []NodeID
	if interned {
		got, ok := v.NodesWithString(prop, match, operand)
		if !ok {
			return nil, false
		}
		base = got
	}
	if !v.Overlay() {
		return base, true
	}
	// The delta contributes the nodes whose written value actually matches,
	// not every node any segment wrote. Base postings are still returned
	// whole -- a segment may have changed or removed the value behind one, so
	// they stay candidates and the caller re-verifies them, exactly as before.
	return unionDeltaTouched(base, v.deltaPropFor(name).matchingStrings(match, operand)), true
}

// HasStringValue reports whether prop is string-valued on ANY node in this
// view, and whether that question could be answered at all (false when the
// property is not interned in the base snapshot, where an overlay could
// still have introduced it).
//
// This is deliberately NOT PropCount(prop) > 0. PropCount adds every
// segment-touched node to its total -- correct for sizing a scan, since
// those nodes must be re-checked -- but it answers "how many candidates",
// not "is this column ever a string". Conflating the two made ORDER BY on a
// numeric property decline the instant ANY write was pending, which is most
// of the time on a live deployment.
//
// The delta is checked exactly rather than approximated: a segment can give
// a numeric column a string value, and ordering a string column would mean
// reproducing PostgreSQL's collation, which this package cannot.
func (v *View) HasStringValue(prop PropID) (bool, bool) {
	idx := v.base.stringIndexFor(prop)
	if idx == nil {
		return false, false
	}
	if len(idx.present) > 0 {
		return true, true
	}
	if v.Overlay() {
		if d := v.deltaPropFor(v.base.Props.Name(prop)); d != nil && len(d.strings) > 0 {
			return true, true
		}
	}
	return false, true
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
		n += v.deltaPropFor(v.base.Props.Name(prop)).count()
	}
	return n, true
}
