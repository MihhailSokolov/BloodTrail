// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"sort"
	"strings"
)

// deltaPropPostings is one property's delta-side answer: which nodes any
// segment in this View's stack wrote a value of `name` for, and what those
// values are.
//
// It replaced a blanket union as the overlay half of every property lookup.
// That union returned every node ANY segment wrote a record for, whatever it
// wrote -- so asking "which nodes have objectid
// ending in -512" against a view carrying 260,000 written nodes returned
// those 260,000 as candidates, and the caller verified every one. Measured on
// the benchmark graph, the shipped "All Global Administrators" prebuilt cost
// 2.7ms of execution against a freshly compacted base and 18-21ms against the
// same base with an overlay, purely from that union; the query's own answer is
// a single row either way.
//
// And an overlay is the normal state of a live BloodHound, not an edge case:
// ingest and the post-processing analysis write continuously, and the
// segments sit there until a compaction folds them into a new base.
//
// Built per NAME rather than for every property at once, and memoized on the
// View. A query asks about a handful of properties, a BloodHound node carries
// dozens, and a View is immutable once published -- the same bargain
// Snapshot.valueIndexFor makes for the base.
type deltaPropPostings struct {
	// strings holds the delta nodes carrying this property as a string,
	// with the value, so an ordered or substring match scans only them.
	strings []deltaStringEntry

	// values maps a scalar's canonical key (valueKey) to the delta nodes
	// holding exactly it, and elements does the same for list membership.
	values   map[string][]NodeID
	elements map[string][]NodeID

	// n is how many distinct delta nodes carry the property at all.
	n int
}

type deltaStringEntry struct {
	id NodeID
	s  string
}

// deltaPropFor builds and memoizes name's delta postings. nil when this View
// has no overlay at all.
func (v *View) deltaPropFor(name string) *deltaPropPostings {
	if !v.Overlay() {
		return nil
	}
	v.deltaPropMu.Lock()
	defer v.deltaPropMu.Unlock()
	if got, ok := v.deltaProp[name]; ok {
		return got
	}

	v.ensureDelta()
	idx := &deltaPropPostings{
		values:   make(map[string][]NodeID),
		elements: make(map[string][]NodeID),
	}
	if v.merged != nil {
		v.merged.IterNodes(func(id uint64, st NodeSegState) bool {
			if st.Tombstoned {
				// The node carries no bag at all. Whatever the BASE index says
				// about it still stands as a candidate and is re-verified
				// there, so nothing is lost by skipping it here.
				return true
			}
			val, ok := st.PropValueByName(name)
			if !ok {
				return true
			}
			dense, ok := v.denseOf(id)
			if !ok {
				return true
			}
			idx.n++
			if s, isStr := val.(string); isStr {
				idx.strings = append(idx.strings, deltaStringEntry{id: dense, s: s})
			}
			if list, isList := val.([]any); isList {
				seen := make(map[string]struct{}, len(list))
				for _, el := range list {
					key, keyable := valueKey(el)
					if !keyable {
						continue
					}
					if _, dup := seen[key]; dup {
						continue
					}
					seen[key] = struct{}{}
					idx.elements[key] = append(idx.elements[key], dense)
				}
				return true
			}
			if key, keyable := valueKey(val); keyable {
				idx.values[key] = append(idx.values[key], dense)
			}
			return true
		})
	}
	sort.Slice(idx.strings, func(a, b int) bool { return idx.strings[a].id < idx.strings[b].id })
	for k, ids := range idx.values {
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		idx.values[k] = dedupeSorted(ids)
	}
	for k, ids := range idx.elements {
		sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
		idx.elements[k] = dedupeSorted(ids)
	}

	if v.deltaProp == nil {
		v.deltaProp = make(map[string]*deltaPropPostings)
	}
	v.deltaProp[name] = idx
	return idx
}

// denseOf resolves a segment's database id to this View's dense id, whether
// the node is base-resident or was added by the delta.
func (v *View) denseOf(dbID uint64) (NodeID, bool) {
	if id, ok := v.base.Dense(dbID); ok {
		return id, true
	}
	id, ok := v.pgToVirtual[dbID]
	return id, ok
}

// matchingStrings returns the delta nodes whose value for this property
// satisfies match/operand, in ascending id order.
func (d *deltaPropPostings) matchingStrings(match StringMatch, operand string) []NodeID {
	if d == nil {
		return nil
	}
	var out []NodeID
	for _, e := range d.strings {
		if matchString(e.s, match, operand) {
			out = append(out, e.id)
		}
	}
	return out
}

// matchString applies one StringMatch, mirroring exactly what the base index
// answers for the same predicate.
func matchString(s string, match StringMatch, operand string) bool {
	switch match {
	case StringEquals:
		return s == operand
	case StringPrefix:
		return strings.HasPrefix(s, operand)
	case StringSuffix:
		return strings.HasSuffix(s, operand)
	case StringContains:
		return strings.Contains(s, operand)
	default:
		return false
	}
}

// count is how many delta nodes carry the property at all -- what a caller
// sizing a candidate source needs, without materializing the postings.
func (d *deltaPropPostings) count() int {
	if d == nil {
		return 0
	}
	return d.n
}

// exact returns the delta nodes whose value for this property is exactly the
// one key encodes -- or, when element is set, whose list value CONTAINS it.
func (d *deltaPropPostings) exact(key string, element bool) []NodeID {
	if d == nil {
		return nil
	}
	if element {
		return d.elements[key]
	}
	return d.values[key]
}

// hasNonString reports whether any delta node carries this property as
// something other than a string -- a number, boolean, null or list. See
// NodesMatchingString for why that makes a string-predicate index unusable
// rather than merely incomplete.
func (d *deltaPropPostings) hasNonString() bool {
	if d == nil {
		return false
	}
	if len(d.elements) > 0 {
		return true
	}
	for key := range d.values {
		if len(key) == 0 || key[0] != 's' {
			return true
		}
	}
	return false
}
