// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
)

// valueIndex is one property's EXACT-match postings: the nodes carrying each
// distinct value, and -- when the value is a list -- the nodes carrying each
// distinct element.
//
// This is the index PostgreSQL does not have. BloodHound's schema indexes
// `node` only by kind_ids and id; `properties` carries no index at all, not
// GIN, not trigram. So a predicate like
//
//	MATCH (u:Base) WHERE 'RC4-HMAC-MD5' IN u.supportedencryptiontypes
//
// is a sequential scan for pg over every Base node in the graph, and it was a
// scan here too -- the string index covers ordered STRING comparisons, and
// neither a list membership nor a boolean equality is one. Indexing them
// turns the scan into a lookup, which is how this engine gets to be faster
// rather than merely equal on the shapes pg has no answer for.
//
// Only exact matching is offered, deliberately. Ordered comparisons on
// strings already have stringIndex, and an ordered index over mixed-type
// property values would have to reproduce PostgreSQL's jsonb type ordering
// to be useful for anything but equality.
type valueIndex struct {
	prop PropID

	// scalar maps a value's canonical key to the nodes carrying exactly that
	// value; element does the same for each member of a list value. A node
	// with a list value appears in element once per distinct member and not
	// in scalar at all.
	scalar  map[string][]NodeID
	element map[string][]NodeID
}

// valueKey encodes v canonically, matching the equality this package's
// callers actually apply (interpret's jsonbEqual: numbers compare as float64,
// a type mismatch is a definite false, JSON null equals only JSON null).
// ok is false for a composite value, which is not exact-matchable here.
func valueKey(v any) (string, bool) {
	switch val := v.(type) {
	case nil:
		return "z", true
	case bool:
		if val {
			return "b1", true
		}
		return "b0", true
	case float64:
		if val == 0 {
			// -0 and 0 are equal under ==, so they must key identically.
			val = 0
		}
		return "f" + strconv.FormatUint(math.Float64bits(val), 16), true
	case string:
		return "s" + val, true
	default:
		return "", false
	}
}

// valueIndexFor builds prop's postings once and memoizes them. nil means the
// property is not interned in this snapshot at all.
func (s *Snapshot) valueIndexFor(prop PropID) *valueIndex {
	s.valueIdxMu.Lock()
	defer s.valueIdxMu.Unlock()
	if idx, ok := s.valueIdx[prop]; ok {
		return idx
	}
	if int(prop) >= len(s.Props.names) {
		return nil
	}

	idx := &valueIndex{
		prop:    prop,
		scalar:  make(map[string][]NodeID),
		element: make(map[string][]NodeID),
	}

	// Built straight off the stored entries rather than through
	// PropStore.Value, which boxes every value into an `any` and -- for an
	// ARRAY -- runs encoding/json over its raw text. One index over a
	// million-node graph would be a million allocations and, for a list
	// property, a million JSON parses; measured, that was most of a
	// quarter-second landing on whichever query happened to touch the
	// property first.
	n := s.NodeCount()
	var elems []string
	// Element keys repeat heavily -- a list property's vocabulary is a
	// handful of values shared by every node that carries it -- so they are
	// interned rather than rebuilt per node. Looking up with a []byte key is
	// allocation-free in Go; only a genuinely new value allocates.
	interned := make(map[string]string)
	for i := 0; i < n; i++ {
		lo, hi := s.Props.nodeOffsets[i], s.Props.nodeOffsets[i+1]
		entries := s.Props.entries[lo:hi]
		j := sort.Search(len(entries), func(j int) bool { return entries[j].prop >= prop })
		if j >= len(entries) || entries[j].prop != prop {
			continue
		}
		e := entries[j]
		if e.kind == propKindArray {
			elems = s.Props.arrayElementKeys(elems[:0], e, interned)
			// A node appears once per DISTINCT element: `'x' IN n.prop` is a
			// membership test, so repeats in the stored list would otherwise
			// make the same node a repeated candidate, and a candidate
			// source must be a set.
			for k, key := range elems {
				dup := false
				for _, earlier := range elems[:k] {
					if earlier == key {
						dup = true
						break
					}
				}
				if !dup {
					idx.element[key] = append(idx.element[key], NodeID(i))
				}
			}
			continue
		}
		if key, ok := s.Props.entryKey(e); ok {
			idx.scalar[key] = append(idx.scalar[key], NodeID(i))
		}
	}

	if s.valueIdx == nil {
		s.valueIdx = make(map[PropID]*valueIndex)
	}
	s.valueIdx[prop] = idx
	return idx
}

// NodesWithValueByName returns the nodes whose property `name` is exactly
// val, and whether the question could be answered.
//
// Resolved by NAME, never by a PropID the caller looked up itself: a PropID
// miss means only that the BASE snapshot never interned the property, which
// says nothing about an overlay's delta -- see NodesWithStringByName, which
// documents what reading that miss as "nothing carries it" cost.
//
// The result is a SUPERSET on an overlay, and callers re-verify every
// candidate, exactly as they do for every other candidate source here.
func (v *View) NodesWithValueByName(name string, val any) ([]NodeID, bool) {
	return v.valuePostings(name, val, false)
}

// NodesWithArrayElementByName returns the nodes whose property `name` is a
// list CONTAINING val -- Cypher's `val IN n.name`.
func (v *View) NodesWithArrayElementByName(name string, val any) ([]NodeID, bool) {
	return v.valuePostings(name, val, true)
}

func (v *View) valuePostings(name string, val any, element bool) ([]NodeID, bool) {
	key, keyable := valueKey(val)
	if !keyable {
		return nil, false
	}

	prop, interned := v.PropIDByName(name)
	if !interned {
		// The base never saw this property. With no delta there is nothing
		// carrying it; with one, every delta-touched node is a candidate.
		if !v.Overlay() {
			return nil, true
		}
		return v.deltaTouchedNodes(), true
	}

	idx := v.base.valueIndexFor(prop)
	if idx == nil {
		return nil, false
	}
	postings := idx.scalar
	if element {
		postings = idx.element
	}

	if !v.Overlay() {
		return postings[key], true
	}
	return unionDeltaTouched(postings[key], v.deltaTouchedNodes()), true
}

// ValuePostingCount is valuePostings' SIZE without materializing it -- what
// the planner asks first, so it can refuse to build a posting list that is
// larger than the candidate source it would replace.
//
// That gate is not an optimization but a requirement: `n.enabled = true`
// matches most of the graph, and copying and delta-unioning a million-id
// posting list per query to then not use it would cost more than the scan it
// was meant to avoid.
func (v *View) ValuePostingCount(name string, val any, element bool) (int, bool) {
	key, keyable := valueKey(val)
	if !keyable {
		return 0, false
	}
	prop, interned := v.PropIDByName(name)
	if !interned {
		if !v.Overlay() {
			return 0, true
		}
		return len(v.deltaTouchedNodes()), true
	}
	idx := v.base.valueIndexFor(prop)
	if idx == nil {
		return 0, false
	}
	postings := idx.scalar
	if element {
		postings = idx.element
	}
	n := len(postings[key])
	if v.Overlay() {
		n += len(v.deltaTouchedNodes())
	}
	return n, true
}

// ValuePopulation reports how many nodes carry prop with an exact-matchable
// value at all -- the size of the source a caller would otherwise scan, used
// to decide whether indexing is worth preferring.
func (v *View) ValuePopulation(name string) (int, bool) {
	prop, interned := v.PropIDByName(name)
	if !interned {
		return 0, false
	}
	idx := v.base.valueIndexFor(prop)
	if idx == nil {
		return 0, false
	}
	total := 0
	for _, ids := range idx.scalar {
		total += len(ids)
	}
	for _, ids := range idx.element {
		total += len(ids)
	}
	if v.Overlay() {
		total += len(v.deltaTouchedNodes())
	}
	return total, true
}

// entryKey renders a non-array entry's canonical key without boxing it.
func (p *PropStore) entryKey(e propEntry) (string, bool) {
	switch e.kind {
	case propKindNull:
		return "z", true
	case propKindFalse:
		return "b0", true
	case propKindTrue:
		return "b1", true
	case propKindNumber:
		return numberKey(e.num), true
	case propKindString:
		return "s" + p.stringAt(e.ref, e.len), true
	default:
		return "", false
	}
}

func numberKey(f float64) string {
	if f == 0 {
		f = 0 // -0 and 0 are equal under ==, so they must key identically
	}
	return "f" + strconv.FormatUint(math.Float64bits(f), 16)
}

// arrayElementKeys appends the canonical key of each element of an array
// entry to dst.
//
// A flat array of plain scalars -- which is what BloodHound's list properties
// are -- is read straight out of the arena text. Anything else (an escape
// sequence, a nested array or object) falls back to encoding/json, so the
// fast scanner never has to be right about the hard cases.
func (p *PropStore) arrayElementKeys(dst []string, e propEntry, interned map[string]string) []string {
	raw := p.arena[e.ref : e.ref+e.len]
	if keys, ok := scanFlatArrayKeys(dst, raw, interned); ok {
		return keys
	}
	var v []any
	if err := json.Unmarshal(raw, &v); err != nil {
		return dst
	}
	for _, el := range v {
		if key, ok := valueKey(el); ok {
			dst = append(dst, key)
		}
	}
	return dst
}

// scanFlatArrayKeys reads a JSON array of plain scalars, reporting false for
// anything it is not certain of so the caller can fall back.
func scanFlatArrayKeys(dst []string, raw []byte, interned map[string]string) ([]string, bool) {
	intern := func(prefix string, b []byte) string {
		if interned == nil {
			return prefix + string(b)
		}
		if got, ok := interned[string(b)]; ok {
			return got
		}
		key := prefix + string(b)
		interned[string(b)] = key
		return key
	}
	i := 0
	skipSpace := func() {
		for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\n' || raw[i] == '\r') {
			i++
		}
	}
	skipSpace()
	if i >= len(raw) || raw[i] != '[' {
		return dst, false
	}
	i++
	for {
		skipSpace()
		if i >= len(raw) {
			return dst, false
		}
		if raw[i] == ']' {
			return dst, true
		}
		switch raw[i] {
		case '"':
			i++
			start := i
			for i < len(raw) && raw[i] != '"' {
				if raw[i] == '\\' {
					return dst, false // an escape: let encoding/json decode it
				}
				i++
			}
			if i >= len(raw) {
				return dst, false
			}
			dst = append(dst, intern("s", raw[start:i]))
			i++
		case '[', '{':
			return dst, false // nested: not a flat array
		case 't', 'f', 'n':
			start := i
			for i < len(raw) && raw[i] >= 'a' && raw[i] <= 'z' {
				i++
			}
			switch string(raw[start:i]) {
			case "true":
				dst = append(dst, "b1")
			case "false":
				dst = append(dst, "b0")
			case "null":
				dst = append(dst, "z")
			default:
				return dst, false
			}
		default:
			start := i
			for i < len(raw) && raw[i] != ',' && raw[i] != ']' && raw[i] != ' ' {
				i++
			}
			f, err := strconv.ParseFloat(string(raw[start:i]), 64)
			if err != nil {
				return dst, false
			}
			dst = append(dst, numberKey(f))
		}
		skipSpace()
		if i < len(raw) && raw[i] == ',' {
			i++
			continue
		}
		if i < len(raw) && raw[i] == ']' {
			return dst, true
		}
		return dst, false
	}
}
