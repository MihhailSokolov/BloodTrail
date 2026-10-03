// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
)

// valueShape records which JSON types one property's values take, for the
// question every TEXT-semantics index lookup has to ask first.
//
// dawgs translates the string predicates -- STARTS WITH, ENDS WITH,
// CONTAINS, =~, `p IN ['a', ...]` -- through `properties ->> 'p'`, and
// `'x' IN n.p` through `jsonb_to_text_array(properties -> 'p')`. Both render
// a non-string JSON value as its TEXT: the number 12345 is '12345', true is
// 'true', a list is its JSON source. So PostgreSQL matches a node whose
// value is not a string at all. The string and element indexes record only
// string values (and typed element keys), so they are complete for a text
// predicate only when the property never takes any other type -- and a
// candidate source must never be short. BloodHound's schema puts no type
// constraint on `properties`, so this cannot be assumed; it has to be asked.
type valueShape struct {
	// nonString: some node carries the property as something other than a
	// string or JSON null (a JSON null extracts to SQL NULL, which no text
	// predicate matches, so it is harmless).
	nonString bool
	// nonStringList: some node carries the property as something other than
	// a list of strings or JSON null -- a scalar (which jsonb_to_text_array
	// rejects with an error PostgreSQL would raise) or a list with any
	// element that is not a string.
	nonStringList bool
}

// valueShapeFor scans prop's stored values once and memoizes the result.
// ok is false when the property is not interned in this snapshot.
func (s *Snapshot) valueShapeFor(prop PropID) (valueShape, bool) {
	s.valueShapeMu.Lock()
	defer s.valueShapeMu.Unlock()
	if got, ok := s.valueShape[prop]; ok {
		return got, true
	}
	if int(prop) >= len(s.Props.names) {
		return valueShape{}, false
	}

	var shape valueShape
	var elems []string
	n := s.NodeCount()
	for i := 0; i < n && (!shape.nonString || !shape.nonStringList); i++ {
		lo, hi := s.Props.nodeOffsets[i], s.Props.nodeOffsets[i+1]
		entries := s.Props.entries[lo:hi]
		j := sort.Search(len(entries), func(j int) bool { return entries[j].prop >= prop })
		if j >= len(entries) || entries[j].prop != prop {
			continue
		}
		e := entries[j]
		switch e.kind {
		case propKindNull:
		case propKindString:
			shape.nonStringList = true
		case propKindArray:
			shape.nonString = true
			if !shape.nonStringList {
				var ok bool
				elems, ok = s.Props.stringOnlyArray(elems[:0], e)
				if !ok {
					shape.nonStringList = true
				}
			}
		default:
			shape.nonString = true
			shape.nonStringList = true
		}
	}

	if s.valueShape == nil {
		s.valueShape = make(map[PropID]valueShape)
	}
	s.valueShape[prop] = shape
	return shape, true
}

// stringOnlyArray reports whether an array entry's elements are ALL strings.
// The fast scanner handles the flat arrays BloodHound writes; anything it is
// unsure of is decoded properly, and anything undecodable counts as not.
func (p *PropStore) stringOnlyArray(scratch []string, e propEntry) ([]string, bool) {
	raw := p.arena[e.ref : e.ref+e.len]
	if keys, ok := scanFlatArrayKeys(scratch, raw, nil); ok {
		for _, k := range keys {
			if len(k) == 0 || k[0] != 's' {
				return keys, false
			}
		}
		return keys, true
	}
	var v []any
	if err := json.Unmarshal(raw, &v); err != nil {
		return scratch, false
	}
	for _, el := range v {
		if _, isStr := el.(string); !isStr {
			return scratch, false
		}
	}
	return scratch, true
}

// StringValuesOnly reports whether every node carrying property `name` --
// base and delta alike -- carries it as a string (or JSON null). A text
// predicate may be answered from the string index only when this holds; see
// valueShape. A name the view has never seen is trivially true.
func (v *View) StringValuesOnly(name string) bool {
	if prop, interned := v.PropIDByName(name); interned {
		shape, ok := v.base.valueShapeFor(prop)
		if !ok || shape.nonString {
			return false
		}
	}
	if d := v.deltaPropFor(name); d != nil && d.nonString {
		return false
	}
	return true
}

// StringListValuesOnly reports whether every node carrying property `name`
// carries it as a list of strings (or JSON null) -- the condition under
// which the element index answers `'x' IN n.name` completely. See valueShape.
func (v *View) StringListValuesOnly(name string) bool {
	if prop, interned := v.PropIDByName(name); interned {
		shape, ok := v.base.valueShapeFor(prop)
		if !ok || shape.nonStringList {
			return false
		}
	}
	if d := v.deltaPropFor(name); d != nil && d.nonStringList {
		return false
	}
	return true
}

// NumbersCanonical reports whether every number property `name` holds -- as
// its value, or inside a list or map value -- base and delta alike, is stored
// in the spelling this package renders its float64 value in
// (numberSpellingCanonical). The replica keeps a number as a float64; only
// where that holds do its comparisons, groupings, text renderings and casts
// of the number answer as PostgreSQL's, which keeps the stored spelling and
// exact value. 9007199254740993 and 9007199254740992 are one float64, so
// DISTINCT, grouping, a join and `= 9007199254740992` merge them; 1.0 is
// the text '1.0' there, which `::int8` rejects. A caller that reads the
// property declines when this is false. A name the view has never seen is
// trivially true.
//
// The base's answer is derived once per snapshot (nonCanonicalNumberProp)
// and a segment's once when it is built, so asking costs a lookup per
// segment and nothing per node.
func (v *View) NumbersCanonical(name string) bool {
	if prop, interned := v.PropIDByName(name); interned && v.base.Props.nonCanonicalNumberProp(prop) {
		return false
	}
	for _, seg := range v.segments {
		if _, marked := seg.nonCanonicalNumberProps[name]; marked {
			return false
		}
	}
	return true
}

// nonCanonicalNumberProp reports whether any node's value of prop carries a
// non-canonical number: a propKindNumberNonCanonical entry, or a list or map
// whose JSON text holds one. Every property is derived in a single pass over
// the store on first use; the store is immutable, so it never changes.
func (p *PropStore) nonCanonicalNumberProp(prop PropID) bool {
	if p == nil {
		return false
	}
	p.numberFactsOnce.Do(func() {
		marks := make([]bool, len(p.names))
		for _, e := range p.entries {
			if int(e.prop) < len(marks) && !marks[e.prop] && entryCarriesNonCanonicalNumber(e, p.arena) {
				marks[e.prop] = true
			}
		}
		p.nonCanonicalNumber = marks
	})
	return int(prop) < len(p.nonCanonicalNumber) && p.nonCanonicalNumber[prop]
}

// entryCarriesNonCanonicalNumber reports whether one stored value is, or
// holds, a non-canonical number; arena is the byte arena its ref points into.
func entryCarriesNonCanonicalNumber(e propEntry, arena []byte) bool {
	switch e.kind {
	case propKindNumberNonCanonical:
		return true
	case propKindArray, propKindObject:
		return jsonCarriesNonCanonicalNumber(arena[e.ref : e.ref+e.len])
	}
	return false
}

// jsonCarriesNonCanonicalNumber reports whether raw, the JSON text of a list
// or map as PostgreSQL rendered it, holds a number whose spelling is not
// canonical -- what `'1' IN n.list` compares and `n.list = [1]` casts. A
// token that does not parse counts as one.
func jsonCarriesNonCanonicalNumber(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		switch c := raw[i]; {
		case c == '"':
			for i++; i < len(raw) && raw[i] != '"'; i++ {
				if raw[i] == '\\' {
					i++
				}
			}
		case c == '-' || (c >= '0' && c <= '9'):
			j := i
			for j < len(raw) && strings.IndexByte("+-.eE0123456789", raw[j]) >= 0 {
				j++
			}
			token := raw[i:j]
			f, err := strconv.ParseFloat(string(token), 64)
			if err != nil || !numberSpellingCanonical(token, f) {
				return true
			}
			i = j - 1
		}
	}
	return false
}

// nonCanonicalNumberProps names every property a live node state in
// nodeStates carries a non-canonical number in -- a Segment's half of
// View.NumbersCanonical, derived once when the segment is built. nil when
// there is none, the ordinary case.
func nonCanonicalNumberProps(nodeStates map[uint64]NodeSegState) map[string]struct{} {
	var out map[string]struct{}
	for _, st := range nodeStates {
		if st.Tombstoned || st.seg == nil {
			continue
		}
		for _, e := range st.entries {
			if !entryCarriesNonCanonicalNumber(e, st.seg.arena) {
				continue
			}
			if out == nil {
				out = make(map[string]struct{})
			}
			out[st.seg.names[e.prop]] = struct{}{}
		}
	}
	return out
}

// mergedNonCanonicalNumberProps unions segs' marked property names, for
// MergeSegments: a merged segment answers for every state it carries over.
func mergedNonCanonicalNumberProps(segs []*Segment) map[string]struct{} {
	var out map[string]struct{}
	for _, seg := range segs {
		if seg == nil {
			continue
		}
		for name := range seg.nonCanonicalNumberProps {
			if out == nil {
				out = make(map[string]struct{})
			}
			out[name] = struct{}{}
		}
	}
	return out
}
