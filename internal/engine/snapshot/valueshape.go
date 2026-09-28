// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"encoding/json"
	"sort"
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
