// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"unsafe"
)

// PropID is a dense, interned identifier for a property name, scoped to one
// Snapshot's PropStore (analogous to KindID for kind names).
type PropID uint16

// Property value kinds, stored per entry in propEntry.kind. These mirror
// the JSON value types a PostgreSQL jsonb column round-trips: null, the two
// booleans, number, string, array, and object.
const (
	propKindNull uint8 = iota
	propKindFalse
	propKindTrue
	propKindNumber
	propKindString
	propKindArray
	propKindObject
)

// propEntry is one property value belonging to one node: which property
// (prop), what kind of JSON value it is (kind), and where its payload
// lives. Scalars that fit in a machine word are stored inline (num for
// numbers; kind alone distinguishes true/false/null). Strings, arrays, and
// objects instead store a byte range [ref, ref+len) into the PropStore's
// shared arena -- for strings, the raw string content; for arrays and
// objects, the raw JSON text, decoded with encoding/json on each access
// (see PropStore.decode).
//
// A node's entries are stored as a contiguous slice (see PropStore.entries
// and nodeOffsets) sorted by prop, so Value can binary-search a single
// node's properties rather than scanning them.
type propEntry struct {
	prop PropID
	kind uint8
	num  float64
	ref  uint32
	len  uint32
}

// PropStore holds every node's property bag in the snapshot, packed into a
// compact columnar form: an interned property-name table, one flat entries
// array (each node's slice sorted by PropID), per-node offsets into it, and
// one shared byte arena backing string and raw-JSON (array/object) values.
// It also carries an exact-match index from the `objectid` property (where
// present and string-valued) to the owning node, since predicates and path
// lookups keyed by objectid are the most common Cypher entry point into a
// BloodHound graph.
//
// A PropStore is built once by Builder.Build and is immutable thereafter,
// which is what makes StringAt-style zero-copy string returns over the
// shared arena safe (see stringAt).
type PropStore struct {
	names []string // dense, indexed by PropID
	ids   map[string]PropID

	entries     []propEntry // all nodes' entries concatenated; each node's slice sorted by prop
	nodeOffsets []uint32    // len NodeCount()+1, into entries

	arena []byte // shared string / raw-JSON byte arena

	objectIndex map[string]NodeID // objectid string value -> node, only for string-valued objectid
}

// IDByName returns the PropID interned for name, and whether one exists.
func (p *PropStore) IDByName(name string) (PropID, bool) {
	id, ok := p.ids[name]
	return id, ok
}

// Name returns the property name registered for id, or "" if id is out of
// range.
func (p *PropStore) Name(id PropID) string {
	if int(id) >= len(p.names) {
		return ""
	}
	return p.names[id]
}

// Value returns node n's value for property id. The returned any is
// strictly the post-JSON model encoding/json produces by default:
// float64 | string | bool | []any | map[string]any | nil. ok is false only
// when the key is absent from n's bag; a stored JSON null returns (nil,
// true), distinguishable from absence.
func (p *PropStore) Value(n NodeID, id PropID) (any, bool) {
	lo, hi := p.nodeOffsets[n], p.nodeOffsets[n+1]
	entries := p.entries[lo:hi]

	i := sort.Search(len(entries), func(i int) bool { return entries[i].prop >= id })
	if i >= len(entries) || entries[i].prop != id {
		return nil, false
	}
	return p.decode(entries[i]), true
}

// NodeMap returns node n's full property bag as a fresh map on every call,
// suitable for materializing a Cypher node value. It deep-equals what
// json.Unmarshal of the original jsonb bag into a map[string]any would
// produce, including JSON-null keys present with nil values.
func (p *PropStore) NodeMap(n NodeID) map[string]any {
	lo, hi := p.nodeOffsets[n], p.nodeOffsets[n+1]
	entries := p.entries[lo:hi]

	m := make(map[string]any, len(entries))
	for _, e := range entries {
		m[p.names[e.prop]] = p.decode(e)
	}
	return m
}

// NodeByObjectID returns the node whose `objectid` property has the exact
// string value v, and whether one was found. Only nodes whose objectid
// value is a JSON string are indexed; a numeric or otherwise non-string
// objectid never matches.
func (p *PropStore) NodeByObjectID(v string) (NodeID, bool) {
	id, ok := p.objectIndex[v]
	return id, ok
}

// decode turns one propEntry into the post-JSON model value Value/NodeMap
// return. Arrays and objects are decoded from their raw JSON arena bytes on
// every call rather than kept resident in decoded form -- this keeps the
// snapshot's steady-state memory to the packed columnar arrays, since
// arrays/objects inside node properties are rare and typically small
// (BloodHound's array-valued properties, e.g. spns/serviceprincipalnames,
// are short lists of strings).
func (p *PropStore) decode(e propEntry) any {
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
		return p.stringAt(e.ref, e.len)
	case propKindArray:
		var v []any
		if err := json.Unmarshal(p.arena[e.ref:e.ref+e.len], &v); err != nil {
			// The arena only ever holds bytes this package itself wrote from
			// already-validated JSON (see parseNodeProps), so a decode
			// failure here means PropStore's own invariant was violated,
			// not bad input -- a programmer error worth failing loudly on.
			panic(fmt.Sprintf("snapshot: PropStore: corrupt array bytes for prop %d: %v", e.prop, err))
		}
		return v
	case propKindObject:
		var v map[string]any
		if err := json.Unmarshal(p.arena[e.ref:e.ref+e.len], &v); err != nil {
			panic(fmt.Sprintf("snapshot: PropStore: corrupt object bytes for prop %d: %v", e.prop, err))
		}
		return v
	default:
		return nil
	}
}

// stringAt returns the arena substring [ref, ref+length) as a Go string via
// unsafe.String, aliasing the PropStore's backing arena rather than copying
// it. This is safe only because a PropStore is immutable once Build returns:
// its arena is allocated once and never mutated, appended to, or resized
// afterward, so every string handed out this way remains valid for as long
// as the owning Snapshot (and therefore its arena) is reachable.
func (p *PropStore) stringAt(ref, length uint32) string {
	if length == 0 {
		return ""
	}
	return unsafe.String(&p.arena[ref], length)
}

// Approximate per-entry/per-name-map-entry byte overhead used by
// ApproxBytes. Fixed, non-runtime-measured estimates, in the same spirit as
// Snapshot's and KindTable's own ApproxBytes constants.
const (
	// bytesPerPropEntry is the actual in-memory size of one propEntry
	// (PropID + kind + num + ref + len, with struct padding); computed via
	// unsafe.Sizeof rather than guessed, since propEntry has only
	// fixed-width fields.
	bytesPerPropEntry = uint64(unsafe.Sizeof(propEntry{}))

	approxPropNameMapEntryBytes = 24 // string key + PropID value + map bucket overhead
	approxObjectIndexEntryBytes = 24 // NodeID value + map bucket overhead (key bytes counted via the arena, see below)
)

// ApproxBytes estimates the PropStore's resident memory footprint:
//
//	total = len(arena)                          -- exact: every stored string/raw-JSON byte
//	      + len(entries)      * bytesPerPropEntry -- fixed per-entry overhead
//	      + len(nodeOffsets)  * bytesPerUint32     -- fixed per-node overhead
//	      + sum(len(name) for name in names)       -- interned property names
//	      + len(ids)          * approxPropNameMapEntryBytes
//	      + len(objectIndex)  * approxObjectIndexEntryBytes
//
// The objectIndex's string keys are not double-counted: they are produced
// by Value/decode via stringAt, which aliases the arena (see stringAt), so
// their bytes are already counted in len(arena) above -- only the map's own
// per-entry bucket/pointer overhead is added.
func (p *PropStore) ApproxBytes() uint64 {
	var total uint64

	total += uint64(len(p.arena))
	total += uint64(len(p.entries)) * bytesPerPropEntry
	total += uint64(len(p.nodeOffsets)) * bytesPerUint32

	for _, name := range p.names {
		total += uint64(len(name))
	}
	total += uint64(len(p.ids)) * approxPropNameMapEntryBytes

	total += uint64(len(p.objectIndex)) * approxObjectIndexEntryBytes

	return total
}

// parsedProp is one property value from a node's jsonb bag, fully decoded
// and validated by parseNodeProps but not yet committed into a Builder's
// arena or entries array. Keeping parse and commit separate lets AddNode
// validate an entire bag before mutating any Builder state, so a decode
// error leaves the Builder exactly as if AddNode had not been called (see
// Builder.AddNode).
type parsedProp struct {
	name  string
	kind  uint8
	num   float64
	bytes []byte // raw payload for kindString (content) / kindArray / kindObject (raw JSON)
}

// parseNodeProps decodes a node's jsonb property bag into parsedProp
// values. propsJSON may be nil or empty, meaning no properties. Malformed
// JSON is impossible in practice (jsonb guarantees valid JSON on the way
// out of PostgreSQL), but is still surfaced as an error rather than
// panicking or silently dropping data, per the brief: a decode failure here
// can only mean programmer error (e.g. hand-written test JSON), and should
// fail loudly.
func parseNodeProps(propsJSON []byte) ([]parsedProp, error) {
	if len(propsJSON) == 0 {
		return nil, nil
	}

	var raw map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(propsJSON))
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode property bag: %w", err)
	}

	out := make([]parsedProp, 0, len(raw))
	for name, val := range raw {
		trimmed := bytes.TrimSpace(val)
		pp := parsedProp{name: name}

		if len(trimmed) == 0 {
			pp.kind = propKindNull
			out = append(out, pp)
			continue
		}

		switch trimmed[0] {
		case 'n':
			pp.kind = propKindNull
		case 't':
			pp.kind = propKindTrue
		case 'f':
			pp.kind = propKindFalse
		case '"':
			var s string
			if err := json.Unmarshal(trimmed, &s); err != nil {
				return nil, fmt.Errorf("decode property %q: %w", name, err)
			}
			pp.kind = propKindString
			pp.bytes = []byte(s)
		case '[':
			pp.kind = propKindArray
			pp.bytes = trimmed
		case '{':
			pp.kind = propKindObject
			pp.bytes = trimmed
		default:
			var f float64
			if err := json.Unmarshal(trimmed, &f); err != nil {
				return nil, fmt.Errorf("decode property %q: %w", name, err)
			}
			pp.kind = propKindNumber
			pp.num = f
		}
		out = append(out, pp)
	}
	return out, nil
}

// internProp returns the PropID for name, interning it (assigning the next
// dense id) if this is the first time name has been seen by this Builder.
func (b *Builder) internProp(name string) PropID {
	if id, ok := b.propIDs[name]; ok {
		return id
	}
	if b.propIDs == nil {
		b.propIDs = make(map[string]PropID)
	}
	id := PropID(len(b.propNames))
	b.propNames = append(b.propNames, name)
	b.propIDs[name] = id
	return id
}

// commitNodeProps interns each parsed property's name, appends its payload
// (if any) to the Builder's shared arena, sorts the node's entries by
// PropID, and appends them to the Builder's flat entries/offsets arrays.
// Called only after parseNodeProps has already validated the whole bag, so
// this step cannot itself fail.
func (b *Builder) commitNodeProps(parsed []parsedProp) {
	entries := make([]propEntry, len(parsed))
	for i, pp := range parsed {
		e := propEntry{prop: b.internProp(pp.name), kind: pp.kind, num: pp.num}
		switch pp.kind {
		case propKindString, propKindArray, propKindObject:
			e.ref = uint32(len(b.propArena))
			b.propArena = append(b.propArena, pp.bytes...)
			e.len = uint32(len(pp.bytes))
		}
		entries[i] = e
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].prop < entries[j].prop })

	b.propEntries = append(b.propEntries, entries...)
	b.propOffsets = append(b.propOffsets, uint32(len(b.propEntries)))
}

// buildPropStore finalizes the Builder's staged property data (copied, not
// aliased, matching how Build copies every other staged slice into the
// Snapshot) into an immutable PropStore, then builds the objectid index
// over the finished store.
func (b *Builder) buildPropStore(n int) *PropStore {
	names := append([]string(nil), b.propNames...)
	ids := make(map[string]PropID, len(b.propIDs))
	for k, v := range b.propIDs {
		ids[k] = v
	}
	entries := append([]propEntry(nil), b.propEntries...)
	nodeOffsets := append([]uint32(nil), b.propOffsets...)
	arena := append([]byte(nil), b.propArena...)

	p := &PropStore{
		names:       names,
		ids:         ids,
		entries:     entries,
		nodeOffsets: nodeOffsets,
		arena:       arena,
	}

	objID, hasObjID := p.IDByName("objectid")
	p.objectIndex = make(map[string]NodeID)
	if hasObjID {
		for i := 0; i < n; i++ {
			v, ok := p.Value(NodeID(i), objID)
			if !ok {
				continue
			}
			if s, ok := v.(string); ok {
				p.objectIndex[s] = NodeID(i)
			}
		}
	}

	return p
}
