// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
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

	// objectIndex maps a string-valued objectid to one node carrying it --
	// the first one encountered during buildPropStore's ascending-NodeID
	// scan. PostgreSQL enforces no uniqueness constraint on objectid, so
	// real BloodHound data can and does contain more than one node sharing
	// the same value; when that happens the colliding value's *complete*
	// node set is additionally recorded in objectIndexDup, and objectIndex
	// keeps just the one arbitrary member for NodeByObjectID's O(1)
	// point-lookup fast path. objectIndexDup is nil (and adds zero memory)
	// whenever every objectid in the snapshot is unique, which is the
	// overwhelming common case.
	objectIndex    map[string]NodeID
	objectIndexDup map[string][]NodeID
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

// NodeByObjectID returns ONE node whose `objectid` property has the exact
// string value v, and whether any match was found. Only nodes whose
// objectid value is a JSON string are indexed; a numeric or otherwise
// non-string objectid never matches.
//
// PostgreSQL enforces no uniqueness constraint on objectid, so more than one
// node can legitimately carry the same value. When that happens,
// NodeByObjectID returns an arbitrary one of them (which one is unspecified
// and must not be relied on) -- it exists purely as an O(1) point-lookup
// convenience for callers that only need *a* witness (e.g. "does this value
// exist at all"). A caller that must not silently drop rows on a duplicate
// -- an anchor scan seeding a candidate set, or a constraint check deciding
// whether a specific node id satisfies an objectid predicate -- must use
// NodesByObjectID instead, which returns every match.
func (p *PropStore) NodeByObjectID(v string) (NodeID, bool) {
	id, ok := p.objectIndex[v]
	return id, ok
}

// NodesByObjectID returns every node whose `objectid` property has the
// exact string value v, and whether any match was found. In the common
// case (v is unique or absent) this costs no more than NodeByObjectID: the
// single match, if any, comes straight from objectIndex with no extra
// lookup or allocation. Only when v collides across more than one node does
// it consult objectIndexDup for the complete set.
func (p *PropStore) NodesByObjectID(v string) ([]NodeID, bool) {
	if dup, ok := p.objectIndexDup[v]; ok {
		return dup, true
	}
	if id, ok := p.objectIndex[v]; ok {
		return []NodeID{id}, true
	}
	return nil, false
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
//	      + sum(len(ids) for ids in objectIndexDup) * bytesPerUint32 -- dup slots
//
// The objectIndex's string keys are not double-counted: they are produced
// by Value/decode via stringAt, which aliases the arena (see stringAt), so
// their bytes are already counted in len(arena) above -- only the map's own
// per-entry bucket/pointer overhead is added. objectIndexDup is empty (and
// contributes nothing) unless the snapshot actually contains a duplicate
// objectid value; when it does, only the additional NodeIDs beyond the one
// already counted via objectIndex are added, plus the map's own overhead.
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

	total += uint64(len(p.objectIndexDup)) * approxObjectIndexEntryBytes
	for _, ids := range p.objectIndexDup {
		total += uint64(len(ids)) * bytesPerUint32
	}

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

// ParsedProps is a node's property bag, already parsed and validated by
// ParseProps, ready to be staged via Builder.AddParsedNode. It carries no
// exported fields: the only thing a caller outside this package can do with
// one is hand it to AddParsedNode.
//
// ParseProps/AddParsedNode exist to split AddNode's two halves -- JSON
// parsing (pure, CPU-bound, safe to run concurrently) and committing into
// the Builder's shared arena/intern table (stateful, must run on one
// goroutine, in strictly ascending databaseID order) -- so a caller loading
// many nodes can parallelize the first half across a worker pool while
// keeping the second on a single, ordered goroutine. See
// internal/engine/load.go's loadNodes for the intended pipeline shape.
type ParsedProps struct {
	parsed []parsedProp
}

// ParseProps parses and validates propsJSON exactly as AddNode does
// internally (see parseNodeProps), without touching any Builder state.
// Unlike AddNode, it is safe to call concurrently from multiple goroutines
// -- it allocates and returns fresh state, touching nothing shared -- which
// is the whole point: pair it with Builder.AddParsedNode to parallelize
// property-bag parsing ahead of Builder's own serial, ordered commit step.
func ParseProps(propsJSON []byte) (ParsedProps, error) {
	parsed, err := parseNodeProps(propsJSON)
	if err != nil {
		return ParsedProps{}, err
	}
	return ParsedProps{parsed: parsed}, nil
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

// maxPropID is the largest value PropID (a uint16) can hold, and therefore
// the largest number of distinct property names minus one -- math.MaxUint16
// distinct names (ids 0..maxPropID) is exactly as many as a PropID can ever
// represent.
const maxPropID = math.MaxUint16

// internProp returns the PropID for name, interning it (assigning the next
// dense id) if this is the first time name has been seen by this Builder.
// Returns an error, refusing to intern name at all, if this Builder has
// already interned math.MaxUint16+1 distinct names (PropID's own full
// range): assigning one more would silently wrap PropID(len(b.propNames))
// back to an id already in use, aliasing two entirely different property
// names under the same PropID -- every subsequent Value/NodeMap lookup for
// either name would then read whichever one happened to sort first for a
// given node, a silent correctness corruption, not merely a missing
// feature. A database with more than 65536 distinct property names across
// an entire graph is not something real BloodHound data is expected to
// ever produce, but this Builder had no
// guard against it at all before this fix -- failing loudly here instead
// means Build (and therefore LoadSnapshot) simply refuses the snapshot,
// which leaves the engine with nothing to serve from, so every query
// delegates to PostgreSQL -- this package's and the wider engine's usual
// "reject at build/plan time, delegate" posture, applied one layer
// earlier than usual.
func (b *Builder) internProp(name string) (PropID, error) {
	if id, ok := b.propIDs[name]; ok {
		return id, nil
	}
	if len(b.propNames) > maxPropID {
		return 0, fmt.Errorf("snapshot: internProp: more than %d distinct property names (PropID, a uint16, would wrap)", maxPropID+1)
	}
	if b.propIDs == nil {
		b.propIDs = make(map[string]PropID)
	}
	id := PropID(len(b.propNames))
	b.propNames = append(b.propNames, name)
	b.propIDs[name] = id
	return id, nil
}

// commitNodeProps interns each parsed property's name, appends its payload
// (if any) to the Builder's shared arena, sorts the node's entries by
// PropID, and appends them to the Builder's flat entries/offsets arrays.
// Called only after parseNodeProps has already validated the whole bag, so
// the only way this can now fail is internProp's own PropID-wrap guard
// (see its doc) -- and on that error, nothing here has yet been appended to
// b.propEntries/b.propOffsets (entries is a local slice, only committed
// to Builder state once the whole loop succeeds), so the Builder's
// property-storage state is left exactly as it was before this call, the
// same "no partial mutation on error" contract AddNode's own doc promises
// for a parse failure.
func (b *Builder) commitNodeProps(parsed []parsedProp) error {
	entries := make([]propEntry, len(parsed))
	for i, pp := range parsed {
		propID, err := b.internProp(pp.name)
		if err != nil {
			return err
		}
		e := propEntry{prop: propID, kind: pp.kind, num: pp.num}
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
	return nil
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
			s, ok := v.(string)
			if !ok {
				continue
			}
			id := NodeID(i)
			prev, seen := p.objectIndex[s]
			switch {
			case !seen:
				p.objectIndex[s] = id
			case p.objectIndexDup[s] != nil:
				p.objectIndexDup[s] = append(p.objectIndexDup[s], id)
			default:
				if p.objectIndexDup == nil {
					p.objectIndexDup = make(map[string][]NodeID)
				}
				p.objectIndexDup[s] = []NodeID{prev, id}
			}
		}
	}

	return p
}
