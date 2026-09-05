// SPDX-License-Identifier: Apache-2.0
package snapshot

// KindTable is a snapshot-owned, bidirectional mapping between database kind
// ids and their string names, built once from the PostgreSQL `kind` table's
// entire contents (a global table, not scoped to any one graph -- see
// LoadSnapshot) and immutable thereafter.
//
// Name lookups are O(1) via a dense slice indexed by KindID; ID lookups are
// O(1) via a map keyed by name, for resolving labels and relationship types
// parsed out of Cypher text.
type KindTable struct {
	names []string // dense, indexed by KindID; "" where no kind occupies that id
	ids   map[string]KindID
}

// NewKindTable builds a KindTable from id->name pairs, such as those scanned
// from the `kind` table. A nil or empty pairs is a valid, empty table.
func NewKindTable(pairs map[KindID]string) *KindTable {
	var maxID KindID
	for id := range pairs {
		if id > maxID {
			maxID = id
		}
	}

	names := make([]string, int(maxID)+1)
	ids := make(map[string]KindID, len(pairs))
	for id, name := range pairs {
		if id >= 0 {
			names[id] = name
		}
		ids[name] = id
	}

	return &KindTable{names: names, ids: ids}
}

// Name returns the name registered for id, and whether one was found.
func (t *KindTable) Name(id KindID) (string, bool) {
	if id < 0 || int(id) >= len(t.names) {
		return "", false
	}
	name := t.names[id]
	if name == "" {
		return "", false
	}
	return name, true
}

// ID returns the KindID registered for name, and whether one was found.
func (t *KindTable) ID(name string) (KindID, bool) {
	id, ok := t.ids[name]
	return id, ok
}

// Len returns the number of kinds registered in the table.
func (t *KindTable) Len() int {
	return len(t.ids)
}

// Approximate per-entry/header byte overheads used by ApproxBytes. These are
// rough, fixed estimates rather than runtime-measured sizes, mirroring the
// approach Snapshot.ApproxBytes takes for its own unexported maps.
const (
	approxKindTableMapEntryBytes = 24 // string key + KindID value + map bucket overhead
	bytesPerSliceHeader          = 24 // slice header: data pointer + len + cap
)

// ApproxBytes estimates the table's resident memory footprint: the sum of
// the registered names' byte lengths, plus a rough per-entry overhead for
// the id->name map and the dense names slice's own header.
func (t *KindTable) ApproxBytes() uint64 {
	var total uint64

	total += bytesPerSliceHeader
	for _, name := range t.names {
		total += uint64(len(name))
	}
	total += uint64(len(t.ids)) * approxKindTableMapEntryBytes

	return total
}
