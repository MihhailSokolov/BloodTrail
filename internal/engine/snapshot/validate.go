// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"fmt"
)

// validateSnapshotStructure checks the structural invariants every reader of
// a Snapshot relies on, and which nothing in the wire format itself
// guarantees. ReadSnapshotFile runs it after the CRC32 trailer has verified
// the bytes and before finalizeDerived touches them.
//
// The CRC is a corruption checksum, not a structural check: it proves the
// file is the one that was written, not that the numbers in it describe a
// graph. A file whose bytes were edited and whose CRC was recomputed -- or
// one produced by a future writer with a different idea of the layout --
// otherwise reaches code that indexes straight into these arrays. Two of
// those paths are memory-unsafe rather than merely wrong:
//
//   - PropStore.stringAt hands a property's (ref, len) to unsafe.String,
//     which does not bounds-check the length. A too-large len yields a
//     string aliasing whatever heap follows the arena -- silently serving
//     out-of-bounds memory as a property value, or crashing outright.
//   - An entry's prop is used unchecked as p.names[e.prop] by NodeMap, so
//     an out-of-range id panics on the request path, far from the load that
//     accepted it.
//
// The rest (offsets that do not start at zero, run backwards, or do not end
// at the array they index; targets or edge indices out of range) would panic
// or silently answer the wrong node, on whichever query first happened to
// touch that slot.
//
// Every violation is reported as ErrCorrupt, so boot treats such a file the
// way it treats any other unusable one: reject it and rebuild from
// PostgreSQL.
func validateSnapshotStructure(s *Snapshot) error {
	n := len(s.GraphIDs)
	e := len(s.OutTargets)

	for _, c := range []struct {
		name string
		got  int
		want int
	}{
		{"OutKinds", len(s.OutKinds), e},
		{"OutEdgeIDs", len(s.OutEdgeIDs), e},
		{"InTargets", len(s.InTargets), e},
		{"InKinds", len(s.InKinds), e},
		{"InEdgeIdx", len(s.InEdgeIdx), e},
		{"edgeIDPerm", len(s.edgeIDPerm), e},
		{"OutOffsets", len(s.OutOffsets), n + 1},
		{"InOffsets", len(s.InOffsets), n + 1},
		{"KindOffsets", len(s.KindOffsets), n + 1},
	} {
		if c.got != c.want {
			return fmt.Errorf("%w: %s has %d entries, want %d", ErrCorrupt, c.name, c.got, c.want)
		}
	}

	if err := validateOffsets64("OutOffsets", s.OutOffsets, e); err != nil {
		return err
	}
	if err := validateOffsets64("InOffsets", s.InOffsets, e); err != nil {
		return err
	}
	if err := validateOffsets32("KindOffsets", s.KindOffsets, len(s.NodeKinds)); err != nil {
		return err
	}

	// Dense ids are assigned in ascending database-id order, and both
	// Snapshot.Dense (through idIndex) and every caller that relies on
	// "dense ascending == database-id ascending" -- traverse's result
	// ordering among them -- depend on it. Equal ids would additionally
	// collapse two dense nodes onto one idIndex entry.
	for i := 1; i < n; i++ {
		if s.GraphIDs[i] <= s.GraphIDs[i-1] {
			return fmt.Errorf("%w: GraphIDs not strictly ascending at %d (%d after %d)", ErrCorrupt, i, s.GraphIDs[i], s.GraphIDs[i-1])
		}
	}

	for i, t := range s.OutTargets {
		if int(t) >= n {
			return fmt.Errorf("%w: OutTargets[%d] = %d, out of range for %d nodes", ErrCorrupt, i, t, n)
		}
	}
	for i, t := range s.InTargets {
		if int(t) >= n {
			return fmt.Errorf("%w: InTargets[%d] = %d, out of range for %d nodes", ErrCorrupt, i, t, n)
		}
	}
	for i, idx := range s.InEdgeIdx {
		if int(idx) >= e {
			return fmt.Errorf("%w: InEdgeIdx[%d] = %d, out of range for %d edges", ErrCorrupt, i, idx, e)
		}
	}
	for i, idx := range s.edgeIDPerm {
		if int(idx) >= e {
			return fmt.Errorf("%w: edgeIDPerm[%d] = %d, out of range for %d edges", ErrCorrupt, i, idx, e)
		}
	}

	return validatePropStore(s.Props, n)
}

// validatePropStore is the half of the structural check that guards memory
// safety: an entry's arena slice is handed to unsafe.String, and its prop id
// indexes the name table directly.
func validatePropStore(p *PropStore, n int) error {
	if p == nil {
		return fmt.Errorf("%w: property store missing", ErrCorrupt)
	}
	if len(p.nodeOffsets) != n+1 {
		return fmt.Errorf("%w: property nodeOffsets has %d entries, want %d", ErrCorrupt, len(p.nodeOffsets), n+1)
	}
	if err := validateOffsets32("property nodeOffsets", p.nodeOffsets, len(p.entries)); err != nil {
		return err
	}

	arenaLen := uint64(len(p.arena))
	names := len(p.names)
	for i, entry := range p.entries {
		if int(entry.prop) >= names {
			return fmt.Errorf("%w: property entry %d names id %d, but only %d names are registered", ErrCorrupt, i, entry.prop, names)
		}
		switch entry.kind {
		case propKindString, propKindArray, propKindObject:
			if uint64(entry.ref)+uint64(entry.len) > arenaLen {
				return fmt.Errorf("%w: property entry %d spans arena bytes [%d,%d) of %d", ErrCorrupt, i, entry.ref, uint64(entry.ref)+uint64(entry.len), arenaLen)
			}
		case propKindNull, propKindFalse, propKindTrue, propKindNumber:
			// Value kinds carried inline; no arena reference to check.
		default:
			return fmt.Errorf("%w: property entry %d has unknown kind %d", ErrCorrupt, i, entry.kind)
		}
	}
	return nil
}

// validateOffsets64 checks one CSR offset array: it must start at zero, never
// run backwards, and end exactly at the length of the array it indexes.
func validateOffsets64(name string, offsets []uint64, indexed int) error {
	if len(offsets) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrCorrupt, name)
	}
	if offsets[0] != 0 {
		return fmt.Errorf("%w: %s starts at %d, not 0", ErrCorrupt, name, offsets[0])
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] {
			return fmt.Errorf("%w: %s runs backwards at %d (%d after %d)", ErrCorrupt, name, i, offsets[i], offsets[i-1])
		}
	}
	if last := offsets[len(offsets)-1]; last != uint64(indexed) {
		return fmt.Errorf("%w: %s ends at %d, but it indexes %d entries", ErrCorrupt, name, last, indexed)
	}
	return nil
}

// validateOffsets32 is validateOffsets64 for the uint32-wide offset arrays.
func validateOffsets32(name string, offsets []uint32, indexed int) error {
	if len(offsets) == 0 {
		return fmt.Errorf("%w: %s is empty", ErrCorrupt, name)
	}
	if offsets[0] != 0 {
		return fmt.Errorf("%w: %s starts at %d, not 0", ErrCorrupt, name, offsets[0])
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] {
			return fmt.Errorf("%w: %s runs backwards at %d (%d after %d)", ErrCorrupt, name, i, offsets[i], offsets[i-1])
		}
	}
	if last := offsets[len(offsets)-1]; last != uint32(indexed) {
		return fmt.Errorf("%w: %s ends at %d, but it indexes %d entries", ErrCorrupt, name, last, indexed)
	}
	return nil
}
