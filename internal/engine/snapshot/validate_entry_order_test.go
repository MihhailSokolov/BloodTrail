// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestReadSnapshotFileRefusesUnorderedPropertyEntries writes a snapshot
// whose first node's property entries are out of order -- a file whose CRC32
// is right for its bytes, as one edited and re-checksummed, or written by a
// buggy writer, would be -- and checks it is refused. Every reader of a
// node's properties (PropStore.Value's binary search, and through it the
// objectid index finalizeDerived builds) assumes a node's entries ascend by
// property id; out of order, a property the node carries reads as absent,
// and queries that filter on it come back short.
func TestReadSnapshotFileRefusesUnorderedPropertyEntries(t *testing.T) {
	s := buildFileFixture(t)
	s.WatermarkLineage = testLineage

	lo, hi := s.Props.nodeOffsets[0], s.Props.nodeOffsets[1]
	if hi-lo < 2 {
		t.Fatalf("fixture node 0 has %d property entries, want at least 2 (fixture assumption)", hi-lo)
	}
	entries := s.Props.entries[lo:hi]
	entries[0], entries[len(entries)-1] = entries[len(entries)-1], entries[0]
	displaced := s.Props.names[entries[0].prop] // now at the front of a range sorted by prop id

	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, testStamp); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}

	got, _, err := ReadSnapshotFile(path)
	if errors.Is(err, ErrCorrupt) {
		return
	}
	if err != nil {
		t.Fatalf("ReadSnapshotFile = %v, want ErrCorrupt", err)
	}
	_, found := NewView(got).PropValueByName(0, displaced)
	t.Fatalf("ReadSnapshotFile accepted a file whose node 0 property entries are out of order (PropValueByName(0, %q) found = %v although the node carries it); want ErrCorrupt", displaced, found)
}
