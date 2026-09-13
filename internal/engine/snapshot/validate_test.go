// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// writeOneNodeFile writes a snapshot holding a single node with one
// string-valued property, the smallest file that exercises the property
// arena, and returns its path.
func writeOneNodeFile(t *testing.T) string {
	t.Helper()
	b := NewBuilder(1)
	if err := b.AddNode(1, []KindID{1}, []byte(`{"a":"WXYZ"}`)); err != nil {
		t.Fatal(err)
	}
	b.SetKinds(map[KindID]string{1: "User"})
	snap, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.bin")
	if err := WriteSnapshotFile(path, snap, 7); err != nil {
		t.Fatal(err)
	}
	return path
}

// repatch rewrites the file with old replaced by new and the trailing CRC32
// recomputed, so the result is a file that passes the checksum and has to be
// rejected on its structure alone -- a hand-edited file, or one written by a
// writer that disagrees about the layout.
func repatch(t *testing.T, path string, old, new []byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(data, old)
	if at < 0 {
		t.Fatalf("pattern %x not found in the snapshot file", old)
	}
	if bytes.Contains(data[at+1:], old) {
		t.Fatalf("pattern %x is not unique in the snapshot file", old)
	}
	copy(data[at:], new)
	body := data[len(snapshotMagic) : len(data)-4]
	binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.ChecksumIEEE(body))
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// propEntryWire encodes one property entry the way writePropStore does, so a
// test can find and replace it without hard-coding a file offset.
func propEntryWire(prop uint16, kind uint8, ref, length uint32) []byte {
	out := make([]byte, 0, propEntryWireSize)
	out = binary.LittleEndian.AppendUint16(out, prop)
	out = append(out, kind)
	out = binary.LittleEndian.AppendUint64(out, 0) // num, zero for a string
	out = binary.LittleEndian.AppendUint32(out, ref)
	out = binary.LittleEndian.AppendUint32(out, length)
	return out
}

// TestReadSnapshotFileRejectsAnOversizedPropertyLength is the memory-safety
// case: a property's length is handed to unsafe.String, which does not
// bounds-check it, so a file claiming a 200-byte string in a 4-byte arena
// would otherwise hand out a string aliasing unrelated heap (or crash).
// The CRC cannot catch this -- it is recomputed here, exactly as an editor
// of the file would have to.
func TestReadSnapshotFileRejectsAnOversizedPropertyLength(t *testing.T) {
	path := writeOneNodeFile(t)
	repatch(t, path,
		propEntryWire(0, propKindString, 0, 4),
		propEntryWire(0, propKindString, 0, 200))

	snap, _, err := ReadSnapshotFile(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if snap != nil {
		t.Error("a rejected file must not yield a snapshot")
	}
}

// TestReadSnapshotFileRejectsAnUnknownPropertyName pins the other unchecked
// index: NodeMap uses an entry's prop id directly as names[id], so an
// out-of-range id would panic on the request path rather than at load.
func TestReadSnapshotFileRejectsAnUnknownPropertyName(t *testing.T) {
	path := writeOneNodeFile(t)
	repatch(t, path,
		propEntryWire(0, propKindString, 0, 4),
		propEntryWire(9999, propKindString, 0, 4))

	if _, _, err := ReadSnapshotFile(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

// TestValidateSnapshotStructureCatchesBrokenArrays covers the checks on a
// built snapshot directly, which is cheaper than crafting a file per case and
// keeps every invariant the readers depend on pinned in one place.
func TestValidateSnapshotStructureCatchesBrokenArrays(t *testing.T) {
	build := func(t *testing.T) *Snapshot {
		t.Helper()
		b := NewBuilder(1)
		for i := uint64(1); i <= 3; i++ {
			if err := b.AddNode(i, []KindID{1}, []byte(`{"a":"WXYZ"}`)); err != nil {
				t.Fatal(err)
			}
		}
		b.AddEdge(1, 1, 2, 1)
		b.AddEdge(2, 2, 3, 1)
		b.SetKinds(map[KindID]string{1: "User"})
		snap, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		return snap
	}

	if err := validateSnapshotStructure(build(t)); err != nil {
		t.Fatalf("a freshly built snapshot must validate: %v", err)
	}

	cases := []struct {
		name    string
		corrupt func(*Snapshot)
	}{
		{"out target out of range", func(s *Snapshot) { s.OutTargets[0] = NodeID(len(s.GraphIDs)) }},
		{"in target out of range", func(s *Snapshot) { s.InTargets[0] = NodeID(len(s.GraphIDs)) }},
		{"reverse edge index out of range", func(s *Snapshot) { s.InEdgeIdx[0] = uint32(len(s.OutTargets)) }},
		{"edge id permutation out of range", func(s *Snapshot) { s.edgeIDPerm[0] = uint32(len(s.OutTargets)) }},
		{"offsets run backwards", func(s *Snapshot) { s.OutOffsets[1] = s.OutOffsets[2] + 1 }},
		{"offsets do not start at zero", func(s *Snapshot) { s.OutOffsets[0] = 1 }},
		{"offsets do not cover the edges", func(s *Snapshot) { s.OutOffsets[len(s.OutOffsets)-1] = 1 }},
		{"kind offsets do not cover the kinds", func(s *Snapshot) { s.KindOffsets[len(s.KindOffsets)-1] = 0 }},
		{"database ids not ascending", func(s *Snapshot) { s.GraphIDs[2] = s.GraphIDs[1] }},
		{"aligned array truncated", func(s *Snapshot) { s.OutKinds = s.OutKinds[:len(s.OutKinds)-1] }},
		{"property offsets do not cover the entries", func(s *Snapshot) {
			s.Props.nodeOffsets[len(s.Props.nodeOffsets)-1] = 0
		}},
		{"property arena overrun", func(s *Snapshot) {
			s.Props.entries[0].len = uint32(len(s.Props.arena)) + 1
		}},
		{"unknown property kind", func(s *Snapshot) { s.Props.entries[0].kind = 250 }},
		{"property store missing", func(s *Snapshot) { s.Props = nil }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := build(t)
			c.corrupt(s)
			if err := validateSnapshotStructure(s); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
		})
	}
}

// TestOverlayIndexesAnEmptyObjectID pins that an objectid of "" written
// through a delta segment resolves the same way before and after a fold.
// The empty string is an ordinary property value, and the base PropStore
// indexes it; keying the segment index off ObjectID != "" instead made such
// a node findable only once a compaction happened to fold it in, so the same
// lookup gave two different answers depending on when it ran.
func TestOverlayIndexesAnEmptyObjectID(t *testing.T) {
	b := NewBuilder(1)
	if err := b.AddNode(10, []KindID{1}, []byte(`{"objectid":""}`)); err != nil {
		t.Fatal(err)
	}
	b.SetKinds(map[KindID]string{1: "User"})
	base, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}

	sb := &SegmentBuilder{}
	if err := sb.AddNodeState(11, []KindID{1}, []byte(`{"objectid":""}`)); err != nil {
		t.Fatal(err)
	}
	seg := sb.Build()

	overlay := NewView(base).WithSegment(seg)
	overlayIDs, _ := overlay.NodesByObjectID("")

	folded, err := Fold(base, []*Segment{seg})
	if err != nil {
		t.Fatal(err)
	}
	foldedIDs, _ := NewView(folded).NodesByObjectID("")

	if len(overlayIDs) != len(foldedIDs) {
		t.Fatalf("objectid \"\" resolves to %d nodes through the overlay but %d after folding the same writes",
			len(overlayIDs), len(foldedIDs))
	}
	if len(foldedIDs) != 2 {
		t.Fatalf("objectid \"\" resolves to %d nodes, want both", len(foldedIDs))
	}
}
