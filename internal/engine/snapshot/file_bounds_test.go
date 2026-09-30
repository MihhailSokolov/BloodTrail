// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// maxAllocRefusingCorruptFile bounds what ReadSnapshotFile may allocate
// before it refuses a file whose count or length field is larger than the
// file itself: its 1 MiB read buffer plus bookkeeping, and nothing sized by
// the corrupt field.
const maxAllocRefusingCorruptFile = 4 << 20

// overwriteFileU32 overwrites the 4 little-endian bytes at off in data with
// v, after checking they currently hold want -- the proof that off really is
// the field the caller means, so a drifted offset fails loudly instead of
// patching some other byte and passing vacuously.
func overwriteFileU32(t *testing.T, data []byte, off int, want, v uint32) {
	t.Helper()
	if off < 0 || off+4 > len(data) {
		t.Fatalf("offset %d out of range for a %d-byte file", off, len(data))
	}
	if got := binary.LittleEndian.Uint32(data[off:]); got != want {
		t.Fatalf("field at offset %d holds %d, want %d (offset drifted from the format)", off, got, want)
	}
	binary.LittleEndian.PutUint32(data[off:], v)
}

// overwriteFileU64 is overwriteFileU32 for an 8-byte field.
func overwriteFileU64(t *testing.T, data []byte, off int, want, v uint64) {
	t.Helper()
	if off < 0 || off+8 > len(data) {
		t.Fatalf("offset %d out of range for a %d-byte file", off, len(data))
	}
	if got := binary.LittleEndian.Uint64(data[off:]); got != want {
		t.Fatalf("field at offset %d holds %d, want %d (offset drifted from the format)", off, got, want)
	}
	binary.LittleEndian.PutUint64(data[off:], v)
}

// TestReadSnapshotFileRefusesCountsLargerThanTheFile patches, one at a
// time, every count and length field ReadSnapshotFile sizes an allocation
// by, to a value whose wire size is far beyond what the whole file holds,
// and checks the file is refused as ErrCorrupt without that allocation
// being made. The CRC32 trailer that would catch the corruption is checked
// only after the whole body has been read, so until then the only honest
// bound on a count is the bytes the file still has left; bounded only by
// maxReadAlloc (1 TiB), a single flipped high bit in any of these fields
// made every boot attempt an allocation large enough to be killed for, until
// someone deleted the file.
//
// The patched values keep the demanded sizes to tens of MiB -- enough to
// tell "sized by the field" from "refused first" by a wide margin, while
// staying harmless on a build that still allocates before it checks.
func TestReadSnapshotFileRefusesCountsLargerThanTheFile(t *testing.T) {
	s := buildFileFixture(t)
	s.WatermarkLineage = testLineage
	dir := t.TempDir()
	validPath := filepath.Join(dir, "valid.bin")
	if err := WriteSnapshotFile(validPath, s, testStamp); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}
	valid, err := os.ReadFile(validPath)
	if err != nil {
		t.Fatal(err)
	}

	// Field offsets, replayed from the format rather than hand-counted.
	nodeCountOff := snapshotHeaderLen
	kindCountOff := snapshotFixedHeaderLen(t, s)
	firstKindNameLenOff := kindCountOff + 8 + 2 // count, then the first entry's int16 id
	firstKindName, _ := s.Kinds.Name(1)         // the fixture's lowest registered kind id
	propNameCountOff := kindCountOff + kindTableWireLen(t, s.Kinds)
	entryCountOff := propNameCountOff + 8
	for _, name := range s.Props.names {
		entryCountOff += 4 + len(name)
	}
	arenaLenOff := entryCountOff + 8 + len(s.Props.entries)*propEntryWireSize + (s.NodeCount()+1)*4

	for _, tc := range []struct {
		name  string
		patch func(t *testing.T, data []byte)
	}{
		{"node count", func(t *testing.T, data []byte) {
			overwriteFileU64(t, data, nodeCountOff, uint64(s.NodeCount()), 1<<21)
		}},
		{"edge count", func(t *testing.T, data []byte) {
			overwriteFileU64(t, data, nodeCountOff+8, uint64(s.EdgeCount()), 1<<22)
		}},
		{"node kinds length", func(t *testing.T, data []byte) {
			overwriteFileU64(t, data, nodeCountOff+16, uint64(len(s.NodeKinds)), 1<<23)
		}},
		{"kind name length", func(t *testing.T, data []byte) {
			overwriteFileU32(t, data, firstKindNameLenOff, uint32(len(firstKindName)), 1<<25)
		}},
		{"property name length", func(t *testing.T, data []byte) {
			overwriteFileU32(t, data, propNameCountOff+8, uint32(len(s.Props.names[0])), 1<<25)
		}},
		{"property entry count", func(t *testing.T, data []byte) {
			overwriteFileU64(t, data, entryCountOff, uint64(len(s.Props.entries)), 1<<20)
		}},
		{"arena length", func(t *testing.T, data []byte) {
			overwriteFileU64(t, data, arenaLenOff, uint64(len(s.Props.arena)), 1<<25)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := append([]byte(nil), valid...)
			tc.patch(t, data)
			path := filepath.Join(dir, "patched.bin")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			_, _, err := ReadSnapshotFile(path)
			runtime.ReadMemStats(&after)

			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("ReadSnapshotFile = %v, want ErrCorrupt", err)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("%d-byte file refused after allocating %d bytes: %v", len(data), allocated, err)
			if allocated > maxAllocRefusingCorruptFile {
				t.Fatalf("refusing a %d-byte file allocated %d bytes, want at most %d: the corrupt field was allocated for before it was checked against the bytes the file holds", len(data), allocated, maxAllocRefusingCorruptFile)
			}
		})
	}
}
