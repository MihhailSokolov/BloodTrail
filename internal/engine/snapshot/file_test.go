// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// buildFileFixture builds a Snapshot exercising every corner of the wire
// format WriteSnapshotFile/ReadSnapshotFile round-trip: a property bag
// carrying every JSON value kind a propEntry can hold (string, number,
// bool, null, array, object), a duplicate objectid pair (nodes 30 and 40
// both carry "DUP"), a kind-less node (50), multiple registered kinds, and
// two dangling edges that Build drops.
func buildFileFixture(t *testing.T) *Snapshot {
	t.Helper()

	b := NewBuilder(7)
	b.SetKinds(map[KindID]string{1: "User", 2: "Group", 3: "Computer"})

	mustAddNodeJSON(t, b, 10, []KindID{1},
		`{"objectid":"S-1-A","name":"alice","enabled":true,"lastlogon":1725500000,"spns":["a/b","c/d"],"weird":null,"nested":{"a":1}}`)
	mustAddNodeJSON(t, b, 20, []KindID{1, 2}, `{"objectid":"S-1-B","name":"bob","enabled":false}`)
	mustAddNodeJSON(t, b, 30, []KindID{2}, `{"objectid":"DUP","name":"carol"}`)
	mustAddNodeJSON(t, b, 40, []KindID{3}, `{"objectid":"DUP","name":"dave"}`)
	mustAddNodeJSON(t, b, 50, nil, `{}`) // kind-less node

	b.AddEdge(1, 10, 20, 5)
	b.AddEdge(2, 20, 30, 6)
	b.AddEdge(3, 30, 40, 5)
	b.AddEdge(4, 10, 999, 5) // dangling end -> dropped
	b.AddEdge(5, 999, 40, 5) // dangling start -> dropped

	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if s.DroppedEdges != 2 {
		t.Fatalf("fixture DroppedEdges = %d, want 2 (fixture assumption broken)", s.DroppedEdges)
	}
	return s
}

// assertSnapshotsEqual checks that got (typically just reloaded via
// ReadSnapshotFile) is indistinguishable from want (the Snapshot that was
// written) via every public accessor: the scalar fields the wire format
// carries directly, the kind table, EdgeByID, ApproxBytes, and -- via
// compareViewContents, the fold suite's own full-content comparator -- every
// node's aliveness, kinds, and property bag, plus objectid resolution
// (including the duplicate-objectid set).
func assertSnapshotsEqual(t *testing.T, want, got *Snapshot) {
	t.Helper()

	if got.GraphID != want.GraphID {
		t.Fatalf("GraphID = %d, want %d", got.GraphID, want.GraphID)
	}
	if got.NodeCount() != want.NodeCount() {
		t.Fatalf("NodeCount() = %d, want %d", got.NodeCount(), want.NodeCount())
	}
	if got.EdgeCount() != want.EdgeCount() {
		t.Fatalf("EdgeCount() = %d, want %d", got.EdgeCount(), want.EdgeCount())
	}
	if got.MaxKindID != want.MaxKindID {
		t.Fatalf("MaxKindID = %d, want %d", got.MaxKindID, want.MaxKindID)
	}
	if got.DroppedEdges != want.DroppedEdges {
		t.Fatalf("DroppedEdges = %d, want %d", got.DroppedEdges, want.DroppedEdges)
	}
	if got.MultiGraph != want.MultiGraph {
		t.Fatalf("MultiGraph = %v, want %v", got.MultiGraph, want.MultiGraph)
	}

	if got.Kinds.Len() != want.Kinds.Len() {
		t.Fatalf("Kinds.Len() = %d, want %d", got.Kinds.Len(), want.Kinds.Len())
	}
	for k := KindID(0); k <= want.MaxKindID+1; k++ {
		wantName, wantOK := want.Kinds.Name(k)
		gotName, gotOK := got.Kinds.Name(k)
		if wantName != gotName || wantOK != gotOK {
			t.Fatalf("Kinds.Name(%d) = (%q, %v), want (%q, %v)", k, gotName, gotOK, wantName, wantOK)
		}
	}

	for id := uint64(0); id < 6; id++ { // every fixture edge id, plus one miss
		wantIdx, wantOK := want.EdgeByID(id)
		gotIdx, gotOK := got.EdgeByID(id)
		if wantOK != gotOK {
			t.Fatalf("EdgeByID(%d) ok = %v, want %v", id, gotOK, wantOK)
		}
		if !wantOK {
			continue
		}
		if want.OutTargets[wantIdx] != got.OutTargets[gotIdx] ||
			want.OutKinds[wantIdx] != got.OutKinds[gotIdx] ||
			want.OutEdgeIDs[wantIdx] != got.OutEdgeIDs[gotIdx] {
			t.Fatalf("EdgeByID(%d): edge details mismatch", id)
		}
	}

	if got.ApproxBytes() != want.ApproxBytes() {
		t.Fatalf("ApproxBytes() = %d, want %d", got.ApproxBytes(), want.ApproxBytes())
	}

	compareViewContents(t, NewView(want), NewView(got))
}

// TestSnapshotFileRoundTrip writes the fixture snapshot to a file and reads
// it back, checking every accessor agrees and the watermark survives --
// once with MultiGraph false and once true, since it's a plain bool field
// or'd in among everything else derived/copied.
func TestSnapshotFileRoundTrip(t *testing.T) {
	for _, multiGraph := range []bool{false, true} {
		t.Run(fmt.Sprintf("MultiGraph=%v", multiGraph), func(t *testing.T) {
			s := buildFileFixture(t)
			s.MultiGraph = multiGraph

			path := filepath.Join(t.TempDir(), "snap.bin")
			const watermark = uint64(424242)

			if err := WriteSnapshotFile(path, s, watermark); err != nil {
				t.Fatalf("WriteSnapshotFile: %v", err)
			}

			got, gotWatermark, err := ReadSnapshotFile(path)
			if err != nil {
				t.Fatalf("ReadSnapshotFile: %v", err)
			}
			if gotWatermark != watermark {
				t.Fatalf("watermark = %d, want %d", gotWatermark, watermark)
			}

			assertSnapshotsEqual(t, s, got)
		})
	}
}

// TestSnapshotFileRoundTripEmpty checks the degenerate zero-node,
// zero-edge, zero-kind snapshot a Builder that never staged anything
// produces -- every count is 0 and every N+1-length array is just [0], so
// this exercises the format's edge-of-range lengths.
func TestSnapshotFileRoundTripEmpty(t *testing.T) {
	s, err := NewBuilder(3).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	path := filepath.Join(t.TempDir(), "empty.bin")
	if err := WriteSnapshotFile(path, s, 0); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}

	got, watermark, err := ReadSnapshotFile(path)
	if err != nil {
		t.Fatalf("ReadSnapshotFile: %v", err)
	}
	if watermark != 0 {
		t.Fatalf("watermark = %d, want 0", watermark)
	}
	assertSnapshotsEqual(t, s, got)
}

// TestSnapshotFileWrongMagic checks that a file whose first bytes are not
// snapshotMagic is rejected as ErrNotSnapshot, distinctly from a corrupt or
// wrong-version one.
func TestSnapshotFileWrongMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.bin")
	if err := os.WriteFile(path, []byte("NOT-A-BLOODTRAIL-SNAPSHOT"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err := ReadSnapshotFile(path)
	if !errors.Is(err, ErrNotSnapshot) {
		t.Fatalf("ReadSnapshotFile(wrong magic) = %v, want ErrNotSnapshot", err)
	}
}

// TestSnapshotFileWrongVersion writes a valid file, then patches the
// version field (right after the magic) to a value ReadSnapshotFile has
// never produced, checking it is rejected as ErrVersionMismatch -- caught
// before the CRC is even consulted, so no CRC fixup is needed here.
func TestSnapshotFileWrongVersion(t *testing.T) {
	s := buildFileFixture(t)
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, 1); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	versionOff := len(snapshotMagic)
	binary.LittleEndian.PutUint32(data[versionOff:versionOff+4], snapshotFormatVersion+1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err = ReadSnapshotFile(path)
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("ReadSnapshotFile(wrong version) = %v, want ErrVersionMismatch", err)
	}
}

// TestSnapshotFileCorruptByte flips one byte well past the header of an
// otherwise-valid file and checks it is rejected as ErrCorrupt -- via a
// CRC32 mismatch in the common case, since the flipped byte almost
// certainly lands inside a packed array or the property arena rather than
// a length field.
func TestSnapshotFileCorruptByte(t *testing.T) {
	s := buildFileFixture(t)
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, 1); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mid := len(data) / 2
	if mid <= len(snapshotMagic) || mid >= len(data)-4 {
		t.Fatalf("fixture file too small (%d bytes) to corrupt a byte strictly mid-file", len(data))
	}
	data[mid] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err = ReadSnapshotFile(path)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadSnapshotFile(corrupt byte) = %v, want ErrCorrupt", err)
	}
}

// TestSnapshotFileTruncated checks that a file cut off partway through
// (payload present but incomplete, no CRC trailer at all) is rejected --
// generically, per the brief; a truncated body surfaces as ErrCorrupt in
// this implementation (a short read partway through the packed arrays),
// but the assertion only requires an error, not that specific sentinel.
func TestSnapshotFileTruncated(t *testing.T) {
	s := buildFileFixture(t)
	path := filepath.Join(t.TempDir(), "snap.bin")
	if err := WriteSnapshotFile(path, s, 1); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	truncated := data[:len(data)/2]
	if err := os.WriteFile(path, truncated, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, _, err := ReadSnapshotFile(path); err == nil {
		t.Fatal("ReadSnapshotFile(truncated file): want error, got nil")
	}
}

// TestSnapshotFileAtomicWriteLeavesNoTempFile checks WriteSnapshotFile's
// atomicity contract at the filesystem level: after a successful write, the
// target directory contains exactly the final file -- no leftover
// ".snapshot-*.tmp" from the temp-file-then-rename sequence.
func TestSnapshotFileAtomicWriteLeavesNoTempFile(t *testing.T) {
	s := buildFileFixture(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.bin")

	if err := WriteSnapshotFile(path, s, 1); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("directory contents = %v, want exactly [%q]", names, filepath.Base(path))
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("snapshot file mode = %o, want 0600", perm)
	}
}
