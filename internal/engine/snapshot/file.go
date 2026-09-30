// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// snapshotMagic opens every snapshot file, letting ReadSnapshotFile reject a
// file that isn't one of ours before it tries to interpret anything else in
// it as this format's header.
var snapshotMagic = []byte("BTSNAP\x00")

// snapshotFormatVersion is the only format version WriteSnapshotFile
// produces and ReadSnapshotFile accepts today. A future incompatible format
// change bumps this and ReadSnapshotFile rejects anything else via
// ErrVersionMismatch.
//
// Version 2 added the watermark lineage (Snapshot.WatermarkLineage). A
// version 1 file records no lineage, so nothing can prove it belongs to the
// lineage PostgreSQL is in now; refusing it as a version mismatch is exactly
// right, and costs an upgraded deployment one ordinary rebuild.
const snapshotFormatVersion uint32 = 2

// Sentinel errors ReadSnapshotFile wraps its failures in, so a caller (the
// engine boot path) can tell "this file was never a snapshot" apart from
// "wrong version" apart from "looked like one but its bytes don't check
// out" via errors.Is, while still getting a specific, loggable message from
// Error().
var (
	// ErrNotSnapshot means the file's first bytes are not snapshotMagic --
	// either a completely unrelated file, or one too short to even hold the
	// magic.
	ErrNotSnapshot = errors.New("snapshot: not a snapshot file (bad magic)")

	// ErrVersionMismatch means the magic matched but the format version
	// that follows it is not one this build of ReadSnapshotFile understands.
	ErrVersionMismatch = errors.New("snapshot: unsupported snapshot format version")

	// ErrCorrupt means the file passed the magic/version checks but its
	// payload could not be reconstituted -- a short/truncated read, a
	// nonsensical length that would need an absurd allocation, or (most
	// commonly) a CRC32 mismatch against the trailer written by
	// WriteSnapshotFile.
	ErrCorrupt = errors.New("snapshot: corrupt snapshot file")
)

// Stamp is what a snapshot file records about PostgreSQL at the moment it
// was written, as distinct from the graph it carries: the watermark counter
// the file is complete for, and where PostgreSQL's node and edge id
// sequences stood. The engine's boot weighs both -- the sequences against
// where they stood when its own process started, to catch rows inserted in
// between by a writer the counter cannot see.
type Stamp struct {
	Watermark uint64

	// NodeIDSeq and EdgeIDSeq are the last values the node and edge id
	// sequences handed out, 0 for a sequence never used.
	NodeIDSeq, EdgeIDSeq int64
}

// Header is the fixed-size start of a snapshot file: the graph it holds,
// the watermark lineage its contents were read in, and its Stamp --
// everything a boot weighs before the graph itself. See
// ReadSnapshotFileHeader.
type Header struct {
	GraphID int32
	Lineage Lineage
	Stamp   Stamp
}

// WriteSnapshotFile atomically writes s and stamp to path in this
// package's versioned binary format (see the format comment above
// ReadSnapshotFile).
//
// "Atomically" means: the file at path either doesn't change at all, or
// ends up as a complete, valid snapshot -- never a partially-written one, a
// crash or an error partway through can't leave path in a half-written
// state. This is achieved the standard way: write to a fresh temp file in
// the SAME directory as path (so the final rename is same-filesystem and
// therefore atomic), fsync it, close it, os.Rename it over path, then fsync
// the directory, so the rename itself survives a power loss -- the file's
// own fsync covers its bytes, not the directory entry naming them. Any
// error before the rename removes the temp file rather than leaving it
// behind. An error syncing the directory comes after the rename: the file
// is complete and in place, only its durability is unknown, and a caller
// that must not leave a file behind has to treat that error accordingly.
//
// The temp file (and therefore the final file) is created with mode 0600 --
// os.CreateTemp's default -- since a snapshot's payload is a full copy of
// the graph's data, including every node's property bag.
func WriteSnapshotFile(path string, s *Snapshot, stamp Stamp) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: create temp file: %w", err)
	}
	tmpPath := tmp.Name()

	// success is flipped to true only once the rename below has actually
	// happened; this defer's job is purely cleanup on any earlier failure
	// (an already-closed tmp double-Closes harmlessly, and the temp file no
	// longer exists post-rename so the Remove is a harmless no-op then too
	// -- but success being true skips both).
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriterSize(tmp, 1<<20)
	if _, err := bw.Write(snapshotMagic); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: write magic: %w", err)
	}

	// Everything from here on is written through mw, which fans each write
	// out to both the file (via bw) and the running CRC32 -- one pass over
	// the data, not two. The CRC trailer itself is written straight to bw
	// afterward, deliberately bypassing mw, so it is not included in its
	// own checksum.
	h := crc32.NewIEEE()
	mw := io.MultiWriter(bw, h)
	if err := writeSnapshotBody(mw, s, stamp); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: %w", err)
	}

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], h.Sum32())
	if _, err := bw.Write(crcBuf[:]); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: write crc: %w", err)
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: flush: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: rename: %w", err)
	}

	success = true
	if err := syncParentDir(dir); err != nil {
		return fmt.Errorf("snapshot: WriteSnapshotFile: %w", err)
	}
	return nil
}

// RemoveSnapshotFile removes the snapshot file at path and then syncs its
// directory, so the removal survives a power loss: an unlink still only in
// the page cache when the power goes brings the file back at the next boot,
// and a file is removed precisely when it must not come back (the engine's
// invalidation after a watermark bump failed). removed reports whether a
// file was there to remove. A missing file is no error, and the directory is
// synced all the same -- an earlier removal's own unlink may not be durable
// yet -- unless the directory is missing too, when there is nothing a power
// loss could bring back. An error syncing comes after the unlink: the file is
// gone, only the durability of that is unknown.
func RemoveSnapshotFile(path string) (removed bool, err error) {
	switch err := os.Remove(path); {
	case err == nil:
		removed = true
	case errors.Is(err, fs.ErrNotExist):
	default:
		return false, fmt.Errorf("snapshot: RemoveSnapshotFile: %w", err)
	}
	if err := syncParentDir(filepath.Dir(path)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return removed, fmt.Errorf("snapshot: RemoveSnapshotFile: %w", err)
	}
	return removed, nil
}

// syncParentDir is the directory sync WriteSnapshotFile and
// RemoveSnapshotFile run after they change a directory entry: syncDir, held
// in a variable only so a test can observe the calls and make one fail.
var syncParentDir = syncDir

// syncDir flushes dir's own entries -- the names in it, as renames and
// unlinks left them -- to stable storage. A platform or filesystem that
// cannot sync a directory at all (dirSyncUnsupported; Windows, where a
// directory cannot be opened for it) counts as synced: there is no stronger
// guarantee to be had there, and failing every save over it would only lose
// the file.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil && !dirSyncUnsupported(syncErr) {
		return fmt.Errorf("sync directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("sync directory: close: %w", closeErr)
	}
	return nil
}

// dirSyncUnsupported reports whether err, from syncing a directory, means
// the filesystem does not sync directories at all rather than that the sync
// failed.
func dirSyncUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP)
}

// writeSnapshotBody writes every field of the format after the magic:
// version, graph id, watermark, watermark lineage, the id sequence
// positions, node/edge/kind counts, the packed CSR arrays, the kind table,
// the PropStore, and finally MultiGraph and DroppedEdges. See
// ReadSnapshotFile's doc comment for the exact field-by-field layout this
// must match.
func writeSnapshotBody(w io.Writer, s *Snapshot, stamp Stamp) error {
	bw := &binWriter{w: w}

	bw.u32(snapshotFormatVersion)
	bw.i32(s.GraphID)
	bw.u64(stamp.Watermark)
	bw.bytes(s.WatermarkLineage[:])
	bw.u64(uint64(stamp.NodeIDSeq))
	bw.u64(uint64(stamp.EdgeIDSeq))

	bw.u64(uint64(s.NodeCount()))
	bw.u64(uint64(s.EdgeCount()))
	bw.u64(uint64(len(s.NodeKinds)))

	bw.u64s(s.GraphIDs)
	bw.u64s(s.OutOffsets)
	bw.u32s(s.OutTargets)
	bw.i16s(s.OutKinds)
	bw.u64s(s.OutEdgeIDs)
	bw.u64s(s.InOffsets)
	bw.u32s(s.InTargets)
	bw.i16s(s.InKinds)
	bw.u32s(s.InEdgeIdx)
	bw.u32s(s.KindOffsets)
	bw.i16s(s.NodeKinds)
	bw.u32s(s.edgeIDPerm)

	writeKindTable(bw, s.Kinds)
	writePropStore(bw, s.Props)

	if s.MultiGraph {
		bw.u8(1)
	} else {
		bw.u8(0)
	}
	bw.u64(uint64(s.DroppedEdges))

	return bw.err
}

// writeKindTable writes kt as: entry count (uint64), then for each
// registered (non-empty-name) entry, its id (int16) followed by its
// length-prefixed name.
func writeKindTable(bw *binWriter, kt *KindTable) {
	count := 0
	for _, name := range kt.names {
		if name != "" {
			count++
		}
	}
	bw.u64(uint64(count))
	for id, name := range kt.names {
		if name == "" {
			continue
		}
		bw.i16(int16(id))
		bw.str(name)
	}
}

// propEntryWireSize is the number of bytes writePropStore/readPropStore
// actually put on the wire for one propEntry: prop uint16 (2) + kind uint8
// (1) + num float64 bits as uint64 (8) + ref uint32 (4) + len uint32 (4) =
// 19. It is deliberately its own named constant, distinct from
// bytesPerPropEntry (props.go) -- propEntry's padded IN-MEMORY size (24
// bytes, via unsafe.Sizeof, used for heap-footprint accounting in
// ApproxBytes) -- so the two can never be silently conflated: one describes
// what's on disk, the other what's resident in RAM once loaded, and a
// Go-compiler layout change to propEntry must not be read as a wire format
// change (see the format-version comment on ReadSnapshotFile).
const propEntryWireSize = 19

// writePropStore writes p's packed fields in order: names (length-prefixed
// each, preceded by a count), entries (a fixed propEntryWireSize-byte wire
// layout per entry -- prop uint16, kind uint8, num float64 bits as uint64,
// ref uint32, len uint32 -- deliberately NOT propEntry's raw in-memory
// bytes, since unsafe.Sizeof padding is a Go-compiler implementation
// detail, not a wire format), nodeOffsets (raw uint32s, N+1 of them, N
// recoverable from the header so no separate count is written), and
// finally the arena (length-prefixed raw bytes). p.ids and
// p.objectIndex/objectIndexDup are derived, not written -- see
// finalizePropStore.
func writePropStore(bw *binWriter, p *PropStore) {
	bw.u64(uint64(len(p.names)))
	for _, name := range p.names {
		bw.str(name)
	}

	bw.u64(uint64(len(p.entries)))
	for _, e := range p.entries {
		bw.u16(uint16(e.prop))
		bw.u8(e.kind)
		bw.f64(e.num)
		bw.u32(e.ref)
		bw.u32(e.len)
	}

	bw.u32s(p.nodeOffsets)

	bw.u64(uint64(len(p.arena)))
	bw.bytes(p.arena)
}

// ReadSnapshotFile reads a snapshot file WriteSnapshotFile produced, in
// full, before returning anything -- reconstituting every packed array,
// verifying the trailing CRC32 against everything read, and only then
// re-deriving idIndex/kindBitmaps/MaxKindID/Props.ids/Props.objectIndex(Dup)
// via finalizeDerived (the same derivation Builder.Build uses). A caller
// never observes a partially-verified snapshot: on any error the returned
// *Snapshot is nil.
//
// Format v2, every multi-byte integer little-endian:
//
//	magic          [7]byte  "BTSNAP\x00"                    -- NOT covered by the trailing CRC
//	version        uint32   snapshotFormatVersion (2)
//	graphID        int32    Snapshot.GraphID
//	watermark      uint64   Stamp.Watermark
//	lineage        [16]byte Snapshot.WatermarkLineage
//	nodeIDSeq      int64    Stamp.NodeIDSeq
//	edgeIDSeq      int64    Stamp.EdgeIDSeq
//	nodeCount (N)  uint64   len(GraphIDs)
//	edgeCount (E)  uint64   len(OutTargets) (== len(InTargets))
//	nodeKindsLen   uint64   len(NodeKinds)
//	GraphIDs       [N]uint64
//	OutOffsets     [N+1]uint64
//	OutTargets     [E]uint32   (NodeID)
//	OutKinds       [E]int16    (KindID)
//	OutEdgeIDs     [E]uint64
//	InOffsets      [N+1]uint64
//	InTargets      [E]uint32
//	InKinds        [E]int16
//	InEdgeIdx      [E]uint32
//	KindOffsets    [N+1]uint32
//	NodeKinds      [nodeKindsLen]int16
//	edgeIDPerm     [E]uint32
//	kindCount      uint64
//	kindCount x { id int16; nameLen uint32; name [nameLen]byte }
//	propNameCount  uint64
//	propNameCount x { nameLen uint32; name [nameLen]byte }
//	entryCount     uint64
//	entryCount x { prop uint16; kind uint8; num uint64 (float64 bits); ref uint32; len uint32 }
//	nodeOffsets    [N+1]uint32
//	arenaLen       uint64
//	arena          [arenaLen]byte
//	multiGraph     uint8    0 or 1
//	droppedEdges   uint64
//	crc32          uint32   IEEE CRC32 of every byte after the magic, up to (not including) this field
//
// Everything from version through edgeIDSeq is the file's Header, which
// ReadSnapshotFileHeader reads on its own.
//
// ReadSnapshotFile never panics on a corrupt file: a nonsensical count
// field (e.g. one big enough to demand an implausible allocation) is caught
// and turned into ErrCorrupt rather than crashing the process.
func ReadSnapshotFile(path string) (snap *Snapshot, stamp Stamp, err error) {
	defer func() {
		if r := recover(); r != nil {
			snap = nil
			stamp = Stamp{}
			err = fmt.Errorf("snapshot: ReadSnapshotFile: %w: panic: %v", ErrCorrupt, r)
		}
	}()

	f, openErr := os.Open(path)
	if openErr != nil {
		return nil, Stamp{}, fmt.Errorf("snapshot: ReadSnapshotFile: open: %w", openErr)
	}
	defer func() { _ = f.Close() }()

	raw := bufio.NewReaderSize(f, 1<<20)
	if err := readMagic(raw); err != nil {
		return nil, Stamp{}, fmt.Errorf("snapshot: ReadSnapshotFile: %w", err)
	}

	// Everything from here on is read through tr, which feeds each byte to
	// h as it's consumed -- one pass, matching WriteSnapshotFile's mw. The
	// trailing CRC field itself is read straight from raw afterward,
	// deliberately bypassing tr, so it is not part of what it's checked
	// against.
	h := crc32.NewIEEE()
	tr := io.TeeReader(raw, h)
	br := &binReader{r: tr}

	header, err := readHeader(br)
	if err != nil {
		return nil, Stamp{}, fmt.Errorf("snapshot: ReadSnapshotFile: %w", err)
	}
	n := br.u64()
	e := br.u64()
	nodeKindsLen := br.u64()

	graphIDs := br.u64s(n)
	outOffsets := br.u64s(n + 1)
	outTargets := br.u32s(e)
	outKinds := br.i16s(e)
	outEdgeIDs := br.u64s(e)
	inOffsets := br.u64s(n + 1)
	inTargets := br.u32s(e)
	inKinds := br.i16s(e)
	inEdgeIdx := br.u32s(e)
	kindOffsets := br.u32s(n + 1)
	nodeKinds := br.i16s(nodeKindsLen)
	edgeIDPerm := br.u32s(e)

	kinds := readKindTable(br)
	props := readPropStore(br, n)

	multiGraph := br.u8() != 0
	droppedEdges := br.u64()

	if br.err != nil {
		return nil, Stamp{}, fmt.Errorf("snapshot: ReadSnapshotFile: %w", wrapCorrupt(br.err))
	}

	var storedCRC [4]byte
	if _, readErr := io.ReadFull(raw, storedCRC[:]); readErr != nil {
		return nil, Stamp{}, fmt.Errorf("snapshot: ReadSnapshotFile: read crc: %w", wrapCorrupt(readErr))
	}
	if binary.LittleEndian.Uint32(storedCRC[:]) != h.Sum32() {
		return nil, Stamp{}, fmt.Errorf("snapshot: ReadSnapshotFile: %w", ErrCorrupt)
	}

	s := &Snapshot{
		GraphID:      header.GraphID,
		GraphIDs:     graphIDs,
		OutOffsets:   outOffsets,
		OutTargets:   outTargets,
		OutKinds:     outKinds,
		OutEdgeIDs:   outEdgeIDs,
		InOffsets:    inOffsets,
		InTargets:    inTargets,
		InKinds:      inKinds,
		InEdgeIdx:    inEdgeIdx,
		KindOffsets:  kindOffsets,
		NodeKinds:    nodeKinds,
		Kinds:        kinds,
		Props:        props,
		DroppedEdges: int(droppedEdges),
		// BuiltAt intentionally is NOT part of the wire format: it's a
		// bookkeeping timestamp for when this in-memory Snapshot struct was
		// constructed, not part of the graph data itself, so reloading from
		// disk gets a fresh one, exactly as a fresh Build would.
		BuiltAt:          time.Now(),
		MultiGraph:       multiGraph,
		WatermarkLineage: header.Lineage,
		edgeIDPerm:       edgeIDPerm,
	}
	// The CRC above proves these are the bytes that were written, not that
	// they describe a graph. finalizeDerived and every later reader index
	// straight into these arrays -- one of them through unsafe.String --
	// so the structure is checked before anything touches it.
	if validateErr := validateSnapshotStructure(s); validateErr != nil {
		return nil, Stamp{}, fmt.Errorf("snapshot: ReadSnapshotFile: %w", validateErr)
	}
	finalizeDerived(s)

	return s, header.Stamp, nil
}

// ReadSnapshotFileHeader reads only a snapshot file's Header -- after
// checking its magic and format version -- without reading the graph that
// follows. Nothing it returns is verified: the CRC32 that vouches for these
// bytes covers the whole file, so a header read is good for refusing a file
// early, never for trusting one. The engine's boot uses it to turn away a
// file its header already rules out before paying to read what can be
// gigabytes of graph, and weighs ReadSnapshotFile's verified result, not
// this, before it adopts anything.
//
// Errors wrap the same sentinels ReadSnapshotFile's do: a missing file is
// fs.ErrNotExist, a foreign one ErrNotSnapshot, another format version
// ErrVersionMismatch, and one too short to hold a header ErrCorrupt.
func ReadSnapshotFileHeader(path string) (Header, error) {
	f, err := os.Open(path)
	if err != nil {
		return Header{}, fmt.Errorf("snapshot: ReadSnapshotFileHeader: open: %w", err)
	}
	defer func() { _ = f.Close() }()

	raw := bufio.NewReader(f)
	if err := readMagic(raw); err != nil {
		return Header{}, fmt.Errorf("snapshot: ReadSnapshotFileHeader: %w", err)
	}
	header, err := readHeader(&binReader{r: raw})
	if err != nil {
		return Header{}, fmt.Errorf("snapshot: ReadSnapshotFileHeader: %w", err)
	}
	return header, nil
}

// readMagic consumes the magic that opens every snapshot file, failing with
// ErrNotSnapshot when r does not start with it.
func readMagic(r io.Reader) error {
	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return fmt.Errorf("read magic: %w", ErrNotSnapshot)
	}
	if !bytes.Equal(magic, snapshotMagic) {
		return ErrNotSnapshot
	}
	return nil
}

// readHeader reads the format version and then the Header's fields, in the
// order writeSnapshotBody writes them -- ReadSnapshotFile's and
// ReadSnapshotFileHeader's one copy of that order.
func readHeader(br *binReader) (Header, error) {
	version := br.u32()
	if br.err != nil {
		return Header{}, fmt.Errorf("read version: %w", wrapCorrupt(br.err))
	}
	if version != snapshotFormatVersion {
		return Header{}, fmt.Errorf("version %d: %w", version, ErrVersionMismatch)
	}

	var header Header
	header.GraphID = br.i32()
	header.Stamp.Watermark = br.u64()
	copy(header.Lineage[:], br.bytes(uint64(len(header.Lineage))))
	header.Stamp.NodeIDSeq = int64(br.u64())
	header.Stamp.EdgeIDSeq = int64(br.u64())
	if br.err != nil {
		return Header{}, fmt.Errorf("read header: %w", wrapCorrupt(br.err))
	}
	return header, nil
}

// maxKindTableEntries bounds readKindTable's entry count by KindID's own
// domain -- KindID is an int16 (bitset.go), so at most math.MaxUint16+1
// distinct values can ever exist -- rather than by the generic
// maxReadAlloc byte budget. maxReadAlloc alone is the wrong guard here: it
// compares an entry COUNT against a BYTE budget, so a corrupt count
// anywhere below maxReadAlloc (e.g. a few hundred million, still far under
// 1<<40) sails past that check and drives make(map[KindID]string, count)
// to attempt a preallocation sized for that many buckets -- an
// unrecoverable OOM well before io.ReadFull, let alone the trailing CRC32,
// ever gets a chance to reject the file.
const maxKindTableEntries = uint64(math.MaxUint16) + 1

// readKindTable is writeKindTable's mirror: reads the entry count, then
// that many (id, name) pairs, and builds them into a KindTable via
// NewKindTable exactly as Builder.Build does from Builder.SetKinds.
func readKindTable(br *binReader) *KindTable {
	count := br.u64()
	if br.err != nil {
		return nil
	}
	if count > maxKindTableEntries {
		br.err = fmt.Errorf("snapshot: kind table entry count %d exceeds KindID's range (max %d)", count, maxKindTableEntries)
		return nil
	}
	pairs := make(map[KindID]string, count)
	for i := uint64(0); i < count; i++ {
		id := br.i16()
		name := br.str()
		if br.err != nil {
			return nil
		}
		pairs[id] = name
	}
	return NewKindTable(pairs)
}

// readPropStore is writePropStore's mirror: reads names, entries, and
// nodeOffsets (N+1 of them, N supplied by the caller since the format
// doesn't repeat it), and arena, leaving ids/objectIndex/objectIndexDup for
// finalizePropStore (via finalizeDerived) to fill in.
func readPropStore(br *binReader, n uint64) *PropStore {
	nameCount := br.u64()
	if br.err != nil {
		return nil
	}
	// Bound nameCount by PropID's own domain (props.go's maxPropID, the
	// package constant internProp already enforces distinct-name-count
	// against) rather than the generic maxReadAlloc byte budget -- the same
	// "count vs. byte budget" gap maxKindTableEntries closes above: a
	// corrupt nameCount under maxReadAlloc but over maxPropID+1 would
	// otherwise reach make([]string, nameCount) and attempt an
	// unrecoverable OOM allocation before the CRC32 trailer is checked.
	if nameCount > uint64(maxPropID)+1 {
		br.err = fmt.Errorf("snapshot: property name count %d exceeds PropID's range (max %d)", nameCount, uint64(maxPropID)+1)
		return nil
	}
	names := make([]string, nameCount)
	for i := range names {
		names[i] = br.str()
	}

	entryCount := br.u64()
	if br.err != nil {
		return nil
	}
	// The cap below divides maxReadAlloc by bytesPerPropEntry (props.go),
	// propEntry's padded IN-MEMORY size (24 bytes) -- not by
	// propEntryWireSize (19 bytes), the number of bytes this loop actually
	// reads per entry off the wire. That's deliberate, not an oversight:
	// entryCount drives make([]propEntry, entryCount), a heap allocation
	// sized by the LARGER in-memory layout, so bytesPerPropEntry is the
	// divisor that actually bounds that allocation's byte size. Dividing by
	// the smaller propEntryWireSize instead would LOOSEN this cap -- it
	// would permit more entries for the same maxReadAlloc budget than the
	// resulting []propEntry could safely occupy. Keep the two sizes named
	// separately so a future change to either doesn't quietly conflate
	// "how big this is on the wire" with "how big this is once loaded".
	if entryCount > maxReadAlloc/bytesPerPropEntry {
		br.err = fmt.Errorf("snapshot: refusing to allocate %d prop entries", entryCount)
		return nil
	}
	entries := make([]propEntry, entryCount)
	for i := range entries {
		prop := br.u16()
		kind := br.u8()
		num := br.f64()
		ref := br.u32()
		length := br.u32()
		entries[i] = propEntry{prop: PropID(prop), kind: kind, num: num, ref: ref, len: length}
	}

	nodeOffsets := br.u32s(n + 1)
	arenaLen := br.u64()
	arena := br.bytes(arenaLen)

	if br.err != nil {
		return nil
	}
	return &PropStore{
		names:       names,
		entries:     entries,
		nodeOffsets: nodeOffsets,
		arena:       arena,
	}
}

// wrapCorrupt wraps a low-level read error (typically io.ErrUnexpectedEOF
// or io.EOF from a truncated file) as ErrCorrupt, so every ReadSnapshotFile
// failure past the magic/version checks is errors.Is-able as ErrCorrupt
// while still naming the underlying cause in Error().
func wrapCorrupt(err error) error {
	return fmt.Errorf("%w: %v", ErrCorrupt, err)
}

// binWriter is a tiny sticky-error binary encoder: every method is a no-op
// once bw.err is set, so writeSnapshotBody/writeKindTable/writePropStore
// read as a flat sequence of field writes with one error check at the end,
// rather than an if-err-return-err chain after every single field.
type binWriter struct {
	w   io.Writer
	err error
	buf [8]byte
}

func (bw *binWriter) write(p []byte) {
	if bw.err != nil {
		return
	}
	if _, err := bw.w.Write(p); err != nil {
		bw.err = err
	}
}

func (bw *binWriter) u8(v uint8) { bw.write([]byte{v}) }

func (bw *binWriter) u16(v uint16) {
	binary.LittleEndian.PutUint16(bw.buf[:2], v)
	bw.write(bw.buf[:2])
}

func (bw *binWriter) i16(v int16) { bw.u16(uint16(v)) }

func (bw *binWriter) u32(v uint32) {
	binary.LittleEndian.PutUint32(bw.buf[:4], v)
	bw.write(bw.buf[:4])
}

func (bw *binWriter) i32(v int32) { bw.u32(uint32(v)) }

func (bw *binWriter) u64(v uint64) {
	binary.LittleEndian.PutUint64(bw.buf[:8], v)
	bw.write(bw.buf[:8])
}

func (bw *binWriter) f64(v float64) { bw.u64(math.Float64bits(v)) }

func (bw *binWriter) bytes(p []byte) { bw.write(p) }

// str writes a length-prefixed string: a uint32 byte length, then the raw
// bytes.
func (bw *binWriter) str(s string) {
	bw.u32(uint32(len(s)))
	bw.write([]byte(s))
}

// u64s/u32s/i16s each encode the whole slice into one scratch byte buffer,
// then issue a single Write call -- one syscall-adjacent call per array
// instead of one per element, which matters at millions-of-elements scale.
// This is the "single io.ReadFull per array" idea applied to the write
// side.
func (bw *binWriter) u64s(s []uint64) {
	if bw.err != nil || len(s) == 0 {
		return
	}
	buf := make([]byte, len(s)*8)
	for i, v := range s {
		binary.LittleEndian.PutUint64(buf[i*8:], v)
	}
	bw.write(buf)
}

func (bw *binWriter) u32s(s []uint32) {
	if bw.err != nil || len(s) == 0 {
		return
	}
	buf := make([]byte, len(s)*4)
	for i, v := range s {
		binary.LittleEndian.PutUint32(buf[i*4:], v)
	}
	bw.write(buf)
}

func (bw *binWriter) i16s(s []int16) {
	if bw.err != nil || len(s) == 0 {
		return
	}
	buf := make([]byte, len(s)*2)
	for i, v := range s {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(v))
	}
	bw.write(buf)
}

// binReader is binWriter's mirror on the read side: every method is a
// no-op (returning the zero value) once br.err is set, so
// ReadSnapshotFile/readKindTable/readPropStore read as a flat sequence of
// field reads with the error checked only where it actually matters (before
// counts are trusted enough to drive further allocations, and once more at
// the very end). buf mirrors binWriter's own scratch field: it backs every
// fixed-size scalar read (see readScratch) so u8/u16/u32/u64 don't each
// heap-allocate -- at 5M-node scale a read touches tens of millions of
// these, so one make([]byte, n) per call was showing up as significant GC
// pressure.
type binReader struct {
	r   io.Reader
	err error
	buf [8]byte
}

// maxReadAlloc caps any single length-prefixed read this package will
// attempt to satisfy in one allocation. It exists solely so a corrupt count
// field (e.g. a byte flipped inside what should have been a small number)
// fails fast as ErrCorrupt instead of the process attempting a
// multi-exabyte allocation -- deliberately far above any real BloodHound
// snapshot's actual array sizes (5M nodes/tens of millions of edges is
// still only low gigabytes per array), not a tight bound.
const maxReadAlloc = 1 << 40

func (br *binReader) read(n int) []byte {
	if br.err != nil {
		return nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(br.r, buf); err != nil {
		br.err = err
		return nil
	}
	return buf
}

// readScratch reads exactly n (<= len(br.buf)) bytes into br's scratch
// buffer and returns a slice of it, instead of read's make([]byte, n) --
// used only by the fixed-size scalar helpers below (u8/u16/u32/u64). Each
// of those decodes the returned bytes into a typed value (a uint8/16/32/64,
// never the slice itself) and returns that value, so the caller never
// retains a reference into br.buf past the call -- the next readScratch
// (from the very next field read) is free to overwrite it. bytes/u64s/
// u32s/i16s -- the variable-length and bulk-array reads, which do need
// exact-size allocations that outlive the call -- deliberately keep using
// read/make instead.
func (br *binReader) readScratch(n int) []byte {
	if br.err != nil {
		return nil
	}
	buf := br.buf[:n]
	if _, err := io.ReadFull(br.r, buf); err != nil {
		br.err = err
		return nil
	}
	return buf
}

func (br *binReader) u8() uint8 {
	b := br.readScratch(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (br *binReader) u16() uint16 {
	b := br.readScratch(2)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

func (br *binReader) i16() int16 { return int16(br.u16()) }

func (br *binReader) u32() uint32 {
	b := br.readScratch(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

func (br *binReader) i32() int32 { return int32(br.u32()) }

func (br *binReader) u64() uint64 {
	b := br.readScratch(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (br *binReader) f64() float64 { return math.Float64frombits(br.u64()) }

// bytes reads n raw bytes, guarded by maxReadAlloc (see its doc).
func (br *binReader) bytes(n uint64) []byte {
	if br.err != nil {
		return nil
	}
	if n > maxReadAlloc {
		br.err = fmt.Errorf("snapshot: refusing to allocate %d bytes for one field", n)
		return nil
	}
	return br.read(int(n))
}

// str reads a length-prefixed string: a uint32 byte length, then that many
// raw bytes.
func (br *binReader) str() string {
	n := br.u32()
	b := br.bytes(uint64(n))
	if b == nil {
		return ""
	}
	return string(b)
}

// u64s/u32s/i16s each read the whole array in one io.ReadFull into a
// scratch byte buffer, then decode it in a tight loop -- the read-side
// mirror of binWriter's u64s/u32s/i16s, and the "single io.ReadFull per
// array into exact-size allocations" this package's array (de)serialization
// is built around.
func (br *binReader) u64s(n uint64) []uint64 {
	if br.err != nil {
		return nil
	}
	if n > maxReadAlloc/8 {
		br.err = fmt.Errorf("snapshot: refusing to allocate %d uint64s for one array", n)
		return nil
	}
	out := make([]uint64, n)
	if n == 0 {
		return out
	}
	buf := br.read(int(n) * 8)
	if buf == nil {
		return nil
	}
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(buf[i*8:])
	}
	return out
}

func (br *binReader) u32s(n uint64) []uint32 {
	if br.err != nil {
		return nil
	}
	if n > maxReadAlloc/4 {
		br.err = fmt.Errorf("snapshot: refusing to allocate %d uint32s for one array", n)
		return nil
	}
	out := make([]uint32, n)
	if n == 0 {
		return out
	}
	buf := br.read(int(n) * 4)
	if buf == nil {
		return nil
	}
	for i := range out {
		out[i] = binary.LittleEndian.Uint32(buf[i*4:])
	}
	return out
}

func (br *binReader) i16s(n uint64) []int16 {
	if br.err != nil {
		return nil
	}
	if n > maxReadAlloc/2 {
		br.err = fmt.Errorf("snapshot: refusing to allocate %d int16s for one array", n)
		return nil
	}
	out := make([]int16, n)
	if n == 0 {
		return out
	}
	buf := br.read(int(n) * 2)
	if buf == nil {
		return nil
	}
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(buf[i*2:]))
	}
	return out
}
