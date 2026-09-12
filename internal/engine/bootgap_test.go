// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"testing"

	"github.com/specterops/dawgs/graph"
)

// TestBootGapCoveredAt pins the trust predicate the snapshot-file boot rests
// on: the file may be adopted exactly when the buffered counters at or below
// the decision's FROZEN pg snapshot are precisely
// {fileWatermark+1, ..., pgSnapshot} -- the old equality check's empty gap
// included -- and never on a hole, a counter the file already contains, a
// duplicate anywhere, or a file somehow ahead of pg. Counters ABOVE the
// frozen snapshot are permitted (and their entries replayed): they belong to
// writes that landed after the target was frozen, which the settle-wait
// design explicitly tolerates -- observed ones ride the replay, parked ones
// land as ordinary deltas after publish (adoptSnapshotFileView's doc).
func TestBootGapCoveredAt(t *testing.T) {
	cases := []struct {
		name     string
		file, pg uint64
		counters []uint64
		want     bool
	}{
		{"empty gap, zero (first-ever boot)", 0, 0, nil, true},
		{"empty gap, nonzero (quiet restart)", 42, 42, nil, true},
		{"one buffered write covering the gap", 42, 43, []uint64{43}, true},
		{"several buffered writes, out of order", 42, 45, []uint64{45, 43, 44}, true},
		{"gap with nothing buffered (out-of-band write)", 41, 42, nil, false},
		{"hole in the middle", 42, 45, []uint64{43, 45}, false},
		{"counter below the gap (already in the file)", 42, 44, []uint64{42, 43}, false},
		{"counter above the frozen snapshot alone (write after freeze, gap empty)", 42, 42, []uint64{43}, true},
		{"gap covered plus counters above the freeze", 42, 44, []uint64{43, 44, 45, 46}, true},
		{"hole below the freeze is not excused by counters above it", 42, 45, []uint64{43, 45, 46}, false},
		{"duplicate below the freeze", 42, 44, []uint64{43, 43, 44}, false},
		{"duplicate above the freeze", 42, 43, []uint64{43, 44, 44}, false},
		{"file ahead of pg (should never happen; refused all the same)", 43, 42, nil, false},
		{"file ahead of pg with counters", 43, 42, []uint64{43}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := bootGapCoveredAt(tc.file, tc.pg, tc.counters); got != tc.want {
				t.Fatalf("bootGapCoveredAt(%d, %d, %v) = %v, want %v", tc.file, tc.pg, tc.counters, got, tc.want)
			}
		})
	}
}

// bumpedScope builds a WriteScope carrying a successful eager bump at
// counter; the caller records whatever changes the case needs on its
// ChangeSet.
func bumpedScope(counter uint64) *WriteScope {
	s := NewWriteScope()
	s.SetWatermark(counter)
	return s
}

// TestBootGapBufferObserveRecordsBumpedWrites pins the buffer's ordinary
// path: an armed buffer records each bumped write's counter and ChangeSet,
// keeps a bumped-but-empty write as a counter-only entry (its counter must
// still plug the sequence -- Apply drops the scope before the no-snapshot
// branch, so this is observe's only chance to see it), and take() hands
// everything back exactly once while disarming.
func TestBootGapBufferObserveRecordsBumpedWrites(t *testing.T) {
	var b bootGapBuffer
	b.activate()

	withChanges := bumpedScope(7)
	withChanges.Changes().RecordNodeID(101)
	b.observe(withChanges)

	b.observe(bumpedScope(8)) // bumped, empty ChangeSet: counter-only

	entries, poisoned := b.take()
	if poisoned != "" {
		t.Fatalf("buffer poisoned %q, want clean", poisoned)
	}
	if len(entries) != 2 {
		t.Fatalf("take() returned %d entries, want 2", len(entries))
	}
	if entries[0].counter != 7 || entries[0].cs == nil {
		t.Fatalf("entry 0 = {counter %d, cs nil=%v}, want {7, non-nil}", entries[0].counter, entries[0].cs == nil)
	}
	if entries[1].counter != 8 || entries[1].cs != nil {
		t.Fatalf("entry 1 = {counter %d, cs nil=%v}, want {8, nil} (counter-only)", entries[1].counter, entries[1].cs != nil)
	}

	// take() disarmed the buffer: nothing recorded after it can ever be
	// seen again, and a second take is empty.
	b.observe(bumpedScope(9))
	if entries, _ := b.take(); len(entries) != 0 {
		t.Fatalf("take() after take() returned %d entries, want 0 (buffer was consumed and disarmed)", len(entries))
	}
}

// TestBootGapBufferInactiveIsANoOp pins the buffer's whole post-boot cost
// model: an unarmed buffer records nothing, whatever is thrown at it --
// including the poison shapes, which must not stick either (a fallback
// write AFTER the file attempt concluded is Apply's business, not a
// judgment about a buffer nothing will ever read).
func TestBootGapBufferInactiveIsANoOp(t *testing.T) {
	var b bootGapBuffer

	b.observe(bumpedScope(7))
	b.observe(nil)
	b.observeAbandoned(bumpedScope(8))

	entries, poisoned := b.take()
	if len(entries) != 0 || poisoned != "" {
		t.Fatalf("unarmed buffer take() = (%d entries, poisoned %q), want (0, clean)", len(entries), poisoned)
	}
}

// TestBootGapBufferPoisonShapes pins each shape the replay cannot
// faithfully reproduce: a nil scope, a fallback-carrying ChangeSet, and the
// belt-and-braces unbumped-but-non-empty write. Each poisons rather than
// records, the FIRST reason wins, and an unbumped EMPTY scope is the one
// non-recording shape that stays harmless.
func TestBootGapBufferPoisonShapes(t *testing.T) {
	shapes := []struct {
		name       string
		observe    func(b *bootGapBuffer)
		wantReason string
	}{
		{
			"nil scope",
			func(b *bootGapBuffer) { b.observe(nil) },
			"nil write scope",
		},
		{
			"fallback-carrying ChangeSet",
			func(b *bootGapBuffer) {
				s := bumpedScope(7)
				s.Changes().RecordFallback("raw cypher")
				b.observe(s)
			},
			"fallback write during boot: raw cypher",
		},
		{
			"unbumped write that recorded changes",
			func(b *bootGapBuffer) {
				s := NewWriteScope()
				s.Changes().RecordNodeID(101)
				b.observe(s)
			},
			"unbumped write recorded changes",
		},
	}
	for _, tc := range shapes {
		t.Run(tc.name, func(t *testing.T) {
			var b bootGapBuffer
			b.activate()

			tc.observe(&b)

			// A later, perfectly ordinary write does not un-poison, and a
			// later poison does not overwrite the first reason.
			b.observe(bumpedScope(9))
			b.observe(nil)

			_, poisoned := b.take()
			if poisoned != tc.wantReason {
				t.Fatalf("poisoned = %q, want %q (first reason, kept)", poisoned, tc.wantReason)
			}
		})
	}

	t.Run("unbumped empty scope is skipped, not poison", func(t *testing.T) {
		var b bootGapBuffer
		b.activate()
		b.observe(NewWriteScope())
		entries, poisoned := b.take()
		if len(entries) != 0 || poisoned != "" {
			t.Fatalf("take() = (%d entries, poisoned %q) after an unbumped empty scope, want (0, clean)", len(entries), poisoned)
		}
	})
}

// TestBootGapBufferObserveAbandoned pins the counter-only entry an
// abandoned write leaves behind: bumped means its counter must plug the
// sequence (the write advanced pg's counter and then committed nothing),
// unbumped means it left no trace the sequence could miss.
func TestBootGapBufferObserveAbandoned(t *testing.T) {
	var b bootGapBuffer
	b.activate()

	b.observeAbandoned(bumpedScope(7))
	b.observeAbandoned(NewWriteScope()) // never bumped: nothing to account
	b.observeAbandoned(nil)             // guarded, matching ResolveAbandonedWrite's own nil guard

	entries, poisoned := b.take()
	if poisoned != "" {
		t.Fatalf("buffer poisoned %q, want clean", poisoned)
	}
	if len(entries) != 1 || entries[0].counter != 7 || entries[0].cs != nil {
		t.Fatalf("entries = %+v, want exactly one counter-only entry at 7", entries)
	}
}

// TestBootGapBufferOverflowPoisons pins both caps: the entry-count cap and
// the total-key cap each poison the buffer with an overflow reason rather
// than dropping writes silently -- a dropped write would be a hole the
// adoption could not see, and a poisoned buffer is a rejected file, which
// is the honest outcome.
func TestBootGapBufferOverflowPoisons(t *testing.T) {
	t.Run("entry cap", func(t *testing.T) {
		var b bootGapBuffer
		b.activate()
		for i := 0; i <= maxBootGapEntries; i++ {
			b.observe(bumpedScope(uint64(i + 1)))
		}
		entries, poisoned := b.take()
		if poisoned != "overflow: too many buffered writes" {
			t.Fatalf("poisoned = %q, want the entry-cap overflow reason", poisoned)
		}
		if len(entries) != maxBootGapEntries {
			t.Fatalf("len(entries) = %d, want exactly maxBootGapEntries (%d) recorded before the overflow", len(entries), maxBootGapEntries)
		}
	})

	t.Run("key cap", func(t *testing.T) {
		var b bootGapBuffer
		b.activate()

		// Two scopes: the first fits under the key cap, the second pushes
		// past it. perScope node ids each; the counts only need to
		// straddle maxBootGapKeys, not land on it exactly.
		perScope := maxBootGapKeys/2 + 1
		for i := 0; i < 2; i++ {
			s := bumpedScope(uint64(i + 1))
			for k := 0; k < perScope; k++ {
				s.Changes().RecordNodeID(graphID(i*perScope + k))
			}
			b.observe(s)
		}

		entries, poisoned := b.take()
		if poisoned != "overflow: too many buffered keys" {
			t.Fatalf("poisoned = %q, want the key-cap overflow reason", poisoned)
		}
		if len(entries) != 1 {
			t.Fatalf("len(entries) = %d, want 1 (the scope that still fit)", len(entries))
		}
	})
}

// TestChangeSetKeyCount pins keyCount against every key-bearing Record*
// method, and against what it deliberately excludes: fallback reasons are
// not keys (they poison the buffer through HasFallback long before any
// size cap matters).
// TestBootGapBufferPeekDoesNotConsume pins peek's whole contract: it
// reports what the buffer holds without disarming it or dropping anything
// -- an observe AFTER a peek still lands (the settle-wait loop depends on
// exactly this: it peeks while the Applies it is waiting for are still
// arriving), and the eventual take still returns everything exactly once.
func TestBootGapBufferPeekDoesNotConsume(t *testing.T) {
	var b bootGapBuffer
	b.activate()

	b.observe(bumpedScope(7))

	entries, poisoned := b.peek()
	if poisoned != "" || len(entries) != 1 || entries[0].counter != 7 {
		t.Fatalf("peek after one observe = (%d entries, poisoned %q), want exactly counter 7 and no poison", len(entries), poisoned)
	}

	b.observe(bumpedScope(8)) // must still land: peek must not disarm

	entries, _ = b.peek()
	if len(entries) != 2 {
		t.Fatalf("peek after a post-peek observe = %d entries, want 2 (peek disarmed or dropped the buffer)", len(entries))
	}

	taken, poisoned := b.take()
	if poisoned != "" || len(taken) != 2 {
		t.Fatalf("take after peeks = (%d entries, poisoned %q), want both entries and no poison", len(taken), poisoned)
	}
	if b.active.Load() {
		t.Fatalf("buffer still active after take")
	}
}

func TestChangeSetKeyCount(t *testing.T) {
	var c ChangeSet
	if got := c.keyCount(); got != 0 {
		t.Fatalf("zero ChangeSet keyCount() = %d, want 0", got)
	}

	kind := graph.StringKind("K")
	c.RecordNodeID(graph.ID(1))
	c.RecordNodeID(graph.ID(1)) // duplicate: same key
	c.RecordNodeObjectID("oid-1")
	c.RecordEdgeID(graph.ID(2))
	c.RecordEdgeTriple(graph.ID(1), graph.ID(2), kind)
	c.RecordEdgeTripleByObjectID("a", "b", kind)
	c.RecordDeleteNodesByKinds(nil, nil)
	c.RecordDeleteRelationshipsByKinds(graph.Kinds{kind})
	c.RecordFallback("not a key")

	if got := c.keyCount(); got != 7 {
		t.Fatalf("keyCount() = %d, want 7 (one per distinct key/criteria, fallbacks excluded)", got)
	}
}

// graphID converts a test loop index to a graph.ID; a helper only so the
// overflow test reads as intent rather than casts.
func graphID(i int) graph.ID {
	return graph.ID(uint64(i))
}
