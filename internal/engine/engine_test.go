// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestFreshness drives Engine's private snapshot/generation state directly
// (package engine, not engine_test): a nil pgDriver/pool is fine here since
// Fresh and NoteWrite never touch them.
func TestFreshness(t *testing.T) {
	e := New(nil, nil, Config{})

	if snap, fresh := e.Fresh(); snap != nil || fresh {
		t.Fatalf("Fresh() on a brand-new Engine = (%v, %v), want (nil, false)", snap, fresh)
	}

	snap := &snapshot.Snapshot{Generation: 0}
	e.snap.Store(snap)

	if got, fresh := e.Fresh(); !fresh || got != snap {
		t.Fatalf("Fresh() right after storing a snapshot at the current generation = (%v, %v), want (%v, true)", got, fresh, snap)
	}

	e.NoteWrite()

	if got, fresh := e.Fresh(); fresh || got != snap {
		t.Fatalf("Fresh() after NoteWrite = (%v, %v), want (%v, false)", got, fresh, snap)
	}
}

// TestSnapshotStillCurrentDetectsSnapshotSwap covers the race the step-6
// recheck in TryAllShortestPaths exists to catch: a write lands mid-query
// (generation 5->6) and a concurrent RebuildNow adopts a brand-new snapshot
// B stamped Generation 6 before the recheck runs. A second Fresh() call at
// that point would judge B -- whose Generation matches the live counter --
// and wrongly report fresh, even though the actual computation ran entirely
// against the stale snapshot A (Generation 5). snapshotStillCurrent, given
// the specific snapshot A the caller captured, must report it stale
// regardless of what e.snap now holds.
func TestSnapshotStillCurrentDetectsSnapshotSwap(t *testing.T) {
	e := New(nil, nil, Config{})

	snapA := &snapshot.Snapshot{Generation: 5}
	e.snap.Store(snapA)
	e.generation.Store(5)

	// A write lands mid-query.
	e.generation.Store(6)

	// A concurrent RebuildNow adopts a new snapshot B stamped at the new
	// generation.
	snapB := &snapshot.Snapshot{Generation: 6}
	e.snap.Store(snapB)

	// Fresh() now reports the *current* snapshot (B) as fresh -- correct for
	// a caller starting a new query now, but not what the step-6 recheck
	// needs to ask.
	if got, fresh := e.Fresh(); !fresh || got != snapB {
		t.Fatalf("Fresh() after snapshot swap = (%v, %v), want (%v, true)", got, fresh, snapB)
	}

	// snapshotStillCurrent, asked about the snapshot the query actually
	// used (A), must say it is stale even though Fresh() reports fresh.
	if e.snapshotStillCurrent(snapA) {
		t.Fatalf("snapshotStillCurrent(snapA) = true after a newer snapshot was adopted, want false")
	}

	// And it must still correctly report the new snapshot as current.
	if !e.snapshotStillCurrent(snapB) {
		t.Fatalf("snapshotStillCurrent(snapB) = false for the just-adopted current snapshot, want true")
	}
}

// TestTryAllShortestPathsDeclinesWhenDisabled and
// TestTryAllShortestPathsDeclinesWithNoSnapshot exercise two decline paths
// that never touch pgDriver/pool/tx, so they run without a database.

func TestTryAllShortestPathsDeclinesWhenDisabled(t *testing.T) {
	e := New(nil, nil, Config{Enabled: false})

	if _, served := e.TryAllShortestPaths(context.Background(), nil, recognize.PathQuery{}); served {
		t.Fatalf("TryAllShortestPaths served while Enabled is false")
	}
}

func TestTryAllShortestPathsDeclinesWithNoSnapshot(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})

	if _, served := e.TryAllShortestPaths(context.Background(), nil, recognize.PathQuery{}); served {
		t.Fatalf("TryAllShortestPaths served before RebuildNow ever ran")
	}
}
