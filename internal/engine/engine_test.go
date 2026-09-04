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
