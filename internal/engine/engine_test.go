// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestServeStateRequiresSnapshotAndServingState drives Engine's private
// serving state directly (package engine, not engine_test): a nil
// pgDriver/pool is fine here since serveState/Fresh never touch them.
//
// Both conditions are necessary: a brand-new Engine is in stateServing but
// has no snapshot, and an Engine with a snapshot still declines while it is
// in stateFallback. serveState takes no write-history parameter at all --
// write-through means a published View already reflects every committed
// write, so whether one may be served depends only on these two things.
func TestServeStateRequiresSnapshotAndServingState(t *testing.T) {
	e := New(nil, nil, Config{})

	if view, ok := e.serveState(); view != nil || ok {
		t.Fatalf("serveState() on a brand-new Engine = (%v, %v), want (nil, false)", view, ok)
	}

	view := snapshot.NewView(&snapshot.Snapshot{})
	e.snap.Store(view)

	if got, ok := e.serveState(); !ok || got != view {
		t.Fatalf("serveState() right after storing a snapshot = (%v, %v), want (%v, true)", got, ok, view)
	}

	e.state.Store(stateFallback)
	if got, ok := e.serveState(); ok || got != view {
		t.Fatalf("serveState() in fallback = (%v, %v), want (%v, false)", got, ok, view)
	}

	e.state.Store(stateServing)
	if _, ok := e.serveState(); !ok {
		t.Fatalf("serveState() back in stateServing = false, want true")
	}
}

// TestFreshMirrorsServeState pins Fresh, kept as a test-observability
// wrapper now that the poller (its last production caller) is retired, to
// serveState's answer.
func TestFreshMirrorsServeState(t *testing.T) {
	e := New(nil, nil, Config{})

	view := snapshot.NewView(&snapshot.Snapshot{})
	e.snap.Store(view)

	if got, fresh := e.Fresh(); !fresh || got != view {
		t.Fatalf("Fresh() while serving = (%v, %v), want (%v, true)", got, fresh, view)
	}

	e.state.Store(stateFallback)
	if got, fresh := e.Fresh(); fresh || got != view {
		t.Fatalf("Fresh() in fallback = (%v, %v), want (%v, false)", got, fresh, view)
	}
}

// TestAdoptRebuiltViewRejectsRacedApply covers the guard that keeps a
// rebuild from silently discarding a written-through delta: a rebuild reads
// applyEpoch before it starts loading, and may only publish if that value is
// unchanged by the time it does. An Apply landing in between means the
// loaded snapshot may predate that write, so the rebuild must be retried
// rather than published.
func TestAdoptRebuiltViewRejectsRacedApply(t *testing.T) {
	e := New(nil, nil, Config{})

	epoch := e.applyEpoch.Load()
	fresh := snapshot.NewView(&snapshot.Snapshot{})

	if !e.adoptRebuiltView(context.Background(), fresh, epoch, e.settledDirtyGen.Load()) {
		t.Fatalf("adoptRebuiltView with an unchanged epoch = false, want true")
	}
	if got := e.snap.Load(); got != fresh {
		t.Fatalf("adoptRebuiltView did not publish the view")
	}

	e.applyEpoch.Add(1) // an Apply landed while the next rebuild was loading
	racedOut := snapshot.NewView(&snapshot.Snapshot{})
	if e.adoptRebuiltView(context.Background(), racedOut, epoch, e.settledDirtyGen.Load()) {
		t.Fatalf("adoptRebuiltView with a bumped epoch = true, want false")
	}
	if got := e.snap.Load(); got != fresh {
		t.Fatalf("adoptRebuiltView published despite a raced Apply: the delta would have been lost")
	}
}

// TestAdoptRebuiltViewExitsFallback pins recovery to adoption rather than to
// whichever goroutine performed the rebuild: any adopted snapshot is
// complete and current (that is exactly what the epoch check proves), so it
// ends the fallback -- a boot-load or manual rebuild included.
func TestAdoptRebuiltViewExitsFallback(t *testing.T) {
	e := New(nil, nil, Config{})
	e.state.Store(stateFallback)

	if !e.adoptRebuiltView(context.Background(), snapshot.NewView(&snapshot.Snapshot{}), e.applyEpoch.Load(), e.settledDirtyGen.Load()) {
		t.Fatalf("adoptRebuiltView with an unchanged epoch = false, want true")
	}
	if got := e.state.Load(); got != stateServing {
		t.Fatalf("state after an adopted rebuild = %d, want stateServing (%d)", got, stateServing)
	}

	e.state.Store(stateFallback)
	e.applyEpoch.Add(1)
	if e.adoptRebuiltView(context.Background(), snapshot.NewView(&snapshot.Snapshot{}), 0, e.settledDirtyGen.Load()) {
		t.Fatalf("adoptRebuiltView with a bumped epoch = true, want false")
	}
	if got := e.state.Load(); got != stateFallback {
		t.Fatalf("state after a NOT-adopted rebuild = %d, want it to stay stateFallback (%d)", got, stateFallback)
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

// TestShouldLogRefusal exercises RebuildNow's refusal-warning rate limit in
// isolation from RebuildNow itself (which needs a live PostgreSQL pool via
// LoadSnapshot): the very first call always logs, and immediately-following
// calls within refusalLogInterval do not -- deterministic without a fake
// clock, since "immediately after" is trivially still within a 10-minute
// window. refusalLastLoggedNano's zero value means "never logged"; storing
// it back to zero directly (nothing in production does this -- there is no
// "reset on success", matching queryErrorLogInterval's own plain sliding
// window) exercises that same boundary condition again on demand, without
// waiting out a real refusalLogInterval.
func TestShouldLogRefusal(t *testing.T) {
	e := New(nil, nil, Config{})

	if !e.shouldLogRefusal() {
		t.Fatalf("shouldLogRefusal() = false on the first call, want true (the very first refusal always logs)")
	}
	if e.shouldLogRefusal() {
		t.Fatalf("shouldLogRefusal() = true on an immediate second call, want false (rate-limited within refusalLogInterval)")
	}
	if e.shouldLogRefusal() {
		t.Fatalf("shouldLogRefusal() = true on a third call, want false (still rate-limited)")
	}

	e.refusalLastLoggedNano.Store(0)
	if !e.shouldLogRefusal() {
		t.Fatalf("shouldLogRefusal() = false immediately after refusalLastLoggedNano is reset to zero, want true (0 always means \"never logged\")")
	}
}
