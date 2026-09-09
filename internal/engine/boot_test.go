// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"
)

// -----------------------------------------------------------------------
// F1 (Task 12 review): Start's boot-load goroutine and the fallback
// recovery goroutine must never run concurrently. Before this fix, boot.go
// claimed they never could -- "Apply only ever calls enterFallback when
// e.snap.Load() != nil" -- but apply.go's nil-scope and HasFallback
// branches call enterFallback BEFORE Apply's own nil-snapshot check runs
// (see apply.go's numbered steps 3-4 vs. step 7), so a Run()/WipeGraph()/
// unrecognized write landing before boot load's first adoption could start
// a second, redundant rebuild loop racing the first. Both tests below drive
// claimRebuildLoop's shared CAS on fallbackRebuilding directly, the same
// way apply_test.go's F4 tests drive finishFallbackRebuild/startFallbackRebuild
// -- deterministic, no goroutine scheduling involved, and no database.
// -----------------------------------------------------------------------

// TestStartDoesNotDoubleARebuildLoopAgainstAPreBootFallback simulates the
// race's first ordering: some write's enterFallback call wins the shared
// CAS before Start ever runs (standing in for fallbackRebuilding.Store(true)
// -- exactly what startFallbackRebuild's own CAS success would have left
// behind). Start's own claimRebuildLoop attempt must then lose and return
// without spawning runBootLoad, leaving the flag exactly as the winning
// caller left it.
func TestStartDoesNotDoubleARebuildLoopAgainstAPreBootFallback(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.fallbackRebuilding.Store(true) // stand-in for a fallback trip that won the race first

	e.Start(context.Background())

	if !e.fallbackRebuilding.Load() {
		t.Fatalf("fallbackRebuilding = false after Start lost the CAS, want it to stay true (still held by the pre-boot fallback loop)")
	}
	if got := e.state.Load(); got != stateServing {
		t.Fatalf("state after Start lost the CAS = %d, want it untouched at stateServing (%d) -- Start must not itself flip fallback state", got, stateServing)
	}
}

// TestEnterFallbackDoesNotDoubleARebuildLoopDuringBoot simulates the race's
// other ordering: Start's boot-load goroutine wins the shared CAS first --
// the common case, since Start normally runs before any write could
// possibly fail -- and a write then trips enterFallback while boot load is
// still retrying. enterFallback's own state transition still fires
// (a write really could not be replayed, so "fallback entered" is a true
// event here, not noise -- see boot.go's Start doc), but its
// startFallbackRebuild call must lose the CAS to the already-running boot
// loop rather than start a second one.
func TestEnterFallbackDoesNotDoubleARebuildLoopDuringBoot(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.fallbackRebuilding.Store(true) // stand-in for Start's boot-load goroutine already claiming the gate

	e.enterFallback(context.Background(), "write during boot")

	if got := e.state.Load(); got != stateFallback {
		t.Fatalf("state after enterFallback = %d, want stateFallback (%d): a write that cannot be replayed is a genuine fallback trip even before boot load has adopted anything", got, stateFallback)
	}
	if !e.fallbackRebuilding.Load() {
		t.Fatalf("fallbackRebuilding = false after enterFallback, want it to stay true -- still held by the boot-load loop, not reclaimed for a second one")
	}
}

// TestRunBootLoadNeverFlipsToFallbackOnContextCancel pins the other half of
// F1: ordinary startup -- no write ever calling enterFallback -- must never
// flip state to stateFallback and so must never log "fallback entered"
// (that line is only ever reachable through enterFallback's own state CAS).
// bgCancel makes runBootLoad's very first check return immediately, before
// it would ever reach rebuildOnce/LoadSnapshot, so this runs synchronously
// with no database and no goroutine.
func TestRunBootLoadNeverFlipsToFallbackOnContextCancel(t *testing.T) {
	e := New(nil, nil, Config{Enabled: true})
	e.bgCancel()
	e.fallbackRebuilding.Store(true) // stand-in for Start's own claimRebuildLoop call having already run

	e.runBootLoad(context.Background())

	if got := e.state.Load(); got != stateServing {
		t.Fatalf("state after runBootLoad's context-cancelled return = %d, want stateServing (%d): plain startup must never call enterFallback", got, stateServing)
	}
	if e.fallbackRebuilding.Load() {
		t.Fatalf("fallbackRebuilding still true after runBootLoad's context-cancelled return, want it cleared")
	}
}
