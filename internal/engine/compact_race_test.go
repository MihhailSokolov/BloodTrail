// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// raceStressBound is this test's own hard wall-clock cap: it must finish
// well inside it, and a hang (a genuine deadlock this test exists to catch)
// fails loudly instead of relying on `go test`'s own default 10-minute
// timeout to eventually notice.
const raceStressBound = 50 * time.Second

// raceIDBase keeps every synthetic node id this test generates strictly
// above buildApplyView's own base ids (1-3): Fold's bigserial-monotonicity
// assertion (snapshot/fold.go) requires every delta-added node's id to
// exceed the CURRENT base's highest id at fold time, and since a
// compaction's own folded output becomes the next base, staying above the
// ORIGINAL base's ids is not enough on its own -- what actually makes this
// safe is that every id below is drawn from one shared, monotonically
// increasing atomic.Uint64 counter (raceStressIDCounter, below): a
// freshly-generated id is, by construction, larger than every id any
// earlier segment (already folded into some prior base, or not) could
// possibly carry.
const raceIDBase = 10_000

// mustRaceNodeSegment builds a one-node-upsert Segment for id, the same
// shape newNodeSegment (compact_test.go) builds, but safe to call from a
// goroutine other than the test's own: AddNodeState cannot fail for this
// fixed, always-well-formed input, so a failure here is a genuine bug this
// test wants to crash loudly on (panic) rather than a call to t.Fatal a
// non-test goroutine is not allowed to make.
func mustRaceNodeSegment(id uint64) *snapshot.Segment {
	var b snapshot.SegmentBuilder
	if err := b.AddNodeState(id, []snapshot.KindID{applyKindUser}, []byte(fmt.Sprintf(`{"objectid":"race-%d"}`, id))); err != nil {
		panic(fmt.Sprintf("mustRaceNodeSegment(%d): AddNodeState: %v", id, err))
	}
	return b.Build()
}

// forceCompactionAttempt mirrors maybeStartCompaction's own capture-and-spawn
// logic (compact.go) exactly, minus the size-threshold check: it is this
// test's way of forcing a compaction attempt on a schedule the production
// thresholds don't control, while still going through the exact same
// capture discipline (segments captured under applyMu, alongside the base
// they layer onto) and the exact same `compacting` CAS every real trigger
// respects -- so a forced attempt that loses the CAS to an already-running
// compaction is dropped exactly as a threshold-triggered one would be,
// never queued or retried.
func forceCompactionAttempt(e *Engine) {
	e.applyMu.Lock()
	view := e.snap.Load()
	var base *snapshot.Snapshot
	var segs []*snapshot.Segment
	ready := view != nil && e.state.Load() == stateServing && len(view.Segments()) > 0
	if ready {
		base = view.Base()
		segs = view.Segments()
	}
	e.applyMu.Unlock()

	if !ready {
		return
	}
	if !e.compacting.CompareAndSwap(false, true) {
		return
	}
	go e.runCompaction(base, segs)
}

// checkViewAccessors walks a representative sample of view's accessors --
// the same shape a real query executor's traversal would -- over up to
// raceReaderSampleSize dense ids, purely to give the race detector surface
// area to catch a genuinely unsynchronized access. It reports the first
// error via t.Errorf (never Fatal -- this runs on a reader goroutine, and
// only the test's own goroutine may call the Fatal family, per the testing
// package's own concurrency contract).
const raceReaderSampleSize = 64

func checkViewAccessors(t *testing.T, view *snapshot.View) {
	n := view.NodeCount()
	if n > raceReaderSampleSize {
		n = raceReaderSampleSize
	}
	for i := 0; i < n; i++ {
		id := snapshot.NodeID(i)
		if !view.Alive(id) {
			continue
		}
		_ = view.KindIDsOf(id)
		_ = view.GraphID(id)
		_ = view.PropNodeMap(id)
		view.OutEdges(id, func(snapshot.NodeID, snapshot.KindID, uint64) bool { return true })
		view.InEdges(id, func(snapshot.NodeID, snapshot.KindID, uint64) bool { return true })
	}

	if err := snapshot.CheckViewConsistent(view); err != nil {
		t.Errorf("CheckViewConsistent: %v", err)
	}
}

// TestCompactRace is this file's mandatory `-race` stress test (the brief's
// Step 2): 4 applier goroutines publish synthetic segments concurrently
// through the exact same publishAppend seam the unit tests above use (so
// every real Apply-tail code path -- mergeSegmentTailIfNeeded,
// maybeStartCompaction, and (via forceCompactionAttempt above) the
// compaction goroutine itself -- runs for real, concurrently, under the
// race detector), while 8 reader goroutines concurrently walk View
// accessors and CheckViewConsistent against whatever View happens to be
// current at each instant, and a compaction is forced roughly every 50
// applies (in addition to whatever cfg.CompactEntries/CompactBytes trigger
// on their own).
//
// There is no separate correctness oracle beyond CheckViewConsistent and
// the race detector itself during the run: the property this test exists
// to prove is that concurrent applies, segment-merges, and compactions
// (including their base-swap/state/segment-tail discard paths) never
// corrupt a View a concurrent reader is walking, and never trip a data
// race. A final pass, after every goroutine has finished and any trailing
// compaction has settled, DOES check a genuine correctness oracle: every
// node id any applier ever published must resolve and be Alive in the
// engine's final View, regardless of how many compactions folded it along
// the way.
//
// Run with: go test -race -run CompactRace ./internal/engine/ -count=1
func TestCompactRace(t *testing.T) {
	const (
		numAppliers          = 4
		numReaders           = 8
		iterationsPerApplier = 400
		forceCompactionEvery = 50
	)

	ctx := context.Background()
	baseView := buildApplyView(t)

	e := New(nil, nil, Config{
		Enabled: true,
		// Small enough to also trigger organically well before every
		// forced attempt below, so both trigger paths (threshold-driven
		// and forced) exercise maybeStartCompaction's/runCompaction's CAS
		// against each other, not just against themselves.
		CompactEntries: 30,
		Log:            quietCompactTestLogger(),
	})
	e.snap.Store(baseView)

	var idCounter atomic.Uint64
	idCounter.Store(raceIDBase)
	var applyCounter atomic.Uint64

	publishedIDs := make(chan uint64, numAppliers*iterationsPerApplier)

	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	for i := 0; i < numReaders; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if view := e.snap.Load(); view != nil {
					checkViewAccessors(t, view)
				}
			}
		}()
	}

	var applierWG sync.WaitGroup
	for i := 0; i < numAppliers; i++ {
		applierWG.Add(1)
		go func() {
			defer applierWG.Done()
			for iter := 0; iter < iterationsPerApplier; iter++ {
				id := idCounter.Add(1)
				seg := mustRaceNodeSegment(id)
				publishAppend(ctx, e, seg)
				publishedIDs <- id

				if applyCounter.Add(1)%forceCompactionEvery == 0 {
					forceCompactionAttempt(e)
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		applierWG.Wait()
		close(publishedIDs)
		// Let readers keep hammering the View while any trailing
		// compaction (forced by the very last few applies) is still
		// in flight, then stop them and wait for the final one to
		// settle before the correctness pass below.
		waitCompactionSettled(t, e)
		close(stop)
		readerWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(raceStressBound):
		t.Fatalf("compact race stress test did not complete within %s", raceStressBound)
	}

	// Correctness oracle: every id any applier published must still
	// resolve, and be Alive, in the engine's final View -- no compaction,
	// however many ran, may have lost a write.
	final := e.snap.Load()
	var ids []uint64
	for id := range publishedIDs {
		ids = append(ids, id)
	}
	if len(ids) != numAppliers*iterationsPerApplier {
		t.Fatalf("collected %d published ids, want %d", len(ids), numAppliers*iterationsPerApplier)
	}
	for _, id := range ids {
		dense, ok := final.Dense(id)
		if !ok {
			t.Fatalf("published node %d does not resolve in the final View", id)
		}
		if !final.Alive(dense) {
			t.Fatalf("published node %d resolves but is not Alive in the final View", id)
		}
	}

	if err := snapshot.CheckViewConsistent(final); err != nil {
		t.Fatalf("CheckViewConsistent(final): %v", err)
	}

	t.Logf("compact race stress: %d applies, %d compactions adopted", len(ids), e.CompactionCount())
}
