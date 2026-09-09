// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// quietCompactTestLogger discards engine log output during this file's
// tests: compaction logs at Info/Debug, and several of these tests trigger
// more than one, which would otherwise spam `go test -v` output with lines
// the tests themselves already assert on through their own return values
// and CompactionCount.
func quietCompactTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newNodeSegment builds a one-node-upsert Segment for id, tagged with
// applyKindUser (apply_test.go's fixture kind) so it participates in
// NodesOfKind/Kinds() comparisons the same way a real write-through delta
// would.
//
// id should avoid buildApplyView's own base ids (1-3) purely so a caller
// gets a genuinely NEW node rather than an accidental override of one of
// the fixture's -- not because Fold requires it: a delta-added id below
// the base's own max id is ordinary, valid input (the F2 fix,
// TestCompactionSurvivesLowIDTailAcrossTwoCompactions below and
// TestFoldDeltaAddedNodeBelowBaseMaxRoundTrips, snapshot/fold_test.go), not
// a bigserial-monotonicity violation the way an EARLIER version of Fold
// once mistakenly rejected it as.
func newNodeSegment(t *testing.T, id uint64) *snapshot.Segment {
	t.Helper()

	var b snapshot.SegmentBuilder
	if err := b.AddNodeState(id, []snapshot.KindID{applyKindUser}, []byte(fmt.Sprintf(`{"objectid":"compact-%d"}`, id))); err != nil {
		t.Fatalf("AddNodeState(%d): %v", id, err)
	}
	return b.Build()
}

// publishAppend simulates one write-through Apply call's own tail
// (apply.go), without needing a live PostgreSQL pool behind read-back:
// append seg onto e's current View, publish it, then run the exact same
// post-publish maintenance step Apply itself runs (maintainAfterPublish,
// compact.go) -- see that method's own doc for why it exists as a
// separate, test-reachable method in the first place.
func publishAppend(ctx context.Context, e *Engine, seg *snapshot.Segment) *snapshot.View {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()

	newView := e.snap.Load().WithSegment(seg)
	e.snap.Store(newView)
	e.maintainAfterPublish(ctx, newView)
	return newView
}

// waitCompactionSettled polls until no compaction is running (e.compacting
// back to false), returning an error past a generous bound rather than
// failing the test itself. Every compaction in this file's and
// TestCompactRace's own tests folds at most a handful to a few thousand
// nodes, so anything still running past this bound is a bug (or a wedged
// goroutine), not a slow machine.
//
// Returning an error, rather than calling t.Fatal directly the way an
// earlier version of this helper did, is deliberate: t.Fatal (and the rest
// of the FailNow family) may only be called from the goroutine actually
// running the test function, per the testing package's own concurrency
// contract -- and this helper is called from two different goroutines
// across this package's tests. compact_test.go's own callers run in the
// test's own goroutine and may call t.Fatal on the returned error directly;
// TestCompactRace (compact_race_test.go) calls this from a SEPARATE
// goroutine it spawns, and reports the error back through a variable for
// the test's own main goroutine to act on -- see that call site's own
// comment.
func waitCompactionSettled(e *Engine) error {
	deadline := time.Now().Add(5 * time.Second)
	for e.compacting.Load() {
		if time.Now().After(deadline) {
			return fmt.Errorf("compaction did not settle within 5s")
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// ---- Step 1's four required cases ---------------------------------------

// TestCompactionTriggersExactlyOnceAndMatchesUncompactedStack is the
// brief's core case: with cfg.CompactEntries forced tiny, a run of Applies
// that crosses the threshold exactly once must trigger exactly one
// background compaction (CompactionCount), and the resulting View's
// content must be indistinguishable, by every accessor
// (snapshot.CheckViewsEquivalent), from the same base with the same
// segments stacked directly and never compacted at all.
func TestCompactionTriggersExactlyOnceAndMatchesUncompactedStack(t *testing.T) {
	ctx := context.Background()
	baseView := buildApplyView(t)
	originalBase := baseView.Base()

	e := New(nil, nil, Config{
		Enabled:        true,
		CompactEntries: 3, // crossed on the 4th one-entry segment below
		Log:            quietCompactTestLogger(),
	})
	e.snap.Store(baseView)

	reference := baseView // never touched by compaction; stacked in parallel below
	for i := 0; i < 4; i++ {
		seg := newNodeSegment(t, uint64(200+i))
		reference = reference.WithSegment(seg)
		publishAppend(ctx, e, seg)
	}

	if err := waitCompactionSettled(e); err != nil {
		t.Fatal(err)
	}

	if got := e.CompactionCount(); got != 1 {
		t.Fatalf("CompactionCount() = %d, want exactly 1", got)
	}

	compacted := e.snap.Load()
	if compacted.Base() == originalBase {
		t.Fatal("a compaction ran but the engine's base snapshot pointer never changed")
	}
	if compacted.SegmentCount() != 0 {
		t.Fatalf("SegmentCount() after compaction = %d, want 0 (nothing published while the fold ran)", compacted.SegmentCount())
	}

	if err := snapshot.CheckViewsEquivalent(compacted, reference); err != nil {
		t.Fatalf("compacted view diverges from the never-compacted stack: %v", err)
	}
}

// TestAdoptCompactionRebasesSegmentsAddedDuringFold is the brief's
// mid-fold-injection case, driven synchronously (Fold called directly
// rather than through the async runCompaction goroutine, exactly as the
// brief's own Step 1 describes): a segment published AFTER the fold's
// inputs were captured but BEFORE adoption runs must survive as the
// adopted View's own rebased tail, and the result must match a
// never-compacted reference stacking the same two segments.
//
// Config here leaves CompactEntries/CompactBytes at their zero value
// (compactionThresholdExceeded's own doc: zero means "no bound on that
// dimension"), so publishAppend's own maintainAfterPublish call never
// triggers a REAL background compaction of its own to race this test's
// manual one -- every capture/fold/adopt step below is this test's alone.
func TestAdoptCompactionRebasesSegmentsAddedDuringFold(t *testing.T) {
	ctx := context.Background()
	baseView := buildApplyView(t)

	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(baseView)

	seg1 := newNodeSegment(t, 300)
	v1 := publishAppend(ctx, e, seg1)

	// Capture exactly what maybeStartCompaction would have captured at
	// this instant, and fold it -- standing in for runCompaction's own
	// out-of-lock Fold call.
	capturedBase := v1.Base()
	capturedSegs := v1.Segments()
	folded, err := snapshot.Fold(capturedBase, capturedSegs)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}

	// A write lands WHILE that fold was "running": a second segment is
	// published onto the same lineage before adoption is attempted.
	seg2 := newNodeSegment(t, 301)
	v2 := publishAppend(ctx, e, seg2)

	if !e.adoptCompaction(capturedBase, capturedSegs, folded) {
		t.Fatal("adoptCompaction refused; want it to adopt and rebase the mid-fold segment")
	}

	adopted := e.snap.Load()
	if adopted.Base() != folded {
		t.Fatal("adopted view's base is not the freshly folded snapshot")
	}
	if adopted.SegmentCount() != 1 {
		t.Fatalf("SegmentCount() after adoption = %d, want 1 (the rebased mid-fold segment)", adopted.SegmentCount())
	}
	if e.CompactionCount() != 1 {
		t.Fatalf("CompactionCount() = %d, want 1", e.CompactionCount())
	}

	// Content must equal the never-compacted reference: baseView + seg1 +
	// seg2, which is exactly v2.
	if err := snapshot.CheckViewsEquivalent(adopted, v2); err != nil {
		t.Fatalf("adopted view diverges from the never-compacted reference: %v", err)
	}
}

// TestAdoptCompactionDiscardsWhenBaseSwapped is the brief's
// base-swap-mid-compaction case: a concurrent rebuild (adoptRebuiltView,
// engine.go) publishing a whole new base while a fold was in flight must
// make that fold's own adoption attempt refuse outright, leaving the
// rebuilt View untouched.
func TestAdoptCompactionDiscardsWhenBaseSwapped(t *testing.T) {
	ctx := context.Background()
	baseView := buildApplyView(t)

	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(baseView)

	seg := newNodeSegment(t, 400)
	v1 := publishAppend(ctx, e, seg)

	capturedBase := v1.Base()
	capturedSegs := v1.Segments()
	folded, err := snapshot.Fold(capturedBase, capturedSegs)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}

	// A rebuild swaps in an entirely independent base while that fold was
	// "running" -- exactly what adoptRebuiltView does on a successful
	// RebuildNow.
	rebuiltView := buildApplyView(t)
	e.applyMu.Lock()
	e.snap.Store(rebuiltView)
	e.applyMu.Unlock()

	if e.adoptCompaction(capturedBase, capturedSegs, folded) {
		t.Fatal("adoptCompaction adopted over a swapped base; want it to refuse")
	}
	if e.snap.Load() != rebuiltView {
		t.Fatal("adoptCompaction's refusal still altered e.snap; want the rebuilt view left untouched")
	}
	if e.CompactionCount() != 0 {
		t.Fatalf("CompactionCount() = %d, want 0 (the discarded fold must not count as adopted)", e.CompactionCount())
	}
}

// TestCompactionSurvivesLowIDTailAcrossTwoCompactions is the F2 finding's
// own end-to-end reproduction at the compaction level (task-16 review): the
// "ordinary concurrency" scenario the finding names, played out across TWO
// real compactions rather than Fold alone (TestFoldDeltaAddedNodeBelowBaseMaxRoundTrips,
// snapshot/fold_test.go, covers the same shape one layer down).
//
// Tx B (database id 501) commits and is published FIRST; tx A (database id
// 500, allocated by PostgreSQL's bigserial sequence BEFORE 501, but slower
// to reach Apply) is published SECOND, landing on the view while the first
// compaction's fold is already "in flight" (captured but not yet adopted) --
// exactly TestAdoptCompactionRebasesSegmentsAddedDuringFold's own
// mid-fold-injection shape. The first compaction folds base+segB only, so it
// never sees id 500 at all; adoption rebases segA (id 500) as the new base's
// own tail. The SECOND compaction then must fold that new base (whose max id
// is now 501) together with segA -- a delta-added id (500) BELOW the base's
// own max, id-for-id the shape the F2 finding says can poison every fold
// from here on: a pre-fix Fold rejects 500 as a bigserial-monotonicity
// violation, adoption never gets a folded snapshot to publish, the tail is
// never cleared, and the identical failure repeats on every later attempt
// (compaction AND SaveSnapshot) forever. This test's critical assertion is
// that the second Fold succeeds -- see the task-16 report for this test's
// own RED evidence against the pre-fix code.
func TestCompactionSurvivesLowIDTailAcrossTwoCompactions(t *testing.T) {
	ctx := context.Background()
	baseView := buildApplyView(t) // ids 1-3

	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(baseView)

	// Tx B (id 501) commits first.
	segB := newNodeSegment(t, 501)
	v1 := publishAppend(ctx, e, segB)

	// Compaction #1 captures (base, [segB]) and folds it.
	capturedBase1 := v1.Base()
	capturedSegs1 := v1.Segments()
	folded1, err := snapshot.Fold(capturedBase1, capturedSegs1)
	if err != nil {
		t.Fatalf("Fold #1: %v", err)
	}

	// Tx A (id 500, numerically LOWER, allocated before 501 by PostgreSQL's
	// own sequence, but slower to reach Apply) lands on the view while that
	// fold was "in flight".
	segA := newNodeSegment(t, 500)
	publishAppend(ctx, e, segA)

	if !e.adoptCompaction(capturedBase1, capturedSegs1, folded1) {
		t.Fatal("adoptCompaction #1 refused; want it to adopt and rebase segA as the new tail")
	}
	if e.CompactionCount() != 1 {
		t.Fatalf("CompactionCount() = %d after adoption #1, want 1", e.CompactionCount())
	}

	adopted1 := e.snap.Load()
	if adopted1.Base() != folded1 {
		t.Fatal("adopted1's base is not the freshly folded snapshot")
	}
	if adopted1.SegmentCount() != 1 {
		t.Fatalf("SegmentCount() after adoption #1 = %d, want 1 (segA rebased as the tail)", adopted1.SegmentCount())
	}

	// Compaction #2: fold folded1 (max base id 501) together with the
	// rebased tail carrying id 500 -- a delta-added id BELOW the base's own
	// max. This is the exact call that fails forever under the pre-fix
	// two-phase Fold.
	capturedBase2 := adopted1.Base()
	capturedSegs2 := adopted1.Segments()
	folded2, err := snapshot.Fold(capturedBase2, capturedSegs2)
	if err != nil {
		t.Fatalf("Fold #2 (delta-added id 500 below base max 501): %v -- this is exactly the F2 poisoning the fix must close", err)
	}

	if !e.adoptCompaction(capturedBase2, capturedSegs2, folded2) {
		t.Fatal("adoptCompaction #2 refused; want it to adopt")
	}
	if e.CompactionCount() != 2 {
		t.Fatalf("CompactionCount() = %d after adoption #2, want 2", e.CompactionCount())
	}

	adopted2 := e.snap.Load()
	if adopted2.SegmentCount() != 0 {
		t.Fatalf("SegmentCount() after adoption #2 = %d, want 0", adopted2.SegmentCount())
	}

	// Content must equal the never-compacted reference: baseView + segB +
	// segA, in publish order.
	reference := baseView.WithSegment(segB).WithSegment(segA)
	if err := snapshot.CheckViewsEquivalent(adopted2, reference); err != nil {
		t.Fatalf("twice-compacted view diverges from the never-compacted reference: %v", err)
	}
}

// ---- additional targeted coverage ---------------------------------------

// TestAdoptCompactionDiscardsWhenNotServing covers adoptCompaction's second
// refusal condition: a fallback entered while a fold was in flight (the
// base itself unchanged) must still refuse adoption, mirroring
// saveSnapshotPreconditionsFor's identical reasoning for SaveSnapshot
// (persist.go).
func TestAdoptCompactionDiscardsWhenNotServing(t *testing.T) {
	ctx := context.Background()
	baseView := buildApplyView(t)

	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(baseView)

	seg := newNodeSegment(t, 500)
	v1 := publishAppend(ctx, e, seg)

	capturedBase := v1.Base()
	capturedSegs := v1.Segments()
	folded, err := snapshot.Fold(capturedBase, capturedSegs)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}

	e.state.Store(stateFallback)

	if e.adoptCompaction(capturedBase, capturedSegs, folded) {
		t.Fatal("adoptCompaction adopted while state == stateFallback; want it to refuse")
	}
	if e.snap.Load() != v1 {
		t.Fatal("adoptCompaction's refusal still altered e.snap")
	}
}

// TestAdoptCompactionDiscardsWhenSegmentTailCollapsed covers the third
// refusal condition -- segmentsAfter finding the captured prefix no longer
// intact -- via the one production path that can actually cause it: a
// concurrent collapseSegmentStackIfNeeded collapsing the ENTIRE segment stack
// (including the captured prefix) into one merged Segment while a fold was
// in flight. The base pointer is unchanged and state stays serving, so
// only the prefix-identity check can be what refuses this.
func TestAdoptCompactionDiscardsWhenSegmentTailCollapsed(t *testing.T) {
	ctx := context.Background()
	baseView := buildApplyView(t)

	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(baseView)

	seg := newNodeSegment(t, 600)
	v1 := publishAppend(ctx, e, seg)

	capturedBase := v1.Base()
	capturedSegs := v1.Segments()
	folded, err := snapshot.Fold(capturedBase, capturedSegs)
	if err != nil {
		t.Fatalf("Fold: %v", err)
	}

	// Simulate collapseSegmentStackIfNeeded firing mid-fold: the same base,
	// but the one captured segment replaced by a merged Segment that is a
	// different, freshly-built *Segment (not pointer-equal to capturedSegs[0],
	// even though it carries equivalent content).
	collapsed := snapshot.NewView(capturedBase).WithSegment(snapshot.MergeSegments(capturedSegs))
	e.applyMu.Lock()
	e.snap.Store(collapsed)
	e.applyMu.Unlock()

	if e.adoptCompaction(capturedBase, capturedSegs, folded) {
		t.Fatal("adoptCompaction adopted after the captured prefix was collapsed away; want it to refuse")
	}
	if e.snap.Load() != collapsed {
		t.Fatal("adoptCompaction's refusal still altered e.snap")
	}
}

// TestCollapseSegmentStackIfNeededCollapsesPastMaxSegments pins the
// synchronous half of this file's two size bounds (Produces item (a)):
// once a View's segment stack grows past maxSegments, the very next
// publish collapses it down to exactly one segment, with content
// unchanged.
func TestCollapseSegmentStackIfNeededCollapsesPastMaxSegments(t *testing.T) {
	ctx := context.Background()
	baseView := buildApplyView(t)

	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(baseView)

	reference := baseView
	for i := 0; i < maxSegments+1; i++ {
		seg := newNodeSegment(t, uint64(1000+i))
		reference = reference.WithSegment(seg)
		publishAppend(ctx, e, seg)
	}

	current := e.snap.Load()
	if current.SegmentCount() != 1 {
		t.Fatalf("SegmentCount() = %d after exceeding maxSegments (%d), want 1 (collapsed)", current.SegmentCount(), maxSegments)
	}
	if current.Base() != baseView.Base() {
		t.Fatal("collapseSegmentStackIfNeeded changed the base snapshot; it must only ever collapse segments, never fold")
	}

	if err := snapshot.CheckViewsEquivalent(current, reference); err != nil {
		t.Fatalf("collapsed view diverges from the uncollapsed stack: %v", err)
	}
}

// TestCompactionThresholdExceeded pins compactionThresholdExceeded's own
// decision as a table, mirroring this package's other pure-decision tests
// (TestSaveSnapshotPreconditionsForRequiresEveryCondition, persist_test.go;
// TestFallbackRetryDelay*, boot/apply tests) -- most importantly the "both
// zero" row, which is what keeps every pre-existing unit test in this
// package (built with a zero-value Config) from ever triggering a
// compaction it never asked for.
func TestCompactionThresholdExceeded(t *testing.T) {
	cases := []struct {
		name           string
		compactEntries int
		compactBytes   size.Size
		entries        int
		bytes          uint64
		want           bool
	}{
		{"both zero, never exceeded", 0, 0, 1 << 30, 1 << 40, false},
		{"entries bound only, under", 100, 0, 50, 1 << 40, false},
		{"entries bound only, over", 100, 0, 101, 0, true},
		{"entries bound only, exactly at bound", 100, 0, 100, 0, false},
		{"bytes bound only, under", 0, 1000, 0, 500, false},
		{"bytes bound only, over", 0, 1000, 0, 1001, true},
		{"bytes bound only, exactly at bound", 0, 1000, 0, 1000, false},
		{"both set, entries side trips it", 100, 1000, 101, 500, true},
		{"both set, bytes side trips it", 100, 1000, 50, 1001, true},
		{"both set, neither trips it", 100, 1000, 50, 500, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := compactionThresholdExceeded(c.compactEntries, c.compactBytes, c.entries, c.bytes); got != c.want {
				t.Errorf("compactionThresholdExceeded(%d, %d, %d, %d) = %v, want %v",
					c.compactEntries, c.compactBytes, c.entries, c.bytes, got, c.want)
			}
		})
	}
}

// TestSegmentsAfter pins segmentsAfter's own pointer-identity contract
// directly, independent of the higher-level adoptCompaction tests above.
func TestSegmentsAfter(t *testing.T) {
	segA := newNodeSegment(t, 700)
	segB := newNodeSegment(t, 701)
	segC := newNodeSegment(t, 702)

	t.Run("exact match, no tail", func(t *testing.T) {
		tail, ok := segmentsAfter([]*snapshot.Segment{segA, segB}, []*snapshot.Segment{segA, segB})
		if !ok || len(tail) != 0 {
			t.Fatalf("segmentsAfter = (%v, %v), want (empty, true)", tail, ok)
		}
	})
	t.Run("prefix matches, tail follows", func(t *testing.T) {
		tail, ok := segmentsAfter([]*snapshot.Segment{segA, segB, segC}, []*snapshot.Segment{segA, segB})
		if !ok || len(tail) != 1 || tail[0] != segC {
			t.Fatalf("segmentsAfter = (%v, %v), want ([segC], true)", tail, ok)
		}
	})
	t.Run("current shorter than captured", func(t *testing.T) {
		_, ok := segmentsAfter([]*snapshot.Segment{segA}, []*snapshot.Segment{segA, segB})
		if ok {
			t.Fatal("segmentsAfter reported ok for a current shorter than captured")
		}
	})
	t.Run("prefix diverges", func(t *testing.T) {
		_, ok := segmentsAfter([]*snapshot.Segment{segC, segB}, []*snapshot.Segment{segA, segB})
		if ok {
			t.Fatal("segmentsAfter reported ok for a diverged prefix")
		}
	})
}
