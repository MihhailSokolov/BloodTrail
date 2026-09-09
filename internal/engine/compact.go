// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// maxSegments bounds how many delta Segments Apply lets a View's overlay
// stack grow to before collapsing it, synchronously, into one merged
// Segment (mergeSegmentTailIfNeeded). Every overlay accessor's first read
// of a View pays for a full MergeSegments pass over the whole stack
// (View.ensureDelta, snapshot/view.go) -- bounding the stack bounds that
// one-time cost, independent of how large cfg.CompactEntries/CompactBytes
// are set: compaction (this file's OTHER size bound) runs far less often
// and rebases onto a fresh base entirely, so this bound exists purely to
// keep the stack itself from growing unboundedly large in between two
// compactions.
//
// 32 is a deliberately generous bound -- far above what any single
// Apply-per-write workload should reach between two compactions at the
// default thresholds below -- chosen so this collapse is a rare safety
// net, not a steady-state cost center.
const maxSegments = 32

// DefaultCompactEntries and DefaultCompactBytes are
// BLOODTRAIL_COMPACT_ENTRIES/BLOODTRAIL_COMPACT_BYTES's built-in defaults
// (settings.go, root package): the delta size -- summed across every
// Segment layered on the engine's current View, in entries
// (Segment.NodeCount()+EdgeCount(), tombstones included) and approximate
// bytes (Segment.ApproxBytes()) -- past which maybeStartCompaction spawns a
// background compaction (see deltaSize and compactionThresholdExceeded).
//
// Exported, rather than kept as unexported constants private to this file,
// so the root package's settings.go can default to them directly instead
// of duplicating the numbers: both packages then have exactly one place,
// this one, that states what "large" means for a delta.
//
// Provisional: the plan that introduced these two flags calls for
// revisiting both once a dedicated write-throughput benchmark measures
// compaction cost at scale.
const (
	DefaultCompactEntries = 1_000_000
	DefaultCompactBytes   = 512 * size.Mebibyte
)

// deltaSize sums the entries (NodeCount()+EdgeCount(), tombstones
// included -- cheap, since both are just len() of the Segment's own id
// slices) and ApproxBytes() of every Segment in segs, without merging them
// first.
//
// This is a trigger heuristic, not the compaction itself: double-counting
// an id two segments both touch costs nothing worse than a slightly eager
// trigger, and computing this sum is far cheaper than computing (or
// reusing) the fully merged delta just to decide whether it is time to
// replace it with one.
func deltaSize(segs []*snapshot.Segment) (entries int, bytes uint64) {
	for _, seg := range segs {
		entries += seg.NodeCount() + seg.EdgeCount()
		bytes += seg.ApproxBytes()
	}
	return entries, bytes
}

// compactionThresholdExceeded reports whether entries/bytes have grown past
// the given compaction thresholds. Extracted as a pure function for direct
// unit testing, mirroring this package's other pure-decision extractions
// (saveSnapshotPreconditionsFor, fallbackRetryDelay, rebuildStillNeeded,
// snapshotFileTrustedAtBoot).
//
// Zero on either threshold means "no bound on that dimension" -- the same
// convention Config.MemoryLimit already uses (apply.go's own
// `if e.cfg.MemoryLimit > 0` guard) -- so an Engine built with a zero-value
// Config (every unit test in this package that constructs one with
// `Config{}` or `Config{Enabled: true}` and never mentions compaction at
// all) never trips this: both checks below are skipped entirely rather
// than comparing against a zero threshold that any nonempty delta would
// exceed. Production defaults to nonzero values for both (settings.go), so
// this only ever behaves as "never compact" for a Config that explicitly,
// or by test omission, leaves both at zero.
func compactionThresholdExceeded(compactEntries int, compactBytes size.Size, entries int, bytes uint64) bool {
	if compactEntries > 0 && entries > compactEntries {
		return true
	}
	if compactBytes > 0 && bytes > uint64(compactBytes) {
		return true
	}
	return false
}

// mergeSegmentTailIfNeeded is the synchronous half of this file's two size
// bounds: once view's segment stack has grown past maxSegments, it
// collapses the ENTIRE stack into one merged Segment (MergeSegments --
// oldest first, newest wins, exactly like the lazy per-read collapse every
// overlay accessor already performs, snapshot/view.go's ensureDelta) and
// publishes the result as the engine's new current View, replacing view.
//
// Called from Apply, still under applyMu, immediately after view itself
// was published (maintainAfterPublish, below): cheap (MergeSegments costs
// O(total entries across at most maxSegments+1 segments) -- a small, fixed
// amount of work under a lock every write already holds) and
// content-neutral (a merged stack and an unmerged one are equivalent under
// every View accessor -- MergeSegments' own doc), so folding it into the
// same critical section that just published view needs no synchronization
// of its own beyond that lock.
//
// This can invalidate a concurrently-running background compaction's own
// captured segment prefix (runCompaction/adoptCompaction, below): a
// compaction that captured view's segments BEFORE this collapse fires no
// longer finds that exact prefix in the View this produces (it now holds
// ONE merged Segment where the compaction's capture saw many). That is
// discovered and handled safely by adoptCompaction's own pointer-identity
// check, which simply discards the stale fold rather than mis-adopt it --
// see its doc (segmentsAfter). Losing that compaction's work to a rare
// mid-flight merge is an acceptable cost -- it is simply retried by the
// very write that next grows the delta past threshold again -- for keeping
// this collapse itself simple: replace everything, rather than track which
// prefix some other goroutine might currently be depending on.
//
// Returns the View the caller should treat as current from this point on:
// either view unchanged (SegmentCount() <= maxSegments, the common case),
// or the freshly collapsed and already-published replacement.
func (e *Engine) mergeSegmentTailIfNeeded(ctx context.Context, view *snapshot.View) *snapshot.View {
	segs := view.Segments()
	if len(segs) <= maxSegments {
		return view
	}

	merged := snapshot.NewView(view.Base()).WithSegment(snapshot.MergeSegments(segs))
	e.snap.Store(merged)

	e.cfg.Log.DebugContext(ctx, "bloodtrail: segment stack merged",
		slog.Int("segments_before", len(segs)),
		slog.Int("segments_after", merged.SegmentCount()),
	)
	return merged
}

// maybeStartCompaction spawns the background compaction goroutine
// (runCompaction) exactly when three things all hold: the engine is
// actually serving right now (state == stateServing -- a fallback recovery
// rebuild is about to load a whole fresh base of its own, which would make
// any compaction against the CURRENT base pointless work before it even
// starts), view's delta has grown past cfg.CompactEntries or
// cfg.CompactBytes (compactionThresholdExceeded), and no compaction is
// already running (the `compacting` CAS -- deliberately its own flag,
// separate from fallbackRebuilding: a compaction and a fallback rebuild are
// unrelated operations that happen to share nothing but the applyMu they
// each eventually take to publish, so serializing them against EACH OTHER
// via a shared flag, rather than only against a same-kind sibling, would be
// pure unneeded contention -- the two are kept from racing destructively by
// adoptCompaction's own state/base rechecks at publish time instead, not by
// preventing them from ever overlapping in the first place).
//
// Called from Apply, still under applyMu, right after publishing (and,
// where it fired, after mergeSegmentTailIfNeeded has already run) -- so the
// (base, segments) pair captured here for the spawned goroutine is exactly
// what the very next reader would see, and capturing it under the same lock
// that serializes every publish is what makes the capture race-free: no
// OTHER Apply can still be appending to view's own segments slice
// concurrently (WithSegment never mutates a View in place -- snapshot/
// view.go's own package doc), only ever build a newer View of its own on
// top of it.
//
// A fallback entered WHILE a compaction this call just started is running
// is NOT prevented here -- state is only checked at trigger time, not for
// the spawned goroutine's whole lifetime -- it is instead caught by
// adoptCompaction's own state recheck at publish time, which discards the
// fold rather than adopt it over a replica that might no longer be
// trustworthy. See that method's doc.
func (e *Engine) maybeStartCompaction(ctx context.Context, view *snapshot.View) {
	if e.state.Load() != stateServing {
		return
	}

	segs := view.Segments()
	if len(segs) == 0 {
		return
	}

	entries, bytes := deltaSize(segs)
	if !compactionThresholdExceeded(e.cfg.CompactEntries, e.cfg.CompactBytes, entries, bytes) {
		return
	}

	if !e.compacting.CompareAndSwap(false, true) {
		return
	}

	e.cfg.Log.DebugContext(ctx, "bloodtrail: compaction triggered",
		slog.Int("segments", len(segs)),
		slog.Int("entries", entries),
		slog.Uint64("bytes", bytes),
	)
	go e.runCompaction(view.Base(), segs)
}

// maintainAfterPublish runs the two size-triggered maintenance steps every
// successful publish must consider, in order. It is Apply's own tail
// (apply.go), and also this package's synthetic-apply test seam
// (compact_test.go, compact_race_test.go): those tests drive a "write" by
// building a Segment directly and publishing it exactly the way Apply
// does, bypassing read-back (which needs a live PostgreSQL pool Apply's own
// unit tests likewise cannot stand up -- see e.g. apply_test.go's
// nil-pool Engines) -- so this method is what lets them exercise the real
// trigger/merge/compaction logic rather than a reimplementation of it.
//
// view must already be the engine's current, published View (e.snap.Store
// already called) when this runs, and the caller must still hold applyMu:
// see mergeSegmentTailIfNeeded's and maybeStartCompaction's own docs for
// why both size checks need to run inside that same critical section.
func (e *Engine) maintainAfterPublish(ctx context.Context, view *snapshot.View) {
	current := e.mergeSegmentTailIfNeeded(ctx, view)
	e.maybeStartCompaction(ctx, current)
}

// segmentsAfter reports whether current's first len(captured) elements are
// IDENTICAL to captured -- same length, same *Segment pointers, in the same
// order -- and if so, returns the suffix beyond that prefix: every segment
// appended to the View since captured was taken.
//
// Pointer identity, not deep equality, is the right check, and is sound
// because of one property View.WithSegment guarantees by construction
// (snapshot/view.go): every View's segments slice is APPEND-ONLY across the
// whole lineage descended from one base -- WithSegment always allocates a
// fresh backing array and copies the receiver's own elements into its
// prefix verbatim; it never mutates or reorders them. So for any two Views
// descended from the same lineage, one's segments slice is always either an
// exact prefix of the other's (by pointer, element for element) or the two
// have diverged onto different bases entirely (a rebuild) -- there is no
// third case where the same prefix LENGTH holds different segments.
// adoptCompaction's caller already rules out the diverged-base case (its
// own current.Base() == capturedBase check) before this ever runs, so by
// the time this is called, a length/pointer mismatch here can only mean
// mergeSegmentTailIfNeeded collapsed across the captured boundary while the
// fold this is validating was in flight (see its own doc) -- and this
// reports that honestly as "no valid tail" rather than guessing at one.
func segmentsAfter(current, captured []*snapshot.Segment) (tail []*snapshot.Segment, ok bool) {
	if len(current) < len(captured) {
		return nil, false
	}
	for i, seg := range captured {
		if current[i] != seg {
			return nil, false
		}
	}
	return current[len(captured):], true
}

// adoptCompaction publishes folded -- Fold(capturedBase, capturedSegs)'s
// result, computed outside applyMu by runCompaction -- as the engine's new
// base, carrying forward (rebased onto it) whatever segments were appended
// to the View while that fold ran. Reports whether it actually adopted.
//
// Three checks, any one of which refuses adoption and discards the fold
// entirely (logged by the caller, runCompaction) rather than publish
// something unsound:
//
//  1. current.Base() must still be capturedBase, by pointer. A rebuild
//     (adoptRebuiltView, engine.go) publishes a whole new base loaded
//     fresh from PostgreSQL, completely independent of the one this
//     compaction folded; adopting this fold over that base would silently
//     discard every write the rebuild's own base already reflects but this
//     fold never saw.
//  2. state must be stateServing. An engine that fell into fallback while
//     this fold ran carries a View that is not known to match PostgreSQL
//     (apply.go's enterFallback) -- exactly the same reasoning
//     saveSnapshotPreconditionsFor's own doc walks through for SaveSnapshot,
//     applied here to a compaction's adoption instead of a file write.
//     Refusing is always safe: the recovery rebuild that follows publishes
//     its own trustworthy base regardless of what this compaction does.
//  3. segmentsAfter(current.Segments(), capturedSegs) must find capturedSegs
//     as an exact, identical prefix -- see its own doc for the one thing
//     that can make this fail (a concurrent mergeSegmentTailIfNeeded
//     collapsing across the capture boundary) and why finding it false is
//     always the correct, safe answer rather than a bug.
//
// No epoch check, and deliberately: applyEpoch exists so a rebuild can tell
// whether a WRITE landed while it loaded (adoptRebuiltView's own doc) --
// compaction folds no new write into anything, it only repacks writes
// already published, so it neither needs to consult applyEpoch nor may
// advance it (advancing it would make a concurrent rebuild's own epoch
// check spuriously see "a write happened" and retry for no reason, even
// though nothing this compaction did could ever be the write such a retry
// would be looking for).
//
// Publishing carries no segments forward when the tail is empty (the
// overwhelmingly common case: nothing else was appended during the fold),
// producing a bare NewView(folded) rather than an overlay View wrapping one
// pointlessly empty merged segment.
func (e *Engine) adoptCompaction(capturedBase *snapshot.Snapshot, capturedSegs []*snapshot.Segment, folded *snapshot.Snapshot) bool {
	e.applyMu.Lock()
	defer e.applyMu.Unlock()

	current := e.snap.Load()
	if current == nil || current.Base() != capturedBase {
		return false
	}
	if e.state.Load() != stateServing {
		return false
	}

	tail, ok := segmentsAfter(current.Segments(), capturedSegs)
	if !ok {
		return false
	}

	newView := snapshot.NewView(folded)
	if len(tail) > 0 {
		newView = newView.WithSegment(snapshot.MergeSegments(tail))
	}
	e.snap.Store(newView)
	e.compactionCount.Add(1)
	return true
}

// runCompaction is the compaction goroutine body maybeStartCompaction
// spawns: fold (capturedBase, capturedSegs) into one fresh Snapshot outside
// applyMu (Fold is a pure, read-only pass over its inputs -- nothing about
// it needs the lock, and holding it across a fold this size would block
// every Apply for as long as the fold takes, defeating the entire point of
// running compaction in the background), then hand the result to
// adoptCompaction to publish -- or discard, per its own checks.
//
// Two bgCtx checks bound how much of this a shutdown-in-flight (Stop,
// boot.go) actually still does: one before the fold starts, one after it
// finishes but before adoption. Stop's own contract is "cancel, do not
// wait" (its own doc), and this goroutine is not made an exception to
// that: instead of Stop blocking on it, this is written to notice bgCtx's
// cancellation at its own next checkpoint and quietly discard whatever it
// was doing -- exactly the shape runFallbackRebuild's and runBootLoad's own
// context checks already have. The one place this cannot help is the fold
// itself: Fold is a single CPU-bound pass with no cancellation points of
// its own, so a Stop landing mid-fold is only noticed once that fold
// returns, not before -- acceptable, since the fold's cost is bounded by
// the very delta size that triggered this compaction, never unbounded.
//
// Always clears `compacting` on return (deferred, first) so a failed or
// discarded compaction never wedges every future trigger shut.
//
// A successful adoption's own file save (SaveSnapshot, persist.go) is
// best-effort, and needs no special handling to stay correct here: it runs
// its OWN fresh probe/commit cycle (saveSnapshotProbe/saveSnapshotCommit)
// against whatever is current by the time it acquires applyMu -- which may
// already be newer than what this call just adopted, if another Apply or
// even another compaction trigger ran in between -- so this call never
// needs to hand SaveSnapshot anything about what was just adopted, and a
// racing write can never make it persist something stale (the epoch guard
// saveSnapshotCommit's own doc proves in full covers this exactly as it
// covers every other SaveSnapshot caller). A failure here is logged and
// otherwise ignored, the same tolerance Driver.Close's own SaveSnapshot
// call extends to it.
func (e *Engine) runCompaction(capturedBase *snapshot.Snapshot, capturedSegs []*snapshot.Segment) {
	defer e.compacting.Store(false)

	start := time.Now()
	entries, bytes := deltaSize(capturedSegs)
	e.cfg.Log.InfoContext(e.bgCtx, "bloodtrail: compaction started",
		slog.Int("segments", len(capturedSegs)),
		slog.Int("entries", entries),
		slog.Uint64("bytes", bytes),
	)

	if e.bgCtx.Err() != nil {
		e.cfg.Log.InfoContext(e.bgCtx, "bloodtrail: compaction discarded", slog.String("reason", "engine stopping"))
		return
	}

	folded, err := snapshot.Fold(capturedBase, capturedSegs)
	if err != nil {
		e.cfg.Log.WarnContext(e.bgCtx, "bloodtrail: compaction failed", slog.Any("error", err))
		return
	}

	if e.bgCtx.Err() != nil {
		e.cfg.Log.InfoContext(e.bgCtx, "bloodtrail: compaction discarded", slog.String("reason", "engine stopping"))
		return
	}

	if !e.adoptCompaction(capturedBase, capturedSegs, folded) {
		e.cfg.Log.InfoContext(e.bgCtx, "bloodtrail: compaction discarded",
			slog.String("reason", "base or segment stack changed while folding"),
			slog.Duration("duration", time.Since(start)),
		)
		return
	}

	e.cfg.Log.InfoContext(e.bgCtx, "bloodtrail: compaction finished",
		slog.Int("nodes", folded.NodeCount()),
		slog.Int("edges", folded.EdgeCount()),
		slog.Duration("duration", time.Since(start)),
	)

	if err := e.SaveSnapshot(e.bgCtx); err != nil {
		e.cfg.Log.WarnContext(e.bgCtx, "bloodtrail: compaction snapshot save failed", slog.Any("error", err))
	}
}
