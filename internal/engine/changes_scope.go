// SPDX-License-Identifier: Apache-2.0

package engine

// WriteScope carries the ChangeSet (changes.go) a driver-level write
// accumulates as it happens, from the moment write_observer.go's overrides
// start recording onto it until Apply (apply.go) reads it back once the
// write commits.
//
// This type used to also carry the kind-scoped Touch*/Delete*/Upsert*
// bookkeeping the retired freshness-marks machinery needed (see the removed
// marks.go, and NoteWrite alongside it) -- a write's blast radius pinned
// down as which node/edge KINDS it touched, for a generation-scoped
// cleanliness check nothing consults any more (write-through replaced it
// with the SERVING/FALLBACK state, apply.go). That bookkeeping is gone;
// this type survives purely as the ChangeSet's carrier, which is the shape
// every write_observer.go override and Apply's own read-back path still
// need: the write's actual read-back keys, not merely which kinds it
// touched.
//
// The zero value returned by NewWriteScope is empty (Empty() is true) and
// ready for its Changes() accessor's Record* methods to be called.
//
// Not safe for concurrent use: a WriteScope is meant to be built by the one
// goroutine handling a single write (transaction or batch) and handed to
// Apply once that write commits, the same way a database transaction itself
// is single-goroutine.
type WriteScope struct {
	changes ChangeSet

	// watermark and watermarkBumped are SetWatermark's own storage -- see
	// its doc, and Watermark's, for what they hold and who reads/writes
	// them.
	watermark       uint64
	watermarkBumped bool

	// watermarkBumpFailed records that this write's own eager bump genuinely
	// failed (NoteWatermarkBumpFailure, watermark.go) -- the mutually
	// exclusive opposite of watermarkBumped, and the only thing that can
	// ever settle the watermark trust generation that failure opened. It is
	// CONSUMED, not merely read, by takeWatermarkBumpFailure below.
	watermarkBumpFailed bool
}

// NewWriteScope returns an empty WriteScope, ready for its Changes()
// accessor's Record* methods to be called.
func NewWriteScope() *WriteScope {
	return &WriteScope{}
}

// Empty reports whether scope's ChangeSet has recorded anything at all --
// equivalent to scope.Changes().Empty(). Apply's own no-op check
// (apply.go) consults the ChangeSet directly rather than this method, but
// wrote() (write_observer.go's observingTransaction/observingBatch) keys on
// this exact question to decide whether a write has landed onto scope since
// it began.
func (s *WriteScope) Empty() bool {
	return s.changes.Empty()
}

// Changes returns the ChangeSet write_observer.go's overrides populate --
// see ChangeSet's own doc (changes.go) for what it records and why. The
// returned pointer aliases s's own storage: every observer override in
// write_observer.go calls s.Changes().RecordXxx(...) to populate it, and
// Apply (apply.go) reads it back the same way once it has been handed s.
// The zero value of WriteScope already carries a ready-to-use zero-value
// ChangeSet, so this never needs its own lazy-init check.
func (s *WriteScope) Changes() *ChangeSet {
	return &s.changes
}

// SetWatermark records that this scope's write already bumped the pg
// watermark counter (watermark.go's BumpWatermark) to counter -- called
// once, by whichever call site made the eager bump (the root package's
// write_observer.go ensureBumped, for the first mutating call of a
// transaction/batch, or driver.go's own top-of-method bump for
// Run/WipeGraph/SetDefaultGraph/DeleteNodesByKinds/
// DeleteRelationshipsByKinds), so that Apply and AdvanceWatermark
// (apply.go, watermark.go) can later fold counter into
// e.appliedWatermark's monotonic max and retire this scope's
// e.inflightBumps entry exactly once, no matter how the write this scope
// belongs to eventually turns out.
//
// A second call is not expected -- every call site checks Watermark's own
// bumped flag first -- and simply overwrites: there is no dedup key to
// enforce here the way ChangeSet's Record* methods have one, since this is
// meant to be set at most once per scope by construction, not accumulated.
func (s *WriteScope) SetWatermark(counter uint64) {
	s.watermark = counter
	s.watermarkBumped = true
}

// Watermark returns the counter SetWatermark last recorded, and whether it
// was ever called at all. false means this scope's eager bump either never
// ran, or ran and failed (BumpWatermark's own caller records a ChangeSet
// fallback and calls NoteWatermarkBumpFailure instead of SetWatermark in that
// case, which is what markWatermarkBumpFailed below records) -- either way,
// there is no counter here for Apply/AdvanceWatermark to fold in.
func (s *WriteScope) Watermark() (uint64, bool) {
	return s.watermark, s.watermarkBumped
}

// markWatermarkBumpFailed records that this scope's own eager bump genuinely
// failed, so that whichever call site later learns this write's outcome is
// final in PostgreSQL (Apply, or ResolveAbandonedWrite for a write that
// produced no effect) can settle the watermark trust generation that failure
// opened. Called only by NoteWatermarkBumpFailure (watermark.go), which
// advances that generation in the same breath -- the two must not drift
// apart, which is why neither is exported on its own.
//
// Reports whether this call is the one that SET the mark -- true only the
// first time it is called on a given scope; false on every later call while
// the mark is still unconsumed. A single write scope's mutating calls all
// share one scope (ensureBumped's own "bumped exactly once" guard notwithstanding
// -- a scope whose first bump failed has no way to become bumped=true, so its
// later mutating calls retry the same failing bump and fail again), so a
// scope whose bump keeps failing must open exactly one watermark trust
// generation, not one per retry: NoteWatermarkBumpFailure uses this return
// value to advance e.dirtyGen only on the transition, keeping it in lockstep
// with settleWatermarkFailure's own once-per-scope consume
// (takeWatermarkBumpFailure) -- both counters move by exactly one per scope
// that ever fails, however many times that scope's own bump is retried.
func (s *WriteScope) markWatermarkBumpFailed() bool {
	if s.watermarkBumpFailed {
		return false
	}
	s.watermarkBumpFailed = true
	return true
}

// takeWatermarkBumpFailure reports whether this scope carries an unsettled
// bump failure, CONSUMING it: a second call returns false.
//
// Consuming rather than merely reading is what makes settleWatermarkFailure's
// counter (e.settledDirtyGen, watermark.go) exact. That counter is only
// meaningful compared against e.dirtyGen -- equality means "every failure ever
// noted has settled" -- so counting one failure twice would push settled past
// dirty and make trust unrecoverable, exactly as counting it zero times would
// make trust unearned. A scope reaching both settling call sites, or an Apply
// running twice for one scope, therefore still counts once.
func (s *WriteScope) takeWatermarkBumpFailure() bool {
	failed := s.watermarkBumpFailed
	s.watermarkBumpFailed = false
	return failed
}
