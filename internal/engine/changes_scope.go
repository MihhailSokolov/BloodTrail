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
// fallback and sets e.watermarkDirty instead of calling SetWatermark in
// that case) -- either way, there is no counter here for
// Apply/AdvanceWatermark to fold in.
func (s *WriteScope) Watermark() (uint64, bool) {
	return s.watermark, s.watermarkBumped
}
