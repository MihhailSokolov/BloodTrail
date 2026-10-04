// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestMaxWatermark pins the adoptions' resolvedDirtyGen comparison in
// isolation -- see maxWatermark's doc.
func TestMaxWatermark(t *testing.T) {
	if got := maxWatermark(5, 3); got != 5 {
		t.Fatalf("maxWatermark(5, 3) = %d, want 5 (candidate smaller than cur)", got)
	}
	if got := maxWatermark(5, 9); got != 9 {
		t.Fatalf("maxWatermark(5, 9) = %d, want 9 (candidate larger than cur)", got)
	}
	if got := maxWatermark(5, 5); got != 5 {
		t.Fatalf("maxWatermark(5, 5) = %d, want 5 (equal)", got)
	}
}

// TestWatermarkConvergedForRequiresBothConditions pins watermarkConverged's
// pure comparison: convergence needs the pg counter to equal applied AND
// no bumped scope left unresolved -- see its own doc for why either
// condition alone is unsound.
func TestWatermarkConvergedForRequiresBothConditions(t *testing.T) {
	cases := []struct {
		name      string
		pgCounter uint64
		applied   uint64
		inflight  int64
		want      bool
	}{
		{"equal counters, nothing in flight", 5, 5, 0, true},
		{"equal counters, one bumped scope still unresolved", 5, 5, 1, false},
		{"applied behind the pg counter", 6, 5, 0, false},
		{"applied ahead of the pg counter (should never happen; still false)", 5, 6, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := watermarkConvergedFor(tc.pgCounter, tc.applied, tc.inflight); got != tc.want {
				t.Fatalf("watermarkConvergedFor(%d, %d, %d) = %v, want %v", tc.pgCounter, tc.applied, tc.inflight, got, tc.want)
			}
		})
	}
}

// TestWatermarkTrustedForRequiresEveryCondition pins WatermarkTrusted's pure
// decision: all three conditions are independently necessary -- every
// watermark failure resolved by an adopted snapshot, the counter bookkeeping
// converged, and the replica serving. See WatermarkTrusted's own doc for why
// none of the three implies another.
func TestWatermarkTrustedForRequiresEveryCondition(t *testing.T) {
	cases := []struct {
		name      string
		dirty     uint64
		resolved  uint64
		converged bool
		state     int32
		want      bool
	}{
		{"no failure ever noted, converged, serving", 0, 0, true, stateServing, true},
		{"every failure resolved, converged, serving", 3, 3, true, stateServing, true},
		{"a failure not yet resolved by any adoption", 3, 2, true, stateServing, false},
		{"resolved, but the counters have not converged", 3, 3, false, stateServing, false},
		{"resolved and converged, but the replica is in fallback", 3, 3, true, stateFallback, false},
		{"nothing holds", 3, 1, false, stateFallback, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := watermarkTrustedFor(tc.dirty, tc.resolved, tc.converged, tc.state); got != tc.want {
				t.Fatalf("watermarkTrustedFor(%d, %d, %v, %d) = %v, want %v", tc.dirty, tc.resolved, tc.converged, tc.state, got, tc.want)
			}
		})
	}
}

// wantLedger asserts how far l accounts for every value, and whether exactly.
func wantLedger(t *testing.T, l *watermarkLedger, through uint64, exact bool, why string) {
	t.Helper()
	gotThrough, gotExact := l.resolvedThrough()
	if gotThrough != through || gotExact != exact {
		t.Fatalf("ledger = (through %d, exact %v), want (through %d, exact %v): %s", gotThrough, gotExact, through, exact, why)
	}
}

// TestAdvanceWatermarkAccountsOnlyForAContiguousPrefix pins what
// convergence rests on: a counter resolved ahead of a lower one never
// vouches for the lower one. A highest-resolved max took the later value as
// "caught up" and so absorbed a value this engine never resolved -- another
// server's bump, or its own bump still on the wire.
func TestAdvanceWatermarkAccountsOnlyForAContiguousPrefix(t *testing.T) {
	e := New(nil, nil, Config{})
	e.inflightBumps.Store(5)

	e.AdvanceWatermark(2)
	wantLedger(t, &e.appliedWatermark, 0, true, "2 resolved, 1 not")

	e.AdvanceWatermark(1)
	wantLedger(t, &e.appliedWatermark, 2, true, "1 fills the gap below 2")

	e.AdvanceWatermark(5)
	e.AdvanceWatermark(3)
	wantLedger(t, &e.appliedWatermark, 3, true, "4 still missing below 5")

	e.AdvanceWatermark(4)
	wantLedger(t, &e.appliedWatermark, 5, true, "4 joins 3 and 5")
	if got := e.inflightBumps.Load(); got != 0 {
		t.Fatalf("inflightBumps = %d after five resolutions of five bumps, want 0", got)
	}
}

// TestWatermarkLedgerMergesRunsAboveAGap pins the bookkeeping above a gap:
// values resolved out of order coalesce into runs, and the gap closing
// carries the prefix through all of them at once.
func TestWatermarkLedgerMergesRunsAboveAGap(t *testing.T) {
	var l watermarkLedger
	for _, c := range []uint64{9, 7, 3, 8, 5, 4, 10} {
		l.resolve(c)
	}
	if got, want := l.above, []counterRange{{3, 5}, {7, 10}}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("runs above the gap = %v, want %v", got, want)
	}
	wantLedger(t, &l, 0, true, "1, 2 and 6 unresolved")

	l.resolve(6)
	if got := l.above; len(got) != 1 || got[0] != (counterRange{3, 10}) {
		t.Fatalf("runs = %v after 6 resolved, want [{3 10}]", got)
	}
	l.resolve(8)
	if got := l.above; len(got) != 1 || got[0] != (counterRange{3, 10}) {
		t.Fatalf("runs = %v after resolving 8 a second time, want [{3 10}] unchanged", got)
	}
	l.resolve(2)
	l.resolve(1)
	wantLedger(t, &l, 10, true, "every value up to 10 resolved")
	if len(l.above) != 0 {
		t.Fatalf("runs = %v once the prefix reached them, want none", l.above)
	}
}

// TestWatermarkLedgerRebaseCoversEverythingAtOrBelowItsFloor pins what an
// adoption does: every value up to its floor is accounted for, however it
// got there, and values resolved above the floor still count.
func TestWatermarkLedgerRebaseCoversEverythingAtOrBelowItsFloor(t *testing.T) {
	var l watermarkLedger
	l.resolve(7)
	l.resolve(9)
	wantLedger(t, &l, 0, true, "nothing below 7 accounted for")

	l.rebase(6)
	wantLedger(t, &l, 7, true, "the floor reaches the run starting at 7")

	l.rebase(4)
	wantLedger(t, &l, 7, true, "a lower floor takes nothing back")

	l.rebase(8)
	wantLedger(t, &l, 9, true, "the floor covers 8, and 9 was resolved")

	l.resolve(8)
	wantLedger(t, &l, 9, true, "a value the floor already covers changes nothing")
}

// TestWatermarkLedgerIsInexactPastItsRangeCap pins the memory bound: a ledger
// whose gaps outnumber maxLedgerRanges stops keeping the runs above them,
// reports itself inexact -- never converged -- and becomes exact again only
// once a rebase covers every value it may have dropped.
func TestWatermarkLedgerIsInexactPastItsRangeCap(t *testing.T) {
	var l watermarkLedger
	// Every other value: each resolved one is its own run above a gap.
	for i := uint64(0); i <= maxLedgerRanges; i++ {
		l.resolve(2 * (i + 1))
	}
	wantLedger(t, &l, 0, false, "past the cap")
	if l.above != nil {
		t.Fatalf("a lost ledger kept %d runs, want none", len(l.above))
	}
	highest := uint64(2 * (maxLedgerRanges + 1))

	l.resolve(highest + 1)
	l.rebase(highest)
	wantLedger(t, &l, highest, false, "a floor below a value resolved while lost")

	l.rebase(highest + 1)
	wantLedger(t, &l, highest+1, true, "a floor covering every value ever resolved")

	l.resolve(highest + 3)
	l.resolve(highest + 2)
	wantLedger(t, &l, highest+3, true, "exact bookkeeping resumes after the rebase")
}

// TestAdoptRebuiltViewAndRebase pins that a rebuild's ledger rebase goes
// with its adoption and never without it: an adopted view whose load read
// the counter rebases; a refused one (a write applied during the load), or
// one whose load could not read the counter, leaves the ledger alone.
func TestAdoptRebuiltViewAndRebase(t *testing.T) {
	ctx := context.Background()
	loaded := loadedWatermark{counter: 5, counterOK: true}

	t.Run("adopted", func(t *testing.T) {
		e := New(nil, nil, Config{})
		if !e.adoptRebuiltViewAndRebase(ctx, snapshot.NewView(&snapshot.Snapshot{}), e.applyEpoch.Load(), 0, loaded) {
			t.Fatalf("adoptRebuiltViewAndRebase = false with no write applied during the load, want adopted")
		}
		wantLedger(t, &e.appliedWatermark, 5, true, "adopted with the counter its load read")
	})

	t.Run("refused", func(t *testing.T) {
		e := New(nil, nil, Config{})
		if e.adoptRebuiltViewAndRebase(ctx, snapshot.NewView(&snapshot.Snapshot{}), e.applyEpoch.Load()+1, 0, loaded) {
			t.Fatalf("adoptRebuiltViewAndRebase = true across an applied write, want refused")
		}
		wantLedger(t, &e.appliedWatermark, 0, true, "a refused snapshot vouches for nothing")
	})

	t.Run("counter not read", func(t *testing.T) {
		e := New(nil, nil, Config{})
		if !e.adoptRebuiltViewAndRebase(ctx, snapshot.NewView(&snapshot.Snapshot{}), e.applyEpoch.Load(), 0, loadedWatermark{}) {
			t.Fatalf("adoptRebuiltViewAndRebase = false, want adopted")
		}
		wantLedger(t, &e.appliedWatermark, 0, true, "no counter read, no rebase")
	})

	t.Run("a panicking adoption releases the lock", func(t *testing.T) {
		e := New(nil, nil, Config{})
		func() {
			defer func() { _ = recover() }()
			e.adoptRebuiltViewAndRebase(ctx, nil, e.applyEpoch.Load(), 0, loaded) // a nil view panics in adoptRebuiltView
			t.Fatalf("adopting a nil view did not panic; the test needs it to")
		}()
		if !e.watermarkRebaseMu.TryLock() {
			t.Fatalf("watermarkRebaseMu still held after the adoption panicked: every convergence read would block for good")
		}
		e.watermarkRebaseMu.Unlock()
		wantLedger(t, &e.appliedWatermark, 0, true, "no rebase past a panicked adoption")
	})
}

// TestWatermarkReadingConvergedAndUnaccounted pins the two verdicts a
// convergence reading gives: converged only with nothing in flight and every
// value up to PostgreSQL's counter accounted for; unaccounted only when a
// value is missing that no write of this process is still carrying.
func TestWatermarkReadingConvergedAndUnaccounted(t *testing.T) {
	cases := []struct {
		name                   string
		reading                watermarkReading
		converged, unaccounted bool
	}{
		{"caught up", watermarkReading{pgCounter: 5, through: 5, exact: true}, true, false},
		{"a write of this process in flight", watermarkReading{pgCounter: 5, inflight: 1, through: 4, exact: true}, false, false},
		{"a value nobody here resolved", watermarkReading{pgCounter: 5, through: 4, exact: true}, false, true},
		{"resolved past the counter read", watermarkReading{pgCounter: 5, through: 6, exact: true}, false, false},
		{"a lost ledger", watermarkReading{pgCounter: 5, through: 5}, false, true},
		{"a lost ledger with a write in flight", watermarkReading{pgCounter: 5, inflight: 2, through: 5}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.reading.converged(); got != tc.converged {
				t.Fatalf("converged() = %v, want %v", got, tc.converged)
			}
			if got := tc.reading.unaccounted(); got != tc.unaccounted {
				t.Fatalf("unaccounted() = %v, want %v", got, tc.unaccounted)
			}
		})
	}
}

// TestAdvanceWatermarkDecrementsInflightExactlyOnce pins the other half of
// AdvanceWatermark's bookkeeping: each call retires exactly one
// inflightBumps entry.
func TestAdvanceWatermarkDecrementsInflightExactlyOnce(t *testing.T) {
	e := New(nil, nil, Config{})
	e.inflightBumps.Store(2)

	e.AdvanceWatermark(1)
	if got := e.inflightBumps.Load(); got != 1 {
		t.Fatalf("inflightBumps = %d after one AdvanceWatermark call, want 1", got)
	}

	e.AdvanceWatermark(2)
	if got := e.inflightBumps.Load(); got != 0 {
		t.Fatalf("inflightBumps = %d after two AdvanceWatermark calls, want 0", got)
	}
}

// wantGens asserts the engine's three watermark trust generations
// (engine.go) in one place, since almost every test below is a statement
// about how one event moved exactly one of them.
func wantGens(t *testing.T, e *Engine, dirty, settled, resolved uint64) {
	t.Helper()

	gotDirty, gotSettled, gotResolved := e.dirtyGen.Load(), e.settledDirtyGen.Load(), e.resolvedDirtyGen.Load()
	if gotDirty != dirty || gotSettled != settled || gotResolved != resolved {
		t.Fatalf("watermark generations = (dirty %d, settled %d, resolved %d), want (dirty %d, settled %d, resolved %d)",
			gotDirty, gotSettled, gotResolved, dirty, settled, resolved)
	}
}

// TestNoteWatermarkBumpFailureOpensGenerationAndMarksScope pins the two
// halves NoteWatermarkBumpFailure must always do together: withdraw trust
// immediately (dirtyGen), and leave the write's own scope carrying the one
// mark that can ever settle it.
func TestNoteWatermarkBumpFailureOpensGenerationAndMarksScope(t *testing.T) {
	e := New(nil, nil, Config{})
	scope := NewWriteScope()

	e.NoteWatermarkBumpFailure(context.Background(), scope, errors.New("boom"))

	wantGens(t, e, 1, 0, 0)
	if e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = true immediately after a bump failure, want false")
	}
	if !scope.takeWatermarkBumpFailure() {
		t.Fatalf("scope carries no bump-failure mark after NoteWatermarkBumpFailure, so nothing could ever settle it")
	}
}

// TestNoteWatermarkBumpFailureCountsOncePerScopeAcrossRepeatedRetries is
// F1's regression: a write scope whose eager bump keeps failing on every one
// of its own later mutating calls -- exactly the shape ensureBumped's own
// bumped-exactly-once guard produces once a scope's FIRST bump fails
// (Watermark's bumped flag never becomes true for a scope whose bump never
// succeeds, so every later call on that same transaction/batch retries the
// identical failing bump) -- must open exactly ONE watermark trust
// generation, not one per retry. Before this fix, e.dirtyGen advanced on
// every call while settleWatermarkFailure's consume-once mark only ever
// settled one of them, so N failing calls on one scope left settledDirtyGen
// permanently N-1 behind dirtyGen: no amount of adoption could ever resolve
// it, distrusting the engine for good under this failure mode's ordinary
// shape.
func TestNoteWatermarkBumpFailureCountsOncePerScopeAcrossRepeatedRetries(t *testing.T) {
	ctx := context.Background()
	e := New(nil, nil, Config{})
	scope := NewWriteScope()

	if opened := e.NoteWatermarkBumpFailure(ctx, scope, errors.New("boom 1")); !opened {
		t.Fatalf("NoteWatermarkBumpFailure (first call for this scope) = false, want true")
	}
	wantGens(t, e, 1, 0, 0)

	if opened := e.NoteWatermarkBumpFailure(ctx, scope, errors.New("boom 2")); opened {
		t.Fatalf("NoteWatermarkBumpFailure (second call, same scope) = true, want false: the mark is already set")
	}
	wantGens(t, e, 1, 0, 0) // dirty stays at 1, not 2 -- the over-count this test guards against.

	// The write this scope belongs to eventually lands: its Apply settles
	// the one mark both failing calls shared, exactly once.
	e.Apply(ctx, scope)
	wantGens(t, e, 1, 1, 0)

	// A snapshot adopted after that settling resolves the generation --
	// recoverable, exactly as a single failure would be.
	adoptSnapshot(t, e, e.settledDirtyGen.Load())
	wantGens(t, e, 1, 1, 1)
	if !e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = false after the adoption, want true: two failing calls on one scope must resolve like one")
	}
}

// TestWatermarkFailureWithoutSettlingStaysUnresolved is the generation
// algebra's central claim, and the one a clearable dirty flag got wrong: a
// failure whose write has not settled yet cannot be resolved by ANY amount of
// unrelated activity, including a concurrent write that reaches full
// convergence and an adoption that happens while that write is still in
// flight. Only settling, and then an adoption whose captured generation
// includes it, restores trust.
func TestWatermarkFailureWithoutSettlingStaysUnresolved(t *testing.T) {
	ctx := context.Background()
	e := New(nil, nil, Config{})

	// W's eager bump fails; W itself is still in flight (its Apply has not
	// run), exactly the window the wrong-trust race lives in.
	scopeW := NewWriteScope()
	e.NoteWatermarkBumpFailure(ctx, scopeW, errors.New("boom"))
	wantGens(t, e, 1, 0, 0)

	// A concurrent write C, whose own bump succeeded, resolves completely.
	scopeC := NewWriteScope()
	scopeC.SetWatermark(7)
	e.Apply(ctx, scopeC)
	if e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = true after an unrelated write resolved, want false: W has not settled")
	}

	// A snapshot adopted while W is STILL in flight cannot resolve W either:
	// it captured the settled generation before W settled.
	adoptSnapshot(t, e, e.settledDirtyGen.Load())
	wantGens(t, e, 1, 0, 0)
	if e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = true after an adoption that predates W's own settling, want false")
	}

	// W's Apply finally runs: the write has landed, so its failure settles --
	// but an adoption is still owed before trust can return.
	e.Apply(ctx, scopeW)
	wantGens(t, e, 1, 1, 0)
	if e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = true on settling alone, want false: no adopted snapshot contains W yet")
	}

	// The next adoption captures the settled generation and resolves it.
	adoptSnapshot(t, e, e.settledDirtyGen.Load())
	wantGens(t, e, 1, 1, 1)
	if !e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = false after an adoption whose captured generation includes W, want true")
	}
}

// adoptSnapshot drives one successful adoption with settledGen as the
// generation rebuildOnce would have captured before its load began -- the
// engine-side half of a rebuild, without the PostgreSQL half a unit test
// cannot have. The epoch is read here rather than passed in, so the adoption
// always succeeds; TestAdoptionRefusedByEpochResolvesNothing covers the
// refused case.
func adoptSnapshot(t *testing.T, e *Engine, settledGen uint64) {
	t.Helper()

	if !e.adoptRebuiltView(context.Background(), snapshot.NewView(&snapshot.Snapshot{}), e.applyEpoch.Load(), settledGen) {
		t.Fatalf("adoptRebuiltView with an unchanged epoch = false, want true")
	}
}

// TestAdoptionRefusedByEpochResolvesNothing pins the interleaving where a
// watermark failure settles DURING a rebuild's load: Apply settles the
// failure and then bumps applyEpoch, so the rebuild that was loading across
// that moment is refused at publish time -- and a refused adoption must
// resolve nothing at all, since its snapshot is dropped rather than served.
// The next rebuild, started afterwards, resolves it.
func TestAdoptionRefusedByEpochResolvesNothing(t *testing.T) {
	ctx := context.Background()
	e := New(nil, nil, Config{})

	// A rebuild starts: it captures the epoch, then the settled generation,
	// then begins loading (rebuildOnce's own order).
	epochAtLoadStart := e.applyEpoch.Load()
	genAtLoadStart := e.settledDirtyGen.Load()

	// While it loads, W's bump failure is noted and W's Apply settles it.
	scopeW := NewWriteScope()
	e.NoteWatermarkBumpFailure(ctx, scopeW, errors.New("boom"))
	e.Apply(ctx, scopeW)
	wantGens(t, e, 1, 1, 0)

	if e.adoptRebuiltView(ctx, snapshot.NewView(&snapshot.Snapshot{}), epochAtLoadStart, genAtLoadStart) {
		t.Fatalf("adoptRebuiltView = true for a load that spanned an Apply, want false (the epoch moved)")
	}
	wantGens(t, e, 1, 1, 0)
	if e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = true after a REFUSED adoption, want false: nothing was published")
	}

	adoptSnapshot(t, e, e.settledDirtyGen.Load())
	if !e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = false after the next adoption, want true")
	}
}

// TestApplySettlesBumpFailureOnEveryBranch pins that the settle is
// unconditional on everything Apply decides afterwards: this engine is
// disabled, so Apply returns at its very first check, and the failure must
// settle anyway (Apply's own doc). The counter fold is checked alongside it
// for the same reason.
func TestApplySettlesBumpFailureOnEveryBranch(t *testing.T) {
	ctx := context.Background()
	e := New(nil, nil, Config{}) // Enabled false: Apply's earliest return

	scope := NewWriteScope()
	e.NoteWatermarkBumpFailure(ctx, scope, errors.New("boom"))

	e.Apply(ctx, scope)

	wantGens(t, e, 1, 1, 0)
	if got := e.applyEpoch.Load(); got != 1 {
		t.Fatalf("applyEpoch = %d after one Apply, want 1", got)
	}
}

// TestSettleWatermarkFailureConsumesTheMarkExactlyOnce pins the property that
// makes comparing two independent counters sound: a scope that somehow
// reaches a settling call site twice still advances settledDirtyGen once.
func TestSettleWatermarkFailureConsumesTheMarkExactlyOnce(t *testing.T) {
	e := New(nil, nil, Config{})
	scope := NewWriteScope()
	e.NoteWatermarkBumpFailure(context.Background(), scope, errors.New("boom"))

	if !e.settleWatermarkFailure(scope) {
		t.Fatalf("settleWatermarkFailure = false for a scope carrying a bump failure, want true")
	}
	if e.settleWatermarkFailure(scope) {
		t.Fatalf("settleWatermarkFailure = true on the second call for the same scope, want false")
	}
	wantGens(t, e, 1, 1, 0)

	if e.settleWatermarkFailure(NewWriteScope()) {
		t.Fatalf("settleWatermarkFailure = true for a scope with no failure, want false")
	}
	if e.settleWatermarkFailure(nil) {
		t.Fatalf("settleWatermarkFailure = true for a nil scope, want false")
	}
	wantGens(t, e, 1, 1, 0)
}

// TestResolveAbandonedWriteSettlesFailureAndFoldsCounter covers the driver's
// own error branches (driver.go's WriteTransaction/Run/... failure paths):
// a bump that succeeded folds in, and a bump that FAILED settles -- the write
// returned an error, so it left nothing uncounted behind it.
func TestResolveAbandonedWriteSettlesFailureAndFoldsCounter(t *testing.T) {
	ctx := context.Background()

	t.Run("bumped scope folds its counter", func(t *testing.T) {
		e := New(nil, nil, Config{})
		e.appliedWatermark.rebase(3)
		scope := NewWriteScope()
		scope.SetWatermark(4)
		e.inflightBumps.Store(1)

		e.ResolveAbandonedWrite(ctx, scope)

		wantLedger(t, &e.appliedWatermark, 4, true, "the abandoned write's counter resolved")
		if got := e.inflightBumps.Load(); got != 0 {
			t.Fatalf("inflightBumps = %d, want 0", got)
		}
		wantGens(t, e, 0, 0, 0)
	})

	t.Run("failed bump settles", func(t *testing.T) {
		e := New(nil, nil, Config{})
		scope := NewWriteScope()
		e.NoteWatermarkBumpFailure(ctx, scope, errors.New("boom"))

		e.ResolveAbandonedWrite(ctx, scope)

		wantGens(t, e, 1, 1, 0)
		if got := e.inflightBumps.Load(); got != 0 {
			t.Fatalf("inflightBumps = %d after resolving a scope that never bumped, want 0 (untouched)", got)
		}
	})

	t.Run("nil scope is a no-op", func(t *testing.T) {
		e := New(nil, nil, Config{})
		e.ResolveAbandonedWrite(ctx, nil)
		wantGens(t, e, 0, 0, 0)
	})
}

// TestNoteSelfSettlingWatermarkFailureSettlesItself pins the DDL-failure
// shape (ensureWatermarkTable's own recording call): a failure that guarded
// no write settles the instant it is noted, so the very next adoption
// resolves it -- see noteSelfSettlingWatermarkFailure's doc for why that is
// sound where it would not be for a bump failure.
func TestNoteSelfSettlingWatermarkFailureSettlesItself(t *testing.T) {
	e := New(nil, nil, Config{})

	e.noteSelfSettlingWatermarkFailure()

	wantGens(t, e, 1, 1, 0)
	if e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = true before any adoption, want false")
	}

	adoptSnapshot(t, e, e.settledDirtyGen.Load())
	if !e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = false after the next adoption, want true")
	}
}

// TestRelaunchAfterAdoption pins finishFallbackRebuild's own relaunch
// decision in isolation -- both reasons an adopted rebuild can leave work
// behind (with their differing urgency: a fallback state relaunches
// immediately, a trust-only gap goes through the rate limiter), the one
// case where it may exit for good, and the precedence when both reasons
// hold at once (fallback wins: a suspect replica is never made to wait on
// a trust rate limit).
func TestRelaunchAfterAdoption(t *testing.T) {
	cases := []struct {
		name     string
		state    int32
		settled  uint64
		resolved uint64
		want     relaunchKind
	}{
		{"serving, every settled failure resolved", stateServing, 2, 2, relaunchNone},
		{"raced back into fallback", stateFallback, 2, 2, relaunchRecovery},
		{"a failure settled after this rebuild's load began", stateServing, 3, 2, relaunchTrust},
		{"both (fallback wins)", stateFallback, 3, 2, relaunchRecovery},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := relaunchAfterAdoption(tc.state, tc.settled, tc.resolved); got != tc.want {
				t.Fatalf("relaunchAfterAdoption(%d, %d, %d) = %v, want %v", tc.state, tc.settled, tc.resolved, got, tc.want)
			}
		})
	}
}

// TestWatermarkTrustedNeedsAConvergenceProof pins the live half of the
// predicate: this engine's generations are trivially resolved (no failure was
// ever noted) and it is serving, but with no pool at all watermarkConverged
// cannot prove convergence, so trust must be false rather than assumed.
func TestWatermarkTrustedNeedsAConvergenceProof(t *testing.T) {
	e := New(nil, nil, Config{})

	if !e.watermarkGensResolved() {
		t.Fatalf("watermarkGensResolved = false on a fresh engine, want true")
	}
	if e.WatermarkTrusted(context.Background()) {
		t.Fatalf("WatermarkTrusted = true with no pg pool to prove convergence against, want false")
	}
}

// TestBumpWatermarkReturnsErrWatermarkUnavailableWithNoPool pins
// BumpWatermark's own nil-pool guard: no panic, and specifically
// ErrWatermarkUnavailable rather than some other error, so callers
// (write_observer.go's ensureBumped) can tell "never going to track a
// watermark" apart from "a real bump attempt failed".
func TestBumpWatermarkReturnsErrWatermarkUnavailableWithNoPool(t *testing.T) {
	e := New(nil, nil, Config{})

	counter, err := e.BumpWatermark(context.Background())
	if err != ErrWatermarkUnavailable {
		t.Fatalf("BumpWatermark error = %v, want ErrWatermarkUnavailable", err)
	}
	if counter != 0 {
		t.Fatalf("BumpWatermark counter = %d, want 0 on failure", counter)
	}
	if got := e.inflightBumps.Load(); got != 0 {
		t.Fatalf("inflightBumps = %d after a failed bump, want 0 (never touched)", got)
	}
}

// TestReadWatermarkReturnsErrWatermarkUnavailableWithNoPool mirrors
// BumpWatermark's identical guard on the read side.
func TestReadWatermarkReturnsErrWatermarkUnavailableWithNoPool(t *testing.T) {
	e := New(nil, nil, Config{})

	if _, err := e.ReadWatermark(context.Background()); err != ErrWatermarkUnavailable {
		t.Fatalf("ReadWatermark error = %v, want ErrWatermarkUnavailable", err)
	}
}

// TestEnsureWatermarkTableIsNoOpWithNoPool pins Start's own reliance on
// this: a unit test's Engine with no database behind it at all must not
// panic when ensureWatermarkTable runs (boot_test.go's Start-driven tests
// depend on exactly this), and must not record a failure it never had.
func TestEnsureWatermarkTableIsNoOpWithNoPool(t *testing.T) {
	e := New(nil, nil, Config{})
	e.ensureWatermarkTable(context.Background())

	wantGens(t, e, 0, 0, 0)
}

// TestNoteWatermarkBumpFailureInvalidatesTheSnapshotFile pins that a failed
// eager bump also takes the saved snapshot file out of play. The write it
// guarded reaches PostgreSQL without advancing the counter, so the file's
// stamp still matches what PostgreSQL reads: a boot after a hard stop would
// see a zero-sized gap, call the file fully covered, and adopt a replica
// permanently missing that write. The in-memory generation the failure opens
// is invisible to that next process, so the file itself has to go.
//
// A path that cannot be resolved (no default graph yet) must not panic and
// must still open the generation; the removal is best effort.
func TestNoteWatermarkBumpFailureInvalidatesTheSnapshotFile(t *testing.T) {
	dir := t.TempDir()
	e := New(nil, nil, Config{Enabled: true, SnapshotDir: dir})

	if opened := e.NoteWatermarkBumpFailure(context.Background(), NewWriteScope(), errors.New("boom")); !opened {
		t.Fatal("a bump failure must open a watermark generation")
	}
	wantGens(t, e, 1, 0, 0)
}

// TestInvalidateSnapshotFileRemovesTheFile covers the removal itself against
// a resolvable path, and that a missing file is not an error: the contract is
// that no adoptable file is left behind, not that one was found.
func TestInvalidateSnapshotFileRemovesTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph-1.btsnap")
	if err := os.WriteFile(path, []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}

	e := New(nil, nil, Config{Enabled: true, SnapshotDir: dir})

	e.removeSnapshotFile(context.Background(), path, invalidateUncountedWrite)
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("stat after invalidation = %v, want fs.ErrNotExist", err)
	}

	// Idempotent: nothing left to remove is a success, not a failure.
	e.removeSnapshotFile(context.Background(), path, invalidateUncountedWrite)
}

// TestInsertedSinceFile pins when a snapshot file's stamp shows rows
// inserted behind the counter's back: only when the counter still read, at
// start, exactly what the file was stamped with, and an id sequence had
// moved anyway. A counter that moved says nothing about the sequences --
// BloodTrail's own writes move both -- and nothing captured means no check.
func TestInsertedSinceFile(t *testing.T) {
	stamp := snapshot.Stamp{Watermark: 7, NodeIDSeq: 100, EdgeIDSeq: 50}
	cases := []struct {
		name string
		at   *startState
		want bool
	}{
		{"nothing captured", nil, false},
		{"nothing moved", &startState{counter: 7, nodeSeq: 100, edgeSeq: 50}, false},
		{"node ids drawn behind the counter", &startState{counter: 7, nodeSeq: 101, edgeSeq: 50}, true},
		{"edge ids drawn behind the counter", &startState{counter: 7, nodeSeq: 100, edgeSeq: 51}, true},
		{"a sequence reset behind the counter", &startState{counter: 7, nodeSeq: 0, edgeSeq: 50}, true},
		{"counted writes since the file", &startState{counter: 9, nodeSeq: 140, edgeSeq: 90}, false},
		{"counter behind the file (counterBehindFile's case)", &startState{counter: 5, nodeSeq: 140, edgeSeq: 90}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := insertedSinceFile(stamp, tc.at); got != tc.want {
				t.Fatalf("insertedSinceFile(%+v, %+v) = %v, want %v", stamp, tc.at, got, tc.want)
			}
		})
	}
}

// TestCounterBehindFile pins when the counter at start shows PostgreSQL gone
// back past a snapshot file's stamp: strictly behind it. Level is a quiet
// restart, ahead is BloodTrail's own writes since, and nothing captured
// means no check.
func TestCounterBehindFile(t *testing.T) {
	stamp := snapshot.Stamp{Watermark: 7}
	cases := []struct {
		name string
		at   *startState
		want bool
	}{
		{"nothing captured", nil, false},
		{"level with the stamp", &startState{counter: 7}, false},
		{"ahead of the stamp", &startState{counter: 9}, false},
		{"behind the stamp", &startState{counter: 6}, true},
		{"reset to zero", &startState{counter: 0}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := counterBehindFile(stamp, tc.at); got != tc.want {
				t.Fatalf("counterBehindFile(%+v, %+v) = %v, want %v", stamp, tc.at, got, tc.want)
			}
		})
	}
}

// TestFileRefusal pins the order and the reasons of the refusals that come
// before any counter is weighed: a file from another lineage is refused for
// that whatever its stamp says, and one from PostgreSQL's own lineage when
// the counter at start was behind its stamp, when rows were inserted behind
// the counter since it was written, or when nothing was captured at start at
// all -- the checks the capture feeds cannot be made then, and a file is
// refused rather than adopted unchecked.
func TestFileRefusal(t *testing.T) {
	lineage := snapshot.Lineage{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	other := snapshot.Lineage{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	stamp := snapshot.Stamp{Watermark: 7, NodeIDSeq: 100, EdgeIDSeq: 50}
	quiet := &startState{counter: 7, nodeSeq: 100, edgeSeq: 50}
	inserted := &startState{counter: 7, nodeSeq: 101, edgeSeq: 50}
	restored := &startState{counter: 5, nodeSeq: 90, edgeSeq: 50}

	cases := []struct {
		name        string
		fileLineage snapshot.Lineage
		at          *startState
		want        string
	}{
		{"same lineage, nothing inserted", lineage, quiet, ""},
		{"same lineage, nothing captured", lineage, nil, reasonStartStateUnknown},
		{"same lineage, rows inserted", lineage, inserted, reasonInsertedBehindCounter},
		{"same lineage, counter behind the stamp", lineage, restored, reasonCounterBehindFile},
		{"another lineage", other, quiet, reasonLineageChanged},
		{"another lineage and nothing captured", other, nil, reasonLineageChanged},
		{"another lineage and rows inserted", other, inserted, reasonLineageChanged},
		{"another lineage and counter behind the stamp", other, restored, reasonLineageChanged},
		{"no lineage at all", snapshot.Lineage{}, quiet, reasonLineageChanged},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New(nil, nil, Config{})
			if tc.at != nil {
				e.atStart.Store(tc.at)
			}
			reason, attrs := e.fileRefusal(tc.fileLineage, stamp, lineage)
			if reason != tc.want {
				t.Fatalf("fileRefusal reason = %q, want %q", reason, tc.want)
			}
			if (reason == "") != (len(attrs) == 0) {
				t.Fatalf("fileRefusal returned attrs %v with reason %q; attrs belong with a reason only", attrs, reason)
			}
		})
	}
}
