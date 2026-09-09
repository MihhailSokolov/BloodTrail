// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"testing"
)

// TestMaxWatermark pins AdvanceWatermark's own comparison in isolation --
// see maxWatermark's doc.
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

// TestAdvanceWatermarkMonotonicMax pins AdvanceWatermark's own max-advance
// behavior against the live atomic field: a smaller counter arriving after
// a larger one (the out-of-order completion concurrent bumped scopes can
// produce -- see AdvanceWatermark's own doc) must never regress
// appliedWatermark.
func TestAdvanceWatermarkMonotonicMax(t *testing.T) {
	e := New(nil, nil, Config{})

	e.AdvanceWatermark(context.Background(), 5)
	if got := e.appliedWatermark.Load(); got != 5 {
		t.Fatalf("appliedWatermark = %d after AdvanceWatermark(5), want 5", got)
	}

	e.AdvanceWatermark(context.Background(), 3)
	if got := e.appliedWatermark.Load(); got != 5 {
		t.Fatalf("appliedWatermark = %d after a smaller, later-resolving counter, want it to stay 5", got)
	}

	e.AdvanceWatermark(context.Background(), 9)
	if got := e.appliedWatermark.Load(); got != 9 {
		t.Fatalf("appliedWatermark = %d after AdvanceWatermark(9), want 9", got)
	}
}

// TestAdvanceWatermarkDecrementsInflightExactlyOnce pins the other half of
// AdvanceWatermark's bookkeeping: each call retires exactly one
// inflightBumps entry.
func TestAdvanceWatermarkDecrementsInflightExactlyOnce(t *testing.T) {
	e := New(nil, nil, Config{})
	e.inflightBumps.Store(2)

	e.AdvanceWatermark(context.Background(), 1)
	if got := e.inflightBumps.Load(); got != 1 {
		t.Fatalf("inflightBumps = %d after one AdvanceWatermark call, want 1", got)
	}

	e.AdvanceWatermark(context.Background(), 2)
	if got := e.inflightBumps.Load(); got != 0 {
		t.Fatalf("inflightBumps = %d after two AdvanceWatermark calls, want 0", got)
	}
}

// TestAdvanceWatermarkSkipsConvergedCheckWhenNotDirty pins the "no DB round
// trip in the common case" optimization watermarkDirty gates: this Engine
// has no pool at all (nil), so if AdvanceWatermark ever reached
// watermarkConverged's own ReadWatermark call here, it would return
// ErrWatermarkUnavailable cleanly (not panic) -- but the point of this test
// is that it must not even try when watermarkDirty is false, which nothing
// in this test ever sets.
func TestAdvanceWatermarkSkipsConvergedCheckWhenNotDirty(t *testing.T) {
	e := New(nil, nil, Config{})

	e.AdvanceWatermark(context.Background(), 1)

	if e.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty = true, want it to stay false: nothing in this test ever set it")
	}
}

// TestAdvanceWatermarkLeavesDirtySetWhenConvergenceCannotBeProven pins the
// other half of the dirty-clearing rule: when watermarkDirty IS set,
// AdvanceWatermark does re-check convergence (watermarkConverged, a live pg
// read) -- but with no pool at all, that read fails (ErrWatermarkUnavailable),
// so watermarkConverged safely reports false and dirty must stay set.
func TestAdvanceWatermarkLeavesDirtySetWhenConvergenceCannotBeProven(t *testing.T) {
	e := New(nil, nil, Config{})
	e.watermarkDirty.Store(true)

	e.AdvanceWatermark(context.Background(), 1)

	if !e.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty = false, want it to stay true: watermarkConverged can't prove convergence with no pg pool at all")
	}
}

// TestNoteWatermarkBumpFailureSetsDirty pins NoteWatermarkBumpFailure's own
// side effect in isolation.
func TestNoteWatermarkBumpFailureSetsDirty(t *testing.T) {
	e := New(nil, nil, Config{})

	e.NoteWatermarkBumpFailure(context.Background(), errors.New("boom"))

	if !e.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty = false after NoteWatermarkBumpFailure, want true")
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
// depend on exactly this).
func TestEnsureWatermarkTableIsNoOpWithNoPool(t *testing.T) {
	e := New(nil, nil, Config{})
	e.ensureWatermarkTable(context.Background())

	if e.watermarkDirty.Load() {
		t.Fatalf("watermarkDirty = true after ensureWatermarkTable with a nil pool, want it to stay false (a no-op, not a failure)")
	}
}
