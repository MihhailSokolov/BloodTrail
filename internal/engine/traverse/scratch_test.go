// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"math"
	"testing"
)

// TestScratchResetWrapSkipsEpochZero pins reset's wrap handling: epoch zero
// is the one value reset must never land on, because a mark array is zero
// wherever it has never been written -- a scratch running at epoch zero
// would read every untouched node as "set, at distance 0". The wrap clears
// the marks and continues from epoch one, so validity semantics hold across
// it.
func TestScratchResetWrapSkipsEpochZero(t *testing.T) {
	sc := newScratch(4)
	sc.epoch = math.MaxUint32
	sc.set(1, 5)

	sc.reset()

	if sc.epoch == 0 {
		t.Fatalf("epoch wrapped to zero; reset must skip it")
	}
	if _, ok := sc.get(2); ok {
		t.Fatalf("an unwritten node reads as set after the epoch wrapped")
	}
	if _, ok := sc.get(1); ok {
		t.Fatalf("a pre-wrap distance survived the reset that wrapped the epoch")
	}

	sc.set(3, 7)
	if d, ok := sc.get(3); !ok || d != 7 {
		t.Fatalf("get(3) = (%d, %v) after set post-wrap, want (7, true)", d, ok)
	}
}

// TestGetScratchSizing pins the pool's one size rule: whatever the pool
// held, the returned scratch has at least n valid slots -- an under-sized
// pooled scratch is dropped rather than handed out (it would index out of
// range against a snapshot that grew), and an over-sized one is fine (the
// extra slots are simply never indexed). Assertions are on the
// postcondition only, deliberately: sync.Pool makes no delivery promises,
// and this test must hold whether or not the Get actually returned the
// scratch the test just put.
func TestGetScratchSizing(t *testing.T) {
	putScratch(newScratch(2))
	sc := getScratch(4)
	if len(sc.mark) < 4 || len(sc.dist) < 4 {
		t.Fatalf("getScratch(4) returned a scratch with %d/%d slots, want at least 4", len(sc.mark), len(sc.dist))
	}

	putScratch(newScratch(8))
	sc = getScratch(4)
	if len(sc.mark) < 4 || len(sc.dist) < 4 {
		t.Fatalf("getScratch(4) after pooling an 8-slot scratch returned %d/%d slots, want at least 4", len(sc.mark), len(sc.dist))
	}
	sc.reset()
	sc.set(3, 2)
	if d, ok := sc.get(3); !ok || d != 2 {
		t.Fatalf("get(3) = (%d, %v) on a pool-sourced scratch, want (2, true)", d, ok)
	}
}
