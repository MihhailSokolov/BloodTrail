// SPDX-License-Identifier: Apache-2.0

package engine

import "testing"

// TestSaveSnapshotPreconditionsForRequiresEveryCondition pins
// saveSnapshotPreconditionsFor's own predicate, mirroring
// TestWatermarkTrustedForRequiresEveryCondition's identical table shape
// (watermark_test.go) for WatermarkTrusted's own pure decision.
//
// The "fallback, otherwise clean and converged" case is the one worth
// reading twice: it pins the exact scenario saveSnapshotPreconditionsFor's
// own doc walks through in full -- an engine caught in stateFallback by an
// everyday ChangeSet fallback record (Run/WipeGraph/SetDefaultGraph, or a
// read-back/segment-build failure inside Apply) can have fully "clean"
// watermark generations and a fully converged counter at the same time,
// because dirtyGen/resolvedGen and watermarkConverged track a write's
// COUNTER bookkeeping, never whether its DATA effect could be replayed.
// Without the state==stateServing half of this predicate, SaveSnapshot
// would fold and persist that stale View anyway, stamped with a watermark
// a later boot would consider a perfect, trustworthy match.
func TestSaveSnapshotPreconditionsForRequiresEveryCondition(t *testing.T) {
	cases := []struct {
		name      string
		state     int32
		dirty     uint64
		resolved  uint64
		converged bool
		want      bool
	}{
		{"serving, no failure ever noted, converged", stateServing, 0, 0, true, true},
		{"serving, every failure resolved, converged", stateServing, 3, 3, true, true},
		{"serving, a failure not yet resolved by any adoption", stateServing, 3, 2, true, false},
		{"serving, resolved but the counters have not converged", stateServing, 3, 3, false, false},
		{"fallback, otherwise clean and converged", stateFallback, 3, 3, true, false},
		{"fallback, nothing holds", stateFallback, 3, 1, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := saveSnapshotPreconditionsFor(tc.state, tc.dirty, tc.resolved, tc.converged); got != tc.want {
				t.Fatalf("saveSnapshotPreconditionsFor(%d, %d, %d, %v) = %v, want %v",
					tc.state, tc.dirty, tc.resolved, tc.converged, got, tc.want)
			}
		})
	}
}
