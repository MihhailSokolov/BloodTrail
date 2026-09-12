// SPDX-License-Identifier: Apache-2.0
package main

import (
	"testing"
	"time"
)

// TestMeasuredPathThresholds pins the enforced bars to their documented
// values, mirroring builderbench's TestMeasuredEngineAbsoluteCaps: the pair
// bar is evidence-based (worst measured 5M p95 103.7ms x ~1.75 -> 180ms,
// derivation and run table in the README and on the constant itself), so
// changing it requires fresh 5M-scale evidence recorded alongside the
// change -- a silent edit fails here.
func TestMeasuredPathThresholds(t *testing.T) {
	if pairP95Threshold != 180*time.Millisecond {
		t.Errorf("pairP95Threshold = %v, want 180ms (worst measured 5M p95 103.7ms x ~1.75; re-derive with fresh evidence before moving it)", pairP95Threshold)
	}
	if domainAdminsThreshold != 5*time.Second {
		t.Errorf("domainAdminsThreshold = %v, want 5s", domainAdminsThreshold)
	}
}
