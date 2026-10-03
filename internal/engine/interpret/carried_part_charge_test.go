// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"testing"
)

// TestCarriedPartChargesItsMatchBeforeCloning: the WITH boundary's cross
// join matches the following Part once per carried row and then clones the
// carried row into every match, so one seed costs its match plus a full
// second copy of it. Only the clones were charged, and only after they had
// all been built -- so a match that already exceeds the live-row cap was
// doubled in memory before anything refused it. The match itself is now
// charged first, and the clone loop never starts.
func TestCarriedPartChargesItsMatchBeforeCloning(t *testing.T) {
	const groups = 600
	snap := carriedCrossJoinFixture(t, groups)
	q := planQuery(t, snap, `MATCH (u:User) WITH u MATCH (g:Group) RETURN u, g`)
	if len(q.Parts) != 2 {
		t.Fatalf("got %d Parts, want 2 (the query no longer has a WITH boundary)", len(q.Parts))
	}
	part := &q.Parts[1]

	seed := NewRow()
	seed.SetNode("u", 0)

	// Under the cap: the match and its clones are both served.
	meter := &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxLiveRows: groups + 1}}
	rows, err := runCarriedPart(&Env{Snap: snap}, part, meter, seed)
	if err != nil {
		t.Fatalf("runCarriedPart under a %d-row cap: %v", groups+1, err)
	}
	if len(rows) != groups {
		t.Fatalf("got %d rows, want %d (one per group)", len(rows), groups)
	}

	// Over the cap: refused, and refused on the match rather than on the
	// clone set the old order had already allocated.
	meter = &workMeter{budget: Budgets{MaxRows: 1_000_000, MaxLiveRows: groups - 1}}
	rows, err = runCarriedPart(&Env{Snap: snap}, part, meter, seed)
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("runCarriedPart = (%d rows, %v) under a %d-row cap, want ErrBudget", len(rows), err, groups-1)
	}
	if meter.peakRows > groups {
		t.Fatalf("peakRows = %d, want at most %d: the clone set was built before the cap was checked", meter.peakRows, groups)
	}
}
