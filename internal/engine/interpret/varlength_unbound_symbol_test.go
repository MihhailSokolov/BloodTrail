// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestVarLengthTrailWalksRejectUnboundSymbol is the variable-length
// counterpart of TestFixedStepExpansionRejectsUnboundSymbol
// (named_chain_limit_test.go): both trail walkers in expand.go open with a
// boundNode call on the seed row -- the forward walker on step.FromSym, the
// reverse walker on step.ToSym -- and neither guard has a test of its own,
// because every caller in the package binds the symbol before it calls (the
// forward walker is fed scanAnchorHinted's own rows, the reverse walker the
// far-endpoint scan's). Those callers are what the guard is FOR: it catches a
// future one whose seed rows were scanned for a different symbol, so it has to
// be asserted directly rather than through a query.
//
// What it must not do is read the missing binding as dense node 0 and walk
// from whatever node 0 happens to reach: that is a SERVE -- the query answers
// from the replica instead of declining to PostgreSQL -- and what it serves is
// whatever node 0's neighbourhood says, which has nothing to do with the
// pattern. Measured with both guards replaced by a defaulting read, this
// fixture serves zero rows rather than an error for both walkers (dense node 0
// is database id 1, the Computer branchingChainViews' own doc calls out as
// unreachable), so the wrong answer here is a confidently empty one.
//
// The closing bound-seed walk is what keeps that from being a vacuous pin: the
// same walker on a properly bound seed does serve trails from this fixture, so
// "no rows" is not the only thing it can produce.
//
// Both views are exercised: the guard reads the Row, not the snapshot, but an
// overlay is the steady state of a live replica and costs nothing to cover.
func TestVarLengthTrailWalksRejectUnboundSymbol(t *testing.T) {
	for viewName, snap := range branchingChainViews(t) {
		t.Run(viewName, func(t *testing.T) {
			env := &Env{Snap: snap}
			part := &planQuery(t, snap, `MATCH (u:User)-[:MemberOf*1..2]->(g:Group) RETURN u, g`).Parts[0]
			step := &part.Chains[0]
			if step.Range.Max == 0 {
				t.Fatalf("fixture query did not plan a variable-length step (range %+v)", step.Range)
			}

			// The continuation sets the real callers resolve once per
			// component, so each walker is entered exactly as it is in
			// production -- only the seed row's binding differs.
			forwardContIDs, err := varLengthWalkSetup(env, step, true)
			if err != nil {
				t.Fatalf("varLengthWalkSetup(outgoing): %v", err)
			}
			reverseContIDs, err := varLengthWalkSetup(env, step, false)
			if err != nil {
				t.Fatalf("varLengthWalkSetup(incoming): %v", err)
			}

			// Each walker's seed row binds the OTHER endpoint only, so the
			// symbol the walk itself starts from is the one that is missing.
			onlyTo := NewRow()
			onlyTo.SetNode(step.ToSym, 0)
			onlyFrom := NewRow()
			onlyFrom.SetNode(step.FromSym, 0)

			if rows, err := expandVarLengthTrailsForSeed(env, &workMeter{budget: generousBudget},
				step, part.Nodes[step.ToSym], onlyTo, "", forwardContIDs); !errors.Is(err, errUnboundSymbol) {
				t.Errorf("expandVarLengthTrailsForSeed with %s unbound: %d row(s), err = %v, want errUnboundSymbol",
					step.FromSym, len(rows), err)
			}

			if rows, err := expandVarLengthTrailsToSeed(env, &workMeter{budget: generousBudget},
				step, part.Nodes[step.FromSym], onlyFrom, "", 0, reverseContIDs); !errors.Is(err, errUnboundSymbol) {
				t.Errorf("expandVarLengthTrailsToSeed with %s unbound: %d row(s), err = %v, want errUnboundSymbol",
					step.ToSym, len(rows), err)
			}

			// The same walkers, on properly bound seeds, do serve trails from
			// this fixture -- so the errors above are the guards firing, not
			// an empty graph answering.
			seed := NewRow()
			seed.SetNode(step.FromSym, denseNodeOf(t, snap, 3))
			rows, err := expandVarLengthTrailsForSeed(env, &workMeter{budget: generousBudget},
				step, part.Nodes[step.ToSym], seed, "", forwardContIDs)
			if err != nil {
				t.Fatalf("expandVarLengthTrailsForSeed on a bound seed: %v", err)
			}
			if len(rows) == 0 {
				t.Fatal("expandVarLengthTrailsForSeed served no trail from a bound User seed; the fixture no longer distinguishes a guard from an empty answer")
			}
		})
	}
}

// denseNodeOf resolves database id to its dense node in snap, failing the
// test when the fixture does not hold it.
func denseNodeOf(t *testing.T, snap *snapshot.View, id uint64) snapshot.NodeID {
	t.Helper()

	dense, ok := snap.Dense(id)
	if !ok {
		t.Fatalf("fixture does not hold database node %d", id)
	}
	return dense
}
