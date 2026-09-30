// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// newEdgeSegment builds a one-edge-upsert Segment, the delta a write that
// created edge id (start -> end) publishes.
func newEdgeSegment(id, start, end uint64, kind snapshot.KindID) *snapshot.Segment {
	var b snapshot.SegmentBuilder
	b.AddEdgeState(id, start, end, kind)
	return b.Build()
}

// TestCompactionKeepsADeltaEdgeWhoseEndpointAppliesLater pins the one
// window in which a delta edge legitimately points at a node the replica
// does not know yet: two writes whose Applies ran in the opposite order to
// their commits. The node's write committed first and the edge's write
// second (PostgreSQL would not have accepted the edge otherwise), but the
// edge's Apply published first; until the node's segment lands, the overlay
// keeps the edge record and simply cannot show it. A compaction that
// captured the stack in that window must carry the edge forward rather than
// drop it: the node arrives as the fold's rebased tail, and the edge then
// has to be there to be seen.
func TestCompactionKeepsADeltaEdgeWhoseEndpointAppliesLater(t *testing.T) {
	ctx := context.Background()
	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(buildApplyView(t))

	// The edge's write applies first: node 100 is not in the replica yet.
	v1 := publishAppend(ctx, e, newEdgeSegment(40, 1, 100, applyKindAdminTo))
	if _, _, _, ok := v1.EdgeStateByID(40); ok {
		t.Fatal("edge 40 visible before its endpoint's write applied (fixture assumption)")
	}

	// A compaction captures the stack exactly as maybeStartCompaction does...
	capturedBase, capturedSegs := v1.Base(), v1.Segments()

	// ...and the node's write applies while it folds.
	reference := publishAppend(ctx, e, newNodeSegment(t, 100))
	if _, _, _, ok := reference.EdgeStateByID(40); !ok {
		t.Fatal("edge 40 not visible once both of its endpoints are in the replica (fixture assumption)")
	}

	e.runCompaction(capturedBase, capturedSegs)
	if got := e.CompactionCount(); got != 1 {
		t.Fatalf("CompactionCount() = %d, want 1", got)
	}

	compacted := e.snap.Load()
	start, end, kind, ok := compacted.EdgeStateByID(40)
	if !ok {
		t.Fatal("edge 40 lost by the compaction: it was folded while its endpoint was unknown and never came back")
	}
	if compacted.GraphID(start) != 1 || compacted.GraphID(end) != 100 || kind != applyKindAdminTo {
		t.Fatalf("edge 40 = (%d -> %d, kind %d), want (1 -> 100, kind %d)", compacted.GraphID(start), compacted.GraphID(end), kind, applyKindAdminTo)
	}
	if err := snapshot.CheckViewsEquivalent(compacted, reference); err != nil {
		t.Fatalf("compacted view diverges from the never-compacted stack: %v", err)
	}
}

// TestCompactionCarriesAPendingEdgeUntilItsEndpointArrives covers the
// endpoint arriving only after the compaction was adopted: the adopted View
// keeps the edge record (in the one segment it now carries), the endpoint's
// segment then makes the edge visible, and the next compaction -- with
// nothing left pending -- folds it into the base and publishes a bare base
// again.
func TestCompactionCarriesAPendingEdgeUntilItsEndpointArrives(t *testing.T) {
	ctx := context.Background()
	e := New(nil, nil, Config{Enabled: true, Log: quietCompactTestLogger()})
	e.snap.Store(buildApplyView(t))

	v1 := publishAppend(ctx, e, newEdgeSegment(40, 1, 100, applyKindAdminTo))
	e.runCompaction(v1.Base(), v1.Segments())
	if got := e.CompactionCount(); got != 1 {
		t.Fatalf("CompactionCount() = %d, want 1", got)
	}
	adopted := e.snap.Load()
	if adopted.SegmentCount() != 1 {
		t.Fatalf("SegmentCount() after the compaction = %d, want 1 (the pending edge carried forward)", adopted.SegmentCount())
	}
	if _, _, _, ok := adopted.EdgeStateByID(40); ok {
		t.Fatal("edge 40 visible although its endpoint is still unknown")
	}

	arrived := publishAppend(ctx, e, newNodeSegment(t, 100))
	if _, _, _, ok := arrived.EdgeStateByID(40); !ok {
		t.Fatal("edge 40 not visible after its endpoint arrived on top of the compacted base")
	}

	e.runCompaction(arrived.Base(), arrived.Segments())
	if got := e.CompactionCount(); got != 2 {
		t.Fatalf("CompactionCount() = %d, want 2", got)
	}
	final := e.snap.Load()
	if final.SegmentCount() != 0 {
		t.Fatalf("SegmentCount() after the second compaction = %d, want 0 (nothing pending any more)", final.SegmentCount())
	}
	if _, ok := final.Base().EdgeByID(40); !ok {
		t.Fatal("edge 40 not folded into the base once both endpoints were known")
	}
	if err := snapshot.CheckViewsEquivalent(final, arrived); err != nil {
		t.Fatalf("second compaction diverges from the view it folded: %v", err)
	}
}
