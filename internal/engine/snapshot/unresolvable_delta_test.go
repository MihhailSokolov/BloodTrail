// SPDX-License-Identifier: Apache-2.0
package snapshot

import "testing"

// TestDeltaHoldsOnlyUnresolvableEdges walks every delta shape the classifier
// has to separate. true is reserved for the one shape a fold carries forward
// unchanged, in full, however many times it runs -- so the engine's
// post-compaction save can tell an unbounded wait from a short one
// (noteSkippedCompactionSave, ../persist.go).
func TestDeltaHoldsOnlyUnresolvableEdges(t *testing.T) {
	base, _ := buildOverlayFixture(t)

	cases := []struct {
		name  string
		build func(sb *SegmentBuilder)
		want  bool
		why   string
	}{
		{
			name:  "one edge with a pending end",
			build: func(sb *SegmentBuilder) { sb.AddEdgeState(2001, 10, 900, 5) },
			want:  true,
			why:   "900 is known neither to the base nor to the delta, so every fold carries this edge again",
		},
		{
			name: "several edges, every endpoint pending",
			build: func(sb *SegmentBuilder) {
				sb.AddEdgeState(2001, 10, 900, 5)
				sb.AddEdgeState(2002, 901, 902, 6)
				sb.AddEdgeState(2003, 903, 20, 6)
			},
			want: true,
			why:  "a pending endpoint on either side is enough, and nothing else is in the delta",
		},
		{
			name: "the pending endpoint has arrived",
			build: func(sb *SegmentBuilder) {
				sb.AddEdgeState(2001, 10, 900, 5)
				mustAddNodeState(t, sb, 900, []KindID{1}, `{"objectid":"S-obj-900"}`)
			},
			want: false,
			why:  "the next fold folds both the node and the edge into the base, so the delta can empty",
		},
		{
			name: "one pending edge beside a resolvable one",
			build: func(sb *SegmentBuilder) {
				sb.AddEdgeState(2001, 10, 900, 5)
				sb.AddEdgeState(2002, 10, 20, 5)
			},
			want: false,
			why:  "the resolvable edge is folded in, so this delta is not what a fold carries unchanged",
		},
		{
			name:  "only a node record",
			build: func(sb *SegmentBuilder) { mustAddNodeState(t, sb, 70, []KindID{1}, `{}`) },
			want:  false,
			why:   "a node record is always folded into the base",
		},
		{
			name: "a pending edge beside an unrelated node record",
			build: func(sb *SegmentBuilder) {
				sb.AddEdgeState(2001, 10, 900, 5)
				mustAddNodeState(t, sb, 70, []KindID{1}, `{}`)
			},
			want: false,
			why:  "the node record is consumed by the next fold, so the delta still shrinks",
		},
		{
			name: "a pending edge beside an edge tombstone",
			build: func(sb *SegmentBuilder) {
				sb.AddEdgeState(2001, 10, 900, 5)
				sb.TombstoneEdge(1001)
			},
			want: false,
			why:  "a tombstoned edge record is dropped by the next fold, not carried",
		},
		{
			name: "an edge whose endpoint the delta tombstoned",
			build: func(sb *SegmentBuilder) {
				sb.AddEdgeState(2001, 10, 50, 5)
				sb.TombstoneNode(50)
			},
			want: false,
			why:  "a GONE endpoint is dropped by the fold (nothing can bring node 50 back), not carried",
		},
		{
			name:  "a base edge the delta re-upserts",
			build: func(sb *SegmentBuilder) { sb.AddEdgeState(1001, 10, 20, 5) },
			want:  false,
			why:   "both endpoints are base nodes, so the fold folds the record into the base",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sb := &SegmentBuilder{}
			tc.build(sb)
			seg := sb.Build()
			v := NewView(base).WithSegment(seg)

			if got := v.DeltaHoldsOnlyUnresolvableEdges(); got != tc.want {
				t.Fatalf("DeltaHoldsOnlyUnresolvableEdges() = %v, want %v: %s", got, tc.want, tc.why)
			}

			// The claim a true answer also makes: this View serves no row
			// through any record in its delta. Checked against the fold
			// itself, which is the thing that would carry them -- every
			// record is one FoldWithPendingEdges hands back as pending.
			if !tc.want {
				return
			}
			folded, pending, err := FoldWithPendingEdges(base, []*Segment{seg})
			if err != nil {
				t.Fatalf("FoldWithPendingEdges: %v", err)
			}
			if pending == nil || pending.EdgeCount() != seg.EdgeCount() {
				t.Fatalf("the fold carried %v of %d delta edges, want all of them: a true answer claims this delta is exactly what a fold carries",
					pending, seg.EdgeCount())
			}
			seg.IterEdges(func(id uint64, _ EdgeSegState) bool {
				if _, _, _, visible := v.EdgeStateByID(id); visible {
					t.Errorf("edge %d is visible although it was classified unresolvable", id)
				}
				if _, ok := folded.EdgeByID(id); ok {
					t.Errorf("edge %d was folded into the base although it was classified unresolvable", id)
				}
				return true
			})
		})
	}
}

// TestDeltaHoldsOnlyUnresolvableEdgesOnABareView: a View with no delta at all
// has nothing a fold would carry, so the question is false rather than
// vacuously true -- the save path's empty-delta case is handled before this
// is ever asked, and answering true here would ask for a rebuild on every
// quiet engine.
func TestDeltaHoldsOnlyUnresolvableEdgesOnABareView(t *testing.T) {
	base, bare := buildOverlayFixture(t)

	if bare.DeltaHoldsOnlyUnresolvableEdges() {
		t.Error("a View with no segments reports a delta of unresolvable edges")
	}
	if NewView(base).WithSegment((&SegmentBuilder{}).Build()).DeltaHoldsOnlyUnresolvableEdges() {
		t.Error("a View whose one segment records nothing reports a delta of unresolvable edges")
	}
}
