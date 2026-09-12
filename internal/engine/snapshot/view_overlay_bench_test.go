// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"fmt"
	"testing"
)

// BenchmarkFirstOverlayRead measures what every freshly published View
// charges its first overlay reader: WithSegment returns a View whose memos
// are all zero-valued, so the first read re-runs MergeSegments over the
// whole stack and rebuilds the projections it needs -- work proportional to
// the TOTAL delta, not to the one write that just published (the
// characteristic README.md's "Delta reads cost O(total delta), per
// published View" section records). Apply itself is such a reader
// (buildApplySegment walks the current View for cascades and read-back
// staging), and it holds applyMu while paying this, so this curve is the
// per-write cost of letting the delta grow -- the number that decides what
// DefaultCompactEntries (engine/compact.go) should be.
//
// The delta is spread over 8 segments (a realistic post-collapse stack:
// maxSegments collapses far deeper stacks synchronously), split evenly
// between node states carrying a small property bag and edge states between
// base nodes, over a 100k-node base. The measured op is: stack the segments
// onto a fresh View, then force exactly the memos Apply's own walk forces --
// ensureDelta via a Dense miss, and the edge-tombstone set plus delta
// adjacency via one OutEdges walk.
func BenchmarkFirstOverlayRead(b *testing.B) {
	const baseNodes = 100_000

	base := buildBenchBase(b, baseNodes)

	for _, entries := range []int{16_384, 65_536, 262_144, 1_048_576} {
		b.Run(fmt.Sprintf("entries=%d", entries), func(b *testing.B) {
			segs := buildBenchSegments(b, entries, 8, baseNodes)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v := NewView(base)
				for _, seg := range segs {
					v = v.WithSegment(seg)
				}
				// A database id no node has: the base misses, so this is
				// the ensureDelta trigger with no base fast path.
				if _, ok := v.Dense(baseNodes * 10); ok {
					b.Fatal("unexpected hit for a nonexistent id")
				}
				v.OutEdges(0, func(NodeID, KindID, uint64) bool { return true })
			}
		})
	}
}

// buildBenchBase builds a baseNodes-node base snapshot with one kind and a
// small chain of edges -- enough structure for OutEdges to have base slots
// to consult, while keeping the build itself far from dominating the
// benchmark's setup time.
func buildBenchBase(b *testing.B, baseNodes int) *Snapshot {
	b.Helper()

	builder := NewBuilder(1)
	builder.SetKinds(map[KindID]string{1: "User", 2: "MemberOf"})
	for i := 0; i < baseNodes; i++ {
		props := fmt.Sprintf(`{"objectid":"S-bench-%d"}`, i)
		if err := builder.AddNode(uint64(i+1), []KindID{1}, []byte(props)); err != nil {
			b.Fatalf("AddNode: %v", err)
		}
	}
	for i := 0; i < baseNodes-1; i += 2 {
		builder.AddEdge(uint64(1_000_000+i), uint64(i+1), uint64(i+2), 2)
	}

	s, err := builder.Build()
	if err != nil {
		b.Fatalf("Build: %v", err)
	}
	return s
}

// buildBenchSegments builds `segments` delta segments totaling `entries`
// states, half node upserts (small bag, an existing base id, so the merge
// exercises the override path rather than growing virtual ids) and half
// edge states between base nodes.
func buildBenchSegments(b *testing.B, entries, segments, baseNodes int) []*Segment {
	b.Helper()

	perSeg := entries / segments
	segs := make([]*Segment, 0, segments)
	next := 0
	for s := 0; s < segments; s++ {
		var sb SegmentBuilder
		for j := 0; j < perSeg; j++ {
			id := uint64(next%baseNodes + 1)
			if j%2 == 0 {
				props := fmt.Sprintf(`{"objectid":"S-bench-%d","touched":%d}`, id-1, next)
				if err := sb.AddNodeState(id, []KindID{1}, []byte(props)); err != nil {
					b.Fatalf("AddNodeState: %v", err)
				}
			} else {
				sb.AddEdgeState(uint64(2_000_000+next), id, uint64((next+1)%baseNodes+1), 2)
			}
			next++
		}
		segs = append(segs, sb.Build())
	}
	return segs
}
