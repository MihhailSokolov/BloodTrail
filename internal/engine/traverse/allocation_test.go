// SPDX-License-Identifier: Apache-2.0
package traverse

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildLayeredFixture builds the shape these measurements need: one source,
// `layers` fully-connected layers of `width` nodes each, one sink. Every
// source-to-sink path is a shortest path, there are width^layers of them,
// and each holds layers+2 nodes -- so one small graph produces a known,
// exactly-controlled number of paths of a known length, which is what lets
// a per-path allocation figure mean anything.
//
// Dense ids: 0 is the source, 1 the sink, 2.. the layer nodes. Every node
// carries node kind 1 and every edge kind 1; kind filtering is not what
// these tests measure.
func buildLayeredFixture(tb testing.TB, layers, width int) (view *snapshot.View, source, sink snapshot.NodeID) {
	tb.Helper()
	b := snapshot.NewBuilder(1)
	for i := uint64(0); i < uint64(2+layers*width); i++ {
		if err := b.AddNode(i, []snapshot.KindID{1}, nil); err != nil {
			tb.Fatalf("AddNode(%d): %v", i, err)
		}
	}

	id := func(layer, i int) uint64 { return uint64(2 + layer*width + i) }
	var e uint64
	addEdge := func(from, to uint64) {
		b.AddEdge(e, from, to, 1)
		e++
	}
	for i := 0; i < width; i++ {
		addEdge(0, id(0, i))
	}
	for l := 0; l+1 < layers; l++ {
		for i := 0; i < width; i++ {
			for j := 0; j < width; j++ {
				addEdge(id(l, i), id(l+1, j))
			}
		}
	}
	for i := 0; i < width; i++ {
		addEdge(id(layers-1, i), 1)
	}

	s, err := b.Build()
	if err != nil {
		tb.Fatalf("Build: %v", err)
	}
	return snapshot.NewView(s), snapshot.NodeID(0), snapshot.NodeID(1)
}

// accountedBytes is what memBudget charges for paths: the same 12 bytes per
// node plus 48 per path that enumerate/pairEnumerate add as they go, and
// that interpret's shortestPathBudget mirrors to turn a query's row cap into
// a byte limit.
func accountedBytes(paths []Path) uint64 {
	var total uint64
	for _, p := range paths {
		total += uint64(len(p.Nodes))*12 + 48
	}
	return total
}

// allocatedBytes runs fn and returns how many heap bytes it allocated
// (runtime.TotalAlloc's delta, which counts every allocation whether or not
// it survives). Deterministic for a deterministic workload -- unlike a peak
// or an end-of-run heap reading, which depend on when the collector happens
// to run -- so it is what these tests assert on.
func allocatedBytes(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// allocationBudgetFactor is the most heap a path enumeration may allocate,
// as a multiple of what memBudget charges for its result.
//
// This test exists because that relationship used to run the wrong way by a
// wide margin: a MemoryLimit of X permitted several times X of real heap, so
// the 1 GiB GraphQueryMemoryLimit servePathQuery passes down corresponded to
// a multi-gigabyte resident commitment. The three shapes below measured
// 6.90x, 3.34x and 2.55x of their own accounted charge in total allocation
// (and 4.19x, 1.42x, 1.92x of it in peak heap) when every path was a Path
// holding two freshly allocated slices appended to a growing []Path; they
// measure 1.29x, 0.81x and 0.66x since pathSink replaced that (peak heap
// tracks those figures, because the garbage that used to tower over the
// result is gone rather than merely collected sooner).
//
// 2.0 therefore sits with room to spare above every shape measured here
// while still failing on a return to the old shape, whose cheapest case was
// 2.55x. The margin is deliberately asymmetric: TotalAlloc for a fixed
// workload is exactly reproducible run to run (the figures above repeat to
// the byte), so the only drift worth leaving room for is a future Go
// version's size classes or slice-growth factor, not run-to-run noise.
const allocationBudgetFactor = 2.0

// TestEnumerateAllocatesWithinTheAccountedCharge measures one enumeration's
// real heap allocation against the bytes memBudget charged for its output,
// over three path shapes: a single wide fan-out (the shape that was worst,
// because a million-path level put a million partial paths on the DFS stack
// at once), a mid-width layered graph, and a narrow one at MaxDepth.
func TestEnumerateAllocatesWithinTheAccountedCharge(t *testing.T) {
	for _, tc := range []struct {
		name          string
		layers, width int
		wantPaths     int
	}{
		{"wide fan-out, 3 nodes per path", 1, 20000, 20000},
		{"layered, 5 nodes per path", 3, 40, 64000},
		{"narrow, 15 nodes per path", 13, 2, 8192},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, source, sink := buildLayeredFixture(t, tc.layers, tc.width)
			sc := newScratch(s.NodeCount())
			bfsFrom(s, sink, false, nil, MaxDepth, sc)

			budget := &memBudget{}
			acc := &pathSink{}
			var out []Path
			allocated := allocatedBytes(func() {
				if err := enumerate(s, source, sc, nil, 0, budget, acc, true); err != nil {
					t.Fatalf("enumerate: %v", err)
				}
				out = acc.paths()
			})

			if len(out) != tc.wantPaths {
				t.Fatalf("enumerate produced %d paths, want %d (fixture is wrong)", len(out), tc.wantPaths)
			}
			for _, p := range out {
				if len(p.Nodes) != tc.layers+2 || len(p.Kinds) != tc.layers+1 {
					t.Fatalf("path %+v has %d nodes and %d kinds, want %d and %d",
						p, len(p.Nodes), len(p.Kinds), tc.layers+2, tc.layers+1)
				}
			}

			charged := accountedBytes(out)
			if charged != budget.used {
				t.Fatalf("memBudget charged %d bytes, the same formula over the result gives %d", budget.used, charged)
			}

			factor := float64(allocated) / float64(charged)
			t.Logf("%d paths of %d nodes: charged %d bytes (%.0f B/path), allocated %d bytes (%.0f B/path, %.2fx)",
				len(out), tc.layers+2, charged, float64(charged)/float64(len(out)),
				allocated, float64(allocated)/float64(len(out)), factor)
			if factor > allocationBudgetFactor {
				t.Fatalf("enumerating %d paths of %d nodes allocated %d bytes against a %d-byte charge (%.2fx), want at most %.2fx",
					len(out), tc.layers+2, allocated, charged, factor, allocationBudgetFactor)
			}

			// The output array is exactly the size of the result. This is
			// the structural half of the measurement above: no 1.25x growth
			// cascade can be hiding in a figure that came out this low by
			// luck, and nothing reserved capacity for paths that were never
			// found (see TestEnumerateReservesNothingForPathsItDoesNotFind).
			if cap(out) != len(out) {
				t.Fatalf("result slice has capacity %d for %d paths, want them equal", cap(out), len(out))
			}
		})
	}
}

// TestEnumerateReservesNothingForPathsItDoesNotFind pins the other half of
// the accounting fix: reality was brought down to the charge by removing
// allocation, never by reserving capacity up front from a bound the query
// was merely ALLOWED to reach. Those bounds are enormous -- interpret caps
// a shortestPath component at Budgets.MaxLiveRows, 2,000,000 rows in
// production -- so a sink that sized its arenas off the cap it was handed
// would turn a three-path answer into a hundred megabytes of reservation,
// trading one accounting lie for a worse one.
//
// So: enumerate the two-path diamond under exactly that production cap, and
// require that nothing about what the sink reserved, or what the enumeration
// allocated in total, scales with it.
func TestEnumerateReservesNothingForPathsItDoesNotFind(t *testing.T) {
	const productionRowCap = 2_000_000 // interpret's maxCypherLiveRows

	s := buildFixture(t)
	kinds := maskOf(3, 1, 2)
	sc := newScratch(s.NodeCount())
	bfsFrom(s, 6, false, kinds, MaxDepth, sc)

	budget := &memBudget{}
	acc := &pathSink{}
	var out []Path
	allocated := allocatedBytes(func() {
		if err := enumerate(s, 0, sc, kinds, productionRowCap, budget, acc, true); err != nil {
			t.Fatalf("enumerate: %v", err)
		}
		out = acc.paths()
	})

	want := []Path{
		{Nodes: []snapshot.NodeID{0, 4, 6}, Kinds: []snapshot.KindID{1, 1}},
		{Nodes: []snapshot.NodeID{0, 5, 6}, Kinds: []snapshot.KindID{2, 1}},
	}
	assertPathSet(t, out, want)

	if cap(out) != len(out) {
		t.Fatalf("result slice has capacity %d for %d paths, want them equal", cap(out), len(out))
	}
	if n := len(acc.nodeChunks); n != 1 {
		t.Fatalf("sink used %d arena chunks for a two-path result, want 1", n)
	}
	if c := cap(acc.nodeChunks[0]); c != pathSinkMinChunk {
		t.Fatalf("sink's arena chunk holds %d node ids, want the %d-entry minimum: chunk sizing must not scale with the cap it was handed", c, pathSinkMinChunk)
	}
	if c := cap(acc.kindChunks[0]); c != pathSinkMinChunk {
		t.Fatalf("sink's kind chunk holds %d kinds, want the %d-entry minimum", c, pathSinkMinChunk)
	}

	// One 64-entry chunk pair, a couple of hundred bytes of DFS scratch and
	// the two-element result: a few hundred bytes all told, and in no way a
	// function of the two-million-row cap.
	const smallResultCeiling = 4096
	t.Logf("two paths under a %d-row cap allocated %d bytes", productionRowCap, allocated)
	if allocated > smallResultCeiling {
		t.Fatalf("enumerating 2 paths under a %d-row cap allocated %d bytes, want at most %d",
			productionRowCap, allocated, smallResultCeiling)
	}
}

// TestPathSinkSplitsAndFlattensChunksFaithfully drives the sink across its
// own chunk boundaries -- the one place where paths and their kinds could
// get out of step with each other, since each path lands whole in one chunk
// and a chunk's tail is abandoned rather than split -- and checks that what
// comes out of paths is, path for path, what went in.
func TestPathSinkSplitsAndFlattensChunksFaithfully(t *testing.T) {
	// Lengths chosen so several paths share a chunk, one lands exactly on a
	// boundary, and one is longer than the minimum chunk itself.
	lengths := []int{2, 3, 2, 15, 7}
	var want []Path
	acc := &pathSink{}

	for p := 0; len(acc.lens) < 400; p++ {
		n := lengths[p%len(lengths)]
		if p == 200 {
			n = pathSinkMinChunk + 5
		}
		nodes := make([]snapshot.NodeID, n)
		edgeKinds := make([]snapshot.KindID, n-1)
		for i := range nodes {
			nodes[i] = snapshot.NodeID(p*1000 + i)
		}
		for i := range edgeKinds {
			edgeKinds[i] = snapshot.KindID(p + i)
		}
		reverse := p%3 == 0
		acc.add(nodes, edgeKinds, reverse)
		if reverse {
			reverseNodes(nodes)
			reverseKinds(edgeKinds)
		}
		want = append(want, Path{Nodes: nodes, Kinds: edgeKinds})
	}

	if len(acc.nodeChunks) < 3 {
		t.Fatalf("fixture only filled %d chunks, want at least 3 so a boundary is actually crossed", len(acc.nodeChunks))
	}

	got := acc.paths()
	if len(got) != len(want) {
		t.Fatalf("paths returned %d paths, want %d", len(got), len(want))
	}
	for i := range want {
		if pathKey(got[i]) != pathKey(want[i]) {
			t.Fatalf("path %d: got %+v, want %+v", i, got[i], want[i])
		}
		// Aliasing the arena is the point, but a caller appending to a
		// returned path must not reach the path stored next to it.
		if cap(got[i].Nodes) != len(got[i].Nodes) || cap(got[i].Kinds) != len(got[i].Kinds) {
			t.Fatalf("path %d aliases the arena with spare capacity (%d/%d nodes, %d/%d kinds)",
				i, cap(got[i].Nodes), len(got[i].Nodes), cap(got[i].Kinds), len(got[i].Kinds))
		}
	}

	// reset forgets the paths and releases the arenas, the way
	// shortestLevel.admit needs when a shorter pair restarts a ModeAll
	// result -- and leaves a sink that works.
	acc.reset()
	if acc.n != 0 || len(acc.nodeChunks) != 0 || len(acc.kindChunks) != 0 || len(acc.lens) != 0 {
		t.Fatalf("reset left %d paths, %d node chunks, %d kind chunks, %d lengths behind",
			acc.n, len(acc.nodeChunks), len(acc.kindChunks), len(acc.lens))
	}
	if ps := acc.paths(); ps != nil {
		t.Fatalf("paths after reset = %+v, want nil", ps)
	}
	acc.add([]snapshot.NodeID{7, 8}, []snapshot.KindID{3}, false)
	if ps := acc.paths(); len(ps) != 1 || pathKey(ps[0]) != pathKey(Path{Nodes: []snapshot.NodeID{7, 8}, Kinds: []snapshot.KindID{3}}) {
		t.Fatalf("after reset and one add, paths = %+v", ps)
	}
	// The released arenas are not retained as capacity for the next level:
	// a one-path result after a reset gets a minimum chunk, not whatever the
	// discarded level had grown to.
	if c := cap(acc.nodeChunks[0]); c != pathSinkMinChunk {
		t.Fatalf("first chunk after reset holds %d node ids, want the %d-entry minimum", c, pathSinkMinChunk)
	}
	// Nor are they still reachable through the chunk list's own spare
	// capacity, which is where a plain truncation would have left them for
	// the collector to keep finding (see reset).
	full := acc.nodeChunks[:cap(acc.nodeChunks)]
	for i := len(acc.nodeChunks); i < len(full); i++ {
		if full[i] != nil {
			t.Fatalf("chunk list slot %d past len still references a %d-entry arena after reset", i, cap(full[i]))
		}
	}
	fullKinds := acc.kindChunks[:cap(acc.kindChunks)]
	for i := len(acc.kindChunks); i < len(fullKinds); i++ {
		if fullKinds[i] != nil {
			t.Fatalf("kind chunk list slot %d past len still references a %d-entry arena after reset", i, cap(fullKinds[i]))
		}
	}
}

// TestEnumerateMatchesTheReferenceWalkAfterFlattening cross-checks the
// frame-stack walk and the chunked arenas against an independent,
// deliberately naive enumeration of the same graphs: a recursive walk that
// builds each path as its own slice, exactly the way enumerate used to. The
// two must agree path for path, in order -- the sink is an allocation
// change, not a semantic one, so a difference here is a wrong answer.
func TestEnumerateMatchesTheReferenceWalkAfterFlattening(t *testing.T) {
	for _, tc := range []struct{ layers, width int }{{1, 3}, {2, 2}, {3, 2}, {4, 3}} {
		t.Run(fmt.Sprintf("layers=%d/width=%d", tc.layers, tc.width), func(t *testing.T) {
			s, source, sink := buildLayeredFixture(t, tc.layers, tc.width)
			sc := newScratch(s.NodeCount())
			bfsFrom(s, sink, false, nil, MaxDepth, sc)

			acc := &pathSink{}
			if err := enumerate(s, source, sc, nil, 0, &memBudget{}, acc, true); err != nil {
				t.Fatalf("enumerate: %v", err)
			}
			got := acc.paths()

			want := referenceEnumerate(s, source, sc)
			if len(got) != len(want) {
				t.Fatalf("enumerate produced %d paths, the reference walk %d", len(got), len(want))
			}
			for i := range want {
				if pathKey(got[i]) != pathKey(want[i]) {
					t.Fatalf("path %d: enumerate gave %+v, the reference walk %+v", i, got[i], want[i])
				}
			}
		})
	}
}

// referenceEnumerate is TestEnumerateMatchesTheReferenceWalkAfterFlattening's
// independent oracle: a recursive forward walk down strictly decreasing
// distances, collecting each completed path as its own freshly allocated
// slices. It shares no state with enumerate beyond the distance buffer, and
// visits a node's neighbours in CSR order while enumerate pushes them onto a
// LIFO stack, so it reproduces enumerate's own emission order by walking
// each node's neighbours back to front.
func referenceEnumerate(s *snapshot.View, from snapshot.NodeID, distBuf *scratch) []Path {
	var out []Path
	var walk func(nodes []snapshot.NodeID, kinds []snapshot.KindID)
	walk = func(nodes []snapshot.NodeID, kinds []snapshot.KindID) {
		u := nodes[len(nodes)-1]
		du, ok := distBuf.get(u)
		if !ok {
			return
		}
		if du == 0 {
			if len(nodes) < 2 {
				return
			}
			out = append(out, Path{
				Nodes: append([]snapshot.NodeID(nil), nodes...),
				Kinds: append([]snapshot.KindID(nil), kinds...),
			})
			return
		}
		targets, edgeKinds, _ := s.Out(u)
		for i := len(targets) - 1; i >= 0; i-- {
			w := targets[i]
			if dw, ok := distBuf.get(w); !ok || dw != du-1 {
				continue
			}
			walk(append(nodes, w), append(kinds, edgeKinds[i]))
		}
	}
	walk([]snapshot.NodeID{from}, nil)
	return out
}
