// SPDX-License-Identifier: Apache-2.0
package traverse

import "github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"

// pathSink is where a strategy's enumeration phases put the paths they
// find, and where they keep the DFS scratch they walk with. One sink is
// built per AllShortestPaths call and threaded through every
// enumerate/pairEnumerate call that call makes, so both the arenas below
// and the scratch are reused across pairs instead of reallocated per pair.
// It is NOT safe for concurrent use: strategy B's parallel phase is its BFS
// fan-out (bfsSmallSide), and every enumeration this feeds runs in the
// sequential merge that follows it.
//
// Nodes and kinds are bump-allocated out of fixed-size chunks rather than
// held as one slice per path, and the []Path a caller finally gets
// (paths) is built exactly once, exactly sized, with each Path's Nodes and
// Kinds aliasing the chunk its bytes already live in. That shape exists for
// one reason: memBudget charges 12 bytes per node plus 48 per path, and the
// obvious implementation -- append a Path holding two freshly copied slices
// to a growing []Path -- allocates several times that figure even though it
// RETAINS about as much as it charges. Two multipliers caused it, and both
// are gone here:
//
//   - Growing a large []Path costs more than the array itself. Go grows a
//     slice past 256 elements by 1.25x, so reaching N paths by append
//     allocates about 5 * 48 bytes per path and throws 4 of those 5 away:
//     240 bytes of garbage per path against a 48-byte charge. Chunks are
//     never copied as they fill, and the one real []Path is allocated from
//     an exact count, so the whole 1.25x cascade disappears.
//   - Two heap objects per path (its nodes, its kinds), each rounded up to
//     a size class -- 24 bytes to hold 5 node ids, 8 to hold 4 kinds. Bump
//     allocation inside a chunk stores them end to end, at 4 and 2 bytes
//     per entry with no rounding and no per-path allocation at all.
//
// The aliasing is safe to hand out: every consumer of a traverse.Path reads
// it (interpret's convertPath copies out of it; engine's hydratePaths and
// assemblePath index it), and paths slices each chunk with an explicit
// capacity equal to its length, so even a future caller that appended to a
// Path's Nodes would get a fresh array rather than scribble over the path
// stored next to it.
type pathSink struct {
	// nodeChunks/kindChunks are the arenas, in emission order; the last
	// entry of each is the chunk currently being filled. A path's nodes and
	// its kinds always land in the same chunk index (add keeps the two in
	// lockstep), which is what lets paths walk them together.
	nodeChunks [][]snapshot.NodeID
	kindChunks [][]snapshot.KindID
	// lens is each path's node count, in emission order. uint16 is ample:
	// a path's length is bounded by the int8 distances BFS writes, so no
	// path can exceed 256 nodes however deep a caller's explicit bound goes
	// (MaxRepresentableDepth).
	lens []uint16
	// n is how many paths the sink holds -- the cumulative length the
	// strategies' own Limit arithmetic used to read off len(out).
	n int
	// chunkCap is the size of the next chunk to allocate.
	chunkCap int

	// stack, walk and walkKinds are the DFS scratch enumerate and
	// pairEnumerate borrow for the duration of one call and hand back
	// (grown) on return, so a merge phase that calls enumerate once per
	// reached terminal allocates them once rather than once per terminal.
	stack     []frame
	walk      []snapshot.NodeID
	walkKinds []snapshot.KindID
}

// frame is one node on an enumeration's explicit DFS stack: the node
// itself, the edge kind of the hop that reached it, and its depth along the
// walk (the root sits at depth 0 and carries no kind).
//
// Carrying only a depth is what keeps the stack flat. The previous frame
// held the whole partial path -- two freshly allocated slices per stack
// entry, re-copied at every hop -- which made a wide first level
// (1,000,000 paths of 3 nodes: a million-entry stack, two million live
// objects) cost 7.2x what memBudget charged for the result. Because the
// stack is LIFO, a frame's ancestors are still the first depth entries of
// walk when it is popped: every frame pushed after it, and therefore
// every frame popped before it, sits at depth >= its own and so can only
// truncate walk back to a point at or after its parent.
type frame struct {
	node  snapshot.NodeID
	kind  snapshot.KindID
	depth uint16
}

// pathSinkMinChunk and pathSinkMaxChunk bound the arena chunk sizes, in
// entries. Chunks double from the minimum up to the maximum, so a query
// returning three paths bump-allocates them out of one 64-entry chunk (256
// bytes of node ids) rather than reserving anything sized to what the query
// was ALLOWED to return, and a query returning a million keeps its chunk
// count (and the slack in its final, partly-filled chunk) negligible
// against the arena itself. The maximum stays under 32 KiB of payload --
// 4096 node ids is 16 KiB -- so every chunk is still a small allocation.
const (
	pathSinkMinChunk = 64
	pathSinkMaxChunk = 4096
)

// add stores one path: its nodes, the kinds of its hops, and nothing else.
// reverse copies the walk back to front, for an enumeration that walked the
// graph's edges backward (enumerate's forward=false) and must emit the path
// root-first anyway.
//
// kinds must hold exactly one entry per hop, len(nodes)-1 of them, which is
// Path's own documented invariant and what both callers maintain (walk and
// walkKinds move in lockstep). paths relies on it: it stores only the node
// count per path and derives the kind count back from it, so a caller that
// broke the invariant would misalign every path after it in that chunk.
//
// The caller charges the path to the query's memBudget BEFORE calling this,
// so a path that does not fit the budget is never stored.
func (s *pathSink) add(nodes []snapshot.NodeID, kinds []snapshot.KindID, reverse bool) {
	n := len(nodes)

	last := len(s.nodeChunks) - 1
	if last < 0 || cap(s.nodeChunks[last])-len(s.nodeChunks[last]) < n {
		s.addChunk(n)
		last = len(s.nodeChunks) - 1
	}

	// The kind chunk is allocated with the node chunk's capacity and takes
	// one entry fewer per path, so room for n nodes is always room for the
	// n-1 kinds that come with them: neither append below can reallocate,
	// and the two chunks stay aligned on path boundaries.
	nodeBase := len(s.nodeChunks[last])
	kindBase := len(s.kindChunks[last])
	s.nodeChunks[last] = append(s.nodeChunks[last], nodes...)
	s.kindChunks[last] = append(s.kindChunks[last], kinds...)
	if reverse {
		reverseNodes(s.nodeChunks[last][nodeBase:])
		reverseKinds(s.kindChunks[last][kindBase:])
	}

	s.lens = append(s.lens, uint16(n))
	s.n++
}

// addChunk starts a new chunk pair with room for at least need entries.
func (s *pathSink) addChunk(need int) {
	size := s.chunkCap
	switch {
	case size == 0:
		size = pathSinkMinChunk
	case size < pathSinkMaxChunk:
		size *= 2
	}
	if size < need {
		size = need
	}
	s.chunkCap = size
	s.nodeChunks = append(s.nodeChunks, make([]snapshot.NodeID, 0, size))
	s.kindChunks = append(s.kindChunks, make([]snapshot.KindID, 0, size))
}

// reset forgets every path stored so far, for a caller that has just
// discarded them all (shortestLevel.admit, when a strictly shorter pair
// restarts a ModeAll result). The arenas themselves are released rather
// than kept: the chunk list and the DFS scratch are reused, but the bytes
// the discarded paths occupied become collectable, which is what the
// matching memBudget.reset -- forgetting those same bytes -- is then true
// about.
//
// Clearing the chunk list is what makes that last part true, and is not
// tidiness: truncating a slice of slices to zero length leaves every
// chunk's header sitting in the backing array past len, where the collector
// still finds it, so the discarded level's arenas would stay resident with
// nothing accounting for them. Clearing before the truncation also keeps
// every slot at or past len nil, so no later reset has to revisit one.
func (s *pathSink) reset() {
	clear(s.nodeChunks)
	clear(s.kindChunks)
	s.nodeChunks = s.nodeChunks[:0]
	s.kindChunks = s.kindChunks[:0]
	s.lens = s.lens[:0]
	s.n = 0
	s.chunkCap = 0
}

// paths materializes the result: one exactly-sized []Path whose entries
// alias the arenas, in the order the paths were added. nil when the sink is
// empty, matching what a strategy that appended to a nil []Path used to
// return.
//
// Each chunk is walked front to back, consuming one lens entry per path, so
// the per-path offsets never need storing: a chunk's length is exactly the
// sum of its own paths' node counts (add starts a new chunk rather than
// splitting a path across two).
func (s *pathSink) paths() []Path {
	if s.n == 0 {
		return nil
	}

	out := make([]Path, 0, s.n)
	p := 0
	for c := range s.nodeChunks {
		nodes, kinds := s.nodeChunks[c], s.kindChunks[c]
		nodeOff, kindOff := 0, 0
		for nodeOff < len(nodes) {
			n := int(s.lens[p])
			p++
			out = append(out, Path{
				Nodes: nodes[nodeOff : nodeOff+n : nodeOff+n],
				Kinds: kinds[kindOff : kindOff+n-1 : kindOff+n-1],
			})
			nodeOff += n
			kindOff += n - 1
		}
	}
	return out
}
