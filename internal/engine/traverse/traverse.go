// SPDX-License-Identifier: Apache-2.0

// Package traverse implements kind-filtered BFS and shortest-path
// enumeration over an in-memory snapshot.Snapshot. Its semantics mirror
// BloodHound's PostgreSQL path-finding driver: traversals never exceed
// MaxDepth hops, zero-length paths are never produced, and parallel edges
// admitted under different allowed kinds yield distinct co-minimal paths.
package traverse

import (
	"errors"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// MaxDepth is the hop cap applied to every traversal, mirroring the pg
// driver's translateDefaultMaxTraversalDepth.
const MaxDepth = 15

// Path is one enumerated path. Nodes always has at least two entries (zero-
// length paths are never produced); Kinds holds the edge kind chosen for
// each hop, so len(Kinds) == len(Nodes)-1.
type Path struct {
	Nodes []snapshot.NodeID
	Kinds []snapshot.KindID
}

// scratch holds per-query BFS distance buffers reused across runs via the
// epoch trick: dist[v] is valid iff mark[v] == epoch. reset invalidates
// every previously written distance in O(1) without zeroing the backing
// arrays.
type scratch struct {
	mark  []uint32
	dist  []int8
	epoch uint32
}

// newScratch allocates a scratch sized for n nodes (snapshot.NodeCount()).
// Distances are unset until the first reset followed by set.
func newScratch(n int) *scratch {
	return &scratch{
		mark: make([]uint32, n),
		dist: make([]int8, n),
	}
}

// reset invalidates every distance written since the previous reset.
func (s *scratch) reset() {
	s.epoch++
}

// get returns v's distance and whether it has been set since the last
// reset.
func (s *scratch) get(v snapshot.NodeID) (int8, bool) {
	if s.mark[v] != s.epoch {
		return 0, false
	}
	return s.dist[v], true
}

// set records v's distance for the current epoch.
func (s *scratch) set(v snapshot.NodeID, d int8) {
	s.mark[v] = s.epoch
	s.dist[v] = d
}

// memBudget tracks approximate bytes consumed by enumerate's output,
// erroring once accounting would exceed limit. A zero limit is unbounded.
type memBudget struct{ limit, used uint64 }

// add accounts n more bytes, returning ErrMemoryLimit without recording the
// addition if that would exceed the budget's limit.
func (b *memBudget) add(n uint64) error {
	if b.limit > 0 && b.used+n > b.limit {
		return ErrMemoryLimit
	}
	b.used += n
	return nil
}

// ErrMemoryLimit is returned by enumerate when accounting a completed path
// would exceed the memBudget's limit.
var ErrMemoryLimit = errors.New("bloodtrail: path engine memory limit exceeded")
