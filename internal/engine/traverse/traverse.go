// SPDX-License-Identifier: Apache-2.0

// Package traverse implements kind-filtered BFS and shortest-path
// enumeration over an in-memory snapshot.Snapshot. Its semantics mirror
// BloodHound's PostgreSQL path-finding driver: traversals never exceed
// MaxDepth hops, zero-length paths are never produced, and parallel edges
// admitted under different allowed kinds yield distinct co-minimal paths.
package traverse

import (
	"errors"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"

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
// Guarded by mu so a single budget can be shared across strategy B's
// parallel workers.
type memBudget struct {
	mu          sync.Mutex
	limit, used uint64
}

// add accounts n more bytes, returning ErrMemoryLimit without recording the
// addition if that would exceed the budget's limit.
func (b *memBudget) add(n uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit > 0 && b.used+n > b.limit {
		return ErrMemoryLimit
	}
	b.used += n
	return nil
}

// ErrMemoryLimit is returned by enumerate when accounting a completed path
// would exceed the memBudget's limit.
var ErrMemoryLimit = errors.New("bloodtrail: path engine memory limit exceeded")

// Budgets bounding AllShortestPaths's strategy dispatch: PairBudget caps how
// many (root, terminal) pairs strategy A will run pairPaths over; SideBudget
// caps how many full-graph BFS runs strategy B will perform, one per
// element of whichever endpoint is smaller.
const (
	PairBudget = 4096 // max |roots|x|terminals| for per-pair strategy
	SideBudget = 16   // max full-BFS runs from the constrained side
)

// Mode selects how many shortest paths AllShortestPaths returns per pair.
type Mode int

const (
	// ModeAll returns every shortest path per pair (allShortestPaths /
	// FetchAllShortestPaths).
	ModeAll Mode = iota
	// ModeOne returns a single shortest path per pair (cypher
	// shortestPath()).
	ModeOne
)

// Endpoint constrains one side (roots or terminals) of a Query. At most one
// of IDs and Bits is meaningful at a time; IDs takes precedence when both
// are set. Neither set (both nil) means unconstrained: every node in the
// snapshot.
type Endpoint struct {
	IDs  []snapshot.NodeID // explicit dense ids, ascending; nil => use Bits
	Bits *snapshot.Bitset  // kind-derived set; nil together with IDs => unconstrained
}

// Unconstrained reports whether e matches every node in the snapshot.
func (e Endpoint) Unconstrained() bool {
	return e.IDs == nil && e.Bits == nil
}

// Count returns the number of dense ids e matches, given the snapshot's
// total node count.
func (e Endpoint) Count(total int) int {
	switch {
	case e.IDs != nil:
		return len(e.IDs)
	case e.Bits != nil:
		return e.Bits.Count()
	default:
		return total
	}
}

// Iterate calls fn for each dense id e matches, in ascending order, given
// the snapshot's total node count. Stops early if fn returns false.
func (e Endpoint) Iterate(total int, fn func(snapshot.NodeID) bool) {
	switch {
	case e.IDs != nil:
		for _, id := range e.IDs {
			if !fn(id) {
				return
			}
		}
	case e.Bits != nil:
		e.Bits.Iterate(fn)
	default:
		for i := 0; i < total; i++ {
			if !fn(snapshot.NodeID(i)) {
				return
			}
		}
	}
}

// Query describes an AllShortestPaths request.
type Query struct {
	Roots, Terminals Endpoint
	Kinds            *snapshot.KindMask // nil => every kind allowed
	Mode             Mode
	ExcludeSelf      bool // s<>t: skip pairs with r==t (zero-length paths are never produced regardless)
	MaxDepth         int  // 0 => MaxDepth constant
	Limit            int  // 0 => unbounded
	MemoryLimit      uint64
}

// ErrTooLarge is returned by AllShortestPaths when a Query is too large for
// any of the in-memory engine's strategies; the caller is expected to
// delegate the query to PostgreSQL instead.
var ErrTooLarge = errors.New("bloodtrail: query too large for the path engine")

// AllShortestPaths picks a strategy:
//
//	A. both sides materialize to <= PairBudget pairs: iterate pairs in
//	   (root, terminal) ascending dense order, pairPaths each, respecting
//	   Limit.
//	B. one side materializes to <= SideBudget ids and the other is larger
//	   or unconstrained: one full BFS per small-side element (parallel
//	   across GOMAXPROCS goroutines, each with its own scratch), then
//	   enumerate grouped by (root asc, terminal asc) until Limit.
//	C. otherwise: return ErrTooLarge (caller delegates to PostgreSQL).
//
// Results are ordered by (root dense id, terminal dense id); depths within
// a pair are equal by construction. Dense ascending == database-id
// ascending because the snapshot loads nodes ordered by id.
func AllShortestPaths(s *snapshot.Snapshot, q Query) ([]Path, error) {
	n := s.NodeCount()

	maxDepth := q.MaxDepth
	if maxDepth <= 0 {
		maxDepth = MaxDepth
	}

	kinds := q.Kinds
	if kinds == nil {
		kinds = snapshot.NewKindMask(s.MaxKindID)
		kinds.SetAll()
	}

	budget := &memBudget{limit: q.MemoryLimit}

	rootsUnconstrained := q.Roots.Unconstrained()
	termsUnconstrained := q.Terminals.Unconstrained()
	rootCount := q.Roots.Count(n)
	termCount := q.Terminals.Count(n)

	switch {
	case !rootsUnconstrained && !termsUnconstrained && int64(rootCount)*int64(termCount) <= PairBudget:
		return strategyPairs(s, q, kinds, maxDepth, budget)
	case !rootsUnconstrained && rootCount <= SideBudget:
		return strategySmallSide(s, q, kinds, maxDepth, budget, true)
	case !termsUnconstrained && termCount <= SideBudget:
		return strategySmallSide(s, q, kinds, maxDepth, budget, false)
	default:
		return nil, ErrTooLarge
	}
}

// pathCap returns the cap to pass to pairPaths/enumerate for their next
// call, and whether limit is already satisfied (in which case the caller
// should stop iterating entirely — no further calls for any remaining
// pair/root/terminal).
//
// pairEnumerate and enumerate's shared stop condition is len(out) == cap,
// checked against out's cumulative length across every pair or BFS element
// a strategy merges — not a per-call counter. So cap must be an absolute
// target ("stop once total output reaches N"), never a delta relative to
// this call ("N more from here"): a delta compared against a cumulative
// counter either overruns (the delta is counted from 0 while out already
// holds prior pairs' paths) or, once have exceeds limit/2, never fires at
// all, letting a call run unbounded.
//
// limit<=0 means the query has no Limit. oneMore restricts the upcoming
// call to contribute at most one additional path — ModeOne's "one path per
// pair/root" — expressed as have+1 (not a bare 1) so it composes with
// output already collected by earlier pairs/elements; when limit is also
// set, the tighter of the two absolute targets wins.
func pathCap(limit, have int, oneMore bool) (cap int, done bool) {
	if limit > 0 && have >= limit {
		return 0, true
	}
	cap = limit
	if oneMore {
		if next := have + 1; cap <= 0 || next < cap {
			cap = next
		}
	}
	return cap, false
}

// strategyPairs implements strategy A: iterate every (root, terminal) pair
// in ascending dense order and run pairPaths on each, sharing one set of
// scratch buffers and stopping once Limit is reached.
func strategyPairs(s *snapshot.Snapshot, q Query, kinds *snapshot.KindMask, maxDepth int, budget *memBudget) ([]Path, error) {
	n := s.NodeCount()
	scF, scT, scTmp := newScratch(n), newScratch(n), newScratch(n)
	oneMore := q.Mode == ModeOne

	var out []Path
	var callErr error

	q.Roots.Iterate(n, func(r snapshot.NodeID) bool {
		stopOuter := false
		q.Terminals.Iterate(n, func(t snapshot.NodeID) bool {
			if q.ExcludeSelf && r == t {
				return true
			}
			cap, done := pathCap(q.Limit, len(out), oneMore)
			if done {
				stopOuter = true
				return false
			}
			var err error
			out, err = pairPaths(s, r, t, kinds, maxDepth, cap, budget, scF, scT, scTmp, out)
			if err != nil {
				callErr = err
				stopOuter = true
				return false
			}
			return true
		})
		return !stopOuter && callErr == nil
	})

	if callErr != nil {
		return out, callErr
	}
	return out, nil
}

// smallSideDist is one small-side element's BFS result: dists holds either
// distances-from-elem (elem is a root) or distances-to-elem (elem is a
// terminal), matching bfsFrom's forward parameter used to compute it.
type smallSideDist struct {
	elem  snapshot.NodeID
	dists *scratch
}

// bfsSmallSide runs one full bfsFrom per element of elems (elems must
// already be in ascending order), in parallel across
// min(GOMAXPROCS, len(elems)) workers. Each worker owns and writes only the
// scratch buffers for the elements it is assigned, so no synchronization is
// needed beyond the fan-out/fan-in itself: results[i] is written by exactly
// one goroutine before g.Wait returns.
func bfsSmallSide(s *snapshot.Snapshot, elems []snapshot.NodeID, forward bool, kinds *snapshot.KindMask, maxDepth int) []smallSideDist {
	n := s.NodeCount()
	results := make([]smallSideDist, len(elems))

	workers := runtime.GOMAXPROCS(0)
	if workers > len(elems) {
		workers = len(elems)
	}
	if workers < 1 {
		return results
	}

	chunk := (len(elems) + workers - 1) / workers

	var g errgroup.Group
	for w := 0; w < workers; w++ {
		lo := w * chunk
		hi := lo + chunk
		if hi > len(elems) {
			hi = len(elems)
		}
		if lo >= hi {
			continue
		}
		g.Go(func() error {
			for i := lo; i < hi; i++ {
				sc := newScratch(n)
				bfsFrom(s, elems[i], forward, kinds, maxDepth, sc)
				results[i] = smallSideDist{elem: elems[i], dists: sc}
			}
			return nil
		})
	}
	_ = g.Wait() // bfsFrom cannot fail; no worker ever returns a non-nil error.

	return results
}

// materialize collects e's matched dense ids, in ascending order, into a
// slice.
func materialize(e Endpoint, total int) []snapshot.NodeID {
	elems := make([]snapshot.NodeID, 0, e.Count(total))
	e.Iterate(total, func(id snapshot.NodeID) bool {
		elems = append(elems, id)
		return true
	})
	return elems
}

// strategySmallSide implements strategy B. It runs one full BFS per element
// of whichever endpoint is smaller (smallIsRoots selects which), in
// parallel via bfsSmallSide, then enumerates paths in a strictly ordered
// sequential merge phase so output order (and Limit truncation) is
// independent of goroutine scheduling.
func strategySmallSide(s *snapshot.Snapshot, q Query, kinds *snapshot.KindMask, maxDepth int, budget *memBudget, smallIsRoots bool) ([]Path, error) {
	n := s.NodeCount()

	small := q.Terminals
	if smallIsRoots {
		small = q.Roots
	}
	elems := materialize(small, n)

	// small side = roots -> forward BFS (dist-from-root) per root.
	// small side = terminals -> reverse BFS (dist-to-terminal) per terminal.
	results := bfsSmallSide(s, elems, smallIsRoots, kinds, maxDepth)

	if smallIsRoots {
		return mergeSmallRoots(s, q, kinds, budget, results)
	}
	return mergeSmallTerminals(s, q, kinds, budget, results)
}

// mergeSmallRoots merges strategy B's per-root BFS results (small side =
// roots) into output ordered by (root asc, terminal asc). Because the small
// side already equals the output's outer grouping key, no cross-element
// merge is needed: each root's full contribution is emitted before moving
// to the next. Per root r, distances are FROM r, so each reached terminal
// is enumerated backward over the In-CSR mirror (enumerate's forward=false)
// and reversed into a root-to-terminal Path.
func mergeSmallRoots(s *snapshot.Snapshot, q Query, kinds *snapshot.KindMask, budget *memBudget, results []smallSideDist) ([]Path, error) {
	n := s.NodeCount()
	oneMore := q.Mode == ModeOne
	var out []Path
	var callErr error

	for _, res := range results {
		r, sc := res.elem, res.dists
		stop := false
		q.Terminals.Iterate(n, func(t snapshot.NodeID) bool {
			if q.ExcludeSelf && r == t {
				return true
			}
			if _, reached := sc.get(t); !reached {
				return true
			}
			cap, done := pathCap(q.Limit, len(out), oneMore)
			if done {
				stop = true
				return false
			}
			var err error
			out, err = enumerate(s, t, sc, kinds, cap, budget, out, false)
			if err != nil {
				callErr = err
				stop = true
				return false
			}
			return true
		})
		if stop {
			break
		}
	}

	if callErr != nil {
		return out, callErr
	}
	return out, nil
}

// mergeSmallTerminals merges strategy B's per-terminal BFS results (small
// side = terminals) into output ordered by (root asc, terminal asc). Unlike
// mergeSmallRoots, the small side here is the output's inner key, so the
// merge walks roots ascending over the union of every element's reached
// roots, and for each root walks the (already-ascending) small-side list
// checking each element's distance buffer for reachability. Per terminal
// x, distances are TO x, so each reached root is enumerated forward over
// the Out-CSR (enumerate's forward=true), Task 3's original direction.
func mergeSmallTerminals(s *snapshot.Snapshot, q Query, kinds *snapshot.KindMask, budget *memBudget, results []smallSideDist) ([]Path, error) {
	n := s.NodeCount()
	oneMore := q.Mode == ModeOne
	var out []Path
	var callErr error

	q.Roots.Iterate(n, func(r snapshot.NodeID) bool {
		for _, res := range results {
			t, sc := res.elem, res.dists
			if q.ExcludeSelf && r == t {
				continue
			}
			if _, reached := sc.get(r); !reached {
				continue
			}
			cap, done := pathCap(q.Limit, len(out), oneMore)
			if done {
				return false
			}
			var err error
			out, err = enumerate(s, r, sc, kinds, cap, budget, out, true)
			if err != nil {
				callErr = err
				return false
			}
		}
		return callErr == nil
	})

	if callErr != nil {
		return out, callErr
	}
	return out, nil
}
