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
	"sort"
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
//
// These are package-level defaults, used whenever a Query leaves its own
// PairBudget/SideBudget fields at zero -- see that struct's doc comment for
// how a caller can override them for one call without changing these
// constants (and therefore without touching every other caller's dispatch
// behavior, including this package's own tests and bench/adgen/README.md's
// documented "SideBudget=16" benchmark calibration).
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

// Iterate calls fn for each dense id e matches, in ascending order, against
// s. Stops early if fn returns false.
//
// The IDs and Bits branches are unconditional passthroughs regardless of
// s.Overlay(). Bits is always a NodesOfKind bitmap, already overlay-correct
// by construction -- every bit it sets is, by buildKindBitmap's own
// construction, for a node whose effective state is non-tombstoned, so it
// never carries a dead id (snapshot.View.NodesOfKind's own doc).
//
// IDs does not carry the same guarantee. It is populated by more than one
// caller, and not every caller resolves liveness first: interpret/
// expand.go's resolveEndpointSet does (via scanAnchorVisit's own
// Alive-skip), but engine.go's resolveIDEndpoint and resolveCriteriaEndpoint
// -- servePathQuery's shortestPath()/allShortestPaths() endpoint resolvers
// -- do not. Both map a database id to its dense id via snap.Dense alone,
// which deliberately still resolves a tombstoned base node's id (Dense's
// own doc: a tombstone hides a node, it does not remove its id mapping), so
// a dead id can legitimately appear in IDs, not just in theory.
//
// This is harmless, not a gap this function needs to close: every consumer
// of the id Iterate hands out reaches it through OutEdges/InEdges (or
// through Alive itself), and OutEdges/InEdges both yield nothing at all for
// a non-Alive node (their own doc's opening line) -- so a dead IDs entry
// simply contributes no edges/paths, exactly as PostgreSQL would find none
// through a deleted node id. IDs is left as a pure passthrough here, with no
// added Alive check, on that basis -- not because every producer already
// guarantees liveness.
//
// Only the unconstrained default -- "every dense id" -- gets an explicit
// check here: s.NodeCount() grows to keep a tombstoned base node's dense id
// occupied (snapshot.View.Alive's doc), so iterating the full range verbatim
// would hand a dead id to fn as if it were a real match.
func (e Endpoint) Iterate(s *snapshot.View, fn func(snapshot.NodeID) bool) {
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
		total := s.NodeCount()
		overlay := s.Overlay()
		for i := 0; i < total; i++ {
			id := snapshot.NodeID(i)
			if overlay && !s.Alive(id) {
				continue
			}
			if !fn(id) {
				return
			}
		}
	}
}

// Has reports whether e matches id. IDs is documented as ascending, so the
// IDs branch binary-searches rather than scanning; the Bits branch defers to
// snapshot.Bitset.Has (O(1)); the unconstrained default matches every id.
func (e Endpoint) Has(id snapshot.NodeID) bool {
	switch {
	case e.IDs != nil:
		i := sort.Search(len(e.IDs), func(i int) bool { return e.IDs[i] >= id })
		return i < len(e.IDs) && e.IDs[i] == id
	case e.Bits != nil:
		return e.Bits.Has(id)
	default:
		return true
	}
}

// SelfEndpointConflict reports whether roots and terminals share a dense id
// that has at least one outgoing edge in s (of any kind, not just those the
// query's own KindMask allows -- see below for why). It iterates whichever
// side Count(s.NodeCount()) reports as smaller and probes it against the
// other via Has, checking s.Out only for an id that is actually a member of
// both sides, so the cost is bounded by the smaller side's size plus one
// Out lookup per overlapping id.
//
// This exists to let servePathQuery match a real constraint of PostgreSQL's
// own shortest-path implementation: a root node that is also a terminal
// aborts its recursive seed query the moment that node has an outgoing edge
// (shortest_path_self_endpoint_error, raised from the very first BFS hop --
// see dawgs's cypher/models/pgsql/test/translation_cases/shortest_paths.sql
// and drivers/pg/query/sql/schema_up.sql). That failure is unconditional
// for the whole query, not scoped to the offending pair, and it fires for
// both FetchAllShortestPaths's single-pair shape and a Cypher
// shortestPath()/allShortestPaths() form with no explicit `n <> m` filter
// (Query.ExcludeSelf false) -- exactly the two shapes this engine serves.
//
// The check deliberately ignores the query's edge-kind restriction even
// though PostgreSQL's own trigger may well be scoped to it (the seed join's
// WHERE clause could short-circuit the self-check away for a row a kind
// filter already excludes -- the translation this project can inspect
// doesn't settle it either way for every query shape). Any-kind out-degree
// is a superset of kind-restricted out-degree, so this can only make the
// engine decline in strictly more cases than a kind-scoped check would,
// never fewer: if PostgreSQL's trigger does turn out to be kind-scoped, the
// worst outcome is an unnecessary decline (a query PostgreSQL would have
// served itself falls back to it and gets the same right answer); the
// alternative -- a kind-scoped check that turns out too narrow -- would let
// the exact bug this function exists to close back in, silently serving an
// answer for a request PostgreSQL cannot. Callers should decline outright
// whenever SelfEndpointConflict is true and ExcludeSelf is false, rather
// than trying to reproduce PostgreSQL's trigger any more precisely:
// declining is always at least as correct, since PostgreSQL is the fallback
// either way and returns the same answer (error or otherwise) whether or
// not the engine attempted the query first.
func SelfEndpointConflict(s *snapshot.View, roots, terminals Endpoint) bool {
	total := s.NodeCount()

	small, big := roots, terminals
	if terminals.Count(total) < roots.Count(total) {
		small, big = terminals, roots
	}

	conflict := false
	small.Iterate(s, func(id snapshot.NodeID) bool {
		if !big.Has(id) {
			return true
		}
		hasOut := false
		if !s.Overlay() {
			targets, _, _ := s.Out(id)
			hasOut = len(targets) > 0
		} else {
			s.OutEdges(id, func(snapshot.NodeID, snapshot.KindID, uint64) bool {
				hasOut = true
				return false
			})
		}
		if hasOut {
			conflict = true
			return false
		}
		return true
	})
	return conflict
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

	// PairBudget/SideBudget, when positive, override this package's own
	// PairBudget/SideBudget constants for this call's strategy dispatch
	// only (see AllShortestPaths). Zero (the default) leaves the package
	// constants in effect exactly as before this field existed -- every
	// caller that never sets these (servePathQuery, pathbench, and every
	// test in this package) keeps today's dispatch behavior unconditionally.
	//
	// PairBudget/SideBudget bound how much *preparatory* pair/BFS work a
	// strategy is willing to attempt before a single row exists -- a
	// different axis from Limit/MemoryLimit, which bound the *output* --
	// so a caller that already tracks its own meaningful, per-query work
	// budget (see internal/engine/interpret's Budgets/workMeter) can use
	// these fields to keep a query whose actual affordable search cost is
	// larger than these scale-agnostic package constants from being
	// spuriously declined ErrTooLarge. See
	// internal/engine/interpret/expand.go's strategyBudgetOverrides for the
	// one caller that sets these today, and its doc comment for the
	// investigation that motivated this field.
	PairBudget int
	SideBudget int
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
func AllShortestPaths(s *snapshot.View, q Query) ([]Path, error) {
	n := s.NodeCount()

	maxDepth := q.MaxDepth
	if maxDepth <= 0 {
		maxDepth = MaxDepth
	}

	// q.Kinds nil means "every kind allowed" (the field's own doc); bfs.go's
	// five kinds.Has(k) consumption sites treat a nil kinds exactly that way,
	// so no mask needs to be built here at all. Building one used to require
	// sizing it to s.Base().MaxKindID+SetAll(), which silently excluded any
	// kind first introduced by a delta segment after the base snapshot was
	// built (KindMask.Set/Has both no-op above the mask's own ceiling) --
	// an untyped shortestPath()/allShortestPaths() whose only admissible
	// edges were all delta-added returned zero paths instead of the real
	// ones. Passing kinds through unchanged removes the ceiling instead of
	// trying to widen it.
	kinds := q.Kinds

	budget := &memBudget{limit: q.MemoryLimit}

	pairBudget := int64(PairBudget)
	if q.PairBudget > 0 {
		pairBudget = int64(q.PairBudget)
	}
	sideBudget := SideBudget
	if q.SideBudget > 0 {
		sideBudget = q.SideBudget
	}

	rootsUnconstrained := q.Roots.Unconstrained()
	termsUnconstrained := q.Terminals.Unconstrained()
	rootCount := q.Roots.Count(n)
	termCount := q.Terminals.Count(n)

	switch {
	case !rootsUnconstrained && !termsUnconstrained && int64(rootCount)*int64(termCount) <= pairBudget:
		return strategyPairs(s, q, kinds, maxDepth, budget)
	case !rootsUnconstrained && rootCount <= sideBudget:
		return strategySmallSide(s, q, kinds, maxDepth, budget, true)
	case !termsUnconstrained && termCount <= sideBudget:
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
func strategyPairs(s *snapshot.View, q Query, kinds *snapshot.KindMask, maxDepth int, budget *memBudget) ([]Path, error) {
	n := s.NodeCount()
	scF, scT, scTmp := newScratch(n), newScratch(n), newScratch(n)
	oneMore := q.Mode == ModeOne

	var out []Path
	var callErr error

	q.Roots.Iterate(s, func(r snapshot.NodeID) bool {
		stopOuter := false
		q.Terminals.Iterate(s, func(t snapshot.NodeID) bool {
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
func bfsSmallSide(s *snapshot.View, elems []snapshot.NodeID, forward bool, kinds *snapshot.KindMask, maxDepth int) []smallSideDist {
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
func materialize(e Endpoint, s *snapshot.View) []snapshot.NodeID {
	elems := make([]snapshot.NodeID, 0, e.Count(s.NodeCount()))
	e.Iterate(s, func(id snapshot.NodeID) bool {
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
func strategySmallSide(s *snapshot.View, q Query, kinds *snapshot.KindMask, maxDepth int, budget *memBudget, smallIsRoots bool) ([]Path, error) {
	small := q.Terminals
	if smallIsRoots {
		small = q.Roots
	}
	elems := materialize(small, s)

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
func mergeSmallRoots(s *snapshot.View, q Query, kinds *snapshot.KindMask, budget *memBudget, results []smallSideDist) ([]Path, error) {
	oneMore := q.Mode == ModeOne
	var out []Path
	var callErr error

	for _, res := range results {
		r, sc := res.elem, res.dists
		stop := false
		q.Terminals.Iterate(s, func(t snapshot.NodeID) bool {
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
func mergeSmallTerminals(s *snapshot.View, q Query, kinds *snapshot.KindMask, budget *memBudget, results []smallSideDist) ([]Path, error) {
	oneMore := q.Mode == ModeOne
	var out []Path
	var callErr error

	q.Roots.Iterate(s, func(r snapshot.NodeID) bool {
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
