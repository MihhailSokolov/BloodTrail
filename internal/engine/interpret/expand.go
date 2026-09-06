// SPDX-License-Identifier: Apache-2.0

// Package interpret: this file implements Task 8 of milestone 4 -- the two
// seams Task 7's exec.go left open (see its package doc comment and
// runComponent's dispatch, below): variable-length relationship-pattern
// expansion (`*min..max`) and shortestPath()/allShortestPaths() step
// execution, both driven directly off one snapshot.Snapshot rather than a
// live PostgreSQL round trip.
//
// --- Var-length trail semantics -------------------------------------------
//
// This is pinned, deliberately, to reproduce dawgs' own pg driver's
// recursive-CTE expansion exactly (verified by reading dawgs@v0.8.0
// cypher/models/pgsql/translate/expansion.go and its golden SQL in
// cypher/models/pgsql/test/translation_cases/pattern_expansion.sql):
//
//   - TRAIL semantics: an edge id may not repeat within one emitted path
//     (pattern_expansion.sql's recursive member carries `not (e0.id = any
//     (path.edge_ids))` in every shape); NODES may be revisited via distinct
//     edges (no equivalent node-id check exists anywhere in that file).
//   - Depth counts EDGES, not nodes: the recursive member's join condition
//     is `path.depth < $max_depth` (checked BEFORE taking the next edge),
//     so the deepest row a recursive CTE like this ever emits carries
//     exactly max_depth edges. max_depth is Step.Range.Max when the
//     relationship pattern carried an explicit upper bound (buildStep
//     already resolved this, even past 15 -- see plan.go's MaxExpansionDepth
//     doc), else MaxExpansionDepth itself (a bare `*`/`*1..`/`*..`).
//   - Min-depth is a POST-FILTER: the recursive CTE has no way to "start
//     emitting only once N edges have been walked" (every recursion level
//     is a candidate row by construction); pg's translation instead adds a
//     plain `where path.depth >= $min_depth` over the CTE's complete output.
//     This file matches that exactly -- see expandVarLengthComponent's
//     unconditional per-depth emission, filtered only at the point of
//     appending to out.
//   - `*0..`: dawgs' translation special-cases Range.Min == 0 with a
//     separate zero-length arm of the seed query that selects the *same*
//     row for both pattern endpoints with an empty edge/node list, entirely
//     independent of the depth-1-and-up recursive member (which still only
//     ever fires for depth >= 1). This file mirrors that structurally: the
//     zero-length row is produced once per seed, before the trail DFS even
//     starts, never as a "depth 0" case inside the DFS itself.
//   - FIRST-EDGE SELF-LOOP: pattern_expansion.sql's seed (depth-1) query and
//     its recursive member use different join shapes -- the seed's `where
//     n0.id != n1.id or path.depth > 0`-style guard (self-loop exclusion)
//     applies only to the *first* edge taken from the root, never to a
//     self-loop reached deeper in the recursion. A trail whose first edge
//     is a self-loop is therefore still emitted at depth 1 (the seed row
//     itself is never suppressed, only its own further recursion is), but
//     it can never be extended past depth 1 by the recursive member. A
//     self-loop encountered at depth > 1 (i.e. not the very first edge) has
//     no such restriction and recurses normally.
//   - Multiplicity: no relationship-uniqueness or per-endpoint-pair dedup is
//     applied anywhere in dawgs' translation for this shape -- every
//     distinct trail (including two trails that use different parallel
//     edges of the same kind between the same two nodes) is a separate CTE
//     row, so this file enumerates all of them rather than collapsing by
//     (root, terminal) pair.
//   - Undirected var-length cannot occur: plan.go's buildStep already
//     rejects it at plan time, so Step.Direction is always
//     graph.DirectionOutbound whenever Step.Range != nil, and FromSym is
//     always the CTE's structural seed/root side.
//   - Far-endpoint NodeConstraints (Step.ToSym's Kinds/IDs/ObjectIDAnchor)
//     apply to the terminal binding only, exactly like min-depth: a plain
//     post-filter over whichever node the DFS is currently considering as
//     the trail's endpoint, never a prune of the DFS branch itself (an
//     intermediate node one hop short of the terminal keeps being extended
//     regardless of whether it happens to also satisfy ToSym's own
//     constraint -- there is no dawgs equivalent of "this row's kind
//     matched early, stop recursing").
//
// Work accounting matches exec.go's documented model exactly: adjacency()
// already spends one work unit per adjacency slot inspected; this file adds
// exactly one more per row actually emitted (the same "inspect, then charge
// again for what survives" two-tier pattern expandStep/verifyClosingStep/
// scanAnchor already use).
//
// --- shortestPath/allShortestPaths execution ------------------------------
//
// A shortestPath()/allShortestPaths() Step is not expanded by growing rows
// one adjacency hop at a time the way a tree Step is (see runComponent's
// dispatch below): both of its pattern endpoints are resolved to complete
// in-memory node sets FIRST (resolveEndpointSet, reusing scanAnchor's own
// id/objectid/kind-bitmap/full-scan anchoring plus each endpoint's pushed,
// single-symbol WHERE predicates -- this is the in-memory replacement for
// milestone-2's live-PG endpoint resolution, see task-8-brief.md), and only
// then handed to traverse.AllShortestPaths as one Roots x Terminals query.
// This mirrors dawgs' own translation: a shortestPath()/allShortestPaths()
// call compiles to a single recursive-CTE search seeded from *two* resolved
// node sets, not a chain of per-row hops.
package interpret

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/traverse"
)

// ErrSelfEndpoint is returned by a shortestPath()/allShortestPaths() Step
// whose resolved root and terminal endpoint sets intersect while the owning
// query carries no explicit endpoint inequality over that step's own two
// endpoints (Step.HasExplicitEndpointInequality false). PostgreSQL's own
// shortestPath implementation raises SQLSTATE 22023 for exactly this shape
// (a root that is also a terminal aborts its recursive seed query -- see
// traverse.SelfEndpointConflict's doc comment for the milestone-2 in-depth
// citation); this package cannot raise that error itself (there is no live
// pg statement here to fail), so it returns this sentinel instead, which the
// engine is expected to treat as "decline, delegate to PostgreSQL" so the
// caller still observes the real SQLSTATE 22023 pg itself raises.
var ErrSelfEndpoint = errors.New("interpret: self endpoint")

// --- var-length trail expansion --------------------------------------------

// trailFrame is one partial (or complete) trail on expandVarLengthComponent's
// explicit DFS stack: nodes[len(nodes)-1] is the walk's current frontier
// node, edges[i] is the EdgeRef taken from nodes[i] to nodes[i+1], and
// firstIsSelfLoop records whether edges[0] (if any) is a self-loop -- the one
// piece of history the "first-edge self-loop is never extended" rule needs
// that isn't otherwise recoverable from nodes/edges alone once the DFS has
// moved past depth 1. Fresh slices are allocated per pushed frame (mirroring
// traverse/bfs.go's enumerate/pairEnumerate, the existing precedent for this
// exact iterative-stack-instead-of-recursion shape in this codebase); trails
// are capped at Range.Max (documented as small -- MaxExpansionDepth is 15
// unless a query explicitly asks for deeper), so the repeated copying this
// implies is the "small slices, linear containment" tradeoff the brief
// itself calls out, not a hidden hot loop.
type trailFrame struct {
	nodes           []snapshot.NodeID
	edges           []EdgeRef
	firstIsSelfLoop bool
}

// containsFwd reports whether any edge in edges already carries fwd -- the
// TRAIL semantics check ("an edge id may not repeat within one path"): fwd is
// a forward-CSR slot index, which is unique per physical directed edge in the
// snapshot (see EdgeRef's doc comment), so comparing fwd values here is
// exactly equivalent to comparing the database edge ids dawgs' own
// `e0.id != all(path.edge_ids)` guard compares.
func containsFwd(edges []EdgeRef, fwd uint64) bool {
	for _, e := range edges {
		if e.Fwd == fwd {
			return true
		}
	}
	return false
}

// expandVarLengthComponent expands a single-Step component whose Step is a
// variable-length relationship pattern (`*min..max`, Step.Range != nil),
// producing one row per emitted trail (see this file's package doc for the
// exact pinned semantics). It is runComponent's Task 8 dispatch target for
// exactly that shape -- see runComponent's own doc comment for why this
// package handles only a component consisting of one such Step in isolation.
//
// A named relationship variable on a variable-length pattern (`[r*1..3]`)
// binds `r` to a *list* of relationships in real Cypher -- a value shape
// nothing in this package's Row/EdgeRef model represents (EdgeRef is always
// exactly one edge) -- so that shape is declined outright (errUnsupportedStep)
// rather than silently binding something wrong; no shape in this task's
// required corpus needs it. A pattern reusing the same symbol for both
// endpoints (`(n)-[*1..3]->(n)`) is declined for the same "decline rather
// than guess" reason: it would need an identity constraint between the seed
// and every candidate terminal that this function does not implement (the
// required corpus achieves the same "same concrete node at both ends" shape
// via two distinct symbols each anchored to the same id() instead, which
// this function already handles correctly through the ordinary ToSym
// post-filter).
func expandVarLengthComponent(env *Env, meter *workMeter, part *Part, step *Step) ([]*Row, error) {
	if step.EdgeSym != "" || step.FromSym == step.ToSym {
		return nil, errUnsupportedStep
	}

	seeds, err := scanAnchor(env, meter, step.FromSym, part.Nodes[step.FromSym])
	if err != nil {
		return nil, err
	}

	toNC := part.Nodes[step.ToSym]

	var out []*Row
	for _, seed := range seeds {
		rows, err := expandVarLengthTrailsForSeed(env, meter, step, toNC, seed, step.PathSym)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}

	return out, nil
}

// expandVarLengthTrailsForSeed grows exactly one seed row across step (a
// variable-length Step), producing one output row per emitted trail --
// factored out of expandVarLengthComponent (the standalone single-var-step
// dispatch above, which calls this once per scanAnchor-produced seed) so
// exec.go's expandChainComponent (Task 8b's mixed fixed/var-length chain
// executor) can reuse the identical trail-DFS semantics to grow a row that
// already carries earlier steps' own bindings, instead of a freshly scanned
// seed -- see this file's package doc for the exact pinned semantics
// (trail/depth/*0../first-edge-self-loop/multiplicity), all unchanged by
// this refactor.
//
// pathArcKey, when non-empty, additionally binds each output row's own
// per-step trail as a *PathVal under that key -- expandVarLengthComponent
// passes step.PathSym itself (preserving its exact pre-Task-8b behavior:
// this is the whole standalone pattern's own path), while
// expandChainComponent passes a synthetic per-step key (pathStepArcKey,
// exec.go) instead, since a var-length step's own PathSym (when the whole
// chain is named) belongs to the WHOLE chain, not just this one step's
// segment -- see assembleChainPathVal's doc. "" skips binding entirely,
// matching a caller with no path to assemble.
func expandVarLengthTrailsForSeed(env *Env, meter *workMeter, step *Step, toNC *NodeConstraint, seed *Row, pathArcKey string) ([]*Row, error) {
	root, _ := seed.Node(step.FromSym)
	minDepth, maxHops := step.Range.Min, step.Range.Max

	var out []*Row

	if minDepth == 0 && nodeSatisfiesConstraint(env, toNC, root) {
		nr := cloneRow(seed)
		nr.SetNode(step.ToSym, root)
		if pathArcKey != "" {
			nr.SetPathVar(pathArcKey, &PathVal{})
		}
		if err := meter.spend(1); err != nil {
			return nil, err
		}
		out = append(out, nr)
	}

	if maxHops <= 0 {
		return out, nil
	}

	stack := []trailFrame{{nodes: []snapshot.NodeID{root}}}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		depth := len(cur.edges)
		curNode := cur.nodes[depth]

		if depth >= 1 && depth >= minDepth && nodeSatisfiesConstraint(env, toNC, curNode) {
			nr := cloneRow(seed)
			nr.SetNode(step.ToSym, curNode)
			if pathArcKey != "" {
				nr.SetPathVar(pathArcKey, &PathVal{
					Nodes: append([]snapshot.NodeID(nil), cur.nodes...),
					Edges: append([]EdgeRef(nil), cur.edges...),
				})
			}
			if err := meter.spend(1); err != nil {
				return nil, err
			}
			out = append(out, nr)
		}

		if depth == maxHops || (depth == 1 && cur.firstIsSelfLoop) {
			continue
		}

		cands, err := adjacency(env, meter, step, curNode, true)
		if err != nil {
			return nil, err
		}
		for _, c := range cands {
			if !edgeKindOK(step.EdgeKinds, c.kind) {
				continue
			}
			if containsFwd(cur.edges, c.fwd) {
				continue
			}

			nextNodes := make([]snapshot.NodeID, depth+2)
			copy(nextNodes, cur.nodes)
			nextNodes[depth+1] = c.other

			nextEdges := make([]EdgeRef, depth+1)
			copy(nextEdges, cur.edges)
			nextEdges[depth] = EdgeRef{Fwd: c.fwd}

			stack = append(stack, trailFrame{
				nodes:           nextNodes,
				edges:           nextEdges,
				firstIsSelfLoop: cur.firstIsSelfLoop || (depth == 0 && c.other == curNode),
			})
		}
	}

	return out, nil
}

// --- shortestPath / allShortestPaths execution ------------------------------

// expandShortestPathComponent resolves a single-Step component whose Step is
// a shortestPath()/allShortestPaths() pattern into rows, one per path
// traverse.AllShortestPaths returns (see this file's package doc for the
// resolve-both-endpoint-sets-first design and the self-endpoint rule).
//
// Like expandVarLengthComponent, a named relationship-list variable is
// declined (errUnsupportedStep) for the same "no list-of-relationships value
// shape" reason, and an undirected pattern is declined too: traverse has no
// direction parameter at all (BloodHound path-finding is always directed),
// so there is nothing correct to reproduce graph.DirectionBoth with here --
// Plan's own acceptance surface does not appear to produce this shape for a
// shortestPath Step in practice (every migrated corpus shape is directed),
// but this function declines defensively rather than silently mis-serve it
// if it ever did.
func expandShortestPathComponent(env *Env, meter *workMeter, part *Part, step *Step) ([]*Row, error) {
	if step.EdgeSym != "" || step.Direction == graph.DirectionBoth {
		return nil, errUnsupportedStep
	}

	roots, err := resolveEndpointSet(env, meter, step.FromSym, part.Nodes[step.FromSym])
	if err != nil {
		return nil, err
	}
	terminals, err := resolveEndpointSet(env, meter, step.ToSym, part.Nodes[step.ToSym])
	if err != nil {
		return nil, err
	}

	if !step.HasExplicitEndpointInequality && idsIntersect(roots, terminals) {
		return nil, ErrSelfEndpoint
	}

	mode := traverse.ModeOne
	if step.Shortest == ShortestAll {
		mode = traverse.ModeAll
	}

	maxDepth := 0
	if step.Range != nil {
		maxDepth = step.Range.Max
	}

	q := traverse.Query{
		Roots:       traverse.Endpoint{IDs: roots},
		Terminals:   traverse.Endpoint{IDs: terminals},
		Kinds:       kindMaskFor(env, step.EdgeKinds),
		Mode:        mode,
		ExcludeSelf: step.HasExplicitEndpointInequality,
		MaxDepth:    maxDepth,
	}

	rowCap, memLimit, unbounded := shortestPathBudget(meter, maxDepth)
	if !unbounded {
		if rowCap <= 0 {
			return nil, ErrBudget
		}
		q.Limit = rowCap + 1
		q.MemoryLimit = memLimit
	}

	dense, err := traverse.AllShortestPaths(env.Snap, q)
	if err != nil {
		if errors.Is(err, traverse.ErrTooLarge) || errors.Is(err, traverse.ErrMemoryLimit) {
			return nil, ErrBudget
		}
		return nil, err
	}
	if !unbounded && len(dense) > rowCap {
		// traverse.Query.Limit only stops it from producing more than
		// rowCap+1 dense paths; the executor's own remaining budget can
		// admit at most rowCap of them, so more than that must decline
		// outright (ErrBudget) rather than silently serve the first rowCap
		// and drop the rest -- see shortestPathBudget's own doc comment on
		// the all-or-nothing materialization invariant this preserves.
		return nil, ErrBudget
	}

	out := make([]*Row, 0, len(dense))
	for _, p := range dense {
		nr := NewRow()
		nr.SetNode(step.FromSym, p.Nodes[0])
		nr.SetNode(step.ToSym, p.Nodes[len(p.Nodes)-1])
		if step.PathSym != "" {
			pv, err := convertPath(env, p)
			if err != nil {
				return nil, err
			}
			nr.SetPathVar(step.PathSym, pv)
		}
		if err := meter.spend(1); err != nil {
			return nil, err
		}
		out = append(out, nr)
	}
	return out, nil
}

// shortestPathBudget resolves meter's own remaining Budgets into the
// traverse.Query fields that bound how many dense Paths -- and how many
// bytes' worth of them -- traverse.AllShortestPaths is allowed to
// materialize before expandShortestPathComponent's own per-row meter.spend/
// ErrBudget check (above) ever gets a chance to inspect a single one of
// them. Without this, a ModeAll run over a high-fan-out region of the graph
// (many co-equal shortest paths per pair, or many pairs each with a few) can
// build a []Path far larger than any budget would ever admit entirely
// inside traverse's own strategy dispatch: PairBudget/SideBudget only cap
// how many *pairs*/BFS runs a strategy will attempt, not the total path
// count summed across all of them.
//
// rowCap is derived *only* from Budgets.MaxWork's remaining capacity, never
// from Budgets.MaxRows. dense (this function's return value bounds the size
// of, and the slice expandShortestPathComponent's caller loops over) is this
// component's raw, PRE-Part.Where output -- but Budgets.MaxRows counts
// rows admitted after Where filtering everywhere else in this package
// (workMeter.addFinalRow is only ever called on a row that has already
// survived Part.Where). Folding MaxRows' remaining count into this
// component's own cap would compare a post-filter unit against a pre-filter
// quantity: a shortestPath query carrying a residual cross-symbol WHERE
// conjunct that Plan cannot push into either endpoint's own
// NodeConstraint.Predicates (e.g. `s.prop = t.prop`, which touches both
// pattern variables and so can never be a single-symbol predicate) can
// produce a raw dense count well above MaxRows while its filtered result
// still fits comfortably under it -- mixing MaxRows in here would spuriously
// decline that query with ErrBudget (a safe but wasteful delegate) even
// though nothing downstream would ever actually overflow the row budget.
// MaxWork has no such mismatch: Budgets' own doc comment (exec.go) pins work
// as charged uniformly, "+1 per row produced anywhere" -- including this
// function's own per-row meter.spend call below -- so MaxWork's remaining
// capacity is a unit-correct bound on raw component rows regardless of how
// many of them Part.Where later discards. MaxRows enforcement stays exactly
// where it already lives: workMeter.addFinalRow, once a row has actually
// survived Where.
//
// Query.Limit is then set to rowCap+1, one more than the executor could
// actually use -- enough for traverse to stop enumerating (and accumulating
// memory) the moment it is clear the true result cannot fit the remaining
// work budget, while still letting the caller distinguish "the whole result
// fits" (len(dense) <= rowCap) from "it doesn't" (len(dense) == rowCap+1)
// without ever silently truncating a fitting result down to the budget or
// admitting a non-fitting one -- Query's own documented all-or-nothing
// materialization invariant applies here exactly as it does to every other
// evaluator error in this package.
//
// unbounded is true (rowCap and memLimit meaningless) when Budgets.MaxWork
// itself is unset, matching traverse.Query.Limit/MemoryLimit's own "0 =>
// unbounded" contract -- expandShortestPathComponent leaves both fields at
// their zero value in that case, preserving today's behavior for every
// caller that runs with no work budget, regardless of whether MaxRows is
// set (MaxRows alone bounds nothing about this component's own raw output,
// only what Execute admits afterward).
//
// memLimit derives from the same rowCap via the exact per-path byte formula
// traverse's own memBudget already applies internally (bfs.go's
// enumerate/pairEnumerate: 12 bytes per node plus 48 bytes overhead),
// scaled by the worst-case path length this query could ever produce (the
// resolved max-hop depth plus one node) -- a second, independent guard
// against a small number of very deep paths exhausting memory even in a
// query whose path *count* alone would fit under Limit.
func shortestPathBudget(meter *workMeter, maxDepth int) (rowCap int, memLimit uint64, unbounded bool) {
	if meter.budget.MaxWork <= 0 {
		return 0, 0, true
	}
	remaining := meter.budget.MaxWork - meter.work
	if remaining < 0 {
		remaining = 0
	}
	rowCap = int(remaining)

	depth := maxDepth
	if depth <= 0 {
		depth = traverse.MaxDepth
	}
	bytesPerPath := uint64(depth+1)*12 + 48

	return rowCap, uint64(rowCap+1) * bytesPerPath, false
}

// resolveEndpointSet materializes every dense node id satisfying nc, in
// ascending order, exactly like scanAnchor's own candidate-source selection
// (id() anchor > objectid anchor > smallest kind bitmap > full scan) --
// reused directly rather than re-implemented -- with nc.Predicates
// additionally evaluated per candidate (EvalPredicate against a throwaway
// single-symbol Row) so a pushed single-symbol WHERE conjunct narrows the
// endpoint set exactly the way it would narrow pg's own seed query, instead
// of only being caught later by Execute's full Part.Where pass over the
// eventually-assembled row. An evaluator error aborts the whole query
// (returned as-is), per Query's documented all-or-nothing materialization
// invariant.
//
// The returned slice is never nil, even when nothing survives: an empty
// non-nil slice is a *constrained, empty* traverse.Endpoint ({IDs: []...}),
// whereas a nil IDs slice would make traverse.Endpoint.Unconstrained() true
// -- silently matching every node in the snapshot instead of none.
func resolveEndpointSet(env *Env, meter *workMeter, sym string, nc *NodeConstraint) ([]snapshot.NodeID, error) {
	rows, err := scanAnchor(env, meter, sym, nc)
	if err != nil {
		return nil, err
	}

	ids := make([]snapshot.NodeID, 0, len(rows))
	for _, r := range rows {
		id, _ := r.Node(sym)
		ok := true
		if nc != nil {
			for _, pred := range nc.Predicates {
				t, err := EvalPredicate(env, r, pred)
				if err != nil {
					return nil, err
				}
				if t != TriTrue {
					ok = false
					break
				}
			}
		}
		if ok {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// idsIntersect reports whether ascending-ordered a and b share any element,
// via a linear ascending merge (both scanAnchor's kind-bitmap iteration and
// its id()/objectid/full-scan branches already produce ascending, deduped
// output, so no sort is needed here).
func idsIntersect(a, b []snapshot.NodeID) bool {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			return true
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return false
}

// kindMaskFor builds the *snapshot.KindMask traverse.Query.Kinds expects from
// a Step's EdgeKinds list, or nil (traverse's own "every kind allowed"
// sentinel, matching edgeKindOK's identical "empty = any" contract) when
// EdgeKinds is empty.
func kindMaskFor(env *Env, kinds []snapshot.KindID) *snapshot.KindMask {
	if len(kinds) == 0 {
		return nil
	}
	mask := snapshot.NewKindMask(env.Snap.MaxKindID)
	for _, k := range kinds {
		mask.Set(k)
	}
	return mask
}

// errConvertPathEdgeNotFound is returned by convertPath when a traverse.Path
// hop has no matching (target, kind) slot anywhere in the source node's own
// forward-CSR segment. Per convertPath's own doc comment this should be
// unreachable for any Path traverse.AllShortestPaths actually returns against
// a real snapshot -- it exists only so convertPath can fail loudly (aborting
// the whole query, which the caller is expected to delegate to PostgreSQL --
// see expandShortestPathComponent's own error-handling doc) rather than
// silently synthesize a wrong EdgeRef, if that invariant is ever violated
// (e.g. by a hand-built test snapshot, or a future traverse.Path source this
// package doesn't control).
var errConvertPathEdgeNotFound = errors.New("interpret: convertPath: no forward-CSR edge found for path hop")

// convertPath converts one traverse.Path (a node sequence plus one edge kind
// per hop) into a *PathVal (a node sequence plus one EdgeRef per hop),
// resolving each hop's forward-CSR slot by scanning the source node's own
// Out() segment for a target/kind match. The database's edge table enforces
// uniqueness on (start_id, end_id, kind_id) (see hydrate.go's hydratePaths
// doc comment on edgeKey), so in a real snapshot at most one forward-CSR slot
// can ever match a given hop -- the first match found is therefore *the*
// edge, not merely *an* edge, for every snapshot this package actually
// serves queries against. Only a hand-built test snapshot could stage two
// edges of the same kind between the same ordered pair (Builder.AddEdge
// performs no such uniqueness check), and traverse.Path itself carries no
// edge id to disambiguate between them in that degenerate case; converting
// via the first match is the best available deterministic choice there.
//
// A hop with NO matching slot at all should likewise never happen against a
// real snapshot: every traverse.Path this package ever converts was produced
// by traverse.AllShortestPaths walking that exact snapshot's own forward CSR
// one hop at a time (see bfs.go's enumerate/pairEnumerate, which only ever
// push a candidate hop after reading it straight off s.Out(u)), so the same
// (source, target, kind) triple convertPath re-scans for here is, by
// construction, a triple traverse itself just walked across in that CSR.
// Only a Path fabricated by hand (as this file's own unit test does, to
// exercise this branch at all) or a future caller handing convertPath a Path
// computed against a different snapshot than env.Snap could ever reach this
// case for real. Rather than fall back to a fabricated EdgeRef{Fwd: 0} (which
// would silently alias whatever real, unrelated edge happens to occupy
// forward-CSR slot 0), convertPath reports the mismatch as an error, which
// aborts the whole query -- exactly the same "decline and delegate" posture
// this package uses for every other evaluator error, and strictly safer than
// serving a corrupted PathVal.
func convertPath(env *Env, p traverse.Path) (*PathVal, error) {
	edges := make([]EdgeRef, len(p.Kinds))
	for i, k := range p.Kinds {
		u, w := p.Nodes[i], p.Nodes[i+1]
		targets, kinds := env.Snap.Out(u)
		lo := env.Snap.OutOffsets[u]
		found := false
		for j, t := range targets {
			if t == w && kinds[j] == k {
				edges[i] = EdgeRef{Fwd: lo + uint64(j)}
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("interpret: convertPath: hop %d (node %d -> node %d, kind %d): %w", i, u, w, k, errConvertPathEdgeNotFound)
		}
	}
	return &PathVal{
		Nodes: append([]snapshot.NodeID(nil), p.Nodes...),
		Edges: edges,
	}, nil
}
