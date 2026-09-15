// SPDX-License-Identifier: Apache-2.0

// expand.go implements the two
// seams exec.go left open (see its own file comment and
// runComponent's dispatch, below): variable-length relationship-pattern
// expansion (`*min..max`) and shortestPath()/allShortestPaths() step
// execution, both driven directly off one snapshot.View rather than a
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
//   - SELF-LOOPS force a decline. pg's recursive CTE carries an `is_cycle`
//     guard on its SEED-side edge only (the seed arm computes `start = end`
//     and the recursive member requires `not is_cycle`, resetting it to
//     false for deeper edges): a trail whose seed-side edge is a self-loop
//     is emitted at depth 1 but never extended, while a deeper self-loop
//     recurses normally. Which PATTERN edge that guard lands on depends on
//     which side dawgs seeds the CTE from -- and that choice is made per
//     query by version-specific heuristics (optimize/direction.go's
//     InboundTraversalReversalRule, optimize/lowering_plan.go's
//     traversalDirectionDecisionForStep -- verified: a kinds-only terminal
//     with an unconstrained source seeds from the TERMINAL and walks
//     backward, `e0.end_id = seed.root_id` -- and translate's own baseline
//     OptimizePatternConstraintBalance). Mirroring those heuristics would
//     chain this engine's correctness to dawgs internals that can move
//     under any upstream bump, so it deliberately does not: when the
//     current view provably holds NO self-loop of an admitted kind
//     (View.SelfLoopHazard false -- the overwhelmingly common case in AD
//     graphs), the guard can never fire on either side and the seeding
//     choice is unobservable; otherwise every var-length route declines
//     (errUnsupportedStep -> delegate) rather than risk placing the rule on
//     the wrong edge.
//   - Multiplicity: no relationship-uniqueness or per-endpoint-pair dedup is
//     applied anywhere in dawgs' translation for this shape -- every
//     distinct trail (including two trails that use different parallel
//     edges of the same kind between the same two nodes) is a separate CTE
//     row, so this file enumerates all of them rather than collapsing by
//     (root, terminal) pair.
//   - CROSS-STEP uniqueness in a mixed fixed/var-length chain is asymmetric
//     (pinned from translate/traversal.go's
//     previousRelationshipUniquenessConstraint /
//     expansionPreviousRelationshipUniquenessConstraint and verified against
//     generated SQL): a fixed step's edge is excluded from every
//     variable-length step's trail of the same pattern and vice versa
//     (`e_fixed != all (path)` is emitted whichever of the two comes first),
//     while two variable-length steps' trails may legally share an edge (the
//     constraint builders skip preceding expansions entirely). Implemented
//     via Row's two separate edge sets -- see Row.trailEdges' doc.
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
// None of the rules above says anything about which DIRECTION the graph is
// walked in: every one of them is a property of the finished trail, not of the
// order its edges were discovered. That is what lets a standalone
// variable-length pattern whose far endpoint is far cheaper to resolve than
// its near one be enumerated backward over the snapshot's reverse CSR instead,
// seeded from that endpoint, producing the identical rows for a small fraction
// of the work -- see varLengthReverseEligible for when that swap is taken and
// expandVarLengthTrailsToSeed for the clause-by-clause argument that it
// preserves every rule above (the self-loop decline makes the one
// historically direction-sensitive rule moot: no served graph contains an
// admissible self-loop at all).
//
// Work accounting matches exec.go's documented model exactly: adjacency()
// already spends one work unit per adjacency slot inspected; this file adds
// exactly one more per row actually emitted (the same "inspect, then charge
// again for what survives" two-tier pattern expandStep/verifyClosingStep/
// scanAnchor already use). Both enumeration directions charge at the same
// points, so the reverse route's much lower total is a genuine reduction in
// graph visited, not a gap in accounting.
//
// --- shortestPath/allShortestPaths execution ------------------------------
//
// A shortestPath()/allShortestPaths() Step is not expanded by growing rows
// one adjacency hop at a time the way a tree Step is (see runComponent's
// dispatch below): both of its pattern endpoints are resolved to a
// traverse.Endpoint FIRST (resolveEndpoint), then handed to
// traverse.AllShortestPaths as one Roots x Terminals query. resolveEndpoint
// only actually materializes a concrete node-id set for a *narrowing* side
// (ids/objectid/predicates present -- reusing scanAnchor's own
// id/objectid/full-scan anchoring plus each endpoint's pushed, single-symbol
// WHERE predicates, the in-memory replacement for the live-PG endpoint
// resolution an earlier design used); a kinds-only side is instead handed
// to traverse as a lazy kind bitmap it can iterate/probe directly, and a
// truly unconstrained side as traverse's own "matches every node" sentinel
// -- neither ever visits a candidate node one at a time the way scanAnchor
// does, since traverse's own dispatch (below) already picks whichever side
// is cheapest to seed a search from without this package doing that work
// twice. This mirrors dawgs' own translation: a shortestPath()/
// allShortestPaths() call compiles to a single recursive-CTE search seeded
// from *two* resolved node sets, not a chain of per-row hops.

package interpret

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
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
// traverse.SelfEndpointConflict's doc comment for the in-depth citation); this package cannot raise that error itself (there is no live
// pg statement here to fail), so it returns this sentinel instead, which the
// engine is expected to treat as "decline, delegate to PostgreSQL" so the
// caller still observes the real SQLSTATE 22023 pg itself raises.
var ErrSelfEndpoint = errors.New("interpret: self endpoint")

// --- var-length trail expansion --------------------------------------------

// trailFrame is one partial (or complete) trail on expandVarLengthComponent's
// explicit DFS stack: nodes[len(nodes)-1] is the walk's current frontier
// node, edges[i] is the EdgeRef taken from nodes[i] to nodes[i+1].
// Fresh slices are allocated per pushed frame (mirroring
// traverse/bfs.go's enumerate/pairEnumerate, the existing precedent for this
// exact iterative-stack-instead-of-recursion shape in this codebase); trails
// are capped at Range.Max (documented as small -- MaxExpansionDepth is 15
// unless a query explicitly asks for deeper), so the repeated copying this
// implies is a deliberate "small slices, linear containment" tradeoff,
// not a hidden hot loop.
type trailFrame struct {
	nodes []snapshot.NodeID
	edges []EdgeRef
}

// containsFwd reports whether any edge in edges already carries c's own
// identity -- the TRAIL semantics check ("an edge id may not repeat within
// one path"). !env.Snap.Overlay(): c.fwd is a forward-CSR slot index, unique
// per physical directed edge in the snapshot (see EdgeRef's doc comment), so
// comparing fwd values here is exactly equivalent to comparing the database
// edge ids dawgs' own `e0.id != all(path.edge_ids)` guard compares.
// Overlay(): a forward-CSR slot doesn't cover a delta edge at all, so c's
// own database edge id (c.edgeID, exactly what adjacency yielded) is
// compared instead -- still unique per physical directed edge, since it IS
// the edge's own database id.
func containsFwd(env *Env, edges []EdgeRef, c adjCandidate) bool {
	if !env.Snap.Overlay() {
		for _, e := range edges {
			if e.Fwd == c.fwd {
				return true
			}
		}
		return false
	}
	for _, e := range edges {
		if e.EdgeID == c.edgeID {
			return true
		}
	}
	return false
}

// expandVarLengthComponent expands a single-Step component whose Step is a
// variable-length relationship pattern (`*min..max`, Step.Range != nil),
// producing one row per emitted trail (see this file's package doc for the
// exact pinned semantics). It is runComponent's dispatch target for
// exactly that shape -- see runComponent's own doc comment for why this
// package handles only a component consisting of one such Step in isolation.
//
// A named relationship variable on a variable-length pattern (`[r*1..3]`)
// binds `r` to a *list* of relationships in real Cypher -- a value shape
// nothing in this package's Row/EdgeRef model represents (EdgeRef is always
// exactly one edge) -- so that shape is declined outright (errUnsupportedStep)
// rather than silently binding something wrong; no shape in the required
// corpus needs it. A pattern reusing the same symbol for both
// endpoints (`(n)-[*1..3]->(n)`) is declined for the same "decline rather
// than guess" reason: it would need an identity constraint between the seed
// and every candidate terminal that this function does not implement (the
// required corpus achieves the same "same concrete node at both ends" shape
// via two distinct symbols each anchored to the same id() instead, which
// this function already handles correctly through the ordinary ToSym
// post-filter).
//
// Which of the pattern's two endpoints this function actually seeds from is
// decided by varLengthReverseEligible (below), NOT fixed at FromSym: a pattern
// whose far endpoint resolves to a handful of nodes while its near endpoint
// matches everything (`(s)-[:MemberOf*0..]->(g:Group) WHERE g.objectid ENDS
// WITH '-516'`) is dramatically cheaper to enumerate backward over the
// snapshot's reverse CSR, and produces exactly the same rows. See
// expandVarLengthTrailsToSeed's own doc comment for the trail-by-trail
// argument that the two routes agree, and varLengthReverseEligible for when
// the swap is taken at all.
//
// That decision deliberately lives HERE, in the full executor, and not in
// expandVarLengthComponentFrom -- the tail a chunked LIMIT driver calls once
// per batch of FromSym anchor rows it has already collected. Such a driver's
// whole mechanism is "scan a bounded chunk of the near endpoint, expand it,
// stop once enough rows survive"; a ToSym-seeded walk has no near-endpoint
// chunk to be handed, would ignore the anchor rows it was given, and would
// re-enumerate the entire pattern on every batch. Keeping the choice out of
// that tail also leaves the driver's own zero-cost eligibility probe (a call
// with no anchor rows at all) reading exactly the decline it always did.
// Combining early termination with constrained-side seeding is a separate
// piece of work needing its own stopping rule, not a variation on this one.
//
// User-visible consequence of that gap: a query eligible for the chunked
// LIMIT driver (limitEligibleComponent) always takes the forward route
// above and never reverse-seeds, even when its FromSym side is the
// expensive one -- so adding a LIMIT to an otherwise-identical query can
// make it budget-decline (interpret.ErrBudget) where the unlimited form
// serves fine via reverse seeding, purely because the LIMIT made it take
// the chunked path instead of this function's own dispatch.
func expandVarLengthComponent(env *Env, meter *workMeter, part *Part, step *Step) ([]*Row, error) {
	if step.EdgeSym != "" || step.FromSym == step.ToSym {
		return nil, errUnsupportedStep
	}

	if varLengthReverseEligible(env, part, step) {
		return expandVarLengthComponentReverse(env, meter, part, step)
	}
	return expandVarLengthComponentForward(env, meter, part, step)
}

// expandVarLengthComponentForward is the ordinary route: scan the pattern's
// own FromSym for seed rows, then grow each one forward across step. Split out
// of expandVarLengthComponent purely so the dispatch above reads as a choice
// between two named, independently testable routes -- the behavior here is
// exactly what expandVarLengthComponent always did, including the fact that
// the EdgeSym/FromSym==ToSym decline above happens BEFORE any anchor scan is
// charged for.
func expandVarLengthComponentForward(env *Env, meter *workMeter, part *Part, step *Step) ([]*Row, error) {
	seeds, err := scanAnchor(env, meter, step.FromSym, part.Nodes[step.FromSym])
	if err != nil {
		return nil, err
	}

	return expandVarLengthComponentFrom(env, meter, part, step, seeds)
}

// expandVarLengthComponentFrom is expandVarLengthComponent's own per-seed
// trail expansion (see its doc comment above), factored out so it can grow
// an anchorRows chunk a caller already collected for step.FromSym instead of
// always starting a scanAnchor of its own -- one call to
// expandVarLengthTrailsForSeed per seed row, exactly as expandVarLengthComponent
// itself always ran.
//
// The EdgeSym/FromSym==ToSym precondition expandVarLengthComponent checks
// before ever scanning an anchor is deliberately re-checked here too, not
// hoisted out and left solely to that caller: a future caller reaching this
// tail directly (e.g. a chunked LIMIT driver handed anchorRows for a step it
// never itself validated) must get the same errUnsupportedStep decline
// expandVarLengthComponent would have given it, rather than silently
// expanding a shape this package's Row/EdgeRef model cannot represent (see
// expandVarLengthComponent's own doc for why).
func expandVarLengthComponentFrom(env *Env, meter *workMeter, part *Part, step *Step, anchorRows []*Row) ([]*Row, error) {
	if step.EdgeSym != "" || step.FromSym == step.ToSym {
		return nil, errUnsupportedStep
	}

	toNC := part.Nodes[step.ToSym]

	var out []*Row
	for _, seed := range anchorRows {
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
// exec.go's expandChainComponent (the mixed fixed/var-length chain
// executor) can reuse the identical trail-DFS semantics to grow a row that
// already carries earlier steps' own bindings, instead of a freshly scanned
// seed -- see this file's package doc for the exact pinned semantics
// (trail/depth/*0../first-edge-self-loop/multiplicity), all unchanged by
// this refactor.
//
// pathArcKey, when non-empty, additionally binds each output row's own
// per-step trail as a *PathVal under that key -- expandVarLengthComponent
// passes step.PathSym itself (preserving its standalone behavior:
// this is the whole standalone pattern's own path), while
// expandChainComponent passes a synthetic per-step key (pathStepArcKey,
// exec.go) instead, since a var-length step's own PathSym (when the whole
// chain is named) belongs to the WHOLE chain, not just this one step's
// segment -- see assembleChainPathVal's doc. "" skips binding entirely,
// matching a caller with no path to assemble.
func expandVarLengthTrailsForSeed(env *Env, meter *workMeter, step *Step, toNC *NodeConstraint, seed *Row, pathArcKey string) ([]*Row, error) {
	// A self-loop of an admitted kind anywhere in the view makes pg's
	// seed-side is_cycle guard placement observable, and that placement
	// depends on dawgs heuristics this engine deliberately does not mirror
	// -- decline and delegate. See the package doc's SELF-LOOPS bullet and
	// View.SelfLoopHazard.
	if env.Snap.SelfLoopHazard(step.EdgeKinds) {
		return nil, errUnsupportedStep
	}

	root, _ := seed.Node(step.FromSym)
	minDepth, maxHops := step.Range.Min, step.Range.Max

	var out []*Row

	if minDepth == 0 && nodeSatisfiesConstraint(env, toNC, root) {
		nr := cloneRow(seed)
		nr.SetNode(step.ToSym, root)
		if pathArcKey != "" {
			// Empty either way (the zero-length case); reversePathVal would
			// be a no-op here regardless of step.Reversed, so it is skipped.
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
			// Record the trail's own edges so a later FIXED step of the same
			// chain cannot resolve to one of them (dawgs emits `e.id != all
			// (path)` for the expansion preceding a fixed step); a later
			// var-length step's own trail deliberately does NOT consult this
			// set -- see Row.trailEdges' doc for the full pinned contract.
			for _, e := range cur.edges {
				nr.markTrailEdge(edgeRefIdentity(env.Snap, e))
			}
			if pathArcKey != "" {
				pv := &PathVal{
					Nodes: append([]snapshot.NodeID(nil), cur.nodes...),
					Edges: append([]EdgeRef(nil), cur.edges...),
				}
				if step.Reversed {
					// This trail was walked FromSym (traversal source) to
					// curNode, but step.Reversed means FromSym is itself the
					// pattern's SECOND-written endpoint (buildStep's
					// inbound-arrow swap) -- so the path as built is in the
					// opposite order from what `RETURN pathArcKey` (a whole
					// standalone pattern's own PathSym; never a chain's
					// per-step segment -- see Step.Reversed's own doc) must
					// produce. Flip it back to pattern-written order.
					reversePathVal(pv)
				}
				nr.SetPathVar(pathArcKey, pv)
			}
			if err := meter.spend(1); err != nil {
				return nil, err
			}
			out = append(out, nr)
		}

		if depth == maxHops {
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
			if containsFwd(env, cur.edges, c) {
				continue
			}
			// An edge a FIXED step of the same pattern already consumed is
			// off-limits to this trail (dawgs emits `e_fixed != all (path)`
			// regardless of which side of the expansion the fixed step sits
			// on); seed carries exactly those edges in usedEdges. Edges used
			// by ANOTHER var-length step's trail are deliberately not
			// excluded -- see Row.trailEdges' doc.
			if seed.edgeUsed(candidateIdentity(env.Snap, c)) {
				continue
			}

			nextNodes := make([]snapshot.NodeID, depth+2)
			copy(nextNodes, cur.nodes)
			nextNodes[depth+1] = c.other

			nextEdges := make([]EdgeRef, depth+1)
			copy(nextEdges, cur.edges)
			nextEdges[depth] = edgeRefFor(env.Snap, c)

			stack = append(stack, trailFrame{
				nodes: nextNodes,
				edges: nextEdges,
			})
		}
	}

	return out, nil
}

// --- var-length trail expansion, seeded from the constrained side ----------

// varLengthReverseEligible reports whether step -- a standalone
// variable-length pattern whose EdgeSym/self-pattern preconditions the caller
// has already checked -- is cheaper to enumerate backward from its far
// endpoint than forward from its near one.
//
// The cost test is a comparison of the two endpoints' own CANDIDATE SOURCES
// (anchorRank: an id() lookup beats an objectid lookup beats the smallest
// AND-ed kind bitmap beats a full node scan, ties broken by bitmap
// population), not a fixed "N times smaller" multiple of their sizes.
//
// A fixed multiple is the obvious alternative and it is wrong here, in both
// directions. Too high a multiple rejects the exact shape this route exists
// for: in an Active-Directory-shaped graph roughly one node in eight is a
// Group, so `(s)-[:MemberOf*0..]->(g:Group) WHERE g.objectid ENDS WITH '-516'`
// -- whose far side really resolves to a handful of nodes once the pushed
// predicate runs -- separates its two candidate SOURCES by well under an order
// of magnitude, even though the near side is every node in the graph and the
// far side ends up being four of them. Too low a multiple is not safe either,
// since a raw size ratio says nothing about how much a pushed predicate will
// cut the far side down. The tier comparison sidesteps both: it is exactly the
// ordering the ordinary anchor chooser already trusts to pick a component's
// cheapest starting symbol, applied to the same question here.
//
// Three further conditions make that comparison sound rather than merely
// plausible:
//
//   - The far endpoint must genuinely NARROW -- carry ids, an objectid anchor,
//     or pushed single-symbol predicates -- not merely carry kind labels. A
//     kinds-only far endpoint contributes no filtering beyond what its own
//     candidate source already enumerates, so seeding from it buys nothing
//     while giving up the near side's structure.
//   - The near endpoint must NOT narrow. If it does, it is already the cheap
//     side and the ordinary forward route is the right one.
//   - The near endpoint's own candidate source must COST what a full scan
//     costs (scanEquivalentNearSide) -- not merely be "not narrowing". A kind
//     bitmap is "not narrowing" in endpointNarrows' sense (the bitmap IS its
//     candidate source, so re-checking it filters nothing further), but a
//     SMALL one is still a bounded seed set, and this whole comparison is
//     silent about
//     what happens AFTER seeding: the reverse walk's cost is driven by each
//     far-side seed's IN-DEGREE, which has no relationship to how cheap that
//     seed was to find, and which this package has no way to estimate before
//     walking the edges themselves (unlike a candidate source's SIZE, nothing
//     tracks in-degree ahead of time). A one-node kind bitmap whose lone node
//     happens to be a hub with hundreds of predecessors -- e.g.
//     `(c:Computer)-[:MemberOf*1..]->(g) WHERE g.objectid = '...'`, where `c`
//     ranges over a small Computer kind bitmap rather than a full scan --
//     would pass every OTHER condition here (far side narrows, near side does
//     not, far side's tier beats near side's) while costing far more to
//     reverse than to walk forward. Requiring scan-equivalent cost is the
//     cheapest available way to rule this out: at that population the near
//     side's cost is env.Snap.NodeCount() regardless of graph structure, so
//     there is no structural surprise (like an unlucky seed's in-degree) left
//     to be wrong about on the near side.
//
//     What "scan-equivalent" buys over the exact `tier == tierScan` test this
//     condition used to spell itself as: a kind bitmap holding essentially
//     every node in the graph narrows nothing and costs a full scan to
//     enumerate, so refusing the route for it was a distinction with no
//     difference -- and an expensive one, because BloodHound labels every AD
//     node `Base` and writes its shipped prebuilts against it. See
//     scanEquivalentNearSide for the measurement.
//
// What that leaves bounded, and what it does not:
//
//   - SEEDING is bounded. The far side's candidate source is, by the tier
//     comparison, never a more expensive tier to enumerate than the near
//     side's full scan -- so resolving the far side's seed set (including its
//     pushed single-symbol predicates) costs at most one pass over a
//     candidate source no larger than the one the forward route would have
//     scanned anyway.
//   - TRAVERSAL is NOT bounded by this function at all. Once seeded, the
//     reverse walk fans out over the snapshot's reverse CSR by each seed's
//     actual in-degree and (across further hops) the in-degree of everything
//     it reaches; nothing computed here estimates or caps that. The only
//     backstop is the work meter shared with every other path in this
//     package: a walk that fans out too far spends past its remaining budget
//     and returns ErrBudget from meter.spend, which the caller treats exactly
//     like any other budget overrun -- a decline to PostgreSQL, never a wrong
//     or truncated answer. The point of the conditions above is to make that
//     decline RARE by only reversing when the near side is provably no
//     cheaper to seed from, not to make an oversized reverse walk impossible.
//
// And when this function declines, the forward route runs having spent
// nothing at all: every input to the decision is read from constraint
// metadata and live kind-bitmap populations, with no metered work of its
// own, so an ineligible pattern's row set, error behavior and work total are
// all exactly what they were before this route existed.
//
// The direction requirement is defensive rather than load-bearing: an
// undirected variable-length pattern is already rejected at plan time (a
// compiled Step with a Range always carries an outbound direction), but this
// route walks the reverse CSR unconditionally once chosen, so it states the
// precondition it actually relies on rather than inheriting it.
func varLengthReverseEligible(env *Env, part *Part, step *Step) bool {
	if step.Direction != graph.DirectionOutbound {
		return false
	}
	fromNC, toNC := part.Nodes[step.FromSym], part.Nodes[step.ToSym]
	if !endpointNarrows(toNC) || endpointNarrows(fromNC) {
		return false
	}
	fromRank := rankOf(env, fromNC)
	if !scanEquivalentNearSide(env, fromRank) {
		return false
	}
	return rankOf(env, toNC).better(fromRank)
}

// scanEquivalentSlack sets how much of the graph a kind bitmap may be missing
// and still count as a full scan: at 100, a kind must cover 99% of nodes.
const scanEquivalentSlack = 100

// scanEquivalentNearSide reports whether r -- a variable-length step's NEAR
// endpoint rank -- costs what a full scan costs, which is what
// varLengthReverseEligible's third condition is actually about (see its doc).
//
// A bare tierScan qualifies by definition. So does a kind bitmap that holds
// essentially every node in the snapshot: such a "kind" narrows nothing, so
// enumerating it IS the full scan the condition means to require, and the
// structural hazard the condition guards against -- a SMALL kind bitmap whose
// few seeds turn out to be high in-degree hubs -- cannot arise at that
// population.
//
// This is the second half of the same bug the kindOnlyPredicate rule below
// fixed. That one taught endpointNarrows to ignore a kind test written in
// WHERE; a kind written in the PATTERN never reached endpointNarrows at all,
// it reached this cost test through rankOf -- which returns tierKind for any
// non-empty Kinds list, and anchorRank.better compares tier before size. So
// `(:Base)`, 1,025,105 nodes out of 1,025,106 in bench/shgen's 500k graph,
// outranked a full scan of 1,025,106 and disqualified the constrained-side
// route. On the shipped "all members of Protected Users" prebuilt
// (`(:Base)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-525'`)
// that cost 2551ms where seeding from the far side costs 50ms -- 51x, for the
// identical answer, with the whole difference in which end got seeded.
//
// The threshold is a FRACTION rather than an exact equality deliberately, and
// it is load-bearing in both directions:
//
//   - Exact equality would never fire. BloodHound's datapipe leaves at least
//     one node (its Meta node) unlabelled by `Base`, so the bitmap is always
//     at least one short of the node count.
//   - A fraction is also the semantically right answer on a hybrid graph,
//     where `Base` covers only the AD half of an AD+Entra deployment. There
//     `(:Base)` genuinely IS a narrowing, its population falls far below the
//     threshold, and this declines -- correctly, not as a missed win.
func scanEquivalentNearSide(env *Env, r anchorRank) bool {
	if r.tier == tierScan {
		return true
	}
	if r.tier != tierKind {
		return false
	}
	total := env.Snap.NodeCount()
	return total > 0 && r.size >= total-total/scanEquivalentSlack
}

// endpointNarrows reports whether nc actually cuts its symbol's candidate set
// below whatever its candidate source already enumerates: explicit ids, an
// objectid anchor, or pushed single-symbol WHERE predicates. Kind labels alone
// do not count -- the kind bitmap IS the candidate source for a kinds-only
// constraint, so re-checking it filters nothing.
//
// That last rule applies to a kind test WHEREVER IT IS WRITTEN. BloodHound's
// shipped prebuilt queries spell their near-endpoint type filter as a WHERE
// label disjunction (`WHERE (a:User or a:Computer)`) rather than as a pattern
// label, and partBuilder.pushdown copies every single-symbol conjunct into
// Predicates regardless of its shape -- so without kindOnlyPredicate below,
// such a query counted as "narrowing" purely because of a type test, which
// disqualified the constrained-side route in varLengthReverseEligible and
// sent the whole query down a full scan of every node in the graph. That is
// what made the shipped "all Domain Admins" query 84x slower than PostgreSQL
// on a 1M-node graph (bench/shgen's 500k measurement); the kind test does not
// change which candidate SOURCE the symbol is enumerated from, which is the
// only thing this predicate is meant to be about.
func endpointNarrows(nc *NodeConstraint) bool {
	if nc == nil {
		return false
	}
	if len(nc.IDs) > 0 || nc.ObjectIDAnchor != nil {
		return true
	}
	for _, p := range nc.Predicates {
		if !kindOnlyPredicate(p) {
			return true
		}
	}
	return false
}

// kindOnlyPredicate reports whether expr tests nothing but kind membership --
// a KindMatcher, or any and/or/not/parenthesised combination of them. Such a
// predicate filters exactly what a pattern label would, so endpointNarrows
// treats it the same way it treats pattern labels: as part of the candidate
// source rather than as a narrowing of it. Anything else (a property
// comparison, a function call, a bare truthiness test) returns false, so a
// genuinely selective predicate keeps its narrowing status.
func kindOnlyPredicate(expr cypher.Expression) bool {
	switch typed := unwrapParens(expr).(type) {
	case *cypher.KindMatcher:
		return typed != nil
	case *cypher.Negation:
		return typed != nil && kindOnlyPredicate(typed.Expression)
	case *cypher.Conjunction:
		return typed != nil && allKindOnly(typed.GetAll())
	case *cypher.Disjunction:
		return typed != nil && allKindOnly(typed.GetAll())
	default:
		return false
	}
}

func allKindOnly(exprs []cypher.Expression) bool {
	if len(exprs) == 0 {
		return false
	}
	for _, e := range exprs {
		if !kindOnlyPredicate(e) {
			return false
		}
	}
	return true
}

// expandVarLengthComponentReverse enumerates step's trails backward: it
// resolves the pattern's far endpoint (ToSym) to a concrete node-id set --
// including that endpoint's own pushed single-symbol predicates, exactly the
// way a shortestPath pattern's narrowing side is resolved -- and grows each of
// those nodes backward over the snapshot's reverse CSR, binding the pattern's
// near endpoint (FromSym) on every node it reaches.
//
// Applying ToSym's pushed predicates while seeding is what makes this route
// worth taking at all (they are usually the entire reason the far side is
// small), and it is safe because those predicates are a redundant copy of
// conjuncts the owning Part's complete WHERE still carries: every row this
// route declines to produce for failing one is a row the forward route
// produces and the pipeline's own WHERE pass then discards. The two routes
// therefore agree on exactly the quantity any caller observes -- the
// post-WHERE result -- while this one avoids walking the graph for rows
// destined to be thrown away.
func expandVarLengthComponentReverse(env *Env, meter *workMeter, part *Part, step *Step) ([]*Row, error) {
	fromNC := part.Nodes[step.FromSym]

	seedIDs, err := resolveEndpointSet(env, meter, step.ToSym, part.Nodes[step.ToSym])
	if err != nil {
		return nil, err
	}

	var out []*Row
	for _, id := range seedIDs {
		seed := NewRow()
		seed.SetNode(step.ToSym, id)
		rows, err := expandVarLengthTrailsToSeed(env, meter, step, fromNC, seed, step.PathSym)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}

	return out, nil
}

// reverseTrailFrame is one partial (or complete) trail on
// expandVarLengthTrailsToSeed's explicit DFS stack, the mirror image of
// trailFrame: nodes[0] is the walk's own seed (the pattern's FAR endpoint),
// nodes[len(nodes)-1] is the frontier node currently under consideration as
// the pattern's NEAR endpoint, and edges[i] is the EdgeRef traversed from
// nodes[i+1] to nodes[i] -- i.e. both slices run in BACKWARD-DISCOVERY order,
// the exact reverse of the pattern's own FromSym-to-ToSym order.
type reverseTrailFrame struct {
	nodes []snapshot.NodeID
	edges []EdgeRef
}

// expandVarLengthTrailsToSeed grows exactly one FAR-endpoint seed row backward
// across step, producing one output row per emitted trail -- the mirror image
// of expandVarLengthTrailsForSeed, and required to emit precisely the same set
// of trails the whole-pattern forward enumeration would.
//
// That equality is not an aspiration; it follows from the fact that the pinned
// forward semantics (see this file's package doc) characterize an emitted
// trail entirely by LOCAL properties of the trail itself, with no dependence
// on the order its edges were discovered in. A node sequence n0 -> ... -> nk
// joined by edges e0 .. e(k-1) is emitted exactly when:
//
//   - every ei is distinct (trail semantics; compared by forward-CSR slot,
//     which is unique per physical directed edge),
//   - every ei's kind is admitted by the pattern's edge-kind list,
//   - k is at most the resolved max depth and at least the min depth,
//   - the near endpoint's own constraint holds at n0 and the far endpoint's at
//     nk.
//
// Each clause is checked below on the same trail the forward walk would have
// checked it on. Two of them need care in this direction:
//
//   - Depth is counted in EDGES, identically either way, so the min/max bounds
//     transfer unchanged.
//   - The endpoint constraints swap roles: the seed set is chosen by the FAR
//     endpoint's constraint (plus its pushed predicates, see
//     expandVarLengthComponentReverse), and the NEAR endpoint's constraint is
//     what each reached node is tested against.
//
// The historically direction-sensitive clause -- pg's seed-side self-loop
// dead-end guard -- no longer appears in either walker: both decline
// outright whenever the view holds any self-loop of an admitted kind (see
// the package doc's SELF-LOOPS bullet), so every trail either walker is
// ever allowed to enumerate contains no self-loop at all.
//
// pathArcKey behaves exactly as it does for the forward walk: when non-empty,
// each output row additionally binds its own trail as a *PathVal under that
// key.
func expandVarLengthTrailsToSeed(env *Env, meter *workMeter, step *Step, fromNC *NodeConstraint, seed *Row, pathArcKey string) ([]*Row, error) {
	// Same self-loop hazard decline as the forward walker -- see its comment
	// and the package doc's SELF-LOOPS bullet.
	if env.Snap.SelfLoopHazard(step.EdgeKinds) {
		return nil, errUnsupportedStep
	}

	terminal, _ := seed.Node(step.ToSym)
	minDepth, maxHops := step.Range.Min, step.Range.Max

	var out []*Row

	if minDepth == 0 && nodeSatisfiesConstraint(env, fromNC, terminal) {
		nr := cloneRow(seed)
		nr.SetNode(step.FromSym, terminal)
		if pathArcKey != "" {
			// Empty either way (the zero-length case), so neither the
			// discovery-order flip below nor reversePathVal would change it.
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

	stack := []reverseTrailFrame{{nodes: []snapshot.NodeID{terminal}}}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		depth := len(cur.edges)
		curNode := cur.nodes[depth]

		if depth >= 1 && depth >= minDepth && nodeSatisfiesConstraint(env, fromNC, curNode) {
			nr := cloneRow(seed)
			nr.SetNode(step.FromSym, curNode)
			// Mirror of the forward walker's trail-edge recording -- see
			// Row.trailEdges' doc. Unreachable in a mixed chain today (only
			// standalone single-step components take this route), recorded
			// anyway so both walkers keep identical bookkeeping.
			for _, e := range cur.edges {
				nr.markTrailEdge(edgeRefIdentity(env.Snap, e))
			}
			if pathArcKey != "" {
				pv := &PathVal{
					Nodes: append([]snapshot.NodeID(nil), cur.nodes...),
					Edges: append([]EdgeRef(nil), cur.edges...),
				}
				if !step.Reversed {
					// cur.nodes/cur.edges run in backward-discovery order (the
					// far endpoint first), so pattern order -- which runs
					// FromSym to ToSym -- is their exact reverse. A step whose
					// original Cypher pattern used a backward arrow then needs
					// one FURTHER flip, back to the order the pattern was
					// written in (see reversePathVal's own doc); the two flips
					// cancel, which is why discovery order is kept verbatim for
					// exactly that case and flipped once for every other.
					reversePathVal(pv)
				}
				nr.SetPathVar(pathArcKey, pv)
			}
			if err := meter.spend(1); err != nil {
				return nil, err
			}
			out = append(out, nr)
		}

		if depth == maxHops {
			continue
		}

		cands, err := adjacency(env, meter, step, curNode, false)
		if err != nil {
			return nil, err
		}
		for _, c := range cands {
			if !edgeKindOK(step.EdgeKinds, c.kind) {
				continue
			}
			if containsFwd(env, cur.edges, c) {
				continue
			}
			// Mirror of the forward walker's fixed-edge exclusion -- see
			// Row.trailEdges' doc. A reverse seed is always a fresh row today
			// (standalone components only), so this is a no-op until a caller
			// ever hands this walker a row carrying fixed-step edges.
			if seed.edgeUsed(candidateIdentity(env.Snap, c)) {
				continue
			}

			nextNodes := make([]snapshot.NodeID, depth+2)
			copy(nextNodes, cur.nodes)
			nextNodes[depth+1] = c.other

			nextEdges := make([]EdgeRef, depth+1)
			copy(nextEdges, cur.edges)
			nextEdges[depth] = edgeRefFor(env.Snap, c)

			stack = append(stack, reverseTrailFrame{
				nodes: nextNodes,
				edges: nextEdges,
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

	roots, err := resolveEndpoint(env, meter, step.FromSym, part.Nodes[step.FromSym])
	if err != nil {
		return nil, err
	}
	terminals, err := resolveEndpoint(env, meter, step.ToSym, part.Nodes[step.ToSym])
	if err != nil {
		return nil, err
	}

	if !step.HasExplicitEndpointInequality && endpointsIntersect(env.Snap, roots, terminals) {
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

	sideBudget, pairBudget := strategyBudgetOverrides(meter, env.Snap)

	q := traverse.Query{
		Roots:       roots,
		Terminals:   terminals,
		Kinds:       kindMaskFor(env, step.EdgeKinds),
		Mode:        mode,
		ExcludeSelf: step.HasExplicitEndpointInequality,
		MaxDepth:    maxDepth,
		SideBudget:  sideBudget,
		PairBudget:  pairBudget,
	}

	rowCap, memLimit, unbounded := shortestPathBudget(meter, maxDepth)
	if !unbounded {
		if rowCap <= 0 {
			return nil, ErrBudget
		}
		q.Limit = int(shortestPathLimit(int64(rowCap)+1, meter, part, step))
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
		//
		// This still fires only when the BUDGET, not the user's own LIMIT,
		// is what cut enumeration short: whenever the pushdown above set
		// q.Limit to meter.limitTarget (<= rowCap by construction), dense can
		// never exceed rowCap in the first place, so this branch simply never
		// triggers for that case -- len(dense) == the user's target is a
		// success, served below, with no extra branch needed to say so.
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
			if step.Reversed {
				// p (and therefore pv) runs FromSym (roots) to ToSym
				// (terminals) in TRAVERSAL order, but step.Reversed means
				// FromSym is the pattern's SECOND-written endpoint
				// (buildStep's inbound-arrow swap) -- e.g.
				// `shortestPath((t:Group)<-[:R*1..]-(s))` swaps to
				// FromSym=s, ToSym=t internally, so p goes s->t while the
				// pattern was written t first. Flip pv back to
				// pattern-written order before it is ever handed to a
				// caller (RETURN p); see reversePathVal's own doc. Row's
				// own FromSym/ToSym node bindings above are untouched --
				// they are internal bookkeeping, not the displayed path.
				reversePathVal(pv)
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
// shortestPathLimit (below) additionally narrows that same rowCap+1 down
// to the query's own user-written LIMIT (when eligible -- see its own
// noResidualWhere gate) precisely because doing so sidesteps the exact
// pre/post-Where unit mismatch this comment just spent two paragraphs ruling
// MaxRows out over: MaxRows is unsafe to fold in here unconditionally
// because it counts POST-filter rows while dense is PRE-filter, and Plan can
// leave a residual cross-symbol conjunct (`s.prop = t.prop`) neither this
// function nor resolveEndpoint has any way to evaluate before Path
// materialization. The user's own LIMIT has no such mismatch ONLY when that
// same residual-conjunct case is absent (noResidualWhere's own check): with
// nothing left for Part.Where to filter, every dense Path this component
// produces is already guaranteed to survive into the final result, so
// stopping traversal once LIMIT many exist is equivalent to enumerating
// everything and truncating afterward -- exactly the property MaxRows
// cannot offer here without that same guarantee. This is why the two
// caps -- MaxRows (never used, mismatched by default) and the user's own
// LIMIT (used, but gated behind confirming the same mismatch cannot occur
// for this specific query) -- get such different treatment despite looking
// like the same "row count" idea at a glance.
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

// shortestPathLimit resolves the traverse.Query.Limit value for one
// shortestPath()/allShortestPaths() component: rowCapPlusOne
// (shortestPathBudget's own pre-existing enumeration cutoff) narrowed down
// to the query's own user-written LIMIT exactly when doing so is safe, and
// left unchanged otherwise. Pulled out of expandShortestPathComponent as its
// own function so each of the narrowing conditions below can be exercised
// directly -- traverse.AllShortestPaths' own memory-budget backstop
// (shortestPathBudget's memLimit doc) is calibrated off the query's
// worst-case possible path depth, not the depth paths in a given graph
// actually turn out to have, so it is frequently far looser than rowCap+1
// and can silently absorb a mistake here behind an identical-looking
// ErrBudget from a different cause, with no observable difference at that
// level between "narrowed correctly" and "narrowed wrong".
//
// The narrowing fires only when: a target is actually threaded in
// (limitTargetSet -- workMeter's own doc on why this, not limitTarget's
// zero value, is what "eligible at all" means), that target is strictly
// positive, it is smaller than rowCapPlusOne (never widen the cutoff -- a
// target the budget cannot afford must still decline exactly as an
// unlimited query would), and the component's Part carries no residual
// WHERE this function cannot itself evaluate (noResidualWhere -- see
// shortestPathBudget's own doc for why that gate is what makes this safe at
// all, the same reason Budgets.MaxRows is never folded in here directly).
//
// limitTarget == 0 (a literal `LIMIT 0`) is deliberately excluded even
// though workMeter's own convention treats it as a "set" target like any
// other: traverse.Query.Limit's contract is "0 => unbounded" (traverse.go),
// the OPPOSITE of what forwarding a literal zero would need to mean here.
// The query's final answer comes out right either way -- the top-level
// SKIP/LIMIT pass empties the whole result for a literal LIMIT 0 regardless
// of how many rows this component itself produced -- but handing traverse
// limit == 0 would silently trade the tightest cutoff available
// (rowCapPlusOne) for no cutoff at all, enumerating up to whatever the much
// looser memory backstop allows before erroring out, purely wasted work
// this function exists to avoid paying.
func shortestPathLimit(rowCapPlusOne int64, meter *workMeter, part *Part, step *Step) int64 {
	if meter.limitTargetSet && meter.limitTarget > 0 && meter.limitTarget < rowCapPlusOne && noResidualWhere(part, step) {
		return meter.limitTarget
	}
	return rowCapPlusOne
}

// strategyBudgetOverrides derives traverse.Query.SideBudget/PairBudget
// overrides from meter's own remaining Budgets.MaxWork, so a shortestPath
// component whose actual configured work budget can comfortably afford more
// preparatory search than traverse's own package-level PairBudget/SideBudget
// constants is not spuriously declined by AllShortestPaths' strategy
// dispatch purely because of those constants -- see traverse.Query's own
// doc for why PairBudget/SideBudget (how much *preparatory* pair/BFS work a
// strategy is willing to attempt before any row exists) is a different axis
// from Limit/MemoryLimit (shortestPathBudget's own concern, bounding
// *output*).
//
// This exists because of a genuine root-cause finding, not a guess: a
// shortestPath query with a fully unconstrained source symbol and a
// terminal kind matching more than traverse.SideBudget (16) nodes --
// exactly "shortestPath((s)-[:...*1..]->(t:Tag_Tier_Zero)) WHERE s<>t"
// against this project's own 300-node/18-Tag_Tier_Zero corpus fixture --
// declines with ErrBudget while meter.work sits at a few hundred against a
// 1<<28 MaxWork budget (confirmed by direct instrumentation, not inferred):
// the actual failure is traverse.AllShortestPaths itself returning
// ErrTooLarge, because neither strategy A (root count x terminal count
// exceeds PairBudget=4096, since the root side is effectively the whole
// snapshot) nor strategy B (18 > SideBudget=16 on the terminal side, and
// the root side is far larger than SideBudget too) accepts the shape --
// not a unit-mixing bug in shortestPathBudget's own rowCap
// arithmetic (that was already fixed in an earlier review pass; see its
// own doc). Wiring this component's traverse.Query exactly the way
// servePathQuery (engine.go) does -- leaving PairBudget/SideBudget at their
// package defaults -- reproduces the identical decline: servePathQuery
// would refuse this exact request too, since PairBudget/SideBudget are
// unconditional package constants no caller has ever been able to
// override. bench/adgen/README.md documents SideBudget=16 as a deliberate
// design constant for that pipeline's own 5M-node benchmark (capping
// synthetic Domain Admins groups at 16 to stay under it) -- raising the
// package constant itself would be a shared, cross-pipeline change this
// task has no scale-validated basis for, and would invalidate that
// documented calibration for servePathQuery/pathbench, which never sets
// these new Query fields and so keeps today's exact dispatch behavior
// unconditionally.
//
// Both overrides are floored at traverse's own current package constant
// (0 is returned -- "use the package default" -- whenever the computed
// affordance is not strictly greater than it), so this can only ever
// widen a strategy dispatch that used to decline, never narrow one that
// used to succeed. meter.budget.MaxWork <= 0 ("no work budget configured",
// a test-only convenience -- production callers always configure MaxWork,
// see serve_cypher.go's maxCypherWork) returns (0, 0): shortestPathBudget's
// own Limit/MemoryLimit are left unbounded in that case too, so this
// leaves traverse's package defaults as the only guard, matching today's
// behavior exactly for that scenario rather than guessing at an
// unconditionally large override with nothing to size it against.
//
// The per-run cost estimate is snap.NodeCount()+snap.EdgeCount() work
// units: a single strategySmallSide BFS run visits at most every node and
// every directed edge once (bfsFrom's own worst case), the same
// adjacency-slot-inspection unit meter.spend already charges elsewhere in
// this package, so remaining MaxWork divided by that estimate is a
// unit-correct bound on how many such runs the query's own budget can
// afford.
//
// strategyPairs' own per-pair cost is reused for PairBudget from the exact
// same estimate, but that reuse is looser than a literal reading of "one
// full-graph BFS" would suggest -- pairShortest (bfs.go) runs a bounded
// bidirectional meet-in-the-middle search (phase 1, worst case comparable
// to one full BFS) and then, once the distance is known, TWO more full,
// depth-capped bfsFrom calls to populate both distance buffers for
// enumeration (phase 2) -- roughly 2-4x one full-graph BFS worst case
// overall, not the <=1x this comment previously (incorrectly) claimed. The
// arithmetic below is unchanged by this correction: PairBudget is still
// only ever widened past traverse's own package default, never narrowed,
// so nothing here regresses today's behavior -- but "safely bounds" is too
// strong a claim for what is actually a same-order-of-magnitude estimate,
// not a tight one; a query whose real per-pair cost sits at the upper end
// of that 2-4x range can still spend a small multiple of the override this
// function computes before meter.spend's own accounting (the actual
// enforcement mechanism) catches up.
func strategyBudgetOverrides(meter *workMeter, snap *snapshot.View) (sideBudget, pairBudget int) {
	if meter.budget.MaxWork <= 0 {
		return 0, 0
	}
	remaining := meter.budget.MaxWork - meter.work
	if remaining <= 0 {
		return 0, 0
	}

	perRun := int64(snap.NodeCount() + snap.EdgeCount())
	if perRun <= 0 {
		return 0, 0
	}
	affordable := remaining / perRun

	if affordable > int64(traverse.SideBudget) {
		sideBudget = int(affordable)
	}
	if affordable > int64(traverse.PairBudget) {
		pairBudget = int(affordable)
	}
	return sideBudget, pairBudget
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

// resolveEndpoint builds a traverse.Endpoint for one side (sym, constrained
// by nc) of a shortestPath()/allShortestPaths() pattern, materializing a
// concrete node-id set only when nc actually narrows the candidate set --
// see this file's package doc comment for why a wide side left unmaterialized
// is the whole point of this function existing instead of always calling
// resolveEndpointSet:
//
//   - nc == nil, or nc carries no kind/id/objectid/predicate constraint at
//     all (the symbol is truly unconstrained) -> traverse.Endpoint{}, the
//     same "matches every node" sentinel traverse's own dispatch already
//     understands (traverse.Endpoint.Unconstrained doc).
//   - nc has one or more Kinds and nothing else (no ids/objectid/
//     predicates) -> traverse.Endpoint{Bits: ...}: a single kind reuses the
//     snapshot's own live per-kind bitmap directly (kindsEndpointBitmap), a
//     multi-kind AND intersects them into a freshly allocated one. Neither
//     case visits candidates one at a time the way scanAnchor does, so this
//     is the fix for the wide-kinds-only-side cost this file's package doc
//     describes.
//   - anything that actually narrows the set (ids and/or an objectid anchor
//     and/or pushed single-symbol Predicates) -> exactly today's
//     materialized-IDs path (resolveEndpointSet), unchanged: a predicate
//     needs a real Row to evaluate EvalPredicate against, which traverse has
//     no hook for, so a predicate-bearing side must keep paying scanAnchor's
//     per-candidate cost regardless of how narrow the result ends up being.
//
// The empty-vs-unconstrained invariant resolveEndpointSet's own doc comment
// describes holds here too: a constrained side (kinds-only or narrowing)
// that matches zero nodes returns a non-nil-but-empty Bits/IDs value, never
// Endpoint{} -- an allocated zero-population Bitset and an empty non-nil
// []NodeID slice are both, correctly, "matches nothing", never "matches
// everything".
func resolveEndpoint(env *Env, meter *workMeter, sym string, nc *NodeConstraint) (traverse.Endpoint, error) {
	if nc == nil {
		return traverse.Endpoint{}, nil
	}
	if !endpointNarrows(nc) {
		if len(nc.Kinds) == 0 {
			// nc is non-nil but carries no constraint whatsoever: it means
			// the same thing nc == nil does above, so it gets the same
			// treatment. Plan genuinely produces this shape -- a bare
			// shortestPath endpoint with no label at all, e.g.
			// `shortestPath((s)-[:E*1..]->(t:Target)) WHERE s<>t`, gives s
			// exactly this &NodeConstraint{} (every field nil/empty), not a
			// nil *NodeConstraint -- so this is a real, reachable branch,
			// not dead code.
			//
			// Two behavior consequences follow from treating it as
			// unconstrained instead of materializing it (which is what this
			// function is for -- see its own doc):
			//
			//  1. Before this function existed, resolveEndpointSet's
			//     scanAnchorVisit fell through to its own default case for
			//     this exact shape (no ids/objectid/kinds) -- a full
			//     env.Snap.NodeCount() scan, admitting (and meter.spend-ing
			//     2 units for) every node in the snapshot. Returning
			//     Endpoint{} here instead means that pre-BFS charge drops to
			//     zero, so shortestPathBudget's rowCap and
			//     strategyBudgetOverrides' affordable are both computed
			//     against a larger remaining work budget than they would
			//     have been -- a real, intended effect of this fix (the
			//     same wide-side-must-not-be-materialized goal this file's
			//     package doc describes), not an oversight.
			//
			//  2. traverse.AllShortestPaths' strategyPairs dispatch case
			//     requires BOTH sides to report !Unconstrained() (its own
			//     switch, traverse.go). A bare endpoint that used to
			//     materialize as "every node in the snapshot, as an
			//     explicit IDs slice" satisfied that (a real, if maximal,
			//     constrained set); as Endpoint{} it no longer does, so
			//     strategyPairs is never reachable for a bare-endpoint
			//     query. On a small enough graph (root count * terminal
			//     count within PairBudget) that used to mean served by
			//     strategyPairs; now, with default budgets, it declines
			//     ErrTooLarge, mapped by this package to ErrBudget. That
			//     decline is safe -- the caller falls back to querying
			//     PostgreSQL directly -- but it is a genuine, user-visible
			//     change in which small-graph bare-endpoint queries the
			//     in-memory path engine itself can still serve, not
			//     something this comment gets to assert away.
			return traverse.Endpoint{}, nil
		}
		return traverse.Endpoint{Bits: kindsEndpointBitmap(env, nc.Kinds)}, nil
	}

	ids, err := resolveEndpointSet(env, meter, sym, nc)
	if err != nil {
		return traverse.Endpoint{}, err
	}
	return traverse.Endpoint{IDs: ids}, nil
}

// kindsEndpointBitmap returns the traverse.Endpoint{Bits} value for a
// kinds-only NodeConstraint: kinds are AND-ed together (nodeSatisfiesConstraint
// requires every one of them, and so does this), so a single kind reuses the
// snapshot's own live per-kind bitmap directly (no copy -- neither this
// package nor traverse ever mutates an Endpoint's Bits), while more than one
// intersects them into a freshly allocated Bitset, iterating whichever input
// is smallest so the cost is proportional to the smallest candidate kind
// rather than the snapshot's total node count. Mirrors the servePathQuery
// path's own resolveKindsEndpoint/intersectBitmaps kind-intersection
// approach, reimplemented here (rather than imported) because that code
// lives in a package that already imports this one.
func kindsEndpointBitmap(env *Env, kinds []snapshot.KindID) *snapshot.Bitset {
	if len(kinds) == 1 {
		return env.Snap.NodesOfKind(kinds[0])
	}

	bitmaps := make([]*snapshot.Bitset, len(kinds))
	for i, k := range kinds {
		bitmaps[i] = env.Snap.NodesOfKind(k)
	}
	smallest := bitmaps[0]
	for _, bm := range bitmaps[1:] {
		if bm.Count() < smallest.Count() {
			smallest = bm
		}
	}

	result := snapshot.NewBitset(env.Snap.NodeCount())
	smallest.Iterate(func(id snapshot.NodeID) bool {
		for _, bm := range bitmaps {
			if bm != smallest && !bm.Has(id) {
				return true
			}
		}
		result.Set(id)
		return true
	})
	return result
}

// endpointsIntersect reports whether roots and terminals (snap gives
// Endpoint.Count/Iterate the "matches every node" default's own overlay-
// aware node count and Alive check) share at least one dense id -- the
// traverse.Endpoint-aware replacement for the old materialized-slice
// idsIntersect, preserving its exact semantics (a plain set-membership
// overlap test) rather than traverse.SelfEndpointConflict's additional
// out-degree>0 requirement: see ErrSelfEndpoint's own doc comment for why
// this package's self-endpoint rule must stay exactly what it always was.
// Iterates whichever side Count(total) reports as smaller and probes it
// against the other via Has, so a Bits-vs-Bits or Bits-vs-IDs pair costs at
// most the smaller side's own population, and neither side is ever
// materialized purely to run this check.
func endpointsIntersect(snap *snapshot.View, roots, terminals traverse.Endpoint) bool {
	total := snap.NodeCount()
	small, big := roots, terminals
	if terminals.Count(total) < roots.Count(total) {
		small, big = terminals, roots
	}

	found := false
	small.Iterate(snap, func(id snapshot.NodeID) bool {
		if big.Has(id) {
			found = true
			return false
		}
		return true
	})
	return found
}

// noResidualWhere reports whether part.Where's complete conjunct set (Part's
// own doc: Where is always the Part's complete and sufficient WHERE
// expression, never merely "whatever pushdown left over") is fully accounted
// for by step's own endpoint resolution, so Execute's post-executor
// Part.Where pass over this component's produced rows can never actually
// reject one of them -- the precondition the LIMIT pushdown below needs: a
// row traverse.AllShortestPaths never got a chance to enumerate (because
// enumeration stopped once the user's own LIMIT was reached) must never turn
// out to be one Part.Where would have kept while a row that WAS enumerated
// gets filtered out, which would silently under-serve the query relative to
// full enumeration followed by filtering and LIMIT.
//
// plan.go's planPart flattens every WHERE conjunct (including inline-map-
// desugared equalities) into one slice, walks it once to populate both
// Part.Where (via rebuildConjunction, ANDing the same slice back together)
// and every single-symbol conjunct's NodeConstraint.Predicates entry
// (pushdown) -- the identical conjunct value, not a copy, ends up in both
// places. So re-flattening part.Where (flattenTopLevelConjuncts, the same
// function planPart itself used to build that slice in the first place) and
// checking each resulting conjunct for reference equality against
// step.FromSym's/step.ToSym's own Predicates exactly recovers "did pushdown
// already consume this piece", with no need to re-walk or duplicate
// pushdown's own symbol-touch tracking. The one other shape pushdown leaves
// out of every NodeConstraint.Predicates entirely -- the s<>t/id(s)<>id(t)
// endpoint inequality finalizeShortestPaths reads directly out of the
// conjunct list to set step.HasExplicitEndpointInequality -- is recognized
// here the same structural way that function does (variableInequality/
// idInequality), since that conjunct becomes traverse.Query.ExcludeSelf
// instead of a Predicates entry.
//
// Any other conjunct -- one touching some other symbol entirely (a wholly
// separate MATCH pattern's own predicate sharing this Part), or one touching
// both of step's endpoints together (e.g. `s.group = t.group`, which cannot
// be a single-symbol Predicates entry for either) -- is never accounted for
// by either check, correctly making this false: the pushdown must stay off
// whenever any such conjunct exists, regardless of how small or large its
// eventual filtering effect turns out to be.
func noResidualWhere(part *Part, step *Step) bool {
	fromNC, toNC := part.Nodes[step.FromSym], part.Nodes[step.ToSym]
	for _, c := range flattenTopLevelConjuncts(part.Where) {
		if predicateBelongsTo(fromNC, c) || predicateBelongsTo(toNC, c) {
			continue
		}
		if variableInequality(c, step.FromSym, step.ToSym) || idInequality(c, step.FromSym, step.ToSym) {
			continue
		}
		return false
	}
	return true
}

// predicateBelongsTo reports whether c is, by reference (see
// noResidualWhere's own doc comment for why identity rather than structural
// equality is exactly right here), one of nc's own pushed single-symbol
// Predicates.
func predicateBelongsTo(nc *NodeConstraint, c cypher.Expression) bool {
	if nc == nil {
		return false
	}
	for _, p := range nc.Predicates {
		if p == c {
			return true
		}
	}
	return false
}

// kindMaskFor builds the *snapshot.KindMask traverse.Query.Kinds expects from
// a Step's EdgeKinds list, or nil (traverse's own "every kind allowed"
// sentinel, matching edgeKindOK's identical "empty = any" contract) when
// EdgeKinds is empty.
//
// The mask's own ceiling is max(env.Snap.MaxKindID(), every id in kinds),
// not env.Snap.MaxKindID() alone. Snap.MaxKindID() already raises the base
// snapshot's own MaxKindID (whichever kinds the BASE graph's own nodes and
// edges actually carry, snapshot.Builder.Build's doc) to also cover any kind
// a delta segment introduced later (snapshot.View.MaxKindID's own doc), so
// a step whose relationship pattern names a kind first introduced by a
// delta segment (kinds is resolved from the pattern's own kind names via
// snap.Kinds().ID, which is itself overlay-aware) is already covered without
// this function doing anything extra. The per-kinds loop below instead
// covers a second, independent gap that predates overlays entirely: kind
// NAMES are resolved against the database's whole global `kind` table
// (snapshot.KindTable's own doc), which can register more kinds than this
// one graph's nodes/edges ever used -- so a resolved KindID can still exceed
// even the merged Snap.MaxKindID() ceiling. Either gap left unfixed has the
// identical failure mode: snapshot.KindMask.Set/Has both silently no-op for
// any id above the mask's own ceiling, so every edge of that kind would be
// filtered out of the traversal as if the mask had never been given the
// kind in the first place -- not merely a missed optimization, but a query
// one path down this specific chain (a shortestPath()/allShortestPaths()
// step whose only admissible edges were all of that kind) silently
// under-answers, rather than over-answering, WHICH edges Query.Kinds admits.
func kindMaskFor(env *Env, kinds []snapshot.KindID) *snapshot.KindMask {
	if len(kinds) == 0 {
		return nil
	}
	maxKindID := env.Snap.MaxKindID()
	for _, k := range kinds {
		if k > maxKindID {
			maxKindID = k
		}
	}
	mask := snapshot.NewKindMask(maxKindID)
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
	overlay := env.Snap.Overlay()
	for i, k := range p.Kinds {
		u, w := p.Nodes[i], p.Nodes[i+1]
		found := false
		if !overlay {
			targets, kinds, _ := env.Snap.Out(u)
			lo := env.Snap.Base().OutOffsets[u]
			for j, t := range targets {
				if t == w && kinds[j] == k {
					edges[i] = EdgeRef{Fwd: lo + uint64(j)}
					found = true
					break
				}
			}
		} else {
			env.Snap.OutEdges(u, func(target snapshot.NodeID, kind snapshot.KindID, edgeID uint64) bool {
				if target == w && kind == k {
					edges[i] = EdgeRef{EdgeID: edgeID}
					found = true
					return false
				}
				return true
			})
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

// reversePathVal reverses pv's Nodes and Edges slices in place, turning a
// PathVal built in graph-TRAVERSAL order into one describing the identical
// path in the opposite node sequence -- needed for a step whose ORIGINAL
// Cypher pattern used a backward arrow (`<-`):
// buildStep normalizes such a step by swapping FromSym/ToSym so the
// recorded Step.Direction always matches the graph's own forward-CSR
// traversal direction (Step.Reversed's own doc), which means every PathVal
// this package assembles from a Reversed step's own expansion
// (expandVarLengthTrailsForSeed, expandShortestPathComponent) comes out
// with the far (traversal-source) endpoint first -- correct for a forward
// arrow, backward for one of these, relative to Cypher's (and pg's) own
// "a path's node sequence follows the pattern as WRITTEN" semantics. A
// live-pg differential probe confirmed this directly: `MATCH p =
// (t:Group)<-[:MemberOf*1..]-(a) RETURN p` renders p's first node as t (the
// pattern's own first-written variable) on both a real pg database and any
// correct engine, never a (the traversal source) -- this package's own
// PathVal construction got that backward until this fix.
//
// Reordering the slice alone is sufficient to re-describe the same path
// back-to-front: an EdgeRef's own start/end/kind are derived structurally
// from the snapshot's CSR data by materializeEdge, never from the edge's
// position within a path, so no edge itself needs touching, only which
// slot of Nodes/Edges it occupies.
func reversePathVal(pv *PathVal) {
	if pv == nil {
		return
	}
	for i, j := 0, len(pv.Nodes)-1; i < j; i, j = i+1, j-1 {
		pv.Nodes[i], pv.Nodes[j] = pv.Nodes[j], pv.Nodes[i]
	}
	for i, j := 0, len(pv.Edges)-1; i < j; i, j = i+1, j-1 {
		pv.Edges[i], pv.Edges[j] = pv.Edges[j], pv.Edges[i]
	}
}
