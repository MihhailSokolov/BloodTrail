// SPDX-License-Identifier: Apache-2.0

// pipeline.go implements the WITH
// pipeline (implicit grouping, COUNT/COLLECT aggregation, the COLLECT
// anti-join id-set membership rewrite), RETURN DISTINCT, ORDER BY, and
// SKIP/LIMIT. It replaces exec.go's Task 7 seam (the early gate on
// WithClause/Order/Skip/Limit/Distinct) with the real implementation and
// is the last piece Execute needs to serve a full Query.
//
// --- Multi-part flow -------------------------------------------------------
//
// Query.Parts has length 1 (no WITH) or 2 (exactly one WITH boundary --
// Plan never produces more). For the 2-part case: Part[0]'s pattern is
// matched and WHERE-filtered exactly as a single-Part query would be (see
// exec.go's matchPart/filterRows), then Part[0].With transforms that row set
// into the *carried* row set (runWithStage, below) -- this is pg's "the WITH
// clause materializes as a CTE" step. Part[1]'s own pattern (if it has one at
// all -- a trailing `WITH ... RETURN ...` with no further MATCH is legal,
// see runCarriedPart) is then matched *once per carried row*, mirroring pg's
// "cross-join the CTE against the next MATCH" plan shape: each carried row's
// bindings (node/edge identities, WITH-bound scalars, and any COLLECT
// membership id-sets) are merged into every row Part[1]'s own matching
// produces for that carried row, so Part[1]'s WHERE/RETURN can reference
// both its own freshly-matched symbols and whatever Part[0]'s WITH carried
// forward.
//
// One shape is declined rather than guessed at: a WITH GroupKeys symbol
// (necessarily a *node* identity -- see groupKeysOverlapNodes' doc comment)
// reused inside Part[1]'s own node pattern. Re-anchoring a carried node
// identity against a second, independently re-scanned NodeConstraint has no
// support in this executor's row-merge model (mergeRowInto has no "these two
// bindings must be the same node" concept); no required corpus query needs
// this shape, so Execute declines it via errUnsupportedStep, exactly like
// every other "unverified composition" this package declines elsewhere
// (expand.go's mixed-Step-component decline is the direct precedent).
//
// --- Grouping and aggregation -----------------------------------------------
//
// runWithStage dispatches on len(WithClause.Aggregates): zero means a plain
// per-row pass-through (runWithPassThrough, optionally deduped by DISTINCT);
// non-zero always groups by GroupKeys, regardless of DISTINCT (WithClause's
// own doc comment) -- runWithAggregate. Group keys and DISTINCT-dedup keys
// share one encoding (groupKeyBytes/appendSymbolKey/appendScalarKey): a
// type-tagged, length-prefixed byte encoding of a Row-bound symbol's value
// (node/edge identity by database id, scalar by a ScalarEq-consistent
// encoding of this package's post-JSON value model), designed so that no two
// structurally-different values can ever collide (each component is
// self-delimiting: a fixed tag byte followed by either a fixed-size or
// length-prefixed payload, so concatenating components never introduces
// ambiguity) and so that every ScalarEq-equal pair of values (in particular,
// 1.0 and 1, and byte-identical strings) always produces the identical
// encoding.
//
// WITH DISTINCT combined with aggregation (the corpus's own `WITH DISTINCT
// u, COUNT(c) AS adminCount` shape) is intentionally a no-op beyond ordinary
// grouping: grouping by GroupKeys already collapses the row set to at most
// one row per distinct GroupKeys tuple, and every other field of that output
// row (every Aggregate) is a pure function of the group, so two grouped
// output rows can never be duplicates of each other -- DISTINCT has nothing
// left to remove. runWithAggregate therefore never consults wc.Distinct at
// all.
//
// --- COLLECT membership: the structural rewrite -----------------------------
//
// This closes the "COLLECT-membership execution gap": a
// CollectMembershipAgg's alias is bound to an id-set (idSet, a
// map[uint64]struct{} of database node ids -- NULL ids never enter, though
// this package's own matched rows never produce one, since it implements no
// OPTIONAL MATCH), stored via ordinary Row.SetScalar so no Row/eval.go change
// is needed, but is NEVER handed to the frozen eval.go EvalPredicate/EvalValue
// machinery as a generic value: value.go's In()/eval.go's evalIn operate over
// []any lists via jsonbEqual/text-extraction, and a bound node variable's own
// EvalValue result is its full property map (evalVariableValue), not its id
// -- neither has any way to express "is this node's id a member of that
// id-set". evalWhereWithMembership is this executor's own tree-walking
// predicate evaluator, structurally identical to eval.go's EvalPredicate,
// except that at every Comparison node (and every Negation of one) it first
// checks for the shape `<bound node var> IN <collect alias>` (Plan's
// checkInOperands already guarantees this is the *only* way a collect alias
// can appear in a served query) and answers it directly by database-id
// lookup, before ever falling through to eval.go's generic machinery for
// everything else in the tree. It has to re-implement the boolean-structural
// recursion (Parenthetical/Negation/Conjunction/Disjunction/
// ExclusiveDisjunction) itself, rather than delegating to EvalPredicate and
// somehow "hooking" its recursive calls, because EvalPredicate's own
// recursion always calls itself, with no seam for a caller to intercept a
// nested Comparison -- but every *leaf* case (an ordinary Comparison,
// KindMatcher, or bare-expression truthiness that does not itself reference a
// collect alias) safely delegates to eval.go's own already-frozen unexported
// helpers (evalComparison, evalKindMatcher, evalStringPredicate,
// evalRegexComparison, evalValueTruthiness), since none of those recurse back
// into predicate evaluation themselves -- they only ever call EvalValue for
// their operands, which has no predicate-tree recursion to intercept in the
// first place.

package interpret

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"

	"github.com/specterops/dawgs/cypher/models/cypher"
)

// idSet is the value shape a CollectMembershipAgg's alias is bound to: every
// distinct database node id collected for one WITH group. See this file's
// package doc comment ("COLLECT membership: the structural rewrite") for why
// this is never handed to eval.go's generic EvalValue/EvalPredicate.
type idSet map[uint64]struct{}

// membershipTable maps a bound-in-this-row CollectMembershipAgg alias name to
// its idSet, threaded through evalWhereWithMembership instead of being read
// back out of Row per call.
type membershipTable map[string]idSet

// --- Execute: multi-part flow, DISTINCT, ORDER BY, SKIP/LIMIT ---------------

// runQuery is exec.go's Execute, minus the Task 7 seam: it fully materializes
// q against env, applying every Part's own pattern match + WHERE, the WITH
// pipeline between Part[0] and Part[1] (if any), RETURN DISTINCT, ORDER BY,
// and SKIP/LIMIT, in that order -- matching pg's own logical evaluation
// order (WITH's CTE materializes and is cross-joined against the next MATCH,
// then the final projection is deduped, sorted, and paged).
func runQuery(env *Env, q *Query, meter *workMeter) (*ResultSet, error) {
	if len(q.Parts) < 1 || len(q.Parts) > 2 {
		// Impossible given Plan's own planStages (at most one WITH boundary),
		// but declining defensively costs nothing -- see exec.go's Execute
		// doc on "decline rather than guess".
		return nil, errUnsupportedStep
	}

	// target is -1 for a query LIMIT early termination must never touch (see
	// limitTarget's own doc): every branch below that consults it treats -1
	// as "run exactly the pre-LIMIT-early-termination code", so an
	// ineligible query's observable behavior -- rows, errors, and
	// meter.work alike -- is unchanged by this feature.
	target := limitTarget(q)

	part0 := &q.Parts[0]
	var rows []*Row
	var err error
	if len(q.Parts) == 1 && target >= 0 {
		rows, err = matchPartLimited(env, meter, part0, target)
	} else {
		rows, err = matchPartPlain(env, meter, part0)
	}
	if err != nil {
		return nil, err
	}

	if part0.With != nil {
		if len(q.Parts) != 2 {
			// Part's own doc: With is set on every Part but the last --
			// structurally unreachable, declined defensively.
			return nil, errUnsupportedStep
		}
		rows, err = runWithStage(env, meter, part0.With, rows)
		if err != nil {
			return nil, err
		}

		part1 := &q.Parts[1]
		if groupKeysOverlapNodes(part0.With.GroupKeys, part1.Nodes) {
			return nil, errUnsupportedStep
		}
		collectAliases := collectAliasNames(part0.With.Aggregates)

		var comp1 component
		var anchor1 string
		var limited bool
		if target >= 0 {
			comp1, anchor1, limited = limitEligibleComponent(env, meter, part1)
		}

		merged := make([]*Row, 0, len(rows))
		if limited {
			for _, seed := range rows {
				remaining := target - int64(len(merged))
				if remaining <= 0 {
					// Enough seeds already secured target rows -- every
					// remaining seed's own Part[1] match is skipped
					// entirely, unlike the unlimited path below, which
					// always runs every seed.
					break
				}
				got, err := runComponentLimited(env, meter, part1, comp1, anchor1, seed, collectAliases, remaining)
				if err != nil {
					return nil, err
				}
				merged = append(merged, got...)
			}
			rows = merged
		} else {
			for _, seed := range rows {
				got, err := runCarriedPart(env, part1, meter, seed)
				if err != nil {
					return nil, err
				}
				merged = append(merged, got...)
			}
			rows, err = filterRows(env, merged, part1.Where, collectAliases)
			if err != nil {
				return nil, err
			}
		}
	} else if len(q.Parts) == 2 {
		return nil, errUnsupportedStep
	}

	// Admit every surviving row into the query's row budget -- this is the
	// "final result" MaxRows' own doc comment describes: a 2-part query's
	// Part[0]-filtered-but-pre-WITH rows are an intermediate quantity already
	// bounded by MaxWork's uniform per-row-produced-anywhere accounting
	// (matchPart/expandStep/etc. already spend it), not a second, redundant
	// MaxRows charge.
	for range rows {
		if err := meter.addFinalRow(); err != nil {
			return nil, err
		}
	}

	outRows := make([][]OutVal, len(rows))
	for i, r := range rows {
		outRow, err := projectRow(env, q.Returning, r)
		if err != nil {
			return nil, err
		}
		outRows[i] = outRow
	}

	if q.Returning.Distinct {
		rows, outRows = dedupProjected(env, rows, outRows)
	}

	if len(q.Order) > 0 {
		if err := sortRows(rows, outRows, q.Order, aliasIndexFor(q.Returning)); err != nil {
			return nil, err
		}
	}

	outRows = applySkipLimit(outRows, q.Skip, q.Limit)

	if err := meter.check(); err != nil {
		return nil, err
	}

	return &ResultSet{Keys: projectionKeys(q.Returning), Rows: outRows}, nil
}

// filterRows evaluates expr (a Part's Where, or nil for no filter at all)
// against every row in rows, keeping only the rows it evaluates to TriTrue
// for. When collectAliases is non-empty, evaluation routes through
// evalWhereWithMembership (rebuilding that row's membership table from
// whatever id-sets its own Row.Scalar bindings carry for those alias names)
// instead of eval.go's generic EvalPredicate -- see this file's package doc
// comment.
//
// filterRows spends no work of its own: every row it is handed already paid
// its "produced" charge wherever it was actually produced (matchPart's own
// scanAnchor/expandStep/cartesianJoin, or runWithStage's grouping), and a row
// surviving a boolean predicate test is not a new "row produced" event --
// see runQuery's own doc comment on addFinalRow for the single canonical
// point where a served row is charged.
func filterRows(env *Env, rows []*Row, expr cypher.Expression, collectAliases []string) ([]*Row, error) {
	if expr == nil {
		return rows, nil
	}

	var membership membershipTable
	if len(collectAliases) > 0 {
		membership = make(membershipTable, len(collectAliases))
	}

	out := make([]*Row, 0, len(rows))
	for _, r := range rows {
		var (
			t   Tri
			err error
		)
		if membership != nil {
			for _, alias := range collectAliases {
				if v, ok := r.Scalar(alias); ok {
					if set, isSet := v.(idSet); isSet {
						membership[alias] = set
					}
				}
			}
			t, err = evalWhereWithMembership(env, r, expr, membership)
		} else {
			t, err = EvalPredicate(env, r, expr)
		}
		if err != nil {
			return nil, err
		}
		if t != TriTrue {
			continue
		}
		out = append(out, r)
	}
	return out, nil
}

// runCarriedPart runs part's own pattern matching (if it has one at all) for
// one carried WITH-stage row, merging seed's bindings into every row
// produced -- pg's "cross-join the CTE against the next MATCH" step. A Part
// with an empty pattern (part.Nodes has no entries at all -- a trailing
// `WITH ... RETURN ...` with no further MATCH, the corpus aggregation
// query's own shape) is not run through matchPart: matchPart's own
// groupComponents has nothing to iterate for zero Nodes and would otherwise
// answer "zero components, zero rows", which is wrong for a no-op pattern --
// an empty pattern passes every carried row through unchanged, exactly like
// a cross join against a single-row, zero-column table.
//
// The merge step itself spends no separate work unit: part's own matchPart
// call already charged for producing r (its own scanAnchor/expandStep/
// cartesianJoin accounting), and cloneRow+mergeRowInto's O(bindings) copy is
// not itself a "row produced" event under Budgets' documented model -- the
// merged row is charged exactly once, if and when it survives to become a
// final row (see runQuery's addFinalRow call). Charging it again here, for
// every merged row regardless of whether WHERE or a later stage keeps it,
// would double-count against that single canonical charge.
func runCarriedPart(env *Env, part *Part, meter *workMeter, seed *Row) ([]*Row, error) {
	if len(part.Nodes) == 0 {
		return []*Row{cloneRow(seed)}, nil
	}
	rows, err := matchPart(env, part, meter)
	if err != nil {
		return nil, err
	}
	out := make([]*Row, 0, len(rows))
	for _, r := range rows {
		nr := cloneRow(seed)
		mergeRowInto(nr, r)
		out = append(out, nr)
	}
	return out, nil
}

// groupKeysOverlapNodes reports whether any of a WITH clause's plain
// variable carry-overs (GroupKeys) names a symbol the *following* Part's own
// pattern also declares as a node variable -- see this file's package doc
// comment ("Multi-part flow") for why this executor declines that shape
// rather than guess at re-anchoring semantics it does not implement. Plan's
// own symbol-table rules (declareSymbol's same-kind-required reuse) already
// guarantee any name shared between a WITH's GroupKeys and the next Part's
// own node patterns was necessarily carried as a *node* identity: a carried
// scalar/edge/collect-alias reused as a node-pattern variable would already
// have been rejected at plan time by addNodePattern's declareSymbol call
// (existing kind != symNode). So a plain name-overlap check, needing no
// per-row inspection, is sufficient to catch every instance of this shape.
func groupKeysOverlapNodes(groupKeys []string, nextNodes map[string]*NodeConstraint) bool {
	for _, sym := range groupKeys {
		if _, ok := nextNodes[sym]; ok {
			return true
		}
	}
	return false
}

// collectAliasNames returns the alias of every CollectMembershipAgg in aggs,
// in declaration order.
func collectAliasNames(aggs []WithAggregate) []string {
	var out []string
	for _, a := range aggs {
		if a.Collect != nil {
			out = append(out, a.Alias)
		}
	}
	return out
}

// --- LIMIT early termination -------------------------------------------------

// limitTarget returns how many post-filter final-part rows the query needs
// before production may stop, or -1 when LIMIT early termination must not
// apply at all -- every caller treats -1 as "run the unlimited path,
// unchanged".
//
// ORDER BY needs every row: under a LIMIT, sorting first is what decides
// WHICH rows survive, so stopping the scan early (before every candidate has
// even been produced) could keep the wrong ones. RETURN DISTINCT counts
// deduped PROJECTED rows (dedupProjected, above) -- a quantity this
// pre-projection driver has no way to track, since two distinct matched rows
// can project to the identical output tuple. SKIP rows are produced and
// then discarded by applySkipLimit, exactly like today's unlimited path
// already does -- so they still count toward how many post-filter rows must
// exist before stopping.
func limitTarget(q *Query) int64 {
	if q.Limit < 0 || len(q.Order) > 0 || q.Returning.Distinct {
		return -1
	}
	return q.Skip + q.Limit
}

// limitChunk is the number of anchor rows scanAnchorVisit collects per batch
// on the LIMIT early-termination path (runComponentLimited) before that
// batch is run through the component's own expansion and Where filtering:
// large enough to amortize the fixed cost of running that pipeline once per
// batch rather than once per row, small enough that overshooting the target
// by a whole batch's worth of extra rows (see runComponentLimited's own doc)
// stays cheap relative to the millions of rows this feature exists to avoid
// ever materializing.
const limitChunk = 1024

// matchPartPlain runs part's pattern match and Where filter exactly as the
// pre-LIMIT-early-termination code always did: matchPart, then filterRows
// with no collect-membership aliases (Part[0] never carries any -- those are
// only ever bound by a PRECEDING Part's WITH, and Part[0] has none). Both
// runQuery's own non-eligible cases and matchPartLimited's own fallback (a
// single-part shape limitEligibleComponent declines) route through this one
// function, so every ineligible query -- and every eligible-looking one that
// still isn't actually servable by the chunked driver -- gets exactly the
// same treatment it always got.
func matchPartPlain(env *Env, meter *workMeter, part *Part) ([]*Row, error) {
	rows, err := matchPart(env, part, meter)
	if err != nil {
		return nil, err
	}
	return filterRows(env, rows, part.Where, nil)
}

// matchPartLimited is runQuery's single-part (no WITH boundary) entry point
// into LIMIT early termination: when part's pattern qualifies
// (limitEligibleComponent), it runs the chunked driver (runComponentLimited)
// with no seed and no collect-membership aliases -- matching matchPartPlain's
// own Part[0] call above; otherwise it falls back to matchPartPlain
// unchanged. Callers only reach this at all when limitTarget(q) >= 0 and
// len(q.Parts) == 1 -- see runQuery.
func matchPartLimited(env *Env, meter *workMeter, part *Part, target int64) ([]*Row, error) {
	if comp, anchor, ok := limitEligibleComponent(env, meter, part); ok {
		return runComponentLimited(env, meter, part, comp, anchor, nil, nil, target)
	}
	return matchPartPlain(env, meter, part)
}

// limitEligibleComponent reports whether part's own pattern-match phase can
// run through the chunked driver (runComponentLimited) instead of
// matchPartPlain/runCarriedPart, and if so, which single component and
// anchor symbol (componentAnchorSym) to scan.
//
// Two checks are structural, straight from this feature's own eligibility
// rule: part's pattern must resolve to exactly one connected component
// (matchPart's own cartesianJoin has no way to stop early once a second,
// disjoint component is cartesian-joined against the first -- an early stop
// on one component's own anchor scan says nothing about how many rows an
// independent second component would eventually contribute), and that
// component must carry no shortestPath/allShortestPaths step
// (expandShortestPathComponent resolves both pattern endpoints as complete
// node sets up front rather than growing rows from one symbol's anchor scan
// -- see runComponentFrom's own doc comment for why it has no "given anchor
// rows" tail for that shape at all; such a component is still served
// correctly, just via the ordinary matchPart/runComponent fallback, which
// does dispatch to expandShortestPathComponent).
//
// The third check is a zero-cost PROBE, not a hand-duplicated copy of every
// precondition runComponentFrom's own dispatch enforces (uniformPathSym's
// PathSym agreement, isStrictLinearChain, expandVarLengthComponentFrom's own
// EdgeSym/FromSym==ToSym guard): every one of those checks is decided purely
// from part/comp's own static shape, never from anchorRows' actual content
// or count, and every loop inside runComponentFrom's own tails
// (runComponentTreeFrom/expandStep/expandChainComponentFrom/
// expandVarLengthComponentFrom) is keyed on anchorRows' length -- so calling
// runComponentFrom with a nil anchorRows slice reaches the identical decline
// (if any), with every one of those loops iterating zero times, spending
// exactly zero meter.spend calls either way. This matters because, unlike
// runComponent (which checks every one of these preconditions BEFORE ever
// calling scanAnchor), a caller of runComponentFrom directly -- this chunked
// driver -- would otherwise only discover such a decline AFTER a real,
// meter-charged scanAnchorVisit batch had already run: the unlimited path's
// own equivalent decline never spends that anchor-scan work at all (see
// expandVarLengthComponentFrom's own doc comment for the specific
// EdgeSym/FromSym==ToSym case this guards against). A probe failure falls
// back to matchPartPlain/runCarriedPart, which reach the identical decline
// via runComponent -- so a query this probe rejects still ends up declined
// (or served) exactly as it always was.
func limitEligibleComponent(env *Env, meter *workMeter, part *Part) (comp component, anchor string, ok bool) {
	comps := groupComponents(part)
	if len(comps) != 1 {
		return component{}, "", false
	}
	comp = comps[0]
	if hasShortestStep(part, comp.stepIdxs) {
		return component{}, "", false
	}
	if _, err := runComponentFrom(env, meter, part, comp, nil); err != nil {
		return component{}, "", false
	}
	return comp, componentAnchorSym(env, part, comp), true
}

// componentAnchorSym reports the symbol runComponentFrom's own dispatch
// (see its doc comment) treats an anchorRows chunk as bound to for comp: the
// component's own leftmost chain symbol (part.Chains[comp.stepIdxs[0]].
// FromSym) for a special-step (var-length/shortestPath) or named-path
// component -- exactly the symbol expandVarLengthComponent/
// expandChainComponent themselves scan on the unlimited path -- or
// chooseAnchor's own cost-ranked pick otherwise (the general BFS component,
// including a single isolated node symbol, where chooseAnchor over a
// one-element syms list trivially returns that element).
//
// This does not itself validate that comp is actually servable this way --
// limitEligibleComponent's own probe call does that. A component
// runComponentFrom would ultimately decline (e.g. isStrictLinearChain
// failing) still gets an anchor symbol name back here; it is simply never
// used, since the caller's probe already rejected the component first.
func componentAnchorSym(env *Env, part *Part, comp component) string {
	stepIdxs := comp.stepIdxs
	pathSym, pathUniform := uniformPathSym(part, stepIdxs)
	if pathUniform && (hasSpecialStep(part, stepIdxs) || pathSym != "") {
		return part.Chains[stepIdxs[0]].FromSym
	}
	return chooseAnchor(env, part.Nodes, comp.syms)
}

// runComponentLimited produces post-Where rows for one eligible component
// (limitEligibleComponent's own decision), stopping the underlying anchor
// scan once at least target such rows have accumulated, or once the anchor
// scan itself runs out first, whichever happens sooner.
//
// Anchor rows are scanned via scanAnchorVisit in batches of limitChunk, each
// batch expanded through runComponentFrom (runComponent's own dispatch,
// replayed over a given anchor-row chunk) and then Where-filtered through
// this file's own filterRows -- the SAME two stages the unlimited path
// applies to every row (matchPart's own runComponent call, then this file's
// filterRows call), just chunked instead of run once over the whole anchor
// set. This is exactly why the target is counted against len(acc) -- rows
// that have ALREADY cleared both stages -- rather than the number of anchor
// rows scanned so far: an anchor row is not a result row until it has
// cleared every filtering stage the unlimited path applies to it too, and
// counting anchor rows (or any other pre-filter quantity) instead would
// under-return whenever most anchor candidates fail Where (see this
// package's own sparse-tail test).
//
// seed is non-nil for the carried (stage-1, after a WITH) case: each
// produced batch is cloned-and-merged with seed (cloneRow + mergeRowInto)
// BEFORE filtering, exactly like runCarriedPart's own per-row merge on the
// unlimited path, so a Where conjunct referencing a carried symbol (e.g.
// `NOT c IN exclude`) sees it; collectAliases is threaded through to
// filterRows unchanged, for the identical reason runQuery passes it to its
// own post-WITH filterRows call. seed is nil for the single-part case
// (matchPartLimited), where filterRows runs with no collectAliases, matching
// matchPartPlain's own Part[0] call.
//
// The returned row count may exceed target: the batch that finally reaches
// it is never truncated mid-flush, and the anchor scan's own trailing
// partial batch (flushed once scanAnchorVisit returns nil -- the scan ran
// out on its own, rather than being stopped via errStopScan) is flushed
// unconditionally too, without re-checking target (there is nothing left to
// scan regardless). Both overshoots are intentional: the caller's own
// applySkipLimit trims the eventual overshoot down to exactly q.Skip+q.Limit
// rows later, precisely as it already trims the unlimited path's own (much
// larger) full row set today.
func runComponentLimited(env *Env, meter *workMeter, part *Part, comp component, anchor string, seed *Row, collectAliases []string, target int64) ([]*Row, error) {
	var acc []*Row
	chunk := make([]*Row, 0, limitChunk)

	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		rows, err := runComponentFrom(env, meter, part, comp, chunk)
		chunk = chunk[:0]
		if err != nil {
			return err
		}
		if seed != nil {
			for i, r := range rows {
				nr := cloneRow(seed)
				mergeRowInto(nr, r)
				rows[i] = nr
			}
		}
		rows, err = filterRows(env, rows, part.Where, collectAliases)
		if err != nil {
			return err
		}
		acc = append(acc, rows...)
		return nil
	}

	scanErr := scanAnchorVisit(env, meter, anchor, part.Nodes[anchor], func(r *Row) error {
		chunk = append(chunk, r)
		if len(chunk) < limitChunk {
			return nil
		}
		if err := flush(); err != nil {
			return err
		}
		if int64(len(acc)) >= target {
			return errStopScan
		}
		return nil
	})
	if scanErr != nil && !errors.Is(scanErr, errStopScan) {
		return nil, scanErr
	}
	if scanErr == nil {
		// The anchor scan ran out on its own rather than being stopped via
		// errStopScan: flush whatever partial batch remains (there is
		// nothing left to scan regardless of whether it reaches target).
		if err := flush(); err != nil {
			return nil, err
		}
	}
	return acc, nil
}

// --- WITH stage: grouping, aggregation, plain pass-through ------------------

// runWithStage applies wc to rows (already matched and WHERE-filtered),
// producing the carried row set for whatever comes next -- see this file's
// package doc comment for the grouping-vs-pass-through dispatch rule.
func runWithStage(env *Env, meter *workMeter, wc *WithClause, rows []*Row) ([]*Row, error) {
	if len(wc.Aggregates) == 0 {
		return runWithPassThrough(env, meter, wc, rows)
	}
	return runWithAggregate(env, meter, wc, rows)
}

// runWithPassThrough implements a WITH clause with no aggregates: every
// input row becomes one output row carrying exactly wc's GroupKeys bindings
// and wc.Constants (everything else Cypher's own WITH scoping says does not
// survive the boundary), in input order; wc.Distinct additionally dedupes by
// that same tuple's encoding, keeping only the first occurrence of each
// distinct key (a stable, deterministic policy -- see groupKeyBytes' own doc
// comment on the encoding this shares with grouping).
func runWithPassThrough(env *Env, meter *workMeter, wc *WithClause, rows []*Row) ([]*Row, error) {
	var seen map[string]struct{}
	if wc.Distinct {
		seen = make(map[string]struct{}, len(rows))
	}

	out := make([]*Row, 0, len(rows))
	for _, r := range rows {
		nr := NewRow()
		for _, sym := range wc.GroupKeys {
			copySymbolBinding(nr, r, sym)
		}
		for _, c := range wc.Constants {
			nr.SetScalar(c.Alias, c.Value)
		}

		if wc.Distinct {
			key := string(groupKeyBytes(env, nr, wc.GroupKeys))
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		}

		if err := meter.spend(1); err != nil {
			return nil, err
		}
		out = append(out, nr)
	}
	return out, nil
}

// withGroup accumulates one aggregation group: rep is the first input row
// assigned to it (source of the group's own GroupKeys bindings -- every row
// in the group shares the identical value there, by definition of grouping,
// so any member would do), and rows is every row so assigned, in input
// order, for the aggregate functions to fold over.
type withGroup struct {
	rep  *Row
	rows []*Row
}

// runWithAggregate implements a WITH clause with at least one aggregate:
// grouping by wc.GroupKeys always happens (independent of wc.Distinct --
// this file's package doc comment explains why DISTINCT is a deliberate
// no-op here and is never consulted below), in first-occurrence group order
// for deterministic output. An empty GroupKeys list (the COLLECT anti-join
// shape, `WITH COLLECT(s) AS exclude`) is the SQL "aggregate with no GROUP
// BY" rule: exactly one implicit group over the whole row set, producing
// exactly one output row even when rows is empty -- never zero, unlike the
// non-empty-GroupKeys case, where zero input rows means zero groups and
// therefore zero output rows.
func runWithAggregate(env *Env, meter *workMeter, wc *WithClause, rows []*Row) ([]*Row, error) {
	var order []string
	groups := map[string]*withGroup{}

	if len(wc.GroupKeys) == 0 {
		g := &withGroup{rows: rows}
		if len(rows) > 0 {
			g.rep = rows[0]
		}
		groups[""] = g
		order = []string{""}
	} else {
		for _, r := range rows {
			key := string(groupKeyBytes(env, r, wc.GroupKeys))
			g, ok := groups[key]
			if !ok {
				g = &withGroup{rep: r}
				groups[key] = g
				order = append(order, key)
			}
			g.rows = append(g.rows, r)
			if err := meter.spend(1); err != nil {
				return nil, err
			}
		}
	}

	out := make([]*Row, 0, len(order))
	for _, key := range order {
		g := groups[key]
		nr := NewRow()
		if g.rep != nil {
			for _, sym := range wc.GroupKeys {
				copySymbolBinding(nr, g.rep, sym)
			}
		}
		for _, c := range wc.Constants {
			nr.SetScalar(c.Alias, c.Value)
		}
		for _, agg := range wc.Aggregates {
			applyAggregate(env, nr, agg, g.rows)
		}
		if err := meter.spend(1); err != nil {
			return nil, err
		}
		out = append(out, nr)
	}
	return out, nil
}

// copySymbolBinding copies sym's binding from src to dst, preferring
// Node > Edge > Scalar -- exactly the namespace-priority order every other
// symbol lookup in this package uses (see eval.go's evalVariableValue). A
// sym this Part never actually bound (unreachable given Plan's own
// symbol-table validation of every GroupKeys entry) leaves dst without a
// binding for it, the same safe behavior as any other absent lookup.
func copySymbolBinding(dst, src *Row, sym string) {
	if nodeID, ok := src.Node(sym); ok {
		dst.SetNode(sym, nodeID)
		return
	}
	if edgeRef, ok := src.Edge(sym); ok {
		dst.SetEdge(sym, edgeRef)
		return
	}
	if val, ok := src.Scalar(sym); ok {
		dst.SetScalar(sym, val)
	}
}

// applyAggregate computes agg over groupRows and binds its alias onto out.
func applyAggregate(env *Env, out *Row, agg WithAggregate, groupRows []*Row) {
	switch {
	case agg.Count != nil:
		// Stored as float64, matching this package's uniform numeric
		// representation for every value that might re-enter a comparison
		// (ORDER BY, WHERE) -- see plan.go's CountAgg doc comment and this
		// task's own brief. A future projection layer (Task 10) converts a
		// bare, top-level count alias to pg-parity int64 at the RETURN
		// boundary only.
		out.SetScalar(agg.Alias, float64(countAggregate(env, agg.Count, groupRows)))
	case agg.Collect != nil:
		out.SetScalar(agg.Alias, collectAggregate(env, agg.Collect, groupRows))
	}
}

// countAggregate implements COUNT(sym)/COUNT(DISTINCT sym) over one group.
// agg.Sym is always bound as a node or edge variable for any query this
// milestone's planner can currently produce (Part[0] -- the only Part a
// WithClause's Aggregates are ever attached to -- never has a pre-existing
// scalar symbol of its own, since that would require a second WITH boundary,
// which Plan rejects), and a bound node/edge is unconditionally non-null in
// every row this package's matchPart ever produces (no OPTIONAL MATCH
// support, so a matched row never carries a "missing" pattern variable) --
// so a plain COUNT(sym) over a node/edge symbol is exactly the group's own
// row count. The scalar branch below implements the general Cypher rule
// (count non-NULL values) anyway, for correctness against a hypothetical
// future scalar Sym, though no query this planner accepts today can reach
// it. COUNT(DISTINCT sym) dedupes by node/edge database id ("pg counts
// distinct composites, which equals distinct ids" -- CountAgg's own doc
// comment) or by the same ScalarEq-consistent encoding grouping uses.
func countAggregate(env *Env, agg *CountAgg, rows []*Row) int64 {
	if !agg.Distinct {
		if len(rows) == 0 {
			return 0
		}
		if _, ok := rows[0].Node(agg.Sym); ok {
			return int64(len(rows))
		}
		if _, ok := rows[0].Edge(agg.Sym); ok {
			return int64(len(rows))
		}
		var n int64
		for _, r := range rows {
			if v, ok := r.Scalar(agg.Sym); ok && v != nil {
				n++
			}
		}
		return n
	}

	seen := map[string]struct{}{}
	for _, r := range rows {
		var key []byte
		if nodeID, ok := r.Node(agg.Sym); ok {
			key = appendTaggedUint(nil, 'N', env.Snap.GraphIDs[nodeID])
		} else if edgeRef, ok := r.Edge(agg.Sym); ok {
			key = appendTaggedUint(nil, 'D', env.Snap.OutEdgeIDs[edgeRef.Fwd])
		} else if v, ok := r.Scalar(agg.Sym); ok && v != nil {
			key = appendScalarKey(nil, v)
		} else {
			continue
		}
		seen[string(key)] = struct{}{}
	}
	return int64(len(seen))
}

// collectAggregate implements COLLECT(sym) over one group: sym is guaranteed
// (classifyAggregate, at plan time) to be a node variable, so every row's own
// database node id is added to the resulting idSet -- see this file's
// package doc comment for why the alias is bound to this Go-native type via
// plain Row.SetScalar rather than any []any/eval.go-visible shape.
func collectAggregate(env *Env, agg *CollectMembershipAgg, rows []*Row) idSet {
	set := make(idSet, len(rows))
	for _, r := range rows {
		if nodeID, ok := r.Node(agg.Sym); ok {
			set[env.Snap.GraphIDs[nodeID]] = struct{}{}
		}
	}
	return set
}

// --- group-key / DISTINCT-key encoding --------------------------------------

// groupKeyBytes encodes r's bindings for syms, in order, into one
// self-delimiting byte sequence -- see this file's package doc comment
// ("Grouping and aggregation") for the collision-freedom argument.
func groupKeyBytes(env *Env, r *Row, syms []string) []byte {
	var buf []byte
	for _, sym := range syms {
		buf = appendSymbolKey(env, buf, r, sym)
	}
	return buf
}

// appendSymbolKey appends sym's binding in r to buf: a node/edge by database
// id, or a scalar by appendScalarKey -- EXCEPT when sym is a scalar entirely
// unbound in r (Row.Scalar's ok == false), which gets the same dedicated 'u'
// tag outValKey uses for an absent OutScalar, deliberately distinct from
// appendScalarKey(nil)'s 'z' tag for a genuinely present JSON null. This
// mirrors RETURN DISTINCT's own dedup-key fix (see OutVal.ScalarAbsent's
// doc): PostgreSQL's GROUP BY/DISTINCT groups SQL NULL together with other
// SQL NULLs, but never with the non-NULL jsonb value 'null'::jsonb, so
// collapsing "never bound" and "bound to a present null" into one key would
// under-count groups relative to pg for the same reason it would for a
// RETURN projection.
func appendSymbolKey(env *Env, buf []byte, r *Row, sym string) []byte {
	if nodeID, ok := r.Node(sym); ok {
		return appendTaggedUint(buf, 'N', env.Snap.GraphIDs[nodeID])
	}
	if edgeRef, ok := r.Edge(sym); ok {
		return appendTaggedUint(buf, 'D', env.Snap.OutEdgeIDs[edgeRef.Fwd])
	}
	val, ok := r.Scalar(sym)
	if !ok {
		return append(buf, 'u')
	}
	return appendScalarKey(buf, val)
}

// appendTaggedUint appends a fixed-width, self-delimiting encoding of one
// uint64 under tag to buf.
func appendTaggedUint(buf []byte, tag byte, v uint64) []byte {
	buf = append(buf, tag)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

// appendUint32 appends a fixed-width big-endian uint32 length prefix to buf.
func appendUint32(buf []byte, v uint32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	return append(buf, tmp[:]...)
}

// appendScalarKey appends a type-tagged, self-delimiting encoding of v (this
// package's usual post-JSON value model: nil, string, float64, bool, []any,
// or map[string]any) to buf, designed so two ScalarEq-equal values always
// produce identical bytes and two values of different JSON types can never
// collide (each case starts with its own fixed tag byte, and every payload
// is either fixed-width or explicitly length-prefixed, so the whole
// concatenation is unambiguous to parse even though nothing here ever
// actually re-parses it). A float64 zero is normalized to positive zero
// before encoding (Go's == treats +0/-0 as equal, matching ScalarEq/
// jsonbEqual's own `==`-based float comparison, but their bit patterns
// differ) so -0 and 0 hash identically, as ScalarEq already requires.
func appendScalarKey(buf []byte, v any) []byte {
	switch val := v.(type) {
	case nil:
		return append(buf, 'z')

	case string:
		buf = append(buf, 's')
		buf = appendUint32(buf, uint32(len(val)))
		return append(buf, val...)

	case float64:
		if val == 0 {
			val = 0
		}
		buf = append(buf, 'f')
		var tmp [8]byte
		binary.BigEndian.PutUint64(tmp[:], math.Float64bits(val))
		return append(buf, tmp[:]...)

	case bool:
		buf = append(buf, 'b')
		if val {
			return append(buf, 1)
		}
		return append(buf, 0)

	case []any:
		buf = append(buf, 'a')
		buf = appendUint32(buf, uint32(len(val)))
		for _, el := range val {
			buf = appendScalarKey(buf, el)
		}
		return buf

	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf = append(buf, 'm')
		buf = appendUint32(buf, uint32(len(keys)))
		for _, k := range keys {
			buf = appendUint32(buf, uint32(len(k)))
			buf = append(buf, k...)
			buf = appendScalarKey(buf, val[k])
		}
		return buf

	default:
		// Unreachable: value.go's own doc comment pins this package's value
		// model to exactly the six cases above. Kept only so this encoder is
		// visibly total against that model, mirroring value.go's own
		// defensive default cases (e.g. Compare's, typeRank's).
		return append(buf, 'x')
	}
}

// --- RETURN DISTINCT ---------------------------------------------------------

// dedupProjected removes every row whose projected tuple (outRows[i]) is a
// byte-identical repeat (via outValKey) of an earlier row's, keeping
// first-occurrence order -- the same policy runWithPassThrough's DISTINCT
// uses, applied to the RETURN projection's own output instead of a WITH
// clause's GroupKeys-bound Row.
func dedupProjected(env *Env, rows []*Row, outRows [][]OutVal) ([]*Row, [][]OutVal) {
	seen := make(map[string]struct{}, len(outRows))
	keptRows := make([]*Row, 0, len(rows))
	keptOut := make([][]OutVal, 0, len(outRows))
	for i, out := range outRows {
		key := rowDistinctKey(env, out)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		keptRows = append(keptRows, rows[i])
		keptOut = append(keptOut, out)
	}
	return keptRows, keptOut
}

// rowDistinctKey encodes one projected row (every column, in projection
// order) into one self-delimiting byte sequence, via outValKey per column.
func rowDistinctKey(env *Env, row []OutVal) string {
	var buf []byte
	for _, v := range row {
		buf = append(buf, outValKey(env, v)...)
	}
	return string(buf)
}

// outValKey encodes one projected OutVal the same way appendSymbolKey
// encodes a Row-bound symbol -- nodes/edges by database id, a path by its
// node/edge id sequence, everything else (OutScalar) via appendScalarKey.
//
// An absent OutScalar (v.ScalarAbsent -- an absent property lookup) is
// tagged 'u', deliberately distinct from appendScalarKey(nil)'s 'z' tag for
// a genuinely present JSON null: see OutVal.ScalarAbsent's doc for why
// PostgreSQL itself never dedups the two together (SQL NULL vs the non-NULL
// jsonb value 'null'::jsonb).
func outValKey(env *Env, v OutVal) []byte {
	switch v.Kind {
	case OutNode:
		return appendTaggedUint(nil, 'N', env.Snap.GraphIDs[v.Node])
	case OutEdge:
		return appendTaggedUint(nil, 'D', env.Snap.OutEdgeIDs[v.Edge.Fwd])
	case OutPath:
		buf := []byte{'P'}
		buf = appendUint32(buf, uint32(len(v.Path.Nodes)))
		for _, n := range v.Path.Nodes {
			buf = appendTaggedUint(buf, 'n', env.Snap.GraphIDs[n])
		}
		buf = appendUint32(buf, uint32(len(v.Path.Edges)))
		for _, e := range v.Path.Edges {
			buf = appendTaggedUint(buf, 'e', env.Snap.OutEdgeIDs[e.Fwd])
		}
		return buf
	default: // OutScalar
		if v.ScalarAbsent {
			return []byte{'u'}
		}
		return appendScalarKey(nil, v.Scalar)
	}
}

// --- ORDER BY / SKIP / LIMIT -------------------------------------------------

// aliasIndexFor maps every RETURN item's own alias to its projection index,
// for resolving an ORDER BY item that names a projected column.
func aliasIndexFor(proj Projection) map[string]int {
	idx := make(map[string]int, len(proj.Items))
	for i, item := range proj.Items {
		idx[item.Alias] = i
	}
	return idx
}

// resolveOrderValue resolves one OrderKey.Symbol against either the RETURN
// projection (aliasIndex -- guaranteed scalar-shaped by planOrder's own
// symNode/symEdge/symPath rejection, so outRow[idx].Scalar is always the
// right field) or, for a preceding WITH's COUNT alias that Plan allows even
// when it is not re-projected in RETURN (OrderKey's own doc comment), the
// row's own pre-projection scalar binding -- mirroring eval.go's
// evalVariableValue's Row.Scalar-first priority.
func resolveOrderValue(aliasIndex map[string]int, row *Row, outRow []OutVal, sym string) any {
	if idx, ok := aliasIndex[sym]; ok {
		return outRow[idx].Scalar
	}
	v, _ := row.Scalar(sym)
	return v
}

// sortRows stably sorts rows and outRows in lockstep (both index i always
// describes the same logical row) by order, resolving each key via
// resolveOrderValue and comparing via value.go's Compare. Compare's
// ErrCollation return aborts the sort (and the whole query -- Query's own
// documented all-or-nothing materialization invariant) rather than being
// silently swallowed; sort.SliceStable's comparator signature has no error
// return, so the first Compare error encountered is latched via sortErr and
// checked once the sort call returns.
//
// DESC is implemented as a plain negation of Compare's numeric result, never
// as "compare normally, but always keep NULL last regardless of direction".
// This is deliberate, not an oversight: value.go's Compare already places a
// JSON null via a pair of symmetric branches (`a == nil -> 1`, `b == nil ->
// -1`), which makes Compare antisymmetric by construction -- negating
// Compare(a, b) is therefore always identical to swapping the operands and
// calling Compare(b, a) again. PostgreSQL's own `ORDER BY x DESC` inverts
// the *whole* comparator this way, null rank included: a null sorts last in
// ASC (the highest jsonb type rank) and therefore FIRST in DESC, not "last
// no matter what". A naive DESC that special-cased null to stay last
// independent of direction would silently disagree with pg here -- this is
// exactly the corner the milestone brief calls out by name.
func sortRows(rows []*Row, outRows [][]OutVal, order []OrderKey, aliasIndex map[string]int) error {
	idx := make([]int, len(rows))
	for i := range idx {
		idx[i] = i
	}

	var sortErr error
	sort.SliceStable(idx, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		a, b := idx[i], idx[j]
		for _, ok := range order {
			av := resolveOrderValue(aliasIndex, rows[a], outRows[a], ok.Symbol)
			bv := resolveOrderValue(aliasIndex, rows[b], outRows[b], ok.Symbol)
			c, err := Compare(av, bv)
			if err != nil {
				sortErr = err
				return false
			}
			if ok.Descending {
				c = -c
			}
			if c != 0 {
				return c < 0
			}
		}
		return false
	})
	if sortErr != nil {
		return sortErr
	}

	sortedRows := make([]*Row, len(rows))
	sortedOut := make([][]OutVal, len(outRows))
	for newPos, oldPos := range idx {
		sortedRows[newPos] = rows[oldPos]
		sortedOut[newPos] = outRows[oldPos]
	}
	copy(rows, sortedRows)
	copy(outRows, sortedOut)
	return nil
}

// applySkipLimit applies q.Skip/q.Limit to rows, in that order, over
// whatever order the pipeline so far produced (sorted, if there was an
// ORDER BY; otherwise this package's own deterministic materialization
// order, standing in for pg's "unspecified without ORDER BY" -- see Query's
// own doc comment on Skip/Limit). Both are always literal, non-negative
// int64s by the time Execute ever sees them (literalNonNegativeInt already
// rejects a negative or non-literal SKIP/LIMIT at plan time); Limit == -1
// means no LIMIT was written at all (unbounded).
func applySkipLimit(rows [][]OutVal, skip, limit int64) [][]OutVal {
	if skip > 0 {
		if skip >= int64(len(rows)) {
			return rows[:0]
		}
		rows = rows[skip:]
	}
	if limit >= 0 && limit < int64(len(rows)) {
		rows = rows[:limit]
	}
	return rows
}

// --- WHERE evaluation with COLLECT-membership interception ------------------

// evalWhereWithMembership is EvalPredicate's boolean-structural recursion,
// reimplemented so that a Comparison (or a Negation wrapping one) matching
// the shape `<bound node var> IN <collect alias>` is answered directly by
// database-id-set lookup instead of falling through to eval.go's generic
// evalIn -- see this file's package doc comment for why that fallthrough is
// unsafe for this one shape. Every other node type delegates to eval.go's own
// unexported leaf-level helpers (which never recurse back into predicate
// evaluation themselves, so there is nothing left for this function to
// intercept beneath them).
func evalWhereWithMembership(env *Env, row *Row, expr cypher.Expression, membership membershipTable) (Tri, error) {
	switch e := expr.(type) {
	case nil:
		return TriNull, nil

	case *cypher.Parenthetical:
		if e == nil {
			return TriNull, ErrUnsupported
		}
		return evalWhereWithMembership(env, row, e.Expression, membership)

	case *cypher.Negation:
		if e == nil {
			return TriNull, ErrUnsupported
		}
		return evalNegationWithMembership(env, row, e.Expression, membership)

	case *cypher.Conjunction:
		if e == nil {
			return TriNull, ErrUnsupported
		}
		result := TriTrue
		for _, sub := range e.GetAll() {
			t, err := evalWhereWithMembership(env, row, sub, membership)
			if err != nil {
				return TriNull, err
			}
			result = result.And(t)
		}
		return result, nil

	case *cypher.Disjunction:
		if e == nil {
			return TriNull, ErrUnsupported
		}
		result := TriFalse
		for _, sub := range e.GetAll() {
			t, err := evalWhereWithMembership(env, row, sub, membership)
			if err != nil {
				return TriNull, err
			}
			result = result.Or(t)
		}
		return result, nil

	case *cypher.ExclusiveDisjunction:
		if e == nil {
			return TriNull, ErrUnsupported
		}
		all := e.GetAll()
		if len(all) == 0 {
			return TriFalse, nil
		}
		result, err := evalWhereWithMembership(env, row, all[0], membership)
		if err != nil {
			return TriNull, err
		}
		for _, sub := range all[1:] {
			t, err := evalWhereWithMembership(env, row, sub, membership)
			if err != nil {
				return TriNull, err
			}
			result = result.Xor(t)
		}
		return result, nil

	case *cypher.Comparison:
		if e == nil {
			return TriNull, ErrUnsupported
		}
		if t, matched := tryMembershipComparison(env, row, e, membership); matched {
			return t, nil
		}
		return evalComparison(env, row, e)

	case *cypher.KindMatcher:
		if e == nil {
			return TriNull, ErrUnsupported
		}
		return evalKindMatcher(env, row, e)

	default:
		// A bare PropertyLookup/Variable/FunctionInvocation/Literal used
		// directly as a conjunct: none of these can contain a nested
		// predicate (they are value expressions, not boolean structure), so
		// there is nothing here a collect-membership comparison could hide
		// inside -- safe to delegate to eval.go's own truthiness rule
		// directly.
		return evalValueTruthiness(env, row, expr)
	}
}

// evalNegationWithMembership mirrors eval.go's evalNegation: the
// collect-membership shape is checked first (a simple Tri.Not() over the
// positive membership answer is correct here, since membership is total --
// never TriNull -- for the one operand shape Plan allows, a bound node
// variable), then eval.go's own STARTS WITH/ENDS WITH/CONTAINS/regex
// negation special case (evaluated directly via eval.go's leaf-level
// helpers, which cannot themselves contain a nested collect-membership
// comparison), and only then the generic "evaluate positively (through this
// same recursive function, so a deeper nested membership comparison is still
// intercepted), then negate" fallback.
func evalNegationWithMembership(env *Env, row *Row, expr cypher.Expression, membership membershipTable) (Tri, error) {
	if cmp, isCmp := unwrapParens(expr).(*cypher.Comparison); isCmp && cmp != nil {
		if t, matched := tryMembershipComparison(env, row, cmp, membership); matched {
			return t.Not(), nil
		}
		if len(cmp.Partials) == 1 {
			if partial := cmp.Partials[0]; partial != nil {
				switch partial.Operator {
				case cypher.OperatorStartsWith, cypher.OperatorEndsWith, cypher.OperatorContains:
					return evalStringPredicate(env, row, cmp.Left, partial.Operator, partial.Right, true)
				case cypher.OperatorRegexMatch:
					return evalRegexComparison(env, row, cmp.Left, partial.Right, true)
				}
			}
		}
	}

	t, err := evalWhereWithMembership(env, row, expr, membership)
	if err != nil {
		return TriNull, err
	}
	return t.Not(), nil
}

// tryMembershipComparison recognizes expr (after unwrapping parens) as
// `<left> IN <right>` with right a bare Variable naming a bound collect-alias
// membership set and left a bare Variable bound as a node in row -- the one
// shape Plan's checkInOperands allows a collect alias to appear in. matched
// is false for anything else (the caller falls through to ordinary
// evaluation in that case). Per the brief's pinned semantics: an empty set
// answers TriFalse (pg's `x = ANY(empty)` is false, never null), and this
// answer is always total (never TriNull) since Plan guarantees left is a
// bound node variable, which -- absent OPTIONAL MATCH -- this package's rows
// always carry.
func tryMembershipComparison(env *Env, row *Row, cmp *cypher.Comparison, membership membershipTable) (Tri, bool) {
	if len(membership) == 0 || cmp == nil || len(cmp.Partials) != 1 || cmp.Partials[0] == nil {
		return TriNull, false
	}
	partial := cmp.Partials[0]
	if partial.Operator != cypher.OperatorIn {
		return TriNull, false
	}
	rv, ok := unwrapParens(partial.Right).(*cypher.Variable)
	if !ok || rv == nil {
		return TriNull, false
	}
	set, isMembership := membership[rv.Symbol]
	if !isMembership {
		return TriNull, false
	}
	lv, ok := unwrapParens(cmp.Left).(*cypher.Variable)
	if !ok || lv == nil {
		return TriNull, false
	}
	nodeID, ok := row.Node(lv.Symbol)
	if !ok {
		// Plan's checkInOperands guarantees the left operand is a bound node
		// variable; reaching here would mean this executor's own row somehow
		// lacks it. Report "not a membership comparison after all" rather
		// than fabricate an answer, so the caller's generic fallback (which
		// will itself fail loudly via ErrUnsupported/evalIn) is what surfaces
		// the inconsistency instead of a silently wrong TriFalse.
		return TriNull, false
	}
	_, in := set[env.Snap.GraphIDs[nodeID]]
	return boolToTri(in), true
}
