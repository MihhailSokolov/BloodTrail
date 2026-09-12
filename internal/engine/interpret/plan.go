// SPDX-License-Identifier: Apache-2.0

// plan.go implements the default-deny planner: a walk
// from a parsed dawgs Cypher AST (github.com/specterops/dawgs@v0.8.0
// cypher/models/cypher, produced by frontend.ParseCypher with a zero-filter
// context -- the same context the pg driver itself uses) to an executable
// Query IR, or a plain "no" when the query falls outside the interpreter's
// supported subset.
//
// The governing rule is default-deny: Plan only ever explicitly *accepts* a
// finite list of AST shapes (documented at each accepting branch below).
// Anything it does not recognize -- a construct this file has no case for,
// a malformed/nil AST node, a kind name absent from the snapshot, a regex
// that fails to compile, ... -- falls through to a `return nil, false`,
// which the caller (the engine) treats as "delegate to PostgreSQL, which is
// always correct". Plan itself never panics (a recover() backstops the
// entire walk) and never returns an error: every failure mode is exactly
// ok=false.
//
// Plan does not evaluate anything: it has no snapshot.Snapshot rows to look
// at yet (Plan runs once per query text, before any node is scanned). Its
// jobs are (1) decide whether the query's *shape* is inside the matrix at
// all, consulting only Snapshot.Kinds (to resolve label/type names to
// snapshot.KindID) and (2) compile the query into a plain-data Query value a
// later executor can run directly against the snapshot,
// without ever re-inspecting the AST for feasibility.

package interpret

import (
	"regexp"
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// MaxExpansionDepth mirrors dawgs' own translateDefaultMaxTraversalDepth: the
// hop cap (edges, not nodes) applied to a variable-length relationship
// pattern that has no explicit upper bound (`*1..`, `*..`, bare `*`). An
// explicit upper bound in the query text (`*1..5`) always overrides this,
// even if it is larger than 15 -- see buildStep.
//
// This constant's natural home would be the engine package, alongside
// maxCypherRows/maxCypherWork/edgePropsBatchSize (engine/serve_cypher.go),
// but this package sits below the engine in the import graph (the engine
// imports interpret, never the reverse), so the authoritative copy must
// live here, exported and self-contained -- the budget-constants doc in
// engine/serve_cypher.go explains the same constraint from its side.
const MaxExpansionDepth = 15

// --- Query IR ---------------------------------------------------------------

// ShortestMode selects how a pattern part's relationship should be expanded:
// as an ordinary (possibly variable-length) chain, or as a shortestPath()/
// allShortestPaths() call.
type ShortestMode uint8

const (
	ShortestNone ShortestMode = iota
	ShortestOne               // shortestPath(...): one path per (root, terminal) pair
	ShortestAll               // allShortestPaths(...): every minimum-length trail per pair
)

// Range is a variable-length relationship pattern's inclusive [Min, Max] hop
// bound, counting edges (not nodes) -- e.g. `*1..3` is {Min:1,Max:3}, bare
// `*` or `*..` is {Min:1,Max:MaxExpansionDepth}, `*0..` is {Min:0,Max:
// MaxExpansionDepth}. A fixed-length (non-variable) relationship step has a
// nil *Range on its Step, not a Range{1,1}.
type Range struct {
	Min, Max int
}

// Step is one relationship hop in a pattern chain: FromSym --EdgeSym-->ToSym,
// walked in the direction Direction *as written after normalization* -- see
// buildStep's doc for the inbound-arrow swap. FromSym/ToSym/EdgeSym name
// pattern variables recorded in the owning Part's Nodes (FromSym/ToSym) and
// symbol table (EdgeSym, "" for an anonymous relationship).
//
// Direction is always DirectionOutbound for a step with a non-nil Range
// (variable-length): an undirected variable-length pattern is rejected by
// the planner (mirrors dawgs' own translate-time rejection, "unsupported
// expansion direction"). A fixed-length step (Range == nil) may additionally
// carry DirectionBoth, meaning "walk both CSR directions and require the two
// endpoints differ" -- the executor's job, not this package's.
type Step struct {
	FromSym, EdgeSym, ToSym string
	Direction               graph.Direction
	EdgeKinds               []snapshot.KindID // nil/empty => all kinds
	Range                   *Range            // nil => fixed-length (exactly one hop)
	Shortest                ShortestMode
	// PathSym is the pattern part's own named-path variable ("p" in
	// `MATCH p = ...`), copied onto every Step that pattern part produced,
	// or "" if the pattern was never named (or is anonymous). A future
	// executor identifies "every Step belonging to path p" by this field,
	// not by re-walking the AST -- see Part's doc for why a named path can
	// *only* be projected as a bare RETURN item.
	PathSym string
	// HasExplicitEndpointInequality is set (Shortest != ShortestNone steps
	// only) when the owning Part's WHERE carries an explicit `s<>t` or
	// `id(s)<>id(t)` conjunct over this step's own two endpoints. The
	// executor needs this for the self-pair rule pinned in the design doc:
	// shortestPath/allShortestPaths over endpoint sets that can coincide
	// raises pg's SQLSTATE 22023 unless the query itself already excludes
	// the self-pair via this comparison, in which case self-pairs are
	// silently dropped instead of erroring.
	HasExplicitEndpointInequality bool
	// Reversed records whether buildStep's inbound-arrow swap actually fired
	// for this step -- i.e. the pattern was originally written with a
	// backward arrow (`(t)<-[:R]-(s)`), so FromSym/ToSym above hold the
	// TRAVERSAL direction (s, then t), the opposite of the pattern's own
	// WRITTEN order (t, then s). A named path's PathVal is otherwise always
	// assembled in traversal order (expand.go's expandVarLengthTrailsForSeed/
	// expandShortestPathComponent), which is exactly right for a forward
	// arrow but backward for one of these -- see reversePathVal's own doc for
	// why every PathVal built from a Reversed step's own expansion must have
	// its Nodes/Edges order flipped before it is ever handed to a caller
	// (RETURN p), to match Cypher's
	// (and pg's) own "node sequence follows the pattern as written"
	// semantics regardless of which way any one edge happens to point.
	// A step reached through expandChainComponent (exec.go) can carry
	// Reversed too, but only when it is the chain's OWN single step -- a
	// one-step chain (e.g. `MATCH p = (t)<-[:R]-(s) RETURN p`) trivially
	// satisfies isStrictLinearChain's "each step starts where the last one
	// ended" continuity test (there being no previous step to satisfy it
	// against), and assembleChainPathVal (exec.go) applies the same
	// pattern-order correction for that case that
	// expandVarLengthTrailsForSeed/expandShortestPathComponent apply below.
	// A Reversed step at any OTHER position of a MULTI-step chain can never
	// reach expandChainComponent: isStrictLinearChain's continuity test
	// requires a Reversed step's own FromSym (always that step's freshly
	// introduced pattern variable) to equal the previous step's ToSym,
	// which no continuing chain can satisfy -- assembleChainPathVal guards
	// this explicitly (errUnsupportedStep) rather than relying on it as an
	// emergent property of isStrictLinearChain's own logic.
	Reversed bool
}

// NodeConstraint accumulates everything Plan determined about one pattern
// variable ahead of execution, merged across every occurrence of that
// variable within one Part (the same variable may legally appear in more
// than one node pattern within a Part -- e.g. `(a)-->(b), (b)-->(a)` reusing
// "a" and "b" -- joining by node identity; see the doc on declareSymbol).
//
// Kinds is every kind label attached to the variable across all its pattern
// occurrences, ANDed (Cypher's `(n:A:B)` semantics: n must carry every
// listed kind). IDs holds explicit database ids pulled from `id(sym) =
// <literal>` WHERE conjuncts (deduplicated; two *different* literal values
// for the same symbol reject the whole query rather than silently produce
// an always-empty result -- see extractIDAnchor). ObjectIDAnchor is set from
// a `sym.objectid = <string-literal>` WHERE conjunct, the fast O(1)
// PropStore.NodesByObjectID path (PostgreSQL enforces no uniqueness
// constraint on objectid, so the executor must treat this as a *set* of
// candidate nodes -- usually one, but never assumed to be).
//
// Predicates is every WHERE conjunct (or
// inline-map-desugared equality, see addNodePattern) that Plan determined
// touches this variable *alone* -- a REDUNDANT, additive copy of a subset of
// what Part.Where already carries, pushed here purely as an executor
// optimization (early per-candidate filtering before a full row is
// assembled). Part.Where alone is always the complete, sufficient WHERE
// expression: an executor that ignores Predicates entirely and evaluates
// only Part.Where is slower, never wrong. An executor must always evaluate
// Part.Where in full against every assembled row regardless of what
// Predicates also duplicates for early filtering -- Predicates is never a
// substitute for Where, only a hint layered on top of it.
//
// Kinds/IDs/ObjectIDAnchor are populated *only* from the node's own pattern
// occurrences and from single-symbol id()/objectid WHERE conjuncts -- a
// WHERE-position `n:Kind` matcher is validated (the kind name must resolve)
// and left in Part.Where, but is deliberately *not* folded into Kinds here
// (a scope simplification with no effect on any required serving shape;
// every kind-or-id-constrained shortestPath endpoint in the corpus already
// gets its constraint from the node pattern itself -- see Plan's doc on the
// endpoint-constraint check).
type NodeConstraint struct {
	Kinds          []snapshot.KindID
	IDs            []uint64
	ObjectIDAnchor *string
	Predicates     []cypher.Expression
}

// CountAgg is a WITH/RETURN `COUNT(sym)` or `COUNT(DISTINCT sym)` aggregate.
// `COUNT(*)` is not supported (out of scope: absent from every required
// serving shape; see classifyAggregate).
type CountAgg struct {
	Distinct bool
	Sym      string
}

// CollectMembershipAgg is a WITH `COLLECT(sym)` aggregate, servable *only*
// when sym is a node variable and the resulting alias is consumed
// exclusively as the right-hand side of an IN/NOT IN test later in the
// query (see WithAggregate.Alias and checkInOperands) -- the one shape pg's
// translation lowers to id-set membership rather than a nodecomposite[]
// array (design doc, decision/amendment b).
type CollectMembershipAgg struct {
	Sym string
}

// WithAggregate is one aliased aggregate item in a WITH projection --
// `COUNT(c) AS adminCount` or `COLLECT(s) AS exclude`. Exactly one of Count/
// Collect is non-nil.
type WithAggregate struct {
	Alias   string
	Count   *CountAgg
	Collect *CollectMembershipAgg
}

// WithConstant is a WITH projection item of the form `<literal> AS name`
// (e.g. `WITH 1 AS one`) -- Value is the literal's post-JSON value (this
// package's usual nil|string|float64|bool|[]any|map[string]any model).
type WithConstant struct {
	Alias string
	Value any
}

// WithClause is the plan for one `WITH` boundary between two Parts (the
// supported pipeline is exactly one such boundary -- Plan rejects
// any query with more than one, see planStages). Every WITH projection item
// must be one of exactly four shapes (controller amendment, narrower than
// general Cypher WITH):
//
//  1. a bare, unaliased variable ("plain variable carry-over") -- becomes a
//     GroupKeys entry;
//  2. the WITH projection carries the DISTINCT keyword (orthogonal to the
//     item shapes; recorded here as Distinct);
//  3. an aliased bare COUNT(sym)/COUNT(DISTINCT sym) or COLLECT(sym) --
//     becomes a WithAggregate;
//  4. a `<literal> AS name` constant -- becomes a WithConstant.
//
// Nothing else is accepted: sum/avg/min/max, a WITH-level WHERE (`WITH x
// WHERE ...`), ORDER BY/SKIP/LIMIT on the WITH itself, RETURN *-style ALL,
// an aliased plain variable (`WITH u AS x`), or a projection item that is
// none of the above all reject the whole query (see planWith).
//
// Execution semantics any executor must reproduce, since they
// are not obvious from the field shapes alone: grouping by GroupKeys happens
// automatically whenever len(Aggregates) > 0 (an aggregate function always
// implies "group by every other projected item", independent of the
// DISTINCT keyword); when len(Aggregates) == 0, Distinct decides between a
// plain per-input-row pass-through (Distinct == false: every input row
// survives, GroupKeys is not a deduplication key) and a dedup-by-projected-
// tuple pass (Distinct == true). When GroupKeys is empty *and*
// len(Aggregates) > 0 (the COLLECT anti-join shape: `WITH COLLECT(s) AS
// exclude`, no plain variables at all), the whole matched-row set is one
// implicit group, producing exactly one output row -- not zero.
type WithClause struct {
	Distinct   bool
	GroupKeys  []string
	Aggregates []WithAggregate
	Constants  []WithConstant
}

// Part is one query "part" in the multi-part-query sense: a pattern
// (possibly assembled from several comma-separated PatternParts across
// several sequential MATCH clauses, joined on shared variable identity —
// "duplicate variable reuse" — see declareSymbol) filtered by Where and
// optionally rolled up by a trailing With before the next Part begins.
//
// Chains lists every relationship hop from every pattern in this Part, in
// pattern order; Nodes carries per-symbol constraints for every node
// variable this Part's patterns bind, including variables that never appear
// in any Step (an isolated node pattern like `(n:User)` contributes a Nodes
// entry and no Chains entry at all -- the executor's cue to run a plain
// kind-bitmap-anchored scan for that symbol instead of a chain expansion).
//
// Where is the Part's *complete and sufficient* WHERE expression: every
// WHERE clause from every ReadingClause in this Part, ANDed together with
// every inline node-pattern property map's desugared equality (see
// addNodePattern/desugarPropertyMap) -- never merely "whatever was left
// over after pushdown". The executor must evaluate this in full against
// every assembled row; doing so is both necessary and sufficient to decide
// row membership. NodeConstraint.Predicates duplicates a subset of these
// same conjuncts (plus the same desugared equalities) purely so an executor
// can filter a candidate early, before a full row is even assembled -- an
// executor that ignores Predicates and evaluates only Where is slower,
// never wrong; an executor must always evaluate Where in full regardless of
// what it also did with Predicates. Where is nil when the Part has no WHERE
// clause and no pattern in it carries an inline property map.
//
// With is non-nil exactly on a Part that is followed by another Part (i.e.
// it is never set on Query.Parts' last element): it is the WITH clause that
// transforms this Part's matched-and-filtered rows before the next Part's
// patterns run.
//
// A named, non-shortestPath path variable (`MATCH p = (a)-->(b)`) is
// recorded on its Steps' PathSym exactly like a shortestPath's path
// variable -- eval.go's expression evaluator has no path-value model at all
// (Row.PathVar's doc: "no task before this one materializes a path value"),
// so *no* Part.Where/RETURN expression may reference a path symbol except
// as the sole, bare content of a RETURN/WITH projection item (a future
// executor materializes that value directly from the Steps carrying the
// matching PathSym, bypassing EvalValue entirely for it) -- see the
// planReturn doc and checkExpr's rejection of symPath everywhere else.
type Part struct {
	Chains []Step
	Nodes  map[string]*NodeConstraint
	Where  cypher.Expression
	With   *WithClause
}

// ProjectionOutput is one RETURN item's compiled plan: the column name
// (explicit alias, or a name Plan derives for a bare variable/property
// lookup -- see projectionName), the expression to evaluate per row, and
// BareCallKind.
//
// BareCallKind is "" for an ordinary item; for a RETURN item whose
// expression, taken as a *whole*, is exactly one of id(x)/size(x)/
// datetime().epochseconds/datetime().epochmillis (the controller's
// projection-typing amendment), it is "id"/"size"/"epochseconds"/
// "epochmillis" respectively. A later materialization task is expected to
// convert this package's uniform float64 result to the pg-parity integer
// type (int64 for id, int32 for size, int64 for the epoch accessors) only
// for a non-empty BareCallKind; every other item passes through unconverted.
// Plan itself never accepts one of these four calls *nested* inside
// arithmetic or another function in a RETURN/WITH item (see
// projectionTypingOK) -- BareCallKind being non-empty is therefore always a
// true statement about the item's entire expression, not merely its root.
type ProjectionOutput struct {
	Alias        string
	Expr         cypher.Expression
	BareCallKind string
}

// Projection is the compiled RETURN clause: Distinct is RETURN DISTINCT
// (dedup by the full output tuple); Items is the projection in RETURN
// order.
type Projection struct {
	Distinct bool
	Items    []ProjectionOutput
}

// OrderKey is one ORDER BY item. Symbol names either a RETURN projection
// alias (see planOrder) or, per the controller's amendment ("ORDER BY items
// that aren't projected aliases / bare count aliases" reject), a COUNT
// aggregate alias from the immediately preceding Part's WithClause even
// when that alias is not re-projected in RETURN (the corpus's own
// `WITH DISTINCT u, COUNT(c) AS adminCount RETURN u ORDER BY adminCount
// DESC` shape) -- an executor resolves Symbol against whichever of "this
// row's projected output" or "this row's still-available WITH-stage
// scalars" actually carries it, exactly as eval.go's evalVariableValue
// already checks Row.Scalar before Row.Node.
type OrderKey struct {
	Symbol     string
	Descending bool
}

// Query is Plan's complete output: an executable, plain-data compilation of
// one served Cypher query. Limit == -1 means no LIMIT was specified
// (unbounded); Limit >= 0 is a literal LIMIT value (LIMIT 0 is a valid,
// distinct case: zero rows). Skip defaults to 0 whether or not SKIP was
// written (SKIP 0 and no SKIP are observably identical). Regexes holds
// every distinct `=~` pattern found anywhere in the query, already compiled
// at plan time (regexp.Compile failure rejects the whole query -- see
// checkRegexOperands) and keyed by the pattern's *decoded* text (the same
// string eval.go's Env.compiledRegex would be asked to compile lazily on
// its own) -- an executor may consult this map to skip that lazy compile
// entirely, or may ignore it and let Env's own cache do the work; either way
// the compile-at-most-once guarantee already holds by the time Execute ever
// sees the plan.
//
// Execution invariant this whole file's accept surface depends on (Tasks
// 7-9 implement it, but Plan is written assuming it holds): a served
// Query's results are fully materialized before any row is emitted to the
// caller, and ANY evaluator error encountered anywhere during that
// materialization -- ErrRuntimeCast, ErrCollation, ErrNotComparable,
// ErrUnsupported, or any other eval.go error -- aborts the *whole* query,
// which the engine then delegates to PostgreSQL in full; there is no
// partial serving of a prefix of rows followed by a fallback for the rest.
// This is why Plan is free to accept statically-untypeable constructs whose
// safety can only be confirmed per-row at execution time (e.g.
// `size(n.prop)` when n.prop might not be a list, or `x IN n.prop` when
// n.prop might not be a list at all) -- accepting them is safe precisely
// because a row that turns out to be untypeable never gets emitted on its
// own; it instead aborts materialization and the engine delegates the
// entire query to pg, which is always correct. A "serve what materialized
// successfully, delegate only the rest" execution model would violate this
// contract and require re-auditing every acceptance decision in this file
// that currently relies on it.
//
// Under a final LIMIT with no ORDER BY and no RETURN DISTINCT, the executor
// may stop producing rows once SKIP+LIMIT post-filter rows exist
// (runComponentLimited, pipeline.go). Rows past that cutoff are then never
// evaluated, so an evaluator error lurking there aborts nothing -- the query
// still serves. This matches PostgreSQL, which streams under LIMIT and also
// never evaluates rows past its own cutoff; both outcomes (serve N rows, or
// raise the error) sit inside pg's own nondeterministic behavior envelope for
// such a query. Every row that IS emitted still went through this
// invariant's full evaluation above, so emitted content never diverges from
// what the unlimited path could also have produced.
type Query struct {
	Parts     []Part
	Returning Projection
	Order     []OrderKey
	Skip      int64
	Limit     int64
	Regexes   map[string]*regexp.Regexp
}

// --- Plan entry point ---------------------------------------------------

// Plan walks q (parsed by the caller via frontend.ParseCypher against a
// zero-filter context, exactly as the pg driver parses) and produces an
// executable Query, or ok=false when the query falls outside the
// interpreter's supported subset -- in which case the caller must delegate
// to PostgreSQL, which is always correct for that query. Plan never panics
// (recover() backstops the whole walk: an unexpected AST shape this file
// did not anticipate becomes ok=false, never a crash) and never returns an
// error.
//
// snap.Kinds is the only part of the snapshot Plan consults (to resolve
// label/relationship-type/kind-matcher names to snapshot.KindID); it must be
// non-nil, matching the invariant that Snapshot.Kinds is never nil after
// Build.
func Plan(q *cypher.RegularQuery, snap *snapshot.View) (result *Query, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			result, ok = nil, false
		}
	}()

	if q == nil || snap == nil || snap.Kinds() == nil || q.SingleQuery == nil {
		return nil, false
	}

	stages, ok := planStages(q.SingleQuery)
	if !ok {
		return nil, false
	}

	regexes := map[string]*regexp.Regexp{}
	known := map[string]symKind{}
	countAliases := map[string]bool{}
	numericScalars := map[string]bool{}

	parts := make([]Part, 0, len(stages))
	for _, st := range stages {
		part, nextKnown, ok := planPart(snap, regexes, known, numericScalars, st.reading)
		if !ok {
			return nil, false
		}

		if st.with != nil {
			wc, outputKnown, ok := planWith(nextKnown, st.with)
			if !ok {
				return nil, false
			}
			part.With = &wc
			known = outputKnown
			countAliases = countAliasSet(wc)
			numericScalars = numericScalarSet(wc)
		} else {
			known = nextKnown
		}

		parts = append(parts, part)

		if st.ret != nil {
			if countShortestSteps(parts) > 1 {
				// "more than one shortestPath/allShortestPaths pattern part
				// per query" -- a conservative tightening: the corpus never
				// needs more than one, and this planner has not verified how
				// pg/dawgs' translation composes two independent shortest-
				// path searches (each potentially with its own endpoint set)
				// within one query, so it declines rather than guess. Scoped
				// across every Part of the whole Query, not just one Part,
				// since a two-Part (one-WITH-boundary) query could otherwise
				// smuggle a second shortestPath past a per-Part-only check.
				return nil, false
			}
			proj, order, skip, limit, ok := planReturn(snap, known, countAliases, numericScalars, st.ret)
			if !ok {
				return nil, false
			}
			return &Query{
				Parts:     parts,
				Returning: proj,
				Order:     order,
				Skip:      skip,
				Limit:     limit,
				Regexes:   regexes,
			}, true
		}
	}

	// No stage carried a RETURN: malformed (every real query ends in a
	// RETURN once UpdatingClauses are already rejected).
	return nil, false
}

// queryStage is one segment of the pipeline Plan walks: a run of
// ReadingClauses, optionally followed by either a WITH boundary (with) or
// the query's final RETURN (ret) -- never both.
type queryStage struct {
	reading []*cypher.ReadingClause
	with    *cypher.With
	ret     *cypher.Return
}

// planStages normalizes q's SinglePartQuery/MultiPartQuery shape into a flat
// list of queryStages. Only two shapes are accepted: a bare SinglePartQuery
// (no WITH at all -- the overwhelming majority of served queries, including
// every prebuilt shortestPath shape), or a MultiPartQuery with *exactly one*
// intermediate part (exactly one WITH boundary) -- "multi-part WITH beyond
// the supported pipeline" (two or more WITH boundaries) is rejected here.
// Mutating clauses (UpdatingClauses, including MERGE) anywhere reject
// immediately.
func planStages(sq *cypher.SingleQuery) ([]queryStage, bool) {
	switch {
	case sq.SinglePartQuery != nil && sq.MultiPartQuery == nil:
		spq := sq.SinglePartQuery
		if len(spq.UpdatingClauses) != 0 || spq.Return == nil {
			return nil, false
		}
		return []queryStage{{reading: spq.ReadingClauses, ret: spq.Return}}, true

	case sq.MultiPartQuery != nil && sq.SinglePartQuery == nil:
		mpq := sq.MultiPartQuery
		if len(mpq.Parts) != 1 {
			return nil, false
		}
		p0 := mpq.Parts[0]
		if p0 == nil || len(p0.UpdatingClauses) != 0 || p0.With == nil {
			return nil, false
		}
		final := mpq.SinglePartQuery
		if final == nil || len(final.UpdatingClauses) != 0 || final.Return == nil {
			return nil, false
		}
		return []queryStage{
			{reading: p0.ReadingClauses, with: p0.With},
			{reading: final.ReadingClauses, ret: final.Return},
		}, true

	default:
		// Neither set, or (structurally impossible) both set.
		return nil, false
	}
}

func countAliasSet(wc WithClause) map[string]bool {
	out := make(map[string]bool, len(wc.Aggregates))
	for _, agg := range wc.Aggregates {
		if agg.Count != nil {
			out[agg.Alias] = true
		}
	}
	return out
}

// numericScalarSet returns every alias wc carries forward that is
// statically guaranteed to hold a genuine number on every row -- a
// `<literal> AS name` WithConstant whose literal is itself numeric (planWith
// builds WithConstant.Value via evalLiteralValue, which normalizes every
// numeric literal -- int64, uint64, or float64 -- to a Go float64, this
// package's own post-JSON numeric representation), or a `COUNT(...) AS
// name` WithAggregate (countAggregate always produces a float64). See
// partBuilder.numericScalars' own doc for why the next Part's
// checkComparison (relationalComparisonSafe) needs this: a bare
// reference to such an alias has no numeric-literal AST shape of its own
// for isStaticallyNumericScalar to recognize directly.
func numericScalarSet(wc WithClause) map[string]bool {
	out := make(map[string]bool, len(wc.Aggregates)+len(wc.Constants))
	for _, agg := range wc.Aggregates {
		if agg.Count != nil {
			out[agg.Alias] = true
		}
	}
	for _, c := range wc.Constants {
		if _, ok := c.Value.(float64); ok {
			out[c.Alias] = true
		}
	}
	return out
}

// countShortestSteps counts every Step across every given Part whose
// Shortest is not ShortestNone -- i.e. every shortestPath()/
// allShortestPaths() pattern part compiled so far, across the whole Query
// (not just one Part), for Plan's "more than one shortestPath pattern part
// per query" reject rule.
func countShortestSteps(parts []Part) int {
	n := 0
	for _, p := range parts {
		for _, step := range p.Chains {
			if step.Shortest != ShortestNone {
				n++
			}
		}
	}
	return n
}

// --- Symbol table --------------------------------------------------------

// symKind classifies what a pattern variable's name is bound to, for
// checkExpr's default-deny validation: which operations are legal on it
// mirrors exactly what eval.go's runtime dispatch (Row.Node/Edge/PathVar/
// Scalar) would accept, so Plan never accepts a query eval.go would answer
// with ErrUnsupported at row-evaluation time.
type symKind uint8

const (
	symNode symKind = iota
	symEdge
	// symPath is a named path variable, from either a shortestPath()/
	// allShortestPaths() pattern or an ordinary chain (`MATCH p = ...`).
	// eval.go has no path-value model (see Row.PathVar's doc comment), so a
	// symPath variable is rejected by checkExpr's generic Variable case
	// everywhere; the *only* legal reference is the sole, bare content of a
	// RETURN/WITH projection item, special-cased directly in planReturn.
	symPath
	// symScalar is a plain WITH-carried value: a passed-through node/edge
	// variable's own kind is preserved instead (see planWith) -- symScalar
	// specifically means a WITH COUNT(...) alias or a WITH <literal> AS
	// name constant, both plain float64/string/bool/nil values from here
	// on.
	symScalar
	// symCollectAlias is a WITH COLLECT(sym) AS alias binding: legal *only*
	// as the direct right-hand operand of IN/NOT IN (checkInOperands);
	// referenced any other way, checkExpr's generic Variable case rejects
	// it (the "collect outside pure IN-membership consumption" rule).
	symCollectAlias
)

func cloneKnown(m map[string]symKind) map[string]symKind {
	out := make(map[string]symKind, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// --- Part construction: patterns + WHERE ---------------------------------

// partBuilder accumulates one Part's Chains/Nodes while walking its
// ReadingClauses, and separately validates+pushes down that Part's WHERE
// conjuncts once every pattern has been processed (so WHERE validation sees
// the Part's complete symbol table, regardless of which ReadingClause
// introduced which variable).
type partBuilder struct {
	snap    *snapshot.View
	known   map[string]symKind
	nodes   map[string]*NodeConstraint
	chains  []Step
	anon    int
	regexes map[string]*regexp.Regexp // shared across the whole Query

	// numericScalars names every symbol carried into this Part (via the
	// preceding Part's WithClause, if any) that is statically GUARANTEED to
	// hold a genuine number on every row: a `<literal> AS name` WithConstant
	// whose literal is itself numeric, or a `COUNT(...) AS name`
	// WithAggregate (countAggregate always produces a float64 -- see its own
	// doc). Empty for Part[0] (nothing has been carried into it yet).
	// relationalComparisonSafe consults this so that a bare
	// reference to such a carried alias -- the corpus's own `WITH 60 AS
	// days ... WHERE m.threshold > days` shape -- counts as statically
	// numeric even though, as a bare *cypher.Variable, it carries no
	// numeric-literal AST shape of its own for isStaticallyNumericScalar to
	// see directly.
	numericScalars map[string]bool

	// shortestSteps indexes pb.chains entries produced by a shortestPath/
	// allShortestPaths pattern, for the post-WHERE endpoint-constraint and
	// HasExplicitEndpointInequality finalize pass.
	shortestSteps []int

	// desugaredEqualities accumulates one `sym.k = <literal>` Comparison per
	// key of every inline node-pattern property map encountered while
	// walking this Part's patterns (see addNodePattern/desugarPropertyMap),
	// in pattern-then-sorted-key order. planPart folds these into
	// whereConjuncts -- and therefore into the ordinary checkExpr/pushdown
	// pipeline and ultimately Part.Where -- exactly like any WHERE-clause
	// conjunct the query text wrote directly, so Part.Where stays complete
	// and sufficient on its own; pushdown separately re-derives the
	// redundant NodeConstraint.Predicates copy for these same conjuncts,
	// same as it does for any other single-symbol conjunct.
	desugaredEqualities []cypher.Expression

	// touched is a scratch set, reset before each top-level WHERE conjunct
	// or RETURN/WITH item is checked, recording which known symbols
	// checkExpr's walk actually referenced -- the mechanism pushdown (WHERE
	// conjuncts) and RETURN-item bookkeeping use, so the whole tree only
	// needs to be walked once per item.
	touched map[string]bool
}

// planPart builds one Part from reading (that stage's ReadingClauses),
// seeded with carried (symbols already bound before this Part -- non-empty
// only for the stage after a WITH boundary) and numericScalars (see
// partBuilder.numericScalars' own doc -- likewise non-empty only after a
// WITH boundary). It returns the built Part and this Part's own final
// symbol table (carried forward as-is unless the caller applies a WITH on
// top of it).
func planPart(snap *snapshot.View, regexes map[string]*regexp.Regexp, carried map[string]symKind, numericScalars map[string]bool, reading []*cypher.ReadingClause) (Part, map[string]symKind, bool) {
	pb := &partBuilder{
		snap:           snap,
		known:          cloneKnown(carried),
		nodes:          map[string]*NodeConstraint{},
		regexes:        regexes,
		numericScalars: numericScalars,
	}

	var whereConjuncts []cypher.Expression

	for _, rc := range reading {
		if rc == nil || rc.Unwind != nil || rc.Match == nil {
			return Part{}, nil, false
		}
		m := rc.Match
		if m.Optional {
			return Part{}, nil, false
		}
		if len(m.Pattern) == 0 {
			return Part{}, nil, false
		}
		for _, pp := range m.Pattern {
			if !pb.addPatternPart(pp) {
				return Part{}, nil, false
			}
		}
		if m.Where != nil {
			for _, top := range m.Where.GetAll() {
				whereConjuncts = append(whereConjuncts, flattenTopLevelConjuncts(top)...)
			}
		}
	}

	// Fold every inline-map-desugared equality into the same conjunct list
	// a written-out WHERE clause would populate, so Part.Where ends up
	// complete and sufficient by construction -- see Part's doc.
	whereConjuncts = append(whereConjuncts, pb.desugaredEqualities...)

	for _, conjunct := range whereConjuncts {
		pb.touched = map[string]bool{}
		if !pb.checkExpr(conjunct, true) {
			return Part{}, nil, false
		}
		if !pb.pushdown(conjunct) {
			return Part{}, nil, false
		}
	}

	if !pb.finalizeShortestPaths(whereConjuncts) {
		return Part{}, nil, false
	}

	return Part{
		Chains: pb.chains,
		Nodes:  pb.nodes,
		Where:  rebuildConjunction(whereConjuncts),
	}, pb.known, true
}

// rebuildConjunction ANDs conjuncts back together for storage on Part.Where:
// nil for none, the bare expression for exactly one (avoiding a pointless
// single-element Conjunction wrapper), or a *cypher.Conjunction otherwise.
func rebuildConjunction(conjuncts []cypher.Expression) cypher.Expression {
	switch len(conjuncts) {
	case 0:
		return nil
	case 1:
		return conjuncts[0]
	default:
		return cypher.NewConjunction(conjuncts...)
	}
}

// flattenTopLevelConjuncts expands expr into a flat list of top-level AND
// conjuncts, recursively unwrapping *cypher.Conjunction nodes -- bare, or
// wrapped in a *cypher.Parenthetical -- so a query written with nested ANDs
// and/or parentheses around them produces the same flat list as one written
// without. A Parenthetical wrapping anything else (a Disjunction, a
// Negation, a bare comparison, a KindMatcher, ...) is left exactly as parsed
// and becomes a single conjunct. Ported from recognize.go's
// flattenConjuncts, simplified to never fail: a malformed/nil node just
// becomes (or is dropped from) the flat list, and checkExpr's own nil
// handling rejects it when checked.
func flattenTopLevelConjuncts(expr cypher.Expression) []cypher.Expression {
	switch e := expr.(type) {
	case nil:
		return nil
	case *cypher.Conjunction:
		if e == nil {
			return nil
		}
		var out []cypher.Expression
		for _, sub := range e.GetAll() {
			out = append(out, flattenTopLevelConjuncts(sub)...)
		}
		return out
	case *cypher.Parenthetical:
		if e == nil || e.Expression == nil {
			return nil
		}
		if _, isConj := e.Expression.(*cypher.Conjunction); isConj {
			return flattenTopLevelConjuncts(e.Expression)
		}
		return []cypher.Expression{expr}
	default:
		return []cypher.Expression{expr}
	}
}

// declareSymbol registers sym as kind, or -- if sym is already known within
// this Part -- confirms it was declared with the *same* kind. This is what
// implements "duplicate variable reuse... joins by identity" for node (and
// path) variables: a node pattern variable bound more than once (e.g.
// `(a)-->(b), (b)-->(a)` reusing "a" and "b", or the same node pattern
// appearing in two sequential MATCH clauses) is supported, with the
// executor required to resolve every occurrence to the *same* concrete
// node -- but reusing a name across genuinely different roles (a node
// pattern here, a relationship pattern there) is rejected rather than
// silently picked one way.
//
// This identity-joining reuse is deliberately NOT extended to relationship
// variables: buildStep binds an edge symbol via declareEdgeSymbol instead,
// which rejects any second occurrence outright (this package implements no
// general, name-driven relationship-identity join the way node reuse gets
// -- see buildStep's doc, which also covers the narrower, closing-Step-only
// edge-uniqueness check the executor does implement).
func (pb *partBuilder) declareSymbol(sym string, kind symKind) bool {
	if existing, ok := pb.known[sym]; ok {
		return existing == kind
	}
	pb.known[sym] = kind
	return true
}

// declareEdgeSymbol registers sym as bound to a relationship pattern
// occurrence, rejecting outright if sym already names *anything* (a prior
// edge binding, or a node/path variable of the same name) -- see buildStep's
// doc for why edge symbols do not get declareSymbol's node-friendly
// identity-joining reuse.
func (pb *partBuilder) declareEdgeSymbol(sym string) bool {
	if _, exists := pb.known[sym]; exists {
		return false
	}
	pb.known[sym] = symEdge
	return true
}

func (pb *partBuilder) nodeConstraint(sym string) *NodeConstraint {
	nc, ok := pb.nodes[sym]
	if !ok {
		nc = &NodeConstraint{}
		pb.nodes[sym] = nc
	}
	return nc
}

// symbolFor returns v's own symbol, or synthesizes a fresh one for an
// anonymous pattern element (`(  )`/`-->()`, Variable == nil or ""). "$" can
// never collide with a real Cypher identifier (parameters use it as a
// prefix, but identifiers cannot contain it at all), so these synthetic
// names are safe by construction.
func (pb *partBuilder) symbolFor(v *cypher.Variable) string {
	if v != nil && v.Symbol != "" {
		return v.Symbol
	}
	pb.anon++
	return "$anon" + itoa(pb.anon)
}

// itoa avoids importing strconv purely for this one call site.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// addPatternPart processes one PatternPart (one comma-separated pattern
// within a single MATCH): either a shortestPath()/allShortestPaths() call
// (delegated to addShortestPathPart) or an ordinary node-relationship-node...
// chain of any length, including a bare isolated node pattern (length 1,
// contributing a Nodes entry and no Step).
func (pb *partBuilder) addPatternPart(part *cypher.PatternPart) bool {
	if part == nil || len(part.PatternElements) == 0 {
		return false
	}
	if part.ShortestPathPattern && part.AllShortestPathsPattern {
		return false
	}
	if part.ShortestPathPattern || part.AllShortestPathsPattern {
		return pb.addShortestPathPart(part)
	}
	if len(part.PatternElements)%2 != 1 {
		return false // must alternate node, rel, node, rel, ..., node
	}

	pathSym := ""
	if part.Variable != nil && part.Variable.Symbol != "" {
		pathSym = part.Variable.Symbol
		if !pb.declareSymbol(pathSym, symPath) {
			return false
		}
	}

	firstNode, isNode := part.PatternElements[0].AsNodePattern()
	if !isNode || firstNode == nil {
		return false
	}
	fromSym, ok := pb.addNodePattern(firstNode)
	if !ok {
		return false
	}

	stepsBefore := len(pb.chains)

	for i := 1; i+1 < len(part.PatternElements); i += 2 {
		rel, isRel := part.PatternElements[i].AsRelationshipPattern()
		if !isRel || rel == nil {
			return false
		}
		toNode, isNode := part.PatternElements[i+1].AsNodePattern()
		if !isNode || toNode == nil {
			return false
		}
		toSym, ok := pb.addNodePattern(toNode)
		if !ok {
			return false
		}
		step, ok := pb.buildStep(fromSym, toSym, rel, ShortestNone, pathSym)
		if !ok {
			return false
		}
		pb.chains = append(pb.chains, step)
		fromSym = toSym
	}

	if pathSym != "" && len(pb.chains) == stepsBefore {
		// A named path over a single isolated node (`MATCH p = (n)`) has no
		// Step to carry PathSym, so a future executor would have nothing to
		// build a PathVal from. Not a shape any required corpus/user query
		// needs; reject rather than guess.
		return false
	}

	return true
}

// addShortestPathPart processes a shortestPath()/allShortestPaths()
// PatternPart, which -- mirroring the existing recognize.go recognizer's own
// hard assumption, itself mirroring real Cypher grammar -- must be exactly
// node-relationship-node (3 elements): shortestPath cannot span more than
// one relationship pattern.
func (pb *partBuilder) addShortestPathPart(part *cypher.PatternPart) bool {
	if len(part.PatternElements) != 3 {
		return false
	}
	firstNode, isNode := part.PatternElements[0].AsNodePattern()
	if !isNode || firstNode == nil {
		return false
	}
	rel, isRel := part.PatternElements[1].AsRelationshipPattern()
	if !isRel || rel == nil {
		return false
	}
	secondNode, isNode := part.PatternElements[2].AsNodePattern()
	if !isNode || secondNode == nil {
		return false
	}

	aSym, ok := pb.addNodePattern(firstNode)
	if !ok {
		return false
	}
	bSym, ok := pb.addNodePattern(secondNode)
	if !ok {
		return false
	}

	mode := ShortestOne
	if part.AllShortestPathsPattern {
		mode = ShortestAll
	}

	pathSym := ""
	if part.Variable != nil && part.Variable.Symbol != "" {
		pathSym = part.Variable.Symbol
		if !pb.declareSymbol(pathSym, symPath) {
			return false
		}
	}

	step, ok := pb.buildStep(aSym, bSym, rel, mode, pathSym)
	if !ok {
		return false
	}
	pb.chains = append(pb.chains, step)
	pb.shortestSteps = append(pb.shortestSteps, len(pb.chains)-1)
	return true
}

// addNodePattern registers np's variable (or a fresh anonymous symbol) as a
// node, merges its kind labels into that symbol's NodeConstraint (an
// unresolvable kind name rejects the whole query), and -- if np carries an
// inline property map -- desugars it into `sym.k = <literal>` equality AST
// nodes in sorted-key order (the controller's pinned semantics:
// byte-identical equality, same string-type guard StringEq already applies
// at eval time), queued on pb.desugaredEqualities for planPart to fold into
// the ordinary WHERE-conjunct pipeline (and therefore into Part.Where, the
// completeness contract Part's doc describes -- NOT appended to
// NodeConstraint.Predicates directly here; pushdown does that redundantly,
// same as for any other single-symbol WHERE conjunct, once these equalities
// have been merged into whereConjuncts). An inline map keyed by a
// $parameter, or whose *values* are anything but literals, rejects
// (parameters anywhere; non-literal map values are not a
// compile-time-knowable equality and are out of the matrix).
func (pb *partBuilder) addNodePattern(np *cypher.NodePattern) (string, bool) {
	sym := pb.symbolFor(np.Variable)
	if !pb.declareSymbol(sym, symNode) {
		return "", false
	}
	nc := pb.nodeConstraint(sym)
	for _, k := range np.Kinds {
		id, ok := pb.snap.Kinds().ID(k.String())
		if !ok {
			return "", false
		}
		nc.Kinds = append(nc.Kinds, id)
	}
	if np.Properties != nil {
		props, ok := np.Properties.(*cypher.Properties)
		if !ok || props == nil || props.Parameter != nil {
			return "", false
		}
		preds, ok := desugarPropertyMap(sym, props.Map)
		if !ok {
			return "", false
		}
		pb.desugaredEqualities = append(pb.desugaredEqualities, preds...)
	}
	return sym, true
}

// desugarPropertyMap converts an inline `{k: v, ...}` node-pattern map into
// one `sym.k = v` equality *cypher.Comparison AST node per key, in ascending
// key order -- mirroring exactly the shape the dawgs frontend itself builds
// for a written-out `WHERE sym.k = v` conjunct (a *cypher.PropertyLookup
// over a *cypher.Variable, compared to a *cypher.Literal), so checkExpr's
// validation and pushdown's pattern-matching (extractIDAnchor,
// extractObjectIDAnchor, ...) treat a desugared equality identically to one
// the query text wrote out by hand.
func desugarPropertyMap(sym string, m cypher.MapLiteral) ([]cypher.Expression, bool) {
	if len(m) == 0 {
		return nil, true
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]cypher.Expression, 0, len(keys))
	for _, k := range keys {
		lit, ok := asLiteral(m[k])
		if !ok || lit == nil {
			return nil, false
		}
		out = append(out, &cypher.Comparison{
			Left: &cypher.PropertyLookup{Atom: &cypher.Variable{Symbol: sym}, Symbol: k},
			Partials: []*cypher.PartialComparison{
				{Operator: cypher.OperatorEquals, Right: lit},
			},
		})
	}
	return out, true
}

// buildStep compiles one relationship pattern into a Step, normalizing
// direction so the executor always walks the *declared* traversal
// direction rather than reasoning about arrow orientation itself: an
// inbound arrow (`<-...-`) swaps fromSym/toSym and becomes
// DirectionOutbound; DirectionBoth (a fully undirected `-...-` pattern) is
// only accepted on a fixed-length step (rng == nil) -- an undirected
// variable-length pattern is rejected, mirroring dawgs' own translate-time
// rejection ("unsupported expansion direction").
//
// rel.Properties (an inline map or $parameter on the relationship itself)
// always rejects: edge property access/predicates are out of the matrix
// entirely, and the snapshot has no edge property store to check an inline
// map against even if it were desugared the way node patterns are.
//
// A non-anonymous edge symbol may bind exactly one Step: unlike a node
// variable (see declareSymbol's doc on identity-joining node reuse), this
// package does not implement Cypher's relationship-uniqueness semantics as a
// general, name-driven join the way node-identity reuse gets (the executor
// tracks, per row, only the narrow "has this exact edge already been used
// by some earlier Step in this pattern" fact that a *cyclic* pattern's
// closing-Step verification needs -- exec.go's Row.usedEdges/edgeUsed and
// verifyClosingStep's own doc -- not a general "these two named occurrences
// must resolve to the same edge" identity join), so a second pattern
// occurrence of the same relationship variable name -- whether in another
// step of the same chain or in an entirely different pattern part --
// rejects outright rather than silently
// picking one occurrence or the other. This is a strict narrowing of
// declareSymbol's general "same kind => same identity, allow it" rule,
// deliberately bypassed here for edges specifically.
func (pb *partBuilder) buildStep(fromSym, toSym string, rel *cypher.RelationshipPattern, shortest ShortestMode, pathSym string) (Step, bool) {
	if rel.Properties != nil {
		return Step{}, false
	}

	edgeSym := ""
	if rel.Variable != nil {
		edgeSym = rel.Variable.Symbol
	}
	if edgeSym != "" {
		if !pb.declareEdgeSymbol(edgeSym) {
			return Step{}, false
		}
	}

	var kinds []snapshot.KindID
	for _, k := range rel.Kinds {
		id, ok := pb.snap.Kinds().ID(k.String())
		if !ok {
			return Step{}, false
		}
		kinds = append(kinds, id)
	}

	var rng *Range
	if rel.Range != nil {
		min := 1
		if rel.Range.StartIndex != nil {
			min = int(*rel.Range.StartIndex)
		}
		max := MaxExpansionDepth
		if rel.Range.EndIndex != nil {
			max = int(*rel.Range.EndIndex)
		}
		if min < 0 || max < 0 {
			return Step{}, false
		}
		rng = &Range{Min: min, Max: max}
	}

	// shortestPath()/allShortestPaths() over a variable-length range is
	// accepted only for the plain, unbounded-hop-count case (min exactly 1
	// -- "as many hops as it takes, at least one"). Two narrower shapes are
	// rejected here as a conservative tightening, both because the corpus
	// never needs them and because the path-engine design this planner sits
	// on top of only reasons about shortestPath's minimum as "1 or the
	// zero-length special case", never an arbitrary floor:
	//   - Range.Min > 1 (`*3..5` inside shortestPath): shortestPath already
	//     finds the globally shortest trail; requiring it to additionally be
	//     at least N>1 hops long is a shape no served query needs and one
	//     this planner declines rather than risk misinterpreting.
	//   - Range.Min == 0 (`*0..`, zero-length): a zero-length shortestPath
	//     match (source == target, an empty path) is a degenerate case this
	//     planner has not verified pg/dawgs' own shortestPath translation
	//     handles the same way a plain var-length `*0..` chain does -- see
	//     Plan's doc on the corpus's standalone (non-shortestPath) `*0..`
	//     acceptance, which is unaffected by this rule.
	// A plain (non-shortestPath) var-length pattern keeps its existing,
	// unrestricted Min/Max acceptance -- this check applies only when
	// shortest != ShortestNone.
	if shortest != ShortestNone && rng != nil && rng.Min != 1 {
		return Step{}, false
	}

	direction := rel.Direction
	if rng != nil && direction == graph.DirectionBoth {
		return Step{}, false
	}

	reversed := false
	switch direction {
	case graph.DirectionInbound:
		fromSym, toSym = toSym, fromSym
		direction = graph.DirectionOutbound
		reversed = true
	case graph.DirectionOutbound, graph.DirectionBoth:
		// already the executor's expected shape
	default:
		return Step{}, false
	}

	return Step{
		FromSym: fromSym, EdgeSym: edgeSym, ToSym: toSym,
		Direction: direction, EdgeKinds: kinds, Range: rng,
		Shortest: shortest, PathSym: pathSym, Reversed: reversed,
	}, true
}

// finalizeShortestPaths runs after every pattern and WHERE conjunct in the
// Part has been processed: for every shortestPath/allShortestPaths Step it
// sets HasExplicitEndpointInequality and enforces the endpoint-constraint
// rule (see its own doc below), plus the shortest-step mixing restriction
// (see shortestStepsAreIsolated).
func (pb *partBuilder) finalizeShortestPaths(whereConjuncts []cypher.Expression) bool {
	if !pb.shortestStepsAreIsolated() {
		return false
	}
	for _, idx := range pb.shortestSteps {
		step := &pb.chains[idx]
		step.HasExplicitEndpointInequality = hasEndpointInequality(whereConjuncts, step.FromSym, step.ToSym)

		if !pb.isConstrained(step.FromSym) && !pb.isConstrained(step.ToSym) {
			return false
		}
	}
	return true
}

// shortestStepsAreIsolated reports whether every shortestPath()/
// allShortestPaths() Step recorded so far (pb.shortestSteps) shares no
// pattern-connectivity component -- transitively, via Step FromSym/ToSym,
// exactly like exec.go's own groupComponents union-find -- with any OTHER
// Step in this Part.
//
// addShortestPathPart already restricts a shortestPath()/allShortestPaths()
// call itself to a single, exactly-3-element PatternPart (it cannot span
// more than one relationship), but Cypher's comma-separated pattern list can
// still legally write it alongside an ordinary chain that happens to reuse
// one of its own endpoint symbols (e.g. `MATCH (a)-->(b),
// shortestPath((b)-[*1..]->(c))`), pulling both into the SAME connected
// component the executor would compute. The chain executor
// (expandChainComponent) and the standalone shortestPath executor
// (expandShortestPathComponent) are mutually exclusive: a shortestPath Step
// resolves both endpoints as complete, pre-adjacency node sets before ever
// touching adjacency, and has no way to additionally honor a further chain
// hanging off either endpoint. Rather than let such a query plan
// successfully only to have the executor discover the same fact after a
// full anchor scan (runComponent's own len(stepIdxs) != 1 decline), this
// rejects the shape at plan time.
func (pb *partBuilder) shortestStepsAreIsolated() bool {
	if len(pb.shortestSteps) == 0 {
		return true
	}

	parent := map[string]string{}
	var find func(string) string
	find = func(s string) string {
		if _, ok := parent[s]; !ok {
			parent[s] = s
			return s
		}
		if parent[s] != s {
			parent[s] = find(parent[s])
		}
		return parent[s]
	}
	union := func(a, b string) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[ra] = rb
		}
	}
	for i := range pb.chains {
		union(pb.chains[i].FromSym, pb.chains[i].ToSym)
	}

	for _, idx := range pb.shortestSteps {
		root := find(pb.chains[idx].FromSym)
		for j := range pb.chains {
			if j == idx {
				continue
			}
			if find(pb.chains[j].FromSym) == root {
				return false
			}
		}
	}
	return true
}

// isConstrained reports whether sym's NodeConstraint carries a kind label,
// an id() anchor, an objectid anchor, or at least one pushed single-symbol
// WHERE predicate.
//
// The original implementation of finalizeShortestPaths' "at least one
// endpoint constrained" rule (see that function's doc) checked only
// Kinds/IDs/ObjectIDAnchor, a literal reading of "kind- or
// id-constrained". Predicates was added here after that reading was found
// to exclude a real, required corpus query for no correctness reason: agi.json's
// "Shortest paths to Tier Zero / High Value targets" is
// `shortestPath((s)-[:...*1..]->(t)) WHERE COALESCE(t.system_tags, ”)
// CONTAINS 'admin_tier_0' AND s<>t` -- t carries no kind label at all (the
// AGI corpus convention tags Tier Zero via a property, not a label; agt's
// sibling query uses `t:Tag_Tier_Zero` instead and already satisfied the
// original rule), so isConstrained(t) was false, and s is genuinely bare
// (matching the "at least one" rule's own point 1 precedent for a bare `s`
// with a kind-constrained far side), so isConstrained(s) was false too:
// finalizeShortestPaths declined a query the executor can serve correctly.
// pushdown (above) already records this exact WHERE conjunct into
// nc.Predicates precisely because it touches only t, and
// resolveEndpointSet (expand.go) already evaluates every entry in
// Predicates per full-scan candidate when resolving a shortestPath
// endpoint set -- the same mechanism a kind- or id-constrained symbol
// relies on, just with scanAnchor's full-scan candidate source instead of
// its kind-bitmap or id-anchor ones. A predicate-only endpoint is
// therefore exactly as "narrowable" as a kind- or id-constrained one from
// finalizeShortestPaths' own point of view (guaranteeing at least one side
// has *some* narrowing information, so a shortestPath with literally zero
// information about either endpoint never plans); the difference is only
// which scanAnchor candidate source and how much work resolving it costs,
// which the executor's existing budget accounting already guards.
func (pb *partBuilder) isConstrained(sym string) bool {
	nc, ok := pb.nodes[sym]
	return ok && (len(nc.Kinds) > 0 || len(nc.IDs) > 0 || nc.ObjectIDAnchor != nil || len(nc.Predicates) > 0)
}

// hasEndpointInequality scans conjuncts for `a<>b`/`b<>a` or
// `id(a)<>id(b)`/`id(b)<>id(a)`.
func hasEndpointInequality(conjuncts []cypher.Expression, a, b string) bool {
	for _, c := range conjuncts {
		if variableInequality(c, a, b) || idInequality(c, a, b) {
			return true
		}
	}
	return false
}

// asSingleComparison unwraps expr to a *cypher.Comparison with exactly one
// partial, returning its operator and both operands; ok is false for
// anything else (nil, more than one partial, a nil partial).
func asSingleComparison(expr cypher.Expression) (op cypher.Operator, left, right cypher.Expression, ok bool) {
	cmp, isCmp := unwrapParens(expr).(*cypher.Comparison)
	if !isCmp || cmp == nil || len(cmp.Partials) != 1 || cmp.Partials[0] == nil {
		return "", nil, nil, false
	}
	return cmp.Partials[0].Operator, cmp.Left, cmp.Partials[0].Right, true
}

func variableInequality(expr cypher.Expression, a, b string) bool {
	op, left, right, ok := asSingleComparison(expr)
	if !ok || op != cypher.OperatorNotEquals {
		return false
	}
	lv, lok := unwrapParens(left).(*cypher.Variable)
	rv, rok := unwrapParens(right).(*cypher.Variable)
	if !lok || !rok || lv == nil || rv == nil {
		return false
	}
	return (lv.Symbol == a && rv.Symbol == b) || (lv.Symbol == b && rv.Symbol == a)
}

func idInequality(expr cypher.Expression, a, b string) bool {
	op, left, right, ok := asSingleComparison(expr)
	if !ok || op != cypher.OperatorNotEquals {
		return false
	}
	lv, lok := idFunctionArg(left)
	rv, rok := idFunctionArg(right)
	if !lok || !rok {
		return false
	}
	return (lv == a && rv == b) || (lv == b && rv == a)
}

// idFunctionArg recognizes expr as `id(<var>)`, returning the variable's
// symbol.
func idFunctionArg(expr cypher.Expression) (string, bool) {
	fi, ok := unwrapParens(expr).(*cypher.FunctionInvocation)
	if !ok || fi == nil || strings.ToLower(fi.Name) != cypher.IdentityFunction {
		return "", false
	}
	v, ok := singleVariableArg(fi)
	if !ok {
		return "", false
	}
	return v.Symbol, true
}

// pushdown records a validated, already-touched-tracked WHERE conjunct into
// the single node it exclusively references (if any -- most conjuncts
// reference more than one symbol and are simply left where they are, in
// Part.Where alone), and additionally attempts the id()/objectid anchor
// extractions. It returns false only for a genuine reject: two different
// literal id() anchors for the same symbol (see extractIDAnchor) -- pg
// would happily AND two disjoint point-equalities into an always-empty
// result, but this package declines rather than risk silently serving an
// empty answer a subtly different comparison should not have produced.
func (pb *partBuilder) pushdown(conjunct cypher.Expression) bool {
	if len(pb.touched) != 1 {
		return true
	}
	var sym string
	for s := range pb.touched {
		sym = s
	}
	if pb.known[sym] != symNode {
		return true
	}

	nc := pb.nodeConstraint(sym)
	nc.Predicates = append(nc.Predicates, conjunct)

	if !pb.extractIDAnchor(sym, conjunct) {
		return false
	}
	pb.extractObjectIDAnchor(sym, conjunct)
	return true
}

// extractIDAnchor recognizes `id(sym) = <literal>` (either operand order)
// and folds it into sym's NodeConstraint.IDs. A repeated identical value is
// a harmless duplicate (deduped); a second, *different* literal value
// rejects the whole query -- see the "duplicate conflicting id() anchors"
// reject rule and pushdown's doc.
func (pb *partBuilder) extractIDAnchor(sym string, conjunct cypher.Expression) bool {
	op, left, right, ok := asSingleComparison(conjunct)
	if !ok || op != cypher.OperatorEquals {
		return true
	}

	id, found := idEqualsLiteral(left, right, sym)
	if !found {
		id, found = idEqualsLiteral(right, left, sym)
	}
	if !found {
		return true
	}

	nc := pb.nodeConstraint(sym)
	for _, existing := range nc.IDs {
		if existing == id {
			return true
		}
	}
	if len(nc.IDs) > 0 {
		return false
	}
	nc.IDs = append(nc.IDs, id)
	return true
}

// idEqualsLiteral recognizes `id(<var symbol sym>) = <int literal>` in one
// fixed operand orientation (idSide, litSide).
func idEqualsLiteral(idSide, litSide cypher.Expression, sym string) (uint64, bool) {
	s, ok := idFunctionArg(idSide)
	if !ok || s != sym {
		return 0, false
	}
	lit, ok := asLiteral(litSide)
	if !ok || lit == nil || lit.Null {
		return 0, false
	}
	switch v := lit.Value.(type) {
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case uint64:
		return v, true
	default:
		return 0, false
	}
}

// extractObjectIDAnchor recognizes `sym.objectid = <string literal>`
// (either operand order) and sets sym's NodeConstraint.ObjectIDAnchor.
func (pb *partBuilder) extractObjectIDAnchor(sym string, conjunct cypher.Expression) {
	op, left, right, ok := asSingleComparison(conjunct)
	if !ok || op != cypher.OperatorEquals {
		return
	}
	if v, ok := objectIDEqualsLiteral(left, right, sym); ok {
		pb.nodeConstraint(sym).ObjectIDAnchor = &v
		return
	}
	if v, ok := objectIDEqualsLiteral(right, left, sym); ok {
		pb.nodeConstraint(sym).ObjectIDAnchor = &v
	}
}

func objectIDEqualsLiteral(propSide, litSide cypher.Expression, sym string) (string, bool) {
	pl, ok := unwrapParens(propSide).(*cypher.PropertyLookup)
	if !ok || pl == nil || pl.Symbol != "objectid" {
		return "", false
	}
	v, ok := unwrapParens(pl.Atom).(*cypher.Variable)
	if !ok || v == nil || v.Symbol != sym {
		return "", false
	}
	lit, ok := asLiteral(litSide)
	if !ok || lit == nil || lit.Null {
		return "", false
	}
	raw, ok := lit.Value.(string)
	if !ok {
		return "", false
	}
	decoded, err := decodeCypherStringLiteral(raw)
	if err != nil {
		return "", false
	}
	return decoded, true
}

// --- Expression validation (default-deny) --------------------------------

// checkExpr is the default-deny validator shared by WHERE conjuncts and
// RETURN/WITH item expressions: it walks expr and returns true only if
// every node in its subtree is one this evaluator (eval.go) can compute at
// row-evaluation time without ever hitting ErrUnsupported, AND every
// resolvable static fact (a kind name, a regex pattern, a referenced
// symbol) actually resolves against snap/pb.known. Anything this switch has
// no case for -- a pattern predicate, a quantifier (ALL/ANY/NONE/SINGLE), a
// list/pattern comprehension, a map literal used as a bare expression, a
// FilterExpression/IDInCollection, or any other AST shape -- falls through
// to the implicit default-deny (no case matches, `default: return false`).
//
// predicatePosition tracks whether expr is reachable, in the executor, via
// EvalPredicate/evalWhereWithMembership's own boolean-structural recursion
// (true) or only via EvalValue (false) -- see checkInOperands' own doc
// comment for why this distinction is load-bearing for the one place a
// CollectMembership alias may legally appear. It starts true only for a
// Part's own top-level WHERE conjunct (planPart's call below) and threads
// unchanged through exactly the AST shapes evalWhereWithMembership's own
// recursion also passes through unmodified -- Parenthetical, Negation,
// Conjunction, Disjunction, ExclusiveDisjunction -- becoming false the
// moment expr is validated as a mere *value* operand of something else (a
// Comparison's own operands, a function argument, an arithmetic operand, a
// list element, a RETURN/WITH item): eval.go's EvalValue has no case for a
// *cypher.Comparison at all (it hits ErrUnsupported), so any Comparison
// nested in one of those positions can never be reached by
// EvalPredicate/evalWhereWithMembership's structural walk, regardless of
// its own shape.
//
// As a side effect, every *cypher.Variable node checkExpr accepts is
// recorded into pb.touched (reset by the caller before each top-level
// item), which pushdown/RETURN-item bookkeeping then reads to learn which
// symbols the just-validated expression referenced.
func (pb *partBuilder) checkExpr(expr cypher.Expression, predicatePosition bool) bool {
	switch e := expr.(type) {
	case nil:
		return true

	case *cypher.Parenthetical:
		return e != nil && pb.checkExpr(e.Expression, predicatePosition)

	case *cypher.Negation:
		return e != nil && pb.checkExpr(e.Expression, predicatePosition)

	case *cypher.Conjunction:
		return e != nil && pb.checkExprList(e.GetAll(), predicatePosition)

	case *cypher.Disjunction:
		return e != nil && pb.checkExprList(e.GetAll(), predicatePosition)

	case *cypher.ExclusiveDisjunction:
		return e != nil && pb.checkExprList(e.GetAll(), predicatePosition)

	case *cypher.Comparison:
		return pb.checkComparison(e, predicatePosition)

	case *cypher.KindMatcher:
		return pb.checkKindMatcher(e)

	case *cypher.PatternPredicate:
		return pb.checkPatternPredicate(e)

	case *cypher.Variable:
		if e == nil {
			return false
		}
		k, known := pb.known[e.Symbol]
		if !known || k == symCollectAlias || k == symPath {
			return false
		}
		pb.touched[e.Symbol] = true
		return true

	case *cypher.Literal:
		return e != nil && checkLiteralShape(e)

	case *cypher.Parameter:
		return false

	case *cypher.PropertyLookup:
		return pb.checkPropertyLookup(e)

	case *cypher.ListLiteral:
		// A list's own elements are always value operands (`[a, b, c]` has
		// no predicate position inside it), regardless of predicatePosition
		// at the ListLiteral itself.
		return e != nil && pb.checkExprList(*e, false)

	case *cypher.FunctionInvocation:
		return pb.checkFunction(e)

	case *cypher.ArithmeticExpression:
		return pb.checkArithmetic(e)

	case *cypher.UnaryAddOrSubtractExpression:
		// An arithmetic operand is always a value position.
		return e != nil && pb.checkExpr(e.Right, false)

	default:
		return false
	}
}

func (pb *partBuilder) checkExprList(exprs []cypher.Expression, predicatePosition bool) bool {
	for _, e := range exprs {
		if !pb.checkExpr(e, predicatePosition) {
			return false
		}
	}
	return true
}

func checkLiteralShape(lit *cypher.Literal) bool {
	if lit.Null {
		return true
	}
	switch lit.Value.(type) {
	case string, int64, uint64, float64, bool:
		return true
	default:
		return false
	}
}

// checkComparison validates a (possibly chained) Comparison, mirroring
// eval.go's evalComparison chaining (`a op1 b op2 c` == `(a op1 b) AND (b
// op2 c)`). Every operator eval.go's evalPartialComparison dispatches on is
// accepted here; anything else (there are none left unhandled today, but
// default-deny means a future dawgs operator constant this file has not
// been updated for rejects automatically rather than silently mis-checking
// it) rejects.
//
// # Fail-safe reject: bare property lookup vs. a non-bare scalar literal, `=`/`<>` only
//
// dawgs' own pgsql translator (rewritePropertyLookupOperands,
// cypher/models/pgsql/translate/expression.go:319, dawgs@v0.8.0) lowers a
// direct `n.prop = <expr>`/`n.prop <> <expr>` two different ways depending
// purely on the other operand's own Cypher AST shape, confirmed by dumping
// translate.Translate's generated SQL:
//
//	n.x = 5        -> ((properties -> 'x'))::jsonb = to_jsonb((5)::int8)::jsonb   -- native jsonb equality
//	n.x = -5       -> ((properties ->> 'x'))::int8 = - 5                         -- text-extract-then-cast
//	n.x = +5       -> ((properties ->> 'x'))::int8 = + 5                         -- cast (either sign)
//	n.x = -(-5)    -> ((properties ->> 'x'))::int8 = - (- 5)                     -- cast (any nesting)
//	n.x = (5)      -> ((properties ->> 'x'))::int8 = (5)                         -- cast (bare parens too)
//	n.x = 5 + 0    -> ((properties ->> 'x'))::int8 = 5 + 0                       -- cast (genuine arithmetic)
//
// A follow-up probe found the identical split for a STRING operand,
// confirmed the same way:
//
//	n.x = '5'      -> ((properties -> 'x'))::jsonb = to_jsonb(('5')::text)::jsonb -- native jsonb equality
//	n.x = ('5')    -> ((properties ->> 'x'))::text = ('5')                        -- cast (bare parens)
//	n.x = '5'+''   -> ((properties ->> 'x'))::text = '5' || ''                    -- cast (concatenation)
//
// Only a bare `cypher.Literal` (int64/uint64/float64/bool/string) takes the
// native-jsonb route, via rewriteJSONScalarEqualityOperand: that rewrite
// fires solely when the pgsql-translated operand directly implements
// pgsql.TypeHinted with a JSON-scalar-equality type (Boolean, Int*,
// Float4/8, Numeric, Text) -- true only of the pgsql.Literal a bare
// cypher.Literal translates to. EVERY wrapping this package's own checkExpr
// admits around a scalar value -- a Parenthetical (even just `(5)`/`('5')`,
// no sign at all), a UnaryAddOrSubtractExpression of either sign at any
// nesting depth, or a genuine multi-term ArithmeticExpression (parenthesized
// or not, `+` doubling as string concatenation for Text operands) -- lowers
// to a pgsql.Parenthetical/UnaryExpression/BinaryExpression instead, none of
// which implement pgsql.TypeHinted, so the rewrite silently declines and
// the comparison falls through to the identical cast route a bare negative
// literal takes -- purely an artifact of the operand's own AST shape, not
// its sign or type, verified directly for every row in both tables above
// (an earlier version of this reject, isNegativeNumberLiteral, only matched
// the second numeric row -- a single UnaryAddOrSubtractExpression("-")
// wrapping a bare literal -- missing every other row here; a later version,
// isBareScalarLiteral, covered every numeric/bool row but missed the whole
// string table above it, closed by extending it to string literals too --
// both gaps found the same way: dumping translate.Translate's generated SQL
// for each AST shape and comparing it against the shape actually served).
//
// The two routes disagree on a property that is *present* but an explicit
// JSON null: native jsonb equality treats it as a comparable, non-null
// value (`<>` sees it as a definite "not equal"), while the cast route's
// `->>` extraction yields SQL NULL for it, same as a missing property (`<>`
// nulls out, dropping the row) -- and a cast additionally risks a genuine
// PostgreSQL runtime cast error the in-memory evaluator has no way to
// reproduce, for any heterogeneous-typed jsonb column (an int-shaped cast
// failing outright on a float-valued row). This project's own evaluator
// (eval.go's asLiteral/evalEquality) only recognizes a bare `cypher.Literal`
// -- after peeling Parentheticals only, never Unary/Arithmetic -- as "the
// literal side" in the first place, so for any of these wrapped shapes it
// either treats it as if the native-jsonb route applied (via asLiteral's own
// Parenthetical-unwrapping, e.g. for `(5)`) or falls through to a wholly
// different comparison path (for `+5`/`-(-5)`/`5 + 0`, none of which
// asLiteral recognizes at all) -- neither of which is guaranteed to match
// pg's actual cast-route answer. This reject makes eval.go's own routing
// choice moot for every shape it covers: Plan() declines the whole query
// before Execute() ever runs, so asLiteral is never reached with any of
// these operands paired against a bare property lookup. See eval.go's
// evalLiteralComparison doc for the evaluator-side derivation. This was
// originally discovered as `n.val <> -100.0` failing the random
// differential suite, worked around at the time by restricting that
// suite's own literal generation -- this reject supersedes that workaround,
// see the generator's own history.
//
// `<`/`<=`/`>`/`>=` are unaffected by any of this -- the same translator
// function's default case always takes the cast route for those operators
// regardless of the other operand's shape (confirmed the same way: `n.x <
// 5`, `n.x < -5`, `n.x < +5`, and `n.x < (5)` all produce the identical
// `((properties ->> 'x'))::int8 <op> ...` shape) -- matching what this
// evaluator's evalOrder/OrderCompare already does, so they stay accepted
// unconditionally.
//
// Wrapping the property lookup in a function call (coalesce()/size()/...)
// takes it out of scope too: the translator's rewrite only ever fires when
// the comparison's *immediate* operand (LOperand/ROperand of the binary
// expression) is itself a raw property-lookup binary expression
// (expressionToPropertyLookupBinaryExpression, property.go:15) -- a
// FunctionCall wrapping it never matches that shape, so
// hasLeftPropertyLookup/hasRightPropertyLookup is false and the whole
// native-jsonb-vs-cast branch never runs, for any shape on the other side.
// Verified directly: `coalesce(n.x, 0) = -5` and `coalesce(n.x, 0) = 5` both
// compile to the identical `coalesce(((properties ->> 'x'))::int8, 0)::int8
// = ±5`; `size(n.tags) = -5` and `= 5` both compile to the identical
// `jsonb_array_length((properties -> 'tags'))::int = ±5`. So only a truly
// *bare* property lookup (isBarePropertyLookup below, after unwrapping
// Parentheticals) needs this reject -- a function-wrapped one is left to
// the ordinary checkFunction/checkExpr path, unaffected. Two property
// lookups compared directly (`n.x = n.y`) are unaffected too:
// rewritePropertyLookupOperands special-cases hasLeftPropertyLookup &&
// hasRightPropertyLookup ahead of the single-operand branch above, always
// rewriting both to native jsonb regardless of either side's shape.
func (pb *partBuilder) checkComparison(cmp *cypher.Comparison, predicatePosition bool) bool {
	if cmp == nil || len(cmp.Partials) == 0 {
		return false
	}
	// singlePartial, together with predicatePosition, gates checkInOperands'
	// CollectMembership bypass: that bypass is only sound for a comparison
	// consisting of exactly this one IN partial (Plan's own IR truly is
	// `<nodeVar> IN <alias>` and nothing else), never for a chained
	// comparison (`a op1 b op2 c`, Cypher sugar for `(a op1 b) AND (b op2
	// c)`) that merely has the membership shape as one of several partials
	// -- see checkInOperands' own doc comment.
	singlePartial := len(cmp.Partials) == 1
	left := cmp.Left
	for _, partial := range cmp.Partials {
		if partial == nil {
			return false
		}
		switch partial.Operator {
		case cypher.OperatorIn:
			if !pb.checkInOperands(left, partial.Right, predicatePosition && singlePartial) {
				return false
			}
		case cypher.OperatorRegexMatch:
			if !pb.checkRegexOperands(left, partial.Right) {
				return false
			}
		case cypher.OperatorEquals, cypher.OperatorNotEquals:
			// Every non-IN/regex operator's operands are plain value
			// positions -- eval.go's evalPartialComparison calls EvalValue
			// on both sides regardless of predicatePosition here, so a
			// nested Comparison in either operand (e.g. the membership
			// shape used as a value, `x = c IN exclude`) can never be
			// reached by EvalPredicate's structural recursion.
			if !pb.checkExpr(left, false) || !pb.checkExpr(partial.Right, false) {
				return false
			}
			// Fail-safe reject: see this function's own doc comment for the
			// full derivation -- a bare property lookup compared against a
			// non-bare scalar literal via `=`/`<>`, on either side, always
			// delegates rather than risk this evaluator disagreeing with
			// dawgs' own AST-shape-dependent translation.
			if (isBarePropertyLookup(left) && isNonBareScalarLiteral(partial.Right)) ||
				(isBarePropertyLookup(partial.Right) && isNonBareScalarLiteral(left)) {
				return false
			}
		case cypher.OperatorLessThan, cypher.OperatorLessThanOrEqualTo,
			cypher.OperatorGreaterThan, cypher.OperatorGreaterThanOrEqualTo:
			// Every non-IN/regex operator's operands are plain value
			// positions -- eval.go's evalPartialComparison calls EvalValue
			// on both sides regardless of predicatePosition here, so a
			// nested Comparison in either operand (e.g. the membership
			// shape used as a value, `x = c IN exclude`) can never be
			// reached by EvalPredicate's structural recursion.
			if !pb.checkExpr(left, false) || !pb.checkExpr(partial.Right, false) {
				return false
			}
			// Fail-safe reject: see relationalComparisonSafe's own doc for
			// the full derivation -- unlike `=`/`<>`, dawgs' translator has
			// no bare-literal-only native-jsonb path for `<`/`<=`/`>`/`>=`
			// at all; it ALWAYS casts, and the two shapes that cast
			// unsafely relative to this evaluator (a property compared
			// against another property, and any coalesce()/arithmetic
			// wrapping around a property operand) must delegate instead.
			if !pb.relationalComparisonSafe(left, partial.Right) {
				return false
			}
		case cypher.OperatorStartsWith, cypher.OperatorEndsWith, cypher.OperatorContains,
			cypher.OperatorIs, cypher.OperatorIsNot:
			// Every non-IN/regex operator's operands are plain value
			// positions -- eval.go's evalPartialComparison calls EvalValue
			// on both sides regardless of predicatePosition here, so a
			// nested Comparison in either operand (e.g. the membership
			// shape used as a value, `x = c IN exclude`) can never be
			// reached by EvalPredicate's structural recursion.
			if !pb.checkExpr(left, false) || !pb.checkExpr(partial.Right, false) {
				return false
			}
		default:
			return false
		}
		left = partial.Right
	}
	return true
}

// relationalComparisonSafe decides whether a `<`/`<=`/`>`/`>=` comparison
// between left and right is safe for this evaluator to serve, or must
// delegate.
//
// Unlike `=`/`<>` (checkComparison's own doc: a bare-literal-only
// native-jsonb path, everything else falling to a cast), dawgs' translator
// has NO bare-literal special case for the relational operators at all --
// confirmed directly against translate.Translate's generated SQL, every
// shape takes a cast: `n.x < 5`, `n.x < -5`, `n.x < +5`, and `n.x < (5)` all
// produce the identical `((properties ->> 'x'))::int8 < ...` shape. Two
// distinct ways that cast disagrees with this package's own runtime
// comparison (eval.go's OrderCompare, which requires both operands to
// already be like-typed float64s or bails ErrNotComparable/ErrCollation)
// were confirmed:
//
//   - A property compared against ANOTHER property (`s.x < s.y`): with no
//     literal on either side to hint a cast type from, dawgs falls back to
//     comparing both sides as raw jsonb via PostgreSQL's own jsonb `<`
//     operator -- ordered by jsonb's type-rank system (the identical
//     Null/String/Number/Bool/Array/Object ranking behind planOrder's own
//     ORDER BY divergence note), not by OrderCompare's structural numeric-only
//     rule. The two can disagree on any row where either property is not a
//     number.
//   - A coalesce()-wrapped or arithmetic-wrapped property operand
//     (`coalesce(n.x, 0) < 5`, `n.x + 1 > 5`): the cast target dawgs picks
//     for the WHOLE wrapped expression is derived from its own static type
//     analysis (the same coalesce/`+`-operand typing this package already
//     has to reason about for the `+` concat-vs-numeric split --
//     see classifyAddOperand's doc), which this function does not attempt
//     to re-derive for a relational context; rather than risk an unproven
//     cast-type mismatch, every such shape delegates.
//
// The corpus's own required shape, a bare property lookup relationally
// compared against a statically-numeric expression on the other side (e.g.
// `n.lastlogontimestamp < (datetime().epochseconds - 60*86400)`), is kept:
// pg's cast there is unambiguously numeric (int8/float8, hinted by the
// other side's own static numeric type), and so is this evaluator's --
// EXCEPT for one residual, deliberately accepted divergence, documented in
// the README's known-divergences note: if the property happens to hold a
// non-numeric value on some row (unrealistic for a real timestamp-shaped AD
// property, but not impossible in principle), pg's cast ABORTS the whole
// query with a runtime error, while this evaluator's OrderCompare instead
// answers ErrNotComparable, which TriNull-drops just that one row. Silently
// serving fewer rows than an aborting pg query technically "would have
// returned" (none, since it errors) is the one case this package accepts
// as safe-by-construction rather than a false serve: a dropped row is never
// a WRONG row, and real BloodHound data never puts a string in a timestamp
// property.
//
// isStaticallyNumericScalar (shared with planOrder's own ORDER BY use) is
// exactly the right notion of "statically numeric" here too: it accepts a
// numeric literal, id()/size()/datetime() epoch accessors, and arithmetic
// built only from those, plus -- given pb.numericScalars, threaded through
// as its numericScalars parameter -- a bare reference to a carried numeric
// WITH alias, AT ANY NESTING DEPTH, not just a bare top-level operand: the
// corpus's own `WHERE n.lastlogontimestamp < (datetime().epochseconds -
// (inactive_days * 86400))` shape needs exactly this (inactive_days a `WITH
// 60 AS inactive_days` constant, buried inside arithmetic on the
// comparison's non-property side). Never a bare property lookup, which is
// exactly the operand shape whose static type pg cannot itself prove ahead
// of execution.
func (pb *partBuilder) relationalComparisonSafe(left, right cypher.Expression) bool {
	leftNumeric := isStaticallyNumericScalar(left, pb.numericScalars)
	rightNumeric := isStaticallyNumericScalar(right, pb.numericScalars)
	switch {
	case leftNumeric && rightNumeric:
		return true
	case leftNumeric && isBarePropertyLookup(right):
		return true
	case rightNumeric && isBarePropertyLookup(left):
		return true
	default:
		return false
	}
}

// isBarePropertyLookup reports whether expr (after unwrapping any
// Parentheticals) is a bare `*cypher.PropertyLookup` -- exactly the shape
// dawgs' own translator's expressionToPropertyLookupBinaryExpression
// recognizes for its native-jsonb-vs-cast rewrite split (see
// checkComparison's own doc for the full derivation). A function call
// wrapping the property lookup (`coalesce(n.x, 0)`, `size(n.tags)`, ...)
// deliberately does NOT count -- verified directly against
// translate.Translate's generated SQL to take an identical, sign-
// independent shape regardless.
func isBarePropertyLookup(expr cypher.Expression) bool {
	pl, ok := unwrapParens(expr).(*cypher.PropertyLookup)
	return ok && pl != nil
}

// isNonBareScalarLiteral reports whether expr denotes a numeric, boolean, or
// string literal value while NOT itself being -- with zero unwrapping -- a
// bare `*cypher.Literal`. This is exactly the shape whose translation
// diverges from a true bare literal's (checkComparison's own doc has the
// full derivation and the confirmed SQL for every case below, including its
// own string-literal table): a
// `*cypher.UnaryAddOrSubtractExpression` of EITHER sign at any nesting depth
// (`+5`, `-5`, `-(-5)`, ...), a bare `*cypher.Parenthetical` around a
// literal with no sign at all (`(5)`, `('5')`, `((5))`), and genuine
// multi-term arithmetic (or, for strings, concatenation) over literal
// operands, parenthesized or not (`5 + 0`, `(5 + 0)`, `'5'+”`) -- all of
// it, because dawgs' rewriteJSONScalarEqualityOperand only recognizes a
// pgsql node that is directly `pgsql.TypeHinted`, which a bare
// cypher.Literal's translated pgsql.Literal is and none of
// pgsql.Parenthetical/UnaryExpression/BinaryExpression are.
//
// A non-literal core anywhere in the tree (a property lookup, function
// call, parameter, ...) makes this false: that is not the divergent
// literal shape at all, just some other expression checkExpr has already
// separately validated on its own terms (a wrapped property lookup, e.g.,
// is handled by isBarePropertyLookup's own bare-only scope instead).
func isNonBareScalarLiteral(expr cypher.Expression) bool {
	if isBareScalarLiteral(expr) {
		return false
	}
	return isScalarLiteralTree(expr)
}

// isBareScalarLiteral reports whether expr is, with zero unwrapping, a
// non-null `*cypher.Literal` holding an int64/uint64/float64/bool/string
// value -- exactly (and only) the AST shape dawgs' own native-jsonb rewrite
// recognizes (see isNonBareScalarLiteral's doc for the full derivation).
func isBareScalarLiteral(expr cypher.Expression) bool {
	lit, ok := expr.(*cypher.Literal)
	if !ok || lit == nil || lit.Null {
		return false
	}
	switch lit.Value.(type) {
	case int64, uint64, float64, bool, string:
		return true
	default:
		return false
	}
}

// isScalarLiteralTree reports whether expr, after peeling any
// Parentheticals (unwrapParens) and recursing through
// UnaryAddOrSubtractExpression/ArithmeticExpression nodes, is built
// entirely out of scalar (int64/uint64/float64/bool/string) literals -- i.e.
// is "scalar-literal-shaped" for isNonBareScalarLiteral's purposes,
// regardless of whether it is itself bare (that top-level distinction is
// isNonBareScalarLiteral's own job, not this helper's).
func isScalarLiteralTree(expr cypher.Expression) bool {
	switch e := unwrapParens(expr).(type) {
	case *cypher.Literal:
		return isBareScalarLiteral(e)
	case *cypher.UnaryAddOrSubtractExpression:
		return e != nil && isScalarLiteralTree(e.Right)
	case *cypher.ArithmeticExpression:
		if e == nil || !isScalarLiteralTree(e.Left) {
			return false
		}
		for _, partial := range e.Partials {
			if partial == nil || !isScalarLiteralTree(partial.Right) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// checkInOperands validates `left IN right`/`left NOT IN right` (the latter
// via Negation wrapping this same Comparison shape -- checkExpr's Negation
// case just recurses, no special casing needed there).
//
// The one bypass of the generic Variable rule in this whole file: when
// right is a bare reference to a WITH COLLECT(...) alias (symCollectAlias),
// this is the corpus's "COLLECT anti-join" shape (`NOT c IN exclude`) --
// legal *only* here, and only when left is itself a bare, bound node
// variable (the id-set the alias holds is a set of node ids; comparing
// anything else against it -- a property value, an edge, a path -- is not
// the membership test pg's translation produces), and only when
// membershipAllowed is true. membershipAllowed is
// `predicatePosition && singlePartial` at the call site: this IN must be
// the *entire* comparison (no other Partials chained onto it), AND that
// comparison must itself be reachable, in the executor, as a genuine
// boolean-predicate leaf -- i.e. only through Part.Where's own top-level
// conjunct or a chain of pure Parenthetical/Negation/Conjunction/
// Disjunction/ExclusiveDisjunction wrapping it (see checkExpr's
// predicatePosition doc). Both conditions matter, and for the same reason:
// the executor's own interception (pipeline.go's tryMembershipComparison) is
// only ever reached from evalWhereWithMembership's boolean-structural
// recursion, which never looks inside a Comparison's own operands -- so a
// membership comparison nested there is invisible to it regardless of shape.
// Two concrete shapes this rejects, both accepted before this gate existed:
//   - `u = c IN exclude` -- parses as ONE Comparison (`u = <c IN exclude>`),
//     not a flat two-Partial chain (Cypher's grammar binds IN tighter than
//     `=`): the outer `=`'s own operand check runs checkExpr(right, false),
//     so the nested `c IN exclude` Comparison is checked with
//     predicatePosition=false and rejected here, correctly -- eval.go's
//     EvalValue (what the outer `=`'s evalPartialComparison calls on that
//     operand) has no case for a *cypher.Comparison at all.
//   - `c IN exclude = true` -- symmetric nesting the other way (`<c IN
//     exclude> = true`): same rejection, same reason.
//
// Every other IN shape (a literal list, a list-valued property, a
// membership comparison used as a value anywhere, ...) is checked the
// ordinary way, which itself rejects a collect-alias variable appearing in
// the wrong position (e.g. as the *left* operand, or anywhere outside this
// bypass).
func (pb *partBuilder) checkInOperands(left, right cypher.Expression, membershipAllowed bool) bool {
	if rv, ok := unwrapParens(right).(*cypher.Variable); ok && rv != nil {
		if pb.known[rv.Symbol] == symCollectAlias {
			if !membershipAllowed {
				return false
			}
			lv, ok := unwrapParens(left).(*cypher.Variable)
			if !ok || lv == nil || pb.known[lv.Symbol] != symNode {
				return false
			}
			pb.touched[lv.Symbol] = true
			pb.touched[rv.Symbol] = true
			return true
		}
	}
	return pb.checkExpr(left, false) && pb.checkExpr(right, false)
}

// checkRegexOperands validates `left =~ right`: left is checked the
// ordinary way, but right (the pattern) must be a compile-time string
// literal -- not merely a present-string-valued expression the way
// eval.go's own literalStringValue would accept for STARTS/ENDS/CONTAINS --
// because the controller's plan-time-compilation amendment requires
// regexp.Compile to run here, once, or the whole query rejects. A
// successfully compiled pattern is cached in pb.regexes (shared across the
// whole Query) keyed by its decoded text, so an identical pattern appearing
// more than once in the query compiles exactly once.
func (pb *partBuilder) checkRegexOperands(left, right cypher.Expression) bool {
	if !pb.checkExpr(left, false) {
		return false
	}
	lit, ok := asLiteral(right)
	if !ok || lit == nil || lit.Null {
		return false
	}
	raw, ok := lit.Value.(string)
	if !ok {
		return false
	}
	decoded, err := decodeCypherStringLiteral(raw)
	if err != nil {
		return false
	}
	if _, exists := pb.regexes[decoded]; exists {
		return true
	}
	re, err := regexp.Compile(decoded)
	if err != nil {
		return false
	}
	pb.regexes[decoded] = re
	return true
}

// checkKindMatcher validates `sym:Kind[:Kind2...]` (WHERE-position or any
// other expression position a KindMatcher may appear): sym must be a known
// node or edge variable (eval.go's evalKindMatcher checks Row.Node then
// Row.Edge), and every kind name must resolve against the snapshot.
func (pb *partBuilder) checkKindMatcher(km *cypher.KindMatcher) bool {
	if km == nil {
		return false
	}
	v, ok := unwrapParens(km.Reference).(*cypher.Variable)
	if !ok || v == nil {
		return false
	}
	k, known := pb.known[v.Symbol]
	if !known || (k != symNode && k != symEdge) {
		return false
	}
	for _, kind := range km.Kinds {
		if _, ok := pb.snap.Kinds().ID(kind.String()); !ok {
			return false
		}
	}
	pb.touched[v.Symbol] = true
	return true
}

// checkPatternPredicate validates a WHERE-clause bare relationship pattern
// used as a boolean predicate (`WHERE (n)-[:K]->(m)`, or negated via
// `WHERE NOT (n)-[:K]->(m)` -- checkExpr's *cypher.Negation case already
// recurses here unchanged, and eval.go's evalNegation composes with
// whatever Tri this returns the ordinary way, so NOT needs no special
// handling of its own).
//
// This package accepts only a narrow slice of what pg's own translation can
// actually do for a pattern predicate (verified against dawgs@v0.8.0's
// cypher/models/pgsql/translate/predicate.go): a SINGLE, FIXED-LENGTH
// relationship step between two node patterns that are BARE REFERENCES to
// already-pattern-bound node variables. The grammar for a pattern predicate
// is exactly one oC_RelationshipsPattern -- oC_NodePattern
// (oC_PatternElementChain)+ -- which can itself be a multi-hop chain
// (`(a)-[:X]->(b)-[:Y]->(c)`, 5 PatternElements: Node, Rel, Node, Rel,
// Node); PatternPredicateVisitor (frontend/pattern.go) flattens the whole
// chain into one PatternElements slice regardless of hop count, so any
// chain longer than one hop produces more than 3 elements and rejects here
// via len(PatternElements) == 3.
//
// This is NOT a mirror of some pg limitation, and an earlier version of
// this comment overstated one: pg's own buildPatternPredicates (predicate.go)
// has a general path -- distinct from its "optimized" single-hop existence
// fast path (buildOptimizedRelationshipExistPredicate) -- that lowers a
// multi-hop pattern predicate into a correlated CTE chain, one CTE per
// TraversalStep, each built the same way an ordinary MATCH step would be
// (buildTraversalPatternRootWithOuterCorrelation for the row-correlated
// root step, then buildTraversalPatternStep per subsequent hop), wrapped in
// `EXISTS(SELECT ... FROM <last CTE> WHERE COUNT(*) > 0)`. The "expected
// exactly one pattern part" error translatePatternPredicate raises guards a
// different case entirely (more than one comma-separated PatternPart inside
// one predicate, a shape this grammar cannot even produce), not hop count --
// a multi-hop chain is still exactly one PatternPart, just with more than
// one TraversalStep, which that general path's own per-step loop handles
// directly. So pg genuinely CAN answer a multi-hop pattern predicate; this
// package declining one is a deliberate scope cut (evalPatternPredicate is
// only ever a pure two-endpoint adjacency probe, hasAdjacentEdge, with
// nothing resembling that general path's row-correlated multi-step
// machinery), not something forced by pg's own capabilities.
//
// Also rejected, all deliberately conservative narrowings this package's
// required corpus never needs (REJECTED delegates to PostgreSQL, which is
// always safe -- this package's established convention):
//   - Range != nil (`(n)-[:K*1..]->(m)` inside a pattern predicate): this
//     one IS a genuine pg limitation -- buildPatternPredicates' per-step
//     loop explicitly rejects it outright ("expansion in pattern predicate
//     not supported"), so this package must too rather than silently plan
//     a shape pg itself cannot translate.
//   - A named relationship variable (`(n)-[r:K]->(m)`): existence-only
//     evaluation (evalPatternPredicate) never binds anything from inside a
//     pattern predicate, matching pg's own semantics (a pattern predicate
//     only ever contributes a boolean, never a projectable value) -- a
//     named variable here would be dead syntax at best, so this package
//     declines rather than silently drop a binding a caller might expect.
//   - rel.Properties (an inline map on the relationship): mirrors
//     buildStep's own identical rejection for an ordinary MATCH step (this
//     package's snapshot has no edge property store to check one against).
//   - Either node pattern carrying its own Kinds/Properties, or a fresh
//     (not-already-known) or anonymous node variable: this package's
//     required corpus always writes both endpoints as bare references to
//     variables the outer MATCH already bound (e.g. `WHERE
//     (n)-[:K]-(m)` after `MATCH (n:Domain)...(m:Domain)`); pg's own
//     general path resolves a fresh/anonymous node exactly the way it
//     resolves one in an ordinary MATCH (a genuine existential subquery
//     over a new variable, not merely a semi-join against an existing
//     binding), so -- like the multi-hop case above -- this is this
//     package's own scope cut, not a pg mirror: evalPatternPredicate
//     implements nothing to resolve a fresh node's own constraints inside
//     its pure two-endpoint adjacency check, so that broader shape stays
//     planner-rejected rather than mis-served.
func (pb *partBuilder) checkPatternPredicate(pp *cypher.PatternPredicate) bool {
	if pp == nil || len(pp.PatternElements) != 3 {
		return false
	}

	from, ok := pb.checkPatternPredicateEndpoint(pp.PatternElements[0])
	if !ok {
		return false
	}
	rel, isRel := pp.PatternElements[1].Element.(*cypher.RelationshipPattern)
	if !isRel || rel == nil || rel.Range != nil || rel.Variable != nil || rel.Properties != nil {
		return false
	}
	to, ok := pb.checkPatternPredicateEndpoint(pp.PatternElements[2])
	if !ok {
		return false
	}

	switch rel.Direction {
	case graph.DirectionOutbound, graph.DirectionInbound, graph.DirectionBoth:
	default:
		return false
	}
	for _, k := range rel.Kinds {
		if _, ok := pb.snap.Kinds().ID(k.String()); !ok {
			return false
		}
	}

	// A two-symbol predicate never pushes into either symbol's own
	// NodeConstraint.Predicates (pushdown, below, only pushes when exactly
	// one symbol is touched) -- it stays a residual Part.Where conjunct,
	// evaluated once the whole row is assembled, exactly like any other
	// cross-symbol WHERE conjunct (e.g. `s.prop = t.prop`). from == to
	// (`(n)-[:K]-(n)`) still marks exactly one symbol touched, which *does*
	// push into that single symbol's own Predicates -- also correct,
	// since resolveEndpointSet's per-candidate EvalPredicate call handles a
	// self-referencing predicate the same way any other single-symbol one
	// works.
	pb.touched[from] = true
	pb.touched[to] = true
	return true
}

// checkPatternPredicateEndpoint validates one PatternPredicate.PatternElements
// entry as a bare reference to an already-pattern-bound node variable (see
// checkPatternPredicate's own doc for why: no fresh/anonymous nodes, no
// re-stated Kinds/Properties), returning its symbol.
func (pb *partBuilder) checkPatternPredicateEndpoint(el *cypher.PatternElement) (sym string, ok bool) {
	if el == nil {
		return "", false
	}
	np, isNode := el.Element.(*cypher.NodePattern)
	if !isNode || np == nil || np.Variable == nil || len(np.Kinds) > 0 || np.Properties != nil {
		return "", false
	}
	k, known := pb.known[np.Variable.Symbol]
	if !known || k != symNode {
		return "", false
	}
	return np.Variable.Symbol, true
}

// checkPropertyLookup validates `atom.prop`: atom must be either a node
// variable (edge property access is rejected outright -- the snapshot has
// no edge property store) or a zero-argument `datetime()` call whose
// property is exactly epochseconds/epochmillis (the only two temporal
// components the gate/evaluator support; any other datetime() component,
// or *any* argument to datetime() at all -- which eval.go's
// evalPropertyLookup does not itself validate, silently ignoring an
// argument if one were present -- rejects here defensively rather than risk
// a silently-wrong answer).
func (pb *partBuilder) checkPropertyLookup(pl *cypher.PropertyLookup) bool {
	if pl == nil {
		return false
	}
	atom := unwrapParens(pl.Atom)

	if fi, isFunc := atom.(*cypher.FunctionInvocation); isFunc && fi != nil && strings.ToLower(fi.Name) == cypher.DateTimeFunction {
		if len(fi.Arguments) != 0 {
			return false
		}
		switch strings.ToLower(pl.Symbol) {
		case cypher.ITTCEpochSeconds, cypher.ITTCEpochMilliseconds:
			return true
		default:
			return false
		}
	}

	v, ok := atom.(*cypher.Variable)
	if !ok || v == nil {
		return false
	}
	k, known := pb.known[v.Symbol]
	if !known || k != symNode {
		return false
	}
	pb.touched[v.Symbol] = true
	return true
}

// checkFunction validates a FunctionInvocation against exactly the function
// set eval.go's evalFunction dispatches on: id, labels, type, toLower,
// toUpper, coalesce, size, split. Anything else -- notably count/collect
// (legal *only* inside a WITH aggregate item, never as a general
// expression -- see planWith/classifyAggregate) and sum/avg/min/max/
// startNode/endNode/nodes/relationships/keys/properties/exists/length/
// toString/toInteger (none of which eval.go implements at all, regardless of
// what the design doc's serving-matrix table names as an eventual goal) --
// is an unknown function here and rejects.
func (pb *partBuilder) checkFunction(fi *cypher.FunctionInvocation) bool {
	if fi == nil {
		return false
	}
	switch strings.ToLower(fi.Name) {
	case cypher.IdentityFunction:
		v, ok := singleVariableArg(fi)
		if !ok {
			return false
		}
		k, known := pb.known[v.Symbol]
		if !known || (k != symNode && k != symEdge) {
			return false
		}
		pb.touched[v.Symbol] = true
		return true

	case cypher.NodeLabelsFunction:
		v, ok := singleVariableArg(fi)
		if !ok {
			return false
		}
		if pb.known[v.Symbol] != symNode {
			return false
		}
		pb.touched[v.Symbol] = true
		return true

	case cypher.EdgeTypeFunction:
		v, ok := singleVariableArg(fi)
		if !ok {
			return false
		}
		if pb.known[v.Symbol] != symEdge {
			return false
		}
		pb.touched[v.Symbol] = true
		return true

	case cypher.ToLowerFunction, cypher.ToUpperFunction:
		return len(fi.Arguments) == 1 && pb.checkExpr(fi.Arguments[0], false)

	case cypher.CoalesceFunction:
		if len(fi.Arguments) == 0 {
			return false
		}
		return pb.checkExprList(fi.Arguments, false)

	case cypher.ListSizeFunction:
		// size() serves list-property arguments only (a bare list variable,
		// or a string, would need character-length semantics this
		// evaluator does not reproduce -- see eval.go's evalSizeFunction
		// doc); requiring a literal PropertyLookup argument here is what
		// rejects size() on anything but a property lookup.
		if len(fi.Arguments) != 1 {
			return false
		}
		pl, ok := unwrapParens(fi.Arguments[0]).(*cypher.PropertyLookup)
		if !ok || pl == nil {
			return false
		}
		return pb.checkExpr(pl, false)

	case cypher.StringSplitToArrayFunction:
		return len(fi.Arguments) == 2 && pb.checkExpr(fi.Arguments[0], false) && pb.checkExpr(fi.Arguments[1], false)

	default:
		return false
	}
}

// checkArithmetic validates +, -, *, /, % over a chained
// ArithmeticExpression (mirrors eval.go's evalArithmetic folding, including
// its curKind fold -- see below). `^` (OperatorPowerOf) is a real dawgs
// operator constant but eval.go's applyArithmetic has no case for it, so it
// is deliberately excluded here too -- accepting it in Plan would guarantee
// a runtime ErrUnsupported bail on the very first row, wasting the whole
// materialization pass before falling back to delegation anyway.
//
// Two additional rejections, both specific to `+`, both delegating the
// whole query rather than ever risking a per-row wrong (or pg-refused)
// answer:
//
//  1. Two raw property lookups added together: pg's own translation
//     statically types this as string concatenation, unconditionally,
//     regardless of what they hold at runtime (isConcatenationOperation's
//     "both operands are property lookups" branch -- see applyAdd's doc
//     comment in eval.go for the full pg-parity investigation and the
//     review finding this closes: `n.score + n.score` over numeric scores
//     used to silently serve a numeric 84 where pg returns the
//     concatenated string "4242"). Reproducing pg's choice would mean
//     rendering an arbitrary JSON scalar exactly as pg's own jsonb `->>`
//     operator does, which this package's established "no unpinned
//     rendering reproduction" convention declines to attempt (see
//     applyAdd's doc again) -- and no corpus query needs this shape.
//
//     This can only ever apply to the chain's very first `+` (ae.Left
//     paired with ae.Partials[0].Right): a flat Cypher arithmetic chain
//     folds left-associatively, so for every later `+` the true left
//     operand is whatever the preceding folds already produced -- never,
//     itself, a literal PropertyLookup AST node -- exactly mirroring why
//     pg's own nested pgsql.BinaryExpression tree can only ever see two
//     literal property-lookup operands at that same first position. (The
//     curKind fold below guarantees this structurally -- nextAddKind never
//     produces addPropertyLookup as a folded result -- so the `i == 0`
//     guard is a belt-and-suspenders match of the doc's own reasoning, not
//     load-bearing.)
//
//  2. An addUnresolved operand (a coalesce() call none of whose arguments
//     carry a pg-known type -- classifyCoalesceOperand's doc, eval.go) NOT
//     paired with an addStaticText partner, at ANY position in the chain --
//     unlike rejection 1, this one is NOT limited to the first `+`. A bare
//     property lookup's safety at position i>0 is structural (it can never
//     independently BE a property lookup there -- see above), but an
//     addUnresolved coalesce is a literal FunctionInvocation AST node that
//     can appear as ANY p.Right (or, at i==0, as ae.Left) regardless of
//     position, and its own safety depends only on whether pg statically
//     resolves ITS type -- which never depends on chain position, only on
//     its own arguments. Checking this correctly at i>0 needs the actual
//     folded left-hand kind up to that point (curKind below), not just
//     ae.Left/Partials[0] in isolation, because pg's Text-always-wins rule
//     (isConcatenationOperation checks `lOperandType == Text ||
//     rOperandType == Text` before anything else) means an addUnresolved
//     operand IS safe when paired with a running fold that is itself
//     addStaticText (e.g. `'x' + coalesce(n.a,n.b)` genuinely concatenates
//     in pg) but NOT safe paired with anything else (addUnresolved's own
//     doc has the full derivation: such a coalesce's arguments were never
//     rewritten to a resolved type by translateCoalesceFunction, so pg
//     falls through to a bare `+` over their default `->>` text rendering,
//     which has no PostgreSQL operator against a non-text partner and
//     genuinely errors in real pg, regardless of runtime values).
func (pb *partBuilder) checkArithmetic(ae *cypher.ArithmeticExpression) bool {
	if ae == nil || !pb.checkExpr(ae.Left, false) {
		return false
	}
	curKind := classifyAddOperand(ae.Left)
	for i, p := range ae.Partials {
		if p == nil {
			return false
		}
		switch p.Operator {
		case cypher.OperatorAdd, cypher.OperatorSubtract, cypher.OperatorMultiply, cypher.OperatorDivide, cypher.OperatorModulo:
		default:
			return false
		}
		if !pb.checkExpr(p.Right, false) {
			return false
		}
		rKind := classifyAddOperand(p.Right)

		if p.Operator == cypher.OperatorAdd {
			if i == 0 && curKind == addPropertyLookup && rKind == addPropertyLookup {
				return false
			}
			if curKind != addStaticText && rKind != addStaticText &&
				(curKind == addUnresolved || rKind == addUnresolved) {
				return false
			}
		}

		curKind = nextAddKind(p.Operator, curKind, rKind)
	}
	return true
}

// --- WITH ------------------------------------------------------------------

// planWith validates and compiles one WITH clause's Projection against
// inputKnown (the symbol table as of the end of the preceding Part), per
// WithClause's documented four-shape grammar. It returns the compiled
// WithClause and the *replacement* symbol table visible from this point on
// (Cypher's own WITH scoping: only what the WITH projection actually lists
// survives into the next Part -- everything else, however it was bound
// before, becomes unreachable).
func planWith(inputKnown map[string]symKind, w *cypher.With) (WithClause, map[string]symKind, bool) {
	if w == nil || w.Where != nil {
		return WithClause{}, nil, false
	}
	proj := w.Projection
	if proj == nil || proj.All || proj.Order != nil || proj.Skip != nil || proj.Limit != nil || len(proj.Items) == 0 {
		return WithClause{}, nil, false
	}

	wc := WithClause{Distinct: proj.Distinct}
	outputKnown := map[string]symKind{}

	for _, raw := range proj.Items {
		item, ok := raw.(*cypher.ProjectionItem)
		if !ok || item == nil {
			return WithClause{}, nil, false
		}

		if fi, isFunc := unwrapParens(item.Expression).(*cypher.FunctionInvocation); isFunc && fi != nil {
			if item.Alias == nil || item.Alias.Symbol == "" {
				return WithClause{}, nil, false
			}
			agg, ok := classifyAggregate(inputKnown, fi)
			if !ok {
				return WithClause{}, nil, false
			}
			agg.Alias = item.Alias.Symbol
			if _, dup := outputKnown[agg.Alias]; dup {
				return WithClause{}, nil, false
			}
			wc.Aggregates = append(wc.Aggregates, agg)
			if agg.Collect != nil {
				outputKnown[agg.Alias] = symCollectAlias
			} else {
				outputKnown[agg.Alias] = symScalar
			}
			continue
		}

		if lit, isLit := asLiteral(item.Expression); isLit {
			if item.Alias == nil || item.Alias.Symbol == "" {
				return WithClause{}, nil, false
			}
			val, _, err := evalLiteralValue(lit)
			if err != nil {
				return WithClause{}, nil, false
			}
			if _, dup := outputKnown[item.Alias.Symbol]; dup {
				return WithClause{}, nil, false
			}
			wc.Constants = append(wc.Constants, WithConstant{Alias: item.Alias.Symbol, Value: val})
			outputKnown[item.Alias.Symbol] = symScalar
			continue
		}

		// Plain variable carry-over: bare, unaliased Variable only.
		if item.Alias != nil {
			return WithClause{}, nil, false
		}
		v, isVar := unwrapParens(item.Expression).(*cypher.Variable)
		if !isVar || v == nil {
			return WithClause{}, nil, false
		}
		kind, known := inputKnown[v.Symbol]
		if !known || kind == symPath || kind == symCollectAlias {
			return WithClause{}, nil, false
		}
		if _, dup := outputKnown[v.Symbol]; dup {
			return WithClause{}, nil, false
		}
		wc.GroupKeys = append(wc.GroupKeys, v.Symbol)
		outputKnown[v.Symbol] = kind
	}

	return wc, outputKnown, true
}

// classifyAggregate recognizes fi as a servable WITH aggregate:
// COUNT(sym)/COUNT(DISTINCT sym) or COLLECT(sym) over a variable already
// bound in this Part (`COUNT(*)` -- no argument at all -- and any other
// function name, including sum/avg/min/max, reject). COLLECT specifically
// requires sym to be a *node* variable: the id-set membership semantics
// this matrix serves (design doc amendment b) is defined over node
// identity, not arbitrary scalar collection.
func classifyAggregate(known map[string]symKind, fi *cypher.FunctionInvocation) (WithAggregate, bool) {
	switch strings.ToLower(fi.Name) {
	case cypher.CountFunction:
		if len(fi.Arguments) != 1 {
			return WithAggregate{}, false
		}
		v, ok := unwrapParens(fi.Arguments[0]).(*cypher.Variable)
		if !ok || v == nil {
			return WithAggregate{}, false
		}
		k, isKnown := known[v.Symbol]
		if !isKnown || k == symCollectAlias || k == symPath {
			return WithAggregate{}, false
		}
		return WithAggregate{Count: &CountAgg{Distinct: fi.Distinct, Sym: v.Symbol}}, true

	case cypher.CollectFunction:
		if fi.Distinct || len(fi.Arguments) != 1 {
			return WithAggregate{}, false
		}
		v, ok := unwrapParens(fi.Arguments[0]).(*cypher.Variable)
		if !ok || v == nil {
			return WithAggregate{}, false
		}
		if known[v.Symbol] != symNode {
			return WithAggregate{}, false
		}
		return WithAggregate{Collect: &CollectMembershipAgg{Sym: v.Symbol}}, true

	default:
		return WithAggregate{}, false
	}
}

// --- RETURN ------------------------------------------------------------

// planReturn validates and compiles the query's final RETURN clause against
// known (the symbol table visible at this point), countAliases (COUNT
// aggregate alias names from the immediately preceding Part's WithClause,
// for the ORDER BY exception), and numericScalars (that same WithClause's
// numeric WithConstant/COUNT aliases, per numericScalarSet -- ORDER BY's own
// isStaticallyNumericScalar admission needs this exactly like
// relationalComparisonSafe's WHERE-side check does, for a RETURN item
// referencing a carried numeric alias, however deeply nested inside
// arithmetic); both empty when there was no preceding WITH.
func planReturn(snap *snapshot.View, known map[string]symKind, countAliases, numericScalars map[string]bool, ret *cypher.Return) (Projection, []OrderKey, int64, int64, bool) {
	if ret == nil || ret.Projection == nil {
		return Projection{}, nil, 0, -1, false
	}
	proj := ret.Projection
	if proj.All || len(proj.Items) == 0 {
		return Projection{}, nil, 0, -1, false
	}

	pb := &partBuilder{snap: snap, known: known, regexes: map[string]*regexp.Regexp{}}

	var items []ProjectionOutput
	projectedAliases := map[string]bool{}
	// projectedKinds records, for every RETURN alias, whether its own output
	// is node/edge/path-valued (symNode/symEdge/symPath) or a plain scalar
	// (symScalar, standing in here for "everything else this evaluator
	// produces a plain float64/string/bool/nil for" -- a property lookup, a
	// function call, arithmetic, a literal) -- see planOrder's doc for why
	// this distinction matters.
	projectedKinds := map[string]symKind{}
	// projectedNumeric records, for every RETURN alias, whether
	// isStaticallyNumericScalar accepts its own top-level expression --
	// planOrder's second (beyond bare count aliases) admission criterion.
	projectedNumeric := map[string]bool{}

	for _, raw := range proj.Items {
		item, ok := raw.(*cypher.ProjectionItem)
		if !ok || item == nil {
			return Projection{}, nil, 0, -1, false
		}

		pb.touched = map[string]bool{}
		itemKind := symScalar
		if pv, isVar := unwrapParens(item.Expression).(*cypher.Variable); isVar && pv != nil && pb.known[pv.Symbol] == symPath {
			// Bare path-variable projection: see Part's doc and symPath's
			// doc for why this is the one deliberate bypass of checkExpr --
			// eval.go's EvalValue cannot materialize a path at all, so a
			// future executor is expected to build this value directly
			// from the Step(s) carrying the matching PathSym instead of
			// ever calling EvalValue for it.
			pb.touched[pv.Symbol] = true
			itemKind = symPath
		} else if !pb.checkExpr(item.Expression, false) || !isValueShape(item.Expression) {
			return Projection{}, nil, 0, -1, false
		} else if isVar && pv != nil {
			// A bare Variable's own projected kind is whatever it was bound
			// to (symNode/symEdge/symScalar) -- checkExpr already confirmed
			// it is known and not symCollectAlias/symPath (the symPath case
			// is handled by the branch above).
			itemKind = pb.known[pv.Symbol]
		}

		if !projectionTypingOK(item.Expression) {
			return Projection{}, nil, 0, -1, false
		}

		name, ok := projectionName(item)
		if !ok || projectedAliases[name] {
			return Projection{}, nil, 0, -1, false
		}
		projectedAliases[name] = true
		projectedKinds[name] = itemKind
		projectedNumeric[name] = isStaticallyNumericScalar(item.Expression, numericScalars)

		items = append(items, ProjectionOutput{
			Alias:        name,
			Expr:         item.Expression,
			BareCallKind: bareCallKind(item.Expression),
		})
	}

	orderKeys, ok := planOrder(proj.Order, projectedKinds, projectedNumeric, countAliases)
	if !ok {
		return Projection{}, nil, 0, -1, false
	}
	skip, ok := literalNonNegativeInt(skipExpr(proj.Skip))
	if proj.Skip != nil && !ok {
		return Projection{}, nil, 0, -1, false
	}
	if proj.Skip == nil {
		skip = 0
	}
	limit := int64(-1)
	if proj.Limit != nil {
		var ok bool
		limit, ok = literalNonNegativeInt(proj.Limit.Value)
		if !ok {
			return Projection{}, nil, 0, -1, false
		}
	}

	return Projection{Distinct: proj.Distinct, Items: items}, orderKeys, skip, limit, true
}

func skipExpr(s *cypher.Skip) cypher.Expression {
	if s == nil {
		return nil
	}
	return s.Value
}

// literalNonNegativeInt requires expr to be a non-negative integer literal
// (int64 or uint64) -- never a *cypher.Parameter or any other expression --
// used for SKIP/LIMIT ("non-literal SKIP/LIMIT" and "parameters anywhere"
// both reject via this one check).
func literalNonNegativeInt(expr cypher.Expression) (int64, bool) {
	if expr == nil {
		return 0, false
	}
	lit, ok := asLiteral(expr)
	if !ok || lit == nil || lit.Null {
		return 0, false
	}
	switch v := lit.Value.(type) {
	case int64:
		if v < 0 {
			return 0, false
		}
		return v, true
	case uint64:
		return int64(v), true
	default:
		return 0, false
	}
}

// isValueShape reports whether expr's top-level (paren-unwrapped) node is
// one eval.go's EvalValue actually has a case for: Literal, Variable,
// PropertyLookup, ListLiteral, FunctionInvocation, ArithmeticExpression, or
// UnaryAddOrSubtractExpression. checkExpr itself is shared with WHERE-clause
// validation and therefore also accepts boolean-only shapes (Comparison,
// Conjunction, Disjunction, ExclusiveDisjunction, Negation, KindMatcher) --
// exactly the node types EvalValue's own switch has no case for and would
// answer with ErrUnsupported. Accepting one of those as a RETURN/WITH item's
// *own* top-level expression (e.g. `RETURN n.x = 5 AS flag`) would never
// produce a wrong answer (an EvalValue ErrUnsupported bails the whole query
// to delegation before any row is emitted, per the architecture's
// materialize-then-emit design), but it would guarantee a wasted
// materialization pass on every single invocation for a shape Plan could
// have declined for free -- so this narrower check runs in addition to
// checkExpr for a projection item's own top-level expression specifically
// (nested sub-expressions reached through checkExpr's normal recursion are
// unaffected: a WHERE clause legitimately nests Comparisons everywhere).
func isValueShape(expr cypher.Expression) bool {
	switch unwrapParens(expr).(type) {
	case *cypher.Literal, *cypher.Variable, *cypher.PropertyLookup, *cypher.ListLiteral,
		*cypher.FunctionInvocation, *cypher.ArithmeticExpression, *cypher.UnaryAddOrSubtractExpression:
		return true
	default:
		return false
	}
}

// projectionName derives a RETURN item's output column name: an explicit
// alias always wins; absent one, a bare variable projects under its own
// name and a property lookup projects as "var.prop" (Cypher's own default
// column-naming convention for exactly these two shapes). Anything else
// unaliased (a function call, arithmetic, a path variable, ...) requires an
// explicit alias -- a conservative simplification with no effect on any
// required serving shape, and one that only ever narrows what Plan accepts.
func projectionName(item *cypher.ProjectionItem) (string, bool) {
	if item.Alias != nil && item.Alias.Symbol != "" {
		return item.Alias.Symbol, true
	}
	switch e := unwrapParens(item.Expression).(type) {
	case *cypher.Variable:
		if e == nil || e.Symbol == "" {
			return "", false
		}
		return e.Symbol, true
	case *cypher.PropertyLookup:
		if e == nil {
			return "", false
		}
		v, ok := unwrapParens(e.Atom).(*cypher.Variable)
		if !ok || v == nil {
			return "", false
		}
		return v.Symbol + "." + e.Symbol, true
	default:
		return "", false
	}
}

// bareCallKind classifies expr (a RETURN item's whole expression) as one of
// the controller's four projection-typing-amendment calls, or "" if it is
// not (directly) one of them. See ProjectionOutput.BareCallKind.
func bareCallKind(expr cypher.Expression) string {
	e := unwrapParens(expr)
	if fi, ok := e.(*cypher.FunctionInvocation); ok && fi != nil {
		switch strings.ToLower(fi.Name) {
		case cypher.IdentityFunction:
			return "id"
		case cypher.ListSizeFunction:
			return "size"
		}
		return ""
	}
	if pl, ok := e.(*cypher.PropertyLookup); ok && pl != nil {
		if fi, ok := unwrapParens(pl.Atom).(*cypher.FunctionInvocation); ok && fi != nil && strings.ToLower(fi.Name) == cypher.DateTimeFunction {
			switch strings.ToLower(pl.Symbol) {
			case cypher.ITTCEpochSeconds:
				return "epochseconds"
			case cypher.ITTCEpochMilliseconds:
				return "epochmillis"
			}
		}
	}
	return ""
}

// projectionTypingOK implements the controller's projection-typing
// amendment: id()/size()/datetime().epochseconds/.epochmillis are servable
// only as the *entire* content of a RETURN item (optionally aliased) --
// nested inside arithmetic or another function call rejects the whole
// query, since pg keeps integer typing there (a nested `id(n) + 1` is still
// an int8 addition in SQL) that this package's uniform float64 evaluator
// cannot reproduce.
func projectionTypingOK(expr cypher.Expression) bool {
	if bareCallKind(expr) != "" {
		return true
	}
	return !containsFlaggedCallNested(expr)
}

// containsFlaggedCallNested reports whether one of the four flagged calls
// appears anywhere within expr's subtree (expr itself included) -- called
// only once projectionTypingOK has already established expr as a whole is
// *not* itself one of them, so any occurrence found here is by construction
// a nested one.
func containsFlaggedCallNested(expr cypher.Expression) bool {
	switch e := expr.(type) {
	case nil:
		return false
	case *cypher.Parenthetical:
		return e != nil && containsFlaggedCallNested(e.Expression)
	case *cypher.FunctionInvocation:
		if e == nil {
			return false
		}
		if bareCallKind(e) != "" {
			return true
		}
		for _, a := range e.Arguments {
			if containsFlaggedCallNested(a) {
				return true
			}
		}
		return false
	case *cypher.PropertyLookup:
		if e == nil {
			return false
		}
		if bareCallKind(e) != "" {
			return true
		}
		return containsFlaggedCallNested(e.Atom)
	case *cypher.ArithmeticExpression:
		if e == nil {
			return false
		}
		if containsFlaggedCallNested(e.Left) {
			return true
		}
		for _, p := range e.Partials {
			if p != nil && containsFlaggedCallNested(p.Right) {
				return true
			}
		}
		return false
	case *cypher.UnaryAddOrSubtractExpression:
		return e != nil && containsFlaggedCallNested(e.Right)
	case *cypher.ListLiteral:
		if e == nil {
			return false
		}
		for _, el := range *e {
			if containsFlaggedCallNested(el) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// planOrder validates ORDER BY: every item must be a bare Variable (after
// unwrapping parens) naming either a RETURN-projected alias or -- the
// controller's "bare count alias" exception -- a COUNT aggregate alias from
// the immediately preceding WITH, even when that alias is not re-projected
// in RETURN itself (the corpus's `WITH ... COUNT(c) AS adminCount RETURN u
// ORDER BY adminCount DESC` shape). Anything else (a property lookup, a
// function call, arithmetic, an unrelated symbol) rejects -- "ORDER BY
// expressions other than projected aliases/ids/count".
//
// A projected alias must additionally be provably numeric. This is
// NARROWER than "scalar-valued": eval.go's comparison machinery
// (value.go's Compare, ported line for line from DAWGS' own
// cypher_value_compare plpgsql function) is what this package's ORDER BY
// uses -- but a differential probe against a live PostgreSQL database found
// that dawgs' SQL translation does NOT always route a query's own ORDER BY
// through that same function: a bare property-lookup alias (jsonb-typed,
// no static type pg can prove ahead of execution) instead sorts via
// PostgreSQL's native jsonb btree comparison, whose type-rank order
// (Null < String < Number < Bool < Array < Object, SQL NULL last) does not
// match cypher_value_compare's own ranking. Serving such a query with
// Compare would silently reorder rows relative to pg -- and under a LIMIT,
// reordering changes which rows even survive, not merely their sequence.
// COUNT is exempt because it is always a definite, homogeneously-typed int8
// column, for which native jsonb ordering and Compare's numeric branch
// agree by construction (no cross-type ambiguity is possible). The other
// alias shape this function admits, isStaticallyNumericScalar, extends the
// same reasoning to any RETURN item whose STATIC AST shape guarantees a
// definite number every row regardless of what any underlying property
// holds (id()/size()/datetime() epoch accessors, numeric literals,
// arithmetic over only those) -- never a bare property lookup, which is
// exactly the shape the probe found unsafe. Every other alias shape
// (property lookups, arbitrary function calls, node/edge/path values)
// rejects outright, delegating the whole query to PostgreSQL.
func planOrder(order *cypher.Order, projectedKinds map[string]symKind, projectedNumeric map[string]bool, countAliases map[string]bool) ([]OrderKey, bool) {
	if order == nil {
		return nil, true
	}
	keys := make([]OrderKey, 0, len(order.Items))
	for _, item := range order.Items {
		if item == nil {
			return nil, false
		}
		v, ok := unwrapParens(item.Expression).(*cypher.Variable)
		if !ok || v == nil {
			return nil, false
		}
		if countAliases[v.Symbol] {
			keys = append(keys, OrderKey{Symbol: v.Symbol, Descending: !item.Ascending})
			continue
		}
		kind, isProjected := projectedKinds[v.Symbol]
		if !isProjected || kind == symNode || kind == symEdge || kind == symPath {
			return nil, false
		}
		if !projectedNumeric[v.Symbol] {
			return nil, false
		}
		keys = append(keys, OrderKey{Symbol: v.Symbol, Descending: !item.Ascending})
	}
	return keys, true
}

// isStaticallyNumericScalar reports whether expr (a RETURN item's own top-
// level expression, or -- recursively -- any sub-expression reached while
// deciding that) can only ever evaluate to a Cypher number, judged purely by
// its STATIC AST shape -- never by sniffing a runtime value -- mirroring
// the discipline classifyAddOperand already applies to `+` operands. It
// accepts exactly: a bare numeric literal; id(), size(), or
// datetime().epochseconds/.epochmillis (this package's BareCallKind set,
// each of which evalIDFunction/evalListSizeFunction/evalDateTimeComponent
// always produces as a genuine Go float64, never a property-typed value); a
// bare Variable naming an alias numericScalars marks as always-numeric (a
// carried WITH constant/COUNT -- see partBuilder.numericScalars' own doc;
// nil is a valid, always-empty map for a caller with no such aliases to
// offer, e.g. planOrder's Part[0] context); and arithmetic (+, -, *, /, %,
// unary +/-) whose every leaf is itself one of those shapes -- checked at
// EVERY nesting depth, not just the top level, so a carried alias buried
// inside a larger expression (the corpus's own `datetime().epochseconds -
// (inactive_days * 86400)`, inactive_days a `WITH 60 AS inactive_days`
// constant) is recognized correctly, not just a bare top-level reference to
// one. A bare property lookup is NEVER included, no matter how "obviously
// numeric" its name looks (e.g. lastlogontimestamp): PostgreSQL keeps no
// static type for a jsonb property, so a row where it happens to hold a
// string, a bool, or is absent entirely is not something Plan can rule out
// ahead of execution -- see planOrder's own doc for why that distinction is
// exactly the divergence this function exists to avoid.
func isStaticallyNumericScalar(expr cypher.Expression, numericScalars map[string]bool) bool {
	expr = unwrapParens(expr)

	switch bareCallKind(expr) {
	case "id", "size", "epochseconds", "epochmillis":
		return true
	}

	switch e := expr.(type) {
	case *cypher.Literal:
		if e == nil || e.Null {
			return false
		}
		switch e.Value.(type) {
		case int64, uint64, float64:
			return true
		default:
			return false
		}
	case *cypher.Variable:
		return e != nil && numericScalars[e.Symbol]
	case *cypher.ArithmeticExpression:
		if e == nil || !isStaticallyNumericScalar(e.Left, numericScalars) {
			return false
		}
		for _, p := range e.Partials {
			if p == nil || !isStaticallyNumericScalar(p.Right, numericScalars) {
				return false
			}
		}
		return true
	case *cypher.UnaryAddOrSubtractExpression:
		return e != nil && isStaticallyNumericScalar(e.Right, numericScalars)
	default:
		return false
	}
}
