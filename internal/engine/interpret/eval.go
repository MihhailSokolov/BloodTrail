// SPDX-License-Identifier: Apache-2.0

// eval.go implements the expression evaluator itself,
// walking the dawgs Cypher AST (github.com/specterops/dawgs@v0.8.0
// cypher/models/cypher) directly against one snapshot.View and one bound
// Row, and routing every comparison through value.go's own primitives
// (Tri, StringEq/StringNeq/ScalarEq/PropEq, StringPredicate, In, OrderCompare,
// IsNull/IsNotNull) rather than re-deriving pg's semantics here.
//
// Two entry points are exported: EvalPredicate for boolean (WHERE-clause)
// context, returning a Tri, and EvalValue for scalar/projection context,
// returning a (value, present, error) triple in the same post-JSON value
// model value.go's own package doc comment describes (nil | string | float64 | bool |
// []any | map[string]any, with a separate present/ok flag distinguishing
// absence from a stored JSON null).

package interpret

import (
	"errors"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// ErrUnsupported is returned for an AST shape this evaluator does not
// recognize -- a construct the planner/gate is expected to
// have already rejected before a query ever reaches interpretation. It is
// exported so callers can distinguish "genuinely can't do this locally"
// (this, or ErrCollation) from an outright bug, but reaching it in
// production is a gate bug, not an expected runtime outcome: this package
// never panics on an unrecognized node, it returns ErrUnsupported instead.
var ErrUnsupported = errors.New("interpret: unsupported expression")

// EdgeRef identifies one directed edge instance bound to a Row. Exactly one
// of its two fields is ever meaningful for a given EdgeRef, decided by
// whichever mode the producing Env.Snap was in at the moment the EdgeRef was
// built (a single query execution runs against one Env.Snap throughout, so
// this never varies mid-row):
//
//   - Fwd is a forward-CSR slot: Snap.OutTargets[Fwd]/Snap.OutKinds[Fwd]/
//     Snap.OutEdgeIDs[Fwd] all describe the same edge. Meaningful whenever
//     Env.Snap.Overlay() was false. A reverse-CSR-discovered edge (e.g.
//     found while walking In(n)) is expected to have already been
//     translated to its forward index via Snap.InEdgeIdx by whatever
//     produced the Row -- this package always reads a !Overlay() edge
//     through the forward arrays.
//   - EdgeID is the edge's own database id, used whenever Env.Snap.Overlay()
//     was true: a delta-added or delta-upserted edge has no forward-CSR
//     slot to name at all (snapshot.View.EdgeByID's own doc), so the
//     adjacency walk that discovers it (exec.go's adjacency,
//     expand.go's convertPath) carries the id snapshot.View.OutEdges/
//     InEdges already yielded, rather than recomputing a slot that may not
//     exist.
//
// DatabaseID and Kind below resolve either representation transparently,
// given the same Env.Snap the EdgeRef was produced against; every other
// reader of an EdgeRef in this package and in package engine (serve_cypher.go)
// goes through one of them (or repeats the same Overlay()-guarded pair
// inline, for the one case -- evalIdentityEquality's edge-equality branch --
// where the original field comparison needed to survive unchanged for
// !Overlay() rather than route through a shared helper).
type EdgeRef struct {
	Fwd    uint64
	EdgeID uint64
}

// DatabaseID returns e's database edge id, resolved against snap -- see
// EdgeRef's own doc for which field that means reading.
func (e EdgeRef) DatabaseID(snap *snapshot.View) uint64 {
	if snap.Overlay() {
		return e.EdgeID
	}
	return snap.Base().OutEdgeIDs[e.Fwd]
}

// Kind returns e's edge kind, resolved against snap the same overlay-aware
// way DatabaseID does. Under Overlay(), this resolves through
// snap.EdgeStateByID(e.EdgeID) -- always a hit for an EdgeRef this package
// itself produced (e was minted from a live OutEdges/InEdges yield against
// this exact snap, so the edge is by construction still live and
// resolvable); the zero KindID a miss would otherwise return is left
// unguarded here for the same reason convertPath's own doc gives for not
// defending against a case that cannot arise from this package's own
// production.
func (e EdgeRef) Kind(snap *snapshot.View) snapshot.KindID {
	if snap.Overlay() {
		_, _, kind, _ := snap.EdgeStateByID(e.EdgeID)
		return kind
	}
	return snap.Base().OutKinds[e.Fwd]
}

// Row binds one MATCH solution's pattern variables to concrete snapshot
// values: dense node ids, edge references, path values (a seam left open
// deliberately -- nothing materialized paths when this was written, so
// PathVar's value type is just `any`), and plain scalars (e.g. bound by a
// future UNWIND/WITH implementation, or by a caller pre-seeding a computed
// value). All four namespaces are separate maps rather than one
// `map[string]any`, matching how a Cypher planner would keep them: which
// namespace a symbol lives in is a property of the query's pattern, not
// something this package needs to infer from the stored value's shape.
type Row struct {
	// The four binding namespaces are association SLICES, not maps, for the
	// reason usedEdges below already gives for itself: a row binds the
	// symbols of one pattern, which is a handful, and at that size a linear
	// scan over contiguous memory beats hashing. The difference that made
	// this worth changing is allocation, not lookup -- a lazily-created map
	// costs a header plus its first bucket, so a freshly bound one-symbol
	// row cost three allocations where it now costs one.
	//
	// That cost is paid per CANDIDATE, not per result: scanAnchorVisit binds
	// a row for every node a candidate source yields, and a wide anchor
	// feeding a selective step discards nearly all of them. Measured on the
	// benchmark graph's "All Global Administrators" shape -- 84k AZBase
	// nodes seeding a step whose edge kind has exactly ONE edge in the whole
	// graph -- Row construction was 76% of the query's allocations.
	//
	// Set overwrites in place when the symbol is already bound, so these
	// carry the same one-value-per-symbol semantics a map did. Iteration
	// order is now insertion order rather than random; nothing depends on
	// it (cloneRow and mergeRowInto are this package's only iterators, and
	// both build a complete copy).
	nodes   []nodeBinding
	edges   []edgeBinding
	paths   []anyBinding
	scalars []anyBinding

	// usedEdges records the forward-CSR index of every edge any Step has
	// bound while constructing this row, regardless of whether that Step
	// named it (SetEdge above is keyed by symbol, purely for RETURN/WHERE
	// value lookup, and is never populated for an anonymous relationship
	// pattern at all) -- markEdgeUsed/edgeUsed below serve a different,
	// narrower purpose: exec.go's verifyClosingStep consults this to
	// enforce Cypher's relationship-uniqueness rule (no two Steps of the
	// same pattern may resolve to the identical relationship) for exactly
	// the one shape that rule can silently violate here -- a "closing" Step
	// completing a cycle back to an already-bound node (see
	// verifyClosingStep's own doc). A plain slice, not a map: a matched
	// row's own hop count is always small (this executor's variable-length
	// depth cap, MaxExpansionDepth, is 15), so a linear scan in edgeUsed
	// costs less than a map's hashing overhead would, and a plain append in
	// markEdgeUsed needs no lazy-allocate-on-first-use dance the way the
	// map-valued fields above do.
	usedEdges []uint64

	// trailEdges records the same overlay-aware edge identities as usedEdges
	// (see candidateIdentity), but for edges consumed by a variable-length
	// step's own emitted trail rather than by a fixed step. The two sets are
	// deliberately separate because dawgs' cross-step relationship-uniqueness
	// constraints are asymmetric (pinned from dawgs@v0.8.0
	// translate/traversal.go's previousRelationshipUniquenessConstraint /
	// expansionPreviousRelationshipUniquenessConstraint, and verified against
	// its generated SQL): a fixed step may reuse neither another fixed step's
	// edge (`e1.id != e0.id`) nor any edge inside a variable-length step's
	// trail (`e1.id != all (path)`), and a trail may not reuse a fixed step's
	// edge (`e0 != all (path)` emitted from the fixed step's side in both
	// orders) -- but two variable-length steps' trails MAY share an edge (the
	// constraint builders skip a preceding expansion entirely). So fixed-step
	// expansion checks BOTH sets, while the trail DFS checks only usedEdges
	// and records into trailEdges.
	trailEdges []uint64
}

// nodeBinding, edgeBinding and anyBinding are one symbol's entry in a Row's
// corresponding namespace -- see Row's own doc for why these are slices.
type nodeBinding struct {
	sym string
	id  snapshot.NodeID
}

type edgeBinding struct {
	sym string
	ref EdgeRef
}

type anyBinding struct {
	sym string
	val any
}

// NewRow returns an empty Row ready for SetNode/SetEdge/SetPathVar/
// SetScalar.
func NewRow() *Row {
	return &Row{}
}

// SetNode binds sym to a node's dense NodeID, replacing any previous
// binding for sym.
func (r *Row) SetNode(sym string, id snapshot.NodeID) {
	for i := range r.nodes {
		if r.nodes[i].sym == sym {
			r.nodes[i].id = id
			return
		}
	}
	r.nodes = append(r.nodes, nodeBinding{sym: sym, id: id})
}

// Node returns the dense NodeID bound to sym, and whether sym is bound as a
// node variable at all.
func (r *Row) Node(sym string) (snapshot.NodeID, bool) {
	for i := range r.nodes {
		if r.nodes[i].sym == sym {
			return r.nodes[i].id, true
		}
	}
	return 0, false
}

// SetEdge binds sym to an edge reference, replacing any previous binding
// for sym.
func (r *Row) SetEdge(sym string, ref EdgeRef) {
	for i := range r.edges {
		if r.edges[i].sym == sym {
			r.edges[i].ref = ref
			return
		}
	}
	r.edges = append(r.edges, edgeBinding{sym: sym, ref: ref})
}

// Edge returns the EdgeRef bound to sym, and whether sym is bound as an edge
// variable at all.
func (r *Row) Edge(sym string) (EdgeRef, bool) {
	for i := range r.edges {
		if r.edges[i].sym == sym {
			return r.edges[i].ref, true
		}
	}
	return EdgeRef{}, false
}

// SetPathVar binds sym to a path value, replacing any previous binding for
// sym. Nothing produced a path value when this was written, so v's shape is
// not defined by anything other than the caller; this exists purely so
// Row's namespace shape already carries the path slot, ahead of the code
// that populates it.
func (r *Row) SetPathVar(sym string, v any) {
	r.paths = setAnyBinding(r.paths, sym, v)
}

// PathVar returns the path value bound to sym, and whether sym is bound as a
// path variable at all.
func (r *Row) PathVar(sym string) (any, bool) {
	return getAnyBinding(r.paths, sym)
}

// SetScalar binds sym to a plain computed/bound value (e.g. an UNWIND
// element), in the same post-JSON value model as everything else in this
// package, replacing any previous binding for sym.
func (r *Row) SetScalar(sym string, v any) {
	r.scalars = setAnyBinding(r.scalars, sym, v)
}

// Scalar returns the scalar value bound to sym, and whether sym is bound as
// a scalar variable at all.
func (r *Row) Scalar(sym string) (any, bool) {
	return getAnyBinding(r.scalars, sym)
}

// setAnyBinding and getAnyBinding are the paths/scalars namespaces' shared
// bodies -- both are []anyBinding, so the scan is written once.
func setAnyBinding(bs []anyBinding, sym string, v any) []anyBinding {
	for i := range bs {
		if bs[i].sym == sym {
			bs[i].val = v
			return bs
		}
	}
	return append(bs, anyBinding{sym: sym, val: v})
}

func getAnyBinding(bs []anyBinding, sym string) (any, bool) {
	for i := range bs {
		if bs[i].sym == sym {
			return bs[i].val, true
		}
	}
	return nil, false
}

// markEdgeUsed records fwd (a forward-CSR index) as consumed by some Step
// while constructing this row -- see usedEdges' own doc for why this exists
// separately from SetEdge. Marking the same fwd more than once is harmless
// (edgeUsed only ever needs a yes/no answer, never a count).
func (r *Row) markEdgeUsed(fwd uint64) {
	r.usedEdges = append(r.usedEdges, fwd)
}

// edgeUsed reports whether fwd was already recorded via markEdgeUsed on this
// row or on whichever row it was cloned/merged from (cloneRow/mergeRowInto
// carry usedEdges forward like every other Row field).
func (r *Row) edgeUsed(fwd uint64) bool {
	for _, used := range r.usedEdges {
		if used == fwd {
			return true
		}
	}
	return false
}

// markTrailEdge records identity (the same overlay-aware value
// candidateIdentity produces) as consumed by a variable-length step's trail
// on this row -- see trailEdges' own doc for why this is a separate set from
// usedEdges. Like markEdgeUsed, re-marking the same identity is harmless.
func (r *Row) markTrailEdge(identity uint64) {
	r.trailEdges = append(r.trailEdges, identity)
}

// trailEdgeUsed reports whether identity was recorded via markTrailEdge on
// this row or on whichever row it was cloned/merged from. Same linear-scan
// reasoning as edgeUsed: a row's trail hop count is bounded by the
// variable-length depth cap, so the slice stays small.
func (r *Row) trailEdgeUsed(identity uint64) bool {
	for _, used := range r.trailEdges {
		if used == identity {
			return true
		}
	}
	return false
}

// Env carries the state one query's evaluation needs beyond the AST and the
// current Row: the snapshot to read node/edge/property data from, and Now --
// fixed once per query so every datetime().epochseconds/epochmillis call
// within the same query (across every row it evaluates) sees the same
// instant, exactly like a single pg statement's now().
//
// Env also owns a small regex compilation cache (regexCache, guarded by
// regexMu), populated lazily by evalRegexPredicate. See that function's doc
// comment for why this exists: without it, a `=~` predicate evaluated over
// many rows (the normal case -- one Env is constructed once per query and
// then handed to EvalPredicate/EvalValue once per candidate row) would
// recompile the same regexp.Regexp from source text on every single row.
// Both fields are safe to leave at their zero value; a nil map is treated as
// empty and lazily allocated on first use.
type Env struct {
	Snap *snapshot.View
	Now  time.Time

	regexMu    sync.Mutex
	regexCache map[string]*RegexMatcher
}

// compiledRegex returns a compiled, cached *regexp.Regexp for pattern,
// compiling and caching it on first use. See Env's doc comment for why this
// cache exists instead of compiling on every call the way value.go's own
// matchString (used for the STARTS WITH/ENDS WITH/CONTAINS operators, which
// have no compilation cost to amortize) deliberately still does.
func (e *Env) compiledRegex(pattern string) (*RegexMatcher, error) {
	e.regexMu.Lock()
	defer e.regexMu.Unlock()

	if re, ok := e.regexCache[pattern]; ok {
		return re, nil
	}
	re, err := NewRegexMatcher(pattern)
	if err != nil {
		return nil, err
	}
	if e.regexCache == nil {
		e.regexCache = make(map[string]*RegexMatcher)
	}
	e.regexCache[pattern] = re
	return re, nil
}

// --- EvalPredicate: boolean (WHERE-clause) context --------------------------

// EvalPredicate evaluates expr against row under env, returning Cypher's
// three-valued boolean result. Every AST shape this
// package documents itself as consuming is handled explicitly; anything else returns
// ErrUnsupported rather than panicking (see that sentinel's doc comment).
func EvalPredicate(env *Env, row *Row, expr cypher.Expression) (Tri, error) {
	switch typed := expr.(type) {
	case nil:
		return TriNull, nil

	case *cypher.Parenthetical:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return EvalPredicate(env, row, typed.Expression)

	case *cypher.Negation:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return evalNegation(env, row, typed.Expression)

	case *cypher.Conjunction:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return evalConjunction(env, row, typed.GetAll())

	case *cypher.Disjunction:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return evalDisjunction(env, row, typed.GetAll())

	case *cypher.ExclusiveDisjunction:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return evalExclusiveDisjunction(env, row, typed.GetAll())

	case *cypher.Comparison:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return evalComparison(env, row, typed)

	case *cypher.KindMatcher:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return evalKindMatcher(env, row, typed)

	case *cypher.PatternPredicate:
		if typed == nil {
			return TriNull, ErrUnsupported
		}
		return evalPatternPredicate(env, row, typed)

	default:
		// Anything else (a bare PropertyLookup, Variable, FunctionInvocation,
		// or Literal used directly as a WHERE conjunct, e.g. `WHERE n.flag`)
		// falls through to plain value truthiness: evaluate it as a value and
		// cast to boolean under pg's `(->>)::bool` rules -- see
		// evalValueTruthiness.
		return evalValueTruthiness(env, row, expr)
	}
}

// evalValueTruthiness implements the "bare property/expression as a WHERE
// conjunct" case: pg runs `(properties ->> 'flag')::bool`, which is NULL for
// an absent (or present-JSON-null) property, the property's own value for a
// present JSON boolean, and a genuine runtime cast error -- reproduced here
// as ErrRuntimeCast -- for any other present value (a number, string, array,
// or object never parses as a pg boolean).
func evalValueTruthiness(env *Env, row *Row, expr cypher.Expression) (Tri, error) {
	val, ok, err := EvalValue(env, row, expr)
	if err != nil {
		return TriNull, err
	}
	if !ok || val == nil {
		return TriNull, nil
	}
	b, isBool := val.(bool)
	if !isBool {
		return TriNull, ErrRuntimeCast
	}
	return boolToTri(b), nil
}

// evalConjunction folds AND over exprs starting from TriTrue (AND's
// identity), so an empty conjunction -- never produced by a real WHERE
// clause, but harmless to define -- is vacuously true.
func evalConjunction(env *Env, row *Row, exprs []cypher.Expression) (Tri, error) {
	result := TriTrue
	for _, e := range exprs {
		t, err := EvalPredicate(env, row, e)
		if err != nil {
			return TriNull, err
		}
		result = result.And(t)
	}
	return result, nil
}

// evalDisjunction folds OR over exprs starting from TriFalse (OR's
// identity).
func evalDisjunction(env *Env, row *Row, exprs []cypher.Expression) (Tri, error) {
	result := TriFalse
	for _, e := range exprs {
		t, err := EvalPredicate(env, row, e)
		if err != nil {
			return TriNull, err
		}
		result = result.Or(t)
	}
	return result, nil
}

// evalExclusiveDisjunction folds Cypher's XOR left-associatively:
// ((a XOR b) XOR c) XOR ..., matching Tri.Xor's own NULL-propagation (any
// NULL operand anywhere in the chain makes the whole fold sticky-NULL from
// that point on, which is what "XOR is a boolean (not SQL-total) operator"
// requires).
func evalExclusiveDisjunction(env *Env, row *Row, exprs []cypher.Expression) (Tri, error) {
	if len(exprs) == 0 {
		return TriFalse, nil
	}
	result, err := EvalPredicate(env, row, exprs[0])
	if err != nil {
		return TriNull, err
	}
	for _, e := range exprs[1:] {
		t, err := EvalPredicate(env, row, e)
		if err != nil {
			return TriNull, err
		}
		result = result.Xor(t)
	}
	return result, nil
}

// evalNegation implements NOT. Its one special case is a direct STARTS
// WITH/ENDS WITH/CONTAINS/=~ comparison (optionally wrapped in
// Parentheticals): per value.go's StringPredicate doc, `NOT x STARTS WITH y`
// is not `Tri.Not(x STARTS WITH y)` -- negating a NULL result would still be
// NULL, but Cypher wants a missing/NULL value to satisfy the negation. So
// that shape is evaluated with StringPredicate/evalRegexPredicate's own
// negated=true branch directly, rather than negating the positive Tri.
// Every other predicate shape negates the ordinary way: evaluate positively,
// then Tri.Not().
func evalNegation(env *Env, row *Row, expr cypher.Expression) (Tri, error) {
	if cmp, isCmp := unwrapParens(expr).(*cypher.Comparison); isCmp && cmp != nil && len(cmp.Partials) == 1 {
		if partial := cmp.Partials[0]; partial != nil {
			switch partial.Operator {
			case cypher.OperatorStartsWith, cypher.OperatorEndsWith, cypher.OperatorContains:
				return evalStringPredicate(env, row, cmp.Left, partial.Operator, partial.Right, true)
			case cypher.OperatorRegexMatch:
				return evalRegexComparison(env, row, cmp.Left, partial.Right, true)
			}
		}
	}

	t, err := EvalPredicate(env, row, expr)
	if err != nil {
		return TriNull, err
	}
	return t.Not(), nil
}

// unwrapParens peels away every layer of *cypher.Parenthetical wrapping
// expr, returning the first non-Parenthetical expression found (or expr
// itself, unchanged, if it's nil or already bare). Mirrors
// recognize/cypher.go's classifyConjunct.
func unwrapParens(expr cypher.Expression) cypher.Expression {
	for {
		paren, isParen := expr.(*cypher.Parenthetical)
		if !isParen || paren == nil {
			return expr
		}
		expr = paren.Expression
	}
}

// evalComparison evaluates a (possibly chained) Comparison: `a op1 b op2 c`
// desugars to `(a op1 b) AND (b op2 c)`, matching openCypher's chained
// comparison semantics. The overwhelmingly common case is a single Partial,
// for which this is exactly evalPartialComparison(cmp.Left, ...).
func evalComparison(env *Env, row *Row, cmp *cypher.Comparison) (Tri, error) {
	if len(cmp.Partials) == 0 {
		return TriNull, ErrUnsupported
	}

	result := TriTrue
	left := cmp.Left
	for _, partial := range cmp.Partials {
		if partial == nil {
			return TriNull, ErrUnsupported
		}
		t, err := evalPartialComparison(env, row, left, partial.Operator, partial.Right)
		if err != nil {
			return TriNull, err
		}
		result = result.And(t)
		left = partial.Right
	}
	return result, nil
}

// evalPartialComparison dispatches one `left op right` step to the value.go
// primitive its operator calls for, per this package's pinned routing table.
func evalPartialComparison(env *Env, row *Row, leftExpr cypher.Expression, op cypher.Operator, rightExpr cypher.Expression) (Tri, error) {
	switch op {
	case cypher.OperatorEquals, cypher.OperatorNotEquals:
		return evalEquality(env, row, leftExpr, op, rightExpr)

	case cypher.OperatorLessThan, cypher.OperatorLessThanOrEqualTo, cypher.OperatorGreaterThan, cypher.OperatorGreaterThanOrEqualTo:
		return evalOrder(env, row, leftExpr, op, rightExpr)

	case cypher.OperatorStartsWith, cypher.OperatorEndsWith, cypher.OperatorContains:
		return evalStringPredicate(env, row, leftExpr, op, rightExpr, false)

	case cypher.OperatorRegexMatch:
		return evalRegexComparison(env, row, leftExpr, rightExpr, false)

	case cypher.OperatorIn:
		return evalIn(env, row, leftExpr, rightExpr)

	case cypher.OperatorIs:
		val, ok, err := EvalValue(env, row, leftExpr)
		if err != nil {
			return TriNull, err
		}
		return IsNull(val, ok), nil

	case cypher.OperatorIsNot:
		val, ok, err := EvalValue(env, row, leftExpr)
		if err != nil {
			return TriNull, err
		}
		return IsNotNull(val, ok), nil

	default:
		return TriNull, ErrUnsupported
	}
}

// asLiteral reports whether expr (after unwrapping any Parentheticals) is a
// *cypher.Literal, per DAWGS' pgsql translator: whether a comparison's
// operand is syntactically a literal (vs. a property lookup or any other
// expression) is exactly what decides which equality primitive
// applies (StringEq/StringNeq for a string-literal counterpart, ScalarEq for
// any other literal, PropEq when neither side is a literal) -- not the
// runtime type of what it evaluates to.
func asLiteral(expr cypher.Expression) (*cypher.Literal, bool) {
	lit, ok := unwrapParens(expr).(*cypher.Literal)
	return lit, ok
}

// evalEquality implements `=`/`<>` per that same routing table:
// literal-vs-anything routes through evalLiteralComparison (which further
// splits on the literal's own type), a bare `<node var> op <node var>` (or
// the analogous edge-variable shape) routes through evalIdentityEquality
// (see its own doc comment for why), and anything else (property vs
// property, or either/both sides a computed function/arithmetic result)
// routes through PropEq, which NULLs on either side's absence and otherwise
// does raw structural equality -- correct for two computed values just as
// much as for two raw property lookups, since a computed value is never
// "absent" in the JSONB-column sense PropEq's ok flags exist for.
func evalEquality(env *Env, row *Row, leftExpr cypher.Expression, op cypher.Operator, rightExpr cypher.Expression) (Tri, error) {
	if rightLit, ok := asLiteral(rightExpr); ok {
		return evalLiteralComparison(env, row, leftExpr, op, rightLit)
	}
	if leftLit, ok := asLiteral(leftExpr); ok {
		return evalLiteralComparison(env, row, rightExpr, op, leftLit)
	}

	if t, ok := evalIdentityEquality(env, row, leftExpr, rightExpr); ok {
		if op == cypher.OperatorNotEquals {
			t = t.Not()
		}
		return t, nil
	}

	aVal, aOk, err := EvalValue(env, row, leftExpr)
	if err != nil {
		return TriNull, err
	}
	bVal, bOk, err := EvalValue(env, row, rightExpr)
	if err != nil {
		return TriNull, err
	}
	t := PropEq(aVal, aOk, bVal, bOk)
	if op == cypher.OperatorNotEquals {
		t = t.Not()
	}
	return t, nil
}

// evalIdentityEquality special-cases `<bound node var> = <bound node var>`
// (and the analogous edge-variable shape) as a NodeID/EdgeRef identity
// comparison, pre-empting evalEquality's generic PropEq path for exactly
// this shape. This matters because evalVariableValue's EvalValue result for
// a bare node variable is its full property map (needed for `RETURN n`'s own
// value shape -- see that function's doc comment), which has no defined
// equality semantics of its own and, critically, discards identity entirely:
// two textually distinct nodes that happen to carry identical property bags
// (the common case for a bare/property-less node, and not even a rare one
// for two real nodes that share every property) would otherwise compare
// "equal" via PropEq's structural jsonbEqual, when real Cypher/pg compares
// two node (or relationship) values by identity. This is exactly the shape
// shortestPath's own self-pair rule depends on (`WHERE s<>t`, s and t both
// bare node variables) -- and, like the identical "COLLECT-membership
// execution gap" pipeline.go documents, an instance of a pattern this
// codebase already flags: identity-based semantics over a node variable
// must be special-cased structurally before falling through to generic
// property-value comparison.
//
// ok is false (deferring to the generic PropEq path, unchanged) for every
// other shape -- either operand not a bare Variable, either operand not
// resolving to the same value namespace (both nodes, or both edges) in row,
// or a node compared against an edge.
func evalIdentityEquality(env *Env, row *Row, leftExpr, rightExpr cypher.Expression) (t Tri, ok bool) {
	lv, lIsVar := unwrapParens(leftExpr).(*cypher.Variable)
	rv, rIsVar := unwrapParens(rightExpr).(*cypher.Variable)
	if !lIsVar || !rIsVar || lv == nil || rv == nil {
		return TriFalse, false
	}
	if ln, lIsNode := row.Node(lv.Symbol); lIsNode {
		rn, rIsNode := row.Node(rv.Symbol)
		return boolToTri(rIsNode && ln == rn), rIsNode
	}
	if le, lIsEdge := row.Edge(lv.Symbol); lIsEdge {
		re, rIsEdge := row.Edge(rv.Symbol)
		if !env.Snap.Overlay() {
			return boolToTri(rIsEdge && le.Fwd == re.Fwd), rIsEdge
		}
		return boolToTri(rIsEdge && le.EdgeID == re.EdgeID), rIsEdge
	}
	return TriFalse, false
}

// evalLiteralComparison implements `otherExpr op lit` (or `lit op otherExpr`,
// pre-normalized by evalEquality so lit is always the literal side): a
// comparison against the literal `null` is always NULL under Cypher's
// three-valued logic regardless of otherExpr (there is no bare-`=` shortcut
// to IS NULL); a string literal routes through StringEq/StringNeq's
// jsonb_typeof guard; any other literal (number, bool, list, map) routes
// through ScalarEq, complemented for `<>` since pg's jsonb `=`/`<>` are
// simple complements of each other once collation isn't in play (ScalarEq is
// already total over present values -- see its doc comment).
//
// A NUMBER literal used to be a known, deliberately *not* handled, edge
// case here -- now guarded upstream instead (see below). dawgs' own pgsql
// translator compiles a *direct* `n.prop = <literal>`/`n.prop <> <literal>`
// via native jsonb comparison (`(n.properties -> 'prop')::jsonb = to_jsonb
// (<literal>)::jsonb`, matching ScalarEq's present-JSON-null-is-a-definite-
// value semantics) when <literal> is a bare positive numeric literal, but via
// text-extraction-then-cast (`(n.properties ->> 'prop')::float8 = <literal>`,
// which nulls out on a present JSON null exactly like a missing property)
// when <literal> is *negative* (a cypher.UnaryAddOrSubtractExpression
// wrapping the positive literal, not a plain cypher.Literal) -- confirmed by
// dumping translate.Translate's own generated SQL for both shapes. `<`/`<=`/
// `>`/`>=`/IN are unaffected (always the cast route, any sign -- see
// evalOrder/In's own already-correct null handling), and wrapping the
// property in a function call (coalesce()/size()/...) is unaffected too
// (verified the same way -- see plan.go's checkComparison doc).
//
// Note this function is never even reached for the negative-literal shape
// in the first place: a negative literal is a
// *cypher.UnaryAddOrSubtractExpression, which asLiteral above does not
// recognize, so evalEquality's generic PropEq/EvalValue path handles it
// instead -- a path that shares ScalarEq's same jsonbEqual primitive and
// therefore the same present-JSON-null-is-a-value semantics that only match
// pg's *positive*-literal (native jsonb) route. Reproducing dawgs' own
// sign-dependent split correctly in this evaluator would mean threading
// "was the literal AST a bare cypher.Literal or a Negation" all the way
// through evalEquality, just to imitate what looks like an accidental
// inconsistency in dawgs' own translator/optimizer rather than a real
// Cypher semantic.
//
// Fixed instead at plan time: interpret/plan.go's checkComparison rejects
// (delegates) any `=`/`<>` between a bare property lookup and a negative
// numeric literal, on either side, so this evaluator never actually sees
// the dangerous combination -- see that function's doc comment for the full
// derivation. random_cypher_differential_integration_test.go (repo root)
// originally discovered this comparing `n.val <> -100.0` against a fixture
// property deliberately mixing numbers with an explicit JSON null, and
// worked around it at the test-generator level (restricting `=`/`<>` to
// non-negative literals) before this plan-time reject existed; the
// generator now draws negative literals freely again, relying on
// delegation to make the differential assertion hold by construction.
func evalLiteralComparison(env *Env, row *Row, otherExpr cypher.Expression, op cypher.Operator, lit *cypher.Literal) (Tri, error) {
	if lit.Null {
		return TriNull, nil
	}

	val, ok, err := EvalValue(env, row, otherExpr)
	if err != nil {
		return TriNull, err
	}
	litVal, _, err := evalLiteralValue(lit)
	if err != nil {
		return TriNull, err
	}

	if s, isString := litVal.(string); isString {
		if op == cypher.OperatorEquals {
			return StringEq(val, ok, s), nil
		}
		return StringNeq(val, ok, s), nil
	}

	t := ScalarEq(val, ok, litVal)
	if op == cypher.OperatorNotEquals {
		t = t.Not()
	}
	return t, nil
}

// evalOrder implements `<`/`<=`/`>`/`>=`: an absent operand on either side is
// TriNull (pg: a comparison against SQL NULL is NULL) before OrderCompare is
// even consulted, since OrderCompare itself has no presence/absence concept
// (see its doc comment); otherwise OrderCompare decides, with
// ErrNotComparable folded to TriNull (a type mismatch/non-orderable pair is
// a genuine Cypher NULL, not a delegation signal) and ErrCollation bubbled
// as-is (string-vs-string ordering must delegate to pg's own collation).
func evalOrder(env *Env, row *Row, leftExpr cypher.Expression, op cypher.Operator, rightExpr cypher.Expression) (Tri, error) {
	aVal, aOk, err := EvalValue(env, row, leftExpr)
	if err != nil {
		return TriNull, err
	}
	bVal, bOk, err := EvalValue(env, row, rightExpr)
	if err != nil {
		return TriNull, err
	}
	if !aOk || !bOk {
		return TriNull, nil
	}

	c, err := OrderCompare(aVal, bVal)
	if err != nil {
		if errors.Is(err, ErrNotComparable) {
			return TriNull, nil
		}
		return TriNull, err
	}

	switch op {
	case cypher.OperatorLessThan:
		return boolToTri(c < 0), nil
	case cypher.OperatorLessThanOrEqualTo:
		return boolToTri(c <= 0), nil
	case cypher.OperatorGreaterThan:
		return boolToTri(c > 0), nil
	case cypher.OperatorGreaterThanOrEqualTo:
		return boolToTri(c >= 0), nil
	default:
		return TriNull, ErrUnsupported
	}
}

// evalStringPredicate implements STARTS WITH/ENDS WITH/CONTAINS, routing
// straight through value.go's StringPredicate: these three ops match via
// plain strings.HasPrefix/HasSuffix/Contains, with no per-call compilation
// cost to amortize the way regex has, so there is no need for the
// evalRegexPredicate/Env-cache treatment below.
func evalStringPredicate(env *Env, row *Row, leftExpr cypher.Expression, op cypher.Operator, rightExpr cypher.Expression, negated bool) (Tri, error) {
	val, ok, err := EvalValue(env, row, leftExpr)
	if err != nil {
		return TriNull, err
	}
	needle, err := literalStringValue(env, row, rightExpr)
	if err != nil {
		return TriNull, err
	}

	var sop StringOp
	switch op {
	case cypher.OperatorStartsWith:
		sop = OpStartsWith
	case cypher.OperatorEndsWith:
		sop = OpEndsWith
	case cypher.OperatorContains:
		sop = OpContains
	default:
		return TriNull, ErrUnsupported
	}
	return StringPredicate(sop, val, ok, needle, negated)
}

// evalRegexComparison implements `=~`. See evalRegexPredicate for why this
// does not route through value.go's StringPredicate the way the other three
// string predicates do.
func evalRegexComparison(env *Env, row *Row, leftExpr, rightExpr cypher.Expression, negated bool) (Tri, error) {
	val, ok, err := EvalValue(env, row, leftExpr)
	if err != nil {
		return TriNull, err
	}
	pattern, err := literalStringValue(env, row, rightExpr)
	if err != nil {
		return TriNull, err
	}
	return evalRegexPredicate(env, val, ok, pattern, negated)
}

// evalRegexPredicate is StringPredicate's regex case, called against a
// compiled pattern from Env's cache instead of a needle string that would be
// recompiled via regexp.Compile on every call.
//
// Design note (regex compilation seam): value.go's StringPredicate takes a
// needle string, not a compiled pattern, and recompiles via
// regexp.Compile on every call for OpRegex -- its own doc comment already
// flags this as "a performance concern for a future plan-execution loop"
// and explicitly declines to fix it there, since StringPredicate is a
// frozen, already-reviewed API and changing its signature would ripple into
// value_test.go for no change in observable behavior. But Env is exactly
// the object a real query's row loop constructs once and then threads
// through every row's EvalPredicate/EvalValue call, which makes it the
// natural place to cache
// a compiled pattern across that whole loop without needing a separate
// plan/prepare pass: the first row for a given regex literal compiles and
// caches it (keyed by source pattern text, via Env.compiledRegex), and every
// subsequent row -- for that predicate, or any other predicate in the same
// query reusing the same pattern text -- reuses the cached *regexp.Regexp.
// A future planner can still replace this with a prepare-once
// artifact attached to the plan node instead of a live cache keyed by
// pattern text; until then, this is the seam that avoids the unacceptable
// "compile once per row" cost.
//
// The null/absent/non-string/negation policy itself is NOT duplicated here:
// it lives in exactly one place, value.go's stringPredicateCore, shared by
// both StringPredicate (needle-string matching) and value.RegexPredicate
// (pre-compiled-matcher matching, which this function delegates to). A
// pattern that fails to compile yields a nil *regexp.Regexp, which
// RegexPredicate treats as unconditional no-match, exactly like
// value.go's matchString does for an uncompilable OpRegex needle -- Cypher
// regex literals are expected to be validated before a query reaches
// interpretation.
func evalRegexPredicate(env *Env, val any, ok bool, pattern string, negated bool) (Tri, error) {
	re, compileErr := env.compiledRegex(pattern)
	if compileErr != nil {
		re = nil
	}
	return RegexMatcherPredicate(re, val, ok, negated)
}

// literalStringValue evaluates expr and requires the result to be a present
// string, as STARTS WITH/ENDS WITH/CONTAINS/=~'s right-hand needle/pattern
// always is by the time a query reaches this evaluator (the future gate is
// expected to reject anything else at plan time) -- so anything other than a
// present string here is ErrUnsupported, not a runtime cast error.
func literalStringValue(env *Env, row *Row, expr cypher.Expression) (string, error) {
	val, ok, err := EvalValue(env, row, expr)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrUnsupported
	}
	s, isString := val.(string)
	if !isString {
		return "", ErrUnsupported
	}
	return s, nil
}

// evalIn implements Cypher's `x IN list`, evaluating both operands to values
// via EvalValue -- so list may be a literal ListLiteral or a list-valued
// property lookup equally well -- and delegating the membership decision to
// value.go's In. An absent (or present-JSON-null) right-hand side is not a
// shape In() itself models (it always receives a concrete []any), so it is
// handled here: pg's `x = ANY(NULL::anyarray)` is NULL, mirrored as TriNull.
func evalIn(env *Env, row *Row, leftExpr, rightExpr cypher.Expression) (Tri, error) {
	val, ok, err := EvalValue(env, row, leftExpr)
	if err != nil {
		return TriNull, err
	}
	listVal, listOk, err := EvalValue(env, row, rightExpr)
	if err != nil {
		return TriNull, err
	}
	if !listOk || listVal == nil {
		return TriNull, nil
	}
	list, isList := listVal.([]any)
	if !isList {
		return TriNull, ErrUnsupported
	}
	return In(val, ok, list)
}

// evalKindMatcher implements `n:Kind` (and edge kind matchers, `r:TYPE`,
// which the frontend parses the same way): the referenced variable's kind
// list is compared against km.Kinds per km.IsExclusive -- true (the shape
// the frontend always produces for a source-level `:A:B` matcher, see
// dawgs@v0.8.0 cypher/frontend/expression.go's KindMatcher construction)
// means "has ALL of these kinds" (Cypher's (n:A:B) semantics), false means
// "has ANY of these kinds" (the KindMatcher type's general PG @>-vs-&&
// contract, for a matcher this package might synthesize itself later). A
// kind name absent from Snap.Kinds is documented as unreachable post-plan
// (the future gate rejects an unknown label at plan time); reached anyway,
// it is simply treated as "this node/edge does not have that kind" rather
// than erroring, since KindMatcher's job is total over any well-formed node.
func evalKindMatcher(env *Env, row *Row, km *cypher.KindMatcher) (Tri, error) {
	v, isVar := unwrapParens(km.Reference).(*cypher.Variable)
	if !isVar || v == nil {
		return TriNull, ErrUnsupported
	}

	if nodeID, ok := row.Node(v.Symbol); ok {
		return matchKinds(env, env.Snap.KindIDsOf(nodeID), km.Kinds, km.IsExclusive), nil
	}
	if edgeRef, ok := row.Edge(v.Symbol); ok {
		return matchKinds(env, []snapshot.KindID{edgeRef.Kind(env.Snap)}, km.Kinds, km.IsExclusive), nil
	}
	return TriNull, ErrUnsupported
}

// matchKinds reports whether have (a node's or edge's own kind list)
// satisfies want under exclusive's ALL-vs-ANY semantics; see
// evalKindMatcher's doc comment.
func matchKinds(env *Env, have []snapshot.KindID, want graph.Kinds, exclusive bool) Tri {
	matched := 0
	for _, k := range want {
		kindID, found := env.Snap.Kinds().ID(k.String())
		if found && containsKindID(have, kindID) {
			matched++
		} else if exclusive {
			return TriFalse
		}
	}
	if exclusive {
		return TriTrue
	}
	return boolToTri(matched > 0)
}

func containsKindID(have []snapshot.KindID, k snapshot.KindID) bool {
	for _, h := range have {
		if h == k {
			return true
		}
	}
	return false
}

// evalPatternPredicate evaluates a WHERE-clause bare relationship pattern
// used as a boolean predicate -- `(n)-[:K]->(m)`, or negated via evalNegation's
// own generic "evaluate positively, then Tri.Not()" path -- as a pure
// existence check: does at least one edge whose kind is in rel.Kinds (empty
// meaning "any kind", edgeKindOK's own convention) connect the two
// row-bound endpoints in the pattern's declared direction?
//
// checkPatternPredicate (plan.go) has already validated pp's shape at plan
// time -- exactly one fixed-length step between two already-bound node
// variables, every named kind resolvable against this same snapshot -- so
// every failure mode below (a type assertion failing, row.Node missing a
// symbol, a kind name not resolving) is expected to be unreachable against
// any Query this package's own Plan produced; each still returns
// ErrUnsupported defensively (delegate to PostgreSQL) rather than panic,
// matching this file's own established convention (e.g. evalKindMatcher)
// of never trusting an unconditional type assertion in evaluator code.
//
// The result is always TriTrue or TriFalse, never TriNull: a pattern
// predicate's existence check has nothing to be NULL about (no property
// lookup is ever involved -- only
// row-bound node identity and a snapshot-resolved kind mask), matching
// pg's own translation, which lowers this to `EXISTS(SELECT 1 FROM edge
// WHERE ...)` -- a SQL boolean, never SQL NULL.
func evalPatternPredicate(env *Env, row *Row, pp *cypher.PatternPredicate) (Tri, error) {
	if len(pp.PatternElements) != 3 {
		return TriNull, ErrUnsupported
	}
	fromNode, isNode := pp.PatternElements[0].Element.(*cypher.NodePattern)
	rel, isRel := pp.PatternElements[1].Element.(*cypher.RelationshipPattern)
	toNode, isNode2 := pp.PatternElements[2].Element.(*cypher.NodePattern)
	if !isNode || !isRel || !isNode2 || fromNode == nil || rel == nil || toNode == nil {
		return TriNull, ErrUnsupported
	}
	kinds := make([]snapshot.KindID, 0, len(rel.Kinds))
	for _, k := range rel.Kinds {
		id, found := env.Snap.Kinds().ID(k.String())
		if !found {
			return TriNull, ErrUnsupported
		}
		kinds = append(kinds, id)
	}

	// One endpoint may be an ANONYMOUS node carrying kind labels -- the
	// `(:Group)` in `WHERE NOT (u)-[:MemberOf]->(:Group)`. It binds nothing,
	// so the predicate is the existential pg lowers it to ("is there an edge
	// from u to SOME node labelled Group"), reached by walking the bound
	// side's adjacency and testing each neighbour's labels instead of
	// probing one already-known pair. A NAMED fresh variable is still
	// rejected at plan time, because that one really would have to bind.
	fromBound, fromOK := patternEndpointNode(row, fromNode)
	toBound, toOK := patternEndpointNode(row, toNode)

	switch {
	case fromOK && toOK:
		return boolToTri(adjacentInDirection(env, rel.Direction, fromBound, toBound, kinds)), nil
	case fromOK:
		return boolToTri(hasKindedNeighbor(env, fromBound, rel.Direction, nodeKindIDs(env, toNode), kinds)), nil
	case toOK:
		return boolToTri(hasKindedNeighbor(env, toBound, reverseDirection(rel.Direction), nodeKindIDs(env, fromNode), kinds)), nil
	default:
		return TriNull, ErrUnsupported
	}
}

// patternEndpointNode resolves a pattern-predicate endpoint to the node the
// row already bound it to, or reports that it is not a bound variable.
func patternEndpointNode(row *Row, np *cypher.NodePattern) (snapshot.NodeID, bool) {
	if np.Variable == nil || np.Variable.Symbol == "" {
		return 0, false
	}
	return row.Node(np.Variable.Symbol)
}

// nodeKindIDs resolves a pattern node's kind labels to snapshot ids. A label
// this snapshot has never interned yields the sentinel "no node can match"
// value, which is correct rather than an error: nothing carries a kind that
// does not exist. nil (no labels at all) means "any node".
func nodeKindIDs(env *Env, np *cypher.NodePattern) []snapshot.KindID {
	if len(np.Kinds) == 0 {
		return nil
	}
	out := make([]snapshot.KindID, 0, len(np.Kinds))
	for _, k := range np.Kinds {
		id, found := env.Snap.Kinds().ID(k.String())
		if !found {
			return []snapshot.KindID{kindIDNoMatch}
		}
		out = append(out, id)
	}
	return out
}

// kindIDNoMatch is a kind id no node can carry, used to represent a pattern
// label this snapshot never interned. Kind ids are assigned densely upward
// from zero, so a negative one is unreachable by construction.
const kindIDNoMatch = snapshot.KindID(-1)

// reverseDirection flips a relationship direction so a predicate anchored on
// its OTHER endpoint walks the correct adjacency. DirectionBoth is its own
// reverse.
func reverseDirection(d graph.Direction) graph.Direction {
	switch d {
	case graph.DirectionOutbound:
		return graph.DirectionInbound
	case graph.DirectionInbound:
		return graph.DirectionOutbound
	default:
		return d
	}
}

// adjacentInDirection is the both-endpoints-bound probe, unchanged: does an
// admissible edge connect these two specific nodes the declared way round?
func adjacentInDirection(env *Env, dir graph.Direction, from, to snapshot.NodeID, kinds []snapshot.KindID) bool {
	switch dir {
	case graph.DirectionOutbound:
		return hasAdjacentEdge(env, from, to, kinds)
	case graph.DirectionInbound:
		return hasAdjacentEdge(env, to, from, kinds)
	case graph.DirectionBoth:
		return hasAdjacentEdge(env, from, to, kinds) || hasAdjacentEdge(env, to, from, kinds)
	default:
		return false
	}
}

// hasKindedNeighbor reports whether src has at least one edge, in dir and of
// an admissible edge kind, to a node carrying every label in nodeKinds (nil
// nodeKinds admitting any node). It stops at the first hit, so the common
// answer on a dense graph costs one edge.
func hasKindedNeighbor(env *Env, src snapshot.NodeID, dir graph.Direction, nodeKinds, edgeKinds []snapshot.KindID) bool {
	match := func(n snapshot.NodeID, k snapshot.KindID) bool {
		return edgeKindOK(edgeKinds, k) && nodeHasAllKinds(env, n, nodeKinds)
	}
	found := false
	visit := func(n snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
		if match(n, k) {
			found = true
			return false
		}
		return true
	}
	if dir == graph.DirectionOutbound || dir == graph.DirectionBoth {
		env.Snap.OutEdges(src, visit)
	}
	if !found && (dir == graph.DirectionInbound || dir == graph.DirectionBoth) {
		env.Snap.InEdges(src, visit)
	}
	return found
}

// nodeHasAllKinds reports whether n carries every kind in kinds -- Cypher's
// `(n:A:B)` AND semantics. An empty list admits every node.
func nodeHasAllKinds(env *Env, n snapshot.NodeID, kinds []snapshot.KindID) bool {
	if len(kinds) == 0 {
		return true
	}
	have := env.Snap.KindIDsOf(n)
	for _, want := range kinds {
		ok := false
		for _, h := range have {
			if h == want {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

// hasAdjacentEdge reports whether src has at least one outgoing edge to dst
// whose kind is in kinds (edgeKindOK's own "empty means any" convention,
// reused directly so this file and exec.go's adjacency() never disagree on
// what "kind matches" means): the same (start, end, kind) forward-CSR data
// adjacency() walks to grow a row's candidate set, but read directly here
// as a targeted probe against one already-known (src, dst) pair rather than
// a full candidate enumeration -- a pattern predicate never produces a new,
// unbound row the way a MATCH step's own adjacency walk does, so
// adjacency()'s row-growing machinery (and the *workMeter it requires) does
// not apply; see evalPatternPredicate's own doc for why this file's other
// evaluator functions are consistently unmetered.
func hasAdjacentEdge(env *Env, src, dst snapshot.NodeID, kinds []snapshot.KindID) bool {
	if !env.Snap.Overlay() {
		targets, edgeKinds, _ := env.Snap.Out(src)
		for i, t := range targets {
			if t == dst && edgeKindOK(kinds, edgeKinds[i]) {
				return true
			}
		}
		return false
	}

	found := false
	env.Snap.OutEdges(src, func(t snapshot.NodeID, k snapshot.KindID, _ uint64) bool {
		if t == dst && edgeKindOK(kinds, k) {
			found = true
			return false
		}
		return true
	})
	return found
}

// --- EvalValue: scalar/projection context -----------------------------------

// EvalValue evaluates expr to a value in this package's post-JSON value
// model. ok is false only when expr denotes an absent property lookup (or an
// expression that itself bottoms out in one, e.g. tolower() of an absent
// property); a present JSON null is (nil, true, nil), matching PropStore's
// own contract.
func EvalValue(env *Env, row *Row, expr cypher.Expression) (val any, ok bool, err error) {
	switch typed := expr.(type) {
	case nil:
		return nil, false, nil

	case *cypher.Literal:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return evalLiteralValue(typed)

	case *cypher.Parenthetical:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return EvalValue(env, row, typed.Expression)

	case *cypher.Variable:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return evalVariableValue(env, row, typed)

	case *cypher.PropertyLookup:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return evalPropertyLookup(env, row, typed)

	case *cypher.ListLiteral:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return evalListLiteral(env, row, typed)

	case *cypher.FunctionInvocation:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return evalFunction(env, row, typed)

	case *cypher.ArithmeticExpression:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return evalArithmetic(env, row, typed)

	case *cypher.UnaryAddOrSubtractExpression:
		if typed == nil {
			return nil, false, ErrUnsupported
		}
		return evalUnary(env, row, typed)

	default:
		return nil, false, ErrUnsupported
	}
}

// evalLiteralValue converts a parsed *cypher.Literal into this package's
// value model. The frontend hands string literals to us in Cypher source
// form -- the raw token including surrounding quotes, with escape sequences
// left un-decoded (see cypher.Literal's doc comment in
// dawgs@v0.8.0/cypher/models/cypher/model.go) -- so a string value is
// decoded via decodeCypherStringLiteral first. Integers arrive as int64,
// doubles as float64 (dawgs@v0.8.0 cypher/frontend/literal.go); both become
// float64 here so a literal number compares equal (via ScalarEq/PropEq's
// jsonbEqual) to a PropStore-decoded JSON number regardless of which Go
// numeric type produced it, matching pg's own scale-insensitive numeric
// equality.
func evalLiteralValue(lit *cypher.Literal) (any, bool, error) {
	if lit.Null {
		return nil, true, nil
	}
	switch v := lit.Value.(type) {
	case string:
		s, err := decodeCypherStringLiteral(v)
		if err != nil {
			return nil, false, err
		}
		return s, true, nil
	case int64:
		return float64(v), true, nil
	case uint64:
		return float64(v), true, nil
	case float64:
		return v, true, nil
	case bool:
		return v, true, nil
	default:
		return nil, false, ErrUnsupported
	}
}

// decodeCypherStringLiteral decodes a Cypher string literal from the raw
// source-form token the frontend stores in Literal.Value (surrounding quote
// characters intact, escape sequences un-decoded) into the literal string
// value it denotes.
//
// This is a deliberate port of dawgs@v0.8.0
// cypher/models/pgsql/translate/translator.go's own unexported
// decodeCypherStringLiteral, line for line: that function is what DAWGS'
// own pgsql translator calls on exactly this same raw Literal.Value before
// embedding a string literal into generated SQL, so reproducing it here
// (rather than writing an independent decoder that might disagree on some
// escape) is what makes this package's answer match what pg would have
// computed from the same query text. It is unexported in that package, so it
// cannot be imported directly.
func decodeCypherStringLiteral(raw string) (string, error) {
	if len(raw) < 2 {
		return "", ErrUnsupported
	}
	quote := raw[0]
	if (quote != '\'' && quote != '"') || raw[len(raw)-1] != quote {
		return "", ErrUnsupported
	}

	body := raw[1 : len(raw)-1]
	var b strings.Builder
	b.Grow(len(body))

	for i := 0; i < len(body); i++ {
		if body[i] != '\\' {
			b.WriteByte(body[i])
			continue
		}
		if i+1 >= len(body) {
			return "", ErrUnsupported
		}
		switch c := body[i+1]; c {
		case '\\', '\'', '"':
			b.WriteByte(c)
			i++
		case 'b', 'B':
			b.WriteByte('\b')
			i++
		case 'f', 'F':
			b.WriteByte('\f')
			i++
		case 'n', 'N':
			b.WriteByte('\n')
			i++
		case 'r', 'R':
			b.WriteByte('\r')
			i++
		case 't', 'T':
			b.WriteByte('\t')
			i++
		default:
			return "", ErrUnsupported
		}
	}
	return b.String(), nil
}

// evalVariableValue evaluates a bare Variable used as a value (as opposed to
// via a PropertyLookup): a scalar binding (e.g. from a future UNWIND) wins
// first if present, then a node variable materializes to its full property
// map (the shape a Cypher RETURN n would need). Edge and path variables have
// no defined "materialize as a value" shape here (no edge-property store
// exists in the snapshot, and no path type existed when this was written),
// so both are ErrUnsupported for now.
func evalVariableValue(env *Env, row *Row, v *cypher.Variable) (any, bool, error) {
	if val, ok := row.Scalar(v.Symbol); ok {
		return val, true, nil
	}
	if nodeID, ok := row.Node(v.Symbol); ok {
		return env.Snap.PropNodeMap(nodeID), true, nil
	}
	return nil, false, ErrUnsupported
}

// evalPropertyLookup evaluates `atom.prop`. Two atom shapes are recognized:
// datetime().epochseconds/.epochmillis (see below), and a plain node
// variable's property, read from Snap.Props. An edge variable's property
// lookup is ErrUnsupported: the snapshot's PropStore only models node
// property bags (see props.go), and nothing in this package's supported
// query set calls for relationship properties, so there is nowhere to read r.prop from yet.
func evalPropertyLookup(env *Env, row *Row, pl *cypher.PropertyLookup) (any, bool, error) {
	atom := unwrapParens(pl.Atom)

	if fi, isFunc := atom.(*cypher.FunctionInvocation); isFunc && fi != nil && strings.ToLower(fi.Name) == cypher.DateTimeFunction {
		return evalDateTimeComponent(env, pl.Symbol)
	}

	v, isVar := atom.(*cypher.Variable)
	if !isVar || v == nil {
		return nil, false, ErrUnsupported
	}

	if nodeID, ok := row.Node(v.Symbol); ok {
		// Resolved BY NAME, deliberately, rather than through the
		// PropIDByName+PropValue pair: a PropID only ever names something the
		// BASE snapshot's own PropStore interned at load time, so a property
		// name that first appears in a delta segment -- a brand-new key a
		// written-through update added to a node -- has no PropID at all and
		// would read back as absent (View.PropIDByName's own doc). Resolving
		// by name answers correctly for both, and costs the same single map
		// lookup on the base path.
		val, ok := env.Snap.PropValueByName(nodeID, pl.Symbol)
		return val, ok, nil
	}

	if _, ok := row.Edge(v.Symbol); ok {
		return nil, false, ErrUnsupported
	}

	return nil, false, ErrUnsupported
}

// evalDateTimeComponent implements datetime().epochseconds/.epochmillis:
// the only two ITTC (instant-type/temporal-component) accessors this
// package supports, since the gate is expected to reject any other
// datetime() component at plan time (before it ever reaches this
// evaluator). Both derive from Env.Now, fixed once per query.
//
// Numeric representation note: the natural reading of these accessors is
// that they return "int64", and evalIDFunction's doc comment carries the
// same note -- see it for the full rationale. In short: this package's value model (value.go's
// own doc comment) is "every JSON number is float64", which is what lets a
// single set of primitives (ScalarEq/PropEq/OrderCompare/In, all frozen
// value.go code that only type-switches on float64) work uniformly over every
// numeric value regardless of where it came from. Handing back a literal Go
// int64 here would silently opt these two accessors out of that model --
// e.g. `n.lastlogontimestamp < (datetime().epochseconds - (60 * 86400))`,
// one of this package's own named test scenarios, requires the epochseconds
// value to flow through evalArithmetic and then OrderCompare, both of which
// only recognize float64. So this returns float64, trading away exact int64
// precision above 2^53 (~9e15) for internal consistency; Unix epoch
// seconds/millis are nowhere near that range within any plausible use of
// this system, so nothing meaningful is lost in practice.
func evalDateTimeComponent(env *Env, symbol string) (any, bool, error) {
	switch strings.ToLower(symbol) {
	case cypher.ITTCEpochSeconds:
		return float64(env.Now.Unix()), true, nil
	case cypher.ITTCEpochMilliseconds:
		return float64(env.Now.UnixMilli()), true, nil
	default:
		return nil, false, ErrUnsupported
	}
}

// evalListLiteral evaluates each element of a ListLiteral via EvalValue. An
// absent element (only possible if a list literal somehow embedded a
// property lookup, e.g. `[n.a, n.b]`) is represented as JSON null in the
// resulting slice -- there is no per-element presence flag in a materialized
// list's own value-model shape (a []any element is always "present", it can
// just be nil).
func evalListLiteral(env *Env, row *Row, list *cypher.ListLiteral) (any, bool, error) {
	out := make([]any, 0, len(*list))
	for _, e := range *list {
		v, ok, err := EvalValue(env, row, e)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			v = nil
		}
		out = append(out, v)
	}
	return out, true, nil
}

// evalFunction dispatches a FunctionInvocation by its lower-cased name,
// mirroring pg (Cypher function names are case-insensitive; DAWGS' own
// translator lower-cases before matching against the same
// cypher.XxxFunction name constants used below -- see
// dawgs@v0.8.0/cypher/models/cypher/functions.go). Any function not in the
// package's supported list is ErrUnsupported: the future gate is expected to
// have already rejected it (or, for size(string) specifically, to delegate
// the whole query to pg instead -- see evalSizeFunction).
func evalFunction(env *Env, row *Row, fi *cypher.FunctionInvocation) (any, bool, error) {
	switch strings.ToLower(fi.Name) {
	case cypher.IdentityFunction:
		return evalIDFunction(env, row, fi)
	case cypher.NodeLabelsFunction:
		return evalLabelsFunction(env, row, fi)
	case cypher.EdgeTypeFunction:
		return evalTypeFunction(env, row, fi)
	case cypher.ToLowerFunction:
		return evalCaseFunction(env, row, fi, false)
	case cypher.ToUpperFunction:
		return evalCaseFunction(env, row, fi, true)
	case cypher.CoalesceFunction:
		return evalCoalesce(env, row, fi)
	case cypher.ListSizeFunction:
		return evalSizeFunction(env, row, fi)
	case cypher.StringSplitToArrayFunction:
		return evalSplitFunction(env, row, fi)
	default:
		return nil, false, ErrUnsupported
	}
}

// singleVariableArg extracts fi's sole argument as a *cypher.Variable, for
// the functions (id, labels, type) that only ever take one bare pattern
// variable.
func singleVariableArg(fi *cypher.FunctionInvocation) (*cypher.Variable, bool) {
	if len(fi.Arguments) != 1 {
		return nil, false
	}
	v, isVar := unwrapParens(fi.Arguments[0]).(*cypher.Variable)
	if !isVar || v == nil {
		return nil, false
	}
	return v, true
}

// evalIDFunction implements id(): a node variable's id is its database node
// id (Snap.GraphIDs[dense id]); an edge variable's id is its database edge
// id (Snap.OutEdgeIDs[forward slot]).
//
// Numeric representation note (a deliberate deviation): id() is naturally
// read as returning "int64". This evaluator
// instead returns float64, deliberately keeping id() inside this package's
// one uniform numeric representation -- value.go's doc comment states the
// value model is "exactly one of nil, string, float64, bool, []any, or
// map[string]any", and every comparison/arithmetic primitive in that file
// (ScalarEq, PropEq, OrderCompare, In, jsonbEqual) type-switches on float64
// for numbers, with no int64 case. Returning a real int64 here would make
// `WHERE id(n) = 5` silently fail (jsonbEqual's default case returns false
// for an unrecognized dynamic type, since int64 isn't float64) and would
// make id(n) unusable in arithmetic -- caught concretely by this package's
// own `datetime().epochseconds - ...` test scenario needing the same
// treatment.
// So id() and datetime()'s epoch accessors both normalize to float64,
// trading exact precision above 2^53 (~9e15) for correctness everywhere
// else in the package; no realistic BloodHound database id gets close to
// that range.
func evalIDFunction(env *Env, row *Row, fi *cypher.FunctionInvocation) (any, bool, error) {
	v, ok := singleVariableArg(fi)
	if !ok {
		return nil, false, ErrUnsupported
	}
	if nodeID, ok := row.Node(v.Symbol); ok {
		return float64(env.Snap.GraphID(nodeID)), true, nil
	}
	if edgeRef, ok := row.Edge(v.Symbol); ok {
		return float64(edgeRef.DatabaseID(env.Snap)), true, nil
	}
	return nil, false, ErrUnsupported
}

// evalLabelsFunction implements labels(n): the node's kind ids, resolved to
// names via Snap.Kinds, in the same order they appear in the node's own
// kind_ids slice (Snap.NodeKinds[lo:hi]) -- no sorting or deduplication, so
// the result's order matches whatever order the node was originally built
// with (see snapshot.Builder.AddNode's kinds argument).
func evalLabelsFunction(env *Env, row *Row, fi *cypher.FunctionInvocation) (any, bool, error) {
	v, ok := singleVariableArg(fi)
	if !ok {
		return nil, false, ErrUnsupported
	}
	nodeID, ok := row.Node(v.Symbol)
	if !ok {
		return nil, false, ErrUnsupported
	}

	kinds := env.Snap.KindIDsOf(nodeID)
	out := make([]any, 0, len(kinds))
	for _, k := range kinds {
		if name, found := env.Snap.Kinds().Name(k); found {
			out = append(out, name)
		}
	}
	return out, true, nil
}

// evalTypeFunction implements type(r): the edge's single kind, resolved to
// its name via Snap.Kinds.
func evalTypeFunction(env *Env, row *Row, fi *cypher.FunctionInvocation) (any, bool, error) {
	v, ok := singleVariableArg(fi)
	if !ok {
		return nil, false, ErrUnsupported
	}
	edgeRef, ok := row.Edge(v.Symbol)
	if !ok {
		return nil, false, ErrUnsupported
	}
	name, found := env.Snap.Kinds().Name(edgeRef.Kind(env.Snap))
	if !found {
		return nil, false, ErrUnsupported
	}
	return name, true, nil
}

// evalCaseFunction implements toLower()/toUpper() per the controller's
// amendment to the original brief: these serve STRING values only. An
// absent property, or a *present* JSON null, is NULL (ok=false) -- JSON null
// is explicitly carved out from the "present non-string" error case below,
// since pg's `->>` extraction of a JSON null is itself SQL NULL, not a
// non-string value to choke on. Any other present non-string (a number,
// bool, array, or object) is ErrRuntimeCast: reproducing pg's own
// lower()/upper() applied to a non-text `->>` rendering would require
// reproducing pg's numeric/boolean text rendering in Go, which this package
// declines to do for the same reason value.go's StringPredicate declines to
// (see its doc comment) -- so the caller bails to delegation instead of
// risking a silently-wrong answer.
func evalCaseFunction(env *Env, row *Row, fi *cypher.FunctionInvocation, upper bool) (any, bool, error) {
	if len(fi.Arguments) != 1 {
		return nil, false, ErrUnsupported
	}
	val, ok, err := EvalValue(env, row, fi.Arguments[0])
	if err != nil {
		return nil, false, err
	}
	if !ok || val == nil {
		return nil, false, nil
	}
	s, isString := val.(string)
	if !isString {
		return nil, false, ErrRuntimeCast
	}
	if upper {
		return strings.ToUpper(s), true, nil
	}
	return strings.ToLower(s), true, nil
}

// evalCoalesce implements coalesce(): the first argument that is neither
// absent nor a present JSON null wins; if every argument is NULL (in either
// sense), the result itself is NULL. This is naive by design (per the
// brief): heterogeneous-type arguments are assumed already rejected upstream
// by the gate, so no type-unification is attempted here.
func evalCoalesce(env *Env, row *Row, fi *cypher.FunctionInvocation) (any, bool, error) {
	for _, arg := range fi.Arguments {
		val, ok, err := EvalValue(env, row, arg)
		if err != nil {
			return nil, false, err
		}
		if ok && val != nil {
			return val, true, nil
		}
	}
	return nil, false, nil
}

// evalSizeFunction implements size(list-property) -> list length, returned
// as a float64 -- NOT a Go int32 -- because this package's entire value
// model is float64-only for numbers (see value.go's doc comment: "exactly
// one of nil, string, float64, bool, []any, or map[string]any"). Every
// comparison/arithmetic primitive this evaluator routes through
// (ScalarEq/PropEq/OrderCompare/In, and evalArithmetic's own float64 type
// assertions) type-switches on float64 with no int32 case, so a bare int32
// result here would silently break `WHERE size(n.spns) = 3`,
// `size(n.spns) > 2`, and `size(n.spns) IN [3, 4]` -- each would compare an
// int32 against a float64 literal and always come out false/no-match rather
// than erroring loudly. A later projection layer (a future task, once
// RETURN values are serialized back to callers) is expected to convert a
// bare, top-level size() result to a pg-parity int32 at that boundary --
// this function's job is only to make size() compose correctly with every
// other primitive inside WHERE/comparison/arithmetic evaluation, which
// requires float64 here.
//
// size() of a string is a different pg rendering (character length, not
// array length) that this evaluator does not attempt to
// reproduce -- the future gate is expected to route
// size(<string-typed operand>) to delegation at plan time rather than ever
// construct a call this function would see, so a non-list, non-absent
// argument here is ErrUnsupported rather than a silent wrong answer.
func evalSizeFunction(env *Env, row *Row, fi *cypher.FunctionInvocation) (any, bool, error) {
	if len(fi.Arguments) != 1 {
		return nil, false, ErrUnsupported
	}
	val, ok, err := EvalValue(env, row, fi.Arguments[0])
	if err != nil {
		return nil, false, err
	}
	if !ok {
		// Absent: pg's `->` yields SQL NULL and jsonb_array_length(NULL)
		// is NULL, so NULL here matches.
		return nil, false, nil
	}
	if val == nil {
		// Present, and the value is JSON null -- a different thing from
		// absent, and PropStore keeps them apart. pg's `-> 'k'` yields
		// 'null'::jsonb rather than SQL NULL for this row, and
		// jsonb_array_length('null'::jsonb) raises "cannot get array
		// length of a scalar", which aborts the whole delegated query.
		// Answering NULL would quietly drop a row (or emit a column)
		// PostgreSQL never produces at all, so the query delegates and
		// fails the same way stock BloodHound does.
		return nil, false, ErrRuntimeCast
	}
	list, isList := val.([]any)
	if !isList {
		return nil, false, ErrUnsupported
	}
	return float64(len(list)), true, nil
}

// evalSplitFunction implements split(str, sep) -> []any of string. Both
// arguments must be present strings; an absent/null operand yields NULL
// (matching Cypher's usual "NULL in, NULL out" for scalar functions) and any
// other non-string operand is ErrRuntimeCast (the same reasoning as
// evalCaseFunction: no local pg-text-rendering reproduction for non-string
// JSON values).
func evalSplitFunction(env *Env, row *Row, fi *cypher.FunctionInvocation) (any, bool, error) {
	if len(fi.Arguments) != 2 {
		return nil, false, ErrUnsupported
	}
	sVal, sOk, err := EvalValue(env, row, fi.Arguments[0])
	if err != nil {
		return nil, false, err
	}
	sepVal, sepOk, err := EvalValue(env, row, fi.Arguments[1])
	if err != nil {
		return nil, false, err
	}
	if !sOk || sVal == nil || !sepOk || sepVal == nil {
		return nil, false, nil
	}
	s, isString := sVal.(string)
	sep, sepIsString := sepVal.(string)
	if !isString || !sepIsString {
		return nil, false, ErrRuntimeCast
	}

	// PostgreSQL's string_to_array, which is what dawgs emits for split(),
	// disagrees with strings.Split on both degenerate inputs, and empty
	// string properties are ordinary in BloodHound data:
	//
	//	string_to_array('', ',')     -> {}        strings.Split -> [""]
	//	string_to_array('abc', '')   -> {abc}     strings.Split -> [a b c]
	//
	// The first one changes rows, not just values: `'' IN split(n.s, ',')`
	// is true for the Go result and false for PostgreSQL's empty array.
	switch {
	case s == "":
		return []any{}, true, nil
	case sep == "":
		return []any{s}, true, nil
	}

	parts := strings.Split(s, sep)
	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}
	return out, true, nil
}

// evalArithmetic implements +, -, *, /, % over numbers, folding left to
// right across an ArithmeticExpression's chained Partials the same way
// evalComparison folds a chained Comparison.
//
// Alongside the runtime accumulator (cur/curOk), this also folds curKind --
// the STATIC addOperandKind (see classifyAddOperand) of whatever cur
// currently represents -- purely from the AST shapes involved, never from
// what anything evaluates to. curKind starts at classifyAddOperand(ae.Left)
// and is refolded after every step via nextAddKind: any non-`+` operator
// always produces a definite number (addOther), and a `+` step itself
// produces addStaticText exactly when applyAdd took its concatenation
// branch (see nextAddKind's own doc) -- so a chain like `'a' + n.b + n.c`
// still concatenates all the way through, matching how pg's own nested
// BinaryExpression tree would carry a Text type forward once the first `+`
// resolves to one.
func evalArithmetic(env *Env, row *Row, ae *cypher.ArithmeticExpression) (any, bool, error) {
	cur, curOk, err := EvalValue(env, row, ae.Left)
	if err != nil {
		return nil, false, err
	}
	curKind := classifyAddOperand(ae.Left)
	for _, partial := range ae.Partials {
		if partial == nil {
			return nil, false, ErrUnsupported
		}
		rVal, rOk, err := EvalValue(env, row, partial.Right)
		if err != nil {
			return nil, false, err
		}
		rKind := classifyAddOperand(partial.Right)
		cur, curOk, err = applyArithmetic(curKind, cur, curOk, partial.Operator, rKind, rVal, rOk)
		if err != nil {
			return nil, false, err
		}
		curKind = nextAddKind(partial.Operator, curKind, rKind)
	}
	return cur, curOk, nil
}

// addOperandKind classifies one `+` operand by its STATIC AST shape --
// never by what it evaluates to -- mirroring the analysis pg's own
// translator performs on the *unevaluated* expression tree before ever
// running the query (dawgs@v0.8.0 cypher/models/pgsql/translate/
// expression.go's rewriteBinaryExpression/isConcatenationOperation, and
// hinting.go's InferExpressionType/GetTypeHint, which is how pg arrives at
// each operand's static pgsql.DataType in the first place). See applyAdd's
// own doc for the full investigation this replaces (a prior version of
// this evaluator sniffed *runtime* operand types instead, which answers a
// different question than pg's and was confirmed to disagree with it: `n.
// score + n.score` over two numeric properties served a numeric 84 where
// pg -- which types two raw property lookups as concatenation
// unconditionally, regardless of what they hold -- returns "4242").
type addOperandKind int

const (
	// addOther is any operand pg would statically type as something other
	// than Text and other than an untyped/unresolvable operand: a numeric
	// or boolean literal, an arithmetic sub-expression, id()/datetime()'s
	// epoch accessors, or any other function call this evaluator supports
	// that isn't specifically called out below. Treated as "not statically
	// Text, and not a property lookup" by applyAdd/nextAddKind below.
	addOther addOperandKind = iota

	// addStaticText is any operand pg statically types as Text: a string
	// literal, or a call to one of the text-returning functions this
	// evaluator itself implements -- toLower()/toUpper()/type(), all
	// emitted as `CastType: pgsql.Text` FunctionCalls by dawgs' own
	// translation (cypher/models/pgsql/translate/function.go).
	addStaticText

	// addPropertyLookup is a bare `atom.prop` read of a node's own
	// property: a raw jsonb access pg cannot statically type at all
	// (InferExpressionType's OperatorPropertyLookup/OperatorJSONField case
	// answers pgsql.UnknownDataType). checkArithmetic (plan.go) rejects a
	// `+` where BOTH operands classify this way at plan time -- pg's own
	// rule concatenates that shape unconditionally, which this evaluator
	// cannot safely reproduce without rendering an arbitrary JSON scalar
	// exactly as pg's jsonb `->>` operator does (see applyAdd's doc) -- so
	// by the time applyAdd runs against any query Plan actually accepted,
	// at most one operand of the chain's first `+` is ever this kind.
	addPropertyLookup

	// addUnresolved is a coalesce() call none of whose arguments carry a
	// pg-known type -- e.g. `coalesce(n.a, n.b)`, every argument a bare
	// property lookup (see classifyCoalesceOperand's doc for the full
	// derivation against dawgs' translateCoalesceFunction). This is
	// deliberately NOT the same bucket as addPropertyLookup, even though
	// both ultimately trace back to "pg has no static type for this":
	// a bare property lookup's safety depends on what it's paired with in
	// the SAME `+` (rewritePropertyLookupOperands casts it to match a
	// known-typed partner -- see checkArithmetic's doc), but an unresolved
	// coalesce's own arguments were ALREADY rewritten (or, here, left
	// unrewritten) by translateCoalesceFunction before the outer `+` is
	// ever reached, entirely independent of whatever it's added to -- so
	// pairing it with a known-typed operand does not rescue it the way it
	// rescues a bare property lookup. checkArithmetic (plan.go) rejects a
	// `+` with an addUnresolved operand UNLESS the other operand is
	// addStaticText (the one case pg itself resolves unconditionally, Text
	// winning regardless of the other side -- see isConcatenationOperation).
	addUnresolved
)

// classifyAddOperand inspects expr's own AST shape (after unwrapping any
// Parentheticals) to produce one addOperandKind. Shared, unmodified, by
// checkArithmetic (plan.go, the plan-time rejects) and evalArithmetic/
// applyAdd here (the eval-time concat-vs-numeric dispatch), so the two can
// never disagree about which bucket one operand falls into.
//
// # Audit: every classifyAddOperand call site's actual pg static type
//
// A follow-up review of this function's original coalesce() handling (see
// classifyCoalesceOperand's own doc for the fix itself) asked for every
// OTHER function this evaluator recognizes to be re-verified directly
// against dawgs@v0.8.0's translator, not assumed from this function's own
// prior comments. Each row below cites the exact dawgs function.go case
// that fixes the operand's CastType (or, for coalesce, derives it), and
// notes whether a classification gap here could ever surface as a served,
// WRONG value (as opposed to a spurious-but-harmless decline):
//
//   - id()            pgsql.Int8, a known non-Text type (IdentityFunction
//     case, translateFunction: `CompoundIdentifier{..., ColumnID}`, and
//     InferExpressionType's own CompoundIdentifier/ColumnID case). addOther
//     is correct. evalIDFunction always returns a Go float64 -- consistent.
//   - toLower()/toUpper()  pgsql.Text (ToLowerFunction/ToUpperFunction
//     cases, both `CastType: pgsql.Text`). addStaticText is correct,
//     unchanged.
//   - type()          pgsql.Text (EdgeTypeFunction case: wraps kind_name()
//     in a `CastType: pgsql.Text` FunctionCall) -- the exact same shape as
//     toLower/toUpper. THIS WAS A GAP: previously bucketed addOther by this
//     function's fallthrough default, now fixed to addStaticText. Because
//     evalTypeFunction always returns a genuine Go string, the old gap
//     never produced a WRONG served number (applyAdd's numeric branch
//     already bails ErrRuntimeCast on any present runtime string) -- but it
//     spuriously declined queries pg would concatenate correctly, e.g.
//     `type(r) + n.name` over two real strings (see TestEvalStringConcatenation's
//     regression test for the fix, and checkArithmetic's own doc for why
//     the plan-time behavior is unaffected either way).
//   - labels()         pgsql.TextArray (NodeLabelsFunction case,
//     translateNodeLabelsExpression: a TypeCast to TextArray).
//     isConcatenationOperation checks IsArrayType() BEFORE the Text check,
//     so pg treats any array-typed `+` operand as list concatenation
//     unconditionally -- a third semantics this package implements nowhere
//     (no addListConcat kind, no runtime list-append/element-cast). Left as
//     addOther, deliberately: evalLabelsFunction always returns a Go
//     []any, which can satisfy neither applyAdd's `.(string)` check (the
//     addStaticText branch) nor its `.(float64)` check (the numeric
//     default branch) -- so ANY use of labels() as a `+` operand this
//     package would ever actually reach at eval time bails (ErrRuntimeCast
//     if paired with an addStaticText operand, ErrUnsupported otherwise),
//     regardless of which static bucket it is filed under. Safe by
//     construction, not by a correct type mirror -- flagged here for
//     whoever next loosens either of those two type assertions in
//     applyAdd, since doing so would silently remove this accidental
//     guard.
//   - split()          pgsql.TextArray (StringSplitToArrayFunction case) --
//     the identical array rule and the identical "safe by construction"
//     argument as labels() above (evalSplitFunction also always returns a
//     Go []any). Left as addOther, unchanged.
//   - coalesce()       NOT a single fixed type -- derived from its own
//     arguments by translateCoalesceFunction. See classifyCoalesceOperand's
//     own doc for the full derivation; this is the Important finding this
//     fix closes.
func classifyAddOperand(expr cypher.Expression) addOperandKind {
	expr = unwrapParens(expr)

	if lit, isLit := expr.(*cypher.Literal); isLit && lit != nil && !lit.Null {
		if _, isString := lit.Value.(string); isString {
			return addStaticText
		}
		return addOther
	}

	if fi, isFunc := expr.(*cypher.FunctionInvocation); isFunc && fi != nil {
		switch strings.ToLower(fi.Name) {
		case cypher.ToLowerFunction, cypher.ToUpperFunction, cypher.EdgeTypeFunction:
			return addStaticText
		case cypher.CoalesceFunction:
			return classifyCoalesceOperand(fi)
		default:
			return addOther
		}
	}

	if pl, isPL := expr.(*cypher.PropertyLookup); isPL && pl != nil {
		// datetime().epochseconds/.epochmillis (evalDateTimeComponent) is a
		// known-numeric accessor, not a dynamic jsonb property read -- the
		// only other PropertyLookup atom shape this evaluator recognizes
		// (evalPropertyLookup) is a plain node variable's own property.
		if _, isFuncAtom := unwrapParens(pl.Atom).(*cypher.FunctionInvocation); isFuncAtom {
			return addOther
		}
		return addPropertyLookup
	}

	return addOther
}

// classifyCoalesceOperand classifies a coalesce(...) call the same way
// dawgs' translateCoalesceFunction (cypher/models/pgsql/translate/
// function.go, lines ~1071-1131) derives its STATIC pgsql type: that
// function pops each argument, skips any whose InferExpressionType is not
// IsKnown() ("Properties have no type information and should be skipped" --
// its own comment), and assigns the coalesce call's CastType to the first
// KNOWN type it finds among the rest (erroring if a later argument's known
// type disagrees, a genuinely malformed-Cypher edge case this function does
// not separately detect -- see below). This is a per-argument OR, not an
// ALL: a single known-typed argument decides the whole call's type, no
// matter how many other arguments are bare, untyped property lookups.
//
//   - ANY argument classifies addStaticText -> the whole call is
//     addStaticText, exactly mirroring isConcatenationOperation's own
//     unconditional "either operand's type is Text" rule (checked before
//     its "both sides are dynamic property lookups" carve-out) --
//     `coalesce(n.score, 'default')` is statically Text in pg regardless of
//     what n.score holds, so `coalesce(n.score, 'default') + 1`
//     CONCATENATES in pg, never numerically adds.
//   - Else, ANY argument classifies as a known non-Text type (addOther --
//     a numeric/boolean literal, id(), arithmetic, etc.) -> the whole call
//     is addOther: `coalesce(n.a, 5)` gets pg's CastType = the numeric
//     literal's type (the property argument is simply skipped by the type
//     scan), so `coalesce(n.a, 5) + 1` really is numeric addition in pg,
//     regardless of whether n.a happens to hold a number this row.
//   - Else (every argument is addPropertyLookup, or is itself an
//     addUnresolved nested coalesce -- i.e. NO argument carries any known
//     type at all) -> addUnresolved. This is the one case that needed
//     tracing through dawgs source rather than assumed: translateCoalesceFunction
//     leaves such a call's CastType at its zero value, pgsql.UnsetDataType
//     ("" -- see pgtypes.go). When the outer `+` later calls
//     InferExpressionType on this FunctionCall, the `pgsql.TypeHinted` case
//     returns that CastType VERBATIM -- pgsql.UnsetDataType, NOT
//     pgsql.UnknownDataType. isConcatenationOperation's own "both sides are
//     dynamic property lookups -> concatenate" carve-out requires literal
//     equality with pgsql.UnknownDataType (`lOperandType ==
//     pgsql.UnknownDataType`) AND `isPropertyLookup(lOperand)` -- a
//     FunctionCall is never that shape, so an unresolved coalesce fails
//     BOTH conditions and never qualifies for that carve-out, regardless of
//     what it is paired with. It also never receives
//     rewritePropertyLookupOperands' cast-injection, which only ever
//     rewrites an operand that is DIRECTLY a property-lookup-shaped
//     BinaryExpression -- a coalesce FunctionCall wrapping property lookups
//     internally is never that shape either, no matter which side of the
//     `+` it sits on. So pg falls through to plain arithmetic `+` over
//     whatever the coalesce's own unrewritten property-lookup arguments
//     default to (`->>`, i.e. genuine SQL text) -- `text + integer` has no
//     PostgreSQL operator, so this shape errors outright in real pg,
//     unconditionally, regardless of what the underlying properties
//     actually hold at runtime. Reproducing that would mean either
//     guessing at whether pg errors (a much larger surface than this
//     package's established "no unpinned rendering reproduction"
//     convention already declines elsewhere -- applyAdd's own doc, again)
//     or risking a served numeric answer for a query real pg refuses to run
//     at all -- worse than the original finding's silent-wrong-number bug,
//     not merely a repeat of it. checkArithmetic (plan.go) therefore
//     rejects the whole query whenever an addUnresolved operand appears in
//     a `+` without an addStaticText partner (see checkArithmetic's own
//     doc); this delegates to PostgreSQL, which then either concatenates
//     correctly (if paired with Text) or raises its own genuine error --
//     never a value this package invented.
//
// Not handled: two arguments with DIFFERENT known types (e.g.
// `coalesce(5, 'x')`) makes translateCoalesceFunction itself error out at
// translation time ("types in coalesce function must match") -- a
// malformed-Cypher shape this function does not specially detect, so it
// picks whichever known type it happens to see, exactly as pg's own
// first-known-type-wins loop would before its later mismatch check fires.
// This is not a silent-wrong-answer risk: whichever branch above is taken,
// applyAdd's own runtime type assertions (a coalesce built from
// deliberately mismatched argument types can only ever concretely evaluate
// to ONE of them per row) still bail (ErrRuntimeCast/ErrUnsupported) rather
// than serve a number pg's own comparator error
// (newFunctionCallComparatorError) would have refused to produce -- and
// this exact shape has no corpus query, so declining costs nothing.
func classifyCoalesceOperand(fi *cypher.FunctionInvocation) addOperandKind {
	sawKnownNonText := false
	for _, arg := range fi.Arguments {
		switch classifyAddOperand(arg) {
		case addStaticText:
			return addStaticText
		case addPropertyLookup, addUnresolved:
			// No type information from this argument -- keep scanning
			// (mirrors translateCoalesceFunction's own "properties have no
			// type information and should be skipped").
		default:
			sawKnownNonText = true
		}
	}
	if sawKnownNonText {
		return addOther
	}
	return addUnresolved
}

// nextAddKind folds the running addOperandKind forward across one
// arithmetic step, for evalArithmetic's own chain-tracking use (see its doc
// comment): any operator other than `+` always yields a definite number
// (subtraction/multiplication/division/modulo have no Cypher/pg
// concatenation analog at all), and a `+` step itself yields addStaticText
// exactly when applyAdd would take (or did take) its concatenation branch --
// i.e. either input operand is itself addStaticText. Neither a
// property-lookup nor an unresolved-coalesce input is ever carried forward
// past the first step that introduces it: once folded into a `+`, the
// accumulated result is no longer a literal PropertyLookup or
// FunctionInvocation AST node, exactly like pg's own nested
// BinaryExpression tree (see checkArithmetic's doc for why its "both
// property lookups"/"unresolved coalesce" plan-time rejects only ever need
// to look at each `+` step as it is introduced, not at some later folded
// position). A step that folds an addUnresolved operand together with an
// addStaticText partner (the only combination checkArithmetic ever accepts
// for it) correctly yields addStaticText here too, so a chain like
// `coalesce(n.a,n.b) + 'x' + n.c` keeps concatenating all the way through.
func nextAddKind(op cypher.Operator, aKind, bKind addOperandKind) addOperandKind {
	if op != cypher.OperatorAdd {
		return addOther
	}
	if aKind == addStaticText || bKind == addStaticText {
		return addStaticText
	}
	return addOther
}

// applyArithmetic implements one arithmetic step over float64 operands, plus
// `+`'s own string-concatenation disambiguation (applyAdd, below) -- every
// other operator (-, *, /, %) has no Cypher/pg concatenation analog, so those
// stay numeric-only exactly as before: a non-numeric operand is
// ErrUnsupported (expected to be pre-rejected, or handled by a different
// code path, at plan time -- checkArithmetic (plan.go) does not itself
// type-check operands for any of these operators, mirroring this function's
// own runtime-only dispatch). aKind/bKind (unused by every operator but `+`)
// are the operands' static addOperandKind, per evalArithmetic's own doc.
func applyArithmetic(aKind addOperandKind, a any, aOk bool, op cypher.Operator, bKind addOperandKind, b any, bOk bool) (any, bool, error) {
	if !aOk || !bOk || a == nil || b == nil {
		return nil, false, nil
	}

	if op == cypher.OperatorAdd {
		return applyAdd(aKind, a, bKind, b)
	}

	af, aIsNum := a.(float64)
	bf, bIsNum := b.(float64)
	if !aIsNum || !bIsNum {
		return nil, false, ErrUnsupported
	}

	switch op {
	case cypher.OperatorSubtract:
		return af - bf, true, nil
	case cypher.OperatorMultiply:
		return af * bf, true, nil
	case cypher.OperatorDivide:
		if bf == 0 {
			return nil, false, ErrRuntimeCast
		}
		return af / bf, true, nil
	case cypher.OperatorModulo:
		if bf == 0 {
			return nil, false, ErrRuntimeCast
		}
		return math.Mod(af, bf), true, nil
	default:
		return nil, false, ErrUnsupported
	}
}

// applyAdd implements Cypher's `+`, which pg's own translation disambiguates
// per call into either numeric addition or string concatenation -- and,
// critically, decides which one STATICALLY, from the unevaluated expression
// tree, never from what either operand holds at runtime (pinned by reading
// dawgs@v0.8.0's cypher/models/pgsql/translate/expression.go directly, not
// guessed): rewriteBinaryExpression's OperatorAdd case infers each
// operand's static pgsql.DataType and calls isConcatenationOperation, which
// chooses concatenation when either operand's inferred type is Text (a
// string literal, or anything else pg can statically type as text), or --
// "to prefer Cypher's string concatenation form instead of emitting invalid
// jsonb + jsonb SQL" (that function's own comment) -- when BOTH operands
// are property lookups (pg has no static type for a raw jsonb property, so
// it defaults an all-property `+` to concatenation rather than arithmetic,
// unconditionally, regardless of what the properties hold at runtime).
// Array-typed operands also concatenate (list concatenation) under pg's
// rule; out of scope here (no addListConcat kind, no runtime list-append) --
// split()/labels() are both actually array-typed in pg (see
// classifyAddOperand's own audit table) but stay classified addOther here,
// safely: their Go runtime shape ([]any) can never satisfy either this
// function's `.(string)` check or its `.(float64)` check, so any reachable
// use bails rather than ever computing a wrong list-concatenation or
// numeric result.
//
// aKind/bKind are the operands' STATIC addOperandKind (classifyAddOperand,
// folded through a chain by evalArithmetic/nextAddKind) -- not anything
// derived from a/b's runtime value. This replaces an earlier version of
// this function that dispatched purely on runtime type (whether a or b
// happened to be a Go string at the moment this ran), which answers a
// different question than pg's own static rule and was confirmed to
// disagree with it for exactly the shape checkArithmetic (plan.go) now
// rejects at plan time instead: `n.score + n.score` over two numeric
// properties used to silently serve a numeric 84 where pg's own static
// rule -- typing two raw property lookups as concatenation unconditionally
// -- returns the string "4242".
//
//   - Either operand is addStaticText (CONCAT semantics, matching
//     isConcatenationOperation's Text-operand branch): both runtime values
//     must be present, actual strings to concatenate. An absent operand
//     never reaches this function at all -- applyArithmetic's own
//     !aOk||!bOk||a==nil||b==nil guard short-circuits to NULL first,
//     unconditionally, for every operator including this one -- so "either
//     value present but non-string" (a bool/float64/list/map property, or
//     a non-string result from some other supported function) bails
//     ErrRuntimeCast (declines the whole query -- see cypherExecReason's
//     identical mapping for every other evaluator sentinel -- to delegate
//     to PostgreSQL) rather than guess at a text rendering. This is
//     deliberately more conservative than pg itself: e.g.
//     `n.numericProp + 'x'` pg statically types as concatenation (the
//     literal is Text) and would render numericProp via `->>` (jsonb's own
//     "always renders as text" contract, which never itself errors for a
//     present scalar) -- but a `->>` text rendering does not necessarily
//     equal Go's fmt-style stringification of the same JSON value for
//     every type (e.g. a JSON float requires matching pg's exact numeric
//     text formatting to agree byte-for-byte), and this package has no
//     tested, pinned reproduction of that formatting to fall back on
//     safely. Declining is always correct regardless of which side turns
//     out right, since PostgreSQL remains the fallback either way -- see
//     this package's own repeated "REJECTED is safe" precedent (e.g.
//     expandShortestPathComponent's identical preference for a spurious
//     decline over a guessed answer).
//   - Neither operand is addStaticText, but BOTH are addPropertyLookup, OR
//     either operand is addUnresolved (an all-property-argument coalesce()
//     call, classifyCoalesceOperand's doc): per checkArithmetic's own doc,
//     Plan already rejects both of these exact shapes for any query this
//     evaluator is actually asked to run -- reached anyway (e.g. a direct
//     EvalValue call bypassing Plan, in a test), this bails ErrUnsupported
//     defensively rather than guess at either interpretation, matching this
//     file's established convention for a plan-guaranteed-unreachable shape
//     (e.g. evalPatternPredicate's identical defensive ErrUnsupported for a
//     shape checkPatternPredicate already validated away).
//   - Otherwise (NUMERIC semantics, matching pg's own "neither side is
//     Text, and not both are untyped property lookups" fallthrough, which
//     pg renders as an arithmetic `+` and therefore expects both sides to
//     already be numeric): both operands present numbers add normally; a
//     present, non-numeric operand that happens to be a runtime STRING
//     (e.g. `1 + n.name` where n.name is a genuine string property) bails
//     ErrRuntimeCast -- pg's own `(properties ->> 'name')::numeric`-style
//     cast would itself raise a runtime error for that same row, so this
//     mirrors a genuine pg failure rather than silently concatenating (the
//     bug this rule replaces: a purely-runtime-sniffing implementation
//     would group "not statically Text" operands with "whatever they
//     evaluate to", so two operands that both merely *happen* to be
//     runtime strings could fall into ordinary concatenation here even
//     though that is not pg's rule for this shape at all). Any other
//     present, non-numeric, non-string operand (a bool/list/map) still
//     bails ErrUnsupported, exactly as it always has.
func applyAdd(aKind addOperandKind, a any, bKind addOperandKind, b any) (any, bool, error) {
	switch {
	case aKind == addStaticText || bKind == addStaticText:
		aStr, aIsStr := a.(string)
		bStr, bIsStr := b.(string)
		if !aIsStr || !bIsStr {
			return nil, false, ErrRuntimeCast
		}
		return aStr + bStr, true, nil

	case aKind == addPropertyLookup && bKind == addPropertyLookup:
		return nil, false, ErrUnsupported

	case aKind == addUnresolved || bKind == addUnresolved:
		// checkArithmetic (plan.go) rejects a `+` with an addUnresolved
		// operand unless its partner is addStaticText -- a combination
		// already handled by the case above, since that case's `||` fires
		// first whenever either side is addStaticText. Reaching this case
		// therefore means an addUnresolved operand paired with anything
		// else, a shape Plan never accepts for a served query (see
		// classifyCoalesceOperand's doc for why pg itself cannot safely
		// resolve it either) -- bail defensively, same convention as the
		// addPropertyLookup&&addPropertyLookup case above.
		return nil, false, ErrUnsupported

	default:
		if _, isStr := a.(string); isStr {
			return nil, false, ErrRuntimeCast
		}
		if _, isStr := b.(string); isStr {
			return nil, false, ErrRuntimeCast
		}
		af, aIsNum := a.(float64)
		bf, bIsNum := b.(float64)
		if !aIsNum || !bIsNum {
			return nil, false, ErrUnsupported
		}
		return af + bf, true, nil
	}
}

// evalUnary implements unary +/- over a numeric operand.
func evalUnary(env *Env, row *Row, u *cypher.UnaryAddOrSubtractExpression) (any, bool, error) {
	val, ok, err := EvalValue(env, row, u.Right)
	if err != nil {
		return nil, false, err
	}
	if !ok || val == nil {
		return nil, false, nil
	}
	f, isNum := val.(float64)
	if !isNum {
		return nil, false, ErrRuntimeCast
	}
	if u.Operator == cypher.OperatorSubtract {
		return -f, true, nil
	}
	return f, true, nil
}
