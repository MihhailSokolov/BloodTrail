// SPDX-License-Identifier: Apache-2.0

// Package interpret: this file implements the expression evaluator itself,
// walking the dawgs Cypher AST (github.com/specterops/dawgs@v0.8.0
// cypher/models/cypher) directly against one snapshot.Snapshot and one bound
// Row, and routing every comparison through value.go's Task 4 primitives
// (Tri, StringEq/StringNeq/ScalarEq/PropEq, StringPredicate, In, OrderCompare,
// IsNull/IsNotNull) rather than re-deriving pg's semantics here.
//
// Two entry points are exported: EvalPredicate for boolean (WHERE-clause)
// context, returning a Tri, and EvalValue for scalar/projection context,
// returning a (value, present, error) triple in the same post-JSON value
// model value.go's doc comment describes (nil | string | float64 | bool |
// []any | map[string]any, with a separate present/ok flag distinguishing
// absence from a stored JSON null).
package interpret

import (
	"errors"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// ErrUnsupported is returned for an AST shape this evaluator does not
// recognize -- a construct the future planner/gate (Task 6+) is expected to
// have already rejected before a query ever reaches interpretation. It is
// exported so callers can distinguish "genuinely can't do this locally"
// (this, or ErrCollation) from an outright bug, but reaching it in
// production is a gate bug, not an expected runtime outcome: this package
// never panics on an unrecognized node, it returns ErrUnsupported instead.
var ErrUnsupported = errors.New("interpret: unsupported expression")

// EdgeRef identifies one directed edge instance bound to a Row by its
// forward-CSR slot: Snap.OutTargets[Fwd]/Snap.OutKinds[Fwd]/
// Snap.OutEdgeIDs[Fwd] all describe the same edge. A reverse-CSR-discovered
// edge (e.g. found while walking In(n)) is expected to have already been
// translated to its forward index via Snap.InEdgeIdx by whatever produced
// the Row -- this package always reads edges through the forward arrays.
type EdgeRef struct {
	Fwd uint64
}

// Row binds one MATCH solution's pattern variables to concrete snapshot
// values: dense node ids, edge references, path values (a seam for a future
// task -- nothing in this milestone materializes paths yet, so PathVar's
// value type is deliberately just `any`), and plain scalars (e.g. bound by a
// future UNWIND/WITH implementation, or by a caller pre-seeding a computed
// value). All four namespaces are separate maps rather than one
// `map[string]any`, matching how a Cypher planner would keep them: which
// namespace a symbol lives in is a property of the query's pattern, not
// something this package needs to infer from the stored value's shape.
type Row struct {
	nodes   map[string]snapshot.NodeID
	edges   map[string]EdgeRef
	paths   map[string]any
	scalars map[string]any

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
}

// NewRow returns an empty Row ready for SetNode/SetEdge/SetPathVar/
// SetScalar.
func NewRow() *Row {
	return &Row{}
}

// SetNode binds sym to a node's dense NodeID.
func (r *Row) SetNode(sym string, id snapshot.NodeID) {
	if r.nodes == nil {
		r.nodes = make(map[string]snapshot.NodeID)
	}
	r.nodes[sym] = id
}

// Node returns the dense NodeID bound to sym, and whether sym is bound as a
// node variable at all.
func (r *Row) Node(sym string) (snapshot.NodeID, bool) {
	id, ok := r.nodes[sym]
	return id, ok
}

// SetEdge binds sym to an edge reference.
func (r *Row) SetEdge(sym string, ref EdgeRef) {
	if r.edges == nil {
		r.edges = make(map[string]EdgeRef)
	}
	r.edges[sym] = ref
}

// Edge returns the EdgeRef bound to sym, and whether sym is bound as an edge
// variable at all.
func (r *Row) Edge(sym string) (EdgeRef, bool) {
	ref, ok := r.edges[sym]
	return ref, ok
}

// SetPathVar binds sym to a path value. No task before this one produces a
// path value, so v's shape is not yet defined by anything other than the
// caller; this exists purely so Row's namespace shape matches the brief's
// contract ahead of the task that will populate it.
func (r *Row) SetPathVar(sym string, v any) {
	if r.paths == nil {
		r.paths = make(map[string]any)
	}
	r.paths[sym] = v
}

// PathVar returns the path value bound to sym, and whether sym is bound as a
// path variable at all.
func (r *Row) PathVar(sym string) (any, bool) {
	v, ok := r.paths[sym]
	return v, ok
}

// SetScalar binds sym to a plain computed/bound value (e.g. an UNWIND
// element), in the same post-JSON value model as everything else in this
// package.
func (r *Row) SetScalar(sym string, v any) {
	if r.scalars == nil {
		r.scalars = make(map[string]any)
	}
	r.scalars[sym] = v
}

// Scalar returns the scalar value bound to sym, and whether sym is bound as
// a scalar variable at all.
func (r *Row) Scalar(sym string) (any, bool) {
	v, ok := r.scalars[sym]
	return v, ok
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
	Snap *snapshot.Snapshot
	Now  time.Time

	regexMu    sync.Mutex
	regexCache map[string]*regexp.Regexp
}

// compiledRegex returns a compiled, cached *regexp.Regexp for pattern,
// compiling and caching it on first use. See Env's doc comment for why this
// cache exists instead of compiling on every call the way value.go's own
// matchString (used for the STARTS WITH/ENDS WITH/CONTAINS operators, which
// have no compilation cost to amortize) deliberately still does.
func (e *Env) compiledRegex(pattern string) (*regexp.Regexp, error) {
	e.regexMu.Lock()
	defer e.regexMu.Unlock()

	if re, ok := e.regexCache[pattern]; ok {
		return re, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	if e.regexCache == nil {
		e.regexCache = make(map[string]*regexp.Regexp)
	}
	e.regexCache[pattern] = re
	return re, nil
}

// --- EvalPredicate: boolean (WHERE-clause) context --------------------------

// EvalPredicate evaluates expr against row under env, returning Cypher's
// three-valued boolean result. Every AST shape the brief documents this
// package as consuming is handled explicitly; anything else returns
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

// evalPartialComparison dispatches one `left op right` step to the Task 4
// primitive its operator calls for, per the brief's pinned routing table.
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
// expression) is exactly what decides which Task 4 equality primitive
// applies (StringEq/StringNeq for a string-literal counterpart, ScalarEq for
// any other literal, PropEq when neither side is a literal) -- not the
// runtime type of what it evaluates to.
func asLiteral(expr cypher.Expression) (*cypher.Literal, bool) {
	lit, ok := unwrapParens(expr).(*cypher.Literal)
	return lit, ok
}

// evalEquality implements `=`/`<>` per the brief's routing table:
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

	if t, ok := evalIdentityEquality(row, leftExpr, rightExpr); ok {
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
// task-8-brief.md's shortestPath self-pair rule depends on
// (`WHERE s<>t`, s and t both bare node variables) -- and, per task-6's own
// report ("The COLLECT-membership execution gap"), an instance of a
// documented pattern this codebase already flags: identity-based semantics
// over a node variable must be special-cased structurally before falling
// through to generic property-value comparison.
//
// ok is false (deferring to the generic PropEq path, unchanged) for every
// other shape -- either operand not a bare Variable, either operand not
// resolving to the same value namespace (both nodes, or both edges) in row,
// or a node compared against an edge.
func evalIdentityEquality(row *Row, leftExpr, rightExpr cypher.Expression) (t Tri, ok bool) {
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
		return boolToTri(rIsEdge && le.Fwd == re.Fwd), rIsEdge
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
// flags this as "a performance concern for a future milestone's plan-
// execution loop" and explicitly declines to fix it there, since
// StringPredicate is Task 4's frozen, already-reviewed API and changing its
// signature would ripple into value_test.go for no change in observable
// behavior. But this task's Env is exactly the object a real query's row
// loop constructs once and then threads through every row's
// EvalPredicate/EvalValue call, which makes it the natural place to cache
// a compiled pattern across that whole loop without needing a separate
// plan/prepare pass: the first row for a given regex literal compiles and
// caches it (keyed by source pattern text, via Env.compiledRegex), and every
// subsequent row -- for that predicate, or any other predicate in the same
// query reusing the same pattern text -- reuses the cached *regexp.Regexp.
// A future planner (Task 6+) can still replace this with a prepare-once
// artifact attached to the plan node instead of a live cache keyed by
// pattern text; until then, this is the seam that avoids the "compile once
// per row" cost the brief calls out as unacceptable.
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
	return RegexPredicate(re, val, ok, negated)
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
		lo, hi := env.Snap.KindOffsets[nodeID], env.Snap.KindOffsets[nodeID+1]
		return matchKinds(env, env.Snap.NodeKinds[lo:hi], km.Kinds, km.IsExclusive), nil
	}
	if edgeRef, ok := row.Edge(v.Symbol); ok {
		return matchKinds(env, []snapshot.KindID{env.Snap.OutKinds[edgeRef.Fwd]}, km.Kinds, km.IsExclusive), nil
	}
	return TriNull, ErrUnsupported
}

// matchKinds reports whether have (a node's or edge's own kind list)
// satisfies want under exclusive's ALL-vs-ANY semantics; see
// evalKindMatcher's doc comment.
func matchKinds(env *Env, have []snapshot.KindID, want graph.Kinds, exclusive bool) Tri {
	matched := 0
	for _, k := range want {
		kindID, found := env.Snap.Kinds.ID(k.String())
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
// The result is always TriTrue or TriFalse, never TriNull: per the
// milestone brief's own pin, a pattern predicate's existence check has
// nothing to be NULL about (no property lookup is ever involved -- only
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
	if fromNode.Variable == nil || toNode.Variable == nil {
		return TriNull, ErrUnsupported
	}

	fromID, ok := row.Node(fromNode.Variable.Symbol)
	if !ok {
		return TriNull, ErrUnsupported
	}
	toID, ok := row.Node(toNode.Variable.Symbol)
	if !ok {
		return TriNull, ErrUnsupported
	}

	kinds := make([]snapshot.KindID, 0, len(rel.Kinds))
	for _, k := range rel.Kinds {
		id, found := env.Snap.Kinds.ID(k.String())
		if !found {
			return TriNull, ErrUnsupported
		}
		kinds = append(kinds, id)
	}

	switch rel.Direction {
	case graph.DirectionOutbound:
		return boolToTri(hasAdjacentEdge(env, fromID, toID, kinds)), nil
	case graph.DirectionInbound:
		return boolToTri(hasAdjacentEdge(env, toID, fromID, kinds)), nil
	case graph.DirectionBoth:
		return boolToTri(hasAdjacentEdge(env, fromID, toID, kinds) || hasAdjacentEdge(env, toID, fromID, kinds)), nil
	default:
		return TriNull, ErrUnsupported
	}
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
	targets, edgeKinds := env.Snap.Out(src)
	for i, t := range targets {
		if t == dst && edgeKindOK(kinds, edgeKinds[i]) {
			return true
		}
	}
	return false
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
// no defined "materialize as a value" shape yet in this milestone (no
// edge-property store exists, and no path type has been built by any task
// before this one), so both are ErrUnsupported for now.
func evalVariableValue(env *Env, row *Row, v *cypher.Variable) (any, bool, error) {
	if val, ok := row.Scalar(v.Symbol); ok {
		return val, true, nil
	}
	if nodeID, ok := row.Node(v.Symbol); ok {
		return env.Snap.Props.NodeMap(nodeID), true, nil
	}
	return nil, false, ErrUnsupported
}

// evalPropertyLookup evaluates `atom.prop`. Two atom shapes are recognized:
// datetime().epochseconds/.epochmillis (see below), and a plain node
// variable's property, read from Snap.Props. An edge variable's property
// lookup is ErrUnsupported: the snapshot's PropStore only models node
// property bags (see props.go), and nothing in this milestone's brief calls
// for relationship properties, so there is nowhere to read r.prop from yet.
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
		propID, found := env.Snap.Props.IDByName(pl.Symbol)
		if !found {
			// This property name was never interned by this snapshot's
			// PropStore at all, meaning no node anywhere in the graph carries
			// it -- so it is certainly absent from this one.
			return nil, false, nil
		}
		v, ok := env.Snap.Props.Value(nodeID, propID)
		return v, ok, nil
	}

	if _, ok := row.Edge(v.Symbol); ok {
		return nil, false, ErrUnsupported
	}

	return nil, false, ErrUnsupported
}

// evalDateTimeComponent implements datetime().epochseconds/.epochmillis:
// the only two ITTC (instant-type/temporal-component) accessors the brief
// documents as supported, since the gate is expected to reject any other
// datetime() component at plan time (before it ever reaches this
// evaluator). Both derive from Env.Now, fixed once per query.
//
// Numeric representation note: the brief describes this as returning
// "int64", and evalIDFunction's doc comment carries the same note -- see it
// for the full rationale. In short: this package's value model (value.go's
// own doc comment) is "every JSON number is float64", which is what lets a
// single set of primitives (ScalarEq/PropEq/OrderCompare/In, all frozen
// Task 4 code that only type-switches on float64) work uniformly over every
// numeric value regardless of where it came from. Handing back a literal Go
// int64 here would silently opt these two accessors out of that model --
// e.g. `n.lastlogontimestamp < (datetime().epochseconds - (60 * 86400))`,
// one of this task's own named test scenarios, requires the epochseconds
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
// brief's supported list is ErrUnsupported: the future gate is expected to
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
// Numeric representation note (deviation from a literal reading of the
// brief): the brief describes id() as returning "int64". This evaluator
// instead returns float64, deliberately keeping id() inside this package's
// one uniform numeric representation -- value.go's doc comment states the
// value model is "exactly one of nil, string, float64, bool, []any, or
// map[string]any", and every comparison/arithmetic primitive in that file
// (ScalarEq, PropEq, OrderCompare, In, jsonbEqual) type-switches on float64
// for numbers, with no int64 case. Returning a real int64 here would make
// `WHERE id(n) = 5` silently fail (jsonbEqual's default case returns false
// for an unrecognized dynamic type, since int64 isn't float64) and would
// make id(n) unusable in arithmetic -- caught concretely by this task's own
// `datetime().epochseconds - ...` test scenario needing the same treatment.
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
		return float64(env.Snap.GraphIDs[nodeID]), true, nil
	}
	if edgeRef, ok := row.Edge(v.Symbol); ok {
		return float64(env.Snap.OutEdgeIDs[edgeRef.Fwd]), true, nil
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

	lo, hi := env.Snap.KindOffsets[nodeID], env.Snap.KindOffsets[nodeID+1]
	kinds := env.Snap.NodeKinds[lo:hi]
	out := make([]any, 0, len(kinds))
	for _, k := range kinds {
		if name, found := env.Snap.Kinds.Name(k); found {
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
	name, found := env.Snap.Kinds.Name(env.Snap.OutKinds[edgeRef.Fwd])
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
// Per the brief, size() of a string is a different pg rendering (character
// length, not array length) that this evaluator does not attempt to
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
	if !ok || val == nil {
		return nil, false, nil
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
func evalArithmetic(env *Env, row *Row, ae *cypher.ArithmeticExpression) (any, bool, error) {
	cur, curOk, err := EvalValue(env, row, ae.Left)
	if err != nil {
		return nil, false, err
	}
	for _, partial := range ae.Partials {
		if partial == nil {
			return nil, false, ErrUnsupported
		}
		rVal, rOk, err := EvalValue(env, row, partial.Right)
		if err != nil {
			return nil, false, err
		}
		cur, curOk, err = applyArithmetic(cur, curOk, partial.Operator, rVal, rOk)
		if err != nil {
			return nil, false, err
		}
	}
	return cur, curOk, nil
}

// applyArithmetic implements one arithmetic step over float64 operands.
// Cypher also defines `+` for string/list concatenation, but the brief
// scopes this evaluator to numeric arithmetic only, so a non-numeric operand
// is ErrUnsupported (expected to be pre-rejected, or handled by a different
// code path, at plan time).
func applyArithmetic(a any, aOk bool, op cypher.Operator, b any, bOk bool) (any, bool, error) {
	if !aOk || !bOk || a == nil || b == nil {
		return nil, false, nil
	}
	af, aIsNum := a.(float64)
	bf, bIsNum := b.(float64)
	if !aIsNum || !bIsNum {
		return nil, false, ErrUnsupported
	}

	switch op {
	case cypher.OperatorAdd:
		return af + bf, true, nil
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
