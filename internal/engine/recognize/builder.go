// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

// KindConstraint is one KindMatcher conjunct recognized from a graph.
// Criteria tree, carrying the two facts dawgs' PostgreSQL translator
// (cypher/models/pgsql/translate/kind.go, newPGKindIDMatcher) actually
// needs to evaluate it the same way PostgreSQL would:
//
//   - AllOf=false, from a KindMatcher with IsExclusive=false -- what
//     query.Kind and query.KindIn always build (query/model.go: query.Kind
//     sets IsExclusive: false directly, query.KindIn via
//     cypher.NewKindMatcher(reference, kinds, false)) -- means the entity
//     must carry at least one of Kinds. On a node variable this compiles to
//     an array-overlap test ("&&" in PostgreSQL); on the relationship
//     variable it compiles to "kind_id = ANY(...)" regardless of
//     IsExclusive, since edge-kind checking is a strict equality in the
//     translator and exclusivity never applies there -- either way, ANY-OF.
//   - AllOf=true, from a KindMatcher with IsExclusive=true -- what the
//     Cypher frontend always builds for a `:Label` matcher parsed from
//     query text (cypher/frontend/expression.go,
//     NonArithmeticOperatorExpressionVisitor.EnterOC_NodeLabels: "Cypher-
//     generated KindMatchers should _always_ be exclusive to jive with the
//     spec") -- means the entity must carry every one of Kinds. On a node
//     variable this compiles to an array-containment test ("@>"); as above,
//     it has no distinct meaning on the relationship variable.
//
// FromNodeCriteria and FromRelCriteria both decline (rather than model) a
// KindMatcher whose Kinds is empty. The translator's containment branch
// only fires when IsExclusive is true AND len(kindIDs) > 0; an empty list
// always falls through to the overlap branch regardless of IsExclusive, and
// an overlap test against an empty right-hand side matches nothing at all
// -- a meaning KindConstraint's zero value (Kinds: nil, no constraint) could
// never represent, so that shape is rejected outright rather than risk
// being read as "unconstrained."
type KindConstraint struct {
	Kinds graph.Kinds
	AllOf bool
}

// NodeSpec is a graph.Criteria tree recognized over the bare node variable
// (query.Node(), symbol "n") -- the shape BloodHound's builder queries use
// for e.g. FetchNodeIDsByKind -- translated by FromNodeCriteria into the
// form the in-memory engine can serve directly.
//
// IDs is nil when the criteria never constrained id(n) at all; a non-nil
// (possibly zero-length) IDs means it did constrain it -- see
// FromNodeCriteria's doc for how multiple id() conjuncts combine. A
// zero-length, non-nil IDs is a well-formed spec that can never match any
// node (e.g. id(n) = 1 AND id(n) = 2).
//
// Constraints holds one KindConstraint per KindMatcher conjunct
// FromNodeCriteria encountered, kept separate rather than merged into a
// single Kinds/AllOf pair -- see FromNodeCriteria's doc for why that
// distinction matters. A node satisfies NodeSpec's kind requirement only if
// it satisfies every entry (Cypher's AND semantics for multiple conjuncts).
type NodeSpec struct {
	IDs         []graph.ID
	Constraints []KindConstraint
}

// ConstraintKinds returns the union (deduped via graph.Kinds.Add, in
// first-occurrence order) of every KindConstraint.Kinds in s.Constraints.
// It tells a caller which kinds could possibly be relevant to evaluating
// s.Constraints at all -- e.g. which kind-partitioned indexes to consult --
// not whether any single one of them is sufficient: s.Constraints still
// ANDs together, and each individual KindConstraint keeps its own AllOf
// semantics (see KindConstraint's doc). An empty Constraints returns nil.
func (s NodeSpec) ConstraintKinds() graph.Kinds {
	var union graph.Kinds
	for _, constraint := range s.Constraints {
		union = union.Add(constraint.Kinds...)
	}
	return union
}

// RelSpec is a graph.Criteria tree recognized over a relationship
// pattern's three variables -- query.Start()/"s", query.Relationship()/"r",
// query.End()/"e" -- translated by FromRelCriteria into the form the
// in-memory engine can serve directly.
//
// StartIDs and EndIDs follow NodeSpec.IDs' nil-vs-non-nil convention,
// independently per endpoint: an id() conjunct naming one endpoint never
// affects the other's IDs.
//
// EdgeKinds is nil when no KindMatcher over "r" was present in the
// criteria at all, meaning "all kinds"; a *second* such KindMatcher rejects
// the whole criteria instead of being modeled (see FromRelCriteria's doc
// for why). Unlike KindConstraint, there is no separate AllOf flag for it:
// as KindConstraint's doc explains, the relationship variable's kind check
// is a strict "= ANY(...)" test regardless of IsExclusive, so ANY-OF is the
// only meaning EdgeKinds ever has.
//
// StartConstraints and EndConstraints follow NodeSpec.Constraints': one
// KindConstraint per KindMatcher conjunct over that endpoint, kept as
// separate entries (unlike EdgeKinds, multiple are accepted, not rejected)
// rather than merged.
type RelSpec struct {
	StartIDs, EndIDs                 []graph.ID
	EdgeKinds                        graph.Kinds
	StartConstraints, EndConstraints []KindConstraint
}

// NodeConstraintKinds returns the union (deduped, see NodeSpec.
// ConstraintKinds) of every kind named by either endpoint's constraints --
// s.StartConstraints and s.EndConstraints combined. The same caveats as
// NodeSpec.ConstraintKinds apply, independently to each endpoint's own
// constraint list.
func (s RelSpec) NodeConstraintKinds() graph.Kinds {
	var union graph.Kinds
	for _, constraint := range s.StartConstraints {
		union = union.Add(constraint.Kinds...)
	}
	for _, constraint := range s.EndConstraints {
		union = union.Add(constraint.Kinds...)
	}
	return union
}

// FromNodeCriteria recognizes a graph.Criteria tree built against the bare
// node variable (query.Node(), symbol "n") into a NodeSpec.
//
// criteria may be a bare accepted conjunct -- e.g. the single KindMatcher
// FetchNodeIDsByKind builds with no wrapping query.And, or a bare
// query.InIDs(...) -- or a *cypher.Conjunction of them; nested
// conjunctions flatten (query.And(a, query.And(b, c)) is equivalent to
// query.And(a, b, c)). Every conjunct must reference only the "n" symbol;
// each is classified as one of:
//
//  1. A KindMatcher over "n" -- appended to Constraints as its own
//     KindConstraint (AllOf from IsExclusive; see KindConstraint's doc). It
//     is never merged with another KindMatcher conjunct:
//     query.And(query.Kind(n, A), query.Kind(n, B)) produces two
//     Constraints entries -- "carries A" AND "carries B" -- which is a
//     materially stricter requirement than the single
//     query.KindIn(n, A, B) ("carries A or B"); collapsing the two shapes
//     into one Kinds/AllOf pair would erase that distinction, which is
//     exactly why NodeSpec keeps a slice instead of a single field.
//  2. id(n) = <id> (matchIDEquals) or id(n) IN <ids> (matchIDIn) -- folded
//     into NodeSpec.IDs. Multiple id conjuncts intersect rather than union
//     (see idAccumulator): id(n) = 1 AND id(n) IN [1, 2] means "the id
//     that is both 1 and in {1, 2}", i.e. {1}, not {1, 2}. Duplicate ids,
//     whether repeated across conjuncts or within one IN list, dedupe.
//
// Anything else rejects the whole criteria (ok=false): a property lookup
// (e.g. query.Equals(query.NodeProperty(...), ...) -- its left side isn't
// id(), so neither matchIDEquals nor matchIDIn recognizes it), a Negation
// (query.Not -- e.g. the IgnoreMetaFilter shape
// query.Not(query.KindIn(...))), a Parenthetical/Disjunction (query.Or), a
// comparison with an operator other than = or IN over id(), a conjunct
// naming any symbol other than "n" (including "s"/"e"/"r" -- a relationship
// endpoint constraint has no place in node-only criteria), an unknown
// symbol, a KindMatcher with empty Kinds (see KindConstraint's doc), or a
// nil/non-Expression criteria. This is a default-deny walk: any conjunct
// type this function doesn't explicitly recognize rejects the whole
// criteria, even if every other conjunct is well-formed. FromNodeCriteria
// never panics: every AST level is nil-checked before use.
func FromNodeCriteria(criteria graph.Criteria) (NodeSpec, bool) {
	expr, isExpression := criteria.(cypher.Expression)
	if !isExpression {
		return NodeSpec{}, false
	}

	acc := &nodeAccumulator{}
	if !walkNodeConjunct(expr, acc) {
		return NodeSpec{}, false
	}

	spec := NodeSpec{Constraints: acc.constraints}
	if acc.ids.touched {
		spec.IDs = acc.ids.ids
	}

	return spec, true
}

// nodeAccumulator collects FromNodeCriteria's running state while
// walkNodeConjunct walks criteria's conjuncts: the running id-intersection
// (idAccumulator) and the independent list of kind constraints.
type nodeAccumulator struct {
	ids         idAccumulator
	constraints []KindConstraint
}

// addKindMatcher validates and records a KindMatcher conjunct over "n",
// reporting false for anything that doesn't match that exact shape (nil
// matcher, empty Kinds, a non-Variable or wrong-symbol Reference).
func (acc *nodeAccumulator) addKindMatcher(km *cypher.KindMatcher) bool {
	if km == nil || len(km.Kinds) == 0 {
		return false
	}

	variable, isVariable := km.Reference.(*cypher.Variable)
	if !isVariable || variable == nil || variable.Symbol != nodeSymbol {
		return false
	}

	acc.constraints = append(acc.constraints, KindConstraint{Kinds: km.Kinds, AllOf: km.IsExclusive})
	return true
}

// addID folds a single id() equality conjunct's (symbol, id) pair -- as
// returned by matchIDEquals -- into acc.ids, reporting false unless symbol
// is "n".
func (acc *nodeAccumulator) addID(symbol string, id graph.ID) bool {
	if symbol != nodeSymbol {
		return false
	}
	acc.ids.add([]graph.ID{id})
	return true
}

// addIDs folds an id() IN conjunct's (symbol, ids) pair -- as returned by
// matchIDIn -- into acc.ids, reporting false unless symbol is "n".
func (acc *nodeAccumulator) addIDs(symbol string, ids []graph.ID) bool {
	if symbol != nodeSymbol {
		return false
	}
	acc.ids.add(ids)
	return true
}

// walkNodeConjunct classifies expr as one of FromNodeCriteria's accepted
// shapes, recursing into a *cypher.Conjunction's children (so nested
// conjunctions flatten to arbitrary depth) and folding a leaf conjunct's
// contribution into acc. It returns false -- meaning FromNodeCriteria must
// reject the whole criteria -- for any expr it doesn't recognize.
func walkNodeConjunct(expr cypher.Expression, acc *nodeAccumulator) bool {
	switch typed := expr.(type) {
	case *cypher.Conjunction:
		if typed == nil {
			return false
		}
		for _, child := range typed.GetAll() {
			if !walkNodeConjunct(child, acc) {
				return false
			}
		}
		return true

	case *cypher.KindMatcher:
		return acc.addKindMatcher(typed)

	case *cypher.Comparison:
		if symbol, id, matched := matchIDEquals(typed); matched {
			return acc.addID(symbol, id)
		}
		if symbol, ids, matched := matchIDIn(typed); matched {
			return acc.addIDs(symbol, ids)
		}
		return false

	default:
		// Includes *cypher.Negation, *cypher.Parenthetical/Disjunction,
		// *cypher.PropertyLookup, and any other conjunct type: default-deny.
		return false
	}
}

// FromRelCriteria recognizes a graph.Criteria tree built against a
// relationship pattern's three variables -- query.Start()/"s",
// query.Relationship()/"r", query.End()/"e" -- into a RelSpec.
//
// criteria accepts the same wrapping shapes as FromNodeCriteria (a bare
// accepted conjunct, or a *cypher.Conjunction that flattens through nested
// conjunctions), but over three symbols instead of one. Each conjunct is
// classified as one of:
//
//  1. A KindMatcher over "r" -- becomes RelSpec.EdgeKinds. Unlike a
//     KindMatcher over "n" (FromNodeCriteria) or "s"/"e" (case 2 below),
//     AllOf is not tracked for it, and at most one is accepted: see
//     RelSpec's doc for why ANY-OF is the only meaning EdgeKinds needs, and
//     why a second KindMatcher over "r" rejects the whole criteria
//     (query.And(query.KindIn(r, A, B), query.KindIn(r, C)) means "kind is
//     in {A,B} AND kind is in {C}" -- the AND of two ANY-OF sets -- which a
//     single Kinds slice can't represent without recomputing their
//     intersection; declining is simpler and always correct).
//  2. A KindMatcher over "s" or "e" -- appended to StartConstraints or
//     EndConstraints respectively, one KindConstraint per conjunct (see
//     FromNodeCriteria's doc, case 1, for why these don't merge; here,
//     unlike case 1 above, more than one is accepted).
//  3. id(s) or id(e), = or IN -- folded into StartIDs/EndIDs independently
//     (see FromNodeCriteria's doc, case 2, for the intersection/dedup
//     rules, applied separately per endpoint).
//
// A conjunct naming "n" (the bare-node symbol) anywhere rejects the whole
// criteria: FromRelCriteria only recognizes relationship-pattern criteria,
// and "n" has no role in one. The same rejections as FromNodeCriteria apply
// otherwise (property lookups, Negation, Parenthetical/Disjunction, an
// unknown symbol, an empty-Kinds KindMatcher, a comparison operator other
// than = or IN over id(), a nil/non-Expression criteria) -- this is a
// default-deny walk exactly like FromNodeCriteria's. FromRelCriteria never
// panics: every AST level is nil-checked before use.
func FromRelCriteria(criteria graph.Criteria) (RelSpec, bool) {
	expr, isExpression := criteria.(cypher.Expression)
	if !isExpression {
		return RelSpec{}, false
	}

	acc := &relAccumulator{}
	if !walkRelConjunct(expr, acc) {
		return RelSpec{}, false
	}

	spec := RelSpec{
		EdgeKinds:        acc.edgeKinds,
		StartConstraints: acc.startConstraints,
		EndConstraints:   acc.endConstraints,
	}
	if acc.startIDs.touched {
		spec.StartIDs = acc.startIDs.ids
	}
	if acc.endIDs.touched {
		spec.EndIDs = acc.endIDs.ids
	}

	return spec, true
}

// relAccumulator collects FromRelCriteria's running state while
// walkRelConjunct walks criteria's conjuncts: separate id-intersections and
// kind-constraint lists for the start and end endpoints, and the single
// (reject-on-second) relationship kind matcher.
type relAccumulator struct {
	startIDs, endIDs                 idAccumulator
	startConstraints, endConstraints []KindConstraint
	edgeKinds                        graph.Kinds
	haveEdgeKinds                    bool
}

// addKindMatcher validates and records a KindMatcher conjunct over "s",
// "e", or "r", reporting false for anything that doesn't match one of
// those shapes: a nil matcher, empty Kinds, a non-Variable Reference, an
// unrecognized symbol (including "n" -- FromRelCriteria never accepts it),
// or a second KindMatcher over "r".
func (acc *relAccumulator) addKindMatcher(km *cypher.KindMatcher) bool {
	if km == nil || len(km.Kinds) == 0 {
		return false
	}

	variable, isVariable := km.Reference.(*cypher.Variable)
	if !isVariable || variable == nil {
		return false
	}

	switch variable.Symbol {
	case edgeSymbol:
		if acc.haveEdgeKinds {
			return false
		}
		acc.edgeKinds, acc.haveEdgeKinds = km.Kinds, true
		return true

	case startSymbol:
		acc.startConstraints = append(acc.startConstraints, KindConstraint{Kinds: km.Kinds, AllOf: km.IsExclusive})
		return true

	case endSymbol:
		acc.endConstraints = append(acc.endConstraints, KindConstraint{Kinds: km.Kinds, AllOf: km.IsExclusive})
		return true

	default:
		return false
	}
}

// addID folds a single id() equality conjunct's (symbol, id) pair -- as
// returned by matchIDEquals -- into the matching endpoint's id
// accumulator, reporting false unless symbol is "s" or "e".
func (acc *relAccumulator) addID(symbol string, id graph.ID) bool {
	switch symbol {
	case startSymbol:
		acc.startIDs.add([]graph.ID{id})
		return true
	case endSymbol:
		acc.endIDs.add([]graph.ID{id})
		return true
	default:
		return false
	}
}

// addIDs folds an id() IN conjunct's (symbol, ids) pair -- as returned by
// matchIDIn -- into the matching endpoint's id accumulator, reporting
// false unless symbol is "s" or "e".
func (acc *relAccumulator) addIDs(symbol string, ids []graph.ID) bool {
	switch symbol {
	case startSymbol:
		acc.startIDs.add(ids)
		return true
	case endSymbol:
		acc.endIDs.add(ids)
		return true
	default:
		return false
	}
}

// walkRelConjunct classifies expr as one of FromRelCriteria's accepted
// shapes, recursing into a *cypher.Conjunction's children (so nested
// conjunctions flatten to arbitrary depth) and folding a leaf conjunct's
// contribution into acc. It returns false -- meaning FromRelCriteria must
// reject the whole criteria -- for any expr it doesn't recognize.
func walkRelConjunct(expr cypher.Expression, acc *relAccumulator) bool {
	switch typed := expr.(type) {
	case *cypher.Conjunction:
		if typed == nil {
			return false
		}
		for _, child := range typed.GetAll() {
			if !walkRelConjunct(child, acc) {
				return false
			}
		}
		return true

	case *cypher.KindMatcher:
		return acc.addKindMatcher(typed)

	case *cypher.Comparison:
		if symbol, id, matched := matchIDEquals(typed); matched {
			return acc.addID(symbol, id)
		}
		if symbol, ids, matched := matchIDIn(typed); matched {
			return acc.addIDs(symbol, ids)
		}
		return false

	default:
		// Includes *cypher.Negation, *cypher.Parenthetical/Disjunction,
		// *cypher.PropertyLookup, and any other conjunct type: default-deny.
		return false
	}
}

// idAccumulator computes the running intersection of every id(<var>) = <id>
// / id(<var>) IN <ids> conjunct FromNodeCriteria or FromRelCriteria has
// seen for one endpoint, matching Cypher's AND semantics for two such
// conjuncts naming the same variable: the entity's id must satisfy every
// one of them at once, so id() conjuncts combine by set intersection
// rather than union. touched distinguishes "no id() conjunct was present
// at all" (the endpoint is otherwise unconstrained by id; FromNodeCriteria
// / FromRelCriteria leave the corresponding *Spec field nil) from "the
// id() conjuncts seen intersect to the empty set" (ids is a non-nil,
// zero-length slice: a well-formed spec that can never match any entity,
// e.g. id(s) = 1 AND id(s) = 2). Every add call also dedupes its own input
// first (id(n) IN [1, 1, 2] contributes the set {1, 2}), so duplicates can
// never survive into the final result regardless of how many conjuncts
// contribute them or in what order.
type idAccumulator struct {
	ids     []graph.ID
	touched bool
}

// add intersects ids into the running result: the first call seeds it
// (deduped, in first-occurrence order); every later call keeps only the
// elements already present in both, preserving the running result's
// existing order.
func (a *idAccumulator) add(ids []graph.ID) {
	deduped := dedupeIDs(ids)

	if !a.touched {
		a.ids, a.touched = deduped, true
		return
	}

	present := make(map[graph.ID]bool, len(deduped))
	for _, id := range deduped {
		present[id] = true
	}

	kept := make([]graph.ID, 0, len(a.ids))
	for _, id := range a.ids {
		if present[id] {
			kept = append(kept, id)
		}
	}
	a.ids = kept
}

// dedupeIDs returns ids with duplicates removed, preserving first-
// occurrence order. A nil or empty input returns a non-nil, empty slice, so
// idAccumulator.add can always tell "seeded with zero ids" (ids non-nil,
// touched true) apart from "never seeded" (touched false) purely via
// touched, never by checking whether the stored slice happens to be nil.
func dedupeIDs(ids []graph.ID) []graph.ID {
	seen := make(map[graph.ID]bool, len(ids))
	deduped := make([]graph.ID, 0, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			deduped = append(deduped, id)
		}
	}
	return deduped
}

// matchIDIn reports whether cmp has the shape id(<var>) IN <ids>, as built
// by query.InIDs(query.StartID()/EndID()/Relationship()/Node(), ids...): a
// single-partial "IN" comparison whose left side is a 1-argument id()
// FunctionInvocation over a bare Variable -- query.InIDs wraps a bare
// Variable reference in Identity() first (query/model.go), so the left
// side is id(<var>) either way, exactly like matchIDEquals's left side --
// and whose right side unwraps to a []graph.ID via idListFrom. On match it
// returns the referenced variable's symbol and the unwrapped ids in their
// original (non-deduped) order; ok is false for any other shape, including
// a nil cmp. Callers must not assume symbol is one of the known constants,
// or that ids is deduped, just because ok is true -- deduping and
// set-intersection are FromNodeCriteria / FromRelCriteria's job (via
// idAccumulator), not matchIDIn's.
func matchIDIn(cmp *cypher.Comparison) (symbol string, ids []graph.ID, ok bool) {
	if cmp == nil || len(cmp.Partials) != 1 {
		return "", nil, false
	}

	partial := cmp.Partials[0]
	if partial == nil || partial.Operator != cypher.OperatorIn {
		return "", nil, false
	}

	fn, isFunctionInvocation := cmp.Left.(*cypher.FunctionInvocation)
	if !isFunctionInvocation || fn == nil || fn.Name != "id" || len(fn.Arguments) != 1 {
		return "", nil, false
	}

	variable, isVariable := fn.Arguments[0].(*cypher.Variable)
	if !isVariable || variable == nil {
		return "", nil, false
	}

	resultIDs, unwrapped := idListFrom(partial.Right)
	if !unwrapped {
		return "", nil, false
	}

	return variable.Symbol, resultIDs, true
}

// idListFrom unwraps a matchIDIn right-hand side into a []graph.ID. Two
// shapes are accepted:
//
//   - A *cypher.Parameter or *cypher.Literal wrapping one of the slice
//     shapes idsFromValue accepts -- what query.InIDs and a hand-built
//     query builder call produce (a single Go slice value boxed in the
//     Parameter/Literal).
//   - A bare *cypher.ListLiteral -- the shape a cypher-text `IN [1, 2, 3]`
//     parses to (cypher/frontend/literal.go): each element is its own
//     Expression, typically a *cypher.Literal, rather than a single Go
//     slice value boxed in a Parameter/Literal. Every element is unwrapped
//     with literalToID (reused rather than reimplemented); one that
//     doesn't unwrap fails the whole list rather than silently dropping it.
//
// A nil expr, a null Literal, or any other expression type fails.
func idListFrom(expr cypher.Expression) ([]graph.ID, bool) {
	switch typed := expr.(type) {
	case *cypher.Parameter:
		if typed == nil {
			return nil, false
		}
		return idsFromValue(typed.Value)

	case *cypher.Literal:
		if typed == nil || typed.Null {
			return nil, false
		}
		return idsFromValue(typed.Value)

	case *cypher.ListLiteral:
		if typed == nil {
			return nil, false
		}
		ids := make([]graph.ID, 0, len(*typed))
		for _, elem := range *typed {
			id, unwrapped := literalToID(elem)
			if !unwrapped {
				return nil, false
			}
			ids = append(ids, id)
		}
		return ids, true

	default:
		return nil, false
	}
}

// idsFromValue converts value -- already unwrapped from a *cypher.Parameter
// or *cypher.Literal by idListFrom -- into a []graph.ID. Accepted shapes:
// []graph.ID (what query.InIDs(ids...) wraps via query.Parameter), plain
// []int64 / []uint64 (a literal Go slice, for a query built by hand rather
// than through query.InIDs), and []any whose every element is one of those
// three scalar types via valueToID (recognize.go, shared with
// literalToID), in case some encoding path boxes a slice as []any
// element-by-element. Anything else, including a value that isn't a slice
// at all or a []any with one unsupported element, fails outright rather
// than returning a partial list.
func idsFromValue(value any) ([]graph.ID, bool) {
	switch v := value.(type) {
	case []graph.ID:
		return v, true

	case []int64:
		ids := make([]graph.ID, len(v))
		for i, elem := range v {
			ids[i] = graph.ID(elem)
		}
		return ids, true

	case []uint64:
		ids := make([]graph.ID, len(v))
		for i, elem := range v {
			ids[i] = graph.ID(elem)
		}
		return ids, true

	case []any:
		ids := make([]graph.ID, 0, len(v))
		for _, elem := range v {
			id, unwrapped := valueToID(elem)
			if !unwrapped {
				return nil, false
			}
			ids = append(ids, id)
		}
		return ids, true

	default:
		return nil, false
	}
}
