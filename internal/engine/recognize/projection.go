// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

// RowProjection identifies which of the three canonical relationship-query
// RETURN shapes -- all built by dawgs' query.Returning -- FromReturning
// recognized, so a served query's row-scan step knows which columns to
// synthesize per relationship without re-deriving the shape from the AST
// again at scan time.
type RowProjection int

const (
	// ProjectionStartEnd is query.Returning(query.StartID(), query.EndID())
	// -- dawgs container/fetch.go's bulk edge-list fetch, which only needs
	// each relationship's two endpoint ids.
	ProjectionStartEnd RowProjection = iota

	// ProjectionStepOutbound is query.Returning(query.EndID(),
	// query.KindsOf(query.End()), query.RelationshipID(),
	// query.KindsOf(query.Relationship())) -- dawgs traversal/traversal.go's
	// shallowFetchRelationships for an outbound step: the far endpoint's id
	// and kinds, plus the traversed relationship's id and kind.
	ProjectionStepOutbound

	// ProjectionStepInbound is query.Returning(query.StartID(),
	// query.KindsOf(query.Start()), query.RelationshipID(),
	// query.KindsOf(query.Relationship())) -- shallowFetchRelationships'
	// inbound-step mirror of ProjectionStepOutbound.
	ProjectionStepInbound
)

// FromReturning recognizes criteria as one of the three RowProjection shapes
// documented above, as built by dawgs' query.Returning against a
// relationship pattern's three variables (query.Start()/"s",
// query.Relationship()/"r", query.End()/"e"). Match is purely structural and
// positional: criteria must be a non-nil *cypher.Return whose Projection is
// non-nil, has Distinct/All/Order/Skip/Limit all unset -- query.Returning
// never sets any of them unless one of its variadic arguments is itself a
// *cypher.Order/*cypher.Limit/*cypher.Skip, which none of the three
// recognized call sites pass -- and whose Items are, in exact order and
// count, unaliased *cypher.ProjectionItem values wrapping the expected
// id()/labels()/type() FunctionInvocation over the expected variable symbol
// (see matchIdentityOf/matchFunctionOf). Any other shape, including the
// right items in the wrong order, an extra or missing item, an item over the
// wrong variable, or Distinct set, reports ok=false. FromReturning never
// panics: every AST level is nil-checked before use.
func FromReturning(criteria graph.Criteria) (RowProjection, bool) {
	items, ok := returnItems(criteria)
	if !ok {
		return 0, false
	}

	switch {
	case matchStartEndProjection(items):
		return ProjectionStartEnd, true
	case matchStepProjection(items, endSymbol):
		return ProjectionStepOutbound, true
	case matchStepProjection(items, startSymbol):
		return ProjectionStepInbound, true
	default:
		return 0, false
	}
}

// returnItems unwraps criteria into its projection items' expressions, in
// order, reporting ok=false for anything that isn't a well-formed, plain
// (no Distinct/All/Order/Skip/Limit) *cypher.Return -- see FromReturning's
// doc -- or whose Items contains anything but an unaliased
// *cypher.ProjectionItem.
func returnItems(criteria graph.Criteria) ([]cypher.Expression, bool) {
	ret, isReturn := criteria.(*cypher.Return)
	if !isReturn || ret == nil || ret.Projection == nil {
		return nil, false
	}

	projection := ret.Projection
	if projection.Distinct || projection.All || projection.Order != nil || projection.Skip != nil || projection.Limit != nil {
		return nil, false
	}

	items := make([]cypher.Expression, 0, len(projection.Items))
	for _, raw := range projection.Items {
		item, isItem := raw.(*cypher.ProjectionItem)
		if !isItem || item == nil || item.Alias != nil {
			return nil, false
		}
		items = append(items, item.Expression)
	}

	return items, true
}

// matchStartEndProjection reports whether items is exactly
// [id(s), id(e)] -- query.Returning(query.StartID(), query.EndID()).
func matchStartEndProjection(items []cypher.Expression) bool {
	return len(items) == 2 &&
		matchIdentityOf(items[0], startSymbol) &&
		matchIdentityOf(items[1], endSymbol)
}

// matchStepProjection reports whether items is exactly [id(<endpoint>),
// labels(<endpoint>), id(r), type(r)] for the given endpoint symbol
// (endSymbol for ProjectionStepOutbound, startSymbol for
// ProjectionStepInbound) -- the shape shallowFetchRelationships builds for
// that step direction.
func matchStepProjection(items []cypher.Expression, endpointSymbol string) bool {
	return len(items) == 4 &&
		matchIdentityOf(items[0], endpointSymbol) &&
		matchFunctionOf(items[1], "labels", endpointSymbol) &&
		matchIdentityOf(items[2], edgeSymbol) &&
		matchFunctionOf(items[3], "type", edgeSymbol)
}

// matchIdentityOf reports whether expr is id(<symbol>) -- exactly what
// query.Identity (and therefore query.StartID/EndID/RelationshipID/NodeID)
// builds: a 1-argument FunctionInvocation named "id" over a bare Variable
// carrying the given symbol. Unlike matchIDEquals/matchIDIn in recognize.go,
// which match id(<var>) as one side of a Comparison, here the
// FunctionInvocation is itself the whole projection-item expression, so
// matchIdentityOf is a thin wrapper over the shared matchFunctionOf.
func matchIdentityOf(expr cypher.Expression, symbol string) bool {
	return matchFunctionOf(expr, "id", symbol)
}

// matchFunctionOf reports whether expr is a 1-argument FunctionInvocation
// named name over a bare Variable carrying the given symbol -- the shape
// every projection-item expression FromReturning recognizes takes (id(),
// query.KindsOf's "labels" for a node-side variable, or its "type" for the
// relationship variable; see query/model.go's Identity and KindsOf). A nil
// expr, a non-FunctionInvocation, a wrong name, an argument count other than
// one, a non-Variable argument, or a wrong symbol all fail.
func matchFunctionOf(expr cypher.Expression, name, symbol string) bool {
	fn, isFunctionInvocation := expr.(*cypher.FunctionInvocation)
	if !isFunctionInvocation || fn == nil || fn.Name != name || len(fn.Arguments) != 1 {
		return false
	}

	variable, isVariable := fn.Arguments[0].(*cypher.Variable)
	return isVariable && variable != nil && variable.Symbol == symbol
}

// OrderIsEdgeIDAscending reports whether orderCriteria -- the variadic
// argument list a graph.RelationshipQuery.OrderBy(...) call was given --
// is exactly one criterion requesting ascending order on the relationship's
// id. Both spellings dawgs itself uses are accepted:
//
//   - query.Order(query.Identity(query.Relationship()), query.Ascending())
//     (traversal/traversal.go:539, shallowFetchRelationships' paging order)
//   - query.Order(query.Relationship(), query.Ascending())
//     (ops/traversal.go:100, TraversalPlan's paging order)
//
// query.Order always returns a *cypher.SortItem regardless of which
// spelling was used, so both collapse to the same check: len(orderCriteria)
// == 1, that one element is a non-nil *cypher.SortItem with Ascending true,
// and its Expression is either id(r) (matchIdentityOf) or the bare
// relationship Variable "r" itself. Anything else -- zero or more than one
// criterion, a descending SortItem, an order over any other variable
// (notably id(n), query.NodeID()'s shape), or an element that isn't a
// *cypher.SortItem at all -- reports false. OrderIsEdgeIDAscending never
// panics: every level is nil-checked before use.
func OrderIsEdgeIDAscending(orderCriteria []graph.Criteria) bool {
	if len(orderCriteria) != 1 {
		return false
	}

	sortItem, isSortItem := orderCriteria[0].(*cypher.SortItem)
	if !isSortItem || sortItem == nil || !sortItem.Ascending {
		return false
	}

	if matchIdentityOf(sortItem.Expression, edgeSymbol) {
		return true
	}

	variable, isVariable := sortItem.Expression.(*cypher.Variable)
	return isVariable && variable != nil && variable.Symbol == edgeSymbol
}
