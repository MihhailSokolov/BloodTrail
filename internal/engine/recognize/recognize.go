// SPDX-License-Identifier: Apache-2.0

// Package recognize identifies BloodHound's canonical FetchAllShortestPaths
// query shapes -- the graph.Criteria tree the API builds directly
// (FromCriteria), plus the builder-query shapes recognized elsewhere in this
// package (builder.go/projection.go) -- and translates them into a
// PathQuery the in-memory engine can serve without delegating to
// PostgreSQL.
//
// This package's own Cypher-text equivalent of FromCriteria (FromCypher)
// existed only through milestone 3; milestone 4's general-purpose Cypher
// interpreter (internal/engine/interpret) superseded it entirely, and
// engine.TryCypher now serves Cypher text directly through that interpreter
// instead of recognizing a fixed shortestPath/allShortestPaths shape here.
//
// It intentionally does not import internal/engine/traverse: Mode mirrors
// traverse.Mode's constants as a separate, plain-int type so the two
// packages avoid an import cycle; engine callers convert between them.
package recognize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

// Variable symbols dawgs/query's identifier constructors attach to the
// Cypher AST nodes they build (see dawgs's query/identifiers.go):
// NodeSymbol "n", EdgeSymbol "r" for the relationship being matched, and
// EdgeStartSymbol "s" / EdgeEndSymbol "e" for its endpoints. Pinned here as
// private constants -- recognize walks the AST by concrete type rather than
// importing dawgs/query for its constructors, but must still recognize
// exactly the symbols those constructors use.
const (
	nodeSymbol  = "n"
	edgeSymbol  = "r"
	startSymbol = "s"
	endSymbol   = "e"
)

// Mode selects how many shortest paths a PathQuery should return per pair,
// mirroring internal/engine/traverse.Mode's constants. Defined separately
// here (rather than imported) to avoid a recognize <-> traverse import
// cycle; engine callers convert between the two.
type Mode int

const (
	// ModeAll returns every shortest path per pair (allShortestPaths /
	// FetchAllShortestPaths).
	ModeAll Mode = iota
	// ModeOne returns a single shortest path per pair (cypher
	// shortestPath()).
	ModeOne
)

// Endpoint constrains one side (start or end) of a recognized PathQuery.
// IDs holds explicit database ids when the query names them directly; nil
// means the endpoint is predicate-defined instead, in which case Kinds
// and/or Criteria narrow it. Criteria is non-nil iff property predicates
// exist; when set, it is built with the dawgs query package against
// query.Node(), kinds already included, ready for tx.Nodes().Filter(...).
type Endpoint struct {
	IDs      []graph.ID
	Kinds    graph.Kinds
	Criteria graph.Criteria
}

// PathQuery is a recognized FetchAllShortestPaths (or equivalent) request,
// ready for the in-memory engine to serve directly instead of delegating to
// PostgreSQL.
type PathQuery struct {
	Start, End  Endpoint
	EdgeKinds   graph.Kinds // empty => all kinds
	Mode        Mode
	ExcludeSelf bool
	Limit       int
}

// matchIDEquals reports whether cmp has the shape id(<var>) = <id>, as built
// by query.Equals(query.StartID()/EndID(), id): a single-partial "="
// comparison whose left side is a 1-argument id() FunctionInvocation over a
// bare Variable. On match it returns the referenced variable's symbol
// (compare against startSymbol/endSymbol/etc.) and the unwrapped id; ok is
// false for any other shape, including a nil cmp -- callers must not assume
// symbol is one of the known constants just because ok is true.
func matchIDEquals(cmp *cypher.Comparison) (symbol string, id graph.ID, ok bool) {
	if cmp == nil || len(cmp.Partials) != 1 {
		return "", 0, false
	}

	partial := cmp.Partials[0]
	if partial == nil || partial.Operator != cypher.OperatorEquals {
		return "", 0, false
	}

	fn, isFunctionInvocation := cmp.Left.(*cypher.FunctionInvocation)
	if !isFunctionInvocation || fn == nil || fn.Name != "id" || len(fn.Arguments) != 1 {
		return "", 0, false
	}

	variable, isVariable := fn.Arguments[0].(*cypher.Variable)
	if !isVariable || variable == nil {
		return "", 0, false
	}

	resultID, unwrapped := literalToID(partial.Right)
	if !unwrapped {
		return "", 0, false
	}

	return variable.Symbol, resultID, true
}

// literalToID unwraps a query.Equals right-hand side to a graph.ID. The
// dawgs query package always wraps the value in query.Parameter (a
// *cypher.Parameter with an empty Symbol), but a bare *cypher.Literal is
// accepted too since a cypher-text recognizer sees literals
// parsed straight from query text. The wrapped value itself must be a
// graph.ID, int64, or uint64; anything else -- including a nil expr, a null
// Literal, or a Parameter/Literal wrapping some other Go type -- fails.
func literalToID(expr cypher.Expression) (graph.ID, bool) {
	var value any

	switch typed := expr.(type) {
	case *cypher.Parameter:
		if typed == nil {
			return 0, false
		}
		value = typed.Value
	case *cypher.Literal:
		if typed == nil || typed.Null {
			return 0, false
		}
		value = typed.Value
	default:
		return 0, false
	}

	return valueToID(value)
}

// valueToID converts an already-unwrapped Parameter/Literal payload (see
// literalToID) into a graph.ID: accepted types are graph.ID, int64, and
// uint64; anything else fails. Factored out of literalToID so builder.go's
// matchIDIn can apply the same scalar-conversion rule to each element of a
// []any id list (query.InIDs itself never produces one -- it always wraps a
// single []graph.ID -- but a hand-built or differently-encoded id list
// might) without duplicating the type switch.
func valueToID(value any) (graph.ID, bool) {
	switch v := value.(type) {
	case graph.ID:
		return v, true
	case int64:
		return graph.ID(v), true
	case uint64:
		return graph.ID(v), true
	default:
		return 0, false
	}
}

// matchRelationshipKinds reports whether km is query.KindIn(query.Relationship(), ...)
// -- a KindMatcher whose reference is the bare relationship variable
// (edgeSymbol) -- returning its kinds. A KindMatcher over any other
// reference (notably query.Node(), nodeSymbol) fails, as does a nil km.
func matchRelationshipKinds(km *cypher.KindMatcher) (graph.Kinds, bool) {
	if km == nil {
		return nil, false
	}

	variable, isVariable := km.Reference.(*cypher.Variable)
	if !isVariable || variable == nil || variable.Symbol != edgeSymbol {
		return nil, false
	}

	return km.Kinds, true
}
