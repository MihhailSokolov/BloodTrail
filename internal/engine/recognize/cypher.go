// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// FromCypher recognizes the pre-built shortestPath / allShortestPaths Cypher
// text BloodHound's UI sends (bh-shared-ui's commonSearches*.ts), translating
// it into a PathQuery the in-memory engine can serve directly.
//
// Accepted shape (anything else reports ok=false): a single SinglePartQuery
// with exactly one MATCH, no WITH/UNION/UNWIND/mutations; one PatternPart
// whose ShortestPathPattern or AllShortestPathsPattern flag is set (mapping
// to ModeOne / ModeAll respectively) and whose pattern is node-relationship-
// node with an explicit direction (either arrow -- an undirected pattern is
// rejected) and a variable-length range whose lower bound is absent or 1
// (*1.., *..; a lower bound of 0 is rejected since it admits zero-length
// paths, and any upper bound is rejected); endpoint kind labels come from
// the node patterns, edge kinds from the relationship pattern (empty means
// "all kinds"). WHERE, if present, must be a conjunction (possibly nested in
// parentheses) of conjuncts, each classified as one of:
//
//  1. <var> <> <var> naming the two pattern variables (ExcludeSelf).
//  2. id(<var>) = <literal> (Endpoint.IDs).
//  3. <var>:Kind, a kind-label matcher on one pattern variable
//     (Endpoint.Kinds, merged with that node's pattern-declared kinds).
//  4. Endpoint predicate lifting: any other expression whose variable
//     references are exactly one pattern variable (start or end) is
//     deep-copied, every reference to that variable is rewritten to the
//     dawgs query-builder's node symbol ("n", what query.Node() returns),
//     and the result is appended to that endpoint's Endpoint.Criteria
//     (query.And(kindIn, lifted...) -- kinds first, then lifted predicates
//     in source order). This subsumes what used to be closed-form
//     <var>.prop = <literal> / <var>.prop ENDS WITH <string> handling --
//     those are just ordinary Comparisons under the same lifting rule now
//     -- and extends to NOT, OR/AND, regex (=~), and function calls
//     (COALESCE, ...) as long as the whole conjunct stays scoped to a
//     single pattern variable.
//  5. Anything else rejects the whole query: a conjunct naming both
//     pattern variables (other than case 1), the path variable, the
//     relationship variable, an unknown variable, no variable at all, a
//     $parameter anywhere in the conjunct, or a node shape this walker
//     doesn't recognize.
//
// RETURN must project exactly the path
// variable with an optional LIMIT; SKIP, ORDER BY, DISTINCT, and any other
// projection shape are rejected. For a pattern written with a `<-` arrow the
// element order is reversed from the traversal direction, so FromCypher
// swaps start/end to its normalized meaning: PathQuery.Start is always the
// arrow's tail (the traversal's source), End its head.
//
// FromCypher never panics: every AST level is nil-checked before use, and a
// recover() backstops the walk in case an assumption about the parser's
// output shape proves wrong.
func FromCypher(text string) (result PathQuery, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			result, ok = PathQuery{}, false
		}
	}()

	regularQuery, err := frontend.ParseCypher(frontend.DefaultCypherContext(), text)
	if err != nil || regularQuery == nil {
		return PathQuery{}, false
	}

	singleQuery := regularQuery.SingleQuery
	if singleQuery == nil || singleQuery.MultiPartQuery != nil || singleQuery.SinglePartQuery == nil {
		// A MultiPartQuery means the query has a WITH boundary; either way
		// we require exactly a SinglePartQuery.
		return PathQuery{}, false
	}

	singlePart := singleQuery.SinglePartQuery
	if len(singlePart.UpdatingClauses) != 0 || len(singlePart.ReadingClauses) != 1 {
		return PathQuery{}, false
	}

	readingClause := singlePart.ReadingClauses[0]
	if readingClause == nil || readingClause.Unwind != nil {
		return PathQuery{}, false
	}

	patternMatch, ok := matchPattern(readingClause.Match)
	if !ok {
		return PathQuery{}, false
	}

	start := &endpointAccumulator{symbol: patternMatch.startSymbol, patternKinds: patternMatch.startKinds}
	end := &endpointAccumulator{symbol: patternMatch.endSymbol, patternKinds: patternMatch.endKinds}

	excludeSelf, ok := matchWhere(readingClause.Match.Where, start, end)
	if !ok {
		return PathQuery{}, false
	}

	limit, ok := matchReturn(singlePart.Return, patternMatch.pathSymbol)
	if !ok {
		return PathQuery{}, false
	}

	return PathQuery{
		Start: Endpoint{
			IDs:      start.ids,
			Kinds:    patternMatch.startKinds,
			Criteria: start.criteria(),
		},
		End: Endpoint{
			IDs:      end.ids,
			Kinds:    patternMatch.endKinds,
			Criteria: end.criteria(),
		},
		EdgeKinds:   patternMatch.edgeKinds,
		Mode:        patternMatch.mode,
		ExcludeSelf: excludeSelf,
		Limit:       limit,
	}, true
}

// patternMatchResult is matchPattern's decoded, normalized view of the
// MATCH clause's single PatternPart: mode, the path variable's symbol, the
// two endpoint variables' symbols and kind labels (already reordered so
// start/end reflect the traversal direction rather than pattern-element
// order), and the relationship's kind labels.
type patternMatchResult struct {
	mode                   Mode
	pathSymbol             string
	startSymbol, endSymbol string
	startKinds, endKinds   graph.Kinds
	edgeKinds              graph.Kinds
}

// matchPattern recognizes match's single PatternPart as a shortestPath /
// allShortestPaths node-relationship-node pattern with an explicit
// direction and an acceptable variable-length range (see FromCypher's
// doc). It never panics on a nil or malformed match.
func matchPattern(match *cypher.Match) (patternMatchResult, bool) {
	if match == nil || match.Optional || len(match.Pattern) != 1 {
		return patternMatchResult{}, false
	}

	part := match.Pattern[0]
	if part == nil || part.Variable == nil || part.Variable.Symbol == "" {
		return patternMatchResult{}, false
	}

	mode, ok := matchMode(part)
	if !ok {
		return patternMatchResult{}, false
	}

	if len(part.PatternElements) != 3 {
		return patternMatchResult{}, false
	}

	firstNode, isNode := part.PatternElements[0].AsNodePattern()
	if !isNode || firstNode == nil || firstNode.Variable == nil || firstNode.Variable.Symbol == "" {
		return patternMatchResult{}, false
	}

	relationship, isRelationship := part.PatternElements[1].AsRelationshipPattern()
	if !isRelationship || relationship == nil {
		return patternMatchResult{}, false
	}

	secondNode, isNode := part.PatternElements[2].AsNodePattern()
	if !isNode || secondNode == nil || secondNode.Variable == nil || secondNode.Variable.Symbol == "" {
		return patternMatchResult{}, false
	}

	if !matchRange(relationship.Range) {
		return patternMatchResult{}, false
	}

	var startNode, endNode *cypher.NodePattern

	switch relationship.Direction {
	case graph.DirectionOutbound:
		// (start)-[...]->(end): pattern element order already matches
		// traversal direction.
		startNode, endNode = firstNode, secondNode
	case graph.DirectionInbound:
		// (end)<-[...]-(start): the arrow's tail is the second element, so
		// swap to normalize Start to the arrow's tail.
		startNode, endNode = secondNode, firstNode
	default:
		// DirectionBoth (an undirected "-...-" pattern, or one written with
		// arrowheads on both ends): not an explicit single direction.
		return patternMatchResult{}, false
	}

	return patternMatchResult{
		mode:        mode,
		pathSymbol:  part.Variable.Symbol,
		startSymbol: startNode.Variable.Symbol,
		endSymbol:   endNode.Variable.Symbol,
		startKinds:  startNode.Kinds,
		endKinds:    endNode.Kinds,
		edgeKinds:   relationship.Kinds,
	}, true
}

// matchMode reports the PathQuery Mode implied by part's shortestPath /
// allShortestPaths flags. Exactly one of the two must be set; a plain
// (non-shortest-path) pattern part, or one with both flags set, fails.
func matchMode(part *cypher.PatternPart) (Mode, bool) {
	switch {
	case part.ShortestPathPattern && !part.AllShortestPathsPattern:
		return ModeOne, true
	case part.AllShortestPathsPattern && !part.ShortestPathPattern:
		return ModeAll, true
	default:
		return 0, false
	}
}

// matchRange reports whether r is an acceptable variable-length range: no
// upper bound, and a lower bound that is either absent (*..) or exactly 1
// (*1..). A missing range (a fixed single-hop relationship with no `*` at
// all), an explicit lower bound of 0 (*0.. -- zero-length paths change
// ExcludeSelf/self-path semantics), any other explicit lower bound, or any
// upper bound (*1..3) are all rejected.
func matchRange(r *cypher.PatternRange) bool {
	if r == nil || r.EndIndex != nil {
		return false
	}

	return r.StartIndex == nil || *r.StartIndex == 1
}

// endpointAccumulator collects, per pattern endpoint variable, the facts
// matchWhere discovers while walking the WHERE conjunction: explicit ids
// (from id(<var>) = <literal>), kind labels added via a <var>:Kind
// predicate (on top of the node pattern's own kind labels), and lifted
// predicates -- deep-copied WHERE conjuncts scoped to this endpoint alone,
// with every reference to its parse-time variable symbol rewritten to the
// dawgs query-builder's node symbol (see liftConjunct). criteria()
// assembles the final Endpoint.Criteria from these, matching FromCypher's
// documented contract: nil unless WHERE actually constrains this endpoint
// beyond its plain kind labels.
type endpointAccumulator struct {
	symbol       string
	patternKinds graph.Kinds
	extraKinds   graph.Kinds
	lifted       []graph.Criteria
	ids          []graph.ID
}

// touched reports whether WHERE contributed anything -- a kind predicate or
// a lifted predicate -- targeting this endpoint.
func (e *endpointAccumulator) touched() bool {
	return len(e.extraKinds) > 0 || len(e.lifted) > 0
}

// criteria builds this endpoint's graph.Criteria, or nil if WHERE never
// touched it (an endpoint with only pattern kind labels gets Kinds
// populated and nil Criteria, per FromCypher's contract). Kinds come first,
// then lifted predicates in the source order matchWhere encountered them.
func (e *endpointAccumulator) criteria() graph.Criteria {
	if !e.touched() {
		return nil
	}

	kinds := append(append(graph.Kinds{}, e.patternKinds...), e.extraKinds...)

	parts := make([]graph.Criteria, 0, len(e.lifted)+1)
	if len(kinds) > 0 {
		parts = append(parts, query.KindIn(query.Node(), kinds...))
	}
	parts = append(parts, e.lifted...)

	return query.And(parts...)
}

// matchWhere walks where's conjunction (nil means "no WHERE clause", which
// is accepted trivially): it flattens the top-level AND structure into a
// flat list of conjuncts (flattenConjuncts), classifies each one
// (classifyConjunct), and aggregates whether an s<>t (ExcludeSelf)
// predicate over the two endpoint variables was present. Any conjunct that
// doesn't classify -- see FromCypher's doc for the five cases -- fails the
// whole match.
func matchWhere(where *cypher.Where, start, end *endpointAccumulator) (excludeSelf, ok bool) {
	if where == nil {
		return false, true
	}

	conjuncts, ok := flattenConjuncts(where.GetAll())
	if !ok {
		return false, false
	}

	for _, expr := range conjuncts {
		conjunctExcludeSelf, matched := classifyConjunct(expr, start, end)
		if !matched {
			return false, false
		}
		excludeSelf = excludeSelf || conjunctExcludeSelf
	}

	return excludeSelf, true
}

// flattenConjuncts expands exprs into a flat list of WHERE conjuncts,
// recursively unwrapping *cypher.Conjunction nodes -- bare, or wrapped in a
// *cypher.Parenthetical -- so a query written with nested "AND"s and/or
// parentheses around them produces the same flat conjunct list as one
// written without. Only AND-conjunctions unwrap this way: a Parenthetical
// wrapping anything else (a Disjunction, a Negation, a bare comparison, ...)
// is left exactly as parsed and becomes a single conjunct -- that's the
// scope endpoint-predicate lifting (classifyConjunct's case 4) treats as a
// unit, and keeping its Parenthetical intact preserves the source's
// grouping in whatever gets lifted, rather than risking an operator's
// precedence changing once it's embedded as one conjunct among several in
// Endpoint.Criteria's own query.And(...).
func flattenConjuncts(exprs []cypher.Expression) ([]cypher.Expression, bool) {
	flat := make([]cypher.Expression, 0, len(exprs))

	for _, expr := range exprs {
		switch typed := expr.(type) {
		case *cypher.Conjunction:
			if typed == nil {
				return nil, false
			}
			nested, ok := flattenConjuncts(typed.GetAll())
			if !ok {
				return nil, false
			}
			flat = append(flat, nested...)

		case *cypher.Parenthetical:
			if typed == nil || typed.Expression == nil {
				return nil, false
			}
			if _, isConjunction := typed.Expression.(*cypher.Conjunction); isConjunction {
				nested, ok := flattenConjuncts([]cypher.Expression{typed.Expression})
				if !ok {
					return nil, false
				}
				flat = append(flat, nested...)
			} else {
				flat = append(flat, expr)
			}

		default:
			flat = append(flat, expr)
		}
	}

	return flat, true
}

// classifyConjunct recognizes expr as one of the five WHERE conjunct shapes
// documented on FromCypher. It peels away any number of leading
// *cypher.Parenthetical layers (defensively -- "possibly nested/
// parenthesized", same as flattenConjuncts) purely to _look for_ the three
// special shapes (ExcludeSelf, id() equals, kind matcher), none of which
// produce a Criteria subtree, so discarding their parentheses is harmless.
// Anything that doesn't match one of those three falls through to
// liftConjunct with the *original*, still-possibly-parenthesized expr, so a
// lifted predicate's own parenthetical grouping (if the source had one) is
// preserved.
func classifyConjunct(expr cypher.Expression, start, end *endpointAccumulator) (excludeSelf, ok bool) {
	shape := expr
	for {
		paren, isParenthetical := shape.(*cypher.Parenthetical)
		if !isParenthetical || paren == nil || paren.Expression == nil {
			break
		}
		shape = paren.Expression
	}

	switch typed := shape.(type) {
	case *cypher.Comparison:
		if excl, matched := matchExcludeSelfComparison(typed, start, end); matched {
			return excl, true
		}
		if symbol, id, matched := matchIDEquals(typed); matched {
			switch symbol {
			case start.symbol:
				start.ids = append(start.ids, id)
				return false, true
			case end.symbol:
				end.ids = append(end.ids, id)
				return false, true
			default:
				// id(<var>) = <literal> for a variable that's neither
				// pattern endpoint (the path or relationship variable, or
				// an unknown symbol): not a shape liftConjunct would ever
				// accept either (it references exactly one variable, but
				// not a pattern endpoint), so reject outright rather than
				// re-deriving the same answer through the generic path.
				return false, false
			}
		}

	case *cypher.KindMatcher:
		if matchKindPredicate(typed, start, end) {
			return false, true
		}
	}

	return liftConjunct(expr, start, end)
}

// matchExcludeSelfComparison recognizes cmp as the s<>t ExcludeSelf shape: a
// single-partial "<>" comparison whose both sides are bare Variables naming
// exactly start and end's symbols (in either order).
func matchExcludeSelfComparison(cmp *cypher.Comparison, start, end *endpointAccumulator) (excludeSelf, matched bool) {
	if cmp == nil || len(cmp.Partials) != 1 {
		return false, false
	}

	partial := cmp.Partials[0]
	if partial == nil || partial.Operator != cypher.OperatorNotEquals {
		return false, false
	}

	leftVariable, isLeftVariable := cmp.Left.(*cypher.Variable)
	rightVariable, isRightVariable := partial.Right.(*cypher.Variable)
	if !isLeftVariable || !isRightVariable || leftVariable == nil || rightVariable == nil {
		return false, false
	}

	symbols := map[string]bool{leftVariable.Symbol: true, rightVariable.Symbol: true}
	if len(symbols) == 2 && symbols[start.symbol] && symbols[end.symbol] {
		return true, true
	}

	return false, false
}

// liftConjunct implements classifyConjunct's case 4/5: it determines
// whether expr's variable references are scoped to exactly one of the two
// pattern endpoints (scanExpression), and if so deep-copies expr
// (cypher.Copy, the dawgs model's own recursive copy support -- criteria
// outlive the parse, so the original AST node can never be reused
// directly), rewrites every reference to that endpoint's parse-time symbol
// to the dawgs query-builder's node symbol (query.NodeSymbol, what
// query.Node() returns), and appends the result to that endpoint's lifted
// predicates. Anything else -- both endpoints referenced, the path or
// relationship variable, an unknown variable, no variable at all, a
// $parameter anywhere in expr, or a node shape scanExpression doesn't
// recognize -- fails the whole match (case 5): FromCypher never guesses at
// a shape it can't fully account for.
func liftConjunct(expr cypher.Expression, start, end *endpointAccumulator) (excludeSelf, ok bool) {
	symbols := make(map[string]bool)
	if !scanExpression(expr, symbols) || len(symbols) != 1 {
		return false, false
	}

	var targetSymbol string
	for symbol := range symbols {
		targetSymbol = symbol
	}

	var target *endpointAccumulator
	switch targetSymbol {
	case start.symbol:
		target = start
	case end.symbol:
		target = end
	default:
		return false, false
	}

	copied := cypher.Copy[cypher.Expression](expr)
	rewriteVariableSymbol(copied, targetSymbol, query.NodeSymbol)
	target.lifted = append(target.lifted, copied)

	return false, true
}

// scanExpression recursively walks expr and every child Expression node it
// contains, recording every *cypher.Variable symbol reached into symbols.
// It reports ok=false -- meaning liftConjunct must reject the whole query
// rather than risk mis-scoping a predicate -- if it finds a *cypher.
// Parameter anywhere in the subtree (a $parameter's value isn't known at
// recognition time, so it can never be safely lifted) or a node type this
// walker doesn't specifically know how to see through. That conservative
// default only affects less-common constructs (list comprehensions,
// pattern predicates, map literals, ...); every shape FromCypher's brief
// calls out explicitly -- comparisons of every operator, NOT, AND/OR/XOR,
// parentheses, function calls (COALESCE, id(), ...), property lookups,
// arithmetic, and list literals -- is covered below.
func scanExpression(expr cypher.Expression, symbols map[string]bool) bool {
	switch typed := expr.(type) {
	case nil:
		return true

	case *cypher.Variable:
		if typed == nil {
			return true
		}
		symbols[typed.Symbol] = true
		return true

	case *cypher.Literal:
		return true

	case *cypher.Parameter:
		return false

	case *cypher.PropertyLookup:
		if typed == nil {
			return true
		}
		return scanExpression(typed.Atom, symbols)

	case *cypher.KindMatcher:
		if typed == nil {
			return true
		}
		return scanExpression(typed.Reference, symbols)

	case *cypher.Negation:
		if typed == nil {
			return true
		}
		return scanExpression(typed.Expression, symbols)

	case *cypher.Parenthetical:
		if typed == nil {
			return true
		}
		return scanExpression(typed.Expression, symbols)

	case *cypher.Comparison:
		if typed == nil {
			return true
		}
		if !scanExpression(typed.Left, symbols) {
			return false
		}
		for _, partial := range typed.Partials {
			if partial == nil || !scanExpression(partial.Right, symbols) {
				return false
			}
		}
		return true

	case *cypher.Conjunction:
		if typed == nil {
			return true
		}
		return scanExpressionList(typed.GetAll(), symbols)

	case *cypher.Disjunction:
		if typed == nil {
			return true
		}
		return scanExpressionList(typed.GetAll(), symbols)

	case *cypher.ExclusiveDisjunction:
		if typed == nil {
			return true
		}
		return scanExpressionList(typed.GetAll(), symbols)

	case *cypher.FunctionInvocation:
		if typed == nil {
			return true
		}
		return scanExpressionList(typed.Arguments, symbols)

	case *cypher.ArithmeticExpression:
		if typed == nil {
			return true
		}
		if !scanExpression(typed.Left, symbols) {
			return false
		}
		for _, partial := range typed.Partials {
			if partial == nil || !scanExpression(partial.Right, symbols) {
				return false
			}
		}
		return true

	case *cypher.UnaryAddOrSubtractExpression:
		if typed == nil {
			return true
		}
		return scanExpression(typed.Right, symbols)

	case *cypher.ListLiteral:
		if typed == nil {
			return true
		}
		return scanExpressionList(*typed, symbols)

	default:
		return false
	}
}

// scanExpressionList runs scanExpression over every element of exprs,
// failing (and short-circuiting) on the first one it doesn't recognize.
func scanExpressionList(exprs []cypher.Expression, symbols map[string]bool) bool {
	for _, expr := range exprs {
		if !scanExpression(expr, symbols) {
			return false
		}
	}
	return true
}

// rewriteVariableSymbol mutates every *cypher.Variable reachable from expr
// -- via the same node shapes scanExpression recognizes -- whose Symbol
// equals from, setting it to to. It's only ever called on a fresh
// cypher.Copy of a scanExpression-approved subtree (liftConjunct), so it
// never touches the original parsed AST, and every reachable Variable is
// already known (scanExpression already verified it) to carry exactly the
// symbol being rewritten.
func rewriteVariableSymbol(expr cypher.Expression, from, to string) {
	switch typed := expr.(type) {
	case *cypher.Variable:
		if typed != nil && typed.Symbol == from {
			typed.Symbol = to
		}

	case *cypher.PropertyLookup:
		if typed != nil {
			rewriteVariableSymbol(typed.Atom, from, to)
		}

	case *cypher.KindMatcher:
		if typed != nil {
			rewriteVariableSymbol(typed.Reference, from, to)
		}

	case *cypher.Negation:
		if typed != nil {
			rewriteVariableSymbol(typed.Expression, from, to)
		}

	case *cypher.Parenthetical:
		if typed != nil {
			rewriteVariableSymbol(typed.Expression, from, to)
		}

	case *cypher.Comparison:
		if typed == nil {
			return
		}
		rewriteVariableSymbol(typed.Left, from, to)
		for _, partial := range typed.Partials {
			if partial != nil {
				rewriteVariableSymbol(partial.Right, from, to)
			}
		}

	case *cypher.Conjunction:
		if typed != nil {
			for _, e := range typed.GetAll() {
				rewriteVariableSymbol(e, from, to)
			}
		}

	case *cypher.Disjunction:
		if typed != nil {
			for _, e := range typed.GetAll() {
				rewriteVariableSymbol(e, from, to)
			}
		}

	case *cypher.ExclusiveDisjunction:
		if typed != nil {
			for _, e := range typed.GetAll() {
				rewriteVariableSymbol(e, from, to)
			}
		}

	case *cypher.FunctionInvocation:
		if typed != nil {
			for _, arg := range typed.Arguments {
				rewriteVariableSymbol(arg, from, to)
			}
		}

	case *cypher.ArithmeticExpression:
		if typed == nil {
			return
		}
		rewriteVariableSymbol(typed.Left, from, to)
		for _, partial := range typed.Partials {
			if partial != nil {
				rewriteVariableSymbol(partial.Right, from, to)
			}
		}

	case *cypher.UnaryAddOrSubtractExpression:
		if typed != nil {
			rewriteVariableSymbol(typed.Right, from, to)
		}

	case *cypher.ListLiteral:
		if typed != nil {
			for _, e := range *typed {
				rewriteVariableSymbol(e, from, to)
			}
		}
	}
	// *cypher.Literal, *cypher.Parameter (scanExpression already rejects
	// any subtree containing one), nil, and any node type scanExpression
	// doesn't recognize (also already rejected): nothing to rewrite.
}

// matchKindPredicate recognizes km as a <var>:Kind predicate over one of
// start/end's symbols, appending its kinds to that endpoint's accumulator.
// A matcher over any other variable -- an unknown symbol, the path
// variable, or a non-Variable reference -- fails.
func matchKindPredicate(km *cypher.KindMatcher, start, end *endpointAccumulator) bool {
	if km == nil {
		return false
	}

	variable, isVariable := km.Reference.(*cypher.Variable)
	if !isVariable || variable == nil {
		return false
	}

	switch variable.Symbol {
	case start.symbol:
		start.extraKinds = append(start.extraKinds, km.Kinds...)
		return true
	case end.symbol:
		end.extraKinds = append(end.extraKinds, km.Kinds...)
		return true
	default:
		return false
	}
}

// matchReturn recognizes ret as projecting exactly the path variable
// (identified by pathSymbol), with no alias, DISTINCT, additional
// projection items, ORDER BY, or SKIP. An optional LIMIT's integer value is
// returned; absent a LIMIT clause, limit is 0.
func matchReturn(ret *cypher.Return, pathSymbol string) (limit int, ok bool) {
	if ret == nil || ret.Projection == nil {
		return 0, false
	}

	projection := ret.Projection
	if projection.Distinct || projection.All || projection.Order != nil || projection.Skip != nil {
		return 0, false
	}

	if len(projection.Items) != 1 {
		return 0, false
	}

	item, isItem := projection.Items[0].(*cypher.ProjectionItem)
	if !isItem || item == nil || item.Alias != nil {
		return 0, false
	}

	variable, isVariable := item.Expression.(*cypher.Variable)
	if !isVariable || variable == nil || variable.Symbol == "" || variable.Symbol != pathSymbol {
		return 0, false
	}

	if projection.Limit == nil {
		return 0, true
	}

	literal, isLiteral := projection.Limit.Value.(*cypher.Literal)
	if !isLiteral || literal == nil || literal.Null {
		return 0, false
	}

	switch value := literal.Value.(type) {
	case int64:
		return int(value), true
	case uint64:
		return int(value), true
	default:
		return 0, false
	}
}
