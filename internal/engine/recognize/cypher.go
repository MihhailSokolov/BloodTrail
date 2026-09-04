// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"fmt"
	"strings"

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
// parentheses) of: <var>.prop = <literal>, <var>.prop ENDS WITH <string>,
// id(<var>) = <literal>, <var> <> <var> naming the two pattern variables
// (ExcludeSelf), and <var>:Kind. RETURN must project exactly the path
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
// predicate (on top of the node pattern's own kind labels), and property
// comparisons (already built as graph.Criteria via the dawgs query
// package). criteria() assembles the final Endpoint.Criteria from these,
// matching FromCypher's documented contract: nil unless WHERE actually
// constrains this endpoint beyond its plain kind labels.
type endpointAccumulator struct {
	symbol       string
	patternKinds graph.Kinds
	extraKinds   graph.Kinds
	properties   []graph.Criteria
	ids          []graph.ID
}

// touched reports whether WHERE contributed anything -- a kind predicate or
// a property comparison -- targeting this endpoint.
func (e *endpointAccumulator) touched() bool {
	return len(e.extraKinds) > 0 || len(e.properties) > 0
}

// criteria builds this endpoint's graph.Criteria, or nil if WHERE never
// touched it (an endpoint with only pattern kind labels gets Kinds
// populated and nil Criteria, per FromCypher's contract).
func (e *endpointAccumulator) criteria() graph.Criteria {
	if !e.touched() {
		return nil
	}

	kinds := append(append(graph.Kinds{}, e.patternKinds...), e.extraKinds...)

	parts := make([]graph.Criteria, 0, len(e.properties)+1)
	if len(kinds) > 0 {
		parts = append(parts, query.KindIn(query.Node(), kinds...))
	}
	parts = append(parts, e.properties...)

	return query.And(parts...)
}

// matchWhere walks where's conjunction (nil means "no WHERE clause", which
// is accepted trivially) recognizing exactly the predicate shapes described
// in FromCypher's doc, feeding endpoint-specific facts into start/end and
// reporting whether an s<>t (ExcludeSelf) predicate over the two endpoint
// variables was present. Any predicate that doesn't match one of the
// accepted shapes, or that names a variable other than start/end's symbols,
// fails the whole match.
func matchWhere(where *cypher.Where, start, end *endpointAccumulator) (excludeSelf, ok bool) {
	if where == nil {
		return false, true
	}

	return matchConjuncts(where.GetAll(), start, end)
}

// matchConjuncts recognizes each expression in exprs as one accepted WHERE
// predicate shape (recursing into nested Parenthetical/Conjunction nodes to
// flatten "possibly nested/parenthesized" conjunctions), aggregating
// ExcludeSelf across all of them. It fails on the first expression that
// doesn't match an accepted shape.
func matchConjuncts(exprs []cypher.Expression, start, end *endpointAccumulator) (excludeSelf, ok bool) {
	for _, expr := range exprs {
		switch typed := expr.(type) {
		case *cypher.Conjunction:
			if typed == nil {
				return false, false
			}
			nestedExcludeSelf, matched := matchConjuncts(typed.GetAll(), start, end)
			if !matched {
				return false, false
			}
			excludeSelf = excludeSelf || nestedExcludeSelf

		case *cypher.Parenthetical:
			if typed == nil || typed.Expression == nil {
				return false, false
			}
			nestedExcludeSelf, matched := matchConjuncts([]cypher.Expression{typed.Expression}, start, end)
			if !matched {
				return false, false
			}
			excludeSelf = excludeSelf || nestedExcludeSelf

		case *cypher.KindMatcher:
			if !matchKindPredicate(typed, start, end) {
				return false, false
			}

		case *cypher.Comparison:
			nestedExcludeSelf, matched := matchComparisonPredicate(typed, start, end)
			if !matched {
				return false, false
			}
			excludeSelf = excludeSelf || nestedExcludeSelf

		default:
			// Negation (NOT), Disjunction (OR), ExclusiveDisjunction (XOR),
			// and anything else outside the accepted shape.
			return false, false
		}
	}

	return excludeSelf, true
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

// matchComparisonPredicate recognizes cmp as one of the three accepted
// comparison shapes -- id(<var>) = <literal>, <var> <> <var> naming the two
// endpoints (reported back as excludeSelf), or <var>.prop {=|ENDS WITH}
// <literal> -- feeding the result into start/end. Any other comparison
// shape, operator, or variable reference fails.
func matchComparisonPredicate(cmp *cypher.Comparison, start, end *endpointAccumulator) (excludeSelf, ok bool) {
	if symbol, id, matched := matchIDEquals(cmp); matched {
		switch symbol {
		case start.symbol:
			start.ids = append(start.ids, id)
			return false, true
		case end.symbol:
			end.ids = append(end.ids, id)
			return false, true
		default:
			return false, false
		}
	}

	if cmp == nil || len(cmp.Partials) != 1 {
		return false, false
	}

	partial := cmp.Partials[0]
	if partial == nil {
		return false, false
	}

	if partial.Operator == cypher.OperatorNotEquals {
		return matchExcludeSelf(cmp.Left, partial.Right, start, end)
	}

	if partial.Operator != cypher.OperatorEquals && partial.Operator != cypher.OperatorEndsWith {
		return false, false
	}

	lookup, isLookup := cmp.Left.(*cypher.PropertyLookup)
	if !isLookup || lookup == nil {
		return false, false
	}

	variable, isVariable := lookup.Atom.(*cypher.Variable)
	if !isVariable || variable == nil {
		return false, false
	}

	literal, isLiteral := partial.Right.(*cypher.Literal)
	if !isLiteral || literal == nil {
		return false, false
	}

	value, decoded := decodeLiteralValue(literal)
	if !decoded {
		return false, false
	}

	var criterion graph.Criteria

	if partial.Operator == cypher.OperatorEquals {
		criterion = query.Equals(query.NodeProperty(lookup.Symbol), value)
	} else {
		stringValue, isString := value.(string)
		if !isString {
			return false, false
		}
		criterion = query.StringEndsWith(query.NodeProperty(lookup.Symbol), stringValue)
	}

	switch variable.Symbol {
	case start.symbol:
		start.properties = append(start.properties, criterion)
		return false, true
	case end.symbol:
		end.properties = append(end.properties, criterion)
		return false, true
	default:
		return false, false
	}
}

// matchExcludeSelf recognizes left <> right as the ExcludeSelf predicate:
// both sides bare variables, naming exactly start and end's symbols (in
// either order).
func matchExcludeSelf(left, right cypher.Expression, start, end *endpointAccumulator) (excludeSelf, ok bool) {
	leftVariable, isLeftVariable := left.(*cypher.Variable)
	rightVariable, isRightVariable := right.(*cypher.Variable)
	if !isLeftVariable || !isRightVariable || leftVariable == nil || rightVariable == nil {
		return false, false
	}

	symbols := map[string]bool{leftVariable.Symbol: true, rightVariable.Symbol: true}
	if len(symbols) == 2 && symbols[start.symbol] && symbols[end.symbol] {
		return true, true
	}

	return false, false
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

// decodeLiteralValue unwraps lit into the plain Go value it represents: the
// parser already decodes integer, double, and boolean literals into
// int64/uint64, float64, and bool, but a string literal's Value is left in
// Cypher source form (quotes and escapes intact, per *cypher.Literal's
// documented contract) and needs decodeCypherString to recover the actual
// string. A null literal, or a literal wrapping any other Go type (a list
// or map literal, notably), fails.
func decodeLiteralValue(lit *cypher.Literal) (any, bool) {
	if lit == nil || lit.Null {
		return nil, false
	}

	switch value := lit.Value.(type) {
	case string:
		decoded, err := decodeCypherString(value)
		if err != nil {
			return nil, false
		}
		return decoded, true
	case int64, uint64, float64, bool:
		return value, true
	default:
		return nil, false
	}
}

// decodeCypherString decodes raw -- a string literal's source-form token as
// produced by the dawgs Cypher parser (surrounding ' or " quotes intact,
// escape sequences un-decoded) -- into the string it represents. Recognizes
// the same escapes the Cypher grammar defines: \\, \', \", \b, \f, \n, \r,
// \t.
func decodeCypherString(raw string) (string, error) {
	if len(raw) < 2 {
		return "", fmt.Errorf("invalid cypher string literal: %q", raw)
	}

	quote := raw[0]
	if (quote != '\'' && quote != '"') || raw[len(raw)-1] != quote {
		return "", fmt.Errorf("invalid cypher string literal: missing or mismatched surrounding quotes: %q", raw)
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
			return "", fmt.Errorf("dangling escape in string literal")
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
			return "", fmt.Errorf("invalid escape \\%c", c)
		}
	}

	return b.String(), nil
}
