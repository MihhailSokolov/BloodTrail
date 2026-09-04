// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
)

// FromCriteria recognizes the graph.Criteria tree BloodHound's API builds
// for FetchAllShortestPaths (BloodHound v9.6.0, cmd/api/src/queries/graph.go,
// getAllShortestPathsInternal + parseRelationshipKindsParam):
//
//	query.And(
//	    query.Equals(query.StartID(), startNode.ID),
//	    query.Equals(query.EndID(), endNode.ID),
//	    query.KindIn(query.Relationship(), edgeKinds...), // optional
//	)
//
// The three conjuncts may appear in any order (query.And makes no ordering
// guarantee), and the KindIn conjunct may be omitted entirely -- in that
// case EdgeKinds comes back empty, meaning "all kinds". Any other shape --
// an extra or missing conjunct, a duplicate role, a KindMatcher over
// anything but the relationship variable, or criteria that isn't a
// *cypher.Conjunction at all -- reports ok=false. FromCriteria never
// panics: every AST level is nil-checked before use.
//
// On success, Mode is always ModeAll, Limit is always 0, ExcludeSelf is
// always false, and both endpoints carry their id as a single-element IDs
// slice -- matching what getAllShortestPathsInternal always requests.
func FromCriteria(criteria graph.Criteria) (PathQuery, bool) {
	conjunction, isConjunction := criteria.(*cypher.Conjunction)
	if !isConjunction || conjunction == nil {
		return PathQuery{}, false
	}

	var (
		startID, endID     graph.ID
		haveStart, haveEnd bool
		edgeKinds          graph.Kinds
		haveKinds          bool
	)

	for _, expr := range conjunction.Expressions {
		switch typed := expr.(type) {
		case *cypher.Comparison:
			symbol, id, matched := matchIDEquals(typed)
			if !matched {
				return PathQuery{}, false
			}

			switch symbol {
			case startSymbol:
				if haveStart {
					return PathQuery{}, false
				}
				startID, haveStart = id, true
			case endSymbol:
				if haveEnd {
					return PathQuery{}, false
				}
				endID, haveEnd = id, true
			default:
				// id(n) = ..., id(r) = ..., or anything else: not part of
				// upstream's shape.
				return PathQuery{}, false
			}

		case *cypher.KindMatcher:
			kinds, matched := matchRelationshipKinds(typed)
			if !matched {
				return PathQuery{}, false
			}
			if haveKinds {
				return PathQuery{}, false
			}
			edgeKinds, haveKinds = kinds, true

		default:
			return PathQuery{}, false
		}
	}

	if !haveStart || !haveEnd {
		return PathQuery{}, false
	}

	return PathQuery{
		Start:     Endpoint{IDs: []graph.ID{startID}},
		End:       Endpoint{IDs: []graph.ID{endID}},
		EdgeKinds: edgeKinds,
		Mode:      ModeAll,
	}, true
}
