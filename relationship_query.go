// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
)

// recordingRelationshipQuery wraps a live graph.RelationshipQuery, recording
// every criteria the caller filters by so a subsequent FetchAllShortestPaths
// call has a chance to recognize the query BloodHound's API builds for its
// shortest-paths endpoint (recognize.FromCriteria's documented shape) and
// serve it from the in-memory engine.
//
// graph.RelationshipQuery is embedded, so every method this file does not
// override (Count, First, Query, Fetch, FetchDirection, FetchIDs,
// FetchTriples, FetchKinds) is promoted straight through to the inner query
// unchanged.
//
// The zero value is not useful; construct one via wrappedTransaction's own
// Relationships().
type recordingRelationshipQuery struct {
	graph.RelationshipQuery

	tx *wrappedTransaction

	// criteria accumulates every Filter/Filterf argument, in call order.
	// FetchAllShortestPaths only attempts to recognize and serve the query
	// when exactly one criteria was recorded -- recognize.FromCriteria's
	// accepted shape is always a single conjunction built in one Filter (or
	// Filterf) call, never several composed together.
	criteria []graph.Criteria

	// tainted is set by OrderBy, Offset, Limit, Update or Delete: any of
	// these changes what FetchAllShortestPaths would need to mean (an
	// order/offset/limit the engine's own path search does not implement,
	// or a mutation that must go straight to PostgreSQL), so once set the
	// engine must not be consulted for this query at all.
	tainted bool
}

// Filter records criteria and delegates to the inner query, returning this
// same wrapper so the fluent chain keeps flowing through
// recordingRelationshipQuery.
func (r *recordingRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	r.criteria = append(r.criteria, criteria)
	r.RelationshipQuery = r.RelationshipQuery.Filter(criteria)
	return r
}

// Filterf calls criteriaDelegate once to record its result, then passes
// criteriaDelegate itself through to the inner query's own Filterf --
// rather than a closure fixed to the already-observed value -- so the inner
// query's own semantics for calling the provider are unaffected.
func (r *recordingRelationshipQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.RelationshipQuery {
	r.criteria = append(r.criteria, criteriaDelegate())
	r.RelationshipQuery = r.RelationshipQuery.Filterf(criteriaDelegate)
	return r
}

// OrderBy taints the query (see tainted's doc) and delegates.
func (r *recordingRelationshipQuery) OrderBy(criteria ...graph.Criteria) graph.RelationshipQuery {
	r.tainted = true
	r.RelationshipQuery = r.RelationshipQuery.OrderBy(criteria...)
	return r
}

// Offset taints the query (see tainted's doc) and delegates.
func (r *recordingRelationshipQuery) Offset(skip int) graph.RelationshipQuery {
	r.tainted = true
	r.RelationshipQuery = r.RelationshipQuery.Offset(skip)
	return r
}

// Limit taints the query (see tainted's doc) and delegates.
func (r *recordingRelationshipQuery) Limit(limit int) graph.RelationshipQuery {
	r.tainted = true
	r.RelationshipQuery = r.RelationshipQuery.Limit(limit)
	return r
}

// Update taints the query (see tainted's doc) and delegates.
func (r *recordingRelationshipQuery) Update(properties *graph.Properties) error {
	r.tainted = true
	return r.RelationshipQuery.Update(properties)
}

// Delete taints the query (see tainted's doc) and delegates.
func (r *recordingRelationshipQuery) Delete() error {
	r.tainted = true
	return r.RelationshipQuery.Delete()
}

// FetchAllShortestPaths attempts to serve the query from the engine when
// nothing has tainted it, the owning transaction has not been declined
// (wrappedTransaction.declined, set by WithGraph), and exactly one criteria
// was recorded: recognize.FromCriteria is run against it, and on a
// recognized shape, engine.TryAllShortestPaths is tried. On a served result,
// the returned graph.PathSet is wrapped in engine.NewPathCursor and handed
// to delegate exactly as a live database cursor would be -- any error
// delegate returns is propagated unchanged. Every other case (tainted,
// declined, criteria count != 1, unrecognized shape, or the engine itself
// declining) falls through to the inner query's own FetchAllShortestPaths.
func (r *recordingRelationshipQuery) FetchAllShortestPaths(delegate func(cursor graph.Cursor[graph.Path]) error) error {
	if !r.tainted && !r.tx.declined && len(r.criteria) == 1 {
		if pq, ok := recognize.FromCriteria(r.criteria[0]); ok {
			if paths, served := r.tx.engine.TryAllShortestPaths(context.Background(), r.tx, pq); served {
				return delegate(engine.NewPathCursor(context.Background(), paths))
			}
		}
	}
	return r.RelationshipQuery.FetchAllShortestPaths(delegate)
}
