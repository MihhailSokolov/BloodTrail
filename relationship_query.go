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
// shortest-paths endpoint (recognize.FromCriteria's documented shape), a
// subsequent Count/FetchIDs/FetchTriples/FetchKinds call has a chance to
// recognize the structural shape BloodHound's builder queries construct
// against a relationship pattern (recognize.FromRelCriteria's documented
// shape), and a subsequent Query call has a chance to recognize both that
// shape and one of the three canonical RETURN projections dawgs' own
// container.FetchDirectedGraph and traversal.LightweightDriver issue
// (recognize.FromReturning's documented shape) -- serving any of these from
// the in-memory engine.
//
// graph.RelationshipQuery is embedded, so every method this file does not
// override (First, Fetch, FetchDirection) is promoted straight through to
// the inner query unchanged.
//
// The zero value is not useful; construct one via wrappedTransaction's own
// Relationships().
type recordingRelationshipQuery struct {
	graph.RelationshipQuery

	tx *wrappedTransaction

	// criteria accumulates every Filter/Filterf argument, in call order.
	// FetchAllShortestPaths, Count/FetchIDs/FetchTriples/FetchKinds, and
	// Query all only attempt to recognize and serve the query when exactly
	// one criteria was recorded -- recognize.FromCriteria's and
	// recognize.FromRelCriteria's accepted shape is always a single
	// conjunction built in one Filter (or Filterf) call, never several
	// composed together.
	criteria []graph.Criteria

	// tainted is set by Offset, Limit, Update, Delete, or an OrderBy call
	// recognize.OrderIsEdgeIDAscending does not recognize (see
	// orderByEdgeID's doc for the one OrderBy shape that does NOT taint):
	// any of these changes what FetchAllShortestPaths or Count/FetchIDs/
	// FetchTriples/FetchKinds would need to mean (an order/offset/limit the
	// engine's own path search or structural scan does not implement, or a
	// mutation that must go straight to PostgreSQL), so once set the engine
	// must not be consulted for this query at all.
	tainted bool

	// orderByEdgeID is set by OrderBy when its criteria recognizably
	// requests ascending order on the relationship's id
	// (recognize.OrderIsEdgeIDAscending) -- exactly the paging order dawgs'
	// own traversal.LightweightDriver (traversal/traversal.go's
	// shallowFetchRelationships) and ops/traversal.go's TraversalPlan apply
	// to every relationship query they issue before calling Query.
	//
	// Unlike an unrecognized OrderBy, this does NOT taint the query -- but
	// it is not simply ignored either: FetchAllShortestPaths still declines
	// once it's set (an ordered query is not the canonical shortest-paths
	// shape its own doc requires), and so do Count/FetchIDs/FetchTriples/
	// FetchKinds (an order has no meaning for any of those four answer
	// shapes, so serving one while silently dropping the order would be
	// wrong). Only Query passes it through, as the orderByEdgeID argument to
	// engine.TryRelQueryRows, which is what actually lets the two upstream
	// callers above -- both of which OrderBy before Query -- be served.
	orderByEdgeID bool
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

// OrderBy sets orderByEdgeID (see its doc) when criteria recognizably
// requests ascending order on the relationship's id
// (recognize.OrderIsEdgeIDAscending); otherwise it taints the query (see
// tainted's doc). Either way, OrderBy always delegates to the inner query
// and returns this same wrapper, exactly like Offset/Limit below.
func (r *recordingRelationshipQuery) OrderBy(criteria ...graph.Criteria) graph.RelationshipQuery {
	if recognize.OrderIsEdgeIDAscending(criteria) {
		r.orderByEdgeID = true
	} else {
		r.tainted = true
	}
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
// (wrappedTransaction.declined, set by WithGraph), orderByEdgeID has not
// been set (an ordered query is not the canonical path shape
// recognize.FromCriteria models -- see orderByEdgeID's own doc), and exactly
// one criteria was recorded: recognize.FromCriteria is run against it, and
// on a recognized shape, engine.TryAllShortestPaths is tried. On a served
// result, the returned graph.PathSet is wrapped in engine.NewPathCursor and
// handed to delegate exactly as a live database cursor would be -- any error
// delegate returns is propagated unchanged. Every other case (tainted,
// declined, ordered, criteria count != 1, unrecognized shape, or the engine
// itself declining) falls through to the inner query's own
// FetchAllShortestPaths.
//
// The cursor is closed by this method (not by delegate), matching the pg
// driver's own convention (drivers/pg/relationship.go's
// FetchAllShortestPaths: `cursor := ...; defer cursor.Close(); return
// delegate(cursor)`): a delegate that returns early without fully draining
// Chan() would otherwise leak the feeder goroutine, which blocks forever on
// an unbuffered send until something cancels its context -- only Close()
// does that.
func (r *recordingRelationshipQuery) FetchAllShortestPaths(delegate func(cursor graph.Cursor[graph.Path]) error) error {
	if !r.tainted && !r.tx.declined && !r.orderByEdgeID && len(r.criteria) == 1 {
		if pq, ok := recognize.FromCriteria(r.criteria[0]); ok {
			if paths, served := r.tx.engine.TryAllShortestPaths(context.Background(), r.tx, pq); served {
				cursor := engine.NewPathCursor(context.Background(), paths)
				defer cursor.Close()
				return delegate(cursor)
			}
		}
	}
	return r.RelationshipQuery.FetchAllShortestPaths(delegate)
}

// Count attempts to serve the query from the engine when nothing has
// tainted it, the owning transaction has not been declined
// (wrappedTransaction.declined, set by WithGraph), orderByEdgeID has not
// been set (an order has no meaning for a bare count -- see orderByEdgeID's
// own doc), and exactly one criteria was recorded: recognize.FromRelCriteria
// is run against it, and on a recognized shape, engine.TryRelCount is tried.
// Every other case (tainted, declined, ordered, criteria count != 1,
// unrecognized shape, or the engine itself declining) falls through to the
// inner query's own Count.
func (r *recordingRelationshipQuery) Count() (int64, error) {
	if !r.tainted && !r.tx.declined && !r.orderByEdgeID && len(r.criteria) == 1 {
		if spec, ok := recognize.FromRelCriteria(r.criteria[0]); ok {
			if n, served := r.tx.engine.TryRelCount(context.Background(), spec); served {
				return n, nil
			}
		}
	}
	return r.RelationshipQuery.Count()
}

// FetchIDs attempts to serve the query from the engine under the same
// conditions as Count (see its doc). On a served result, the returned
// graph.Cursor[graph.ID] is handed to delegate exactly as a live database
// cursor would be -- any error delegate returns is propagated unchanged.
//
// The cursor is closed by this method (not by delegate), matching
// FetchAllShortestPaths' identical convention above and
// recordingNodeQuery.FetchIDs' (node_query.go): a delegate that returns
// early without fully draining Chan() would otherwise leak the feeder
// goroutine, which blocks forever on an unbuffered send until something
// cancels its context -- only Close() does that.
func (r *recordingRelationshipQuery) FetchIDs(delegate func(cursor graph.Cursor[graph.ID]) error) error {
	if !r.tainted && !r.tx.declined && !r.orderByEdgeID && len(r.criteria) == 1 {
		if spec, ok := recognize.FromRelCriteria(r.criteria[0]); ok {
			if cursor, served := r.tx.engine.TryRelFetchIDs(context.Background(), spec); served {
				defer cursor.Close()
				return delegate(cursor)
			}
		}
	}
	return r.RelationshipQuery.FetchIDs(delegate)
}

// FetchTriples attempts to serve the query from the engine under the same
// conditions as Count (see its doc). See FetchIDs' doc for the cursor-close
// convention, applied identically here.
func (r *recordingRelationshipQuery) FetchTriples(delegate func(cursor graph.Cursor[graph.RelationshipTripleResult]) error) error {
	if !r.tainted && !r.tx.declined && !r.orderByEdgeID && len(r.criteria) == 1 {
		if spec, ok := recognize.FromRelCriteria(r.criteria[0]); ok {
			if cursor, served := r.tx.engine.TryRelFetchTriples(context.Background(), spec); served {
				defer cursor.Close()
				return delegate(cursor)
			}
		}
	}
	return r.RelationshipQuery.FetchTriples(delegate)
}

// FetchKinds attempts to serve the query from the engine under the same
// conditions as Count (see its doc). See FetchIDs' doc for the cursor-close
// convention, applied identically here.
func (r *recordingRelationshipQuery) FetchKinds(delegate func(cursor graph.Cursor[graph.RelationshipKindsResult]) error) error {
	if !r.tainted && !r.tx.declined && !r.orderByEdgeID && len(r.criteria) == 1 {
		if spec, ok := recognize.FromRelCriteria(r.criteria[0]); ok {
			if cursor, served := r.tx.engine.TryRelFetchKinds(context.Background(), spec); served {
				defer cursor.Close()
				return delegate(cursor)
			}
		}
	}
	return r.RelationshipQuery.FetchKinds(delegate)
}

// projectionDirectionConsistent reports whether proj's row direction is
// compatible with spec's own id() anchoring, as Query requires before ever
// consulting the engine: a ProjectionStepOutbound row's far/varying side is
// the end (recognize.RowProjection's own doc), which only makes sense when
// the start is the one held fixed, i.e. spec.StartIDs is anchored (non-nil);
// symmetrically, a ProjectionStepInbound row's far side is the start, which
// needs spec.EndIDs anchored. ProjectionStartEnd names no far side at all,
// so it is always consistent.
//
// This is a cheap pre-filter, not the authoritative check: engine.
// TryRelQueryRows makes the identical comparison itself
// (reasonProjectionMismatch) and would decline exactly the same queries
// this function would reject, so a caller that skipped this check would
// still get correct (if slightly more expensive) behavior via the engine's
// own decline-and-fall-through. Checking here first simply avoids
// constructing a context and making the call at all for a shape that could
// never succeed -- see reasonProjectionMismatch's own doc for why a
// genuinely upstream-shaped query should never actually hit either check.
func projectionDirectionConsistent(proj recognize.RowProjection, spec recognize.RelSpec) bool {
	switch proj {
	case recognize.ProjectionStepOutbound:
		return spec.StartIDs != nil
	case recognize.ProjectionStepInbound:
		return spec.EndIDs != nil
	default:
		return true
	}
}

// Query attempts to serve a structural row-projection query entirely from
// the engine when nothing has tainted it, the owning transaction has not
// been declined (wrappedTransaction.declined, set by WithGraph), and exactly
// one criteria was recorded and exactly one finalCriteria was given --
// unlike Count/FetchIDs/FetchTriples/FetchKinds above, Query's caller
// supplies its own RETURN-shaped criteria rather than this wrapper choosing
// one, and every real upstream caller (dawgs' own container.
// FetchDirectedGraph and traversal.shallowFetchRelationships, reached via
// traversal.LightweightDriver) passes exactly one. Unlike those four
// methods, orderByEdgeID does not disqualify Query from being served at all
// -- it is instead passed straight through to engine.TryRelQueryRows (see
// orderByEdgeID's own doc for why Query is the one method that needs to
// honor it rather than decline on it).
//
// finalCriteria[0] is recognized via recognize.FromReturning into one of the
// three canonical RowProjection shapes, r.criteria[0] via
// recognize.FromRelCriteria into a RelSpec exactly as Count/FetchIDs/
// FetchTriples/FetchKinds do, and the two must additionally agree on
// direction (projectionDirectionConsistent, above) before
// engine.TryRelQueryRows is even attempted. Every other case (tainted,
// declined, criteria or finalCriteria count != 1, either recognition
// failing, a direction mismatch, or the engine itself declining) falls
// through to the inner query's own Query, passed delegate and finalCriteria
// unchanged.
//
// On a served result, the returned graph.Result is closed by this method
// (not by delegate) before returning, following the same provider-closes
// convention as FetchAllShortestPaths/Count/FetchIDs/FetchTriples/
// FetchKinds above.
func (r *recordingRelationshipQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	if !r.tainted && !r.tx.declined && len(r.criteria) == 1 && len(finalCriteria) == 1 {
		if proj, ok := recognize.FromReturning(finalCriteria[0]); ok {
			if spec, ok := recognize.FromRelCriteria(r.criteria[0]); ok && projectionDirectionConsistent(proj, spec) {
				if result, served := r.tx.engine.TryRelQueryRows(context.Background(), spec, proj, r.orderByEdgeID); served {
					defer result.Close()
					return delegate(result)
				}
			}
		}
	}
	return r.RelationshipQuery.Query(delegate, finalCriteria...)
}
