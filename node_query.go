// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
)

// recordingNodeQuery wraps a live graph.NodeQuery, recording every criteria
// the caller filters by so a subsequent Count, FetchIDs, or FetchKinds call
// has a chance to recognize the query BloodHound's builder queries construct
// against the bare node variable (recognize.FromNodeCriteria's documented
// shape, e.g. FetchNodeIDsByKind's single KindMatcher, possibly combined with
// an id() filter) and serve it from the in-memory engine.
//
// graph.NodeQuery is embedded, so every method this file does not override
// (Fetch, First) is promoted straight through to the inner query unchanged.
//
// The zero value is not useful; construct one via wrappedTransaction's own
// Nodes().
type recordingNodeQuery struct {
	graph.NodeQuery

	tx *wrappedTransaction

	// criteria accumulates every Filter/Filterf argument, in call order.
	// Count/FetchIDs/FetchKinds only attempt to recognize and serve the query
	// when exactly one criteria was recorded -- recognize.FromNodeCriteria's
	// accepted shape is always a single conjunction built in one Filter (or
	// Filterf) call, never several composed together (mirrors
	// recordingRelationshipQuery.criteria; see relationship_query.go's doc).
	criteria []graph.Criteria

	// tainted is set by OrderBy, Offset, Limit, Update, Delete, or Query: any
	// of these changes what Count/FetchIDs/FetchKinds would need to mean --
	// an order/offset/limit the engine's own bitset-backed answer does not
	// implement, a mutation that must go straight to PostgreSQL, or a
	// caller-supplied projection (Query's delegate) the engine has no way to
	// build -- so once set the engine must not be consulted for any of the
	// three for the rest of this query's life (mirrors
	// recordingRelationshipQuery.tainted).
	tainted bool
}

// Filter records criteria and delegates to the inner query, returning this
// same wrapper so the fluent chain keeps flowing through recordingNodeQuery.
func (r *recordingNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	r.criteria = append(r.criteria, criteria)
	r.NodeQuery = r.NodeQuery.Filter(criteria)
	return r
}

// Filterf calls criteriaDelegate once to record its result, then passes
// criteriaDelegate itself through to the inner query's own Filterf -- rather
// than a closure fixed to the already-observed value -- so the inner query's
// own semantics for calling the provider are unaffected.
func (r *recordingNodeQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.NodeQuery {
	r.criteria = append(r.criteria, criteriaDelegate())
	r.NodeQuery = r.NodeQuery.Filterf(criteriaDelegate)
	return r
}

// OrderBy taints the query (see tainted's doc) and delegates.
func (r *recordingNodeQuery) OrderBy(criteria ...graph.Criteria) graph.NodeQuery {
	r.tainted = true
	r.NodeQuery = r.NodeQuery.OrderBy(criteria...)
	return r
}

// Offset taints the query (see tainted's doc) and delegates.
func (r *recordingNodeQuery) Offset(skip int) graph.NodeQuery {
	r.tainted = true
	r.NodeQuery = r.NodeQuery.Offset(skip)
	return r
}

// Limit taints the query (see tainted's doc) and delegates.
func (r *recordingNodeQuery) Limit(limit int) graph.NodeQuery {
	r.tainted = true
	r.NodeQuery = r.NodeQuery.Limit(limit)
	return r
}

// Update taints the query (see tainted's doc) and delegates.
func (r *recordingNodeQuery) Update(properties *graph.Properties) error {
	r.tainted = true
	return r.NodeQuery.Update(properties)
}

// Delete taints the query (see tainted's doc) and delegates.
func (r *recordingNodeQuery) Delete() error {
	r.tainted = true
	return r.NodeQuery.Delete()
}

// Query taints the query (see tainted's doc) and delegates. Unlike Count/
// FetchIDs/FetchKinds, Query hands the caller a raw graph.Result to project
// however it likes (optionally narrowed further by finalCriteria) -- a shape
// the engine has no fixed answer type for, so it is never a candidate for
// engine service and is tainted unconditionally rather than merely being
// left unintercepted, so that a later Count/FetchIDs/FetchKinds call on the
// same chained query also stops consulting the engine.
func (r *recordingNodeQuery) Query(delegate func(results graph.Result) error, finalCriteria ...graph.Criteria) error {
	r.tainted = true
	return r.NodeQuery.Query(delegate, finalCriteria...)
}

// Count attempts to serve the query from the engine when nothing has tainted
// it, the owning transaction has not been declined
// (wrappedTransaction.declined, set by WithGraph), and exactly one criteria
// was recorded: recognize.FromNodeCriteria is run against it, and on a
// recognized shape, engine.TryNodeCount is tried. Every other case (tainted,
// declined, criteria count != 1, unrecognized shape, or the engine itself
// declining) falls through to the inner query's own Count.
func (r *recordingNodeQuery) Count() (int64, error) {
	if !r.tainted && !r.tx.declined && len(r.criteria) == 1 {
		if spec, ok := recognize.FromNodeCriteria(r.criteria[0]); ok {
			if n, served := r.tx.engine.TryNodeCount(context.Background(), spec); served {
				return n, nil
			}
		}
	}
	return r.NodeQuery.Count()
}

// FetchIDs attempts to serve the query from the engine under the same
// conditions as Count (see its doc). On a served result, the returned
// graph.Cursor[graph.ID] is handed to delegate exactly as a live database
// cursor would be -- any error delegate returns is propagated unchanged.
//
// The cursor is closed by this method (not by delegate), matching the pg
// driver's own convention and recordingRelationshipQuery.
// FetchAllShortestPaths' identical convention (relationship_query.go): a
// delegate that returns early without fully draining Chan() would otherwise
// leak the feeder goroutine, which blocks forever on an unbuffered send
// until something cancels its context -- only Close() does that.
func (r *recordingNodeQuery) FetchIDs(delegate func(cursor graph.Cursor[graph.ID]) error) error {
	if !r.tainted && !r.tx.declined && len(r.criteria) == 1 {
		if spec, ok := recognize.FromNodeCriteria(r.criteria[0]); ok {
			if cursor, served := r.tx.engine.TryNodeFetchIDs(context.Background(), spec); served {
				defer cursor.Close()
				return delegate(cursor)
			}
		}
	}
	return r.NodeQuery.FetchIDs(delegate)
}

// FetchKinds attempts to serve the query from the engine under the same
// conditions as Count (see its doc). See FetchIDs' doc for the cursor-close
// convention, applied identically here.
func (r *recordingNodeQuery) FetchKinds(delegate func(cursor graph.Cursor[graph.KindsResult]) error) error {
	if !r.tainted && !r.tx.declined && len(r.criteria) == 1 {
		if spec, ok := recognize.FromNodeCriteria(r.criteria[0]); ok {
			if cursor, served := r.tx.engine.TryNodeFetchKinds(context.Background(), spec); served {
				defer cursor.Close()
				return delegate(cursor)
			}
		}
	}
	return r.NodeQuery.FetchKinds(delegate)
}
