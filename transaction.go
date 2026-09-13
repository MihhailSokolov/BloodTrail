// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// wrappedTransaction wraps a live graph.Transaction so that queries running
// under it get a chance to be served from the in-memory path engine instead
// of PostgreSQL, wherever engine.TryCypher/TryAllShortestPaths recognizes
// and can serve the shape.
//
// graph.Transaction is embedded, so the seven methods this file does not
// override (CreateNode, UpdateNode, CreateRelationshipByIDs,
// UpdateRelationship, Raw, Commit, GraphQueryMemoryLimit) are promoted
// straight through to the inner transaction unchanged.
//
// The zero value is not useful; a wrappedTransaction is only ever
// constructed by Driver.ReadTransaction (driver.go), once per delegate
// invocation, so declined never leaks across a dawgs-driven retry of the
// same TransactionDelegate.
type wrappedTransaction struct {
	graph.Transaction

	engine *engine.Engine

	// declined is set by WithGraph: a transaction retargeted at a non-default
	// graph is outside what the engine's single-graph snapshot can answer
	// for, so the engine must not serve any further call on it -- Query
	// falls straight through to the inner transaction instead of trying
	// TryCypher, and recordingRelationshipQuery.FetchAllShortestPaths
	// (relationship_query.go) checks it too.
	declined bool
}

// WithGraph delegates to the inner transaction (so graph-scoping behavior is
// unchanged) and returns a fresh wrapper around the result with declined set
// -- once a transaction has been retargeted, the engine must not attempt to
// serve any query run under it.
//
// The receiver is declined too, not just the returned wrapper. dawgs' pg
// transaction retargets itself and returns the same object (it assigns its
// own targetSchema and returns the receiver), so after this call the
// PostgreSQL side of THIS wrapper answers for g as well -- and every query
// wrapper already handed out from it binds that same retargeted
// transaction. Leaving the receiver undeclined let a caller that retargets
// and keeps using its original handle get default-graph answers out of the
// snapshot for a transaction PostgreSQL had moved to another graph.
func (t *wrappedTransaction) WithGraph(g graph.Graph) graph.Transaction {
	inner := t.Transaction.WithGraph(g)
	t.declined = true
	return &wrappedTransaction{
		Transaction: inner,
		engine:      t.engine,
		declined:    true,
	}
}

// Relationships returns a recordingRelationshipQuery wrapping the inner
// transaction's own RelationshipQuery, so a subsequent FetchAllShortestPaths
// call has a chance to be served from the engine.
func (t *wrappedTransaction) Relationships() graph.RelationshipQuery {
	return &recordingRelationshipQuery{
		RelationshipQuery: t.Transaction.Relationships(),
		tx:                t,
	}
}

// Nodes returns a recordingNodeQuery wrapping the inner transaction's own
// NodeQuery, so a subsequent Count, FetchIDs, or FetchKinds call has a chance
// to be served from the engine.
func (t *wrappedTransaction) Nodes() graph.NodeQuery {
	return &recordingNodeQuery{
		NodeQuery: t.Transaction.Nodes(),
		tx:        t,
	}
}

// Query attempts to serve query/parameters from the engine via TryCypher,
// falling back to the inner transaction's own Query whenever the engine
// declines -- disabled, no snapshot, unrecognized text, bound parameters,
// or any other reason (see engine.TryCypher's decline reasons) -- or when
// this transaction has already been declined by a prior WithGraph call.
func (t *wrappedTransaction) Query(query string, parameters map[string]any) graph.Result {
	if !t.declined {
		if result, served := t.engine.TryCypher(context.Background(), t, query, parameters); served {
			return result
		}
	}
	return t.Transaction.Query(query, parameters)
}
