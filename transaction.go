// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
)

// servingEngine is the slice of *engine.Engine the read wrappers consult:
// every Try* serving entry point, and nothing that changes engine state. It
// exists so a unit test can observe what the wrappers hand the engine (the
// caller's context, above all) without a live snapshot; production always
// stores the Driver's own *engine.Engine here.
type servingEngine interface {
	TryCypher(ctx context.Context, tx graph.Transaction, text string, params map[string]any) (graph.Result, bool)
	TryAllShortestPaths(ctx context.Context, tx graph.Transaction, pq recognize.PathQuery) (graph.PathSet, bool)
	TryNodeCount(ctx context.Context, spec recognize.NodeSpec) (int64, bool)
	TryNodeFetchIDs(ctx context.Context, spec recognize.NodeSpec) (graph.Cursor[graph.ID], bool)
	TryNodeFetchKinds(ctx context.Context, spec recognize.NodeSpec) (graph.Cursor[graph.KindsResult], bool)
	TryRelCount(ctx context.Context, spec recognize.RelSpec) (int64, bool)
	TryRelFetchIDs(ctx context.Context, spec recognize.RelSpec) (graph.Cursor[graph.ID], bool)
	TryRelFetchTriples(ctx context.Context, spec recognize.RelSpec) (graph.Cursor[graph.RelationshipTripleResult], bool)
	TryRelFetchKinds(ctx context.Context, spec recognize.RelSpec) (graph.Cursor[graph.RelationshipKindsResult], bool)
	TryRelQueryRows(ctx context.Context, spec recognize.RelSpec, proj recognize.RowProjection, orderByEdgeID bool) (graph.Result, bool)
}

var _ servingEngine = (*engine.Engine)(nil)

// wrappedTransaction wraps a live graph.Transaction so that queries running
// under it get a chance to be served from the in-memory path engine instead
// of PostgreSQL, wherever engine.TryCypher/TryAllShortestPaths recognizes
// and can serve the shape.
//
// graph.Transaction is embedded, so the two methods this file does not
// override (Commit, GraphQueryMemoryLimit) are promoted straight through to
// the inner transaction unchanged. The four write methods and Raw are
// overridden only to note the write (readWrites' doc) before delegating.
//
// The zero value is not useful; a wrappedTransaction is only ever
// constructed by Driver.ReadTransaction (driver.go), once per delegate
// invocation, so declined never leaks across a dawgs-driven retry of the
// same TransactionDelegate.
type wrappedTransaction struct {
	graph.Transaction

	engine servingEngine

	// ctx is the context Driver.ReadTransaction was called with, handed to
	// every engine serving call made under this transaction. A served read
	// is not purely in-memory -- hydration reads rows back from PostgreSQL
	// through the engine's own pool -- so serving it under
	// context.Background() ignored the caller's cancellation and deadline,
	// and held a second pooled connection with no bound on it while this
	// transaction already holds one (at MaxConns, a pool that can then wedge
	// on itself). nil only for a wrapper a unit test builds directly;
	// readContext turns that into context.Background().
	ctx context.Context

	// declined is set by WithGraph: a transaction retargeted at a non-default
	// graph is outside what the engine's single-graph snapshot can answer
	// for, so the engine must not serve any further call on it -- Query
	// falls straight through to the inner transaction instead of trying
	// TryCypher, and recordingRelationshipQuery.FetchAllShortestPaths
	// (relationship_query.go) checks it too. Read it through serveable,
	// never directly: a write through this transaction declines it as well.
	declined bool

	// writes is shared by every wrapper one Driver.ReadTransaction call hands
	// out, WithGraph's included -- see readWrites' doc. nil only for a
	// wrapper a unit test builds directly.
	writes *readWrites
}

// readWrites records the writes made through one Driver.ReadTransaction
// call.
//
// dawgs' pg ReadTransaction is not read-only: it opens no transaction at all
// (drivers/pg/manager.go's SchemaManager.ReadTransaction hands the delegate
// a bare pooled connection -- no BEGIN, let alone BEGIN READ ONLY), so a
// write made through it autocommits statement by statement. Rejecting such
// writes would change behavior upstream BloodHound may depend on (nothing
// proves it never writes inside a ReadTransaction), so they are let through
// and accounted for the way a write made anywhere else is: the first one
// bumps the watermark eagerly (ensureBumped), every one records a ChangeSet
// fallback -- none of these calls tells this package which keys it touched
// -- and Driver.ReadTransaction Applies the scope once the delegate
// returns, whatever it returned, because each write was durable the moment
// its statement ran. Until then the replica cannot reflect them, so a
// write also stops every later engine serve on the same call
// (wrappedTransaction.serveable).
//
// Without this, such a write reached PostgreSQL with no watermark bump and
// no Apply: the replica kept serving the pre-write graph indefinitely, and a
// shutdown snapshot was stamped as matching PostgreSQL's counter.
type readWrites struct {
	eng *engine.Engine
	ctx context.Context

	// scope is nil until the first write.
	scope *engine.WriteScope
}

// note records one write. It runs before the write is delegated: the
// watermark protocol bumps ahead of a write's effect, never after it.
func (w *readWrites) note(reason string) {
	if w == nil {
		return
	}
	if w.scope == nil {
		w.scope = engine.NewWriteScope()
	}
	w.scope.Changes().RecordFallback(reason)
	ensureBumped(w.ctx, w.eng, w.scope)
}

// wrote reports whether any write has been noted.
func (w *readWrites) wrote() bool {
	return w != nil && w.scope != nil
}

// settle Applies the noted writes, if there were any. Driver.ReadTransaction
// calls it once, after its delegate returns.
func (w *readWrites) settle() {
	if !w.wrote() || w.eng == nil {
		return
	}
	w.eng.Apply(applyContext(w.ctx), w.scope)
}

// serveable reports whether the engine may be consulted for a read on this
// transaction: not once WithGraph has retargeted it (declined's doc), and
// not once anything has written through it (readWrites' doc).
func (t *wrappedTransaction) serveable() bool {
	return !t.declined && !t.writes.wrote()
}

// readContext is the context an engine serving call runs under -- see ctx's
// doc.
func (t *wrappedTransaction) readContext() context.Context {
	return applyContext(t.ctx)
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
		ctx:         t.ctx,
		declined:    true,
		writes:      t.writes,
	}
}

// CreateNode notes the write (readWrites' doc) and delegates.
func (t *wrappedTransaction) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	t.writes.note("ReadTransaction: CreateNode escapes changelog tracking")
	return t.Transaction.CreateNode(properties, kinds...)
}

// UpdateNode notes the write (readWrites' doc) and delegates.
func (t *wrappedTransaction) UpdateNode(node *graph.Node) error {
	t.writes.note("ReadTransaction: UpdateNode escapes changelog tracking")
	return t.Transaction.UpdateNode(node)
}

// CreateRelationshipByIDs notes the write (readWrites' doc) and delegates.
func (t *wrappedTransaction) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	t.writes.note("ReadTransaction: CreateRelationshipByIDs escapes changelog tracking")
	return t.Transaction.CreateRelationshipByIDs(startNodeID, endNodeID, kind, properties)
}

// UpdateRelationship notes the write (readWrites' doc) and delegates.
func (t *wrappedTransaction) UpdateRelationship(relationship *graph.Relationship) error {
	t.writes.note("ReadTransaction: UpdateRelationship escapes changelog tracking")
	return t.Transaction.UpdateRelationship(relationship)
}

// Raw notes a write unconditionally and delegates: its query is
// driver-specific SQL this package cannot classify (observingTransaction.Raw
// makes the same call on the write side). Nothing upstream runs a graph Raw
// query through the live graph's ReadTransaction today (its only graph Raw
// reads target Neo4j, in the migration tooling), so this costs no rebuilds
// in practice.
func (t *wrappedTransaction) Raw(query string, parameters map[string]any) graph.Result {
	t.writes.note("ReadTransaction: Raw escapes changelog tracking")
	return t.Transaction.Raw(query, parameters)
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
// this transaction is no longer serveable (a prior WithGraph, or a write).
//
// Text the engine did not serve is sniffed for an updating clause
// (readQueryMutates) before it goes to PostgreSQL, and noted as a write when
// it has one (readWrites' doc). The sniff runs only on that path: the engine
// never plans an updating clause, so a served query is never a write, and a
// served read does not pay for a second parse.
func (t *wrappedTransaction) Query(query string, parameters map[string]any) graph.Result {
	if t.serveable() {
		if result, served := t.engine.TryCypher(t.readContext(), t, query, parameters); served {
			return result
		}
	}
	if readQueryMutates(query) {
		t.writes.note("ReadTransaction: mutating Cypher escapes changelog tracking")
	}
	return t.Transaction.Query(query, parameters)
}

// readQueryMutates reports whether Cypher text run through a ReadTransaction
// can write. It parses exactly as the pg driver does before executing
// anything (drivers/pg/compiler.go in the pinned dawgs v0.8.0:
// frontend.ParseCypher with a filterless frontend.NewContext()), so the
// answer is exact: text that fails that parse never reaches PostgreSQL, and
// is no write, and text that passes it writes exactly when it carries an
// updating clause. A parser panic still reports true, the conservative
// answer.
//
// cypherMutates is deliberately not reused here. It parses under
// frontend.DefaultCypherContext, whose filters reject every query with a
// $parameter or a procedure CALL as well as every updating clause -- so it
// reports each of those as mutating, which on this path would turn every
// parameterized read into a replica rebuild.
func readQueryMutates(text string) (mutates bool) {
	defer func() {
		if recover() != nil {
			mutates = true
		}
	}()

	regularQuery, err := frontend.ParseCypher(frontend.NewContext(), text)
	if err != nil || regularQuery == nil || regularQuery.SingleQuery == nil {
		return false
	}
	return singleQueryMutates(regularQuery.SingleQuery)
}
