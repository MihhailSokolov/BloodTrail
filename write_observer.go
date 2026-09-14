// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// ensureBumped bumps the pg watermark counter for scope's write, exactly
// once (WriteScope.Watermark's own bumped flag is the guard) -- called at
// the top of every observer method that is about to reach PostgreSQL with
// a mutating call, so the counter advances before this call's own pg
// effect, per the watermark protocol's spec amendment
// (internal/engine/watermark.go's BumpWatermark doc): the counter must
// still advance even for a write later rolled back, which is only possible
// if it is recorded before, not after, this call's own attempt.
//
// eng == nil is silently a no-op: every construction site under this
// package's control (driver.go's WriteTransaction/BatchOperation, and
// every wrapper's own Nodes()/Relationships()/WithGraph, below) always
// carries a real *engine.Engine, so a nil eng only ever happens in a unit
// test built to exercise ChangeSet recording in isolation, with no engine
// behind it at all (write_observer_test.go's newObservingTransaction and
// its observingNodeQuery/observingRelationshipQuery equivalents) -- the
// same "no database behind it" accommodation engine.claimRebuildLoop's own
// doc already makes for a nil pool.
//
// engine.ErrWatermarkUnavailable is BumpWatermark's own signal that this
// engine was never going to track a watermark at all (no pg pool bound to
// it -- see that error's own doc): distinct from every other bump failure,
// this never logs, never records a ChangeSet fallback, and never opens a
// watermark trust generation, matching the same tolerance the nil-eng branch
// above already gives unit tests (write_observer_test.go's disabledEngine()
// helper is exactly Enabled: false with a nil pool).
//
// Any other error is a genuine failure: BumpWatermark's own protocol
// promise is that it never blocks the write it guards, so this logs once
// (every call, including a retry -- see below) and, the first time this
// scope's bump fails, opens a watermark trust generation for this write (via
// eng.NoteWatermarkBumpFailure, which also marks scope as the one thing that
// can later settle it) and records a ChangeSet fallback -- the applier can no
// longer trust a narrow delta for a write whose counter was never recorded,
// and that same fallback record is what starts the rebuild whose adoption
// eventually restores trust (engine.WatermarkTrusted's own doc).
//
// A scope whose bump has already failed once reaches this same default
// branch again on every later mutating call the same transaction/batch
// makes: Watermark's bumped flag never becomes true for a scope whose bump
// keeps failing, so the early-return guard above never fires, and the retry
// fails the identical bump again. eng.NoteWatermarkBumpFailure reports
// whether THIS call is the one that opened the generation (false on every
// such retry, since the scope is already marked -- its own doc); the
// ChangeSet fallback record is only added on that first call, since a scope
// already carrying a fallback record has nothing new for a repeat "watermark:
// bump failed: <err>" reason to add (ChangeSet.HasFallback() already reports
// true, apply.go's own rebuild trigger already fires) -- recording it again
// per retry would only grow scope's fallback reason set for no benefit.
func ensureBumped(ctx context.Context, eng *engine.Engine, scope *engine.WriteScope) {
	if eng == nil || scope == nil {
		return
	}
	if _, bumped := scope.Watermark(); bumped {
		return
	}

	// applyContext, not ctx directly: every call site below passes a
	// wrapper's own ctx field, which -- exactly like observingTransaction/
	// observingBatch's own ctx, applyContext's original callers -- may be
	// nil for a unit test that constructs one of these types directly. A
	// nil eng already short-circuits above for most of those tests (this
	// function's own doc); this guards the remaining case, a real
	// *engine.Engine with no ctx set on the wrapper that reached it.
	ctx = applyContext(ctx)

	counter, err := eng.BumpWatermark(ctx)
	switch {
	case err == nil:
		scope.SetWatermark(counter)
	case errors.Is(err, engine.ErrWatermarkUnavailable):
		// Nothing to do -- see this function's own doc.
	default:
		if eng.NoteWatermarkBumpFailure(ctx, scope, err) {
			scope.Changes().RecordFallback(fmt.Sprintf("watermark: bump failed: %v", err))
		}
	}
}

// resolveAbandonedWrite resolves scope's watermark bookkeeping
// (engine.ResolveAbandonedWrite) without a full Apply, for a write known to
// have produced no committed effect at all: driver.go's WriteTransaction error
// branch, and the error branch of every driver-level method that bumps
// eagerly at its own top (Run, WipeGraph, SetDefaultGraph,
// DeleteNodesByKinds, DeleteRelationshipsByKinds). See that method's own doc
// for both halves it resolves -- a bump that succeeded still has to fold into
// the applied counter, and a bump that FAILED settles here, since a write that
// returned an error left nothing behind for the replica to be missing.
//
// A no-op when eng or scope is nil, mirroring ensureBumped's own tolerance.
func resolveAbandonedWrite(ctx context.Context, eng *engine.Engine, scope *engine.WriteScope) {
	if eng == nil || scope == nil {
		return
	}
	eng.ResolveAbandonedWrite(applyContext(ctx), scope)
}

// nodeIDSymbol and edgeIDSymbol are the Cypher variable symbols dawgs/query's
// query.Node()/query.NodeID() and query.Relationship()/query.RelationshipID()
// constructors attach (query/identifiers.go's NodeSymbol/EdgeSymbol) --
// hardcoded here rather than imported, mirroring relationshipKindMatcherKinds'
// own "r" literal below and its doc's reasoning: this file stays free of a
// dawgs/query import the same way it stays free of a recognize import, since
// nothing here needs that package's general-purpose criteria builders, only
// the two fixed symbols its InIDs helper always uses.
const (
	nodeIDSymbol = "n"
	edgeIDSymbol = "r"
)

// observingTransaction wraps a live graph.Transaction so that Driver.
// WriteTransaction (driver.go) can learn -- from the very same calls the
// caller was always going to make -- the ChangeSet (changes.go) Apply
// (apply.go) needs to replay the write into the in-memory engine: the
// actual read-back keys (node/edge database ids, or the objectid values an
// upsert identified its target by), or, where a key can't be pinned down, a
// fallback record naming why. Every override records onto scope before (or,
// for Query, without knowing whether it needs to) delegating to the inner
// transaction; the one call this file does not override
// (GraphQueryMemoryLimit) is promoted straight through by embedding,
// unchanged. Commit IS overridden, below, for its own reason.
//
// The zero value is not useful; construct one with a non-nil scope. scope is
// shared with every observingNodeQuery/observingRelationshipQuery this
// transaction hands out (Nodes/Relationships below) and with the
// observingTransaction WithGraph returns, so every write reachable from one
// WriteTransaction call accumulates onto the one scope Driver.
// WriteTransaction hands to engine.Apply once the call succeeds.
//
// eng is used by Commit alone, to flush scope immediately on a
// delegate-issued mid-transaction commit, mirroring observingBatch's own eng
// field and Commit override (see Commit's doc below) -- it is never
// consulted to decide whether a read can be served from the engine, and no
// method on this type, observingNodeQuery, or observingRelationshipQuery
// ever calls into eng (or any engine) for a read: every one of them wraps
// the driver's raw graph.Transaction/NodeQuery/RelationshipQuery directly
// (Driver.WriteTransaction, driver.go), never a wrappedTransaction/
// recordingNodeQuery/recordingRelationshipQuery (transaction.go,
// node_query.go, relationship_query.go) -- so there is no serving decision
// point anywhere on a WriteTransaction's read path for wrote() (below) to
// guard today. This is a verified, pinned fact, not an assumption: see
// TestWriteTransactionReadAfterWriteDelegatesToPG and
// TestBatchReadAfterWriteDelegatesToPG (staleness_integration_test.go), and
// this type's own wrote() doc, for the full argument.
//
// wrote() is still added, and referenced from a short note on every method
// below where a read flows through this type (Query, Nodes, Relationships),
// as defense against a future change that gives any of these types, or the
// query wrappers they hand out, direct engine access: whoever adds that must
// consult wrote() (or the owning transaction's) first and decline -- fall
// through to the inner, PostgreSQL-backed value -- whenever it reports true,
// the same way recordingNodeQuery/recordingRelationshipQuery already consult
// wrappedTransaction.declined today.
type observingTransaction struct {
	graph.Transaction

	scope *engine.WriteScope
	eng   *engine.Engine

	// ctx is the context Driver.WriteTransaction was called with, carried
	// here purely so a mid-transaction Commit's engine.Apply call (which
	// reads written keys back from PostgreSQL) runs under the caller's own
	// deadline and cancellation rather than an unbounded one. It may be nil
	// -- unit tests construct this type directly, and nothing else on it
	// needs a context -- which applyContext turns into context.Background().
	ctx context.Context
}

// wrote reports whether this transaction has recorded any write onto scope
// since Driver.WriteTransaction (driver.go) began it -- equivalently,
// whether scope's ChangeSet is non-empty (engine.WriteScope.Empty's doc).
//
// Once wrote() is true, PostgreSQL -- never this package's in-memory engine
// -- is the only source of truth for any further read run against this same
// transaction: the engine's snapshot is only ever brought up to date by the
// Apply call Driver.WriteTransaction (or, for a mid-transaction commit,
// Commit below) makes once a write is known to have landed, so by
// construction it cannot yet reflect a write this same transaction is still
// in the middle of. See the type doc above for why nothing on this type's
// read path actually consults wrote() today, and for exactly which future
// change would need to.
func (t *observingTransaction) wrote() bool {
	return !t.scope.Empty()
}

// CreateNode delegates, then, once the delegate reports success, records
// the new node's own database id (only ever known from its return value,
// which this method used to discard) as a ChangeSet read-back key.
func (t *observingTransaction) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	ensureBumped(t.ctx, t.eng, t.scope)
	node, err := t.Transaction.CreateNode(properties, kinds...)
	if err == nil && node != nil {
		t.scope.Changes().RecordNodeID(node.ID)
	}
	return node, err
}

// UpdateNode records node's own database id as a ChangeSet read-back key
// unconditionally, before delegating: the applier's read-back re-reads
// whatever PostgreSQL's row actually holds after the update, regardless of
// whether this particular call changed labels, properties, or both.
func (t *observingTransaction) UpdateNode(node *graph.Node) error {
	ensureBumped(t.ctx, t.eng, t.scope)
	if node != nil {
		t.scope.Changes().RecordNodeID(node.ID)
	}
	return t.Transaction.UpdateNode(node)
}

// CreateRelationshipByIDs delegates, then, once the delegate reports
// success, records the new relationship's own database id (only ever known
// from its return value, which this method used to discard) as a ChangeSet
// read-back key.
func (t *observingTransaction) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	ensureBumped(t.ctx, t.eng, t.scope)
	rel, err := t.Transaction.CreateRelationshipByIDs(startNodeID, endNodeID, kind, properties)
	if err == nil && rel != nil {
		t.scope.Changes().RecordEdgeID(rel.ID)
	}
	return rel, err
}

// UpdateRelationship records relationship's own database id as a ChangeSet
// read-back key -- unconditionally, before delegating: relationship.ID is
// already known from the argument itself (unlike CreateRelationshipByIDs'
// captured return), so there is no reason to wait on the delegate's outcome
// the way a captured-return call must. A write-through applier needs this
// edge id key every time this call runs, even though it only ever changes
// properties (a graph.Relationship's Kind is fixed at creation).
func (t *observingTransaction) UpdateRelationship(relationship *graph.Relationship) error {
	ensureBumped(t.ctx, t.eng, t.scope)
	if relationship != nil {
		t.scope.Changes().RecordEdgeID(relationship.ID)
	}
	return t.Transaction.UpdateRelationship(relationship)
}

// Nodes returns an observingNodeQuery wrapping the inner transaction's own
// NodeQuery, so a subsequent Delete()/Update() on it can still reach scope
// -- and, now, still reach eng/ctx so ITS OWN Delete()/Update() can bump
// the watermark eagerly too (a transaction whose only mutating call is a
// NodeQuery delete/update must still have its counter bumped -- see
// observingNodeQuery.Delete/Update's own ensureBumped calls). The returned
// query is never engine-serving-capable today (it wraps the raw NodeQuery
// straight from the driver's transaction, not a recordingNodeQuery) -- see
// the type doc's composition-point note; a future change that gives it one
// must decline whenever wrote() is true.
func (t *observingTransaction) Nodes() graph.NodeQuery {
	return &observingNodeQuery{NodeQuery: t.Transaction.Nodes(), scope: t.scope, eng: t.eng, ctx: t.ctx}
}

// Relationships returns an observingRelationshipQuery wrapping the inner
// transaction's own RelationshipQuery, so a subsequent Delete() or Update()
// on it can still reach scope (and eng/ctx, for the same reason Nodes'
// doc immediately above gives). See that doc for the identical
// composition-point note.
func (t *observingTransaction) Relationships() graph.RelationshipQuery {
	return &observingRelationshipQuery{RelationshipQuery: t.Transaction.Relationships(), scope: t.scope, eng: t.eng, ctx: t.ctx}
}

// Query sniffs query for a Cypher updating clause (cypherMutates) and, if
// found, records a ChangeSet fallback: raw Cypher text can do anything a
// CREATE/SET/REMOVE/DELETE/MERGE clause allows, and nothing short of a full
// recognizer (out of scope here) could say what it actually touched, so
// the only sound answer for the applier is the same one a nil write scope
// gives. query always runs against the inner transaction regardless of what
// cypherMutates reports; the sniff only ever adds a scope mark, never blocks
// or rewrites the call itself.
//
// This method never attempts to serve query from the engine, whether or not
// wrote() is true: t.Transaction is always the driver's raw
// graph.Transaction (Driver.WriteTransaction, driver.go), never a
// wrappedTransaction (transaction.go), so there is no engine.TryCypher call
// on this path to guard -- see the type doc's composition-point note. A
// future change that adds one here (e.g. giving a write transaction's
// not-yet-written reads the same chance ReadTransaction's
// wrappedTransaction.Query already gets) must skip it whenever wrote()
// reports true.
func (t *observingTransaction) Query(query string, parameters map[string]any) graph.Result {
	if cypherMutates(query) {
		t.scope.Changes().RecordFallback("Query: mutating Cypher escapes changelog tracking")
		ensureBumped(t.ctx, t.eng, t.scope)
	}
	return t.Transaction.Query(query, parameters)
}

// Raw always records a ChangeSet fallback: unlike Query, whose text is at
// least Cypher that cypherMutates can attempt to classify, Raw's query is
// driver-specific (SQL, for the PostgreSQL backend this driver wraps) and
// this package has no way to parse it at all.
func (t *observingTransaction) Raw(query string, parameters map[string]any) graph.Result {
	t.scope.Changes().RecordFallback("Raw: driver-specific query escapes changelog tracking")
	ensureBumped(t.ctx, t.eng, t.scope)
	return t.Transaction.Raw(query, parameters)
}

// WithGraph records a ChangeSet fallback -- a transaction retargeted at a
// non-default graph is outside anything this package's changelog tracking
// reasons about, mirroring wrappedTransaction.WithGraph's own "declined"
// treatment on the read side -- and returns a fresh observingTransaction
// wrapping the inner WithGraph's result, sharing the *same* scope (so writes
// against the retargeted graph still land in the one WriteScope this
// WriteTransaction call will eventually report) and the same eng (so Commit
// still works correctly on the retargeted wrapper).
func (t *observingTransaction) WithGraph(graphSchema graph.Graph) graph.Transaction {
	t.scope.Changes().RecordFallback("WithGraph: graph retarget escapes changelog tracking")
	return &observingTransaction{Transaction: t.Transaction.WithGraph(graphSchema), scope: t.scope, eng: t.eng, ctx: t.ctx}
}

// Commit delegates to the inner transaction's own Commit FIRST, then applies
// the accumulated scope to the engine (eng.Apply), then resets scope to a
// fresh, empty WriteScope. Under the pinned dawgs pg driver, a delegate that
// calls tx.Commit() mid-transaction will cause the outer WriteTransaction's
// final Commit to return ErrTxClosed; writes persist, and this override
// ensures the replica is brought up to date at the commit point. This
// override is therefore defensive today: it guards against the
// mid-transaction commit scenario, matching observingBatch.Commit's pattern,
// though that scenario's actual feasibility under the pg driver remains
// unverified (Driver.WriteTransaction still calls Apply once more after the
// delegate returns, reading this transaction's *current* scope value at that
// point -- which by then may be a different *WriteScope than the one this
// method reset it to here, exactly as intended).
//
// The inner Commit runs BEFORE Apply, and this order is load-bearing, not
// cosmetic: Apply's read-back queries PostgreSQL on the pool, a separate
// connection from this transaction, so a read-back run before this
// transaction's own commit would not see this transaction's own
// still-uncommitted writes at all (read-back is not looking through this
// transaction's own eyes) -- Apply would then read the PRE-write state and
// publish nothing, or the wrong thing, over the current View, and reset
// scope having never actually replayed the write it just discarded. Calling
// Commit first is what makes the write visible to Apply's read-back at all.
//
// Apply itself runs unconditionally, even when the inner Commit returns an
// error -- mirroring observingBatch.Commit's identical "apply regardless"
// choice (see its doc for the full reasoning): read-back reads PostgreSQL's
// own current committed state per key, so applying after a failed commit is
// always safe, never wrong -- a key whose write never landed (the whole
// transaction rolled back) simply reads back as it already was, and a key
// whose write landed via some other path this call cannot see is reconciled
// exactly as if this call had never run.
func (t *observingTransaction) Commit() error {
	err := t.Transaction.Commit()
	t.eng.Apply(applyContext(t.ctx), t.scope)
	t.scope = engine.NewWriteScope()
	return err
}

// applyContext returns ctx, or context.Background() when ctx is nil -- the
// case for an observingTransaction/observingBatch constructed directly by a
// unit test rather than by Driver.WriteTransaction/BatchOperation. An apply
// with no caller context of its own is still worth performing; it simply has
// no deadline to inherit.
func applyContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// observingNodeQuery wraps a live graph.NodeQuery so that Delete() and
// Update() can mark scope before delegating. graph.NodeQuery is embedded, so
// every method this file does not override (Query, Count, First, Fetch,
// FetchIDs, FetchKinds) is promoted straight through to the inner query
// unchanged.
//
// The zero value is not useful; construct one via observingTransaction's own
// Nodes() or observingBatch's own Nodes().
type observingNodeQuery struct {
	graph.NodeQuery

	scope *engine.WriteScope

	// eng and ctx are Nodes()'s own eng/ctx, carried here purely so
	// Delete()/Update() can bump the watermark eagerly (ensureBumped)
	// before delegating -- the same fields, and the same nil tolerance, as
	// observingTransaction/observingBatch's own eng/ctx.
	eng *engine.Engine
	ctx context.Context

	// criteria accumulates every Filter/Filterf argument, in call order,
	// mirroring observingRelationshipQuery's own field -- see its doc.
	// Delete and Update only attempt to recognize an InIDs-shaped target
	// list when exactly one criteria was recorded; see
	// nodeIDsFromCriteria's doc.
	criteria []graph.Criteria
}

// Filter records criteria and delegates to the inner query, returning this
// same wrapper so the fluent chain keeps flowing through observingNodeQuery
// -- without this, a subsequent .Delete()/.Update() on the chain's result
// would reach the inner query directly, skipping this wrapper's scope
// marking entirely.
func (q *observingNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	q.criteria = append(q.criteria, criteria)
	q.NodeQuery = q.NodeQuery.Filter(criteria)
	return q
}

// Filterf calls criteriaDelegate once to record its result, then passes
// criteriaDelegate itself through to the inner query's own Filterf --
// rather than a closure fixed to the already-observed value -- so the
// inner query's own semantics for calling the provider are unaffected
// (mirroring observingRelationshipQuery.Filterf's identical reasoning).
func (q *observingNodeQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.NodeQuery {
	q.criteria = append(q.criteria, criteriaDelegate())
	q.NodeQuery = q.NodeQuery.Filterf(criteriaDelegate)
	return q
}

// OrderBy delegates to the inner query and re-wraps the result so the fluent
// chain keeps flowing through observingNodeQuery -- see Filter's doc for why
// that matters. Without this override, the embedded graph.NodeQuery's own
// OrderBy would be promoted straight through and return whatever the inner
// query's OrderBy returns (itself, unwrapped, for the pg driver's own
// NodeQuery), silently detaching the chain from this wrapper: a subsequent
// .Delete() would then land on the inner query directly and scope would
// never learn about it. Unlike recordingRelationshipQuery's identical-shaped
// override on the read side, this does not taint anything -- an observer's
// only job is to keep the chain observed, not to decide whether a later
// call is safe to serve from the engine.
func (q *observingNodeQuery) OrderBy(criteria ...graph.Criteria) graph.NodeQuery {
	q.NodeQuery = q.NodeQuery.OrderBy(criteria...)
	return q
}

// Offset is OrderBy's Offset equivalent; see its doc for why re-wrapping
// (rather than promoting) matters here.
func (q *observingNodeQuery) Offset(skip int) graph.NodeQuery {
	q.NodeQuery = q.NodeQuery.Offset(skip)
	return q
}

// Limit is OrderBy's Limit equivalent; see its doc for why re-wrapping
// (rather than promoting) matters here.
func (q *observingNodeQuery) Limit(limit int) graph.NodeQuery {
	q.NodeQuery = q.NodeQuery.Limit(limit)
	return q
}

// Delete records either a recognized InIDs target list or a fallback
// before delegating: this query's criteria could match nodes of any kind
// the caller didn't explicitly name, and deleting a node cascades to every
// edge incident to it, so a criteria shape nodeIDsFromCriteria does not
// recognize gives the applier no sound way to replay this delete narrowly
// -- RecordFallback is the honest answer (a kind-only node delete goes
// through Driver.DeleteNodesByKinds instead of this query -- see driver.go
// -- so this method's own recognizer only ever needs to look for InIDs, not
// a kind matcher).
func (q *observingNodeQuery) Delete() error {
	ensureBumped(q.ctx, q.eng, q.scope)
	if ids, ok := nodeIDsFromCriteria(q.criteria); ok {
		for _, id := range ids {
			q.scope.Changes().RecordNodeID(id)
		}
	} else {
		q.scope.Changes().RecordFallback("NodeQuery.Delete: unrecognized criteria")
	}
	return q.NodeQuery.Delete()
}

// Update records either a recognized InIDs target list or a fallback
// before delegating, mirroring Delete's own reasoning immediately above.
func (q *observingNodeQuery) Update(properties *graph.Properties) error {
	ensureBumped(q.ctx, q.eng, q.scope)
	if ids, ok := nodeIDsFromCriteria(q.criteria); ok {
		for _, id := range ids {
			q.scope.Changes().RecordNodeID(id)
		}
	} else {
		q.scope.Changes().RecordFallback("NodeQuery.Update: unrecognized criteria")
	}
	return q.NodeQuery.Update(properties)
}

// observingRelationshipQuery wraps a live graph.RelationshipQuery, recording
// every criteria the caller filters by (mirroring relationship_query.go's
// read-side recordingRelationshipQuery) so a subsequent Delete() or Update()
// has a chance to recognize a scoped target and mark (and record) only what
// it actually affects, instead of the fully conservative fallback. graph.
// RelationshipQuery is embedded, so every method this file does not override
// (Count, First, Query, Fetch, FetchDirection, FetchIDs, FetchTriples,
// FetchKinds, FetchAllShortestPaths) is promoted straight through unchanged.
//
// The zero value is not useful; construct one via observingTransaction's own
// Relationships() or observingBatch's own Relationships().
type observingRelationshipQuery struct {
	graph.RelationshipQuery

	scope *engine.WriteScope

	// eng and ctx are Relationships()'s own eng/ctx, carried here purely so
	// Delete()/Update() can bump the watermark eagerly (ensureBumped)
	// before delegating -- the same fields, and the same nil tolerance, as
	// observingTransaction/observingBatch's own eng/ctx.
	eng *engine.Engine
	ctx context.Context

	// criteria accumulates every Filter/Filterf argument, in call order.
	// Delete only attempts to recognize a kind-scoped shape when exactly one
	// criteria was recorded -- the shape edgeKindsFromCriteria (and its
	// eventual replacement, recognize.FromRelCriteria) recognizes is
	// always a single conjunction or bare KindMatcher built in one Filter
	// call, never several composed together.
	criteria []graph.Criteria
}

// Filter records criteria and delegates to the inner query, returning this
// same wrapper so the fluent chain keeps flowing through
// observingRelationshipQuery (see observingNodeQuery.Filter's doc for why
// that matters).
func (r *observingRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	r.criteria = append(r.criteria, criteria)
	r.RelationshipQuery = r.RelationshipQuery.Filter(criteria)
	return r
}

// Filterf calls criteriaDelegate once to record its result, then passes
// criteriaDelegate itself through to the inner query's own Filterf -- rather
// than a closure fixed to the already-observed value -- so the inner
// query's own semantics for calling the provider are unaffected (mirroring
// recordingRelationshipQuery.Filterf's identical reasoning).
func (r *observingRelationshipQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.RelationshipQuery {
	r.criteria = append(r.criteria, criteriaDelegate())
	r.RelationshipQuery = r.RelationshipQuery.Filterf(criteriaDelegate)
	return r
}

// OrderBy delegates to the inner query and re-wraps the result so the
// fluent chain keeps flowing through observingRelationshipQuery -- mirroring
// observingNodeQuery.OrderBy's identical reasoning (see its doc): without
// this override, the promoted embedded method would return the inner pg
// query unwrapped, and a chain ending in .Delete() after .OrderBy(...) would
// silently skip this wrapper's scope marking. This does not taint the
// query the way recordingRelationshipQuery.OrderBy does on the read side --
// an observer only needs to stay attached to the chain, not to decide
// whether a later call can be served from the engine.
func (r *observingRelationshipQuery) OrderBy(criteria ...graph.Criteria) graph.RelationshipQuery {
	r.RelationshipQuery = r.RelationshipQuery.OrderBy(criteria...)
	return r
}

// Offset is OrderBy's Offset equivalent; see its doc for why re-wrapping
// (rather than promoting) matters here.
func (r *observingRelationshipQuery) Offset(skip int) graph.RelationshipQuery {
	r.RelationshipQuery = r.RelationshipQuery.Offset(skip)
	return r
}

// Limit is OrderBy's Limit equivalent; see its doc for why re-wrapping
// (rather than promoting) matters here.
func (r *observingRelationshipQuery) Limit(limit int) graph.RelationshipQuery {
	r.RelationshipQuery = r.RelationshipQuery.Limit(limit)
	return r
}

// Delete marks scope via relationshipDeleteScope (see its doc for exactly
// which shapes are recognized, and why any conjunct beyond bare relationship
// KindMatchers falls back rather than being ignored). The recognized
// branch's ChangeSet entry is RecordDeleteRelationshipsByKinds(kinds), not
// an enumerated edge id list: relationshipDeleteScope's own recognized shape
// is a kind matcher, not an InIDs target list, so "delete every relationship
// of these kinds" is the operation this delete actually performs, and the
// one the applier should replay -- an id list captured before the delete
// ran could go stale by the time the applier reads it back. The
// unrecognized branch falls back, same as every other unrecognized
// criteria in this file.
//
// Unlike every other observer in this file, the recognized entry is recorded
// only AFTER the delete has actually succeeded. It is the one entry that is
// an exact instruction to mutate the replica rather than a read-back key: a
// kind-criteria scope issues no read-back query at all, so nothing later
// consults PostgreSQL about it and nothing can correct it. Recorded ahead of
// a delete that then failed (a statement timeout, a deadlock, a reset
// connection), it would tombstone every edge of those kinds in the replica
// while PostgreSQL still holds them, with no fallback recorded and the
// engine still reporting itself healthy -- every memory-served query would
// silently omit them until some unrelated rebuild. A failed delete records
// a fallback instead, which is the honest description of what the replica
// now knows: nothing.
func (r *observingRelationshipQuery) Delete() error {
	ensureBumped(r.ctx, r.eng, r.scope)
	kinds, touchAll := relationshipDeleteScope(r.criteria)
	if touchAll {
		r.scope.Changes().RecordFallback("RelationshipQuery.Delete: unrecognized criteria")
		return r.RelationshipQuery.Delete()
	}
	if err := r.RelationshipQuery.Delete(); err != nil {
		r.scope.Changes().RecordFallback("RelationshipQuery.Delete: delete failed")
		return err
	}
	r.scope.Changes().RecordDeleteRelationshipsByKinds(kinds)
	return nil
}

// Update records either a recognized InIDs target list or a fallback
// before delegating -- the RelationshipQuery half of
// observingNodeQuery.Update's identical reasoning.
func (r *observingRelationshipQuery) Update(properties *graph.Properties) error {
	ensureBumped(r.ctx, r.eng, r.scope)
	if ids, ok := edgeIDsFromCriteria(r.criteria); ok {
		for _, id := range ids {
			r.scope.Changes().RecordEdgeID(id)
		}
	} else {
		r.scope.Changes().RecordFallback("RelationshipQuery.Update: unrecognized criteria")
	}
	return r.RelationshipQuery.Update(properties)
}

// relationshipDeleteScope decides what an observingRelationshipQuery.
// Delete() call's ChangeSet entry should be, given every criteria its
// caller filtered by: the RecordDeleteRelationshipsByKinds entry it feeds
// is replayed verbatim by the applier (apply.go) to decide which edges to
// tombstone in the in-memory replica, so a reported kind set that is too
// WIDE is unsound there -- PostgreSQL only deleted the rows also matching
// whatever else the query narrowed by, so the applier would tombstone
// edges PostgreSQL never touched.
//
// When exactly one criteria was recorded and edgeKindsFromCriteria
// recognizes it with a non-empty result, kinds is that result and touchAll
// is false: edgeKindsFromCriteria's own recognized shape (see its doc) is
// now exactly "one or more bare relationship KindMatchers, ANDed together
// with nothing else that could narrow the match further" -- so the
// operation the query actually performs really is "delete every edge of
// these kinds", which is exactly what the applier needs to replay it
// exactly. Every other case -- zero or more than one criteria, an
// unrecognized shape, a Conjunction carrying any conjunct that is not
// itself a bare relationship KindMatcher, or a recognized KindMatcher whose
// Kinds came back empty (which means "matches every kind", the opposite of
// a narrow scope, per the KindMatcher/PathQuery.EdgeKinds convention
// documented on recognize.PathQuery) -- reports touchAll instead, so the
// applier falls back to a full rebuild rather than guessing.
func relationshipDeleteScope(criteria []graph.Criteria) (kinds graph.Kinds, touchAll bool) {
	if len(criteria) == 1 {
		if ks, ok := edgeKindsFromCriteria(criteria[0]); ok && len(ks) > 0 {
			return ks, false
		}
	}
	return nil, true
}

// edgeKindsFromCriteria is a minimal stand-in for a not-yet-written
// recognize.FromRelCriteria: it recognizes just enough of a
// relationship-delete's criteria to scope the delete soundly for
// relationshipDeleteScope's RecordDeleteRelationshipsByKinds ChangeSet
// entry (see its doc), without depending on a recognizer package that
// doesn't exist yet. That entry is replayed verbatim by the applier, so the
// reported kinds must describe EXACTLY what the delete removes, not merely
// a safe-to-over-invalidate approximation.
//
// Recognized shapes are a bare *cypher.KindMatcher over the relationship
// variable "r" (what dawgs' query.Kind(query.Relationship(), k)/
// query.KindIn(query.Relationship(), ks...) builds), or a
// *cypher.Conjunction all of whose expressions are such KindMatchers --
// nothing else may appear alongside them. Multiple KindMatchers union their
// kinds: a delete matching kind K1 or K2 still only touches K1 and K2's
// edges.
//
// This is deliberately NOT the "ignore what you don't recognize, fall back
// to RecordFallback" pattern this file's other recognizers use, where
// over-invalidating is always safe. A Conjunction additionally narrowed by,
// say, a property filter or an endpoint id removes only a SUBSET of the
// named kinds' edges, which means the kinds this function would otherwise
// report describe a SUPERSET of what the query actually deletes -- fine
// for a plain safe-superset fallback, but unsound to hand the applier as
// an exact tombstone criteria (it would then
// delete edges PostgreSQL never touched). So any conjunct that is not
// itself a bare relationship KindMatcher -- whatever kind of expression it
// is, or a KindMatcher over the wrong variable -- fails the WHOLE
// Conjunction closed (ok=false) instead of being silently dropped.
//
// Anything else -- a nil criteria, one that isn't a Conjunction or bare
// KindMatcher, a KindMatcher over any variable but "r", an empty
// Conjunction, or a Conjunction containing so much as one conjunct that
// fails the check above -- reports ok=false.
func edgeKindsFromCriteria(criteria graph.Criteria) (graph.Kinds, bool) {
	switch typed := criteria.(type) {
	case *cypher.KindMatcher:
		return relationshipKindMatcherKinds(typed)

	case *cypher.Conjunction:
		if typed == nil || len(typed.Expressions) == 0 {
			return nil, false
		}

		var kinds graph.Kinds
		for _, expr := range typed.Expressions {
			km, isKindMatcher := expr.(*cypher.KindMatcher)
			if !isKindMatcher {
				// A non-KindMatcher sibling narrows the match beyond the
				// kinds alone -- see the doc above for why that makes a
				// kinds-only report unsound.
				return nil, false
			}
			ks, matched := relationshipKindMatcherKinds(km)
			if !matched {
				// A KindMatcher over some other variable (or reference) is
				// exactly as narrowing as any other conjunct type here --
				// fail the whole Conjunction closed rather than dropping
				// just this one.
				return nil, false
			}
			kinds = append(kinds, ks...)
		}
		return kinds, true

	default:
		return nil, false
	}
}

// relationshipKindMatcherKinds reports whether km is a KindMatcher over the
// bare relationship variable "r" -- the symbol dawgs/query's
// query.Relationship() constructor attaches (see internal/engine/recognize/
// recognize.go's edgeSymbol constant and matchRelationshipKinds, whose
// AST-matching this is a deliberate copy of, kept local rather than
// exported/imported so this file's dependency on the recognize package's
// surface stays at zero until a real FromRelCriteria exists to call
// instead). A KindMatcher over any other reference (notably the node
// variable "n"), or a nil km, fails.
func relationshipKindMatcherKinds(km *cypher.KindMatcher) (graph.Kinds, bool) {
	if km == nil {
		return nil, false
	}

	variable, isVariable := km.Reference.(*cypher.Variable)
	if !isVariable || variable == nil || variable.Symbol != "r" {
		return nil, false
	}

	return km.Kinds, true
}

// nodeIDsFromCriteria recognizes criteria as exactly one recorded
// criteria, shaped like query.InIDs(query.NodeID(), ids...) -- see
// singleInIDsCriteria's doc for the exact AST shape recognized. Used by
// observingNodeQuery.Delete and Update to record a precise ChangeSet entry
// instead of a bare fallback whenever the caller named specific node ids
// this way (the shape a target-by-id delete or update is expected to use;
// see observingNodeQuery.Delete's own doc for why a kind-scoped node
// delete never reaches this recognizer at all).
func nodeIDsFromCriteria(criteria []graph.Criteria) ([]graph.ID, bool) {
	if len(criteria) != 1 {
		return nil, false
	}
	return singleInIDsCriteria(criteria[0], nodeIDSymbol)
}

// edgeIDsFromCriteria is nodeIDsFromCriteria's relationship equivalent,
// recognizing query.InIDs(query.RelationshipID(), ids...). Used by
// observingRelationshipQuery.Update; observingRelationshipQuery.Delete
// uses relationshipDeleteScope/edgeKindsFromCriteria instead, recognizing a
// kind matcher rather than an id list (see that method's own doc for why).
func edgeIDsFromCriteria(criteria []graph.Criteria) ([]graph.ID, bool) {
	if len(criteria) != 1 {
		return nil, false
	}
	return singleInIDsCriteria(criteria[0], edgeIDSymbol)
}

// singleInIDsCriteria recognizes criteria as exactly the AST shape
// query.InIDs(query.NodeID()/query.RelationshipID(), ids...) builds
// (dawgs' query/model.go and query/identifiers.go): a *cypher.Comparison
// with exactly one partial, operator IN, whose left side is a
// single-argument id() *cypher.FunctionInvocation over a bare *cypher.
// Variable named wantSymbol, and whose right side is a *cypher.Parameter
// wrapping a []graph.ID (query.Parameter's own shape for the ids query.
// InIDs was called with -- see query/model.go's InIDs/Parameter). ok is
// false for any other shape, including a nil criteria, a criteria built by
// hand as raw Cypher text (a bare *cypher.ListLiteral right-hand side, the
// shape parsed Cypher text produces, is deliberately NOT recognized here),
// or an id() call over anything but a bare Variable (e.g. query.StartID()/
// query.EndID(), which wrap a differently-symbolled Variable and are never
// what NodeQuery/RelationshipQuery.Update/Delete criteria in this codebase
// use).
//
// A recognized criteria naming zero ids (an empty, non-nil []graph.ID from
// query.InIDs(query.NodeID()/query.RelationshipID())) still reports ok=true,
// with an empty ids result -- this is deliberate, not an edge case the
// recognizer merely tolerates: nodeIDsFromCriteria/edgeIDsFromCriteria's own
// callers then record nothing beyond this recognized-but-empty target list,
// which is the correct changelog entry for that write, not a fallback --
// an `id IN []` filter matches zero rows, so an empty ChangeSet is the
// truth about what it did.
//
// This is deliberately narrower than dawgs' own more general id-list
// recognition (internal/engine/recognize's unexported, read-path-only
// idListFrom/matchIDIn): mirroring edgeKindsFromCriteria's own doc, this
// file has a long-standing zero-dependency-on-recognize convention, and the
// one shape this package's write path actually needs to recognize --
// ids built by query.InIDs itself, never a hand-rolled Cypher list literal
// -- is exactly this one.
func singleInIDsCriteria(criteria graph.Criteria, wantSymbol string) ([]graph.ID, bool) {
	cmp, isComparison := criteria.(*cypher.Comparison)
	if !isComparison || cmp == nil || len(cmp.Partials) != 1 {
		return nil, false
	}

	partial := cmp.Partials[0]
	if partial == nil || partial.Operator != cypher.OperatorIn {
		return nil, false
	}

	fn, isFunctionInvocation := cmp.Left.(*cypher.FunctionInvocation)
	if !isFunctionInvocation || fn == nil || fn.Name != "id" || len(fn.Arguments) != 1 {
		return nil, false
	}

	variable, isVariable := fn.Arguments[0].(*cypher.Variable)
	if !isVariable || variable == nil || variable.Symbol != wantSymbol {
		return nil, false
	}

	param, isParameter := partial.Right.(*cypher.Parameter)
	if !isParameter || param == nil {
		return nil, false
	}

	ids, isIDSlice := param.Value.([]graph.ID)
	return ids, isIDSlice
}

// observingBatch wraps a live graph.Batch so that Driver.BatchOperation
// (driver.go) can learn the ChangeSet a batch's writes need to be replayed
// into the in-memory engine, the same way observingTransaction does for
// WriteTransaction. graph.Batch has no method this file leaves promoted
// unmodified -- every one of its methods either changes graph data (and so
// needs a ChangeSet entry) or is Commit, which this file overrides for its
// own reason (see Commit's doc) -- so, unlike the other wrappers in this
// file, embedding graph.Batch here exists only to satisfy the interface's
// method set at compile time, not to promote anything.
//
// The zero value is not useful; construct one with a non-nil scope and eng.
// eng is only ever used by Commit, to flush scope immediately rather than
// waiting for the enclosing Driver.BatchOperation call to finish (see
// Commit's doc) -- never to decide whether a read can be served from the
// engine; see observingTransaction's identical doc (write_observer.go above)
// for why, which applies to this type unchanged.
type observingBatch struct {
	graph.Batch

	scope *engine.WriteScope
	eng   *engine.Engine

	// ctx is Driver.BatchOperation's own context, carried for the same
	// reason (and with the same nil tolerance) as observingTransaction.ctx.
	ctx context.Context
}

// wrote reports whether this batch has recorded any write onto scope since
// it began (Driver.BatchOperation, driver.go) or since the last Commit
// flush reset it (Commit's own doc) -- equivalently, whether scope is
// non-empty. See observingTransaction.wrote's doc for the full reasoning:
// identical here, substituting "batch" for "transaction" throughout.
func (b *observingBatch) wrote() bool {
	return !b.scope.Empty()
}

// CreateNode delegates, then, once the delegate reports success, records a
// ChangeSet read-back key for the write via recordBatchCreateNodeIdentity --
// see that function's own doc for exactly which of node.ID or an "objectid"
// property it prefers, and why a create that offers neither records a
// fallback instead of an enumerable key.
func (b *observingBatch) CreateNode(node *graph.Node) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	err := b.Batch.CreateNode(node)
	if err == nil {
		recordBatchCreateNodeIdentity(b.scope, node)
	}
	return err
}

// recordBatchCreateNodeIdentity records observingBatch.CreateNode's
// ChangeSet entry for a create the delegate has already reported success
// for. Unlike observingTransaction.CreateNode's tx-level equivalent, a
// batch INSERT (graph.Batch.CreateNode's own doc: reports success/failure
// only) never returns the row's generated id, so this method must instead
// look at what the caller itself gave node:
//
//   - If node.ID is a real preset id -- neither graph.UnregisteredNodeID,
//     the sentinel graph.PrepareNode assigns every node this codebase
//     constructs for a plain "let the database assign an id" create (see
//     that function's own doc, and this package's own convention, verified
//     against hydrate_integration_test.go's PrepareNode usage), nor 0,
//     which the pg batch treats identically to that sentinel (its
//     flushNodeCreateBuffer routes `node.ID == 0 || node.ID ==
//     graph.UnregisteredNodeID` to the generate-an-id path) -- the caller
//     preset a real id itself. The one caller known to do this is the
//     neo4j-to-PostgreSQL migration tool's own path, which carries over
//     each node's original neo4j id rather than letting a fresh one be
//     assigned; that preset id is exactly what a later read-back needs, so
//     it is recorded via RecordNodeID. Recording id 0 instead would key the
//     read-back on a row that cannot exist, so the applier would find
//     nothing, tombstone nothing, and leave the node PostgreSQL really
//     created absent from the replica with no fallback to correct it.
//   - Otherwise, if node's own Properties carry a string "objectid" value
//     (objectIDFromProperties -- the same low-level read
//     nodeUpsertObjectIDFor's declared-identity check uses, called directly
//     here since a plain create has no IdentityProperties to check at all),
//     that objectid is recorded via RecordNodeObjectID instead: a read-back
//     by objectid finds whatever row the plain INSERT produced, multi-match
//     included, exactly the same way an UpdateNodeBy upsert's own
//     objectid-keyed read-back does.
//   - Otherwise, this create gave the applier no key to re-read the new row
//     by at all, so it records a fallback.
func recordBatchCreateNodeIdentity(scope *engine.WriteScope, node *graph.Node) {
	if node.ID != graph.UnregisteredNodeID && node.ID != 0 {
		scope.Changes().RecordNodeID(node.ID)
		return
	}
	if objectID, ok := objectIDFromProperties(node.Properties); ok {
		scope.Changes().RecordNodeObjectID(objectID)
		return
	}
	scope.Changes().RecordFallback("Batch.CreateNode: no id or objectid to key read-back")
}

// CreateNodes implements graph.NodeBatchCreator, delegating to the inner
// batch's own CreateNodes when it supports that optional bulk-create
// contract (retriever/load.go's own caller-side type assertion, in dawgs,
// is the production shape this mirrors: `creator, ok :=
// batch.(graph.NodeBatchCreator)`). Go interfaces are static -- an
// observingBatch cannot expose CreateNodes only when the inner batch
// happens to -- so this method always exists; when the inner batch does
// NOT implement NodeBatchCreator, it returns a descriptive error instead
// of the ids a caller's own type assertion would otherwise expect, the
// same failure shape retriever/load.go's own assertion-failure branch
// already produces for a batch with no bulk-create support at all.
//
// Once the delegate reports success, every returned id is recorded onto the
// ChangeSet as a read-back key (RecordNodeID) -- the ids are documented to
// align index-for-index with nodes (graph.NodeBatchCreator's own doc:
// "returns generated IDs in input order"), but this only needs the ids
// themselves, not that alignment, so no attempt is made to pair a specific
// id back to a specific input node.
func (b *observingBatch) CreateNodes(nodes []*graph.Node) ([]graph.ID, error) {
	creator, ok := b.Batch.(graph.NodeBatchCreator)
	if !ok {
		return nil, fmt.Errorf("bloodtrail: batch %T does not support correlated bulk node creation", b.Batch)
	}

	// After the type-assertion check, not before: a batch whose inner Batch
	// doesn't support NodeBatchCreator at all never reaches PostgreSQL for
	// this call, so bumping ahead of that check would advance the counter
	// for a call that had no pg effect at all.
	ensureBumped(b.ctx, b.eng, b.scope)

	ids, err := creator.CreateNodes(nodes)
	if err != nil {
		return ids, err
	}

	for _, id := range ids {
		b.scope.Changes().RecordNodeID(id)
	}
	return ids, nil
}

// DeleteNode records id onto the ChangeSet as a read-back key: the applier
// re-reads it from PostgreSQL the same way every other RecordNodeID
// caller's target is re-read, finding it gone and applying the delete.
func (b *observingBatch) DeleteNode(id graph.ID) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	b.scope.Changes().RecordNodeID(id)
	return b.Batch.DeleteNode(id)
}

// Nodes returns an observingNodeQuery wrapping the inner batch's own
// NodeQuery, exactly as observingTransaction.Nodes does -- including eng/
// ctx, for the same reason (a batch whose only mutating call is a NodeQuery
// delete/update must still have its counter bumped), and the same
// composition-point note: the returned query is never engine-serving-
// capable today, and a future change that gives it one must decline
// whenever wrote() is true.
func (b *observingBatch) Nodes() graph.NodeQuery {
	return &observingNodeQuery{NodeQuery: b.Batch.Nodes(), scope: b.scope, eng: b.eng, ctx: b.ctx}
}

// Relationships returns an observingRelationshipQuery wrapping the inner
// batch's own RelationshipQuery, exactly as observingTransaction.
// Relationships does. See Nodes' doc immediately above for the identical
// composition-point note.
func (b *observingBatch) Relationships() graph.RelationshipQuery {
	return &observingRelationshipQuery{RelationshipQuery: b.Batch.Relationships(), scope: b.scope, eng: b.eng, ctx: b.ctx}
}

// UpdateNodeBy records update's ChangeSet entry via recordNodeUpsertIdentity,
// then delegates.
func (b *observingBatch) UpdateNodeBy(update graph.NodeUpdate) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	recordNodeUpsertIdentity(b.scope, update)
	return b.Batch.UpdateNodeBy(update)
}

// recordNodeUpsertIdentity records update's ChangeSet entry: RecordNodeObjectID
// when update's identity is recognized (nodeUpsertObjectID's doc), or
// RecordFallback otherwise. Shared by observingBatch.UpdateNodeBy directly,
// and by recordRelationshipUpsertIdentity below for each of
// UpdateRelationshipBy's two endpoints.
func recordNodeUpsertIdentity(scope *engine.WriteScope, update graph.NodeUpdate) {
	if objectID, ok := nodeUpsertObjectID(update); ok {
		scope.Changes().RecordNodeObjectID(objectID)
		return
	}
	scope.Changes().RecordFallback("Batch.UpdateNodeBy: unrecognized identity")
}

// nodeUpsertObjectID recognizes update as identifying its target node by a
// bare "objectid" property: update.IdentityProperties must be exactly
// ["objectid"], and update.Node.Properties must carry a string value under
// that key. Any other shape -- a nil Node, a nil Properties, a different
// or additional identity property, or a non-string/absent objectid value
// -- fails. This mirrors nodeUpsertObjectIDFor's identical logic, factored
// out so both UpdateNodeBy's own identity and UpdateRelationshipBy's two
// endpoint identities (which carry the properties slightly differently --
// Start/End *graph.Node plus Start/EndIdentityProperties, rather than one
// combined NodeUpdate) go through the same recognition rule.
func nodeUpsertObjectID(update graph.NodeUpdate) (string, bool) {
	return nodeUpsertObjectIDFor(update.Node, update.IdentityProperties)
}

// nodeUpsertObjectIDFor is nodeUpsertObjectID's shared implementation; see
// its doc.
func nodeUpsertObjectIDFor(node *graph.Node, identityProperties []string) (string, bool) {
	if node == nil {
		return "", false
	}
	if len(identityProperties) != 1 || identityProperties[0] != "objectid" {
		return "", false
	}
	return objectIDFromProperties(node.Properties)
}

// objectIDFromProperties reads a string "objectid" value off properties,
// reporting ok=false when properties is nil, the key is absent, or the
// stored value isn't a string. This is the shared low-level read behind
// nodeUpsertObjectIDFor's declared-identity check (an UpdateNodeBy/
// UpdateRelationshipBy upsert whose IdentityProperties names "objectid")
// and observingBatch.CreateNode's own undeclared "does this freshly created
// node happen to carry one" probe below -- the latter has no
// IdentityProperties to check at all (CreateNode's caller never sets one),
// so it calls this function directly rather than through
// nodeUpsertObjectIDFor.
func objectIDFromProperties(properties *graph.Properties) (string, bool) {
	if properties == nil {
		return "", false
	}
	value, err := properties.Get("objectid").String()
	if err != nil {
		return "", false
	}
	return value, true
}

// UpdateNodes records each node's own database id as a ChangeSet read-back
// key, then delegates.
func (b *observingBatch) UpdateNodes(nodes []*graph.Node) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	for _, node := range nodes {
		if node != nil {
			b.scope.Changes().RecordNodeID(node.ID)
		}
	}
	return b.Batch.UpdateNodes(nodes)
}

// CreateRelationship records a ChangeSet edge triple, then delegates.
func (b *observingBatch) CreateRelationship(relationship *graph.Relationship) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	if relationship != nil {
		b.scope.Changes().RecordEdgeTriple(relationship.StartID, relationship.EndID, relationship.Kind)
	}
	return b.Batch.CreateRelationship(relationship)
}

// CreateRelationshipByIDs records a ChangeSet edge triple, then delegates.
// graph.Batch's own CreateRelationshipByIDs is deprecated in favor of
// CreateRelationship, but this wrapper calls it anyway rather than
// rewriting the call into a CreateRelationship of its own: this method
// exists purely to observe and pass through whatever the caller invoked,
// and the two methods are not documented to behave identically for every
// graph.Batch implementation (graph.Batch's own CreateRelationship carries
// a TODO about incorrect upsert-on-conflict behavior that
// CreateRelationshipByIDs may or may not share) -- silently rerouting the
// call here could change behavior for a Batch implementation this package
// has no way to know about.
//
//nolint:staticcheck // SA1019: deliberate passthrough of a deprecated call, see doc above.
func (b *observingBatch) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	b.scope.Changes().RecordEdgeTriple(startNodeID, endNodeID, kind)
	return b.Batch.CreateRelationshipByIDs(startNodeID, endNodeID, kind, properties)
}

// DeleteRelationship records id onto the ChangeSet as a read-back key
// (mirroring DeleteNode's identical reasoning), then delegates.
func (b *observingBatch) DeleteRelationship(id graph.ID) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	b.scope.Changes().RecordEdgeID(id)
	return b.Batch.DeleteRelationship(id)
}

// UpdateRelationshipBy records update's ChangeSet entry via
// recordRelationshipUpsertIdentity, then delegates.
func (b *observingBatch) UpdateRelationshipBy(update graph.RelationshipUpdate) error {
	ensureBumped(b.ctx, b.eng, b.scope)
	recordRelationshipUpsertIdentity(b.scope, update)
	return b.Batch.UpdateRelationshipBy(update)
}

// recordRelationshipUpsertIdentity records update's ChangeSet entry.
// update.Relationship must be non-nil, and both its Start and End
// endpoints must be recognized by nodeUpsertObjectIDFor (update's own
// StartIdentityProperties/EndIdentityProperties, each exactly ["objectid"],
// resolving to a string value on the corresponding Start/End node) -- the
// pg upsert this call mirrors upserts both endpoint nodes AND the
// relationship in one statement, so a triple keyed on the endpoints'
// objectids is only sound to record when both endpoints are actually
// resolvable that way. When recognized, this records the edge triple AND
// a RecordNodeObjectID for each endpoint (the upsert's own effect on the
// endpoint nodes, which the applier's read-back needs independently of the
// relationship itself). Any other shape -- including just one endpoint
// unrecognized -- records a single fallback instead.
func recordRelationshipUpsertIdentity(scope *engine.WriteScope, update graph.RelationshipUpdate) {
	startOID, startOK := nodeUpsertObjectIDFor(update.Start, update.StartIdentityProperties)
	endOID, endOK := nodeUpsertObjectIDFor(update.End, update.EndIdentityProperties)

	if update.Relationship == nil || !startOK || !endOK {
		scope.Changes().RecordFallback("Batch.UpdateRelationshipBy: unrecognized identity")
		return
	}

	scope.Changes().RecordEdgeTripleByObjectID(startOID, endOID, update.Relationship.Kind)
	scope.Changes().RecordNodeObjectID(startOID)
	scope.Changes().RecordNodeObjectID(endOID)
}

// WithGraph records a ChangeSet fallback and returns a fresh observingBatch
// wrapping the inner WithGraph's result, sharing the same scope -- mirroring
// observingTransaction.WithGraph's identical reasoning.
func (b *observingBatch) WithGraph(graphSchema graph.Graph) graph.Batch {
	b.scope.Changes().RecordFallback("WithGraph: graph retarget escapes changelog tracking")
	return &observingBatch{Batch: b.Batch.WithGraph(graphSchema), scope: b.scope, eng: b.eng, ctx: b.ctx}
}

// Commit delegates to the inner batch's own Commit FIRST -- which flushes
// every operation still sitting in the buffer (graph.Batch's own doc on
// Commit: "calls to commit this batch transaction right away") -- then
// flushes scope to the engine (eng.Apply(scope)), then resets scope to a
// fresh WriteScope for whatever this batch does next. A batch is documented
// to support being committed mid-delegate and continuing to receive more
// operations afterward (dawgs' pg batch implementation executes each
// buffered operation immediately on the connection rather than inside one
// long-lived database transaction, which is what actually makes "commit,
// then keep writing" work at the pg level for a batch in a way it is not
// documented, or verified, to for a plain WriteTransaction -- see driver.go's
// BatchOperation doc for the verified source-level detail), so a caller
// relying on that to make an early chunk of a large batch visible needs the
// engine's replica brought up to date at that same moment, not held back
// until Driver.BatchOperation's own Apply call after the whole batch
// delegate returns. observingTransaction.Commit (write_observer.go above)
// overrides Commit for the same "don't leave a flush invisible" reason,
// without relying on -- or needing -- that same continue-after-commit
// guarantee.
//
// The inner Commit running before Apply is load-bearing, not cosmetic, for
// exactly the same reason as observingTransaction.Commit (see its doc):
// Apply's read-back queries PostgreSQL on the pool, not through this batch's
// own connection, so anything still buffered (not yet flushed because
// batchWriteSize's threshold was never reached) would not exist in
// PostgreSQL at all if Apply ran first -- the inner Commit's own tryFlush is
// what makes it exist before read-back goes looking for it.
//
// Apply runs unconditionally, even when the inner Commit returns an error,
// for the same "read-back reads whatever is actually there" reasoning
// Driver.BatchOperation's own always-apply choice documents: whatever
// chunks did flush (including everything tryFlush(0) just pushed through
// above) are already durable and worth reflecting, and a key that never
// landed simply reads back as it already was.
//
// Driver.BatchOperation still calls Apply once more after the delegate
// returns (driver.go), reporting whatever scope accumulated since this
// Commit call (or the whole batch, if Commit was never called mid-delegate)
// -- reading b.scope's current value at that point, which by then may be a
// different *WriteScope than the one this method reset it to, exactly as
// intended.
func (b *observingBatch) Commit() error {
	err := b.Batch.Commit()
	b.eng.Apply(applyContext(b.ctx), b.scope)
	b.scope = engine.NewWriteScope()
	return err
}

// parseCypherFrontend is cypherMutates' parsing step, factored out into a
// package-level var purely as a test seam: write_observer_test.go's
// TestCypherMutatesRecoversFromPanicByAssumingMutation stubs it to panic
// unconditionally, which is the only reliable way to exercise cypherMutates'
// recover path below -- finding real Cypher text that is actually known to
// crash the dawgs frontend is not a dependency this test wants to take on.
// Production code (Open, indirectly, via observingTransaction.Query) always
// runs with this default value; nothing else in this package ever reassigns
// it.
var parseCypherFrontend = func(text string) (*cypher.RegularQuery, error) {
	return frontend.ParseCypher(frontend.DefaultCypherContext(), text)
}

// cypherMutates parses text via parseCypherFrontend -- the same dawgs Cypher
// frontend package (github.com/specterops/dawgs/cypher/frontend) the engine's
// own Cypher interpreter uses (internal/engine/interpret's Plan, and
// engine.TryCypher itself) -- and reports whether its AST contains any
// updating clause (CREATE, SET,
// REMOVE, DELETE, or MERGE) anywhere in the query body -- in the top-level
// SinglePartQuery, or in any part of a MultiPartQuery (a query with one or
// more WITH boundaries). A parse failure, or a panic from the frontend
// itself (see the recover below), reports true: this is the same
// conservative default Driver.Run's own ChangeSet fallback record gives its
// raw-Cypher callers (driver.go), and text that fails to parse -- or
// crashes parsing -- here is exactly as unknown to this package as it would
// be to whatever eventually rejects it downstream.
//
// This deliberately does not attempt to recognize which specific kinds an
// updating clause touches (unlike edgeKindsFromCriteria above, which does,
// for the one delete shape narrow enough to be worth the effort) -- a
// mutating Cypher statement can create, relabel, or delete nodes and edges
// of arbitrary kinds named anywhere in its pattern or SET/REMOVE items, and
// nothing here attempts to walk that out. observingTransaction.Query calls
// this and records a ChangeSet fallback on true.
//
// mutates is a NAMED return, not a plain bool, and this matters: an unnamed
// return would still let the deferred recover below run on a panic, but the
// function would then hand back whatever the interrupted `return` statement
// was about to produce -- for a bare panic with no preceding return, that is
// bool's zero value, false ("does not mutate"), the exact opposite of the
// conservative answer this doc has always promised. Naming the return and
// assigning mutates = true from inside the deferred recover is what makes a
// panic actually report true instead of silently reporting false.
func cypherMutates(text string) (mutates bool) {
	defer func() {
		// The dawgs frontend is not documented to never panic on malformed
		// input; a recover here keeps a parser surprise from crashing the
		// write path it would otherwise run underneath, converting it into
		// the same conservative "assume it mutates" answer a parse error
		// already gets -- see mutates' own doc above for why this only works
		// because the return value is named.
		if recover() != nil {
			mutates = true
		}
	}()

	regularQuery, err := parseCypherFrontend(text)
	if err != nil || regularQuery == nil || regularQuery.SingleQuery == nil {
		return true
	}

	return singleQueryMutates(regularQuery.SingleQuery)
}

// singleQueryMutates reports whether sq's SinglePartQuery, or any part of
// its MultiPartQuery, carries at least one updating clause. Every clause the
// dawgs Cypher grammar accepts into a SinglePartQuery/MultiPartQueryPart's
// UpdatingClauses slice is a *cypher.UpdatingClause wrapping exactly one of
// *cypher.Create, *cypher.Set, *cypher.Remove, *cypher.Delete, or
// *cypher.Merge (see dawgs' cypher/frontend/query.go, which only ever
// constructs one via NewUpdatingClause before appending it) -- so a
// non-empty UpdatingClauses slice, on its own, already answers "does this
// query mutate" without needing to inspect which of the five clause types
// each element wraps.
func singleQueryMutates(sq *cypher.SingleQuery) bool {
	if sq.SinglePartQuery != nil && len(sq.SinglePartQuery.UpdatingClauses) > 0 {
		return true
	}

	multiPart := sq.MultiPartQuery
	if multiPart == nil {
		return false
	}

	for _, part := range multiPart.Parts {
		if part != nil && len(part.UpdatingClauses) > 0 {
			return true
		}
	}

	return multiPart.SinglePartQuery != nil && len(multiPart.SinglePartQuery.UpdatingClauses) > 0
}
