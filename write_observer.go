// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

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
// caller was always going to make -- which node and edge kinds a write
// touched, instead of the all-or-nothing "something changed" a bare
// engine.NoteWrite(nil) reports. Every override records onto scope before
// (or, for Query, without knowing whether it needs to) delegating to the
// inner transaction; every call this file does not override (UpdateRelationship,
// GraphQueryMemoryLimit) is promoted straight through by embedding,
// unchanged. Commit IS overridden, below, for its own reason.
//
// UpdateRelationship is deliberately not overridden: a graph.Relationship's
// Kind is fixed at creation (relationships.go has no AddedKind/DeletedKind
// concept the way graph.Node does), so updating one only ever changes
// properties -- nothing scope needs to know about.
//
// The zero value is not useful; construct one with a non-nil scope. scope is
// shared with every observingNodeQuery/observingRelationshipQuery this
// transaction hands out (Nodes/Relationships below) and with the
// observingTransaction WithGraph returns, so every write reachable from one
// WriteTransaction call accumulates onto the one scope Driver.
// WriteTransaction hands to engine.NoteWrite once the call succeeds.
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
}

// wrote reports whether this transaction has recorded any write onto scope
// since Driver.WriteTransaction (driver.go) began it -- equivalently,
// whether scope is non-empty (engine.WriteScope.Empty's doc: no kinds named,
// no deletions or upserts recorded, and neither TouchAllNodes nor
// TouchAllEdges called).
//
// Once wrote() is true, PostgreSQL -- never this package's in-memory engine
// -- is the only source of truth for any further read run against this same
// transaction: the engine's snapshot is only ever brought up to date by the
// NoteWrite call Driver.WriteTransaction (or, for a mid-transaction commit,
// Commit below) makes once a write is known to have landed, so by
// construction it cannot yet reflect a write this same transaction is still
// in the middle of. See the type doc above for why nothing on this type's
// read path actually consults wrote() today, and for exactly which future
// change would need to.
func (t *observingTransaction) wrote() bool {
	return !t.scope.Empty()
}

// CreateNode touches every kind the new node is created with -- a brand new
// node's Kinds is exactly the set of kinds that comes into existence -- then
// delegates, and, once the delegate reports success, records the new
// node's own database id (only ever known from its return value, which
// this method used to discard) as a ChangeSet read-back key.
func (t *observingTransaction) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	t.scope.TouchNodeKinds(kinds)
	node, err := t.Transaction.CreateNode(properties, kinds...)
	if err == nil && node != nil {
		t.scope.Changes().RecordNodeID(node.ID)
	}
	return node, err
}

// UpdateNode touches node's AddedKinds and DeletedKinds -- the label delta
// this call actually applies -- when either is non-empty, and touches
// nothing otherwise: UpdateNode never creates (see graph.Transaction's own
// doc, "will not create missing Node entries"), so a node with neither
// AddedKinds nor DeletedKinds set is a pure property update, which no kind
// mark needs to reflect. node's base Kinds field is deliberately not touched
// here -- unlike observingBatch.UpdateNodeBy's upsert case below, this call
// can only ever be touching a node the graph already had, so Kinds carries
// no information the delta doesn't already capture.
//
// This is verified, not assumed, against dawgs' actual pg driver: the
// tx-level UpdateNode this call delegates to (drivers/pg/transaction.go)
// reads only node.AddedKinds and node.DeletedKinds when building its
// AddKinds/DeleteKinds update statements -- it never references node.Kinds.
// So the delta this method touches is exactly, not just approximately, the
// database's real kind-membership effect; contrast observingBatch.
// UpdateNodes below, whose analogous-looking pg batch write path does read
// node.Kinds and therefore needs an additional UpsertNodeKinds call (see
// touchNodeKindDelta's doc for the side-by-side comparison).
func (t *observingTransaction) UpdateNode(node *graph.Node) error {
	touchNodeKindDelta(t.scope, node)
	if node != nil {
		t.scope.Changes().RecordNodeID(node.ID)
	}
	return t.Transaction.UpdateNode(node)
}

// CreateRelationshipByIDs touches kind -- the only kind the new relationship
// can carry -- then delegates, and, once the delegate reports success,
// records the new relationship's own database id (only ever known from its
// return value, which this method used to discard) as a ChangeSet
// read-back key.
func (t *observingTransaction) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	t.scope.TouchEdgeKind(kind)
	rel, err := t.Transaction.CreateRelationshipByIDs(startNodeID, endNodeID, kind, properties)
	if err == nil && rel != nil {
		t.scope.Changes().RecordEdgeID(rel.ID)
	}
	return rel, err
}

// Nodes returns an observingNodeQuery wrapping the inner transaction's own
// NodeQuery, so a subsequent Delete() on it can still reach scope. The
// returned query is never engine-serving-capable today (it wraps the raw
// NodeQuery straight from the driver's transaction, not a
// recordingNodeQuery) -- see the type doc's composition-point note; a
// future change that gives it one must decline whenever wrote() is true.
func (t *observingTransaction) Nodes() graph.NodeQuery {
	return &observingNodeQuery{NodeQuery: t.Transaction.Nodes(), scope: t.scope}
}

// Relationships returns an observingRelationshipQuery wrapping the inner
// transaction's own RelationshipQuery, so a subsequent Delete() on it has a
// chance to scope more narrowly than TouchAllEdges (see
// observingRelationshipQuery.Delete's doc). See Nodes' doc immediately above
// for the identical composition-point note.
func (t *observingTransaction) Relationships() graph.RelationshipQuery {
	return &observingRelationshipQuery{RelationshipQuery: t.Transaction.Relationships(), scope: t.scope}
}

// Query sniffs query for a Cypher updating clause (cypherMutates) and, if
// found, marks the whole scope dirty -- raw Cypher text can do anything a
// CREATE/SET/REMOVE/DELETE/MERGE clause allows, and nothing short of a full
// recognizer (out of scope here) could say which kinds narrower than "all"
// it actually touched. query always runs against the inner transaction
// regardless of what cypherMutates reports; the sniff only ever adds a scope
// mark, never blocks or rewrites the call itself.
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
		t.scope.TouchAll()
		t.scope.Changes().RecordFallback("Query: mutating Cypher escapes changelog tracking")
	}
	return t.Transaction.Query(query, parameters)
}

// Raw always marks the whole scope dirty: unlike Query, whose text is at
// least Cypher that cypherMutates can attempt to classify, Raw's query is
// driver-specific (SQL, for the PostgreSQL backend this driver wraps) and
// this package has no way to parse it at all -- the only sound answer is the
// same conservative one NoteWrite gives a nil scope.
func (t *observingTransaction) Raw(query string, parameters map[string]any) graph.Result {
	t.scope.TouchAll()
	t.scope.Changes().RecordFallback("Raw: driver-specific query escapes changelog tracking")
	return t.Transaction.Raw(query, parameters)
}

// WithGraph marks the whole scope dirty -- a transaction retargeted at a
// non-default graph is outside anything this package's kind-scoped tracking
// reasons about, mirroring wrappedTransaction.WithGraph's own "declined"
// treatment on the read side -- and returns a fresh observingTransaction
// wrapping the inner WithGraph's result, sharing the *same* scope (so writes
// against the retargeted graph still land in the one WriteScope this
// WriteTransaction call will eventually report) and the same eng (so Commit
// still works correctly on the retargeted wrapper).
func (t *observingTransaction) WithGraph(graphSchema graph.Graph) graph.Transaction {
	t.scope.TouchAll()
	t.scope.Changes().RecordFallback("WithGraph: graph retarget escapes changelog tracking")
	return &observingTransaction{Transaction: t.Transaction.WithGraph(graphSchema), scope: t.scope, eng: t.eng}
}

// Commit flushes the accumulated scope to the engine (eng.NoteWrite), then
// resets scope to a fresh, empty WriteScope, before delegating to the inner
// transaction's own Commit. Under the pinned dawgs pg driver, a delegate that
// calls tx.Commit() mid-transaction will cause the outer WriteTransaction's
// final Commit to return ErrTxClosed; writes persist, and this override
// ensures invalidation is recorded at the commit point. This override is
// therefore defensive today: it guards against the mid-transaction commit
// scenario, matching observingBatch.Commit's pattern, though that scenario's
// actual feasibility under the pg driver remains unverified (Driver.
// WriteTransaction still calls NoteWrite once more after the delegate returns,
// reading this transaction's *current* scope value at that point -- which by
// then may be a different *WriteScope than the one this method reset it to
// here, exactly as intended).
func (t *observingTransaction) Commit() error {
	t.eng.NoteWrite(t.scope)
	t.scope = engine.NewWriteScope()
	return t.Transaction.Commit()
}

// touchNodeKindDelta marks scope with node's AddedKinds and DeletedKinds
// when either is non-empty, and does nothing otherwise. This is the shared
// rule behind observingTransaction.UpdateNode and observingBatch.
// UpdateNodes -- both calls that only ever update a node already known to
// exist (contrast observingBatch.UpdateNodeBy, an upsert that may instead be
// creating the node, where the base Kinds field must be touched too; see
// that method's doc).
//
// For observingTransaction.UpdateNode this delta IS the whole story: dawgs'
// pg driver's tx-level UpdateNode (drivers/pg/transaction.go) builds its
// update statement solely from query.AddKinds(query.Node(), node.AddedKinds)
// and query.DeleteKinds(query.Node(), node.DeletedKinds) when each is
// non-empty -- it never reads node.Kinds at all -- so there is no
// information a Kinds-only touch could add. This is a verified fact about
// the real driver, not an assumption; see observingTransaction.UpdateNode's
// own doc.
//
// For observingBatch.UpdateNodes, by contrast, this delta is only PART of
// the story: dawgs' pg driver's batch UpdateNodes additionally unions the
// node's full Kinds field into the database row regardless of AddedKinds
// (NodeUpdateParameters.Append/FormatNodesUpdate in drivers/pg/batch.go and
// drivers/pg/query/format.go). observingBatch.UpdateNodes therefore calls
// this AND separately calls scope.UpsertNodeKinds(node.ID, node.Kinds) --
// see that method's own doc, and engine.WriteScope.UpsertNodeKinds' doc, for
// why a snapshot-scoped set-difference there is both necessary and
// sufficient to close the gap this function alone would leave open for that
// one caller.
func touchNodeKindDelta(scope *engine.WriteScope, node *graph.Node) {
	if len(node.AddedKinds) == 0 && len(node.DeletedKinds) == 0 {
		return
	}
	scope.TouchNodeKinds(node.AddedKinds)
	scope.TouchNodeKinds(node.DeletedKinds)
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

// Delete marks the whole scope dirty for both nodes and edges before
// delegating: deleting a node cascades to every edge incident to it (the
// same reasoning DeleteNodeID's resolution path in engine/marks.go
// documents for the transaction-level delete-by-id case), and this query's
// criteria could match nodes of any kind the caller didn't explicitly name
// -- there is no narrower sound answer without re-deriving exactly which
// nodes and edges the query's criteria matched, which nothing here attempts.
// This TouchAllNodes/TouchAllEdges marking is unconditional and unchanged by
// the ChangeSet recording added below: recognizing an InIDs-shaped criteria
// only ever adds a narrower changelog entry alongside the same conservative
// mark, never replaces it (a kind-only node delete goes through
// Driver.DeleteNodesByKinds instead of this query -- see driver.go -- so
// this method's own recognizer only ever needs to look for InIDs, not a
// kind matcher).
func (q *observingNodeQuery) Delete() error {
	q.scope.TouchAllNodes()
	q.scope.TouchAllEdges()
	if ids, ok := nodeIDsFromCriteria(q.criteria); ok {
		for _, id := range ids {
			q.scope.Changes().RecordNodeID(id)
		}
	} else {
		q.scope.Changes().RecordFallback("NodeQuery.Delete: unrecognized criteria")
	}
	return q.NodeQuery.Delete()
}

// Update touches TouchAllNodes unconditionally before delegating: unlike
// this file's other overrides, a property-only update was previously left
// entirely unobserved (NodeQuery.Update only ever sets properties, never
// labels, so the ORIGINAL kind-scoped marks design deliberately left it
// promoted -- see this type's pre-ChangeSet doc history). That gating
// choice was sound for the kind-scoped freshness marks alone: a property
// change carries no kind information for them to act on. But a
// write-through applier building a changelog from these overrides cannot
// tolerate a write it never even observes, so this override both records a
// ChangeSet entry (a recognized InIDs target list, or a fallback) AND
// conservatively marks TouchAllNodes -- mirroring Delete's own
// unconditional, no-narrower-safe-answer marking above -- rather than
// leaving the pre-ChangeSet "touch nothing" behavior in place. This is a
// deliberate, narrow behavior change scoped to exactly this newly-added
// override; every other Touch*/Delete* call site in this file is
// unchanged.
func (q *observingNodeQuery) Update(properties *graph.Properties) error {
	q.scope.TouchAllNodes()
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

	// criteria accumulates every Filter/Filterf argument, in call order.
	// Delete only attempts to recognize a kind-scoped shape when exactly one
	// criteria was recorded -- the shape edgeKindsFromCriteria (and its
	// eventual Task 4 replacement, recognize.FromRelCriteria) recognizes is
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
// which shapes are recognized and why ignoring extra conjuncts stays sound)
// before delegating to the inner query. The recognized branch's ChangeSet
// entry is RecordDeleteRelationshipsByKinds(kinds), not an enumerated edge
// id list: relationshipDeleteScope's own recognized shape is a kind
// matcher, not an InIDs target list, so "delete every relationship of
// these kinds" is the operation this delete actually performs, and the
// one the applier should replay -- an id list captured before the delete
// ran could go stale by the time the applier reads it back. The
// unrecognized branch falls back, same as every other unrecognized
// criteria in this file.
func (r *observingRelationshipQuery) Delete() error {
	if kinds, touchAll := relationshipDeleteScope(r.criteria); touchAll {
		r.scope.TouchAllEdges()
		r.scope.Changes().RecordFallback("RelationshipQuery.Delete: unrecognized criteria")
	} else {
		r.scope.TouchEdgeKinds(kinds)
		r.scope.Changes().RecordDeleteRelationshipsByKinds(kinds)
	}
	return r.RelationshipQuery.Delete()
}

// Update touches TouchAllEdges unconditionally before delegating, and
// records either a recognized InIDs target list or a fallback -- the
// RelationshipQuery half of observingNodeQuery.Update's identical
// reasoning; see its doc for the full explanation of why this newly-added
// override marks TouchAllEdges where the pre-ChangeSet design left
// property-only updates unobserved.
func (r *observingRelationshipQuery) Update(properties *graph.Properties) error {
	r.scope.TouchAllEdges()
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
// Delete() call should mark, given every criteria its caller filtered by.
// When exactly one criteria was recorded and edgeKindsFromCriteria
// recognizes it with a non-empty result, kinds is that result and touchAll
// is false: deleting a query narrowed to specific relationship kinds can
// only ever remove edges of those kinds, no matter what else the query's
// (ignored) other conjuncts narrow the match by -- a subset of kind K's
// edges is still only kind K. Every other case -- zero or more than one
// criteria, an unrecognized shape, or a recognized KindMatcher whose Kinds
// came back empty (which means "matches every kind", the opposite of a
// narrow scope, per the KindMatcher/PathQuery.EdgeKinds convention
// documented on recognize.PathQuery) -- reports touchAll instead.
func relationshipDeleteScope(criteria []graph.Criteria) (kinds graph.Kinds, touchAll bool) {
	if len(criteria) == 1 {
		if ks, ok := edgeKindsFromCriteria(criteria[0]); ok && len(ks) > 0 {
			return ks, false
		}
	}
	return nil, true
}

// edgeKindsFromCriteria is a minimal stand-in for Task 4's
// recognize.FromRelCriteria (not written as of this file): it recognizes
// just enough of a relationship-delete's criteria to scope the delete
// soundly, without depending on a recognizer package that doesn't exist
// yet. Recognized shapes are a bare *cypher.KindMatcher over the
// relationship variable "r" (what dawgs' query.Kind(query.Relationship(),
// k)/query.KindIn(query.Relationship(), ks...) builds), or a
// *cypher.Conjunction containing one or more such KindMatchers among any
// number of other conjuncts. Every other conjunct in a Conjunction is
// deliberately ignored: a delete additionally narrowed by, say, a property
// filter or an endpoint id still only removes a subset of the named kinds'
// edges, which is exactly what the returned kinds already describe -- an
// ignored conjunct can only make the query's real effect a subset of what
// this function reports, never a superset, so ignoring it can never make
// the reported scope unsound. Multiple KindMatchers union their kinds, for
// the same reason: a delete matching kind K1 or K2 still only touches K1
// and K2's edges.
//
// Anything else -- a nil criteria, one that isn't a Conjunction or bare
// KindMatcher, a KindMatcher over any variable but "r", or a Conjunction
// containing no relationship KindMatcher at all -- reports ok=false.
func edgeKindsFromCriteria(criteria graph.Criteria) (graph.Kinds, bool) {
	switch typed := criteria.(type) {
	case *cypher.KindMatcher:
		return relationshipKindMatcherKinds(typed)

	case *cypher.Conjunction:
		if typed == nil {
			return nil, false
		}

		var kinds graph.Kinds
		found := false
		for _, expr := range typed.Expressions {
			km, isKindMatcher := expr.(*cypher.KindMatcher)
			if !isKindMatcher {
				continue
			}
			if ks, matched := relationshipKindMatcherKinds(km); matched {
				kinds = append(kinds, ks...)
				found = true
			}
		}
		return kinds, found

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
// surface stays at zero until Task 4 gives it a real FromRelCriteria to call
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
// (driver.go) can learn which node and edge kinds a batch touched, the same
// way observingTransaction does for WriteTransaction. graph.Batch has no
// method this file leaves promoted unmodified -- every one of its methods
// either changes graph data (and so needs a scope mark) or is Commit, which
// this file overrides for its own reason (see Commit's doc) -- so, unlike
// the other wrappers in this file, embedding graph.Batch here exists only to
// satisfy the interface's method set at compile time, not to promote
// anything.
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
}

// wrote reports whether this batch has recorded any write onto scope since
// it began (Driver.BatchOperation, driver.go) or since the last Commit
// flush reset it (Commit's own doc) -- equivalently, whether scope is
// non-empty. See observingTransaction.wrote's doc for the full reasoning:
// identical here, substituting "batch" for "transaction" throughout.
func (b *observingBatch) wrote() bool {
	return !b.scope.Empty()
}

// CreateNode touches every kind the new node carries, then delegates.
func (b *observingBatch) CreateNode(node *graph.Node) error {
	b.scope.TouchNodeKinds(node.Kinds)
	return b.Batch.CreateNode(node)
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
// Each input node's Kinds are touched before delegating (mirroring
// CreateNode's own before-delegate touch), and, once the delegate reports
// success, every returned id is recorded onto the ChangeSet as a read-back
// key (RecordNodeID) -- the ids are documented to align index-for-index
// with nodes (graph.NodeBatchCreator's own doc: "returns generated IDs in
// input order"), but this only needs the ids themselves, not that
// alignment, so no attempt is made to pair a specific id back to a
// specific input node.
func (b *observingBatch) CreateNodes(nodes []*graph.Node) ([]graph.ID, error) {
	creator, ok := b.Batch.(graph.NodeBatchCreator)
	if !ok {
		return nil, fmt.Errorf("bloodtrail: batch %T does not support correlated bulk node creation", b.Batch)
	}

	for _, node := range nodes {
		if node != nil {
			b.scope.TouchNodeKinds(node.Kinds)
		}
	}

	ids, err := creator.CreateNodes(nodes)
	if err != nil {
		return ids, err
	}

	for _, id := range ids {
		b.scope.Changes().RecordNodeID(id)
	}
	return ids, nil
}

// DeleteNode records id for scope's own resolution against the engine's
// current snapshot (WriteScope.DeleteNodeID's doc): unlike this file's other
// delete paths, a batch delete-by-id names no kinds at all, so there is
// nothing for this method itself to touch directly. id is also recorded
// onto the ChangeSet as a read-back key: the applier re-reads it from
// PostgreSQL the same way every other RecordNodeID caller's target is
// re-read, finding it gone and applying the delete.
func (b *observingBatch) DeleteNode(id graph.ID) error {
	b.scope.DeleteNodeID(id)
	b.scope.Changes().RecordNodeID(id)
	return b.Batch.DeleteNode(id)
}

// Nodes returns an observingNodeQuery wrapping the inner batch's own
// NodeQuery, exactly as observingTransaction.Nodes does -- including the
// same composition-point note: the returned query is never
// engine-serving-capable today, and a future change that gives it one must
// decline whenever wrote() is true.
func (b *observingBatch) Nodes() graph.NodeQuery {
	return &observingNodeQuery{NodeQuery: b.Batch.Nodes(), scope: b.scope}
}

// Relationships returns an observingRelationshipQuery wrapping the inner
// batch's own RelationshipQuery, exactly as observingTransaction.
// Relationships does. See Nodes' doc immediately above for the identical
// composition-point note.
func (b *observingBatch) Relationships() graph.RelationshipQuery {
	return &observingRelationshipQuery{RelationshipQuery: b.Batch.Relationships(), scope: b.scope}
}

// UpdateNodeBy touches update.Node's Kinds, AddedKinds, and DeletedKinds
// unconditionally, then delegates. Unlike observingTransaction.UpdateNode,
// this call is an upsert (graph.Batch's own doc on UpdateNodeBy: "in the
// case where the node does not yet exist, created") -- if it creates rather
// than updates, update.Node.Kinds is the new node's entire kind set, not a
// delta, and there is no way from here to tell which case actually happened,
// so Kinds must be touched every time alongside whatever AddedKinds/
// DeletedKinds delta was also given.
//
// Verified sound against dawgs' actual pg batch write path (not just assumed
// safe by analogy): UpdateNodeBy buffers into nodeUpdateByBuffer, flushed via
// flushNodeUpsertBatch -> NodeUpsertParameters.Append (drivers/pg/batch.go),
// which reads update.Node.Kinds ONLY -- never AddedKinds/DeletedKinds -- into
// the upsert statement's excluded.kind_ids, and FormatNodeUpsert's SQL
// (drivers/pg/query/format.go) unions it into the row's kind_ids on conflict
// ("kind_ids = uniq(sort(n.kind_ids || excluded.kind_ids))"). Touching
// update.Node.Kinds unconditionally, as this method already does, is
// therefore not merely a safe superset of that effect -- it is an exact
// match. The additional AddedKinds/DeletedKinds touches are conservative
// extras this call has always made (neither field reaches the database
// through this write path at all) and cost nothing to keep, so this method's
// body is left as-is; contrast observingBatch.UpdateNodes below, whose
// distinct pg batch write path required an actual code change to stay sound.
func (b *observingBatch) UpdateNodeBy(update graph.NodeUpdate) error {
	if update.Node != nil {
		b.scope.TouchNodeKinds(update.Node.Kinds)
		b.scope.TouchNodeKinds(update.Node.AddedKinds)
		b.scope.TouchNodeKinds(update.Node.DeletedKinds)
	}
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
	scope.Changes().RecordFallback("unrecognized node upsert identity")
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
	if node == nil || node.Properties == nil {
		return "", false
	}
	if len(identityProperties) != 1 || identityProperties[0] != "objectid" {
		return "", false
	}

	value, err := node.Properties.Get("objectid").String()
	if err != nil {
		return "", false
	}
	return value, true
}

// UpdateNodes touches each node's AddedKinds/DeletedKinds delta via
// touchNodeKindDelta, AND separately calls scope.UpsertNodeKinds(node.ID,
// node.Kinds) for every node with a non-empty Kinds field.
//
// graph.Batch's own doc describes this call as updating existing nodes "by
// ID", not upserting -- which might suggest, as it correctly does for
// observingTransaction.UpdateNode's identical-looking case, that the
// AddedKinds/DeletedKinds delta is the whole story and Kinds itself carries
// no extra information. Verified against dawgs' actual pg batch write path,
// it is not: UpdateNodes buffers into nodeUpdateBuffer, flushed via
// flushNodeUpdateBatch -> NodeUpdateParameters.Append (drivers/pg/batch.go),
// which reads node.Kinds -- the node's FULL kind set, not node.AddedKinds --
// into the update statement's "added_kinds" SQL parameter, and
// FormatNodesUpdate's SQL (drivers/pg/query/format.go) unions it into
// kind_ids unconditionally: "kind_ids = uniq(sort(kind_ids - u.deleted_kinds
// || u.added_kinds))". So a caller that sets Kinds without also calling
// node.AddKinds(...) -- nothing in graph.Node's API or graph.Batch's
// documented contract forbids this -- changes kind membership in the
// database with zero information in AddedKinds/DeletedKinds for
// touchNodeKindDelta to see. (The batch's largeUpdate path, taken instead of
// flushNodeUpdateBatch above LargeNodeUpdateThreshold nodes, reads node.Kinds
// the same way via LargeNodeUpdateRows.Append/FormatMergeNodeLargeUpdate, so
// this call's per-node marking below covers both without needing to know
// which path a given UpdateNodes call will take.)
//
// The UpsertNodeKinds call does not naively mark every node.Kinds entry
// dirty -- doing so would dirty hot, already-clean kinds (User, Computer,
// ...) on every tagging-style UpdateNodes call that sets Kinds alongside
// AddedKinds (the shape every caller in this codebase uses today), defeating
// this milestone's whole design goal of narrow, kind-scoped invalidation.
// Instead resolution is deferred to NoteWrite time, against the engine's
// current snapshot, so only kinds the union could have genuinely added get
// marked; see engine.WriteScope.UpsertNodeKinds' doc for the full soundness
// argument.
func (b *observingBatch) UpdateNodes(nodes []*graph.Node) error {
	for _, node := range nodes {
		if node != nil {
			touchNodeKindDelta(b.scope, node)
			if len(node.Kinds) > 0 {
				b.scope.UpsertNodeKinds(node.ID, node.Kinds)
			}
			b.scope.Changes().RecordNodeID(node.ID)
		}
	}
	return b.Batch.UpdateNodes(nodes)
}

// CreateRelationship touches relationship's Kind, then delegates.
func (b *observingBatch) CreateRelationship(relationship *graph.Relationship) error {
	if relationship != nil {
		b.scope.TouchEdgeKind(relationship.Kind)
		b.scope.Changes().RecordEdgeTriple(relationship.StartID, relationship.EndID, relationship.Kind)
	}
	return b.Batch.CreateRelationship(relationship)
}

// CreateRelationshipByIDs touches kind, then delegates. graph.Batch's own
// CreateRelationshipByIDs is deprecated in favor of CreateRelationship, but
// this wrapper calls it anyway rather than rewriting the call into a
// CreateRelationship of its own: this method exists purely to observe and
// pass through whatever the caller invoked, and the two methods are not
// documented to behave identically for every graph.Batch implementation
// (graph.Batch's own CreateRelationship carries a TODO about incorrect
// upsert-on-conflict behavior that CreateRelationshipByIDs may or may not
// share) -- silently rerouting the call here could change behavior for a
// Batch implementation this package has no way to know about.
//
//nolint:staticcheck // SA1019: deliberate passthrough of a deprecated call, see doc above.
func (b *observingBatch) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	b.scope.TouchEdgeKind(kind)
	b.scope.Changes().RecordEdgeTriple(startNodeID, endNodeID, kind)
	return b.Batch.CreateRelationshipByIDs(startNodeID, endNodeID, kind, properties)
}

// DeleteRelationship records id for scope's own resolution against the
// engine's current snapshot (WriteScope.DeleteEdgeID's doc), records the
// same id onto the ChangeSet as a read-back key (mirroring DeleteNode's
// identical reasoning), then delegates.
func (b *observingBatch) DeleteRelationship(id graph.ID) error {
	b.scope.DeleteEdgeID(id)
	b.scope.Changes().RecordEdgeID(id)
	return b.Batch.DeleteRelationship(id)
}

// UpdateRelationshipBy touches update.Relationship's Kind unconditionally,
// then delegates. Like UpdateNodeBy above, this is an upsert (graph.Batch's
// own doc: "in the case where the relationship does not yet exist,
// created"), so Kind must be touched regardless of whether this call turns
// out to update or create -- a relationship's Kind is fixed for its
// lifetime (graph.Relationship has no Added/DeletedKind concept the way
// graph.Node does), so there is no narrower delta to touch instead.
func (b *observingBatch) UpdateRelationshipBy(update graph.RelationshipUpdate) error {
	if update.Relationship != nil {
		b.scope.TouchEdgeKind(update.Relationship.Kind)
	}
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
		scope.Changes().RecordFallback("unrecognized relationship upsert identity")
		return
	}

	scope.Changes().RecordEdgeTripleByObjectID(startOID, endOID, update.Relationship.Kind)
	scope.Changes().RecordNodeObjectID(startOID)
	scope.Changes().RecordNodeObjectID(endOID)
}

// WithGraph marks the whole scope dirty and returns a fresh observingBatch
// wrapping the inner WithGraph's result, sharing the same scope -- mirroring
// observingTransaction.WithGraph's identical reasoning.
func (b *observingBatch) WithGraph(graphSchema graph.Graph) graph.Batch {
	b.scope.TouchAll()
	b.scope.Changes().RecordFallback("WithGraph: graph retarget escapes changelog tracking")
	return &observingBatch{Batch: b.Batch.WithGraph(graphSchema), scope: b.scope, eng: b.eng}
}

// Commit flushes scope to the engine immediately -- eng.NoteWrite(scope),
// then a fresh WriteScope for whatever this batch does next -- before
// delegating to the inner batch's own Commit. A batch is documented to
// support being committed mid-delegate and continuing to receive more
// operations afterward (graph.Batch's own doc on Commit: "calls to commit
// this batch transaction right away"; dawgs' pg batch implementation
// executes each buffered operation immediately on the connection rather
// than inside one long-lived database transaction, which is what actually
// makes "commit, then keep writing" work at the pg level for a batch in a
// way it is not documented, or verified, to for a plain WriteTransaction),
// so a caller relying on that to make an early chunk of a large batch
// visible needs the engine's snapshot invalidated at that same moment, not
// held back until Driver.BatchOperation's own NoteWrite call after the
// whole batch delegate returns. observingTransaction.Commit
// (write_observer.go above) overrides Commit for the same
// "don't leave a flush invisible" reason, without relying on -- or needing
// -- that same continue-after-commit guarantee.
//
// Driver.BatchOperation still calls NoteWrite once more after the
// delegate returns (driver.go), reporting whatever scope accumulated since
// this Commit call (or the whole batch, if Commit was never called
// mid-delegate) -- reading b.scope's current value at that point, which by
// then may be a different *WriteScope than the one this method reset it to,
// exactly as intended.
func (b *observingBatch) Commit() error {
	b.eng.NoteWrite(b.scope)
	b.scope = engine.NewWriteScope()
	return b.Batch.Commit()
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
// conservative default NoteWrite(nil) has always meant for Run's raw-Cypher
// callers (driver.go), and text that fails to parse -- or crashes parsing --
// here is exactly as unknown to this package as it would be to whatever
// eventually rejects it downstream.
//
// This deliberately does not attempt to recognize which specific kinds an
// updating clause touches (unlike edgeKindsFromCriteria above, which does,
// for the one delete shape narrow enough to be worth the effort) -- a
// mutating Cypher statement can create, relabel, or delete nodes and edges
// of arbitrary kinds named anywhere in its pattern or SET/REMOVE items, and
// nothing here attempts to walk that out. observingTransaction.Query calls
// this and marks the whole scope dirty on true.
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
