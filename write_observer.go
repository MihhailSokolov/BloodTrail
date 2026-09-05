// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// observingTransaction wraps a live graph.Transaction so that Driver.
// WriteTransaction (driver.go) can learn -- from the very same calls the
// caller was always going to make -- which node and edge kinds a write
// touched, instead of the all-or-nothing "something changed" a bare
// engine.NoteWrite(nil) reports. Every override records onto scope before
// (or, for Query, without knowing whether it needs to) delegating to the
// inner transaction; every call this file does not override (Commit,
// UpdateRelationship, GraphQueryMemoryLimit) is promoted straight through by
// embedding, unchanged.
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
type observingTransaction struct {
	graph.Transaction

	scope *engine.WriteScope
}

// CreateNode touches every kind the new node is created with -- a brand new
// node's Kinds is exactly the set of kinds that comes into existence -- then
// delegates.
func (t *observingTransaction) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	t.scope.TouchNodeKinds(kinds)
	return t.Transaction.CreateNode(properties, kinds...)
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
func (t *observingTransaction) UpdateNode(node *graph.Node) error {
	touchNodeKindDelta(t.scope, node)
	return t.Transaction.UpdateNode(node)
}

// CreateRelationshipByIDs touches kind -- the only kind the new relationship
// can carry -- then delegates.
func (t *observingTransaction) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	t.scope.TouchEdgeKind(kind)
	return t.Transaction.CreateRelationshipByIDs(startNodeID, endNodeID, kind, properties)
}

// Nodes returns an observingNodeQuery wrapping the inner transaction's own
// NodeQuery, so a subsequent Delete() on it can still reach scope.
func (t *observingTransaction) Nodes() graph.NodeQuery {
	return &observingNodeQuery{NodeQuery: t.Transaction.Nodes(), scope: t.scope}
}

// Relationships returns an observingRelationshipQuery wrapping the inner
// transaction's own RelationshipQuery, so a subsequent Delete() on it has a
// chance to scope more narrowly than TouchAllEdges (see
// observingRelationshipQuery.Delete's doc).
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
func (t *observingTransaction) Query(query string, parameters map[string]any) graph.Result {
	if cypherMutates(query) {
		t.scope.TouchAll()
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
	return t.Transaction.Raw(query, parameters)
}

// WithGraph marks the whole scope dirty -- a transaction retargeted at a
// non-default graph is outside anything this package's kind-scoped tracking
// reasons about, mirroring wrappedTransaction.WithGraph's own "declined"
// treatment on the read side -- and returns a fresh observingTransaction
// wrapping the inner WithGraph's result, sharing the *same* scope so writes
// against the retargeted graph still land in the one WriteScope this
// WriteTransaction call will eventually report.
func (t *observingTransaction) WithGraph(graphSchema graph.Graph) graph.Transaction {
	t.scope.TouchAll()
	return &observingTransaction{Transaction: t.Transaction.WithGraph(graphSchema), scope: t.scope}
}

// touchNodeKindDelta marks scope with node's AddedKinds and DeletedKinds
// when either is non-empty, and does nothing otherwise. This is the shared
// rule behind observingTransaction.UpdateNode and observingBatch.
// UpdateNodes -- both calls that only ever update a node already known to
// exist, so the label delta is the whole story; contrast observingBatch.
// UpdateNodeBy, an upsert that may instead be creating the node, where the
// base Kinds field must be touched too (see that method's doc).
func touchNodeKindDelta(scope *engine.WriteScope, node *graph.Node) {
	if len(node.AddedKinds) == 0 && len(node.DeletedKinds) == 0 {
		return
	}
	scope.TouchNodeKinds(node.AddedKinds)
	scope.TouchNodeKinds(node.DeletedKinds)
}

// observingNodeQuery wraps a live graph.NodeQuery so that Delete() can mark
// scope before delegating. graph.NodeQuery is embedded, so every method this
// file does not override (Query, Update, OrderBy, Offset, Limit, Count,
// First, Fetch, FetchIDs, FetchKinds) is promoted straight through to the
// inner query unchanged -- Update in particular is promoted deliberately:
// NodeQuery.Update only ever sets properties (graph.NodeQuery's own doc,
// "updates all candidate nodes with the given properties"), never labels, so
// there is no kind information for it to report.
//
// The zero value is not useful; construct one via observingTransaction's own
// Nodes() or observingBatch's own Nodes().
type observingNodeQuery struct {
	graph.NodeQuery

	scope *engine.WriteScope
}

// Filter delegates to the inner query and re-wraps the result so the fluent
// chain keeps flowing through observingNodeQuery -- without this, a
// subsequent .Delete() on the chain's result would reach the inner query
// directly, skipping this wrapper's scope marking entirely.
func (q *observingNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	q.NodeQuery = q.NodeQuery.Filter(criteria)
	return q
}

// Filterf is Filter's graph.CriteriaProvider-accepting equivalent; see
// Filter's doc for why re-wrapping matters.
func (q *observingNodeQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.NodeQuery {
	q.NodeQuery = q.NodeQuery.Filterf(criteriaDelegate)
	return q
}

// Delete marks the whole scope dirty for both nodes and edges before
// delegating: deleting a node cascades to every edge incident to it (the
// same reasoning DeleteNodeID's resolution path in engine/marks.go
// documents for the transaction-level delete-by-id case), and this query's
// criteria could match nodes of any kind the caller didn't explicitly name
// -- there is no narrower sound answer without re-deriving exactly which
// nodes and edges the query's criteria matched, which nothing here attempts.
func (q *observingNodeQuery) Delete() error {
	q.scope.TouchAllNodes()
	q.scope.TouchAllEdges()
	return q.NodeQuery.Delete()
}

// observingRelationshipQuery wraps a live graph.RelationshipQuery, recording
// every criteria the caller filters by (mirroring relationship_query.go's
// read-side recordingRelationshipQuery) so a subsequent Delete() has a
// chance to recognize a kind-scoped delete and mark only the kinds it
// actually affects, instead of TouchAllEdges. graph.RelationshipQuery is
// embedded, so every method this file does not override (Update, OrderBy,
// Offset, Limit, Count, First, Query, Fetch, FetchDirection, FetchIDs,
// FetchTriples, FetchKinds, FetchAllShortestPaths) is promoted straight
// through unchanged -- Update in particular is promoted deliberately, for
// the same property-only reason observingNodeQuery's doc gives.
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

// Delete marks scope via relationshipDeleteScope (see its doc for exactly
// which shapes are recognized and why ignoring extra conjuncts stays sound)
// before delegating to the inner query.
func (r *observingRelationshipQuery) Delete() error {
	if kinds, touchAll := relationshipDeleteScope(r.criteria); touchAll {
		r.scope.TouchAllEdges()
	} else {
		r.scope.TouchEdgeKinds(kinds)
	}
	return r.RelationshipQuery.Delete()
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
// Commit's doc for why a batch specifically needs this and a transaction
// does not).
type observingBatch struct {
	graph.Batch

	scope *engine.WriteScope
	eng   *engine.Engine
}

// CreateNode touches every kind the new node carries, then delegates.
func (b *observingBatch) CreateNode(node *graph.Node) error {
	b.scope.TouchNodeKinds(node.Kinds)
	return b.Batch.CreateNode(node)
}

// DeleteNode records id for scope's own resolution against the engine's
// current snapshot (WriteScope.DeleteNodeID's doc): unlike this file's other
// delete paths, a batch delete-by-id names no kinds at all, so there is
// nothing for this method itself to touch directly.
func (b *observingBatch) DeleteNode(id graph.ID) error {
	b.scope.DeleteNodeID(id)
	return b.Batch.DeleteNode(id)
}

// Nodes returns an observingNodeQuery wrapping the inner batch's own
// NodeQuery, exactly as observingTransaction.Nodes does.
func (b *observingBatch) Nodes() graph.NodeQuery {
	return &observingNodeQuery{NodeQuery: b.Batch.Nodes(), scope: b.scope}
}

// Relationships returns an observingRelationshipQuery wrapping the inner
// batch's own RelationshipQuery, exactly as observingTransaction.
// Relationships does.
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
func (b *observingBatch) UpdateNodeBy(update graph.NodeUpdate) error {
	if update.Node != nil {
		b.scope.TouchNodeKinds(update.Node.Kinds)
		b.scope.TouchNodeKinds(update.Node.AddedKinds)
		b.scope.TouchNodeKinds(update.Node.DeletedKinds)
	}
	return b.Batch.UpdateNodeBy(update)
}

// UpdateNodes touches each node's AddedKinds/DeletedKinds delta via
// touchNodeKindDelta -- the same rule observingTransaction.UpdateNode
// applies, for the same reason: graph.Batch's own doc describes this call as
// updating existing nodes "by ID", not upserting, so Kinds itself carries no
// information the delta doesn't already capture.
func (b *observingBatch) UpdateNodes(nodes []*graph.Node) error {
	for _, node := range nodes {
		if node != nil {
			touchNodeKindDelta(b.scope, node)
		}
	}
	return b.Batch.UpdateNodes(nodes)
}

// CreateRelationship touches relationship's Kind, then delegates.
func (b *observingBatch) CreateRelationship(relationship *graph.Relationship) error {
	if relationship != nil {
		b.scope.TouchEdgeKind(relationship.Kind)
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
	return b.Batch.CreateRelationshipByIDs(startNodeID, endNodeID, kind, properties)
}

// DeleteRelationship records id for scope's own resolution against the
// engine's current snapshot (WriteScope.DeleteEdgeID's doc), then delegates.
func (b *observingBatch) DeleteRelationship(id graph.ID) error {
	b.scope.DeleteEdgeID(id)
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
	return b.Batch.UpdateRelationshipBy(update)
}

// WithGraph marks the whole scope dirty and returns a fresh observingBatch
// wrapping the inner WithGraph's result, sharing the same scope -- mirroring
// observingTransaction.WithGraph's identical reasoning.
func (b *observingBatch) WithGraph(graphSchema graph.Graph) graph.Batch {
	b.scope.TouchAll()
	return &observingBatch{Batch: b.Batch.WithGraph(graphSchema), scope: b.scope, eng: b.eng}
}

// Commit flushes scope to the engine immediately -- eng.NoteWrite(scope),
// then a fresh WriteScope for whatever this batch does next -- before
// delegating to the inner batch's own Commit. A batch, unlike a transaction,
// is documented to support being committed mid-delegate and continuing to
// receive more operations afterward (graph.Batch's own doc on Commit: "calls
// to commit this batch transaction right away"), so a caller relying on that
// to make an early chunk of a large batch visible needs the engine's
// snapshot invalidated at that same moment, not held back until Driver.
// BatchOperation's own NoteWrite call after the whole batch delegate
// returns. Driver.BatchOperation still calls NoteWrite once more after the
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

// cypherMutates parses text with the same dawgs Cypher frontend
// internal/engine/recognize/cypher.go's FromCypher uses and reports whether
// its AST contains any updating clause (CREATE, SET, REMOVE, DELETE, or
// MERGE) anywhere in the query body -- in the top-level SinglePartQuery, or
// in any part of a MultiPartQuery (a query with one or more WITH
// boundaries). A parse failure reports true: this is the same conservative
// default NoteWrite(nil) has always meant for Run's raw-Cypher callers
// (driver.go), and text that fails to parse here is exactly as unknown to
// this package as it would be to whatever eventually rejects it downstream.
//
// This deliberately does not attempt to recognize which specific kinds an
// updating clause touches (unlike edgeKindsFromCriteria above, which does,
// for the one delete shape narrow enough to be worth the effort) -- a
// mutating Cypher statement can create, relabel, or delete nodes and edges
// of arbitrary kinds named anywhere in its pattern or SET/REMOVE items, and
// nothing here attempts to walk that out. observingTransaction.Query calls
// this and marks the whole scope dirty on true.
func cypherMutates(text string) bool {
	defer func() {
		// The dawgs frontend is not documented to never panic on malformed
		// input; a recover here keeps a parser surprise from crashing the
		// write path it would otherwise run underneath, converting it into
		// the same conservative "assume it mutates" answer a parse error
		// already gets.
		_ = recover()
	}()

	regularQuery, err := frontend.ParseCypher(frontend.DefaultCypherContext(), text)
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
