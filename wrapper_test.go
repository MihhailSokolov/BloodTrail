// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// -----------------------------------------------------------------------
// Minimal hand-rolled stubs for graph.Transaction and graph.RelationshipQuery.
// Every method wrappedTransaction/recordingRelationshipQuery do not
// exercise in these tests panics: reaching one would mean a test is
// driving more of the interface than it claims to, which should fail
// loudly rather than silently no-op (mirrors internal/engine/
// cypher_integration_test.go's stubResultTx).
// -----------------------------------------------------------------------

// mockQueryCall records one Query(query, parameters) invocation on
// mockTransaction.
type mockQueryCall struct {
	query      string
	parameters map[string]any
}

// mockTransaction is a minimal graph.Transaction whose Relationships()
// returns a configurable mockRelationshipQuery, whose Nodes() returns a
// configurable mockNodeQuery, and whose Query records every call and returns
// a fixed result, so wrapper_test.go can assert on all three without a live
// database.
type mockTransaction struct {
	relQuery  *mockRelationshipQuery
	nodeQuery *mockNodeQuery

	queryCalls  []mockQueryCall
	queryResult graph.Result

	withGraphCalls int
}

func (m *mockTransaction) WithGraph(graph.Graph) graph.Transaction {
	m.withGraphCalls++
	return m
}

func (m *mockTransaction) CreateNode(*graph.Properties, ...graph.Kind) (*graph.Node, error) {
	panic("mockTransaction: CreateNode not implemented")
}

func (m *mockTransaction) UpdateNode(*graph.Node) error {
	panic("mockTransaction: UpdateNode not implemented")
}

func (m *mockTransaction) Nodes() graph.NodeQuery {
	return m.nodeQuery
}

func (m *mockTransaction) CreateRelationshipByIDs(graph.ID, graph.ID, graph.Kind, *graph.Properties) (*graph.Relationship, error) {
	panic("mockTransaction: CreateRelationshipByIDs not implemented")
}

func (m *mockTransaction) UpdateRelationship(*graph.Relationship) error {
	panic("mockTransaction: UpdateRelationship not implemented")
}

func (m *mockTransaction) Relationships() graph.RelationshipQuery {
	return m.relQuery
}

func (m *mockTransaction) Raw(string, map[string]any) graph.Result {
	panic("mockTransaction: Raw not implemented")
}

func (m *mockTransaction) Query(query string, parameters map[string]any) graph.Result {
	m.queryCalls = append(m.queryCalls, mockQueryCall{query: query, parameters: parameters})
	return m.queryResult
}

func (m *mockTransaction) Commit() error {
	panic("mockTransaction: Commit not implemented")
}

func (m *mockTransaction) GraphQueryMemoryLimit() size.Size {
	return size.Gibibyte
}

var _ graph.Transaction = (*mockTransaction)(nil)

// emptyPathCursor is a graph.Cursor[graph.Path] with nothing to yield, used
// as the cursor mockRelationshipQuery.FetchAllShortestPaths hands to its
// delegate -- these tests only care that the inner FetchAllShortestPaths
// was reached, not about the paths it would report.
type emptyPathCursor struct{}

func (emptyPathCursor) Error() error { return nil }
func (emptyPathCursor) Close()       {}
func (emptyPathCursor) Chan() chan graph.Path {
	ch := make(chan graph.Path)
	close(ch)
	return ch
}

var _ graph.Cursor[graph.Path] = emptyPathCursor{}

// mockRelationshipQuery is a minimal graph.RelationshipQuery recording every
// call recordingRelationshipQuery might delegate to.
type mockRelationshipQuery struct {
	filterCalls  []graph.Criteria
	filterfCalls int
	orderByCalls int
	offsetCalls  int
	limitCalls   int
	updateCalls  int
	deleteCalls  int

	fetchAllShortestPathsCalls int

	countCalls        int
	fetchIDsCalls     int
	fetchTriplesCalls int
	fetchKindsCalls   int

	// queryCalls counts every Query invocation reaching this mock; the most
	// recent call's finalCriteria is kept in lastQueryFinalCriteria so a test
	// can assert the inner query received exactly the arguments
	// recordingRelationshipQuery.Query was given, unchanged.
	queryCalls             int
	lastQueryFinalCriteria []graph.Criteria
}

func (m *mockRelationshipQuery) Filter(criteria graph.Criteria) graph.RelationshipQuery {
	m.filterCalls = append(m.filterCalls, criteria)
	return m
}

func (m *mockRelationshipQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.RelationshipQuery {
	m.filterfCalls++
	m.filterCalls = append(m.filterCalls, criteriaDelegate())
	return m
}

func (m *mockRelationshipQuery) Update(*graph.Properties) error {
	m.updateCalls++
	return nil
}

func (m *mockRelationshipQuery) Delete() error {
	m.deleteCalls++
	return nil
}

func (m *mockRelationshipQuery) OrderBy(...graph.Criteria) graph.RelationshipQuery {
	m.orderByCalls++
	return m
}

func (m *mockRelationshipQuery) Offset(int) graph.RelationshipQuery {
	m.offsetCalls++
	return m
}

func (m *mockRelationshipQuery) Limit(int) graph.RelationshipQuery {
	m.limitCalls++
	return m
}

func (m *mockRelationshipQuery) Count() (int64, error) {
	m.countCalls++
	return 0, nil
}

func (m *mockRelationshipQuery) First() (*graph.Relationship, error) {
	panic("mockRelationshipQuery: First not implemented")
}

// Query records the call and hands delegate an empty, error-free
// graph.Result (graph.NewErrorResult(nil)) -- these tests only care that the
// inner Query was reached, with which finalCriteria, not about any rows it
// would report.
func (m *mockRelationshipQuery) Query(delegate func(graph.Result) error, finalCriteria ...graph.Criteria) error {
	m.queryCalls++
	m.lastQueryFinalCriteria = finalCriteria
	return delegate(graph.NewErrorResult(nil))
}

func (m *mockRelationshipQuery) Fetch(func(graph.Cursor[*graph.Relationship]) error) error {
	panic("mockRelationshipQuery: Fetch not implemented")
}

func (m *mockRelationshipQuery) FetchDirection(graph.Direction, func(graph.Cursor[graph.DirectionalResult]) error) error {
	panic("mockRelationshipQuery: FetchDirection not implemented")
}

func (m *mockRelationshipQuery) FetchIDs(delegate func(graph.Cursor[graph.ID]) error) error {
	m.fetchIDsCalls++
	return delegate(emptyIDCursor{})
}

func (m *mockRelationshipQuery) FetchTriples(delegate func(graph.Cursor[graph.RelationshipTripleResult]) error) error {
	m.fetchTriplesCalls++
	return delegate(emptyRelationshipTripleCursor{})
}

func (m *mockRelationshipQuery) FetchAllShortestPaths(delegate func(cursor graph.Cursor[graph.Path]) error) error {
	m.fetchAllShortestPathsCalls++
	return delegate(emptyPathCursor{})
}

func (m *mockRelationshipQuery) FetchKinds(delegate func(cursor graph.Cursor[graph.RelationshipKindsResult]) error) error {
	m.fetchKindsCalls++
	return delegate(emptyRelationshipKindsCursor{})
}

// emptyRelationshipTripleCursor is a
// graph.Cursor[graph.RelationshipTripleResult] with nothing to yield, used
// as the cursor mockRelationshipQuery.FetchTriples hands to its delegate
// (mirrors emptyPathCursor).
type emptyRelationshipTripleCursor struct{}

func (emptyRelationshipTripleCursor) Error() error { return nil }
func (emptyRelationshipTripleCursor) Close()       {}
func (emptyRelationshipTripleCursor) Chan() chan graph.RelationshipTripleResult {
	ch := make(chan graph.RelationshipTripleResult)
	close(ch)
	return ch
}

var _ graph.Cursor[graph.RelationshipTripleResult] = emptyRelationshipTripleCursor{}

// emptyRelationshipKindsCursor is a
// graph.Cursor[graph.RelationshipKindsResult] with nothing to yield, used as
// the cursor mockRelationshipQuery.FetchKinds hands to its delegate (mirrors
// emptyPathCursor; distinct from emptyKindsCursor below, which is
// mockNodeQuery.FetchKinds' graph.Cursor[graph.KindsResult] counterpart --
// the node and relationship kinds-result types differ).
type emptyRelationshipKindsCursor struct{}

func (emptyRelationshipKindsCursor) Error() error { return nil }
func (emptyRelationshipKindsCursor) Close()       {}
func (emptyRelationshipKindsCursor) Chan() chan graph.RelationshipKindsResult {
	ch := make(chan graph.RelationshipKindsResult)
	close(ch)
	return ch
}

var _ graph.Cursor[graph.RelationshipKindsResult] = emptyRelationshipKindsCursor{}

var _ graph.RelationshipQuery = (*mockRelationshipQuery)(nil)

// emptyIDCursor is a graph.Cursor[graph.ID] with nothing to yield, used as
// the cursor mockNodeQuery.FetchIDs and mockRelationshipQuery.FetchIDs each
// hand their delegate -- these tests only care that the inner FetchIDs was
// reached, not about the ids it would report (mirrors emptyPathCursor).
type emptyIDCursor struct{}

func (emptyIDCursor) Error() error { return nil }
func (emptyIDCursor) Close()       {}
func (emptyIDCursor) Chan() chan graph.ID {
	ch := make(chan graph.ID)
	close(ch)
	return ch
}

var _ graph.Cursor[graph.ID] = emptyIDCursor{}

// emptyKindsCursor is a graph.Cursor[graph.KindsResult] with nothing to
// yield, used as the cursor mockNodeQuery.FetchKinds hands to its delegate
// (mirrors emptyPathCursor/emptyIDCursor).
type emptyKindsCursor struct{}

func (emptyKindsCursor) Error() error { return nil }
func (emptyKindsCursor) Close()       {}
func (emptyKindsCursor) Chan() chan graph.KindsResult {
	ch := make(chan graph.KindsResult)
	close(ch)
	return ch
}

var _ graph.Cursor[graph.KindsResult] = emptyKindsCursor{}

// mockNodeQuery is a minimal graph.NodeQuery recording every call
// recordingNodeQuery might delegate to (mirrors mockRelationshipQuery).
type mockNodeQuery struct {
	filterCalls  []graph.Criteria
	filterfCalls int
	orderByCalls int
	offsetCalls  int
	limitCalls   int
	updateCalls  int
	deleteCalls  int
	queryCalls   int

	countCalls      int
	fetchIDsCalls   int
	fetchKindsCalls int
}

func (m *mockNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	m.filterCalls = append(m.filterCalls, criteria)
	return m
}

func (m *mockNodeQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.NodeQuery {
	m.filterfCalls++
	m.filterCalls = append(m.filterCalls, criteriaDelegate())
	return m
}

func (m *mockNodeQuery) Query(func(graph.Result) error, ...graph.Criteria) error {
	m.queryCalls++
	return nil
}

func (m *mockNodeQuery) Delete() error {
	m.deleteCalls++
	return nil
}

func (m *mockNodeQuery) Update(*graph.Properties) error {
	m.updateCalls++
	return nil
}

func (m *mockNodeQuery) OrderBy(...graph.Criteria) graph.NodeQuery {
	m.orderByCalls++
	return m
}

func (m *mockNodeQuery) Offset(int) graph.NodeQuery {
	m.offsetCalls++
	return m
}

func (m *mockNodeQuery) Limit(int) graph.NodeQuery {
	m.limitCalls++
	return m
}

func (m *mockNodeQuery) Count() (int64, error) {
	m.countCalls++
	return 0, nil
}

func (m *mockNodeQuery) First() (*graph.Node, error) {
	panic("mockNodeQuery: First not implemented")
}

func (m *mockNodeQuery) Fetch(func(graph.Cursor[*graph.Node]) error, ...graph.Criteria) error {
	panic("mockNodeQuery: Fetch not implemented")
}

func (m *mockNodeQuery) FetchIDs(delegate func(cursor graph.Cursor[graph.ID]) error) error {
	m.fetchIDsCalls++
	return delegate(emptyIDCursor{})
}

func (m *mockNodeQuery) FetchKinds(delegate func(cursor graph.Cursor[graph.KindsResult]) error) error {
	m.fetchKindsCalls++
	return delegate(emptyKindsCursor{})
}

var _ graph.NodeQuery = (*mockNodeQuery)(nil)

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

// disabledEngine returns an *engine.Engine that declines every
// TryAllShortestPaths/TryCypher call unconditionally (Enabled: false
// short-circuits before pgDriver/pool are ever touched), so these tests can
// exercise the wrapper's own recording/tainting/decline-routing logic
// without a live database.
func disabledEngine() *engine.Engine {
	return engine.New(nil, nil, engine.Config{Enabled: false})
}

func newWrappedTransaction(inner *mockTransaction) *wrappedTransaction {
	return &wrappedTransaction{Transaction: inner, engine: disabledEngine()}
}

// -----------------------------------------------------------------------
// recordingRelationshipQuery
// -----------------------------------------------------------------------

func TestRecordingRelationshipQueryFilterRecordsAndDelegates(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})

	criteria := graph.Criteria("start-id-equals")
	got := tx.Relationships().Filter(criteria)

	rrq, ok := got.(*recordingRelationshipQuery)
	if !ok {
		t.Fatalf("Filter returned %T, want *recordingRelationshipQuery", got)
	}
	if len(rrq.criteria) != 1 || rrq.criteria[0] != criteria {
		t.Fatalf("recorded criteria = %v, want [%v]", rrq.criteria, criteria)
	}
	if len(innerRel.filterCalls) != 1 || innerRel.filterCalls[0] != criteria {
		t.Fatalf("Filter did not delegate to the inner query: %v", innerRel.filterCalls)
	}
}

func TestRecordingRelationshipQueryFilterfRecordsAndPassesProviderThrough(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})

	criteria := graph.Criteria("end-id-equals")
	providerCalls := 0
	provider := func() graph.Criteria {
		providerCalls++
		return criteria
	}

	got := tx.Relationships().Filterf(provider)

	rrq, ok := got.(*recordingRelationshipQuery)
	if !ok {
		t.Fatalf("Filterf returned %T, want *recordingRelationshipQuery", got)
	}
	if len(rrq.criteria) != 1 || rrq.criteria[0] != criteria {
		t.Fatalf("recorded criteria = %v, want [%v]", rrq.criteria, criteria)
	}
	if innerRel.filterfCalls != 1 {
		t.Fatalf("Filterf did not delegate to the inner query: filterfCalls = %d", innerRel.filterfCalls)
	}
	// Called once by recordingRelationshipQuery.Filterf itself (to record
	// the result) and once more inside the inner mock's own Filterf (which
	// also calls the delegate it was handed) -- proving the *same* provider
	// was passed through rather than a closure wrapping the already-observed
	// value.
	if providerCalls != 2 {
		t.Fatalf("provider called %d times, want exactly 2 (record + inner delegate)", providerCalls)
	}
}

func TestRecordingRelationshipQueryOrderByOffsetLimitTaintAndDelegate(t *testing.T) {
	cases := []struct {
		name  string
		apply func(graph.RelationshipQuery) graph.RelationshipQuery
		check func(*mockRelationshipQuery) int
	}{
		{"OrderBy", func(rq graph.RelationshipQuery) graph.RelationshipQuery { return rq.OrderBy(graph.Criteria("x")) }, func(m *mockRelationshipQuery) int { return m.orderByCalls }},
		{"Offset", func(rq graph.RelationshipQuery) graph.RelationshipQuery { return rq.Offset(5) }, func(m *mockRelationshipQuery) int { return m.offsetCalls }},
		{"Limit", func(rq graph.RelationshipQuery) graph.RelationshipQuery { return rq.Limit(5) }, func(m *mockRelationshipQuery) int { return m.limitCalls }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			innerRel := &mockRelationshipQuery{}
			tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})

			// A single recognizable-shaped criteria recorded first, so that
			// absent tainting, FetchAllShortestPaths would at least attempt
			// to consult the engine.
			rq := tx.Relationships().Filter(graph.Criteria("start-id-equals"))
			rq = tc.apply(rq)

			rrq, ok := rq.(*recordingRelationshipQuery)
			if !ok {
				t.Fatalf("%s returned %T, want *recordingRelationshipQuery", tc.name, rq)
			}
			if !rrq.tainted {
				t.Fatalf("%s did not taint the query", tc.name)
			}
			if got := tc.check(innerRel); got != 1 {
				t.Fatalf("%s did not delegate to the inner query (calls = %d)", tc.name, got)
			}

			// FetchAllShortestPaths must go straight to the inner query once
			// tainted, regardless of the recorded criteria.
			if err := rq.FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error { return nil }); err != nil {
				t.Fatalf("FetchAllShortestPaths: unexpected error: %v", err)
			}
			if innerRel.fetchAllShortestPathsCalls != 1 {
				t.Fatalf("FetchAllShortestPaths did not delegate to the inner query after tainting")
			}
		})
	}
}

func TestRecordingRelationshipQueryUpdateDeleteTaint(t *testing.T) {
	t.Run("Update", func(t *testing.T) {
		innerRel := &mockRelationshipQuery{}
		tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
		rq := tx.Relationships()
		if err := rq.Update(graph.NewProperties()); err != nil {
			t.Fatalf("Update: unexpected error: %v", err)
		}
		rrq := rq.(*recordingRelationshipQuery)
		if !rrq.tainted {
			t.Fatalf("Update did not taint the query")
		}
		if innerRel.updateCalls != 1 {
			t.Fatalf("Update did not delegate to the inner query")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		innerRel := &mockRelationshipQuery{}
		tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
		rq := tx.Relationships()
		if err := rq.Delete(); err != nil {
			t.Fatalf("Delete: unexpected error: %v", err)
		}
		rrq := rq.(*recordingRelationshipQuery)
		if !rrq.tainted {
			t.Fatalf("Delete did not taint the query")
		}
		if innerRel.deleteCalls != 1 {
			t.Fatalf("Delete did not delegate to the inner query")
		}
	})
}

func TestRecordingRelationshipQueryFetchAllShortestPathsFallsThroughWithoutASingleRecognizedCriteria(t *testing.T) {
	t.Run("no criteria", func(t *testing.T) {
		innerRel := &mockRelationshipQuery{}
		tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
		if err := tx.Relationships().FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error { return nil }); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if innerRel.fetchAllShortestPathsCalls != 1 {
			t.Fatalf("expected the inner query's FetchAllShortestPaths to run")
		}
	})

	t.Run("more than one criteria", func(t *testing.T) {
		innerRel := &mockRelationshipQuery{}
		tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
		rq := tx.Relationships().Filter(graph.Criteria("a")).Filter(graph.Criteria("b"))
		if err := rq.FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error { return nil }); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if innerRel.fetchAllShortestPathsCalls != 1 {
			t.Fatalf("expected the inner query's FetchAllShortestPaths to run")
		}
	})

	t.Run("unrecognized single criteria", func(t *testing.T) {
		innerRel := &mockRelationshipQuery{}
		tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
		// A plain string never satisfies recognize.FromCriteria's type
		// assertion against *cypher.Conjunction.
		rq := tx.Relationships().Filter(graph.Criteria("not-a-conjunction"))
		if err := rq.FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error { return nil }); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if innerRel.fetchAllShortestPathsCalls != 1 {
			t.Fatalf("expected the inner query's FetchAllShortestPaths to run")
		}
	})
}

func TestRecordingRelationshipQueryFetchAllShortestPathsDeclinedByEngineFallsThrough(t *testing.T) {
	// A single criteria value the engine would decline (disabledEngine
	// always declines) still must fall through cleanly to the inner query,
	// whether or not recognize.FromCriteria would have accepted it.
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
	rq := tx.Relationships().Filter(graph.Criteria("start-id-equals"))

	if err := rq.FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if innerRel.fetchAllShortestPathsCalls != 1 {
		t.Fatalf("expected the inner query's FetchAllShortestPaths to run when the engine declines")
	}
}

// testEdgeKind is a stand-in graph.Kind used to build recognize.
// FromRelCriteria-recognized KindMatcher criteria below -- its identity
// doesn't matter to any of these tests, only that it is non-empty (see
// recognize.KindConstraint's doc for why an empty-Kinds KindMatcher is
// rejected outright rather than modeled).
var testEdgeKind = graph.StringKind("TestEdge")

func TestRecordingRelationshipQueryOrderByEdgeIDAscendingSetsFlagWithoutTainting(t *testing.T) {
	// Both spellings recognize.OrderIsEdgeIDAscending accepts (its own doc):
	// dawgs' traversal.LightweightDriver uses the Identity(Relationship())
	// form, ops/traversal.go's TraversalPlan uses the bare Relationship()
	// form.
	cases := []struct {
		name  string
		order graph.Criteria
	}{
		{"Identity(Relationship())", query.Order(query.Identity(query.Relationship()), query.Ascending())},
		{"bare Relationship()", query.Order(query.Relationship(), query.Ascending())},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			innerRel := &mockRelationshipQuery{}
			tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})

			rq := tx.Relationships().OrderBy(tc.order)

			rrq, ok := rq.(*recordingRelationshipQuery)
			if !ok {
				t.Fatalf("OrderBy returned %T, want *recordingRelationshipQuery", rq)
			}
			if !rrq.orderByEdgeID {
				t.Fatalf("OrderBy(%s) did not set orderByEdgeID", tc.name)
			}
			if rrq.tainted {
				t.Fatalf("OrderBy(%s) tainted the query, want orderByEdgeID set instead", tc.name)
			}
			if innerRel.orderByCalls != 1 {
				t.Fatalf("OrderBy did not delegate to the inner query (calls = %d)", innerRel.orderByCalls)
			}
		})
	}
}

func TestRecordingRelationshipQueryOrderByUnrecognizedTaintsNotOrderByEdgeID(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})

	// A plain string never satisfies recognize.OrderIsEdgeIDAscending's type
	// assertion against *cypher.SortItem, so this must taint rather than set
	// orderByEdgeID -- mirrors TestRecordingRelationshipQueryOrderByOffsetLimitTaintAndDelegate's
	// "OrderBy" case, with the added orderByEdgeID assertion that test does
	// not make.
	rq := tx.Relationships().OrderBy(graph.Criteria("not-a-sort-item"))

	rrq, ok := rq.(*recordingRelationshipQuery)
	if !ok {
		t.Fatalf("OrderBy returned %T, want *recordingRelationshipQuery", rq)
	}
	if !rrq.tainted {
		t.Fatalf("unrecognized OrderBy did not taint the query")
	}
	if rrq.orderByEdgeID {
		t.Fatalf("unrecognized OrderBy incorrectly set orderByEdgeID")
	}
	if innerRel.orderByCalls != 1 {
		t.Fatalf("OrderBy did not delegate to the inner query (calls = %d)", innerRel.orderByCalls)
	}
}

// TestRecordingRelationshipQueryOrderByEdgeIDDeclinesShortestPathsServing is
// the path-serving regression FetchAllShortestPaths' own doc documents:
// orderByEdgeID must gate FetchAllShortestPaths off even when every other
// condition (untainted, non-declined transaction, exactly one recognized
// criteria) holds, since an ordered query is not the canonical shortest-paths
// shape recognize.FromCriteria models.
func TestRecordingRelationshipQueryOrderByEdgeIDDeclinesShortestPathsServing(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})

	pathCriteria := query.And(
		query.Equals(query.StartID(), graph.ID(1)),
		query.Equals(query.EndID(), graph.ID(2)),
	)
	rq := tx.Relationships().Filter(pathCriteria).OrderBy(
		query.Order(query.Identity(query.Relationship()), query.Ascending()),
	)

	if err := rq.FetchAllShortestPaths(func(graph.Cursor[graph.Path]) error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if innerRel.fetchAllShortestPathsCalls != 1 {
		t.Fatalf("expected the inner query's FetchAllShortestPaths to run once orderByEdgeID is set")
	}
}

// TestRecordingRelationshipQueryStructuralFetchesFallThroughWithoutASingleRecognizedCriteria
// mirrors TestRecordingNodeQueryCountFetchIDsFetchKindsFallThroughWithoutASingleRecognizedCriteria
// (node_query.go's counterpart), generalized over all four of Count/
// FetchIDs/FetchTriples/FetchKinds via a table of operations.
func TestRecordingRelationshipQueryStructuralFetchesFallThroughWithoutASingleRecognizedCriteria(t *testing.T) {
	type op struct {
		name string
		call func(graph.RelationshipQuery) error
		toll func(*mockRelationshipQuery) int
	}
	ops := []op{
		{"Count", func(rq graph.RelationshipQuery) error { _, err := rq.Count(); return err }, func(m *mockRelationshipQuery) int { return m.countCalls }},
		{"FetchIDs", func(rq graph.RelationshipQuery) error {
			return rq.FetchIDs(func(graph.Cursor[graph.ID]) error { return nil })
		}, func(m *mockRelationshipQuery) int { return m.fetchIDsCalls }},
		{"FetchTriples", func(rq graph.RelationshipQuery) error {
			return rq.FetchTriples(func(graph.Cursor[graph.RelationshipTripleResult]) error { return nil })
		}, func(m *mockRelationshipQuery) int { return m.fetchTriplesCalls }},
		{"FetchKinds", func(rq graph.RelationshipQuery) error {
			return rq.FetchKinds(func(graph.Cursor[graph.RelationshipKindsResult]) error { return nil })
		}, func(m *mockRelationshipQuery) int { return m.fetchKindsCalls }},
	}

	scenarios := []struct {
		name  string
		build func(graph.RelationshipQuery) graph.RelationshipQuery
	}{
		{"no criteria", func(rq graph.RelationshipQuery) graph.RelationshipQuery { return rq }},
		{"more than one criteria", func(rq graph.RelationshipQuery) graph.RelationshipQuery {
			return rq.Filter(graph.Criteria("a")).Filter(graph.Criteria("b"))
		}},
		{"unrecognized single criteria", func(rq graph.RelationshipQuery) graph.RelationshipQuery {
			// A plain string never satisfies recognize.FromRelCriteria's type
			// assertion against cypher.Expression.
			return rq.Filter(graph.Criteria("not-an-expression"))
		}},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			for _, o := range ops {
				t.Run(o.name, func(t *testing.T) {
					innerRel := &mockRelationshipQuery{}
					tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
					rq := scenario.build(tx.Relationships())

					if err := o.call(rq); err != nil {
						t.Fatalf("%s: unexpected error: %v", o.name, err)
					}
					if got := o.toll(innerRel); got != 1 {
						t.Fatalf("expected the inner query's %s to run (calls = %d)", o.name, got)
					}
				})
			}
		})
	}
}

// TestRecordingRelationshipQueryStructuralFetchesDeclinedByEngineFallThrough
// mirrors TestRecordingNodeQueryFetchIDsFetchKindsDeclinedByEngineFallThrough:
// a single recognized criteria the engine would decline (disabledEngine
// always declines) must still fall through cleanly to the inner query, for
// all four of Count/FetchIDs/FetchTriples/FetchKinds.
func TestRecordingRelationshipQueryStructuralFetchesDeclinedByEngineFallThrough(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
	rq := tx.Relationships().Filter(query.Kind(query.Relationship(), testEdgeKind))

	if _, err := rq.Count(); err != nil {
		t.Fatalf("Count: unexpected error: %v", err)
	}
	if innerRel.countCalls != 1 {
		t.Fatalf("expected the inner query's Count to run when the engine declines")
	}

	if err := rq.FetchIDs(func(graph.Cursor[graph.ID]) error { return nil }); err != nil {
		t.Fatalf("FetchIDs: unexpected error: %v", err)
	}
	if innerRel.fetchIDsCalls != 1 {
		t.Fatalf("expected the inner query's FetchIDs to run when the engine declines")
	}

	if err := rq.FetchTriples(func(graph.Cursor[graph.RelationshipTripleResult]) error { return nil }); err != nil {
		t.Fatalf("FetchTriples: unexpected error: %v", err)
	}
	if innerRel.fetchTriplesCalls != 1 {
		t.Fatalf("expected the inner query's FetchTriples to run when the engine declines")
	}

	if err := rq.FetchKinds(func(graph.Cursor[graph.RelationshipKindsResult]) error { return nil }); err != nil {
		t.Fatalf("FetchKinds: unexpected error: %v", err)
	}
	if innerRel.fetchKindsCalls != 1 {
		t.Fatalf("expected the inner query's FetchKinds to run when the engine declines")
	}
}

// TestRecordingRelationshipQueryStructuralFetchesDeclinedTransactionFallsThrough
// mirrors TestRecordingNodeQueryDeclinedTransactionFallsThrough: a declined
// transaction (WithGraph's contract) must skip the engine even for an
// otherwise-recognizable, untainted, single-criteria query, for all four of
// Count/FetchIDs/FetchTriples/FetchKinds.
func TestRecordingRelationshipQueryStructuralFetchesDeclinedTransactionFallsThrough(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
	retargeted := tx.WithGraph(graph.Graph{Name: "other"}).(*wrappedTransaction)

	rq := retargeted.Relationships().Filter(query.Kind(query.Relationship(), testEdgeKind))
	if _, err := rq.Count(); err != nil {
		t.Fatalf("Count: unexpected error: %v", err)
	}
	if innerRel.countCalls != 1 {
		t.Fatalf("expected the inner query's Count to run on a declined transaction")
	}
}

// relQueryProjections holds the three canonical query.Returning shapes
// recognize.FromReturning recognizes (see its own doc), reused across the
// Query tests below.
var (
	relQueryStartEndReturning     = query.Returning(query.StartID(), query.EndID())
	relQueryStepOutboundReturning = query.Returning(
		query.EndID(), query.KindsOf(query.End()),
		query.RelationshipID(), query.KindsOf(query.Relationship()),
	)
	relQueryStepInboundReturning = query.Returning(
		query.StartID(), query.KindsOf(query.Start()),
		query.RelationshipID(), query.KindsOf(query.Relationship()),
	)
)

// TestRecordingRelationshipQueryQueryFallsThroughOnGuardMismatches covers
// every way Query must decline the engine and fall through to the inner
// query unchanged, before ever reaching engine.TryRelQueryRows: tainted, a
// declined transaction, a criteria/finalCriteria count other than one on
// either side, an unrecognized RETURN shape, an unrecognized rel-criteria
// shape, and a recognized RowProjection whose direction doesn't match the
// recognized RelSpec's own anchoring (projectionDirectionConsistent).
func TestRecordingRelationshipQueryQueryFallsThroughOnGuardMismatches(t *testing.T) {
	recognizedRelCriteria := query.Kind(query.Relationship(), testEdgeKind)
	startAnchored := query.Equals(query.StartID(), graph.ID(1))
	endAnchored := query.Equals(query.EndID(), graph.ID(2))

	cases := []struct {
		name  string
		build func(graph.RelationshipQuery) graph.RelationshipQuery
		final []graph.Criteria
	}{
		{
			name: "tainted",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(recognizedRelCriteria).Offset(1)
			},
			final: []graph.Criteria{relQueryStartEndReturning},
		},
		{
			name: "multi-finalCriteria",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(recognizedRelCriteria)
			},
			final: []graph.Criteria{relQueryStartEndReturning, relQueryStartEndReturning},
		},
		{
			name: "no finalCriteria",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(recognizedRelCriteria)
			},
			final: nil,
		},
		{
			name: "multiple initial criteria",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(recognizedRelCriteria).Filter(startAnchored)
			},
			final: []graph.Criteria{relQueryStartEndReturning},
		},
		{
			name: "unrecognized returning",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(recognizedRelCriteria)
			},
			final: []graph.Criteria{query.Returning(query.Node())},
		},
		{
			name: "unrecognized rel criteria",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(graph.Criteria("not-an-expression"))
			},
			final: []graph.Criteria{relQueryStartEndReturning},
		},
		{
			name: "direction mismatch: step-outbound without StartIDs anchored",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(endAnchored)
			},
			final: []graph.Criteria{relQueryStepOutboundReturning},
		},
		{
			name: "direction mismatch: step-inbound without EndIDs anchored",
			build: func(rq graph.RelationshipQuery) graph.RelationshipQuery {
				return rq.Filter(startAnchored)
			},
			final: []graph.Criteria{relQueryStepInboundReturning},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			innerRel := &mockRelationshipQuery{}
			tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
			rq := tc.build(tx.Relationships())

			if err := rq.Query(func(graph.Result) error { return nil }, tc.final...); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if innerRel.queryCalls != 1 {
				t.Fatalf("expected the inner query's Query to run (calls = %d)", innerRel.queryCalls)
			}
			if len(innerRel.lastQueryFinalCriteria) != len(tc.final) {
				t.Fatalf("inner Query received %d finalCriteria, want %d (unchanged pass-through)", len(innerRel.lastQueryFinalCriteria), len(tc.final))
			}
		})
	}
}

// TestRecordingRelationshipQueryQueryDeclinedTransactionFallsThrough covers
// the one guard TestRecordingRelationshipQueryQueryFallsThroughOnGuardMismatches
// does not (it needs a *wrappedTransaction.WithGraph call, not just a
// differently-built query): a declined transaction must skip the engine even
// for an otherwise fully recognized, direction-consistent Query call.
func TestRecordingRelationshipQueryQueryDeclinedTransactionFallsThrough(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
	retargeted := tx.WithGraph(graph.Graph{Name: "other"}).(*wrappedTransaction)

	rq := retargeted.Relationships().Filter(query.Kind(query.Relationship(), testEdgeKind))
	if err := rq.Query(func(graph.Result) error { return nil }, relQueryStartEndReturning); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if innerRel.queryCalls != 1 {
		t.Fatalf("expected the inner query's Query to run on a declined transaction")
	}
}

// TestRecordingRelationshipQueryQueryDeclinedByEngineFallsThrough covers the
// last case: a fully recognized, direction-consistent Query call that the
// engine itself declines (disabledEngine always declines) must still fall
// through cleanly to the inner query.
func TestRecordingRelationshipQueryQueryDeclinedByEngineFallsThrough(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})
	rq := tx.Relationships().Filter(query.Kind(query.Relationship(), testEdgeKind))

	if err := rq.Query(func(graph.Result) error { return nil }, relQueryStartEndReturning); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if innerRel.queryCalls != 1 {
		t.Fatalf("expected the inner query's Query to run when the engine declines")
	}
}

// -----------------------------------------------------------------------
// wrappedTransaction
// -----------------------------------------------------------------------

func TestWrappedTransactionWithGraphDeclinesAndQueryGoesToInner(t *testing.T) {
	innerTx := &mockTransaction{queryResult: graph.NewErrorResult(nil)}
	tx := newWrappedTransaction(innerTx)

	retargeted := tx.WithGraph(graph.Graph{Name: "other"})
	wt, ok := retargeted.(*wrappedTransaction)
	if !ok {
		t.Fatalf("WithGraph returned %T, want *wrappedTransaction", retargeted)
	}
	if !wt.declined {
		t.Fatalf("WithGraph did not mark the returned wrapper declined")
	}
	if innerTx.withGraphCalls != 1 {
		t.Fatalf("WithGraph did not delegate to the inner transaction")
	}

	result := retargeted.Query("MATCH (n) RETURN n", nil)
	if len(innerTx.queryCalls) != 1 {
		t.Fatalf("declined Query did not delegate to the inner transaction")
	}
	if result != innerTx.queryResult {
		t.Fatalf("declined Query did not return the inner transaction's result")
	}
}

func TestWrappedTransactionQueryWithParamsDelegates(t *testing.T) {
	innerTx := &mockTransaction{queryResult: graph.NewErrorResult(nil)}
	tx := newWrappedTransaction(innerTx)

	params := map[string]any{"x": 1}
	result := tx.Query("MATCH (n) WHERE n.x = $x RETURN n", params)

	if len(innerTx.queryCalls) != 1 {
		t.Fatalf("Query did not delegate to the inner transaction")
	}
	call := innerTx.queryCalls[0]
	if call.query != "MATCH (n) WHERE n.x = $x RETURN n" {
		t.Errorf("delegated query text = %q", call.query)
	}
	if call.parameters["x"] != 1 {
		t.Errorf("delegated parameters = %v", call.parameters)
	}
	if result != innerTx.queryResult {
		t.Fatalf("Query did not return the inner transaction's result")
	}
}

func TestWrappedTransactionRelationshipsReturnsRecordingWrapper(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	tx := newWrappedTransaction(&mockTransaction{relQuery: innerRel})

	rq := tx.Relationships()
	rrq, ok := rq.(*recordingRelationshipQuery)
	if !ok {
		t.Fatalf("Relationships() returned %T, want *recordingRelationshipQuery", rq)
	}
	if rrq.tx != tx {
		t.Fatalf("recordingRelationshipQuery.tx = %p, want %p", rrq.tx, tx)
	}
}

// -----------------------------------------------------------------------
// recordingNodeQuery
// -----------------------------------------------------------------------

func TestRecordingNodeQueryFilterRecordsAndDelegates(t *testing.T) {
	innerNode := &mockNodeQuery{}
	tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})

	criteria := graph.Criteria("kind-equals")
	got := tx.Nodes().Filter(criteria)

	rnq, ok := got.(*recordingNodeQuery)
	if !ok {
		t.Fatalf("Filter returned %T, want *recordingNodeQuery", got)
	}
	if len(rnq.criteria) != 1 || rnq.criteria[0] != criteria {
		t.Fatalf("recorded criteria = %v, want [%v]", rnq.criteria, criteria)
	}
	if len(innerNode.filterCalls) != 1 || innerNode.filterCalls[0] != criteria {
		t.Fatalf("Filter did not delegate to the inner query: %v", innerNode.filterCalls)
	}
}

func TestRecordingNodeQueryFilterfRecordsAndPassesProviderThrough(t *testing.T) {
	innerNode := &mockNodeQuery{}
	tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})

	criteria := graph.Criteria("id-in")
	providerCalls := 0
	provider := func() graph.Criteria {
		providerCalls++
		return criteria
	}

	got := tx.Nodes().Filterf(provider)

	rnq, ok := got.(*recordingNodeQuery)
	if !ok {
		t.Fatalf("Filterf returned %T, want *recordingNodeQuery", got)
	}
	if len(rnq.criteria) != 1 || rnq.criteria[0] != criteria {
		t.Fatalf("recorded criteria = %v, want [%v]", rnq.criteria, criteria)
	}
	if innerNode.filterfCalls != 1 {
		t.Fatalf("Filterf did not delegate to the inner query: filterfCalls = %d", innerNode.filterfCalls)
	}
	// Called once by recordingNodeQuery.Filterf itself (to record the
	// result) and once more inside the inner mock's own Filterf (which also
	// calls the delegate it was handed) -- proving the *same* provider was
	// passed through rather than a closure wrapping the already-observed
	// value.
	if providerCalls != 2 {
		t.Fatalf("provider called %d times, want exactly 2 (record + inner delegate)", providerCalls)
	}
}

func TestRecordingNodeQueryOrderByOffsetLimitTaintAndDelegate(t *testing.T) {
	cases := []struct {
		name  string
		apply func(graph.NodeQuery) graph.NodeQuery
		check func(*mockNodeQuery) int
	}{
		{"OrderBy", func(nq graph.NodeQuery) graph.NodeQuery { return nq.OrderBy(graph.Criteria("x")) }, func(m *mockNodeQuery) int { return m.orderByCalls }},
		{"Offset", func(nq graph.NodeQuery) graph.NodeQuery { return nq.Offset(5) }, func(m *mockNodeQuery) int { return m.offsetCalls }},
		{"Limit", func(nq graph.NodeQuery) graph.NodeQuery { return nq.Limit(5) }, func(m *mockNodeQuery) int { return m.limitCalls }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			innerNode := &mockNodeQuery{}
			tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})

			// A single recognizable-shaped criteria recorded first, so that
			// absent tainting, Count/FetchIDs/FetchKinds would at least
			// attempt to consult the engine.
			nq := tx.Nodes().Filter(graph.Criteria("kind-equals"))
			nq = tc.apply(nq)

			rnq, ok := nq.(*recordingNodeQuery)
			if !ok {
				t.Fatalf("%s returned %T, want *recordingNodeQuery", tc.name, nq)
			}
			if !rnq.tainted {
				t.Fatalf("%s did not taint the query", tc.name)
			}
			if got := tc.check(innerNode); got != 1 {
				t.Fatalf("%s did not delegate to the inner query (calls = %d)", tc.name, got)
			}

			// Count/FetchIDs/FetchKinds must go straight to the inner query
			// once tainted, regardless of the recorded criteria.
			if _, err := nq.Count(); err != nil {
				t.Fatalf("Count: unexpected error: %v", err)
			}
			if innerNode.countCalls != 1 {
				t.Fatalf("Count did not delegate to the inner query after tainting")
			}
			if err := nq.FetchIDs(func(graph.Cursor[graph.ID]) error { return nil }); err != nil {
				t.Fatalf("FetchIDs: unexpected error: %v", err)
			}
			if innerNode.fetchIDsCalls != 1 {
				t.Fatalf("FetchIDs did not delegate to the inner query after tainting")
			}
			if err := nq.FetchKinds(func(graph.Cursor[graph.KindsResult]) error { return nil }); err != nil {
				t.Fatalf("FetchKinds: unexpected error: %v", err)
			}
			if innerNode.fetchKindsCalls != 1 {
				t.Fatalf("FetchKinds did not delegate to the inner query after tainting")
			}
		})
	}
}

func TestRecordingNodeQueryUpdateDeleteQueryTaint(t *testing.T) {
	t.Run("Update", func(t *testing.T) {
		innerNode := &mockNodeQuery{}
		tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
		nq := tx.Nodes()
		if err := nq.Update(graph.NewProperties()); err != nil {
			t.Fatalf("Update: unexpected error: %v", err)
		}
		rnq := nq.(*recordingNodeQuery)
		if !rnq.tainted {
			t.Fatalf("Update did not taint the query")
		}
		if innerNode.updateCalls != 1 {
			t.Fatalf("Update did not delegate to the inner query")
		}
	})

	t.Run("Delete", func(t *testing.T) {
		innerNode := &mockNodeQuery{}
		tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
		nq := tx.Nodes()
		if err := nq.Delete(); err != nil {
			t.Fatalf("Delete: unexpected error: %v", err)
		}
		rnq := nq.(*recordingNodeQuery)
		if !rnq.tainted {
			t.Fatalf("Delete did not taint the query")
		}
		if innerNode.deleteCalls != 1 {
			t.Fatalf("Delete did not delegate to the inner query")
		}
	})

	t.Run("Query", func(t *testing.T) {
		innerNode := &mockNodeQuery{}
		tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
		nq := tx.Nodes()
		if err := nq.Query(func(graph.Result) error { return nil }); err != nil {
			t.Fatalf("Query: unexpected error: %v", err)
		}
		rnq := nq.(*recordingNodeQuery)
		if !rnq.tainted {
			t.Fatalf("Query did not taint the query")
		}
		if innerNode.queryCalls != 1 {
			t.Fatalf("Query did not delegate to the inner query")
		}
	})
}

func TestRecordingNodeQueryCountFetchIDsFetchKindsFallThroughWithoutASingleRecognizedCriteria(t *testing.T) {
	t.Run("no criteria", func(t *testing.T) {
		innerNode := &mockNodeQuery{}
		tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
		if _, err := tx.Nodes().Count(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if innerNode.countCalls != 1 {
			t.Fatalf("expected the inner query's Count to run")
		}
	})

	t.Run("more than one criteria", func(t *testing.T) {
		innerNode := &mockNodeQuery{}
		tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
		nq := tx.Nodes().Filter(graph.Criteria("a")).Filter(graph.Criteria("b"))
		if _, err := nq.Count(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if innerNode.countCalls != 1 {
			t.Fatalf("expected the inner query's Count to run")
		}
	})

	t.Run("unrecognized single criteria", func(t *testing.T) {
		innerNode := &mockNodeQuery{}
		tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
		// A plain string never satisfies recognize.FromNodeCriteria's type
		// assertion against cypher.Expression.
		nq := tx.Nodes().Filter(graph.Criteria("not-an-expression"))
		if _, err := nq.Count(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if innerNode.countCalls != 1 {
			t.Fatalf("expected the inner query's Count to run")
		}
	})
}

func TestRecordingNodeQueryFetchIDsFetchKindsDeclinedByEngineFallThrough(t *testing.T) {
	// A single criteria value the engine would decline (disabledEngine always
	// declines) still must fall through cleanly to the inner query.
	innerNode := &mockNodeQuery{}
	tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
	nq := tx.Nodes().Filter(graph.Criteria("kind-equals"))

	if err := nq.FetchIDs(func(graph.Cursor[graph.ID]) error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if innerNode.fetchIDsCalls != 1 {
		t.Fatalf("expected the inner query's FetchIDs to run when the engine declines")
	}

	if err := nq.FetchKinds(func(graph.Cursor[graph.KindsResult]) error { return nil }); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if innerNode.fetchKindsCalls != 1 {
		t.Fatalf("expected the inner query's FetchKinds to run when the engine declines")
	}
}

func TestRecordingNodeQueryDeclinedTransactionFallsThrough(t *testing.T) {
	// A declined transaction (WithGraph's contract; see wrappedTransaction's
	// own doc) must skip the engine even for an otherwise-recognizable,
	// untainted, single-criteria query.
	innerNode := &mockNodeQuery{}
	tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})
	retargeted := tx.WithGraph(graph.Graph{Name: "other"}).(*wrappedTransaction)

	nq := retargeted.Nodes().Filter(graph.Criteria("kind-equals"))
	if _, err := nq.Count(); err != nil {
		t.Fatalf("Count: unexpected error: %v", err)
	}
	if innerNode.countCalls != 1 {
		t.Fatalf("expected the inner query's Count to run on a declined transaction")
	}
}

func TestWrappedTransactionNodesReturnsRecordingWrapper(t *testing.T) {
	innerNode := &mockNodeQuery{}
	tx := newWrappedTransaction(&mockTransaction{nodeQuery: innerNode})

	nq := tx.Nodes()
	rnq, ok := nq.(*recordingNodeQuery)
	if !ok {
		t.Fatalf("Nodes() returned %T, want *recordingNodeQuery", nq)
	}
	if rnq.tx != tx {
		t.Fatalf("recordingNodeQuery.tx = %p, want %p", rnq.tx, tx)
	}
}

// -----------------------------------------------------------------------
// Driver: Run/WipeGraph/DeleteNodesByKinds/DeleteRelationshipsByKinds
// -----------------------------------------------------------------------
//
// These four capability methods are promoted from *pg.Driver unmodified by
// plain embedding, but embedding has no virtual dispatch: pg.Driver.Run and
// pg.Driver.WipeGraph both call the *pg.Driver's own WriteTransaction
// internally (a concrete, same-package call), and DeleteNodesByKinds /
// DeleteRelationshipsByKinds use a raw pooled connection -- none of the four
// ever reaches this package's own WriteTransaction override, so each is
// overridden separately on *Driver (driver.go) to call engine.NoteWrite()
// itself after a nil-error return.
//
// A live *pg.Driver is unavoidable here (Driver embeds the concrete type,
// not an interface), so the success path -- NoteWrite() actually firing --
// is not covered by these unit tests; it is covered by the engine-serving
// integration suite instead (engine_serving_integration_test.go's
// TestWipeGraphInvalidatesEngineSnapshot). What *is* cleanly testable
// without a live database is the error path: unreachablePGDriver points at
// an address nothing listens on, so every one of the four calls fails
// quickly (observed at single-digit milliseconds; connection refused, not a
// timeout) with a clean error rather than a panic, and NoteWrite() must not
// fire when the embedded call itself failed.

// unreachablePGDriver returns a *pg.Driver backed by a pool pointed at
// 127.0.0.1 on a low port nothing listens on, so pool.Acquire (and
// everything built on it) fails fast with "connection refused" -- no live
// database required, and no risk of hanging the test suite.
func unreachablePGDriver(t *testing.T) *pg.Driver {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), "postgres://bloodtrail:bloodtrail@127.0.0.1:1/bloodtrail")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	return pg.NewDriver(size.Gibibyte, pool)
}

// newDriverWithUnreachablePG returns a *Driver embedding unreachablePGDriver
// and a disabledEngine, plus that same engine for the test to inspect via
// its exported Generation() (added purely for this kind of test
// observability -- see its doc comment).
func newDriverWithUnreachablePG(t *testing.T) (*Driver, *engine.Engine) {
	t.Helper()
	eng := disabledEngine()
	return &Driver{Driver: unreachablePGDriver(t), engine: eng}, eng
}

func TestDriverMutatingCapabilityMethodsDoNotBumpGenerationOnError(t *testing.T) {
	cases := []struct {
		name string
		call func(ctx context.Context, d *Driver) error
	}{
		{"Run", func(ctx context.Context, d *Driver) error {
			return d.Run(ctx, "MATCH (n) RETURN n", nil)
		}},
		{"WipeGraph", func(ctx context.Context, d *Driver) error {
			return d.WipeGraph(ctx, nil)
		}},
		{"DeleteNodesByKinds", func(ctx context.Context, d *Driver) error {
			// Both kind sets empty still reaches execDelete (a genuine
			// "delete every node" statement), which acquires a pooled
			// connection -- so this still exercises a real network failure,
			// not a short-circuited no-op.
			return d.DeleteNodesByKinds(ctx, nil, nil)
		}},
		{"DeleteRelationshipsByKinds", func(ctx context.Context, d *Driver) error {
			// A non-empty kind is required: DeleteRelationshipsByKinds
			// short-circuits to a nil-error no-op for an empty kind slice
			// without ever touching the pool, which would prove nothing
			// about the error path.
			return d.DeleteRelationshipsByKinds(ctx, graph.Kinds{graph.StringKind("Probe")})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, eng := newDriverWithUnreachablePG(t)
			before := eng.Generation()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := tc.call(ctx, d); err == nil {
				t.Fatalf("%s against an unreachable PostgreSQL: expected an error, got nil", tc.name)
			}

			if after := eng.Generation(); after != before {
				t.Fatalf("%s: generation changed from %d to %d after a failed call, want unchanged", tc.name, before, after)
			}
		})
	}
}
