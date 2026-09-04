// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
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
// returns a configurable mockRelationshipQuery and whose Query records every
// call and returns a fixed result, so wrapper_test.go can assert on both
// without a live database.
type mockTransaction struct {
	relQuery *mockRelationshipQuery

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
	panic("mockTransaction: Nodes not implemented")
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
	panic("mockRelationshipQuery: Count not implemented")
}

func (m *mockRelationshipQuery) First() (*graph.Relationship, error) {
	panic("mockRelationshipQuery: First not implemented")
}

func (m *mockRelationshipQuery) Query(func(graph.Result) error, ...graph.Criteria) error {
	panic("mockRelationshipQuery: Query not implemented")
}

func (m *mockRelationshipQuery) Fetch(func(graph.Cursor[*graph.Relationship]) error) error {
	panic("mockRelationshipQuery: Fetch not implemented")
}

func (m *mockRelationshipQuery) FetchDirection(graph.Direction, func(graph.Cursor[graph.DirectionalResult]) error) error {
	panic("mockRelationshipQuery: FetchDirection not implemented")
}

func (m *mockRelationshipQuery) FetchIDs(func(graph.Cursor[graph.ID]) error) error {
	panic("mockRelationshipQuery: FetchIDs not implemented")
}

func (m *mockRelationshipQuery) FetchTriples(func(graph.Cursor[graph.RelationshipTripleResult]) error) error {
	panic("mockRelationshipQuery: FetchTriples not implemented")
}

func (m *mockRelationshipQuery) FetchAllShortestPaths(delegate func(cursor graph.Cursor[graph.Path]) error) error {
	m.fetchAllShortestPathsCalls++
	return delegate(emptyPathCursor{})
}

func (m *mockRelationshipQuery) FetchKinds(func(cursor graph.Cursor[graph.RelationshipKindsResult]) error) error {
	panic("mockRelationshipQuery: FetchKinds not implemented")
}

var _ graph.RelationshipQuery = (*mockRelationshipQuery)(nil)

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
