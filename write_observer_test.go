// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"reflect"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// -----------------------------------------------------------------------
// Fakes.
//
// wrapper_test.go's mockTransaction/mockRelationshipQuery panic on most of
// the methods this file needs to exercise (CreateNode, UpdateNode, Nodes,
// CreateRelationshipByIDs, Raw): the read-side wrappedTransaction they were
// built for never calls those. fakeTransaction, fakeNodeQuery and fakeBatch
// below implement every method of their respective interfaces fully instead
// (recording the call and returning a configured value), so delegation for
// every observingTransaction/observingNodeQuery/observingBatch override --
// and every method left promoted -- can be verified directly.
// mockRelationshipQuery is reused as-is for RelationshipQuery-shaped fakes:
// it already implements the full interface and records exactly what these
// tests need (filterCalls, deleteCalls, updateCalls, ...).
// -----------------------------------------------------------------------

// fakeCreateRelByIDsCall records one CreateRelationshipByIDs invocation,
// shared by fakeTransaction and fakeBatch (whose methods of that name have
// identical parameter shapes modulo return type).
type fakeCreateRelByIDsCall struct {
	start, end graph.ID
	kind       graph.Kind
	properties *graph.Properties
}

// fakeTransaction is a graph.Transaction implementing every method fully.
type fakeTransaction struct {
	createNodeProperties []*graph.Properties
	createNodeKinds      [][]graph.Kind
	createNodeReturn     *graph.Node
	createNodeErr        error

	updateNodeCalls []*graph.Node
	updateNodeErr   error

	nodesReturn graph.NodeQuery

	createRelByIDsCalls  []fakeCreateRelByIDsCall
	createRelByIDsReturn *graph.Relationship
	createRelByIDsErr    error

	updateRelationshipCalls []*graph.Relationship
	updateRelationshipErr   error

	relationshipsReturn graph.RelationshipQuery

	rawCalls  []mockQueryCall
	rawResult graph.Result

	queryCalls  []mockQueryCall
	queryResult graph.Result

	commitCalls int
	commitErr   error

	withGraphCalls  int
	withGraphReturn graph.Transaction
}

func (f *fakeTransaction) WithGraph(graph.Graph) graph.Transaction {
	f.withGraphCalls++
	return f.withGraphReturn
}

func (f *fakeTransaction) CreateNode(properties *graph.Properties, kinds ...graph.Kind) (*graph.Node, error) {
	f.createNodeProperties = append(f.createNodeProperties, properties)
	f.createNodeKinds = append(f.createNodeKinds, kinds)
	return f.createNodeReturn, f.createNodeErr
}

func (f *fakeTransaction) UpdateNode(node *graph.Node) error {
	f.updateNodeCalls = append(f.updateNodeCalls, node)
	return f.updateNodeErr
}

func (f *fakeTransaction) Nodes() graph.NodeQuery {
	return f.nodesReturn
}

func (f *fakeTransaction) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) (*graph.Relationship, error) {
	f.createRelByIDsCalls = append(f.createRelByIDsCalls, fakeCreateRelByIDsCall{startNodeID, endNodeID, kind, properties})
	return f.createRelByIDsReturn, f.createRelByIDsErr
}

func (f *fakeTransaction) UpdateRelationship(relationship *graph.Relationship) error {
	f.updateRelationshipCalls = append(f.updateRelationshipCalls, relationship)
	return f.updateRelationshipErr
}

func (f *fakeTransaction) Relationships() graph.RelationshipQuery {
	return f.relationshipsReturn
}

func (f *fakeTransaction) Raw(query string, parameters map[string]any) graph.Result {
	f.rawCalls = append(f.rawCalls, mockQueryCall{query: query, parameters: parameters})
	return f.rawResult
}

func (f *fakeTransaction) Query(query string, parameters map[string]any) graph.Result {
	f.queryCalls = append(f.queryCalls, mockQueryCall{query: query, parameters: parameters})
	return f.queryResult
}

func (f *fakeTransaction) Commit() error {
	f.commitCalls++
	return f.commitErr
}

func (f *fakeTransaction) GraphQueryMemoryLimit() size.Size {
	return size.Gibibyte
}

var _ graph.Transaction = (*fakeTransaction)(nil)

// fakeNodeQuery is a graph.NodeQuery implementing every fluent/delegating
// method this file's tests exercise (Filter, Filterf, Delete, Update,
// OrderBy, Offset, Limit) fully, and panicking on the rest (Query, Count,
// First, Fetch, FetchIDs, FetchKinds) -- none of observingNodeQuery's
// contract touches those, so reaching one here would mean a test drives
// more of the interface than it claims to (mirrors wrapper_test.go's own
// stub convention).
type fakeNodeQuery struct {
	filterCalls  []graph.Criteria
	filterfCalls int

	deleteCalls int
	deleteErr   error

	updateCalls int
	updateErr   error

	orderByCalls int
	offsetCalls  int
	limitCalls   int
}

func (f *fakeNodeQuery) Filter(criteria graph.Criteria) graph.NodeQuery {
	f.filterCalls = append(f.filterCalls, criteria)
	return f
}

func (f *fakeNodeQuery) Filterf(criteriaDelegate graph.CriteriaProvider) graph.NodeQuery {
	f.filterfCalls++
	f.filterCalls = append(f.filterCalls, criteriaDelegate())
	return f
}

func (f *fakeNodeQuery) Query(func(graph.Result) error, ...graph.Criteria) error {
	panic("fakeNodeQuery: Query not implemented")
}

func (f *fakeNodeQuery) Delete() error {
	f.deleteCalls++
	return f.deleteErr
}

func (f *fakeNodeQuery) Update(*graph.Properties) error {
	f.updateCalls++
	return f.updateErr
}

func (f *fakeNodeQuery) OrderBy(...graph.Criteria) graph.NodeQuery {
	f.orderByCalls++
	return f
}

func (f *fakeNodeQuery) Offset(int) graph.NodeQuery {
	f.offsetCalls++
	return f
}

func (f *fakeNodeQuery) Limit(int) graph.NodeQuery {
	f.limitCalls++
	return f
}

func (f *fakeNodeQuery) Count() (int64, error) {
	panic("fakeNodeQuery: Count not implemented")
}

func (f *fakeNodeQuery) First() (*graph.Node, error) {
	panic("fakeNodeQuery: First not implemented")
}

func (f *fakeNodeQuery) Fetch(func(graph.Cursor[*graph.Node]) error, ...graph.Criteria) error {
	panic("fakeNodeQuery: Fetch not implemented")
}

func (f *fakeNodeQuery) FetchIDs(func(graph.Cursor[graph.ID]) error) error {
	panic("fakeNodeQuery: FetchIDs not implemented")
}

func (f *fakeNodeQuery) FetchKinds(func(graph.Cursor[graph.KindsResult]) error) error {
	panic("fakeNodeQuery: FetchKinds not implemented")
}

var _ graph.NodeQuery = (*fakeNodeQuery)(nil)

// fakeBatch is a graph.Batch implementing every method fully.
type fakeBatch struct {
	createNodeCalls []*graph.Node
	createNodeErr   error

	deleteNodeCalls []graph.ID
	deleteNodeErr   error

	nodesReturn         graph.NodeQuery
	relationshipsReturn graph.RelationshipQuery

	updateNodeByCalls []graph.NodeUpdate
	updateNodeByErr   error

	updateNodesCalls [][]*graph.Node
	updateNodesErr   error

	createRelationshipCalls []*graph.Relationship
	createRelationshipErr   error

	createRelByIDsCalls []fakeCreateRelByIDsCall
	createRelByIDsErr   error

	deleteRelationshipCalls []graph.ID
	deleteRelationshipErr   error

	updateRelationshipByCalls []graph.RelationshipUpdate
	updateRelationshipByErr   error

	withGraphCalls  int
	withGraphReturn graph.Batch

	commitCalls int
	commitErr   error
}

func (f *fakeBatch) WithGraph(graph.Graph) graph.Batch {
	f.withGraphCalls++
	return f.withGraphReturn
}

func (f *fakeBatch) CreateNode(node *graph.Node) error {
	f.createNodeCalls = append(f.createNodeCalls, node)
	return f.createNodeErr
}

func (f *fakeBatch) DeleteNode(id graph.ID) error {
	f.deleteNodeCalls = append(f.deleteNodeCalls, id)
	return f.deleteNodeErr
}

func (f *fakeBatch) Nodes() graph.NodeQuery {
	return f.nodesReturn
}

func (f *fakeBatch) Relationships() graph.RelationshipQuery {
	return f.relationshipsReturn
}

func (f *fakeBatch) UpdateNodeBy(update graph.NodeUpdate) error {
	f.updateNodeByCalls = append(f.updateNodeByCalls, update)
	return f.updateNodeByErr
}

func (f *fakeBatch) UpdateNodes(nodes []*graph.Node) error {
	f.updateNodesCalls = append(f.updateNodesCalls, nodes)
	return f.updateNodesErr
}

func (f *fakeBatch) CreateRelationship(relationship *graph.Relationship) error {
	f.createRelationshipCalls = append(f.createRelationshipCalls, relationship)
	return f.createRelationshipErr
}

func (f *fakeBatch) CreateRelationshipByIDs(startNodeID, endNodeID graph.ID, kind graph.Kind, properties *graph.Properties) error {
	f.createRelByIDsCalls = append(f.createRelByIDsCalls, fakeCreateRelByIDsCall{startNodeID, endNodeID, kind, properties})
	return f.createRelByIDsErr
}

func (f *fakeBatch) DeleteRelationship(id graph.ID) error {
	f.deleteRelationshipCalls = append(f.deleteRelationshipCalls, id)
	return f.deleteRelationshipErr
}

func (f *fakeBatch) UpdateRelationshipBy(update graph.RelationshipUpdate) error {
	f.updateRelationshipByCalls = append(f.updateRelationshipByCalls, update)
	return f.updateRelationshipByErr
}

func (f *fakeBatch) Commit() error {
	f.commitCalls++
	return f.commitErr
}

var _ graph.Batch = (*fakeBatch)(nil)

// kindsEqual reports whether got and want name the same kinds in the same
// order -- exact enough for these tests, which control construction order
// on both sides.
func kindsEqual(got, want graph.Kinds) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].String() != want[i].String() {
			return false
		}
	}
	return true
}

// assertScope fails the test unless scope's exported inspection accessors
// (engine.WriteScope's TouchedNodeKinds/TouchedEdgeKinds/TouchedAllNodes/
// TouchedAllEdges/DeletedNodes/DeletedEdges) report exactly the given
// dimensions -- nil for wantNodeKinds/wantEdgeKinds/wantDeletedNodes/
// wantDeletedEdges means "none". This exists so a test can assert the EXACT
// dimension an override touched, rather than only scope.Empty()'s coarse
// "touched something": a regression that marks the wrong dimension (e.g. a
// CreateNode that accidentally touched edgeKinds instead of nodeKinds) would
// still pass an Empty()-only assertion but is caught here.
func assertScope(t *testing.T, scope *engine.WriteScope, wantNodeKinds, wantEdgeKinds []string, wantAllNodes, wantAllEdges bool, wantDeletedNodes, wantDeletedEdges []graph.ID) {
	t.Helper()

	if got := scope.TouchedNodeKinds(); !reflect.DeepEqual(got, wantNodeKinds) {
		t.Fatalf("TouchedNodeKinds() = %v, want %v", got, wantNodeKinds)
	}
	if got := scope.TouchedEdgeKinds(); !reflect.DeepEqual(got, wantEdgeKinds) {
		t.Fatalf("TouchedEdgeKinds() = %v, want %v", got, wantEdgeKinds)
	}
	if got := scope.TouchedAllNodes(); got != wantAllNodes {
		t.Fatalf("TouchedAllNodes() = %v, want %v", got, wantAllNodes)
	}
	if got := scope.TouchedAllEdges(); got != wantAllEdges {
		t.Fatalf("TouchedAllEdges() = %v, want %v", got, wantAllEdges)
	}
	if got := scope.DeletedNodes(); !reflect.DeepEqual(got, wantDeletedNodes) {
		t.Fatalf("DeletedNodes() = %v, want %v", got, wantDeletedNodes)
	}
	if got := scope.DeletedEdges(); !reflect.DeepEqual(got, wantDeletedEdges) {
		t.Fatalf("DeletedEdges() = %v, want %v", got, wantDeletedEdges)
	}
}

// relVariable and nodeVariable build fresh *cypher.Variable values for the
// relationship ("r") and node ("n") symbols dawgs/query's constructors
// attach -- see relationshipKindMatcherKinds' doc.
func relVariable() *cypher.Variable  { return &cypher.Variable{Symbol: "r"} }
func nodeVariable() *cypher.Variable { return &cypher.Variable{Symbol: "n"} }

// -----------------------------------------------------------------------
// cypherMutates
// -----------------------------------------------------------------------

func TestCypherMutates(t *testing.T) {
	cases := []struct {
		name string
		text string
		want bool
	}{
		{"plain match return", "MATCH (n) RETURN n", false},
		{"set", "MATCH (n) SET n.x = 1", true},
		{"create", "CREATE (n)", true},
		{"garbage", "not even close to cypher {{{", true},
		{"empty text", "", true},
		{"detach delete", "MATCH (n) DETACH DELETE n", true},
		{"remove", "MATCH (n) REMOVE n:Foo RETURN n", true},
		{"merge", "MERGE (n:Foo {id: 1})", true},
		{"multi-part read only", "MATCH (n) WITH n MATCH (n)-[r]->(m) RETURN m", false},
		{"multi-part mutation in tail", "MATCH (n) WITH n SET n.x = 1 RETURN n", true},
		{"multi-part mutation before with boundary", "MATCH (n) SET n.x = 1 WITH n MATCH (n)-[r]->(m) RETURN m", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cypherMutates(tc.text); got != tc.want {
				t.Fatalf("cypherMutates(%q) = %v, want %v", tc.text, got, tc.want)
			}
		})
	}
}

// -----------------------------------------------------------------------
// relationshipKindMatcherKinds / edgeKindsFromCriteria
// -----------------------------------------------------------------------

func TestRelationshipKindMatcherKinds(t *testing.T) {
	hasSession := graph.StringKind("HasSession")

	if _, ok := relationshipKindMatcherKinds(nil); ok {
		t.Fatalf("nil KindMatcher recognized, want ok=false")
	}

	if kinds, ok := relationshipKindMatcherKinds(cypher.NewKindMatcher(relVariable(), graph.Kinds{hasSession}, false)); !ok {
		t.Fatalf("KindMatcher over the relationship variable not recognized")
	} else if !kindsEqual(kinds, graph.Kinds{hasSession}) {
		t.Fatalf("kinds = %v, want %v", kinds, graph.Kinds{hasSession})
	}

	if _, ok := relationshipKindMatcherKinds(cypher.NewKindMatcher(nodeVariable(), graph.Kinds{hasSession}, false)); ok {
		t.Fatalf("KindMatcher over the node variable recognized, want ok=false")
	}

	if _, ok := relationshipKindMatcherKinds(cypher.NewKindMatcher(&cypher.Literal{Value: "r"}, graph.Kinds{hasSession}, false)); ok {
		t.Fatalf("KindMatcher over a non-Variable reference recognized, want ok=false")
	}
}

func TestEdgeKindsFromCriteria(t *testing.T) {
	hasSession := graph.StringKind("HasSession")
	adminTo := graph.StringKind("AdminTo")

	cases := []struct {
		name      string
		criteria  graph.Criteria
		wantKinds graph.Kinds
		wantOK    bool
	}{
		{
			name:      "bare relationship kind matcher",
			criteria:  cypher.NewKindMatcher(relVariable(), graph.Kinds{hasSession}, false),
			wantKinds: graph.Kinds{hasSession},
			wantOK:    true,
		},
		{
			name:     "bare kind matcher over node variable",
			criteria: cypher.NewKindMatcher(nodeVariable(), graph.Kinds{hasSession}, false),
			wantOK:   false,
		},
		{
			name:      "bare relationship kind matcher with empty kinds is still recognized",
			criteria:  cypher.NewKindMatcher(relVariable(), nil, false),
			wantKinds: nil,
			wantOK:    true,
		},
		{
			name: "conjunction with one relationship kind matcher among other conjuncts",
			criteria: cypher.NewConjunction(
				cypher.NewKindMatcher(nodeVariable(), graph.Kinds{hasSession}, false),
				cypher.NewKindMatcher(relVariable(), graph.Kinds{adminTo}, false),
			),
			wantKinds: graph.Kinds{adminTo},
			wantOK:    true,
		},
		{
			name: "conjunction with two relationship kind matchers unions",
			criteria: cypher.NewConjunction(
				cypher.NewKindMatcher(relVariable(), graph.Kinds{hasSession}, false),
				cypher.NewKindMatcher(relVariable(), graph.Kinds{adminTo}, false),
			),
			wantKinds: graph.Kinds{hasSession, adminTo},
			wantOK:    true,
		},
		{
			name: "conjunction with no relationship kind matcher",
			criteria: cypher.NewConjunction(
				cypher.NewKindMatcher(nodeVariable(), graph.Kinds{hasSession}, false),
			),
			wantOK: false,
		},
		{
			name:     "nil conjunction",
			criteria: (*cypher.Conjunction)(nil),
			wantOK:   false,
		},
		{
			name:     "unrelated criteria shape",
			criteria: "not-a-cypher-node",
			wantOK:   false,
		},
		{
			name:     "nil criteria",
			criteria: nil,
			wantOK:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotKinds, gotOK := edgeKindsFromCriteria(tc.criteria)
			if gotOK != tc.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tc.wantOK)
			}
			if tc.wantOK && !kindsEqual(gotKinds, tc.wantKinds) {
				t.Fatalf("kinds = %v, want %v", gotKinds, tc.wantKinds)
			}
		})
	}
}

// -----------------------------------------------------------------------
// relationshipDeleteScope
// -----------------------------------------------------------------------

func TestRelationshipDeleteScope(t *testing.T) {
	hasSession := graph.StringKind("HasSession")

	cases := []struct {
		name         string
		criteria     []graph.Criteria
		wantKinds    graph.Kinds
		wantTouchAll bool
	}{
		{"no criteria", nil, nil, true},
		{"two criteria", []graph.Criteria{graph.Criteria("a"), graph.Criteria("b")}, nil, true},
		{
			"one recognized criteria",
			[]graph.Criteria{cypher.NewKindMatcher(relVariable(), graph.Kinds{hasSession}, false)},
			graph.Kinds{hasSession},
			false,
		},
		{
			"one recognized criteria with empty kinds falls back to touchAll",
			[]graph.Criteria{cypher.NewKindMatcher(relVariable(), nil, false)},
			nil,
			true,
		},
		{
			"one unrecognized criteria",
			[]graph.Criteria{graph.Criteria("not-a-conjunction")},
			nil,
			true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotKinds, gotTouchAll := relationshipDeleteScope(tc.criteria)
			if gotTouchAll != tc.wantTouchAll {
				t.Fatalf("touchAll = %v, want %v", gotTouchAll, tc.wantTouchAll)
			}
			if !tc.wantTouchAll && !kindsEqual(gotKinds, tc.wantKinds) {
				t.Fatalf("kinds = %v, want %v", gotKinds, tc.wantKinds)
			}
		})
	}
}

// -----------------------------------------------------------------------
// observingTransaction
// -----------------------------------------------------------------------

func newObservingTransaction(inner graph.Transaction) (*observingTransaction, *engine.WriteScope) {
	scope := engine.NewWriteScope()
	return &observingTransaction{Transaction: inner, scope: scope}, scope
}

func TestObservingTransactionCreateNodeTouchesScopeAndDelegates(t *testing.T) {
	wantNode := &graph.Node{ID: 1}
	inner := &fakeTransaction{createNodeReturn: wantNode}
	tx, scope := newObservingTransaction(inner)

	kind := graph.StringKind("User")
	node, err := tx.CreateNode(graph.NewProperties(), kind)
	if err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if node != wantNode {
		t.Fatalf("CreateNode did not return the inner transaction's node")
	}
	if len(inner.createNodeKinds) != 1 || len(inner.createNodeKinds[0]) != 1 || inner.createNodeKinds[0][0] != kind {
		t.Fatalf("CreateNode did not delegate kinds correctly: %v", inner.createNodeKinds)
	}
	assertScope(t, scope, []string{kind.String()}, nil, false, false, nil, nil)
}

func TestObservingTransactionCreateNodeNoKindsLeavesScopeEmpty(t *testing.T) {
	inner := &fakeTransaction{}
	tx, scope := newObservingTransaction(inner)

	if _, err := tx.CreateNode(graph.NewProperties()); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if !scope.Empty() {
		t.Fatalf("CreateNode with no kinds touched scope, want untouched")
	}
}

func TestObservingTransactionUpdateNodePropertyOnlyLeavesScopeEmpty(t *testing.T) {
	inner := &fakeTransaction{}
	tx, scope := newObservingTransaction(inner)

	node := &graph.Node{ID: 1, Properties: graph.NewProperties()}
	if err := tx.UpdateNode(node); err != nil {
		t.Fatalf("UpdateNode: unexpected error: %v", err)
	}
	if len(inner.updateNodeCalls) != 1 || inner.updateNodeCalls[0] != node {
		t.Fatalf("UpdateNode did not delegate: %v", inner.updateNodeCalls)
	}
	if !scope.Empty() {
		t.Fatalf("property-only UpdateNode touched scope, want untouched")
	}
}

func TestObservingTransactionUpdateNodeAddedKindsTouchesScope(t *testing.T) {
	inner := &fakeTransaction{}
	tx, scope := newObservingTransaction(inner)

	node := &graph.Node{ID: 1, AddedKinds: graph.Kinds{graph.StringKind("Admin")}}
	if err := tx.UpdateNode(node); err != nil {
		t.Fatalf("UpdateNode: unexpected error: %v", err)
	}
	assertScope(t, scope, []string{"Admin"}, nil, false, false, nil, nil)
}

func TestObservingTransactionUpdateNodeDeletedKindsTouchesScope(t *testing.T) {
	inner := &fakeTransaction{}
	tx, scope := newObservingTransaction(inner)

	node := &graph.Node{ID: 1, DeletedKinds: graph.Kinds{graph.StringKind("Admin")}}
	if err := tx.UpdateNode(node); err != nil {
		t.Fatalf("UpdateNode: unexpected error: %v", err)
	}
	assertScope(t, scope, []string{"Admin"}, nil, false, false, nil, nil)
}

func TestObservingTransactionCreateRelationshipByIDsTouchesScopeAndDelegates(t *testing.T) {
	wantRel := &graph.Relationship{ID: 2}
	inner := &fakeTransaction{createRelByIDsReturn: wantRel}
	tx, scope := newObservingTransaction(inner)

	kind := graph.StringKind("HasSession")
	rel, err := tx.CreateRelationshipByIDs(1, 2, kind, graph.NewProperties())
	if err != nil {
		t.Fatalf("CreateRelationshipByIDs: unexpected error: %v", err)
	}
	if rel != wantRel {
		t.Fatalf("CreateRelationshipByIDs did not return the inner transaction's relationship")
	}
	if len(inner.createRelByIDsCalls) != 1 || inner.createRelByIDsCalls[0].kind != kind {
		t.Fatalf("CreateRelationshipByIDs did not delegate: %v", inner.createRelByIDsCalls)
	}
	assertScope(t, scope, nil, []string{kind.String()}, false, false, nil, nil)
}

func TestObservingTransactionUpdateRelationshipNotOverridden(t *testing.T) {
	inner := &fakeTransaction{}
	tx, scope := newObservingTransaction(inner)

	rel := &graph.Relationship{ID: 5}
	if err := tx.UpdateRelationship(rel); err != nil {
		t.Fatalf("UpdateRelationship: unexpected error: %v", err)
	}
	if len(inner.updateRelationshipCalls) != 1 || inner.updateRelationshipCalls[0] != rel {
		t.Fatalf("UpdateRelationship did not delegate: %v", inner.updateRelationshipCalls)
	}
	if !scope.Empty() {
		t.Fatalf("UpdateRelationship (property-only, not overridden) touched scope, want untouched")
	}
}

func TestObservingTransactionNodesReturnsObservingNodeQuery(t *testing.T) {
	innerNodes := &fakeNodeQuery{}
	inner := &fakeTransaction{nodesReturn: innerNodes}
	tx, scope := newObservingTransaction(inner)

	nq, ok := tx.Nodes().(*observingNodeQuery)
	if !ok {
		t.Fatalf("Nodes() returned %T, want *observingNodeQuery", tx.Nodes())
	}
	if nq.scope != scope {
		t.Fatalf("observingNodeQuery.scope is not the transaction's scope")
	}
	if nq.NodeQuery != innerNodes {
		t.Fatalf("observingNodeQuery does not wrap the inner transaction's NodeQuery")
	}
}

func TestObservingTransactionRelationshipsReturnsObservingRelationshipQuery(t *testing.T) {
	innerRel := &mockRelationshipQuery{}
	inner := &fakeTransaction{relationshipsReturn: innerRel}
	tx, scope := newObservingTransaction(inner)

	rq, ok := tx.Relationships().(*observingRelationshipQuery)
	if !ok {
		t.Fatalf("Relationships() returned %T, want *observingRelationshipQuery", tx.Relationships())
	}
	if rq.scope != scope {
		t.Fatalf("observingRelationshipQuery.scope is not the transaction's scope")
	}
	if rq.RelationshipQuery != innerRel {
		t.Fatalf("observingRelationshipQuery does not wrap the inner transaction's RelationshipQuery")
	}
}

func TestObservingTransactionQueryMutationSniffAndAlwaysDelegates(t *testing.T) {
	cases := []struct {
		name        string
		text        string
		wantTouched bool
	}{
		{"read", "MATCH (n) RETURN n", false},
		{"mutating", "MATCH (n) SET n.x = 1", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &fakeTransaction{queryResult: graph.NewErrorResult(nil)}
			tx, scope := newObservingTransaction(inner)

			params := map[string]any{"x": 1}
			result := tx.Query(tc.text, params)

			if len(inner.queryCalls) != 1 || inner.queryCalls[0].query != tc.text {
				t.Fatalf("Query did not delegate: %v", inner.queryCalls)
			}
			if result != inner.queryResult {
				t.Fatalf("Query did not return the inner transaction's result")
			}
			if tc.wantTouched {
				// TouchAll marks both dimensions, unconditionally -- Query
				// cannot narrow a mutating Cypher statement to specific
				// kinds (cypherMutates' doc).
				assertScope(t, scope, nil, nil, true, true, nil, nil)
			} else {
				assertScope(t, scope, nil, nil, false, false, nil, nil)
			}
		})
	}
}

func TestObservingTransactionRawAlwaysTouchesScopeAndDelegates(t *testing.T) {
	inner := &fakeTransaction{rawResult: graph.NewErrorResult(nil)}
	tx, scope := newObservingTransaction(inner)

	result := tx.Raw("SELECT 1", nil)
	if len(inner.rawCalls) != 1 {
		t.Fatalf("Raw did not delegate: %v", inner.rawCalls)
	}
	if result != inner.rawResult {
		t.Fatalf("Raw did not return the inner transaction's result")
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)
}

func TestObservingTransactionWithGraphTouchesScopeAndKeepsObserving(t *testing.T) {
	retargeted := &fakeTransaction{}
	inner := &fakeTransaction{withGraphReturn: retargeted}
	tx, scope := newObservingTransaction(inner)

	got := tx.WithGraph(graph.Graph{Name: "other"})
	if inner.withGraphCalls != 1 {
		t.Fatalf("WithGraph did not delegate to the inner transaction")
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)

	wrapped, ok := got.(*observingTransaction)
	if !ok {
		t.Fatalf("WithGraph returned %T, want *observingTransaction", got)
	}
	if wrapped.scope != scope {
		t.Fatalf("WithGraph did not keep the same scope")
	}
	if wrapped.Transaction != retargeted {
		t.Fatalf("WithGraph did not wrap the inner transaction's WithGraph result")
	}

	// Keep observing: a call on the retargeted wrapper still reaches
	// retargeted and still accumulates onto the same scope.
	if _, err := wrapped.CreateNode(graph.NewProperties(), graph.StringKind("Foo")); err != nil {
		t.Fatalf("CreateNode on the retargeted wrapper: unexpected error: %v", err)
	}
	if len(retargeted.createNodeKinds) != 1 {
		t.Fatalf("WithGraph's result did not keep observing")
	}
}

// -----------------------------------------------------------------------
// observingNodeQuery
// -----------------------------------------------------------------------

func TestObservingNodeQueryFilterRecordsDelegatesAndRewraps(t *testing.T) {
	inner := &fakeNodeQuery{}
	nq := &observingNodeQuery{NodeQuery: inner, scope: engine.NewWriteScope()}

	criteria := graph.Criteria("crit")
	got := nq.Filter(criteria)
	if _, ok := got.(*observingNodeQuery); !ok {
		t.Fatalf("Filter returned %T, want *observingNodeQuery", got)
	}
	if len(inner.filterCalls) != 1 || inner.filterCalls[0] != criteria {
		t.Fatalf("Filter did not delegate to the inner query: %v", inner.filterCalls)
	}
}

func TestObservingNodeQueryFilterfDelegatesAndRewraps(t *testing.T) {
	inner := &fakeNodeQuery{}
	nq := &observingNodeQuery{NodeQuery: inner, scope: engine.NewWriteScope()}

	provider := func() graph.Criteria { return graph.Criteria("crit") }
	got := nq.Filterf(provider)
	if _, ok := got.(*observingNodeQuery); !ok {
		t.Fatalf("Filterf returned %T, want *observingNodeQuery", got)
	}
	if inner.filterfCalls != 1 {
		t.Fatalf("Filterf did not delegate to the inner query")
	}
}

func TestObservingNodeQueryDeleteTouchesScopeAndDelegates(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	if err := nq.Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.deleteCalls != 1 {
		t.Fatalf("Delete did not delegate to the inner query")
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)
}

func TestObservingNodeQueryUpdatePromotedWithoutTouchingScope(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	if err := nq.Update(graph.NewProperties()); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if inner.updateCalls != 1 {
		t.Fatalf("Update did not delegate to the inner query")
	}
	if !scope.Empty() {
		t.Fatalf("Update (property-only, promoted) touched scope, want untouched")
	}
}

func TestObservingNodeQueryOrderByDelegatesAndRewraps(t *testing.T) {
	inner := &fakeNodeQuery{}
	nq := &observingNodeQuery{NodeQuery: inner, scope: engine.NewWriteScope()}

	got := nq.OrderBy(graph.Criteria("x"))
	if _, ok := got.(*observingNodeQuery); !ok {
		t.Fatalf("OrderBy returned %T, want *observingNodeQuery", got)
	}
	if inner.orderByCalls != 1 {
		t.Fatalf("OrderBy did not delegate to the inner query")
	}
}

func TestObservingNodeQueryOffsetDelegatesAndRewraps(t *testing.T) {
	inner := &fakeNodeQuery{}
	nq := &observingNodeQuery{NodeQuery: inner, scope: engine.NewWriteScope()}

	got := nq.Offset(5)
	if _, ok := got.(*observingNodeQuery); !ok {
		t.Fatalf("Offset returned %T, want *observingNodeQuery", got)
	}
	if inner.offsetCalls != 1 {
		t.Fatalf("Offset did not delegate to the inner query")
	}
}

func TestObservingNodeQueryLimitDelegatesAndRewraps(t *testing.T) {
	inner := &fakeNodeQuery{}
	nq := &observingNodeQuery{NodeQuery: inner, scope: engine.NewWriteScope()}

	got := nq.Limit(5)
	if _, ok := got.(*observingNodeQuery); !ok {
		t.Fatalf("Limit returned %T, want *observingNodeQuery", got)
	}
	if inner.limitCalls != 1 {
		t.Fatalf("Limit did not delegate to the inner query")
	}
}

// TestObservingNodeQueryFilterOrderByDeleteStaysObservedAndTouchesScope is
// the regression test for the critical finding fixed alongside these
// overrides: before OrderBy re-wrapped, Filter(x).OrderBy(y) returned the
// bare inner graph.NodeQuery (whatever the pg implementation's own OrderBy
// returns for chaining, unwrapped), so a trailing .Delete() silently
// escaped observation and scope never learned about the delete.
func TestObservingNodeQueryFilterOrderByDeleteStaysObservedAndTouchesScope(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	chained := nq.Filter(graph.Criteria("x")).OrderBy(graph.Criteria("y"))
	if _, ok := chained.(*observingNodeQuery); !ok {
		t.Fatalf("Filter(...).OrderBy(...) returned %T, want *observingNodeQuery", chained)
	}
	if err := chained.Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.deleteCalls != 1 {
		t.Fatalf("Delete did not reach the inner query: deleteCalls = %d", inner.deleteCalls)
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)
}

// TestObservingNodeQueryOffsetDeleteStaysObservedAndTouchesScope is Offset's
// equivalent of the OrderBy regression test above.
func TestObservingNodeQueryOffsetDeleteStaysObservedAndTouchesScope(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	if err := nq.Offset(1).Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.offsetCalls != 1 || inner.deleteCalls != 1 {
		t.Fatalf("Offset/Delete did not both reach the inner query: offsetCalls=%d deleteCalls=%d", inner.offsetCalls, inner.deleteCalls)
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)
}

// TestObservingNodeQueryLimitDeleteStaysObservedAndTouchesScope is Limit's
// equivalent of the OrderBy regression test above.
func TestObservingNodeQueryLimitDeleteStaysObservedAndTouchesScope(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	if err := nq.Limit(1).Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.limitCalls != 1 || inner.deleteCalls != 1 {
		t.Fatalf("Limit/Delete did not both reach the inner query: limitCalls=%d deleteCalls=%d", inner.limitCalls, inner.deleteCalls)
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)
}

// -----------------------------------------------------------------------
// observingRelationshipQuery
// -----------------------------------------------------------------------

func TestObservingRelationshipQueryFilterRecordsDelegatesAndRewraps(t *testing.T) {
	inner := &mockRelationshipQuery{}
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: engine.NewWriteScope()}

	criteria := graph.Criteria("crit")
	got := rq.Filter(criteria)
	orq, ok := got.(*observingRelationshipQuery)
	if !ok {
		t.Fatalf("Filter returned %T, want *observingRelationshipQuery", got)
	}
	if len(orq.criteria) != 1 || orq.criteria[0] != criteria {
		t.Fatalf("recorded criteria = %v, want [%v]", orq.criteria, criteria)
	}
	if len(inner.filterCalls) != 1 || inner.filterCalls[0] != criteria {
		t.Fatalf("Filter did not delegate to the inner query: %v", inner.filterCalls)
	}
}

func TestObservingRelationshipQueryFilterfRecordsAndPassesProviderThrough(t *testing.T) {
	inner := &mockRelationshipQuery{}
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: engine.NewWriteScope()}

	criteria := graph.Criteria("crit")
	providerCalls := 0
	provider := func() graph.Criteria {
		providerCalls++
		return criteria
	}

	got := rq.Filterf(provider)
	orq, ok := got.(*observingRelationshipQuery)
	if !ok {
		t.Fatalf("Filterf returned %T, want *observingRelationshipQuery", got)
	}
	if len(orq.criteria) != 1 || orq.criteria[0] != criteria {
		t.Fatalf("recorded criteria = %v, want [%v]", orq.criteria, criteria)
	}
	if inner.filterfCalls != 1 {
		t.Fatalf("Filterf did not delegate to the inner query")
	}
	// Called once by observingRelationshipQuery.Filterf itself (to record
	// the result) and once more inside the inner mock's own Filterf --
	// proving the *same* provider was passed through, mirroring
	// recordingRelationshipQuery's identical contract.
	if providerCalls != 2 {
		t.Fatalf("provider called %d times, want exactly 2 (record + inner delegate)", providerCalls)
	}
}

func TestObservingRelationshipQueryDeleteRecognizedKindTouchesScopeAndDelegates(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	kind := graph.StringKind("HasSession")
	rq.Filter(cypher.NewKindMatcher(relVariable(), graph.Kinds{kind}, false))

	if err := rq.Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.deleteCalls != 1 {
		t.Fatalf("Delete did not delegate to the inner query")
	}
	assertScope(t, scope, nil, []string{kind.String()}, false, false, nil, nil)
}

func TestObservingRelationshipQueryDeleteUnrecognizedTouchesScopeAndDelegates(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	// No Filter call at all: len(criteria) == 0, so Delete must fall back to
	// TouchAllEdges (verified precisely by TestRelationshipDeleteScope; this
	// test only checks the method's outward behavior).
	if err := rq.Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.deleteCalls != 1 {
		t.Fatalf("Delete did not delegate to the inner query")
	}
	assertScope(t, scope, nil, nil, false, true, nil, nil)
}

func TestObservingRelationshipQueryUpdatePromotedWithoutTouchingScope(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	if err := rq.Update(graph.NewProperties()); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if inner.updateCalls != 1 {
		t.Fatalf("Update did not delegate to the inner query")
	}
	if !scope.Empty() {
		t.Fatalf("Update (property-only, promoted) touched scope, want untouched")
	}
}

func TestObservingRelationshipQueryOrderByDelegatesAndRewraps(t *testing.T) {
	inner := &mockRelationshipQuery{}
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: engine.NewWriteScope()}

	got := rq.OrderBy(graph.Criteria("x"))
	if _, ok := got.(*observingRelationshipQuery); !ok {
		t.Fatalf("OrderBy returned %T, want *observingRelationshipQuery", got)
	}
	if inner.orderByCalls != 1 {
		t.Fatalf("OrderBy did not delegate to the inner query")
	}
}

func TestObservingRelationshipQueryOffsetDelegatesAndRewraps(t *testing.T) {
	inner := &mockRelationshipQuery{}
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: engine.NewWriteScope()}

	got := rq.Offset(5)
	if _, ok := got.(*observingRelationshipQuery); !ok {
		t.Fatalf("Offset returned %T, want *observingRelationshipQuery", got)
	}
	if inner.offsetCalls != 1 {
		t.Fatalf("Offset did not delegate to the inner query")
	}
}

func TestObservingRelationshipQueryLimitDelegatesAndRewraps(t *testing.T) {
	inner := &mockRelationshipQuery{}
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: engine.NewWriteScope()}

	got := rq.Limit(5)
	if _, ok := got.(*observingRelationshipQuery); !ok {
		t.Fatalf("Limit returned %T, want *observingRelationshipQuery", got)
	}
	if inner.limitCalls != 1 {
		t.Fatalf("Limit did not delegate to the inner query")
	}
}

// TestObservingRelationshipQueryFilterOrderByDeleteStaysObservedAndTouchesScope
// is the observingRelationshipQuery half of the critical finding's
// regression test (see observingNodeQuery's identical-purpose test for the
// full explanation): before OrderBy re-wrapped, Filter(x).OrderBy(y) handed
// back the bare inner graph.RelationshipQuery, so a trailing .Delete() never
// reached this wrapper and scope never learned about the delete.
func TestObservingRelationshipQueryFilterOrderByDeleteStaysObservedAndTouchesScope(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	kind := graph.StringKind("HasSession")
	chained := rq.Filter(cypher.NewKindMatcher(relVariable(), graph.Kinds{kind}, false)).OrderBy(graph.Criteria("y"))
	if _, ok := chained.(*observingRelationshipQuery); !ok {
		t.Fatalf("Filter(...).OrderBy(...) returned %T, want *observingRelationshipQuery", chained)
	}
	if err := chained.Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.deleteCalls != 1 {
		t.Fatalf("Delete did not reach the inner query: deleteCalls = %d", inner.deleteCalls)
	}
	// The single recognized-kind criteria recorded by Filter survives the
	// OrderBy hop, so Delete still narrows to that kind instead of falling
	// back to TouchAllEdges.
	assertScope(t, scope, nil, []string{kind.String()}, false, false, nil, nil)
}

// TestObservingRelationshipQueryOffsetDeleteStaysObservedAndTouchesScope is
// Offset's equivalent of the OrderBy regression test above.
func TestObservingRelationshipQueryOffsetDeleteStaysObservedAndTouchesScope(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	if err := rq.Offset(1).Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.offsetCalls != 1 || inner.deleteCalls != 1 {
		t.Fatalf("Offset/Delete did not both reach the inner query: offsetCalls=%d deleteCalls=%d", inner.offsetCalls, inner.deleteCalls)
	}
	// No criteria recorded at all, so Delete falls back to TouchAllEdges.
	assertScope(t, scope, nil, nil, false, true, nil, nil)
}

// TestObservingRelationshipQueryLimitDeleteStaysObservedAndTouchesScope is
// Limit's equivalent of the OrderBy regression test above.
func TestObservingRelationshipQueryLimitDeleteStaysObservedAndTouchesScope(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	if err := rq.Limit(1).Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.limitCalls != 1 || inner.deleteCalls != 1 {
		t.Fatalf("Limit/Delete did not both reach the inner query: limitCalls=%d deleteCalls=%d", inner.limitCalls, inner.deleteCalls)
	}
	assertScope(t, scope, nil, nil, false, true, nil, nil)
}

// -----------------------------------------------------------------------
// observingBatch
// -----------------------------------------------------------------------

func TestObservingBatchCreateNodeTouchesScopeAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	node := &graph.Node{Kinds: graph.Kinds{graph.StringKind("User")}}
	if err := b.CreateNode(node); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if len(inner.createNodeCalls) != 1 || inner.createNodeCalls[0] != node {
		t.Fatalf("CreateNode did not delegate: %v", inner.createNodeCalls)
	}
	assertScope(t, scope, []string{"User"}, nil, false, false, nil, nil)
}

func TestObservingBatchDeleteNodeRecordsIDAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	if err := b.DeleteNode(graph.ID(7)); err != nil {
		t.Fatalf("DeleteNode: unexpected error: %v", err)
	}
	if len(inner.deleteNodeCalls) != 1 || inner.deleteNodeCalls[0] != graph.ID(7) {
		t.Fatalf("DeleteNode did not delegate: %v", inner.deleteNodeCalls)
	}
	assertScope(t, scope, nil, nil, false, false, []graph.ID{7}, nil)
}

func TestObservingBatchNodesAndRelationshipsReturnObservingWrappers(t *testing.T) {
	innerNodes := &fakeNodeQuery{}
	innerRel := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	b := &observingBatch{
		Batch: &fakeBatch{nodesReturn: innerNodes, relationshipsReturn: innerRel},
		scope: scope,
		eng:   disabledEngine(),
	}

	nq, ok := b.Nodes().(*observingNodeQuery)
	if !ok {
		t.Fatalf("Nodes() returned %T, want *observingNodeQuery", b.Nodes())
	}
	if nq.scope != scope || nq.NodeQuery != innerNodes {
		t.Fatalf("Nodes() did not wrap the inner batch's NodeQuery with the batch's scope")
	}

	rq, ok := b.Relationships().(*observingRelationshipQuery)
	if !ok {
		t.Fatalf("Relationships() returned %T, want *observingRelationshipQuery", b.Relationships())
	}
	if rq.scope != scope || rq.RelationshipQuery != innerRel {
		t.Fatalf("Relationships() did not wrap the inner batch's RelationshipQuery with the batch's scope")
	}
}

func TestObservingBatchUpdateNodeByTouchesBaseKindsAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	// Base Kinds only, no Added/DeletedKinds -- must still touch scope,
	// since UpdateNodeBy is an upsert that may be creating this node.
	update := graph.NodeUpdate{Node: &graph.Node{Kinds: graph.Kinds{graph.StringKind("Base")}}}
	if err := b.UpdateNodeBy(update); err != nil {
		t.Fatalf("UpdateNodeBy: unexpected error: %v", err)
	}
	if len(inner.updateNodeByCalls) != 1 {
		t.Fatalf("UpdateNodeBy did not delegate")
	}
	assertScope(t, scope, []string{"Base"}, nil, false, false, nil, nil)
}

func TestObservingBatchUpdateNodesNoKindDeltaLeavesScopeEmpty(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	nodes := []*graph.Node{{Properties: graph.NewProperties()}}
	if err := b.UpdateNodes(nodes); err != nil {
		t.Fatalf("UpdateNodes: unexpected error: %v", err)
	}
	if len(inner.updateNodesCalls) != 1 {
		t.Fatalf("UpdateNodes did not delegate")
	}
	if !scope.Empty() {
		t.Fatalf("UpdateNodes with no kind delta touched scope, want untouched")
	}
}

func TestObservingBatchUpdateNodesKindDeltaTouchesScope(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	nodes := []*graph.Node{{AddedKinds: graph.Kinds{graph.StringKind("Admin")}}}
	if err := b.UpdateNodes(nodes); err != nil {
		t.Fatalf("UpdateNodes: unexpected error: %v", err)
	}
	assertScope(t, scope, []string{"Admin"}, nil, false, false, nil, nil)
}

func TestObservingBatchCreateRelationshipTouchesScopeAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	rel := &graph.Relationship{Kind: graph.StringKind("HasSession")}
	if err := b.CreateRelationship(rel); err != nil {
		t.Fatalf("CreateRelationship: unexpected error: %v", err)
	}
	if len(inner.createRelationshipCalls) != 1 || inner.createRelationshipCalls[0] != rel {
		t.Fatalf("CreateRelationship did not delegate: %v", inner.createRelationshipCalls)
	}
	assertScope(t, scope, nil, []string{"HasSession"}, false, false, nil, nil)
}

func TestObservingBatchCreateRelationshipByIDsTouchesScopeAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	kind := graph.StringKind("AdminTo")
	if err := b.CreateRelationshipByIDs(1, 2, kind, graph.NewProperties()); err != nil {
		t.Fatalf("CreateRelationshipByIDs: unexpected error: %v", err)
	}
	if len(inner.createRelByIDsCalls) != 1 || inner.createRelByIDsCalls[0].kind != kind {
		t.Fatalf("CreateRelationshipByIDs did not delegate: %v", inner.createRelByIDsCalls)
	}
	assertScope(t, scope, nil, []string{kind.String()}, false, false, nil, nil)
}

func TestObservingBatchDeleteRelationshipRecordsIDAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	if err := b.DeleteRelationship(graph.ID(3)); err != nil {
		t.Fatalf("DeleteRelationship: unexpected error: %v", err)
	}
	if len(inner.deleteRelationshipCalls) != 1 || inner.deleteRelationshipCalls[0] != graph.ID(3) {
		t.Fatalf("DeleteRelationship did not delegate: %v", inner.deleteRelationshipCalls)
	}
	assertScope(t, scope, nil, nil, false, false, nil, []graph.ID{3})
}

func TestObservingBatchUpdateRelationshipByTouchesScopeAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	update := graph.RelationshipUpdate{Relationship: &graph.Relationship{Kind: graph.StringKind("MemberOf")}}
	if err := b.UpdateRelationshipBy(update); err != nil {
		t.Fatalf("UpdateRelationshipBy: unexpected error: %v", err)
	}
	if len(inner.updateRelationshipByCalls) != 1 {
		t.Fatalf("UpdateRelationshipBy did not delegate")
	}
	assertScope(t, scope, nil, []string{"MemberOf"}, false, false, nil, nil)
}

func TestObservingBatchWithGraphTouchesScopeAndKeepsObserving(t *testing.T) {
	retargeted := &fakeBatch{}
	inner := &fakeBatch{withGraphReturn: retargeted}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	got := b.WithGraph(graph.Graph{Name: "other"})
	if inner.withGraphCalls != 1 {
		t.Fatalf("WithGraph did not delegate to the inner batch")
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)

	wrapped, ok := got.(*observingBatch)
	if !ok {
		t.Fatalf("WithGraph returned %T, want *observingBatch", got)
	}
	if wrapped.scope != scope {
		t.Fatalf("WithGraph did not keep the same scope")
	}
	if wrapped.Batch != retargeted {
		t.Fatalf("WithGraph did not wrap the inner batch's WithGraph result")
	}

	if err := wrapped.CreateNode(&graph.Node{Kinds: graph.Kinds{graph.StringKind("Foo")}}); err != nil {
		t.Fatalf("CreateNode on the retargeted wrapper: unexpected error: %v", err)
	}
	if len(retargeted.createNodeCalls) != 1 {
		t.Fatalf("WithGraph's result did not keep observing")
	}
}

func TestObservingBatchCommitFlushesNowAndResetsScope(t *testing.T) {
	inner := &fakeBatch{}
	eng := disabledEngine()
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: eng}

	if err := b.CreateNode(&graph.Node{Kinds: graph.Kinds{graph.StringKind("User")}}); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}

	genBefore := eng.Generation()
	if err := b.Commit(); err != nil {
		t.Fatalf("Commit: unexpected error: %v", err)
	}
	if inner.commitCalls != 1 {
		t.Fatalf("Commit did not delegate to the inner batch")
	}
	if got := eng.Generation(); got != genBefore+1 {
		t.Fatalf("Commit did not bump the engine generation immediately: got %d, want %d", got, genBefore+1)
	}
	if b.scope == scope {
		t.Fatalf("Commit did not replace scope with a new instance")
	}
	if !b.scope.Empty() {
		t.Fatalf("Commit did not reset scope to a fresh, empty WriteScope")
	}

	// Writes after the mid-batch commit are still observed, into the new
	// scope.
	if err := b.CreateNode(&graph.Node{Kinds: graph.Kinds{graph.StringKind("Computer")}}); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if b.scope.Empty() {
		t.Fatalf("post-Commit writes were not observed into the new scope")
	}
}

func TestObservingBatchCommitBumpsGenerationEvenWithEmptyScope(t *testing.T) {
	inner := &fakeBatch{}
	eng := disabledEngine()
	b := &observingBatch{Batch: inner, scope: engine.NewWriteScope(), eng: eng}

	genBefore := eng.Generation()
	if err := b.Commit(); err != nil {
		t.Fatalf("Commit: unexpected error: %v", err)
	}
	if got := eng.Generation(); got != genBefore+1 {
		t.Fatalf("Commit with an empty scope did not bump the generation: got %d, want %d", got, genBefore+1)
	}
}
