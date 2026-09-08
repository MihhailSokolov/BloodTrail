// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"errors"
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

// fakeNodeBatchCreator wraps a *fakeBatch, additionally implementing
// graph.NodeBatchCreator -- exercising observingBatch.CreateNodes'
// delegate-when-supported branch needs an inner batch that actually
// implements the optional interface; a plain *fakeBatch (which does not)
// exercises the other branch.
type fakeNodeBatchCreator struct {
	*fakeBatch

	createNodesCalls [][]*graph.Node
	createNodesIDs   []graph.ID
	createNodesErr   error
}

func (f *fakeNodeBatchCreator) CreateNodes(nodes []*graph.Node) ([]graph.ID, error) {
	f.createNodesCalls = append(f.createNodesCalls, nodes)
	return f.createNodesIDs, f.createNodesErr
}

var _ graph.NodeBatchCreator = (*fakeNodeBatchCreator)(nil)

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

// inIDsCriteria builds the exact AST shape query.InIDs(query.NodeID()/
// query.RelationshipID(), ids...) produces (dawgs' query package,
// query/model.go and query/identifiers.go), for tests exercising
// singleInIDsCriteria/nodeIDsFromCriteria/edgeIDsFromCriteria without
// taking a dawgs/query import -- mirroring relVariable/nodeVariable's own
// reasoning for hardcoding "r"/"n" rather than importing query.Relationship/
// query.Node.
func inIDsCriteria(symbol string, ids ...graph.ID) graph.Criteria {
	return &cypher.Comparison{
		Left: &cypher.FunctionInvocation{
			Name:      "id",
			Arguments: []cypher.Expression{&cypher.Variable{Symbol: symbol}},
		},
		Partials: []*cypher.PartialComparison{{
			Operator: cypher.OperatorIn,
			Right:    &cypher.Parameter{Value: ids},
		}},
	}
}

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

// TestCypherMutatesRecoversFromPanicByAssumingMutation covers cypherMutates'
// recover path directly: a panic from the Cypher frontend must make
// cypherMutates report true ("assume it mutates"), not false. This is worth
// a dedicated test because the two are easy to get backwards -- an unnamed
// bool return combined with a deferred `_ = recover()` compiles fine and
// looks conservative, but actually reports false (the return statement's
// already-computed zero value) on any panic, silently telling
// observingTransaction.Query "this does not mutate" for a query this
// package was completely unable to analyze. Finding real Cypher text known
// to crash the dawgs frontend isn't a dependency this test wants, so it
// stubs parseCypherFrontend (the seam this file's cypherMutates delegates
// parsing to) to panic unconditionally instead -- test-only, restored via
// t.Cleanup so no other test in this package observes the stub.
func TestCypherMutatesRecoversFromPanicByAssumingMutation(t *testing.T) {
	original := parseCypherFrontend
	t.Cleanup(func() { parseCypherFrontend = original })

	parseCypherFrontend = func(string) (*cypher.RegularQuery, error) {
		panic("simulated dawgs frontend panic")
	}

	if got := cypherMutates("MATCH (n) RETURN n"); !got {
		t.Fatalf("cypherMutates after a parser panic = %v, want true (conservative recover)", got)
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

// TestObservingTransactionCreateNodeRecordsReturnedNodeID covers the
// captured-return case CreateNode's doc describes: the pg-returned node's
// own id (only known from the return value, which this method used to
// discard) must land in the ChangeSet via RecordNodeID.
func TestObservingTransactionCreateNodeRecordsReturnedNodeID(t *testing.T) {
	wantNode := &graph.Node{ID: 42}
	inner := &fakeTransaction{createNodeReturn: wantNode}
	tx, scope := newObservingTransaction(inner)

	if _, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("User")); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if got, want := scope.Changes().NodeIDs(), []uint64{42}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
	}
}

// TestObservingTransactionCreateNodeErrorDoesNotRecordID covers the
// "AFTER a nil error check" half of CreateNode's doc: an error return must
// not record anything, even if the inner transaction also returned a
// non-nil node alongside it.
func TestObservingTransactionCreateNodeErrorDoesNotRecordID(t *testing.T) {
	wantErr := errors.New("boom")
	inner := &fakeTransaction{createNodeReturn: &graph.Node{ID: 7}, createNodeErr: wantErr}
	tx, scope := newObservingTransaction(inner)

	if _, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("User")); err != wantErr {
		t.Fatalf("CreateNode: error = %v, want %v", err, wantErr)
	}
	if got := scope.Changes().NodeIDs(); got != nil {
		t.Fatalf("Changes().NodeIDs() = %v, want nil (error return must not record)", got)
	}
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
	// Unlike the Touch*/Delete* dimensions above, RecordNodeID is
	// unconditional -- the applier needs to know this node id was written
	// to, regardless of whether the write happened to change kind
	// membership.
	if got, want := scope.Changes().NodeIDs(), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
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

// TestObservingTransactionCreateRelationshipByIDsRecordsReturnedEdgeID
// covers the captured-return case CreateRelationshipByIDs' doc describes:
// the pg-returned relationship's own id (only known from the return
// value, which this method used to discard) must land in the ChangeSet
// via RecordEdgeID.
func TestObservingTransactionCreateRelationshipByIDsRecordsReturnedEdgeID(t *testing.T) {
	wantRel := &graph.Relationship{ID: 99}
	inner := &fakeTransaction{createRelByIDsReturn: wantRel}
	tx, scope := newObservingTransaction(inner)

	if _, err := tx.CreateRelationshipByIDs(1, 2, graph.StringKind("HasSession"), graph.NewProperties()); err != nil {
		t.Fatalf("CreateRelationshipByIDs: unexpected error: %v", err)
	}
	if got, want := scope.Changes().EdgeIDs(), []uint64{99}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().EdgeIDs() = %v, want %v", got, want)
	}
}

// TestObservingTransactionCreateRelationshipByIDsErrorDoesNotRecordID is
// CreateNode's identical-purpose error-case test's relationship
// equivalent.
func TestObservingTransactionCreateRelationshipByIDsErrorDoesNotRecordID(t *testing.T) {
	wantErr := errors.New("boom")
	inner := &fakeTransaction{createRelByIDsReturn: &graph.Relationship{ID: 5}, createRelByIDsErr: wantErr}
	tx, scope := newObservingTransaction(inner)

	if _, err := tx.CreateRelationshipByIDs(1, 2, graph.StringKind("HasSession"), graph.NewProperties()); err != wantErr {
		t.Fatalf("CreateRelationshipByIDs: error = %v, want %v", err, wantErr)
	}
	if got := scope.Changes().EdgeIDs(); got != nil {
		t.Fatalf("Changes().EdgeIDs() = %v, want nil (error return must not record)", got)
	}
}

// TestObservingTransactionUpdateRelationshipRecordsEdgeIDAndDelegates covers
// C2's fix: UpdateRelationship was previously left entirely unobserved
// (promoted straight through by embedding), so a property-only relationship
// update produced an empty ChangeSet -- an applier could never learn the
// write happened at all. It is now overridden to record the relationship's
// own id unconditionally, while leaving the kind-scoped marks exactly as
// untouched as before: a relationship's Kind can never change after
// creation, so there is genuinely nothing for Touch* to reflect here.
func TestObservingTransactionUpdateRelationshipRecordsEdgeIDAndDelegates(t *testing.T) {
	inner := &fakeTransaction{}
	tx, scope := newObservingTransaction(inner)

	rel := &graph.Relationship{ID: 5}
	if err := tx.UpdateRelationship(rel); err != nil {
		t.Fatalf("UpdateRelationship: unexpected error: %v", err)
	}
	if len(inner.updateRelationshipCalls) != 1 || inner.updateRelationshipCalls[0] != rel {
		t.Fatalf("UpdateRelationship did not delegate: %v", inner.updateRelationshipCalls)
	}
	// Marks stay untouched: a property-only relationship update carries no
	// kind information, and Kind is immutable once created.
	if !scope.Empty() {
		t.Fatalf("UpdateRelationship (property-only) touched the Touch*/Delete* marks, want untouched")
	}
	if got, want := scope.Changes().EdgeIDs(), []uint64{5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().EdgeIDs() = %v, want %v", got, want)
	}
}

// TestObservingTransactionUpdateRelationshipNilDoesNotPanic covers the nil
// guard mirroring UpdateNode's own "if node != nil" check: UpdateRelationship
// must not dereference a nil relationship to read its ID, but should still
// delegate whatever the caller passed (nil included) to the inner
// transaction unchanged.
func TestObservingTransactionUpdateRelationshipNilDoesNotPanic(t *testing.T) {
	inner := &fakeTransaction{}
	tx, scope := newObservingTransaction(inner)

	if err := tx.UpdateRelationship(nil); err != nil {
		t.Fatalf("UpdateRelationship: unexpected error: %v", err)
	}
	if len(inner.updateRelationshipCalls) != 1 || inner.updateRelationshipCalls[0] != nil {
		t.Fatalf("UpdateRelationship did not delegate nil: %v", inner.updateRelationshipCalls)
	}
	if !scope.Changes().Empty() {
		t.Fatalf("UpdateRelationship(nil) touched the ChangeSet, want untouched")
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
				if ok, _ := scope.Changes().HasFallback(); !ok {
					t.Fatalf("HasFallback() = false after a mutating Query, want true")
				}
			} else {
				assertScope(t, scope, nil, nil, false, false, nil, nil)
				if ok, _ := scope.Changes().HasFallback(); ok {
					t.Fatalf("HasFallback() = true after a non-mutating Query, want false")
				}
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
	if ok, _ := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false after Raw, want true")
	}
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
	if ok, _ := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false after WithGraph, want true")
	}

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

func TestObservingTransactionWithGraphKeepsEng(t *testing.T) {
	retargeted := &fakeTransaction{}
	inner := &fakeTransaction{withGraphReturn: retargeted}
	eng := disabledEngine()
	tx := &observingTransaction{Transaction: inner, scope: engine.NewWriteScope(), eng: eng}

	got := tx.WithGraph(graph.Graph{Name: "other"})
	wrapped, ok := got.(*observingTransaction)
	if !ok {
		t.Fatalf("WithGraph returned %T, want *observingTransaction", got)
	}
	if wrapped.eng != eng {
		t.Fatalf("WithGraph did not carry eng over to the retargeted wrapper -- a later Commit on it would nil-dereference")
	}
}

func TestObservingTransactionWroteFalseInitially(t *testing.T) {
	inner := &fakeTransaction{}
	tx, _ := newObservingTransaction(inner)

	if tx.wrote() {
		t.Fatalf("wrote() = true on a transaction that has not made a single call yet")
	}
}

func TestObservingTransactionWroteFalseAfterPureRead(t *testing.T) {
	inner := &fakeTransaction{}
	tx, _ := newObservingTransaction(inner)

	// A non-mutating Cypher read leaves scope untouched (cypherMutates
	// reports false for a bare MATCH/RETURN), so it must not flip wrote().
	tx.Query(`MATCH (n) RETURN n`, nil)

	if tx.wrote() {
		t.Fatalf("wrote() = true after a pure (non-mutating) read, want false")
	}
}

func TestObservingTransactionWroteTrueAfterWrite(t *testing.T) {
	inner := &fakeTransaction{}
	tx, _ := newObservingTransaction(inner)

	if _, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("User")); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}

	if !tx.wrote() {
		t.Fatalf("wrote() = false after CreateNode touched scope, want true")
	}
}

func TestObservingTransactionCommitFlushesNowAndResetsScope(t *testing.T) {
	inner := &fakeTransaction{}
	eng := disabledEngine()
	scope := engine.NewWriteScope()
	tx := &observingTransaction{Transaction: inner, scope: scope, eng: eng}

	if _, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("User")); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}

	genBefore := eng.Generation()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: unexpected error: %v", err)
	}
	if inner.commitCalls != 1 {
		t.Fatalf("Commit did not delegate to the inner transaction")
	}
	if got := eng.Generation(); got != genBefore+1 {
		t.Fatalf("Commit did not bump the engine generation immediately: got %d, want %d", got, genBefore+1)
	}
	if tx.scope == scope {
		t.Fatalf("Commit did not replace scope with a new instance")
	}
	if !tx.scope.Empty() {
		t.Fatalf("Commit did not reset scope to a fresh, empty WriteScope")
	}
	if tx.wrote() {
		t.Fatalf("wrote() = true immediately after Commit reset scope, want false")
	}

	// Writes after the mid-transaction commit are still observed, into the
	// new scope.
	if _, err := tx.CreateNode(graph.NewProperties(), graph.StringKind("Computer")); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if tx.scope.Empty() {
		t.Fatalf("post-Commit writes were not observed into the new scope")
	}
}

func TestObservingTransactionCommitBumpsGenerationEvenWithEmptyScope(t *testing.T) {
	inner := &fakeTransaction{}
	eng := disabledEngine()
	tx := &observingTransaction{Transaction: inner, scope: engine.NewWriteScope(), eng: eng}

	genBefore := eng.Generation()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: unexpected error: %v", err)
	}
	if got := eng.Generation(); got != genBefore+1 {
		t.Fatalf("Commit with an empty scope did not bump the generation: got %d, want %d", got, genBefore+1)
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
	if ok, _ := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false after an unrecognized (no criteria) Delete, want true")
	}
}

// TestObservingNodeQueryDeleteRecognizedInIDsRecordsNodeIDs covers Delete's
// InIDs recognition: the TouchAllNodes/TouchAllEdges marking stays exactly
// as conservative as before (see Delete's own doc for why it never
// narrows), but a single recognized query.InIDs(query.NodeID(), ids...)
// criteria additionally records each id via RecordNodeID instead of a
// fallback.
func TestObservingNodeQueryDeleteRecognizedInIDsRecordsNodeIDs(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	nq.Filter(inIDsCriteria(nodeIDSymbol, 11, 12))
	if err := nq.Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	assertScope(t, scope, nil, nil, true, true, nil, nil)
	if ok, _ := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true, want a recognized InIDs criteria to record ids instead")
	}
	if got, want := scope.Changes().NodeIDs(), []uint64{11, 12}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
	}
}

// TestObservingNodeQueryDeleteZeroInIDsRecordsEmptyNonFallbackChangeSet pins
// M2's deliberate behavior: a recognized query.InIDs(query.NodeID()) target
// list naming zero ids is still ok=true (singleInIDsCriteria's own doc), so
// Delete records nothing at all onto the ChangeSet -- crucially, NOT a
// fallback. This is correct, not a gap: an `id IN []` filter matches zero
// rows, so an empty ChangeSet is the truth about what this delete did.
func TestObservingNodeQueryDeleteZeroInIDsRecordsEmptyNonFallbackChangeSet(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	nq.Filter(inIDsCriteria(nodeIDSymbol))
	if err := nq.Delete(); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if inner.deleteCalls != 1 {
		t.Fatalf("Delete did not delegate to the inner query")
	}
	if ok, reasons := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true (%v), want false: a recognized zero-id InIDs is not a fallback", reasons)
	}
	if !scope.Changes().Empty() {
		t.Fatalf("Changes().Empty() = false, want true for a recognized zero-id InIDs target")
	}
}

// TestObservingNodeQueryUpdateUnrecognizedCriteriaTouchesAllNodesAndRecordsFallback
// covers Update's newly-added override: before ChangeSet capture existed,
// NodeQuery.Update was left promoted straight through (no override at
// all), on the reasoning that a property-only update carries no kind
// information for the kind-scoped marks to act on. A write-through
// applier's changelog can't tolerate a write it never observes at all, so
// Update is now overridden -- deliberately marking TouchAllNodes
// unconditionally (mirroring Delete's own no-narrower-safe-answer marking)
// alongside a ChangeSet entry, a real behavior change from the old
// "touch nothing" promoted path, scoped to exactly this override.
func TestObservingNodeQueryUpdateUnrecognizedCriteriaTouchesAllNodesAndRecordsFallback(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	if err := nq.Update(graph.NewProperties()); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if inner.updateCalls != 1 {
		t.Fatalf("Update did not delegate to the inner query")
	}
	assertScope(t, scope, nil, nil, true, false, nil, nil)
	if ok, reasons := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = %v, %v, want a recorded fallback for unrecognized criteria", ok, reasons)
	}
}

// TestObservingNodeQueryUpdateRecognizedInIDsRecordsNodeIDs covers Update's
// InIDs recognition branch: a single query.InIDs(query.NodeID(), ids...)
// criteria records each id via RecordNodeID instead of a fallback, while
// still marking TouchAllNodes unconditionally (see the unrecognized-case
// test's doc for why the mark itself never narrows).
func TestObservingNodeQueryUpdateRecognizedInIDsRecordsNodeIDs(t *testing.T) {
	inner := &fakeNodeQuery{}
	scope := engine.NewWriteScope()
	nq := &observingNodeQuery{NodeQuery: inner, scope: scope}

	nq.Filter(inIDsCriteria(nodeIDSymbol, 5, 7))
	if err := nq.Update(graph.NewProperties()); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	assertScope(t, scope, nil, nil, true, false, nil, nil)
	if ok, _ := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true, want a recognized InIDs criteria to record ids instead")
	}
	if got, want := scope.Changes().NodeIDs(), []uint64{5, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
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
	// The recognized branch records a kind-scoped delete criteria, not an
	// enumerated id list or a fallback: relationshipDeleteScope's own
	// recognized shape is a kind matcher, and "delete every relationship of
	// this kind" is the operation this delete actually performs.
	if ok, _ := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true, want a recognized kind matcher to record RecordDeleteRelationshipsByKinds instead")
	}
	got := scope.Changes().EdgeKindCriteria()
	if len(got) != 1 || !kindsEqual(got[0], graph.Kinds{kind}) {
		t.Fatalf("Changes().EdgeKindCriteria() = %v, want [[%v]]", got, kind)
	}
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
	if ok, _ := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false after an unrecognized Delete, want true")
	}
}

// TestObservingRelationshipQueryUpdateUnrecognizedCriteriaTouchesAllEdgesAndRecordsFallback
// is observingNodeQuery's identical-purpose test's RelationshipQuery half
// -- see its doc for why Update went from promoted-and-unobserved to an
// override that unconditionally marks TouchAllEdges.
func TestObservingRelationshipQueryUpdateUnrecognizedCriteriaTouchesAllEdgesAndRecordsFallback(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	if err := rq.Update(graph.NewProperties()); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	if inner.updateCalls != 1 {
		t.Fatalf("Update did not delegate to the inner query")
	}
	assertScope(t, scope, nil, nil, false, true, nil, nil)
	if ok, reasons := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = %v, %v, want a recorded fallback for unrecognized criteria", ok, reasons)
	}
}

// TestObservingRelationshipQueryUpdateRecognizedInIDsRecordsEdgeIDs is
// TestObservingNodeQueryUpdateRecognizedInIDsRecordsNodeIDs' RelationshipQuery
// half.
func TestObservingRelationshipQueryUpdateRecognizedInIDsRecordsEdgeIDs(t *testing.T) {
	inner := &mockRelationshipQuery{}
	scope := engine.NewWriteScope()
	rq := &observingRelationshipQuery{RelationshipQuery: inner, scope: scope}

	rq.Filter(inIDsCriteria(edgeIDSymbol, 3, 9))
	if err := rq.Update(graph.NewProperties()); err != nil {
		t.Fatalf("Update: unexpected error: %v", err)
	}
	assertScope(t, scope, nil, nil, false, true, nil, nil)
	if ok, _ := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true, want a recognized InIDs criteria to record ids instead")
	}
	if got, want := scope.Changes().EdgeIDs(), []uint64{3, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().EdgeIDs() = %v, want %v", got, want)
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

	// ID left at graph.UnregisteredNodeID, mirroring graph.PrepareNode's own
	// "let the database assign one" convention for a node with no id or
	// objectid identity to key a read-back by; recordBatchCreateNodeIdentity
	// covers that branch (fallback) separately below.
	node := &graph.Node{ID: graph.UnregisteredNodeID, Kinds: graph.Kinds{graph.StringKind("User")}}
	if err := b.CreateNode(node); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if len(inner.createNodeCalls) != 1 || inner.createNodeCalls[0] != node {
		t.Fatalf("CreateNode did not delegate: %v", inner.createNodeCalls)
	}
	assertScope(t, scope, []string{"User"}, nil, false, false, nil, nil)
}

// TestObservingBatchCreateNodeRecordsPresetNodeID covers C1's first
// recognized branch: node.ID != graph.UnregisteredNodeID means the caller
// preset a real database id itself (the neo4j-to-PostgreSQL migration
// tool's own path is the one caller known to do this), so that id is the
// read-back key, recorded via RecordNodeID regardless of whether node also
// happens to carry an objectid property.
func TestObservingBatchCreateNodeRecordsPresetNodeID(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	node := &graph.Node{
		ID:         500,
		Kinds:      graph.Kinds{graph.StringKind("User")},
		Properties: graph.NewProperties().Set("objectid", "S-1-5-21"),
	}
	if err := b.CreateNode(node); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if ok, reasons := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true (%v), want a preset id to record RecordNodeID instead", reasons)
	}
	if got, want := scope.Changes().NodeIDs(), []uint64{500}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
	}
	if got := scope.Changes().NodeObjectIDs(); got != nil {
		t.Fatalf("Changes().NodeObjectIDs() = %v, want nil (preset id takes priority)", got)
	}
}

// TestObservingBatchCreateNodeRecordsObjectIDWhenIDUnregistered covers C1's
// second recognized branch: node.ID is graph.UnregisteredNodeID (a plain
// INSERT, no id ever returned), but node's own Properties carry a string
// "objectid" value -- recorded via RecordNodeObjectID so a read-back by
// objectid can find whatever row the INSERT produced.
func TestObservingBatchCreateNodeRecordsObjectIDWhenIDUnregistered(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	node := &graph.Node{
		ID:         graph.UnregisteredNodeID,
		Kinds:      graph.Kinds{graph.StringKind("User")},
		Properties: graph.NewProperties().Set("objectid", "S-1-5-21"),
	}
	if err := b.CreateNode(node); err != nil {
		t.Fatalf("CreateNode: unexpected error: %v", err)
	}
	if ok, reasons := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true (%v), want a recognized objectid to record RecordNodeObjectID instead", reasons)
	}
	if got, want := scope.Changes().NodeObjectIDs(), []string{"S-1-5-21"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeObjectIDs() = %v, want %v", got, want)
	}
	if got := scope.Changes().NodeIDs(); got != nil {
		t.Fatalf("Changes().NodeIDs() = %v, want nil", got)
	}
}

// TestObservingBatchCreateNodeRecordsFallbackWhenNoIDOrObjectID covers C1's
// third branch: node.ID is graph.UnregisteredNodeID and node carries no
// (string) "objectid" property at all, table-driven over every way that can
// happen -- there is nothing here for the applier to key a read-back by, so
// this records a fallback instead.
func TestObservingBatchCreateNodeRecordsFallbackWhenNoIDOrObjectID(t *testing.T) {
	cases := []struct {
		name       string
		properties *graph.Properties
	}{
		{"nil properties", nil},
		{"empty properties", graph.NewProperties()},
		{"objectid present but not a string", graph.NewProperties().Set("objectid", 12345)},
		{"a different property, no objectid at all", graph.NewProperties().Set("name", "n")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &fakeBatch{}
			scope := engine.NewWriteScope()
			b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

			node := &graph.Node{
				ID:         graph.UnregisteredNodeID,
				Kinds:      graph.Kinds{graph.StringKind("User")},
				Properties: tc.properties,
			}
			if err := b.CreateNode(node); err != nil {
				t.Fatalf("CreateNode: unexpected error: %v", err)
			}
			if ok, reasons := scope.Changes().HasFallback(); !ok {
				t.Fatalf("HasFallback() = false, want true for no id or objectid")
			} else if want := "Batch.CreateNode: no id or objectid to key read-back"; len(reasons) != 1 || reasons[0] != want {
				t.Fatalf("HasFallback() reasons = %v, want [%q]", reasons, want)
			}
			if got := scope.Changes().NodeIDs(); got != nil {
				t.Fatalf("Changes().NodeIDs() = %v, want nil", got)
			}
			if got := scope.Changes().NodeObjectIDs(); got != nil {
				t.Fatalf("Changes().NodeObjectIDs() = %v, want nil", got)
			}
		})
	}
}

// TestObservingBatchCreateNodeErrorDoesNotRecordIdentity covers CreateNode's
// error path: kinds are still touched (mirroring every other before-delegate
// touch in this file, which similarly doesn't roll back on a downstream
// error), but recordBatchCreateNodeIdentity must not run at all when the
// delegate itself failed -- there is no successfully created row for any of
// its three branches to key a read-back for.
func TestObservingBatchCreateNodeErrorDoesNotRecordIdentity(t *testing.T) {
	wantErr := errors.New("boom")
	inner := &fakeBatch{createNodeErr: wantErr}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	node := &graph.Node{ID: 500, Kinds: graph.Kinds{graph.StringKind("User")}}
	if err := b.CreateNode(node); err != wantErr {
		t.Fatalf("CreateNode: error = %v, want %v", err, wantErr)
	}
	assertScope(t, scope, []string{"User"}, nil, false, false, nil, nil)
	if !scope.Changes().Empty() {
		t.Fatalf("CreateNode recorded a ChangeSet entry despite a delegate error, want untouched")
	}
}

// TestObservingBatchCreateNodesDelegatesTouchesKindsAndRecordsIDs covers
// the NodeBatchCreator passthrough's supported branch: the inner batch's
// own CreateNodes is called, every input node's kinds are touched, and
// every returned id lands in the ChangeSet.
func TestObservingBatchCreateNodesDelegatesTouchesKindsAndRecordsIDs(t *testing.T) {
	inner := &fakeNodeBatchCreator{
		fakeBatch:      &fakeBatch{},
		createNodesIDs: []graph.ID{10, 11},
	}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	nodes := []*graph.Node{
		{Kinds: graph.Kinds{graph.StringKind("User")}},
		{Kinds: graph.Kinds{graph.StringKind("Computer")}},
	}
	ids, err := b.CreateNodes(nodes)
	if err != nil {
		t.Fatalf("CreateNodes: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(ids, []graph.ID{10, 11}) {
		t.Fatalf("CreateNodes ids = %v, want [10 11]", ids)
	}
	if len(inner.createNodesCalls) != 1 || !reflect.DeepEqual(inner.createNodesCalls[0], nodes) {
		t.Fatalf("CreateNodes did not delegate to the inner batch: %v", inner.createNodesCalls)
	}
	assertScope(t, scope, []string{"Computer", "User"}, nil, false, false, nil, nil)
	if got, want := scope.Changes().NodeIDs(), []uint64{10, 11}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
	}
}

// TestObservingBatchCreateNodesErrorDoesNotRecordIDs covers the inner
// delegate returning an error: kinds are still touched (mirroring
// CreateNode's own before-delegate touch, which similarly doesn't roll
// back on a downstream error), but no ids are recorded, since none were
// actually returned by the delegate.
func TestObservingBatchCreateNodesErrorDoesNotRecordIDs(t *testing.T) {
	wantErr := errors.New("boom")
	inner := &fakeNodeBatchCreator{fakeBatch: &fakeBatch{}, createNodesErr: wantErr}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	nodes := []*graph.Node{{Kinds: graph.Kinds{graph.StringKind("User")}}}
	if _, err := b.CreateNodes(nodes); err != wantErr {
		t.Fatalf("CreateNodes: error = %v, want %v", err, wantErr)
	}
	assertScope(t, scope, []string{"User"}, nil, false, false, nil, nil)
	if got := scope.Changes().NodeIDs(); got != nil {
		t.Fatalf("Changes().NodeIDs() = %v, want nil (error return must not record)", got)
	}
}

// TestObservingBatchCreateNodesUnsupportedReturnsDescriptiveError covers
// the inner batch NOT implementing graph.NodeBatchCreator: a plain
// *fakeBatch doesn't, mirroring a real dawgs batch implementation with no
// bulk-create support (retriever/load.go's own caller-side assertion
// failure is the production shape this method's error mirrors). Nothing
// is touched or recorded in this branch.
func TestObservingBatchCreateNodesUnsupportedReturnsDescriptiveError(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	ids, err := b.CreateNodes([]*graph.Node{{Kinds: graph.Kinds{graph.StringKind("User")}}})
	if err == nil {
		t.Fatalf("CreateNodes: error = nil, want a descriptive error")
	}
	if ids != nil {
		t.Fatalf("CreateNodes ids = %v, want nil", ids)
	}
	if !scope.Empty() {
		t.Fatalf("CreateNodes touched scope for an unsupported inner batch, want untouched")
	}
	if !scope.Changes().Empty() {
		t.Fatalf("CreateNodes touched the ChangeSet for an unsupported inner batch, want untouched")
	}
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
	if got, want := scope.Changes().NodeIDs(), []uint64{7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
	}
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
	// No IdentityProperties/Properties given at all, so the identity is
	// unrecognized -- see TestObservingBatchUpdateNodeByRecognizedObjectIDIdentity
	// for the recognized case.
	if ok, reasons := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false, want true for an unrecognized upsert identity")
	} else if want := "Batch.UpdateNodeBy: unrecognized identity"; len(reasons) != 1 || reasons[0] != want {
		// M1: the fallback reason must follow this file's "<Method>: reason"
		// convention, matching every other RecordFallback call site.
		t.Fatalf("HasFallback() reasons = %v, want [%q]", reasons, want)
	}
}

// TestObservingBatchUpdateNodeByRecognizedObjectIDIdentity covers the one
// identity shape write_observer.go's recognizer accepts: IdentityProperties
// == ["objectid"], with a string value under that key in the node's own
// Properties.
func TestObservingBatchUpdateNodeByRecognizedObjectIDIdentity(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	update := graph.NodeUpdate{
		Node:               &graph.Node{Properties: graph.NewProperties().Set("objectid", "S-1-5-21")},
		IdentityProperties: []string{"objectid"},
	}
	if err := b.UpdateNodeBy(update); err != nil {
		t.Fatalf("UpdateNodeBy: unexpected error: %v", err)
	}
	if ok, reasons := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true (%v), want a recognized identity to record RecordNodeObjectID instead", reasons)
	}
	if got, want := scope.Changes().NodeObjectIDs(), []string{"S-1-5-21"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeObjectIDs() = %v, want %v", got, want)
	}
}

// TestObservingBatchUpdateNodeByUnrecognizedIdentityRecordsFallback covers
// every rejection case nodeUpsertObjectIDFor's doc lists, table-driven.
func TestObservingBatchUpdateNodeByUnrecognizedIdentityRecordsFallback(t *testing.T) {
	cases := []struct {
		name   string
		update graph.NodeUpdate
	}{
		{
			name:   "nil Node",
			update: graph.NodeUpdate{IdentityProperties: []string{"objectid"}},
		},
		{
			name: "nil Properties",
			update: graph.NodeUpdate{
				Node:               &graph.Node{},
				IdentityProperties: []string{"objectid"},
			},
		},
		{
			name: "additional identity property",
			update: graph.NodeUpdate{
				Node:               &graph.Node{Properties: graph.NewProperties().Set("objectid", "S-1").Set("name", "n")},
				IdentityProperties: []string{"objectid", "name"},
			},
		},
		{
			name: "different identity property",
			update: graph.NodeUpdate{
				Node:               &graph.Node{Properties: graph.NewProperties().Set("name", "n")},
				IdentityProperties: []string{"name"},
			},
		},
		{
			name: "objectid property missing from Properties",
			update: graph.NodeUpdate{
				Node:               &graph.Node{Properties: graph.NewProperties()},
				IdentityProperties: []string{"objectid"},
			},
		},
		{
			name: "objectid property is not a string",
			update: graph.NodeUpdate{
				Node:               &graph.Node{Properties: graph.NewProperties().Set("objectid", 12345)},
				IdentityProperties: []string{"objectid"},
			},
		},
		{
			name:   "no IdentityProperties at all",
			update: graph.NodeUpdate{Node: &graph.Node{Properties: graph.NewProperties()}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &fakeBatch{}
			scope := engine.NewWriteScope()
			b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

			if err := b.UpdateNodeBy(tc.update); err != nil {
				t.Fatalf("UpdateNodeBy: unexpected error: %v", err)
			}
			if ok, _ := scope.Changes().HasFallback(); !ok {
				t.Fatalf("HasFallback() = false, want true for an unrecognized identity")
			}
			if got := scope.Changes().NodeObjectIDs(); got != nil {
				t.Fatalf("Changes().NodeObjectIDs() = %v, want nil", got)
			}
		})
	}
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

	nodes := []*graph.Node{{ID: 4, AddedKinds: graph.Kinds{graph.StringKind("Admin")}}}
	if err := b.UpdateNodes(nodes); err != nil {
		t.Fatalf("UpdateNodes: unexpected error: %v", err)
	}
	assertScope(t, scope, []string{"Admin"}, nil, false, false, nil, nil)
	if got := scope.UpsertedNodeKinds(); got != nil {
		t.Fatalf("UpsertedNodeKinds() = %v, want nil: node.Kinds was empty, nothing should be recorded", got)
	}
	// RecordNodeID is unconditional, regardless of whether a kind delta was
	// present -- the applier needs to know this node id was written to.
	if got, want := scope.Changes().NodeIDs(), []uint64{4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", got, want)
	}
}

// TestObservingBatchUpdateNodesKindsOnlyRecordsUpsertPairs is the regression
// test for the finding this fix addresses: dawgs' pg batch driver unions the
// FULL node.Kinds field into the database row on every UpdateNodes call
// (NodeUpdateParameters.Append/FormatNodesUpdate, see observingBatch.
// UpdateNodes' own doc), regardless of AddedKinds -- so a node updated with
// only Kinds set (no AddedKinds/DeletedKinds at all, the exact shape
// touchNodeKindDelta alone cannot see) must still have its Kinds recorded
// for NoteWrite to resolve against the snapshot. This asserts that recording
// directly via the UpsertedNodeKinds test accessor, since resolution itself
// (subset-of-snapshot vs. novel) is covered by marks_test.go's own
// UpsertNodeKinds tests, not re-derivable from this file's fakes alone.
func TestObservingBatchUpdateNodesKindsOnlyRecordsUpsertPairs(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	nodes := []*graph.Node{
		{ID: 1, Kinds: graph.Kinds{graph.StringKind("User")}, Properties: graph.NewProperties()},
		{ID: 2, Kinds: graph.Kinds{graph.StringKind("Computer")}, Properties: graph.NewProperties()},
	}
	if err := b.UpdateNodes(nodes); err != nil {
		t.Fatalf("UpdateNodes: unexpected error: %v", err)
	}
	if len(inner.updateNodesCalls) != 1 {
		t.Fatalf("UpdateNodes did not delegate")
	}

	// touchNodeKindDelta sees no AddedKinds/DeletedKinds on either node, so
	// the plain node/edge kind marks and all* flags stay untouched -- the
	// Kinds-only upsert pairs are recorded on the side, not folded into
	// TouchedNodeKinds, since their eventual dirtiness depends on the
	// snapshot at NoteWrite time (marks_test.go), not on anything knowable
	// here.
	assertScope(t, scope, nil, nil, false, false, nil, nil)

	got := scope.UpsertedNodeKinds()
	want := map[graph.ID]graph.Kinds{
		1: {graph.StringKind("User")},
		2: {graph.StringKind("Computer")},
	}
	if len(got) != len(want) {
		t.Fatalf("UpsertedNodeKinds() = %v, want %v", got, want)
	}
	for id, wantKinds := range want {
		if !kindsEqual(got[id], wantKinds) {
			t.Fatalf("UpsertedNodeKinds()[%d] = %v, want %v", id, got[id], wantKinds)
		}
	}

	if gotIDs, want := scope.Changes().NodeIDs(), []uint64{1, 2}; !reflect.DeepEqual(gotIDs, want) {
		t.Fatalf("Changes().NodeIDs() = %v, want %v", gotIDs, want)
	}
}

// TestObservingBatchUpdateNodesNilNodeSkipped covers UpdateNodes' existing
// nil-guard: a nil entry in nodes must not panic on either the
// touchNodeKindDelta call or the new UpsertNodeKinds call.
func TestObservingBatchUpdateNodesNilNodeSkipped(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	nodes := []*graph.Node{nil}
	if err := b.UpdateNodes(nodes); err != nil {
		t.Fatalf("UpdateNodes: unexpected error: %v", err)
	}
	if !scope.Empty() {
		t.Fatalf("UpdateNodes([nil]) touched scope, want untouched")
	}
	if !scope.Changes().Empty() {
		t.Fatalf("UpdateNodes([nil]) touched the ChangeSet, want untouched")
	}
}

func TestObservingBatchCreateRelationshipTouchesScopeAndDelegates(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	kind := graph.StringKind("HasSession")
	rel := &graph.Relationship{StartID: 1, EndID: 2, Kind: kind}
	if err := b.CreateRelationship(rel); err != nil {
		t.Fatalf("CreateRelationship: unexpected error: %v", err)
	}
	if len(inner.createRelationshipCalls) != 1 || inner.createRelationshipCalls[0] != rel {
		t.Fatalf("CreateRelationship did not delegate: %v", inner.createRelationshipCalls)
	}
	assertScope(t, scope, nil, []string{"HasSession"}, false, false, nil, nil)
	// The edge's own id is never known from this call (graph.Batch.
	// CreateRelationship reports success/failure only), so it's recorded
	// by endpoints and kind instead.
	want := []engine.EdgeTripleRef{{Start: 1, End: 2, Kind: kind}}
	if got := scope.Changes().EdgeTriples(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().EdgeTriples() = %+v, want %+v", got, want)
	}
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
	want := []engine.EdgeTripleRef{{Start: 1, End: 2, Kind: kind}}
	if got := scope.Changes().EdgeTriples(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().EdgeTriples() = %+v, want %+v", got, want)
	}
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
	if got, want := scope.Changes().EdgeIDs(), []uint64{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Changes().EdgeIDs() = %v, want %v", got, want)
	}
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
	// Neither endpoint carries an identity at all, so the whole upsert
	// identity is unrecognized.
	if ok, reasons := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false, want true for an unrecognized upsert identity")
	} else if want := "Batch.UpdateRelationshipBy: unrecognized identity"; len(reasons) != 1 || reasons[0] != want {
		// M1: the fallback reason must follow this file's "<Method>: reason"
		// convention, matching every other RecordFallback call site.
		t.Fatalf("HasFallback() reasons = %v, want [%q]", reasons, want)
	}
}

// TestObservingBatchUpdateRelationshipByRecognizedObjectIDIdentity covers
// the recognized shape: both Start and End carry a bare ["objectid"]
// identity with a string value, so the upsert also upserts both endpoint
// nodes -- recordRelationshipUpsertIdentity's doc.
func TestObservingBatchUpdateRelationshipByRecognizedObjectIDIdentity(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	kind := graph.StringKind("MemberOf")
	update := graph.RelationshipUpdate{
		Relationship:            &graph.Relationship{Kind: kind},
		Start:                   &graph.Node{Properties: graph.NewProperties().Set("objectid", "start-oid")},
		StartIdentityProperties: []string{"objectid"},
		End:                     &graph.Node{Properties: graph.NewProperties().Set("objectid", "end-oid")},
		EndIdentityProperties:   []string{"objectid"},
	}
	if err := b.UpdateRelationshipBy(update); err != nil {
		t.Fatalf("UpdateRelationshipBy: unexpected error: %v", err)
	}
	if ok, reasons := scope.Changes().HasFallback(); ok {
		t.Fatalf("HasFallback() = true (%v), want a recognized identity to record instead", reasons)
	}

	wantTriples := []engine.EdgeTripleOIDRef{{StartOID: "start-oid", EndOID: "end-oid", Kind: kind}}
	if got := scope.Changes().EdgeTriplesByObjectID(); !reflect.DeepEqual(got, wantTriples) {
		t.Fatalf("Changes().EdgeTriplesByObjectID() = %+v, want %+v", got, wantTriples)
	}

	wantOIDs := []string{"end-oid", "start-oid"}
	if got := scope.Changes().NodeObjectIDs(); !reflect.DeepEqual(got, wantOIDs) {
		t.Fatalf("Changes().NodeObjectIDs() = %v, want %v", got, wantOIDs)
	}
}

// TestObservingBatchUpdateRelationshipByOneEndpointUnrecognizedRecordsFallback
// covers the "just one endpoint unrecognized" case recordRelationshipUpsertIdentity's
// doc calls out: a relationship triple keyed on both endpoints' objectids
// is only sound to record when BOTH endpoints resolve, so one missing
// identity falls the whole call back, rather than recording a partial
// triple.
func TestObservingBatchUpdateRelationshipByOneEndpointUnrecognizedRecordsFallback(t *testing.T) {
	inner := &fakeBatch{}
	scope := engine.NewWriteScope()
	b := &observingBatch{Batch: inner, scope: scope, eng: disabledEngine()}

	update := graph.RelationshipUpdate{
		Relationship:            &graph.Relationship{Kind: graph.StringKind("MemberOf")},
		Start:                   &graph.Node{Properties: graph.NewProperties().Set("objectid", "start-oid")},
		StartIdentityProperties: []string{"objectid"},
		End:                     &graph.Node{Properties: graph.NewProperties().Set("name", "n")},
		EndIdentityProperties:   []string{"name"},
	}
	if err := b.UpdateRelationshipBy(update); err != nil {
		t.Fatalf("UpdateRelationshipBy: unexpected error: %v", err)
	}
	if ok, _ := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false, want true when only one endpoint is recognized")
	}
	if got := scope.Changes().EdgeTriplesByObjectID(); got != nil {
		t.Fatalf("Changes().EdgeTriplesByObjectID() = %+v, want nil", got)
	}
	if got := scope.Changes().NodeObjectIDs(); got != nil {
		t.Fatalf("Changes().NodeObjectIDs() = %v, want nil", got)
	}
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
	if ok, _ := scope.Changes().HasFallback(); !ok {
		t.Fatalf("HasFallback() = false after WithGraph, want true")
	}

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
