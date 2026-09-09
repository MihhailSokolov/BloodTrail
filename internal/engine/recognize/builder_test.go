// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"reflect"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// Kinds shared across this file's tests, named after the upstream shapes
// they stand in for.
var (
	kAdUser     = graph.StringKind("User")
	kAdComputer = graph.StringKind("Computer")
	kAdGroup    = graph.StringKind("Group")
	kAdEntity   = graph.StringKind("Base")
	kMemberOf   = graph.StringKind("MemberOf")
	kHasSession = graph.StringKind("HasSession")
	kBase1      = graph.StringKind("Base1")
	kBase2      = graph.StringKind("Base2")
	kK1         = graph.StringKind("K1")
	kK2         = graph.StringKind("K2")
	kMeta1      = graph.StringKind("Meta1")
	kMeta2      = graph.StringKind("Meta2")
)

// TestFromNodeCriteria_Accepts covers every upstream node-only shape
// BloodHound's own code actually issues, plus the structural variants
// (nested conjunctions, multiple independent KindMatcher conjuncts, the
// AllOf=true/IsExclusive=true mapping, and id() intersection) that exercise
// FromNodeCriteria's documented contract.
func TestFromNodeCriteria_Accepts(t *testing.T) {
	tests := []struct {
		name string
		crit graph.Criteria
		want NodeSpec
	}{
		{
			name: "FetchNodeIDsByKind: bare KindMatcher, no wrapping And",
			crit: query.Kind(query.Node(), kAdUser),
			want: NodeSpec{Constraints: []KindConstraint{{Kinds: graph.Kinds{kAdUser}, AllOf: false}}},
		},
		{
			name: "bare id(n) = <id>",
			crit: query.Equals(query.NodeID(), graph.ID(5)),
			want: NodeSpec{IDs: []graph.ID{5}},
		},
		{
			name: "bare id(n) IN <ids>, variable wrapped in Identity() by InIDs",
			crit: query.InIDs(query.Node(), 1, 2, 3),
			want: NodeSpec{IDs: []graph.ID{1, 2, 3}},
		},
		{
			name: "two independent KindMatcher conjuncts stay separate (AND of two any-of sets)",
			crit: query.And(query.Kind(query.Node(), kAdUser), query.Kind(query.Node(), kAdGroup)),
			want: NodeSpec{Constraints: []KindConstraint{
				{Kinds: graph.Kinds{kAdUser}, AllOf: false},
				{Kinds: graph.Kinds{kAdGroup}, AllOf: false},
			}},
		},
		{
			name: "IsExclusive=true maps to AllOf=true (cypher pattern-label shape)",
			crit: &cypher.KindMatcher{Reference: query.Node(), Kinds: graph.Kinds{kAdUser, kAdGroup}, IsExclusive: true},
			want: NodeSpec{Constraints: []KindConstraint{{Kinds: graph.Kinds{kAdUser, kAdGroup}, AllOf: true}}},
		},
		{
			name: "id(n) = 1 AND id(n) IN [1,2,3] intersects to {1}",
			crit: query.And(query.Equals(query.NodeID(), graph.ID(1)), query.InIDs(query.Node(), 1, 2, 3)),
			want: NodeSpec{IDs: []graph.ID{1}},
		},
		{
			name: "disjoint id(n) = 1 AND id(n) = 2 intersects to the empty (non-nil) set",
			crit: query.And(query.Equals(query.NodeID(), graph.ID(1)), query.Equals(query.NodeID(), graph.ID(2))),
			want: NodeSpec{IDs: []graph.ID{}},
		},
		{
			name: "duplicate id(n) = 1 conjuncts dedupe",
			crit: query.And(query.Equals(query.NodeID(), graph.ID(1)), query.Equals(query.NodeID(), graph.ID(1))),
			want: NodeSpec{IDs: []graph.ID{1}},
		},
		{
			name: "a three-way id(n) intersection narrows step by step",
			crit: query.And(
				query.InIDs(query.Node(), 1, 2, 3),
				query.InIDs(query.Node(), 2, 3, 4),
				query.Equals(query.NodeID(), graph.ID(3)),
			),
			want: NodeSpec{IDs: []graph.ID{3}},
		},
		{
			name: "duplicate ids within one IN list dedupe",
			crit: query.InIDs(query.Node(), 1, 1, 2),
			want: NodeSpec{IDs: []graph.ID{1, 2}},
		},
		{
			name: "nested conjunctions flatten in source order",
			crit: query.And(
				query.Kind(query.Node(), kAdUser),
				query.And(
					query.Equals(query.NodeID(), graph.ID(9)),
					query.Kind(query.Node(), kAdGroup),
				),
			),
			want: NodeSpec{
				IDs: []graph.ID{9},
				Constraints: []KindConstraint{
					{Kinds: graph.Kinds{kAdUser}, AllOf: false},
					{Kinds: graph.Kinds{kAdGroup}, AllOf: false},
				},
			},
		},
		{
			name: "empty conjunction recognizes as a fully unconstrained spec",
			crit: query.And(),
			want: NodeSpec{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FromNodeCriteria(tc.crit)
			if !ok {
				t.Fatalf("FromNodeCriteria() ok = false, want true")
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("FromNodeCriteria() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestFromNodeCriteria_Rejects covers every shape FromNodeCriteria must
// decline: mandated rejections (Negation, Or, property lookups, a
// wrong-symbol id() conjunct) plus the structural default-deny
// cases (unknown symbol, empty-Kinds KindMatcher, non-=/IN comparison
// operator, nil/non-Expression criteria, a typed-nil Conjunction).
func TestFromNodeCriteria_Rejects(t *testing.T) {
	unknownSymbolID := &cypher.FunctionInvocation{Name: "id", Arguments: []cypher.Expression{&cypher.Variable{Symbol: "x"}}}

	tests := []struct {
		name string
		crit graph.Criteria
	}{
		{
			name: "property lookup",
			crit: query.Equals(query.NodeProperty("objectid"), "x"),
		},
		{
			name: "Negation (IgnoreMetaFilter shape)",
			crit: query.Not(query.KindIn(query.Node(), kMeta1, kMeta2)),
		},
		{
			name: "Disjunction (query.Or)",
			crit: query.Or(query.Kind(query.Node(), kAdUser), query.Kind(query.Node(), kAdGroup)),
		},
		{
			name: "wrong symbol: id(s) inside node criteria",
			crit: query.Equals(query.StartID(), graph.ID(1)),
		},
		{
			name: "wrong symbol: KindMatcher over the relationship variable",
			crit: query.Kind(query.Relationship(), kMemberOf),
		},
		{
			name: "wrong symbol: KindMatcher over the start variable",
			crit: query.Kind(query.Start(), kAdEntity),
		},
		{
			name: "unknown variable symbol",
			crit: query.Equals(unknownSymbolID, graph.ID(1)),
		},
		{
			name: "empty-Kinds KindMatcher matches nothing, declined rather than modeled (IsExclusive=false)",
			crit: query.KindIn(query.Node()),
		},
		{
			name: "empty-Kinds KindMatcher declined regardless of IsExclusive",
			crit: &cypher.KindMatcher{Reference: query.Node(), Kinds: nil, IsExclusive: true},
		},
		{
			name: "comparison operator other than = or IN over id()",
			crit: query.GreaterThan(query.NodeID(), graph.ID(1)),
		},
		{
			name: "one valid conjunct alongside one unrecognized conjunct rejects the whole criteria",
			crit: query.And(query.Kind(query.Node(), kAdUser), query.Equals(query.NodeProperty("x"), 1)),
		},
		{
			name: "nil criteria",
			crit: nil,
		},
		{
			name: "typed nil conjunction",
			crit: (*cypher.Conjunction)(nil),
		},
		{
			name: "unrelated criteria type",
			crit: 42,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("FromNodeCriteria() panicked: %v", r)
				}
			}()

			got, ok := FromNodeCriteria(tc.crit)
			if ok {
				t.Fatalf("FromNodeCriteria() ok = true, want false (got %#v)", got)
			}
		})
	}
}

// TestFromRelCriteria_Accepts covers every upstream relationship-pattern
// shape BloodHound's own code actually issues (traversal step,
// DeleteTransitEdges, LocalToComputer, FetchDirectedGraph, first-degree ACL
// principals, and a bare id list), plus the structural variants that
// exercise FromRelCriteria's documented contract.
func TestFromRelCriteria_Accepts(t *testing.T) {
	tests := []struct {
		name string
		crit graph.Criteria
		want RelSpec
	}{
		{
			name: "traversal step (LightweightDriver / ops.TraversalPlan)",
			crit: query.And(query.Kind(query.Relationship(), kMemberOf), query.Equals(query.EndID(), graph.ID(42))),
			want: RelSpec{EdgeKinds: graph.Kinds{kMemberOf}, EndIDs: []graph.ID{42}},
		},
		{
			name: "DeleteTransitEdges read",
			crit: query.And(
				query.KindIn(query.Start(), kBase1, kBase2),
				query.Kind(query.Relationship(), kK1),
				query.KindIn(query.End(), kBase1, kBase2),
			),
			want: RelSpec{
				EdgeKinds:        graph.Kinds{kK1},
				StartConstraints: []KindConstraint{{Kinds: graph.Kinds{kBase1, kBase2}, AllOf: false}},
				EndConstraints:   []KindConstraint{{Kinds: graph.Kinds{kBase1, kBase2}, AllOf: false}},
			},
		},
		{
			name: "LocalToComputer bulk scan",
			crit: query.And(query.Kind(query.Relationship(), kHasSession), query.Kind(query.End(), kAdComputer)),
			want: RelSpec{
				EdgeKinds:      graph.Kinds{kHasSession},
				EndConstraints: []KindConstraint{{Kinds: graph.Kinds{kAdComputer}, AllOf: false}},
			},
		},
		{
			name: "FetchDirectedGraph: bare KindIn on relationship",
			crit: query.KindIn(query.Relationship(), kK1, kK2),
			want: RelSpec{EdgeKinds: graph.Kinds{kK1, kK2}},
		},
		{
			name: "first-degree ACL principals",
			crit: query.And(
				query.Kind(query.Start(), kAdEntity),
				query.KindIn(query.Relationship(), kK1, kK2),
				query.Equals(query.EndID(), graph.ID(7)),
			),
			want: RelSpec{
				StartConstraints: []KindConstraint{{Kinds: graph.Kinds{kAdEntity}, AllOf: false}},
				EdgeKinds:        graph.Kinds{kK1, kK2},
				EndIDs:           []graph.ID{7},
			},
		},
		{
			name: "bare id list",
			crit: query.InIDs(query.StartID(), 1, 2, 3),
			want: RelSpec{StartIDs: []graph.ID{1, 2, 3}},
		},
		{
			name: "two independent KindMatcher conjuncts over the same endpoint stay separate",
			crit: query.And(query.Kind(query.Start(), kAdUser), query.Kind(query.Start(), kAdGroup)),
			want: RelSpec{StartConstraints: []KindConstraint{
				{Kinds: graph.Kinds{kAdUser}, AllOf: false},
				{Kinds: graph.Kinds{kAdGroup}, AllOf: false},
			}},
		},
		{
			name: "start and end id() conjuncts intersect independently, no cross-contamination",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(1)),
				query.InIDs(query.Start(), 1, 2, 3),
				query.Equals(query.EndID(), graph.ID(5)),
			),
			want: RelSpec{StartIDs: []graph.ID{1}, EndIDs: []graph.ID{5}},
		},
		{
			name: "disjoint id(s) conjuncts intersect to the empty (non-nil) set",
			crit: query.And(query.Equals(query.StartID(), graph.ID(1)), query.Equals(query.StartID(), graph.ID(2))),
			want: RelSpec{StartIDs: []graph.ID{}},
		},
		{
			name: "a three-way id(e) intersection narrows step by step",
			crit: query.And(
				query.InIDs(query.End(), 1, 2, 3),
				query.InIDs(query.End(), 2, 3, 4),
				query.Equals(query.EndID(), graph.ID(3)),
			),
			want: RelSpec{EndIDs: []graph.ID{3}},
		},
		{
			name: "duplicate ids within one IN list dedupe",
			crit: query.InIDs(query.End(), 1, 1, 2),
			want: RelSpec{EndIDs: []graph.ID{1, 2}},
		},
		{
			name: "nested conjunctions flatten in source order",
			crit: query.And(
				query.Kind(query.Relationship(), kK1),
				query.And(
					query.Equals(query.EndID(), graph.ID(9)),
					query.Kind(query.Start(), kAdUser),
				),
			),
			want: RelSpec{
				EdgeKinds:        graph.Kinds{kK1},
				EndIDs:           []graph.ID{9},
				StartConstraints: []KindConstraint{{Kinds: graph.Kinds{kAdUser}, AllOf: false}},
			},
		},
		{
			name: "empty conjunction recognizes as a fully unconstrained spec",
			crit: query.And(),
			want: RelSpec{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FromRelCriteria(tc.crit)
			if !ok {
				t.Fatalf("FromRelCriteria() ok = false, want true")
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("FromRelCriteria() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestFromRelCriteria_Rejects covers every shape FromRelCriteria must
// decline: mandated rejections (a wrong-symbol "n" conjunct, property
// lookups, Negation, Disjunction, a second relationship KindMatcher) plus
// the structural default-deny cases.
func TestFromRelCriteria_Rejects(t *testing.T) {
	unknownSymbolID := &cypher.FunctionInvocation{Name: "id", Arguments: []cypher.Expression{&cypher.Variable{Symbol: "x"}}}

	tests := []struct {
		name string
		crit graph.Criteria
	}{
		{
			name: "property lookup (StringEndsWith over a start property)",
			crit: query.StringEndsWith(query.StartProperty("objectid"), "-515"),
		},
		{
			name: "wrong symbol: KindMatcher over the node variable",
			crit: query.Kind(query.Node(), kAdUser),
		},
		{
			name: "wrong symbol: id(n) inside relationship criteria",
			crit: query.Equals(query.NodeID(), graph.ID(1)),
		},
		{
			name: "Disjunction (query.Or)",
			crit: query.Or(query.Kind(query.Relationship(), kK1), query.Kind(query.Relationship(), kK2)),
		},
		{
			name: "Negation",
			crit: query.Not(query.KindIn(query.Relationship(), kK1, kK2)),
		},
		{
			name: "a second KindMatcher over the relationship variable rejects rather than unions",
			crit: query.And(query.KindIn(query.Relationship(), kK1), query.KindIn(query.Relationship(), kK2)),
		},
		{
			name: "empty-Kinds KindMatcher over the relationship variable (IsExclusive=false)",
			crit: query.KindIn(query.Relationship()),
		},
		{
			name: "empty-Kinds KindMatcher over the relationship variable declined regardless of IsExclusive",
			crit: &cypher.KindMatcher{Reference: query.Relationship(), Kinds: nil, IsExclusive: true},
		},
		{
			name: "empty-Kinds KindMatcher over the start variable",
			crit: query.KindIn(query.Start()),
		},
		{
			name: "empty-Kinds KindMatcher over the end variable declined regardless of IsExclusive",
			crit: &cypher.KindMatcher{Reference: query.End(), Kinds: nil, IsExclusive: true},
		},
		{
			name: "comparison operator other than = or IN over id(s)",
			crit: query.GreaterThan(query.StartID(), graph.ID(1)),
		},
		{
			name: "unknown variable symbol",
			crit: query.Equals(unknownSymbolID, graph.ID(1)),
		},
		{
			name: "nil criteria",
			crit: nil,
		},
		{
			name: "typed nil conjunction",
			crit: (*cypher.Conjunction)(nil),
		},
		{
			name: "unrelated criteria type",
			crit: "not criteria",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("FromRelCriteria() panicked: %v", r)
				}
			}()

			got, ok := FromRelCriteria(tc.crit)
			if ok {
				t.Fatalf("FromRelCriteria() ok = true, want false (got %#v)", got)
			}
		})
	}
}

// TestMatchIDIn exercises matchIDIn's malformed-shape rejections directly,
// plus every accepted right-hand-side shape it must recognize: what
// query.InIDs actually builds (a Parameter wrapping []graph.ID, left
// already id(<var>) whether InIDs was called with a FunctionInvocation or a
// bare Variable), a Parameter/Literal wrapping []int64 / []uint64 / []any,
// and a bare *cypher.ListLiteral (the shape a cypher-text `IN [1, 2, 3]`
// parses to -- a cypher-text recognizer's concern, but matchIDIn must
// already handle it, since FromRelCriteria/FromNodeCriteria are reused
// there).
func TestMatchIDIn(t *testing.T) {
	validRight := &cypher.Parameter{Value: []graph.ID{1}}

	rejects := []struct {
		name string
		cmp  *cypher.Comparison
	}{
		{name: "nil comparison", cmp: nil},
		{
			name: "two partials",
			cmp: &cypher.Comparison{
				Left: query.StartID(),
				Partials: []*cypher.PartialComparison{
					{Operator: cypher.OperatorIn, Right: validRight},
					{Operator: cypher.OperatorIn, Right: validRight},
				},
			},
		},
		{
			name: "nil partial",
			cmp:  &cypher.Comparison{Left: query.StartID(), Partials: []*cypher.PartialComparison{nil}},
		},
		{
			name: "non-IN operator",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorEquals, Right: validRight}},
			},
		},
		{
			name: "left is not a function invocation",
			cmp: &cypher.Comparison{
				Left:     query.StartProperty("x"),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: validRight}},
			},
		},
		{
			name: "function invocation is not id()",
			cmp: &cypher.Comparison{
				Left:     &cypher.FunctionInvocation{Name: "count", Arguments: []cypher.Expression{query.Start()}},
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: validRight}},
			},
		},
		{
			name: "id() with no arguments",
			cmp: &cypher.Comparison{
				Left:     &cypher.FunctionInvocation{Name: "id", Arguments: nil},
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: validRight}},
			},
		},
		{
			name: "id() argument is not a variable",
			cmp: &cypher.Comparison{
				Left:     &cypher.FunctionInvocation{Name: "id", Arguments: []cypher.Expression{query.StartProperty("x")}},
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: validRight}},
			},
		},
		{
			name: "right-hand side is not a slice at all",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: &cypher.Parameter{Value: "not ids"}}},
			},
		},
		{
			name: "right-hand side is a slice of an unsupported element type",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: &cypher.Parameter{Value: []string{"a"}}}},
			},
		},
		{
			name: "right-hand side []any contains one unsupported element",
			cmp: &cypher.Comparison{
				Left: query.StartID(),
				Partials: []*cypher.PartialComparison{{
					Operator: cypher.OperatorIn,
					Right:    &cypher.Parameter{Value: []any{int64(1), "not an id"}},
				}},
			},
		},
		{
			name: "right-hand side is a null literal",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: &cypher.Literal{Null: true}}},
			},
		},
	}

	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("matchIDIn() panicked: %v", r)
				}
			}()
			if _, _, ok := matchIDIn(tc.cmp); ok {
				t.Fatalf("matchIDIn() ok = true, want false")
			}
		})
	}

	accepts := []struct {
		name       string
		cmp        *cypher.Comparison
		wantSymbol string
		wantIDs    []graph.ID
	}{
		{
			name:       "query.InIDs over a FunctionInvocation (StartID)",
			cmp:        query.InIDs(query.StartID(), 1, 2, 3),
			wantSymbol: startSymbol,
			wantIDs:    []graph.ID{1, 2, 3},
		},
		{
			name:       "query.InIDs over a bare Variable (Node), wrapped in Identity() by InIDs",
			cmp:        query.InIDs(query.Node(), 5),
			wantSymbol: nodeSymbol,
			wantIDs:    []graph.ID{5},
		},
		{
			name: "Parameter wrapping []int64",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: &cypher.Parameter{Value: []int64{1, 2}}}},
			},
			wantSymbol: startSymbol,
			wantIDs:    []graph.ID{1, 2},
		},
		{
			name: "Parameter wrapping []uint64",
			cmp: &cypher.Comparison{
				Left:     query.EndID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: &cypher.Parameter{Value: []uint64{3, 4}}}},
			},
			wantSymbol: endSymbol,
			wantIDs:    []graph.ID{3, 4},
		},
		{
			name: "Parameter wrapping []any of mixed scalar id types",
			cmp: &cypher.Comparison{
				Left: query.StartID(),
				Partials: []*cypher.PartialComparison{{
					Operator: cypher.OperatorIn,
					Right:    &cypher.Parameter{Value: []any{int64(1), uint64(2), graph.ID(3)}},
				}},
			},
			wantSymbol: startSymbol,
			wantIDs:    []graph.ID{1, 2, 3},
		},
		{
			name: "bare Literal (not Parameter) wrapping []graph.ID",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorIn, Right: &cypher.Literal{Value: []graph.ID{7, 8}}}},
			},
			wantSymbol: startSymbol,
			wantIDs:    []graph.ID{7, 8},
		},
		{
			name: "bare ListLiteral (cypher-text `IN [1, 2]` shape)",
			cmp: &cypher.Comparison{
				Left: query.StartID(),
				Partials: []*cypher.PartialComparison{{
					Operator: cypher.OperatorIn,
					Right:    &cypher.ListLiteral{&cypher.Literal{Value: int64(1)}, &cypher.Parameter{Value: uint64(2)}},
				}},
			},
			wantSymbol: startSymbol,
			wantIDs:    []graph.ID{1, 2},
		},
	}

	for _, tc := range accepts {
		t.Run(tc.name, func(t *testing.T) {
			symbol, ids, ok := matchIDIn(tc.cmp)
			if !ok {
				t.Fatalf("matchIDIn() ok = false, want true")
			}
			if symbol != tc.wantSymbol {
				t.Fatalf("matchIDIn() symbol = %q, want %q", symbol, tc.wantSymbol)
			}
			if !reflect.DeepEqual(ids, tc.wantIDs) {
				t.Fatalf("matchIDIn() ids = %v, want %v", ids, tc.wantIDs)
			}
		})
	}
}

// TestNodeSpec_ConstraintKinds exercises the union-with-dedup contract:
// kinds repeated across Constraints entries appear once, in first-
// occurrence order, and an empty Constraints list produces a nil union.
func TestNodeSpec_ConstraintKinds(t *testing.T) {
	spec := NodeSpec{Constraints: []KindConstraint{
		{Kinds: graph.Kinds{kAdUser, kAdGroup}, AllOf: false},
		{Kinds: graph.Kinds{kAdGroup, kAdEntity}, AllOf: true},
	}}

	want := graph.Kinds{kAdUser, kAdGroup, kAdEntity}
	if got := spec.ConstraintKinds(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ConstraintKinds() = %v, want %v", got, want)
	}

	if got := (NodeSpec{}).ConstraintKinds(); got != nil {
		t.Fatalf("ConstraintKinds() on an empty spec = %v, want nil", got)
	}
}

// TestRelSpec_NodeConstraintKinds mirrors TestNodeSpec_ConstraintKinds,
// confirming the union spans both endpoints.
func TestRelSpec_NodeConstraintKinds(t *testing.T) {
	spec := RelSpec{
		StartConstraints: []KindConstraint{{Kinds: graph.Kinds{kAdUser}, AllOf: false}},
		EndConstraints:   []KindConstraint{{Kinds: graph.Kinds{kAdUser, kAdComputer}, AllOf: false}},
	}

	want := graph.Kinds{kAdUser, kAdComputer}
	if got := spec.NodeConstraintKinds(); !reflect.DeepEqual(got, want) {
		t.Fatalf("NodeConstraintKinds() = %v, want %v", got, want)
	}

	if got := (RelSpec{}).NodeConstraintKinds(); got != nil {
		t.Fatalf("NodeConstraintKinds() on an empty spec = %v, want nil", got)
	}
}
