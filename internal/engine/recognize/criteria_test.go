// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"reflect"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// wantPathQuery builds the PathQuery FromCriteria is expected to produce for
// a start/end pair with the given edge kinds (nil for "no KindIn conjunct").
func wantPathQuery(start, end graph.ID, edgeKinds graph.Kinds) PathQuery {
	return PathQuery{
		Start:     Endpoint{IDs: []graph.ID{start}},
		End:       Endpoint{IDs: []graph.ID{end}},
		EdgeKinds: edgeKinds,
		Mode:      ModeAll,
		Limit:     0,
	}
}

// TestFromCriteria_Accepts covers the shapes getAllShortestPathsInternal
// actually builds: the full triple (start id, end id, relationship kind
// filter), the pair alone (no kind filter -- FetchAllShortestPaths called
// with no edgeKinds param), and the conjuncts in reversed order (query.And
// makes no order guarantee the recognizer may rely on).
func TestFromCriteria_Accepts(t *testing.T) {
	memberOf := graph.StringKind("MemberOf")
	adminTo := graph.StringKind("AdminTo")

	tests := []struct {
		name string
		crit graph.Criteria
		want PathQuery
	}{
		{
			name: "full triple with kind filter",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(42)),
				query.Equals(query.EndID(), graph.ID(7)),
				query.KindIn(query.Relationship(), memberOf, adminTo),
			),
			want: wantPathQuery(42, 7, graph.Kinds{memberOf, adminTo}),
		},
		{
			name: "no kind filter",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(1)),
				query.Equals(query.EndID(), graph.ID(2)),
			),
			want: wantPathQuery(1, 2, nil),
		},
		{
			name: "reversed conjunct order",
			crit: query.And(
				query.KindIn(query.Relationship(), memberOf),
				query.Equals(query.EndID(), graph.ID(7)),
				query.Equals(query.StartID(), graph.ID(42)),
			),
			want: wantPathQuery(42, 7, graph.Kinds{memberOf}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FromCriteria(tc.crit)
			if !ok {
				t.Fatalf("FromCriteria() ok = false, want true")
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("FromCriteria() = %#v, want %#v", got, tc.want)
			}
			if got.ExcludeSelf {
				t.Errorf("ExcludeSelf = true, want false")
			}
		})
	}
}

// TestFromCriteria_Rejects covers shapes FromCriteria must refuse without
// panicking: anything upstream's builder never produces.
func TestFromCriteria_Rejects(t *testing.T) {
	tests := []struct {
		name string
		crit graph.Criteria
	}{
		{
			name: "extra property conjunct",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(42)),
				query.Equals(query.EndID(), graph.ID(7)),
				query.Equals(query.NodeProperty("x"), 1),
			),
		},
		{
			name: "KindIn on node instead of relationship",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(42)),
				query.Equals(query.EndID(), graph.ID(7)),
				query.KindIn(query.Node(), graph.StringKind("User")),
			),
		},
		{
			name: "missing StartID",
			crit: query.And(
				query.Equals(query.EndID(), graph.ID(7)),
				query.KindIn(query.Relationship(), graph.StringKind("MemberOf")),
			),
		},
		{
			name: "missing EndID",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(42)),
				query.KindIn(query.Relationship(), graph.StringKind("MemberOf")),
			),
		},
		{
			name: "bare non-conjunction criterion of unsupported type",
			crit: query.Equals(query.StartID(), graph.ID(42)),
		},
		{
			name: "duplicate StartID",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(42)),
				query.Equals(query.StartID(), graph.ID(99)),
				query.Equals(query.EndID(), graph.ID(7)),
			),
		},
		{
			name: "duplicate KindIn",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(42)),
				query.Equals(query.EndID(), graph.ID(7)),
				query.KindIn(query.Relationship(), graph.StringKind("MemberOf")),
				query.KindIn(query.Relationship(), graph.StringKind("AdminTo")),
			),
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
			name: "empty conjunction",
			crit: query.And(),
		},
		{
			name: "id comparison over unrelated variable",
			crit: query.And(
				query.Equals(query.StartID(), graph.ID(42)),
				query.Equals(query.EndID(), graph.ID(7)),
				query.Equals(query.NodeID(), graph.ID(1)),
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("FromCriteria() panicked: %v", r)
				}
			}()

			got, ok := FromCriteria(tc.crit)
			if ok {
				t.Fatalf("FromCriteria() ok = true, want false (got %#v)", got)
			}
		})
	}
}

// TestLiteralToID exercises literalToID directly since FromCriteria only
// ever feeds it *cypher.Parameter wrapping a graph.ID (what query.Equals
// actually builds); Task 7's cypher-text recognizer will also feed it bare
// *cypher.Literal nodes (parsed straight from query text) and int64/uint64
// payloads, so those paths need their own coverage here.
func TestLiteralToID(t *testing.T) {
	var nilParameter *cypher.Parameter
	var nilLiteral *cypher.Literal

	tests := []struct {
		name   string
		expr   cypher.Expression
		wantID graph.ID
		wantOK bool
	}{
		{name: "parameter wrapping graph.ID", expr: &cypher.Parameter{Value: graph.ID(42)}, wantID: 42, wantOK: true},
		{name: "parameter wrapping int64", expr: &cypher.Parameter{Value: int64(42)}, wantID: 42, wantOK: true},
		{name: "parameter wrapping uint64", expr: &cypher.Parameter{Value: uint64(42)}, wantID: 42, wantOK: true},
		{name: "parameter wrapping unsupported type", expr: &cypher.Parameter{Value: "42"}, wantOK: false},
		{name: "nil parameter", expr: nilParameter, wantOK: false},
		{name: "literal wrapping graph.ID", expr: &cypher.Literal{Value: graph.ID(7)}, wantID: 7, wantOK: true},
		{name: "literal wrapping int64", expr: &cypher.Literal{Value: int64(7)}, wantID: 7, wantOK: true},
		{name: "literal wrapping uint64", expr: &cypher.Literal{Value: uint64(7)}, wantID: 7, wantOK: true},
		{name: "null literal", expr: &cypher.Literal{Null: true}, wantOK: false},
		{name: "nil literal", expr: nilLiteral, wantOK: false},
		{name: "unsupported expression type", expr: query.Node(), wantOK: false},
		{name: "nil expression", expr: nil, wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, ok := literalToID(tc.expr)
			if ok != tc.wantOK {
				t.Fatalf("literalToID() ok = %v, want %v", ok, tc.wantOK)
			}
			if ok && id != tc.wantID {
				t.Fatalf("literalToID() id = %v, want %v", id, tc.wantID)
			}
		})
	}
}

// TestMatchIDEquals exercises matchIDEquals's malformed-shape rejections
// directly; FromCriteria's table above only reaches a subset of these
// through a full query.And tree.
func TestMatchIDEquals(t *testing.T) {
	validRight := &cypher.Parameter{Value: graph.ID(1)}

	tests := []struct {
		name string
		cmp  *cypher.Comparison
	}{
		{name: "nil comparison", cmp: nil},
		{
			name: "two partials",
			cmp: &cypher.Comparison{
				Left: query.StartID(),
				Partials: []*cypher.PartialComparison{
					{Operator: cypher.OperatorEquals, Right: validRight},
					{Operator: cypher.OperatorEquals, Right: validRight},
				},
			},
		},
		{
			name: "nil partial",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{nil},
			},
		},
		{
			name: "non-equals operator",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorGreaterThan, Right: validRight}},
			},
		},
		{
			name: "left is not a function invocation",
			cmp: &cypher.Comparison{
				Left:     query.NodeProperty("x"),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorEquals, Right: validRight}},
			},
		},
		{
			name: "function invocation is not id()",
			cmp: &cypher.Comparison{
				Left:     &cypher.FunctionInvocation{Name: "count", Arguments: []cypher.Expression{query.Start()}},
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorEquals, Right: validRight}},
			},
		},
		{
			name: "id() with no arguments",
			cmp: &cypher.Comparison{
				Left:     &cypher.FunctionInvocation{Name: "id", Arguments: nil},
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorEquals, Right: validRight}},
			},
		},
		{
			name: "id() argument is not a variable",
			cmp: &cypher.Comparison{
				Left:     &cypher.FunctionInvocation{Name: "id", Arguments: []cypher.Expression{query.NodeProperty("x")}},
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorEquals, Right: validRight}},
			},
		},
		{
			name: "right-hand side does not unwrap to an id",
			cmp: &cypher.Comparison{
				Left:     query.StartID(),
				Partials: []*cypher.PartialComparison{{Operator: cypher.OperatorEquals, Right: &cypher.Parameter{Value: "not an id"}}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("matchIDEquals() panicked: %v", r)
				}
			}()
			if _, _, ok := matchIDEquals(tc.cmp); ok {
				t.Fatalf("matchIDEquals() ok = true, want false")
			}
		})
	}

	if symbol, id, ok := matchIDEquals(query.Equals(query.StartID(), graph.ID(5))); !ok || symbol != startSymbol || id != 5 {
		t.Fatalf("matchIDEquals() = (%q, %v, %v), want (%q, 5, true)", symbol, id, ok, startSymbol)
	}
}

// TestMatchRelationshipKinds exercises matchRelationshipKinds's rejections
// directly, including a reference type FromCriteria's KindMatcher case
// never independently exercises (a non-Variable reference).
func TestMatchRelationshipKinds(t *testing.T) {
	tests := []struct {
		name string
		km   *cypher.KindMatcher
	}{
		{name: "nil matcher", km: nil},
		{name: "reference is not a variable", km: &cypher.KindMatcher{Reference: query.NodeProperty("x")}},
		{name: "reference is the node variable", km: &cypher.KindMatcher{Reference: query.Node()}},
		{name: "nil reference", km: &cypher.KindMatcher{Reference: nil}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := matchRelationshipKinds(tc.km); ok {
				t.Fatalf("matchRelationshipKinds() ok = true, want false")
			}
		})
	}

	memberOf := graph.StringKind("MemberOf")
	kinds, ok := matchRelationshipKinds(query.KindIn(query.Relationship(), memberOf))
	if !ok || len(kinds) != 1 || kinds[0] != memberOf {
		t.Fatalf("matchRelationshipKinds() = (%v, %v), want ([MemberOf], true)", kinds, ok)
	}
}
