// SPDX-License-Identifier: Apache-2.0

package recognize

import (
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// TestFromReturning_Accepts covers the three canonical query.Returning
// shapes BloodHound's own upstream code actually issues: container/fetch.go's
// bulk edge-list fetch and traversal/traversal.go's shallowFetchRelationships
// outbound/inbound step projections.
func TestFromReturning_Accepts(t *testing.T) {
	tests := []struct {
		name string
		crit graph.Criteria
		want RowProjection
	}{
		{
			name: "container/fetch.go bulk edge list: Returning(StartID(), EndID())",
			crit: query.Returning(query.StartID(), query.EndID()),
			want: ProjectionStartEnd,
		},
		{
			name: "shallowFetchRelationships outbound step",
			crit: query.Returning(
				query.EndID(),
				query.KindsOf(query.End()),
				query.RelationshipID(),
				query.KindsOf(query.Relationship()),
			),
			want: ProjectionStepOutbound,
		},
		{
			name: "shallowFetchRelationships inbound step",
			crit: query.Returning(
				query.StartID(),
				query.KindsOf(query.Start()),
				query.RelationshipID(),
				query.KindsOf(query.Relationship()),
			),
			want: ProjectionStepInbound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := FromReturning(tc.crit)
			if !ok {
				t.Fatalf("FromReturning() ok = false, want true")
			}
			if got != tc.want {
				t.Fatalf("FromReturning() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFromReturning_Rejects is an adversarial false-accept hunt: every shape
// that resembles one of the three accepted forms but differs in item order,
// item count, variable symbol, function name, or a Distinct/All/Order/Skip/
// Limit flag, plus the structural default-deny cases (nil criteria, wrong
// concrete type, a nil/malformed Return, a non-ProjectionItem or aliased
// item).
func TestFromReturning_Rejects(t *testing.T) {
	tests := []struct {
		name string
		crit graph.Criteria
	}{
		{
			name: "bare node variable, not an id() projection",
			crit: query.Returning(query.Node()),
		},
		{
			name: "Distinct set (ReturningDistinct)",
			crit: query.ReturningDistinct(query.StartID(), query.EndID()),
		},
		{
			name: "start/end swapped: wrong order",
			crit: query.Returning(query.EndID(), query.StartID()),
		},
		{
			name: "start/end plus an extra item",
			crit: query.Returning(query.StartID(), query.EndID(), query.RelationshipID()),
		},
		{
			name: "start/end missing the second item",
			crit: query.Returning(query.StartID()),
		},
		{
			name: "empty projection",
			crit: query.Returning(),
		},
		{
			name: "step shape with id/labels items swapped",
			crit: query.Returning(
				query.KindsOf(query.End()),
				query.EndID(),
				query.RelationshipID(),
				query.KindsOf(query.Relationship()),
			),
		},
		{
			name: "outbound step with wrong endpoint symbol on the labels() item",
			crit: query.Returning(
				query.EndID(),
				query.KindsOf(query.Start()),
				query.RelationshipID(),
				query.KindsOf(query.Relationship()),
			),
		},
		{
			name: "outbound step with labels(e) instead of type(r) in the last slot",
			crit: query.Returning(
				query.EndID(),
				query.KindsOf(query.End()),
				query.RelationshipID(),
				query.KindsOf(query.End()),
			),
		},
		{
			name: "inbound step with wrong endpoint symbol on the id() item",
			crit: query.Returning(
				query.EndID(),
				query.KindsOf(query.Start()),
				query.RelationshipID(),
				query.KindsOf(query.Relationship()),
			),
		},
		{
			name: "step shape missing the final type(r) item",
			crit: query.Returning(
				query.EndID(),
				query.KindsOf(query.End()),
				query.RelationshipID(),
			),
		},
		{
			name: "step shape with an extra fifth item",
			crit: query.Returning(
				query.EndID(),
				query.KindsOf(query.End()),
				query.RelationshipID(),
				query.KindsOf(query.Relationship()),
				query.EndID(),
			),
		},
		{
			name: "duplicate StartID twice instead of StartID/EndID",
			crit: query.Returning(query.StartID(), query.StartID()),
		},
		{
			name: "Return with a non-ProjectionItem entry",
			crit: &cypher.Return{Projection: &cypher.Projection{Items: []cypher.Expression{query.StartID()}}},
		},
		{
			name: "Return with an aliased projection item",
			crit: &cypher.Return{Projection: &cypher.Projection{Items: []cypher.Expression{
				&cypher.ProjectionItem{Expression: query.StartID(), Alias: &cypher.Variable{Symbol: "x"}},
				&cypher.ProjectionItem{Expression: query.EndID()},
			}}},
		},
		{
			name: "Return with All set (RETURN *)",
			crit: &cypher.Return{Projection: &cypher.Projection{
				All:   true,
				Items: []cypher.Expression{&cypher.ProjectionItem{Expression: query.StartID()}, &cypher.ProjectionItem{Expression: query.EndID()}},
			}},
		},
		{
			name: "Return with Order set on the projection",
			crit: &cypher.Return{Projection: &cypher.Projection{
				Order: query.OrderBy(query.Relationship()),
				Items: []cypher.Expression{&cypher.ProjectionItem{Expression: query.StartID()}, &cypher.ProjectionItem{Expression: query.EndID()}},
			}},
		},
		{
			name: "Return with Skip set on the projection",
			crit: &cypher.Return{Projection: &cypher.Projection{
				Skip:  query.Offset(1),
				Items: []cypher.Expression{&cypher.ProjectionItem{Expression: query.StartID()}, &cypher.ProjectionItem{Expression: query.EndID()}},
			}},
		},
		{
			name: "Return with Limit set on the projection",
			crit: &cypher.Return{Projection: &cypher.Projection{
				Limit: query.Limit(10),
				Items: []cypher.Expression{&cypher.ProjectionItem{Expression: query.StartID()}, &cypher.ProjectionItem{Expression: query.EndID()}},
			}},
		},
		{
			name: "Return with a nil Projection",
			crit: &cypher.Return{},
		},
		{
			name: "typed nil Return",
			crit: (*cypher.Return)(nil),
		},
		{
			name: "nil criteria",
			crit: nil,
		},
		{
			name: "unrelated criteria type",
			crit: "not a return",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("FromReturning() panicked: %v", r)
				}
			}()

			got, ok := FromReturning(tc.crit)
			if ok {
				t.Fatalf("FromReturning() ok = true, want false (got %v)", got)
			}
		})
	}
}

// TestOrderIsEdgeIDAscending_Accepts covers both spellings dawgs itself
// builds for the relationship-id paging order (traversal/traversal.go:539
// and ops/traversal.go:100) -- query.Order always returns a *cypher.SortItem
// regardless of which was used, so both must be recognized identically.
func TestOrderIsEdgeIDAscending_Accepts(t *testing.T) {
	tests := []struct {
		name string
		crit []graph.Criteria
	}{
		{
			name: "traversal.go:539 spelling: Order(Identity(Relationship()), Ascending())",
			crit: []graph.Criteria{query.Order(query.Identity(query.Relationship()), query.Ascending())},
		},
		{
			name: "ops/traversal.go:100 spelling: Order(Relationship(), Ascending())",
			crit: []graph.Criteria{query.Order(query.Relationship(), query.Ascending())},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !OrderIsEdgeIDAscending(tc.crit) {
				t.Fatalf("OrderIsEdgeIDAscending() = false, want true")
			}
		})
	}
}

// TestOrderIsEdgeIDAscending_Rejects is an adversarial false-accept hunt:
// descending order, ordering by the wrong variable (notably NodeID(), the
// most obvious near-miss), the wrong count of criteria, and structural
// default-deny cases.
func TestOrderIsEdgeIDAscending_Rejects(t *testing.T) {
	tests := []struct {
		name string
		crit []graph.Criteria
	}{
		{
			name: "descending order on the relationship id",
			crit: []graph.Criteria{query.Order(query.Identity(query.Relationship()), query.Descending())},
		},
		{
			name: "descending order, bare Relationship() spelling",
			crit: []graph.Criteria{query.Order(query.Relationship(), query.Descending())},
		},
		{
			name: "order by NodeID() ascending",
			crit: []graph.Criteria{query.Order(query.NodeID(), query.Ascending())},
		},
		{
			name: "order by the bare node variable ascending",
			crit: []graph.Criteria{query.Order(query.Node(), query.Ascending())},
		},
		{
			name: "order by StartID() ascending",
			crit: []graph.Criteria{query.Order(query.StartID(), query.Ascending())},
		},
		{
			name: "order by EndID() ascending",
			crit: []graph.Criteria{query.Order(query.EndID(), query.Ascending())},
		},
		{
			name: "empty criteria list",
			crit: []graph.Criteria{},
		},
		{
			name: "nil criteria list",
			crit: nil,
		},
		{
			name: "two criteria, both otherwise valid",
			crit: []graph.Criteria{
				query.Order(query.Relationship(), query.Ascending()),
				query.Order(query.Relationship(), query.Ascending()),
			},
		},
		{
			name: "element is not a SortItem",
			crit: []graph.Criteria{query.Relationship()},
		},
		{
			name: "nil element",
			crit: []graph.Criteria{nil},
		},
		{
			name: "typed nil SortItem",
			crit: []graph.Criteria{(*cypher.SortItem)(nil)},
		},
		{
			name: "SortItem over an unrelated expression",
			crit: []graph.Criteria{&cypher.SortItem{Ascending: true, Expression: query.StartProperty("x")}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("OrderIsEdgeIDAscending() panicked: %v", r)
				}
			}()

			if OrderIsEdgeIDAscending(tc.crit) {
				t.Fatalf("OrderIsEdgeIDAscending() = true, want false")
			}
		})
	}
}
