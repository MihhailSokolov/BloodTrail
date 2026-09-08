// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"
)

func TestChangeSetEmptyInitially(t *testing.T) {
	var c ChangeSet
	if !c.Empty() {
		t.Fatalf("zero-value ChangeSet.Empty() = false, want true")
	}
	if ok, reasons := c.HasFallback(); ok || reasons != nil {
		t.Fatalf("zero-value ChangeSet.HasFallback() = (%v, %v), want (false, nil)", ok, reasons)
	}
	if got := c.NodeIDs(); got != nil {
		t.Fatalf("NodeIDs() = %v, want nil", got)
	}
	if got := c.NodeObjectIDs(); got != nil {
		t.Fatalf("NodeObjectIDs() = %v, want nil", got)
	}
	if got := c.EdgeIDs(); got != nil {
		t.Fatalf("EdgeIDs() = %v, want nil", got)
	}
	if got := c.EdgeTriples(); got != nil {
		t.Fatalf("EdgeTriples() = %v, want nil", got)
	}
	if got := c.EdgeTriplesByObjectID(); got != nil {
		t.Fatalf("EdgeTriplesByObjectID() = %v, want nil", got)
	}
	if got := c.NodeKindCriteria(); got != nil {
		t.Fatalf("NodeKindCriteria() = %v, want nil", got)
	}
	if got := c.EdgeKindCriteria(); got != nil {
		t.Fatalf("EdgeKindCriteria() = %v, want nil", got)
	}
}

func TestChangeSetRecordNodeIDDedupsAndSorts(t *testing.T) {
	var c ChangeSet
	c.RecordNodeID(graph.ID(5))
	c.RecordNodeID(graph.ID(1))
	c.RecordNodeID(graph.ID(5))

	if c.Empty() {
		t.Fatalf("Empty() = true after RecordNodeID, want false")
	}
	if got, want := c.NodeIDs(), []uint64{1, 5}; !reflect.DeepEqual(got, want) {
		t.Fatalf("NodeIDs() = %v, want %v", got, want)
	}
}

func TestChangeSetRecordNodeObjectIDDedupsAndSorts(t *testing.T) {
	var c ChangeSet
	c.RecordNodeObjectID("bbb")
	c.RecordNodeObjectID("aaa")
	c.RecordNodeObjectID("bbb")

	if got, want := c.NodeObjectIDs(), []string{"aaa", "bbb"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("NodeObjectIDs() = %v, want %v", got, want)
	}
}

func TestChangeSetRecordEdgeIDDedupsAndSorts(t *testing.T) {
	var c ChangeSet
	c.RecordEdgeID(graph.ID(9))
	c.RecordEdgeID(graph.ID(2))
	c.RecordEdgeID(graph.ID(9))

	if got, want := c.EdgeIDs(), []uint64{2, 9}; !reflect.DeepEqual(got, want) {
		t.Fatalf("EdgeIDs() = %v, want %v", got, want)
	}
}

func TestChangeSetRecordEdgeTripleDedups(t *testing.T) {
	hasSession := graph.StringKind("HasSession")
	adminTo := graph.StringKind("AdminTo")

	var c ChangeSet
	c.RecordEdgeTriple(1, 2, hasSession)
	c.RecordEdgeTriple(1, 2, hasSession) // exact duplicate
	c.RecordEdgeTriple(3, 4, adminTo)

	want := []EdgeTripleRef{
		{Start: 1, End: 2, Kind: hasSession},
		{Start: 3, End: 4, Kind: adminTo},
	}
	if got := c.EdgeTriples(); !reflect.DeepEqual(got, want) {
		t.Fatalf("EdgeTriples() = %+v, want %+v", got, want)
	}
}

func TestChangeSetRecordEdgeTripleByObjectIDDedups(t *testing.T) {
	memberOf := graph.StringKind("MemberOf")

	var c ChangeSet
	c.RecordEdgeTripleByObjectID("s1", "e1", memberOf)
	c.RecordEdgeTripleByObjectID("s1", "e1", memberOf)
	c.RecordEdgeTripleByObjectID("s2", "e2", memberOf)

	want := []EdgeTripleOIDRef{
		{StartOID: "s1", EndOID: "e1", Kind: memberOf},
		{StartOID: "s2", EndOID: "e2", Kind: memberOf},
	}
	if got := c.EdgeTriplesByObjectID(); !reflect.DeepEqual(got, want) {
		t.Fatalf("EdgeTriplesByObjectID() = %+v, want %+v", got, want)
	}
}

func TestChangeSetRecordDeleteNodesByKindsDedupsRegardlessOfOrder(t *testing.T) {
	user := graph.StringKind("User")
	computer := graph.StringKind("Computer")
	base := graph.StringKind("Base")

	var c ChangeSet
	c.RecordDeleteNodesByKinds(graph.Kinds{user, computer}, graph.Kinds{base})
	// Same include set, different slice order -- must still dedup to one
	// entry, since kindsKey sorts before joining.
	c.RecordDeleteNodesByKinds(graph.Kinds{computer, user}, graph.Kinds{base})
	c.RecordDeleteNodesByKinds(graph.Kinds{user}, nil)

	got := c.NodeKindCriteria()
	if len(got) != 2 {
		t.Fatalf("NodeKindCriteria() = %+v, want 2 entries", got)
	}
}

func TestChangeSetRecordDeleteRelationshipsByKindsDedupsRegardlessOfOrder(t *testing.T) {
	hasSession := graph.StringKind("HasSession")
	adminTo := graph.StringKind("AdminTo")

	var c ChangeSet
	c.RecordDeleteRelationshipsByKinds(graph.Kinds{hasSession, adminTo})
	c.RecordDeleteRelationshipsByKinds(graph.Kinds{adminTo, hasSession})

	got := c.EdgeKindCriteria()
	if len(got) != 1 {
		t.Fatalf("EdgeKindCriteria() = %+v, want 1 entry", got)
	}
}

func TestChangeSetRecordFallbackDedups(t *testing.T) {
	var c ChangeSet
	c.RecordFallback("raw SQL")
	c.RecordFallback("raw SQL")
	c.RecordFallback("WithGraph")

	ok, reasons := c.HasFallback()
	if !ok {
		t.Fatalf("HasFallback() ok = false, want true")
	}
	want := []string{"WithGraph", "raw SQL"}
	if !reflect.DeepEqual(reasons, want) {
		t.Fatalf("HasFallback() reasons = %v, want %v", reasons, want)
	}
}

// TestChangeSetAccessorsReturnFreshCopies covers the "caller may freely
// mutate the result" promise every accessor's doc makes: mutating a
// returned slice must not affect a later call.
func TestChangeSetAccessorsReturnFreshCopies(t *testing.T) {
	var c ChangeSet
	c.RecordNodeID(graph.ID(1))
	c.RecordNodeID(graph.ID(2))

	got := c.NodeIDs()
	got[0] = 999

	if again := c.NodeIDs(); reflect.DeepEqual(again, got) {
		t.Fatalf("NodeIDs() result shares storage across calls -- mutation leaked")
	}
}

// TestChangeSetEmbeddedInWriteScopeViaChanges covers the actual integration
// point observers use: WriteScope.Changes() must return a pointer aliasing
// the same storage every time it's called on the same *WriteScope, so a
// sequence of RecordXxx calls from different observer methods (which each
// call scope.Changes() fresh) accumulate onto one ChangeSet rather than
// silently resetting it.
func TestChangeSetEmbeddedInWriteScopeViaChanges(t *testing.T) {
	scope := NewWriteScope()

	scope.Changes().RecordNodeID(graph.ID(1))
	scope.Changes().RecordNodeID(graph.ID(2))

	if got, want := scope.Changes().NodeIDs(), []uint64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("scope.Changes().NodeIDs() = %v, want %v", got, want)
	}
	// The WriteScope's own Touch*/Delete* dimensions (Empty()) must stay
	// completely unaware of the ChangeSet -- see ChangeSet's own doc.
	if !scope.Empty() {
		t.Fatalf("WriteScope.Empty() = false after only Changes() calls, want true (Touch*/Delete* untouched)")
	}
	if scope.Changes().Empty() {
		t.Fatalf("ChangeSet.Empty() = true after RecordNodeID calls, want false")
	}
}
