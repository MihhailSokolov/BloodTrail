// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Kind ids used throughout this file's hand-built snapshot: arbitrary, but
// fixed, so every test can share one snapshot shape and one fake kind
// mapper/resolver pair. Kept distinct from marks_test.go's buildTwoNodeSnapshot
// fixture (100/200), which exercises unrelated noteResolved logic.
const (
	kindUser     snapshot.KindID = 1
	kindComputer snapshot.KindID = 2
	kindGroup    snapshot.KindID = 3
)

// nodeSpecKindNames maps kindUser/kindComputer/kindGroup to their graph.Kind
// names, the direction e.mapKindNames (TryNodeFetchKinds) needs.
func nodeSpecKindNames() map[snapshot.KindID]graph.Kind {
	return map[snapshot.KindID]graph.Kind{
		kindUser:     graph.StringKind("User"),
		kindComputer: graph.StringKind("Computer"),
		kindGroup:    graph.StringKind("Group"),
	}
}

// fakeKindMapper returns an e.mapKind fake resolving each graph.Kind's name
// via byName, failing the call with an error for any name absent from it --
// mapKind's contract distinguishes "unknown/unmappable kind" from "maps to
// nothing" (see matchConstraint's doc), so this must return an error, not a
// zero KindID, for an unrecognized name.
func fakeKindMapper(byName map[string]snapshot.KindID) func(context.Context, graph.Kind) (int16, error) {
	return func(_ context.Context, kind graph.Kind) (int16, error) {
		id, ok := byName[kind.String()]
		if !ok {
			return 0, fmt.Errorf("fakeKindMapper: no id for kind %s", kind.String())
		}
		return id, nil
	}
}

// nodeSpecKindByName is fakeKindMapper's counterpart to nodeSpecKindNames,
// the name->id direction e.mapKind needs.
func nodeSpecKindByName() map[string]snapshot.KindID {
	return map[string]snapshot.KindID{"User": kindUser, "Computer": kindComputer, "Group": kindGroup}
}

// buildNodeSpecSnapshot builds the five-node fixture every TryNodeCount/
// TryNodeFetchIDs/TryNodeFetchKinds test in this file shares:
//
//	database id 1: User
//	database id 2: User, Computer
//	database id 3: Computer
//	database id 4: Group
//	database id 5: User, Group
//
// database ids are deliberately non-contiguous with dense NodeIDs already
// (1..5 maps to dense 0..4) so a test can't accidentally pass by conflating
// the two; see snapshot.Builder's doc for the dense-assignment contract.
func buildNodeSpecSnapshot(t *testing.T) *snapshot.Snapshot {
	t.Helper()

	b := snapshot.NewBuilder(1)
	nodes := []struct {
		id    uint64
		kinds []snapshot.KindID
	}{
		{1, []snapshot.KindID{kindUser}},
		{2, []snapshot.KindID{kindUser, kindComputer}},
		{3, []snapshot.KindID{kindComputer}},
		{4, []snapshot.KindID{kindGroup}},
		{5, []snapshot.KindID{kindUser, kindGroup}},
	}
	for _, n := range nodes {
		if err := b.AddNode(n.id, n.kinds); err != nil {
			t.Fatalf("AddNode(%d): %v", n.id, err)
		}
	}

	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snap
}

// newNodeSpecEngine builds an Engine wired for this file's tests: enabled,
// snap adopted and stamped at generation gen (also the live counter, so
// Fresh()/allNodesClean/nodeKindsClean all read as clean unless a test calls
// NoteWrite afterward), with mapKind/mapKindNames faked via
// nodeSpecKindByName/nodeSpecKindNames (fakeResolver is marks_test.go's
// existing []KindID->graph.Kinds fake, reused here for e.mapKindNames --
// see the task's seam-reuse guidance).
func newNodeSpecEngine(t *testing.T, snap *snapshot.Snapshot, gen uint64) *Engine {
	t.Helper()

	snap.Generation = gen
	e := New(nil, nil, Config{Enabled: true})
	e.mapKind = fakeKindMapper(nodeSpecKindByName())
	e.mapKindNames = fakeResolver(t, nodeSpecKindNames())
	e.snap.Store(snap)
	e.generation.Store(gen)
	return e
}

// drainIDs fully drains an *ascending* graph.ID cursor into a plain slice,
// failing the test on a cursor error.
func drainIDs(t *testing.T, cursor graph.Cursor[graph.ID]) []graph.ID {
	t.Helper()
	defer cursor.Close()

	var ids []graph.ID
	for id := range cursor.Chan() {
		ids = append(ids, id)
	}
	if err := cursor.Error(); err != nil {
		t.Fatalf("cursor.Error() = %v, want nil", err)
	}
	return ids
}

// drainKinds is drainIDs' graph.KindsResult equivalent.
func drainKinds(t *testing.T, cursor graph.Cursor[graph.KindsResult]) []graph.KindsResult {
	t.Helper()
	defer cursor.Close()

	var rows []graph.KindsResult
	for row := range cursor.Chan() {
		rows = append(rows, row)
	}
	if err := cursor.Error(); err != nil {
		t.Fatalf("cursor.Error() = %v, want nil", err)
	}
	return rows
}

// idsOf extracts just the ids from a []graph.KindsResult, for comparing
// TryNodeFetchKinds' output against TryNodeFetchIDs' with the same
// assertIDs helper.
func idsOf(rows []graph.KindsResult) []graph.ID {
	ids := make([]graph.ID, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}
	return ids
}

// assertIDs compares got against want as sets (both sorted first): engine
// output order is a documented ascending-database-id guarantee, but nothing
// about the fixture data here needs the test itself to assume an order
// beyond what dense-NodeID iteration already gives (ascending) -- sorting
// both sides just makes the assertion robust either way.
func assertIDs(t *testing.T, got []graph.ID, want []graph.ID) {
	t.Helper()
	gotSorted := append([]graph.ID(nil), got...)
	wantSorted := append([]graph.ID(nil), want...)
	sort.Slice(gotSorted, func(i, j int) bool { return gotSorted[i] < gotSorted[j] })
	sort.Slice(wantSorted, func(i, j int) bool { return wantSorted[i] < wantSorted[j] })

	if len(gotSorted) != len(wantSorted) {
		t.Fatalf("ids = %v, want %v", got, want)
	}
	for i := range gotSorted {
		if gotSorted[i] != wantSorted[i] {
			t.Fatalf("ids = %v, want %v", got, want)
		}
	}
}

// kindConstraint is a one-line recognize.KindConstraint builder for this
// file's tests: any-of over the given kind names.
func kindConstraint(allOf bool, names ...string) recognize.KindConstraint {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return recognize.KindConstraint{Kinds: kinds, AllOf: allOf}
}

// TestTryNodeQueriesSingleAnyOf covers a single any-of constraint: TryNodeCount,
// TryNodeFetchIDs, and TryNodeFetchKinds must all agree on the User carriers
// (database ids 1, 2, 5).
func TestTryNodeQueriesSingleAnyOf(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	ctx := context.Background()

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeCount: ok = false, want true")
	}
	if count != 3 {
		t.Fatalf("TryNodeCount: count = %d, want 3", count)
	}

	idCursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchIDs: ok = false, want true")
	}
	assertIDs(t, drainIDs(t, idCursor), []graph.ID{1, 2, 5})

	kindsCursor, ok := e.TryNodeFetchKinds(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchKinds: ok = false, want true")
	}
	rows := drainKinds(t, kindsCursor)
	assertIDs(t, idsOf(rows), []graph.ID{1, 2, 5})
	for _, row := range rows {
		found := false
		for _, k := range row.Kinds {
			if k.String() == "User" {
				found = true
			}
		}
		if !found {
			t.Fatalf("row %+v does not carry User", row)
		}
	}
}

// TestTryNodeQueriesMultiKindAnyOfUnion covers an any-of constraint over two
// kinds: the match set is their union (User: 1,2,5; Group: 4,5 -> 1,2,4,5).
func TestTryNodeQueriesMultiKindAnyOfUnion(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User", "Group")}}
	ctx := context.Background()

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok || count != 4 {
		t.Fatalf("TryNodeCount = (%d, %v), want (4, true)", count, ok)
	}

	cursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchIDs: ok = false, want true")
	}
	assertIDs(t, drainIDs(t, cursor), []graph.ID{1, 2, 4, 5})
}

// TestTryNodeQueriesAllOfIntersection covers an all-of constraint: only node
// 5 carries both User and Group.
func TestTryNodeQueriesAllOfIntersection(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(true, "User", "Group")}}
	ctx := context.Background()

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok || count != 1 {
		t.Fatalf("TryNodeCount = (%d, %v), want (1, true)", count, ok)
	}

	cursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchIDs: ok = false, want true")
	}
	assertIDs(t, drainIDs(t, cursor), []graph.ID{5})
}

// TestTryNodeQueriesKindAndIDsIntersect covers a kind constraint combined
// with spec.IDs: the User carriers (1,2,5) intersected with {2,3,5} leaves
// {2,5}.
func TestTryNodeQueriesKindAndIDsIntersect(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	spec := recognize.NodeSpec{
		Constraints: []recognize.KindConstraint{kindConstraint(false, "User")},
		IDs:         []graph.ID{2, 3, 5},
	}
	ctx := context.Background()

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok || count != 2 {
		t.Fatalf("TryNodeCount = (%d, %v), want (2, true)", count, ok)
	}

	cursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchIDs: ok = false, want true")
	}
	assertIDs(t, drainIDs(t, cursor), []graph.ID{2, 5})
}

// TestTryNodeQueriesUnknownIDDrops covers an id in spec.IDs that the
// snapshot doesn't recognize: it drops silently rather than erroring or
// widening the match.
func TestTryNodeQueriesUnknownIDDrops(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	spec := recognize.NodeSpec{
		Constraints: []recognize.KindConstraint{kindConstraint(false, "User")},
		IDs:         []graph.ID{2, 999},
	}
	ctx := context.Background()

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok || count != 1 {
		t.Fatalf("TryNodeCount = (%d, %v), want (1, true)", count, ok)
	}

	cursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchIDs: ok = false, want true")
	}
	assertIDs(t, drainIDs(t, cursor), []graph.ID{2})
}

// TestTryNodeQueriesNoConstraintDeclines covers the zero-Constraints
// decline: even with spec.IDs set, an id-only node query has no kind to
// prove freshness against and must decline reasonNoKindConstraint.
func TestTryNodeQueriesNoConstraintDeclines(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	spec := recognize.NodeSpec{IDs: []graph.ID{1, 2}}
	ctx := context.Background()

	if _, ok := e.TryNodeCount(ctx, spec); ok {
		t.Fatalf("TryNodeCount: ok = true, want false (no constraints)")
	}
	if _, ok := e.TryNodeFetchIDs(ctx, spec); ok {
		t.Fatalf("TryNodeFetchIDs: ok = true, want false (no constraints)")
	}
	if _, ok := e.TryNodeFetchKinds(ctx, spec); ok {
		t.Fatalf("TryNodeFetchKinds: ok = true, want false (no constraints)")
	}
}

// TestTryNodeQueriesKindStaleAfterNoteWrite covers the kind-scoped
// freshness gate: a NoteWrite that touches the spec's constrained kind
// (User) must make the (unchanged) snapshot decline reasonKindStale, even
// though its own Generation hasn't moved -- exactly the scenario
// nodeKindsClean exists to catch (marks.go's doc). A constraint over an
// untouched kind (Group) must still serve normally.
func TestTryNodeQueriesKindStaleAfterNoteWrite(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	ctx := context.Background()

	scope := NewWriteScope()
	scope.TouchNodeKinds(graph.Kinds{graph.StringKind("User")})
	e.NoteWrite(scope)

	userSpec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	if _, ok := e.TryNodeCount(ctx, userSpec); ok {
		t.Fatalf("TryNodeCount(User): ok = true, want false (kind_stale)")
	}

	groupSpec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "Group")}}
	count, ok := e.TryNodeCount(ctx, groupSpec)
	if !ok {
		t.Fatalf("TryNodeCount(Group): ok = false, want true (Group untouched by the write)")
	}
	if count != 2 {
		t.Fatalf("TryNodeCount(Group) = %d, want 2", count)
	}
}

// TestTryNodeQueriesServeAgainAfterFreshSnapshot covers recovery from a
// kind-stale decline: once a fresh snapshot is adopted at the write's new
// generation (what RebuildNow would do), the previously-stale spec must
// serve again.
func TestTryNodeQueriesServeAgainAfterFreshSnapshot(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	ctx := context.Background()

	scope := NewWriteScope()
	scope.TouchNodeKinds(graph.Kinds{graph.StringKind("User")})
	e.NoteWrite(scope)

	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	if _, ok := e.TryNodeCount(ctx, spec); ok {
		t.Fatalf("TryNodeCount before rebuild: ok = true, want false (kind_stale)")
	}

	freshSnap := buildNodeSpecSnapshot(t)
	freshSnap.Generation = e.Generation()
	e.snap.Store(freshSnap)

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeCount after rebuild: ok = false, want true")
	}
	if count != 3 {
		t.Fatalf("TryNodeCount after rebuild = %d, want 3", count)
	}
}

// TestTryNodeQueriesDisabledDeclines covers cfg.Enabled = false: every entry
// point declines immediately, before ever touching the snapshot (nil here,
// which would otherwise decline reasonNoSnapshot -- proving Enabled is
// actually checked first).
func TestTryNodeQueriesDisabledDeclines(t *testing.T) {
	e := New(nil, nil, Config{Enabled: false})
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	ctx := context.Background()

	if _, ok := e.TryNodeCount(ctx, spec); ok {
		t.Fatalf("TryNodeCount: ok = true, want false (disabled)")
	}
	if _, ok := e.TryNodeFetchIDs(ctx, spec); ok {
		t.Fatalf("TryNodeFetchIDs: ok = true, want false (disabled)")
	}
	if _, ok := e.TryNodeFetchKinds(ctx, spec); ok {
		t.Fatalf("TryNodeFetchKinds: ok = true, want false (disabled)")
	}
}

// TestTryNodeQueriesMapKindErrorDeclines covers a mapKind failure: an
// unrecognized kind name declines reasonError rather than being treated as
// "matches nothing" -- the same ambiguity-averse stance
// resolveKindsEndpoint/buildKindMask already take for the path-query side.
func TestTryNodeQueriesMapKindErrorDeclines(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "Nonexistent")}}
	ctx := context.Background()

	if _, ok := e.TryNodeCount(ctx, spec); ok {
		t.Fatalf("TryNodeCount: ok = true, want false (unmappable kind)")
	}
}

// TestTryNodeFetchKindsResolveErrorDeclines covers TryNodeFetchKinds'
// eager-resolution failure path: e.mapKindNames failing for a KindID
// actually present among the matching nodes declines reasonError before any
// cursor is handed back, rather than surfacing mid-stream.
func TestTryNodeFetchKindsResolveErrorDeclines(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	e.mapKindNames = func(ids []snapshot.KindID) (graph.Kinds, error) {
		return nil, errors.New("boom: kind id resolution failed")
	}

	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	if _, ok := e.TryNodeFetchKinds(context.Background(), spec); ok {
		t.Fatalf("TryNodeFetchKinds: ok = true, want false (mapKindNames failed)")
	}
}

// TestTryNodeFetchIDsCloseDoesNotLeakFeeder is this file's cursor-leak
// regression test, modeled on
// TestFetchAllShortestPathsClosesCursorWhenDelegateReturnsEarly
// (engine_serving_integration_test.go): a cursor that is drained partially
// and then Close()d must not leave its feeder goroutine blocked forever.
// Unlike that integration test (which needs a live database to reach a
// served engine.NewPathCursor at all), TryNodeFetchIDs serves entirely from
// a hand-built snapshot, so this runs as a pure unit test.
//
// The fixture is built large enough (well over rowCursorBuffer) that the
// feeder cannot finish submitting every value into the buffered channel
// before the test goroutine stops draining -- otherwise Close() would have
// nothing left to unblock, and the test would pass even with the pre-fix
// unbuffered-and-never-cancelled version of this cursor.
func TestTryNodeFetchIDsCloseDoesNotLeakFeeder(t *testing.T) {
	const nodeCount = rowCursorBuffer * 4

	b := snapshot.NewBuilder(1)
	for i := uint64(1); i <= nodeCount; i++ {
		if err := b.AddNode(i, []snapshot.KindID{kindUser}); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	e := newNodeSpecEngine(t, snap, 0)
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}

	runtime.GC()
	before := runtime.NumGoroutine()

	const attempts = 20
	for i := 0; i < attempts; i++ {
		cursor, ok := e.TryNodeFetchIDs(context.Background(), spec)
		if !ok {
			t.Fatalf("TryNodeFetchIDs (attempt %d): ok = false, want true", i)
		}

		// Drain a handful of values, then stop early without exhausting
		// Chan() -- the shape that leaked a feeder goroutine before
		// newFeedCursor honored ctx cancellation on Close().
		drained := 0
		for range cursor.Chan() {
			drained++
			if drained >= 3 {
				break
			}
		}
		cursor.Close()
	}

	const slack = 3
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.GC()
		after := runtime.NumGoroutine()
		if after <= before+slack {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("feeder goroutines leaked: before=%d after=%d (delta %d over %d attempts)", before, after, after-before, attempts)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTryNodeQueriesEmptyIDsMatchesNothing covers the nil-vs-empty-IDs contract:
// a non-nil, zero-length IDs slice is a well-formed "matches nothing" spec (e.g.
// id(n)=1 AND id(n)=2). TryNodeCount must return (0, true), and TryNodeFetchIDs
// must yield no rows. This regression test ensures resolveNodeSpec branches on
// spec.IDs != nil instead of len(spec.IDs) > 0.
func TestTryNodeQueriesEmptyIDsMatchesNothing(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	// User kind matches 1, 2, 5, but empty IDs slice means intersection is empty
	spec := recognize.NodeSpec{
		Constraints: []recognize.KindConstraint{kindConstraint(false, "User")},
		IDs:         []graph.ID{},
	}
	ctx := context.Background()

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeCount: ok = false, want true")
	}
	if count != 0 {
		t.Fatalf("TryNodeCount: count = %d, want 0 (empty IDs means matches nothing)", count)
	}

	idCursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchIDs: ok = false, want true")
	}
	ids := drainIDs(t, idCursor)
	if len(ids) != 0 {
		t.Fatalf("TryNodeFetchIDs: got %v, want empty (empty IDs means matches nothing)", ids)
	}

	kindsCursor, ok := e.TryNodeFetchKinds(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchKinds: ok = false, want true")
	}
	rows := drainKinds(t, kindsCursor)
	if len(rows) != 0 {
		t.Fatalf("TryNodeFetchKinds: got %v, want empty (empty IDs means matches nothing)", rows)
	}
}

// TestTryNodeQueriesNilIDsUnconstrained covers the nil-vs-empty-IDs contract:
// a nil IDs means id() was never constrained, so all kind-matched nodes should
// be returned. This test ensures that after fixing the empty-IDs bug, nil IDs
// still work correctly.
func TestTryNodeQueriesNilIDsUnconstrained(t *testing.T) {
	e := newNodeSpecEngine(t, buildNodeSpecSnapshot(t), 0)
	// User kind matches 1, 2, 5; nil IDs means all of them pass through
	spec := recognize.NodeSpec{
		Constraints: []recognize.KindConstraint{kindConstraint(false, "User")},
		IDs:         nil,
	}
	ctx := context.Background()

	count, ok := e.TryNodeCount(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeCount: ok = false, want true")
	}
	if count != 3 {
		t.Fatalf("TryNodeCount: count = %d, want 3 (nil IDs means unconstrained)", count)
	}

	idCursor, ok := e.TryNodeFetchIDs(ctx, spec)
	if !ok {
		t.Fatalf("TryNodeFetchIDs: ok = false, want true")
	}
	assertIDs(t, drainIDs(t, idCursor), []graph.ID{1, 2, 5})
}
