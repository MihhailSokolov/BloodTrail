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

// ---------------------------------------------------------------------------
// Task 7: TryRelCount / TryRelFetchIDs / TryRelFetchTriples /
// TryRelFetchKinds / TryRelQueryRows.
// ---------------------------------------------------------------------------

// Kind ids used by this file's relationship-spec fixture
// (buildRelSpecSnapshot): sharing kindUser/kindComputer/kindGroup above for
// node kinds and adding three edge kinds here, contiguous with them (4, 5,
// 6) rather than leaving a gap -- a real KindMapper (dawgs' pg.SchemaManager
// and InMemoryKindMapper alike) hands out ids sequentially from 1 with no
// gaps, out of one shared space for both node labels and relationship
// types, and selectKindIDs (serve_builder.go) resolves every id up to
// snap.MaxKindID for a step projection regardless of which kinds actually
// appear in this fixture's data -- a gapped, non-sequential choice of
// constants here would make fakeResolver fail on an id no real KindMapper
// would ever have left unassigned, which is not the failure mode this
// file's tests are for.
const (
	kindMemberOf   snapshot.KindID = 4
	kindAdminTo    snapshot.KindID = 5
	kindHasSession snapshot.KindID = 6
)

// relSpecKindNames extends nodeSpecKindNames with this file's three edge
// kinds, so a single e.mapKindNames fake can resolve both node and edge
// KindIDs, matching how one real KindMapper serves both.
func relSpecKindNames() map[snapshot.KindID]graph.Kind {
	names := nodeSpecKindNames()
	names[kindMemberOf] = graph.StringKind("MemberOf")
	names[kindAdminTo] = graph.StringKind("AdminTo")
	names[kindHasSession] = graph.StringKind("HasSession")
	return names
}

// relSpecKindByName is relSpecKindNames' name->id counterpart, for e.mapKind.
func relSpecKindByName() map[string]snapshot.KindID {
	byName := nodeSpecKindByName()
	byName["MemberOf"] = kindMemberOf
	byName["AdminTo"] = kindAdminTo
	byName["HasSession"] = kindHasSession
	return byName
}

// edgeKinds is kindConstraint's plain-graph.Kinds counterpart, for building
// a RelSpec.EdgeKinds value from bare kind names.
func edgeKinds(names ...string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// buildRelSpecSnapshot builds the five-node, seven-edge fixture every
// TryRelCount/TryRelFetchIDs/TryRelFetchTriples/TryRelFetchKinds/
// TryRelQueryRows test in this file shares:
//
//	database id 1: User
//	database id 2: User
//	database id 3: Computer
//	database id 4: Computer
//	database id 5: Group
//
//	edge id 101: 1 -[MemberOf]->    5
//	edge id 102: 2 -[MemberOf]->    5
//	edge id 103: 1 -[AdminTo]->     3
//	edge id 104: 2 -[AdminTo]->     4
//	edge id 105: 3 -[HasSession]->  1
//	edge id 106: 4 -[HasSession]->  2
//	edge id 107: 1 -[AdminTo]->     4
//
// Node 1 has three outgoing edges (exercising a start-anchored scan's
// multi-slot per-node walk), nodes 4 and 5 each have two incoming edges
// (exercising an end-anchored scan the same way), and the AdminTo kind is
// shared by three edges spanning three different (start, end) pairs, giving
// every kind-mask/endpoint-bitmap/anchor combination this file's tests
// exercise a distinct, hand-checkable expected answer. database ids (both
// node and edge) are deliberately unrelated to dense NodeID/CSR slot order.
func buildRelSpecSnapshot(t *testing.T) *snapshot.Snapshot {
	t.Helper()

	b := snapshot.NewBuilder(1)
	nodes := []struct {
		id    uint64
		kinds []snapshot.KindID
	}{
		{1, []snapshot.KindID{kindUser}},
		{2, []snapshot.KindID{kindUser}},
		{3, []snapshot.KindID{kindComputer}},
		{4, []snapshot.KindID{kindComputer}},
		{5, []snapshot.KindID{kindGroup}},
	}
	for _, n := range nodes {
		if err := b.AddNode(n.id, n.kinds); err != nil {
			t.Fatalf("AddNode(%d): %v", n.id, err)
		}
	}

	edges := []struct {
		id, start, end uint64
		kind           snapshot.KindID
	}{
		{101, 1, 5, kindMemberOf},
		{102, 2, 5, kindMemberOf},
		{103, 1, 3, kindAdminTo},
		{104, 2, 4, kindAdminTo},
		{105, 3, 1, kindHasSession},
		{106, 4, 2, kindHasSession},
		{107, 1, 4, kindAdminTo},
	}
	for _, e := range edges {
		b.AddEdge(e.id, e.start, e.end, e.kind)
	}

	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snap
}

// newRelSpecEngine is newNodeSpecEngine's RelSpec counterpart: an Engine
// wired with relSpecKindByName/relSpecKindNames instead of
// nodeSpecKindByName/nodeSpecKindNames, otherwise identical.
func newRelSpecEngine(t *testing.T, snap *snapshot.Snapshot, gen uint64) *Engine {
	t.Helper()

	snap.Generation = gen
	e := New(nil, nil, Config{Enabled: true})
	e.mapKind = fakeKindMapper(relSpecKindByName())
	e.mapKindNames = fakeResolver(t, relSpecKindNames())
	e.snap.Store(snap)
	e.generation.Store(gen)
	return e
}

// drainTriples is drainIDs' graph.RelationshipTripleResult equivalent.
func drainTriples(t *testing.T, cursor graph.Cursor[graph.RelationshipTripleResult]) []graph.RelationshipTripleResult {
	t.Helper()
	defer cursor.Close()

	var rows []graph.RelationshipTripleResult
	for row := range cursor.Chan() {
		rows = append(rows, row)
	}
	if err := cursor.Error(); err != nil {
		t.Fatalf("cursor.Error() = %v, want nil", err)
	}
	return rows
}

// drainRelKinds is drainTriples' graph.RelationshipKindsResult equivalent.
func drainRelKinds(t *testing.T, cursor graph.Cursor[graph.RelationshipKindsResult]) []graph.RelationshipKindsResult {
	t.Helper()
	defer cursor.Close()

	var rows []graph.RelationshipKindsResult
	for row := range cursor.Chan() {
		rows = append(rows, row)
	}
	if err := cursor.Error(); err != nil {
		t.Fatalf("cursor.Error() = %v, want nil", err)
	}
	return rows
}

// assertTriples compares got against want as sets (both sorted by ID first),
// the graph.RelationshipTripleResult equivalent of assertIDs.
func assertTriples(t *testing.T, got []graph.RelationshipTripleResult, want []graph.RelationshipTripleResult) {
	t.Helper()

	gotSorted := append([]graph.RelationshipTripleResult(nil), got...)
	wantSorted := append([]graph.RelationshipTripleResult(nil), want...)
	sort.Slice(gotSorted, func(i, j int) bool { return gotSorted[i].ID < gotSorted[j].ID })
	sort.Slice(wantSorted, func(i, j int) bool { return wantSorted[i].ID < wantSorted[j].ID })

	if len(gotSorted) != len(wantSorted) {
		t.Fatalf("triples = %+v, want %+v", got, want)
	}
	for i := range gotSorted {
		if gotSorted[i] != wantSorted[i] {
			t.Fatalf("triples = %+v, want %+v", got, want)
		}
	}
}

// TestTryRelQueriesStartAnchoredKindAndFarKindFilter covers a start-anchored
// scan (spec.StartIDs = {1}) with both an edge-kind filter (AdminTo) and a
// far-endpoint (end) kind constraint (Computer): node 1's three outgoing
// edges are MemberOf->5, AdminTo->3, AdminTo->4; only the two AdminTo edges
// survive the kind mask, and both happen to end on a Computer, so both
// survive the far-kind filter too.
func TestTryRelQueriesStartAnchoredKindAndFarKindFilter(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	spec := recognize.RelSpec{
		StartIDs:       []graph.ID{1},
		EdgeKinds:      edgeKinds("AdminTo"),
		EndConstraints: []recognize.KindConstraint{kindConstraint(false, "Computer")},
	}

	count, ok := e.TryRelCount(ctx, spec)
	if !ok || count != 2 {
		t.Fatalf("TryRelCount = (%d, %v), want (2, true)", count, ok)
	}

	cursor, ok := e.TryRelFetchTriples(ctx, spec)
	if !ok {
		t.Fatalf("TryRelFetchTriples: ok = false, want true")
	}
	assertTriples(t, drainTriples(t, cursor), []graph.RelationshipTripleResult{
		{ID: 103, StartID: 1, EndID: 3},
		{ID: 107, StartID: 1, EndID: 4},
	})
}

// TestTryRelQueriesEndAnchoredKindAndFarKindFilter is the end-anchored
// mirror: spec.EndIDs = {1}'s only incoming edge is HasSession from node 3,
// and a Computer start constraint keeps it (node 3 is a Computer).
func TestTryRelQueriesEndAnchoredKindAndFarKindFilter(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	spec := recognize.RelSpec{
		EndIDs:           []graph.ID{1},
		EdgeKinds:        edgeKinds("HasSession"),
		StartConstraints: []recognize.KindConstraint{kindConstraint(false, "Computer")},
	}

	count, ok := e.TryRelCount(ctx, spec)
	if !ok || count != 1 {
		t.Fatalf("TryRelCount = (%d, %v), want (1, true)", count, ok)
	}

	cursor, ok := e.TryRelFetchTriples(ctx, spec)
	if !ok {
		t.Fatalf("TryRelFetchTriples: ok = false, want true")
	}
	assertTriples(t, drainTriples(t, cursor), []graph.RelationshipTripleResult{
		{ID: 105, StartID: 3, EndID: 1},
	})
}

// TestTryRelQueriesFullScanPairs covers the no-anchor branch: every MemberOf
// edge in the whole snapshot, regardless of source.
func TestTryRelQueriesFullScanPairs(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	spec := recognize.RelSpec{EdgeKinds: edgeKinds("MemberOf")}

	count, ok := e.TryRelCount(ctx, spec)
	if !ok || count != 2 {
		t.Fatalf("TryRelCount = (%d, %v), want (2, true)", count, ok)
	}

	cursor, ok := e.TryRelFetchTriples(ctx, spec)
	if !ok {
		t.Fatalf("TryRelFetchTriples: ok = false, want true")
	}
	assertTriples(t, drainTriples(t, cursor), []graph.RelationshipTripleResult{
		{ID: 101, StartID: 1, EndID: 5},
		{ID: 102, StartID: 2, EndID: 5},
	})
}

// TestTryRelQueriesBothAnchorsAnchorOnSmallerSide covers the both-anchored
// branch from both size directions -- once with the smaller anchor set on
// Start, once with it on End -- checking that newRelScanIter's "anchor on
// the smaller side" optimization never changes the answer, only which side
// drives the outer loop.
func TestTryRelQueriesBothAnchorsAnchorOnSmallerSide(t *testing.T) {
	ctx := context.Background()

	// Start (1 id) is smaller than End (2 ids): anchors on Start.
	eStartSmaller := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	specStartSmaller := recognize.RelSpec{
		StartIDs:  []graph.ID{1},
		EndIDs:    []graph.ID{3, 4},
		EdgeKinds: edgeKinds("AdminTo"),
	}
	cursor, ok := eStartSmaller.TryRelFetchTriples(ctx, specStartSmaller)
	if !ok {
		t.Fatalf("TryRelFetchTriples (Start smaller): ok = false, want true")
	}
	assertTriples(t, drainTriples(t, cursor), []graph.RelationshipTripleResult{
		{ID: 103, StartID: 1, EndID: 3},
		{ID: 107, StartID: 1, EndID: 4},
	})

	// End (1 id) is smaller than Start (2 ids): anchors on End.
	eEndSmaller := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	specEndSmaller := recognize.RelSpec{
		StartIDs:  []graph.ID{1, 2},
		EndIDs:    []graph.ID{3},
		EdgeKinds: edgeKinds("AdminTo"),
	}
	cursor, ok = eEndSmaller.TryRelFetchTriples(ctx, specEndSmaller)
	if !ok {
		t.Fatalf("TryRelFetchTriples (End smaller): ok = false, want true")
	}
	assertTriples(t, drainTriples(t, cursor), []graph.RelationshipTripleResult{
		{ID: 103, StartID: 1, EndID: 3},
	})
}

// TestTryRelQueriesEmptyVsNilAnchors covers RelSpec's central nil-vs-
// non-nil-empty contract, independently for StartIDs and EndIDs: a non-nil
// empty slice must match nothing, while nil leaves that endpoint
// unconstrained. This regression-tests resolveRelSpec/newRelScanIter
// branching on `spec.StartIDs != nil`/`spec.EndIDs != nil`, never
// `len(...) > 0`.
func TestTryRelQueriesEmptyVsNilAnchors(t *testing.T) {
	ctx := context.Background()
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)

	// Non-nil, empty StartIDs: matches nothing, even though AdminTo has
	// matches overall.
	specEmptyStart := recognize.RelSpec{StartIDs: []graph.ID{}, EdgeKinds: edgeKinds("AdminTo")}
	if count, ok := e.TryRelCount(ctx, specEmptyStart); !ok || count != 0 {
		t.Fatalf("TryRelCount(empty StartIDs) = (%d, %v), want (0, true)", count, ok)
	}
	idCursor, ok := e.TryRelFetchIDs(ctx, specEmptyStart)
	if !ok {
		t.Fatalf("TryRelFetchIDs(empty StartIDs): ok = false, want true")
	}
	if ids := drainIDs(t, idCursor); len(ids) != 0 {
		t.Fatalf("TryRelFetchIDs(empty StartIDs) = %v, want empty", ids)
	}

	// nil StartIDs: unconstrained, so every AdminTo edge matches (3 total).
	specNilStart := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	if count, ok := e.TryRelCount(ctx, specNilStart); !ok || count != 3 {
		t.Fatalf("TryRelCount(nil StartIDs) = (%d, %v), want (3, true)", count, ok)
	}

	// Non-nil, empty EndIDs: matches nothing too.
	specEmptyEnd := recognize.RelSpec{EndIDs: []graph.ID{}, EdgeKinds: edgeKinds("AdminTo")}
	if count, ok := e.TryRelCount(ctx, specEmptyEnd); !ok || count != 0 {
		t.Fatalf("TryRelCount(empty EndIDs) = (%d, %v), want (0, true)", count, ok)
	}
}

// TestTryRelQueriesEdgeKindStaleDeclines covers the edge-kind-scoped
// freshness gate: a NoteWrite that touches the spec's constrained edge kind
// (AdminTo) must decline reasonKindStale, while a spec constrained to an
// untouched edge kind (MemberOf) must still serve.
func TestTryRelQueriesEdgeKindStaleDeclines(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	scope := NewWriteScope()
	scope.TouchEdgeKinds(edgeKinds("AdminTo"))
	e.NoteWrite(scope)

	staleSpec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	if _, ok := e.TryRelCount(ctx, staleSpec); ok {
		t.Fatalf("TryRelCount(AdminTo): ok = true, want false (kind_stale)")
	}

	freshSpec := recognize.RelSpec{EdgeKinds: edgeKinds("MemberOf")}
	if count, ok := e.TryRelCount(ctx, freshSpec); !ok || count != 2 {
		t.Fatalf("TryRelCount(MemberOf) = (%d, %v), want (2, true)", count, ok)
	}
}

// TestTryRelQueriesEndpointNodeKindStaleDeclines covers the node-kind-scoped
// freshness gate for a RelSpec: it must only be checked when the spec
// actually carries an endpoint kind constraint (StartConstraints or
// EndConstraints non-empty) -- a spec with none must serve even though a
// node kind was just touched, while a spec constraining an endpoint by that
// exact touched kind must decline.
func TestTryRelQueriesEndpointNodeKindStaleDeclines(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	scope := NewWriteScope()
	scope.TouchNodeKinds(graph.Kinds{graph.StringKind("Computer")})
	e.NoteWrite(scope)

	unconstrained := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	if _, ok := e.TryRelCount(ctx, unconstrained); !ok {
		t.Fatalf("TryRelCount(no endpoint constraints): ok = false, want true")
	}

	constrained := recognize.RelSpec{
		EdgeKinds:      edgeKinds("AdminTo"),
		EndConstraints: []recognize.KindConstraint{kindConstraint(false, "Computer")},
	}
	if _, ok := e.TryRelCount(ctx, constrained); ok {
		t.Fatalf("TryRelCount(Computer end constraint): ok = true, want false (kind_stale)")
	}
}

// TestTryRelQueriesAllEdgesDirtyDeclines covers allEdgesClean: an unscoped
// TouchAllEdges write must decline every relationship query regardless of
// EdgeKinds, since allEdgesClean(g) is checked unconditionally.
func TestTryRelQueriesAllEdgesDirtyDeclines(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	scope := NewWriteScope()
	scope.TouchAllEdges()
	e.NoteWrite(scope)

	spec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	if _, ok := e.TryRelCount(ctx, spec); ok {
		t.Fatalf("TryRelCount: ok = true, want false (all edges dirty)")
	}
}

// TestTryRelQueriesUnconstrainedKindDeclinesOnAnyEdgeKindDirty covers
// edgeKindsClean's empty-kinds case: a spec with no EdgeKinds filter at all
// depends on *every* recorded edge kind entry, so touching a kind the spec
// never names (HasSession) must still decline it.
func TestTryRelQueriesUnconstrainedKindDeclinesOnAnyEdgeKindDirty(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	scope := NewWriteScope()
	scope.TouchEdgeKinds(edgeKinds("HasSession"))
	e.NoteWrite(scope)

	unconstrained := recognize.RelSpec{}
	if _, ok := e.TryRelCount(ctx, unconstrained); ok {
		t.Fatalf("TryRelCount(unconstrained kinds): ok = true, want false (kind_stale, any edge kind dirty)")
	}
}

// TestTryRelQueriesServeAgainAfterFreshSnapshot covers recovery from a
// kind-stale decline: once a fresh snapshot is adopted at the write's new
// generation, the previously-stale spec must serve again.
func TestTryRelQueriesServeAgainAfterFreshSnapshot(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	scope := NewWriteScope()
	scope.TouchEdgeKinds(edgeKinds("AdminTo"))
	e.NoteWrite(scope)

	spec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	if _, ok := e.TryRelCount(ctx, spec); ok {
		t.Fatalf("TryRelCount before rebuild: ok = true, want false (kind_stale)")
	}

	freshSnap := buildRelSpecSnapshot(t)
	freshSnap.Generation = e.Generation()
	e.snap.Store(freshSnap)

	if count, ok := e.TryRelCount(ctx, spec); !ok || count != 3 {
		t.Fatalf("TryRelCount after rebuild = (%d, %v), want (3, true)", count, ok)
	}
}

// TestTryRelQueriesDisabledDeclines covers cfg.Enabled = false: every Task 7
// entry point declines immediately.
func TestTryRelQueriesDisabledDeclines(t *testing.T) {
	e := New(nil, nil, Config{Enabled: false})
	ctx := context.Background()
	spec := recognize.RelSpec{}

	if _, ok := e.TryRelCount(ctx, spec); ok {
		t.Fatalf("TryRelCount: ok = true, want false (disabled)")
	}
	if _, ok := e.TryRelFetchIDs(ctx, spec); ok {
		t.Fatalf("TryRelFetchIDs: ok = true, want false (disabled)")
	}
	if _, ok := e.TryRelFetchTriples(ctx, spec); ok {
		t.Fatalf("TryRelFetchTriples: ok = true, want false (disabled)")
	}
	if _, ok := e.TryRelFetchKinds(ctx, spec); ok {
		t.Fatalf("TryRelFetchKinds: ok = true, want false (disabled)")
	}
	if _, ok := e.TryRelQueryRows(ctx, spec, recognize.ProjectionStartEnd, false); ok {
		t.Fatalf("TryRelQueryRows: ok = true, want false (disabled)")
	}
}

// TestTryRelQueriesMapKindErrorDeclines covers an unmappable kind name --
// once for spec.EdgeKinds, once for an endpoint KindConstraint -- both must
// decline reasonError rather than being treated as "matches nothing".
func TestTryRelQueriesMapKindErrorDeclines(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	edgeSpec := recognize.RelSpec{EdgeKinds: edgeKinds("Nonexistent")}
	if _, ok := e.TryRelCount(ctx, edgeSpec); ok {
		t.Fatalf("TryRelCount: ok = true, want false (unmappable edge kind)")
	}

	endSpec := recognize.RelSpec{EndConstraints: []recognize.KindConstraint{kindConstraint(false, "Nonexistent")}}
	if _, ok := e.TryRelCount(ctx, endSpec); ok {
		t.Fatalf("TryRelCount: ok = true, want false (unmappable end constraint kind)")
	}
}

// TestTryRelFetchKindsResolvesKindNames covers TryRelFetchKinds' happy path:
// every returned row carries the edge's actual database triple plus its
// resolved graph.Kind name.
func TestTryRelFetchKindsResolvesKindNames(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()
	spec := recognize.RelSpec{StartIDs: []graph.ID{1}, EdgeKinds: edgeKinds("AdminTo")}

	cursor, ok := e.TryRelFetchKinds(ctx, spec)
	if !ok {
		t.Fatalf("TryRelFetchKinds: ok = false, want true")
	}
	rows := drainRelKinds(t, cursor)
	if len(rows) != 2 {
		t.Fatalf("TryRelFetchKinds: got %d rows, want 2: %+v", len(rows), rows)
	}
	for _, row := range rows {
		if row.Kind == nil || row.Kind.String() != "AdminTo" {
			t.Fatalf("row %+v: Kind = %v, want AdminTo", row, row.Kind)
		}
		if row.StartID != 1 {
			t.Fatalf("row %+v: StartID = %d, want 1", row, row.StartID)
		}
	}
}

// TestTryRelFetchKindsResolveErrorDeclines covers TryRelFetchKinds' eager
// kind-name resolution failure path: e.mapKindNames failing declines
// reasonError before any cursor is handed back.
func TestTryRelFetchKindsResolveErrorDeclines(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	e.mapKindNames = func(ids []snapshot.KindID) (graph.Kinds, error) {
		return nil, errors.New("boom: kind id resolution failed")
	}

	spec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	if _, ok := e.TryRelFetchKinds(context.Background(), spec); ok {
		t.Fatalf("TryRelFetchKinds: ok = true, want false (mapKindNames failed)")
	}
}

// TestRowResultDrivesContainerFetchDirectedGraphLoopShape drives a
// ProjectionStartEnd rowResult with the exact consumer loop shape dawgs'
// container.FetchDirectedGraph uses (container/fetch.go): a fixed pair of
// graph.ID targets scanned every row via Scan, inside a plain `for
// result.Next() { ... }` loop -- the task brief's mandated fidelity test.
func TestRowResultDrivesContainerFetchDirectedGraphLoopShape(t *testing.T) {
	eng := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()
	spec := recognize.RelSpec{EdgeKinds: edgeKinds("MemberOf")}

	result, ok := eng.TryRelQueryRows(ctx, spec, recognize.ProjectionStartEnd, false)
	if !ok {
		t.Fatalf("TryRelQueryRows: ok = false, want true")
	}
	defer result.Close()

	// Verbatim: dawgs container/fetch.go's FetchDirectedGraph consumer loop.
	var pairs [][2]graph.ID
	var s, e graph.ID
	for result.Next() {
		if err := result.Scan(&s, &e); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		pairs = append(pairs, [2]graph.ID{s, e})
	}
	if err := result.Error(); err != nil {
		t.Fatalf("Error() = %v, want nil", err)
	}

	want := [][2]graph.ID{{1, 5}, {2, 5}}
	if len(pairs) != len(want) {
		t.Fatalf("pairs = %v, want %v", pairs, want)
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	for i := range want {
		if pairs[i] != want[i] {
			t.Fatalf("pairs = %v, want %v", pairs, want)
		}
	}
}

// TestRowResultDrivesShallowFetchRelationshipsLoopShape drives a
// ProjectionStepOutbound rowResult with the exact consumer loop shape
// dawgs' traversal.shallowFetchRelationships uses for an outbound step
// (traversal/traversal.go): four fixed targets (graph.ID, graph.Kinds,
// graph.ID, graph.Kind) scanned every row -- the task brief's other
// mandated fidelity test.
func TestRowResultDrivesShallowFetchRelationshipsLoopShape(t *testing.T) {
	eng := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()
	spec := recognize.RelSpec{StartIDs: []graph.ID{1}, EdgeKinds: edgeKinds("AdminTo")}

	result, ok := eng.TryRelQueryRows(ctx, spec, recognize.ProjectionStepOutbound, false)
	if !ok {
		t.Fatalf("TryRelQueryRows: ok = false, want true")
	}
	defer result.Close()

	type gotRow struct {
		nid graph.ID
		nk  graph.Kinds
		eid graph.ID
		ek  graph.Kind
	}
	var rows []gotRow

	// Verbatim: dawgs traversal/traversal.go's shallowFetchRelationships
	// outbound consumer loop.
	var nodeID graph.ID
	var nodeKinds graph.Kinds
	var edgeID graph.ID
	var edgeKind graph.Kind
	for result.Next() {
		if err := result.Scan(&nodeID, &nodeKinds, &edgeID, &edgeKind); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		rows = append(rows, gotRow{nodeID, append(graph.Kinds(nil), nodeKinds...), edgeID, edgeKind})
	}
	if err := result.Error(); err != nil {
		t.Fatalf("Error() = %v, want nil", err)
	}

	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want 2 rows", rows)
	}
	byEdge := make(map[graph.ID]gotRow, len(rows))
	for _, row := range rows {
		byEdge[row.eid] = row
	}

	r103, ok := byEdge[103]
	if !ok {
		t.Fatalf("rows = %+v, missing edge 103", rows)
	}
	if r103.nid != 3 || r103.ek == nil || r103.ek.String() != "AdminTo" {
		t.Fatalf("row for edge 103 = %+v, want node 3, kind AdminTo", r103)
	}
	if len(r103.nk) != 1 || r103.nk[0].String() != "Computer" {
		t.Fatalf("row for edge 103 node kinds = %v, want [Computer]", r103.nk)
	}

	r107, ok := byEdge[107]
	if !ok {
		t.Fatalf("rows = %+v, missing edge 107", rows)
	}
	if r107.nid != 4 || r107.ek == nil || r107.ek.String() != "AdminTo" {
		t.Fatalf("row for edge 107 = %+v, want node 4, kind AdminTo", r107)
	}
}

// TestTryRelQueryRowsOrderByEdgeIDAscending covers orderByEdgeID=true
// against an anchored (start) scan: node 1's three outgoing edges (101, 103,
// 107, in that ascending order) must be emitted in exactly that order, not
// CSR/(target,kind) scan order.
func TestTryRelQueryRowsOrderByEdgeIDAscending(t *testing.T) {
	eng := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()
	spec := recognize.RelSpec{StartIDs: []graph.ID{1}}

	result, ok := eng.TryRelQueryRows(ctx, spec, recognize.ProjectionStepOutbound, true)
	if !ok {
		t.Fatalf("TryRelQueryRows: ok = false, want true")
	}
	defer result.Close()

	var nodeID graph.ID
	var nodeKinds graph.Kinds
	var edgeID graph.ID
	var edgeKind graph.Kind
	var eids []graph.ID
	for result.Next() {
		if err := result.Scan(&nodeID, &nodeKinds, &edgeID, &edgeKind); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		eids = append(eids, edgeID)
	}
	if err := result.Error(); err != nil {
		t.Fatalf("Error() = %v, want nil", err)
	}

	want := []graph.ID{101, 103, 107}
	if len(eids) != len(want) {
		t.Fatalf("eids = %v, want %v", eids, want)
	}
	for i := range want {
		if eids[i] != want[i] {
			t.Fatalf("eids = %v, want %v (ascending by edge id)", eids, want)
		}
	}
}

// TestTryRelQueryRowsUnsupportedOrderDeclines covers orderByEdgeID=true
// against a fully unconstrained spec (neither StartIDs nor EndIDs
// non-nil): there is no anchored scan to gather and sort, so this must
// decline reasonUnsupportedOrder.
func TestTryRelQueryRowsUnsupportedOrderDeclines(t *testing.T) {
	eng := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()
	spec := recognize.RelSpec{}

	if _, ok := eng.TryRelQueryRows(ctx, spec, recognize.ProjectionStartEnd, true); ok {
		t.Fatalf("TryRelQueryRows: ok = true, want false (unsupported_order, no anchor)")
	}
}

// TestTryRelQueryRowsProjectionMismatchDeclines covers the projection/
// anchor direction constraint: ProjectionStepOutbound requires StartIDs
// non-nil, ProjectionStepInbound requires EndIDs non-nil.
func TestTryRelQueryRowsProjectionMismatchDeclines(t *testing.T) {
	eng := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()

	outboundSpec := recognize.RelSpec{EndIDs: []graph.ID{1}}
	if _, ok := eng.TryRelQueryRows(ctx, outboundSpec, recognize.ProjectionStepOutbound, false); ok {
		t.Fatalf("TryRelQueryRows(StepOutbound, no StartIDs): ok = true, want false")
	}

	inboundSpec := recognize.RelSpec{StartIDs: []graph.ID{1}}
	if _, ok := eng.TryRelQueryRows(ctx, inboundSpec, recognize.ProjectionStepInbound, false); ok {
		t.Fatalf("TryRelQueryRows(StepInbound, no EndIDs): ok = true, want false")
	}
}

// TestTryRelQueriesUnconstrainedRelSpecServesFully covers the regression:
// a fully-unconstrained recognize.RelSpec{} (no EdgeKinds, no StartIDs, no
// EndIDs, no endpoint constraints) must serve the complete edge set against
// a clean snapshot. This proves the engine handles the most permissive query
// shape and returns all edges with their full (start, edgeID, end) triples.
func TestTryRelQueriesUnconstrainedRelSpecServesFully(t *testing.T) {
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t), 0)
	ctx := context.Background()
	spec := recognize.RelSpec{}

	// TryRelCount must return the total edge count (7 edges in the fixture).
	count, ok := e.TryRelCount(ctx, spec)
	if !ok {
		t.Fatalf("TryRelCount: ok = false, want true (unconstrained RelSpec should serve)")
	}
	if count != 7 {
		t.Fatalf("TryRelCount: count = %d, want 7 (all edges in fixture)", count)
	}

	// TryRelFetchTriples must yield exactly every edge's (start, edgeID, end).
	triplesCursor, ok := e.TryRelFetchTriples(ctx, spec)
	if !ok {
		t.Fatalf("TryRelFetchTriples: ok = false, want true (unconstrained RelSpec should serve)")
	}
	triples := drainTriples(t, triplesCursor)
	wantTriples := []graph.RelationshipTripleResult{
		{ID: 101, StartID: 1, EndID: 5},
		{ID: 102, StartID: 2, EndID: 5},
		{ID: 103, StartID: 1, EndID: 3},
		{ID: 104, StartID: 2, EndID: 4},
		{ID: 105, StartID: 3, EndID: 1},
		{ID: 106, StartID: 4, EndID: 2},
		{ID: 107, StartID: 1, EndID: 4},
	}
	assertTriples(t, triples, wantTriples)

	// TryRelQueryRows with ProjectionStartEnd and orderByEdgeID=false
	// must yield every (start, end) pair.
	rowResult, ok := e.TryRelQueryRows(ctx, spec, recognize.ProjectionStartEnd, false)
	if !ok {
		t.Fatalf("TryRelQueryRows: ok = false, want true (unconstrained RelSpec should serve)")
	}
	defer rowResult.Close()

	var pairs [][2]graph.ID
	var s, end graph.ID
	for rowResult.Next() {
		if err := rowResult.Scan(&s, &end); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		pairs = append(pairs, [2]graph.ID{s, end})
	}
	if err := rowResult.Error(); err != nil {
		t.Fatalf("Error() = %v, want nil", err)
	}

	wantPairs := [][2]graph.ID{
		{1, 5}, {2, 5}, {1, 3}, {2, 4}, {3, 1}, {4, 2}, {1, 4},
	}
	if len(pairs) != len(wantPairs) {
		t.Fatalf("pairs = %v, want %v (all 7 edges)", pairs, wantPairs)
	}
	// Sort both for comparison (order may vary, but set must match).
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	sort.Slice(wantPairs, func(i, j int) bool {
		if wantPairs[i][0] != wantPairs[j][0] {
			return wantPairs[i][0] < wantPairs[j][0]
		}
		return wantPairs[i][1] < wantPairs[j][1]
	})
	for i := range wantPairs {
		if pairs[i] != wantPairs[i] {
			t.Fatalf("pairs = %v, want %v (all 7 edges)", pairs, wantPairs)
		}
	}
}

// TestTryRelFetchIDsCloseDoesNotLeakFeeder is this file's Task 7
// cursor-leak regression test, modeled directly on
// TestTryNodeFetchIDsCloseDoesNotLeakFeeder above: a cursor that is drained
// partially and then Close()d must not leave its feeder goroutine blocked
// forever. The fixture is a long chain (well over rowCursorBuffer edges) so
// the feeder cannot finish submitting every value before the test goroutine
// stops draining.
func TestTryRelFetchIDsCloseDoesNotLeakFeeder(t *testing.T) {
	const edgeCount = rowCursorBuffer * 4

	b := snapshot.NewBuilder(1)
	for i := uint64(1); i <= edgeCount+1; i++ {
		if err := b.AddNode(i, []snapshot.KindID{kindUser}); err != nil {
			t.Fatalf("AddNode(%d): %v", i, err)
		}
	}
	for i := uint64(1); i <= edgeCount; i++ {
		b.AddEdge(i, i, i+1, kindMemberOf)
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	e := newRelSpecEngine(t, snap, 0)
	spec := recognize.RelSpec{EdgeKinds: edgeKinds("MemberOf")}

	runtime.GC()
	before := runtime.NumGoroutine()

	const attempts = 20
	for i := 0; i < attempts; i++ {
		cursor, ok := e.TryRelFetchIDs(context.Background(), spec)
		if !ok {
			t.Fatalf("TryRelFetchIDs (attempt %d): ok = false, want true", i)
		}

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
