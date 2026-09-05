// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildTwoNodeSnapshot builds a two-node snapshot for marks tests: node 1
// (kind 100) has one outgoing edge, database id edgeID and kind edgeKind, to
// node 2 (kind 200). It is deliberately tiny -- these tests exercise
// noteResolved's kind-resolution logic, not the snapshot builder itself
// (see snapshot/builder_test.go for that).
func buildTwoNodeSnapshot(t *testing.T, edgeID uint64, edgeKind snapshot.KindID) *snapshot.Snapshot {
	t.Helper()

	b := snapshot.NewBuilder(1)
	if err := b.AddNode(1, []snapshot.KindID{100}); err != nil {
		t.Fatalf("AddNode(1): %v", err)
	}
	if err := b.AddNode(2, []snapshot.KindID{200}); err != nil {
		t.Fatalf("AddNode(2): %v", err)
	}
	b.AddEdge(edgeID, 1, 2, edgeKind)

	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snap
}

// fakeResolver returns a noteResolved resolve function backed by a plain
// map, standing in for the real e.kindNamesByID (which needs a live
// KindMapper) the way the task brief's injectable seam intends. Looking up
// an id absent from byID is a test-setup bug, not a case under test, so it
// fails loudly via t.Fatalf rather than returning an error a caller might
// confuse with the "resolver failed" path under test elsewhere.
func fakeResolver(t *testing.T, byID map[snapshot.KindID]graph.Kind) func([]snapshot.KindID) (graph.Kinds, error) {
	t.Helper()
	return func(ids []snapshot.KindID) (graph.Kinds, error) {
		kinds := make(graph.Kinds, len(ids))
		for i, id := range ids {
			kind, ok := byID[id]
			if !ok {
				t.Fatalf("fakeResolver: no fake mapping for kind id %d", id)
			}
			kinds[i] = kind
		}
		return kinds, nil
	}
}

// failResolver returns a resolve function that fails the test if ever
// called, for scopes under test that should never need id->name resolution
// (no Delete*ID entries).
func failResolver(t *testing.T) func([]snapshot.KindID) (graph.Kinds, error) {
	t.Helper()
	return func(ids []snapshot.KindID) (graph.Kinds, error) {
		t.Fatalf("resolve called unexpectedly with %v", ids)
		return nil, nil
	}
}

func TestWriteScopeEmpty(t *testing.T) {
	s := NewWriteScope()
	if !s.Empty() {
		t.Fatalf("NewWriteScope().Empty() = false, want true")
	}

	s.TouchNodeKinds(graph.Kinds{graph.StringKind("User")})
	if s.Empty() {
		t.Fatalf("Empty() = true after TouchNodeKinds, want false")
	}
}

// TestNoteWriteScopedTouchDirtiesOnlyThatKind covers the core kind-scoped
// contract: a write naming exactly one node kind must dirty that kind (and
// only that kind) at the generation it was stamped with, and leave it (and
// everything else) clean at the generation before the write.
func TestNoteWriteScopedTouchDirtiesOnlyThatKind(t *testing.T) {
	e := New(nil, nil, Config{})
	preGen := e.Generation()

	scope := NewWriteScope()
	scope.TouchNodeKinds(graph.Kinds{graph.StringKind("User")})
	e.noteResolved(scope, failResolver(t))

	postGen := e.Generation()
	if postGen != preGen+1 {
		t.Fatalf("Generation() after NoteWrite = %d, want %d", postGen, preGen+1)
	}

	if e.nodeKindsClean(preGen, graph.Kinds{graph.StringKind("User")}) {
		t.Fatalf("nodeKindsClean(preGen, User) = true, want false: User was just touched")
	}
	if !e.nodeKindsClean(postGen, graph.Kinds{graph.StringKind("User")}) {
		t.Fatalf("nodeKindsClean(postGen, User) = false, want true")
	}
	if !e.nodeKindsClean(preGen, graph.Kinds{graph.StringKind("Computer")}) {
		t.Fatalf("nodeKindsClean(preGen, Computer) = false, want true: Computer was never touched")
	}
	if !e.edgeKindsClean(preGen, nil) {
		t.Fatalf("edgeKindsClean(preGen, nil) = false, want true: no edge kind was touched")
	}
	// preGen, not postGen: allNodesGen/allEdgesGen being stamped with the
	// write's own generation would still read as "clean" at postGen (postGen
	// IS that generation), so only checking against the fixed pre-write
	// generation can actually catch an incorrect all* fallback.
	if !e.allNodesClean(preGen) || !e.allEdgesClean(preGen) {
		t.Fatalf("a specific-kind touch must not fall back to allNodes/allEdges")
	}
}

// TestNoteWriteTouchAllDirtiesBothAllGens covers WriteScope.TouchAll: both
// allNodesGen and allEdgesGen must advance, regardless of which specific
// kinds (none, here) the scope also names.
func TestNoteWriteTouchAllDirtiesBothAllGens(t *testing.T) {
	e := New(nil, nil, Config{})
	preGen := e.Generation()

	scope := NewWriteScope()
	scope.TouchAll()
	e.noteResolved(scope, failResolver(t))

	postGen := e.Generation()

	if e.allNodesClean(preGen) {
		t.Fatalf("allNodesClean(preGen) = true, want false")
	}
	if e.allEdgesClean(preGen) {
		t.Fatalf("allEdgesClean(preGen) = true, want false")
	}
	if !e.allNodesClean(postGen) {
		t.Fatalf("allNodesClean(postGen) = false, want true")
	}
	if !e.allEdgesClean(postGen) {
		t.Fatalf("allEdgesClean(postGen) = false, want true")
	}
}

// TestNoteWriteDeleteEdgeIDResolvedDirtiesExactKind covers DeleteEdgeID's
// happy path: an edge id present in the current snapshot resolves to
// exactly its own kind, dirtying that kind and nothing else -- not another
// edge kind, not any node kind, and not the allEdges catch-all.
func TestNoteWriteDeleteEdgeIDResolvedDirtiesExactKind(t *testing.T) {
	e := New(nil, nil, Config{})
	e.snap.Store(buildTwoNodeSnapshot(t, 42, 10))
	preGen := e.Generation()

	scope := NewWriteScope()
	scope.DeleteEdgeID(graph.ID(42))

	resolve := fakeResolver(t, map[snapshot.KindID]graph.Kind{10: graph.StringKind("MemberOf")})
	e.noteResolved(scope, resolve)
	postGen := e.Generation()

	if e.edgeKindsClean(preGen, graph.Kinds{graph.StringKind("MemberOf")}) {
		t.Fatalf("edgeKindsClean(preGen, MemberOf) = true, want false")
	}
	if !e.edgeKindsClean(postGen, graph.Kinds{graph.StringKind("MemberOf")}) {
		t.Fatalf("edgeKindsClean(postGen, MemberOf) = false, want true")
	}
	if !e.edgeKindsClean(preGen, graph.Kinds{graph.StringKind("HasSession")}) {
		t.Fatalf("edgeKindsClean(preGen, HasSession) = false, want true: untouched edge kind")
	}
	if !e.allEdgesClean(preGen) {
		t.Fatalf("allEdgesClean(preGen) = false, want true: a resolved delete must not fall back to allEdges")
	}
	if !e.nodeKindsClean(preGen, nil) {
		t.Fatalf("nodeKindsClean(preGen, nil) = false, want true: an edge deletion must not touch node marks")
	}
}

// TestNoteWriteDeleteEdgeIDUnresolvedDirtiesAllEdges covers both documented
// reasons a DeleteEdgeID can't be resolved to a specific kind (a nil
// snapshot, and an id the current snapshot doesn't recognize): either must
// fall back to marking allEdges dirty, and must not touch node marks.
func TestNoteWriteDeleteEdgeIDUnresolvedDirtiesAllEdges(t *testing.T) {
	cases := []struct {
		name    string
		hasSnap bool
	}{
		{"nil snapshot", false},
		{"id absent from snapshot", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New(nil, nil, Config{})
			if tc.hasSnap {
				e.snap.Store(buildTwoNodeSnapshot(t, 42, 10))
			}
			preGen := e.Generation()

			scope := NewWriteScope()
			scope.DeleteEdgeID(graph.ID(999)) // never staged above

			e.noteResolved(scope, failResolver(t))
			postGen := e.Generation()

			if e.allEdgesClean(preGen) {
				t.Fatalf("allEdgesClean(preGen) = true, want false")
			}
			if !e.allEdgesClean(postGen) {
				t.Fatalf("allEdgesClean(postGen) = false, want true")
			}
			if !e.allNodesClean(preGen) {
				t.Fatalf("allNodesClean(preGen) = false, want true: an edge-only fallback must not touch node marks")
			}
		})
	}
}

// TestNoteWriteDeleteNodeIDDirtiesNodeAndIncidentEdgeKinds covers
// DeleteNodeID's happy path: a node id present in the current snapshot
// resolves to its own kind(s) plus every incident edge's kind, dirtying
// both -- but not an unrelated node kind, and not the allNodes/allEdges
// catch-alls.
func TestNoteWriteDeleteNodeIDDirtiesNodeAndIncidentEdgeKinds(t *testing.T) {
	e := New(nil, nil, Config{})
	// node 1 (kind 100) --edge 42 (kind 10)--> node 2 (kind 200)
	e.snap.Store(buildTwoNodeSnapshot(t, 42, 10))
	preGen := e.Generation()

	scope := NewWriteScope()
	scope.DeleteNodeID(graph.ID(1))

	resolve := fakeResolver(t, map[snapshot.KindID]graph.Kind{
		100: graph.StringKind("User"),
		10:  graph.StringKind("MemberOf"),
	})
	e.noteResolved(scope, resolve)
	postGen := e.Generation()

	if e.nodeKindsClean(preGen, graph.Kinds{graph.StringKind("User")}) {
		t.Fatalf("nodeKindsClean(preGen, User) = true, want false: User is the deleted node's own kind")
	}
	if !e.nodeKindsClean(postGen, graph.Kinds{graph.StringKind("User")}) {
		t.Fatalf("nodeKindsClean(postGen, User) = false, want true")
	}
	if e.edgeKindsClean(preGen, graph.Kinds{graph.StringKind("MemberOf")}) {
		t.Fatalf("edgeKindsClean(preGen, MemberOf) = true, want false: MemberOf is an incident edge kind")
	}
	if !e.edgeKindsClean(postGen, graph.Kinds{graph.StringKind("MemberOf")}) {
		t.Fatalf("edgeKindsClean(postGen, MemberOf) = false, want true")
	}
	if !e.nodeKindsClean(preGen, graph.Kinds{graph.StringKind("Computer")}) {
		t.Fatalf("nodeKindsClean(preGen, Computer) = false, want true: untouched node kind")
	}
	if !e.allNodesClean(preGen) || !e.allEdgesClean(preGen) {
		t.Fatalf("a resolved node deletion must not fall back to allNodes/allEdges")
	}
}

// TestNoteWriteDeleteNodeIDUnresolvedDirtiesAllNodesAndAllEdges covers
// DeleteNodeID's fallback: unlike a lone edge deletion, a node deletion
// that can't be resolved has no narrower safe answer than marking both
// allNodes and allEdges dirty (its incident edges are unknown too).
func TestNoteWriteDeleteNodeIDUnresolvedDirtiesAllNodesAndAllEdges(t *testing.T) {
	e := New(nil, nil, Config{})
	e.snap.Store(buildTwoNodeSnapshot(t, 42, 10))
	preGen := e.Generation()

	scope := NewWriteScope()
	scope.DeleteNodeID(graph.ID(999)) // never staged above

	e.noteResolved(scope, failResolver(t))
	postGen := e.Generation()

	if e.allNodesClean(preGen) {
		t.Fatalf("allNodesClean(preGen) = true, want false")
	}
	if e.allEdgesClean(preGen) {
		t.Fatalf("allEdgesClean(preGen) = true, want false")
	}
	if !e.allNodesClean(postGen) {
		t.Fatalf("allNodesClean(postGen) = false, want true")
	}
	if !e.allEdgesClean(postGen) {
		t.Fatalf("allEdgesClean(postGen) = false, want true")
	}
}

// TestNoteWriteDeleteEdgeIDResolveErrorDirtiesEverything covers the "on
// error, fall back to TouchAll" contract for a genuine resolver failure (as
// opposed to a plain id-not-found miss, covered above): since a KindMapper
// failure gives no reliable information about *anything* this write
// touched, both allNodes and allEdges must be marked dirty, not just
// allEdges.
func TestNoteWriteDeleteEdgeIDResolveErrorDirtiesEverything(t *testing.T) {
	e := New(nil, nil, Config{})
	e.snap.Store(buildTwoNodeSnapshot(t, 42, 10))
	preGen := e.Generation()

	if !e.allNodesClean(preGen) {
		t.Fatalf("allNodesClean(preGen) = false before the write, want true")
	}

	scope := NewWriteScope()
	scope.DeleteEdgeID(graph.ID(42)) // present, but resolve itself fails below

	failingResolve := func(_ []snapshot.KindID) (graph.Kinds, error) {
		return nil, errors.New("marks_test: kind mapper unreachable")
	}
	e.noteResolved(scope, failingResolve)

	// preGen, not the write's own postGen: a mark stamped with postGen would
	// still read as "clean" when checked at postGen itself, so only preGen
	// can distinguish "marked dirty by this write" from "never marked".
	if e.allNodesClean(preGen) {
		t.Fatalf("allNodesClean(preGen) = true, want false: a KindMapper failure must fall back to TouchAll, not just edges")
	}
	if e.allEdgesClean(preGen) {
		t.Fatalf("allEdgesClean(preGen) = true, want false")
	}
}

// TestNoteWriteNilDirtiesEverything covers NoteWrite(nil) end-to-end
// (through the exported NoteWrite, not noteResolved) -- the "unknown write,
// touch everything" compatibility path every driver.go call site uses
// today. pgDriver is nil on e, proving this path never needs a KindMapper.
func TestNoteWriteNilDirtiesEverything(t *testing.T) {
	e := New(nil, nil, Config{})
	preGen := e.Generation()

	e.NoteWrite(nil)

	postGen := e.Generation()
	if postGen != preGen+1 {
		t.Fatalf("Generation() after NoteWrite(nil) = %d, want %d", postGen, preGen+1)
	}
	if e.allNodesClean(preGen) || e.allEdgesClean(preGen) {
		t.Fatalf("allNodesClean/allEdgesClean(preGen) = true after NoteWrite(nil), want false")
	}
	if !e.allNodesClean(postGen) || !e.allEdgesClean(postGen) {
		t.Fatalf("allNodesClean/allEdgesClean(postGen) = false after NoteWrite(nil), want true")
	}
}

// TestNoteWriteEmptyScopeOnlyBumpsGeneration covers the other end of the
// spectrum from a nil scope: an explicitly Empty() scope still means "a
// write happened" (the generation counter advances) but must not mark any
// kind, or either all* catch-all, dirty.
func TestNoteWriteEmptyScopeOnlyBumpsGeneration(t *testing.T) {
	e := New(nil, nil, Config{})
	preGen := e.Generation()

	e.noteResolved(NewWriteScope(), failResolver(t))

	postGen := e.Generation()
	if postGen != preGen+1 {
		t.Fatalf("Generation() after an empty-scope write = %d, want %d", postGen, preGen+1)
	}
	// preGen, not postGen: see the identical note in
	// TestNoteWriteDeleteEdgeIDResolveErrorDirtiesEverything.
	if !e.allNodesClean(preGen) || !e.allEdgesClean(preGen) {
		t.Fatalf("an empty scope must not mark allNodes/allEdges dirty")
	}
}

// TestEdgeKindsCleanEmptyKindsSeesAnyDirtyKind covers edgeKindsClean's
// documented empty-kinds behavior: asking about "every edge kind" (an empty
// or nil graph.Kinds) must go false the instant any single edge kind is
// dirty, not just when every kind happens to be named explicitly.
func TestEdgeKindsCleanEmptyKindsSeesAnyDirtyKind(t *testing.T) {
	e := New(nil, nil, Config{})
	preGen := e.Generation()

	scope := NewWriteScope()
	scope.TouchEdgeKind(graph.StringKind("MemberOf"))
	e.noteResolved(scope, failResolver(t))

	if e.edgeKindsClean(preGen, nil) {
		t.Fatalf("edgeKindsClean(preGen, nil) = true, want false: MemberOf was touched after preGen")
	}
	if !e.edgeKindsClean(e.Generation(), nil) {
		t.Fatalf("edgeKindsClean(postGen, nil) = false, want true")
	}
}

// TestAllNodeKindsClean covers allNodeKindsClean's equivalence to
// nodeKindsClean(g, nil): both the allNodesGen catch-all and any
// individually recorded node-kind entry must be able to make it report
// dirty.
func TestAllNodeKindsClean(t *testing.T) {
	e := New(nil, nil, Config{})
	preGen := e.Generation()

	scope := NewWriteScope()
	scope.TouchNodeKinds(graph.Kinds{graph.StringKind("User")})
	e.noteResolved(scope, failResolver(t))

	if e.allNodeKindsClean(preGen) {
		t.Fatalf("allNodeKindsClean(preGen) = true, want false: User was touched after preGen")
	}
	if !e.allNodeKindsClean(e.Generation()) {
		t.Fatalf("allNodeKindsClean(postGen) = false, want true")
	}
}
