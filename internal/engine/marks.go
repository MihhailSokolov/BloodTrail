// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// WriteScope describes, for a single write, which kinds of nodes and edges it
// touched -- the information NoteWrite needs to keep serving queries over
// kinds the write left alone, instead of invalidating the whole snapshot the
// way the plain generation counter does.
//
// A scope is built up by calling its Touch*/Delete* methods any number of
// times before handing it to NoteWrite; NoteWrite itself never mutates it.
// The zero value returned by NewWriteScope is empty (Empty() is true) and
// describes a write that touched nothing the engine tracks -- callers with
// nothing to report can pass it through unmodified rather than passing nil,
// which instead means "unknown scope, touch everything" (see NoteWrite's
// doc).
//
// Not safe for concurrent use: a WriteScope is meant to be built by the one
// goroutine handling a single write (transaction or batch) and handed to
// NoteWrite once that write commits, the same way a database transaction
// itself is single-goroutine. Nothing about the type needs to change that to
// support concurrent writes -- each concurrent write simply gets its own
// WriteScope and its own NoteWrite call.
type WriteScope struct {
	nodeKinds map[string]struct{}
	edgeKinds map[string]struct{}

	deleteNodeIDs []graph.ID
	deleteEdgeIDs []graph.ID

	allNodes bool
	allEdges bool
}

// NewWriteScope returns an empty WriteScope, ready for its Touch*/Delete*
// methods to be called.
func NewWriteScope() *WriteScope {
	return &WriteScope{}
}

// TouchNodeKinds records that the write touched (created, updated, or
// removed a label from) at least one node of each kind in kinds. A nil Kind
// in kinds is ignored rather than panicking on the String() call below --
// nothing in the write path is expected to produce one, but silently
// ignoring it is cheap insurance against a caller mistake more forgiving
// than a crash.
func (s *WriteScope) TouchNodeKinds(kinds graph.Kinds) {
	for _, kind := range kinds {
		if kind == nil {
			continue
		}
		if s.nodeKinds == nil {
			s.nodeKinds = make(map[string]struct{}, len(kinds))
		}
		s.nodeKinds[kind.String()] = struct{}{}
	}
}

// TouchEdgeKind records that the write touched (created or removed) at least
// one edge of kind. A nil kind is ignored; see TouchNodeKinds' doc.
func (s *WriteScope) TouchEdgeKind(kind graph.Kind) {
	if kind == nil {
		return
	}
	if s.edgeKinds == nil {
		s.edgeKinds = make(map[string]struct{}, 1)
	}
	s.edgeKinds[kind.String()] = struct{}{}
}

// TouchEdgeKinds is TouchEdgeKind over every kind in kinds.
func (s *WriteScope) TouchEdgeKinds(kinds graph.Kinds) {
	for _, kind := range kinds {
		s.TouchEdgeKind(kind)
	}
}

// DeleteNodeID records that the write deleted the node with database id id.
// NoteWrite resolves id against the current snapshot to find the specific
// node and edge kinds this deletion actually affects (the node's own kinds,
// plus every kind carried by an edge incident to it -- deleting a node
// deletes its edges too); see NoteWrite's doc for the conservative fallback
// when id can't be resolved.
func (s *WriteScope) DeleteNodeID(id graph.ID) {
	s.deleteNodeIDs = append(s.deleteNodeIDs, id)
}

// DeleteEdgeID records that the write deleted the edge with database id id.
// NoteWrite resolves id against the current snapshot to find the specific
// edge kind this deletion actually affects; see NoteWrite's doc for the
// conservative fallback when id can't be resolved.
func (s *WriteScope) DeleteEdgeID(id graph.ID) {
	s.deleteEdgeIDs = append(s.deleteEdgeIDs, id)
}

// TouchAllNodes records that the write may have touched a node of any kind,
// including a kind not otherwise named anywhere on this scope. Use this when
// the write path knows it changed nodes but cannot enumerate which kinds --
// the conservative choice over guessing.
func (s *WriteScope) TouchAllNodes() {
	s.allNodes = true
}

// TouchAllEdges is TouchAllNodes' edge equivalent.
func (s *WriteScope) TouchAllEdges() {
	s.allEdges = true
}

// TouchAll marks both nodes and edges of every kind as touched -- the same
// conservative effect NoteWrite gives a nil scope, available here for a
// caller that has a *WriteScope in hand already (e.g. one also recording
// specific Delete*IDs) but has separately learned it cannot bound the rest
// of the write's effect.
func (s *WriteScope) TouchAll() {
	s.TouchAllNodes()
	s.TouchAllEdges()
}

// Empty reports whether scope describes a write that touched nothing the
// engine tracks: no kinds named, no deletions recorded, and neither
// TouchAllNodes nor TouchAllEdges called. NoteWrite still bumps the
// generation counter for an Empty scope -- a write happened -- but leaves
// every kind mark untouched.
func (s *WriteScope) Empty() bool {
	return len(s.nodeKinds) == 0 && len(s.edgeKinds) == 0 &&
		len(s.deleteNodeIDs) == 0 && len(s.deleteEdgeIDs) == 0 &&
		!s.allNodes && !s.allEdges
}

// marks is Engine's kind-scoped write history: for each kind name ever
// touched, the generation of the most recent write that touched it. This is
// the complement to Engine's single blunt generation counter
// (NoteWrite/Fresh/snapshotStillCurrent, unchanged by this file) -- where
// that counter can only answer "has anything changed since generation G",
// marks lets a caller (Task 6) ask the narrower "has kind K changed since
// generation G", so a query touching only kinds nothing recent has written
// to can keep being served from an older snapshot even while unrelated
// writes keep advancing the plain counter.
//
// A kind name absent from nodeKinds/edgeKinds has never been touched by any
// NoteWrite call and reads as clean against every generation, including 0 --
// there is no need to pre-populate an entry for every kind the schema might
// ever see. allNodesGen and allEdgesGen separately record the generation of
// the most recent write whose node/edge scope NoteWrite could not pin to
// specific kinds (a nil WriteScope, TouchAllNodes/TouchAllEdges/TouchAll, an
// unresolvable delete, or a kind-name-resolution failure -- see NoteWrite's
// doc). Every freshness query below folds the relevant allNodesGen/
// allEdgesGen into its answer alongside whatever specific kinds it was
// asked about: an unscoped write can dirty a kind despite no entry naming
// it, so skipping that check would silently reintroduce the exact staleness
// bug marks exists to catch.
//
// mu guards every field below it. It is a plain sync.Mutex rather than a
// sync.RWMutex: the freshness queries this unlocks for Task 6 are expected
// to run far less often than NoteWrite, and are cheap even when they do (a
// handful of map lookups and uint64 compares under the lock) -- a
// reader/writer lock's extra bookkeeping would not pay for itself here.
//
// Monotonic-max invariant: Every generation value stamped into nodeKinds,
// edgeKinds, allNodesGen, and allEdgesGen must be monotonically increasing or
// stay the same; values must never decrease. When two concurrent writes
// commit out of generation order (a slower write with a smaller generation
// landing after a faster write with a larger generation), the larger
// generation must survive. This prevents durable corruption: if a cleanliness
// check reads a mark entry at generation 5 when generation 6 already dirtied
// the kind, the check would wrongly report clean. The stampMarks and
// stampNamesLocked methods enforce this by taking max(existing, gen) rather
// than unconditionally overwriting.
//
// Ordering with Engine's lock-free generation counter: NoteWrite bumps the
// counter first (a single atomic Add, exactly as the no-arg NoteWrite always
// did) and only afterwards acquires mu to stamp marks with that new value.
// The counter's advance is therefore visible to any concurrent reader (e.g.
// Fresh(), via snapshotStillCurrent) immediately, before the corresponding
// marks entries are necessarily written; a caller that read the live
// generation counter directly and, in the same instant, asked marks about a
// kind this in-flight write is about to dirty could -- for the brief window
// between the Add and NoteWrite's later mu.Lock() -- see that kind reported
// clean. This matches how the plain counter has always worked (Fresh() and
// snapshotStillCurrent already tolerate "the write is in flight" as a
// momentary ambiguity, resolved by TryAllShortestPaths' own step-7 recheck
// happening after the query's work is done, not at the instant a write
// lands) and does not, by itself, let a *stale result* through: every actual
// consumer of these freshness queries is expected to check against a
// snapshot generation G fixed *before* the write in question could possibly
// have started (G < the write's new generation, by construction, since the
// counter only increases), do its work, and then re-check -- mirroring
// snapshotStillCurrent's own recheck pattern -- immediately before serving,
// by which point NoteWrite's synchronous call (bump, resolve, stamp, in
// that order, all before NoteWrite returns to its WriteTransaction/
// BatchOperation caller) has either not started at all or has already fully
// completed. The only residual race is a recheck landing in the same
// instant a write's marks stamp is being written, which is the same class
// of "was it committed before or after this instant" ambiguity every
// generation-counter check in this package already accepts.
type marks struct {
	mu sync.Mutex

	nodeKinds map[string]uint64
	edgeKinds map[string]uint64

	allNodesGen uint64
	allEdgesGen uint64
}

// NoteWrite records that the underlying graph changed, invalidating the
// current snapshot for TryAllShortestPaths purposes until the next
// RebuildNow picks up a snapshot stamped with the new generation -- and
// records which kinds scope describes as touched, for the kind-scoped
// freshness queries below (nodeKindsClean and friends, consumed starting
// with Task 6).
//
// scope nil means "unknown write, touch everything": every driver.go call
// site passes nil today (Task 3 gives them real scopes built from what the
// write path actually observed), and so may any other caller with no more
// specific information to give. A non-nil scope's Delete*ID entries are
// resolved against the engine's *current* snapshot (e.snap, not necessarily
// the one any in-flight query is using) to find the specific kinds a
// deletion affects; see noteResolved's doc for exactly how, and for the
// conservative fallbacks when resolution isn't possible.
//
// Safe for concurrent use; intended to be called from the driver's write
// path once a write has committed successfully.
func (e *Engine) NoteWrite(scope *WriteScope) {
	e.noteResolved(scope, e.kindNamesByID)
}

// kindNamesByID is NoteWrite's production id->name resolver: it asks the
// driver's KindMapper to translate KindIDs (read off a deleted edge or node
// in the current snapshot) back into the graph.Kind names marks are keyed
// by. context.Background() is used deliberately -- NoteWrite has no ctx of
// its own to plumb through (matching the no-arg NoteWrite this replaces),
// and a kind lookup that outlives whatever request triggered the write is
// exactly what's wanted here, the same way RebuildNow's own background
// poller context outlives any one caller.
//
// This is the one place in this file that reaches into e.pgDriver:
// noteResolved itself takes the resolver as a plain function value instead
// of calling this method directly, so unit tests can substitute a fake
// resolver without standing up a real KindMapper (see marks_test.go).
func (e *Engine) kindNamesByID(ids []snapshot.KindID) (graph.Kinds, error) {
	return e.pgDriver.KindMapper().MapKindIDs(context.Background(), ids)
}

// errUnresolvedDelete marks a DeleteEdgeID/DeleteNodeID that couldn't be
// looked up in the current snapshot -- a nil snapshot, or an id the snapshot
// doesn't recognize (already gone by the time this write landed, or never
// present in the first place). It is a plain sentinel: nothing about it
// varies per call, and every use immediately converts it into a specific,
// narrow conservative fallback rather than propagating it further.
var errUnresolvedDelete = errors.New("engine: delete id not present in current snapshot")

// noteResolved is NoteWrite's implementation, parameterized on resolve so
// unit tests can supply a fake id->name lookup instead of a real KindMapper
// (which needs a live PostgreSQL connection). NoteWrite itself just calls
// this with e.kindNamesByID.
//
// Sequence:
//  1. Bump the generation counter once, unconditionally -- gen is the new
//     value, and everything this call stamps into marks uses it. This
//     happens before any of the (potentially slow, network-bound) kind
//     resolution below, so Fresh()'s plain, whole-snapshot staleness check
//     reflects the write immediately, exactly as it did before this method
//     existed.
//  2. A nil scope means "unknown write, touch everything": mark allNodesGen
//     and allEdgesGen with gen and return, without resolving anything.
//  3. Otherwise, start from scope's own explicit TouchNodeKinds/
//     TouchEdgeKind(s)/TouchAllNodes/TouchAllEdges, then fold in
//     scope.deleteEdgeIDs and scope.deleteNodeIDs:
//     - Each deleteEdgeIDs entry resolves via the current snapshot's
//     EdgeByID to a forward-CSR slot, whose OutKinds entry is the edge's
//     KindID; resolve translates the batch of KindIDs back to names. A
//     nil snapshot or an id EdgeByID doesn't recognize marks allEdges
//     dirty for this write (errUnresolvedDelete) -- deliberately narrow,
//     since "this specific edge is already gone" says nothing about node
//     kinds. A resolve error (the KindMapper itself failed, not merely "id
//     not found") instead falls back to marking *everything* dirty: unlike
//     a missing id, a resolver failure gives no information to be
//     conservative *about*, so the only safe answer is TouchAll's effect.
//     - Each deleteNodeIDs entry resolves via Dense to a dense NodeID, whose
//     own kinds (KindOffsets/NodeKinds) and every incident edge's kind
//     (Out and In, both directions -- deleting a node deletes its edges
//     too) are collected and resolved the same way. Any failure here --
//     nil snapshot, an id Dense doesn't recognize, or a resolve error --
//     marks both allNodes and allEdges dirty: a node deletion's blast
//     radius always includes edges, so there is no narrower safe fallback
//     the way there is for a lone edge deletion.
//  4. Acquire marks.mu once and commit every resolved name and every all*
//     flag from step 3, all stamped with the same gen. This is the only
//     step that holds marks.mu, and it never performs I/O -- see the marks
//     type's doc for why resolution deliberately happens before, not under,
//     the lock.
func (e *Engine) noteResolved(scope *WriteScope, resolve func([]snapshot.KindID) (graph.Kinds, error)) {
	gen := e.generation.Add(1)

	if scope == nil {
		e.stampMarks(gen, true, true, nil, nil)
		return
	}
	if scope.Empty() {
		return
	}

	touchAllNodes := scope.allNodes
	touchAllEdges := scope.allEdges

	var nodeNames, edgeNames []string
	for name := range scope.nodeKinds {
		nodeNames = append(nodeNames, name)
	}
	for name := range scope.edgeKinds {
		edgeNames = append(edgeNames, name)
	}

	if len(scope.deleteEdgeIDs) > 0 && !touchAllEdges {
		names, err := resolveDeletedEdgeKinds(e.snap.Load(), scope.deleteEdgeIDs, resolve)
		switch {
		case err == nil:
			edgeNames = append(edgeNames, names...)
		case errors.Is(err, errUnresolvedDelete):
			touchAllEdges = true
		default:
			// The KindMapper itself failed: we have no reliable information
			// about anything this write touched, not just edges.
			touchAllNodes, touchAllEdges = true, true
		}
	}

	if len(scope.deleteNodeIDs) > 0 && (!touchAllNodes || !touchAllEdges) {
		nNames, eNames, ok := resolveDeletedNodeKinds(e.snap.Load(), scope.deleteNodeIDs, resolve)
		if ok {
			nodeNames = append(nodeNames, nNames...)
			edgeNames = append(edgeNames, eNames...)
		} else {
			touchAllNodes, touchAllEdges = true, true
		}
	}

	e.stampMarks(gen, touchAllNodes, touchAllEdges, nodeNames, edgeNames)
}

// stampMarks is noteResolved's single commit point: it acquires marks.mu
// once and records gen against allNodesGen/allEdgesGen (if allNodes/allEdges)
// and against every name in nodeNames/edgeNames, then releases it. Called at
// most once per NoteWrite call.
//
// Crucially, this method observes a monotonic invariant: when two concurrent
// writes commit out of generation order (a slower write with a smaller
// generation landing after a faster write with a larger generation), the
// larger generation must survive. This prevents durable corruption where a
// cleanliness check incorrectly reports clean despite a higher-generation
// write having already dirtied the kind. The stamp never decreases: if an
// entry already holds a value >= gen, it is left unchanged.
func (e *Engine) stampMarks(gen uint64, allNodes, allEdges bool, nodeNames, edgeNames []string) {
	e.marks.mu.Lock()
	defer e.marks.mu.Unlock()

	if allNodes && gen > e.marks.allNodesGen {
		e.marks.allNodesGen = gen
	}
	if allEdges && gen > e.marks.allEdgesGen {
		e.marks.allEdgesGen = gen
	}
	stampNamesLocked(&e.marks.nodeKinds, nodeNames, gen)
	stampNamesLocked(&e.marks.edgeKinds, edgeNames, gen)
}

// stampNamesLocked sets (*m)[name] to the maximum of gen and any existing
// value for each name in names, allocating *m on first use. This enforces the
// monotonic-max invariant documented in stampMarks: when two writes commit out
// of order, the larger generation must survive, preventing durable corruption
// from a stale cleanliness check. Must be called with marks.mu already held.
func stampNamesLocked(m *map[string]uint64, names []string, gen uint64) {
	if len(names) == 0 {
		return
	}
	if *m == nil {
		*m = make(map[string]uint64, len(names))
	}
	for _, name := range names {
		if gen > (*m)[name] {
			(*m)[name] = gen
		}
	}
}

// kindIDNames converts kinds (as returned by a resolve call, aligned
// index-for-index with the KindIDs passed in) to their string names, via
// graph.Kind.String(). A nil entry -- not expected from a real KindMapper,
// but cheap to guard against -- is skipped rather than panicking.
func kindIDNames(kinds graph.Kinds) []string {
	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		if kind != nil {
			names = append(names, kind.String())
		}
	}
	return names
}

// resolveDeletedEdgeKinds resolves the kind name of every edge id in ids via
// snap.EdgeByID (id -> forward-CSR slot -> OutKinds) followed by one batched
// resolve call. It returns errUnresolvedDelete if snap is nil or any id in
// ids is absent from it -- the narrow failure noteResolved's doc describes,
// which its caller turns into "mark allEdges dirty" and nothing more. Any
// other error is resolve's own (wrapped for context), which noteResolved's
// caller treats as "mark everything dirty" instead.
func resolveDeletedEdgeKinds(snap *snapshot.Snapshot, ids []graph.ID, resolve func([]snapshot.KindID) (graph.Kinds, error)) ([]string, error) {
	if snap == nil {
		return nil, errUnresolvedDelete
	}

	kindIDs := make([]snapshot.KindID, 0, len(ids))
	for _, id := range ids {
		fwdIdx, ok := snap.EdgeByID(uint64(id))
		if !ok {
			return nil, errUnresolvedDelete
		}
		kindIDs = append(kindIDs, snap.OutKinds[fwdIdx])
	}

	kinds, err := resolve(kindIDs)
	if err != nil {
		return nil, fmt.Errorf("engine: resolve deleted edge kinds: %w", err)
	}
	return kindIDNames(kinds), nil
}

// resolveDeletedNodeKinds resolves, for every node id in ids, the node's own
// kinds (via snap.Dense and KindOffsets/NodeKinds) and the kinds of every
// edge incident to it in either direction (via Out and In) -- deleting a
// node deletes its edges too, so both must be reported. ok is false --
// meaning the caller should fall back to marking both allNodes and allEdges
// dirty, per noteResolved's doc -- if snap is nil, any id in ids is absent
// from it, or either resolve call fails; there is no narrower fallback for a
// node deletion the way there is for a lone edge deletion, so every failure
// mode here is handled identically.
func resolveDeletedNodeKinds(snap *snapshot.Snapshot, ids []graph.ID, resolve func([]snapshot.KindID) (graph.Kinds, error)) (nodeNames, edgeNames []string, ok bool) {
	if snap == nil {
		return nil, nil, false
	}

	var nodeKindIDs, edgeKindIDs []snapshot.KindID
	for _, id := range ids {
		dense, found := snap.Dense(uint64(id))
		if !found {
			return nil, nil, false
		}

		lo, hi := snap.KindOffsets[dense], snap.KindOffsets[dense+1]
		nodeKindIDs = append(nodeKindIDs, snap.NodeKinds[lo:hi]...)

		_, outKinds := snap.Out(dense)
		edgeKindIDs = append(edgeKindIDs, outKinds...)
		_, inKinds := snap.In(dense)
		edgeKindIDs = append(edgeKindIDs, inKinds...)
	}

	if len(nodeKindIDs) > 0 {
		kinds, err := resolve(nodeKindIDs)
		if err != nil {
			return nil, nil, false
		}
		nodeNames = kindIDNames(kinds)
	}

	if len(edgeKindIDs) > 0 {
		kinds, err := resolve(edgeKindIDs)
		if err != nil {
			return nil, nil, false
		}
		edgeNames = kindIDNames(kinds)
	}

	return nodeNames, edgeNames, true
}

// cleanAgainst reports whether every kind name in kinds -- or, if kinds is
// empty, every kind name ever recorded in byGen -- has a mark generation
// ≤ g, and allGen (the generation of the most recent write whose node/edge
// scope NoteWrite could not pin to specific kinds) is also ≤ g. A missing
// entry in byGen reads as generation 0 (never touched), which is ≤ any g.
//
// The empty-kinds case reads every entry in byGen rather than trivially
// returning true: an empty kind list means the caller isn't restricting by
// kind at all (e.g. a query with no edge-kind filter, matching
// buildKindMask's own "empty EdgeKinds means every kind allowed" contract),
// so its answer must depend on *every* kind that exists, not none of them.
// Must be called with marks.mu already held.
func cleanAgainst(g uint64, byGen map[string]uint64, allGen uint64, kinds graph.Kinds) bool {
	if allGen > g {
		return false
	}

	if len(kinds) == 0 {
		for _, gen := range byGen {
			if gen > g {
				return false
			}
		}
		return true
	}

	for _, kind := range kinds {
		if kind == nil {
			continue
		}
		if gen, ok := byGen[kind.String()]; ok && gen > g {
			return false
		}
	}
	return true
}

// nodeKindsClean reports whether every kind in kinds is clean relative to
// snapshot generation g: no write touching that kind (specifically, via
// TouchNodeKinds/DeleteNodeID, or unscoped, via a nil NoteWrite scope,
// TouchAllNodes/TouchAll, or an unresolvable delete) landed after g. An
// empty kinds asks "is every node kind clean" -- see cleanAgainst's doc.
func (e *Engine) nodeKindsClean(g uint64, kinds graph.Kinds) bool {
	e.marks.mu.Lock()
	defer e.marks.mu.Unlock()
	return cleanAgainst(g, e.marks.nodeKinds, e.marks.allNodesGen, kinds)
}

// edgeKindsClean is nodeKindsClean's edge equivalent: every kind in kinds
// (or, if kinds is empty, every edge kind ever recorded) clean relative to
// generation g.
func (e *Engine) edgeKindsClean(g uint64, kinds graph.Kinds) bool {
	e.marks.mu.Lock()
	defer e.marks.mu.Unlock()
	return cleanAgainst(g, e.marks.edgeKinds, e.marks.allEdgesGen, kinds)
}

// allNodesClean reports whether the most recent unscoped-for-nodes write
// (a nil NoteWrite scope, TouchAllNodes/TouchAll, or an unresolvable
// DeleteNodeID) landed at or before generation g. It says nothing about any
// individual node kind's own mark -- pair it with nodeKindsClean (or use
// allNodeKindsClean) for the full picture.
func (e *Engine) allNodesClean(g uint64) bool {
	e.marks.mu.Lock()
	defer e.marks.mu.Unlock()
	return e.marks.allNodesGen <= g
}

// allEdgesClean is allNodesClean's edge equivalent.
func (e *Engine) allEdgesClean(g uint64) bool {
	e.marks.mu.Lock()
	defer e.marks.mu.Unlock()
	return e.marks.allEdgesGen <= g
}

// allNodeKindsClean reports whether *every* node kind is clean relative to
// generation g: every recorded nodeKinds entry, plus allNodesGen. It is
// exactly nodeKindsClean(g, nil) -- given its own name so a caller asking
// "is every node kind clean" (e.g. for a fully kind-unconstrained node
// endpoint) can say so directly rather than relying on an empty graph.Kinds
// value being read that way.
func (e *Engine) allNodeKindsClean(g uint64) bool {
	return e.nodeKindsClean(g, nil)
}
