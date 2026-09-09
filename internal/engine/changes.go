// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"sort"
	"strings"

	"github.com/specterops/dawgs/graph"
)

// ChangeSet is the change log a WriteScope accumulates (see WriteScope's own
// doc, changes_scope.go): the actual read-back keys (node/edge database ids,
// or the objectid values an upsert identified its target by) and, where a
// key can't be pinned down, the coarser operation (a kind-scoped delete
// criteria, or a bare "this write escaped tracking" fallback) Apply
// (apply.go) needs in order to replay the write's effect into the in-memory
// engine, rather than re-deriving it from the observer call sequence itself.
//
// Every Record* method is additive and idempotent: recording the same
// logical entry more than once (e.g. two calls that both name node id 7,
// or two RecordFallback calls with the same reason) has the same effect as
// recording it once -- see each method's own doc for its exact dedup key.
// Nothing on this type reads e.snap or any other engine state, and nothing
// here decides what Apply should DO with a recorded entry; every method is a
// pure, in-memory accumulation, and every accessor below returns a fresh
// copy the caller may freely mutate without affecting the ChangeSet.
//
// The zero value is ready to use: every map field is allocated lazily, on
// first use, by the Record* method that needs it. Not safe for concurrent
// use, for the same reason WriteScope itself isn't: a ChangeSet is built by
// the one goroutine handling a single write and read back once, after that
// write commits.
type ChangeSet struct {
	nodeIDs       map[uint64]struct{}
	nodeObjectIDs map[string]struct{}
	edgeIDs       map[uint64]struct{}

	edgeTriples    map[edgeTripleKey]EdgeTripleRef
	edgeTriplesOID map[edgeTripleOIDKey]EdgeTripleOIDRef

	nodeKindDeletes map[string]NodeKindDeleteCriteria
	edgeKindDeletes map[string]graph.Kinds

	fallbacks map[string]struct{}
}

// EdgeTripleRef is one (start, end, kind) triple RecordEdgeTriple has
// recorded: a relationship whose own database id is unknown to the write
// path that recorded it (a batch CreateRelationship/CreateRelationshipByIDs
// call, which reports success or failure but never the new edge's id), so
// the applier must instead re-resolve it by its endpoints and kind.
type EdgeTripleRef struct {
	Start, End uint64
	Kind       graph.Kind
}

// EdgeTripleOIDRef is EdgeTripleRef's objectid-keyed equivalent, recorded by
// RecordEdgeTripleByObjectID for an UpdateRelationshipBy upsert whose
// endpoints were themselves identified by objectid rather than by database
// id -- the applier resolves StartOID/EndOID to node ids during its own
// read-back, the same way the pg upsert this call mirrors resolves them
// against the database.
type EdgeTripleOIDRef struct {
	StartOID, EndOID string
	Kind             graph.Kind
}

// NodeKindDeleteCriteria is one (include, exclude) pair RecordDeleteNodesByKinds
// has recorded, mirroring graph.Database.DeleteNodesByKinds' own
// includeAny/excludeAny parameters: the applier replays this as the same
// kind-scoped criteria, rather than an enumerated id list, since a
// kind-scoped node delete's own blast radius (every node matching the
// criteria, whatever their ids happen to be) is exactly what these two
// fields already describe.
type NodeKindDeleteCriteria struct {
	Include, Exclude graph.Kinds
}

// kindName is graph.Kind.String(), guarded against a nil Kind: nothing in
// the write path is expected to produce one, but silently tolerating it is
// cheap insurance against a caller mistake more forgiving than a crash.
func kindName(kind graph.Kind) string {
	if kind == nil {
		return ""
	}
	return kind.String()
}

// kindsKey builds a canonical, order-independent string key for kinds --
// its sorted kind names joined by a separator no kind name can itself
// produce ambiguity around (","; a kind name containing a comma would need
// to collide on every other sorted name too to produce a false match,
// which is the same class of edge case graph.Kinds.HashInto already accepts
// for its own delimiter-joined hash). This is the shared dedup key behind
// RecordDeleteNodesByKinds and RecordDeleteRelationshipsByKinds: two calls
// naming the same kinds in a different order must dedup to one entry, since
// the kinds -- not the call's argument order -- are the criteria's actual
// identity.
func kindsKey(kinds graph.Kinds) string {
	if len(kinds) == 0 {
		return ""
	}
	names := make([]string, len(kinds))
	for i, kind := range kinds {
		names[i] = kindName(kind)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// edgeTripleKey is RecordEdgeTriple's dedup key: the same (start, end, kind)
// triple recorded twice -- e.g. a batch that calls CreateRelationshipByIDs
// with identical arguments twice, however that might arise -- is one entry,
// not two, in EdgeTriples' result.
type edgeTripleKey struct {
	start, end uint64
	kind       string
}

// edgeTripleOIDKey is edgeTripleKey's objectid-keyed equivalent, behind
// RecordEdgeTripleByObjectID.
type edgeTripleOIDKey struct {
	startOID, endOID, kind string
}

// RecordNodeID records that the write named node id id as a read-back key:
// the applier should re-read this node's current row from PostgreSQL and
// apply whatever it finds. Called by every write path that names a node id
// directly and either doesn't yet know (a create) or doesn't need
// (an update or delete by id) any more specific information than the id
// itself -- see write_observer.go's observingTransaction.CreateNode (the
// pg-returned node's own id, captured after CreateNode's nil-error check),
// observingTransaction.UpdateNode, observingBatch.UpdateNodes,
// observingBatch.DeleteNode, and the recognized-InIDs branch of
// observingNodeQuery.Delete/Update and observingBatch's NodeBatchCreator
// passthrough.
//
// Recording the same id more than once within one ChangeSet has the same
// effect as recording it once; NodeIDs() returns each recorded id exactly
// once, in ascending order.
func (c *ChangeSet) RecordNodeID(id graph.ID) {
	if c.nodeIDs == nil {
		c.nodeIDs = make(map[uint64]struct{}, 1)
	}
	c.nodeIDs[uint64(id)] = struct{}{}
}

// RecordNodeObjectID records that the write identified its target node by
// an "objectid" property value rather than by database id: an
// UpdateNodeBy (or UpdateRelationshipBy endpoint) upsert whose identity
// shape write_observer.go's recognizer confirms is exactly
// IdentityProperties == ["objectid"], with objectID read from the node's
// own Properties. The applier resolves objectID to a node id during its
// own read-back, the same way the pg upsert this call mirrors resolves it
// against the database's own objectid index.
//
// Recording the same objectID more than once has the same effect as
// recording it once; NodeObjectIDs() returns each recorded value exactly
// once, in sorted order.
func (c *ChangeSet) RecordNodeObjectID(objectID string) {
	if c.nodeObjectIDs == nil {
		c.nodeObjectIDs = make(map[string]struct{}, 1)
	}
	c.nodeObjectIDs[objectID] = struct{}{}
}

// RecordEdgeID is RecordNodeID's edge equivalent: the write named edge id
// id as a read-back key. Called by observingTransaction.
// CreateRelationshipByIDs (the pg-returned relationship's own id, captured
// after its nil-error check), observingBatch.DeleteRelationship, and the
// recognized-InIDs branch of observingRelationshipQuery.Update.
//
// Recording the same id more than once has the same effect as recording it
// once; EdgeIDs() returns each recorded id exactly once, in ascending
// order.
func (c *ChangeSet) RecordEdgeID(id graph.ID) {
	if c.edgeIDs == nil {
		c.edgeIDs = make(map[uint64]struct{}, 1)
	}
	c.edgeIDs[uint64(id)] = struct{}{}
}

// RecordEdgeTriple records a relationship the write created whose own
// database id this write path never learns: observingBatch.
// CreateRelationship and CreateRelationshipByIDs both report success or
// failure only, never the new edge's id (unlike observingTransaction's
// tx-level equivalents, which do -- see RecordEdgeID's doc). The applier
// instead resolves this triple by its endpoints and kind during read-back.
//
// Recording the same (start, end, kind) triple more than once has the same
// effect as recording it once; EdgeTriples() returns each recorded triple
// exactly once.
func (c *ChangeSet) RecordEdgeTriple(start, end graph.ID, kind graph.Kind) {
	if c.edgeTriples == nil {
		c.edgeTriples = make(map[edgeTripleKey]EdgeTripleRef, 1)
	}
	key := edgeTripleKey{start: uint64(start), end: uint64(end), kind: kindName(kind)}
	c.edgeTriples[key] = EdgeTripleRef{Start: uint64(start), End: uint64(end), Kind: kind}
}

// RecordEdgeTripleByObjectID is RecordEdgeTriple's objectid-keyed
// equivalent, for observingBatch.UpdateRelationshipBy once both endpoints'
// identities are recognized (write_observer.go's recognizer): the pg
// upsert this call mirrors resolves startOID/endOID and kind to (possibly
// newly created) node rows and a relationship row all in the same
// statement, so the applier needs the same three-part key to replay it.
//
// Recording the same (startOID, endOID, kind) triple more than once has the
// same effect as recording it once; EdgeTriplesByObjectID() returns each
// recorded triple exactly once.
func (c *ChangeSet) RecordEdgeTripleByObjectID(startOID, endOID string, kind graph.Kind) {
	if c.edgeTriplesOID == nil {
		c.edgeTriplesOID = make(map[edgeTripleOIDKey]EdgeTripleOIDRef, 1)
	}
	key := edgeTripleOIDKey{startOID: startOID, endOID: endOID, kind: kindName(kind)}
	c.edgeTriplesOID[key] = EdgeTripleOIDRef{StartOID: startOID, EndOID: endOID, Kind: kind}
}

// RecordDeleteNodesByKinds records a kind-scoped node delete criteria --
// Driver.DeleteNodesByKinds' own includeAny/excludeAny, and the recognized
// (non-InIDs) shape of a criteria this package's node-delete recognizer
// maps to a kind matcher for -- as an operation rather than an enumerated
// id list: the applier replays "delete every node matching this criteria"
// directly, since the criteria describes the delete's blast radius more
// durably than any id list captured before the delete ran.
//
// Recording the same (include, exclude) pair more than once -- comparing
// kinds by name, regardless of slice order -- has the same effect as
// recording it once; NodeKindCriteria() returns each recorded pair exactly
// once.
func (c *ChangeSet) RecordDeleteNodesByKinds(include, exclude graph.Kinds) {
	if c.nodeKindDeletes == nil {
		c.nodeKindDeletes = make(map[string]NodeKindDeleteCriteria, 1)
	}
	key := kindsKey(include) + "|" + kindsKey(exclude)
	c.nodeKindDeletes[key] = NodeKindDeleteCriteria{Include: include, Exclude: exclude}
}

// RecordDeleteRelationshipsByKinds is RecordDeleteNodesByKinds' relationship
// equivalent: Driver.DeleteRelationshipsByKinds' own kinds parameter, and
// observingRelationshipQuery.Delete's recognized-kind-matcher branch
// (relationshipDeleteScope/edgeKindsFromCriteria), recorded as "delete
// every relationship of these kinds" rather than an enumerated id list.
//
// Recording the same kinds more than once -- by name, regardless of slice
// order -- has the same effect as recording it once; EdgeKindCriteria()
// returns each recorded set exactly once.
func (c *ChangeSet) RecordDeleteRelationshipsByKinds(kinds graph.Kinds) {
	if c.edgeKindDeletes == nil {
		c.edgeKindDeletes = make(map[string]graph.Kinds, 1)
	}
	c.edgeKindDeletes[kindsKey(kinds)] = kinds
}

// RecordFallback records that some part of the write escaped this
// package's changelog tracking entirely -- a mutating raw Cypher Query, a
// Raw SQL call, a WithGraph retarget, an unrecognized Update/Delete
// criteria, or an unrecognized upsert identity shape -- alongside reason, a
// short human-readable description of which. Apply's only sound response to
// a fallback is to treat the write as unknown and enter fallback (a full
// resync), rather than trying to replay it narrowly.
//
// Recording the same reason string more than once has the same effect as
// recording it once; HasFallback() returns each distinct reason exactly
// once. Reason strings are not required to be unique across call sites --
// see each RecordFallback call site's own doc for the exact text it uses --
// but callers should keep them descriptive enough to tell fallback sources
// apart in a log or test failure.
func (c *ChangeSet) RecordFallback(reason string) {
	if c.fallbacks == nil {
		c.fallbacks = make(map[string]struct{}, 1)
	}
	c.fallbacks[reason] = struct{}{}
}

// Empty reports whether nothing has been recorded on c at all -- no ids, no
// object ids, no triples, no kind-scoped delete criteria, and no fallback.
// WriteScope.Empty() (changes_scope.go) is exactly this call on the
// WriteScope's own ChangeSet.
func (c *ChangeSet) Empty() bool {
	return len(c.nodeIDs) == 0 && len(c.nodeObjectIDs) == 0 &&
		len(c.edgeIDs) == 0 &&
		len(c.edgeTriples) == 0 && len(c.edgeTriplesOID) == 0 &&
		len(c.nodeKindDeletes) == 0 && len(c.edgeKindDeletes) == 0 &&
		len(c.fallbacks) == 0
}

// HasFallback reports whether RecordFallback has ever been called on c,
// alongside every distinct reason recorded, in sorted order. ok is false,
// and reasons is nil, when RecordFallback has never been called.
func (c *ChangeSet) HasFallback() (ok bool, reasons []string) {
	if len(c.fallbacks) == 0 {
		return false, nil
	}
	return true, sortedKeys(c.fallbacks)
}

// sortedKeys returns the keys of m in sorted order, or nil if m is empty --
// the shared implementation behind HasFallback and NodeObjectIDs.
func sortedKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortedUint64Keys returns the keys of m in ascending order, or nil if m is
// empty -- NodeIDs/EdgeIDs' shared implementation, mirroring sortedKeys
// above for map[string]uint64 keys.
func sortedUint64Keys(m map[uint64]struct{}) []uint64 {
	if len(m) == 0 {
		return nil
	}
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// NodeIDs returns every node id RecordNodeID has recorded on c, deduped, in
// ascending order. A nil result means nothing was ever recorded.
func (c *ChangeSet) NodeIDs() []uint64 {
	return sortedUint64Keys(c.nodeIDs)
}

// NodeObjectIDs returns every objectid RecordNodeObjectID has recorded on
// c, deduped, in sorted order. A nil result means nothing was ever
// recorded.
func (c *ChangeSet) NodeObjectIDs() []string {
	return sortedKeys(c.nodeObjectIDs)
}

// EdgeIDs is NodeIDs' edge equivalent, over RecordEdgeID.
func (c *ChangeSet) EdgeIDs() []uint64 {
	return sortedUint64Keys(c.edgeIDs)
}

// EdgeTriples returns every (start, end, kind) triple RecordEdgeTriple has
// recorded on c, deduped, ordered by (Start, End, Kind name). A nil result
// means nothing was ever recorded.
func (c *ChangeSet) EdgeTriples() []EdgeTripleRef {
	if len(c.edgeTriples) == 0 {
		return nil
	}
	out := make([]EdgeTripleRef, 0, len(c.edgeTriples))
	for _, ref := range c.edgeTriples {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		if out[i].End != out[j].End {
			return out[i].End < out[j].End
		}
		return kindName(out[i].Kind) < kindName(out[j].Kind)
	})
	return out
}

// EdgeTriplesByObjectID is EdgeTriples' objectid-keyed equivalent, over
// RecordEdgeTripleByObjectID, ordered by (StartOID, EndOID, Kind name).
func (c *ChangeSet) EdgeTriplesByObjectID() []EdgeTripleOIDRef {
	if len(c.edgeTriplesOID) == 0 {
		return nil
	}
	out := make([]EdgeTripleOIDRef, 0, len(c.edgeTriplesOID))
	for _, ref := range c.edgeTriplesOID {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartOID != out[j].StartOID {
			return out[i].StartOID < out[j].StartOID
		}
		if out[i].EndOID != out[j].EndOID {
			return out[i].EndOID < out[j].EndOID
		}
		return kindName(out[i].Kind) < kindName(out[j].Kind)
	})
	return out
}

// NodeKindCriteria returns every (include, exclude) pair
// RecordDeleteNodesByKinds has recorded on c, deduped, ordered by their
// canonical kindsKey. A nil result means nothing was ever recorded.
func (c *ChangeSet) NodeKindCriteria() []NodeKindDeleteCriteria {
	if len(c.nodeKindDeletes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(c.nodeKindDeletes))
	for k := range c.nodeKindDeletes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]NodeKindDeleteCriteria, len(keys))
	for i, k := range keys {
		out[i] = c.nodeKindDeletes[k]
	}
	return out
}

// EdgeKindCriteria returns every kind set RecordDeleteRelationshipsByKinds
// has recorded on c, deduped, ordered by their canonical kindsKey. A nil
// result means nothing was ever recorded.
func (c *ChangeSet) EdgeKindCriteria() []graph.Kinds {
	if len(c.edgeKindDeletes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(c.edgeKindDeletes))
	for k := range c.edgeKindDeletes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]graph.Kinds, len(keys))
	for i, k := range keys {
		out[i] = c.edgeKindDeletes[k]
	}
	return out
}
