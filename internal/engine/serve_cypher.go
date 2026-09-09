// SPDX-License-Identifier: Apache-2.0

// serve_cypher.go implements the materialization layer between
// the in-memory Cypher interpreter (internal/engine/interpret's Plan/
// Execute) and dawgs' graph.Result contract: turning a snapshot.NodeID/
// interpret.EdgeRef/interpret.PathVal into a full *graph.Node/*graph.
// Relationship/graph.Path, and wrapping a fully-materialized interpret.
// ResultSet as a graph.Result (cypherRowsResult) the way pathResult
// (result.go) and rowResult (rowresult.go) already wrap this milestone's
// two earlier serving pipelines.
//
// engine.go's TryCypher wires all of this together, via
// safeExecuteCypher/buildCypherRowsResult, into a served graph.Result.
// Everything here is otherwise pure, snapshot-only
// materialization: no I/O, no PostgreSQL round trip, nothing that can fail
// on its own -- exactly the same posture pathResult/rowResult already take
// (their Error() always returns nil), so cypherRowsResult's does too.

package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/interpret"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// Budget/batch constants for the Cypher interpreter pipeline (the milestone
// plan's "Global Constants" section), defined here as this task's file is
// their designated home.
//
// maxExpansionDepth's own mirror lives in interpret.MaxExpansionDepth
// (interpret/plan.go): that constant caps the *hop count* of an unbounded
// variable-length relationship pattern at plan time, before a snapshot row
// is ever produced, whereas the three constants below cap what happens to
// rows and edge lookups *after* planning -- a different axis of the same
// "don't let one query eat unbounded resources" concern. It cannot simply
// be redefined here and referenced from interpret: interpret sits below
// this package in the import graph (this package imports interpret, never
// the reverse), so interpret.MaxExpansionDepth necessarily stays its own,
// self-contained copy -- see that constant's own doc comment, which
// anticipates exactly this file's existence.
const (
	// maxCypherRows caps the number of rows a served Cypher query may
	// produce, mirroring interpret.Budgets.MaxRows -- the executor already
	// enforces this during materialization (interpret.ErrBudget), so a
	// caller wiring interpret.Execute for the TryCypher pipeline (Task 13)
	// is expected to pass Budgets{MaxRows: maxCypherRows, ...} rather than
	// leave it unbounded.
	maxCypherRows = 100_000

	// maxCypherWork caps interpret.Budgets.MaxWork, the executor's generic
	// per-query work-unit counter (nodes scanned, adjacency slots inspected,
	// rows produced -- see interpret.Budgets' own doc). 1<<28 is generous
	// enough for any query this interpreter is expected to serve locally
	// while still bounding a pathological pattern (e.g. a dense var-length
	// expansion) well short of the kind of runaway cost that would make
	// local serving slower than simply delegating to PostgreSQL.
	maxCypherWork = 1 << 28

	// edgePropsBatchSize caps how many database edge ids one Task 11
	// property-hydration query batches into a single `WHERE id = ANY($1)`
	// round trip, mirroring hydrate.go's own edgeBatchSize for node/edge
	// hydration (500 triples there; edge-id-only batches here can run much
	// larger since each bound parameter is a single uint64, not a three-
	// column triple).
	edgePropsBatchSize = 10_000
)

// --- TryCypher pipeline support -------------------------------------------
//
// collectEdgeIDs and cypherExecReason are the two pieces of Task 13's
// TryCypher pipeline (engine.go) that are pure functions of an already-
// executed interpret.ResultSet/error rather than engine.Engine methods in
// their own right, so -- like this file's materialization helpers above and
// projectionValueKinds below -- they live here rather than in engine.go
// itself.

// collectEdgeIDs gathers every database edge id any OutEdge or OutPath
// column of rs's rows references, resolved through interpret.EdgeRef's own
// overlay-aware DatabaseID (e.Fwd's forward-CSR index when !snap.Overlay(),
// e.EdgeID directly otherwise -- see EdgeRef's doc) to the database edge id
// materializeEdge/materializePath ultimately need -- exactly the key
// hydrateEdgePropsByID batches on.
//
// The returned slice may contain duplicates (the same edge reached via two
// different rows or columns, or via more than one path segment) and is in
// no particular order: hydrateEdgePropsByIDBatched already deduplicates its
// input before issuing any query (dedupeUint64s), so there is no reason to
// do that work twice here. A nil/empty return means rs carries no edge or
// path value at all -- TryCypher's own signal to skip hydration entirely for
// a pure-scalar/node result that never touched anything hydration could
// affect. (Write-through (apply.go) retired the post-hydration recheck this
// doc used to name here -- a published View is immutable and current for
// the whole life of the query that captured it, so there is nothing left to
// recheck after hydration either.)
func collectEdgeIDs(snap *snapshot.View, rs *interpret.ResultSet) []uint64 {
	var ids []uint64
	for _, row := range rs.Rows {
		for _, v := range row {
			switch v.Kind {
			case interpret.OutEdge:
				ids = append(ids, v.Edge.DatabaseID(snap))
			case interpret.OutPath:
				if v.Path == nil {
					continue
				}
				for _, e := range v.Path.Edges {
					ids = append(ids, e.DatabaseID(snap))
				}
			}
		}
	}
	return ids
}

// cypherExecReason maps one interpret.Execute error to the TryCypher decline
// reason it logs, per the milestone's pinned sentinel-to-reason table:
// interpret.ErrBudget (a MaxRows/MaxWork budget was exceeded) ->
// reasonBudget; interpret.ErrCollation (the answer depends on PostgreSQL's
// own collation) -> reasonCollation; interpret.ErrSelfEndpoint (mirrors
// servePathQuery's own reasonSelfEndpoint decline, reused rather than
// duplicated) -> reasonSelfEndpoint; interpret.ErrRuntimeCast,
// interpret.ErrNotComparable, and interpret.ErrUnsupported (a runtime type
// mismatch, a non-orderable comparison, and an AST shape the evaluator
// itself does not recognize, respectively -- see each sentinel's own doc in
// interpret/eval.go and interpret/value.go) -> reasonUnsupported, the same
// "delegate, no further diagnosis needed" bucket Plan's own decline already
// uses.
//
// Every other error -- including interpret's own unexported
// errUnsupportedStep sentinel, which exec.go's doc comment deliberately
// keeps unexported so that no caller (this one included) ever special-cases
// it -- falls to reasonError, matching servePathQuery's own identical
// catch-all. This is a deliberate, documented departure from naming
// errUnsupportedStep's bucket "unsupported" explicitly: Go simply has no way
// to errors.Is against an identifier this package does not export, and
// exec.go's own doc explains why that is intentional, not an oversight this
// function should work around (e.g. by matching on err.Error() text, which
// would be one internal rename away from silently breaking). The decline
// behavior -- return false, let the caller delegate to PostgreSQL -- is
// identical either way; only the logged reason attr's exact label differs
// for that one internal case.
func cypherExecReason(err error) string {
	switch {
	case errors.Is(err, errCypherPanic):
		return reasonPanic
	case errors.Is(err, interpret.ErrBudget):
		return reasonBudget
	case errors.Is(err, interpret.ErrCollation):
		return reasonCollation
	case errors.Is(err, interpret.ErrSelfEndpoint):
		return reasonSelfEndpoint
	case errors.Is(err, interpret.ErrRuntimeCast), errors.Is(err, interpret.ErrNotComparable), errors.Is(err, interpret.ErrUnsupported):
		return reasonUnsupported
	default:
		return reasonError
	}
}

// errCypherPanic is safeExecuteCypher's own sentinel, wrapped (via %w, so
// errors.Is still finds it) around whatever recover() returned, letting
// cypherExecReason (above) recognize a recovered panic as a distinct
// decline reason (reasonPanic) rather than folding it into the generic
// reasonError bucket.
var errCypherPanic = errors.New("interpret: recovered panic during Execute")

// executeCypher is interpret.Execute, called through this package-level
// variable rather than directly so a test can substitute a function that
// deliberately panics -- proving safeExecuteCypher's recover (below)
// actually converts that into errCypherPanic, without needing to first find
// a genuinely poisonous (*interpret.Query, interpret.Env) pair that
// provokes a real panic from outside the interpret package. Production
// code never reassigns this.
var executeCypher = interpret.Execute

// safeExecuteCypher runs executeCypher (interpret.Execute in production)
// under a recover, converting any panic into a plain error (wrapping
// errCypherPanic) rather than letting it propagate out of TryCypher and
// crash whatever goroutine is holding TryCypher's caller -- a
// fail-safe backstop. This package's interpreter is not proven panic-free
// by construction (unlike interpret.Plan, which already carries its own
// top-level recover), so this and buildCypherRowsResult's identical
// recover on the materialization side (below) together make "a panic
// reaching a caller after TryCypher has already returned true" impossible
// regardless of what future interpreter bug might otherwise cause one.
func safeExecuteCypher(env *interpret.Env, q *interpret.Query, b interpret.Budgets) (rs *interpret.ResultSet, err error) {
	defer func() {
		if r := recover(); r != nil {
			rs, err = nil, fmt.Errorf("%w: %v", errCypherPanic, r)
		}
	}()
	return executeCypher(env, q, b)
}

// --- Node/edge/path materialization -----------------------------------

// materializeNode builds a full graph.Node value for a snapshot's dense
// node id, mirroring what the pg driver would decode from the same node's
// own `node` table row: ID from GraphIDs (the node's database id), Kinds
// resolved from the node's own kind_ids slice (NodeKinds[KindOffsets[n]:
// KindOffsets[n+1]]) through the snapshot's own local KindTable -- in
// exactly the array order Builder.AddNode originally received them,
// matching interpret/eval.go's evalLabelsFunction's identical "no sorting
// or deduplication" contract for labels(n) -- and Properties built directly
// from Props.NodeMap(n) via graph.AsProperties, which aliases the map
// rather than copying it (see graph.AsProperties' doc comment): NodeMap
// already allocates a fresh map on every call, so this aliasing is safe.
//
// A KindID present in NodeKinds that snap.Kinds cannot resolve to a name is
// silently skipped from the result rather than surfaced as a placeholder
// Kind -- mirroring evalLabelsFunction's own identical skip, so labels(n)
// and a materialized node's own Kinds field never disagree. This is
// expected to be unreachable in practice: snap.Kinds is built from the
// database's entire global `kind` table (see LoadSnapshot), independent of
// which kinds any one node happens to carry, so every KindID a node's
// kind_ids slice can contain was already registered there.
func materializeNode(snap *snapshot.View, n snapshot.NodeID) *graph.Node {
	kindIDs := snap.KindIDsOf(n)
	kinds := make(graph.Kinds, 0, len(kindIDs))
	for _, id := range kindIDs {
		if name, ok := snap.Kinds().Name(id); ok {
			kinds = append(kinds, graph.StringKind(name))
		}
	}
	return graph.NewNode(graph.ID(snap.GraphID(n)), graph.AsProperties(snap.PropNodeMap(n)), kinds...)
}

// materializeEdge builds a full graph.Relationship for one forward-CSR
// slot: ID/Kind from that slot's own OutEdgeIDs/OutKinds entries, StartID/
// EndID from GraphIDs at the slot's source and target dense ids. props is
// supplied by the caller (Task 11's edge-property hydration, wired in Task
// 13) and passed straight through -- this function never inspects
// edgeProps itself, so an empty, non-nil *graph.Properties (e.g. from a
// test, or from edgePropsFor's own missing-entry fallback below) is exactly
// as valid a props argument as a fully hydrated one.
//
// !snap.Overlay(): e.Fwd is always a *forward* CSR index, whether the edge
// itself was originally discovered by walking a node's outgoing adjacency or
// its incoming one: see EdgeRef's own doc comment (interpret/eval.go) -- a
// reverse-CSR-discovered edge is already translated to its forward index
// via Snap.InEdgeIdx before it ever reaches a Row, so this function (like
// every other reader of an EdgeRef in the interpret package) only ever
// needs to read the forward arrays.
//
// The slot's source node is not itself stored per-slot -- OutTargets/
// OutKinds/OutEdgeIDs are aligned to the *target* side only, addressed by a
// flat forward-CSR index that spans every source node's segment
// contiguously -- so it is recovered by binary-searching OutOffsets, the
// same CSR row-pointer array interpret/eval.go's adjacency walks by adding
// its own loop index to OutOffsets[bound] (see adjCandidate's doc comment
// there). edgeSource below inverts that arithmetic for a caller that has
// only the resulting flat index left.
//
// snap.Overlay(): e.Fwd has no forward-CSR slot to name at all for a delta
// edge (EdgeRef's own doc), so start/end/kind instead resolve through
// snap.EdgeStateByID(e.EdgeID) -- overlay-complete over base, delta-added,
// and delta-upserted edges alike (its own doc). This always hits for an
// EdgeRef this package's own executors produced (it was minted from a live
// OutEdges/InEdges yield against this exact snap), so the miss case is left
// unguarded here the same way Kind's doc explains.
func materializeEdge(snap *snapshot.View, e interpret.EdgeRef, props *graph.Properties) *graph.Relationship {
	var start, end snapshot.NodeID
	var kind snapshot.KindID
	edgeID := e.DatabaseID(snap)
	if !snap.Overlay() {
		fwd := e.Fwd
		start = edgeSource(snap, fwd)
		end = snap.Base().OutTargets[fwd]
		kind = snap.Base().OutKinds[fwd]
	} else {
		start, end, kind, _ = snap.EdgeStateByID(edgeID)
	}
	name, _ := snap.Kinds().Name(kind)

	return graph.NewRelationship(
		graph.ID(edgeID),
		graph.ID(snap.GraphID(start)),
		graph.ID(snap.GraphID(end)),
		props,
		graph.StringKind(name),
	)
}

// edgeSource returns the dense NodeID that owns forward-CSR slot fwd -- the
// unique i such that OutOffsets[i] <= fwd < OutOffsets[i+1]. OutOffsets
// (length NodeCount()+1) is a monotonically non-decreasing prefix sum of
// each node's out-degree by construction (snapshot/builder.go's
// packForward: offsets[i+1] = offsets[i] + degree[i]), so sort.Search can
// find the smallest i with OutOffsets[i+1] > fwd -- exactly that unique
// row, including correctly skipping over any zero-out-degree node whose
// segment is empty (OutOffsets[i] == OutOffsets[i+1]).
func edgeSource(snap *snapshot.View, fwd uint64) snapshot.NodeID {
	n := len(snap.Base().OutOffsets) - 1
	i := sort.Search(n, func(i int) bool { return snap.Base().OutOffsets[i+1] > fwd })
	return snapshot.NodeID(i)
}

// materializePath builds a full graph.Path from a *interpret.PathVal:
// Nodes materialized via materializeNode in traversal order (Nodes[i]
// connected to Nodes[i+1] by Edges[i], per PathVal's own doc comment in
// interpret/exec.go), Edges via materializeEdge. edgeProps supplies each
// edge's properties, keyed by database edge id (e.DatabaseID(snap)) --
// deliberately a plain edge-id key rather than the (start, end, kind)
// triple hydrate.go's older edgeKey uses, since materializeEdge already
// derives start/end/kind from the snapshot alone; Task 11's hydration query
// needs nothing more than a `WHERE id = ANY($1)` over the edge table to
// produce this map.
//
// A PathVal edge with no corresponding edgeProps entry gets an empty,
// non-nil *graph.Properties (via edgePropsFor) rather than an error: this
// function has no way to distinguish "not yet hydrated" from "hydrated as
// genuinely empty", and the milestone's execution model (Task 13) is
// responsible for guaranteeing every edge a materialized path can reference
// was included in that path's own hydration batch before this function is
// ever called -- so a missing entry here would only reflect a caller bug
// upstream, which is not this function's job to detect or report.
//
// A nil p returns the zero graph.Path{} rather than panicking -- purely
// defensive, since nothing in this milestone's execution model is expected
// to call this with a nil *PathVal (OutVal.Path is always non-nil whenever
// OutVal.Kind is OutPath).
func materializePath(snap *snapshot.View, p *interpret.PathVal, edgeProps map[uint64]*graph.Properties) graph.Path {
	if p == nil {
		return graph.Path{}
	}

	nodes := make([]*graph.Node, len(p.Nodes))
	for i, n := range p.Nodes {
		nodes[i] = materializeNode(snap, n)
	}

	edges := make([]*graph.Relationship, len(p.Edges))
	for i, e := range p.Edges {
		edges[i] = materializeEdge(snap, e, edgePropsFor(snap, edgeProps, e))
	}

	return graph.Path{Nodes: nodes, Edges: edges}
}

// edgePropsFor looks up e's hydrated properties in edgeProps by database
// edge id, falling back to a fresh, empty *graph.Properties (never nil)
// when absent. Shared by materializePath (per-edge, for every edge on a
// path) and materializeValue below (for a bare, top-level OutEdge
// projection column) so the two never disagree on the missing-entry
// fallback -- see materializePath's doc for why absence is not treated as
// an error here.
func edgePropsFor(snap *snapshot.View, edgeProps map[uint64]*graph.Properties, e interpret.EdgeRef) *graph.Properties {
	if props, ok := edgeProps[e.DatabaseID(snap)]; ok && props != nil {
		return props
	}
	return graph.NewProperties()
}

// --- Scalar double-decode ------------------------------------------------

// decodeScalarString mirrors dawgs' pg driver's decodeJSONValue string
// branch (drivers/pg/result.go:81-99, dawgs@v0.8.0) byte for byte: a scalar
// string whose strings.TrimSpace begins with '{', '[', or '"' is assumed to
// be one more layer of JSON-encoded text -- pg's own jsonb text rendering
// of an object/array/string scalar column -- and is re-parsed via
// json.Unmarshal. On parse failure, or when the trimmed value is empty, or
// when it does not start with one of those three bytes, the original
// string is returned completely unchanged: dawgs' own decodeJSONValue
// reports ok=false in exactly those cases, and its caller (decodeJSONValues)
// then leaves the original raw value in place rather than substituting
// anything -- so mirroring that requires returning the pre-trim, original
// string s itself, not the trimmed copy, whenever decoding does not apply.
func decodeScalarString(s string) any {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) == 0 {
		return s
	}
	switch trimmed[0] {
	case '{', '[', '"':
	default:
		return s
	}

	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err == nil {
		return decoded
	}
	return s
}

// --- Projection value-kind resolution ------------------------------------

// valueKind names the pg-parity numeric conversion a materialized scalar
// column needs, beyond the interpreter's own uniform float64 representation
// (see interpret/value.go's package doc and evalIDFunction/
// evalSizeFunction/evalDateTimeComponent's doc comments for why the
// evaluator itself stays float64-only). valueDefault covers every ordinary
// projection: no conversion is applied, and the interpreter's post-JSON
// value (nil | string | float64 | bool | []any | map[string]any) passes
// through materializeScalar unchanged (aside from decodeScalarString's
// double-decode rule, which applies uniformly regardless of valueKind).
type valueKind uint8

const (
	// valueDefault applies to every projection item that is not one of the
	// controller's four projection-typing-amendment calls (or a WITH-COUNT
	// alias reference to one) -- a property lookup, a bare node/edge/path
	// variable, arithmetic, a literal, labels()/type()/toLower()/toUpper()/
	// coalesce()/split(), or a datetime() component other than epochseconds/
	// epochmillis.
	valueDefault valueKind = iota
	// valueInt64 applies to id(), datetime().epochseconds, datetime().
	// epochmillis, and any bare reference (renamed or not) to a WITH
	// COUNT(...) alias -- the pinned pg-parity type for all four is int64.
	valueInt64
	// valueInt32 applies to size() -- pg-parity int32.
	valueInt32
)

// projectionValueKinds resolves each of q.Returning.Items' pg-parity output
// type, index-aligned with q.Returning.Items -- and therefore with every
// interpret.ResultSet.Keys/Rows[i] column interpret.Execute produces from
// the same *interpret.Query, since Plan/Execute share exactly this column
// ordering (interpret.ResultSet's own doc comment).
//
// ProjectionOutput.BareCallKind ("id"/"epochseconds"/"epochmillis" ->
// valueInt64, "size" -> valueInt32) already flags three of the controller's
// four amendment calls directly at plan time -- see interpret/plan.go's
// bareCallKind/projectionTypingOK. COUNT is the one exception, and it needs
// a second pass here rather than a BareCallKind of its own: COUNT is never
// itself a RETURN item's own top-level expression under this planner's
// accepted grammar (a bare `RETURN count(x)` is rejected outright --
// checkExpr's generic FunctionInvocation switch, interpret/plan.go, has no
// case for "count" at all; classifyAggregate is the *only* place that name
// is recognized, and it is reachable only from planWith). A WITH COUNT(sym)
// AS alias instead flows into RETURN as an ordinary bare-variable reference
// to that alias (planWith's outputKnown[alias] = symScalar), indistinguishable
// at the RETURN item's own level from any other scalar carry-over -- exactly
// the gap this resolver's second pass closes: it separately collects every
// COUNT alias declared by any Part's WithClause (q.Parts[i].With.Aggregates,
// Count != nil -- at most one Part can carry a non-nil With under this
// planner's "at most one WITH boundary" restriction, but every Part is
// scanned regardless, for robustness against that restriction ever
// loosening) and flags a RETURN item as valueInt64 whenever its own
// expression is a bare reference to one of those aliases, however deeply
// parenthesized, and regardless of whether the RETURN item itself renames
// the output column via AS.
func projectionValueKinds(q *interpret.Query) []valueKind {
	countAliases := map[string]bool{}
	for _, part := range q.Parts {
		if part.With == nil {
			continue
		}
		for _, agg := range part.With.Aggregates {
			if agg.Count != nil {
				countAliases[agg.Alias] = true
			}
		}
	}

	kinds := make([]valueKind, len(q.Returning.Items))
	for i, item := range q.Returning.Items {
		switch item.BareCallKind {
		case "id", "epochseconds", "epochmillis":
			kinds[i] = valueInt64
			continue
		case "size":
			kinds[i] = valueInt32
			continue
		}
		if v, ok := unwrapParens(item.Expr).(*cypher.Variable); ok && v != nil && countAliases[v.Symbol] {
			kinds[i] = valueInt64
		}
	}
	return kinds
}

// unwrapParens strips any number of *cypher.Parenthetical wrappers off expr,
// mirroring interpret package's own unexported helper of the same name
// (interpret/plan.go) -- duplicated here in miniature rather than exported
// from interpret, since this is the only place in this package that needs
// it and the two packages' own paren-unwrapping needs are otherwise
// unrelated.
func unwrapParens(expr cypher.Expression) cypher.Expression {
	for {
		p, ok := expr.(*cypher.Parenthetical)
		if !ok || p == nil {
			return expr
		}
		expr = p.Expression
	}
}

// materializeScalar converts one interpret.OutScalar value into its final
// projected form: decodeScalarString's double-decode rule is applied first
// (unconditionally, to every string scalar, regardless of vk -- it is
// pg-parity text-column handling, orthogonal to the numeric-type
// conversions below), then vk's int64/int32 conversion is applied if the
// (possibly just-decoded) value is a float64. A vk of valueInt64/valueInt32
// over a non-float64 value (only possible for a NULL/absent bare call
// result, e.g. id(n) can never be absent but a hypothetical future bare
// call might) leaves the value unconverted rather than panicking or
// coercing a NULL into a numeric zero.
func materializeScalar(v any, vk valueKind) any {
	if s, isString := v.(string); isString {
		v = decodeScalarString(s)
	}
	switch vk {
	case valueInt64:
		if f, ok := v.(float64); ok {
			return int64(f)
		}
	case valueInt32:
		if f, ok := v.(float64); ok {
			return int32(f)
		}
	}
	return v
}

// --- cypherRowsResult: graph.Result over a materialized ResultSet --------

// cypherRowsResult implements graph.Result over an interpret.ResultSet
// that has already been fully computed by interpret.Execute -- one row per
// ResultSet.Rows entry, each interpret.OutVal materialized into a *graph.
// Node/*graph.Relationship/graph.Path/converted-scalar column.
//
// Materialization happens EAGERLY, for every row, inside the constructor
// (newCypherRowsResult) rather than lazily inside Next() -- a deliberate
// change from this type's original design: TryCypher builds this result
// under buildCypherRowsResult's own
// recover (below), specifically so that a panic anywhere in
// materialization is caught THERE, before TryCypher ever returns true, not
// later inside some caller's own Next() loop after TryCypher has already
// committed to serving -- at which point a panic would be an unrecoverable
// false serve, not a graceful decline. rs.Rows is already fully resident in
// memory and capped at maxCypherRows (interpret.Execute's own contract),
// so converting every row upfront costs no more than converting them all
// lazily would have anyway for the overwhelmingly common case (ops.
// FetchByQuery always drains a result to completion); the only real
// tradeoff given up is doing that conversion work for a row a caller might
// have abandoned before reaching, which -- unlike the panic safety this
// buys -- has never been a documented guarantee of this type.
//
// The zero value is not useful; construct with newCypherRowsResult.
type cypherRowsResult struct {
	snap      *snapshot.View
	keys      []string
	kinds     []valueKind // index-aligned with the original rows/keys, from projectionValueKinds
	edgeProps map[uint64]*graph.Properties

	// materialized holds every row's already-converted []any values,
	// index-aligned with keys -- computed once, eagerly, by
	// newCypherRowsResult (see this type's own doc for why).
	materialized [][]any

	// idx is the current row, starting one before the first (-1) --
	// mirroring pathResult/rowResult's identical "Next() advances before
	// checking bounds" convention (result.go, rowresult.go).
	idx int
}

// newCypherRowsResult wraps rs as a graph.Result, eagerly materializing
// every row right here (see cypherRowsResult's own doc for why). kinds must
// be index-aligned with rs.Keys/rs.Rows[i] -- the caller is expected to have
// produced it via projectionValueKinds(q) against the same *interpret.Query
// that planned rs. edgeProps supplies every path/edge column's relationship
// properties (Task 11's hydration, keyed by database edge id) and may be
// nil or incomplete: edgePropsFor's missing-entry fallback (an empty,
// non-nil *graph.Properties) covers both cases, so a test may pass nil to
// exercise column shapes without hydrating anything.
//
// Callers reached from TryCypher's own pipeline must go through
// buildCypherRowsResult instead of calling this directly, so that a panic
// during the eager materialization this performs is recovered rather than
// escaping to TryCypher's caller -- this function itself does
// NOT recover anything, matching every other constructor in this package;
// test code that calls it directly (as several serve_cypher_test.go cases
// do, deliberately exercising known-good fixtures) gets an ordinary panic
// on a genuine bug, same as before this fix.
func newCypherRowsResult(snap *snapshot.View, rs *interpret.ResultSet, kinds []valueKind, edgeProps map[uint64]*graph.Properties) graph.Result {
	r := &cypherRowsResult{
		snap:      snap,
		keys:      rs.Keys,
		kinds:     kinds,
		edgeProps: edgeProps,
		idx:       -1,
	}

	r.materialized = make([][]any, len(rs.Rows))
	for i, row := range rs.Rows {
		r.materialized[i] = r.materializeRow(row)
	}

	return r
}

// buildCypherRowsResult wraps newCypherRowsResult's own eager row
// materialization under a recover, converting any panic there into
// ok == false rather than letting it escape TryCypher -- see
// cypherRowsResult's own doc. This is the constructor
// TryCypher's pipeline actually calls; newCypherRowsResult itself stays
// available, unwrapped, for test code exercising known-good fixtures.
func buildCypherRowsResult(snap *snapshot.View, rs *interpret.ResultSet, kinds []valueKind, edgeProps map[uint64]*graph.Properties) (result graph.Result, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			result, ok = nil, false
		}
	}()
	return newCypherRowsResult(snap, rs, kinds, edgeProps), true
}

// Next advances to the next row and reports whether one exists. Every
// row's values were already materialized eagerly by newCypherRowsResult
// (see that constructor's own doc for why); this just walks r.materialized.
func (r *cypherRowsResult) Next() bool {
	r.idx++
	return r.idx >= 0 && r.idx < len(r.materialized)
}

// materializeRow converts one already-planned row of interpret.OutVal into
// its final []any projection, one column at a time via materializeValue.
func (r *cypherRowsResult) materializeRow(row []interpret.OutVal) []any {
	out := make([]any, len(row))
	for i, v := range row {
		out[i] = r.materializeValue(v, r.kindAt(i))
	}
	return out
}

// kindAt returns r.kinds[i], or valueDefault if kinds is shorter than the
// row (not expected under newCypherRowsResult's documented contract, but
// harmless to default rather than panic on an index caller error).
func (r *cypherRowsResult) kindAt(i int) valueKind {
	if i < len(r.kinds) {
		return r.kinds[i]
	}
	return valueDefault
}

// materializeValue dispatches one OutVal to the matching materializer:
// OutNode/OutEdge/OutPath ignore vk entirely (it is only ever meaningful for
// OutScalar -- a node/edge/path projection is never one of the controller's
// four bare-call amendment shapes), OutScalar routes through
// materializeScalar.
func (r *cypherRowsResult) materializeValue(v interpret.OutVal, vk valueKind) any {
	switch v.Kind {
	case interpret.OutNode:
		return materializeNode(r.snap, v.Node)
	case interpret.OutEdge:
		return materializeEdge(r.snap, v.Edge, edgePropsFor(r.snap, r.edgeProps, v.Edge))
	case interpret.OutPath:
		return materializePath(r.snap, v.Path, r.edgeProps)
	default: // interpret.OutScalar
		return materializeScalar(v.Scalar, vk)
	}
}

// Keys names each projection column, in RETURN order -- interpret.
// ResultSet.Keys already carries exactly this (see its own doc comment), so
// this is a direct pass-through.
func (r *cypherRowsResult) Keys() []string {
	return r.keys
}

// Values returns the current row's already-materialized values (computed
// eagerly by newCypherRowsResult, not on demand here). Calling Values()
// before any Next() call, or after Next() has returned false, returns nil
// -- matching pathResult/rowResult's identical convention.
func (r *cypherRowsResult) Values() []any {
	if r.idx < 0 || r.idx >= len(r.materialized) {
		return nil
	}
	return r.materialized[r.idx]
}

// Mapper returns a graph.ValueMapper recognizing this result's own
// already-typed raw values: mapPathValue (result.go, shared with
// pathResult) for a graph.Path rawValue into a *graph.Path target,
// mapCypherNodeValue for a *graph.Node rawValue into a *graph.Node target,
// and mapCypherRelationshipValue for a *graph.Relationship rawValue into a
// *graph.Relationship target. Every other (rawValue, target) combination is
// declined by all three -- including a *graph.Kinds target presented with a
// node's own rawValue, which none of the three match -- and dawgs' own
// defaultMapValue (graph/mapper.go), appended automatically by graph.
// NewValueMapper, has no case at all for *graph.Node/*graph.Relationship/
// graph.Path targets either (see mapCypherNodeValue's doc for why), so a
// plain scalar column (int64/int32/float64/string/bool/[]any/map[string]any)
// falls all the way through every MapFunc and reaches ops.FetchByQuery's
// final `else` branch, which wraps it as a graph.Literal -- exactly the
// behavior the brief's "decline everything else" requirement is aimed at.
func (r *cypherRowsResult) Mapper() graph.ValueMapper {
	return graph.NewValueMapper(mapPathValue, mapCypherNodeValue, mapCypherRelationshipValue)
}

// Scan is graph.Result's deprecated convenience method, implemented via
// graph.ScanNextResult -- the same shape pathResult.Scan/rowResult.Scan use.
func (r *cypherRowsResult) Scan(targets ...any) error {
	return graph.ScanNextResult(r, targets...)
}

// Error always returns nil: a cypherRowsResult is built from an already-
// computed interpret.ResultSet (interpret.Execute's own "fully materialized
// before any row is emitted" contract), so nothing that could fail is left
// to happen during iteration -- matching pathResult/rowResult's identical
// posture.
func (r *cypherRowsResult) Error() error {
	return nil
}

// Close is a no-op: a cypherRowsResult holds no external resource (cursor,
// connection, file, goroutine) to release.
func (r *cypherRowsResult) Close() {}

// mapCypherNodeValue is cypherRowsResult's MapFunc for a *graph.Node target.
// A cypherRowsResult row's raw node value is always an already-typed
// *graph.Node (materializeNode's own return type), never a raw driver
// scalar -- so, exactly like rowResult's mapRowIDValue/mapRowKindsValue/
// mapRowKindValue (rowresult.go's own doc comments explain the same root
// cause in more depth), dawgs' own defaultMapValue cannot be relied on
// alone: graph/mapper.go's defaultMapValue has no case at all for a *graph.
// Node target, so without this MapFunc every node-valued row would fall
// through every mapper and reach ops.FetchByQuery's final literal branch,
// silently misclassifying a node as a Literal instead of adding it to
// currentPath.Nodes.
func mapCypherNodeValue(rawValue, target any) bool {
	node, isNode := rawValue.(*graph.Node)
	if !isNode || node == nil {
		return false
	}
	nodeTarget, isNodeTarget := target.(*graph.Node)
	if !isNodeTarget {
		return false
	}
	*nodeTarget = *node
	return true
}

// mapCypherRelationshipValue is mapCypherNodeValue's counterpart for a
// *graph.Relationship target -- see that function's doc comment for why
// dawgs' own defaultMapValue cannot handle this case either.
func mapCypherRelationshipValue(rawValue, target any) bool {
	rel, isRel := rawValue.(*graph.Relationship)
	if !isRel || rel == nil {
		return false
	}
	relTarget, isRelTarget := target.(*graph.Relationship)
	if !isRelTarget {
		return false
	}
	*relTarget = *rel
	return true
}
