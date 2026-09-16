// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// --- helpers: run one standalone var-length component BOTH ways -------------

// varLengthPartAndStep plans query against snap and returns Part[0] together
// with its single standalone variable-length Step -- exactly the (part, step)
// pair runComponent hands expandVarLengthComponent -- so a test can drive the
// forward-seeded and the constrained-side-seeded executors directly, side by
// side, over identical inputs.
func varLengthPartAndStep(t *testing.T, snap *snapshot.View, query string) (*Part, *Step) {
	t.Helper()
	q := planQuery(t, snap, query)
	if len(q.Parts) != 1 {
		t.Fatalf("query %q: got %d Parts, want 1", query, len(q.Parts))
	}
	part := &q.Parts[0]
	comps := groupComponents(part)
	if len(comps) != 1 || len(comps[0].stepIdxs) != 1 {
		t.Fatalf("query %q: unexpected component shape %+v", query, comps)
	}
	step := &part.Chains[comps[0].stepIdxs[0]]
	if step.Range == nil {
		t.Fatalf("query %q: component step is not variable-length", query)
	}
	return part, step
}

// varLengthRowSigs renders a standalone variable-length component's own output
// rows as a sorted, comparable multiset: each row's two endpoint bindings by
// database id plus, when the pattern names a path, that path's complete
// node/edge signature (pathSig). Sorting makes the comparison order-
// independent while keeping MULTIPLICITY significant -- two rows binding the
// same endpoint pair via two distinct trails stay two separate entries.
func varLengthRowSigs(t *testing.T, snap *snapshot.View, step *Step, rows []*Row) []string {
	t.Helper()
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		from, okFrom := r.Node(step.FromSym)
		to, okTo := r.Node(step.ToSym)
		if !okFrom || !okTo {
			t.Fatalf("row is missing an endpoint binding (%s bound: %v, %s bound: %v)", step.FromSym, okFrom, step.ToSym, okTo)
		}
		sig := fmt.Sprintf("%s=%d %s=%d", step.FromSym, snap.GraphID(from), step.ToSym, snap.GraphID(to))
		if step.PathSym != "" {
			v, ok := r.PathVar(step.PathSym)
			if !ok {
				t.Fatalf("row is missing path binding %q", step.PathSym)
			}
			pv, ok := v.(*PathVal)
			if !ok {
				t.Fatalf("path binding %q is %T, want *PathVal", step.PathSym, v)
			}
			sig += " " + pathSig(snap, pv)
		}
		out = append(out, sig)
	}
	sort.Strings(out)
	return out
}

// runVarLengthBothWays runs query's single variable-length component twice --
// once through the plain forward expansion seeded from the pattern's own
// FromSym, once through the constrained-side expansion seeded from ToSym and
// walked backward over the reverse CSR -- over the identical planned
// Part/Step, applying the Part's complete WHERE to each result exactly as the
// pipeline does, and returns both surviving row multisets plus the work each
// direction spent.
//
// Filtering both sides through Part.Where before comparing is what makes the
// comparison meaningful rather than merely lenient: the constrained-side
// executor additionally evaluates ToSym's own pushed single-symbol predicates
// while selecting its seeds, so its RAW output legitimately omits rows whose
// ToSym binding fails such a predicate -- rows the forward executor does
// produce and Part.Where (which always carries those same conjuncts in full)
// then discards. Post-WHERE is the only stage at which the two are required to
// agree, and the only one any caller ever observes.
func runVarLengthBothWays(t *testing.T, snap *snapshot.View, query string) (forward, reverse []string, forwardWork, reverseWork int64) {
	t.Helper()
	env := &Env{Snap: snap}
	part, step := varLengthPartAndStep(t, snap, query)

	fwdMeter := &workMeter{budget: generousBudget}
	fwdRows, err := expandVarLengthComponentForward(env, fwdMeter, part, step)
	if err != nil {
		t.Fatalf("query %q: forward expansion: %v", query, err)
	}
	fwdRows, err = filterRows(env, fwdRows, part.Where, nil)
	if err != nil {
		t.Fatalf("query %q: forward filterRows: %v", query, err)
	}

	revMeter := &workMeter{budget: generousBudget}
	revRows, err := expandVarLengthComponentReverse(env, revMeter, part, step)
	if err != nil {
		t.Fatalf("query %q: reverse expansion: %v", query, err)
	}
	revRows, err = filterRows(env, revRows, part.Where, nil)
	if err != nil {
		t.Fatalf("query %q: reverse filterRows: %v", query, err)
	}

	return varLengthRowSigs(t, snap, step, fwdRows), varLengthRowSigs(t, snap, step, revRows), fwdMeter.work, revMeter.work
}

// assertVarLengthRowsAgree asserts query's forward and constrained-side
// expansions produce identical post-WHERE row multisets (see
// runVarLengthBothWays), calling both executors directly regardless of
// whether the dispatcher (varLengthReverseEligible) would actually pick the
// reverse route for this shape. Most callers want assertVarLengthDirectionsAgree
// instead, which adds that eligibility check as a non-vacuousness guard; this
// version exists for fixtures that deliberately exercise
// expandVarLengthComponentReverse's own correctness (e.g. its NEAR-endpoint
// constraint check on each reached node) using a near side that the
// dispatcher's scan-tier requirement now correctly refuses to reverse for
// cost reasons -- see varLengthReverseEligible's doc comment on why a kind
// bitmap near side, though not "narrowing", is still excluded.
func assertVarLengthRowsAgree(t *testing.T, snap *snapshot.View, query string) {
	t.Helper()
	forward, reverse, _, _ := runVarLengthBothWays(t, snap, query)
	if len(forward) != len(reverse) {
		t.Fatalf("query %q: reverse produced %d rows, forward produced %d\nreverse: %v\nforward: %v", query, len(reverse), len(forward), reverse, forward)
	}
	for i := range forward {
		if forward[i] != reverse[i] {
			t.Fatalf("query %q: row multiset mismatch\nreverse: %v\nforward: %v", query, reverse, forward)
		}
	}
}

// assertVarLengthDirectionsAgree asserts query's forward and constrained-side
// expansions produce identical post-WHERE row multisets (see
// runVarLengthBothWays), and -- so the comparison can never quietly become
// vacuous -- that the dispatcher's own eligibility rule actually selects the
// reverse route for this shape.
func assertVarLengthDirectionsAgree(t *testing.T, snap *snapshot.View, query string) {
	t.Helper()
	env := &Env{Snap: snap}
	part, step := varLengthPartAndStep(t, snap, query)
	if !varLengthReverseEligible(env, part, step) {
		t.Fatalf("query %q: varLengthReverseEligible = false; this harness only compares shapes the dispatcher actually reverses", query)
	}
	assertVarLengthRowsAgree(t, snap, query)
}

// --- fixtures ---------------------------------------------------------------

const (
	revKindSrc    snapshot.KindID = 1
	revKindTarget snapshot.KindID = 2
	revKindE      snapshot.KindID = 10
	revKindF      snapshot.KindID = 11
)

var revKindTable = map[snapshot.KindID]string{
	revKindSrc:    "Src",
	revKindTarget: "Target",
	revKindE:      "E",
	revKindF:      "F",
}

// buildReverseEqualityFixture builds one deliberately awkward snapshot that
// exercises, in a single graph, every trail-semantics corner the two
// enumeration directions have to agree about:
//
//   - a diamond fan-in (node 1 reaches node 6 through both node 3 and node 4),
//   - parallel edges of the same kind between the same ordered pair (2->6
//     twice), which must stay two distinct trails,
//   - a directed cycle (5->6->7->5) exercising the relationship-uniqueness
//     rule in both walk directions,
//   - a node carrying BOTH endpoint kinds (node 5), so `*0..` can bind the two
//     pattern endpoints to the same node,
//   - a second edge kind (2->3 via F) that a `[:E...]` pattern must exclude,
//   - a Target node whose objectid does NOT match (node 7), so the
//     constrained-side seed selection has something to reject,
//   - an isolated node (8) reachable from nothing.
func buildReverseEqualityFixture(t *testing.T) *snapshot.View {
	t.Helper()
	padNodes, padEdges := reverseEqualityPadding()
	return buildExecSnapshot(t, revKindTable,
		append([]execNodeSpec{
			{1, []snapshot.KindID{revKindSrc}, nil},
			{2, []snapshot.KindID{revKindSrc}, nil},
			{3, nil, nil},
			{4, nil, nil},
			{5, []snapshot.KindID{revKindSrc, revKindTarget}, map[string]any{"objectid": "T-516"}},
			{6, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "T-516"}},
			{7, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "T-999"}},
			{8, nil, nil},
		}, padNodes...),
		append([]execEdgeSpec{
			{100, 1, 3, revKindE},
			{101, 3, 6, revKindE},
			{102, 1, 4, revKindE},
			{103, 4, 6, revKindE},
			{104, 2, 6, revKindE},
			{105, 2, 6, revKindE},
			{106, 6, 7, revKindE},
			{108, 7, 5, revKindE},
			{109, 5, 6, revKindE},
			{110, 2, 3, revKindF},
		}, padEdges...),
	)
}

// reverseEqualityPadding returns a chain of nodes joined by E edges that
// reaches no Target, so it contributes no rows to any query in this file
// while making the NEAR side genuinely wide.
//
// It exists because the reverse route's eligibility rule is a COST
// comparison: it reverses when the near side is large and the far side is
// several times cheaper. On a seven-node fixture every side is small and the
// comparison cannot express anything, so a case named "wide near side" would
// be asserting about a near side of five nodes. Padding with isolated nodes
// would not work either -- the edge-kind hint prices the near side as the
// nodes carrying an admissible OUT edge, which isolated nodes do not.
func reverseEqualityPadding() ([]execNodeSpec, []execEdgeSpec) {
	const padding = 60
	var nodes []execNodeSpec
	var edges []execEdgeSpec
	for i := 0; i < padding; i++ {
		id := uint64(1000 + i)
		nodes = append(nodes, execNodeSpec{id, nil, nil})
		if i > 0 {
			edges = append(edges, execEdgeSpec{uint64(2000 + i), id - 1, id, revKindE})
		}
	}
	return nodes, edges
}

// TestVarLengthReverseEqualsForwardAcrossShapes is this change's central
// correctness guard: over one graph packed with trail-semantics corner cases
// (buildReverseEqualityFixture), every reverse-eligible pattern spelling must
// produce exactly the rows -- and, for a named path, exactly the node/edge
// sequences -- the plain forward expansion produces for the same query.
func TestVarLengthReverseEqualsForwardAcrossShapes(t *testing.T) {
	snap := buildReverseEqualityFixture(t)

	for _, query := range []string{
		// objectid equality anchor on the far endpoint.
		`MATCH (s)-[:E*1..3]->(t:Target) WHERE t.objectid = 'T-516' RETURN s, t`,
		`MATCH p = (s)-[:E*1..4]->(t:Target) WHERE t.objectid = 'T-516' RETURN p`,
		// Lower bounds: the zero-length arm, a plain 1, and a raised floor.
		`MATCH p = (s)-[:E*0..2]->(t:Target) WHERE t.objectid = 'T-516' RETURN p`,
		`MATCH p = (s)-[:E*2..3]->(t:Target) WHERE t.objectid = 'T-516' RETURN p`,
		// Backward-arrow spelling: the path must still render in the order the
		// pattern was WRITTEN, regardless of which way either executor walked.
		`MATCH p = (t:Target)<-[:E*1..3]-(s) WHERE t.objectid = 'T-516' RETURN p`,
		// id() anchor on the far endpoint instead of an objectid.
		`MATCH (s)-[:E*1..5]->(x) WHERE id(x) = 6 RETURN s, x`,
		`MATCH p = (s)-[:E*1..5]->(x) WHERE id(x) = 6 RETURN p`,
		// Edge-kind disjunction, and the far endpoint narrowed by a plain
		// predicate rather than an anchor -- the shape that motivates this
		// whole route (a wide, unconstrained near side; a kind bitmap cut down
		// to a handful of nodes by a pushed single-symbol predicate).
		`MATCH p = (s)-[:E|F*1..3]->(t:Target) WHERE t.objectid ENDS WITH '-516' RETURN p`,
		`MATCH (s)-[:E*1..4]->(t:Target) WHERE t.objectid ENDS WITH '-516' RETURN s, t`,
	} {
		t.Run(query, func(t *testing.T) {
			assertVarLengthDirectionsAgree(t, snap, query)
		})
	}

	// A kind-constrained near endpoint, checked on every REACHED node. This
	// shape is no longer dispatcher-eligible (rankOf(fromNC).tier == tierKind,
	// not tierScan -- see varLengthReverseEligible's doc comment on why a
	// kind bitmap near side is excluded regardless of its size), so it is
	// compared via assertVarLengthRowsAgree (calls both executors directly)
	// rather than assertVarLengthDirectionsAgree, but the underlying
	// primitive expandVarLengthComponentReverse must still enforce FromSym's
	// constraint correctly on every node it reaches wherever it is invoked.
	t.Run("kind-constrained near endpoint (not dispatcher-eligible; primitive correctness only)", func(t *testing.T) {
		const query = `MATCH (s:Src)-[:E*0..3]->(t:Target) WHERE t.objectid = 'T-516' RETURN s, t`
		env := &Env{Snap: snap}
		part, step := varLengthPartAndStep(t, snap, query)
		if varLengthReverseEligible(env, part, step) {
			t.Fatalf("query %q: want reverse-ineligible (kind-constrained near side)", query)
		}
		assertVarLengthRowsAgree(t, snap, query)
	})
}

// TestVarLengthReverseMultiplicity pins COLLECT-visible multiplicity through
// the full pipeline, against hand-derived expectations: a reverse-seeded run
// must emit one row per distinct TRAIL, never one row per endpoint pair, both
// when the duplication comes from parallel edges and when it comes from two
// different routes through the graph.
func TestVarLengthReverseMultiplicity(t *testing.T) {
	snap := buildReverseEqualityFixture(t)

	pair := func(from, to uint64) string {
		t.Helper()
		f, _ := snap.Dense(from)
		to2, _ := snap.Dense(to)
		return rowKey([]OutVal{{Kind: OutNode, Node: f}, {Kind: OutNode, Node: to2}})
	}
	requireReversed := func(t *testing.T, query string) {
		t.Helper()
		part, step := varLengthPartAndStep(t, snap, query)
		if !varLengthReverseEligible(&Env{Snap: snap}, part, step) {
			t.Fatalf("query %q: want reverse-eligible", query)
		}
	}

	t.Run("parallel edges stay two rows", func(t *testing.T) {
		// The two matching Targets are nodes 5 and 6. Node 6's in-edges are
		// 3->6, 4->6, and the parallel pair 2->6/2->6; node 5's is 7->5.
		const query = `MATCH (s)-[:E*1..1]->(t:Target) WHERE t.objectid = 'T-516' RETURN s, t`
		requireReversed(t, query)
		assertVarLengthDirectionsAgree(t, snap, query)
		assertRowSet(t, mustExec(t, snap, query, generousBudget), []string{
			pair(2, 6), pair(2, 6), // parallel edges 104 and 105
			pair(3, 6),
			pair(4, 6),
			pair(5, 6),
			pair(7, 5),
		})
	})

	t.Run("two routes onto one pair stay two rows", func(t *testing.T) {
		// 1-3-6 and 1-4-6 are two distinct length-2 trails onto the SAME
		// endpoint pair. 3-3-6 is not among the answers: its first edge is the
		// 3->3 self-loop, which a two-edge trail may never begin with.
		const query = `MATCH (s)-[:E*2..2]->(t:Target) WHERE t.objectid = 'T-516' RETURN s, t`
		requireReversed(t, query)
		assertVarLengthDirectionsAgree(t, snap, query)
		assertRowSet(t, mustExec(t, snap, query, generousBudget), []string{
			pair(1, 6), pair(1, 6), // via node 3 and via node 4
			pair(7, 6), // 7->5 then 5->6
			pair(6, 5), // 6->7 then 7->5
		})
	})
}

// buildReverseSelfLoopFixture builds a fixture whose node 1 carries a
// self-loop of the pattern's own edge kind (E) -- since the self-loop hazard
// gate landed, a graph like this makes EVERY [:E*..] pattern decline, in
// both walk directions, because pg's seed-side is_cycle guard placement
// would otherwise be observable (see expand.go's SELF-LOOPS package-doc
// bullet). Node 3's F-kind edge exists so the kind-scoped half of the gate
// has something to prove: a pattern admitting only F must still serve.
func buildReverseSelfLoopFixture(t *testing.T) *snapshot.View {
	t.Helper()
	return buildExecSnapshot(t, revKindTable,
		[]execNodeSpec{
			{1, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "S-516"}},
			{2, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "S-516"}},
			{3, nil, nil},
		},
		[]execEdgeSpec{
			{200, 1, 1, revKindE},
			{201, 1, 2, revKindE},
			{202, 3, 1, revKindF},
		},
	)
}

// TestVarLengthSelfLoopHazardDeclinesBothDirections: a self-loop of an
// admitted kind anywhere in the view declines the var-length pattern in
// BOTH enumeration directions (the reverse-eligible spelling included),
// while a pattern admitting only a kind with no self-loops still serves.
func TestVarLengthSelfLoopHazardDeclinesBothDirections(t *testing.T) {
	snap := buildReverseSelfLoopFixture(t)

	// Reverse-eligible spelling (unconstrained near side, anchored far side):
	// the dispatcher would pick the backward route; the gate must decline
	// before either walker enumerates anything.
	const reversedQuery = `MATCH p = (s)-[:E*1..3]->(t:Target) WHERE t.objectid = 'S-516' RETURN p`
	if err := execExpectErr(t, snap, reversedQuery, generousBudget); !errors.Is(err, errUnsupportedStep) {
		t.Fatalf("query %q: err = %v, want errUnsupportedStep", reversedQuery, err)
	}

	// Forward spelling (constrained near side).
	const forwardQuery = `MATCH p = (t:Target)-[:E*1..3]->(x) WHERE t.objectid = 'S-516' RETURN p`
	if err := execExpectErr(t, snap, forwardQuery, generousBudget); !errors.Is(err, errUnsupportedStep) {
		t.Fatalf("query %q: err = %v, want errUnsupportedStep", forwardQuery, err)
	}

	// Kind-scoped: the only F edge is 3->1 (no F self-loop exists), so a
	// pattern admitting only F is unaffected by the E self-loop.
	assertPathSigs(t, snap, `MATCH p = (s)-[:F*1..3]->(t:Target) WHERE t.objectid = 'S-516' RETURN p`, 0,
		[]string{"N:3,1,|E:202,"})
}

// TestVarLengthReverseZeroLengthBindsSameNode pins the `*0..` arm under
// backward seeding: a zero-length row binds both pattern endpoints to the same
// node, and is admitted only when that node satisfies the NEAR endpoint's own
// constraint too -- the mirror image of the forward executor checking the FAR
// endpoint's constraint on each seed.
func TestVarLengthReverseZeroLengthBindsSameNode(t *testing.T) {
	snap := buildReverseEqualityFixture(t)

	t.Run("unconstrained near endpoint admits every seed", func(t *testing.T) {
		const query = `MATCH (s)-[:E*0..0]->(t:Target) WHERE t.objectid = 'T-516' RETURN s, t`
		assertVarLengthDirectionsAgree(t, snap, query)

		rs := mustExec(t, snap, query, generousBudget)
		n5, _ := snap.Dense(5)
		n6, _ := snap.Dense(6)
		assertRowSet(t, rs, []string{
			rowKey([]OutVal{{Kind: OutNode, Node: n5}, {Kind: OutNode, Node: n5}}),
			rowKey([]OutVal{{Kind: OutNode, Node: n6}, {Kind: OutNode, Node: n6}}),
		})
	})

	t.Run("kind-constrained near endpoint filters the seed", func(t *testing.T) {
		// Node 5 carries both Src and Target; node 6 carries only Target, so
		// only node 5 can satisfy `(s:Src)` at zero length.
		//
		// This shape is no longer dispatcher-eligible (a kind-constrained
		// near side is now categorically excluded -- see
		// varLengthReverseEligible's doc comment), so mustExec below actually
		// runs the ordinary forward route; assertVarLengthRowsAgree (not
		// assertVarLengthDirectionsAgree) is used to additionally pin that
		// expandVarLengthComponentReverse's own zero-length arm still checks
		// the near endpoint's constraint correctly wherever it is invoked.
		const query = `MATCH (s:Src)-[:E*0..0]->(t:Target) WHERE t.objectid = 'T-516' RETURN s, t`
		env := &Env{Snap: snap}
		part, step := varLengthPartAndStep(t, snap, query)
		if varLengthReverseEligible(env, part, step) {
			t.Fatalf("query %q: want reverse-ineligible (kind-constrained near side)", query)
		}
		assertVarLengthRowsAgree(t, snap, query)

		rs := mustExec(t, snap, query, generousBudget)
		n5, _ := snap.Dense(5)
		assertRowSet(t, rs, []string{
			rowKey([]OutVal{{Kind: OutNode, Node: n5}, {Kind: OutNode, Node: n5}}),
		})
	})
}

// TestVarLengthReversePatternOrderPath pins path assembly under backward
// discovery against hand-derived signatures for both arrow spellings: a
// forward arrow renders FromSym first, a backward arrow renders the
// pattern's own first-written endpoint first, in both cases regardless of the
// direction the executor actually walked.
func TestVarLengthReversePatternOrderPath(t *testing.T) {
	padNodes, padEdges := reverseEqualityPadding()
	snap := buildExecSnapshot(t, revKindTable,
		append([]execNodeSpec{
			{1, []snapshot.KindID{revKindSrc}, nil},
			{2, nil, nil},
			{3, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "P-516"}},
		}, padNodes...),
		append([]execEdgeSpec{
			{300, 1, 2, revKindE},
			{301, 2, 3, revKindE},
		}, padEdges...),
	)

	t.Run("forward arrow", func(t *testing.T) {
		const query = `MATCH p = (s)-[:E*1..3]->(t:Target) WHERE t.objectid = 'P-516' RETURN p`
		assertVarLengthDirectionsAgree(t, snap, query)
		assertPathSigs(t, snap, query, 0, []string{
			"N:2,3,|E:301,",
			"N:1,2,3,|E:300,301,",
		})
	})

	t.Run("backward arrow", func(t *testing.T) {
		const query = `MATCH p = (t:Target)<-[:E*1..3]-(s) WHERE t.objectid = 'P-516' RETURN p`
		assertVarLengthDirectionsAgree(t, snap, query)
		assertPathSigs(t, snap, query, 0, []string{
			"N:3,2,|E:301,",
			"N:3,2,1,|E:301,300,",
		})
	})
}

// TestVarLengthReverseMultiSeedDisjointComponents pins that every matching
// far-endpoint seed is expanded, not just the first: two structurally
// identical, mutually unreachable clusters each carry their own matching
// Target.
func TestVarLengthReverseMultiSeedDisjointComponents(t *testing.T) {
	padNodes, padEdges := reverseEqualityPadding()
	snap := buildExecSnapshot(t, revKindTable,
		append([]execNodeSpec{
			{1, nil, nil},
			{2, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "D1-516"}},
			{11, nil, nil},
			{12, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "D2-516"}},
			{21, nil, nil},
			{22, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "D3-999"}},
		}, padNodes...),
		append([]execEdgeSpec{
			{400, 1, 2, revKindE},
			{401, 11, 12, revKindE},
			{402, 21, 22, revKindE},
		}, padEdges...),
	)

	const query = `MATCH (s)-[:E*1..2]->(t:Target) WHERE t.objectid ENDS WITH '-516' RETURN s, t`
	assertVarLengthDirectionsAgree(t, snap, query)

	rs := mustExec(t, snap, query, generousBudget)
	n1, _ := snap.Dense(1)
	n2, _ := snap.Dense(2)
	n11, _ := snap.Dense(11)
	n12, _ := snap.Dense(12)
	assertRowSet(t, rs, []string{
		rowKey([]OutVal{{Kind: OutNode, Node: n1}, {Kind: OutNode, Node: n2}}),
		rowKey([]OutVal{{Kind: OutNode, Node: n11}, {Kind: OutNode, Node: n12}}),
	})
}

// --- eligibility ------------------------------------------------------------

// TestVarLengthReverseEligible pins the eligibility rule shape by shape: the
// far endpoint must genuinely narrow (ids/objectid/pushed predicates), the
// near endpoint must not (it is the wide side this route exists to avoid
// enumerating), and the far side's own candidate source must be strictly
// cheaper to enumerate than the near side's.
func TestVarLengthReverseEligible(t *testing.T) {
	snap := buildReverseEqualityFixture(t)
	env := &Env{Snap: snap}

	for _, tc := range []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "wide near side, objectid-anchored far side",
			query: `MATCH (s)-[:E*1..3]->(t:Target) WHERE t.objectid = 'T-516' RETURN s, t`,
			want:  true,
		},
		{
			name:  "wide near side, predicate-narrowed kind bitmap far side",
			query: `MATCH (s)-[:E*1..3]->(t:Target) WHERE t.objectid ENDS WITH '-516' RETURN s, t`,
			want:  true,
		},
		{
			// A kind bitmap near side is not "narrowing" in endpointNarrows'
			// sense, but it is still a bounded seed set whose members' own
			// IN-DEGREE this rule cannot see -- exactly the shape
			// TestVarLengthReverseRejectsHighInDegreeHub demonstrates can
			// cost far more to reverse than to walk forward. Only a genuine
			// full scan on the near side (no Kinds either) is eligible; see
			// varLengthReverseEligible's own doc for why tierScan
			// specifically is required, not just "does not narrow".
			name:  "kind-constrained near side: bounded seed set, unbounded fan-out risk",
			query: `MATCH (s:Src)-[:E*1..3]->(x) WHERE id(x) = 6 RETURN s, x`,
			want:  false,
		},
		{
			name:  "far side carries kinds only: nothing narrows it",
			query: `MATCH (s)-[:E*1..3]->(t:Target) RETURN s, t`,
			want:  false,
		},
		{
			name:  "far side wholly unconstrained",
			query: `MATCH (s:Src)-[:E*1..3]->(t) RETURN s, t`,
			want:  false,
		},
		{
			// Deliberately arranged so the far side WOULD win the cost
			// comparison on its own (an objectid anchor against a kind
			// bitmap): only the "the near side already narrows" guard can
			// reject this one, so it is what this case actually pins.
			name:  "near side narrows too: it is already the cheap side",
			query: `MATCH (s:Src)-[:E*1..3]->(t:Target) WHERE s.name STARTS WITH 'a' AND t.objectid = 'T-516' RETURN s, t`,
			want:  false,
		},
		{
			name:  "near side anchored by id(): already the cheap side",
			query: `MATCH (s)-[:E*1..3]->(t:Target) WHERE id(s) = 1 AND t.objectid = 'T-516' RETURN s, t`,
			want:  false,
		},
		{
			name:  "far side's candidate source is not cheaper than the near side's",
			query: `MATCH (s:Target)-[:E*1..3]->(t:Src) WHERE t.objectid ENDS WITH '-516' RETURN s, t`,
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			part, step := varLengthPartAndStep(t, snap, tc.query)
			if got := varLengthReverseEligible(env, part, step); got != tc.want {
				t.Fatalf("varLengthReverseEligible(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

// buildReverseHighInDegreeHubFixture builds the shape that defeats a cost
// rule based solely on candidate-source tier: the near side resolves through
// a KIND bitmap holding exactly one node (id 1, kind Src), which itself has
// zero out-edges, so the forward route's own expansion work is negligible
// once that one seed is found. The far side is anchored by id() -- not by
// kind -- on a hub (id 2) with fanIn direct predecessors, and, one level
// further back, fanIn more predecessors of THOSE (a second backward level),
// so a reverse walk seeded from the hub visits on the order of 2*fanIn edges
// before it can determine no trail reaches node 1 at all.
//
// A rule that only asks "does the near side narrow, and is the far side's
// candidate SOURCE tier cheaper" cannot see any of this: an id() lookup
// (tierID) is always a cheaper tier than a one-node kind bitmap (tierKind),
// regardless of that bitmap's one member's in-degree. Requiring the near
// side to additionally be a full scan (tierScan) is what rules this out --
// see varLengthReverseEligible's doc comment.
func buildReverseHighInDegreeHubFixture(t *testing.T, fanIn int) *snapshot.View {
	t.Helper()
	const hubID = 2
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{revKindSrc}, nil}, // near side: 1-node kind bitmap, zero out-edges
		{hubID, nil, nil},                       // far side: anchored by id(), not by kind
	}
	var edges []execEdgeSpec
	nextEdgeID := uint64(1)
	nextNodeID := uint64(hubID + 1)
	for i := 0; i < fanIn; i++ {
		p := nextNodeID
		nextNodeID++
		nodes = append(nodes, execNodeSpec{p, nil, nil})
		edges = append(edges, execEdgeSpec{nextEdgeID, p, hubID, revKindE}) // p -> hub
		nextEdgeID++

		q := nextNodeID
		nextNodeID++
		nodes = append(nodes, execNodeSpec{q, nil, nil})
		edges = append(edges, execEdgeSpec{nextEdgeID, q, p, revKindE}) // q -> p (second backward level)
		nextEdgeID++
	}
	return buildExecSnapshot(t, revKindTable, nodes, edges)
}

// TestVarLengthReverseRejectsHighInDegreeHub pins the gap a tier-only cost
// comparison leaves open: without also requiring the near side to be a full
// scan, this exact shape (a one-node KIND bitmap near side, an id()-anchored
// far side with hundreds of predecessors across two backward levels) would be
// judged eligible on tier alone -- id() (tierID) beats a kind bitmap
// (tierKind) regardless of size -- and reversed into a walk that visits
// roughly 2*fanIn edges to answer a query the forward route settles in
// essentially zero work (node 1's kind bitmap has exactly one member, which
// has no out-edges at all).
//
// This asserts three things: (1) the rule now refuses eligibility outright,
// (2) the magnitude of the regression it would otherwise reintroduce (the
// reverse primitive, called directly, costs orders of magnitude more than
// the forward one on this fixture), and (3) the actually-dispatched route
// (expandVarLengthComponent) spends exactly the forward baseline's work and
// returns exactly the forward baseline's rows -- not merely "less than
// reverse would have."
func TestVarLengthReverseRejectsHighInDegreeHub(t *testing.T) {
	const fanIn = 500
	snap := buildReverseHighInDegreeHubFixture(t, fanIn)
	env := &Env{Snap: snap}
	const query = `MATCH (s:Src)-[:E*1..3]->(x) WHERE id(x) = 2 RETURN s, x`

	part, step := varLengthPartAndStep(t, snap, query)
	if varLengthReverseEligible(env, part, step) {
		t.Fatalf("query %q: want reverse-ineligible (kind-bitmap near side, not scan-tier)", query)
	}

	// Demonstrate the magnitude of the regression this rejection guards
	// against: called directly (bypassing the now-corrected dispatcher), the
	// reverse primitive costs far more than the forward one on this fixture.
	fwdMeter := &workMeter{budget: generousBudget}
	fwdRows, err := expandVarLengthComponentForward(env, fwdMeter, part, step)
	if err != nil {
		t.Fatalf("expandVarLengthComponentForward: %v", err)
	}
	revMeter := &workMeter{budget: generousBudget}
	if _, err := expandVarLengthComponentReverse(env, revMeter, part, step); err != nil {
		t.Fatalf("expandVarLengthComponentReverse: %v", err)
	}
	if revMeter.work <= fwdMeter.work*100 {
		t.Fatalf("fixture does not demonstrate the regression: forward work = %d, reverse work = %d, want reverse to be at least two orders of magnitude larger", fwdMeter.work, revMeter.work)
	}
	t.Logf("forward work = %d, reverse work = %d (%.0fx)", fwdMeter.work, revMeter.work, float64(revMeter.work)/float64(fwdMeter.work))

	// The actually-dispatched route must equal the forward baseline exactly,
	// and rows must be correct: node 1 (the sole Src) has no out-edges, so no
	// trail exists at all.
	dispatchMeter := &workMeter{budget: generousBudget}
	dispatchRows, err := expandVarLengthComponent(env, dispatchMeter, part, step)
	if err != nil {
		t.Fatalf("expandVarLengthComponent: %v", err)
	}
	if dispatchMeter.work != fwdMeter.work {
		t.Fatalf("expandVarLengthComponent spent %d work, want exactly the forward baseline %d (the reverse route must not have been taken)", dispatchMeter.work, fwdMeter.work)
	}
	if len(dispatchRows) != len(fwdRows) {
		t.Fatalf("expandVarLengthComponent returned %d rows, want %d (matching the forward route)", len(dispatchRows), len(fwdRows))
	}
	if len(fwdRows) != 0 {
		t.Fatalf("fixture invariant broken: node 1 has no out-edges, so 0 trails should exist, got %d", len(fwdRows))
	}
}

// --- work reduction ---------------------------------------------------------

// buildReverseAsymmetricFixture builds the asymmetric shape this whole route
// exists for: many unconstrained nodes, exactly one of which reaches the
// single objectid-anchored Target. Forward expansion has to visit every one of
// them (an anchor scan plus one expansion per node); backward expansion starts
// from the one Target and walks a single edge.
//
// EVERY wide node carries an outgoing E edge, which matters: the edge-kind
// endpoint index (edgeHint) narrows a forward seed scan to the nodes that can
// actually start an admissible edge, and it would collapse this fixture to a
// single seed if only node 1 had one -- making the forward route optimal and
// leaving the test proving nothing. The chain 1->2->...->wide->2 gives them
// all one while keeping node 1 UNREACHABLE, so it stays the only node that
// reaches the Target and the answer stays a single row.
//
// What remains asymmetric, and what the reverse route is for, is the shape
// the hint cannot help with: a relationship kind that nearly every node
// carries, paired with a far side of exactly one node.
func buildReverseAsymmetricFixture(t *testing.T, wide int) *snapshot.View {
	t.Helper()
	nodes := make([]execNodeSpec, 0, wide+1)
	for i := 1; i <= wide; i++ {
		nodes = append(nodes, execNodeSpec{uint64(i), nil, nil})
	}
	nodes = append(nodes, execNodeSpec{9000, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "W-516"}})

	edges := []execEdgeSpec{{5000, 1, 9000, revKindE}}
	var eid uint64 = 6000
	for i := 1; i < wide; i++ {
		eid++
		edges = append(edges, execEdgeSpec{eid, uint64(i), uint64(i + 1), revKindE})
	}
	if wide > 1 {
		eid++
		edges = append(edges, execEdgeSpec{eid, uint64(wide), 2, revKindE})
	}
	return buildExecSnapshot(t, revKindTable, nodes, edges)
}

// TestVarLengthReverseSpendsFarLessWork calibrates the two directions against
// each other on that asymmetric fixture rather than hard-coding either figure:
// the reverse route must cost a small fraction of the forward route while
// producing the identical rows.
func TestVarLengthReverseSpendsFarLessWork(t *testing.T) {
	const wide = 400
	snap := buildReverseAsymmetricFixture(t, wide)
	const query = `MATCH (s)-[:E*1..3]->(t:Target) WHERE t.objectid = 'W-516' RETURN s, t`

	forward, reverse, forwardWork, reverseWork := runVarLengthBothWays(t, snap, query)
	if len(forward) != 1 || len(reverse) != 1 || forward[0] != reverse[0] {
		t.Fatalf("row multiset mismatch\nreverse: %v\nforward: %v", reverse, forward)
	}
	if forwardWork < 2*wide {
		t.Fatalf("forward work = %d, want >= %d (the forward route must actually scan the wide side)", forwardWork, 2*wide)
	}
	if reverseWork*10 >= forwardWork {
		t.Fatalf("reverse work = %d, want well under a tenth of the forward route's %d", reverseWork, forwardWork)
	}
}

// TestVarLengthReverseDeclinesOnBudget pins that a genuinely huge BACKWARD
// subgraph still declines cleanly rather than serving a truncated answer: an
// 8-node clique whose one objectid-anchored Target is reachable backward from
// everywhere, searched `*1..4` under a tiny work budget.
func TestVarLengthReverseDeclinesOnBudget(t *testing.T) {
	// Sized past the eligibility rule's near-side floor: below a couple of
	// dozen seeds the forward walk is cheap whatever its shape, so the route
	// would stay forward and this test would prove nothing about the reverse
	// walk's budget behaviour.
	const clique = 20
	nodes := []execNodeSpec{{1, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "C-516"}}}
	for i := uint64(2); i <= clique; i++ {
		nodes = append(nodes, execNodeSpec{i, nil, nil})
	}
	var edges []execEdgeSpec
	nextEdgeID := uint64(1)
	for u := uint64(1); u <= clique; u++ {
		for v := uint64(1); v <= clique; v++ {
			if u == v {
				continue
			}
			edges = append(edges, execEdgeSpec{nextEdgeID, u, v, revKindE})
			nextEdgeID++
		}
	}
	snap := buildExecSnapshot(t, revKindTable, nodes, edges)

	const query = `MATCH p = (s)-[:E*1..4]->(t:Target) WHERE t.objectid = 'C-516' RETURN p`
	env := &Env{Snap: snap}
	part, step := varLengthPartAndStep(t, snap, query)
	if !varLengthReverseEligible(env, part, step) {
		t.Fatalf("query %q: want reverse-eligible", query)
	}

	err := execExpectErr(t, snap, query, Budgets{MaxRows: 1_000_000, MaxWork: 1200})
	if !errors.Is(err, ErrBudget) {
		t.Fatalf("Execute() error = %v, want ErrBudget", err)
	}
}

// --- isolation from the chunked LIMIT driver --------------------------------

// TestVarLengthReverseUsedEvenWithALimit pins the SECOND half of the "all
// Domain Admins" regression fix, and deliberately REVERSES what this test
// asserted before.
//
// It used to pin the opposite rule: that a LIMIT handed the component to the
// chunked early-termination driver, which grows a caller-supplied chunk of
// FromSym anchor rows and therefore structurally cannot host a ToSym-seeded
// walk, so a LIMIT-ed query kept the forward route even where the unlimited
// executor would reverse. That was a deliberate, documented choice -- and
// measurement overturned it: on a 1M-node graph BloodHound's shipped "all
// Domain Admins" prebuilt (every shipped prebuilt carries LIMIT 1000) spent
// 20,031 work units through the chunked driver against 19 for the identical
// query without its LIMIT, for the same answer. The early termination a LIMIT
// buys is worth far less than the seeding choice it was costing, so
// limitEligibleComponent now declines exactly the shapes
// componentPrefersReverseSeeding identifies and lets the ordinary executor
// (and its reverse route) run instead; the LIMIT still applies at the
// pipeline's own SKIP/LIMIT pass.
//
// The assertion is the reverse decomposition's own figure, built from the
// BACKWARD trail primitive rather than from the component tail under test, so
// a baseline and a bug cannot move in lockstep. The fixture is sized past the
// driver's chunk size so a tail that ignored its anchor rows would show up as
// duplicated rows rather than merely a different work total.
func TestVarLengthReverseUsedEvenWithALimit(t *testing.T) {
	const wide = 2*limitChunk + 1
	snap := buildReverseAsymmetricFixture(t, wide)
	env := &Env{Snap: snap}

	const base = `MATCH (s)-[:E*1..3]->(t:Target) WHERE t.objectid = 'W-516' RETURN s, t`
	part, step := varLengthPartAndStep(t, snap, base)
	if !varLengthReverseEligible(env, part, step) {
		t.Fatalf("query %q: want reverse-eligible (otherwise this test proves nothing)", base)
	}

	// Reconstruct the REVERSE figure from its own pieces.
	rev := &workMeter{budget: generousBudget}
	seedIDs, err := resolveEndpointSet(env, rev, step.ToSym, part.Nodes[step.ToSym])
	if err != nil {
		t.Fatalf("resolveEndpointSet: %v", err)
	}
	var rows []*Row
	for _, id := range seedIDs {
		seed := NewRow()
		seed.SetNode(step.ToSym, id)
		grown, err := expandVarLengthTrailsToSeed(env, rev, step, part.Nodes[step.FromSym], seed, step.PathSym, 0)
		if err != nil {
			t.Fatalf("expandVarLengthTrailsToSeed: %v", err)
		}
		rows = append(rows, grown...)
	}
	rows, err = filterRows(env, rows, part.Where, nil)
	if err != nil {
		t.Fatalf("filterRows: %v", err)
	}
	wantWork := rev.work + int64(len(rows)) // one addFinalRow charge per surviving row

	q := planQuery(t, snap, base+` LIMIT 5`)
	meter := &workMeter{budget: generousBudget}
	rs, err := runQuery(env, q, meter)
	if err != nil {
		t.Fatalf("runQuery: %v", err)
	}
	if len(rs.Rows) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(rs.Rows), len(rows))
	}
	if meter.work != wantWork {
		t.Fatalf("meter.work = %d, want %d (a LIMIT must no longer force the forward route for a reverse-eligible component)", meter.work, wantWork)
	}
	// And the whole point: this must be far below a near-side full scan.
	if meter.work >= 2*int64(wide) {
		t.Fatalf("meter.work = %d, which is at or above the %d-unit near-side full scan the reverse route exists to avoid", meter.work, 2*wide)
	}
}

// buildDomainAdminsFixture mirrors a real BloodHound forest's shape at small
// scale, for the two regression tests below: many ordinary users all holding
// a Domain Users membership (the wide NEAR side), and a short nested-admin
// chain into a Domain Admins group whose objectid ends '-512' (the narrow FAR
// side). That asymmetry -- a handful of groups reachable backward against
// every user forward -- is exactly what the constrained-side route exists to
// exploit, and what BloodHound's shipped "all Domain Admins" prebuilt walks.
func buildDomainAdminsFixture(t *testing.T, users int) *snapshot.View {
	t.Helper()
	const (
		daUser     snapshot.KindID = 1
		daComputer snapshot.KindID = 2
		daGroup    snapshot.KindID = 3
		daMemberOf snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{
		daUser: "User", daComputer: "Computer", daGroup: "Group", daMemberOf: "MemberOf",
	}

	nodes := []execNodeSpec{
		{1, []snapshot.KindID{daGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-512"}},  // Domain Admins
		{2, []snapshot.KindID{daGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-1001"}}, // Server Admins
		{3, []snapshot.KindID{daGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-1002"}}, // IT Admins
		{4, []snapshot.KindID{daGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-1003"}}, // Helpdesk
		{5, []snapshot.KindID{daGroup}, map[string]any{"objectid": "S-1-5-21-1-1-1-513"}},  // Domain Users hub
	}
	edges := []execEdgeSpec{
		{100, 2, 1, daMemberOf},
		{101, 3, 2, daMemberOf},
		{102, 4, 3, daMemberOf},
	}
	edgeID := uint64(1000)
	for i := 0; i < users; i++ {
		id := uint64(1000 + i)
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{daUser}, map[string]any{
			"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", 10000+i),
		}})
		edges = append(edges, execEdgeSpec{edgeID, id, 5, daMemberOf})
		edgeID++
	}
	// One user actually reaches Domain Admins, through the nested chain.
	edges = append(edges, execEdgeSpec{edgeID, 1000, 4, daMemberOf})

	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestVarLengthReverseEligibleIgnoresKindOnlyPredicates pins the fix for the
// 84x "all Domain Admins" regression the 500k benchmark found.
//
// BloodHound's shipped prebuilt writes its near-endpoint type filter as a
// WHERE label disjunction -- `(a:User or a:Computer)` -- rather than as a
// pattern label. pushdown copies every single-symbol conjunct into
// NodeConstraint.Predicates, so that disjunction made endpointNarrows(fromNC)
// true and disqualified the constrained-side route, sending a query whose far
// endpoint resolves to a handful of groups down a full scan of every node in
// the graph instead. A kind test is exactly what endpointNarrows' own doc
// says must NOT count ("the kind bitmap IS the candidate source"), whether it
// is written in the pattern or in WHERE.
func TestVarLengthReverseEligibleIgnoresKindOnlyPredicates(t *testing.T) {
	snap := buildDomainAdminsFixture(t, 200)
	env := &Env{Snap: snap}

	for _, tc := range []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "kind disjunction in WHERE does not block the reverse route",
			query: `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE (a:User or a:Computer) AND t.objectid ENDS WITH '-512' RETURN p`,
			want:  true,
		},
		{
			name:  "single kind test in WHERE does not block it either",
			query: `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE a:User AND t.objectid ENDS WITH '-512' RETURN p`,
			want:  true,
		},
		{
			name:  "no near-side predicate at all stays eligible",
			query: `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE t.objectid ENDS WITH '-512' RETURN p`,
			want:  true,
		},
		{
			// A genuine PROPERTY predicate on the near side still blocks it:
			// that side really can be cut down before expanding, which is the
			// case the rule was written for.
			name:  "a property predicate on the near side still blocks it",
			query: `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE a.objectid ENDS WITH '-1234' AND t.objectid ENDS WITH '-512' RETURN p`,
			want:  false,
		},
		{
			// Mixed: a kind test AND a property predicate -- the property one
			// still counts, so the near side is still treated as narrowing.
			name:  "kind test mixed with a property predicate still blocks it",
			query: `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE a:User AND a.objectid ENDS WITH '-1234' AND t.objectid ENDS WITH '-512' RETURN p`,
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			part, step := varLengthPartAndStep(t, snap, tc.query)
			if got := varLengthReverseEligible(env, part, step); got != tc.want {
				t.Fatalf("varLengthReverseEligible = %v, want %v\nquery: %s", got, tc.want, tc.query)
			}
		})
	}
}

// TestLimitedQueryStillReverseSeeds is the second half of the "all Domain
// Admins" regression fix. Every BloodHound prebuilt carries `LIMIT 1000`, and
// a LIMIT used to hand the whole component to the chunked early-termination
// driver, which only ever scans the pattern's near endpoint forward and never
// takes the constrained-side route -- so the shipped query stayed on the full
// scan even once its kind test stopped disqualifying the reverse route.
//
// Measured rather than asserted structurally: the same query with and without
// its LIMIT must both produce the same rows, and the LIMIT-ed one must not
// cost dramatically more work than the unlimited one (it used to cost the
// whole graph).
func TestLimitedQueryStillReverseSeeds(t *testing.T) {
	snap := buildDomainAdminsFixture(t, 5000)

	const (
		limited   = `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE (a:User or a:Computer) AND t.objectid ENDS WITH '-512' RETURN p LIMIT 1000`
		unlimited = `MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE (a:User or a:Computer) AND t.objectid ENDS WITH '-512' RETURN p`
	)

	run := func(query string) (int64, int) {
		t.Helper()
		plan := planQuery(t, snap, query)
		meter := &workMeter{budget: Budgets{MaxRows: 100000, MaxWork: 1 << 34}}
		rs, err := runQuery(&Env{Snap: snap}, plan, meter)
		if err != nil {
			t.Fatalf("runQuery(%q): %v", query, err)
		}
		return meter.work, len(rs.Rows)
	}

	limWork, limRows := run(limited)
	unlWork, unlRows := run(unlimited)
	t.Logf("limited: work=%d rows=%d   unlimited: work=%d rows=%d", limWork, limRows, unlWork, unlRows)

	if limRows != unlRows {
		t.Fatalf("LIMIT changed the answer: %d rows vs %d unlimited", limRows, unlRows)
	}
	// The unlimited form reverse-seeds and touches a handful of nodes. Adding
	// a LIMIT larger than the result set must not turn that into a full scan;
	// allow generous headroom, but a whole-graph walk is ~4x the node count.
	if limWork > unlWork*4+1000 {
		t.Fatalf("adding a LIMIT cost %d work units against the unlimited form's %d -- the chunked driver is still refusing the constrained-side route", limWork, unlWork)
	}
}

// --- graph-covering kind on the near side -----------------------------------

// buildBaseLabelledFixture mirrors how BloodHound actually labels an AD
// graph: every node carries the universal `Base` kind IN ADDITION to its
// specific one, and the shipped prebuilts write their near endpoint as
// `(:Base)` rather than leaving it bare. Group is a small minority of nodes,
// User and Computer are large minorities, and Base covers everything -- so
// one fixture exercises all three sides of scanEquivalentNearSide's rule.
func buildBaseLabelledFixture(t *testing.T, users, computers, entraUsers int) *snapshot.View {
	t.Helper()
	const (
		blBase     snapshot.KindID = 1
		blUser     snapshot.KindID = 2
		blComputer snapshot.KindID = 3
		blGroup    snapshot.KindID = 4
		blAZBase   snapshot.KindID = 5
		blAZUser   snapshot.KindID = 6
		blMemberOf snapshot.KindID = 10
	)
	kinds := map[snapshot.KindID]string{
		blBase: "Base", blUser: "User", blComputer: "Computer",
		blGroup: "Group", blAZBase: "AZBase", blAZUser: "AZUser", blMemberOf: "MemberOf",
	}

	group := func(id uint64, rid string) execNodeSpec {
		return execNodeSpec{id, []snapshot.KindID{blBase, blGroup}, map[string]any{
			"objectid": "S-1-5-21-1-1-1-" + rid,
		}}
	}
	nodes := []execNodeSpec{
		group(1, "512"),  // Domain Admins
		group(2, "1001"), // Server Admins
		group(3, "1002"), // IT Admins
		group(4, "1003"), // Helpdesk
		group(5, "513"),  // Domain Users hub
	}
	edges := []execEdgeSpec{
		{100, 2, 1, blMemberOf},
		{101, 3, 2, blMemberOf},
		{102, 4, 3, blMemberOf},
	}

	edgeID := uint64(1000)
	nodeID := uint64(1000)
	add := func(n int, kind snapshot.KindID) {
		for i := 0; i < n; i++ {
			id := nodeID
			nodeID++
			nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{blBase, kind}, map[string]any{
				"objectid": fmt.Sprintf("S-1-5-21-1-1-1-%d", id),
			}})
			edges = append(edges, execEdgeSpec{edgeID, id, 5, blMemberOf}) // -> Domain Users
			edgeID++
		}
	}
	add(users, blUser)
	add(computers, blComputer)

	// entraUsers carry AZBase/AZUser and NOT Base -- the hybrid shape, where
	// `Base` is a large majority of the graph rather than the whole of it.
	for i := 0; i < entraUsers; i++ {
		id := nodeID
		nodeID++
		nodes = append(nodes, execNodeSpec{id, []snapshot.KindID{blAZBase, blAZUser}, map[string]any{
			"objectid": fmt.Sprintf("%08X-0000-0000-0000-%012X", i, i),
		}})
	}

	// Exactly one principal actually reaches Domain Admins, through the
	// nested chain -- so the answer is a handful of rows however it is found.
	edges = append(edges, execEdgeSpec{edgeID, 1000, 4, blMemberOf})

	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestVarLengthReverseEligibleWithGraphCoveringKindNearSide pins how a kind
// label on the NEAR side is priced: by how many nodes it actually covers,
// against what the far side costs, rather than by whether it happens to cover
// a majority of the graph.
//
// The majority rule this replaced refused the route for every near side that
// was merely large, which is where three shipped prebuilts lost to
// PostgreSQL. What survives it is the floor: a near side of a handful stays
// on the forward route, because the ratio between two small numbers decides
// nothing and forward is cheap there regardless.
func TestVarLengthReverseEligibleWithGraphCoveringKindNearSide(t *testing.T) {
	snap := buildBaseLabelledFixture(t, 60, 60, 0)
	env := &Env{Snap: snap}

	for _, tc := range []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "a graph-covering kind on the near side is scan-equivalent",
			query: `MATCH p = (:Base)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`,
			want:  true,
		},
		{
			name:  "a bare near side stays eligible, as before",
			query: `MATCH p = (a)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`,
			want:  true,
		},
		{
			// 60 of 125 nodes, against a far side of one or two. This used to
			// be REFUSED, on the rule that a near side had to cover a
			// majority of the graph before the route would switch -- and
			// that is the defect this case now guards the fix for. Measured
			// on the benchmark graph, shipped prebuilts lost to PostgreSQL
			// because 84,481 near-side nodes against twenty on the far side
			// is not a majority of a million. A near side that is large in
			// absolute terms and several times more expensive than the far
			// side is exactly when reversing pays.
			name:  "a large minority kind on the near side now reverses",
			query: `MATCH p = (:User)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`,
			want:  true,
		},
		{
			// Still refused, and for a reason that survives the rule change:
			// a handful of near-side seeds is cheap to walk forward whatever
			// their shape, so the ratio between two small numbers is not
			// worth acting on.
			name:  "a small kind on the near side still blocks it",
			query: `MATCH p = (:Group)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`,
			want:  false,
		},
		{
			// The far side must still genuinely narrow: a kinds-only far side
			// buys nothing, whatever the near side is.
			name:  "a kinds-only far side is still ineligible",
			query: `MATCH p = (:Base)-[:MemberOf*1..]->(g:Group) RETURN p`,
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			part, step := varLengthPartAndStep(t, snap, tc.query)
			if got := varLengthReverseEligible(env, part, step); got != tc.want {
				t.Fatalf("varLengthReverseEligible = %v, want %v\nquery: %s", got, tc.want, tc.query)
			}
		})
	}

	// The HYBRID shape is the rule's hardest case and the reason the
	// threshold is a majority rather than near-totality: with an Entra side
	// present, `Base` labels only the AD nodes -- a large majority, not the
	// whole graph -- and a first version of this rule (>=99% coverage)
	// refused the reverse route for it, which walked the shipped
	// Protected-Users prebuilt forward for ~3.0s on the hybrid benchmark
	// graph against ~20ms of far-side seeding.
	t.Run("a large-majority kind on a hybrid graph is scan-equivalent", func(t *testing.T) {
		hybrid := buildBaseLabelledFixture(t, 60, 60, 20) // Base = 125/145 ~ 86%
		env := &Env{Snap: hybrid}
		part, step := varLengthPartAndStep(t, hybrid,
			`MATCH p = (:Base)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p`)
		if !varLengthReverseEligible(env, part, step) {
			t.Fatal("a Base kind covering ~86% of a hybrid graph must be scan-equivalent")
		}
	})
}

// TestBaseLabelledPrebuiltDoesNotScan is the end-to-end half: the shipped
// prebuilt spelling (`(:Base)` near side, a suffix predicate on the far side,
// a LIMIT) must now cost what the bare-source spelling costs and return
// exactly the same rows -- not a full scan of the graph.
//
// Measured against the bare spelling rather than an absolute bar, because the
// two queries differ ONLY in how the near endpoint is written and are
// therefore required to be the same query: on the 500k benchmark graph this
// difference was 2551ms versus 50ms for an identical 108-node answer.
func TestBaseLabelledPrebuiltDoesNotScan(t *testing.T) {
	snap := buildBaseLabelledFixture(t, 2000, 2000, 600)
	nodeCount := snap.NodeCount()

	const (
		baseLabelled = `MATCH p = (:Base)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p LIMIT 1000`
		bareSource   = `MATCH p = (a)-[:MemberOf*1..]->(g:Group) WHERE g.objectid ENDS WITH '-512' RETURN p LIMIT 1000`
	)

	run := func(query string) (int64, int) {
		t.Helper()
		plan := planQuery(t, snap, query)
		meter := &workMeter{budget: Budgets{MaxRows: 100000, MaxWork: 1 << 34}}
		rs, err := runQuery(&Env{Snap: snap}, plan, meter)
		if err != nil {
			t.Fatalf("runQuery(%q): %v", query, err)
		}
		return meter.work, len(rs.Rows)
	}

	baseWork, baseRows := run(baseLabelled)
	bareWork, bareRows := run(bareSource)
	t.Logf("nodes=%d  (:Base): work=%d rows=%d   (a): work=%d rows=%d",
		nodeCount, baseWork, baseRows, bareWork, bareRows)

	if baseRows != bareRows {
		t.Fatalf("the two spellings answered differently: (:Base) gave %d rows, (a) gave %d", baseRows, bareRows)
	}
	if baseRows == 0 {
		t.Fatal("fixture invariant broken: one principal reaches Domain Admins, so the answer must be non-empty")
	}
	if baseWork > bareWork*2+100 {
		t.Fatalf("(:Base) spent %d work against the bare spelling's %d -- the graph-covering kind is still refusing the constrained-side route", baseWork, bareWork)
	}
	if int(baseWork) >= nodeCount {
		t.Fatalf("(:Base) spent %d work on a %d-node graph -- that is still a full scan", baseWork, nodeCount)
	}
}

// TestVarLengthReverseIgnoresNegatedNearPredicates pins that a NEGATED
// predicate on the near side does not count as narrowing.
//
// A negation is the complement of what it wraps, so its selectivity is the
// complement's: the more selective `x ENDS WITH '-512'` is, the less
// selective `NOT x ENDS WITH '-512'` is. Counting it as a narrowing kept the
// shipped "Nested groups within Tier Zero / High Value" prebuilt seeding from
// every Group in the graph -- its near side excludes two groups out of
// thousands while its far side is the handful tagged admin_tier_0 -- and it
// measured 3.9x slower than the database this engine replaces.
func TestVarLengthReverseIgnoresNegatedNearPredicates(t *testing.T) {
	snap := buildReverseEqualityFixture(t)
	env := &Env{Snap: snap}

	for _, tc := range []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "NOT ... ENDS WITH on the near side does not block it",
			query: `MATCH (s)-[:E*1..3]->(t:Target) WHERE NOT s.objectid ENDS WITH '-999' AND t.objectid = 'T-516' RETURN s, t`,
			want:  true,
		},
		{
			name:  "<> on the near side does not block it",
			query: `MATCH (s)-[:E*1..3]->(t:Target) WHERE s.objectid <> 'X' AND t.objectid = 'T-516' RETURN s, t`,
			want:  true,
		},
		{
			// A POSITIVE predicate still blocks it: that one really can be
			// selective, and rankOf cannot see by how much.
			name:  "a positive near-side predicate still blocks it",
			query: `MATCH (s)-[:E*1..3]->(t:Target) WHERE s.objectid ENDS WITH '-1' AND t.objectid = 'T-516' RETURN s, t`,
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			part, step := varLengthPartAndStep(t, snap, tc.query)
			if got := varLengthReverseEligible(env, part, step); got != tc.want {
				t.Fatalf("varLengthReverseEligible = %v, want %v\nquery: %s", got, tc.want, tc.query)
			}
			// Where the route IS taken, it must produce the forward answer.
			// (The helper only compares shapes the dispatcher reverses.)
			if tc.want {
				assertVarLengthDirectionsAgree(t, snap, tc.query)
			}
		})
	}
}

// TestVarLengthReverseHonoursTheLimit pins that the reverse walk stops at the
// query's LIMIT instead of enumerating every trail and letting the pipeline
// throw the rest away.
//
// It matters because this route is deliberately kept away from the chunked
// LIMIT driver (componentPrefersReverseSeeding), so if the walk does not
// honour the limit itself, nothing does until every trail exists. The fixture
// is a chain deep enough that the full enumeration is far larger than the
// limit, under a work budget that only the truncated walk can afford.
func TestVarLengthReverseHonoursTheLimit(t *testing.T) {
	// A caterpillar: a spine of Src nodes, each also pointed at by several
	// leaves, all reaching the single Target. Trails through it are many
	// times the limit below.
	const spine, leaves = 40, 6
	// Node ids must be staged ascending, so the Target comes first.
	const targetID = uint64(1)
	nodes := []execNodeSpec{{targetID, []snapshot.KindID{revKindTarget}, map[string]any{"objectid": "L-516"}}}
	var edges []execEdgeSpec
	var eid uint64
	next := func() uint64 { eid++; return eid }
	prev := targetID
	id := uint64(2)
	for i := 0; i < spine; i++ {
		s := id
		id++
		nodes = append(nodes, execNodeSpec{s, nil, nil})
		for j := 0; j < leaves; j++ {
			l := id
			id++
			nodes = append(nodes, execNodeSpec{l, nil, nil})
			edges = append(edges, execEdgeSpec{next(), l, s, revKindE})
		}
		edges = append(edges, execEdgeSpec{next(), s, prev, revKindE})
		prev = s
	}
	snap := buildExecSnapshot(t, revKindTable, nodes, edges)
	env := &Env{Snap: snap}

	const query = `MATCH (s)-[:E*1..15]->(t:Target) WHERE t.objectid = 'L-516' RETURN s, t LIMIT 5`
	part, step := varLengthPartAndStep(t, snap, query)
	if !varLengthReverseEligible(env, part, step) {
		t.Fatal("fixture must exercise the reverse route")
	}

	// Enumerating everything costs far more than this; only a walk that stops
	// at the limit fits.
	rs, err := Execute(env, planQuery(t, snap, query), Budgets{MaxRows: 1000, MaxWork: 200, MaxLiveRows: 10000})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(rs.Rows) != 5 {
		t.Fatalf("got %d rows, want 5", len(rs.Rows))
	}

	// Without a LIMIT the same query must still produce everything it always
	// did -- the cap is an optimization, not a new bound on the answer.
	full := mustExec(t, snap, `MATCH (s)-[:E*1..15]->(t:Target) WHERE t.objectid = 'L-516' RETURN s, t`,
		Budgets{MaxRows: 100000, MaxWork: 10000000, MaxLiveRows: 100000})
	if len(full.Rows) <= 5 {
		t.Fatalf("unlimited query returned %d rows; the fixture is not exercising truncation", len(full.Rows))
	}
}
