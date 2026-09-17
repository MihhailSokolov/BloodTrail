// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

const (
	ehKindWide   snapshot.KindID = 1
	ehKindNarrow snapshot.KindID = 2
	ehEdgeRare   snapshot.KindID = 3
	ehEdgeCommon snapshot.KindID = 4
)

// buildEdgeHintFixture: `wide` nodes of a wide kind, one node of a narrow
// kind, and exactly ONE edge of the rare kind joining them -- the shape of the
// shipped "All Global Administrators" prebuilt, where 84,481 AZBase nodes
// surround the single AZGlobalAdmin edge in the graph.
//
// Every wide node also carries a COMMON edge, so a seeding strategy that
// merely skipped edgeless nodes would still scan them all; only one that
// narrows by the STEP'S OWN relationship kind gets down to one seed.
func buildEdgeHintFixture(t *testing.T, wide int) *snapshot.View {
	t.Helper()
	kinds := map[snapshot.KindID]string{
		ehKindWide: "Wide", ehKindNarrow: "Narrow",
		ehEdgeRare: "Rare", ehEdgeCommon: "Common",
	}
	nodes := make([]execNodeSpec, 0, wide+1)
	for i := 1; i <= wide; i++ {
		nodes = append(nodes, execNodeSpec{uint64(i), []snapshot.KindID{ehKindWide},
			map[string]any{"name": fmt.Sprintf("W%d", i)}})
	}
	nodes = append(nodes, execNodeSpec{9000, []snapshot.KindID{ehKindNarrow},
		map[string]any{"name": "TARGET"}})

	var edges []execEdgeSpec
	var eid uint64
	next := func() uint64 { eid++; return eid }
	edges = append(edges, execEdgeSpec{next(), 1, 9000, ehEdgeRare})
	for i := 1; i <= wide; i++ {
		edges = append(edges, execEdgeSpec{next(), uint64(i), uint64(i%wide + 1), ehEdgeCommon})
	}
	return buildExecSnapshot(t, kinds, nodes, edges)
}

// TestEdgeKindHintNarrowsTheSeedScan pins the access path PostgreSQL has and
// this engine did not: an index on relationship kind.
//
// Without it, `(:Wide)-[:Rare*1..]->(:Narrow)` seeds its walk from every node
// carrying the wide kind, because the only candidate sources the executor
// knew about were node kinds, ids, objectids and property values. On the
// benchmark graph that is 84,481 AZBase nodes enumerated to reach ONE
// AZGlobalAdmin edge, and it measured 2.7x slower than the database this
// engine replaces.
//
// The budget here is far too small to have enumerated the wide kind, so a
// passing run is proof the seed scan never touched it.
func TestEdgeKindHintNarrowsTheSeedScan(t *testing.T) {
	const wide = 4000
	snap := buildEdgeHintFixture(t, wide)

	// Enough for a handful of candidates and the single match; nowhere near
	// enough for 4000 seeds, each of which would cost work of its own.
	tight := Budgets{MaxRows: 100, MaxWork: 200, MaxLiveRows: 1000}

	for _, tc := range []struct {
		name  string
		query string
	}{
		{"var-length", `MATCH (a:Wide)-[:Rare*1..]->(b:Narrow) RETURN a, b`},
		{"var-length named path", `MATCH p = (a:Wide)-[:Rare*1..]->(b:Narrow) RETURN p`},
		{"fixed length", `MATCH (a:Wide)-[:Rare]->(b:Narrow) RETURN a, b`},
		{"fixed length named path", `MATCH p = (a:Wide)-[:Rare]->(b:Narrow) RETURN p`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, tight)
			if len(rs.Rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rs.Rows))
			}
		})
	}
}

// TestEdgeKindHintKeepsTheSameAnswer is the correctness half: narrowing the
// seed scan must not change which rows come out, for any of the shapes where
// the hint applies or is deliberately refused.
func TestEdgeKindHintKeepsTheSameAnswer(t *testing.T) {
	const wide = 60
	snap := buildEdgeHintFixture(t, wide)
	loose := Budgets{MaxRows: 100000, MaxWork: 10000000, MaxLiveRows: 100000}

	for _, tc := range []struct {
		name  string
		query string
		want  int
	}{
		{"rare kind, one match", `MATCH (a:Wide)-[:Rare*1..]->(b:Narrow) RETURN a, b`, 1},
		{"common kind still matches everything", `MATCH (a:Wide)-[:Common]->(b:Wide) RETURN a, b`, wide},
		{
			// *0.. is satisfied by a zero-length match that traverses no
			// edge, so a node with NO admissible edge still matches and the
			// hint must not be applied. Every wide node matches itself.
			name:  "a zero-length range must not be narrowed",
			query: `MATCH (a:Wide)-[:Rare*0..]->(b:Wide) RETURN a, b`,
			want:  wide,
		},
		{
			// Undirected, anchored on the endpoint that has only an INCOMING
			// Rare edge. Applying the outgoing endpoint set here would
			// resolve `a` to nothing and lose the match entirely, so this is
			// the case that proves the refusal rather than merely exercising
			// it.
			name:  "an undirected step is not narrowed",
			query: `MATCH (a:Narrow)-[:Rare]-(b:Wide) RETURN a, b`,
			want:  1,
		},
		{
			// No declared kind: every edge qualifies, nothing to narrow by.
			name:  "an untyped relationship is not narrowed",
			query: `MATCH (a:Wide)-[]->(b:Narrow) RETURN a, b`,
			want:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := mustExec(t, snap, tc.query, loose)
			if len(rs.Rows) != tc.want {
				t.Fatalf("got %d rows, want %d", len(rs.Rows), tc.want)
			}
		})
	}
}

// TestEdgeKindHintRankedConsistently pins the invariant that made the
// dispatch test fail while this was being written: rankOf and the anchor scan
// must price the same candidate source, or two seeding paths that should be
// interchangeable spend different work for identical rows.
func TestEdgeKindHintRankedConsistently(t *testing.T) {
	const wide = 500
	snap := buildEdgeHintFixture(t, wide)
	env := &Env{Snap: snap}

	q := planQuery(t, snap, `MATCH (a:Wide)-[:Rare*1..]->(b:Narrow) RETURN a, b`)
	part := &q.Parts[0]
	step := &part.Chains[0]
	nc := part.Nodes[step.FromSym]

	hint := newEdgeHint(env, step, step.FromSym)
	if !hint.usable() {
		t.Fatal("the hint must resolve for a directed, kinded, hop-mandating step")
	}
	if hint.size() != 1 {
		t.Fatalf("hint resolved %d candidates, want 1", hint.size())
	}
	if !edgeHintPreferred(env, nc, hint) {
		t.Fatalf("a 1-node hint must beat the %d-node kind bitmap",
			smallestKindBitmap(env, nc.Kinds).Count())
	}
	plain, hinted := rankOf(env, nc), rankOfHinted(env, nc, hint)
	if !hinted.better(plain) {
		t.Fatalf("rankOfHinted{tier:%d size:%d} must beat rankOf{tier:%d size:%d}",
			hinted.tier, hinted.size, plain.tier, plain.size)
	}
}
