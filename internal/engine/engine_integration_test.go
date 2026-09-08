// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// adcsFixturePath is the second root fixture the differential test loads
// alongside hydrateFixturePath (../../testdata/dawgs/traversal_shapes.json,
// declared in hydrate_integration_test.go -- this file shares its package).
const adcsFixturePath = "../../testdata/dawgs/adcs_fanout.json"

// adcsObjectID is testdata/dawgs/adcs_fanout.json's only node carrying an
// identifying property (node "n", a Group), used to exercise
// recognize.Endpoint.Criteria resolution.
const adcsObjectID = "S-1-5-21-2643190041-1319121918-239771340-513"

// testEngineLogger discards engine log output so differential test runs
// stay quiet; the log lines themselves aren't part of what this test
// verifies.
func testEngineLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// diffCase is one differential-test table entry: a recognize.PathQuery the
// engine must serve, with allowEmpty opting out of the "oracle found
// nothing" vacuousness guard for the cases that are supposed to have zero
// paths (disconnected pair, unknown id).
type diffCase struct {
	name       string
	pq         recognize.PathQuery
	allowEmpty bool
}

// TestTryAllShortestPathsDifferential is the milestone's core evidence: for
// a table of recognize.PathQuery shapes spanning both root fixtures, every
// resolveEndpoint branch (explicit IDs, Criteria, single- and multi-kind
// Kinds), both Modes, an edge-kind restriction, a disconnected pair, and an
// unknown-id endpoint, it checks the engine's TryAllShortestPaths output
// against an oracle built independently from the plain pg driver's
// FetchAllShortestPaths -- never calling into the engine's own resolution
// code -- per (start, end) pair, exactly as BloodHound's API queries
// PostgreSQL today (recognize.FromCriteria's documented shape).
func TestTryAllShortestPathsDifferential(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)
	for k, v := range graphtest.LoadDataset(t, pgDriver, adcsFixturePath) {
		ids[k] = v
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, triggerManual, time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	cases := []diffCase{
		{
			name: "chain, explicit ids, ModeAll",
			pq: recognize.PathQuery{
				Start: recognize.Endpoint{IDs: []graph.ID{ids["c0"]}},
				End:   recognize.Endpoint{IDs: []graph.ID{ids["c10"]}},
				Mode:  recognize.ModeAll,
			},
		},
		{
			name: "diamond, explicit ids, ModeAll",
			pq: recognize.PathQuery{
				Start: recognize.Endpoint{IDs: []graph.ID{ids["d0"]}},
				End:   recognize.Endpoint{IDs: []graph.ID{ids["d4"]}},
				Mode:  recognize.ModeAll,
			},
		},
		{
			name: "diamond, explicit ids, ModeOne",
			pq: recognize.PathQuery{
				Start: recognize.Endpoint{IDs: []graph.ID{ids["d0"]}},
				End:   recognize.Endpoint{IDs: []graph.ID{ids["d4"]}},
				Mode:  recognize.ModeOne,
			},
		},
		{
			// Exercises resolveKindsEndpoint's multi-kind intersection
			// (every traversal_shapes.json node carries two kinds; Kinds:
			// [TraversalNode, DiamondNode] must narrow to exactly d0..d4)
			// paired against an explicit-id end, multi-pair ModeAll.
			name: "multi-kind intersection start x explicit-id end, ModeAll",
			pq: recognize.PathQuery{
				Start: recognize.Endpoint{Kinds: graph.Kinds{graph.StringKind("TraversalNode"), graph.StringKind("DiamondNode")}},
				End:   recognize.Endpoint{IDs: []graph.ID{ids["d4"]}},
				Mode:  recognize.ModeAll,
			},
		},
		{
			// s0->s1->s2->s3 is "Allowed", s0->t1->t2->t3 is "Blocked":
			// restricting EdgeKinds to Allowed must find the s3 path.
			name: "edge-kind restricted to Allowed reaches s3",
			pq: recognize.PathQuery{
				Start:     recognize.Endpoint{IDs: []graph.ID{ids["s0"]}},
				End:       recognize.Endpoint{IDs: []graph.ID{ids["s3"]}},
				EdgeKinds: graph.Kinds{graph.StringKind("Allowed")},
				Mode:      recognize.ModeAll,
			},
		},
		{
			// The same pair restricted to Blocked-only edges must find
			// nothing: s0's only Blocked edge leads to t1, not s3.
			name:       "edge-kind restricted to Blocked cannot reach s3",
			allowEmpty: true,
			pq: recognize.PathQuery{
				Start:     recognize.Endpoint{IDs: []graph.ID{ids["s0"]}},
				End:       recognize.Endpoint{IDs: []graph.ID{ids["s3"]}},
				EdgeKinds: graph.Kinds{graph.StringKind("Blocked")},
				Mode:      recognize.ModeAll,
			},
		},
		{
			// Restricting to Blocked-only edges reaching t3 (the mirror of
			// the case above) must find the path Allowed-restriction blocks.
			name: "edge-kind restricted to Blocked reaches t3",
			pq: recognize.PathQuery{
				Start:     recognize.Endpoint{IDs: []graph.ID{ids["s0"]}},
				End:       recognize.Endpoint{IDs: []graph.ID{ids["t3"]}},
				EdgeKinds: graph.Kinds{graph.StringKind("Blocked")},
				Mode:      recognize.ModeAll,
			},
		},
		{
			name:       "disconnected pair",
			allowEmpty: true,
			pq: recognize.PathQuery{
				Start: recognize.Endpoint{IDs: []graph.ID{ids["x0"]}},
				End:   recognize.Endpoint{IDs: []graph.ID{ids["x1"]}},
				Mode:  recognize.ModeAll,
			},
		},
		{
			// An id Dense() can't resolve must produce an empty *served*
			// result (not a decline): matching PostgreSQL, which finds no
			// paths for a nonexistent node rather than erroring.
			name:       "unknown-id endpoint",
			allowEmpty: true,
			pq: recognize.PathQuery{
				Start: recognize.Endpoint{IDs: []graph.ID{graph.ID(999_999_999)}},
				End:   recognize.Endpoint{IDs: []graph.ID{ids["c5"]}},
				Mode:  recognize.ModeAll,
			},
		},
		{
			// adcs_fanout.json: Criteria-resolved start (the Group node
			// carrying adcsObjectID) x Kinds-resolved end (Domain) x a
			// restricted EdgeKinds set modeling an ESC1-shaped path
			// (n -Enroll-> ca, then either TrustedForNTAuth/NTAuthStoreFor
			// or IssuedSignedBy/RootCAFor to reach "domain"; "other-domain"
			// stays unreached since it only hangs off EnterpriseCAFor,
			// which isn't in the allowed set).
			name: "adcs: criteria start x kind end x restricted edge kinds",
			pq: recognize.PathQuery{
				Start: recognize.Endpoint{Criteria: query.And(
					query.KindIn(query.Node(), graph.StringKind("Group")),
					query.Equals(query.NodeProperty("objectid"), adcsObjectID),
				)},
				End: recognize.Endpoint{Kinds: graph.Kinds{graph.StringKind("Domain")}},
				EdgeKinds: graph.Kinds{
					graph.StringKind("MemberOf"),
					graph.StringKind("Enroll"),
					graph.StringKind("TrustedForNTAuth"),
					graph.StringKind("NTAuthStoreFor"),
					graph.StringKind("IssuedSignedBy"),
					graph.StringKind("RootCAFor"),
				},
				Mode: recognize.ModeAll,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			startIDs := oracleEndpointIDs(t, ctx, pgDriver, tc.pq.Start)
			endIDs := oracleEndpointIDs(t, ctx, pgDriver, tc.pq.End)

			oracleByPair := oraclePathsByPair(t, ctx, pgDriver, startIDs, endIDs, tc.pq.EdgeKinds)

			var (
				engineOut graph.PathSet
				served    bool
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				engineOut, served = eng.TryAllShortestPaths(ctx, tx, tc.pq)
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction: %v", err)
			}
			if !served {
				t.Fatalf("TryAllShortestPaths declined, want served")
			}

			switch tc.pq.Mode {
			case recognize.ModeOne:
				assertModeOne(t, oracleByPair, engineOut, tc.allowEmpty)
			default:
				var oracleAll graph.PathSet
				for _, paths := range oracleByPair {
					oracleAll = append(oracleAll, paths...)
				}
				assertSameSet(t, engineOut, oracleAll, tc.allowEmpty)
			}
		})
	}
}

// TestTryAllShortestPathsSurvivesWritesAndDeclinesInFallback replaces a test
// that asserted the opposite of what write-through guarantees: that a write
// landing between RebuildNow and a query made the engine decline.
//
// It no longer does, and must not: a write publishes itself into the replica
// (apply.go), so the snapshot a query captures is already current. The one
// condition that stops the path-serving path now is the engine being in
// fallback -- a write that could NOT be replayed -- which this test drives
// directly (the state is what serving reads; how it got set is Apply's
// business, exercised end-to-end by the root package's own suites).
func TestTryAllShortestPathsSurvivesWritesAndDeclinesInFallback(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, triggerManual, time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	pq := recognize.PathQuery{
		Start: recognize.Endpoint{IDs: []graph.ID{ids["c0"]}},
		End:   recognize.Endpoint{IDs: []graph.ID{ids["c1"]}},
		Mode:  recognize.ModeAll,
	}

	serves := func() bool {
		t.Helper()
		var served bool
		if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
			_, served = eng.TryAllShortestPaths(ctx, tx, pq)
			return nil
		}); err != nil {
			t.Fatalf("ReadTransaction: %v", err)
		}
		return served
	}

	if !serves() {
		t.Fatalf("TryAllShortestPaths declined right after RebuildNow, want served")
	}

	// A write, on its own, must not stop the engine serving.
	eng.NoteWrite(nil)
	if !serves() {
		t.Fatalf("TryAllShortestPaths declined after a write; write-through leaves the replica servable")
	}

	// Fallback does.
	eng.state.Store(stateFallback)
	if serves() {
		t.Fatalf("TryAllShortestPaths served while the engine is in fallback, want declined")
	}

	eng.state.Store(stateServing)
	if !serves() {
		t.Fatalf("TryAllShortestPaths declined after leaving fallback, want served")
	}
}

// TestTryAllShortestPathsDeclinesSelfEndpoint is a targeted regression test
// for a mismatch Task 14's randomized differential suite surfaced: querying
// a node against itself (Start and End both resolving to c0, which has an
// outgoing edge in traversal_shapes.json's chain) is a request PostgreSQL's
// own shortest-path implementation cannot serve at all -- it raises
// SQLSTATE 22023 ("shortest path endpoints must not resolve to the same
// node") from the very first BFS hop rather than returning an empty result
// (see traverse.SelfEndpointConflict's doc for the underlying SQL). Before
// this fix the engine had no equivalent guard: traverse.AllShortestPaths
// happily searched for a nontrivial cycle back to c0 and TryAllShortestPaths
// served whatever it found, silently answering a request the live system
// has always refused. The engine must now decline (reasonSelfEndpoint) so
// the caller falls back to PostgreSQL and gets the same refusal it always
// has.
//
// The second case checks the escape hatch stays open: with ExcludeSelf set
// (the shape a Cypher `n <> m` predicate produces), traverse's own
// strategyPairs/strategySmallSide already skip r==t pairs cleanly with no
// PostgreSQL-side error to match, so the engine must still serve -- here,
// trivially empty, since c0's only pair is excluded.
//
// The third case checks the complementary, degree-dependent half of
// reasonSelfEndpoint's condition: c10 (the chain's terminal node, with no
// outgoing edge of its own) queried against itself must still be served,
// not declined -- PostgreSQL's guard only fires once a matching root's
// outgoing-edge join actually produces a row, so a sink node querying
// itself never reaches it and completes normally with zero paths (no edge
// can originate a path back to c10 from itself, trivial or otherwise). An
// engine that declined unconditionally on any same-node query, without
// checking for an outgoing edge, would diverge from PostgreSQL here in the
// opposite direction: needlessly falling back for a request it could have
// served correctly itself.
func TestTryAllShortestPathsDeclinesSelfEndpoint(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	ids := graphtest.LoadDataset(t, pgDriver, hydrateFixturePath)

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, triggerManual, time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	selfPQ := recognize.PathQuery{
		Start: recognize.Endpoint{IDs: []graph.ID{ids["c0"]}},
		End:   recognize.Endpoint{IDs: []graph.ID{ids["c0"]}},
		Mode:  recognize.ModeAll,
	}

	var served bool
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		_, served = eng.TryAllShortestPaths(ctx, tx, selfPQ)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if served {
		t.Fatalf("TryAllShortestPaths served a same-node query (c0, c0), want declined -- PostgreSQL cannot serve this shape")
	}

	excludeSelfPQ := selfPQ
	excludeSelfPQ.ExcludeSelf = true

	var (
		excludeSelfOut    graph.PathSet
		excludeSelfServed bool
	)
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		excludeSelfOut, excludeSelfServed = eng.TryAllShortestPaths(ctx, tx, excludeSelfPQ)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if !excludeSelfServed {
		t.Fatalf("TryAllShortestPaths declined a same-node query with ExcludeSelf set, want served (traverse already skips r==t pairs)")
	}
	if len(excludeSelfOut) != 0 {
		t.Fatalf("TryAllShortestPaths(ExcludeSelf) for (c0, c0) returned %d paths, want 0", len(excludeSelfOut))
	}

	sinkSelfPQ := recognize.PathQuery{
		Start: recognize.Endpoint{IDs: []graph.ID{ids["c10"]}},
		End:   recognize.Endpoint{IDs: []graph.ID{ids["c10"]}},
		Mode:  recognize.ModeAll,
	}

	var (
		sinkSelfOut    graph.PathSet
		sinkSelfServed bool
	)
	if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		sinkSelfOut, sinkSelfServed = eng.TryAllShortestPaths(ctx, tx, sinkSelfPQ)
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction: %v", err)
	}
	if !sinkSelfServed {
		t.Fatalf("TryAllShortestPaths declined a same-node query on a sink node (c10, c10), want served -- PostgreSQL's guard never fires when the shared node has no outgoing edge")
	}
	if len(sinkSelfOut) != 0 {
		t.Fatalf("TryAllShortestPaths(c10, c10) returned %d paths, want 0", len(sinkSelfOut))
	}
}

// oracleEndpointIDs independently resolves ep to the database ids it
// matches, entirely through the plain dawgs query layer against pgDriver --
// never touching the engine's snapshot/bitmap machinery -- so it serves as
// ground truth for the oracle's per-pair queries and, via the differential
// comparison, as an independent check on the engine's own endpoint
// resolution.
func oracleEndpointIDs(t *testing.T, ctx context.Context, pgDriver *pg.Driver, ep recognize.Endpoint) []graph.ID {
	t.Helper()

	switch {
	case len(ep.IDs) > 0:
		return append([]graph.ID(nil), ep.IDs...)
	case ep.Criteria != nil:
		return oracleFetchIDs(t, ctx, pgDriver, ep.Criteria)
	case len(ep.Kinds) > 0:
		sets := make([][]graph.ID, len(ep.Kinds))
		for i, kind := range ep.Kinds {
			sets[i] = oracleFetchIDs(t, ctx, pgDriver, query.KindIn(query.Node(), kind))
		}
		return intersectIDs(sets)
	default:
		t.Fatalf("oracleEndpointIDs: this differential test harness does not support unconstrained endpoints")
		return nil
	}
}

// oracleFetchIDs runs criteria through a fresh read transaction on the plain
// pg driver and collects the matching database ids.
func oracleFetchIDs(t *testing.T, ctx context.Context, pgDriver *pg.Driver, criteria graph.Criteria) []graph.ID {
	t.Helper()

	var ids []graph.ID
	err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Filter(criteria).FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
			for id := range cursor.Chan() {
				ids = append(ids, id)
			}
			return cursor.Error()
		})
	})
	if err != nil {
		t.Fatalf("oracleFetchIDs: %v", err)
	}
	return ids
}

// intersectIDs returns the ids present in every one of sets, sorted
// ascending.
func intersectIDs(sets [][]graph.ID) []graph.ID {
	if len(sets) == 0 {
		return nil
	}

	counts := make(map[graph.ID]int)
	for _, set := range sets {
		seen := make(map[graph.ID]bool, len(set))
		for _, id := range set {
			if !seen[id] {
				seen[id] = true
				counts[id]++
			}
		}
	}

	var out []graph.ID
	for id, c := range counts {
		if c == len(sets) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// oraclePathsByPair is the differential test's oracle: for every (s, e)
// pair in the cartesian product of startIDs x endIDs, it runs the exact
// query shape BloodHound's API builds for FetchAllShortestPaths (see
// recognize.FromCriteria's doc) against the plain pg driver -- never the
// engine -- and returns the non-empty results keyed by pair, so ModeOne
// comparisons can check per-pair depth and path membership.
func oraclePathsByPair(t *testing.T, ctx context.Context, pgDriver *pg.Driver, startIDs, endIDs []graph.ID, edgeKinds graph.Kinds) map[[2]graph.ID]graph.PathSet {
	t.Helper()

	out := make(map[[2]graph.ID]graph.PathSet)

	err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		for _, s := range startIDs {
			for _, e := range endIDs {
				criteria := []graph.Criteria{
					query.Equals(query.StartID(), s),
					query.Equals(query.EndID(), e),
				}
				if len(edgeKinds) > 0 {
					criteria = append(criteria, query.KindIn(query.Relationship(), edgeKinds...))
				}

				var paths graph.PathSet
				err := tx.Relationships().Filter(query.And(criteria...)).FetchAllShortestPaths(func(cursor graph.Cursor[graph.Path]) error {
					for p := range cursor.Chan() {
						paths = append(paths, p)
					}
					return cursor.Error()
				})
				if err != nil {
					return fmt.Errorf("pair (%d, %d): %w", s, e, err)
				}
				if len(paths) > 0 {
					out[[2]graph.ID{s, e}] = paths
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("oraclePathsByPair: %v", err)
	}

	return out
}

// canonNode/canonEdge/canonPath give graph.Path a canonical, order-
// independent-once-sorted rendering for set comparison: node database ids
// and edge kind strings plus every node/edge's own property map, exactly as
// the task brief specifies.
type canonNode struct {
	ID    uint64         `json:"id"`
	Props map[string]any `json:"props"`
}
type canonEdge struct {
	Kind  string         `json:"kind"`
	Props map[string]any `json:"props"`
}
type canonPath struct {
	Nodes []canonNode `json:"nodes"`
	Edges []canonEdge `json:"edges"`
}

func canonicalize(p graph.Path) canonPath {
	cp := canonPath{Nodes: make([]canonNode, len(p.Nodes)), Edges: make([]canonEdge, len(p.Edges))}
	for i, n := range p.Nodes {
		cp.Nodes[i] = canonNode{ID: uint64(n.ID), Props: n.Properties.MapOrEmpty()}
	}
	for i, e := range p.Edges {
		cp.Edges[i] = canonEdge{Kind: e.Kind.String(), Props: e.Properties.MapOrEmpty()}
	}
	return cp
}

// renderSet canonicalizes and JSON-marshals every path in ps (encoding/json
// sorts map keys, so this is deterministic per path), then sorts the
// resulting strings so two path sets containing the same paths in different
// orders render identically.
func renderSet(ps graph.PathSet) []string {
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		b, err := json.Marshal(canonicalize(p))
		if err != nil {
			panic(fmt.Sprintf("engine: renderSet: marshal canonical path: %v", err))
		}
		out = append(out, string(b))
	}
	sort.Strings(out)
	return out
}

// assertSameSet requires got and want to render to exactly the same
// canonical path set (ModeAll's contract: every shortest path, no more, no
// fewer). Unless allowEmpty, an empty want fails outright -- a vacuous
// oracle means the test case itself is broken, not that the engine passed.
func assertSameSet(t *testing.T, got, want graph.PathSet, allowEmpty bool) {
	t.Helper()

	gotRendered := renderSet(got)
	wantRendered := renderSet(want)

	if !allowEmpty && len(wantRendered) == 0 {
		t.Fatalf("oracle returned no paths; the test case is vacuous")
	}
	if !reflect.DeepEqual(gotRendered, wantRendered) {
		t.Fatalf("path sets differ\n engine (%d): %v\n oracle (%d): %v", len(gotRendered), gotRendered, len(wantRendered), wantRendered)
	}
}

// assertModeOne checks ModeOne's weaker contract: exactly one served path
// per reachable pair, each at the oracle's minimum depth and drawn from the
// oracle's own shortest-path set for that pair (so every served edge is
// known-valid), with every reachable pair served exactly once.
func assertModeOne(t *testing.T, oracleByPair map[[2]graph.ID]graph.PathSet, engineOut graph.PathSet, allowEmpty bool) {
	t.Helper()

	if !allowEmpty && len(oracleByPair) == 0 {
		t.Fatalf("oracle found no reachable pairs; the test case is vacuous")
	}

	if len(engineOut) != len(oracleByPair) {
		t.Fatalf("ModeOne served %d paths, want %d (one per reachable pair)", len(engineOut), len(oracleByPair))
	}

	seenPairs := make(map[[2]graph.ID]bool, len(engineOut))

	for _, p := range engineOut {
		if len(p.Nodes) < 2 {
			t.Fatalf("ModeOne: served a zero-length path")
		}

		pair := [2]graph.ID{p.Nodes[0].ID, p.Nodes[len(p.Nodes)-1].ID}
		if seenPairs[pair] {
			t.Fatalf("ModeOne: pair (%d, %d) served more than once", pair[0], pair[1])
		}
		seenPairs[pair] = true

		oraclePaths, ok := oracleByPair[pair]
		if !ok {
			t.Fatalf("ModeOne: served a path for pair (%d, %d), which the oracle never reaches", pair[0], pair[1])
		}

		wantDepth := len(oraclePaths[0].Edges)
		if len(p.Edges) != wantDepth {
			t.Fatalf("ModeOne: pair (%d, %d) depth = %d, want the oracle's minimum %d", pair[0], pair[1], len(p.Edges), wantDepth)
		}

		rendered := renderSet(graph.PathSet{p})[0]
		if !slices.Contains(renderSet(oraclePaths), rendered) {
			t.Fatalf("ModeOne: served path for pair (%d, %d) is not among the oracle's shortest paths", pair[0], pair[1])
		}
	}

	for pair := range oracleByPair {
		if !seenPairs[pair] {
			t.Fatalf("ModeOne: reachable pair (%d, %d) was never served", pair[0], pair[1])
		}
	}
}
