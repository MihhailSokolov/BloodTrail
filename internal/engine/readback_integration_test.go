// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestReadBack seeds a small graph, records a ChangeSet naming a mix of
// present and absent keys across every dimension readBack resolves, and
// asserts the returned readbackResult matches exactly what PostgreSQL holds.
//
// Seed shape:
//   - n1, n2, n3: RBNode-kind nodes; n1 carries objectid "n1-oid" (a single
//     match), n2 and n3 carry no objectid.
//   - dup1, dup2: RBNode-kind nodes both carrying objectid "dup-oid" -- the
//     duplicate-objectid pair readBackNodesByObjectID must return both
//     halves of.
//   - edge1 = n1 -> n2 (RBEdge), edge2 = n2 -> n3 (RBEdge), edge3 = n1 ->
//     dup1 (RBEdge) -- edge3 backs exactly one half of the objectid-triple
//     cross product below; the other half (n1 -> dup2) is deliberately
//     left unseeded.
//   - runtimeNode: created with RuntimeKind only AFTER the engine's
//     snapshot is built (RebuildNow), so RuntimeKind is absent from that
//     snapshot's own kind table -- the "kind you register at runtime" case
//     readBack's resolvedKinds must resolve.
//
// ChangeSet:
//   - NodeIDs: runtimeNode.ID, n2.ID, and a fabricated id -> two present,
//     one absent.
//   - NodeObjectIDs: "dup-oid" (matches dup1+dup2), "missing-oid" (matches
//     nothing), "n1-oid" (matches n1, needed to resolve the objectid-triple
//     below -- mirroring write_observer.go's own
//     RecordEdgeTripleByObjectID+RecordNodeObjectID pairing).
//   - EdgeIDs: edge1.ID and a fabricated id -> one present, one absent.
//   - EdgeTriples: (n2, n3, RBEdge) -- resolvable, matches edge2; (n1, n2,
//     NeverRegisteredKind) -- a kind never asserted anywhere, so it can't
//     resolve to a KindID at all.
//   - EdgeTriplesByObjectID: ("n1-oid", "missing-oid", RBEdge) -- the end
//     endpoint's objectid matches no node, so the whole triple is
//     unresolvable. ("n1-oid", "dup-oid", RBEdge), recorded twice
//     (dedup must hold) -- both endpoints resolve, "n1-oid" to the single
//     node n1 and "dup-oid" to the pair dup1/dup2, so this expands into the
//     id-pair cross product {(n1,dup1), (n1,dup2)}: (n1,dup1) matches
//     edge3 and must appear in result.edges, while (n1,dup2) has no
//     backing edge and must appear in absentTriples with RBEdge's real,
//     resolved KindID (not the unresolvedTripleKind sentinel, since RBEdge
//     itself resolves fine -- only the specific triple doesn't exist).
func TestReadBack(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	rbNodeKind := graph.StringKind("RBNode")
	rbEdgeKind := graph.StringKind("RBEdge")
	neverRegisteredKind := graph.StringKind("NeverRegisteredKind")
	// Suffixed with this run's own nanosecond timestamp, deliberately: the
	// `kind` table is global and permanent (WipeGraph truncates nodes and
	// edges, never kinds), so a fixed name would already be registered from
	// an earlier run of this suite and would therefore be present in the
	// snapshot's own kind table BEFORE the assertion below re-registers it
	// -- making resolvedKinds legitimately empty and this test pass only
	// ever on a pristine database.
	runtimeKind := graph.StringKind(fmt.Sprintf("RuntimeKind%d", time.Now().UnixNano()))

	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{rbNodeKind, rbEdgeKind}); err != nil {
		t.Fatalf("assert seed kinds: %v", err)
	}

	var n1, n2, n3, dup1, dup2 graph.ID
	var edge1, edge2, edge3 graph.ID

	err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		node1, err := tx.CreateNode(graph.NewProperties().Set("objectid", "n1-oid"), rbNodeKind)
		if err != nil {
			return err
		}
		n1 = node1.ID

		node2, err := tx.CreateNode(graph.NewProperties(), rbNodeKind)
		if err != nil {
			return err
		}
		n2 = node2.ID

		node3, err := tx.CreateNode(graph.NewProperties(), rbNodeKind)
		if err != nil {
			return err
		}
		n3 = node3.ID

		d1, err := tx.CreateNode(graph.NewProperties().Set("objectid", "dup-oid"), rbNodeKind)
		if err != nil {
			return err
		}
		dup1 = d1.ID

		d2, err := tx.CreateNode(graph.NewProperties().Set("objectid", "dup-oid"), rbNodeKind)
		if err != nil {
			return err
		}
		dup2 = d2.ID

		e1, err := tx.CreateRelationshipByIDs(n1, n2, rbEdgeKind, graph.NewProperties())
		if err != nil {
			return err
		}
		edge1 = e1.ID

		e2, err := tx.CreateRelationshipByIDs(n2, n3, rbEdgeKind, graph.NewProperties())
		if err != nil {
			return err
		}
		edge2 = e2.ID

		e3, err := tx.CreateRelationshipByIDs(n1, dup1, rbEdgeKind, graph.NewProperties())
		if err != nil {
			return err
		}
		edge3 = e3.ID

		return nil
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	e := New(pgDriver, pool, Config{})
	if err := e.RebuildNow(ctx, "test", time.Time{}); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// RuntimeKind is asserted and used only now, after the snapshot above
	// was already built -- it must be unknown to that snapshot's own kind
	// table.
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{runtimeKind}); err != nil {
		t.Fatalf("assert runtime kind: %v", err)
	}

	var runtimeNode graph.ID
	err = pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		node, err := tx.CreateNode(graph.NewProperties(), runtimeKind)
		if err != nil {
			return err
		}
		runtimeNode = node.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed runtime-kind node: %v", err)
	}

	runtimeKindID, err := pgDriver.KindMapper().MapKind(ctx, runtimeKind)
	if err != nil {
		t.Fatalf("map runtime kind: %v", err)
	}

	rbEdgeKindID, err := pgDriver.KindMapper().MapKind(ctx, rbEdgeKind)
	if err != nil {
		t.Fatalf("map RBEdge kind: %v", err)
	}

	fabricatedNodeID := graph.ID(uint64(n3) + 1_000_000)
	fabricatedEdgeID := graph.ID(uint64(edge2) + 1_000_000)

	cs := &ChangeSet{}
	cs.RecordNodeID(runtimeNode)
	cs.RecordNodeID(n2)
	cs.RecordNodeID(fabricatedNodeID)

	cs.RecordNodeObjectID("dup-oid")
	cs.RecordNodeObjectID("missing-oid")
	cs.RecordNodeObjectID("n1-oid")

	cs.RecordEdgeID(edge1)
	cs.RecordEdgeID(fabricatedEdgeID)

	cs.RecordEdgeTriple(n2, n3, rbEdgeKind)
	cs.RecordEdgeTriple(n1, n2, neverRegisteredKind)

	cs.RecordEdgeTripleByObjectID("n1-oid", "missing-oid", rbEdgeKind)
	cs.RecordEdgeTripleByObjectID("n1-oid", "dup-oid", rbEdgeKind)
	cs.RecordEdgeTripleByObjectID("n1-oid", "dup-oid", rbEdgeKind) // recorded twice: dedup must hold

	got, err := e.readBack(ctx, cs)
	if err != nil {
		t.Fatalf("readBack: %v", err)
	}

	// --- nodes ---
	wantNodeIDs := map[uint64]struct{}{
		uint64(n1): {}, uint64(n2): {}, uint64(runtimeNode): {},
		uint64(dup1): {}, uint64(dup2): {},
	}
	if len(got.nodes) != len(wantNodeIDs) {
		t.Fatalf("nodes = %d entries, want %d: %+v", len(got.nodes), len(wantNodeIDs), got.nodes)
	}
	for _, ns := range got.nodes {
		if _, ok := wantNodeIDs[ns.id]; !ok {
			t.Fatalf("nodes: unexpected id %d", ns.id)
		}
		if len(ns.propsJSON) == 0 {
			t.Fatalf("nodes: id %d has empty propsJSON", ns.id)
		}
	}

	if len(got.absentNodeIDs) != 1 || got.absentNodeIDs[0] != uint64(fabricatedNodeID) {
		t.Fatalf("absentNodeIDs = %v, want [%d]", got.absentNodeIDs, uint64(fabricatedNodeID))
	}

	if len(got.absentObjectIDs) != 1 || got.absentObjectIDs[0] != "missing-oid" {
		t.Fatalf("absentObjectIDs = %v, want [missing-oid]", got.absentObjectIDs)
	}

	// --- edges ---
	wantEdgeIDs := map[uint64]struct {
		start, end uint64
		kindID     int16
	}{
		uint64(edge1): {uint64(n1), uint64(n2), rbEdgeKindID},
		uint64(edge2): {uint64(n2), uint64(n3), rbEdgeKindID},
		uint64(edge3): {uint64(n1), uint64(dup1), rbEdgeKindID},
	}
	if len(got.edges) != len(wantEdgeIDs) {
		t.Fatalf("edges = %d entries, want %d: %+v", len(got.edges), len(wantEdgeIDs), got.edges)
	}
	for _, es := range got.edges {
		want, ok := wantEdgeIDs[es.id]
		if !ok {
			t.Fatalf("edges: unexpected id %d", es.id)
		}
		if es.start != want.start || es.end != want.end || es.kindID != want.kindID {
			t.Fatalf("edges: id %d = (%d -> %d, kind %d), want (%d -> %d, kind %d)",
				es.id, es.start, es.end, es.kindID, want.start, want.end, want.kindID)
		}
	}

	if len(got.absentEdgeIDs) != 1 || got.absentEdgeIDs[0] != uint64(fabricatedEdgeID) {
		t.Fatalf("absentEdgeIDs = %v, want [%d]", got.absentEdgeIDs, uint64(fabricatedEdgeID))
	}

	// (n1, n2, NeverRegisteredKind) can't resolve its kind at all, so it's
	// reported under the unresolvedTripleKind sentinel; (n1, dup2, RBEdge)
	// is the unbacked half of the objectid-triple cross product, reported
	// with RBEdge's real, resolved KindID. Recording ("n1-oid", "dup-oid",
	// RBEdge) twice above must not duplicate this second entry.
	wantAbsentTriples := map[tripleKey]struct{}{
		{start: uint64(n1), end: uint64(n2), kindID: unresolvedTripleKind}: {},
		{start: uint64(n1), end: uint64(dup2), kindID: rbEdgeKindID}:       {},
	}
	if len(got.absentTriples) != len(wantAbsentTriples) {
		t.Fatalf("absentTriples = %+v, want %d entries: %+v", got.absentTriples, len(wantAbsentTriples), wantAbsentTriples)
	}
	for _, tk := range got.absentTriples {
		if _, ok := wantAbsentTriples[tk]; !ok {
			t.Fatalf("absentTriples: unexpected entry %+v", tk)
		}
	}

	// --- resolvedKinds ---
	if len(got.resolvedKinds) != 1 {
		t.Fatalf("resolvedKinds = %+v, want exactly 1 entry", got.resolvedKinds)
	}
	if name, ok := got.resolvedKinds[runtimeKindID]; !ok || name != runtimeKind.String() {
		t.Fatalf("resolvedKinds[%d] = %q, %v, want %q, true", runtimeKindID, name, ok, runtimeKind.String())
	}
}

// TestReadBackEmptyChangeSet checks that an empty ChangeSet -- naming
// nothing at all -- produces an empty readbackResult without issuing any
// query beyond the (unavoidable) unknown-kind check, which also finds
// nothing to resolve since no node or edge was read back.
func TestReadBackEmptyChangeSet(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	e := New(pgDriver, pool, Config{})

	got, err := e.readBack(ctx, &ChangeSet{})
	if err != nil {
		t.Fatalf("readBack: %v", err)
	}

	if len(got.nodes) != 0 || len(got.absentNodeIDs) != 0 || len(got.absentObjectIDs) != 0 {
		t.Fatalf("readBack(empty ChangeSet): non-empty node results: %+v", got)
	}
	if len(got.edges) != 0 || len(got.absentEdgeIDs) != 0 || len(got.absentTriples) != 0 {
		t.Fatalf("readBack(empty ChangeSet): non-empty edge results: %+v", got)
	}
	if len(got.resolvedKinds) != 0 {
		t.Fatalf("readBack(empty ChangeSet): resolvedKinds = %+v, want empty", got.resolvedKinds)
	}
}
