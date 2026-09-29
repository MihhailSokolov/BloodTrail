// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherOverlayRegexAnchorMatchesOracle compares regex queries the
// engine may answer from its per-distinct-value regex anchor with
// PostgreSQL, over a View whose delta rewrote the anchored property to a
// value that is not a string. pg's `->>` renders an object or a list as its
// JSON text, which a pattern can match (`{"os": "Windows XP"}` contains XP),
// while the anchor's delta half used to recognise a non-string only by its
// postings -- where an object has no key and an empty list no element -- so
// the rewritten node dropped out of the candidates and the answer came back
// one row short. `os2` gets only string rewrites, so its anchor must still
// answer over the same overlay.
func TestTryCypherOverlayRegexAnchorMatchesOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	kind := graph.StringKind("OvlComputer")
	oses := []string{"Windows XP", "Windows 10", "Windows 7", "Linux"}
	var nodes []*graph.Node
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for i := 0; i < 400; i++ {
			props := graph.NewProperties().
				Set("name", fmt.Sprintf("H%d", i)).
				Set("operatingsystem", oses[i%len(oses)]).
				Set("os2", oses[i%len(oses)])
			n, err := tx.CreateNode(props, kind)
			if err != nil {
				return err
			}
			nodes = append(nodes, n)
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	// Each rewrite reaches the engine the way a committed write does: through
	// Apply, which reads the node back and publishes it as a delta segment
	// over the base. The rewrite itself is a plain jsonb merge, since dawgs'
	// own UpdateNode cannot write an empty list.
	for _, w := range []struct {
		node  int
		patch string
	}{
		{3, `{"operatingsystem": {"os": "Windows XP"}, "os2": "Windows XP SP3"}`},
		{5, `{"operatingsystem": [], "os2": "Windows 2000"}`},
		{7, `{"operatingsystem": [{"os": "Windows XP"}], "os2": "Linux"}`},
	} {
		n := nodes[w.node]
		if _, err := pool.Exec(ctx, `update node set properties = properties || $2::jsonb where id = $1`, int64(n.ID), w.patch); err != nil {
			t.Fatalf("rewrite node %d: %v", w.node, err)
		}
		scope := NewWriteScope()
		scope.Changes().RecordNodeID(n.ID)
		eng.Apply(ctx, scope)
	}
	if snap, serving := eng.serveState(); snap == nil || !snap.Overlay() || !serving {
		t.Fatalf("the rewrites did not leave the engine serving an overlay; this test would be vacuous")
	}

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (c:OvlComputer) WHERE c.operatingsystem =~ '.*XP.*' RETURN c`, false},
		{`MATCH (c:OvlComputer) WHERE c.operatingsystem =~ '.*' RETURN c`, false},
		{`MATCH (c:OvlComputer) WHERE c.operatingsystem =~ '.*\\[.*' RETURN c`, false},
		{`MATCH (c:OvlComputer) WHERE c.os2 =~ '.*XP.*' RETURN c`, true},
	})
}
