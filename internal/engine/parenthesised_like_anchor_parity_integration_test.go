// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
)

// TestTryCypherParenthesisedLikeAnchorMatchesOracle compares STARTS WITH /
// ENDS WITH / CONTAINS over a parenthesised property with PostgreSQL. The
// parentheses make the subject something other than a plain property, so
// dawgs hands the literal needle to LIKE unescaped: `(n.name) STARTS WITH
// 'a_b'` is `like 'a_b%'`, where _ matches any one character and aXb2
// matches too. The evaluator applies that LIKE, but the string anchor
// narrowed the candidates on the literal needle -- only names beginning
// with "a_b" -- and served a short answer; it now narrows on a literal run
// of the LIKE pattern. The same queries are asked again over an overlay,
// whose delta holds matches the base index never saw.
func TestTryCypherParenthesisedLikeAnchorMatchesOracle(t *testing.T) {
	nodes := []plannerShapeNode{
		{kinds: []string{"LikeUser"}, props: map[string]any{"name": "a_b1"}},
		{kinds: []string{"LikeUser"}, props: map[string]any{"name": "aXb2"}},
		{kinds: []string{"LikeUser"}, props: map[string]any{"name": "tail_x_z"}},
		{kinds: []string{"LikeUser"}, props: map[string]any{"name": "tailYxQz"}},
		{kinds: []string{"LikeUser"}, props: map[string]any{"name": "mid-q_q-1"}},
		{kinds: []string{"LikeUser"}, props: map[string]any{"name": "mid-qWq-2"}},
	}
	// Enough other users that narrowing on the property index is what the
	// executor chooses over the kind bitmap.
	for i := 0; i < 30; i++ {
		nodes = append(nodes, plannerShapeNode{kinds: []string{"LikeUser"}, props: map[string]any{"name": fmt.Sprintf("filler%02d", i)}})
	}
	pgDriver, pool, eng, ids := seedPlannerShapeGraph(t, nodes, nil)

	cases := []typedCase{
		{`MATCH (n:LikeUser) WHERE (n.name) STARTS WITH 'a_b' RETURN n`, true},
		{`MATCH (n:LikeUser) WHERE (n.name) STARTS WITH 'a%' RETURN n`, true},
		{`MATCH (n:LikeUser) WHERE (n.name) ENDS WITH 'x_z' RETURN n`, true},
		{`MATCH (n) WHERE (n.name) CONTAINS 'q_q' RETURN n`, true},
		// No literal run to narrow on, and an escaped _ that is literal.
		{`MATCH (n:LikeUser) WHERE (n.name) STARTS WITH '_Xb' RETURN n`, true},
		{`MATCH (n:LikeUser) WHERE (n.name) STARTS WITH 'a\\_b' RETURN n`, true},
		// A plain property gets its needle escaped: the literal match.
		{`MATCH (n:LikeUser) WHERE n.name STARTS WITH 'a_b' RETURN n`, true},
	}
	t.Run("base", func(t *testing.T) {
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})

	// Each write reaches the engine the way a committed write does: through
	// Apply, which reads the node back and publishes it as a delta segment
	// over the base.
	ctx := context.Background()
	var written []graph.ID
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, name := range []string{"a_b3", "aYb4", "tail2-xQz", "mid-qRq-3"} {
			n, err := tx.CreateNode(graph.NewProperties().Set("name", name), graph.StringKind("LikeUser"))
			if err != nil {
				return err
			}
			written = append(written, n.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("create delta nodes: %v", err)
	}
	renamed := ids[len(ids)-1]
	if _, err := pool.Exec(ctx, `update node set properties = properties || '{"name": "aZb5"}'::jsonb where id = $1`, int64(renamed)); err != nil {
		t.Fatalf("rename node: %v", err)
	}
	for _, id := range append(written, renamed) {
		scope := NewWriteScope()
		scope.Changes().RecordNodeID(id)
		eng.Apply(ctx, scope)
	}
	if snap, serving := eng.serveState(); snap == nil || !snap.Overlay() || !serving {
		t.Fatalf("the writes did not leave the engine serving an overlay; the overlay half would be vacuous")
	}
	t.Run("overlay", func(t *testing.T) {
		assertTypedCasesMatchOracle(t, pgDriver, eng, cases)
	})
}
