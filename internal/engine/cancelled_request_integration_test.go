// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
)

// TestServedReadsDeclineCancelledRequest: a read served from the replica
// runs under the request's context, as the PostgreSQL read it stands in for
// does. A request cancelled while its transaction was open still got the
// engine's answer, computed in full and returned with a nil error, while
// PostgreSQL answers the same context with its cancellation error. The
// engine now declines under a done context -- the Cypher interpreter and
// the builder counts alike -- so the wrapper hands the read to PostgreSQL
// and the caller sees PostgreSQL's outcome.
func TestServedReadsDeclineCancelledRequest(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"CtxUser", map[string]any{"name": "c0"}},
		{"CtxUser", map[string]any{"name": "c1"}},
	})
	const cypherText = `MATCH (n:CtxUser) RETURN n.name`
	kind := graph.StringKind("CtxUser")
	spec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{{Kinds: graph.Kinds{kind}}}}

	// A live request is served.
	live := context.Background()
	if err := pgDriver.ReadTransaction(live, func(tx graph.Transaction) error {
		result, served := eng.TryCypher(live, tx, cypherText, nil)
		if !served {
			t.Fatalf("TryCypher declined a live request")
		}
		rows := 0
		for result.Next() {
			rows++
		}
		result.Close()
		if rows != 2 {
			t.Fatalf("TryCypher served %d rows, want 2", rows)
		}
		if n, served := eng.TryNodeCount(live, spec); !served || n != 2 {
			t.Fatalf("TryNodeCount = (%d, %v), want (2, true)", n, served)
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadTransaction (live): %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
		cancel()

		if result, served := eng.TryCypher(ctx, tx, cypherText, nil); served {
			rows := 0
			for result.Next() {
				rows++
			}
			result.Close()
			t.Errorf("TryCypher served %d rows (err %v) to a cancelled request", rows, result.Error())
		}
		if n, served := eng.TryNodeCount(ctx, spec); served {
			t.Errorf("TryNodeCount served %d to a cancelled request", n)
		}

		// What the wrapper falls back to: PostgreSQL reports the cancellation.
		pgResult := tx.Query(cypherText, map[string]any{})
		for pgResult.Next() {
		}
		pgResult.Close()
		if err := pgResult.Error(); !errors.Is(err, context.Canceled) {
			t.Errorf("PostgreSQL Query under the cancelled context: err = %v, want context.Canceled", err)
		}
		if _, err := tx.Nodes().Filter(query.Kind(query.Node(), kind)).Count(); !errors.Is(err, context.Canceled) {
			t.Errorf("PostgreSQL Count under the cancelled context: err = %v, want context.Canceled", err)
		}
		return nil
	})
}
