// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
)

// TestBuilderCountsDeclineDoneContext: TryNodeCount and TryRelCount compute
// their answer inline -- a bitmap population, a relationship scan -- and
// answered whatever the request's context said, where PostgreSQL answers a
// cancelled or expired context with its error. Both now decline a done
// context on entry, so the wrapper hands the call to PostgreSQL and the
// caller sees PostgreSQL's outcome.
func TestBuilderCountsDeclineDoneContext(t *testing.T) {
	nodes := newNodeSpecEngine(t, buildNodeSpecSnapshot(t))
	rels := newRelSpecEngine(t, buildRelSpecSnapshot(t))
	nodeSpec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	relSpec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}

	// A live context is served: three users, three AdminTo edges.
	if n, ok := nodes.TryNodeCount(context.Background(), nodeSpec); !ok || n != 3 {
		t.Fatalf("TryNodeCount(live) = (%d, %v), want (3, true)", n, ok)
	}
	if n, ok := rels.TryRelCount(context.Background(), relSpec); !ok || n != 3 {
		t.Fatalf("TryRelCount(live) = (%d, %v), want (3, true)", n, ok)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancelExpired()
	for name, ctx := range map[string]context.Context{"cancelled": cancelled, "deadline passed": expired} {
		if n, ok := nodes.TryNodeCount(ctx, nodeSpec); ok {
			t.Errorf("TryNodeCount(%s) = (%d, true), want a decline", name, n)
		}
		if n, ok := rels.TryRelCount(ctx, relSpec); ok {
			t.Errorf("TryRelCount(%s) = (%d, true), want a decline", name, n)
		}
	}
}
