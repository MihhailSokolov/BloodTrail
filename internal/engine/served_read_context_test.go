// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// servedReadContextTx is the transaction servePathQuery is handed: an
// ID-endpoint path query reads only the memory limit from it, so every other
// method stays the embedded nil interface's.
type servedReadContextTx struct{ graph.Transaction }

func (servedReadContextTx) GraphQueryMemoryLimit() size.Size { return size.Gibibyte }

// newServedReadContextEngine builds an enabled engine over the relationship
// fixture with a pg driver whose pool is nil: every entry point under test
// either declines or serves entirely from the replica, so nothing reaches
// the pool.
func newServedReadContextEngine(t *testing.T) *Engine {
	t.Helper()
	e := newRelSpecEngine(t, buildRelSpecSnapshot(t))
	e.pgDriver = pg.NewDriver(size.Gibibyte, nil)
	return e
}

// TestServedReadsDeclineDoneContextPerEntryPoint: every served read runs on
// behalf of a request, and PostgreSQL answers a cancelled or expired request
// with its own error. The counts already declined a done context; the
// cursor- and Result-returning builder reads, and the shortest-path
// pipeline, did not -- they computed the answer and returned it with no
// error at all. All of them now decline on entry, so the wrapper hands the
// read to PostgreSQL and the caller sees PostgreSQL's outcome.
func TestServedReadsDeclineDoneContextPerEntryPoint(t *testing.T) {
	nodeSpec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	relSpec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	// Node 5 has no outgoing edge, so the (5 -> 1) query serves an empty
	// answer without hydrating anything from PostgreSQL.
	pathQuery := recognize.PathQuery{Start: recognize.Endpoint{IDs: []graph.ID{5}}, End: recognize.Endpoint{IDs: []graph.ID{1}}}

	entries := map[string]func(e *Engine, ctx context.Context) bool{
		"TryNodeFetchIDs": func(e *Engine, ctx context.Context) bool {
			cursor, ok := e.TryNodeFetchIDs(ctx, nodeSpec)
			if ok {
				cursor.Close()
			}
			return ok
		},
		"TryNodeFetchKinds": func(e *Engine, ctx context.Context) bool {
			cursor, ok := e.TryNodeFetchKinds(ctx, nodeSpec)
			if ok {
				cursor.Close()
			}
			return ok
		},
		"TryRelFetchIDs": func(e *Engine, ctx context.Context) bool {
			cursor, ok := e.TryRelFetchIDs(ctx, relSpec)
			if ok {
				cursor.Close()
			}
			return ok
		},
		"TryRelFetchTriples": func(e *Engine, ctx context.Context) bool {
			cursor, ok := e.TryRelFetchTriples(ctx, relSpec)
			if ok {
				cursor.Close()
			}
			return ok
		},
		"TryRelFetchKinds": func(e *Engine, ctx context.Context) bool {
			cursor, ok := e.TryRelFetchKinds(ctx, relSpec)
			if ok {
				cursor.Close()
			}
			return ok
		},
		"TryRelQueryRows": func(e *Engine, ctx context.Context) bool {
			result, ok := e.TryRelQueryRows(ctx, relSpec, recognize.ProjectionStartEnd, false)
			if ok {
				result.Close()
			}
			return ok
		},
		"TryAllShortestPaths": func(e *Engine, ctx context.Context) bool {
			_, ok := e.TryAllShortestPaths(ctx, servedReadContextTx{}, pathQuery)
			return ok
		},
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancelExpired()

	for name, serve := range entries {
		t.Run(name, func(t *testing.T) {
			e := newServedReadContextEngine(t)
			if !serve(e, context.Background()) {
				t.Fatalf("declined a live request: the fixture no longer reaches the serving path")
			}
			for ctxName, ctx := range map[string]context.Context{"cancelled": cancelled, "deadline passed": expired} {
				if serve(e, ctx) {
					t.Errorf("served a %s request, where PostgreSQL answers it with its own error", ctxName)
				}
			}
		})
	}
}

// relQueryRowsContextSnapshot builds a snapshot with one User node and
// `edges` AdminTo edges out of it to distinct Computer nodes, so an
// unanchored rel-query scan yields more rows than cancelCheckInterval and
// rowResult.Next actually reaches its batched context check.
func relQueryRowsContextSnapshot(t *testing.T, edges int) *snapshot.Snapshot {
	t.Helper()
	b := snapshot.NewBuilder(1)
	kindTable := make(map[snapshot.KindID]string, len(relSpecKindNames()))
	for id, kind := range relSpecKindNames() {
		kindTable[id] = kind.String()
	}
	b.SetKinds(kindTable)
	if err := b.AddNode(1, []snapshot.KindID{kindUser}, nil); err != nil {
		t.Fatalf("AddNode(1): %v", err)
	}
	for i := 0; i < edges; i++ {
		id := uint64(100 + i)
		if err := b.AddNode(id, []snapshot.KindID{kindComputer}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
		b.AddEdge(uint64(10_000+i), 1, id, kindAdminTo)
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return snap
}

// TestRelQueryRowsStopsPullingOnCancellation: TryRelQueryRows hands back a
// pull-based graph.Result the caller drives itself, with no feeder goroutine
// watching anything -- so a request cancelled after the first rows were
// pulled kept yielding the rest of the scan and reported no error at all,
// where PostgreSQL's cursor fails the remaining fetches. Next now consults
// the request's context on the work meter's own cadence and stops there,
// reporting the cancellation through Error().
func TestRelQueryRowsStopsPullingOnCancellation(t *testing.T) {
	const edges = cancelCheckInterval * 3
	e := newRelSpecEngine(t, relQueryRowsContextSnapshot(t, edges))
	spec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result, served := e.TryRelQueryRows(ctx, spec, recognize.ProjectionStartEnd, false)
	if !served {
		t.Fatalf("TryRelQueryRows declined a live request")
	}
	defer result.Close()

	rows := 0
	for result.Next() {
		rows++
		if rows == 1 {
			cancel()
		}
	}
	if !errors.Is(result.Error(), context.Canceled) {
		t.Fatalf("Error() = %v after %d of %d rows, want context.Canceled", result.Error(), rows, edges)
	}
	if rows > cancelCheckInterval {
		t.Fatalf("pulled %d of %d rows after the cancellation, want at most %d", rows, edges, cancelCheckInterval)
	}
}
