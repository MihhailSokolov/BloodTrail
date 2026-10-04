// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"regexp"
	"strings"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/recognize"
)

// multiGraphDeclineReason extracts the reason attr of the last decline event
// (path or builder) in a captured log.
var multiGraphDeclineReason = regexp.MustCompile(`reason=(\S+)`)

// multiGraphStubTx is the transaction handed to TryAllShortestPaths: an
// ID-endpoint query reads only the memory limit from it, so every other
// method stays the embedded nil interface's.
type multiGraphStubTx struct{ graph.Transaction }

func (multiGraphStubTx) GraphQueryMemoryLimit() size.Size { return size.Gibibyte }

// newMultiGraphServingEngine builds an enabled engine over the relationship
// fixture with a Debug logger it captures, flagging the snapshot as
// multi-graph or not. The pg driver is a bare one (nil pool): the entry
// points under test decline, or serve from the replica, before anything
// reaches it.
func newMultiGraphServingEngine(t *testing.T, multiGraph bool) (*Engine, *strings.Builder) {
	t.Helper()

	snap := buildRelSpecSnapshot(t)
	snap.MultiGraph = multiGraph

	e := newRelSpecEngine(t, snap)
	e.pgDriver = pg.NewDriver(size.Gibibyte, nil)
	logs := &strings.Builder{}
	e.cfg.Log = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return e, logs
}

// TestServingDeclinesOnMultiGraphSnapshot: dawgs' PostgreSQL reads are not
// scoped by graph (only its create statements use the graph id), so once a
// second graph holds nodes a builder count or shortest path spans every
// graph while the replica holds only the default one -- serving from it
// would return SHORT answers. Every builder-serving entry point and the
// shortest-path pipeline must decline reasonMultiGraph on such a snapshot,
// exactly as TryCypher does, and must still serve the same call on a
// single-graph snapshot.
func TestServingDeclinesOnMultiGraphSnapshot(t *testing.T) {
	ctx := context.Background()

	nodeSpec := recognize.NodeSpec{Constraints: []recognize.KindConstraint{kindConstraint(false, "User")}}
	relSpec := recognize.RelSpec{EdgeKinds: edgeKinds("AdminTo")}
	// Node 5 has no outgoing edge, so the (5 -> 1) query serves an empty
	// answer without hydrating anything from PostgreSQL.
	pathQuery := recognize.PathQuery{Start: recognize.Endpoint{IDs: []graph.ID{5}}, End: recognize.Endpoint{IDs: []graph.ID{1}}}

	entries := []struct {
		name  string
		serve func(e *Engine) bool
	}{
		{"TryNodeCount", func(e *Engine) bool { _, ok := e.TryNodeCount(ctx, nodeSpec); return ok }},
		{"TryNodeFetchIDs", func(e *Engine) bool {
			cursor, ok := e.TryNodeFetchIDs(ctx, nodeSpec)
			if ok {
				cursor.Close()
			}
			return ok
		}},
		{"TryNodeFetchKinds", func(e *Engine) bool {
			cursor, ok := e.TryNodeFetchKinds(ctx, nodeSpec)
			if ok {
				cursor.Close()
			}
			return ok
		}},
		{"TryRelCount", func(e *Engine) bool { _, ok := e.TryRelCount(ctx, relSpec); return ok }},
		{"TryRelFetchIDs", func(e *Engine) bool {
			cursor, ok := e.TryRelFetchIDs(ctx, relSpec)
			if ok {
				cursor.Close()
			}
			return ok
		}},
		{"TryRelFetchTriples", func(e *Engine) bool {
			cursor, ok := e.TryRelFetchTriples(ctx, relSpec)
			if ok {
				cursor.Close()
			}
			return ok
		}},
		{"TryRelFetchKinds", func(e *Engine) bool {
			cursor, ok := e.TryRelFetchKinds(ctx, relSpec)
			if ok {
				cursor.Close()
			}
			return ok
		}},
		{"TryRelQueryRows", func(e *Engine) bool {
			result, ok := e.TryRelQueryRows(ctx, relSpec, recognize.ProjectionStartEnd, false)
			if ok {
				result.Close()
			}
			return ok
		}},
		{"TryAllShortestPaths", func(e *Engine) bool { _, ok := e.TryAllShortestPaths(ctx, multiGraphStubTx{}, pathQuery); return ok }},
	}

	for _, tc := range entries {
		t.Run(tc.name, func(t *testing.T) {
			single, _ := newMultiGraphServingEngine(t, false)
			if !tc.serve(single) {
				t.Fatalf("declined on a single-graph snapshot: the fixture no longer reaches the serving path")
			}

			multi, logs := newMultiGraphServingEngine(t, true)
			if tc.serve(multi) {
				t.Fatalf("served from a multi-graph snapshot, whose replica holds only one of the graphs PostgreSQL reads span")
			}
			matches := multiGraphDeclineReason.FindStringSubmatch(logs.String())
			if matches == nil || matches[1] != reasonMultiGraph {
				t.Fatalf("decline reason = %v, want %q\nlog:\n%s", matches, reasonMultiGraph, logs.String())
			}
		})
	}
}
