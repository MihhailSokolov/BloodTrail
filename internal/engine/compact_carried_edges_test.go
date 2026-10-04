// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

// carriedEdgeLogRecorder is a slog.Handler that keeps one named integer
// attribute of every record, by message, so a test can read what a log line
// actually reported.
type carriedEdgeLogRecorder struct {
	attr string

	mu     sync.Mutex
	values map[string][]int64
}

func newCarriedEdgeLogRecorder(attr string) *carriedEdgeLogRecorder {
	return &carriedEdgeLogRecorder{attr: attr, values: map[string][]int64{}}
}

func (r *carriedEdgeLogRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *carriedEdgeLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	found := false
	var value int64
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == r.attr {
			found, value = true, a.Value.Int64()
			return false
		}
		return true
	})
	if !found {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[rec.Message] = append(r.values[rec.Message], value)
	return nil
}

func (r *carriedEdgeLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }

func (r *carriedEdgeLogRecorder) WithGroup(string) slog.Handler { return r }

// reported returns the attribute's values for message, in order.
func (r *carriedEdgeLogRecorder) reported(message string) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.values[message]...)
}

// TestCompactionFinishedReportsCarriedEdges: a delta edge whose endpoint has
// not arrived is re-carried by every compaction, so the adopted View always
// has a segment and the post-compaction save, which requires an empty delta,
// is skipped every time -- at Debug, naming only the segment count. An
// endpoint that never arrives makes that permanent: no snapshot file is ever
// written again, and every restart rebuilds from PostgreSQL. The
// "compaction finished" line has to say how many edges it carried, which is
// the only place an operator can see it.
func TestCompactionFinishedReportsCarriedEdges(t *testing.T) {
	ctx := context.Background()
	const finished = "bloodtrail: compaction finished"

	t.Run("an edge waiting for its endpoint", func(t *testing.T) {
		logs := newCarriedEdgeLogRecorder("carried_edges")
		e := New(nil, nil, Config{Enabled: true, Log: slog.New(logs)})
		e.snap.Store(buildApplyView(t))

		v1 := publishAppend(ctx, e, newEdgeSegment(40, 1, 100, applyKindAdminTo))
		e.runCompaction(v1.Base(), v1.Segments())

		if got := e.CompactionCount(); got != 1 {
			t.Fatalf("CompactionCount() = %d, want 1 (fixture assumption)", got)
		}
		if got := logs.reported(finished); len(got) != 1 || got[0] != 1 {
			t.Fatalf("%q reported carried_edges %v, want exactly one line reporting 1", finished, got)
		}
	})

	t.Run("nothing pending", func(t *testing.T) {
		logs := newCarriedEdgeLogRecorder("carried_edges")
		e := New(nil, nil, Config{Enabled: true, Log: slog.New(logs)})
		e.snap.Store(buildApplyView(t))

		v1 := publishAppend(ctx, e, newNodeSegment(t, 100))
		e.runCompaction(v1.Base(), v1.Segments())

		if got := logs.reported(finished); len(got) != 1 || got[0] != 0 {
			t.Fatalf("%q reported carried_edges %v, want exactly one line reporting 0", finished, got)
		}
	})
}
