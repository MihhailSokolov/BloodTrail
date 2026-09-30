// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// TestServableViewNeverPairsServingWithThePreAdoptionView places a fallback
// recovery's adoption between serveState's two loads, whichever of them
// runs first. The engine is in fallback over the View from before the write
// that tripped it -- that write's call has already returned -- and the
// adoption publishes the rebuilt View, then flips back to serving. A caller
// may be told to decline, or to serve the rebuilt View, but never to SERVE
// the View that is missing the write.
func TestServableViewNeverPairsServingWithThePreAdoptionView(t *testing.T) {
	stale := buildApplyView(t)

	b := snapshot.NewBuilder(1)
	b.SetKinds(applyKindNames())
	for _, id := range []uint64{1, 2, 3, 4} { // 4 is the write that tripped the fallback
		if err := b.AddNode(id, []snapshot.KindID{applyKindUser}, nil); err != nil {
			t.Fatalf("AddNode(%d): %v", id, err)
		}
	}
	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	rebuilt := snapshot.NewView(snap)

	e := New(nil, nil, Config{Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	e.snap.Store(stale)
	e.state.Store(stateFallback)

	adopted := false
	adoptOnce := func() {
		if adopted {
			return
		}
		adopted = true
		if !e.adoptRebuiltView(context.Background(), rebuilt, e.applyEpoch.Load(), 0) {
			t.Fatalf("adoptRebuiltView refused the rebuilt View")
		}
	}

	view, serving := servableView(
		func() int32 { state := e.state.Load(); adoptOnce(); return state },
		func() *snapshot.View { v := e.snap.Load(); adoptOnce(); return v },
	)
	if !adopted {
		t.Fatalf("servableView loaded neither the state nor the View")
	}
	if serving && view != rebuilt {
		t.Fatalf("servableView reported SERVING with the View from before the adoption, which lacks the write that tripped the fallback")
	}

	if view, serving := e.serveState(); !serving || view != rebuilt {
		t.Fatalf("serveState after the adoption = (rebuilt View: %v, serving: %v), want (true, true)", view == rebuilt, serving)
	}
}
