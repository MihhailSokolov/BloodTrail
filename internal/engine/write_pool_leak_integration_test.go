// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// writePathPoolGoroutineFrame is the one frame every live pgxpool.Pool has a
// goroutine parked in: NewWithConfig starts exactly one per pool, which
// creates the pool's idle connections and then sits in this loop until the
// pool's own Close closes the channel it selects on (pgx v5.10.0
// pgxpool/pool.go). So a goroutine in this frame is a pool nobody has
// closed, and its disappearance is proof that Close ran -- neither of which
// a connection count can show, since the write path's pool is sized
// MinConns 0 and times its connections out (writePathPoolIdleTime) while the
// pool itself stays open.
const writePathPoolGoroutineFrame = "pgxpool.(*Pool).backgroundHealthCheck"

// poolCloseSettleTimeout bounds how long assertNoPoolGoroutineAddedSince
// waits for a closed pool's goroutine to actually go: the engine's own close
// runs pgxpool's Close on a goroutine of its own (writePathPool.close), and
// the health-check loop leaves only on its next trip round the select. Long
// enough that a loaded machine cannot fail this; it bounds nothing else,
// since the loop returns the moment the goroutine is gone.
const poolCloseSettleTimeout = 10 * time.Second

// livePoolGoroutines returns the set of goroutine IDs currently parked in a
// pgxpool health check, as the runtime's own all-goroutine dump reports
// them.
//
// IDs, not a count: this binary runs some seventy engines, and an earlier
// test's leaked pool -- or the pg driver's own pool -- would make a count
// meaningless. A caller compares two sets taken around the window it cares
// about and looks only at what the window itself added, so nothing outside
// it can make the check pass or fail.
func livePoolGoroutines() map[string]bool {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	ids := map[string]bool{}
	id := ""
	for _, line := range strings.Split(string(buf), "\n") {
		if rest, ok := strings.CutPrefix(line, "goroutine "); ok {
			id, _, _ = strings.Cut(rest, " ")
			continue
		}
		if id != "" && strings.Contains(line, writePathPoolGoroutineFrame) {
			ids[id] = true
		}
	}
	return ids
}

// assertNoPoolGoroutineAddedSince fails unless every pgxpool health-check
// goroutine alive now was already alive when before was taken.
//
// The settle loop waits for a disappearance, never for an appearance, which
// is what makes this deterministic on a shared machine: a pool whose Close
// ran drops its goroutine on the channel close, so the loop ends as soon as
// the close propagates (the engine's own close runs it on a goroutine), and
// a pool nobody closed has nothing that will ever end it -- no amount of
// waiting, and no amount of unrelated load, turns that failure into a pass.
func assertNoPoolGoroutineAddedSince(t *testing.T, before map[string]bool) {
	t.Helper()

	start := time.Now()
	for {
		added := []string{}
		for id := range livePoolGoroutines() {
			if !before[id] {
				added = append(added, id)
			}
		}
		if len(added) == 0 {
			return
		}
		if time.Since(start) > poolCloseSettleTimeout {
			// Errorf, not Fatalf: the caller's remaining cases each take
			// their own "before" set, so one leak does not hide the next.
			t.Errorf("%d pgxpool health-check goroutine(s) outlived the test that created the pool (goroutine IDs %s): the write path's own pool was never closed",
				len(added), strings.Join(added, ", "))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWritePathPoolDoesNotOutliveTheTestThatCreatedIt is the leak the
// write-path pool's own doc used to merely advise against: an Engine a test
// builds directly, writes through, and then simply drops -- the shape most
// of this package's integration tests have -- left its pool, its idle
// connections and pgxpool's own background goroutine alive for the rest of
// the process.
//
// Production never had this: Driver.Close runs CloseWritePool on every path
// out (driver.go), after Stop and the shutdown save. Tests had it because
// nothing enforced what they could forget -- so the enforcement is in the
// fixture every test that touches PostgreSQL already goes through
// (graphtest.PGAvailable, whose hook this package registers in
// write_pool_leak_guard_integration_test.go), not in a production lifecycle
// that is already correct.
//
// Both halves are pinned, because the fixture must not make the explicit
// teardown wrong: an engine nobody closes, and an engine closed exactly as
// Driver.Close closes one (stopEngineAndCloseWritePool), leave the same
// nothing behind.
func TestWritePathPoolDoesNotOutliveTheTestThatCreatedIt(t *testing.T) {
	dsn := graphtest.PGAvailable(t)

	for _, tc := range []struct {
		name  string
		close func(*Engine)
	}{
		{name: "an engine the test never closes", close: func(*Engine) {}},
		{name: "an engine the test closes itself", close: stopEngineAndCloseWritePool},
	} {
		before := livePoolGoroutines()

		// A subtest, so the boundary the pool must not outlive is a test
		// that has demonstrably ended: t.Run runs the subtest's own cleanups
		// before it returns.
		t.Run(tc.name, func(t *testing.T) {
			// The fixture call every test in this package that touches
			// PostgreSQL starts with, made from the test whose end the pool
			// must not outlive.
			graphtest.PGAvailable(t)
			pgDriver, pool := graphtest.OpenPG(t, dsn)

			eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})

			// A real write-path statement, which is what creates the pool on
			// any server that has ingested anything: an eager watermark bump,
			// the first thing every driver-level write does.
			resetWatermarkTable(t, context.Background(), eng)
			if _, err := eng.BumpWatermark(context.Background()); err != nil {
				t.Fatalf("BumpWatermark: %v", err)
			}
			if got := eng.writePool.get(pool, eng.cfg.Log); got == nil || got == pool {
				t.Fatal("the write path did not get a pool of its own (fixture assumption)")
			}

			tc.close(eng)
		})

		assertNoPoolGoroutineAddedSince(t, before)
	}
}
