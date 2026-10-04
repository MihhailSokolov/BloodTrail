// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// This file is what makes "an Engine nobody closes does not leak the write
// path's pool" true of this package's tests, and it is the whole fix for
// that leak: production never had it (Driver.Close runs CloseWritePool on
// every path out, after Stop and the shutdown save), so there is no
// production lifecycle to change -- only tests, which build Engines directly
// and have nothing but a convention to remember.
//
// The enforcement is hung off graphtest.PGAvailable (graphtest.OnPGTest),
// the one call every test here that touches PostgreSQL starts with, rather
// than off a new fixture tests would have to adopt one by one: a test that
// never heard of any of this, and a test added later, both get the cleanup.
// stopEngineAndCloseWritePool stays exactly as it was and is still the right
// teardown to write -- it closes the pool at the point the engine is done
// with it, in Driver.Close's own order, instead of at the end of the test --
// and double-closing is safe (pgxpool.Pool.Close is once-only), so the two
// compose.
//
// What this deliberately does NOT do is fail a test that forgot. The pool
// cannot outlive the test either way, which is the property that matters;
// failing instead would only move the burden back onto every test author
// for no gain in what the suite actually leaves running.

func init() { graphtest.OnPGTest(trackWritePathPools) }

// writePathPoolScope collects the write-path pools created while one test
// (or subtest) is the innermost tracked one.
type writePathPoolScope struct {
	pools []*pgxpool.Pool
}

var (
	// trackMu guards trackScopes and every scope's pools: a pool can be
	// created from a goroutine of the test's own (concurrent writers), not
	// just from the test goroutine.
	trackMu     sync.Mutex
	trackScopes []*writePathPoolScope

	// trackOnce installs the newWritePathPool wrapper exactly once, before
	// the first tracked test runs, so the package-level var is never written
	// again while tests are running -- a goroutine some earlier test left
	// behind could still be reading it through get.
	trackOnce sync.Once
)

// trackWritePathPools makes every write-path pool created from here until t
// ends close when t ends.
//
// Scopes nest: PGAvailable is called by a parent and again by its subtests,
// and a pool is recorded against the innermost scope open when it is
// created, so it closes with the test that actually created it rather than
// with an enclosing one. Cleanups unwind in reverse order of registration,
// so the scope a cleanup removes is always the one it pushed.
//
// The close itself runs on a goroutine, exactly as the engine's own
// writePathPool.close does and for the same reason: pgxpool's Close blocks
// until every connection is handed back, and a test's teardown must not be
// able to hang on a statement some goroutine of that test left in flight.
// Nothing in a test waits on the close, so nothing needs it synchronous --
// TestWritePathPoolDoesNotOutliveTheTestThatCreatedIt, the test that proves
// this works, waits for the pool's goroutine to go.
func trackWritePathPools(t *testing.T) {
	trackOnce.Do(func() {
		create := newWritePathPool
		newWritePathPool = func(ctx context.Context, cfg *pgxpool.Config) (*pgxpool.Pool, error) {
			pool, err := create(ctx, cfg)
			if err != nil {
				return nil, err
			}
			trackMu.Lock()
			if n := len(trackScopes); n > 0 {
				trackScopes[n-1].pools = append(trackScopes[n-1].pools, pool)
			}
			trackMu.Unlock()
			return pool, nil
		}
	})

	scope := &writePathPoolScope{}
	trackMu.Lock()
	trackScopes = append(trackScopes, scope)
	trackMu.Unlock()

	t.Cleanup(func() {
		trackMu.Lock()
		for i := len(trackScopes) - 1; i >= 0; i-- {
			if trackScopes[i] == scope {
				trackScopes = append(trackScopes[:i], trackScopes[i+1:]...)
				break
			}
		}
		pools := scope.pools
		scope.pools = nil
		trackMu.Unlock()

		for _, pool := range pools {
			go pool.Close()
		}
	})
}
