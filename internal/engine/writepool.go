// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newWritePathPool is pgxpool.NewWithConfig, held in a package-level var
// purely as a test seam -- the same device write_observer.go's
// parseCypherFrontend uses, and for the same reason. NewWithConfig (pgx
// v5.10.0) returns an error only for a configuration puddle rejects, which
// means only MaxConns < 1, and get below always sets MaxConns itself: so
// nothing a caller can do reaches the fall-back-to-the-main-pool branch, and
// that branch is precisely the one that must not stay silent. Production
// always runs with this default value; nothing but writepool_test.go ever
// reassigns it.
var newWritePathPool = pgxpool.NewWithConfig

// writePathPoolConns and writePathPoolIdleTime size the write path's own
// pool (writePathPool): two connections -- the watermark bumps queue on one
// row lock in PostgreSQL anyway, and Apply's read-backs are serialized by
// applyMu, so a second connection is all it takes for neither to wait on
// the other -- closed after a minute idle, since an engine a caller never
// stops (every test that builds one directly) would otherwise keep them
// open for the life of the process. Never reduce it to 1: an acquire here uses
// the caller's context, and TestSaveSnapshotRefusesWhileAnEarlierBumpIsInFlight
// holds one connection on a stalled bump while a later write needs another, so
// a single connection makes that test hang to the package timeout rather than
// fail.
const (
	writePathPoolConns    = 2
	writePathPoolIdleTime = time.Minute
)

// writePathPool is the small pool the write path's own statements run on:
// the eager watermark bump (BumpWatermark) and Apply's read-back
// (readBack). Both can run while the caller holds one of the main pool's
// connections -- a bump happens at a transaction's or batch's first
// mutating call, inside its delegate; a mid-batch Commit applies, and so
// reads back, before its batch lets go of its connection -- and drawing a
// second connection from that same pool is what made writers wait on each
// other once it was saturated: with a deadline the writes failed and the
// watermark stopped being trusted; without one (a daemon's context) they
// waited for good. Statements on this pool never wait for a connection of
// the main pool, and never hold one of this pool while waiting for another
// of it, so nothing here can close that circle again.
//
// Created from the main pool's own configuration on first use, minus the
// pg driver's connection hooks (its composite-type registration, which the
// write path's plain SQL never reads, and whose release hook would destroy
// every connection made before the schema exists). Closed by
// CloseWritePool, after the shutdown save (Driver.Close); from then on get
// hands out the main pool again, which is what a write racing shutdown used
// before this pool existed.
//
// An engine nobody closes -- every test that builds one directly, and
// nothing in production, where Driver.Close always runs CloseWritePool --
// leaks this pool: its idle connections time out after writePathPoolIdleTime,
// but pgxpool's own background health-check goroutine runs until the pool is
// closed. Which is why such a test should close it (CloseWritePool) rather
// than only Stop the engine.
type writePathPool struct {
	mu     sync.Mutex
	pool   *pgxpool.Pool
	closed bool

	// warned records that the Warn below has already been logged, so a
	// pool that cannot be created does not log once per write-path
	// statement for the rest of the process's life. The creation itself is
	// still retried on every get: the failure may be transient, and
	// recovering from it is worth more than a second log line.
	warned bool
}

// get returns the pool to run a write-path statement on: this one, created
// on first use, or main when it is closed or cannot be created. nil when
// main is.
//
// Falling back to main is not silent: that pool is the one the write's own
// caller may already hold a connection of, which is the hazard this type
// exists to remove, so the first failure is logged at Warn with its error
// (see warned). log may be nil, for a test that constructs this type
// directly.
func (w *writePathPool) get(main *pgxpool.Pool, log *slog.Logger) *pgxpool.Pool {
	if main == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return main
	}
	if w.pool == nil {
		cfg := main.Config()
		cfg.MaxConns = writePathPoolConns
		cfg.MinConns = 0
		cfg.MinIdleConns = 0
		cfg.MaxConnIdleTime = writePathPoolIdleTime
		cfg.AfterConnect = nil
		cfg.AfterRelease = nil
		pool, err := newWritePathPool(context.Background(), cfg)
		if err != nil {
			if log != nil && !w.warned {
				w.warned = true
				log.Warn("bloodtrail: the write path's own connection pool could not be created",
					slog.String("consequence", "watermark bumps and read-backs run on BloodHound's pool, where a write holding a connection can wait on another's"),
					slog.Any("error", err),
				)
			}
			return main
		}
		w.pool = pool
	}
	return w.pool
}

// close closes the pool, without waiting for a statement still running on
// it, and makes get hand out the main pool from here on. Idempotent.
func (w *writePathPool) close() {
	w.mu.Lock()
	pool := w.pool
	w.pool, w.closed = nil, true
	w.mu.Unlock()

	if pool != nil {
		go pool.Close()
	}
}
