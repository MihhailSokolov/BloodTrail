// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// writePathPoolConns and writePathPoolIdleTime size the write path's own
// pool (writePathPool): two connections -- the watermark bumps queue on one
// row lock in PostgreSQL anyway, and Apply's read-backs are serialized by
// applyMu, so a second connection is all it takes for neither to wait on
// the other -- closed after a minute idle, since an engine a caller never
// stops (every test that builds one directly) would otherwise keep them
// open for the life of the process.
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
// every connection made before the schema exists). Closed by Stop; from
// then on get hands out the main pool again, which is what a write racing
// shutdown used before this pool existed.
type writePathPool struct {
	mu     sync.Mutex
	pool   *pgxpool.Pool
	closed bool
}

// get returns the pool to run a write-path statement on: this one, created
// on first use, or main when it is closed or cannot be created. nil when
// main is.
func (w *writePathPool) get(main *pgxpool.Pool) *pgxpool.Pool {
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
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
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
