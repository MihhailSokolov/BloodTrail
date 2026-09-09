// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// watermarkDDL creates bloodtrail_watermark (a single row, id=1, holding a
// monotonically increasing counter every mutating write bumps) and seeds
// its one row -- both idempotently: CREATE TABLE IF NOT EXISTS and INSERT
// ... ON CONFLICT DO NOTHING both tolerate running against a database that
// already has the table/row from a previous run. ensureWatermarkTable
// (below) execs this once, at engine start, before boot load.
//
// This table, and the protocol built on it (BumpWatermark, ReadWatermark,
// AdvanceWatermark, watermarkConverged, below), exists for a future
// snapshot-file consumer, not this package today: a persisted snapshot
// file is only safe to trust if it can prove no write landed in PostgreSQL
// after the file's own snapshot was taken. Stamping every mutating write
// with a counter this table alone owns, bumped BEFORE that write's own pg
// effect (see BumpWatermark's own doc for why "before" specifically), is
// what will let a future loader compare a file's stamped counter against
// pg's current one and refuse a file that is behind.
const watermarkDDL = `
create table if not exists bloodtrail_watermark (
	id smallint primary key default 1 check (id = 1),
	counter bigint not null default 0,
	updated_at timestamptz not null default now()
);
insert into bloodtrail_watermark (id) values (1) on conflict do nothing;
`

// ErrWatermarkUnavailable is BumpWatermark's and ReadWatermark's own return
// when this Engine has no pg pool to run the watermark protocol against at
// all. Every real *Driver-constructed Engine always has one (Open,
// driver.go, refuses to construct a Driver without a pool at all), so this
// only ever fires for an Engine a test built directly with
// New(pgDriver, nil, ...) -- the same "no database behind it"
// accommodation claimRebuildLoop's own doc already makes for a nil pool,
// generalized here to this protocol's two pg-touching calls.
//
// Callers in the root package (write_observer.go's ensureBumped/
// advanceIfBumped, driver.go's own call sites) treat this distinctly from
// every other error BumpWatermark can return: it means "this engine was
// never going to track a watermark," not "the write's eager bump failed,"
// so it is never logged, never recorded as a ChangeSet fallback, and never
// sets watermarkDirty.
var ErrWatermarkUnavailable = errors.New("engine: watermark unavailable: no pg pool bound to this engine")

// ensureWatermarkTable execs watermarkDDL on e.pool, logging (Warn) and
// setting watermarkDirty on failure rather than returning an error: Start
// (boot.go) calls this before launching boot load, and a DDL failure must
// never block startup, any more than a later bump failure is allowed to
// block the write it guards (BumpWatermark's own doc makes the same
// promise) -- a future snapshot-file writer simply keeps refusing to trust
// convergence (watermarkDirty) until this engine, or a later bump+apply
// pair, proves the table is reachable after all.
//
// A no-op when e.pool is nil -- see ErrWatermarkUnavailable's doc for
// exactly which callers that accommodates (this method has no error to
// hand back to a caller that already knows there is no database at all).
func (e *Engine) ensureWatermarkTable(ctx context.Context) {
	if e.pool == nil {
		return
	}
	if _, err := e.pool.Exec(ctx, watermarkDDL); err != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: watermark table DDL failed", slog.Any("error", err))
		e.watermarkDirty.Store(true)
	}
}

// bumpWatermarkSQL atomically increments bloodtrail_watermark's one row and
// returns the new value in the same round trip -- BumpWatermark's entire
// pg-facing implementation.
const bumpWatermarkSQL = `update bloodtrail_watermark set counter = counter + 1, updated_at = now() where id = 1 returning counter`

// BumpWatermark atomically increments the pg watermark counter and returns
// its new value, incrementing e.inflightBumps the instant the UPDATE
// commits -- before this method even returns, let alone before the write
// this bump guards ever reaches PostgreSQL itself. That ordering is the
// whole point of calling this EAGERLY, before a write's own pg effect
// (every call site: write_observer.go's ensureBumped, for the first
// mutating call of a transaction/batch, and driver.go's own top-of-method
// call for Run/WipeGraph/SetDefaultGraph/DeleteNodesByKinds/
// DeleteRelationshipsByKinds): the counter must advance even for a write
// that goes on to fail or roll back, which is only possible if the bump
// happens before that write is even attempted, not after. See
// watermarkConverged's own doc for why inflightBumps -- not just the
// counter's own value -- is what makes that safe to rely on.
//
// AdvanceWatermark is every bumped call's matching resolution, called
// exactly once per successful bump regardless of how the write it guarded
// eventually turned out (a full Apply on success, or AdvanceWatermark
// called directly for a write known to have produced no committed effect
// at all) -- see its own doc.
//
// Returns ErrWatermarkUnavailable, without ever touching inflightBumps or
// reaching the pool at all, when e.pool is nil. Any other error is the pg
// round trip itself failing (an unreachable database, say); either way,
// the caller's own contract (ensureBumped) is to never let this block the
// write it was about to guard.
func (e *Engine) BumpWatermark(ctx context.Context) (uint64, error) {
	if e.pool == nil {
		return 0, ErrWatermarkUnavailable
	}

	var counter uint64
	if err := e.pool.QueryRow(ctx, bumpWatermarkSQL).Scan(&counter); err != nil {
		return 0, fmt.Errorf("engine: BumpWatermark: %w", err)
	}

	e.inflightBumps.Add(1)
	return counter, nil
}

// ReadWatermark reads the pg watermark counter's current value without
// bumping it: watermarkConverged's own live check, and a future
// snapshot-file loader's way to validate a file's stamped counter against
// pg's current one.
//
// Returns ErrWatermarkUnavailable when e.pool is nil, mirroring
// BumpWatermark's own doc.
func (e *Engine) ReadWatermark(ctx context.Context) (uint64, error) {
	if e.pool == nil {
		return 0, ErrWatermarkUnavailable
	}

	var counter uint64
	if err := e.pool.QueryRow(ctx, `select counter from bloodtrail_watermark where id = 1`).Scan(&counter); err != nil {
		return 0, fmt.Errorf("engine: ReadWatermark: %w", err)
	}
	return counter, nil
}

// NoteWatermarkBumpFailure logs (Warn) that a bump genuinely failed -- not
// ErrWatermarkUnavailable, which never reaches this method at all (see its
// own doc) -- and sets watermarkDirty. The caller (write_observer.go's
// ensureBumped, or a driver.go call site) is expected to record its own
// ChangeSet fallback on the write's own WriteScope; this method never
// touches one, since it lives on the engine, not on any one write.
func (e *Engine) NoteWatermarkBumpFailure(ctx context.Context, err error) {
	e.cfg.Log.WarnContext(ctx, "bloodtrail: watermark bump failed", slog.Any("error", err))
	e.watermarkDirty.Store(true)
}

// maxWatermark returns the larger of cur and candidate -- AdvanceWatermark's
// own monotonic max-advance comparison, extracted as a pure function
// (mirroring apply.go's fallbackRetryDelay) purely so it has a direct,
// atomic-free unit test.
func maxWatermark(cur, candidate uint64) uint64 {
	if candidate > cur {
		return candidate
	}
	return cur
}

// AdvanceWatermark folds counter into e.appliedWatermark's monotonic max
// (maxWatermark) and retires exactly one e.inflightBumps entry -- the
// bookkeeping every bumped WriteScope needs exactly once, regardless of how
// its write turned out. Callers must only ever pass a counter a successful
// BumpWatermark call actually returned for THIS scope (every real caller
// gets there via WriteScope.Watermark's own bumped flag, never by
// guessing): this method trusts its caller completely and decrements
// inflightBumps unconditionally, so calling it for a scope that never
// bumped would corrupt the count.
//
// The max-advance, rather than a plain overwrite, is what makes this safe
// under concurrency: two bumped scopes can finish resolving in either
// order (the one issued first is not guaranteed to finish first), so a
// later call's own counter can be smaller than one an earlier-finishing
// concurrent call already advanced past -- folding in the smaller value
// must never regress appliedWatermark.
//
// Two callers, both giving this the same "this scope is now fully
// resolved" meaning:
//
//   - Apply's own deferred call (apply.go), for every write whose eager
//     bump succeeded -- on every branch Apply can take, success or a fresh
//     trip into fallback, not just the "published a new View" branch: the
//     pg watermark counter already advanced the moment the bump committed,
//     independent of what Apply goes on to do with the write's own effect.
//   - driver.go's WriteTransaction error branch (and the error branch of
//     every driver-level method that bumps eagerly at its own top: Run,
//     WipeGraph, SetDefaultGraph, DeleteNodesByKinds,
//     DeleteRelationshipsByKinds), calling this directly instead of a full
//     Apply: a rolled-back transaction, or a failed one-shot call, has no
//     committed effect for a read-back to replay, but the counter itself
//     still has to resolve.
//
// If watermarkDirty is currently set, this also re-checks watermarkConverged
// (a live pg read -- see its own doc) and clears watermarkDirty the instant
// convergence is reached again: a later successful bump+apply pair is
// exactly the recovery watermarkDirty's own doc promises. Skipped while
// dirty is already clear, so the overwhelmingly common case -- no bump has
// ever failed -- never pays for that extra round trip on every single
// write.
func (e *Engine) AdvanceWatermark(ctx context.Context, counter uint64) {
	for {
		cur := e.appliedWatermark.Load()
		next := maxWatermark(cur, counter)
		if next == cur {
			break
		}
		if e.appliedWatermark.CompareAndSwap(cur, next) {
			break
		}
	}

	e.inflightBumps.Add(-1)

	if e.watermarkDirty.Load() {
		if _, converged := e.watermarkConverged(ctx); converged {
			e.watermarkDirty.Store(false)
		}
	}
}

// watermarkConvergedFor is watermarkConverged's pure comparison, extracted
// for unit testing without a live pg read: true iff no bumped scope is
// still unresolved (inflight == 0) and pgCounter equals applied. See
// watermarkConverged's own doc for why both conditions are necessary.
func watermarkConvergedFor(pgCounter, applied uint64, inflight int64) bool {
	return inflight == 0 && pgCounter == applied
}

// watermarkConverged reports pg's current watermark counter, and whether
// this engine's own applied-side bookkeeping is fully caught up with it:
// true iff no bumped scope is still unresolved (e.inflightBumps == 0) and
// the pg counter it just read equals e.appliedWatermark's own value
// (watermarkConvergedFor).
//
// Both halves are necessary, not redundant. inflightBumps == 0 alone does
// not prove the counter matches (a concurrent BumpWatermark could commit
// after this read began, moving pg's counter without yet touching
// inflightBumps -- see BumpWatermark's own ordering doc); the counter
// matching alone does not prove nothing is in flight either (a write's
// eager bump can commit and advance pg's counter well before that same
// write's own effect has committed or rolled back, which is exactly the
// window inflightBumps exists to cover). Only both together mean every
// write that has ever bumped the counter has also had its own effect fully
// resolved -- committed and reflected, or rolled back and reconciled to
// nothing -- which is the actual promise a future snapshot-file writer
// needs before it can trust a file it is about to stamp with this same
// counter.
//
// A ReadWatermark failure (including ErrWatermarkUnavailable) reports
// (0, false): "can't currently prove convergence" is always the safe
// answer when the live read itself didn't succeed.
func (e *Engine) watermarkConverged(ctx context.Context) (uint64, bool) {
	pgCounter, err := e.ReadWatermark(ctx)
	if err != nil {
		return 0, false
	}
	return pgCounter, watermarkConvergedFor(pgCounter, e.appliedWatermark.Load(), e.inflightBumps.Load())
}
