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
// resolveAbandonedWrite, driver.go's own call sites) treat this distinctly
// from every other error BumpWatermark can return: it means "this engine was
// never going to track a watermark," not "the write's eager bump failed,"
// so it is never logged, never recorded as a ChangeSet fallback, and never
// opens a watermark trust generation (NoteWatermarkBumpFailure).
var ErrWatermarkUnavailable = errors.New("engine: watermark unavailable: no pg pool bound to this engine")

// ensureWatermarkTable execs watermarkDDL on e.pool, logging (Warn) and
// recording a watermark failure on failure rather than returning an error:
// Start (boot.go) calls this before launching boot load, and a DDL failure
// must never block startup, any more than a later bump failure is allowed to
// block the write it guards (BumpWatermark's own doc makes the same
// promise) -- a snapshot-file writer simply keeps refusing to trust this
// engine (WatermarkTrusted) until an adopted snapshot resolves that failure's
// own generation.
//
// The failure is recorded through noteSelfSettlingWatermarkFailure, not
// NoteWatermarkBumpFailure: a DDL exec that failed guarded no write at all,
// so there is no write whose settling could ever retire it -- see that
// method's own doc.
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
		e.noteSelfSettlingWatermarkFailure()
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
// eventually turned out (Apply, for a write that committed, or
// ResolveAbandonedWrite, for one known to have produced no committed effect
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

// NoteWatermarkBumpFailure logs (Warn) that scope's own eager bump genuinely
// failed -- not ErrWatermarkUnavailable, which never reaches this method at
// all (see its own doc) -- and opens a new watermark trust generation for it:
// e.dirtyGen advances immediately, which makes WatermarkTrusted report false
// from this instant on, and scope is marked so that this same failure can be
// settled exactly once later, when the write it guards reaches a final
// outcome in PostgreSQL (settleWatermarkFailure's own doc).
//
// Both halves have to happen here, at the one call site (write_observer.go's
// ensureBumped) that knows both the engine and the write's own scope: the
// generation is what withdraws trust, and the mark is the only thing that can
// ever give it back. The caller additionally records its own ChangeSet
// fallback on the same scope, which is what makes that write's Apply rebuild
// the replica rather than replay a delta for a write whose counter was never
// recorded.
//
// A nil scope still advances dirtyGen, deliberately, but can never be
// settled -- the engine would then stay distrusted for the rest of its life.
// No caller passes nil (ensureBumped returns early on a nil scope before it
// ever attempts a bump); fail-closed is the right answer if one ever did.
func (e *Engine) NoteWatermarkBumpFailure(ctx context.Context, scope *WriteScope, err error) {
	e.cfg.Log.WarnContext(ctx, "bloodtrail: watermark bump failed", slog.Any("error", err))

	e.dirtyGen.Add(1)
	if scope != nil {
		scope.markWatermarkBumpFailed()
	}
}

// noteSelfSettlingWatermarkFailure records a watermark failure that guards no
// write at all, and therefore settles itself the instant it is noted:
// ensureWatermarkTable's failed DDL exec is the only one today.
//
// The distinction from NoteWatermarkBumpFailure is the whole reason this
// exists. A failed BUMP means some write is about to reach PostgreSQL
// uncounted, so its generation must not be settled until that write's own
// outcome is final -- otherwise a snapshot loaded while the write is still in
// flight could resolve a generation whose write it does not contain. A failed
// DDL exec missed no write: it is a statement about the watermark table's own
// reachability at that moment, so the very next adopted snapshot is enough to
// resolve it (it will have been loaded after this call, and the failure named
// no data at all). Settling it immediately is what lets that next adoption --
// at boot, Start's own boot load, which runs right after this
// (boot.go) -- restore trust, rather than leaving the engine distrusted until
// some unrelated write happens to fail its bump.
//
// dirtyGen is advanced before settledDirtyGen so the documented
// settledDirtyGen <= dirtyGen invariant (engine.go) holds at every instant a
// concurrent WatermarkTrusted call could observe.
func (e *Engine) noteSelfSettlingWatermarkFailure() {
	e.dirtyGen.Add(1)
	e.settledDirtyGen.Add(1)
}

// settleWatermarkFailure records that the write scope belongs to has reached a
// final outcome in PostgreSQL, retiring the watermark failure that write
// carries (if any) into e.settledDirtyGen. Reports whether it actually
// settled one, so the caller can request the rebuild that will eventually
// resolve it.
//
// Called from exactly the two places that know a write's outcome is final:
//
//   - Apply (apply.go), which is only ever called once a write has actually
//     landed in PostgreSQL (its own doc), unconditionally on every branch
//     Apply can then take.
//   - ResolveAbandonedWrite (below), for a write whose driver-level call
//     returned an error -- it rolled back, or never reached PostgreSQL at
//     all, so nothing of it was ever missed.
//
// "Final outcome" is the exact property the trust argument needs, and it is
// why this is not folded into NoteWatermarkBumpFailure: a failure noted while
// its write is still in flight must keep trust withdrawn, since a snapshot
// loaded in that window would not contain the write.
//
// The scope's mark is CONSUMED (takeWatermarkBumpFailure), so a scope that
// somehow reached both call sites -- or an Apply that somehow ran twice for
// one scope -- still advances the counter exactly once, which is what makes
// "settledDirtyGen == dirtyGen means every failure has settled" a sound
// reading of two independent counters.
func (e *Engine) settleWatermarkFailure(scope *WriteScope) bool {
	if scope == nil || !scope.takeWatermarkBumpFailure() {
		return false
	}
	e.settledDirtyGen.Add(1)
	return true
}

// ResolveAbandonedWrite resolves every piece of watermark bookkeeping scope
// carries, for a write that is known to have produced no committed effect at
// all: driver.go's WriteTransaction error branch, and the error branch of
// every driver-level method that bumps eagerly at its own top (Run,
// WipeGraph, SetDefaultGraph, DeleteNodesByKinds, DeleteRelationshipsByKinds).
// Those branches deliberately never call Apply -- there is no committed effect
// for a read-back to replay -- so this is what stands in for it:
//
//   - A successful eager bump still has to resolve (AdvanceWatermark), since
//     the pg counter advanced the moment that UPDATE committed, whatever the
//     write went on to do.
//   - A FAILED eager bump settles here too. The write it guarded returned an
//     error, so it left nothing behind in PostgreSQL for the replica to be
//     missing -- the same premise those error branches already bet the
//     replica's correctness on by skipping Apply entirely. Settling it is
//     what keeps one transient bump failure on a write that then failed
//     anyway from distrusting this engine permanently.
//
// A settled failure additionally requests a rebuild: unlike the Apply path,
// where the write's own ChangeSet fallback record trips enterFallback and
// starts recovery on its own, nothing else here would ever schedule the
// adopted snapshot that resolves this generation (WatermarkTrusted's own
// doc). The engine does NOT enter fallback: the replica is not suspect (the
// write landed nothing), only this engine's authority to call a snapshot file
// trustworthy is, so queries keep being served from memory while the rebuild
// runs.
func (e *Engine) ResolveAbandonedWrite(ctx context.Context, scope *WriteScope) {
	if scope == nil {
		return
	}

	if counter, bumped := scope.Watermark(); bumped {
		e.AdvanceWatermark(counter)
	}

	if e.settleWatermarkFailure(scope) {
		e.cfg.Log.DebugContext(ctx, "bloodtrail: watermark failure settled by a write that produced no effect; rebuild requested to restore trust")
		e.startFallbackRebuild()
	}
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

// AdvanceWatermark resolves one bumped scope's counter: it folds counter into
// e.appliedWatermark's monotonic max (maxWatermark) and retires exactly one
// e.inflightBumps entry -- pure atomic arithmetic, with no pg round trip and,
// despite Apply's own call site running it under applyMu, nothing that
// actually REQUIRES that lock: both appliedWatermark's CompareAndSwap loop
// and inflightBumps' Add are already safe under concurrent, unsynchronized
// callers on their own.
//
// The max-advance, rather than a plain overwrite, is what makes this safe
// under concurrency: two bumped scopes can finish resolving in either
// order (the one issued first is not guaranteed to finish first), so a
// later call's own counter can be smaller than one an earlier-finishing
// concurrent call already advanced past -- folding in the smaller value
// must never regress appliedWatermark.
//
// Callers must only ever pass a counter a successful BumpWatermark call
// actually returned for THIS scope (every real caller gets there via
// WriteScope.Watermark's own bumped flag, never by guessing): this method
// trusts its caller completely and decrements inflightBumps unconditionally,
// so calling it for a scope that never bumped would corrupt the count.
//
// Two production callers, both giving this the same "this scope's own bump is
// now resolved" meaning: Apply (apply.go), for a write that committed, and
// ResolveAbandonedWrite (above), for one that did not. It stays exported as
// part of the watermark protocol's own surface; the root package's driver
// error branches reach the second of those two through ResolveAbandonedWrite
// rather than calling this directly, so that a scope's failed bump and its
// successful one are always resolved by the same call.
func (e *Engine) AdvanceWatermark(counter uint64) {
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
}

// watermarkTrustedFor is WatermarkTrusted's pure decision, extracted for a
// unit test that needs no database at all: trust requires every watermark
// failure ever noted to have been resolved by an adopted snapshot
// (dirtyGen == resolvedGen), the counter bookkeeping to be caught up with
// PostgreSQL (converged), and the replica itself to be trustworthy right now
// (state == stateServing). See WatermarkTrusted for why each is necessary.
func watermarkTrustedFor(dirtyGen, resolvedGen uint64, converged bool, state int32) bool {
	return dirtyGen == resolvedGen && converged && state == stateServing
}

// watermarkGens loads the noted and resolved trust generations as one pair
// for a caller to compare -- always through this method, so that every
// comparison anywhere is made on values read in the one order that makes the
// comparison meaningful.
//
// resolvedDirtyGen is read FIRST, and dirtyGen second, and that order is
// load-bearing rather than incidental. Both counters only ever increase, and
// resolvedDirtyGen <= settledDirtyGen <= dirtyGen holds at every instant
// (engine.go), so reading resolved at t1 and dirty at t2 > t1 gives
// resolved(t1) <= dirty(t1) <= dirty(t2): equality can only mean dirtyGen did
// not move between the two reads, and therefore that no failure was noted
// that a comparison of this pair has not accounted for. Reading them the
// other way round would admit exactly the opposite: a stale, smaller dirtyGen
// read before a new failure was noted, compared against a resolvedGen read
// after -- two equal numbers describing two different moments, which is the
// shape of every wrong-trust bug this whole design exists to rule out. Which
// is also why the pair is returned rather than re-loaded per condition: two
// reads of the same counter in one decision are two different moments again.
func (e *Engine) watermarkGens() (dirtyGen, resolvedGen uint64) {
	resolvedGen = e.resolvedDirtyGen.Load()
	return e.dirtyGen.Load(), resolvedGen
}

// watermarkGensResolved reports whether every watermark failure this engine
// has ever noted has been resolved by an adopted snapshot -- the generation
// half of WatermarkTrusted, split out because it is the half that costs
// nothing (two atomic loads, no pg round trip) and the half unit tests can
// drive without a database.
func (e *Engine) watermarkGensResolved() bool {
	dirtyGen, resolvedGen := e.watermarkGens()
	return dirtyGen == resolvedGen
}

// WatermarkTrusted reports whether a snapshot file stamped with the pg
// watermark counter right now could be trusted to reflect every write
// PostgreSQL has: the watermark protocol's single answer to a future
// snapshot-file writer, replacing the clearable "dirty" flag an earlier
// version of this file carried.
//
// Trust is COMPUTED, never stored and never cleared. That is the entire
// point: a flag has to be cleared by somebody, and whoever clears it is
// reasoning about a moment that may already be stale for some OTHER write
// still in flight. Three independent conditions must all hold at the moment
// of the call:
//
//  1. Every watermark failure ever noted has been resolved by an adopted
//     snapshot (watermarkGensResolved).
//  2. The counter bookkeeping is caught up with PostgreSQL: the pg counter
//     equals e.appliedWatermark and no bumped scope is still unresolved
//     (watermarkConverged, a live pg read -- see its own doc for why both of
//     ITS halves are needed).
//  3. The replica is trustworthy right now: state == stateServing, i.e. no
//     write is currently known to have failed to replay.
//
// # How a failure is resolved, and why it takes an adopted snapshot
//
// A genuine bump failure (NoteWatermarkBumpFailure, from write_observer.go's
// ensureBumped) is the hard case, because the write W it guarded leaves NO
// trace at all in the counter bookkeeping condition 2 checks: there is no
// counter to record, since the bump itself is what failed, so W's own scope
// is bumped=false and e.appliedWatermark/e.inflightBumps never learn W
// existed. Condition 2 can therefore read true while W is mid-flight, and
// condition 3 can too (W's Apply, which is what trips the engine into
// fallback via the ChangeSet fallback record ensureBumped pairs with the
// failure, has not run yet). Only the generations rule W out, in three steps:
//
//   - dirtyGen advances the instant the failure is noted -- BEFORE W's own pg
//     effect is even attempted, since the bump is eager (BumpWatermark's own
//     doc). From that instant, condition 1 is false.
//   - settledDirtyGen advances only when W's outcome is final in PostgreSQL:
//     in W's own Apply, which is only ever called once W has committed
//     (Apply's own doc), or in ResolveAbandonedWrite, for a W whose
//     driver-level call returned an error and therefore left nothing behind.
//     A failure whose write is still in flight is counted in dirtyGen and not
//     in settledDirtyGen, so condition 1 stays false no matter what any other
//     write concurrently observes or does. This is exactly the window a
//     clearable flag got wrong: a concurrent write C, whose own bump
//     succeeded and whose own Apply finds the engine converged and serving,
//     has no way to clear anything here, because trust is not a thing anyone
//     clears.
//   - resolvedDirtyGen advances only in adoptRebuiltView, and only to the
//     settledDirtyGen value rebuildOnce read BEFORE its LoadSnapshot began
//     (engine.go). So resolvedDirtyGen >= g means: some snapshot was adopted
//     whose load began after every one of those g failures' writes had
//     already settled in PostgreSQL -- hence a load whose own read
//     transaction sees their committed rows, and an adoption that published
//     exactly that data as the current View.
//
// Put together: condition 1 holding means every failure ever noted belongs to
// a write that had settled before the load of a snapshot this engine went on
// to adopt, so the replica contains them all. Condition 3 rules out any write
// since then that failed to replay, and condition 2 rules out any write that
// bumped the counter but has not resolved yet. A failure noted after this
// call returns is not this answer's business: like every read of a live
// system, this is a point-in-time answer, and a caller that acts on it (a
// snapshot-file writer stamping a file) must re-check it after stamping,
// exactly as it must re-read the counter.
//
// # Liveness: every failure is guaranteed a rebuild that can resolve it
//
// Nothing above would matter if a failure could stay unresolved forever, so
// each of the three ways a failure is recorded is paired with a rebuild that
// will eventually adopt:
//
//   - A bump failure whose write commits: ensureBumped records a ChangeSet
//     fallback on the same scope, so that write's Apply calls enterFallback,
//     which starts the recovery rebuild (apply.go). Apply also requests one
//     directly when it settles a failure, so this does not depend on that
//     pairing being maintained.
//   - A bump failure whose write failed: ResolveAbandonedWrite requests the
//     rebuild itself (its own doc).
//   - A DDL failure: Start runs boot load immediately afterwards (boot.go),
//     and noteSelfSettlingWatermarkFailure has already settled it, so that
//     very first adoption resolves it.
//
// Two rebuild-scheduling races are closed elsewhere: a request that arrives
// while a rebuild loop is already running is a no-op (claimRebuildLoop), and
// that running loop's own adoption may predate the settle it needed to see --
// so finishFallbackRebuild relaunches whenever settledDirtyGen is still ahead
// of resolvedDirtyGen (apply.go), the same recheck it already performs for a
// state that raced back into fallback.
//
// A disabled engine (cfg.Enabled false) is the one place a failure can stay
// unresolved indefinitely: it never rebuilds at all (claimRebuildLoop), so
// nothing can ever adopt. That is the correct answer rather than a gap -- an
// engine that never serves and never rebuilds has no basis on which to call
// anything trustworthy -- and it is also why this reports false, not true, in
// that case.
func (e *Engine) WatermarkTrusted(ctx context.Context) bool {
	dirtyGen, resolvedGen := e.watermarkGens()
	if dirtyGen != resolvedGen {
		// Checked before the live pg read below, and short-circuited rather
		// than folded into the single decision at the end: the overwhelmingly
		// common case is that no failure was ever noted (both generations
		// zero), and a distrusted engine has nothing to learn from the round
		// trip anyway.
		return false
	}

	_, converged := e.watermarkConverged(ctx)
	return watermarkTrustedFor(dirtyGen, resolvedGen, converged, e.state.Load())
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

// WatermarkConverged is watermarkConverged's exported form, added purely for
// test observability -- mirroring ApplyCount/RebuildCount/Fresh's identical
// role (engine.go's own doc on each): the root package's own integration
// suites drive writes through the real Driver.WriteTransaction/
// BatchOperation/Run wiring (write_observer.go's ensureBumped/
// resolveAbandonedWrite), which this package cannot exercise directly without an
// import cycle (this file's own package-placement constraint -- see
// apply_integration_test.go's doc for the identical reasoning applied to
// Apply's own write-shape suite), and need a way to assert convergence from
// outside this package without reaching into the unexported method above.
func (e *Engine) WatermarkConverged(ctx context.Context) (uint64, bool) {
	return e.watermarkConverged(ctx)
}
