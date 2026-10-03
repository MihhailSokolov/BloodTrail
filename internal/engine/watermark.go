// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
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
// pg's current one and refuse a file that is behind. The counter alone
// cannot see a writer that does not bump it; watermarkLineageDDL is what
// covers those.
const watermarkDDL = `
create table if not exists bloodtrail_watermark (
	id smallint primary key default 1 check (id = 1),
	counter bigint not null default 0,
	updated_at timestamptz not null default now()
);
insert into bloodtrail_watermark (id) values (1) on conflict do nothing;
`

// watermarkLineageDDL gives bloodtrail_watermark its lineage column: a
// random uuid naming the run of counter values the table is currently in.
// ensureWatermarkTable runs it right after watermarkDDL, as a statement of
// its own, so a database that cannot run it (gen_random_uuid needs
// PostgreSQL 13) loses only the snapshot file -- which then has no lineage
// to be written or adopted under -- never the counter every write bumps.
//
// The installer ends lineages with its own statement over the same table
// and columns (internal/dbswitch's endLineageSQL); renaming anything here
// means renaming it there, and lineage_integration_test.go, which runs that
// statement against this table, is what fails if only one side changes.
//
// It reads the catalog before it alters anything, because ALTER TABLE takes
// an ACCESS EXCLUSIVE lock before it ever gets to IF NOT EXISTS: run bare on
// every start, it would queue BloodHound's startup behind any reader of the
// table -- a pg_dump holds every table for as long as the dump runs --
// where watermarkDDL itself takes no lock such a reader blocks. With the
// check, only the one start that actually adds the column takes that lock,
// and it gives up after lock_timeout rather than hold startup hostage: all
// that costs is the snapshot file until a later start adds the column.
//
// # Why a counter needs a lineage
//
// The counter proves a snapshot file complete only against writes that bump
// it, and only BloodTrail's own driver does. Anything else that writes the
// graph -- the stock BloodHound image `bloodtrail rollback` restores,
// BloodHound's Neo4j migrator, the TRUNCATE behind `bloodtrail install
// --replace-postgres-graph`, a different database behind the same snapshot
// directory -- changes PostgreSQL without moving the counter, and a file
// stamped with the counter's unchanged value then reads as exactly current.
// The boot adopts it and serves a graph PostgreSQL no longer holds, with
// nothing to ever correct it: the process's own later saves stamp the same
// stale replica again.
//
// A counter value therefore means something only within its lineage. Every
// snapshot records the lineage its contents were read in
// (snapshot.Snapshot.WatermarkLineage: stamped by the load, loadSnapshot,
// inside the same repeatable-read transaction as the rows; carried by Fold;
// written into the file), and the boot adopts a file only while PostgreSQL
// is still in that lineage (adoptSnapshotFileView). A lineage ends by being
// replaced with a fresh random uuid, never reused, so no counter value
// counted before can vouch for anything after:
//
//   - the column's default gives a table created from scratch -- a new or
//     reset database -- a lineage of its own;
//   - `bloodtrail install` and `bloodtrail rollback` end it
//     (internal/dbswitch's EndWatermarkLineage) around every stretch in
//     which a writer that does not bump the counter owns the graph.
//
// The end has to fall between the last load a BloodTrail process made
// before such a writer touched the graph and the first load one makes after
// the writer is done: every file from the old lineage then names a lineage
// PostgreSQL has left, and nothing loaded in the new one can predate the
// writer's writes. The installer ends it on both sides of the stock image's
// time -- once the rollback has the stock image running, and again right
// before an install starts BloodTrail. Anything else that writes the graph
// outside BloodTrail takes on the same obligation: end the lineage
// (`update bloodtrail_watermark set lineage = gen_random_uuid()`) or delete
// the snapshot file before BloodTrail loads again. That includes BloodHound's
// own tool API switching a running server to the plain pg driver
// (/graph-db/switch/pg), and restoring a database backup -- even one that
// restores the very lineage a file names: the bump commits in a transaction
// of its own before the write it guards (BumpWatermark), so a dump taken
// while writes land can hold counter N without write N, and a file stamped N
// would then vouch for a write the restored database never saw.
const watermarkLineageDDL = `
do $$
begin
	if not exists (select 1 from pg_attribute
	               where attrelid = to_regclass('bloodtrail_watermark') and attname = 'lineage' and not attisdropped) then
		perform set_config('lock_timeout', '2s', true);
		alter table bloodtrail_watermark add column if not exists lineage uuid not null default gen_random_uuid();
	end if;
end
$$;
`

// selectWatermarkLineageSQL reads the lineage bloodtrail_watermark is in --
// the rebuild's read inside its own load transaction (loadSnapshot).
const selectWatermarkLineageSQL = `select lineage from bloodtrail_watermark where id = 1`

// selectWatermarkAndLineageSQL reads the counter together with the lineage
// it counts in, from the one row in one statement: the snapshot-file boot's
// frozen target (readWatermarkAndLineage).
const selectWatermarkAndLineageSQL = `select counter, lineage from bloodtrail_watermark where id = 1`

// sequencePositionsSQL is where PostgreSQL's node and edge id sequences
// stand -- the last value each handed out, 0 for one never used -- as two
// select-list expressions. A database whose graph tables do not exist yet
// (a start ahead of the first AssertSchema) reads as 0 rather than failing.
//
// These are the positions a snapshot file is stamped with
// (saveSnapshotProbe) and the ones Start captures before this process can
// write anything (captureStartState); insertedSinceFile says why they are
// worth comparing.
const sequencePositionsSQL = `
	coalesce(case when to_regclass('node') is null then null
		else pg_sequence_last_value(pg_get_serial_sequence('node', 'id')::regclass) end, 0),
	coalesce(case when to_regclass('edge') is null then null
		else pg_sequence_last_value(pg_get_serial_sequence('edge', 'id')::regclass) end, 0)`

// startState is where PostgreSQL stood when this process started, before it
// could write anything: the watermark counter and the id sequence positions
// (captureStartState).
type startState struct {
	counter          uint64
	nodeSeq, edgeSeq int64
}

// startStateTimeout bounds captureStartState's one read. Start runs it on
// the caller's own context, which may carry no deadline at all, and a read
// that hangs must not hold BloodHound's startup with it: giving up only
// costs the check it feeds.
const startStateTimeout = 5 * time.Second

// captureStartState records where PostgreSQL stood before this process could
// write anything -- the watermark counter and the id sequence positions, in
// one statement -- for insertedSinceFile to compare a snapshot file's stamp
// against. Start calls it before it returns, and so before Open can hand the
// caller a driver to write through: every write this process makes lands
// after the capture, every write before it was someone else's.
//
// A failed read leaves nothing captured, which only switches that one check
// off: the watermark lineage and counters still stand between any file and
// adoption, as they did before this check existed. Logged at Warn so the
// weaker boot is visible.
func (e *Engine) captureStartState(ctx context.Context) {
	if e.pool == nil {
		return
	}
	readCtx, cancel := context.WithTimeout(ctx, startStateTimeout)
	defer cancel()

	var s startState
	if err := e.pool.QueryRow(readCtx, `select counter, `+sequencePositionsSQL+` from bloodtrail_watermark where id = 1`).
		Scan(&s.counter, &s.nodeSeq, &s.edgeSeq); err != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: could not record where PostgreSQL stood at start; a snapshot file will not be checked for rows inserted behind the watermark",
			slog.Any("error", err))
		return
	}
	e.atStart.Store(&s)
}

// readSequencePositions reads where the node and edge id sequences stand
// (sequencePositionsSQL): a snapshot file's own stamp.
func (e *Engine) readSequencePositions(ctx context.Context) (nodeSeq, edgeSeq int64, err error) {
	if e.pool == nil {
		return 0, 0, ErrWatermarkUnavailable
	}
	if err := e.pool.QueryRow(ctx, `select `+sequencePositionsSQL).Scan(&nodeSeq, &edgeSeq); err != nil {
		return 0, 0, fmt.Errorf("engine: read id sequence positions: %w", err)
	}
	return nodeSeq, edgeSeq, nil
}

// insertedSinceFile reports whether rows were inserted after a snapshot file
// was stamped by a writer the watermark counter never saw -- the stock
// driver after BloodHound's tool API switched a running server to it, the
// stock image started by hand, an INSERT from psql: the counter still read,
// when this process started, exactly what the file was stamped with, so
// nothing BloodTrail counts was written in between, yet the id sequences
// had moved.
//
// Sound as a refusal and nothing more. Every write BloodTrail makes bumps
// the counter before it can touch a sequence (BumpWatermark) -- or, when
// its bump fails, takes the snapshot file out of play first
// (NoteWatermarkBumpFailure) -- so a sequence that moved while the counter
// did not moved for someone else; and a write of this process cannot be
// what moved it, since start is captured before this process can write at
// all. It sees only inserts that draw their ids from those sequences, and
// only those made after the file was saved: an UPDATE or DELETE from
// outside BloodTrail leaves the sequences where they were, and an insert
// made while the process that saved the file was still running is behind
// the positions it stamped, which is why the lineage (watermarkLineageDDL)
// and the obligation it documents still stand. A sequence that moved for
// any other reason -- one reset, a crash that let PostgreSQL skip ahead, or
// a second BloodTrail process writing while this one started -- costs a
// rebuild, never a wrong adoption.
//
// A nil at -- nothing captured -- reports false: the check is then simply
// not made (captureStartState's doc).
func insertedSinceFile(stamp snapshot.Stamp, at *startState) bool {
	return at != nil && at.counter == stamp.Watermark &&
		(at.nodeSeq != stamp.NodeIDSeq || at.edgeSeq != stamp.EdgeIDSeq)
}

// counterBehindFile reports whether PostgreSQL's watermark counter, when this
// process started, was behind the value a snapshot file is stamped with.
// Within one lineage the counter only ever advances, so that is PostgreSQL
// gone back in time -- a backup restored without ending the lineage, whose
// rows predate some of the writes the file holds. No sequence of BloodTrail
// writes produces it, and none can repair it: this process's own boot writes
// carry the counter back up past the stamp, and at the value the file
// claims they are different writes from the file's. The boot gap cover
// cannot tell those apart once the counter has caught up (a boot write
// bumped, not yet applied, reads as a quiet restart), so the file is
// refused on the start state alone, before any counter is weighed.
//
// A nil at -- nothing captured -- reports false, like insertedSinceFile: the
// boot gap cover's own contradiction check (bootGapCoveredAt) is what is
// left then.
func counterBehindFile(stamp snapshot.Stamp, at *startState) bool {
	return at != nil && at.counter < stamp.Watermark
}

// Snapshot-file refusals fileRefusal can name, each a "reason" on the
// "bloodtrail: snapshot file rejected" line.
const (
	reasonLineageChanged        = "watermark lineage changed since the file was written"
	reasonCounterBehindFile     = "the watermark counter was behind the file's stamp when this process started"
	reasonInsertedBehindCounter = "rows were inserted since the file was written by a writer that did not advance the watermark"
)

// fileRefusal is why a snapshot file must be refused before its counters
// are weighed at all, or "" when nothing about its lineage or stamp rules it
// out: a file from another lineage than the one PostgreSQL is in (pgLineage;
// watermarkLineageDDL); one stamped ahead of where the counter stood when
// this process started (counterBehindFile); or one whose stamp shows rows
// inserted behind the counter's back (insertedSinceFile). The boot asks it
// twice: of the file's unverified header, to spare the read of a file it
// would refuse anyway, and of what ReadSnapshotFile verified, which is the
// answer adoption rests on.
func (e *Engine) fileRefusal(lineage snapshot.Lineage, stamp snapshot.Stamp, pgLineage snapshot.Lineage) (reason string, attrs []any) {
	if lineage.IsZero() || lineage != pgLineage {
		return reasonLineageChanged, []any{
			slog.String("file_lineage", lineage.String()),
			slog.String("pg_lineage", pgLineage.String()),
			slog.Uint64("file_watermark", stamp.Watermark),
		}
	}
	at := e.atStart.Load()
	if counterBehindFile(stamp, at) {
		return reasonCounterBehindFile, []any{
			slog.Uint64("file_watermark", stamp.Watermark),
			slog.Uint64("start_watermark", at.counter),
		}
	}
	if insertedSinceFile(stamp, at) {
		return reasonInsertedBehindCounter, []any{
			slog.Uint64("file_watermark", stamp.Watermark),
			slog.Int64("file_node_id_seq", stamp.NodeIDSeq),
			slog.Int64("start_node_id_seq", at.nodeSeq),
			slog.Int64("file_edge_id_seq", stamp.EdgeIDSeq),
			slog.Int64("start_edge_id_seq", at.edgeSeq),
		}
	}
	return "", nil
}

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
// The lineage column (watermarkLineageDDL) is added afterwards, and its
// failure is only logged: it guards no write either, and the one thing a
// missing column costs -- no snapshot loaded from this database has a
// lineage, so none is written to a file and no file is adopted -- is
// already the fail-closed outcome, with nothing for a trust generation to
// add.
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
		return
	}
	if _, err := e.pool.Exec(ctx, watermarkLineageDDL); err != nil {
		e.cfg.Log.WarnContext(ctx, "bloodtrail: watermark lineage DDL failed; no snapshot file will be written or adopted",
			slog.Any("error", err))
	}
}

// bumpWatermarkSQL atomically increments bloodtrail_watermark's one row and
// returns the new value in the same round trip -- BumpWatermark's entire
// pg-facing implementation.
const bumpWatermarkSQL = `update bloodtrail_watermark set counter = counter + 1, updated_at = now() where id = 1 returning counter`

// selectWatermarkCounterSQL reads the counter alone: ReadWatermark's live
// read, and the rebuild's read inside its own load transaction
// (loadSnapshot).
const selectWatermarkCounterSQL = `select counter from bloodtrail_watermark where id = 1`

// BumpWatermark atomically increments the pg watermark counter and returns
// its new value. The bump is counted in e.inflightBumps BEFORE its UPDATE is
// even sent, and so before it can commit -- let alone before the write it
// guards reaches PostgreSQL itself. Both orderings carry weight:
//
//   - Calling this EAGERLY, before a write's own pg effect (every call site:
//     write_observer.go's ensureBumped, for the first mutating call of a
//     transaction/batch, and driver.go's own top-of-method call for
//     Run/WipeGraph/SetDefaultGraph/DeleteNodesByKinds/
//     DeleteRelationshipsByKinds), is what makes the counter advance even
//     for a write that goes on to fail or roll back.
//   - Counting it before the UPDATE is what leaves no instant at which the
//     counter holds this bump's value while nothing in this engine does. A
//     bump counted only once its UPDATE returned was invisible for the whole
//     round trip after its commit -- a response on the wire, a descheduled
//     goroutine -- and a convergence check in that window could find every
//     other value resolved and nothing in flight, then vouch for this one
//     (watermarkConverged's doc).
//
// A failed UPDATE is counted out again before returning. Its value may still
// have committed -- a response lost on a dropped connection -- which leaves
// a value no write of this engine resolves: the ledger then cannot account
// for it (watermarkLedger), convergence stays false, and the failure's own
// handling (NoteWatermarkBumpFailure: the file removed, a fallback, the
// rebuild whose adoption rebases the ledger) is what restores it.
//
// The UPDATE runs on the write path's own pool (writePathPool), never on
// e.pool: its caller is usually inside a transaction or batch that holds
// one of e.pool's connections already.
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

	e.inflightBumps.Add(1)
	var counter uint64
	if err := e.writePool.get(e.pool, e.cfg.Log).QueryRow(ctx, bumpWatermarkSQL).Scan(&counter); err != nil {
		e.inflightBumps.Add(-1)
		return 0, fmt.Errorf("engine: BumpWatermark: %w", err)
	}
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
	if err := e.pool.QueryRow(ctx, selectWatermarkCounterSQL).Scan(&counter); err != nil {
		return 0, fmt.Errorf("engine: ReadWatermark: %w", err)
	}
	return counter, nil
}

// readWatermarkAndLineage reads the pg watermark counter together with the
// lineage it is counting in (watermarkLineageDDL), from the one row in one
// statement, so the pair describes a single moment: the snapshot-file
// boot's frozen target (adoptSnapshotFileView). Returns
// ErrWatermarkUnavailable when e.pool is nil, mirroring ReadWatermark; a
// database without the lineage column fails the read, which the boot treats
// like any other failed watermark read -- the file is refused.
func (e *Engine) readWatermarkAndLineage(ctx context.Context) (uint64, snapshot.Lineage, error) {
	if e.pool == nil {
		return 0, snapshot.Lineage{}, ErrWatermarkUnavailable
	}

	var (
		counter uint64
		lineage [16]byte
	)
	if err := e.pool.QueryRow(ctx, selectWatermarkAndLineageSQL).Scan(&counter, &lineage); err != nil {
		return 0, snapshot.Lineage{}, fmt.Errorf("engine: read watermark and lineage: %w", err)
	}
	return counter, snapshot.Lineage(lineage), nil
}

// NoteWatermarkBumpFailure logs (Warn) that scope's own eager bump genuinely
// failed -- not ErrWatermarkUnavailable, which never reaches this method at
// all (see its own doc) -- and, the FIRST time this is called for scope,
// opens a new watermark trust generation for it: e.dirtyGen advances, which
// makes WatermarkTrusted report false from this instant on, and scope is
// marked so that this same failure can be settled exactly once later, when
// the write it guards reaches a final outcome in PostgreSQL
// (settleWatermarkFailure's own doc).
//
// Reports whether it actually opened a generation (true on that first call,
// false on every later one for the same scope), so the caller can decide
// whether there is anything new to record about it (write_observer.go's
// ensureBumped's own doc, on the ChangeSet fallback it conditions on this).
//
// A LATER call for a scope whose mark is already set is expected, not a
// bug: a write scope whose eager bump fails once has no way to become
// bumped=true (SetWatermark is never called), so ensureBumped's own guard --
// "bump exactly once, then skip" -- keeps retrying the SAME failing bump on
// every later mutating call the transaction/batch makes, and each retry
// fails again. Advancing e.dirtyGen on every such retry, rather than once
// per scope, would open N generations for one underlying failure while
// settleWatermarkFailure's consume-once mark only ever settles one of them --
// settledDirtyGen could then never catch up to dirtyGen, permanently
// distrusting the engine under this failure mode's ordinary shape (a
// transaction that keeps writing after its first bump fails, or a batch with
// several calls). Gating on markWatermarkBumpFailed's own transition report
// is what keeps dirtyGen's per-scope count exactly paired with
// settledDirtyGen's per-scope count, one increment each, no matter how many
// times that scope's bump is retried.
//
// Both halves -- the generation and the mark -- have to happen here, at the
// one call site (write_observer.go's ensureBumped) that knows both the
// engine and the write's own scope: the generation is what withdraws trust,
// and the mark is the only thing that can ever give it back.
//
// A nil scope still advances dirtyGen on every call, deliberately -- there is
// no mark to gate on, and no way to ever settle it, so the engine stays
// distrusted for the rest of its life. No caller passes nil (ensureBumped
// returns early on a nil scope before it ever attempts a bump); fail-closed
// is the right answer if one ever did.
func (e *Engine) NoteWatermarkBumpFailure(ctx context.Context, scope *WriteScope, err error) bool {
	e.cfg.Log.WarnContext(ctx, "bloodtrail: watermark bump failed", slog.Any("error", err))

	if scope == nil || scope.markWatermarkBumpFailed() {
		e.dirtyGen.Add(1)
		// The write this bump was guarding is about to reach PostgreSQL
		// uncounted, which makes any saved snapshot file a trap. Its
		// watermark stamp equals what PostgreSQL's counter still reads, so
		// a boot after a hard stop -- before the recovery rebuild that
		// would have healed this ever ran -- finds a zero-sized gap,
		// declares the file fully covered, and adopts it as trusted,
		// permanently missing this write's committed rows. The in-memory
		// generation advanced just above says nothing to that next
		// process, and nothing in the file's header does either.
		//
		// Removing the file is the one durable statement available without
		// a format change, and it is purely local: it still works when the
		// database is exactly what has become unreachable. The cost is one
		// slow boot (a full PostgreSQL rebuild), which is the correct
		// trade against serving a graph that silently lacks a committed
		// write.
		e.invalidateSnapshotFile(ctx)
		return true
	}
	return false
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
// one scope -- still advances the counter exactly once. That is only half of
// what makes "settledDirtyGen == dirtyGen means every failure has settled" a
// sound reading of two independent counters: the other half is
// NoteWatermarkBumpFailure's matching once-per-scope gate on dirtyGen (its
// own doc), without which a scope whose bump keeps failing on every retry
// would open one dirtyGen generation per retry that this method's own
// consume-once mark could never settle more than one of. With both halves in
// place, each counter moves by exactly one per scope that ever fails its
// bump, however many times that scope's own mutating calls retry it.
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
// runs -- which is also why the request goes through requestTrustRebuild's
// rate limiter (apply.go) rather than launching directly: a sustained stream
// of errored writes with failed bumps must not run full snapshot loads back
// to back on an engine that is serving correctly.
func (e *Engine) ResolveAbandonedWrite(ctx context.Context, scope *WriteScope) {
	if scope == nil {
		return
	}

	if counter, bumped := scope.Watermark(); bumped {
		e.AdvanceWatermark(counter)
	}

	// The boot gap buffer needs this write's counter too (counter-only --
	// there is nothing to replay for a write that committed nothing), or a
	// rolled-back boot write would leave a hole in the counter sequence and
	// force the snapshot file's rejection for no reason
	// (observeAbandoned's own doc, bootgap.go).
	e.bootGap.observeAbandoned(scope)

	if e.settleWatermarkFailure(scope) {
		e.cfg.Log.DebugContext(ctx, "bloodtrail: watermark failure settled by a write that produced no effect; rebuild requested to restore trust")
		e.requestTrustRebuild()
	}
}

// maxWatermark returns the larger of cur and candidate: the monotonic max an
// adoption folds resolvedDirtyGen through (adoptRebuiltView,
// adoptSnapshotFileView), extracted as a pure function (mirroring apply.go's
// fallbackRetryDelay) purely so it has a direct, atomic-free unit test.
func maxWatermark(cur, candidate uint64) uint64 {
	if candidate > cur {
		return candidate
	}
	return cur
}

// maxLedgerRanges bounds how many separate runs of resolved values a
// watermarkLedger keeps above its gaps: 16 bytes each, 64 KiB at the cap.
// Runs only pile up behind values that no write of this process is going to
// resolve -- another writer's counters, interleaved with this process's own
// -- since a gap an in-flight write of this process leaves is filled as soon
// as that write resolves, and everything resolved behind it merges into one
// run meanwhile. Past the cap the ledger stops keeping the runs at all
// (lost): it could not prove convergence over those gaps anyway, and a
// rebase that covers everything it dropped is what makes it exact again.
const maxLedgerRanges = 4096

// counterRange is a run of consecutive resolved counter values, lo through hi.
type counterRange struct{ lo, hi uint64 }

// watermarkLedger records which watermark counter values this engine can
// account for, as e.appliedWatermark: every value up to through, and the
// runs of resolved values above it (above). A value is accounted for when
// this process resolved the write that bumped it (resolve: Apply, or
// ResolveAbandonedWrite for a write that committed nothing), or when a
// rebase covered it (rebase: an adopted snapshot that holds every write
// whose bump committed at or below its floor, except writes of this process
// still in flight -- which inflightBumps holds instead).
//
// Convergence needs the CONTIGUOUS prefix, not the highest value resolved.
// Counters are handed out in bump order, not resolved in it, and not every
// value is this process's: a value another BloodTrail server bumped, or one
// whose bump response this process lost, is never resolved here at all. The
// highest resolved value reaches PostgreSQL's counter the moment any later
// write of this process resolves -- absorbing every such value beneath it --
// while through stops at the first gap until a write fills it or a rebase
// covers it (watermarkConverged).
//
// A rebase covers another writer's values only as far as the adopted
// snapshot holds their writes: one committed before the load's transaction
// began is in it, but one whose bump committed and whose own write had not
// yet -- invisible from here -- is covered without being held. That is the
// limit of what one process can see of another's writes, which is why the
// supported deployment is one BloodTrail server per database; what the
// ledger guarantees is that such a writer's values are never absorbed by
// this process's own writes, so a save refuses them (and says so,
// saveSnapshotProbe) until a rebuild has loaded what they wrote.
//
// Safe for concurrent use; the zero value accounts for nothing but 0, the
// counter's own starting value.
type watermarkLedger struct {
	mu sync.Mutex

	// through is the highest value such that every value at or below it is
	// accounted for.
	through uint64

	// above holds the resolved values above through+1: sorted, disjoint,
	// non-adjacent runs, never more than maxLedgerRanges of them.
	above []counterRange

	// lost is set when above outgrew maxLedgerRanges and was dropped: from
	// then on the ledger cannot say which values above through are
	// resolved, and reports itself inexact until a rebase to at least
	// highest makes that question moot again.
	lost bool

	// highest is the highest value ever resolved -- how far a rebase has to
	// reach to cover everything a lost ledger dropped.
	highest uint64
}

// resolve records that this process resolved counter's write.
func (l *watermarkLedger) resolve(counter uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if counter > l.highest {
		l.highest = counter
	}
	if counter <= l.through || l.lost {
		return
	}
	l.insertLocked(counter)
	l.advanceLocked()
}

// rebase records that every value at or below floor is accounted for:
// called under an adoption, with the counter the adopted snapshot is
// complete for (rebuildOnce, adoptSnapshotFileAttempt). A floor below
// through changes nothing -- the values above it are resolved already.
func (l *watermarkLedger) rebase(floor uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if floor > l.through {
		l.through = floor
	}
	if l.lost {
		if floor < l.highest {
			return
		}
		l.lost = false
	}
	l.advanceLocked()
}

// resolvedThrough reports through, and whether it is exact: false while the
// ledger is lost, when values above through may be resolved without the
// ledger knowing which.
func (l *watermarkLedger) resolvedThrough() (through uint64, exact bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.through, !l.lost
}

// insertLocked adds counter, known to be above through and not lost, to
// above: merged into a neighboring run where it extends one, dropped where a
// run already holds it. Callers hold mu.
func (l *watermarkLedger) insertLocked(counter uint64) {
	i := sort.Search(len(l.above), func(i int) bool { return l.above[i].lo > counter })
	if i > 0 && l.above[i-1].hi >= counter {
		return
	}
	extendsPrev := i > 0 && l.above[i-1].hi+1 == counter
	extendsNext := i < len(l.above) && l.above[i].lo == counter+1
	switch {
	case extendsPrev && extendsNext:
		l.above[i-1].hi = l.above[i].hi
		l.above = append(l.above[:i], l.above[i+1:]...)
	case extendsPrev:
		l.above[i-1].hi = counter
	case extendsNext:
		l.above[i].lo = counter
	default:
		if len(l.above) >= maxLedgerRanges {
			l.above = nil
			l.lost = true
			return
		}
		l.above = append(l.above, counterRange{})
		copy(l.above[i+1:], l.above[i:])
		l.above[i] = counterRange{lo: counter, hi: counter}
	}
}

// advanceLocked moves through past every run that now starts at or below
// through+1, dropping them. Callers hold mu.
func (l *watermarkLedger) advanceLocked() {
	n := 0
	for n < len(l.above) && l.above[n].lo <= l.through+1 {
		if l.above[n].hi > l.through {
			l.through = l.above[n].hi
		}
		n++
	}
	if n > 0 {
		l.above = append(l.above[:0], l.above[n:]...)
	}
}

// AdvanceWatermark resolves one bumped scope's counter: it records counter
// in e.appliedWatermark (watermarkLedger.resolve) and only then retires
// exactly one e.inflightBumps entry -- in that order, so a convergence read,
// which loads inflightBumps before the ledger, can never see this bump
// neither in flight nor resolved (watermarkConverged's doc). No pg round
// trip, and, despite Apply's own call site running it under applyMu, nothing
// that REQUIRES that lock: the ledger has its own mutex, and inflightBumps is
// atomic.
//
// Resolving in any order is safe: two bumped scopes can finish in either
// order (the one issued first is not guaranteed to finish first), and the
// ledger keeps a value resolved out of order until the values beneath it
// resolve too, without ever claiming the gap between them.
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
	e.appliedWatermark.resolve(counter)
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
//  2. The counter bookkeeping is caught up with PostgreSQL: e.appliedWatermark
//     accounts for every value up to the pg counter and no bumped scope is
//     still unresolved (watermarkConverged, a live pg read -- see its own
//     doc for why both of ITS halves are needed).
//  3. The replica is trustworthy right now: state == stateServing, i.e. no
//     write is currently known to have failed to replay.
//
// Condition 1 is actually sampled TWICE -- once before conditions 2 and 3 are
// read, and once again after, requiring both samples to agree -- rather than
// once. The implementation (below) explains why; it costs two atomic loads
// and touches nothing in the argument above, which only ever needed
// condition 1 to hold at some instant covered by the other two.
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
// state that raced back into fallback. Trust-only launches -- this recheck's
// generations case, and both settle-time requests above -- are rate-limited
// to one per trustRebuildMinInterval (requestTrustRebuild, apply.go); that
// delays resolution, never loses it: the limiter's delayed launcher re-checks
// and relaunches through the same path, so every unresolved generation still
// gets its adopted rebuild.
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
	state := e.state.Load()

	// Re-read the same pair, in the same resolved-then-dirty order
	// (watermarkGens' own doc, load-bearing there too), AFTER the live pg
	// round trip and the state load above, and require it to still match
	// the pair sampled before them. Neither watermarkConverged nor
	// e.state.Load() consult the generations at all, so a failure noted
	// while either of those two calls was in flight -- opening a new
	// generation this decision would otherwise never learn about -- would
	// go unnoticed by the single-sample version: dirtyGen == resolvedGen
	// checked above could already be stale by the time converged/state are
	// read moments later. Comparing both reads costs two more atomic loads
	// and narrows that window to nothing, for free: it does not touch the
	// proof above (which only ever needed dirtyGen == resolvedGen to hold at
	// SOME instant covered by the other two conditions), since agreeing
	// twice is strictly stronger evidence of that than agreeing once, never
	// weaker.
	dirtyGen2, resolvedGen2 := e.watermarkGens()
	if dirtyGen2 != dirtyGen || resolvedGen2 != resolvedGen {
		return false
	}

	return watermarkTrustedFor(dirtyGen, resolvedGen, converged, state)
}

// adoptRebuiltViewAndRebase is rebuildOnce's adoption: adoptRebuiltView,
// and -- only if the view was adopted and its load read the counter -- a
// rebase of the watermark ledger to that counter. An adopted snapshot holds
// every write whose bump committed at or below the counter its load read,
// bar this process's own writes still in flight (loadedWatermark's doc), so
// the ledger accounts for every such value from here on, including ones no
// write of this process will ever resolve: that is what lets convergence
// recover from a lost bump response, or from another writer's counter, once
// a rebuild has loaded their writes. A refused snapshot vouches for nothing,
// so there is no rebase without the adoption.
//
// The two are one step for convergence reads (watermarkRebaseMu), and the
// lock is released however adoptRebuiltView returns, a panic included: held
// past it, it would stall every convergence read -- every snapshot save with
// it -- for good.
func (e *Engine) adoptRebuiltViewAndRebase(ctx context.Context, view *snapshot.View, epoch, settledGen uint64, loaded loadedWatermark) bool {
	e.watermarkRebaseMu.Lock()
	defer e.watermarkRebaseMu.Unlock()

	if !e.adoptRebuiltView(ctx, view, epoch, settledGen) {
		return false
	}
	if loaded.counterOK {
		e.appliedWatermark.rebase(loaded.counter)
	}
	return true
}

// watermarkConvergedFor is watermarkConverged's pure comparison, extracted
// for unit testing without a live pg read: true iff no bumped scope is
// still unresolved (inflight == 0) and pgCounter equals applied, the value
// through which the ledger accounts for every counter. See
// watermarkConverged's own doc for why both conditions are necessary.
func watermarkConvergedFor(pgCounter, applied uint64, inflight int64) bool {
	return inflight == 0 && pgCounter == applied
}

// watermarkReading is one convergence read (readWatermarkConvergence):
// PostgreSQL's counter, then how many bumps were in flight, then how far the
// ledger accounts for every value -- taken in that order.
type watermarkReading struct {
	pgCounter uint64
	inflight  int64
	through   uint64
	exact     bool
}

// converged is the reading's verdict: nothing in flight, and every value up
// to PostgreSQL's counter accounted for (watermarkConverged).
func (r watermarkReading) converged() bool {
	return r.exact && watermarkConvergedFor(r.pgCounter, r.through, r.inflight)
}

// unaccounted reports that the counter holds a value at or below what was
// read that no write of this process is carrying or has resolved -- one this
// process never bumped (another BloodTrail server writing the same database)
// or one whose bump response it lost -- rather than a write of its own
// still being on its way.
func (r watermarkReading) unaccounted() bool {
	return r.inflight == 0 && (!r.exact || r.through < r.pgCounter)
}

// readWatermarkConvergence takes one convergence reading: see
// watermarkConverged's doc for what it proves and why its order matters.
func (e *Engine) readWatermarkConvergence(ctx context.Context) (watermarkReading, error) {
	pgCounter, err := e.ReadWatermark(ctx)
	if err != nil {
		return watermarkReading{}, err
	}

	e.watermarkRebaseMu.RLock()
	defer e.watermarkRebaseMu.RUnlock()
	r := watermarkReading{pgCounter: pgCounter, inflight: e.inflightBumps.Load()}
	r.through, r.exact = e.appliedWatermark.resolvedThrough()
	return r, nil
}

// watermarkConverged reports pg's current watermark counter, and whether
// this engine's own applied-side bookkeeping is fully caught up with it:
// true iff no bumped scope is still unresolved (e.inflightBumps == 0) and
// e.appliedWatermark accounts for every value up to the pg counter it just
// read (watermarkReading.converged).
//
// That is the promise a snapshot-file writer needs before it stamps a file
// with this counter: every write whose bump holds a value at or below it has
// had its own effect resolved -- committed and reflected in the replica, or
// rolled back and reconciled to nothing. It rests on three reads, taken in
// this order (readWatermarkConvergence):
//
//  1. PostgreSQL's counter, P. Every bump of this engine that committed at or
//     below P was counted in inflightBumps before its UPDATE was sent
//     (BumpWatermark), so before this read.
//  2. inflightBumps. Zero means each of those bumps has resolved since, and
//     a bump is recorded in the ledger before it leaves inflightBumps
//     (AdvanceWatermark).
//  3. The ledger: through == P means every value up to P is accounted for
//     -- resolved by this process, or covered by the rebase of an adopted
//     snapshot that holds its write.
//
// The ledger, not the highest counter resolved, is what makes the third read
// sound. Counters resolve out of order, and not every counter is this
// process's own: a value another BloodTrail server bumped for its own write,
// or one whose bump response this process lost, is never resolved here. A
// highest-resolved comparison absorbed every such value the moment any later
// write of this process resolved, and a file then vouched for a write its
// replica never saw -- another server's update, or this process's own write
// whose bump had committed but not yet returned. The ledger stops at the
// first value nobody accounted for (watermarkLedger), and a
// saveSnapshotProbe that finds such a value says so (unaccounted).
//
// The in-flight and ledger loads run under watermarkRebaseMu, so a rebuild's
// adoption and its ledger rebase (rebuildOnce) are one step to this read.
// A counter that moves after read 1 only makes the verdict more
// conservative: a bump committing later is either still in flight at read 2
// or resolved above P by read 3, and neither reads as converged at P.
//
// A ReadWatermark failure (including ErrWatermarkUnavailable) reports
// (0, false): "can't currently prove convergence" is always the safe
// answer when the live read itself didn't succeed.
func (e *Engine) watermarkConverged(ctx context.Context) (uint64, bool) {
	r, err := e.readWatermarkConvergence(ctx)
	if err != nil {
		return 0, false
	}
	return r.pgCounter, r.converged()
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
