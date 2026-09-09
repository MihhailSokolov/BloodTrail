# applybench

`applybench` benchmarks BloodTrail's **write-through path** -- `Engine.Apply`
(`internal/engine/apply.go`), run synchronously inside every mutating driver
call (`WriteTransaction`/`BatchOperation`/`Run`/..., see the root package's
`driver.go`) -- against a graph already loaded into PostgreSQL, normally by
[`bench/adgen`](../adgen).

Like [`bench/builderbench`](../builderbench) and
[`bench/cypherbench`](../cypherbench) (and unlike
[`bench/pathbench`](../pathbench), which constructs `internal/engine`
directly), `applybench` opens the *real* production driver --
`dawgs.Open(ctx, bloodtrail.DriverName, cfg)` -- for every measurement that
drives writes or reads through it: write-through and its serving path only
exist on that real path. Each measurement opens its own driver instance on
its own dedicated `*pgxpool.Pool`, with `BLOODTRAIL_ENGINE`/
`BLOODTRAIL_COMPACT_ENTRIES`/`BLOODTRAIL_COMPACT_BYTES`/
`BLOODTRAIL_SNAPSHOT_DIR` set however that measurement needs -- these are
process-wide environment variables read once at `dawgs.Open` time, so
`applybench` never runs two phases needing different settings concurrently.

```
go run ./bench/applybench -dsn <pg dsn> [-ingest 12000] [-flush 20000] [-repeats 3] [-queries 50] [-compact-entries 2000] [-cap 10m] [-enforce] [-cpuprofile <file>]
```

Or, against `BLOODTRAIL_TEST_PG` (this also runs `bench/adgen` first, forwarding `ARGS` to it):

```
make bench-apply ARGS='-users 50000'
```

## What it measures

### (a) Sustained apply throughput

Ingest-shaped writes -- `batch.UpdateNodeBy` objectid upserts for new `User`
nodes, `batch.UpdateRelationshipBy` upserts for their `MemberOf` edge to an
existing hub `Group` node, flushed via an explicit `batch.Commit()` every
`-flush` operations (default `20000`, matching production's write-flush
size) -- written `-repeats` times with the engine **on**
(`BLOODTRAIL_ENGINE=on`) and `-repeats` times with it **off**
(`BLOODTRAIL_ENGINE=off`). `applybench` reports both sides' total write wall
time (p50 over the repeats) and the engine's own apply+read-back overhead as
a percentage of the disabled baseline: `(on_p50 - off_p50) / off_p50 * 100`.

`BLOODTRAIL_ENGINE=off` is a true "no apply overhead" baseline, not merely
"a different code path that happens to be cheaper": `Engine.Apply` itself
gives up before any PostgreSQL round trip the instant it sees a disabled
engine (`internal/engine/apply.go`'s own doc, step 2) -- the only work both
sides still pay identically is the watermark bump every mutating call makes
regardless of whether the engine is enabled (`internal/engine/boot.go`'s
`Start` doc: watermark tracking runs "even when `!cfg.Enabled`"), so it
cancels out of the comparison.

### (b) Query latency during active ingest

A two-node `FetchAllShortestPaths` query (the same `graph.Criteria` shape
BloodHound's own API builds, and `bench/pathbench`'s pair-query shape)
sampled **continuously for as long as each engine-on ingest repeat runs**,
compared against an idle baseline -- the same query shape sampled `-queries`
times (default `50`) with no concurrent writes at all. Both sides report
p50/p95.

Compaction is deliberately disabled for this measurement
(`BLOODTRAIL_COMPACT_ENTRIES` set effectively unbounded) so the delta stays
populated and pre-compaction throughout -- exactly the state the task brief
calls for. (c) below measures compaction separately, with its own low
threshold.

### (c) Compaction duration at scale

A separate driver instance with `BLOODTRAIL_COMPACT_ENTRIES` set low
(`-compact-entries`, default `2000`) so several background compactions
actually fire while ingest-shaped writes keep flowing in. `applybench` has
no way to reach `internal/engine.Engine.CompactionCount()` from outside the
root package (it's a method on an unexported field of the root package's
`Driver`) -- so instead it installs a temporary `slog.Handler`
(`logCapture` in `main.go`) that captures the engine's own
`"bloodtrail: compaction finished"` log line, reading each occurrence's
already-computed `duration` attribute straight off the log record. This is
the same signal a real operator would have (structured logs), not a
synthetic test hook.

### (d) Snapshot file write and load duration at scale

`BLOODTRAIL_SNAPSHOT_DIR` pointed at a scratch directory. Each repeat:
times `Close()`'s `Stop`-then-`SaveSnapshot` sequence end to end (the
engine's own `"bloodtrail: snapshot file written"` log line, captured the
same way as (c), carries the exact fold+write duration too, and its
presence is what `applybench` checks to confirm the save actually
happened rather than being silently skipped by one of `SaveSnapshot`'s own
no-op preconditions -- see `internal/engine/persist.go`); then reads the
same file back directly via `internal/engine/snapshot.ReadSnapshotFile` --
the exact function `boot.go`'s own `tryLoadSnapshotFile` calls -- timed
directly, the same way `bench/builderbench`/`bench/cypherbench` measure
"build duration" via a direct `engine.LoadSnapshot` call rather than
watching the driver's own boot goroutine.

This does **not** open a fresh driver and wait for its boot-load goroutine
to adopt the file the way an earlier version of this measurement did:
`Start` (`boot.go`) launches that goroutine immediately, and its own first
(and only -- no retry schedule of its own) attempt to read
`e.pgDriver.DefaultGraph()` races `applybench`'s own `AssertSchema` call,
made only after `dawgs.Open` itself returns -- a race the boot-load
goroutine, which has no I/O of its own to do first, wins essentially every
time in practice. For the ordinary PostgreSQL-rebuild boot path that race
is harmless (the retry loop tries again a moment later), but the
one-shot file-load attempt gets no second chance, so a fresh driver open
essentially never exercises it at all -- confirmed by running exactly that
version of this measurement and watching it hang until `-cap`'s own
watchdog tripped. Reading the file directly sidesteps the race while still
measuring the operation that actually scales with file size.

Every measurement prints a human-readable line and a machine-greppable
`APPLYBENCH_*` summary line (`grep '^APPLYBENCH_'`), ending in
`APPLYBENCH_RESULT PASS` or `FAIL`.

## A scratch database constraint, created and dropped around every run

`batch.UpdateNodeBy`/`batch.UpdateRelationshipBy`, keyed by a bare
`"objectid"` identity property (exactly the shape (a) needs, and the shape
production BloodHound's own graphify ingestion uses), compile to `insert
... on conflict ((properties->>'objectid')) do update ...`. PostgreSQL
refuses that clause outright unless a matching unique index already
exists on the target graph partition. Production BloodHound's own schema
declares one; the schema this repo's benches and integration tests share
does not -- see `apply_integration_test.go`'s own
`TestApplyObjectIDUpsertServesNewNode` doc for why NOT adding one to that
long-lived, widely shared database is otherwise the right call (every
integration suite in this repo writes into the same `bloodtrail_test`
graph, and some fixtures intentionally create duplicate `objectid` values,
which a permanent index would then reject).

`applybench` needs the real upsert shape to exist regardless, so it
declares this constraint through dawgs' own schema-reconciliation
mechanism (`graph.Schema.DefaultGraph.NodeConstraints`, asserted by every
`AssertSchema` call this package makes -- see `main.go`'s `benchSchema`
doc), and drops it again once the run finishes, success or failure alike
(`main.go`'s `dropObjectIDUpsertIndex`). This is a best-effort,
self-cleaning setup step, not a permanent schema change -- but it is not
risk-free: if `applybench` is killed before its own deferred cleanup can
run (a `SIGKILL`, not a panic -- Go's `defer` still runs on a panic), the
constraint is left behind, and a later suite creating a genuine duplicate
`objectid` in this same graph would fail until it is dropped. The very
next `AssertSchema` call **any** tool in this repo makes against this
graph (none of which declare this constraint) drops it as an incidental
side effect of its own ordinary reconciliation -- a real safety net, but
not a guarantee. To drop it by hand immediately:

```sql
DROP INDEX IF EXISTS node_<graph id>_objectid_constraint;
```

(`<graph id>` is `bloodtrail_test`'s numeric id, e.g. `select id from graph
where name = 'bloodtrail_test'` -- dawgs derives the constraint's actual
name from the partition table name and the property name alone, so it is
not a fixed string; see `main.go`'s `objectIDConstraintName`.)

## `-enforce`

With `-enforce`, `applybench` exits nonzero if **either** of two bars is
missed:

| Measurement                     | Bar                                    |
|----------------------------------|-----------------------------------------|
| (a) apply overhead               | `<= 25%` of the engine-disabled write wall time |
| (b) during-ingest query p95      | `<= 3x` the idle p95                    |

**(c) and (d) are reported only, never enforced.** Both bars above, and the
absence of any bar on (c)/(d), are explained below.

**CI must never pass `-enforce`.** Any other failure (a database error, a
missing base graph, a phase exceeding its own `-cap` watchdog) aborts the run
with a nonzero exit regardless of `-enforce`.

### These two bars are PROVISIONAL

`applyOverheadMaxPct` (25%) and `idleP95Multiplier` (3x) are placeholders
chosen before any measurement at production scale existed -- **not**
evidence, unlike every cap in `bench/builderbench`'s and
`bench/cypherbench`'s own README cap tables. Per the task brief, a follow-up
task must:

1. Run `applybench -enforce` against a 5M-scale graph
   (`bench/adgen -users 2800000 -domains 4 -wipe`, matching the other
   benches' large-scale workflow), several times to see the honest spread
   under ordinary machine-load variance.
2. Replace both constants (`main.go`) with evidence-based bars: **measured
   worst case x ~1.75**, rounded -- the exact convention
   `bench/builderbench`'s `shapeThresholds` doc and
   `bench/cypherbench`'s `ridSuffixScanMinRatio`/`flagScanMinRatio` docs
   already establish and justify at length (a bar set from an invented
   number, rather than a measurement, has twice been proven undersized on
   this project -- see either README's own incident history).
3. Fill in this section with the measured numbers and the resulting bar,
   the same way those two READMEs' own "Measured at 5M" sections do.
4. Consider whether (c) and (d) deserve enforced bars of their own once
   their own 5M-scale numbers exist (this task deliberately left them
   unenforced -- see below).

### Why (c) and (d) have no bar at all yet

Compaction duration and snapshot file I/O duration both scale with the size
of what they're folding/serializing/parsing -- the base snapshot for (d),
the accumulated delta for (c) -- in a way this task's small-scale smoke run
cannot usefully bound. Rather than invent a placeholder number with no
measurement behind it at all (exactly the mistake the PROVISIONAL bars
above already are, once), this task leaves them reported-only; the
follow-up task's 5M run should give both a first real number to anchor a
cap to.

## Watchdog

Every phase that drives writes or waits on an asynchronous engine event (a
background compaction adopting, a boot-load goroutine loading a snapshot
file) is bounded by `-cap` (default `10m`), applied **per operation** via
`context.WithTimeout` wrapped directly around that operation -- the same
fail-fast convention `bench/cypherbench`'s and `bench/builderbench`'s own
`-bt-cap` use for their engine-side calls. Exceeding it always **aborts the
whole run** (nonzero exit, never a graceful "capped" data point the way
`-pg-cap`'s `pg_capped` is for those siblings -- there is no PostgreSQL
baseline half here to fall back on judging).

This is a hard requirement, not a nicety: an earlier 2026-09 5M-scale bench
run on this project hung for **17.5 hours** before being killed by hand,
because nothing was watching a wall clock at all (see
`bench/cypherbench`'s README/`defaultBTCap` doc for the full incident).
`applybench`'s own long-running write loop also prints progress every
`-flush` operations, and the compaction/snapshot-load waits print progress
every 2s while polling, so a genuinely slow (not hung) run is visibly making
progress -- "check the log cadence, not just liveness" -- rather than
looking indistinguishable from a wedged one. Never retry past a `-cap` trip
in a loop; investigate why the operation didn't finish instead.

## Usage at small scale (the smoke run)

```
go run ./bench/adgen -dsn "$BLOODTRAIL_TEST_PG" -users 50000 -wipe
go run ./bench/applybench -dsn "$BLOODTRAIL_TEST_PG"
```

or, equivalently, the single Makefile target:

```
make bench-apply ARGS='-users 50000'
```

`applybench`'s own defaults (`-ingest 12000 -flush 20000 -repeats 3 -queries
50 -compact-entries 2000`) are sized to finish comfortably in well under a
minute at this scale against ordinary hardware -- `-cap`'s 10-minute default
is a safety ceiling for this smoke run, not the expected runtime. Do **not**
pass `-enforce` at this scale for anything beyond validating the harness
itself runs end to end: 50,000 users is far too small a graph, and the two
enforced bars are themselves PROVISIONAL (see above) -- a pass or fail here
proves the harness works, not that the engine meets its real bar.

`applybench` requires a graph already loaded (it discovers an existing
`Group` node to use as every ingested user's `MemberOf` hub, and samples
existing `(User, Computer)` pairs for the latency queries) -- run
`bench/adgen` first, or use the Makefile target above, which does this for
you. `-wipe` truncates the `node`/`edge` tables for *every* graph in the
database; fine for a disposable test database, not for anything you care
about.

## At large scale (the 5M-node workflow)

Following `bench/adgen`'s own large-scale guidance:

```
go run ./bench/adgen -dsn <pg dsn> -users 2800000 -domains 4 -wipe
go run ./bench/applybench -dsn <pg dsn> -enforce
```

**Do not run this from within this task** -- see the task's own brief: a 5M
measurement is a follow-up task's job, and it needs the machine for a long
time. This section exists so that follow-up task does not have to guess the
invocation.

## Flags

| Flag               | Default   | Meaning                                                                                      |
|---------------------|-----------|------------------------------------------------------------------------------------------------|
| `-dsn`              | (none)    | PostgreSQL connection string. Required.                                                       |
| `-ingest`           | `12000`   | Ingest units written per repeat for (a)/(b) (1 unit = 1 new User node + 1 new MemberOf edge).  |
| `-flush`            | `20000`   | Batch flush size in operations (an explicit `batch.Commit()` every this many calls), matching production's write flush size. |
| `-repeats`          | `3`       | Repeats per measurement (p50 is taken over these); must be `>= 3`.                            |
| `-queries`          | `50`      | Queries sampled for the idle latency baseline (the during-ingest sample is unbounded by this -- it runs for as long as ingest does). |
| `-compact-entries`  | `2000`    | `BLOODTRAIL_COMPACT_ENTRIES` for measurement (c) only; deliberately low so compaction fires.  |
| `-seed`             | `1`       | Seed for deterministic query-pair sampling.                                                    |
| `-cap`              | `10m`     | Per-operation wall-clock watchdog; ABORTS THE WHOLE RUN on expiry (nonzero exit) -- see "Watchdog" above. |
| `-enforce`          | `false`   | Exit nonzero on a missed (a)/(b) bar (never pass this in CI).                                 |
| `-cpuprofile`       | (none)    | Write a pprof CPU profile to this file (`go tool pprof -top`/`-cum` to read it).               |
