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
go run ./bench/applybench -dsn <pg dsn> [-ingest 12000] [-flush 20000] [-latency-flush 2000] [-repeats 3] [-queries 200] [-compact-entries 2000] [-run-id <id>] [-cap 10m] [-enforce] [-cpuprofile <file>]
```

Or, against `BLOODTRAIL_TEST_PG` (this also runs `bench/adgen` first, forwarding `ARGS` to it):

```
make bench-apply ARGS='-users 50000'
```

> **`make bench-apply` wipes the database.** The target runs
> `bench/adgen -wipe` before `applybench`, and it passes `-wipe`
> unconditionally -- with **no** `ARGS` at all it still wipes, then
> regenerates at `adgen`'s own default size. `-wipe` truncates the
> `node`/`edge` tables for *every* graph in that database. Point it only at a
> disposable test database.

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

**The two arms are symmetric and interleaved.** Neither arm runs a
concurrent reader (that belongs to (b), which owns it exclusively), and the
repeats alternate `on, off, on, off, ...` rather than running as two blocks.
Both properties are corrections to a first version that had neither, and
whose numbers were consequently not comparable:

- The engine-on arm used to run with a reader goroutine contending for the
  same driver, pool and CPU throughout, while the engine-off arm ran with
  none. Measured directly, by running the on arm both ways against the same
  off arm: **29.05% quiet vs 18.64% noisy** -- ten points of spread on a 25%
  bar, produced by the harness rather than the engine.
- All on repeats used to run before all off repeats, so every off repeat
  wrote into a graph the on repeats had already grown by
  `-repeats x -ingest` rows. Interleaving cannot eliminate growth, but it
  spreads it evenly across both arms instead of concentrating it in one.

The on arm also asserts the engine actually applied
(`"bloodtrail: write-through applied"`, via the same log capture (c) and (d)
use) and aborts if it never did: an engine that never adopted a snapshot
returns early from `Apply` with no read-back at all, which would otherwise
report as near-zero overhead -- a passing bar measuring nothing.

### (b) Query latency during active ingest

A two-node `FetchAllShortestPaths` query (the same `graph.Criteria` shape
BloodHound's own API builds, and `bench/pathbench`'s pair-query shape)
sampled **continuously for as long as one engine-on ingest runs**, then --
on the same still-warm driver, with no writes at all -- as the idle
baseline. Three numbers are reported: the full during-ingest window, the
**delta-populated subset** of it, and idle.

**The delta-populated subset is what `-enforce` scores**, and it exists
because write-through `Apply` only runs at a `Commit` boundary
(`write_observer.go`'s `observingBatch.Commit`). With `-flush 20000` and
`-ingest 12000` (= 24,000 operations) exactly **one** mid-delegate commit
ever happened, around unit 10,000 of 12,000 -- so roughly 83% of the sampled
window queried a graph with no published delta at all, which is the idle
state, not the state (b) is about. (b) therefore uses its own smaller
`-latency-flush` (default `2000`, giving 12 applies inside the window), and
splits its samples at the window's first `"bloodtrail: write-through
applied"` record so the two states are reported separately rather than
averaged together. The `APPLYBENCH_LATENCY_DURING_INGEST` line carries
`during_n`, `delta_n`, `applies` and `served` so the split is auditable.

**The idle baseline is warmed up and window-matched.** `warmupQueries` (30)
queries run and are discarded first, and the baseline then samples for as
long as the ingest window ran (capped at 60s; both windows are printed).
The first version did neither -- it was the very first thing the whole bench
did, on its own freshly opened driver, for a fixed 50 samples -- and so
absorbed every one-time cost in the process: **first-50-samples p50 7.85ms /
p95 19.18ms, against p50 0.17ms / p95 10.46ms over a later 200-sample window
on the same driver.** Enforcing "within 3x idle p95" against that inflated
denominator makes the bar meaningless in the lenient direction, and turns
"queries are *faster* under load" from an ordering artifact into an apparent
finding.

(b) also asserts the engine actually served
(`"bloodtrail: path engine served"`) and actually applied during the window,
aborting if either never happened -- otherwise a cold engine's samples would
be reported as the engine's latency when they are PostgreSQL's.

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

### (d) Snapshot file save, load and boot duration at scale

`BLOODTRAIL_SNAPSHOT_DIR` pointed at a scratch directory. Each repeat
measures three distinct things, deliberately kept apart:

| Number | What it times | What it excludes |
|---|---|---|
| **save** | `Close()`'s `Stop`-then-`SaveSnapshot` sequence, wall clock **and** the engine's own logged fold+write `duration` | -- |
| **load** | `internal/engine/snapshot.ReadSnapshotFile` -- parsing the file, the part that scales with its size | the boot path's own `ReadWatermark` round trip and adoption |
| **boot** | a fresh production driver open (`dawgs.Open`, then `AssertSchema`) until the engine logs `"bloodtrail: snapshot file loaded"` | nothing -- but it *includes* the driver open and up to one 100ms boot-load retry interval, so it is an upper bound, not a parse timing |

Reporting the save's wall clock *and* the engine's own logged duration
matters because they differ: the wall clock also covers `Stop`'s quiescing
and the pg driver's own `Close`. The presence of the
`"bloodtrail: snapshot file written"` line (captured the same way as (c)) is
also what confirms the save actually happened rather than being silently
skipped by one of `SaveSnapshot`'s own no-op preconditions -- see
`internal/engine/persist.go`.

**The boot measurement only became possible once the engine's boot-load path
was fixed.** An earlier version of this measurement did exactly this and
hung until `-cap`'s watchdog tripped, every time: `bloodtrail.Open` calls
`Start` *before* returning the driver a caller needs in order to call
`AssertSchema` at all, and `pg.NewDriverWithOptions` does no database I/O --
so the boot-load goroutine's one-shot file attempt always ran at an instant
where `pgDriver.DefaultGraph()` could not possibly have resolved, returned
`("", false)` without reading (or logging anything about) the file, and was
spent. `runBootLoad` now makes that attempt inside its retry loop, gated on
the default graph having resolved, so a fresh driver open really does load
the file. The wait below doubles as the harness-side regression check for
that: if the boot path ever stops reaching the file again, (d) fails loudly
instead of quietly reporting a parse timing in its place.

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
| (b) during-ingest, **delta-populated** query p95 | `<= 3x` the idle p95    |

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
`applybench`'s own long-running write loop also prints progress every 5,000
ingest units, and the compaction/snapshot-boot waits print progress every 2s
while polling, so a genuinely slow (not hung) run is visibly making progress
-- "check the log cadence, not just liveness" -- rather than looking
indistinguishable from a wedged one. Never retry past a `-cap` trip in a
loop; investigate why the operation didn't finish instead.

## Usage at small scale (the smoke run)

```
go run ./bench/adgen -dsn "$BLOODTRAIL_TEST_PG" -users 50000 -wipe
go run ./bench/applybench -dsn "$BLOODTRAIL_TEST_PG"
```

or, equivalently, the single Makefile target:

```
make bench-apply ARGS='-users 50000'
```

`applybench`'s own defaults (`-ingest 12000 -flush 20000 -latency-flush 2000
-repeats 3 -queries 200 -compact-entries 2000`) are sized to finish
comfortably in about a minute at this scale against ordinary hardware -- `-cap`'s 10-minute default
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
| `-flush`            | `20000`   | **(a)'s** batch flush size in operations (an explicit `batch.Commit()` every this many calls), matching production's write flush size. |
| `-latency-flush`    | `2000`    | **(b)'s** batch flush size in operations -- smaller on purpose, so several write-through applies land *inside* the sampled window. |
| `-repeats`          | `3`       | Repeats per measurement (p50 is taken over these); must be `>= 3`.                            |
| `-queries`          | `200`     | *Minimum* samples for (b)'s idle baseline; it also samples for as long as the during-ingest window ran (capped at 60s), so more are usually taken. |
| `-compact-entries`  | `2000`    | `BLOODTRAIL_COMPACT_ENTRIES` for measurement (c) only; deliberately low so compaction fires.  |
| `-seed`             | `1`       | Seed for deterministic query-pair sampling.                                                    |
| `-run-id`           | timestamp | objectid namespace for every node/edge this run writes. See "Re-running without re-wiping" below. |
| `-cap`              | `10m`     | Per-operation wall-clock watchdog; ABORTS THE WHOLE RUN on expiry (nonzero exit) -- see "Watchdog" above. |
| `-enforce`          | `false`   | Exit nonzero on a missed (a)/(b) bar (never pass this in CI).                                 |
| `-cpuprofile`       | (none)    | Write a pprof CPU profile to this file (`go tool pprof -top`/`-cum` to read it).               |

## Re-running without re-wiping

Every objectid `applybench` writes is namespaced by a per-invocation run id
(`APPLYBENCH-<run id>-<phase>-<repeat>-<i>`), printed at the top of every
run and overridable with `-run-id`.

That namespacing is load-bearing. The write shape (a) and (b) measure is an
**upsert** (`batch.UpdateNodeBy`/`UpdateRelationshipBy`), so with fully
deterministic objectids -- which is what this bench used before -- a second
run against a database that had not been re-wiped in between silently turned
every insert into an update of the row the previous run left behind. That is
a materially different and cheaper workload (no new rows, no index growth, a
read-back that finds an existing id rather than a new one) than the ingest
being claimed, and nothing in the output said so. Pass an explicit `-run-id`
only when you actually want two runs to collide.

## Troubleshooting

**`ERROR: there is no unique or exclusion constraint matching the ON CONFLICT
specification (SQLSTATE 42P10)`** at the first flush -- the scratch objectid
constraint was not created. Usually this means the graph already contains
**duplicate `objectid` values** (another suite's fixtures deliberately do
this), which makes the unique constraint impossible to create, and dawgs
reports the failure only later, at the first upsert. Find them with:

```sql
SELECT properties->>'objectid' AS objectid, count(*)
FROM node WHERE graph_id = <graph id>
GROUP BY 1 HAVING count(*) > 1;
```

The fix is to run `applybench` against a freshly generated graph
(`bench/adgen ... -wipe`, or `make bench-apply`, which does this for you) --
not to drop the duplicates from a database another suite is using.
