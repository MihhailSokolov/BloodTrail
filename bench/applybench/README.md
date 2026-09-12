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
populated and pre-compaction throughout -- exactly the state this
measurement is meant to characterize. (c) below measures compaction
separately, with its own low threshold.

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

### (e) Boot adoption under concurrent writes

The end-to-end proof of the boot gap buffer (`internal/engine/bootgap.go`):
recognized writes landing while the snapshot file loads must not cost the
file its adoption. BloodHound writes to the graph within milliseconds of
every real boot (its startup analysis request plus a zero-start-delay data
pipe), so a restart path that only worked on a perfectly quiet boot would
be unreachable in production -- exactly what the plain watermark-equality
check used to make it at scale.

Each repeat saves a file through a real driver `Close` exactly as (d) does,
then opens a fresh production driver and writes (a)'s own objectid-keyed
ingest shape through it from the instant the open returns, so writes land
inside the boot-load window. Two writer scenarios run per bench run:

- **burst** -- 4 batch calls of 25 units, then silence: BloodHound's
  ordinary boot shape (the startup analysis writes within milliseconds of
  the API coming up, then goes quiet).
- **sustained** -- flat out until the boot's file attempt concludes: a
  restart landing mid-ingest. A batch write's watermark bump commits when
  its first operation is buffered (eagerly) while its Apply only lands at
  the flush that commits the chunk, so a writer that never stops keeps an
  unaccounted in-flight bump alive at essentially every instant -- and a
  one-instant adoption decision measurably lost this scenario 3/3 when it
  was first tried. The adoption path's settle-wait (freeze the watermark
  target once, wait -- bounded, 5s -- for the in-flight writes at or below
  it to finish and be accounted; `internal/engine/boot.go`) exists to win
  exactly this, and the scenario is its regression proof: post-settle-wait,
  the same flat-out writer adopts 3/3 at smoke scale, replaying 16-35
  in-flight writes per boot.

Two outcomes are legitimate per boot:

- **ADOPTED** -- `"bloodtrail: snapshot file loaded"` with its
  `replayed_writes` attribute counting the buffered writes folded onto the
  file before publication. The expected outcome in BOTH scenarios; each
  scenario must land it at least once or the phase fails. (On a graph small
  enough that the load outruns the burst's first commit, `replayed_writes`
  can be legitimately 0 -- the writes simply landed after adoption as
  ordinary deltas.)
- **GAP-REJECTED** -- `"bloodtrail: snapshot file rejected"` with reason
  `boot gap not covered by buffered writes` and a `waited` duration: a
  bump was still unaccounted when the settle window closed, i.e. a write
  outlived the 5s wait. Rare in either scenario, and the pg-rebuild
  fallback is correct when it happens.

Any other rejection reason, a writer error mid-boot, or a boot that reaches
neither marker inside `-cap` aborts the run. Reported only, never enforced
(the same judgment as (c)/(d): correctness either way, and the cost either
way is a background rebuild, not a served query's latency).

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
| (a) apply overhead               | `<= 60%` of the engine-disabled write wall time |
| (b) during-ingest, **delta-populated** query p95 | `<= 1.75x` the idle p95    |

**(c) and (d) are reported only, never enforced** -- see below for why, now
that both have real 5M-scale numbers behind them.

**CI must never pass `-enforce`.** Any other failure (a database error, a
missing base graph, a phase exceeding its own `-cap` watchdog) aborts the run
with a nonzero exit regardless of `-enforce`.

### Cap derivation: evidence-based

`applyOverheadMaxPct` and `idleP95Multiplier` were placeholders (25%, 3x)
until a 5M-scale measurement existed to anchor them to -- the same
"measured worst case x ~1.75, rounded" convention `bench/builderbench`'s
`shapeThresholds` doc and `bench/cypherbench`'s `ridSuffixScanMinRatio`/
`flagScanMinRatio` docs already establish and justify at length (a bar set
from an invented number, rather than a measurement, has twice been proven
undersized on this project -- see either README's own incident history).
Three full runs against `bench/adgen -users 2800000 -domains 4 -seed 1`
(~4.76M-4.94M nodes across the three runs -- `applybench` writes real rows
into the same database run over run, per-run namespaced; see "Re-running
without re-wiping" below), on an otherwise-busy shared machine:

| Run | (a) overhead | (b) delta p95 / idle p95 |
|---|---:|---:|
| 1 | 33.52% (on p50 1193ms / off p50 894ms) | 0.44x (359ms / 817ms) |
| 2 | 25.22% (on p50 1230ms / off p50 982ms) | 0.48x (402ms / 846ms) |
| 3 | 26.05% (on p50 1241ms / off p50 984ms) | 0.98x (341ms / 349ms) |
| **worst** | **33.52%** | **0.98x** |

Both bars are the worst of the three x ~1.75, rounded up to a clean number:
`applyOverheadMaxPct = 60.0` (33.52 x 1.75 = 58.66) and
`idleP95Multiplier = 1.75` (0.98 x 1.75 = 1.71). Pinned by
`TestMeasuredEngineOverheadCaps` (`main_test.go`); changing either requires
fresh 5M-scale evidence recorded here and in that test together. A fourth
confirmation run with these caps compiled in passed both (see "Measured at
5M" below).

(b)'s own spread is the more interesting number here: the delta/idle ratio
swings nearly 2.2x across three runs (0.44x to 0.98x) not because
during-ingest latency is unstable -- `during_p95` sits in a tight
340-402ms band across all three -- but because **idle p95 itself** carries
a wide tail on this shared machine (349-846ms over 200 samples), the same
kind of ambient-load noise `bench/pathbench`'s own p95 bar has shown
repeatedly on this project (see the root README's own measured-results
history). A wider idle tail shrinks the ratio, not the other way round, so
this spread is a statement about the denominator's noise, not about the
engine's own during-ingest behavior degrading.

### Why (c) and (d) have no bar at all yet, even with real numbers now

Both now have real 5M-scale evidence (below), but neither gates `-enforce`:
compaction and snapshot-file I/O are **background/operational** costs, not
costs a served query ever waits on. A slow compaction fold runs
concurrently with ordinary serving (Apply keeps applying new deltas while
it folds, per this package's own README on write-through); a slow snapshot
save/load only affects a graceful shutdown or a cold boot, never a query
in between. Regressing either is worth noticing -- which is exactly what
reporting the numbers here achieves -- but gating a build on them would
conflate "got slower" with "a user would notice," the same distinction
`bench/builderbench`'s own `group_members_bfs` shape drew before it got an
absolute cap instead of a ratio: these two just don't have the "a user is
waiting on this" property that (a) and (b) do. Revisit this once there is
a history of runs to judge "how much slower is concerning" against, rather
than one measurement session's worth.

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
itself runs end to end: 50,000 users is far too small a graph for the two
enforced bars' 5M-scale derivations to mean anything -- a pass or fail here
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

## Measured at 5M (`bench/adgen`'s ~4.76M-4.94M-node graph)

Four runs total (2026-09), on an otherwise-busy shared machine: three that
produced the cap derivation above, plus a fourth confirmation run with the
resulting caps compiled in.

**(a)/(b)** -- see the cap-derivation table above for the first three runs;
the fourth (confirmation) run, with the resulting caps compiled in,
measured 19.18% overhead (PASS against the 60% cap) and a 0.93x delta/idle
ratio (968ms / 1039ms, PASS against 1.75x) -- `APPLYBENCH_RESULT PASS`.

**(c) compaction duration** -- three compactions observed per run (matching
`-repeats`, each triggered by one isolated, threshold-crossing flush --
see "One flush at a time" in `main.go`'s `measureCompaction` doc for why
that has to be spaced out rather than issued as one burst at this node
count):

| Run | Observed durations | p50 |
|---|---|---:|
| 1 | 25972 / 27466 / 26130 ms | 26130ms |
| 2 | 25757 / 24553 / 28476 ms | 25757ms |
| 3 | 24355 / 25649 / 36223 ms | 25649ms |
| 4 (confirmation) | 25805 / 26652 / 27417 ms | 26652ms |

Tight and consistent across 11 of 12 individual observations (24.4-28.5s);
one outlier (36.2s, run 3's third compaction) under whatever else was
sharing the machine at that moment. Folding this graph's ~4.9M nodes /
~49M edges **entirely in memory, with no PostgreSQL round trip and no JSON
parse** -- the two dominant costs `internal/engine/boot.go`'s own doc
attributes the ~46-52s full rebuild (below) to -- costs roughly half that,
consistently: this is the write-through design's central bet (see the root
README's [Write-through](../../README.md#write-through) section) paying
off as measured, not merely as argued.

**(d) snapshot file save/load/boot** -- three repeats per run (`-repeats`,
default 3):

| Run | save (Close wall / engine-logged), p50 | load (parse only), p50 | boot (open -> loaded), p50 |
|---|---:|---:|---:|
| 1 | 29892ms / 29889ms | 9228ms | 13999ms |
| 2 | 27505ms / 27503ms | 7988ms | 13521ms |
| 3 | 28098ms / 28096ms | 9128ms | 10429ms |
| 4 (confirmation) | 26905ms / 26904ms | 8535ms | 10578ms |

Save (fold + write a ~3.9GiB file) and boot (open the whole driver stack up
to a loaded, servable snapshot) both swing noticeably run to run (save:
26.9-29.9s; boot: 10.4-14.0s) -- consistent with the same shared-machine
variance (a) and (b)'s own spread already shows, not a sign that either
cost scales unpredictably with graph size. Load alone (the pure
`ReadSnapshotFile` parse, excluding the boot path's own watermark round
trip and adoption) is the tightest of the three, 7.99-9.23s, matching
`internal/engine/snapshot/file.go`'s own claim that parsing -- unlike a
PostgreSQL rebuild -- has no I/O-bound round trips to absorb noise from,
only CPU-bound decode work.

For scale, this run's own `engine.LoadSnapshot` (a full PostgreSQL
rebuild) measured 45.1-50.7s across the three base-graph loads (the
`APPLYBENCH_BUILD` line each run prints) -- boot from the snapshot file
(10.4-14.0s, including the driver-open overhead a bare `LoadSnapshot` call
doesn't pay) is **roughly 3-5x faster** than rebuilding from PostgreSQL at
this scale, the whole reason the snapshot file exists.

**(e) boot adoption under concurrent writes** -- measured 2026-09-12, two
full 5M-scale runs (the phase landed after the four runs above; same graph
recipe, ~4.8M nodes / ~48.9M edges, same shared machine):

| Run | Outcomes (of 3 boots) | replayed_writes per adoption | boot under writes (open -> loaded), p50 |
|---|---|---|---:|
| 1 | 3x ADOPTED | 4 / 4 / 4 | 8787ms |
| 2 | 3x ADOPTED | 4 / 4 / 4 | 11513ms |

**Six boots, six adoptions, zero gap-rejections**, each with a 100-unit
burst (200 upsert operations across 4 batch scopes) landing inside the
load window and all four buffered ChangeSets replayed onto the file before
publication -- the boot gap buffer doing at production scale exactly what
it was built for, at a boot cost indistinguishable from (d)'s quiet boots
(8.6-11.8s individual observations vs (d)'s 10.4-14.0s p50 range). The
buffer's caps were never approached: 4 entries against the then-1024 entry
cap (4096 since the sustained scenario's own measurement moved it -- see
the milestone-7 results below), ~200 keys
against 262,144 -- a real BloodHound boot's startup-analysis burst is the
same order of magnitude as this phase's, far below either cap.

These two runs also re-passed both enforced bars on the same day's
heavily-reused database (run 2: (a) 46.0% against the 60% cap, (b) 1.75x
exactly at its bar) -- and a run between them produced this package's
adoption-wait fix: a fixed 2x-build-duration sleep undershot one boot on
the day's I/O-worn disk, (d)'s save driver reached `Close` before any View
existed, and the phase failed on a race the harness itself manufactured.
Every phase now waits on the engine's own adoption marker
(`waitForAdoption`, main.go) instead of predicting boot duration.

**Milestone-7 results (2026-09-13, same graph recipe, ~4.8M nodes):** with
the engine's settle-wait adoption in place and the scenario split into
burst + sustained, one full run measured:

| Scenario | Outcomes (of 3 boots) | replayed_writes per adoption | boot (open -> loaded), p50 |
|---|---|---|---:|
| burst | 3x ADOPTED | 4 / 4 / 4 | 8449ms |
| sustained | 3x ADOPTED | 844 / 1765 / 943 | 10168ms |

The sustained scenario -- the writer that lost 3/3 against the one-instant
decision -- now adopts every boot, replaying its entire load-window
backlog; its cost scales with that backlog (the 1765-replay boot took
19.2s end to end against 44,150 units written meanwhile) and stays
well under the ~45-60s rebuild it replaces. Both enforced bars passed in
the same run ((a) 27.6% against 60, (b) 1.75x at bar). An aborted first
attempt of this run is itself recorded evidence: at the previous 1024
entry cap the sustained writer measured 914 and 997 buffered writes across
a ~11s 5M boot window and overflowed on the third boot -- the measurement
`maxBootGapEntries`' current value (4096) and its pin test are derived
from (internal/engine/bootgap.go); at the new cap this run's worst boot
buffered 1765 with zero overflows.

### A harness bug this run found and fixed

The first attempt at (c) wrote all `-repeats*3+1` compaction-forcing
flushes in one uninterrupted burst before ever checking whether a
compaction had landed, on the theory that "enough flushes should run to
observe several triggers even if some overlap." That theory holds at
small scale but not at 4.76M+ nodes: `maybeStartCompaction`
(`internal/engine/compact.go`) is only ever invoked from `Apply`'s own
tail, never on a timer, so a compaction 2 or 3 only gets a chance to
trigger if ANOTHER write lands after compaction 1 has already adopted. A
whole burst finishes writing in a few seconds (per (a)'s own measurement)
-- long before a single fold over a multi-million-node base can complete --
so every flush after the first lands while a fold is already running,
gets skipped by `maybeStartCompaction`'s own re-trigger guard, and piles up
as one large tail segment with nothing left to write afterward that could
ever re-check the threshold. Measured directly: exactly 1 compaction
observed, then a full 10-minute `-cap` timeout with 0% CPU and static RSS
on the `applybench` process -- not a hang, just genuinely nothing left to
trigger a second one, and `-cap` correctly reported it as an honest timeout
rather than the run silently wedging. `measureCompaction` (`main.go`) now
issues one flush at a time, each large enough to cross the threshold on
its own, waiting for that flush's own compaction to be observed before
issuing the next -- which is what produced the three-per-run counts in the
table above.

### `compactEntries`/`compactBytes` defaults, revisited at this scale

(c) above deliberately forces compaction at a much lower threshold
(`-compact-entries 2000`) specifically so several fire during one
bench run -- it says nothing about whether the *default* threshold sits at
a good point, since (a)/(b) both ingest far too few rows (12,000 units =
24,000 entries per repeat) to ever reach the default at all. What (c)
*does* say about the defaults: a compaction fold at this node count costs
~25-28s typical (independent of how small the triggering delta was -- the
cost is dominated by rebuilding the ~4.9M-node base, not by the entries
that tipped it over), so the defaults' actual effect is exposure time, not
fold cost -- how large a delta (and its per-read overlay overhead) is
allowed to accumulate before that ~25-28s fold reclaims it.

An earlier revision of this section left the then-default of 1,000,000
entries unchanged for lack of a measurement of that exposure, and asked for
one. `BenchmarkFirstOverlayRead`
(`internal/engine/snapshot/view_overlay_bench_test.go`) is that
measurement, taken 2026-09-12: the per-View first-read merge is linear at
roughly 290ns per delta entry (M3 Pro) -- ~4.7ms at 16Ki entries, ~21ms at
64Ki, ~74ms at 256Ki, ~303ms at 1Mi -- and every apply pays it, under the
apply lock, via its own walk of the current `View`. At 1,000,000 entries
the merge alone could therefore reach ~300ms per write before a fold
reclaimed it, two orders of magnitude over the write's own measured work.
`DefaultCompactEntries` is now **65,536**: exposure tops out around ~21ms
(the same class as one write's read-back and apply), while routine full
ingests (~24,000 entries, above) still never trigger the default -- the
exact operational profile every number in this README was measured under.
Pinned by `TestMeasuredCompactDefaults` (`internal/engine/compact_test.go`).

## Known performance characteristics

Two properties of the write path are worth stating plainly, because both
are invisible in the numbers above and both get *worse* at settings this
benchmark did not exercise. Neither is a bug, and neither is being fixed
here -- they are recorded so a future measurement knows where to look.

### Delta reads cost O(total delta), per published View

`View.WithSegment` returns a **new** `View` sharing the base and the segment
stack, but with every memoized field zero-valued: the merged delta, the
virtual id assignment, the kind bitmaps, the edge-tombstone set and the
delta adjacency index are all per-`View` memoizations, and a fresh `View`
has none of them. So the first overlay read against each newly published
`View` re-runs `MergeSegments` over the *whole* stack and rebuilds whatever
projections it needs -- work proportional to the total delta accumulated
since the last compaction fold, not to the segment that was just added.
Every `Apply` publishes a new `View`, so that cost recurs per write.

`maxSegments` (32, `internal/engine/compact.go`) does **not** bound this. It
bounds how many segments the stack may hold, collapsing them when it grows
past that; the collapsed result is still one segment holding every entry, so
total delta *size* keeps growing until a compaction fold rebuilds the base.

This lands under the apply lock, on the write path, whenever
`BLOODTRAIL_MEMORY_LIMIT` is configured: `Apply`'s memory-limit check calls
`newView.ApproxBytes()` (guarded on the limit being set at all --
`internal/engine/apply.go`), and an overlay `View`'s `ApproxBytes` forces
`ensureEdgeTomb` and `ensureDeltaAdjacency` precisely so the estimate does
not depend on which accessors ran first. With the limit unset, the same work
still happens, but lazily, on whichever read touches the new `View` first.

**How large can it get?** `deltaEntries` counts one entry per delta node
and one per delta edge, so (a)/(b)'s 12,000 ingest units (1 node + 1 edge
each) grow the delta to about **24,000 entries** per repeat -- against a
shipped `BLOODTRAIL_COMPACT_ENTRIES` default of **65,536**, at which point
a fold reclaims it. The cost itself is now measured rather than open-ended
(`BenchmarkFirstOverlayRead`, and the defaults section above): linear at
~290ns per entry, so the shipped configuration bounds this term at ~21ms
per apply at the threshold, ~7ms at the deltas (a)/(b) actually ran with.
The 19-34% apply overhead was measured inside that bound; a raised
`BLOODTRAIL_COMPACT_ENTRIES` raises the ceiling proportionally.

### Write concurrency is effectively 1

`Engine.Apply` takes `applyMu` for its entire sequence, and step 4 of that
sequence is the read-back's PostgreSQL round trip (`readback.go`). The lock
is held across that network round trip, not just across the in-memory
publish, so concurrent writers serialize behind each other's read-backs.
The serialization is deliberate -- it is what stops two writes deriving
Views from the same base and losing one another's delta -- but the cost of
holding it across a round trip is a real limit on write throughput, and it
is not what the apply-overhead number above measures: (a) drives a single
writer, so it reports one writer's latency with the lock uncontended.

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
