# builderbench

`builderbench` benchmarks BloodTrail's **builder-query serving path** --
dawgs' structural `Nodes()`/`Relationships()` query builder, as opposed to
[`bench/pathbench`](../pathbench)'s shortest-path queries -- against a graph
already loaded into PostgreSQL, normally by [`bench/adgen`](../adgen).

Unlike `pathbench` (which constructs `internal/engine` directly, bypassing
the poller via a manual rebuild call), `builderbench` opens the *real*
production driver -- `dawgs.Open(ctx, bloodtrail.DriverName, cfg)`, exactly
as BloodHound would -- because the shapes benchmarked here are intercepted
by the driver's `Nodes()`/`Relationships()` wrapping, not by
`internal/engine`'s traverse package. A second `dawgs.Open(ctx,
pg.DriverName, cfg)` on the same DSN and pool is the delegated baseline:
plain PostgreSQL, no engine at all.

```
go run ./bench/builderbench -dsn <pg dsn> [-runs 5] [-pg-cap 120s] [-bt-cap 15m] [-enforce]
```

Or, against `BLOODTRAIL_TEST_PG`:

```
make bench-builder
```

## What it measures

Four query shapes real BloodHound's builder issues in production, each run
as one warmup call per driver (comparing a cheap size metric between the
two as a correctness guard) followed by `-runs` (default 5) further timed
calls per driver:

1. **Whole-kind pair scan**: `container.FetchDirectedGraph` over the
   `MemberOf` edge kind -- `adgen`'s highest-count edge kind by
   construction (every user gets a guaranteed `MemberOf` plus 8 more to
   random groups).
2. **Group-members BFS**: `traversal.BreadthFirst` +
   `traversal.LightweightDriver`, following `MemberOf` edges inbound from
   the graph's highest-in-degree group (found with one SQL query at
   startup) -- every node transitively a member of that group. The density
   is realistic — see [../adgen/README.md](../adgen/README.md).
3. **Node count + full drain**: `Nodes().Filter(Kind(Node(),
   User)).Count()`, and the same filter's `FetchIDs()` fully drained.
4. **The `DeleteTransitEdges` shape**: `Relationships().Filter(And(KindIn(
   Start(), Base), Kind(Relationship(), AdminTo), KindIn(End(),
   Base))).FetchIDs()` fully drained -- the read BloodHound issues before
   recomputing a derived edge kind wholesale. `AdminTo` stands in for a
   genuinely *derived* edge kind here: production BloodHound computes it
   from direct and nested-group admin memberships; `adgen` has no
   synthesized-vs-computed distinction, so `AdminTo` (an edge kind real
   BloodHound does derive) is the closest analog it actually writes.

Every shape prints a human-readable line and a machine-greppable
`BUILDERBENCH_*` summary line (`grep '^BUILDERBENCH_'`), reporting p50/p95
per driver and the **p50 ratio** (delegated / served) -- how many times
faster serving from memory is than delegating to PostgreSQL.

### Waiting for the snapshot

Before any shape is measured, `builderbench` waits for the bloodtrail
driver's background poller to build its first in-memory snapshot. There is
no exported way to observe that build completing from outside the driver,
so `builderbench` instead:

- times its own throwaway `engine.LoadSnapshot` call against the same data
  (this doubles as the cheap node/edge/byte counts it prints), then
- sets `BLOODTRAIL_ENGINE_POLL_INTERVAL=200ms` before opening the
  bloodtrail driver, and
- sleeps a safety multiple of the measured build cost plus the poll
  interval, printing the wait duration (`BUILDERBENCH_WAIT`).

This scales with graph size automatically, unlike a fixed sleep -- at 5M
nodes the wait is dominated by the measured build cost, not the poll
interval.

`builderbench` also creates a scratch `datapipe_status` row (dropped when
it's done) so the poller's tick -- which otherwise does nothing at all if
that table doesn't exist -- can succeed at all. See `createDatapipeStatusTable`'s
doc in `main.go` for why: without it, the engine never builds a snapshot and
every ratio comes back near 1x, not an error.

## `-enforce`

With `-enforce`, `builderbench` exits nonzero if any shape fails its own
**per-shape threshold** -- see the table below -- or (for a shape whose pg
baseline wasn't capped, see `-pg-cap` below) if its two drivers' results
mismatched (a correctness guard: each shape's warmup call compares a count/
node-set-size/edge-set-size between the bloodtrail driver and the plain pg
driver, so a silently-delegating or outright-wrong-serving engine fails
loudly instead of just looking slow).

**CI must never pass `-enforce`.** Any other failure (a database error, an
empty graph, a driver error) aborts the run with a nonzero exit regardless
of `-enforce`.

### Per-shape enforce thresholds

Every shape but one requires the p50 ratio (delegated pg / served bt) to
clear **5x**: at the same PostgreSQL tables and essentially the same
per-query cost either way, a 5x speedup is not achievable by two drivers
that both just forward to PostgreSQL, so clearing 5x is itself strong
evidence the bloodtrail driver actually served the query from its
in-memory snapshot -- `-enforce` needs no separate "did it serve" signal
beyond the ratio itself for these shapes.

`fetch_directed_graph_memberof` is the deliberate exception, at **1.2x**:

| Shape                            | Min p50 ratio | Why                                                                                                                                                        |
|-----------------------------------|:-------------:|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fetch_directed_graph_memberof`   | **1.2x**      | Row-volume-bound: `container.FetchDirectedGraph` must visit every `MemberOf` edge and endpoint node regardless of driver, so the engine's edge is bounded by memory-vs-rows cost, not by skipping a network round trip or a query planner. Measured 1.47x at 5M -- honest physics for this shape, not a bug. |
| `group_members_bfs`               | 5x            | Standard bar; also gated by `-pg-cap` below, since its pg baseline is a per-node query storm.                                                                 |
| `node_count_user`                 | 5x            | Standard bar: a bounded, filtered node count.                                                                                                                 |
| `node_fetchids_user`              | 5x            | Standard bar: a bounded, filtered id drain.                                                                                                                   |
| `delete_transit_edges_admin_to`   | 5x            | Standard bar: a bounded, filtered edge-id drain between `Base`-kind endpoints.                                                                                |

These live in `main.go`'s `shapeThresholds` map, keyed by shape name, next
to `evaluateShape` -- the pure function that actually applies them (see its
doc and `main_test.go`'s `TestEvaluateShape` for the full decision logic,
table-tested independent of any database).

### `-pg-cap`: bounding a runaway pg baseline

`group_members_bfs`'s pg baseline is a *per-node* query storm: BFS-ing a
group's members by issuing one PostgreSQL lookup per frontier node, which
runs for hours at 5M -- entirely unrelated to how fast the bloodtrail
driver answers the same traversal in memory. Without a limit, this one
shape's pg baseline alone would make `-enforce`'s full run unusable at
realistic scale.

`-pg-cap` (default `120s`) bounds every pg-baseline call -- warmup and every
timed run -- to that wall-clock budget, via `context.WithTimeout` wrapped
directly around the query itself (`runPGCapped` in `main.go`), not a timer
between iterations: dawgs' pg driver forwards the caller's context straight
to pgx's `Query`/`Exec`, which sends PostgreSQL a real cancellation request
when the deadline fires, so a single pathologically slow query is cut off
mid-execution, not merely abandoned before the next one starts.

The first time a shape's pg baseline exceeds `-pg-cap`, `builderbench`:

- records `pg_capped=true` for that shape (`BUILDERBENCH_SHAPE` and the
  human-readable line both carry it),
- skips every remaining pg run for that shape (no more monster queries),
  and
- **skips the bt/pg size-match check** if the cap tripped before the
  warmup comparison ever completed -- matching a result you never obtained
  is impossible, so the check is reported as skipped (`match=skip` /
  `match_checked=false`), never as a false mismatch. If the cap instead
  trips partway through the *timed* run loop (after a successful warmup),
  the match check already ran and its real result is still reported.

With no pg measurement left, `evaluateShape` judges a `pg_capped=true`
shape on the **bloodtrail driver's own absolute p50** instead of a ratio,
against `shapeThreshold.engineAbsoluteCap`:

| Shape                            | Engine abs. cap (pg_capped path) | Why                                                                                 |
|-----------------------------------|:---------------------------------:|-----------------------------------------------------------------------------------------|
| `fetch_directed_graph_memberof`   | 15s                                | Conservative estimate for a full 5M-scale `MemberOf` scan; unmeasured as of this task, see below. |
| `group_members_bfs`               | **5s**                             | A controller-suggested estimate like the other four caps, to be validated by the first real 5M run (Task-21-equivalent), same status as its siblings. |
| `node_count_user`                 | 2s                                 | Headroom over a bounded in-memory count; not expected to ever need this path.       |
| `node_fetchids_user`              | 5s                                 | Headroom over a bounded id drain; not expected to ever need this path.              |
| `delete_transit_edges_admin_to`   | 5s                                 | Headroom over a bounded edge-id drain; not expected to ever need this path.          |

**Where these numbers come from:** only `group_members_bfs`'s 5s bound is
load-bearing today (it's the shape the milestone-3 deferral this task
fixes was actually about, and the only one expected to trip `-pg-cap` at
5M). The other four shapes' `engineAbsoluteCap` values are conservative
headroom for a hypothetically much larger corpus, not numbers validated
against a real 5M run -- Task 21's 5M-scale run may tighten them once real
measurements exist.

Forcing the capped path for a smoke test (e.g. `-pg-cap 1ms`) makes every
shape's pg baseline trip immediately, which is a convenient way to verify
the whole `pg_capped=true` code path end to end without waiting for a real
slow query -- see "Usage at small scale" below.

## Usage at small scale

```
go run ./bench/adgen -dsn "$BLOODTRAIL_TEST_PG" -users 2000 -wipe
go run ./bench/builderbench -dsn "$BLOODTRAIL_TEST_PG"
```

**Important: `builderbench` requires a dedicated benchmark database.** It
will refuse to run if `datapipe_status` already exists, since this guards
against accidentally running against a real BloodHound installation where the
deferred table drop would destroy live pipeline state. Always point `-dsn` at
a database created and loaded by `adgen` for benchmarking only.

Do **not** pass `-enforce` at small scale: a 2,000-user graph is small
enough that PostgreSQL itself answers most of these shapes from cache in a
millisecond or two, so the ratios (while still visibly greater than 1x on
shapes 1-2 in practice) are far too close to the noise floor to reliably
clear 5x every run. `-enforce` is meant for an operator run against a
realistically sized graph (see the 5M workflow below), the same way
`pathbench`'s `-enforce` thresholds are.

`-wipe` truncates the `node`/`edge` tables for *every* graph in the
database, not just the one about to be loaded -- fine for a disposable test
database (integration tests reseed their own data on every run), but don't
point it at anything you care about. `builderbench` itself never writes
graph data; the only write is its own scratch `datapipe_status` row,
created and dropped within one run.

### Smoke-testing the `-pg-cap` path

The `pg_capped=true` path (skipped pg runs, skipped match check, engine
judged on its absolute p50) is otherwise only exercised by a genuinely slow
pg baseline, which doesn't happen at small scale. Force it instead:

```
go run ./bench/builderbench -dsn "$BLOODTRAIL_TEST_PG" -pg-cap 1ms
```

An impossibly small cap trips on every shape's very first pg call, so every
`BUILDERBENCH_SHAPE` line comes back `pg_capped=true match_checked=false`,
and the run is judged entirely on each shape's `engineAbsoluteCap` --
comfortably cleared at small scale, so this still reports
`BUILDERBENCH_RESULT PASS` even with `-enforce`.

### `-bt-cap`: a fail-fast watchdog on the engine side

`-pg-cap` bounds the pg baseline; `-bt-cap` (default `15m`) bounds the
**engine-side (bt) call** the same way, via `context.WithTimeout` wrapped
directly around each bt call (`runBTCapped` in `main.go`) -- but unlike
`-pg-cap`, tripping it is never a graceful, recordable outcome. It exists as
a fail-fast safety net for the same failure mode `bench/cypherbench`'s
identical flag was added to catch there (see that package's README for the
2026-09 milestone-4.5 incident): a bt call that has not returned within
`-bt-cap` has almost certainly not been quietly slow -- the engine declined
and the driver silently fell through to PostgreSQL, which can then run
unbounded. `runBTCapped` returns a hard error naming the shape and the cap,
and the whole run **aborts with a nonzero exit** -- never a `bt_capped=true`
data point the way `-pg-cap` records `pg_capped=true`. Seeing this abort
means: stop and investigate the engine's decline, do not wait for the
delegated fallback to finish.

## At large scale (the 5M-node workflow)

Following `bench/adgen`'s own large-scale guidance:

```
go run ./bench/adgen -dsn <pg dsn> -users 2800000 -domains 4 -wipe
go run ./bench/builderbench -dsn <pg dsn> -enforce
```

`-domains 4` matches realistic domain density (see `bench/adgen/README.md`).
At this scale, expect the initial `engine.LoadSnapshot` measurement (and
therefore the wait before shapes start) to take substantially longer than
the small-scale default -- `builderbench` prints exactly how long it waited
(`BUILDERBENCH_WAIT`) and what it measured
(`BUILDERBENCH_BUILD`), so there is no guessing involved.

## Flags

| Flag       | Default | Meaning                                                              |
|------------|---------|-----------------------------------------------------------------------|
| `-dsn`     | (none)  | PostgreSQL connection string. Required.                               |
| `-runs`    | `5`     | Number of warmed-up, timed runs per shape per driver.                 |
| `-pg-cap`  | `120s`  | Per-shape wall-clock cap on the pg baseline; see `-pg-cap` above.      |
| `-bt-cap`  | `15m`   | Per-shape wall-clock cap on the engine-side bt call; ABORTS THE RUN on expiry (nonzero exit) -- see `-bt-cap` above. |
| `-enforce` | `false` | Exit nonzero on a per-shape threshold/match miss (never pass this in CI). |
