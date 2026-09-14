# builderbench

`builderbench` benchmarks BloodTrail's **builder-query serving path** --
dawgs' structural `Nodes()`/`Relationships()` query builder, as opposed to
[`bench/pathbench`](../pathbench)'s shortest-path queries -- against a graph
already loaded into PostgreSQL, normally by [`bench/adgen`](../adgen).

Unlike `pathbench` (which constructs `internal/engine` directly, calling
`LoadSnapshot` itself rather than opening a driver), `builderbench` opens the
*real* production driver -- `dawgs.Open(ctx, bloodtrail.DriverName, cfg)`, exactly
as BloodHound would -- because the shapes benchmarked here are intercepted
by the driver's `Nodes()`/`Relationships()` wrapping, not by
`internal/engine`'s traverse package. A second `dawgs.Open(ctx,
pg.DriverName, cfg)` on the same DSN and pool is the delegated baseline:
plain PostgreSQL, no engine at all.

```
go run ./bench/builderbench -dsn <pg dsn> [-runs 5] [-pg-cap 120s] [-bt-cap 15m] [-enforce] [-cpuprofile <file>]
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
driver's boot-load goroutine (`internal/engine`'s `Start`, launched from
`dawgs.Open`) to finish loading its first in-memory snapshot from
PostgreSQL -- the one rebuild write-through still does, at process startup
(see the root README's [Write-through](../../README.md#write-through)).
There is no exported way to observe that load completing from outside the
driver, so `builderbench` instead:

- times its own throwaway `engine.LoadSnapshot` call against the same data
  (this doubles as the cheap node/edge/byte counts it prints), then
- sleeps a safety multiple of that measured cost, printing the wait
  duration (`BUILDERBENCH_WAIT`).

This scales with graph size automatically, unlike a fixed sleep -- at 5M
nodes the wait is dominated by the measured build cost.

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
against `shapeThreshold.engineAbsoluteCap`. A size mismatch the warmup
already measured still fails the shape, though: the warmup runs before any
capping is possible, so a recorded `match=false` is a real disagreement
between the two drivers, and a slow pg baseline is no reason to forgive it.
Only a comparison that never ran (`match_checked=false`) is passed over.

| Shape                            | Engine abs. cap (pg_capped path) | Why                                                                                 |
|-----------------------------------|:---------------------------------:|-----------------------------------------------------------------------------------------|
| `fetch_directed_graph_memberof`   | **80s**                            | Measured evidence: three independent 5M runs measured bt p50 at 33.0s / 37.3s / 45.3s under varying machine load; 80s is the worst-case p50 (45.3s) x ~1.75, rounded. Replaces a 15s guess that the first genuinely pg-capped run at 5M proved undersized (the shape answered correctly from memory in 33.0s and failed purely on the cap value). |
| `group_members_bfs`               | **95s**                            | Measured evidence: two 5M runs (one under a CPU profile) measured bt p50 within 1% of 27.0s (26967.91ms and 27077.85ms), but a third run on a loaded machine measured 54.2s and failed the 50s cap derived from the first two; 95s is the worst-case p50 (54.2s) x ~1.75, rounded, replacing that 50s (itself a replacement for an invented 5s). See `main.go`'s `shapeThresholds` doc for the profiling finding behind this shape's cost. |
| `node_count_user`                 | 2s                                 | Confirmed by measurement: bt p50 ~0.01ms at 5M -- thousands of times inside this bound. |
| `node_fetchids_user`              | 5s                                 | Confirmed by measurement: bt p50 ~186-193ms at 5M, >25x headroom.                    |
| `delete_transit_edges_admin_to`   | 5s                                 | Confirmed by measurement: bt p50 ~203-204ms at 5M, >24x headroom.                    |

**Where these numbers come from:** every cap in the table is now measured
evidence from real 4.76M-node/~48.9M-edge runs (`bench/adgen -users 2800000
-domains 4`). The three bounded/filtered shapes each get comfortable (>20x,
and often >100x) headroom over their own measured bt p50. The two
traversal-heavy shapes get a deliberately tighter, worst-case-anchored
margin instead: their absolute cost at this scale is tens of seconds and
swings roughly 2x with background machine load (`group_members_bfs`
measured p50s of 27.0s twice on a quieter machine and 54.2s on a loaded
one; `fetch_directed_graph_memberof` measured 33.0s / 37.3s / 45.3s), so
each cap is its shape's worst-case measured p50 x ~1.75, rounded -- wide
enough that a correctly-answering run doesn't fail on load noise, tight
enough that a real regression at this scale still trips it. The
convention was validated the hard way twice: an invented 5s cap and a
15s guess, and later even a 50s cap derived from only the two quiet-run
measurements, each failed runs whose answers were correct.

Forcing the capped path for a smoke test (e.g. `-pg-cap 1ms`) makes every
shape's pg baseline trip immediately, which is a convenient way to verify
the whole `pg_capped=true` code path end to end without waiting for a real
slow query -- see "Usage at small scale" below.

## Usage at small scale

```
go run ./bench/adgen -dsn "$BLOODTRAIL_TEST_PG" -users 2000 -wipe
go run ./bench/builderbench -dsn "$BLOODTRAIL_TEST_PG"
```

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
point it at anything you care about. `builderbench` itself never writes any
graph data of its own.

### Smoke-testing the `-pg-cap` path

The `pg_capped=true` path (skipped pg runs, skipped ratio check, engine
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
2026-09 incident that prompted it): a bt call that has not returned within
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

## Measured at 5M (`bench/adgen`'s 4.76M-node / ~48.9M-edge graph)

Four of the five shapes clear their bar comfortably; `group_members_bfs`'s
own ratio is unmeasurable at this scale (its pg baseline is `pg_capped`
every run) and is instead judged on `engineAbsoluteCap`, which it clears
with room to spare:

| Shape                             | Bar     | Measured p50 ratio (pg/bt) or bt p50 | Verdict |
|-------------------------------------|:-------:|:---------------------------------------:|:-------:|
| `node_count_user`                   | 5x      | ~29,654x-43,832x                        | pass    |
| `delete_transit_edges_admin_to`     | 5x      | ~21.6x-23.6x                            | pass    |
| `node_fetchids_user`                | 5x      | ~6.3x-6.6x                              | pass    |
| `fetch_directed_graph_memberof`     | 1.2x    | ~1.75x-2.66x                            | pass    |
| `group_members_bfs`                 | 50s cap | bt p50 ~27.0s (26.97s / 27.08s across two runs) | pass (absolute-cap judgment) |

Two independent runs (one under a `-cpuprofile`) agreed closely on every
shape, including `group_members_bfs`'s bt p50 (within 1% between runs) --
see the cap-rationale table above for how that number turned into its 50s
`engineAbsoluteCap`, and `main.go`'s `shapeThresholds` doc for the profiling
finding that ruled out a code fix in favor of a measured cap.
`fetch_directed_graph_memberof`'s ratio moved between the two runs (1.75x,
then 2.66x) but stayed clear of its 1.2x bar both times -- ordinary
machine-load variance on a shape whose own absolute cost (tens of seconds)
is large enough to absorb it without threatening the bar, unlike
`bench/cypherbench`'s sub-200ms shapes (see that package's README for where
noise *did* flip a verdict). Every shape's row output matched between the
two drivers in both runs (`match=true`, or `match=skip` where `pg_capped`
legitimately skipped the check) -- this table is purely about relative
speed, not correctness.

## Flags

| Flag       | Default | Meaning                                                              |
|------------|---------|-----------------------------------------------------------------------|
| `-dsn`     | (none)  | PostgreSQL connection string. Required.                               |
| `-runs`    | `5`     | Number of warmed-up, timed runs per shape per driver.                 |
| `-pg-cap`  | `120s`  | Per-shape wall-clock cap on the pg baseline; see `-pg-cap` above.      |
| `-bt-cap`  | `15m`   | Per-shape wall-clock cap on the engine-side bt call; ABORTS THE RUN on expiry (nonzero exit) -- see `-bt-cap` above. |
| `-enforce` | `false` | Exit nonzero on a per-shape threshold/match miss (never pass this in CI). |
| `-cpuprofile` | (none) | Write a pprof CPU profile to this file (`go tool pprof -top`/`-cum` to read it). |
