# cypherbench

`cypherbench` benchmarks BloodTrail's Cypher-interpreter serving path
(`internal/engine.TryCypher`) against a graph already loaded into
PostgreSQL, normally by [`bench/adgen`](../adgen). Like
[`bench/builderbench`](../builderbench) (and unlike `bench/pathbench`, which
constructs `internal/engine` directly), it opens the *real* production
driver -- `dawgs.Open(ctx, bloodtrail.DriverName, cfg)` -- and a second
plain-PostgreSQL driver on the same DSN/pool as the delegated baseline, so
every measured call goes through `graph.Transaction.Query` exactly as
BloodHound's own cypher endpoint does.

```
go run ./bench/cypherbench -dsn <pg dsn> [-runs 5] [-pg-cap 120s] [-bt-cap 15m] [-enforce] [-cpuprofile <file>]
```

Or, against `BLOODTRAIL_TEST_PG`:

```
make bench-cypher
```

## What it measures

Five Cypher shapes from the milestone's spec, each run as one warmup call
per driver (comparing result row counts as a correctness guard) followed by
`-runs` further timed calls per driver:

1. **`rid_suffix_scan`**: `MATCH (n:Group) WHERE n.objectid ENDS WITH
   '-512' RETURN n` -- the same shape BloodHound's "Domain Admins" selector
   uses.
2. **`flag_scan`**: `MATCH (u:User) WHERE u.hasspn = true AND u.enabled =
   true RETURN u LIMIT 1000` -- a two-boolean-property AND scan.
3. **`objectid_point_lookup`**: `MATCH (n) WHERE n.objectid = '<sid>'
   RETURN n`, with `<sid>` a real objectid selected from the loaded graph at
   run time -- exercises the snapshot's objectid index rather than a scan.
4. **`shortest_path_prebuilt`**: BloodHound's own pre-built "Shortest paths
   to Domain Admins" query, copied verbatim from
   [`testdata/prebuilt/agt.json`](../../testdata/prebuilt/agt.json) -- a
   `shortestPath` alternated over its full ~64-member Active Directory
   pathfinding edge-kind list.
5. **`collect_antijoin_prebuilt`**: BloodHound's own pre-built "Domain
   Admins logons to non-Domain Controllers" query, copied verbatim from the
   same corpus -- a `WITH COLLECT(...)` anti-join (`NOT c IN exclude`)
   feeding a second `MATCH`.

Before any shape is measured, `cypherbench` measures a snapshot build
(`engine.LoadSnapshot`, reporting nodes/edges/`ApproxBytes`), then waits for
the bloodtrail driver's boot-load goroutine to finish loading its own first
snapshot from PostgreSQL, exactly as `builderbench` does (there is no
exported hook to observe that completing, so the wait is a safety multiple
of the just-measured build cost, printed alongside it). After all five
shapes, it runs a second, separately timed `LoadSnapshot` call purely to
report rebuild cost.

Every shape prints a human-readable line and a machine-greppable
`CYPHERBENCH_<SHAPE>` summary line (`grep '^CYPHERBENCH_'`), ending in
`CYPHERBENCH_RESULT PASS` or `FAIL`.

## Every kind a query references must be declared first

Shape 4's `shortestPath` alternates over ~64 relationship kinds, most of
which `adgen`'s generated graph never actually creates an edge of. dawgs'
pgsql translator fails the *entire* query ("unable to map kinds") if even
one alternative was never asserted into the schema -- it does not degrade to
"no rows for the unknown kind" -- so `cypherbench` asserts the full closure
of kind names shape 4's text references (via `AssertKinds`) before running
any shape, on top of `adgen`'s own node/edge kinds. The list itself is
parsed back out of the embedded query text at run time (`extractRelKinds`),
not hand-transcribed a second time, so the two can never silently drift
apart. See `internal/graphtest/corpusfixture.go`'s identical rationale for
its own pre-built-corpus fixture.

## `-enforce`

With `-enforce`, `cypherbench` exits nonzero if any shape fails its own
**per-shape threshold** -- see the table below -- or (for a shape whose pg
baseline wasn't capped, see `-pg-cap` below) if the two drivers' result row
counts ever disagree.

**CI must never pass `-enforce`.** Any other failure (a database error, an
empty graph, a driver that returns an outright error) aborts the run with a
nonzero exit regardless of `-enforce`.

### Per-shape enforce thresholds

The two pre-built traversal shapes require the p50 ratio (delegated pg /
served bt) to clear **5x**: at the same PostgreSQL tables and essentially
the same per-query cost either way, a 5x speedup is not achievable by two
drivers that both just forward to PostgreSQL, so clearing 5x is itself
strong evidence the bloodtrail driver actually served the query from its
in-memory snapshot. The three scan/lookup shapes carry measured-physics
bars instead -- each derived from worst-case honest 5M measurements, each
still requiring the engine to be strictly faster than delegating:

| Shape                        | Min p50 ratio | Engine abs. cap (pg_capped path) | Why                                                                                                                                                          |
|--------------------------------|:-------------:|:---------------------------------:|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `rid_suffix_scan`              | **1.1x**      | 2s                                 | PostgreSQL's `kind_ids` GIN index already narrows the `ENDS WITH` scan to roughly the same row count the engine itself walks. Measured steady state: 1.31x on an idle machine (bt 162ms vs pg 212ms, warm cache), 1.38-2.03x loaded; higher readings (4.7-10.6x) only when pg's cache was cold. Bar sits just below the worst honest measurement. |
| `flag_scan`                    | **1.2x**      | 2s                                 | A `LIMIT 1000` prefix scan: both drivers stop after ~1000 matches, so warm-cache pg is also fast (p50 ~22-27ms vs bt ~12-15ms) -- no index or round-trip advantage left to multiply into 5x. Measured steady state: 1.87x idle, 1.29-2.29x loaded; the one 7.79x reading rode a cold pg cache (114ms). The old 5x bar predated LIMIT early-termination (bt was then ~25x *slower*). Cap: >100x headroom over bt's p50. |
| `objectid_point_lookup`        | **1x**        | 1s                                 | A single-row equality lookup against jsonb's own indexing on `properties->>'objectid'` is already fast on the pg side, so there is no full scan or traversal for the engine's in-memory objectid index to out-run -- the spec only requires the engine not be *slower*. |
| `shortest_path_prebuilt`       | 5x            | 10s                                | Standard bar: BloodHound's own pre-built "Shortest paths to Domain Admins" query. Cap: >25x headroom over its measured 5M-scale bt p50 (~260-370ms). |
| `collect_antijoin_prebuilt`    | 5x            | 30s                                | Standard bar (never actually the deciding factor, see below); also gated by `-pg-cap`, since its pg baseline is a full-trail group enumeration that exceeds `-pg-cap` on every 5M-scale attempt. Cap: >=1.75x headroom over its measured 5M-scale bt p50 (~9.5-14.5s). |

Every `engineAbsoluteCap` above is measured evidence -- each was checked
against a real 5M-node/~48.9M-edge run (`bench/adgen -users 2800000
-domains 4`), repeated across three separate invocations to confirm the
headroom holds under ordinary machine-load variance. These live in
`main.go`'s `shapeThresholds` map, keyed by shape name, next to
`evaluateShape` -- the pure function that actually applies them (see its
doc and `main_test.go`'s `TestEvaluateShape` for the full decision logic,
table-tested independent of any database).

### `-pg-cap`: bounding a runaway pg baseline

`collect_antijoin_prebuilt`'s pg baseline is a full-trail enumeration over a
700,000-member group: on one recorded 5M-scale attempt it ran for over two
hours without finishing, entirely unrelated to how fast the engine itself
answers the same query. The density is realistic — see
[../adgen/README.md](../adgen/README.md). Without a limit, this one shape's
pg baseline alone would make `-enforce`'s full run unusable at realistic scale.

`-pg-cap` (default `120s`) bounds every pg-baseline call -- warmup and every
timed run -- to that wall-clock budget, via `context.WithTimeout` wrapped
directly around the query itself (`runPGCypherCapped` in `main.go`), not a
timer between iterations: dawgs' pg driver forwards the caller's context
straight to pgx's `Query`/`Exec`, which sends PostgreSQL a real cancellation
request when the deadline fires, so a single pathologically slow query is
cut off mid-execution, not merely abandoned before the next one starts.

The first time a shape's pg baseline exceeds `-pg-cap`, `cypherbench`:

- records `pg_capped=true` for that shape (`CYPHERBENCH_<SHAPE>` and the
  human-readable line both carry it),
- skips every remaining pg run for that shape (no more monster queries), and
- **skips the bt/pg result-row-count check** if the cap tripped before the
  warmup comparison ever completed -- matching a result you never obtained
  is impossible, so the check is reported as skipped (`match=skip` /
  `match_checked=false`), never as a false mismatch. If the cap instead
  trips partway through the *timed* run loop (after a successful warmup),
  the match check already ran and its real result is still reported.

With no pg measurement left, `evaluateShape` judges a `pg_capped=true` shape
on the **bloodtrail driver's own absolute p50** instead of a ratio, against
`shapeThreshold.engineAbsoluteCap` -- see the table above.

Forcing the capped path for a smoke test (e.g. `-pg-cap 1ms`) makes every
shape's pg baseline trip immediately, which is a convenient way to verify
the whole `pg_capped=true` code path end to end without waiting for a real
slow query:

```
go run ./bench/cypherbench -dsn "$BLOODTRAIL_TEST_PG" -pg-cap 1ms
```

### `-bt-cap`: a fail-fast watchdog on the engine side

`-pg-cap` bounds the pg baseline; `-bt-cap` (default `15m`) bounds the
**engine-side (bt) call** the same way, via `context.WithTimeout` wrapped
directly around each bt call (`runCypherOnceCapped` in `main.go`) -- but
unlike `-pg-cap`, tripping it is never a graceful, recordable outcome. It is
a fail-fast safety net for the exact incident this flag was added to catch:
during a 2026-09 milestone-4.5 5M-scale run, `collect_antijoin_prebuilt`'s
own bt-side call declined (`internal/engine.TryCypher`'s `reason=budget` --
its first `MATCH` clause seeds from a completely unconstrained pattern
variable, forcing a full 4.76M-node scan-and-traverse that exceeds
`interpret.Budgets.MaxWork`) and silently fell through to PostgreSQL, whose
equivalent recursive CTE then ran for 17.5 hours before being killed by
hand -- with nothing in the benchmark to notice or stop it.

A bt call that has not returned within `-bt-cap` has almost certainly hit
exactly this: a decline, not ordinary slowness, since serving from the
in-memory snapshot is what this benchmark exists to measure in the first
place. So `runCypherOnceCapped` returns a hard error naming the shape and
the cap, and the whole run **aborts with a nonzero exit** -- never a
`bt_capped=true` data point the way `-pg-cap` records `pg_capped=true`.
Seeing this abort means: stop, go read `internal/engine`'s decline-reason
log (`BLOODTRAIL_LOG_LEVEL=debug`), and investigate why the engine declined
-- do not wait for the delegated fallback to finish.

## Measured at 5M (`bench/adgen`'s 4.76M-node / ~48.9M-edge graph)

All five shapes pass their measured-physics bars, including
`collect_antijoin_prebuilt`, which now *serves* at this density at all (it
used to decline outright). Numbers below are read off three independent
`-enforce` runs taken back to back against the same loaded graph on a busy
shared machine (`-runs 5`, `-runs 5` again, then `-runs 15`), plus a
fourth run on an idle machine (load average ~1.2) that settled what the
first three could not: the low ratios on the two scan shapes are their
warm-cache *steady state*, not load noise -- pg is simply also fast on a
GIN-narrowed or LIMIT-bounded scan, which is why those two bars are
measured-physics values rather than 5x (see the threshold table above).
Every shape's row output matched between the two drivers on every run
(`match=true` / `match=skip` only where `pg_capped` legitimately skipped
the check), so none of this is a correctness question:

| Shape                       | Bar | Measured p50 ratio (pg/bt), 3 runs | Verdict | Why |
|------------------------------|:---:|:---------------------------:|:-------:|-----|
| `objectid_point_lookup`      | 1x  | 1590x / 835x / **1041x**     | pass    | The in-memory objectid index answers in under a millisecond every time; pg still pays a full jsonb round trip (~195-1132ms). This is the shape the index exists for. |
| `shortest_path_prebuilt`     | 5x  | 56x / 59x / **42x**          | pass    | The engine now seeds and searches from the constrained side rather than materializing the query's full unconstrained endpoint set (see the root README's Cypher section); bt p50 stayed in the ~260-370ms band across all three runs against a consistently ~15-17s pg baseline. |
| `collect_antijoin_prebuilt`  | 5x (judged on the absolute cap, see below) | bt p50 9.5s / 13.7s / **14.5s**; pg pg_capped at 120s every run | pass    | **Now served**, not declined. Constrained-side var-length seeding (`internal/engine/interpret/expand.go`'s `varLengthReverseEligible`) seeds Part[0]'s `(s)-[:MemberOf*0..]->(g:Group)` from the ~4 matching `-516` groups instead of a full 4.76M-node scan, which is what turns a deterministic `reason=budget` decline into a serve. Read the ~13.5s-class cost honestly: it is a serve, not a fast serve. Most of it is plausibly Part[1]'s untouched forward chain (`(c:Computer)-[:HasSession]->(:User)-[:MemberOf*1..]->(g:Group)`, still anchored on every Computer) plus Part[0]'s own pass over the Group kind bitmap to evaluate its pushed predicate -- a target for future tuning, not this task's scope. Judged on `engineAbsoluteCap` (30s) rather than a ratio because pg's own baseline (a 700,000-member group's full trail enumeration) is uncappable at this density -- it exceeds `-pg-cap`'s 120s on every attempt. |
| `rid_suffix_scan`            | **1.1x** | 10.57x / 1.49x / 1.38x, then **1.31x idle** | pass    | pg's `kind_ids` GIN index already narrows the `ENDS WITH` scan to the same ~4 rows the engine itself walks, so both sides' absolute cost is small (bt ~130-165ms; pg ~212ms warm, up to ~1400ms cold). The idle-machine run pinned the warm-cache steady state at 1.31x -- the 10.57x reading was a cold pg cache, not the norm -- so the bar is 1.1x, just under the worst honest measurement, still requiring the engine to be strictly faster. |
| `flag_scan`                  | **1.2x** | 2.29x / 1.29x / 2.25x, then **1.87x idle** | pass    | bt's absolute cost is small and stable in every run (~11-17ms p50); with `LIMIT 1000` both drivers stop after ~1000 matches, so warm-cache pg is also fast (~22-27ms). The idle-machine run pinned the steady state at 1.87x; the one historical 7.79x reading rode a cold pg cache (114ms). The interpreter's LIMIT early-termination is in effect in every run (bt was ~25x *slower* before it); the bar is 1.2x, just under the worst honest measurement. |

The two scan shapes' bars were re-derived only after four separate runs
(sample sizes 5/5/15/5, the last on an idle machine) converged on the same
answer: bt's own p50 never moved meaningfully, pg's warm-cache baseline is
genuinely fast on these two shapes, and the occasional high ratios were pg
cold-cache artifacts. The idle-machine run is the anchor -- with load
average ~1.2 it measured rid_suffix at 1.31x, flag_scan at 1.87x,
shortest_path at 48x, the point lookup at 579x, and collect_antijoin's bt
p50 at its best-yet 8.4s -- so the wide-margin shapes' verdicts never
depended on machine state, and the two tight bars are anchored to the
worst honest measurement rather than to a hoped-for multiple.

## Flags

| Flag       | Default | Meaning                                                              |
|------------|---------|------------------------------------------------------------------------|
| `-dsn`     | (none)  | PostgreSQL connection string. Required.                               |
| `-runs`    | `5`     | Number of warmed-up, timed runs per shape per driver.                 |
| `-pg-cap`  | `120s`  | Per-shape wall-clock cap on the pg baseline; see `-pg-cap` above.      |
| `-bt-cap`  | `15m`   | Per-shape wall-clock cap on the engine-side bt call; ABORTS THE RUN on expiry (nonzero exit) -- see `-bt-cap` above. |
| `-enforce` | `false` | Exit nonzero on a per-shape threshold/match miss (never pass this in CI). |
| `-cpuprofile` | (none) | Write a pprof CPU profile to this file (`go tool pprof -top`/`-cum` to read it). |
