# pathbench

`pathbench` benchmarks the in-memory path engine (`internal/engine`) against
a graph already loaded into PostgreSQL, normally by
[`bench/adgen`](../adgen). It never writes to the database and never imports
`bench/adgen` -- it finds its benchmark inputs purely through SQL, using the
same kind/`objectid` conventions `adgen`'s generated graphs follow.

```
go run ./bench/pathbench -dsn <pg dsn> [-runs 20] [-seed 1] [-enforce] [-cpuprofile <file>]
```

Or, against `BLOODTRAIL_TEST_PG`:

```
make bench-path
```

## What it measures

1. **Build**: `engine.LoadSnapshot`, reporting nodes/edges/`ApproxBytes`.
2. **(a) random pairs**: `-runs` seeded-random User->Computer pairs, each a
   pure `traverse.AllShortestPaths` (`ModeAll`, every edge kind `adgen`
   emits) call -- p50/p95 latency.
3. **(a, hydrated)**: the same pairs served through
   `engine.Engine.TryAllShortestPaths`, the real production path, which
   additionally fetches full node/edge properties from PostgreSQL -- shows
   the hydration cost on top of (a).
4. **(b) shortest paths to Domain Admins**: every `-512` group as an
   unconstrained-root, `ModeOne`, `Limit` 1000 query -- total wall time.
5. **(c) rebuild**: a second, separately timed `LoadSnapshot` call.

Every section prints a human-readable line and a machine-greppable
`PATHBENCH_*` summary line (`grep '^PATHBENCH_'`), ending in
`PATHBENCH_RESULT PASS` or `FAIL`.

## `-enforce`

With `-enforce`, `pathbench` exits nonzero if (a)'s p95 exceeds 180ms or
(b)'s total time exceeds 5s -- the two thresholds this benchmark enforces,
meant for an operator run against a realistically sized graph. **CI must
never pass `-enforce`.** Any other failure (a database error, an empty
graph, an engine decline that should not happen for a single-ID pair query)
aborts the run with a nonzero exit regardless of `-enforce`.

### Where the 180ms bar comes from

Derived 2026-09-12 the way every other enforced bar in `bench/` already is:
worst measured x ~1.75 headroom, pinned by `TestMeasuredPathThresholds`.
Three fresh runs at 5M scale (4,760,000 nodes / 48,886,562 edges,
`adgen -users 2800000 -domains 4 -seed 1`, moderate background load)
measured pairs p95 103.7 / 99.0 / 85.1 ms -> 103.7 x 1.75 ~= 181 -> 180ms.

Bar history: the original 100ms came from milestone 2's target, set before
any 5M measurement existed, and was the only enforced bar in the repository
never re-derived from measurement. At this scale it sits exactly on the
worst pairs' own cost -- a seeded-random pair whose BFS crosses the
per-domain hub groups reaches a large fraction of the graph, and that
breadth genuinely costs ~85-110ms single-threaded -- so runs straddled
PASS/FAIL on ambient load alone (fail 3/3 at milestone 5's close, then
1/3 fail, on the same graph and machine). Two real defects were found and
fixed while running it down, which is the tripwire doing its job: a
discarded per-expansion allocation in backward BFS (p95 255 -> ~124ms,
`56bcbb5`), and per-query scratch allocation -- three fresh ~24MB
mark/dist buffers per `AllShortestPaths` call -- replaced by a
`sync.Pool` (pairs p50 ~0.35 -> ~0.03ms; the p95 pairs keep their genuine
traversal cost). 180ms still trips on both: the first measured 215-255ms,
and an undone pool would resurface as a p95 well past the bar under any
concurrent load.

### Measured 2026-09-12 (post scratch-pool, 5M graph)

| Section | run 1 | run 2 | run 3 |
|---|---|---|---|
| pairs p50 (ms) | 0.022 | 0.025 | 0.046 |
| pairs p95 (ms) | 103.7 | 99.0 | 85.1 |
| Domain Admins (s) | 0.28 | 0.29 | 0.49 |
| build (s) | 44.7 | 44.0 | 43.4 |

## Flags

| Flag          | Default | Meaning                                                          |
|---------------|---------|-------------------------------------------------------------------|
| `-dsn`        | (none)  | PostgreSQL connection string. Required.                           |
| `-runs`       | `20`    | Number of random User->Computer pairs for section (a).            |
| `-seed`       | `1`     | Seed for deterministic pair selection.                             |
| `-enforce`    | `false` | Exit nonzero on a threshold miss (never pass this in CI).          |
| `-cpuprofile` | (none)  | Write a pprof CPU profile to this file.                            |
| `-cap`        | `30m`   | Wall-clock cap on the whole run. Exceeding it **aborts** with a nonzero exit and no report, rather than waiting: every query this benchmark makes -- the two snapshot loads, the rebuild, each sampled pair, and the property hydration behind a served answer -- hangs off that one deadline, so a wedged PostgreSQL round trip is reported instead of parking the run indefinitely. |
