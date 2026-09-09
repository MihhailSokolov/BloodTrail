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

With `-enforce`, `pathbench` exits nonzero if (a)'s p95 exceeds 100ms or
(b)'s total time exceeds 5s -- the two thresholds this benchmark enforces,
meant for an operator run against a realistically sized graph. **CI must
never pass `-enforce`.** Any other failure (a database error, an empty
graph, an engine decline that should not happen for a single-ID pair query)
aborts the run with a nonzero exit regardless of `-enforce`.

## Flags

| Flag          | Default | Meaning                                                          |
|---------------|---------|-------------------------------------------------------------------|
| `-dsn`        | (none)  | PostgreSQL connection string. Required.                           |
| `-runs`       | `20`    | Number of random User->Computer pairs for section (a).            |
| `-seed`       | `1`     | Seed for deterministic pair selection.                             |
| `-enforce`    | `false` | Exit nonzero on a threshold miss (never pass this in CI).          |
| `-cpuprofile` | (none)  | Write a pprof CPU profile to this file.                            |
