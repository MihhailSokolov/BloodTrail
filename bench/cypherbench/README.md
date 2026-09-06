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
go run ./bench/cypherbench -dsn <pg dsn> [-runs 5] [-enforce]
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
the bloodtrail driver's background poller to build its own first snapshot,
exactly as `builderbench` does (there is no exported hook to observe that
completing, so the wait is a safety multiple of the just-measured build cost
plus the poller's own poll interval, printed alongside it). After all five
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

With `-enforce`, `cypherbench` exits nonzero if any shape fails its p50
ratio bar (delegated pg / served bt) or if the two drivers' result row
counts ever disagree:

| Shape                        | Minimum p50 ratio |
|-------------------------------|--------------------|
| `rid_suffix_scan`             | 5x                 |
| `flag_scan`                   | 5x                 |
| `objectid_point_lookup`       | 1x                 |
| `shortest_path_prebuilt`      | 5x                 |
| `collect_antijoin_prebuilt`   | 5x                 |

`objectid_point_lookup`'s bar is weaker than the rest: a single-row equality
lookup against jsonb's own indexing on `properties->>'objectid'` is already
fast on the pg side, so there is no full scan or traversal for the engine's
in-memory objectid index to out-run the way there is for the other four
shapes -- the spec only requires the engine not be *slower*.

**CI must never pass `-enforce`.** Any other failure (a database error, an
empty graph, a driver that returns an outright error) aborts the run with a
nonzero exit regardless of `-enforce`.

## Flags

| Flag       | Default | Meaning                                                   |
|------------|---------|------------------------------------------------------------|
| `-dsn`     | (none)  | PostgreSQL connection string. Required.                    |
| `-runs`    | `5`     | Number of warmed-up, timed runs per shape per driver.       |
| `-enforce` | `false` | Exit nonzero on a threshold miss (never pass this in CI).   |
