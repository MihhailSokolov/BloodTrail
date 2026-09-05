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
go run ./bench/builderbench -dsn <pg dsn> [-runs 5] [-enforce]
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
   startup) -- every node transitively a member of that group.
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

With `-enforce`, `builderbench` exits nonzero if any shape's p50 ratio is
below **5x**, or if that shape's two drivers' results mismatched (a
correctness guard: each shape's warmup call compares a count/node-set-size/
edge-set-size between the bloodtrail driver and the plain pg driver, so a
silently-delegating or outright-wrong-serving engine fails loudly instead of
just looking slow).

A 5x ratio is itself strong evidence the query was actually served from the
in-memory snapshot: delegating is delegating, on the same PostgreSQL tables,
at essentially the same cost either way, so a 5x speedup is not achievable
by two drivers that both just forward to PostgreSQL. This is why
`-enforce` needs no separate "did it serve" signal beyond the ratio itself.

**CI must never pass `-enforce`.** Any other failure (a database error, an
empty graph, a driver error) aborts the run with a nonzero exit regardless
of `-enforce`.

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
point it at anything you care about. `builderbench` itself never writes
graph data; the only write is its own scratch `datapipe_status` row,
created and dropped within one run.

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
| `-enforce` | `false` | Exit nonzero on a ratio/match miss (never pass this in CI).           |
