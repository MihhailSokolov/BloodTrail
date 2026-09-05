# adgen

`adgen` generates a deterministic, Active-Directory-shaped synthetic graph
and bulk-loads it directly into a PostgreSQL database's `node`/`edge`
tables, for benchmarking and manual testing of the path engine at scale.

```
go run ./bench/adgen -dsn <pg dsn> -users 1000 -seed 1 [-wipe]
```

Once a graph is loaded, see [`bench/pathbench`](../pathbench) to actually
benchmark the path engine against it (`make bench-path`).

## What it generates

The shape is driven entirely by `-users` and optionally by `-domains`:

- **Domains**: by default, one domain per 50,000 users (minimum one). Pass
  `-domains N` to override with exactly N domains. Users, Computers
  (`Users/2`), and Groups (`Users/5`, minimum 2) are split as evenly as
  possible across domains. Large Active Directory forests typically have a
  handful of domains rather than dozens; the 5M-node benchmark should be
  generated with `-domains 4` to match realistic domain density and path
  engine constraints (the engine's per-query SideBudget of 16 limits queries
  to forests with at most 16 Domain Admins groups in memory at once).
- Every domain has exactly one **Domain Admins** group (objectid suffix
  `-512`) and one **Domain Users** hub (objectid suffix `-513`), following
  real AD's well-known RIDs. Every other principal gets a synthetic
  domain-scoped RID starting at 1000.
- Every **User** is a member of the domain's Domain Users hub, plus 8 extra
  `MemberOf` edges to random groups.
- **Groups nest**: each non-well-known group has a 30% chance of being a
  member of an earlier group in the same domain, capped at nesting depth 5.
- A small set (~2%) of a domain's groups hold **AdminTo** over 50% of the
  domain's computers.
- 30% of **Computers** have a session (`HasSession`) of a random user.
- **ACL edges** (`GenericAll`, `WriteDacl`, `AddMember`) run from random
  users/groups to random groups, combined density 8 edges per user. One
  explicit chain per domain (a user, a `GenericAll` edge to a group, an
  `AddMember` edge from that group to Domain Admins) is always added on top
  of the random ACL noise, guaranteeing at least one multi-hop path from a
  user to Domain Admins in every generated graph.
- Every node carries kinds `["Base", <User|Computer|Group>]` and properties
  `{"objectid": ..., "name": ...}`.

### Edge density: ~10 edges per node, by default

The milestone's path-engine performance targets are defined at ~5M nodes /
~50M edges, i.e. **~10 edges per node**. `adgen`'s per-user/per-computer
budgets above (8 extra `MemberOf`, 8 ACL edges, 50%/30% `AdminTo`/
`HasSession` computer coverage) were sized so that a plain default run (no
flags beyond `-users`) lands on that design point without needing a
separate density knob:

| `-users`  | domains | nodes (exact)     | edges (measured)  | edges/node |
|-----------|---------|--------------------|--------------------|------------|
| 1,000     | 1       | 1,700              | 17,197             | 10.12      |
| 2,000     | 1       | 3,400              | 34,663             | 10.20      |
| 100,000   | 2       | 170,000            | 1,745,527          | 10.27      |
| 2,800,000 | 56      | 4,760,000 (exact)  | ≈48,900,000 (formula estimate) | ≈10.3 |

The `-users 2800000` row is not run in practice (see "At large scale" below
for why) -- it's derived from the same per-domain formulas as every other
row: 56 domains of exactly 50,000 users each (the same per-domain size as
the measured `-users 100000` row above, which has 2 such domains), so
`4,760,000` nodes is exact arithmetic and `≈48.9M` edges is that row's
measured per-domain edge count (872,763.5 edges/domain) extrapolated to 56
domains -- consistent with the milestone's ~50M-edge target at ~5M nodes.
`TestGenerateDefaultEdgeDensityNear10` in `generate_test.go` pins this down
with a generous [8, 12] edges/node tolerance band at `-users 100000` (fast,
no database needed) rather than asserting an exact ratio, since the ACL/
nesting categories are randomized.

Generation is a pure, deterministic function of `Spec{Users, Seed}`
(`generate.go`'s `Generate`): the same `-users`/`-seed` pair always produces
byte-for-byte the same graph. `generate_test.go` covers this at
`-users 1000` (fast, no database needed): determinism, seed variance, exact
node counts, one `-512` group per domain, every edge referencing a valid
node index, and a guaranteed multi-hop path from a user to Domain Admins.

Duplicate `(start, end, kind)` triples that the random edge categories can
produce (e.g. two different categories independently choosing the same pair)
are deduplicated before the graph is returned, since the database enforces
`unique (start_id, end_id, kind_id, graph_id)` on the edge table.

### The `-users` knob and scale

Node/edge counts scale roughly linearly with `-users` (see the table
above). The generator favors formulas that stay linear in `-users` (no
term scales with the product of two counts that both grow with `-users`,
e.g. `AdminTo` targets a share of computers rather than the full
admin-groups-by-computers cross product) so that large runs stay
tractable; precision against the table above is intentionally loose --
treat the exact totals `adgen` prints to stderr as the source of truth for
a given run.

At large scale, `Generate` builds the entire graph in memory before writing
anything (its signature returns a whole `Graph`), so peak memory scales with
`-users` too. A `-users 2800000` run is a multi-gigabyte, multi-minute
undertaking; start smaller (`-users 1000` to `-users 100000`) unless you
specifically need the large-scale benchmark shape.

## Write path

`adgen` does **not** go through the dawgs driver's normal per-node/edge
write API. Instead:

1. It asserts the schema and default graph through the pg driver (the same
   `bloodtrail_test` graph name `internal/graphtest` and the repo-root
   `driver_integration_test.go` use, so `pathbench` and the engine's
   snapshot loader find the data by resolving the driver's default graph the
   same way the rest of the project does), declaring every node/edge kind
   `Generate` can produce up front.
2. It pre-asserts all kind strings via the driver's `KindMapper` to get
   their integer kind ids.
3. It streams nodes and edges into the partitioned `node`/`edge` tables
   directly with `pgx.CopyFrom` (columns include `graph_id`; PostgreSQL
   routes each row to the right partition, which the schema assertion step
   already created), assigning explicit, pre-computed ids rather than
   relying on the tables' `bigserial` default.
4. It fixes up the `node`/`edge` id sequences afterward
   (`setval(pg_get_serial_sequence(...), ...)`) so that any later writer
   using the driver's normal (`nextval`-based) insert path does not collide
   with the ids `adgen` just assigned.

This is substantially faster than row-at-a-time inserts at the scales this
tool targets, but it means `adgen` writes straight into the graph tables
below the driver's usual write API. **Do not point `-dsn` at a production
database** -- `adgen` is a bench/dev tool for a disposable or test database
only, and `-wipe` truncates the `node`/`edge` tables for *every* graph in
the database, not just the one it is about to load.

The entire load (both `CopyFrom` calls plus the sequence fixups) runs
inside a **single database transaction with no checkpointing or resume
support**: if it fails or is interrupted partway (network blip, OOM,
`Ctrl-C`), the whole transaction rolls back and nothing is left partially
loaded -- but there is no way to resume from where it left off. Re-run the
whole command from scratch (after fixing whatever caused the failure). At
large `-users` values this means a failure late in a multi-minute run costs
the entire run, not just the remainder.

## Flags

| Flag      | Default | Meaning                                                        |
|-----------|---------|------------------------------------------------------------------|
| `-dsn`    | (none)  | PostgreSQL connection string. Required.                          |
| `-users`  | `1000`  | Number of User principals; drives every other count (see above). |
| `-seed`   | `1`     | Random seed; same seed + `-users` reproduces the same graph.     |
| `-domains`| `0`     | Number of domains; `0` (default) uses automatic 1-per-50k rule, `N > 0` forces exactly N domains. |
| `-wipe`   | `false` | Truncate `node`/`edge` (every graph) before loading.             |

Progress is reported to stderr, including a line every ~100,000 rows during
the node and edge loads at large scales.
