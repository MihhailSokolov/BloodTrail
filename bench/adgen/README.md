# adgen

`adgen` generates a deterministic, Active-Directory-shaped synthetic graph
and bulk-loads it directly into a PostgreSQL database's `node`/`edge`
tables, for benchmarking and manual testing of the path engine at scale.

```
go run ./bench/adgen -dsn <pg dsn> -users 1000 -seed 1 [-wipe]
```

## What it generates

The shape is driven entirely by `-users`:

- **Domains**: one domain per 50,000 users, minimum one. Users, Computers
  (`Users/2`), and Groups (`Users/5`, minimum 2) are split as evenly as
  possible across domains.
- Every domain has exactly one **Domain Admins** group (objectid suffix
  `-512`) and one **Domain Users** hub (objectid suffix `-513`), following
  real AD's well-known RIDs. Every other principal gets a synthetic
  domain-scoped RID starting at 1000.
- Every **User** is a member of the domain's Domain Users hub, plus ~3 extra
  `MemberOf` edges to random groups.
- **Groups nest**: each non-well-known group has a 30% chance of being a
  member of an earlier group in the same domain, capped at nesting depth 5.
- A small set (~2%) of a domain's groups hold **AdminTo** over ~30% of the
  domain's computers.
- ~10% of **Computers** have a session (`HasSession`) of a random user.
- **ACL edges** (`GenericAll`, `WriteDacl`, `AddMember`) run from random
  users/groups to random groups, combined density ~1.5 edges per user. One
  explicit chain per domain (a user, a `GenericAll` edge to a group, an
  `AddMember` edge from that group to Domain Admins) is always added on top
  of the random ACL noise, guaranteeing at least one multi-hop path from a
  user to Domain Admins in every generated graph.
- Every node carries kinds `["Base", <User|Computer|Group>]` and properties
  `{"objectid": ..., "name": ...}`.

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

Node/edge counts scale roughly linearly with `-users`; at `-users 1000` this
generates ~1,700 nodes and a few thousand edges. As a rough sizing
reference from the graph engine's benchmark target: `-users 2800000`
generates on the order of several million nodes. Precision here is
intentionally not tuned to hit an exact edges-per-node ratio -- the
generator favors formulas that stay linear in `-users` (no term scales with
the product of two counts that both grow with `-users`) so that large runs
stay tractable; treat the exact totals `adgen` prints to stderr as the
source of truth for a given run rather than this approximation.

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

## Flags

| Flag      | Default | Meaning                                                        |
|-----------|---------|------------------------------------------------------------------|
| `-dsn`    | (none)  | PostgreSQL connection string. Required.                          |
| `-users`  | `1000`  | Number of User principals; drives every other count (see above). |
| `-seed`   | `1`     | Random seed; same seed + `-users` reproduces the same graph.     |
| `-wipe`   | `false` | Truncate `node`/`edge` (every graph) before loading.             |

Progress is reported to stderr, including a line every ~100,000 rows during
the node and edge loads at large scales.
