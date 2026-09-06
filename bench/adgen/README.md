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
  (`Users/2`), and Groups (`Users/5`, minimum 4, to hold the four well-known
  groups below) are split as evenly as possible across domains. Large
  Active Directory forests typically have a handful of domains rather than
  dozens; the 5M-node benchmark should be
  generated with `-domains 4` to match realistic domain density and path
  engine constraints (the engine's per-query SideBudget of 16 limits queries
  to forests with at most 16 Domain Admins groups in memory at once).
- Every domain has exactly one each of four **well-known, RID-suffixed
  groups**, following real AD's well-known RIDs: **Domain Admins**
  (`-512`), **Domain Users** (`-513`, every user's hub), **Domain
  Controllers** (`-516`), and **Enterprise Admins** (`-519`) -- the four
  RID suffixes the cypherbench RID-suffix query shape looks up. Every other
  principal gets a synthetic domain-scoped RID starting at 1000.
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
- Every node carries kinds `["Base", <User|Computer|Group>]` and a
  realistic property bag beyond just `objectid`/`name` -- see "Property
  bags" below.

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

### Density realism

The ~25M `MemberOf` edges out of ~48.9M total edges (>50% of all edges)
reflects realistic Active Directory group-membership structure. The benchmark
hub -- a single 700k-member group in the `-users 2800000 -domains 4` run --
is the per-domain Domain Users group, created by `generateDomainEdges`
(line 532-535 in generate.go): every user gets one guaranteed `MemberOf`
edge to this hub by construction. In real Active Directory, every user is a
member of the domain's Domain Users group via its primaryGroupID, and both
SharpHound and BloodHound materialize this as an explicit `MemberOf` edge.

Beyond the hub, each user gets `extraMemberOfPerUser` (8) additional `MemberOf`
edges to random groups, yielding ~9 memberships per user (1 hub + 8 extra).
Real enterprise Active Directory commonly assigns 10 or more group memberships
per user -- the number is high enough that Kerberos token-bloat and PAC
(Privileged Attribute Certificate) size limits are a real concern in
production deployments. The ~9 figure here is the *low* end of realistic
enterprise group-membership density.

When every user carries ~9 `MemberOf` edges and group-nesting adds further
transitive memberships, `MemberOf` dominates the overall edge count. This is
exactly what real BloodHound graph imports show: real graphs are `MemberOf`-heavy
because real Active Directory is. The generated shapes are therefore authentic
stress cases for pathfinding algorithms: a 700k-in-degree group and group-member
BFS are genuinely bottlenecks in real BloodHound operations, not artifacts of
the generator's construction.

Generation is a pure, deterministic function of `Spec{Users, Seed}`
(`generate.go`'s `Generate`): the same `-users`/`-seed` pair always produces
the same graph *structure* -- same node/edge counts, same objectids, same
edges, same boolean flags and day-offsets in every property bag. The one
deliberate exception is wall-clock-anchored timestamp values (see "Property
bags" below): those differ between two runs of the same `-users`/`-seed`
pair by however much real time passed between the runs, even though the
*offsets* they were computed from are identical. `generate_test.go` covers
determinism at `-users 1000` (fast, no database needed): structural
determinism (with the reference wall-clock time pinned, so property bags
are checked byte-for-byte too), seed variance, exact node counts, one well-
known RID-suffixed group of each kind per domain, every edge referencing a
valid node index, and a guaranteed multi-hop path from a user to Domain
Admins.

Duplicate `(start, end, kind)` triples that the random edge categories can
produce (e.g. two different categories independently choosing the same pair)
are deduplicated before the graph is returned, since the database enforces
`unique (start_id, end_id, kind_id, graph_id)` on the edge table.

### Property bags

Beyond `objectid`/`name`, every node carries a deterministically generated
property bag sized and distributed to approximate fixture-measured upstream
reality (mean ~8.5 properties / ~374 bytes per node overall; principals
15-31 properties / 500-900 bytes in that measurement). This matters for the
milestone's 5M-node cypher/memory benchmarks (task 21): a graph with only
`{objectid, name}` on every node understates both memory footprint and the
property-predicate work real Cypher queries do.

| Field                 | User | Computer | Group | Notes                                                              |
|-----------------------|:----:|:--------:|:-----:|---------------------------------------------------------------------|
| `objectid`, `name`    | Y    | Y        | Y     | Always present (unchanged from before this property-bag work).      |
| `enabled`              | Y    | Y        |       | 95% true.                                                            |
| `admincount`           | Y    | Y        | Y     | 5% true for User/Computer, 10% true for Group.                       |
| `lastlogontimestamp`   | Y    | Y        |       | Epoch seconds, 0-400 days before generation time (see below).       |
| `pwdlastset`           | Y    | Y        |       | Epoch seconds, 0-400 days before generation time.                    |
| `whencreated`          | Y    | Y        |       | Epoch seconds, 0-400 days before generation time.                    |
| `samaccountname`       | Y    | Y        |       | Derived from the node's local name (lowercase for User, `NAME$` for Computer). |
| `distinguishedname`    | Y    | Y        |       | Realistic-depth synthetic DN, ~90 bytes.                             |
| `lastseen`             | Y    | Y        |       | RFC3339 string, set to generation time.                              |
| `hasspn`               | Y    |          |       | 2% true.                                                             |
| `dontreqpreauth`       | Y    |          |       | 1% true.                                                             |
| `pwdneverexpires`      | Y    |          |       | 20% true.                                                            |
| `operatingsystem`      |      | Y        |       | Weighted mix, mostly modern builds with a legacy tail (see below).   |
| `haslaps`              |      | Y        |       | 60% true.                                                            |
| `description`          |      |          | Y     | Realistic-length string, ~60 bytes.                                  |

**Regenerating a graph**: re-running the identical `adgen -users N -seed S`
command later reproduces the same topology, objectids, edges, and every
flag/offset in each property bag -- but *not* byte-identical timestamp
values, since those are anchored to wall-clock generation time rather than
`-seed` (see "Timestamps are anchored to generation time" below). That is
expected, not a determinism regression: don't diff two loads' raw
`lastlogontimestamp`/`pwdlastset`/`whencreated`/`lastseen` values as a
reproducibility check -- diff the node/edge structure and the boolean
flags instead, the way `generate_test.go`'s determinism tests do.

`operatingsystem`'s legacy tail (`WINDOWS SERVER 2008 R2 STANDARD`,
`WINDOWS 7 PROFESSIONAL`) and modern majority (`WINDOWS SERVER 2019
DATACENTER`, `WINDOWS 11 ENTERPRISE`, plus `WINDOWS SERVER 2022 DATACENTER`
and `WINDOWS 10 ENTERPRISE`) are deliberately drawn from strings that match
the pre-built Cypher corpus' own legacy-OS regex
(`(?i).*Windows.* (2000|2003|2008|2012|xp|vista|7|8|me|nt).*`, see
`testdata/prebuilt/agt.json` and `internal/graphtest/corpusfixture.go`'s
`legacyOS` pool) -- see `generate.go`'s `operatingSystems` var.

**Target**: mean marshaled-JSON bag size across every principal (User +
Computer) node lands in 400-700 bytes;
`TestGeneratePrincipalPropertyBags` in `generate_test.go` generates a
sample graph, marshals every principal's bag, and asserts both the mean
size and every flag's true-rate land within a generous tolerance band of
their target probabilities above. `TestGenerateGroupPropertyBag` covers the
Group shape the same way. Edge properties stay `{}` (see "Edges stay
empty" below) and are not part of this target.

#### Timestamps are anchored to generation time, not to `-seed`

`lastlogontimestamp`/`pwdlastset`/`whencreated`/`lastseen` are computed
from the wall-clock time `Generate` runs at (`time.Now()` at the top of the
call, once for the whole graph), not from `-seed`. This is a deliberate
departure from Generate's otherwise-total `-seed`/`-users` determinism:
real AD's own timestamp properties are epoch seconds that Cypher hygiene
queries compare against `datetime().epochseconds - N*86400` at *query*
time (the milestone-4 corpus fixture,
`internal/graphtest/corpusfixture.go`, makes this same tradeoff, see its
"Time-dependent predicates" section). A graph whose timestamps were pinned
to a `-seed`-derived point in time would silently age out of any such
window the longer it sat unqueried after generation -- a `-seed 1` graph
generated today and one generated next month would need identical
"days-old" timestamps to both still look "recently active", which is only
possible if the anchor moves with generation time.

What *is* `-seed`/`-users`-deterministic is each node's **offset**: how
many days before generation time its timestamps land, drawn from the same
seeded `*rand.Rand` stream as every other flag. Re-running
`-users 1000 -seed 1` next week reproduces the identical graph shape --
same objectids, same edges, same enabled/admincount/hasspn/etc. flags, same
*relative* recency ordering between nodes -- but every absolute timestamp
value shifts by the week that passed. `nowFunc` in `generate.go` is the
single seam this goes through (a package variable, not an inlined
`time.Now()` call), which is what lets `generate_test.go` pin it to a fixed
instant and assert full byte-for-byte reproducibility including timestamps
when that matters, and separately assert (`TestGeneratePrincipalTimestampsAnchorToGenerationTime`)
that only the anchor moves between two different pinned instants at the
same seed.

### Edges stay empty

Edge rows are always written with `properties = {}` (see `main.go`'s
`newEdgeCopySource`) -- `Edge` doesn't even have a `Props` field. This is
deliberate, not an oversight: the milestone's path/cypher engine never
keeps edge properties resident in memory (only node property bags and the
graph topology are loaded), and no query in the pre-built Cypher corpus or
the cypherbench query shape filters or projects on an edge property.
Spending bytes on edge property bags at 5M nodes / ~50M edges would inflate
load time and disk/network I/O for data no benchmarked code path ever
reads.

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
| `-seed`   | `1`     | Random seed; same seed + `-users` reproduces the same graph structure and property-bag flags/offsets (timestamps re-anchor to the new run's generation time -- see "Property bags"). |
| `-domains`| `0`     | Number of domains; `0` (default) uses automatic 1-per-50k rule, `N > 0` forces exactly N domains. |
| `-wipe`   | `false` | Truncate `node`/`edge` (every graph) before loading.             |

Progress is reported to stderr, including a line every ~100,000 rows during
the node and edge loads at large scales.
