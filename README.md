# BloodTrail

An in-memory graph engine underneath [BloodHound CE](https://github.com/SpecterOps/BloodHound),
packaged as a [DAWGS](https://github.com/SpecterOps/DAWGS) driver, so that attack-path
analysis and every other graph query stay fast on large Active Directory environments
and ordinary hardware.

**Status:** milestone 4 (Cypher interpreter). Shortest paths, all shortest paths, and a
defined set of structural node/relationship queries BloodHound's query builder issues are
served from an in-memory replica when it is fresh, and so, now, is a much broader surface of
Cypher itself -- property predicates and scans, point lookups, `shortestPath`/
`allShortestPaths` patterns, and a range of aggregations -- reached through a real Cypher
interpreter rather than pattern-matching a handful of recognized shapes. Every query the
interpreter cannot (or should not) answer from memory delegates to PostgreSQL, exactly as
before.

## Why

BloodHound answers multi-hop questions by asking its graph database to expand a
frontier hop by hop: through Neo4j's record store, or through a PL/pgSQL breadth-first
harness on PostgreSQL. On large forests the shipped "shortest paths" queries time out,
and the official sizing guidance above 50,000 users is 96 GB of RAM.

The part of the graph that path questions need, node ids, node kinds and edge kinds,
fits in about 0.6 GB for 5 million nodes and 50 million edges as compressed-sparse-row
arrays, and a single CPU core sweeps every edge in under a second. See
[bench/csrbench](bench/csrbench) for the measurement.

## How it works

- BloodTrail is a DAWGS driver, selected with `graph_driver: "bloodtrail"`. BloodHound's
  ingest, analysis, API and UI are unchanged; they talk to the same `graph.Database`
  interface as before.
- The driver keeps a replica of the graph in memory: dense ids, forward and reverse
  adjacency with an edge id and kind per entry, and kind bitmaps -- what milestone 2's
  shortest-path queries and milestone 3's structural node/relationship queries need --
  plus, as of milestone 4, every node's own property bag and an objectid index, so the
  Cypher interpreter can evaluate property predicates and point lookups without a
  PostgreSQL round trip. A served result's *edge* properties are still hydrated from
  PostgreSQL per query (edges carry no properties in the replica -- see
  [Cypher interpreter](#cypher-interpreter)'s Memory note for why). See
  [In-memory path engine](#in-memory-path-engine),
  [Query-builder serving](#query-builder-serving), and
  [Cypher interpreter](#cypher-interpreter) for what is actually served from the
  replica today.
- PostgreSQL remains the system of record. Writes go to PostgreSQL first; the replica
  itself is rebuilt wholesale from PostgreSQL by a poller (after every completed
  analysis run, and again once the ingest/analysis pipeline goes idle following a
  write) rather than updated write-through, and there is no snapshot-file restart yet.
- Deployment is a patched BloodHound image built from the upstream Dockerfile plus a
  one-file patch (the build script also adds the driver module to `go.mod`), and an
  installer that upgrades an existing BloodHound CE deployment with backup and
  rollback.
- **Planned, not yet built** (see [Roadmap](#roadmap)): write-through updates to the
  replica on commit instead of a poller-driven rebuild, and loading/restoring the
  replica from a snapshot file on startup.

## In-memory path engine

BloodHound's most expensive queries are shortest-path questions: the pathfinding tab, and the
pre-built shortest-path searches its UI ships (to Domain Admins, to Tier Zero, from
Kerberoastable users, and so on). BloodTrail answers these from the in-memory replica instead
of PostgreSQL whenever it safely can, and always falls back to PostgreSQL otherwise, so results
are correct regardless of the replica's state.

- **What is served.** `GET /api/v2/graphs/shortest-path` (BloodHound's `FetchAllShortestPaths`
  call, built from `start_node`/`end_node` object IDs and an optional relationship-kind filter).
  A Cypher `shortestPath(...)`/`allShortestPaths(...)` sent to `POST /api/v2/graphs/cypher` is
  also served from memory, but as of milestone 4 that goes through the general-purpose Cypher
  interpreter described in [Cypher interpreter](#cypher-interpreter), not a narrow
  shortestPath-only recognizer (milestone 2's original recognizer for this shape has since been
  retired in its favor). Every other read -- entity panels, node search, tagging -- is
  unaffected and always goes to PostgreSQL, exactly as in milestone 1.

- **Freshness and fallback.** The engine keeps a compressed in-memory replica (node/edge ids,
  kinds, and, as of milestone 4, node property bags; edge properties are still hydrated from
  PostgreSQL per query -- see [Cypher interpreter](#cypher-interpreter)), rebuilt from
  PostgreSQL after every completed analysis run and again after a write once the
  ingest/analysis pipeline goes idle.
  A query is served from memory only if the engine is enabled, a snapshot exists, that snapshot
  is still current (no write has landed since it was built), the query's endpoints resolve
  inside it, and the traversal fits the request's own memory budget. Any of these failing --
  disabled, no snapshot yet, a write in flight, an unrecognized query shape, or too large a
  traversal -- makes the engine decline outright and PostgreSQL answers instead; the only
  difference an operator or user should ever see is latency.

- **Single-writer assumption.** PostgreSQL stays the system of record: every write goes there
  first, through BloodTrail's own driver. The engine notices a write happened (invalidating its
  current snapshot) through that same driver call, then rebuilds once the pipeline is next idle
  or an analysis run completes. This means the engine assumes it is the only path writes take
  to the graph tables -- the normal shape of a BloodHound CE deployment, a single API server
  process. A second process writing to the same PostgreSQL graph without going through this
  driver instance would go unnoticed until the next analysis run.

- **Configuration** (environment variables, read once at driver startup):
  - `BLOODTRAIL_ENGINE` -- `on` (default) or `off` (also accepts `true`/`false`/`1`/`0`).
    `off` makes every read delegate straight to PostgreSQL, as in milestone 1.
  - `BLOODTRAIL_ENGINE_POLL_INTERVAL` -- the poller's rebuild-check cadence, parsed with Go's
    `time.ParseDuration` (e.g. `5s`, `1m`). Defaults to `5s`.
  - `BLOODTRAIL_MEMORY_LIMIT` -- caps the replica's approximate resident size (e.g. `4GiB`,
    `512MiB`, or a plain byte count). A rebuild that would exceed it is refused, and the engine
    keeps serving from (or falling back from) whatever snapshot it already had. Unset or `0`
    means unbounded.
  - `BLOODTRAIL_LOG_LEVEL` -- `debug`, `info`, `warn`, or `error`. When set, it widens the
    minimum level BloodTrail's own log lines are guaranteed to be visible at, on top of
    whatever already configures the process's logger -- it can only add visibility, never
    take it away. Left unset, it is a complete no-op. `debug` is what surfaces e.g.
    `bloodtrail: builder engine served`.

- **Log markers**, all under a `bloodtrail:` prefix: `bloodtrail: snapshot rebuilt` (Info, on
  every successful rebuild), `bloodtrail: snapshot rebuild refused: exceeds memory limit`
  (Warn, rate-limited), `bloodtrail: path engine served` (Info, once per shortest-path query
  actually answered from memory), `bloodtrail: path engine declined` (Debug, with a `reason`
  attr, whenever a shortest-path query fell back to PostgreSQL), their query-builder
  counterparts `bloodtrail: builder engine served` / `bloodtrail: builder engine declined`,
  and their Cypher-interpreter counterparts `bloodtrail: cypher engine served` /
  `bloodtrail: cypher engine declined` (all three of these last pairs Debug -- a structural
  or Cypher query runs far more often than a shortest-path one, so these stay one level
  quieter).

## Query-builder serving

Milestone 3 extends the same in-memory replica to also answer BloodHound's **query
builder** -- the fluent `Nodes()`/`Relationships()` API BloodHound's own Go code uses
internally, as distinct from a user's Cypher text. This is what backs, among other
things, the entity panel's member and controller listings and the structural scans
analysis itself issues while recomputing derived edges and tags. When a builder query
matches one of a defined set of structural shapes, and the replica is fresh enough for
it, BloodTrail answers it from memory instead of PostgreSQL:

- **Node queries** -- count, fetch ids, or fetch id-plus-kind listings -- for any query
  constrained by at least one node-kind filter. An `id()`-only filter has no kind to
  check freshness against, so it always delegates.
- **Relationship queries** -- count, fetch ids, fetch (id, start, end) triples, and fetch
  id-plus-kind-annotated triples -- for any combination of an edge-kind filter and
  endpoint id/kind filters.
- **Two row projections**: a bare (start, end) pair per edge from DAWGS's bulk directed-graph
  fetch, and a single traversal step's far endpoint (id and kinds) alongside the traversed
  edge's own id and kind, from BloodHound's own traversal driver. Both can honor the
  ascending-by-edge-id ordering that driver's paging relies on, whenever the scan is anchored
  to one endpoint's id.

Property predicates -- anything that filters or projects a node or relationship's
*property* rather than its id or kind -- always go to PostgreSQL through this
query-builder path: the recognized builder shapes above are id/kind-only, unchanged
from milestone 3, even though the replica itself gained node property bags in
milestone 4 for the [Cypher interpreter](#cypher-interpreter)'s own use. So does any
query shaped differently from the above -- more than one chained filter, an
ordering/offset/limit the engine doesn't implement, or a caller-supplied row
projection -- the same "engine declines, PostgreSQL always answers correctly" contract
the path engine already has.

- **Kind-scoped freshness.** Every write is now recorded against the specific node and
  edge kinds it actually touched, not only as a blanket "something changed." A builder
  query is served only if every kind it can observe is clean since the replica was
  built: a node query's own kind constraints; a relationship query's edge-kind filter,
  plus, if it also constrains either endpoint by kind, that endpoint's kinds too. A
  write to an unrelated kind elsewhere in the graph no longer blocks it. Shortest-path
  queries are unaffected by this and keep milestone 2's coarser rule -- any write at all
  invalidates them until the next rebuild. That rebuild now also happens, up to twice,
  *during* a running analysis: once as analysis starts, so builder queries over
  untouched source kinds keep serving fresh reads while analysis is still writing its
  own derived kinds, and once more if that first rebuild is itself made stale by
  analysis's own earliest writes.

- **Memory.** Serving relationship queries needs a bit more than the path engine's bare
  topology: each edge's own database id (~8 bytes), a reverse-index pointer back to it
  (~4 bytes), and a small permutation array to look an edge up by that id (~4 bytes) --
  roughly 16 bytes per edge on top of milestone 2's layout. Milestone 4 adds node
  properties and an objectid index on top of that in turn -- see
  [Cypher interpreter](#cypher-interpreter)'s own Memory note for the full formula and
  the measured total at 4.76 million nodes / ~48.9 million edges. `BLOODTRAIL_MEMORY_LIMIT`
  (see Configuration above) caps the whole replica the same way it always has: a
  rebuild that would exceed it is refused, and the engine keeps serving (or falling
  back from) whatever snapshot it already had.

See [bench/builderbench](bench/builderbench) for the measurement.

## Cypher interpreter

Milestone 4 replaces the narrow shortestPath/allShortestPaths-only Cypher recognizer
milestone 2 shipped with a real interpreter (`internal/engine/interpret`): it plans and
executes a meaningful subset of Cypher directly against the in-memory replica, rather
than pattern-matching a handful of hand-recognized query shapes. This is what backs
`POST /api/v2/graphs/cypher` -- both a user's own Cypher and every pre-built/selector
query BloodHound's UI ships.

- **What is served.** `MATCH` patterns filtered or projected by node/relationship
  property predicates (equality, comparisons, `ENDS WITH`/`STARTS WITH`/`CONTAINS`,
  boolean combinations), a property point lookup (including one served from the
  replica's own objectid index), fixed and variable-length relationship patterns
  including `shortestPath(...)`/`allShortestPaths(...)`, and a range of aggregations
  (`COUNT`, `COLLECT` and the anti-join pattern it commonly feeds -- `WITH COLLECT(...)
  AS x ... WHERE NOT n IN x`) and `ORDER BY` where the ordering is unambiguous without
  PostgreSQL's own collation. See the root package's `*_corpus_integration_test.go` /
  `random_cypher_differential_integration_test.go` for the differential suites that
  pin this behavior against a live PostgreSQL oracle across BloodHound's own pre-built
  query corpus and randomly generated Cypher alike. Speed, honestly: of
  `bench/cypherbench`'s five representative shapes measured at 4.76M nodes, one (an
  objectid point lookup) is ~657x faster than delegating, one is unmeasured at this
  scale (its pg baseline doesn't finish in reasonable time), and three currently miss
  their target ratio -- two of them slower than plain PostgreSQL, for reasons specific
  to each shape (a LIMIT the interpreter doesn't yet apply early; an unconstrained
  shortestPath endpoint set; a scan pg's own index already narrows about as well). See
  `bench/cypherbench/README.md`'s "Measured at 5M" table for the actual numbers and
  the reasoning behind each -- this is real, imperfect, in-progress performance, not a
  claim that every Cypher shape is already faster served locally.
- **What always delegates.** Any query the interpreter's planner does not recognize at
  all; any query bound `$parameters` (BloodHound's own cypher endpoint never sends
  these, so a non-empty `params` map can only mean something this interpreter has no
  way to honor); any updating clause (`CREATE`/`MERGE`/`DELETE`/`SET`/`REMOVE` -- this
  interpreter is read-only); a comparison or `ORDER BY` whose result depends on
  PostgreSQL's own string collation; and a numeric-list `IN` check the interpreter
  cannot evaluate without PostgreSQL's own numeric casting rules. Exactly as
  elsewhere, every decline falls back to PostgreSQL and always returns a correct
  result -- the only difference is latency.
- **The translate gate.** Because the interpreter is a reimplementation of a Cypher
  subset rather than a wrapper around dawgs' own PostgreSQL translator, nothing
  inherently guarantees the two agree on which queries are servable. Before executing
  a query the interpreter *did* accept, BloodTrail asks dawgs' real translator the same
  question a PostgreSQL-backed serve would eventually ask -- without a database round
  trip -- and declines (delegating to PostgreSQL as usual) if the translator would have
  rejected it. This closes the gap between "the interpreter thinks it can answer this"
  and "PostgreSQL would actually have accepted this query at all" -- it does not by
  itself prove the two compute identical answers. That equivalence is established
  empirically: every served query shape is differential-tested against a live
  PostgreSQL oracle (the corpus and randomized suites named in "What is served" above),
  not formally proven, and plan-time rejects exist specifically for every comparison/
  ordering shape those suites found the two engines could otherwise disagree on.
  **Known residual divergences** (both deliberately accepted, neither ever a *wrong*
  row -- only a dropped one or a value that can differ by a small amount):
  - A relational comparison (`<`/`<=`/`>`/`>=`) between a property and a statically
    numeric expression casts the property to a number on both sides; if that property
    holds a non-numeric value on some row, PostgreSQL aborts the whole query with a
    runtime error, while the interpreter just drops that one row instead of erroring.
    Unrealistic for real BloodHound timestamp-shaped data, which is why this shape is
    still served rather than declined outright.
  - `datetime()`'s epoch accessors are evaluated once, against BloodTrail's own host
    clock, at the moment it starts executing the query -- a delegated query instead
    evaluates PostgreSQL's `now()` on the database server's own clock, at whatever
    later instant PostgreSQL itself runs it. The two values can differ by however much
    the two clocks (and the two instants) drift apart -- ordinarily far too small to
    change which rows an `inactive for N days`-style query returns, but a real
    difference in principle, not merely a rounding note.
- **Freshness.** Unlike milestone 3's kind-scoped builder-query freshness, Cypher
  serving uses the coarser whole-generation rule shortest-path queries already have:
  the replica must be the current, unmodified snapshot from the moment planning starts
  through the moment execution finishes -- any write landing in between, anywhere in
  the graph, makes the engine decline and PostgreSQL answers instead. A Cypher query
  can touch far more of the schema than a single builder-query shape's own kind
  constraints can express, so this milestone keeps the simpler, stricter rule rather
  than trying to derive per-query kind scopes for arbitrary Cypher.
- **Budgets.** A served query is capped at 100,000 result rows and a generous internal
  work-unit budget (node/adjacency inspections during execution), and an unbounded
  variable-length relationship pattern (a bare `*`, `*1..`, `*..`) is capped at 15 hops
  -- mirroring dawgs' own default traversal depth cap. Exceeding either budget declines
  the query (falling back to PostgreSQL) rather than serving a truncated result.
- **The multi-graph guard.** The interpreter has no notion of which graph a query is
  scoped to -- unlike the path engine's own endpoint-resolution machinery, which only
  ever walks the one snapshot it was given -- so if `LoadSnapshot` detects the database
  holds more than one graph with nodes in it (`Snapshot.MultiGraph`), Cypher serving is
  disabled entirely for that snapshot: every query delegates to PostgreSQL rather than
  risk silently answering across a graph boundary PostgreSQL itself would respect. A
  single-graph BloodHound deployment (the normal case) is unaffected.
- **Memory.** The replica's per-node/per-edge topology cost is unchanged from milestone
  3 (~20 bytes/node, ~34 bytes/edge, including the builder-query indices above); on top
  of that, milestone 4 adds:
  - **Property bags**: roughly the property values' own raw JSON size times ~1.1
    (interned property names and small per-value framing overhead) plus about 24 bytes
    of fixed per-node overhead.
  - **The objectid index**: about 40 bytes per node (a hash-map entry plus its NodeID
    value; the objectid string itself is not duplicated -- it aliases the same bytes
    already counted in the property bag above).
  - **The (global) kind name table**: negligible -- one entry per distinct node/edge
    kind name in the whole database, not per node or edge.

  **Measured total**, 4.76 million nodes / ~48.9 million edges (`bench/adgen`'s
  synthetic Active Directory graph, with realistic per-node property bags): the whole
  replica's `ApproxBytes` -- topology and builder-query indices, property bags, the
  objectid index, and the kind name table together -- comes to about 3.76 GiB (~4.0 GB).

`BLOODTRAIL_MEMORY_LIMIT` bounds the whole replica -- topology, builder-query indices,
properties, and the objectid index together -- exactly as it always has: a rebuild that
would exceed it is refused, and the engine keeps serving (or falling back from)
whatever snapshot it already had.

See [bench/cypherbench](bench/cypherbench) for the measurement.

## Installing on an existing BloodHound CE deployment

BloodTrail ships as a patched BloodHound image plus an installer. On the host that runs
the compose deployment:

    curl -fsSL https://github.com/MihhailSokolov/BloodTrail/releases/latest/download/install.sh | sh -s -- install

The installer asks for confirmation before it changes anything, reading the answer from
the terminal; add `--yes` to run it unattended.

Until the first release is published, build the CLI with `go build ./cmd/bloodtrail`
instead; the one-liner above describes the intended installation once a release exists.
On macOS the bootstrap script's checksum step needs `sha256sum`, which stock macOS
lacks; verify the release checksum by hand with `shasum -a 256` instead, or install the
CLI from the release archive directly.

The installer inventories the deployment, backs up the application database and the
compose files into `.bloodtrail/backups/`, migrates the graph from Neo4j to PostgreSQL
if needed (using BloodHound's own migrator), switches the `bloodhound` service to the
BloodTrail image through a compose override file, and verifies the result.

Passing `--admin-password` (or setting `BLOODTRAIL_ADMIN_PASSWORD`) adds an
ingest-and-search smoke test to that verification. **It is for test and staging
deployments only**: it permanently ingests a fictional `TESTLAB.LOCAL` domain into the
graph and triggers a full analysis, which on a production-sized graph can run longer
than `--verify-timeout` and leaves the fixture objects behind afterwards.

To undo everything:

    bloodtrail rollback

Supported upstream releases will be the tags published at
`ghcr.io/mihhailsokolov/bloodhound-bloodtrail`; the first supported upstream release is
v9.6.0. Images are built from the upstream Dockerfile with a one-file patch
(`patches/bloodhound-driver.patch`).

### Operating notes

- The installer adds its override file to `COMPOSE_FILE` in `.env`, so a plain
  `docker compose up -d` keeps it. Automation that names files explicitly
  (`docker compose -f docker-compose.yml up -d`) makes docker compose ignore
  `COMPOSE_FILE`, which boots the upstream image against a `bloodtrail` driver setting
  and fails; add `-f docker-compose.bloodtrail.yml` to those commands, or drop the
  explicit `-f` and let `.env` decide.
- `bloodtrail rollback` returns the deployment to the graph it had before the install.
  On a deployment that was running Neo4j, that is the Neo4j graph as it was: anything
  ingested while BloodTrail was active went into PostgreSQL and stays there, invisible
  to the restored deployment. Re-ingest it, or reinstall with
  `--replace-postgres-graph` to migrate the current Neo4j graph again.
- A second install after a rollback is refused while the earlier migration's graph is
  still in PostgreSQL, because BloodHound's migrator would layer the new graph on top of
  the old one instead of replacing it. `--replace-postgres-graph` clears it first; the
  backup taken at the start of that install holds the state it replaced.
- `bloodtrail status` prints both the image the compose files name and the image the
  container is actually running, which differ while an install or rollback is half done.

## Roadmap

1. Packaging: patched image, skeleton driver delegating to PostgreSQL, installer with
   rollback.
2. Path engine: shortest paths, all shortest paths and reachability from memory.
3. Query-builder execution from the replica: entity panels, post-processing, tagging.
4. Cypher interpreter for pre-built and user queries.
5. Write-through, so the replica is never stale.

## Repository layout

```
internal/engine/       The in-memory engine: snapshot rebuild/poller, kind-scoped
                       freshness marks, endpoint resolution and traversal, builder-query
                       serving, and the Cypher interpreter (internal/engine/interpret)
cmd/bloodtrail/        CLI: installs/verifies/reports on/rolls back the driver in an
                       existing BloodHound CE compose deployment
bench/csrbench/        CSR traversal micro-benchmark (self-contained Go module)
bench/adgen/           Generates a synthetic AD-shaped graph and loads it into PostgreSQL
bench/pathbench/       Benchmarks the in-memory path engine against a loaded graph
bench/builderbench/    Benchmarks query-builder serving against a loaded graph
bench/cypherbench/     Benchmarks Cypher-interpreter serving against a loaded graph
build/                 Builds a BloodHound CE image with the BloodTrail driver compiled
                       in (build-image.sh) and the e2e smoke-test script (e2e.sh)
patches/               The upstream BloodHound CE source patch this driver is built
                       against (see Upstream versions below)
scripts/               One-off tooling: extract-prebuilt-queries.go (regenerates
                       testdata/prebuilt/ from an upstream checkout) and install.sh
testdata/              Fixtures for the differential test suites: dawgs/ (ported from
                       specterops/dawgs) and prebuilt/ (BloodHound's own pre-built
                       Cypher query corpus, extracted by scripts/)
```

## Upstream versions

Developed against `github.com/specterops/bloodhound` at `441f20b` and
`github.com/specterops/dawgs` `v0.8.0` (`0ea9646`).

## Licence

Apache-2.0, the same licence as BloodHound and DAWGS. See [LICENSE](LICENSE).
