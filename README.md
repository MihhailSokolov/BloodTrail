# BloodTrail

An in-memory graph engine underneath [BloodHound CE](https://github.com/SpecterOps/BloodHound),
packaged as a [DAWGS](https://github.com/SpecterOps/DAWGS) driver, so that attack-path
analysis and every other graph query stay fast on large Active Directory environments
and ordinary hardware.

**Status:** milestone 2 (path engine). Shortest paths, all shortest paths, and BloodHound's
own pre-built shortest-path searches are served from an in-memory replica when it is fresh;
every other read still goes to PostgreSQL.

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
- As of milestone 2, the driver keeps a replica of the graph's *topology* in memory:
  dense ids, forward and reverse adjacency with an edge kind per entry, and kind
  bitmaps -- exactly what the shortest-path queries described below need. It holds no
  node or edge properties; a served result's properties are hydrated from PostgreSQL
  per query instead. See [In-memory path engine](#in-memory-path-engine) for what is
  actually served from the replica today.
- PostgreSQL remains the system of record. Writes go to PostgreSQL first; the replica
  itself is rebuilt wholesale from PostgreSQL by a poller (after every completed
  analysis run, and again once the ingest/analysis pipeline goes idle following a
  write) rather than updated write-through, and there is no snapshot-file restart yet.
- Deployment is a patched BloodHound image built from the upstream Dockerfile plus a
  one-file patch (the build script also adds the driver module to `go.mod`), and an
  installer that upgrades an existing BloodHound CE deployment with backup and
  rollback.
- **Planned, not yet built** (see [Roadmap](#roadmap)): columnar properties and
  property indexes in the replica itself; an interpreter that executes queries beyond
  shortest-path directly against DAWGS's Cypher syntax tree; write-through updates to
  the replica on commit instead of a poller-driven rebuild; and loading/restoring the
  replica from a snapshot file on startup.

## In-memory path engine

BloodHound's most expensive queries are shortest-path questions: the pathfinding tab, and the
pre-built shortest-path searches its UI ships (to Domain Admins, to Tier Zero, from
Kerberoastable users, and so on). BloodTrail answers these from the in-memory replica instead
of PostgreSQL whenever it safely can, and always falls back to PostgreSQL otherwise, so results
are correct regardless of the replica's state.

- **What is served.** `GET /api/v2/graphs/shortest-path` (BloodHound's `FetchAllShortestPaths`
  call, built from `start_node`/`end_node` object IDs and an optional relationship-kind filter)
  and any Cypher sent to `POST /api/v2/graphs/cypher` that matches BloodHound's own
  `shortestPath(...)` / `allShortestPaths(...)` shape: a single `MATCH` with one shortest-path
  pattern, an optional `WHERE` built from endpoint kind/property predicates and an `s <> t`
  exclusion, and a `RETURN` of just the path with an optional `LIMIT`. Every other read --
  entity panels, node search, tagging, a Cypher query outside that shape -- is unaffected and
  always goes to PostgreSQL, exactly as in milestone 1.

- **Freshness and fallback.** The engine keeps a compressed in-memory replica (node and edge
  ids and kinds only -- no properties; a served result's properties are hydrated from
  PostgreSQL per query), rebuilt from PostgreSQL after every completed analysis run and again
  after a write once the ingest/analysis pipeline goes idle.
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

- **Log markers**, all under a `bloodtrail:` prefix: `bloodtrail: snapshot rebuilt` (Info, on
  every successful rebuild), `bloodtrail: snapshot rebuild refused: exceeds memory limit`
  (Warn, rate-limited), `bloodtrail: path engine served` (Info, once per query actually
  answered from memory), and `bloodtrail: path engine declined` (Debug, with a `reason` attr,
  whenever a query fell back to PostgreSQL).

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
internal/engine/    The in-memory path engine: snapshot rebuild/poller, endpoint
                     resolution, traversal, and Cypher/Criteria recognition
bench/csrbench/      CSR traversal micro-benchmark (self-contained Go module)
bench/adgen/         Generates a synthetic AD-shaped graph and loads it into PostgreSQL
bench/pathbench/     Benchmarks the in-memory path engine against a loaded graph
```

## Upstream versions

Developed against `github.com/specterops/bloodhound` at `441f20b` and
`github.com/specterops/dawgs` `v0.8.0` (`0ea9646`).

## Licence

Apache-2.0, the same licence as BloodHound and DAWGS. See [LICENSE](LICENSE).
