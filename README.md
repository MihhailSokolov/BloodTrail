# BloodTrail

An in-memory graph engine underneath [BloodHound CE](https://github.com/SpecterOps/BloodHound),
packaged as a [DAWGS](https://github.com/SpecterOps/DAWGS) driver, so that attack-path
analysis and every other graph query stay fast on large Active Directory environments
and ordinary hardware.

Shortest paths, the structural queries BloodHound's own code issues, and a broad surface
of Cypher -- including every pre-built query the UI ships, and OpenGraph data -- are
served from an in-memory replica that is kept in sync with PostgreSQL write by write.
Anything the engine cannot answer exactly as PostgreSQL would is delegated to PostgreSQL,
so apart from a few narrow documented cases
([below](#what-is-accelerated-and-what-is-not);
[WHITEPAPER.md §19](WHITEPAPER.md#19-limitations) lists them) the only difference you
should see is latency. On a 1M-node forest, every shipped query plus a set of adversarial
ones runs in 7.9s instead of 85.6s ([BENCHMARK.md](BENCHMARK.md)).

How it works, from the background up, is in [WHITEPAPER.md](WHITEPAPER.md).

## Quick start

On the host that runs your BloodHound CE docker compose deployment (BloodHound
**v9.6.0 or newer** -- see [Upstream versions](#upstream-versions)), from the directory
that holds its compose file:

```sh
curl -fsSL https://github.com/MihhailSokolov/BloodTrail/releases/latest/download/install.sh | sh -s -- install
```

The installer shows what it found and asks before changing anything. It backs up the
application database, the compose file and `.env`, migrates the graph from Neo4j to
PostgreSQL if needed, switches the `bloodhound` service to the BloodTrail image, and
verifies the result. If your deployment still runs Neo4j, **pause ingestion first** (see
[Operating notes](#operating-notes)). Then set a memory target
([how](#set-a-memory-target)).

To undo everything, run the same one-liner with `rollback` from the same directory:

```sh
curl -fsSL https://github.com/MihhailSokolov/BloodTrail/releases/latest/download/install.sh | sh -s -- rollback
```

## Contents

- [Quick start](#quick-start)
- [Why](#why)
- [How it works](#how-it-works)
- [Installing](#installing) -- [operating notes](#operating-notes),
  [memory target](#set-a-memory-target), [fast restarts](#fast-restarts-the-snapshot-directory)
- [Configuration](#configuration)
- [Log markers](#log-markers)
- [What is accelerated, and what is not](#what-is-accelerated-and-what-is-not) --
  [OpenGraph](#opengraph)
- [Upstream versions](#upstream-versions)
- [Developing and testing](#developing-and-testing) -- [repository layout](#repository-layout)
- [Licence](#licence)

## Why

BloodHound answers multi-hop questions by asking its graph database to expand a
frontier hop by hop: through Neo4j's record store, or through a PL/pgSQL breadth-first
harness on PostgreSQL. On large forests the shipped "shortest paths" queries time out,
and the official sizing guidance above 50,000 users is 96 GB of RAM.

The part of the graph that path questions need -- node ids, node kinds and edge kinds --
fits in well under a gigabyte for 5 million nodes and 50 million edges as
compressed-sparse-row arrays, and a single CPU core sweeps every edge in under a second
([bench/csrbench](bench/csrbench)). On a 1M-node hybrid AD/Entra forest, the worst shipped
prebuilt goes from 49 seconds to 63ms; 122 of 182 benchmark scenarios are faster, the
worst case is 1.5x slower (20ms against 13ms), and the cost is memory.
[BENCHMARK.md](BENCHMARK.md) has every scenario, the method, and what the numbers do
not show.

## How it works

- **A driver, not a fork.** BloodTrail is a DAWGS driver named `bloodtrail`, selected by
  BloodHound's `database_switch` table (or `bhe_graph_driver` when that table has no row);
  the installer sets both. BloodHound's ingest, analysis, API and UI are unchanged. The
  image is BloodHound's own Dockerfile plus a small patch
  ([`patches/bloodhound-driver.patch`](patches/bloodhound-driver.patch)).
- **An in-memory replica.** The driver keeps the graph in compact arrays (dense node ids,
  forward and reverse adjacency, one bitmap per node kind) plus every node's property
  bag and a few indexes PostgreSQL does not have. Edge properties stay in PostgreSQL and
  are fetched for the edges a result actually returns.
- **Serve or delegate.** Reads made through BloodHound's read transactions are offered to
  one of three serving paths -- a shortest-path engine, a recognizer for BloodHound's
  structural builder queries, and a Cypher interpreter. If the engine cannot guarantee
  PostgreSQL's exact answer, the read goes to PostgreSQL unchanged.
- **Write-through.** Writes go to PostgreSQL first. Before the call returns, the driver
  re-reads the rows the write touched and layers them onto the replica as an immutable
  delta, so the very next query already sees the write. The few writes that cannot be
  followed that way switch the engine to a fallback mode -- every query goes to
  PostgreSQL -- while the replica reloads in the background.
- **Fast restarts.** With `BLOODTRAIL_SNAPSHOT_DIR` set, the replica is saved to disk and
  reused at the next boot, but only when a write counter in PostgreSQL, and the lineage
  it counts in, prove the file is complete; otherwise the boot rebuilds from PostgreSQL.

[WHITEPAPER.md](WHITEPAPER.md) covers each of these in depth, with references to the code.

## Installing

BloodTrail ships as a patched BloodHound image plus an installer CLI. On the host that
runs the compose deployment, from the directory that holds its compose file:

```sh
curl -fsSL https://github.com/MihhailSokolov/BloodTrail/releases/latest/download/install.sh | sh -s -- install
```

The bootstrap script downloads the latest released CLI for your platform, verifies its
checksum (`sha256sum` where available, `shasum -a 256` on stock macOS), runs it with the
arguments you pass after `--`, and deletes it again. So every command -- `install`,
`status`, `verify`, `rollback` -- is run the same way, with its name in place of
`install`. The CLI looks for `docker-compose.yml` in the current directory; pass
`--compose-file` (and, if the project lives elsewhere, `--project-dir`) otherwise.

The script refuses to run a download unless `checksums.txt` holds exactly one
well-formed SHA-256 entry for the archive, and that entry matches. It runs the CLI from a
temporary directory under `TMPDIR` (default `/tmp`); on a host whose `/tmp` is mounted
`noexec`, set `TMPDIR` to a directory that allows running programs.

- **Confirmation.** `install` and `rollback` show what they will change and ask first,
  reading the answer from the terminal; add `--yes` to run them unattended.
- **Pinning a release.** To run the exact release you audited instead of `latest`, set
  `BLOODTRAIL_VERSION`:

  ```sh
  curl -fsSL https://github.com/MihhailSokolov/BloodTrail/releases/latest/download/install.sh | BLOODTRAIL_VERSION=v0.1.2 sh -s -- install
  ```

  This pins the CLI the script downloads; the bootstrap script itself still comes from
  the latest release unless you fetch `releases/download/<tag>/install.sh`. Use v0.1.2 or
  newer: v0.1.1 can install but predates the installer behaviour described here, and a
  v0.1.0 binary cannot complete an install at all (its image package is no longer
  published).
- **The image.** The installer derives the BloodTrail image from the BloodHound version
  tag your compose configuration names (`<upstream>-bt<version>`). A deployment whose
  image is tagged `latest` -- the upstream example compose file's default -- must set
  `BLOODHOUND_TAG` in `.env` (or you pass `--image`). A released CLI never substitutes
  the moving `<upstream>` alias for its own image: if `<upstream>-bt<version>` is not
  published it stops before changing anything and names the alias, which `--image` then
  selects explicitly.
- **Building from source.** `go build ./cmd/bloodtrail` also works, with one behavioral
  difference: a source-built binary reports version `dev`, so it targets the moving
  `<upstream>` alias rather than a version-pinned image.
- **What install does.** It inventories the deployment, backs up the application
  database, the compose file and `.env` into `.bloodtrail/backups/`, migrates the graph
  from Neo4j to PostgreSQL if needed (using BloodHound's own migrator), switches the
  `bloodhound` service to the BloodTrail image through a compose override file
  (`docker-compose.bloodtrail.yml`), and verifies the result.
- **Verification** (after `install`, or on its own with `verify`) checks that the driver
  setting BloodHound reads (the `database_switch` row, else `bhe_graph_driver`) names
  `bloodtrail`, that the container's *current* run logged `BloodTrail driver active`, and
  that the API answers. It reads the setting from the `app-db` service, so that service
  must be reachable.
- **The smoke test.** Passing `--admin-password` (or setting `BLOODTRAIL_ADMIN_PASSWORD`)
  adds an ingest-and-search smoke test to that verification. **It is for test and staging
  deployments only**: it permanently ingests a fictional `TESTLAB.LOCAL` domain into the
  graph and triggers a full analysis, which on a production-sized graph can run longer
  than `--verify-timeout` and leaves the fixture objects behind afterwards. On a
  deployment that already holds that domain (a repeat run), the search cannot show that
  this run's upload reached the graph, so the smoke test is reported as **INCONCLUSIVE**:
  a warning, not a pass, and not a failure (exit status 0). Delete the `TESTLAB.LOCAL`
  domain first to get a result.

### Operating notes

- **Pause ingestion for the install window** on a deployment migrating from Neo4j.
  BloodHound keeps ingesting into Neo4j while the migration runs, and its migrator
  does not carry mid-migration arrivals over; the installer counts Neo4j again after
  the migration and aborts (with the rollback hint) when PostgreSQL came up short, so
  active ingestion makes the install fail rather than silently lose data. The
  installer also aborts if the bloodhound container restarts mid-migration, or if the
  post-migration Neo4j recount cannot be read at all. It takes the first count before
  changing anything, which needs `cypher-shell` and `NEO4J_AUTH` (as
  `<user>/<password>`) in the `graph-db` service; without them it stops right there.
- **Run one BloodHound API server per database.** BloodTrail only sees writes made
  through its own driver. A write made directly against PostgreSQL (`psql`, a second,
  unpatched server) is invisible to the replica. A second BloodTrail server is detected:
  each one then logs `snapshot file not written` (Warn) and saves no snapshot file until a
  rebuild loads the other's writes.
- **Connections.** BloodTrail uses BloodHound's own connection pool, plus a pool of two
  connections of its own for its write path, opened on the first write.
- **Put your own settings in your own compose files.** Set `BLOODTRAIL_*` variables and
  `GOMEMLIMIT` on the `bloodhound` service in your compose file or its override file,
  not in `docker-compose.bloodtrail.yml`: install rewrites that file and rollback
  deletes it. Do not copy `bhe_graph_driver=bloodtrail` into your own files -- after a
  rollback the stock image would fail to start with it.
- The installer adds its override file to `COMPOSE_FILE` in `.env`, so a plain
  `docker compose up -d` keeps it: by its absolute path when every file in the entry is
  named by an absolute path, and as `docker-compose.bloodtrail.yml` otherwise (docker
  compose resolves a relative name against the directory you run it from). Automation
  that names files explicitly (`docker compose -f docker-compose.yml up -d`) makes docker
  compose ignore `COMPOSE_FILE`, which boots the upstream image against a `bloodtrail`
  driver setting and fails; add `-f docker-compose.bloodtrail.yml` to those commands, or
  drop the explicit `-f` and let `.env` decide.
- Writing that entry replaces docker compose's own file discovery, so the installer
  writes out everything discovery would have found: the compose file it was given and,
  when one sits beside it, the override file compose loads on its own (the first of
  `compose.override.yml`, `compose.override.yaml`, `docker-compose.override.yml` and
  `docker-compose.override.yaml` that exists, whatever the compose file is called).
  The entry it writes names its files relatively, so a project you used to run with
  `docker compose --project-directory <dir>` from elsewhere then has to be run from its
  own directory. Rollback removes the whole entry again when the install created it --
  unless the entry has been changed since (a file added to it, say), in which case it
  takes out only its own override. With no entry, and a compose file under one of
  discovery's names in the directory, the compose file given has to be the one discovery
  picks (`compose.yaml` wins over `docker-compose.yml` in the same directory); otherwise
  the installer stops and asks for `--compose-file` or an entry.
- **The installer stops before changing anything when it cannot be sure which files
  docker compose loads for you**: a `COMPOSE_FILE` entry that is empty, set twice, quoted
  in a way it cannot follow (a quote left open on its line, text after the closing quote,
  quotes inside an unquoted value, or the entry sharing a line with another value), uses
  interpolation or escapes, has an empty or space-padded name, does not list the compose
  file it was given, or lists a file that does not exist; an entry whose first file is
  not in the directory of `.env`, or, with no entry, a compose file given from another
  directory (docker compose takes the first file's directory as the project directory and
  resolves relative paths such as a `./pgdata` bind mount against it, while the installer
  addresses the directory of `.env`); a `COMPOSE_PATH_SEPARATOR` line in `.env`; in the
  shell's environment, `COMPOSE_FILE` (which docker compose takes over the entry in
  `.env`), a non-empty `COMPOSE_PATH_SEPARATOR` other than `:` (which splits that entry
  into names that do not exist), a non-empty `COMPOSE_ENV_FILES` or a true
  `COMPOSE_DISABLE_ENV_FILE` (which leave `.env` unread), or a `COMPOSE_DISABLE_ENV_FILE`
  that is not a boolean (on which docker compose stops with an error); or a `.env` the
  install has to change that this user cannot write. The message names what it found, and
  usually what to change. `.env` is replaced atomically, keeping its mode and owner (and
  a symbolic link), but not extended attributes, ACLs or a hard link to it, and a `.env`
  that is itself a mount point cannot be replaced. `status`, `verify` and `rollback` read
  the project more forgivingly (with several `COMPOSE_FILE` lines, the last wins, as in
  docker compose), so they keep working on whatever an earlier install left behind; where
  an earlier version wrote its override into an empty `COMPOSE_FILE=` entry, rollback
  puts the empty entry back as it was.
- **Rollback of installs made by v0.1.0 to v0.1.2** decides what to do with the
  `COMPOSE_FILE` entry from the copy of `.env` in the backup directory, since those
  versions did not record what they wrote: an entry that was there before the install
  loses only the override, and files you added to an entry the install created stay. A
  second `COMPOSE_FILE` line that v0.1.0 or v0.1.1 appended beside an
  `export COMPOSE_FILE=` entry is removed and yours is kept; when rollback cannot tell
  which line is the install's, it stops before changing anything and says which line to
  delete by hand. If the restored entry still lists a file that does not exist, rollback
  says so, since docker compose cannot load the project until it is put back or taken
  out.
- **When rollback leaves the restart to you.** If the restored project's first
  `COMPOSE_FILE` file (or, with no entry, the compose file the install was given) is not
  in the directory of `.env`, a restart from rollback could recreate a service with its
  data on a different host path. Rollback then does everything else, ending the watermark
  lineage among it, and tells you that BloodTrail is still running: restart with your own
  `docker compose up -d` from the directory you usually run it in. Because the lineage
  ended while BloodTrail still ran, a snapshot file it saves after reloading in that
  window names the new lineage and would stay adoptable after the stock image writes the
  graph. So once the original image is running, end the lineage again (rollback prints
  the `psql` command; see the
  [snapshot directory](#fast-restarts-the-snapshot-directory)), or delete the snapshot
  file before starting BloodTrail any way other than `bloodtrail install`, which ends it
  itself.
- **Rollback returns the deployment to the graph it had before the install.** On a
  deployment that was running Neo4j, that is the Neo4j graph as it was: anything
  ingested while BloodTrail was active went into PostgreSQL and stays there, invisible
  to the restored deployment. Re-ingest it, or reinstall with
  `--replace-postgres-graph` to migrate the current Neo4j graph again.
- A second install after a rollback is refused while the earlier migration's graph is
  still in PostgreSQL, because BloodHound's migrator would layer the new graph on top of
  the old one instead of replacing it. `--replace-postgres-graph` clears it first; the
  backup taken at the start of that install holds the state it replaced.
- `status` prints both the image the compose files name and the image the container is
  actually running, which differ while an install or rollback is half done.
- **Expect ingest to take longer.** Keeping the replica current adds roughly a quarter
  to write time (19-34% measured, [`bench/applybench`](bench/applybench)).

### Set a memory target

BloodTrail holds the graph in memory. Go's collector, left alone, lets the heap grow to
roughly twice what is live before collecting, so give the Go runtime a target with
`GOMEMLIMIT` on the BloodHound service (in your own compose or override file):

```yaml
services:
  bloodhound:
    environment:
      - GOMEMLIMIT=2800MiB
```

Measured on the 1M-node/2.4M-edge benchmark graph, that took the container from 3.9GiB
resident to 1.6-2.2GiB with no latency cost. Pick a value with headroom over the
snapshot size, which the engine logs as `bytes` on every `snapshot rebuilt` line in the
`bloodhound` container's logs (`status` does not report it); allow roughly three times
that, since the derived read indexes and each query's working set live alongside it.

`GOMEMLIMIT` is a soft target, not a container limit: Go collects harder as it
approaches, and never fails an allocation to stay under. A hard limit is a separate
compose setting (`mem_limit`). `GOGC` is not a substitute -- raising it does the
opposite of what is wanted here, and on this graph `GOGC=400` had the container
OOM-killed.

### Fast restarts: the snapshot directory

A restart normally rebuilds the replica from PostgreSQL, which takes tens of seconds on
a large graph. Setting `BLOODTRAIL_SNAPSHOT_DIR` lets it reuse a saved copy instead:

- Point it at a directory on a **volume or bind mount that survives container
  recreation** (a config change followed by `docker compose up -d` recreates the
  container). The directory must already exist and be writable by the container;
  nothing creates it, and a bad path only shows up later as `snapshot file write
  failed`.
- The file is written on a graceful shutdown and after each background compaction, and
  reused at the next boot only if BloodTrail's write counter in PostgreSQL -- and the
  **lineage** it counts in, a random id kept beside it -- prove the file is complete.
  The first boot after `bloodtrail install` always rebuilds, and so, once, does the first
  boot after an upgrade that changes the file's format, as every upgrade from v0.1.2 or
  earlier does: the older file is refused and rebuilt.
- **Rollback and reinstall keep this safe on their own.** Both end the lineage, so a
  file saved before a rollback is never adopted after the stock image has written the
  graph, wherever `BLOODTRAIL_SNAPSHOT_DIR` is configured. `--replace-postgres-graph`
  ends it too.
- **Anything else that writes the graph without BloodTrail** must end the lineage before
  BloodTrail starts again, or delete the snapshot file: `psql`, the stock image started
  by hand, BloodHound's tool API switching a running server to the plain `pg` driver
  (`/graph-db/switch/pg`), and restoring a database backup (even one that restores the
  lineage the file names). To end it:

  ```sql
  update bloodtrail_watermark set lineage = gen_random_uuid();
  ```

  As a backstop, each file also records where the `node` and `edge` id sequences stood,
  and a boot that finds the counter unchanged but either sequence moved refuses the
  file. That catches inserts made behind BloodTrail's back, not updates or deletes. A
  boot also refuses a file stamped ahead of the counter, which is how a restored backup
  usually leaves it; a restore that leaves the counter at or above the stamp is not
  caught, so end the lineage after every restore.

## Configuration

Environment variables on the `bloodhound` service, read once at driver startup. A
malformed value stops startup with an error naming the variable.

| Variable | Default | Meaning |
|---|---|---|
| `BLOODTRAIL_ENGINE` | `on` | `on`/`true`/`1` or `off`/`false`/`0`. `off` sends every read straight to PostgreSQL; the replica is never built or used. Writes still advance the write counter the snapshot file relies on |
| `BLOODTRAIL_SNAPSHOT_DIR` | unset | Directory for the snapshot file ([fast restarts](#fast-restarts-the-snapshot-directory)). Unset disables it: no file is read or written |
| `BLOODTRAIL_COMPACT_ENTRIES` | `65536` | How many changed entries the write-through delta may hold before a background compaction folds it into a new base. `0` means no bound on this dimension |
| `BLOODTRAIL_COMPACT_BYTES` | `512MiB` | The same bound in approximate bytes. `0` means no bound; `0` on both disables compaction |
| `BLOODTRAIL_MEMORY_LIMIT` | unset | Caps the replica's estimated size (e.g. `4GiB`); unset or `0` means unbounded. A rebuild that would exceed it is refused and retried every 10 minutes, and the engine keeps whatever state it had meanwhile (at boot: every read goes to PostgreSQL); an applied write that would push past it switches the engine to fallback |
| `BLOODTRAIL_LOG_LEVEL` | unset | `debug`, `info`, `warn` or `error`. Only ever *widens* which BloodTrail lines are visible, on top of BloodHound's own logging; unset or empty is a no-op |

## Log markers

BloodTrail's log lines carry a `bloodtrail:` prefix, except the startup line
`BloodTrail driver active`. Debug lines are visible when `BLOODTRAIL_LOG_LEVEL=debug` is
set or BloodHound's own log level is debug.

| Marker | Level | Meaning |
|---|---|---|
| `BloodTrail driver active` | Info | The driver started (the installer's verification waits for this line from the container's current run) |
| `path engine served` | Info | A shortest-path request was answered from memory |
| `builder engine served` / `cypher engine served` | Debug | A builder or Cypher query was answered from memory |
| `path engine declined` / `builder engine declined` | Debug | The engine was asked and sent the query to PostgreSQL; `reason` says why (a declined Cypher query logs the path-engine line). Queries the engine is never offered go to PostgreSQL without a line |
| `write-through applied` | Debug | A committed write was replayed into the replica |
| `segment stack merged` | Debug | An overgrown delta stack was collapsed; bookkeeping, not a fallback |
| `fallback entered` / `fallback exited` | Warn / Info | A write could not be followed incrementally, or engine work panicked (`reason`); every query goes to PostgreSQL until the rebuild lands |
| `write-through apply panicked` / `snapshot rebuild panicked` / `snapshot file boot panicked` / `compaction panicked` | Error | An engine bug, logged with its stack; the engine entered fallback (reason `apply panicked: ...` and so on) and keeps retrying the rebuild. A snapshot file whose boot panicked is deleted. Please report it |
| `snapshot rebuilt` | Info | A full load from PostgreSQL was adopted (`trigger`: `startup` or `fallback`) |
| `snapshot rebuild not adopted: a write was applied while it loaded` | Debug | A load was discarded and will be retried |
| `snapshot rebuild refused: exceeds memory limit` | Warn | Rate-limited |
| `boot load waiting for the default graph` | Debug | Expected on every startup |
| `boot load failed` / `fallback rebuild failed` | Warn | A rebuild attempt failed and will be retried |
| `watermark bump failed` / `watermark table DDL failed` | Warn | The write counter could not be advanced / its table created |
| `watermark lineage DDL failed; ...` | Warn | The lineage column could not be added; no snapshot file is written or adopted until a later start adds it |
| `could not read the watermark lineage; ...` | Warn | No snapshot file will be written from that rebuild |
| `could not record where PostgreSQL stood at start; ...` | Warn | That boot does not check its snapshot file for rows inserted behind the counter |
| `watermark failure settled by a write that produced no effect; ...` | Debug | A rebuild was requested to restore trust in the counter |
| `snapshot file loaded` / `snapshot file rejected` | Info | The boot reused the saved file (`replayed_writes`) / declined it (`reason`, or `error` for an unreadable or older-format file) and rebuilt instead |
| `snapshot file written` / `not written` / `skipped` / `write failed` | Info / Debug-Warn / Debug / Warn | Saving the replica on shutdown or after compaction. `not written` at Warn with reason `the watermark counter holds values this process never resolved: ...` means another BloodTrail server may be writing the same database |
| `snapshot file invalidated` | Info | A write reached PostgreSQL uncounted, or booting from the file panicked, so the saved file was deleted |
| `snapshot file not invalidated` / `snapshot file invalidation failed` | Warn | That delete could not happen; delete the file by hand before the next restart |
| `no snapshot file` | Debug | Boot found no file in the snapshot directory |
| `removed stale snapshot temp file` / `failed to remove stale snapshot temp file` | Info / Warn | A half-written file from an earlier process was (or could not be) cleaned up |
| `compaction triggered` / `started` / `finished` / `discarded` / `failed` / `snapshot save failed` | Debug / Info / Info / Info / Warn / Warn | Background compaction, and the snapshot file it writes |

## What is accelerated, and what is not

Served from memory whenever the engine is up to date:

- `GET /api/v2/graphs/shortest-path` (BloodHound's pathfinding).
- The structural builder queries behind entity panels, analysis and tagging: counts, id
  listings, kind listings and (id, start, end) listings of nodes (filtered by at least
  one kind) and relationships (filtered by kind and endpoint id or kind), plus the two
  row shapes DAWGS's own graph-loading and traversal helpers request.
- `POST /api/v2/graphs/cypher` for a broad subset of Cypher: pattern matching with
  property conditions, variable-length patterns, `shortestPath`/`allShortestPaths` (as the
  only pattern before any `WITH`), `OPTIONAL MATCH`, `WITH`, `COUNT`/`COLLECT`,
  `DISTINCT`, numeric `ORDER BY`, `LIMIT`.
  Every pre-built and selector query BloodHound v9.6.0 ships is served.

Always answered by PostgreSQL (correct, just not faster):

- Cypher outside the interpreter's supported subset, Cypher with `$parameters`, and any
  Cypher that changes data.
- Anything whose answer depends on PostgreSQL behaviour the engine does not reproduce
  exactly, such as sorting text (collation) or returning a value PostgreSQL types as an
  exact decimal.
- Builder queries that filter or project properties, that use `Offset` or `Limit`, or
  that sort by anything other than edge id.
- Every query -- Cypher, builder and shortest path -- on a database that holds more than
  one graph with data.
- Any Cypher query that reads a property holding a number stored in a spelling the
  replica cannot reproduce, such as `1.0`, or an integer its float64 cannot spell back
  (one beyond 2^53); BloodHound itself writes neither.

Documented differences ([WHITEPAPER.md §19](WHITEPAPER.md#19-limitations)): in a served
query's conditions, `datetime()`'s epoch accessors read the BloodHound server's clock
when the query starts, where PostgreSQL reads its own `now()`; for a `shortestPath`
without `s <> t`, whether PostgreSQL raises its shared-endpoint error can depend on its
query plan, so on some plans PostgreSQL fails a query BloodTrail answers (where both
answer, the answers agree); and an objectid-keyed edge upsert whose endpoint node that
same upsert created, and another writer re-keyed before the upsert is applied, leaves
that node and its edge out of the replica until a later write names them or a reload (an
endpoint that already existed is followed through the re-key).

Out of scope today: interpreting mutating Cypher and arbitrary update/delete criteria
(both trigger a fallback rebuild), cache coherence across more than one BloodTrail
server writing the same database, and write concurrency beyond one apply at a time.
[WHITEPAPER.md](WHITEPAPER.md) lists the exact rules and why each exists.

### OpenGraph

BloodHound's OpenGraph data (custom node and edge kinds uploaded as JSON, with an
optional extension schema) reaches the graph through the same driver calls collector
data does, so BloodTrail needs nothing OpenGraph-specific:

- **Uploads replay by write-through**: the kinds an upload registers, and its
  objectid-keyed node and edge upserts, including stub endpoints and an AD node gaining
  the source kind through a hybrid edge.
- **Reads are served from memory**: Cypher over custom kinds and every OpenGraph value
  type, variable-length paths, `shortestPath`, both `allShortestPaths` answers (PostgreSQL
  gives every pair's own shortest paths when both endpoints carry a property or id
  constraint, and only the overall shortest paths otherwise; BloodTrail reads which one
  applies from DAWGS's own translation of the query), aggregates, and the pathfinding
  endpoint with an extension's traversable kinds.
- **Deletes replay incrementally**: "Clear database" by source kind, of sourceless data,
  and by edge kind.
- **Still answered by PostgreSQL**: a query naming a kind no row carries yet (such as a
  failed upload's source kind) until the replica learns the kind, and the shapes that
  delegate for any other data.

The evidence is `integration/opengraph_integration_test.go`, which replays upstream's
OpenGraph call shapes against the plain pg driver step by step;
`integration/shortest_path_level_integration_test.go`, which pins both `allShortestPaths`
answers against PostgreSQL; and the end-to-end test's OpenGraph phase
([build/README.md](build/README.md#end-to-end-test)). On a 190k-node organization, path
queries answer 30-70x faster than on the PostgreSQL driver and scans about 2x, for about
a quarter more ingest time ([BENCHMARK.md](BENCHMARK.md#opengraph), [bench/oggen](bench/oggen)).

## Upstream versions

Developed and continuously validated against `github.com/specterops/bloodhound`
**v9.6.0** and `github.com/specterops/dawgs` `v0.8.0` (`0ea9646`). CI also runs the unit
and integration suites against every other dawgs version a supported BloodHound release
resolves to in its image (v0.8.1 today, for v9.7.1), and an image build fails when a
release would ship a dawgs version that is neither of those nor listed as tested.

**Supported upstream tags** are every stable BloodHound CE release from **v9.6.0** on
-- derived from upstream's own release list by `build/upstream-tags.sh`, not written
down. That one list drives the CI patch-application guard, the images each BloodTrail
release publishes (`<upstream>-bt<version>`, one per supported tag), and a weekly
workflow that builds the moving `<upstream>` alias for any supported tag that has none
yet. A new upstream release enters the patch guard on the next CI run, but a released
CLI installs it only once a BloodTrail release has published its
`<upstream>-bt<version>` image; until then `--image <upstream>` selects the alias, which
may be an unreleased build. The validation levels differ, deliberately:

- **v9.6.0** is the tag CI validates most deeply: the full install-and-rollback e2e
  runs against it on every change, alongside the patch-application guard.
- **Newer tags** (v9.7.0 was verified end to end by hand on 2026-09-12) get the
  patch-application guard in CI and are gated at image-build time -- the patch must
  apply cleanly and the patched server must compile, or no image is published -- but
  no automated e2e runs against them per change. A behavioral break upstream could in
  principle ship in a buildable image; the installer's own post-install verification
  is the backstop.

## Developing and testing

Everything below assumes the Go toolchain version from [go.mod](go.mod).

    make test          # unit suite (CI also runs it with -race)
    make lint          # golangci-lint over all code, integration-tagged included, as CI runs it
    make integration   # integration suite -- needs the test database below
    make test-bench-scripts test-build-scripts  # the benchmark and build scripts' own tests
    make tidy          # go mod tidy

The integration suite (every `*_integration_test.go`, behind the `integration` build
tag) runs against a disposable PostgreSQL, pointed at by `BLOODTRAIL_TEST_PG`:

```sh
docker compose -f docker-compose.test.yml up -d
BLOODTRAIL_TEST_PG='postgresql://bloodtrail:bloodtrail@127.0.0.1:55432/bloodtrail' make integration
```

The suite wipes and reseeds that database freely -- never point it at data you care
about. The benchmarks (`make bench-path` / `bench-builder` / `bench-cypher` and
`bench/applybench`) have their own READMEs under [bench/](bench). See
[CONTRIBUTING.md](CONTRIBUTING.md) for the pull-request expectations.

### Repository layout

```
internal/engine/       The in-memory engine: write-through apply and the SERVING/FALLBACK
                       state (apply.go), the watermark trust protocol (watermark.go), the
                       snapshot file and background compaction (persist.go, compact.go),
                       boot-load and fallback recovery (boot.go), endpoint resolution and
                       traversal, builder-query serving, and the Cypher interpreter
                       (internal/engine/interpret)
cmd/bloodtrail/        CLI: installs/verifies/reports on/rolls back the driver in an
                       existing BloodHound CE compose deployment
bench/csrbench/        CSR traversal micro-benchmark (self-contained Go module)
bench/adgen/           Generates a synthetic AD-shaped graph and loads it into PostgreSQL
bench/shgen/           Generates a fictitious AD forest as SharpHound v6 JSON, for
                       benchmarking a whole deployment through BloodHound's own ingest
bench/oggen/           Generates a fictitious source-control organization as OpenGraph
                       JSON, and benchmarks a deployment on it (bench.py)
bench/pathbench/       Benchmarks the in-memory path engine against a loaded graph
bench/builderbench/    Benchmarks query-builder serving against a loaded graph
bench/cypherbench/     Benchmarks Cypher-interpreter serving against a loaded graph
bench/applybench/      Benchmarks the write-through apply path against a loaded graph
build/                 Builds a BloodHound CE image with the BloodTrail driver compiled
                       in (build-image.sh) and the e2e smoke-test script (e2e.sh, with
                       its OpenGraph phase in e2e-opengraph.sh)
patches/               The upstream BloodHound CE source patch this driver is built
                       against (see Upstream versions above)
scripts/               One-off tooling: extract-prebuilt-queries.go (regenerates
                       testdata/prebuilt/ from an upstream checkout) and install.sh
testdata/              Fixtures for the differential test suites: dawgs/ (ported from
                       specterops/dawgs) and prebuilt/ (BloodHound's own pre-built
                       Cypher query corpus, extracted by scripts/), and the e2e
                       test's OpenGraph uploads (opengraph/)
```

## Licence

Apache-2.0, the same licence as BloodHound and DAWGS. See [LICENSE](LICENSE).
