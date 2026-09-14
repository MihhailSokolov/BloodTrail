# BloodTrail

An in-memory graph engine underneath [BloodHound CE](https://github.com/SpecterOps/BloodHound),
packaged as a [DAWGS](https://github.com/SpecterOps/DAWGS) driver, so that attack-path
analysis and every other graph query stay fast on large Active Directory environments
and ordinary hardware.

**Status:** milestone 5 (write-through). Shortest paths, all shortest paths, a defined set of
structural node/relationship queries BloodHound's query builder issues, and a broad surface
of Cypher itself -- property predicates and scans, point lookups, `shortestPath`/
`allShortestPaths` patterns, and a range of aggregations, reached through a real Cypher
interpreter rather than pattern-matching a handful of recognized shapes -- are all served from
an in-memory replica that is now kept in sync with PostgreSQL write by write instead of
rebuilt wholesale by a poller: a recognized write updates the replica synchronously, before
the call that made it returns, so the very next query -- even the one that write's own caller
issues immediately after -- already sees it. Only a small, closed list of write shapes that
cannot be expressed that way fall back to PostgreSQL temporarily while the replica rebuilds in
the background; see [Write-through](#write-through). Every query the engine cannot (or should
not) answer from memory delegates to PostgreSQL, exactly as before.

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
- PostgreSQL remains the system of record. Writes go to PostgreSQL first, through
  BloodTrail's own driver; the driver then replays that same write into its in-memory
  replica before returning to the caller -- no rebuild, no polling, no window in which
  the replica is stale for a write it has already told the caller succeeded. See
  [Write-through](#write-through) for exactly which writes this covers and what happens
  to the rare ones it doesn't.
- A snapshot file (`BLOODTRAIL_SNAPSHOT_DIR`) lets a restart skip the PostgreSQL rebuild:
  the current replica is written to disk, stamped with a watermark counter, on a clean
  shutdown and after every background compaction; the next boot loads it, replays onto it
  whatever recognized writes landed while it was loading (BloodHound writes to the graph
  on every boot, so this replay is what makes the file usable at all in practice), and
  proves via the watermark counters that nothing else got in between -- falling back to a
  normal PostgreSQL rebuild whenever that proof fails, e.g. after a crash or an
  unrecognizable boot-time write. See [Write-through](#write-through) for details.
- Deployment is a patched BloodHound image built from the upstream Dockerfile plus a
  one-file patch (the build script also adds the driver module to `go.mod`), and an
  installer that upgrades an existing BloodHound CE deployment with backup and
  rollback.

## Write-through

Every write BloodHound's driver call makes -- ingest's objectid-keyed upserts, analysis's
node/relationship creates and updates, tagging, derived-edge recomputation's deletes, and
so on -- is replayed into the in-memory replica synchronously, inside the same call that
performs the write, before that call returns to its caller. There is no rebuild in the
normal case, and no window in which the replica can be stale relative to a write the
caller has already been told committed.

- **How.** BloodTrail's write observers record what each write actually did -- not just
  which kinds it touched -- into a change log. Once the write has committed in
  PostgreSQL, the driver reads back every key that change log named (by id, objectid, or
  endpoint triple) and turns the result into one new immutable delta layered on top of
  the engine's current in-memory state: a row that still exists is an upsert, a row
  that's gone is a tombstone -- this covers ordinary deletes, cascaded edge removal, and
  a partially-applied batch uniformly, since "absent on read-back" means the same thing
  regardless of which of those produced it. That delta is what every query issued after
  the write returns already sees.
- **What it costs.** Write-through is not free, and the cost lands on ingest. Measured
  against the same ingest with `BLOODTRAIL_ENGINE=off`, applying a write costs
  **19-34% additional write wall time (median ~25-26%)** at the benchmarked scale --
  the read-back round trip plus building and publishing the delta. Budget roughly a
  quarter more ingest time for the accelerated reads. (The `60%` figure in the
  benchmark's own enforcement table is *not* this cost: it is a CI tripwire set at the
  measured worst case times ~1.75 headroom, so that ordinary run-to-run variance does
  not fail a build.) See [`bench/applybench`](bench/applybench) for the harness and the
  per-run numbers.

  Two caveats qualify that range. It was measured with writes serialized through a
  single apply lock, so it describes one writer's latency and not what happens when
  several contend. And reading through the accumulated delta costs time proportional
  to its total size (measured ~290ns per entry, linear), a cost every apply pays --
  which is exactly what sizes the shipped `BLOODTRAIL_COMPACT_ENTRIES` default
  (65,536): at the threshold that read costs ~21ms, the same class as the write's own
  work, and compaction folds the delta away before it can grow past that. The range
  above was measured at ~24,000-entry deltas, comfortably inside that bound. See
  [`bench/applybench/README.md`](bench/applybench/README.md) for both in detail.
- **SERVING and FALLBACK.** The engine is always in one of two states. In **SERVING**
  (the normal state), every query is answered from an up-to-date in-memory view and
  every write updates it as above. A write whose effect cannot be expressed as such a
  delta -- the closed list below -- flips the engine to **FALLBACK**: every query
  delegates to PostgreSQL (still correct, just not accelerated) while one background
  rebuild reloads the whole replica from PostgreSQL from scratch; the engine returns to
  SERVING the instant that rebuild lands. This is the only rebuild that ever happens
  outside of process startup.
- **The closed fallback list.** Only these write shapes trip FALLBACK -- every other
  write applies incrementally as above:
  - Mutating Cypher sent through `Query`/`Raw` (raw Cypher text can do anything, so it
    isn't parsed to find out what it actually changed).
  - `Run`, the driver-level raw-connection call (same reasoning).
  - `WipeGraph` (a full graph truncation -- rebuilding an empty graph afterwards is
    instant anyway).
  - `SetDefaultGraph` (retargeting which graph is "the" graph the engine is replicating).
  - A write issued through `WithGraph` against a graph other than the default one.
  - A `NodeQuery`/`RelationshipQuery` `.Update`/`.Delete` call whose criteria aren't one
    of the shapes BloodTrail's recognizer can enumerate (an id-list criterion is;
    an arbitrary predicate isn't).
  - A `RelationshipQuery.Delete` whose criteria *were* recognized but whose delete then
    failed in PostgreSQL. The recognized form of that call is replayed as an
    instruction ("tombstone every edge of these kinds") rather than as a read-back
    key, so it is only sound for a delete that actually happened; after a failure the
    replica knows nothing about those edges, and saying so is the honest record.
  - `Batch.CreateNode` for a node with neither a pre-assigned graph id nor a string
    `objectid` property -- a plain `INSERT` with nothing for the read-back to key on, so
    the applier cannot find the row it just created.
  - `Batch.UpdateNodeBy` whose `IdentityProperties` is anything other than exactly
    `["objectid"]` (or whose node carries no string value under that key).
  - `Batch.UpdateRelationshipBy` where *either* endpoint fails that same check, or the
    update carries no relationship at all -- the upsert this mirrors writes both
    endpoint nodes and the relationship in one statement, so the edge triple is only
    sound to record when both endpoints resolve and there is a relationship to key it.

  The first seven are rare in an ordinary BloodHound deployment -- admin actions, not
  anything ingest or analysis routinely does. **The last three are not.** They are
  ingest-shaped: they sit on the very batch API ingest itself writes through, and what
  keeps them off the hot path is not the call but the *shape* of its identity. The
  objectid-keyed upsert BloodTrail recognizes is the shape BloodHound's ingest uses, so
  in practice the fallback branch is reached only by a caller keying a batch write some
  other way -- a different or additional identity property, or a create carrying neither
  an id nor an objectid. That is a narrower and more fragile guarantee than "an admin
  did something unusual": it depends on a caller's identity convention rather than on an
  operator's rare deliberate act, and an upstream change to how ingest keys its writes
  would move traffic onto this branch without anything here changing.

  They are on the list rather than taught to the replica because the alternatives are
  worse: a create with neither an id nor an objectid genuinely offers nothing to re-read
  the new row by, and widening the identity recognizer past bare `objectid` would mean
  reimplementing PostgreSQL's own upsert-key resolution in the observer -- exactly the
  kind of second, drifting implementation of someone else's semantics this design
  avoids everywhere else.

  Separately, and not a write shape at all: a **failed watermark bump** also trips
  FALLBACK. The counter is what makes a delta trustworthy (see Watermark below), so a
  write whose bump never landed cannot be trusted to a narrow delta, and the rebuild is
  what restores trust.

  The write-through differential test suite enforces that the list stays exhaustive:
  every write shape not on it is asserted to apply incrementally with the replica left
  equal to PostgreSQL, immediately, with rebuilds disabled.
- **Single-writer trust model.** PostgreSQL stays the system of record, and the same
  trust assumption the pre-write-through engine already made still holds: this engine
  only knows about writes that go through *this* driver instance. A write made directly
  against PostgreSQL (`psql`, a second, unpatched server) is invisible to it, exactly as
  it always was. A second *BloodTrail-patched* server writing the same database is now at
  least detected at boot -- see Watermark, next -- rather than silently missed forever,
  but the supported deployment shape is still a single API server process.
- **Watermark.** A single-row table, `bloodtrail_watermark`, holds a counter that every
  mutating driver call bumps before its own effect reaches PostgreSQL (inside the same
  transaction where one exists, so a rolled-back write's bump rolls back with it). This
  is what makes the snapshot file (next) safe to trust: a file stamped with counter N is
  provably complete for every write up to N, because the counter cannot have advanced
  without a write whose effect the file's own build would then be missing.
- **Snapshot file.** Set `BLOODTRAIL_SNAPSHOT_DIR` to let a restart skip the PostgreSQL
  rebuild. The engine writes a versioned binary snapshot of its in-memory state, stamped
  with the watermark counter that was live at that instant, at two points: on a graceful
  shutdown, and after every background compaction (next). At boot, it loads that file
  and adopts it when the watermark counters prove nothing is missing: every write that
  lands while no replica exists yet is buffered (its counter and the keys it touched),
  and the file is adopted exactly when the file's stamped counter plus those buffered
  writes' own counters account for a fixed target -- PostgreSQL's counter as read once,
  when the attempt starts -- with no hole. The buffered writes are then replayed onto
  the loaded snapshot, through the same read-back machinery ordinary write-through
  uses, before the result is published as one view. A hole is not refused on sight:
  a write's counter is claimed eagerly (at a batch's first buffered operation) while
  the buffering happens when the write commits, so a busy boot always has a few
  claimed-but-uncommitted counters in flight, and the attempt simply waits -- re-checking
  every 50ms, for up to 5s -- for those writes to finish and fill their own holes.
  Writes that start after the target was read need no accounting at all: whichever
  side of the publish they land on, they end up in the view through the ordinary
  paths. This is what lets a restart landing in the middle of an active ingest still
  adopt the file. A quiet restart is the degenerate case (no buffered writes, counters
  exactly equal, no waiting).
  This buffering is what makes the file useful in practice at all: BloodHound queues a
  full analysis request at startup unconditionally and runs its data-pipe daemon with no
  start delay, so AD post-processing writes land within milliseconds of the API coming
  up on *every* boot -- against a multi-second file load at large scale, an
  equality-only check would lose that race essentially always. (A freshly started
  process needs one thing before the load can even be attempted: which graph is the
  default one, which only resolves once the caller makes its own `AssertSchema` call --
  unavoidable, since the driver's `Open` call has to return that same caller its driver
  handle before it can call anything on it at all. Every ordinary boot briefly waits out
  that one call, logged at Debug (`bloodtrail: boot load waiting for the default
  graph`) since it is expected on every startup, not a fault.)

  **When the file is still rejected.** Adoption is a proof, not a hope, and every way
  the proof can fail falls back to a normal PostgreSQL rebuild -- always correct, just
  slower, and a caller cannot tell the outcomes apart. A counter the boot cannot
  account for even after the settle window rejects the file (`boot gap not covered by
  buffered writes`, with a `waited` duration): a write from a previous process's crash
  window, any writer this process never observed, or an in-flight write that outlived
  the 5s wait. A boot-time write whose effect cannot be expressed as a replay -- raw
  Cypher, a wipe, the same closed list ordinary write-through falls back on -- trips
  fallback and rejects the file immediately (nothing to wait for), as does a buffer
  that outgrew its caps (4,096 writes / 262,144 keys) under a genuinely heavy boot.
  Each rejection is logged at Info with a `reason` and is followed by an ordinary
  `snapshot rebuilt` line; an adoption logs `snapshot file loaded` with a
  `replayed_writes` count.
- **Compaction.** Every applied write layers one more delta on top of the engine's base
  snapshot; past a size threshold (`BLOODTRAIL_COMPACT_ENTRIES`/`BLOODTRAIL_COMPACT_BYTES`,
  see Configuration below), a background compaction folds the base and every delta into
  a fresh base entirely in memory -- no PostgreSQL round trip, no JSON parsing, the two
  costs that make a full rebuild slow. Ordinary writes keep applying (as further deltas)
  while a compaction runs; when it finishes, it also writes the snapshot file, so a
  restart always resumes from something no older than the last compaction.

See [bench/applybench](bench/applybench) for the write-through path's own measurements
(apply overhead against a write-through-disabled baseline, query latency while a delta is
populated, compaction duration, and snapshot file write/load/boot time, all at 5M scale).

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

- **Serving.** The engine keeps a compressed in-memory replica (node/edge ids, kinds, node
  property bags, and, since milestone 5, every write-through delta layered on top of it --
  see [Write-through](#write-through)); edge properties are still hydrated from PostgreSQL
  per query -- see [Cypher interpreter](#cypher-interpreter). A path query is served from
  memory only if the engine is in the SERVING state (not FALLBACK), the query's endpoints
  resolve inside the current view, and the traversal fits the request's own memory budget.
  Any of these failing -- disabled, FALLBACK, an unrecognized query shape, or too large a
  traversal -- makes the engine decline outright and PostgreSQL answers instead; the only
  difference an operator or user should ever see is latency.

- **Configuration** (environment variables, read once at driver startup):
  - `BLOODTRAIL_ENGINE` -- `on` (default) or `off` (also accepts `true`/`false`/`1`/`0`).
    `off` makes every read delegate straight to PostgreSQL, as in milestone 1; writes still
    bump the watermark (see [Write-through](#write-through)) but the engine never applies or
    serves anything.
  - `BLOODTRAIL_SNAPSHOT_DIR` -- directory for the snapshot file described in
    [Write-through](#write-through). Unset (the default) disables the feature entirely: no
    file is ever read or written, and every boot rebuilds from PostgreSQL.
  - `BLOODTRAIL_COMPACT_ENTRIES` / `BLOODTRAIL_COMPACT_BYTES` -- how large the write-through
    delta may grow, in entries and approximate bytes respectively, before a background
    compaction folds it back into the base snapshot (see [Write-through](#write-through)).
    Default to `65536` entries and `512MiB`. `0` on either means "no bound on that
    dimension" (the same convention `BLOODTRAIL_MEMORY_LIMIT` below uses), not "compact on
    every write"; `0` on both disables compaction outright. These thresholds govern how
    large the delta is allowed to get, and reading through a delta costs time
    proportional to its total size on each newly published view -- so a larger threshold
    trades write and read overhead for less frequent folding. The shipped defaults are
    considerably larger than any delta measured so far; see [Known performance
    characteristics](bench/applybench/README.md#known-performance-characteristics).
  - `BLOODTRAIL_MEMORY_LIMIT` -- caps the replica's approximate resident size (e.g. `4GiB`,
    `512MiB`, or a plain byte count). A rebuild, or an applied write, that would push the
    replica's base-plus-delta size past this limit is refused instead: the engine enters
    FALLBACK and retries the resulting rebuild on the existing rate-limited backoff, the same
    way it already recovers from any other fallback trigger. Unset or `0` means unbounded.
  - `BLOODTRAIL_LOG_LEVEL` -- `debug`, `info`, `warn`, or `error`. When set, it widens the
    minimum level BloodTrail's own log lines are guaranteed to be visible at, on top of
    whatever already configures the process's logger -- it can only add visibility, never
    take it away. Left unset -- or declared with an empty value, the shape a
    compose variable with nothing assigned to it produces -- it is a complete no-op. `debug` is what surfaces e.g.
    `bloodtrail: builder engine served` and `bloodtrail: write-through applied`.

- **Log markers**, all under a `bloodtrail:` prefix, grouped by what they cover:
  - **Serving**: `bloodtrail: path engine served` (Info, once per shortest-path query
    actually answered from memory) / `bloodtrail: path engine declined` (Debug, with a
    `reason` attr); their query-builder counterpart `bloodtrail: builder engine served` /
    `bloodtrail: builder engine declined` (both Debug); and the Cypher interpreter's own
    `bloodtrail: cypher engine served` (Debug) -- a *declined* Cypher query logs the
    shared `bloodtrail: path engine declined` line above rather than a distinct marker of
    its own, since only the served side needed one to stay distinguishable from the path
    engine's identically-shaped success case.
  - **Write-through apply**: `bloodtrail: write-through applied` (Debug, per committed write
    that updated the replica) and `bloodtrail: segment stack merged` (Debug, when an
    overgrown delta stack is synchronously collapsed -- an internal bookkeeping event, not a
    fallback).
  - **Fallback**: `bloodtrail: fallback entered` (Warn, with a `reason` attr -- one of the
    closed list in [Write-through](#write-through), a read-back/segment-build/memory-limit
    failure, or a watermark bump that itself failed) and `bloodtrail: fallback exited`
    (Info, once the recovery rebuild lands and serving resumes).
  - **Rebuild** (boot, or fallback recovery -- the only two triggers left): `bloodtrail:
    snapshot rebuilt` (Info, on every successful rebuild), `bloodtrail: snapshot rebuild
    refused: exceeds memory limit` (Warn, rate-limited), `bloodtrail: boot load waiting for
    the default graph` (Debug, expected on every ordinary startup -- see
    [Write-through](#write-through)'s own note), `bloodtrail: boot load failed` (Warn) and
    `bloodtrail: fallback rebuild failed` (Warn).
  - **Watermark**: `bloodtrail: watermark bump failed` (Warn) and `bloodtrail: watermark table
    DDL failed` (Warn).
  - **Snapshot file**: `bloodtrail: snapshot file loaded` (Info), `bloodtrail: snapshot file
    rejected` (Info, with a `reason` attr) / `bloodtrail: no snapshot file` (Debug, the
    ordinary first-boot case), `bloodtrail: snapshot file written` (Info) / `bloodtrail:
    snapshot file not written` (Debug or Warn, depending on why) / `bloodtrail: snapshot file
    skipped` (Debug), `bloodtrail: snapshot file write failed` (Warn), and `bloodtrail:
    removed stale snapshot temp file` (Info, a boot-time reap of a file an earlier process's
    write left half-finished).
  - **Compaction**: `bloodtrail: compaction triggered` (Debug), `bloodtrail: compaction
    started` / `bloodtrail: compaction finished` (Info, the latter with `duration`, `nodes`
    and `edges` attrs), `bloodtrail: compaction discarded` (Info, a stale fold safely thrown
    away rather than adopted) and `bloodtrail: compaction failed` / `bloodtrail: compaction
    snapshot save failed` (Warn).

## Query-builder serving

Milestone 3 extends the same in-memory replica to also answer BloodHound's **query
builder** -- the fluent `Nodes()`/`Relationships()` API BloodHound's own Go code uses
internally, as distinct from a user's Cypher text. This is what backs, among other
things, the entity panel's member and controller listings and the structural scans
analysis itself issues while recomputing derived edges and tags. When a builder query
matches one of a defined set of structural shapes, and the engine is in the SERVING
state (see [Write-through](#write-through)), BloodTrail answers it from memory instead
of PostgreSQL:

- **Node queries** -- count, fetch ids, or fetch id-plus-kind listings -- for any query
  constrained by at least one node-kind filter. An `id()`-only filter matches none of the
  recognizer's shapes (all of which key off a kind constraint), so it always delegates.
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

- **Serving.** Every recognized builder-query shape above is served whenever the engine
  is in the SERVING state and resolves against the current in-memory view -- the same
  SERVING/FALLBACK model every other serving path uses; see
  [Write-through](#write-through). Milestone 3's original per-kind freshness marks (a
  builder query served only if the specific kinds it touched hadn't been written to
  since the replica was last rebuilt) are gone: write-through means the replica's delta
  already reflects every kind's own latest state at all times, so there is no separate
  freshness check left to make -- the same single SERVING check that already covered
  shortest-path queries now covers builder queries too, and just as cheaply (a read
  against an empty delta costs nothing beyond what it always cost).

- **Memory.** Serving relationship queries needs a bit more than the path engine's bare
  topology: each edge's own database id (~8 bytes), a reverse-index pointer back to it
  (~4 bytes), and a small permutation array to look an edge up by that id (~4 bytes) --
  roughly 16 bytes per edge on top of milestone 2's layout. Milestone 4 adds node
  properties and an objectid index on top of that in turn -- see
  [Cypher interpreter](#cypher-interpreter)'s own Memory note for the full formula and
  the measured total at 4.76 million nodes / ~48.9 million edges. `BLOODTRAIL_MEMORY_LIMIT`
  (see Configuration above) caps the whole replica -- base snapshot plus any
  write-through delta layered on it -- the same way it always has: a rebuild, or an
  applied write, that would exceed it is refused instead.

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
  `bench/cypherbench`'s five representative shapes measured at 4.76M nodes, all five
  pass measured per-shape bars. Three win by wide margins (an objectid point lookup
  ~600-1600x faster than delegating, a pre-built shortestPath query ~40-60x faster,
  and a `COLLECT`-based anti-join that used to decline outright and now serves in
  single-to-low-double-digit seconds once seeded from its constrained side rather than
  a full unconstrained node scan). The other two -- a suffix scan and a two-property
  flag scan -- are faster by physics-bound margins rather than multiples: the engine
  answers in ~130-165ms and ~11-17ms respectively, but warm-cache PostgreSQL is also
  fast there (a GIN index narrows the suffix scan to the same handful of rows;
  `LIMIT 1000` lets both sides stop early), so an idle-machine measurement pinned
  their steady state at ~1.3x and ~1.9x and their bars sit just under the worst honest
  measurement -- the engine must stay strictly faster, and a hoped-for 5x the physics
  never supported is not pretended. See `bench/cypherbench/README.md`'s "Measured at
  5M" table for the full numbers, the repeated-run evidence, and the reasoning behind
  each shape -- this is real, in-progress performance, not a claim that every Cypher
  shape is already dramatically faster served locally.
- **What always delegates.** Any query the interpreter's planner does not recognize at
  all; any query bound `$parameters` (BloodHound's own cypher endpoint never sends
  these, so a non-empty `params` map can only mean something this interpreter has no
  way to honor); any updating clause (`CREATE`/`MERGE`/`DELETE`/`SET`/`REMOVE` -- this
  interpreter is read-only); a comparison or `ORDER BY` whose result depends on
  PostgreSQL's own string collation; and a numeric-list `IN` check the interpreter
  cannot evaluate without PostgreSQL's own numeric casting rules. Exactly as
  elsewhere, every decline falls back to PostgreSQL and always returns a correct
  result -- the only difference is latency.

  Four more shapes delegate because dawgs' SQL for them does not mean what Cypher
  means, so serving them would answer a question stock BloodHound answers
  differently or cannot answer at all:
  - A **chained comparison** (`1 < n.val < 5`, `n.val = 1 = true`). Cypher reads it
    as a conjunction; dawgs emits left-associative SQL, which PostgreSQL either
    evaluates differently or rejects outright (`operator does not exist: boolean <
    integer`).
  - A **list literal** containing `null`, a boolean, or a mix of strings and
    numbers. dawgs renders a list as a one-type PostgreSQL array and fails to
    translate all three, so the delegated query errors rather than answering.
  - **`/` and `%` whose operands dawgs types as integers** (`n.val / 2`, `1 / 3`).
    PostgreSQL truncates there; this evaluator has only float arithmetic. Writing
    one operand as a float (`n.val / 2.0`) is served, because dawgs then casts to
    `float8` and both sides agree.
  - **`RETURN DISTINCT` ordered by a carried `COUNT` alias the projection does not
    output.** PostgreSQL rejects that combination ("for SELECT DISTINCT, ORDER BY
    expressions must appear in select list").
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
- **Serving.** Cypher queries are served under the same SERVING/FALLBACK model as every
  other serving path (see [Write-through](#write-through)): a query runs from planning
  through execution against one immutable in-memory view -- the current base-plus-delta
  state at the moment it starts -- with no recheck needed once execution finishes. That
  matches what a single Cypher statement against PostgreSQL already does (one statement,
  one PostgreSQL snapshot), so the two stay equivalent without this engine needing its
  own per-query kind scoping the way milestone 3's now-retired builder-query freshness
  marks once attempted.
- **Budgets.** A served query is capped at 100,000 result rows and a generous internal
  work-unit budget (node/adjacency inspections during execution), and an unbounded
  variable-length relationship pattern (a bare `*`, `*1..`, `*..`) is capped at 15 hops
  -- mirroring dawgs' own default traversal depth cap. Exceeding either budget declines
  the query (falling back to PostgreSQL) rather than serving a truncated result. An
  explicit depth written into the query (`*1..40`) is honored past that default,
  because PostgreSQL honors it too -- but only up to 127 hops, the deepest the
  traversal's own distance buffer can represent; beyond that the query delegates
  rather than being answered from a buffer that cannot hold the answer.
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
properties, and the objectid index together, plus any write-through delta layered on
top -- exactly as it always has: a rebuild, or an applied write, that would exceed it is
refused instead (see [Write-through](#write-through)).

See [bench/cypherbench](bench/cypherbench) for the measurement.

## Installing on an existing BloodHound CE deployment

BloodTrail ships as a patched BloodHound image plus an installer. On the host that runs
the compose deployment:

    curl -fsSL https://github.com/MihhailSokolov/BloodTrail/releases/latest/download/install.sh | sh -s -- install

The installer asks for confirmation before it changes anything, reading the answer from
the terminal; add `--yes` to run it unattended.

Until the first release is published, build the CLI with `go build ./cmd/bloodtrail`
instead; the one-liner above describes the intended installation once a release exists.
The bootstrap script verifies the download with `sha256sum` where available and falls
back to `shasum -a 256` (stock macOS) otherwise.

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
- Writing that entry replaces docker compose's own file discovery, so the installer
  writes out everything discovery would have found: the compose file it was given and,
  when one sits beside it, the conventional `docker-compose.override.yml`. Deployments
  that keep their customizations in that override file therefore keep them, both in the
  installer's own commands and in the operator's afterwards. `bloodtrail rollback`
  removes the whole entry again when the install was what created it, which puts
  discovery back the way it was.
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
5. Write-through, so the replica is never stale. **Done.** What's still explicitly out of
   scope: interpreting mutating Cypher and arbitrary update/delete criteria (both stay a
   fallback trigger rather than an incremental apply -- see
   [Write-through](#write-through)'s closed fallback list); and cache coherence across more
   than one BloodTrail-patched process writing the same database, beyond the boot-time
   watermark detection [Write-through](#write-through) already describes.

## Repository layout

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
bench/pathbench/       Benchmarks the in-memory path engine against a loaded graph
bench/builderbench/    Benchmarks query-builder serving against a loaded graph
bench/cypherbench/     Benchmarks Cypher-interpreter serving against a loaded graph
bench/applybench/      Benchmarks the write-through apply path against a loaded graph
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
