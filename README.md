# BloodTrail

An in-memory graph engine underneath [BloodHound CE](https://github.com/SpecterOps/BloodHound),
packaged as a [DAWGS](https://github.com/SpecterOps/DAWGS) driver, so that attack-path
analysis and every other graph query stay fast on large Active Directory environments
and ordinary hardware.

**Status:** early development. The driver does not exist yet; this repository currently
holds the project scaffolding and a traversal benchmark.

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
  adjacency with an edge kind per entry, kind bitmaps, columnar properties and property
  indexes. Queries are executed against the replica by an interpreter over DAWGS's
  Cypher syntax tree, which is the tree both BloodHound's Go query builder and its text
  Cypher already produce.
- PostgreSQL remains the system of record. Writes go to PostgreSQL first and are
  applied to the replica on commit; on startup the replica is loaded from PostgreSQL or
  from a snapshot file.
- Deployment is a patched BloodHound image built from the upstream Dockerfile plus a
  two-file change, and an installer that upgrades an existing BloodHound CE deployment
  with backup and rollback.

## Roadmap

1. Packaging: patched image, skeleton driver delegating to PostgreSQL, installer with
   rollback.
2. Path engine: shortest paths, all shortest paths and reachability from memory.
3. Query-builder execution from the replica: entity panels, post-processing, tagging.
4. Cypher interpreter for pre-built and user queries.
5. Write-through, so the replica is never stale.

## Repository layout

```
bench/csrbench/   CSR traversal micro-benchmark (self-contained Go module)
```

## Upstream versions

Developed against `github.com/specterops/bloodhound` at `441f20b` and
`github.com/specterops/dawgs` `v0.8.0` (`0ea9646`).

## Licence

Apache-2.0, the same licence as BloodHound and DAWGS. See [LICENSE](LICENSE).
