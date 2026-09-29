# oggen

`oggen` generates a fictitious source-control organization as **BloodHound
OpenGraph JSON**, for uploading through BloodHound's ordinary file-upload
pipeline and comparing a deployment with and without BloodTrail on
OpenGraph data. It is to OpenGraph what [`bench/shgen`](../shgen) is to
SharpHound collections.

```
go run ./bench/oggen -users 100000 [-teams N] [-repos N] [-seed 1] [-chunk 100000] [-ad SHGEN_OUT] [-sync-pct 10] [-out DIR]
```

Defaults: `-teams` = users/10, `-repos` = users/5 (each with an admin,
a write and a read role). The output is `DIR/extension.json`, an extension
schema for `PUT /api/v2/extensions` that marks every edge kind but
`sc_Owns` traversable, and `DIR/data/{nodes,rels}-NNNN.json`: chunked
OpenGraph files, all with `metadata.source_kind` `sc_Base`, nodes before
edges. Generation is deterministic: the same seed and counts produce
identical files.

The organization:

- `sc_User` accounts (the first 0.1% also `sc_OrgOwner`, with `sc_OrgAdmin`
  over the `sc_Organization`), each in one to three `sc_Team`s;
- teams nested under older teams (`sc_MemberOf`), so chains stay
  logarithmic in depth;
- `sc_Repository` nodes with string, integer, float, boolean and array
  properties, each owned by the organization (`sc_Owns`, not traversable)
  and carrying three `sc_RepoRole`s reached by `sc_CanAdmin`,
  `sc_CanWrite` and `sc_CanRead`;
- `sc_HasRole` grants from teams (one to eight each) and directly from a
  fifth of the users;
- with `-ad`, a [`bench/shgen`](../shgen) output directory: its first AD
  users are `sc_SyncedTo` the first organization users (`-sync-pct` of
  them), making hybrid AD-to-OpenGraph paths.

Repository 0 is reachable along fixed paths of two, three and four hops
(users 1, 2 and 0), and five from the first synced AD user, pinned by
`generate_test.go` along with determinism, the absence of self-loops (which
would make BloodTrail delegate every variable-length query over them) and
BloodHound's three-kind limit per node.

## Measuring

[`bench.py`](bench.py) measures one deployment: it uploads the AD forest
(optional), registers the extension, uploads the organization as one job,
times Cypher and pathfinding queries over it warm, and optionally times
clearing the `sc_Base` source kind. Its report has the same shape as
`bench/shgen`'s, so [`../shgen/compare.py`](../shgen/compare.py) renders two of
them side by side, and every query also records a `result_signature` (a
digest of the answer's nodes, edges and literals) that must match between
the arms.

    python3 bench.py --port 8282 --data OGGEN_OUT --ad SHGEN_OUT --password PW --label stock-pg --delete
    python3 bench.py --port 8181 --data OGGEN_OUT --ad SHGEN_OUT --password PW --label bloodtrail --delete
    python3 ../shgen/compare.py og-bench-stock-pg.json og-bench-bloodtrail.json

Run each arm against a deployment that starts empty, one at a time, as
`bench/shgen`'s README describes; compare BloodTrail against BloodHound on
the **PostgreSQL** driver to isolate the engine.

## Measured

A 190k-node organization with 10k hybrid AD edges, three runs per arm: every
answer identical on both arms, path-shaped queries 30-70x faster with
BloodTrail, scans and aggregates 2-8x, ingest a quarter slower. The table and
its caveats are in [BENCHMARK.md](../../BENCHMARK.md#opengraph).
