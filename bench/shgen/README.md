# shgen

`shgen` generates a fictitious corporate Active Directory forest as
**SharpHound v6 collection JSON**, for ingesting through BloodHound's ordinary
file-upload pipeline. Where [`bench/adgen`](../adgen) loads a synthetic graph
*directly into PostgreSQL* (fast, bypasses ingest -- right for benchmarking
the engine's own query paths in isolation), `shgen` produces data BloodHound
treats exactly like a real SharpHound collection: it exercises ingest,
post-processing/analysis and the UI's own queries end to end -- which is what
you want when comparing a whole deployment **with and without BloodTrail**.

```
go run ./bench/shgen -users 500000 [-computers 250000] [-domains 4] [-seed 1] [-chunk 50000] [-out DIR] [-zip forest.zip]
```

Defaults: `-computers` = users/2, `-groups` = users/20 (plus the well-known
groups), `-domains` = 1 per 250k users, forest root `MEGACORP.LOCAL` (children
`DIVNN.MEGACORP.LOCAL`, trusted bidirectionally with the root). Output is one
set of `{"data":[...],"meta":{...}}` files per object type, chunked at
`-chunk` objects per file; `-zip` additionally bundles everything into one
archive for UI drag-and-drop. Generation is fully deterministic -- the same
seed and counts produce byte-identical files -- and streams object by object,
so memory stays flat at any scale (500k users / 250k computers: ~775k
objects, ~730 MB of JSON, ~30 MB zipped, generated in a few seconds).

Everything is synthetic: names cycle fixed lists with numeric suffixes, SIDs
derive from the seed, and no real organization, person or credential appears
anywhere.

**No object ever references itself.** A self-referencing ACL or membership
edge is a self-loop, and a self-loop of an admitted relationship kind makes
BloodTrail's variable-length executor decline the whole pattern and delegate
to PostgreSQL (see the top-level README's "What always delegates"). One stray
self-ACE would therefore push every `[*1..]` query in a benchmark onto the
delegating path and understate the engine for a reason that has nothing to do
with the engine. `TestNoSelfLoopEdges` pins this across several seeds and
shapes.

## Seeded attack paths

Beyond realistic noise (group nesting, ACL fan-out, sessions, stale
passwords), every domain carries a fixed set of attack paths at reserved
indices, each pinned by `generate_test.go` and verified end to end against a
live stock BloodHound (upload -> ingest -> analysis -> the path is returned
by `/api/v2/graphs/shortest-path`):

1. **Nested membership to Domain Admins**: a rank-and-file user sits in
   HELPDESK ⊂ IT ADMINS ⊂ SERVER ADMINS ⊂ DOMAIN ADMINS.
2. **ACL chain**: a user holds `GenericAll` over APP OWNERS, which holds
   `GenericAll` over DOMAIN ADMINS.
3. **Kerberoast pivot**: `SVC-SQL*` (an SPN-bearing, kerberoastable service
   account) is in the local Administrators of a host (AdminTo) on which the
   built-in Administrator -- a Domain Admin -- has a session.
4. **Unconstrained delegation**: a host trusted for unconstrained delegation
   also holds a DA session.
5. **AS-REP roastables**, a percentage of users (`dontreqpreauth`).
6. **Tier-1 breadth**: IT ADMINS is local admin on every fifth machine, and a
   fraction of machines carry random user sessions -- session-hunting works
   the way it does on real data.
7. **Cross-domain**: with `-domains > 1`, bidirectional parent-child trusts
   connect the forest, and the root's ENTERPRISE ADMINS reaches everywhere.

## Measuring BloodTrail's effect

[`bench.py`](bench.py) automates one side of the comparison: it uploads a
generated forest, waits for ingest and analysis, then times the three read
paths BloodTrail serves (pathfinding, Cypher, entity-panel reads), writing a
JSON report. Every phase is bounded by `--deadline`; nothing waits forever.

    python3 bench.py --port 8181 --data DIR --password PW --label stock

**Mind which baseline you are measuring against.** The upstream quickstart
compose defaults to `bhe_graph_driver=neo4j`, so a stock deployment out of
the box stores its graph in Neo4j while BloodTrail always runs on
PostgreSQL. Comparing those two directly answers the operator's question
("what do I gain by installing this?") but conflates two changes -- the
storage swap and the engine. To isolate the engine, set `GRAPH_DRIVER=pg` in
the baseline's `.env` so both sides use PostgreSQL and only the driver
differs. Running all three arms (neo4j, pg, pg+bloodtrail) answers both
questions and costs one extra ingest.

> **Don't mistake the datapipe's tick for a stall.** After the last file is
> uploaded the job sits at `status 6` / `total_files: 0` with every container
> idle for twenty or thirty seconds before the daemon picks it up
> (`Ingest run starting`, `task_count: N` in the `bloodhound` logs). That is
> normal. A fresh `GRAPH_DRIVER=pg` boot also logs `Unable to find expected
> data type: nodecomposite. This database connection will not be pooled.`
> because the server creates its graph schema after caching the schema's
> absence; it is a pooling warning, not a failure, and ingest proceeds.

Run each arm against a deployment that starts **empty** and ingests the same
files, so the ingest-side numbers are comparable and the query-side ones are
measured on identical data:

1. Deploy BloodHound CE (upstream quickstart compose) with the image tag
   pinned in `.env` (`BLOODHOUND_TAG=9.7.0` -- the installer refuses a moving
   `:latest`), and read the initial admin password from the `bloodhound`
   container's first-boot logs.
2. `bench.py ... --label stock-pg` against it.
3. Tear that stack down, bring up an identical empty one, `bloodtrail
   install` it *before* any ingest, then `bench.py ... --label bloodtrail`.
   Running the arms sequentially rather than side by side keeps them from
   competing for the same CPU and disk -- which matters: at 500k scale one
   arm alone saturates several cores.
4. Compare the reports. Expect write-side wall time to cost roughly a quarter
   more (write-through's documented price) and the read side -- pathfinding
   and Cypher especially -- to be where the improvement shows.

Uploading by hand instead: drag the `-zip` archive into the UI's File Ingest
page, or drive the API flow in `internal/verify/smoke.go`.

`bloodtrail rollback` returns a deployment to stock whenever you want to
re-measure a baseline on the same box.

For engine-only microbenchmarks (no ingest, no API), use
[`bench/pathbench`](../pathbench) / [`bench/cypherbench`](../cypherbench) on
an [`bench/adgen`](../adgen)-loaded database instead.
