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

1. Deploy BloodHound CE (the upstream quickstart compose), pin the image tag,
   and note the initial admin password from the `bloodhound` container's
   first-boot logs.
2. Generate a forest at your target scale and upload it: UI (drag the `-zip`
   archive into Administration -> File Ingest) or API (the flow in
   `internal/verify/smoke.go`: `POST /api/v2/file-upload/start`, POST each
   JSON with `X-File-Upload-Name`, `POST .../end`, then poll `/api/v2/file-upload`
   and `/api/v2/datapipe/status`).
3. Time what you care about on stock BloodHound: ingest wall time (upload ->
   job Complete), analysis (datapipe back to idle), and the queries the UI
   leans on -- pathfinding between two principals, the pre-built searches
   under Cypher, group membership listings.
4. `bloodtrail install` (see the top-level [README](../../README.md)), then
   repeat step 3 on the same data. For ingest-side numbers, wipe and re-upload
   the same files so both runs ingest identical input; expect writes to cost
   roughly a quarter more wall time (write-through's documented price) and
   the read side -- pathfinding and Cypher -- to be where the improvement
   shows.
5. `bloodtrail rollback` returns the deployment to stock whenever you want to
   re-measure the baseline.

For engine-only microbenchmarks (no ingest, no API), use
[`bench/pathbench`](../pathbench) / [`bench/cypherbench`](../cypherbench) on
an [`bench/adgen`](../adgen)-loaded database instead.
