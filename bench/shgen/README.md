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

## The Entra/Azure tenant (hybrid data)

Passing `-az-users N` (0 disables the whole azure side) adds one Entra
tenant in AzureHound v2 format, ingested through the same upload as the AD
files: AZUsers (a configurable share hybrid-synced to AD users by on-prem
SID), AZGroups, the built-in directory AZRoles with their real template
GUIDs, AZApps with service principals (including the tenant's Microsoft
Graph SP), AZDevices, and an Azure resource tree (subscriptions, resource
groups, AZVMs, AZKeyVaults). Sizing flags mirror the AD side:
`-az-groups`, `-az-apps`, `-az-devices`, `-az-vms`, `-az-keyvaults`,
`-az-subs`, `-az-sync-pct`.

Seeded azure attack paths (reserved AZUser indices 0-10, each verified
live against a stock deployment -- ingest, analysis, and the derived edges
the prebuilt queries traverse):

- hybrid: AD user (0,0) -> `SyncedToEntraUser` -> the tenant's Global
  Administrator -> `AZGlobalAdmin` -> tenant (and `SyncedToADUser` back);
- PIM: user 1 -> `AZRoleEligible` -> Privileged Role Administrator;
- app owner: user 2 -> `AZOwns` -> app 0 -> `AZRunsAs` -> its SP, which
  holds RoleManagement.ReadWrite.Directory on Microsoft Graph -- analysis
  fans that out into `AZMGGrantRole`/`AZMGGrantAppRoles`/`AZMGAddSecret`;
- role-assignable group: user 3 -> `AZMemberOf` -> TIER ZERO ADMINS ->
  `AZHasRole` -> Privileged Role Administrator;
- key vault: user 4 -> `AZGetSecrets`/`AZGetKeys`/`AZGetCertificates` ->
  vault 0;  VM: user 5 -> `AZVMAdminLogin` -> VM 0;
- Intune: user 6 -> role -> `AZExecuteCommand` -> every Windows AZDevice;
- scoped app admin: user 7 -> `AZAppAdmin` -> app 1; approver: user 8 ->
  `AZRoleApprover` -> the Global Administrator role;
- and the role matrix itself: `AZResetPassword`, `AZAddSecret`,
  `AZAddOwner`, `AZAddMembers` all derive from the seeded role holders.

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

## Measured on the hybrid graph (2026-09-15, 450k AD users + 60k Entra users)

One tenant alongside a 4-domain forest: 692k AD objects + 88k azure items,
~1.0M nodes after ingest. Both arms measured on identical data, one stack
resident at a time, canary-checked. Full corpus = BloodHound's 165 shipped
prebuilt/selector queries + 34 adversarial shapes:

| | stock (pg driver) | BloodTrail |
|---|---|---|
| whole-corpus p50 total | 90.5 s | **29.9 s** (0.33x) |
| azure-touching queries | 55.7 s | **9.0 s** |
| worst single query | 49.5 s (Shortest paths to Azure Subscriptions) | 1.1 s |
| queries >10x slower than the other arm | 3 | **0** |
| queries >5x slower than the other arm | 10 | **3** |
| queries >2x slower than the other arm | 40 | 22 |
| ingest (upload -> datapipe idle) | 262 s | 341 s (+30%, write-through) |
| peak RSS over the sweep | 3.2 GiB | 6.3 GiB |

Every remaining regression is between 2x and 6x, with absolute times from
62 ms to 1.0 s; nothing is catastrophic and nothing is unbounded. A query
whose intermediate row set would exceed `MaxLiveRows` now declines to
PostgreSQL rather than growing until the container is OOM-killed.

All 22 azure derived-edge checks (AZGlobalAdmin, SyncedToEntraUser/
SyncedToADUser, the AZMG* family, AZResetPassword, AZRoleEligible/Approver,
AZExecuteCommand, key-vault and VM access) return identical results on both
arms, and result sizes agree on 43 of 47 azure prebuilts -- the remainder are
the documented LIMIT/shortest-path witness-choice nondeterminism, verified
node-subset-level.

## Measured at 500k (2026-09-15)

One run of all three arms on an M3 Pro laptop (Docker Desktop, 7.65 GiB VM),
`-users 500000 -computers 250000 -domains 4 -seed 1` = 775k objects / 729 MB,
ingested to **1,025,106 nodes / 1,934,684 edges** (the node count exceeds the
object count because each computer's local Administrators group becomes a node).
Every arm started empty and ingested the identical files; `stock-pg` is the
engine-isolating baseline, speedups are relative to it.

| phase | stock-neo4j | stock-pg | bloodtrail |
|---|---|---|---|
| upload | 15.7 s | 16.1 s | 16.5 s |
| ingest | 135.4 s | 150.3 s | 185.4 s |
| analysis | 90.2 s | 75.1 s | 75.2 s |
| **end to end** | 241.3 s | 241.6 s | 277.2 s |

| query (p50, warm) | stock-neo4j | stock-pg | bloodtrail |
|---|---|---|---|
| shortest path, kerberoast pivot | 6.7 ms | 20.0 ms | **3.1 ms** (6.5x) |
| shortest path, membership chain | 6.9 ms | 116.4 ms | **7.9 ms** (14.7x) |
| cypher, all Domain Admins (`MemberOf*1..`) | 10.8 ms | 31.7 ms | **16.5 ms** (1.9x) |
| cypher, kerberoastable scan | 20.2 ms | 21.8 ms | 16.6 ms (1.3x) |
| cypher, objectid point lookup | 4.4 ms | 2.9 ms | 2.7 ms (1.1x) |
| members of Domain Admins | 9.8 ms | 5.3 ms | 4.5 ms (1.2x) |
| members of Domain Users (500k members) | 1469 ms | 1622 ms | 1683 ms (1.0x) |

**Reading these honestly:**

- **Pathfinding is the win** -- 6.5x and 14.7x over the PostgreSQL baseline,
  which is the workload BloodTrail exists for.
- **Write-through's cost is confirmed**: the ingest phase runs 185.4 s vs
  150.3 s, **+23%**, inside the 19-34% band the top-level README documents.
  Analysis is unaffected (75.1 vs 75.2 s).
- **The variable-length Cypher prebuilt was 84x SLOWER (2650 ms) when this
  table was first measured, and is 16.5 ms after the fix that measurement
  prompted.** Two independent blockers each forced BloodHound's most
  recognizable shipped query onto a full scan of every node, and because
  either one alone was sufficient, removing them one at a time each looked
  like a disproof:
  1. The prebuilts write their near-endpoint type filter as a WHERE label
     disjunction, `(a:User or a:Computer)`. Every single-symbol conjunct is
     pushed into `NodeConstraint.Predicates`, and any predicate counted as
     "this endpoint narrows" -- which disqualified the constrained-side
     (reverse) route, even though a kind test is exactly what that rule
     already said must not count.
  2. Every shipped prebuilt carries `LIMIT 1000`, which handed the component
     to the chunked early-termination driver; that driver grows a chunk of
     NEAR-endpoint rows and structurally cannot seed from the far endpoint.
     Measured: 20,031 work units with the LIMIT against 19 without, for the
     same answer.

  Both are fixed in `internal/engine/interpret` (`kindOnlyPredicate`,
  `componentPrefersReverseSeeding`). The second deliberately overturns a
  previously pinned design decision -- early termination had been judged
  worth more than the seeding choice, and measurement said otherwise by
  three orders of magnitude.
- **Large membership listings are unaffected** (1.6 s either way): that
  entity-panel path is not one the engine accelerates today.
- Neo4j and PostgreSQL ingest at the same end-to-end speed here (241 s
  both); they differ in where the time goes, not how much.

The `bloodtrail` column reflects the fixed engine; the pre-fix run is kept
above in prose because the gap it exposed is the reason the fix exists.

Single run, laptop, one graph shape -- directional, not a lab result.

For engine-only microbenchmarks (no ingest, no API), use
[`bench/pathbench`](../pathbench) / [`bench/cypherbench`](../cypherbench) on
an [`bench/adgen`](../adgen)-loaded database instead.
