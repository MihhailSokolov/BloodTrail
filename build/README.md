# Building the BloodTrail image

`build/build-image.sh <upstream-tag> [driver-version] [--push] [--platform …] [--dawgs-only]` builds
BloodHound CE at the given upstream release tag with the BloodTrail driver compiled in.

The only change to BloodHound's own source is `patches/bloodhound-driver.patch`, which touches
two files: `cmd/api/src/bootstrap/util.go` registers the driver name, and
`cmd/api/src/migrations/manifest.go` makes the one PostgreSQL-only graph migration
(`Version_852_Migration`) recognise BloodTrail. Upstream detects PostgreSQL with
`pg.IsPostgreSQLGraph`, a concrete `*pg.Driver` type match that BloodTrail's driver (which
embeds one) does not satisfy, so without that hunk the migration is skipped and still
recorded as done. `go.mod` is edited by the script with `go mod edit`, so upstream
dependency bumps never conflict with the patch.

That edit is not free of side effects on the dependencies. `go mod tidy` resolves upstream's
module graph together with the driver's, and minimum version selection takes the higher
`github.com/specterops/dawgs` of the two, so a release that pins an older dawgs than the driver
was built against ships a newer one than its own tests ever ran with: v9.6.0 pins v0.7.0 and its
image carries v0.8.0 (the driver's engine does not compile against v0.7.0). The script therefore
prints the version upstream pins and the one the image resolves, and fails the build when they
differ unless that exact pair is listed, with the reason, in `dawgs_shift_reason` in
`build-image.sh` (v9.6.0: v0.7.0 to v0.8.0 is the only entry). It also fails for a resolved
version that is neither the one `go.mod` names nor in `dawgs_tested_versions`, the versions the
unit and integration suites are run against (v0.8.0 and v0.8.1 today). The release and weekly
image workflows run this script for every supported release and run no suite themselves, so a
new upstream release that pins a dawgs nobody has tried (a v9.8.0 pinning v0.9.0, say) would
otherwise publish an image before any suite ran on it, which is how v9.7.1's image came to ship
v0.8.1 while `go.mod` said v0.8.0. Such a build now fails, and so does `ci.yml`'s `dawgs` job
(`build/dawgs-suites.sh`, which resolves every supported release the same way), until the version
is listed; that same job then runs the suites against it, as it already does against v0.8.1, and
the change should merge only if they pass. `--dawgs-only` stops after these checks and prints the
resolved version. `build/test-build-image.sh` tests both scripts against stand-ins for git, go and
docker.

Vendoring copies the driver's root Go files plus `internal/engine` -- the in-memory path
engine -- into `packages/go/bloodtrail`, which the upstream Dockerfile's builder stage already
copies wholesale (`COPY --parents go* cmd/api packages/go server ./`), so building the image
compiles the engine along with the driver. `internal/engine`'s own `*_test.go` files are
stripped before vendoring: they reference `internal/graphtest`, a test-only helper package that
is deliberately not vendored, and `go mod tidy` resolves test dependencies for every package
the main module transitively imports, vendored ones included. Everything else under
`internal/` -- the installer, its CLI, the verify fixtures -- is operator tooling with no
business inside the served image and stays out.

Images are tagged `ghcr.io/mihhailsokolov/bloodtrail:<tag>-bt<driver version>`
and `:<tag>`. The second is a moving alias for the newest driver build against that
upstream release -- including the weekly builds from `main`, so it can hold an unreleased
driver. A released installer therefore never falls back to it: when the tag for its own
version is missing it stops and names both, and `--image <alias>` is the explicit way to
accept the alias anyway. Only a source-built (`dev`) installer targets the alias by
default, since it has no version of its own to pin. A leading `v` is stripped from the driver version, so the same
string is stamped into `Version` and used in the image tag.

(Releases up to and including v0.1.0 resolve images under the package's original name,
`ghcr.io/mihhailsokolov/bloodhound-bloodtrail`, which is no longer anonymously pullable
-- so a v0.1.0 CLI cannot complete an install and v0.1.1 is the oldest usable release.
The repo name is compiled into each CLI, so a rename breaks every already-released
binary while leaving no trace in the source tree: after one, publish a release built
against the new name and verify an anonymous pull of the exact `<tag>-bt<version>` the
new CLI will ask for.)

Requires Go 1.26+, Docker with Buildx, and network access to GitHub.

A local build targets the Docker daemon's own platform unless `--platform` says otherwise,
so on Apple Silicon it produces a native `linux/arm64` image:

    ./build/build-image.sh v9.6.0 0.1.0-dev

Do not benchmark an image built for another architecture. Docker runs it under emulation
without complaint, and the engine is CPU-bound enough that emulation distorts it far more
than it distorts BloodHound's HTTP layer: an `amd64` image on an M3 measured a full scan at
775ms against 122ms native. `docker image inspect <image> --format '{{.Architecture}}'`
says what you actually built. A `--push` without `--platform` still publishes `linux/amd64`.

## End-to-end test

`build/e2e.sh [upstream-tag]` (defaults to `v9.6.0`) builds the image, boots the upstream
example compose stack on Neo4j -- with `BLOODTRAIL_LOG_LEVEL=debug` stamped into the
`bloodhound` service's environment on that base compose file before the stack's first boot,
so every marker in the table below is visible from the start, Debug-level ones included, with
no later restart needed just to turn logging up -- and drives `cmd/bloodtrail` against it:
install, verify, roll back, confirm a second install refuses to migrate onto the graph the
first one left in PostgreSQL, clear that with `--replace-postgres-graph`, and roll back again.
The install step passes `--admin-password`, which runs an ingest-and-search smoke test against
the fixture `TESTLAB.LOCAL` domain (`internal/verify/fixture`) -- BloodTrail's own phases then
run against that same fixture, still installed, before rollback:

1. **Zero-rebuild write-through.** Assert the `bloodhound` container's logs show exactly one
   `snapshot rebuilt` (the one-shot boot load, `trigger=startup`) across install+ingest+analysis,
   at least one `write-through applied`, and no rebuild between ingest completion and a
   `POST /api/v2/graphs/cypher` lookup of a node analysis itself creates (the domain's well-known
   `Everyone` principal) -- proof every write, ingest and analysis alike, replayed directly into
   the in-memory replica instead of falling back to a PostgreSQL rebuild.
2. `GET /api/v2/graphs/shortest-path` between two fixture objects known to be connected
   (TESTLAB.LOCAL's built-in Administrator, RID 500, is a direct `MemberOf` member of Domain
   Admins, RID 512) and assert HTTP 200 with a non-empty `data.nodes`, and that the logs show
   more `path engine served` lines after the call than before.
3. `POST /api/v2/graphs/cypher` with a pre-built-shaped `shortestPath` query anchored on the
   same two fixture objects, and assert HTTP 200.
4. `GET /api/v2/groups/{object_id}/members` and two `POST /api/v2/graphs/cypher` queries copied
   from BloodHound's own pre-built query corpus, each asserted against its own served-marker delta
   the same way step 2 was -- `builder engine served` and `cypher engine served` are logged at
   Debug, already visible since the stack's first boot.
5. **OpenGraph** (`build/e2e-opengraph.sh`, with `testdata/opengraph`). Register an extension
   schema (`PUT /api/v2/extensions`), then upload three OpenGraph files through
   `/api/v2/file-upload`: nodes and id-matched edges with a source kind, including a stub
   endpoint and an edge from a fixture AD user; edges matched by name and by property,
   one of them unresolvable (the job must end partially complete with that one warning);
   and a file that fails validation after registering its own source kind. Then run
   fourteen Cypher queries (every value type, multi-kind and stub nodes, variable-length
   paths, `shortestPath`, both `allShortestPaths` answers, a hybrid AD-to-OpenGraph path,
   counts) and three pathfinding calls with `only_traversable=true`. Last, clear sourceless
   data, then the source kind (`POST /api/v2/clear-database`). The expected answers are
   derived from the fixtures and pinned in the script, not read back from the engine under
   test while it runs, and a graph answer is compared by content: the sorted objectIds of
   its nodes and the sorted (source objectId, target objectId, kind) triples of its edges,
   not their number. Their sizes are what stock BloodHound v9.6.0 on PostgreSQL returns,
   with one exception: the whole-graph count after the sourceless delete is a pinned 131,
   and that number came from BloodTrail's own answer in the CI e2e run on `main` before
   it was pinned (run 36621234497), not from stock PostgreSQL. It agrees with the
   118 nodes that BloodHound v9.6.0's ingest and analysis of the SharpHound fixture leave
   before the OpenGraph phase, plus the 13 that `graph.json` adds, but that is a
   cross-check, not a measurement on stock PostgreSQL. It is specific to v9.6.0's ingest
   and analysis and must be re-derived when the BloodHound version the e2e validates
   changes. Every answer must carry its `cypher engine served` or `path engine served`
   marker, and the phase must log no `snapshot rebuilt` and no `fallback entered`.
   `CHECK_SERVED=0` runs the same expectations against a BloodHound on the PostgreSQL
   driver.
6. **Snapshot-file restart.** Enable `BLOODTRAIL_SNAPSHOT_DIR` with a bind-mounted host
   directory (so the file survives the container recreate the config change itself causes),
   then `docker compose restart` the same container -- no further config change, so the
   container is not recreated. `snapshot file written` at that shutdown is asserted
   unconditionally; the reboot is then required to land on exactly one of two outcomes,
   with anything else a failure:
   - **adopted** -- `snapshot file loaded` with **no** new `snapshot rebuilt` line: proof the
     file itself, not a rebuild that happened to produce the same answer, is what the reboot
     served from. This is the expected outcome: BloodHound writes to the graph on every boot
     (it queues a full analysis request at startup and runs the data-pipe daemon with no
     start delay), and those writes are buffered during the file load and replayed onto it
     (the marker's `replayed_writes` attribute counts them).
   - **superseded** -- `snapshot file rejected` for a boot-time write the replay could not
     account for (`reason` is `boot gap not covered by buffered writes` with PostgreSQL's
     counter ahead of the file's, or `the engine entered fallback while the file was
     loading` for a fallback-shaped write), followed by a successful rebuild. Rare but
     correct -- the watermark protocol is refusing a file it cannot prove complete. See
     the snapshot-file section of the top-level
     [README](../README.md#fast-restarts-the-snapshot-directory).

   Either way the boot must have read the file this phase's own shutdown wrote (compared by
   stamped watermark), and a rejection for a corrupt or wrong-version file, a changed
   watermark lineage, rows inserted behind the watermark, a counter behind the file's
   stamp, a buffered boot write that contradicts the file, a failed watermark read, the
   memory limit, or no file attempt at all still fails. One more
   `GET /api/v2/graphs/shortest-path` confirms the engine answers correctly on both paths,
   and that the path engine served it (a new `path engine served` line), not PostgreSQL.

Requires the same tools as `build-image.sh`, plus `docker compose`, `curl` and `jq`. Like a
local `build-image.sh`, it builds for the Docker daemon's own platform, so the stack runs
natively; set `PLATFORM` (e.g. `PLATFORM=linux/amd64 ./build/e2e.sh`) to build for another.

### Log markers

The `bloodhound` container's logs carry BloodTrail's own markers, all prefixed
`bloodtrail:`. The Debug-level ones below appear only under `BLOODTRAIL_LOG_LEVEL=debug`
(which `e2e.sh` sets from the stack's first boot); everything at Info or above is logged
regardless.

| Marker | Level | Meaning |
| --- | --- | --- |
| `write-through applied` | Debug | A committed write was replayed into the in-memory replica with no rebuild. |
| `fallback entered` / `fallback exited` | Warn / Info | A write could not be replayed narrowly; every query declines to PostgreSQL until the recovery rebuild below adopts a fresh snapshot. |
| `watermark bump failed` | Warn | The counter that proves the replica has seen every committed write could not be advanced for one write, so that write is about to reach PostgreSQL uncounted. The engine keeps serving but distrusts its own freshness until a rebuild resolves it, and it deletes any saved snapshot file (`snapshot file invalidated` below). Ordinarily means the watermark table was briefly unwritable; persistent occurrences are worth investigating on the database side. |
| `snapshot rebuilt` | Info | A full PostgreSQL rebuild ran and was adopted -- `trigger` names why: `startup` (the one-shot boot load), `fallback` (recovery from the line above), or `manual`. |
| `snapshot file written` | Info | The current replica was folded and written to `BLOODTRAIL_SNAPSHOT_DIR` -- by a clean shutdown, or by a background compaction once it had adopted its result. |
| `snapshot file loaded` | Info | Boot trusted and loaded that file instead of rebuilding from PostgreSQL, after replaying `replayed_writes` boot-time writes onto it (0 on a quiet restart). |
| `snapshot file rejected` | Info | Boot found a file but declined to trust it; it fell back to a rebuild instead. The causes sharing this marker -- unreadable/corrupt/wrong-version/structurally invalid, a failed PostgreSQL watermark read, a changed watermark lineage, rows inserted behind the watermark, a counter that was behind the file's stamp at start (`the watermark counter was behind the file's stamp when this process started`: PostgreSQL went back, as a restored backup does), a buffered boot write whose counter contradicts the file (`boot write buffer contradicts the file: …`), an over-`BLOODTRAIL_MEMORY_LIMIT` size, a watermark gap the boot's own buffered writes could not cover, a fallback-shaped boot-time write, a poisoned or overflowed boot-write buffer, and a replay failure -- are distinguished by a `reason` attribute on all but the first, which carries `error` instead. The gap, fallback and overflow shapes are the legitimate outcome of boot-time writes the replay cannot account for (or, for an overflow, cannot hold within its caps), not a fault: the file cannot be proven complete and the rebuild that follows is correct. So is `watermark lineage changed since the file was written` (with `file_lineage` and `pg_lineage`) on the first boot after `bloodtrail install`, or against a different or reset database: something that does not advance the counter may have written the graph since the file was saved (see the watermark lineage in the top-level [README](../README.md#fast-restarts-the-snapshot-directory)). A version mismatch on the first boot after an upgrade that changes the file's format is expected too. `rows were inserted since the file was written by a writer that did not advance the watermark` (with `file_node_id_seq`/`start_node_id_seq` and their edge counterparts: where the sequences stood when the file was saved, and when this boot started) is the same finding for a file whose lineage still matched: an id sequence moved while the counter did not, so something outside BloodTrail -- the stock image, `psql` -- inserted rows after the file was saved. |
| `snapshot file invalidated` | Info | A write reached PostgreSQL without advancing the watermark counter (see `watermark bump failed` above), so the saved file was deleted: its stamp still matches what PostgreSQL reads, which would let a later boot declare a zero-sized gap and adopt a replica that is missing that write. Costs one slow boot (a full PostgreSQL rebuild) and nothing else. Also logged, with reason `booting from it panicked`, when the boot's attempt to start from the file panicked (`snapshot file boot panicked`, Error, with the stack) and the boot fell through to a rebuild. A save that was already writing its file when the bump failed deletes that file itself once it lands (`snapshot file not written`, Warn, reason `a watermark bump failed while the file was being written`). `snapshot file invalidation failed` (Warn) means the delete itself, or the directory sync that makes it durable, failed and a later boot may still adopt that file; `snapshot file not invalidated` (Warn) means there was no path to delete yet. |
| `no snapshot file` | Debug | Boot found `BLOODTRAIL_SNAPSHOT_DIR` set but no file there yet (the ordinary first-ever boot against a given directory). The feature being disabled outright (`BLOODTRAIL_SNAPSHOT_DIR` unset) logs nothing here at all -- boot returns from the check before it would ever log. |
| `compaction finished` | Info | A background compaction folded the write-through delta back into the base snapshot. |
| `path engine served` / `builder engine served` / `cypher engine served` | Info / Debug / Debug | The in-memory engine, not PostgreSQL, answered a shortest-path, structural (node/relationship), or Cypher query respectively. |

## First release checklist

The installer derives its image tag from the running upstream tag plus its own version
(`<upstream>-bt<version>`), so the version-suffixed images have to exist before a
released CLI can install anything. `release.yml` builds them itself, from the release
tag's own ref (`git describe` on it is what stamps the driver version), for every
supported upstream tag -- `build/upstream-tags.sh`, the same list `ci.yml`'s patch
guard checks: every stable upstream release from the v9.6.0 floor on -- and refuses
to publish the CLI until each of those exact tags is anonymously pullable. In order:

1. Set the GHCR package `bloodtrail` to public in its package settings.
2. Confirm an unauthenticated client can see it:
   `docker logout ghcr.io && docker manifest inspect ghcr.io/mihhailsokolov/bloodtrail:v9.6.0`.
3. Push the `vX.Y.Z` tag to trigger `release.yml`: it publishes the
   `<upstream>-btX.Y.Z` images, verifies an anonymous pull of each, and only then builds
   the CLI archives, `checksums.txt` and `install.sh`.
4. For an upstream release published AFTER the release tag -- which the release could
   not have known about -- dispatch the `image` workflow from the release tag
   (`gh workflow run image.yml --ref vX.Y.Z -f upstream_tag=v9.8.0`), once the patch
   guard shows the patch applies to it; until then the released CLI refuses to install
   on that upstream version rather than take the alias.
5. On a clean host with a BloodHound CE deployment, run the documented one-liner
   (`curl -fsSL …/install.sh | sh -s -- install`) end to end, including
   `bloodtrail rollback`.

## Merge gate

`ci.yml` ends in a `ci-ok` job and `e2e.yml` in an `e2e-ok` job. Each runs whatever the jobs
before it did and fails unless every job it needs succeeded, skipped and cancelled included.
Those two names are the status checks the default branch's ruleset should require: a ruleset
counts a skipped required job as passing, and a job is skipped when something it needs failed,
so requiring the underlying jobs by name would let a broken upstream list through. Pair them
with an up-to-date-branch requirement (or a merge queue; both workflows run on `merge_group`
too), and let the ruleset's bypass apply to pull requests only, so that merging past a red
check is a deliberate act on the pull request and a direct push to the branch is refused.
A job added to either workflow belongs in that workflow's gate `needs`.
