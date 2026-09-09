# Building the BloodTrail image

`build/build-image.sh <upstream-tag> [driver-version] [--push] [--platform …]` builds
BloodHound CE at the given upstream release tag with the BloodTrail driver compiled in.

The only source change to BloodHound is `patches/bloodhound-driver.patch`
(one file: `cmd/api/src/bootstrap/util.go`). `go.mod` is edited by the script with
`go mod edit`, so upstream dependency bumps never conflict with the patch.

Vendoring copies the driver's root Go files plus `internal/engine` -- the in-memory path
engine -- into `packages/go/bloodtrail`, which the upstream Dockerfile's builder stage already
copies wholesale (`COPY --parents go* cmd/api packages/go server ./`), so building the image
compiles the engine along with the driver. `internal/engine`'s own `*_test.go` files are
stripped before vendoring: they reference `internal/graphtest`, a test-only helper package that
is deliberately not vendored, and `go mod tidy` resolves test dependencies for every package
the main module transitively imports, vendored ones included. Everything else under
`internal/` -- the installer, its CLI, the verify fixtures -- is operator tooling with no
business inside the served image and stays out.

Images are tagged `ghcr.io/mihhailsokolov/bloodhound-bloodtrail:<tag>-bt<driver version>`
and `:<tag>`. The second is a moving alias for the newest driver build against that
upstream release; the installer falls back to it when the tag for its own version has
not been published. A leading `v` is stripped from the driver version, so the same
string is stamped into `Version` and used in the image tag.

Requires Go 1.26+, Docker with Buildx, and network access to GitHub.

Local example on Apple Silicon:

    ./build/build-image.sh v9.6.0 0.1.0-dev --platform linux/arm64

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
5. **Snapshot-file restart.** Enable `BLOODTRAIL_SNAPSHOT_DIR` with a bind-mounted host
   directory (so the file survives the container recreate the config change itself causes),
   then `docker compose restart` the same container -- no further config change, so the
   container is not recreated. `snapshot file written` at that shutdown is asserted
   unconditionally; the reboot is then required to land on exactly one of two outcomes,
   with anything else a failure:
   - **adopted** -- `snapshot file loaded` with **no** new `snapshot rebuilt` line: proof the
     file itself, not a rebuild that happened to produce the same answer, is what the reboot
     served from.
   - **superseded** -- `snapshot file rejected` because a write landed during the reboot
     (`reason` is `a write was applied while the file was loading`, or `watermark mismatch`
     with PostgreSQL's counter ahead of the file's), followed by a successful rebuild.
     BloodHound writes to the graph on every boot (it queues a full analysis request at
     startup and runs the data-pipe daemon with no start delay), so this outcome is common
     and correct -- the watermark protocol is refusing a file that is genuinely stale. See
     the snapshot-file section of the top-level [README](../README.md#write-through).

   Either way the boot must have read the file this phase's own shutdown wrote (compared by
   stamped watermark), and a rejection for a corrupt or wrong-version file, a failed
   watermark read, the memory limit, or no file attempt at all still fails. One more
   `GET /api/v2/graphs/shortest-path` confirms the engine answers correctly on both paths.

Requires the same tools as `build-image.sh`, plus `docker compose`, `curl` and `jq`. Run it
with `PLATFORM=linux/arm64 ./build/e2e.sh` on Apple Silicon to avoid amd64 emulation.

### Log markers

The `bloodhound` container's logs carry BloodTrail's own markers, all prefixed
`bloodtrail:`. The Debug-level ones below appear only under `BLOODTRAIL_LOG_LEVEL=debug`
(which `e2e.sh` sets from the stack's first boot); everything at Info or above is logged
regardless.

| Marker | Level | Meaning |
| --- | --- | --- |
| `write-through applied` | Debug | A committed write was replayed into the in-memory replica with no rebuild. |
| `fallback entered` / `fallback exited` | Warn / Info | A write could not be replayed narrowly; every query declines to PostgreSQL until the recovery rebuild below adopts a fresh snapshot. |
| `snapshot rebuilt` | Info | A full PostgreSQL rebuild ran and was adopted -- `trigger` names why: `startup` (the one-shot boot load), `fallback` (recovery from the line above), or `manual`. |
| `snapshot file written` | Info | The current replica was folded and written to `BLOODTRAIL_SNAPSHOT_DIR` -- by a clean shutdown, or by a background compaction once it had adopted its result. |
| `snapshot file loaded` | Info | Boot trusted and loaded that file instead of rebuilding from PostgreSQL. |
| `snapshot file rejected` | Info | Boot found a file but declined to trust it; it fell back to a rebuild instead. Five distinct causes share this marker -- unreadable/corrupt/wrong-version, a failed PostgreSQL watermark read, a watermark mismatch, an over-`BLOODTRAIL_MEMORY_LIMIT` size, and a write racing the load -- distinguished by a `reason` attribute on all but the first, which carries `error` instead. The last two are the ordinary outcome of a write landing during the boot (BloodHound's startup analysis produces one on every boot) and are not a fault: the file is genuinely stale and the rebuild that follows is correct. |
| `no snapshot file` | Debug | Boot found `BLOODTRAIL_SNAPSHOT_DIR` set but no file there yet (the ordinary first-ever boot against a given directory). The feature being disabled outright (`BLOODTRAIL_SNAPSHOT_DIR` unset) logs nothing here at all -- boot returns from the check before it would ever log. |
| `compaction finished` | Info | A background compaction folded the write-through delta back into the base snapshot. |
| `path engine served` / `builder engine served` / `cypher engine served` | Info / Debug / Debug | The in-memory engine, not PostgreSQL, answered a shortest-path, structural (node/relationship), or Cypher query respectively. |

## First release checklist

The installer derives its image from the running upstream tag, so the images have to be
published before a CLI release is of any use. In order:

1. For each supported upstream tag, dispatch the `image` workflow from the release tag
   (`gh workflow run image.yml --ref vX.Y.Z -f upstream_tag=v9.6.0`), so the built image
   carries the same driver version the released CLI derives.
2. Set the GHCR package `bloodhound-bloodtrail` to public in its package settings.
   Until then anonymous pulls fail and the installer reports the image as missing.
3. Confirm an unauthenticated client can see it:
   `docker logout ghcr.io && docker manifest inspect ghcr.io/mihhailsokolov/bloodhound-bloodtrail:v9.6.0`.
4. Push the `vX.Y.Z` tag to trigger `release.yml`, which builds the CLI archives,
   `checksums.txt` and `install.sh`.
5. On a clean host with a BloodHound CE deployment, run the documented one-liner
   (`curl -fsSL …/install.sh | sh -s -- install`) end to end, including
   `bloodtrail rollback`.
