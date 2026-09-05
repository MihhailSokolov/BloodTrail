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
example compose stack on Neo4j, and drives `cmd/bloodtrail` against it: install, verify,
roll back, confirm a second install refuses to migrate onto the graph the first one left in
PostgreSQL, clear that with `--replace-postgres-graph`, and roll back again. The install step
passes `--admin-password`, which runs an ingest-and-search smoke test against the fixture
`TESTLAB.LOCAL` domain (`internal/verify/fixture`) -- BloodTrail's own path engine phase then
runs against that same fixture, still installed, before rollback:

1. Wait up to 120s for the `bloodhound` container's logs to show `snapshot rebuilt` -- the
   engine has replicated the freshly analyzed graph into memory.
2. `GET /api/v2/graphs/shortest-path` between two fixture objects known to be connected
   (TESTLAB.LOCAL's built-in Administrator, RID 500, is a direct `MemberOf` member of Domain
   Admins, RID 512) and assert HTTP 200 with a non-empty `data.nodes`.
3. `POST /api/v2/graphs/cypher` with a pre-built-shaped `shortestPath` query anchored on the
   same two fixture objects, and assert HTTP 200.
4. Assert the `bloodhound` container's logs show more `path engine served` lines after those
   two calls than before -- the exit-criterion proof that the in-memory engine, not PostgreSQL,
   answered.

Requires the same tools as `build-image.sh`, plus `docker compose`, `curl` and `jq`. Run it
with `PLATFORM=linux/arm64 ./build/e2e.sh` on Apple Silicon to avoid amd64 emulation.

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
