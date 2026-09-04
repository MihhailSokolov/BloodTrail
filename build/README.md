# Building the BloodTrail image

`build/build-image.sh <upstream-tag> [driver-version] [--push] [--platform …]` builds
BloodHound CE at the given upstream release tag with the BloodTrail driver compiled in.

The only source change to BloodHound is `patches/bloodhound-driver.patch`
(one file: `cmd/api/src/bootstrap/util.go`). `go.mod` is edited by the script with
`go mod edit`, so upstream dependency bumps never conflict with the patch.

Images are tagged `ghcr.io/mihhailsokolov/bloodhound-bloodtrail:<tag>-bt<driver version>`
and `:<tag>`. The second is a moving alias for the newest driver build against that
upstream release; the installer falls back to it when the tag for its own version has
not been published. A leading `v` is stripped from the driver version, so the same
string is stamped into `Version` and used in the image tag.

Requires Go 1.26+, Docker with Buildx, and network access to GitHub.

Local example on Apple Silicon:

    ./build/build-image.sh v9.6.0 0.1.0-dev --platform linux/arm64

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
