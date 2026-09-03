# Building the BloodTrail image

`build/build-image.sh <upstream-tag> [driver-version] [--push] [--platform …]` builds
BloodHound CE at the given upstream release tag with the BloodTrail driver compiled in.

The only source change to BloodHound is `patches/bloodhound-driver.patch`
(one file: `cmd/api/src/bootstrap/util.go`). `go.mod` is edited by the script with
`go mod edit`, so upstream dependency bumps never conflict with the patch.

Images are tagged `ghcr.io/mihhailsokolov/bloodhound-bloodtrail:<tag>-bt<driver version>`
and `:<tag>`. Requires Go 1.26+, Docker with Buildx, and network access to GitHub.

Local example on Apple Silicon:

    ./build/build-image.sh v9.6.0 0.1.0-dev --platform linux/arm64
