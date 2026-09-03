#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build a BloodHound CE image with the BloodTrail driver compiled in.
#
#   build/build-image.sh <upstream-tag> [driver-version] [--push] [--platform linux/amd64,linux/arm64]
#
# Steps: shallow-clone upstream at the tag, apply patches/bloodhound-driver.patch,
# vendor the driver source under packages/go/bloodtrail (the upstream Dockerfile
# copies packages/go into the builder stage), point go.mod at it with a replace
# directive, stamp the driver version, and run the upstream Dockerfile.
set -euo pipefail

usage() { sed -n '2,12p' "$0"; exit 2; }

TAG="${1:-}"; [[ -n "$TAG" ]] || usage; shift
DRIVER_VERSION="${1:-}"
if [[ -n "$DRIVER_VERSION" && "$DRIVER_VERSION" != --* ]]; then shift; else DRIVER_VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)"; fi
PUSH=""; PLATFORM="linux/amd64"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --push) PUSH="--push"; shift ;;
    --platform) PLATFORM="$2"; shift 2 ;;
    *) usage ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$REPO_ROOT/.build/upstream-$TAG"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/mihhailsokolov/bloodhound-bloodtrail}"
IMAGE="$IMAGE_REPO:$TAG-bt${DRIVER_VERSION#v}"
ALIAS="$IMAGE_REPO:$TAG"
MODULE="github.com/MihhailSokolov/BloodTrail"
VENDOR_DIR="packages/go/bloodtrail"

echo "==> Upstream checkout $TAG"
if [[ ! -d "$WORK/.git" ]]; then
  git clone --depth 1 --branch "$TAG" https://github.com/SpecterOps/BloodHound.git "$WORK"
fi
git -C "$WORK" checkout HEAD -- . && git -C "$WORK" clean -fdq

echo "==> Applying patch"
git -C "$WORK" apply --check "$REPO_ROOT/patches/bloodhound-driver.patch"
git -C "$WORK" apply --3way "$REPO_ROOT/patches/bloodhound-driver.patch"

echo "==> Vendoring driver source into $VENDOR_DIR"
rm -rf "$WORK/$VENDOR_DIR" && mkdir -p "$WORK/$VENDOR_DIR"
cp "$REPO_ROOT"/*.go "$REPO_ROOT/go.mod" "$REPO_ROOT/go.sum" "$WORK/$VENDOR_DIR/"
rm -f "$WORK/$VENDOR_DIR"/*_test.go
sed -i.bak "s|^var Version = \"dev\"|var Version = \"$DRIVER_VERSION\"|" "$WORK/$VENDOR_DIR/driver.go" && rm -f "$WORK/$VENDOR_DIR/driver.go.bak"
grep -q "Version = \"$DRIVER_VERSION\"" "$WORK/$VENDOR_DIR/driver.go"

echo "==> Wiring go.mod"
(cd "$WORK" && go mod edit -require="$MODULE@v0.0.0" -replace="$MODULE=./$VENDOR_DIR" && go mod tidy)
(cd "$WORK" && go build ./cmd/api/src/cmd/bhapi)   # fail fast before the long Docker build
rm -f "$WORK/bhapi"

echo "==> Building $IMAGE"
docker buildx build \
  -f "$WORK/dockerfiles/bloodhound.Dockerfile" \
  --platform "$PLATFORM" \
  --build-arg "version=$TAG-bt${DRIVER_VERSION#v}" \
  -t "$IMAGE" -t "$ALIAS" \
  ${PUSH:---load} \
  "$WORK"

echo "==> Built $IMAGE (alias $ALIAS)"
