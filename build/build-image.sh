#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build a BloodHound CE image with the BloodTrail driver compiled in.
#
#   build/build-image.sh <upstream-tag> [driver-version] [--push] [--platform linux/amd64,linux/arm64]
#
# --platform defaults to the Docker daemon's own platform for a local build, and
# to linux/amd64 for a --push.
#
# Steps: shallow-clone upstream at the tag, apply patches/bloodhound-driver.patch,
# vendor the driver source under packages/go/bloodtrail (the upstream Dockerfile
# copies packages/go into the builder stage), point go.mod at it with a replace
# directive, stamp the driver version, and run the upstream Dockerfile.
set -euo pipefail

usage() { sed -n '2,12p' "$0"; exit 2; }

TAG="${1:-}"; [[ -n "$TAG" ]] || usage; shift
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

DRIVER_VERSION="${1:-}"
# `git describe` has to run inside this repository: the working directory when
# the script is invoked is not necessarily it.
if [[ -n "$DRIVER_VERSION" && "$DRIVER_VERSION" != --* ]]; then shift; else DRIVER_VERSION="$(git -C "$REPO_ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)"; fi
# The image tag and the version stamped into the driver are the same string,
# without the "v" a git tag carries.
DRIVER_VERSION="${DRIVER_VERSION#v}"
PUSH=""; PLATFORM=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --push) PUSH="--push"; shift ;;
    --platform) PLATFORM="$2"; shift 2 ;;
    *) usage ;;
  esac
done

# With no --platform, a LOCAL build targets the Docker daemon's own platform.
# It used to default to linux/amd64 unconditionally, which on an Apple Silicon
# host produces an image Docker runs under x86 emulation -- quietly, since it
# still works. The engine is CPU-bound in a way BloodHound's HTTP layer is not,
# so emulation hit it far harder than the stock image it gets compared with:
# benchmarked that way, a full scan measured 775ms against 122ms native and
# shortest-path queries 5x slower, and every BloodTrail number in a whole
# benchmark report had to be thrown away.
#
# A push keeps the old default: what gets published should not depend on
# which machine happened to run the script. CI passes --platform explicitly.
if [[ -z "$PLATFORM" ]]; then
  if [[ -n "$PUSH" ]]; then
    PLATFORM="linux/amd64"
  else
    PLATFORM="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}' 2>/dev/null || echo linux/amd64)"
  fi
fi
echo "==> Target platform $PLATFORM"

WORK="$REPO_ROOT/.build/upstream-$TAG"
IMAGE_REPO="${IMAGE_REPO:-ghcr.io/mihhailsokolov/bloodtrail}"
IMAGE="$IMAGE_REPO:$TAG-bt$DRIVER_VERSION"
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
# The in-memory path engine lives under internal/engine; everything else
# under internal/ (the installer, its own CLI, verify fixtures, ...) is
# operator tooling that has no business inside the served image.
mkdir -p "$WORK/$VENDOR_DIR/internal"
cp -R "$REPO_ROOT/internal/engine" "$WORK/$VENDOR_DIR/internal/engine"
# Test files reference test-only helper packages (internal/graphtest, not
# vendored) that `go mod tidy` below would otherwise try to resolve, since it
# also records go.sum entries for the test dependencies of every package the
# main module imports -- transitively, this vendored tree included.
find "$WORK/$VENDOR_DIR/internal/engine" -name '*_test.go' -delete
sed -i.bak "s|^var Version = \"dev\"|var Version = \"$DRIVER_VERSION\"|" "$WORK/$VENDOR_DIR/driver.go" && rm -f "$WORK/$VENDOR_DIR/driver.go.bak"
grep -q "Version = \"$DRIVER_VERSION\"" "$WORK/$VENDOR_DIR/driver.go"

echo "==> Wiring go.mod"
(cd "$WORK" && go mod edit -require="$MODULE@v0.0.0" -replace="$MODULE=./$VENDOR_DIR" && go mod tidy)
(cd "$WORK" && go build ./cmd/api/src/cmd/bhapi)   # fail fast before the long Docker build
rm -f "$WORK/bhapi"

echo "==> Building $IMAGE"
# No --build-arg version: the v9.6.0 Dockerfile declares no such ARG, and
# buildx warns about (and ignores) one that is not declared.
docker buildx build \
  -f "$WORK/dockerfiles/bloodhound.Dockerfile" \
  --platform "$PLATFORM" \
  -t "$IMAGE" -t "$ALIAS" \
  ${PUSH:---load} \
  "$WORK"

echo "==> Built $IMAGE (alias $ALIAS)"
