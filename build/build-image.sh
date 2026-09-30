#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Build a BloodHound CE image with the BloodTrail driver compiled in.
#
#   build/build-image.sh <upstream-tag> [driver-version] [--push] [--platform linux/amd64,linux/arm64] [--dawgs-only]
#
# --platform defaults to the Docker daemon's own platform for a local build, and
# to linux/amd64 for a --push. --dawgs-only stops once the dawgs version the
# image would ship has been checked (see dawgs_shift_reason below) and prints
# that version, alone, on stdout; nothing is built.
#
# Steps: shallow-clone upstream at the tag, apply patches/bloodhound-driver.patch,
# vendor the driver source under packages/go/bloodtrail (the upstream Dockerfile
# copies packages/go into the builder stage), point go.mod at it with a replace
# directive, check the dawgs version, stamp the driver version, and run the
# upstream Dockerfile.
set -euo pipefail

usage() { sed -n '2,11p' "$0"; exit 2; }

TAG="${1:-}"; [[ -n "$TAG" ]] || usage; shift
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

DRIVER_VERSION="${1:-}"
# `git describe` has to run inside this repository: the working directory when
# the script is invoked is not necessarily it.
if [[ -n "$DRIVER_VERSION" && "$DRIVER_VERSION" != --* ]]; then shift; else DRIVER_VERSION="$(git -C "$REPO_ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)"; fi
# The image tag and the version stamped into the driver are the same string,
# without the "v" a git tag carries.
DRIVER_VERSION="${DRIVER_VERSION#v}"
PUSH=""; PLATFORM=""; DAWGS_ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --push) PUSH="--push"; shift ;;
    --platform) PLATFORM="$2"; shift 2 ;;
    --dawgs-only) DAWGS_ONLY="1"; shift ;;
    *) usage ;;
  esac
done
# --dawgs-only prints one thing on stdout, the resolved version; the progress
# lines and everything the tools write go to stderr instead (fd 3 is the real
# stdout).
if [[ -n "$DAWGS_ONLY" ]]; then exec 3>&1 1>&2; fi

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
# A --dawgs-only run builds nothing, so it does not ask Docker at all.
if [[ -z "$PLATFORM" ]]; then
  if [[ -n "$PUSH" || -n "$DAWGS_ONLY" ]]; then
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
DAWGS="github.com/specterops/dawgs"

# `go mod tidy` below resolves the module graph of upstream's go.mod together
# with the vendored driver's, and minimum version selection takes the HIGHER
# dawgs of the two. An upstream release that pins an older dawgs than the driver
# was built against therefore ships a dawgs that release's own tests never ran
# with, and nothing said so: v9.6.0 pins v0.7.0 and its image carried v0.8.0.
# The build now names both versions and stops unless the exact pair is listed in
# dawgs_shift_reason below, with the reason it is acceptable. Where upstream pins
# the same version as the image resolves (v9.7.0 and v9.7.1 today) nothing needs
# listing there, but the version must still be one the suites have run against:
# see dawgs_tested_versions.
#
# Argument: "<upstream tag> <pinned by upstream> <resolved>". Prints the reason,
# or nothing when the pair is not listed.
dawgs_shift_reason() {
  case "$1" in
    "v9.6.0 v0.7.0 v0.8.0")
      echo "the driver's engine calls translate.TranslateWithOptions, translate.Options, translate.OptimizerEnabled and pg.OptimizedTranslationEnabled, which first exist in v0.8.0 (it does not compile against v0.7.0), and build/e2e.sh v9.6.0 validates exactly this pairing" ;;
  esac
}

# The dawgs versions the unit and integration suites have been run against, on
# top of the one this repository's own go.mod names (which ci.yml's test job runs
# them against on every change and which is always accepted). An image is built
# only on one of these. The release workflow and the weekly alias build run this
# script for every supported upstream release and run no suite themselves, so
# without this list a new release that pins a dawgs nobody has tried (a v9.8.0
# pinning v0.9.0, say) would publish an image before any suite ran on it, which
# is how v9.7.1's image shipped v0.8.1 untested. It fails here instead, and so
# does ci.yml's dawgs job, which resolves every supported release the same way,
# until the version is listed. List it in the change whose dawgs job then runs the
# suites against it (for every supported release that resolves to it), and merge
# that only if they pass.
#
#   v0.8.0  what go.mod names; ci.yml's test job.
#   v0.8.1  ci.yml's dawgs job, for the releases that pin it (v9.7.1). Checked when
#           it was listed: the unit and integration suites pass unchanged under
#           it (4,191 runs, 0 failures); it only changes the PostgreSQL edge schema.
dawgs_tested_versions() { echo "v0.8.0 v0.8.1"; }

# check_dawgs fails the build unless UPSTREAM_DAWGS and RESOLVED_DAWGS are equal or
# the pair is listed in dawgs_shift_reason, and RESOLVED_DAWGS is this repository's
# own dawgs or one of dawgs_tested_versions.
check_dawgs() {
  local reason own
  echo "==> dawgs: upstream $TAG pins ${UPSTREAM_DAWGS:-nothing}; the image resolves ${RESOLVED_DAWGS:-nothing}"
  if [[ -z "$UPSTREAM_DAWGS" || -z "$RESOLVED_DAWGS" ]]; then
    echo "error: could not read the version of $DAWGS from $TAG's go.mod before and after the driver is wired in; refusing to guess" >&2
    exit 1
  fi
  if [[ "$UPSTREAM_DAWGS" != "$RESOLVED_DAWGS" ]]; then
    reason="$(dawgs_shift_reason "$TAG $UPSTREAM_DAWGS $RESOLVED_DAWGS")"
    if [[ -z "$reason" ]]; then
      {
        echo "error: the image would ship dawgs $RESOLVED_DAWGS, but upstream $TAG pins $UPSTREAM_DAWGS."
        echo "  Minimum version selection took the higher of upstream's dawgs and the one the driver is built against,"
        echo "  so BloodHound would run on a library its own release never ran with. Run the unit and integration"
        echo "  suites against $RESOLVED_DAWGS, then list \"$TAG $UPSTREAM_DAWGS $RESOLVED_DAWGS\" and the reason in"
        echo "  dawgs_shift_reason in build/build-image.sh."
      } >&2
      exit 1
    fi
    echo "    accepted, listed in build/build-image.sh: $reason"
  fi
  own="$(awk -v m="$DAWGS" '$1 == m { print $2; exit } $1 == "require" && $2 == m { print $3; exit }' "$REPO_ROOT/go.mod")"
  if [[ -z "$own" ]]; then
    echo "error: could not read the version of $DAWGS from $REPO_ROOT/go.mod; refusing to guess" >&2
    exit 1
  fi
  if [[ "$RESOLVED_DAWGS" != "$own" && " $(dawgs_tested_versions) " != *" $RESOLVED_DAWGS "* ]]; then
    {
      echo "error: the image would ship dawgs $RESOLVED_DAWGS (upstream $TAG pins $UPSTREAM_DAWGS), which the suites have never been run against."
      echo "  The unit and integration suites run against $own, the version go.mod names, and against the versions in"
      echo "  dawgs_tested_versions in build/build-image.sh ($(dawgs_tested_versions)). Nothing that builds an image runs a suite,"
      echo "  so a build on any other dawgs would ship a library BloodTrail's tests never saw. Add $RESOLVED_DAWGS to"
      echo "  dawgs_tested_versions in build/build-image.sh: ci.yml's dawgs job then runs the suites against it for every"
      echo "  supported release that resolves to it, and the change should merge only if they pass."
    } >&2
    exit 1
  fi
}

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
# What upstream itself pins, read before the edit below can change it.
# A failure leaves go's own explanation on stderr and check_dawgs refuses to go on.
UPSTREAM_DAWGS="$(cd "$WORK" && go list -m -f '{{.Version}}' "$DAWGS")" || UPSTREAM_DAWGS=""
(cd "$WORK" && go mod edit -require="$MODULE@v0.0.0" -replace="$MODULE=./$VENDOR_DIR" && go mod tidy)
RESOLVED_DAWGS="$(cd "$WORK" && go list -m -f '{{.Version}}' "$DAWGS")" || RESOLVED_DAWGS=""
check_dawgs
if [[ -n "$DAWGS_ONLY" ]]; then printf '%s\n' "$RESOLVED_DAWGS" >&3; exit 0; fi
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
