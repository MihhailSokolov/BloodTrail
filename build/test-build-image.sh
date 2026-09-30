#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Tests for build/build-image.sh's dawgs check and build/dawgs-suites.sh. Each
# script runs in a stand-in repository whose git, go and docker (and, for the
# second, build-image.sh) are shims that only log what they were asked to do, so
# nothing is cloned, compiled, tested or built.
#
#   build/test-build-image.sh
#
# BUILD_IMAGE and DAWGS_SUITES=path test other copies of the scripts.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
BUILD_IMAGE="${BUILD_IMAGE:-$HERE/build-image.sh}"
DAWGS_SUITES="${DAWGS_SUITES:-$HERE/dawgs-suites.sh}"
failures=0
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

ok() { echo "  ok   $1"; }
bad() { echo "  FAIL $1"; failures=$((failures + 1)); }
# expect DESCRIPTION CONDITION...: a passing command is a passing check.
expect() { local what="$1"; shift; if "$@"; then ok "$what"; else bad "$what"; fi; }
logged() { grep -q -- "$1" "$ROOT/log"; }       # the shims logged a command matching $1
not_logged() { ! logged "$1"; }
printed() { grep -q -- "$2" "$1"; }             # file $1 contains $2

# fake_repo prints a fresh stand-in repository root holding a copy of the script
# under test, the files build-image.sh vendors, and the shims.
fake_repo() {
  local root; root="$(mktemp -d "$tmp/repo.XXXXXX")"
  mkdir -p "$root/build" "$root/internal/engine" "$root/patches" "$root/shims" "$root/state"
  cp "$BUILD_IMAGE" "$root/build/build-image.sh"
  printf 'module github.com/MihhailSokolov/BloodTrail\n\ngo 1.26\n' > "$root/go.mod"
  : > "$root/go.sum"
  printf 'package bloodtrail\n\nvar Version = "dev"\n' > "$root/driver.go"
  printf 'package engine\n' > "$root/internal/engine/engine.go"
  : > "$root/patches/bloodhound-driver.patch"
  cat > "$root/shims/git" <<'SHIM'
#!/bin/sh
echo "git $*" >> "$FAKE_LOG"
if [ "$1" = clone ]; then
  for last; do :; done
  mkdir -p "$last/.git" "$last/dockerfiles"
  : > "$last/dockerfiles/bloodhound.Dockerfile"
  : > "$last/go.mod"
fi
exit 0
SHIM
  cat > "$root/shims/go" <<'SHIM'
#!/bin/sh
echo "go $*" >> "$FAKE_LOG"
case "$1 $2" in
  "mod edit") exit 0 ;;
  "mod tidy") : > "$FAKE_STATE/tidied"; exit 0 ;;
  "list -m")
    if [ -n "${FAKE_LIST_FAILS:-}" ]; then echo "go: module is not a known dependency" >&2; exit 1; fi
    if [ -e "$FAKE_STATE/tidied" ]; then printf '%s\n' "$FAKE_RESOLVED"; else printf '%s\n' "$FAKE_PIN"; fi
    exit 0 ;;
esac
if [ "$1" = build ]; then : > bhapi; fi
exit 0
SHIM
  cat > "$root/shims/docker" <<'SHIM'
#!/bin/sh
echo "docker $*" >> "$FAKE_LOG"
if [ "$1" = version ]; then echo linux/amd64; fi
exit 0
SHIM
  chmod +x "$root/shims/git" "$root/shims/go" "$root/shims/docker"
  echo "$root"
}

# run_build TAG PINNED RESOLVED [build-image.sh flags...] runs the script and
# leaves ROOT, CODE, and the files $ROOT/out (stdout), $ROOT/err and $ROOT/log.
run_build() {
  local tag="$1" pinned="$2" resolved="$3"; shift 3
  ROOT="$(fake_repo)"
  ( cd "$ROOT" && PATH="$ROOT/shims:$PATH" FAKE_LOG="$ROOT/log" FAKE_STATE="$ROOT/state" FAKE_PIN="$pinned" FAKE_RESOLVED="$resolved" \
      bash build/build-image.sh "$tag" 0.0.0-test "$@" > "$ROOT/out" 2> "$ROOT/err" )
  CODE=$?
  touch "$ROOT/log"
}

echo "build-image.sh, upstream pins what the image resolves (v9.7.1: v0.8.1)"
run_build v9.7.1 v0.8.1 v0.8.1
expect "the build goes ahead" test "$CODE" -eq 0
expect "it says what upstream pins and what the image resolves" printed "$ROOT/out" "upstream v9.7.1 pins v0.8.1; the image resolves v0.8.1"
expect "the Go build and the image build ran" logged "docker buildx build"

echo "build-image.sh, a listed shift (v9.6.0: v0.7.0 -> v0.8.0)"
run_build v9.6.0 v0.7.0 v0.8.0
expect "the build goes ahead" test "$CODE" -eq 0
expect "it says what upstream pins and what the image resolves" printed "$ROOT/out" "upstream v9.6.0 pins v0.7.0; the image resolves v0.8.0"
expect "it says the pair is listed and why" printed "$ROOT/out" "accepted, listed in build/build-image.sh: .*TranslateWithOptions"
expect "the image build ran" logged "docker buildx build"

echo "build-image.sh, an unlisted shift (v9.7.0: v0.8.0 -> v0.8.1)"
run_build v9.7.0 v0.8.0 v0.8.1
expect "the build fails" test "$CODE" -ne 0
expect "the error names the tag and both versions" printed "$ROOT/err" "would ship dawgs v0.8.1, but upstream v9.7.0 pins v0.8.0"
expect "the error says where to list it" printed "$ROOT/err" "dawgs_shift_reason in build/build-image.sh"
expect "no Go build ran" not_logged "go build"
expect "no image was built" not_logged "docker buildx build"

echo "build-image.sh, a listed tag with another pair (v9.6.0: v0.7.0 -> v0.8.1)"
run_build v9.6.0 v0.7.0 v0.8.1
expect "the build fails (the allow-list is per pair, not per tag)" test "$CODE" -ne 0

echo "build-image.sh, the version cannot be read"
ROOT="$(fake_repo)"
( cd "$ROOT" && PATH="$ROOT/shims:$PATH" FAKE_LOG="$ROOT/log" FAKE_STATE="$ROOT/state" FAKE_LIST_FAILS=1 \
    bash build/build-image.sh v9.7.1 0.0.0-test > "$ROOT/out" 2> "$ROOT/err" )
CODE=$?
expect "the build fails instead of guessing" test "$CODE" -ne 0
expect "the error says so" printed "$ROOT/err" "refusing to guess"

echo "build-image.sh --dawgs-only"
run_build v9.6.0 v0.7.0 v0.8.0 --dawgs-only
expect "a listed shift succeeds" test "$CODE" -eq 0
expect "stdout is the resolved version and nothing else" test "$(cat "$ROOT/out")" = "v0.8.0"
expect "the progress lines went to stderr" printed "$ROOT/err" "upstream v9.6.0 pins v0.7.0; the image resolves v0.8.0"
expect "no Go build ran" not_logged "go build"
expect "Docker was not asked" not_logged "docker"
run_build v9.7.0 v0.8.0 v0.8.1 --dawgs-only
expect "an unlisted shift fails" test "$CODE" -ne 0
expect "and prints no version" test ! -s "$ROOT/out"

# run_suites TAGS_JSON [NAME=value...] runs dawgs-suites.sh in a stand-in repository
# where build-image.sh prints $FAKE_RESOLVED_<tag, dots as underscores> (or fails
# when that is unset, as it does for a shift nobody listed) and `go list -m` says
# FAKE_OWN. It leaves ROOT and CODE, and $ROOT/out, $ROOT/err and $ROOT/log.
run_suites() {
  local tags="$1"; shift
  ROOT="$(mktemp -d "$tmp/repo.XXXXXX")"
  mkdir -p "$ROOT/build" "$ROOT/shims"
  cp "$DAWGS_SUITES" "$ROOT/build/dawgs-suites.sh"
  cat > "$ROOT/build/build-image.sh" <<'SHIM'
#!/bin/sh
echo "build-image.sh $*" >> "$FAKE_LOG"
var="FAKE_RESOLVED_$(echo "$1" | tr . _)"
eval "resolved=\${$var:-}"
if [ -z "$resolved" ]; then echo "error: the image would ship a dawgs nobody listed" >&2; exit 1; fi
echo "==> dawgs: upstream $1 pins something; the image resolves $resolved" >&2
echo "$resolved"
SHIM
  cat > "$ROOT/shims/go" <<'SHIM'
#!/bin/sh
echo "go $*" >> "$FAKE_LOG"
if [ "$1 $2" = "list -m" ]; then echo "$FAKE_OWN"; fi
if [ "$1" = test ] && [ -n "${FAKE_TEST_FAILS:-}" ]; then exit 1; fi
exit 0
SHIM
  cat > "$ROOT/shims/git" <<'SHIM'
#!/bin/sh
echo "git $*" >> "$FAKE_LOG"
exit 0
SHIM
  chmod +x "$ROOT/build/build-image.sh" "$ROOT/shims/go" "$ROOT/shims/git"
  ( cd "$ROOT" && env "$@" PATH="$ROOT/shims:$PATH" FAKE_LOG="$ROOT/log" FAKE_OWN=v0.8.0 \
      bash build/dawgs-suites.sh "$tags" > "$ROOT/out" 2> "$ROOT/err" )
  CODE=$?
  touch "$ROOT/log"
}
count() { grep -c -- "$1" "$ROOT/log" || true; }   # how many logged commands match $1

ALL_TAGS='["v9.6.0","v9.7.0","v9.7.1"]'

echo "dawgs-suites.sh, v9.6.0 and v9.7.0 resolve v0.8.0 (the repository's own), v9.7.1 resolves v0.8.1"
run_suites "$ALL_TAGS" FAKE_RESOLVED_v9_6_0=v0.8.0 FAKE_RESOLVED_v9_7_0=v0.8.0 FAKE_RESOLVED_v9_7_1=v0.8.1
expect "it succeeds" test "$CODE" -eq 0
expect "it asks build-image.sh for every release, with --dawgs-only" test "$(count '^build-image.sh v9\.[67]\.[01] --dawgs-only')" -eq 3
expect "it says what each release resolves" printed "$ROOT/out" "v9.7.1 resolves dawgs v0.8.1"
expect "it moves dawgs to v0.8.1, once" test "$(count '^go get github.com/specterops/dawgs@v0.8.1$')" -eq 1
expect "it runs the unit suite once" test "$(count '^go test -race ./...$')" -eq 1
expect "it runs the integration suite once" test "$(count '^go test -race -tags integration -count=1 -p 1 ./...$')" -eq 1
expect "it restores go.mod and go.sum" logged "^git checkout -- go.mod go.sum$"
expect "it never runs the suites against the repository's own version" test "$(count '^go get github.com/specterops/dawgs@v0.8.0')" -eq 0

echo "dawgs-suites.sh, every release resolves the repository's own dawgs"
run_suites "$ALL_TAGS" FAKE_RESOLVED_v9_6_0=v0.8.0 FAKE_RESOLVED_v9_7_0=v0.8.0 FAKE_RESOLVED_v9_7_1=v0.8.0
expect "it succeeds" test "$CODE" -eq 0
expect "it runs nothing" test "$(count '^go get')" -eq 0
expect "it says why" printed "$ROOT/out" "already cover it"

echo "dawgs-suites.sh, two other versions, one of them resolved by two releases"
run_suites "$ALL_TAGS" FAKE_RESOLVED_v9_6_0=v0.8.1 FAKE_RESOLVED_v9_7_0=v0.9.0 FAKE_RESOLVED_v9_7_1=v0.8.1
expect "it succeeds" test "$CODE" -eq 0
expect "each version is tried once" test "$(count '^go get github.com/specterops/dawgs@')" -eq 2
expect "v0.8.1 is one of them" logged "^go get github.com/specterops/dawgs@v0.8.1$"
expect "v0.9.0 is the other" logged "^go get github.com/specterops/dawgs@v0.9.0$"
expect "each is followed by both suites" test "$(count '^go test ')" -eq 4

echo "dawgs-suites.sh, a release resolves a dawgs build-image.sh refuses"
run_suites "$ALL_TAGS" FAKE_RESOLVED_v9_6_0=v0.8.0 FAKE_RESOLVED_v9_7_1=v0.8.1
expect "it fails" test "$CODE" -ne 0
expect "build-image.sh's message reaches the log" printed "$ROOT/err" "nobody listed"
expect "no suite ran" test "$(count '^go test ')" -eq 0

echo "dawgs-suites.sh, a suite fails"
run_suites "$ALL_TAGS" FAKE_RESOLVED_v9_6_0=v0.8.0 FAKE_RESOLVED_v9_7_0=v0.8.0 FAKE_RESOLVED_v9_7_1=v0.8.1 FAKE_TEST_FAILS=1
expect "it fails" test "$CODE" -ne 0
expect "go.mod and go.sum are restored all the same" logged "^git checkout -- go.mod go.sum$"

if [ "$failures" -ne 0 ]; then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
