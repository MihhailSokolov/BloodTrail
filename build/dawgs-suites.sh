#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Runs the unit and integration suites against every dawgs version the supported
# upstream releases resolve to, other than the one this repository is built
# against (which ci.yml's test job already covers).
#
#   build/dawgs-suites.sh '["v9.6.0","v9.7.0","v9.7.1"]'     (build/upstream-tags.sh's output)
#
# An image ships whatever dawgs build/build-image.sh resolves for its upstream
# release: upstream's own pin, or the driver's when that is higher. v9.7.1 pins
# v0.8.1 while go.mod says v0.8.0, so its image shipped a dawgs no suite had ever
# run against. build-image.sh --dawgs-only fails here, and so does this script,
# for a release that resolves a dawgs that is neither go.mod's nor listed in that
# script's dawgs_tested_versions (a new upstream release pinning a dawgs nobody
# has tried), or whose resolved dawgs differs from its own pin without being
# listed in dawgs_shift_reason. What it accepts, it runs the suites against.
#
# Needs jq and Go, network access to GitHub and the Go module proxy, and
# BLOODTRAIL_TEST_PG for the integration suite. It edits go.mod and go.sum while
# it runs and restores them on the way out.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
DAWGS="github.com/specterops/dawgs"
TAGS="${1:?usage: build/dawgs-suites.sh '<JSON array of upstream tags>'}"

OWN="$(go list -m -f '{{.Version}}' "$DAWGS")"
echo "==> this repository is built against dawgs $OWN"

# What each supported release resolves to, and the distinct versions other than OWN.
extra=""
for tag in $(printf '%s' "$TAGS" | jq -r '.[]'); do
  resolved="$(./build/build-image.sh "$tag" --dawgs-only)"
  echo "==> $tag resolves dawgs $resolved"
  if [[ "$resolved" != "$OWN" && " $extra " != *" $resolved "* ]]; then extra="$extra $resolved"; fi
done

if [[ -z "$extra" ]]; then
  echo "==> every supported release resolves dawgs $OWN; the test job's suites already cover it"
  exit 0
fi

trap 'git checkout -- go.mod go.sum' EXIT
for version in $extra; do
  echo "==> the unit and integration suites against dawgs $version"
  go get "$DAWGS@$version"
  go test -race ./...
  go test -race -tags integration -count=1 -p 1 ./...
  git checkout -- go.mod go.sum
done
echo "==> the suites pass against dawgs$extra"
