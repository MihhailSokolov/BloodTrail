#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
#
# Prints, as a compact JSON array, every stable BloodHound CE release tag at or
# above the supported floor -- the one list the patch guard (ci.yml), the
# release images (release.yml) and the weekly image build (image.yml) all use.
#
# It is derived from upstream's own releases rather than written down because
# a written-down list goes stale silently: v9.7.1 shipped while the list said
# [v9.6.0, v9.7.0], and a released CLI refuses to install on an upstream tag
# it has no image for. A new upstream release now enters the patch guard on
# the next CI run and the images on the next release by itself. Dropping an
# old upstream release is the deliberate act of raising the floor.
#
# Usage: build/upstream-tags.sh [floor]   (needs gh, authenticated, and jq)
set -eu

FLOOR="${1:-v9.6.0}"

tags=$(gh release list -R SpecterOps/BloodHound --exclude-pre-releases --limit 200 \
  --json tagName --jq '.[].tagName' |
  grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' |
  while read -r tag; do
    # At or above the floor: the floor sorts first (or is the tag itself).
    if [ "$(printf '%s\n%s\n' "$FLOOR" "$tag" | sort -V | head -n 1)" = "$FLOOR" ]; then
      echo "$tag"
    fi
  done | sort -V)

# An empty list would turn every matrix into a silent no-op -- a release with
# no images, a patch guard that checks nothing. Fail instead.
if [ -z "$tags" ]; then
  echo "no stable upstream release at or above $FLOOR (is gh authenticated?)" >&2
  exit 1
fi

printf '%s\n' "$tags" | jq -R . | jq -cs .
