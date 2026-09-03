#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# Downloads the bloodtrail binary for this platform from GitHub Releases,
# verifies its checksum, and runs it with the given arguments.
set -eu

REPO="MihhailSokolov/BloodTrail"
VERSION="${BLOODTRAIL_VERSION:-latest}"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) echo "unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
case "$OS" in linux|darwin) ;; *) echo "unsupported OS: $OS" >&2; exit 1 ;; esac

if [ "$VERSION" = "latest" ]; then
  BASE="https://github.com/$REPO/releases/latest/download"
else
  BASE="https://github.com/$REPO/releases/download/$VERSION"
fi
ASSET="bloodtrail_${OS}_${ARCH}.tar.gz"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "Downloading $ASSET from $BASE" >&2
curl -fsSL "$BASE/$ASSET" -o "$TMP/$ASSET"
curl -fsSL "$BASE/checksums.txt" -o "$TMP/checksums.txt"
(cd "$TMP" && grep " $ASSET\$" checksums.txt | sha256sum -c - >/dev/null) || { echo "checksum verification failed" >&2; exit 1; }
tar -xzf "$TMP/$ASSET" -C "$TMP"
exec "$TMP/bloodtrail" "$@"
