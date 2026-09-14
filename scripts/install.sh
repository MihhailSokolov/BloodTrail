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

# GNU coreutils' sha256sum is the norm on Linux; stock macOS ships shasum
# instead, which accepts the same checksums.txt format with -a 256.
if command -v sha256sum >/dev/null 2>&1; then
  SHA256="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  SHA256="shasum -a 256"
else
  echo "neither sha256sum nor shasum is available to verify the download" >&2; exit 1
fi

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
(cd "$TMP" && grep -F " $ASSET" checksums.txt | $SHA256 -c - >/dev/null) || { echo "checksum verification failed" >&2; exit 1; }
tar -xzf "$TMP/$ASSET" -C "$TMP"
set +e
"$TMP/bloodtrail" "$@"
rc=$?
set -e
exit "$rc"
