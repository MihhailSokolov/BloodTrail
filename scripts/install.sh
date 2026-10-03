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
# The binary is run from here, so TMPDIR is the way out on a host whose /tmp is
# mounted noexec. It is named in the template because a bare `mktemp -d` is
# not the same everywhere: GNU's honours TMPDIR, but the one macOS ships uses
# its own per-user directory and ignores it.
TMP="$(mktemp -d "${TMPDIR:-/tmp}/bloodtrail.XXXXXXXX")"
trap 'rm -rf "$TMP"' EXIT

# curl has no time limit of its own: a connection that stalls (a proxy that
# accepts and never answers, a server that stops sending) would hold this
# script, run through `curl | sh`, forever with nothing said. Every download
# has a connect limit and a total one, the total sized for its file: ten
# minutes for the archive, so a slow link still finishes, and one for the few
# hundred bytes of checksums.txt. A download that fails or passes its limit
# (curl's exit status 28) stops the script here, before anything has run.
fetch() {
  url="$1"; out="$2"; max_time="$3"
  curl -fsSL --connect-timeout 15 --max-time "$max_time" "$url" -o "$out" || {
    echo "could not download $url (curl's own message is above; its exit status 28 means the download did not finish in ${max_time}s); check that this host can reach github.com, through its proxy if it needs one, and that BLOODTRAIL_VERSION, if you set it, names a published release, then run this again" >&2
    exit 1
  }
}

echo "Downloading $ASSET from $BASE" >&2
fetch "$BASE/$ASSET" "$TMP/$ASSET" 600
fetch "$BASE/checksums.txt" "$TMP/checksums.txt" 60

# checksums.txt has a "<sha256>  <file>" line per release asset. Take the one
# line for this asset and compare its hash against the download here, rather
# than piping whatever grep finds into `sha256sum -c -`: the sha256sum macOS 14
# and later ship in /sbin exits 0 for input it cannot use -- no line at all,
# or one that is not a checksum -- so a checksums.txt that lacked the entry
# would have passed as a verified download.
NL='
'
ENTRY="$(awk -v asset="$ASSET" 'NF == 2 && ($2 == asset || $2 == "*" asset)' "$TMP/checksums.txt")"
case "$ENTRY" in
  "") echo "checksums.txt has no checksum for $ASSET; refusing to run an unverified download" >&2; exit 1 ;;
  *"$NL"*) echo "checksums.txt has more than one checksum for $ASSET; refusing to run a download it cannot tell how to verify" >&2; exit 1 ;;
esac
EXPECTED="$(printf '%s\n' "$ENTRY" | awk '{ print tolower($1) }')"
case "$EXPECTED" in
  *[!0-9a-f]*) EXPECTED="" ;;
esac
if [ "${#EXPECTED}" -ne 64 ]; then
  echo "the entry for $ASSET in checksums.txt is not a SHA-256 checksum: $ENTRY" >&2; exit 1
fi
ACTUAL="$($SHA256 "$TMP/$ASSET")" || { echo "could not compute the checksum of $ASSET" >&2; exit 1; }
ACTUAL="$(printf '%s\n' "$ACTUAL" | awk '{ print tolower($1) }')"
[ "$ACTUAL" = "$EXPECTED" ] || { echo "checksum verification failed" >&2; exit 1; }
tar -xzf "$TMP/$ASSET" -C "$TMP"
set +e
"$TMP/bloodtrail" "$@"
rc=$?
set -e
# 126 is the shell saying it found the file and could not run it, which
# bloodtrail itself never exits with: most likely $TMP is on a noexec mount.
if [ "$rc" -eq 126 ]; then
  echo "could not run the downloaded bloodtrail from $TMP; if that filesystem is mounted noexec, set TMPDIR to a directory that allows running programs and run this again" >&2
fi
exit "$rc"
