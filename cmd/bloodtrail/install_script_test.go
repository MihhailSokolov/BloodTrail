// SPDX-License-Identifier: Apache-2.0

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// installScriptRun is one run of scripts/install.sh against a release that
// holds a stand-in bloodtrail binary and the checksums.txt a test gives it.
type installScriptRun struct {
	stderr string
	err    error
	ran    bool     // whether the downloaded binary was started
	args   []string // what it was started with
}

// installScriptAsset is the archive name the script asks for on this host.
func installScriptAsset(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("install.sh supports linux and darwin, not %s", runtime.GOOS)
	}
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		t.Skipf("install.sh supports amd64 and arm64, not %s", runtime.GOARCH)
	}
	return "bloodtrail_" + runtime.GOOS + "_" + runtime.GOARCH + ".tar.gz"
}

// installScriptArchive builds the release archive: a bloodtrail binary that
// only records that it ran, and with what.
func installScriptArchive(t *testing.T) []byte {
	t.Helper()
	body := []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$BLOODTRAIL_TEST_MARKER\"\n")
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "bloodtrail", Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// installScriptSHA256 is the checksum line a release publishes for archive.
func installScriptSHA256(archive []byte) string {
	sum := sha256.Sum256(archive)
	return hex.EncodeToString(sum[:])
}

// runInstallScript runs scripts/install.sh with a fake curl that serves the
// archive and checksums, and a sha256sum that behaves like the one macOS 14
// and later ship in /sbin: with -c it exits 0 for input it cannot use -- none
// at all, or a line that is not a checksum -- and only fails for a line it can
// read, which it then really checks.
func runInstallScript(t *testing.T, archive []byte, checksums string, args ...string) installScriptRun {
	t.Helper()
	asset := installScriptAsset(t)

	hasher := ""
	if p, err := exec.LookPath("sha256sum"); err == nil {
		hasher = p
	} else if p, err := exec.LookPath("shasum"); err == nil {
		hasher = p + " -a 256"
	} else {
		t.Skip("neither sha256sum nor shasum is available to stand behind the fake")
	}

	dir := t.TempDir()
	release := filepath.Join(dir, "release")
	bin := filepath.Join(dir, "bin")
	for _, d := range []string{release, bin, filepath.Join(dir, "tmp")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range map[string][]byte{asset: archive, "checksums.txt": []byte(checksums)} {
		if err := os.WriteFile(filepath.Join(release, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	curl := `#!/bin/sh
# Serves the files of the fake release by the last segment of the URL.
out=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out="$2"; shift ;;
    http*) url="$1" ;;
  esac
  shift
done
cp "$BLOODTRAIL_TEST_RELEASE/${url##*/}" "$out"
`
	sha256sum := `#!/bin/sh
HASHER="` + hasher + `"
if [ "$1" = "-c" ]; then
  rc=0
  while IFS= read -r line; do
    hash="${line%% *}"
    name="${line#* }"
    name="${name# }"
    name="${name#\*}"
    case "$hash" in *[!0-9a-fA-F]*|"") continue ;; esac
    [ "${#hash}" -eq 64 ] && [ -n "$name" ] || continue
    actual="$($HASHER "$name" 2>/dev/null)" || { echo "$name: FAILED open or read" >&2; rc=1; continue; }
    [ "$(echo "${actual%% *}" | tr A-F a-f)" = "$(echo "$hash" | tr A-F a-f)" ] || { echo "$name: FAILED" >&2; rc=1; }
  done
  exit $rc
fi
exec $HASHER "$@"
`
	for name, script := range map[string]string{"curl": curl, "sha256sum": sha256sum} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	marker := filepath.Join(dir, "marker")
	cmd := exec.Command("sh", filepath.Join("..", "..", "scripts", "install.sh"))
	cmd.Args = append(cmd.Args, args...)
	cmd.Env = []string{
		"PATH=" + bin + ":/usr/bin:/bin",
		"TMPDIR=" + filepath.Join(dir, "tmp"),
		"BLOODTRAIL_VERSION=v0.0.0-test",
		"BLOODTRAIL_TEST_RELEASE=" + release,
		"BLOODTRAIL_TEST_MARKER=" + marker,
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()

	run := installScriptRun{stderr: stderr.String(), err: err}
	if data, readErr := os.ReadFile(marker); readErr == nil {
		run.ran = true
		run.args = strings.Fields(string(data))
	}
	return run
}

// TestInstallScriptRunsTheBinaryOnlyWhenItsChecksumIsPublished pins the
// checksum step of scripts/install.sh. The step used to hand whatever
// `grep` found to `sha256sum -c -`, and the sha256sum that macOS 14 and later
// ship exits 0 for input it cannot use: a checksums.txt without a line for
// the archive, or with one that is not a checksum, made the unverified
// download run as if it had been checked. It now needs exactly one line for
// the archive, with a SHA-256 in it, and compares that against the archive
// itself.
func TestInstallScriptRunsTheBinaryOnlyWhenItsChecksumIsPublished(t *testing.T) {
	asset := installScriptAsset(t)
	archive := installScriptArchive(t)
	sum := installScriptSHA256(archive)
	other := strings.Repeat("ab", 32)

	cases := []struct {
		name      string
		checksums string
		wantRun   bool
		wantError string // what stderr says when the script stops
	}{
		{"its own line", sum + "  " + asset + "\n", true, ""},
		{"a line among others", other + "  bloodtrail_other.tar.gz\n" + sum + "  " + asset + "\n" + other + "  extra.txt\n", true, ""},
		{"a binary-mode line", sum + " *" + asset + "\n", true, ""},
		{"a hash in capitals", strings.ToUpper(sum) + "  " + asset + "\n", true, ""},
		{"another archive's hash", other + "  " + asset + "\n", false, "checksum verification failed"},
		{"no line for it", other + "  bloodtrail_other.tar.gz\n", false, "no checksum for " + asset},
		{"an empty file", "", false, "no checksum for " + asset},
		{"a line for a longer name only", sum + "  " + asset + ".sig\n", false, "no checksum for " + asset},
		{"two lines for it", sum + "  " + asset + "\n" + sum + "  " + asset + "\n", false, "more than one checksum for " + asset},
		{"a line that is not a checksum", "notahash  " + asset + "\n", false, "is not a SHA-256 checksum"},
		{"a checksum one digit short", sum[:63] + "  " + asset + "\n", false, "is not a SHA-256 checksum"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			run := runInstallScript(t, archive, c.checksums, "install", "--yes")
			if c.wantRun {
				if run.err != nil || !run.ran {
					t.Fatalf("the script did not run the binary (err %v, ran %v):\n%s", run.err, run.ran, run.stderr)
				}
				if got := strings.Join(run.args, " "); got != "install --yes" {
					t.Fatalf("the binary got %q, want the script's own arguments", got)
				}
				return
			}
			if run.ran {
				t.Fatalf("the script ran a download it had not verified:\n%s", run.stderr)
			}
			if run.err == nil {
				t.Fatalf("the script reported success without running anything:\n%s", run.stderr)
			}
			if !strings.Contains(run.stderr, c.wantError) {
				t.Fatalf("stderr does not say %q:\n%s", c.wantError, run.stderr)
			}
		})
	}
}
