// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

// requireEnvSwapped checks that the .env in dir now holds want and that it got
// there the way a write to a secrets file should: by replacing the file with a
// complete new one -- a different inode -- not by truncating and refilling the
// old one, which a crash (or a reader) can catch half done. The new file keeps
// the old one's mode and owner, and no scratch file is left beside it.
func requireEnvSwapped(t *testing.T, dir string, before os.FileInfo, want string) {
	t.Helper()
	path := filepath.Join(dir, ".env")
	got, err := os.ReadFile(path)
	if err != nil || string(got) != want {
		t.Fatalf(".env = %q (err %v), want %q", got, err, want)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) {
		t.Errorf(".env was rewritten in place (same file before and after), so a crash or a reader could see it half written")
	}
	if after.Mode().Perm() != before.Mode().Perm() {
		t.Errorf(".env mode changed from %v to %v", before.Mode().Perm(), after.Mode().Perm())
	}
	if uid, gid, ok := ownerOf(before); ok {
		if uid2, gid2, _ := ownerOf(after); uid2 != uid || gid2 != gid {
			t.Errorf(".env owner changed from %d:%d to %d:%d", uid, gid, uid2, gid2)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".env") && e.Name() != ".env" {
			t.Errorf("a scratch file was left behind: %s", e.Name())
		}
	}
}

// TestInstallReplacesTheEnvFileAtomically pins how the install writes .env:
// it holds the deployment's secrets, and the install used to truncate it and
// write it again in place, so an interruption in between left it empty or cut
// short.
func TestInstallReplacesTheEnvFileAtomically(t *testing.T) {
	dir, composeFile := setupProject(t)
	envPath := filepath.Join(dir, ".env")
	original := "POSTGRES_PASSWORD=not-a-real-secret\nBLOODHOUND_TAG=9.6.0\n"
	if err := os.WriteFile(envPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(envPath, 0o640); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(envPath)
	if err != nil {
		t.Fatal(err)
	}

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	fake := scriptPGInstall(&dockerx.FakeRunner{}, base, filepath.Join(dir, "docker-compose.bloodtrail.yml"), image)
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
	if err := Install(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, opts); err != nil {
		t.Fatalf("install failed: %v", err)
	}
	requireEnvSwapped(t, dir, before, original+"COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n")
}

// TestRollbackReplacesTheEnvFileAtomically is the same for rollback's write.
func TestRollbackReplacesTheEnvFileAtomically(t *testing.T) {
	dir, composeFile, fake, api := rollbackFixture(t)
	envPath := filepath.Join(dir, ".env")
	if err := os.WriteFile(envPath, []byte("POSTGRES_PASSWORD=not-a-real-secret\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(envPath, 0o640); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}},
		Options{ComposeFile: composeFile, Yes: true, APIURL: api.URL, VerifyTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	requireEnvSwapped(t, dir, before, "POSTGRES_PASSWORD=not-a-real-secret\nCOMPOSE_FILE=docker-compose.yml\n")
	if names := dirNames(t, dir); !slices.Contains(names, "docker-compose.yml") {
		t.Fatalf("unexpected directory contents: %v", names)
	}
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// writeEnvFixture writes an .env with the given contents and mode in a fresh
// directory and returns its path.
func writeEnvFixture(t *testing.T, contents string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

// skipAsRoot skips a test that relies on file modes being enforced.
func skipAsRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores file modes")
	}
}

func TestWriteEnvFileKeepsTheModeOfTheFileItReplaces(t *testing.T) {
	for _, mode := range []os.FileMode{0o600, 0o640, 0o644, 0o664, 0o660} {
		t.Run(mode.String(), func(t *testing.T) {
			path := writeEnvFixture(t, "OLD=1\n", mode)
			before, _ := os.Stat(path)
			if err := writeEnvFile(path, []byte("NEW=2\n")); err != nil {
				t.Fatal(err)
			}
			requireEnvSwapped(t, filepath.Dir(path), before, "NEW=2\n")
		})
	}
}

func TestWriteEnvFileCreatesAMissingFileTheWayWriteFileWould(t *testing.T) {
	dir := t.TempDir()
	// What the umask leaves of 0644: a file created here says.
	probe := filepath.Join(dir, "probe")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	want, _ := os.Stat(probe)
	_ = os.Remove(probe)

	path := filepath.Join(dir, ".env")
	if err := writeEnvFile(path, []byte("COMPOSE_FILE=a.yml\n")); err != nil {
		t.Fatal(err)
	}
	got, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Mode().Perm() != want.Mode().Perm() {
		t.Errorf("a new .env has mode %v, want %v", got.Mode().Perm(), want.Mode().Perm())
	}
	if names := dirNames(t, dir); len(names) != 1 || names[0] != ".env" {
		t.Errorf("directory holds %v, want just .env", names)
	}
}

// TestWriteEnvFileFollowsALinkAndKeepsIt covers an .env that is a symbolic
// link to a file kept elsewhere: renaming a new file over the link would
// replace the link with a plain file and leave the real one stale.
func TestWriteEnvFileFollowsALinkAndKeepsIt(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "secrets", "bloodhound.env")
	if err := os.MkdirAll(filepath.Dir(real), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(real, []byte("OLD=1\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(real, 0o640); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, ".env")
	if err := os.Symlink(filepath.Join("secrets", "bloodhound.env"), link); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}

	if err := writeEnvFile(link, []byte("NEW=2\n")); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(link); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf(".env is no longer a symbolic link (%v, %v)", info, err)
	}
	if got, _ := os.ReadFile(real); string(got) != "NEW=2\n" {
		t.Fatalf("the file behind the link holds %q", got)
	}
	if info, _ := os.Stat(real); info.Mode().Perm() != 0o640 {
		t.Fatalf("the file behind the link has mode %v, want 0640", info.Mode().Perm())
	}
	if names := dirNames(t, filepath.Dir(real)); len(names) != 1 {
		t.Fatalf("scratch files left beside the real file: %v", names)
	}
}

func TestWriteEnvFileRefusesWhatItCannotReplaceSafely(t *testing.T) {
	skipAsRoot(t)
	t.Run("a file this user cannot write", func(t *testing.T) {
		path := writeEnvFixture(t, "OLD=1\n", 0o444)
		err := writeEnvFile(path, []byte("NEW=2\n"))
		if err == nil || !strings.Contains(err.Error(), "cannot be written by this user") {
			t.Fatalf("err = %v, want a refusal to swap out a file this user could not write in place", err)
		}
		if got, _ := os.ReadFile(path); string(got) != "OLD=1\n" {
			t.Fatalf(".env holds %q after the refusal", got)
		}
		if names := dirNames(t, filepath.Dir(path)); len(names) != 1 {
			t.Fatalf("scratch files left behind: %v", names)
		}
	})
	t.Run("a directory this user cannot add files to", func(t *testing.T) {
		path := writeEnvFixture(t, "OLD=1\n", 0o600)
		dir := filepath.Dir(path)
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Chmod(dir, 0o700) }()
		err := writeEnvFile(path, []byte("NEW=2\n"))
		if err == nil || !strings.Contains(err.Error(), "cannot create a file in "+dir) {
			t.Fatalf("err = %v, want the directory named", err)
		}
		if got, _ := os.ReadFile(path); string(got) != "OLD=1\n" {
			t.Fatalf(".env holds %q after the failure", got)
		}
	})
	t.Run("a link to nothing", func(t *testing.T) {
		dir := t.TempDir()
		link := filepath.Join(dir, ".env")
		if err := os.Symlink("missing.env", link); err != nil {
			t.Skipf("no symbolic links here: %v", err)
		}
		if err := writeEnvFile(link, []byte("NEW=2\n")); err == nil || !strings.Contains(err.Error(), "does not lead to a file") {
			t.Fatalf("err = %v, want a refusal naming the dangling link", err)
		}
	})
	t.Run("a directory", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, ".env")
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeEnvFile(path, []byte("NEW=2\n")); err == nil || !strings.Contains(err.Error(), "not a regular file") {
			t.Fatalf("err = %v, want a refusal", err)
		}
	})
}

// TestCheckEnvWritableGoesThroughTheSameSteps pins that the check a command
// makes before it changes anything says what the write it is about to make
// will say, and leaves nothing of its own behind.
func TestCheckEnvWritableGoesThroughTheSameSteps(t *testing.T) {
	t.Run("a file it can replace", func(t *testing.T) {
		path := writeEnvFixture(t, "OLD=1\n", 0o640)
		before, _ := os.Stat(path)
		if err := checkEnvWritable(path); err != nil {
			t.Fatal(err)
		}
		after, _ := os.Stat(path)
		if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
			t.Fatal("the check touched the file")
		}
		if got, _ := os.ReadFile(path); string(got) != "OLD=1\n" {
			t.Fatalf(".env holds %q", got)
		}
		if names := dirNames(t, filepath.Dir(path)); len(names) != 1 {
			t.Fatalf("the check left files behind: %v", names)
		}
	})
	t.Run("no file yet, in a directory it can write to", func(t *testing.T) {
		dir := t.TempDir()
		if err := checkEnvWritable(filepath.Join(dir, ".env")); err != nil {
			t.Fatal(err)
		}
		if names := dirNames(t, dir); len(names) != 0 {
			t.Fatalf("the check left files behind: %v", names)
		}
	})
	t.Run("what the write refuses", func(t *testing.T) {
		skipAsRoot(t)
		readOnly := writeEnvFixture(t, "OLD=1\n", 0o444)
		if err := checkEnvWritable(readOnly); err == nil || !strings.Contains(err.Error(), "cannot be written by this user") {
			t.Fatalf("read-only file: err = %v", err)
		}
		locked := writeEnvFixture(t, "OLD=1\n", 0o600)
		if err := os.Chmod(filepath.Dir(locked), 0o500); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Chmod(filepath.Dir(locked), 0o700) }()
		if err := checkEnvWritable(locked); err == nil || !strings.Contains(err.Error(), "cannot create a file in") {
			t.Fatalf("directory without write access: err = %v", err)
		}
	})
}

// TestWriteEnvFileNeverShowsAReaderAHalfWrittenFile is the point of the whole
// exercise: while the file is rewritten again and again, every look at it
// finds one complete version or the other. Written in place, a file this size
// is caught empty or cut short by a reader this busy almost at once.
func TestWriteEnvFileNeverShowsAReaderAHalfWrittenFile(t *testing.T) {
	const size = 512 << 10
	versions := [2]string{strings.Repeat("a", size-1) + "\n", strings.Repeat("b", size-1) + "\n"}
	path := writeEnvFixture(t, versions[0], 0o600)

	done := make(chan struct{})
	problem := make(chan string, 1)
	go func() {
		defer close(problem)
		for {
			select {
			case <-done:
				return
			default:
			}
			got, err := os.ReadFile(path)
			if err != nil {
				problem <- "reading .env: " + err.Error()
				return
			}
			if string(got) != versions[0] && string(got) != versions[1] {
				problem <- "a reader found " + strconv.Itoa(len(got)) + " bytes that are neither version"
				return
			}
		}
	}()
	for i := 0; i < 25; i++ {
		if err := writeEnvFile(path, []byte(versions[(i+1)%2])); err != nil {
			close(done)
			t.Fatal(err)
		}
	}
	close(done)
	if msg := <-problem; msg != "" {
		t.Fatal(msg)
	}
}
