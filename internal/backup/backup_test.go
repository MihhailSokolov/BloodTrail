// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

func TestNewDirUsesUTCTimestamp(t *testing.T) {
	root := t.TempDir()
	dir, err := NewDir(root, time.Date(2026, 9, 2, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if dir != filepath.Join(root, ".bloodtrail", "backups", "20260902T123000Z") {
		t.Fatalf("dir = %q", dir)
	}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		t.Fatalf("directory not created: %v", err)
	}
}

func TestDumpDatabaseWritesFile(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		"docker compose --project-directory /p -f /p/docker-compose.yml exec -T app-db pg_dump -Fc -U bh -d bhdb": []byte("PGDMP..."),
	}}
	c := dockerx.Compose{Runner: fake, File: "/p/docker-compose.yml", ProjectDir: "/p"}
	dest := filepath.Join(t.TempDir(), "app-db.dump")
	if err := DumpDatabase(context.Background(), c, "app-db", "bh", "bhdb", dest); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(dest)
	if string(data) != "PGDMP..." {
		t.Fatalf("dump content = %q", data)
	}
}

func TestDumpDatabaseRejectsEmptyOutput(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		"docker compose --project-directory /p -f /p/docker-compose.yml exec -T app-db pg_dump -Fc -U bh -d bhdb": nil,
	}}
	c := dockerx.Compose{Runner: fake, File: "/p/docker-compose.yml", ProjectDir: "/p"}
	if err := DumpDatabase(context.Background(), c, "app-db", "bh", "bhdb", filepath.Join(t.TempDir(), "x")); err == nil {
		t.Fatal("expected error for empty dump")
	}
}

func TestCopyFilesSkipsMissing(t *testing.T) {
	src := t.TempDir()
	_ = os.WriteFile(filepath.Join(src, "a.yml"), []byte("a"), 0o644)
	dest := t.TempDir()
	if err := CopyFiles(dest, filepath.Join(src, "a.yml"), filepath.Join(src, "missing.env")); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(dest, "a.yml")); err != nil || string(data) != "a" {
		t.Fatalf("copy failed: %v %q", err, data)
	}
}

func TestCopyFilesUsesRestrictivePermissions(t *testing.T) {
	src := t.TempDir()
	srcFile := filepath.Join(src, "credentials.env")
	_ = os.WriteFile(srcFile, []byte("SECRET=value"), 0o644)
	dest := t.TempDir()
	if err := CopyFiles(dest, srcFile); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(dest, "credentials.env"))
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("permissions = %#o, want 0600", st.Mode().Perm())
	}
}
