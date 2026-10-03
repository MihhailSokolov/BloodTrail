// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
)

func TestPriorComposeEntryReadsTheBackupCopy(t *testing.T) {
	backupWith := func(t *testing.T, env *string) manifest.Manifest {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "backup")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if env != nil {
			if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(*env), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return manifest.Manifest{BackupDir: dir}
	}
	for _, c := range []struct {
		name string
		env  *string
		want priorEntry
	}{
		{"an .env without the entry", strPtr("A=b\n"), priorAbsent},
		{"an empty .env", strPtr(""), priorAbsent},
		{"no .env in the backup directory", nil, priorAbsent},
		{"an entry", strPtr("COMPOSE_FILE=a.yml:b.yml\n"), priorPresent},
		{"an exported entry", strPtr("export COMPOSE_FILE=a.yml\n"), priorPresent},
		{"an empty entry", strPtr("COMPOSE_FILE=\n"), priorPresent},
		{"two entries", strPtr("COMPOSE_FILE=a.yml\nexport COMPOSE_FILE=b.yml\n"), priorPresent},
		{"an entry that cannot be followed", strPtr("COMPOSE_FILE=\"a.yml:${B}\"\n"), priorUnknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := priorComposeEntry(backupWith(t, c.env)); got != c.want {
				t.Errorf("priorComposeEntry = %v, want %v", got, c.want)
			}
		})
	}
	t.Run("a manifest without a backup directory", func(t *testing.T) {
		if got := priorComposeEntry(manifest.Manifest{}); got != priorUnknown {
			t.Errorf("priorComposeEntry = %v, want unknown", got)
		}
	})
	t.Run("a backup directory that is gone", func(t *testing.T) {
		if got := priorComposeEntry(manifest.Manifest{BackupDir: filepath.Join(t.TempDir(), "gone")}); got != priorUnknown {
			t.Errorf("priorComposeEntry = %v, want unknown", got)
		}
	})
}

func TestLegacyWroteRecognisesWhatEarlyInstallsWrote(t *testing.T) {
	inProject := manifest.Manifest{ProjectDir: "/srv/bh", ComposeFile: "/srv/bh/docker-compose.yml"}
	for _, c := range []struct {
		name   string
		m      manifest.Manifest
		others []string
		want   bool
	}{
		{"the compose file", inProject, []string{"docker-compose.yml"}, true},
		{"with the override beside it", inProject, []string{"docker-compose.yml", "docker-compose.override.yml"}, true},
		{"with the .yaml spelling", inProject, []string{"docker-compose.yml", "docker-compose.override.yaml"}, true},
		{"with the override compose discovers first", inProject, []string{"docker-compose.yml", "compose.override.yml"}, true},
		{"with a file the operator added", inProject, []string{"docker-compose.yml", "tls.yml"}, false},
		{"a file added first", inProject, []string{"tls.yml", "docker-compose.yml"}, false},
		{"three files", inProject, []string{"docker-compose.yml", "docker-compose.override.yml", "tls.yml"}, false},
		{"the compose file twice", inProject, []string{"docker-compose.yml", "docker-compose.yml"}, false},
		{"the compose file spelled another way", inProject, []string{"./docker-compose.yml"}, false},
		{"only the override", inProject, []string{"docker-compose.override.yml"}, false},
		{"nothing", inProject, nil, false},
		{"a compose file with its own override spelling",
			manifest.Manifest{ProjectDir: "/srv/bh", ComposeFile: "/srv/bh/stack.yml"}, []string{"stack.yml", "stack.override.yml"}, true},
		{"a compose file beside the project directory",
			manifest.Manifest{ProjectDir: "/srv/bh", ComposeFile: "/srv/shared/base.yml"}, []string{"../shared/base.yml", "../shared/docker-compose.override.yml"}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := legacyWrote(c.m, c.others); got != c.want {
				t.Errorf("legacyWrote(%q) = %v, want %v", c.others, got, c.want)
			}
		})
	}
}

// TestRestoreOfALegacyEntryIsIdempotent pins that rollback can be run again
// after a partial failure -- a restart that did not come up, say -- without
// undoing more than the first run did: whatever the first run left of the
// entry, the second leaves alone. The manifest still says "created" for the
// rerun, and the .env now says what the first run made of it.
func TestRestoreOfALegacyEntryIsIdempotent(t *testing.T) {
	m := manifest.Manifest{ProjectDir: "/srv/bh", ComposeFile: "/srv/bh/docker-compose.yml", EnvComposeFileCreated: true}
	const installed = "docker-compose.yml:docker-compose.bloodtrail.yml"
	for _, c := range []struct {
		name  string
		env   string
		prior priorEntry
	}{
		{"unmodified", "A=b\nCOMPOSE_FILE=" + installed + "\n", priorAbsent},
		{"unmodified, backup gone", "A=b\nCOMPOSE_FILE=" + installed + "\n", priorUnknown},
		{"a file added", "COMPOSE_FILE=" + installed + ":tls.yml\n", priorAbsent},
		{"a file added, backup gone", "COMPOSE_FILE=" + installed + ":tls.yml\n", priorUnknown},
		{"appended beside an exported entry", "export COMPOSE_FILE=docker-compose.yml\nCOMPOSE_FILE=" + installed + "\n", priorPresent},
		{"appended beside an exported entry, backup gone", "export COMPOSE_FILE=docker-compose.yml\nCOMPOSE_FILE=" + installed + "\n", priorUnknown},
		{"the operator's entry extended", "COMPOSE_FILE=docker-compose.yml:tls.yml:docker-compose.bloodtrail.yml\n", priorPresent},
		{"an empty entry written into", "COMPOSE_FILE=docker-compose.bloodtrail.yml\n", priorPresent},
		{"an empty entry written into, backup gone", "COMPOSE_FILE=docker-compose.bloodtrail.yml\n", priorUnknown},
		{"only the override left", "COMPOSE_FILE=docker-compose.bloodtrail.yml\n", priorAbsent},
	} {
		t.Run(c.name, func(t *testing.T) {
			first, _, err := restoreComposeFileEntry(c.env, m, c.prior)
			if err != nil {
				t.Fatal(err)
			}
			second, note, err := restoreComposeFileEntry(first, m, c.prior)
			if err != nil {
				t.Fatalf("the rerun: %v", err)
			}
			if second != first {
				t.Errorf("the first run left %q and the rerun changed it to %q", first, second)
			}
			if note != "" && !strings.Contains(first, "COMPOSE_FILE") {
				t.Errorf("the rerun had something to say (%q) about an .env with no entry left", note)
			}
		})
	}
}
