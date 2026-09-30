// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
)

// TestInstallStopsWhenComposeLoadsTheProjectFromAnotherDirectory covers a
// COMPOSE_FILE entry whose first file is not in the directory the .env is in.
// Compose then takes the directory of that first file as the project
// directory, which relative paths in the compose files -- a bind mount such as
// ./pgdata -- resolve against, while every installer command names
// --project-directory <the .env's directory>. With the project name pinned in
// .env the two reach the same containers, so the install went ahead and its
// `up -d` could recreate the operator's database on a directory that is empty.
//
// The entry the install creates, where there is none, is the same thing: it
// lists the compose file first, so once written it moves compose's project
// directory to the compose file's directory -- a layout the refusal above
// rejects on the next run, and that the operator's own plain `docker compose
// up -d` resolves differently from the installer's commands.
func TestInstallStopsWhenComposeLoadsTheProjectFromAnotherDirectory(t *testing.T) {
	for _, c := range []struct {
		name string
		// layout writes the files under root and returns the directory the
		// .env is in, its contents, the compose file the installer is given
		// and the directory compose loads the project from.
		layout func(t *testing.T, root string) (projectDir, env, composeFile, composeDir string)
	}{
		{"the first file is in a subdirectory", func(t *testing.T, root string) (string, string, string, string) {
			file := writeProjectFile(t, root, "docker/compose.yml")
			return root, "COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=docker/compose.yml\n", file, filepath.Dir(file)
		}},
		{"the first file is in another directory altogether", func(t *testing.T, root string) (string, string, string, string) {
			shared := writeProjectFile(t, t.TempDir(), "shared/base.yml")
			file := writeProjectFile(t, root, "docker-compose.yml")
			return root, "COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=" + shared + ":docker-compose.yml\n", file, filepath.Dir(shared)
		}},
		{"the first file is in the parent directory", func(t *testing.T, root string) (string, string, string, string) {
			file := writeProjectFile(t, root, "stack/docker-compose.yml")
			writeProjectFile(t, root, "extra.yml")
			return filepath.Dir(file), "COMPOSE_FILE=../extra.yml:docker-compose.yml\n", file, root
		}},
		{"no entry, and the compose file is in a subdirectory", func(t *testing.T, root string) (string, string, string, string) {
			file := writeProjectFile(t, root, "docker/compose.yml")
			return root, "COMPOSE_PROJECT_NAME=bh\n", file, filepath.Dir(file)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			projectDir, env, composeFile, composeDir := c.layout(t, t.TempDir())
			envPath := filepath.Join(projectDir, ".env")
			if err := os.WriteFile(envPath, []byte(env), 0o644); err != nil {
				t.Fatal(err)
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
			defer api.Close()
			image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"

			// Whichever project the install addresses, it runs to the end: only
			// the refusal can stop it.
			forgiving, err := composeHandle(&dockerx.FakeRunner{}, composeFile, projectDir)
			if err != nil {
				t.Fatal(err)
			}
			fake := scriptPGInstall(&dockerx.FakeRunner{}, "docker "+strings.Join(forgiving.Args(), " ")+" ", filepath.Join(projectDir, "docker-compose.bloodtrail.yml"), image)
			opts := Options{ComposeFile: composeFile, ProjectDir: projectDir, Image: image, APIURL: api.URL, Yes: true,
				VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
			err = Install(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, opts)
			if err == nil || !strings.Contains(err.Error(), composeDir) || !strings.Contains(err.Error(), "--project-directory "+projectDir) {
				t.Fatalf("want a refusal naming both directories, got %v", err)
			}
			// Moving the .env moves what its relative names are relative to, and
			// the installer then has to be told where the compose file is.
			hints := []string{"relative COMPOSE_FILE names", "--compose-file"}
			if !strings.Contains(env, "COMPOSE_FILE") {
				hints = []string{"--compose-file"} // there are no names to rewrite
			}
			for _, want := range hints {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the refusal does not say %q:\n%v", want, err)
				}
			}
			if len(fake.Calls) != 0 {
				t.Fatalf("install ran commands before refusing:\n%s", strings.Join(fake.Calls, "\n"))
			}
			if manifest.Exists(projectDir) {
				t.Fatal("a refused install saved a manifest")
			}
			if got, _ := os.ReadFile(envPath); string(got) != env {
				t.Fatalf("a refused install changed .env to %q", got)
			}

			// status, verify and rollback have to keep working on whatever an
			// install left: they address the project as they always did.
			if files := append([]string{forgiving.File}, forgiving.ExtraFiles...); forgiving.ProjectDir != projectDir || !slices.Contains(files, composeFile) {
				t.Fatalf("composeHandle = %+v; want the project addressed through %s with %s in it", forgiving, projectDir, composeFile)
			}
		})
	}
}

// writeProjectFile writes a compose file that mounts ./pgdata under root's
// name (a path relative to the file's directory, so that where compose
// resolves it says where it loaded the project from) and returns its path.
func writeProjectFile(t *testing.T, root, name string) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	content := "services:\n  app-db:\n    image: postgres:18\n    volumes:\n      - ./pgdata:/var/lib/postgresql/data\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestInstallAcceptsAProjectOnlyWhereComposeAgreesOnItsDirectory asks docker
// compose itself, through `docker compose config` (which only reads and
// prints the project), where a bind mount written as ./pgdata lands for the
// operator's plain command run in the .env's directory and for the command
// line the installer runs, and requires the install to be accepted exactly
// where the two agree. The project name is pinned in .env, as it has to be
// for the two to reach the same containers at all. Skipped where there is no
// docker compose.
func TestInstallAcceptsAProjectOnlyWhereComposeAgreesOnItsDirectory(t *testing.T) {
	requireComposeOracle(t)
	requireFirstFileDirectoryIsTheProjectDirectory(t)
	for _, c := range []struct {
		name   string
		layout func(t *testing.T, root string) (env, composeFile string)
	}{
		{"the compose file beside the .env", func(t *testing.T, root string) (string, string) {
			return "COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=docker-compose.yml\n", writeProjectFile(t, root, "docker-compose.yml")
		}},
		{"no entry, so compose discovers the file beside the .env", func(t *testing.T, root string) (string, string) {
			return "COMPOSE_PROJECT_NAME=bh\n", writeProjectFile(t, root, "docker-compose.yml")
		}},
		{"an absolute first file beside the .env, another in a subdirectory", func(t *testing.T, root string) (string, string) {
			file := writeProjectFile(t, root, "docker-compose.yml")
			writeProjectFile(t, root, "docker/extra.yml")
			return "COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=" + file + ":docker/extra.yml\n", file
		}},
		{"the first file in a subdirectory", func(t *testing.T, root string) (string, string) {
			return "COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=docker/compose.yml\n", writeProjectFile(t, root, "docker/compose.yml")
		}},
		{"the first file in another directory", func(t *testing.T, root string) (string, string) {
			shared := writeProjectFile(t, t.TempDir(), "base.yml")
			return "COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=" + shared + ":docker-compose.yml\n", writeProjectFile(t, root, "docker-compose.yml")
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			env, composeFile := c.layout(t, root)
			if err := os.WriteFile(filepath.Join(root, ".env"), []byte(env), 0o644); err != nil {
				t.Fatal(err)
			}
			operator, err := dockerComposeConfig(root)
			if err != nil {
				t.Fatalf("the operator's own command: %v", err)
			}

			h, refused := installHandle(&dockerx.FakeRunner{}, composeFile, root)
			if refused != nil {
				h, err = composeHandle(&dockerx.FakeRunner{}, composeFile, root)
				if err != nil {
					t.Fatal(err)
				}
			}
			installer, err := dockerComposeConfig(root, h.Args()[1:]...)
			if err != nil {
				t.Fatalf("the installer's command line: %v", err)
			}

			agree := pgdataSources(t, operator) == pgdataSources(t, installer)
			if refused == nil && !agree {
				t.Errorf("install accepted a project it addresses differently from the operator: the bind mount is %s for compose, %s for the installer",
					pgdataSources(t, operator), pgdataSources(t, installer))
			}
			if refused != nil && agree {
				t.Errorf("install refused a project compose and the installer address alike (%s): %v", pgdataSources(t, operator), refused)
			}
		})
	}
}

// TestInstallDoesNotCreateAnEntryThatMovesComposesProjectDirectory asks docker
// compose itself about the project an install leaves when it creates the
// COMPOSE_FILE entry: with no entry, the .env in the project directory and the
// compose file in a subdirectory of it, the entry lists that file first, and
// the operator's plain `docker compose config` then takes the subdirectory as
// the project directory -- where the installer, addressing the project
// through --project-directory <the .env's directory>, saw the parent. The
// install must not go ahead where the project it leaves is one the operator's
// commands resolve differently from its own; where the compose file is beside
// the .env it must.
func TestInstallDoesNotCreateAnEntryThatMovesComposesProjectDirectory(t *testing.T) {
	requireComposeOracle(t)
	requireFirstFileDirectoryIsTheProjectDirectory(t)
	for _, c := range []struct {
		name, composeFile string // the compose file, by its name in the project
	}{
		{"the compose file beside the .env", "docker-compose.yml"},
		{"the compose file in a subdirectory", "docker/compose.yml"},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			composeFile := writeProjectFile(t, root, c.composeFile)
			envPath := filepath.Join(root, ".env")
			if err := os.WriteFile(envPath, []byte("COMPOSE_PROJECT_NAME=bh\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			h, refused := installHandle(&dockerx.FakeRunner{}, composeFile, root)
			if refused != nil {
				return // nothing is written, so nothing can disagree
			}

			// What the install goes on to write: the entry, and the override file
			// it names (which compose needs to be there to load the project).
			var baseFiles []string
			for _, f := range append([]string{h.File}, h.ExtraFiles...) {
				rel, _ := filepath.Rel(root, f)
				baseFiles = append(baseFiles, rel)
			}
			_, updated, err := installEnv(envPath, baseFiles, installOverrideEntry(root))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(envPath, []byte(updated), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "docker-compose.bloodtrail.yml"), []byte("services: {}\n"), 0o644); err != nil {
				t.Fatal(err)
			}

			operator, err := dockerComposeConfig(root)
			if err != nil {
				t.Fatalf("the operator's own command on the .env the install wrote (%q): %v", updated, err)
			}
			installer, err := dockerComposeConfig(root, h.WithExtraFile(filepath.Join(root, "docker-compose.bloodtrail.yml")).Args()[1:]...)
			if err != nil {
				t.Fatalf("the installer's command line: %v", err)
			}
			if a, b := pgdataSources(t, operator), pgdataSources(t, installer); a != b {
				t.Errorf("install accepted a project whose .env it then wrote as %q: the bind mount is %s for the operator's compose, %s for the installer", updated, a, b)
			}
		})
	}
}

// requireFirstFileDirectoryIsTheProjectDirectory skips the test unless this
// docker compose takes the directory of the first file COMPOSE_FILE lists as
// the project directory -- what the installer's refusal is built on, and what
// every compose release the installer supports does, but which the oracle
// comparison below would misreport as an over-refusal on one that did not.
func requireFirstFileDirectoryIsTheProjectDirectory(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	writeProjectFile(t, root, "docker/compose.yml")
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("COMPOSE_FILE=docker/compose.yml\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := dockerComposeConfig(root)
	if err != nil {
		t.Skipf("docker compose cannot read the probe project: %v", err)
	}
	want, err := filepath.EvalSymlinks(filepath.Join(root, "docker"))
	if err != nil {
		t.Fatal(err)
	}
	if got := pgdataSources(t, out); got != filepath.Join(want, "pgdata") {
		t.Skipf("this docker compose resolves ./pgdata to %s, not against the first COMPOSE_FILE entry's directory", got)
	}
}

// pgdataSources returns where the bind mounts of app-db land in the project
// `docker compose config --format json` printed.
func pgdataSources(t *testing.T, config string) string {
	t.Helper()
	var cfg struct {
		Services map[string]struct {
			Volumes []struct {
				Source string `json:"source"`
			} `json:"volumes"`
		} `json:"services"`
	}
	if err := json.Unmarshal([]byte(config), &cfg); err != nil {
		t.Fatalf("decoding docker compose config: %v\n%s", err, config)
	}
	var sources []string
	for _, v := range cfg.Services["app-db"].Volumes {
		// Where the mount lands, not how the path to it is spelled: the
		// directory it is in is resolved (the temporary directory of a test
		// is often reached through a symbolic link, and compose sees the
		// directory it runs in without it), the mount itself does not exist.
		dir, err := filepath.EvalSymlinks(filepath.Dir(v.Source))
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, filepath.Join(dir, filepath.Base(v.Source)))
	}
	return strings.Join(sources, ",")
}

// TestSameDirectoryTakesTheOneDirectoryReachedTwoWays pins that a first file
// listed through a symbolic link to the project directory is not mistaken for
// another directory: the mount ./pgdata lands in the same place either way.
func TestSameDirectoryTakesTheOneDirectoryReachedTwoWays(t *testing.T) {
	root := t.TempDir()
	project := filepath.Join(root, "bh")
	if err := os.Mkdir(project, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "current")
	if err := os.Symlink(project, link); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
	other := filepath.Join(root, "other")
	if err := os.Mkdir(other, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{project, project, true},
		{project + "/.", project, true},
		{link, project, true},
		{other, project, false},
		{filepath.Join(root, "missing"), project, false},
	} {
		if got := sameDirectory(c.a, c.b); got != c.want {
			t.Errorf("sameDirectory(%s, %s) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
	if err := checkProjectDirectory(filepath.Join(project, ".env"), project, filepath.Join(link, "docker-compose.yml"), false); err != nil {
		t.Errorf("a first file listed through a link to the project directory was refused: %v", err)
	}
}
