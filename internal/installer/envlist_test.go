// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
)

// withDir fills the project directory into a fixture written with %[1]s in
// the places it goes.
func withDir(template, dir string) string { return strings.ReplaceAll(template, "%[1]s", dir) }

// TestInstallNamesTheOverrideTheWayTheEntryNamesItsFiles covers how the
// install adds its override to an entry that is there. Compose resolves a
// relative name in COMPOSE_FILE against the directory it is run from, even
// with --project-directory, so an entry of absolute paths -- written to work
// from any directory -- was given a relative name for the override, which
// broke it from every directory but the project's. The override now goes into
// such an entry by its absolute path. Every other list keeps the relative
// name, which moves with the project and is what those lists rely on for the
// files already in them; so does the entry the install creates, which
// replaces compose's file discovery in the directory the operator runs
// compose from.
func TestInstallNamesTheOverrideTheWayTheEntryNamesItsFiles(t *testing.T) {
	for _, c := range []struct {
		name string
		// env and want are the .env before and after, with %[1]s for the
		// project directory; files are the compose files the entry lists, by
		// name in the project directory, in the order they merge.
		env, want string
		files     []string
		created   bool // whether the install creates the entry
	}{
		{"absolute names",
			"COMPOSE_FILE=%[1]s/docker-compose.yml\n",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/docker-compose.bloodtrail.yml\n",
			[]string{"docker-compose.yml"}, false},
		{"several absolute names",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/extra.yml\n",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/extra.yml:%[1]s/docker-compose.bloodtrail.yml\n",
			[]string{"docker-compose.yml", "extra.yml"}, false},
		{"absolute names, exported and quoted",
			"export COMPOSE_FILE='%[1]s/docker-compose.yml' # ours\n",
			"export COMPOSE_FILE='%[1]s/docker-compose.yml:%[1]s/docker-compose.bloodtrail.yml' # ours\n",
			[]string{"docker-compose.yml"}, false},
		{"absolute and relative names",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:extra.yml\n",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n",
			[]string{"docker-compose.yml", "extra.yml"}, false},
		{"relative names",
			"COMPOSE_FILE=docker-compose.yml:extra.yml\n",
			"COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n",
			[]string{"docker-compose.yml", "extra.yml"}, false},
		{"absolute names that already list the override",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/docker-compose.bloodtrail.yml\n",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/docker-compose.bloodtrail.yml\n",
			[]string{"docker-compose.yml"}, false},
		{"absolute names that list the override by its relative name",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:docker-compose.bloodtrail.yml\n",
			"COMPOSE_FILE=%[1]s/docker-compose.yml:docker-compose.bloodtrail.yml\n",
			[]string{"docker-compose.yml"}, false},
		{"no entry",
			"A=b\n",
			"A=b\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n",
			[]string{"docker-compose.yml"}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, composeFile := setupProject(t)
			_ = os.WriteFile(filepath.Join(dir, "extra.yml"), []byte("services: {}\n"), 0o644)
			envPath := filepath.Join(dir, ".env")
			if err := os.WriteFile(envPath, []byte(withDir(c.env, dir)), 0o644); err != nil {
				t.Fatal(err)
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
			defer api.Close()
			image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
			base := "docker compose --project-directory " + dir
			for _, f := range c.files {
				base += " -f " + filepath.Join(dir, f)
			}
			base += " "
			fake := scriptPGInstall(&dockerx.FakeRunner{}, base, filepath.Join(dir, "docker-compose.bloodtrail.yml"), image)
			opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
				VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
			if err := Install(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, opts); err != nil {
				t.Fatalf("install: %v", err)
			}
			if got := readFile(t, envPath); got != withDir(c.want, dir) {
				t.Fatalf(".env = %q, want %q", got, withDir(c.want, dir))
			}
			if m, err := manifest.Load(dir); err != nil || m.EnvComposeFileCreated != c.created {
				t.Fatalf("manifest created = %v (err %v), want %v", m.EnvComposeFileCreated, err, c.created)
			}
		})
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestRollbackTakesOutTheOverrideWhicheverWayTheEntryNamesIt is the other half:
// an override named by its absolute path is the installer's too, and comes
// out again, leaving the operator's list as it was.
func TestRollbackTakesOutTheOverrideWhicheverWayTheEntryNamesIt(t *testing.T) {
	for _, c := range []struct {
		name    string
		env     string // with %s for the project directory
		want    string
		restart []string
	}{
		{"absolute names", "COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/extra.yml:%[1]s/docker-compose.bloodtrail.yml\n", "COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/extra.yml\n", []string{"docker-compose.yml", "extra.yml"}},
		{"only the override in an absolute list is the whole entry", "COMPOSE_FILE=%[1]s/docker-compose.yml:%[1]s/docker-compose.bloodtrail.yml\n", "COMPOSE_FILE=%[1]s/docker-compose.yml\n", []string{"docker-compose.yml"}},
		{"the relative name", "COMPOSE_FILE=%[1]s/docker-compose.yml:docker-compose.bloodtrail.yml\n", "COMPOSE_FILE=%[1]s/docker-compose.yml\n", []string{"docker-compose.yml"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, composeFile := setupProject(t)
			extra := filepath.Join(dir, "extra.yml")
			override := filepath.Join(dir, "docker-compose.bloodtrail.yml")
			for _, f := range []string{extra, override} {
				_ = os.WriteFile(f, []byte("services: {}\n"), 0o644)
			}
			envPath := filepath.Join(dir, ".env")
			if err := os.WriteFile(envPath, []byte(withDir(c.env, dir)), 0o644); err != nil {
				t.Fatal(err)
			}
			row := "pg"
			_ = manifest.Manifest{ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage, OriginalDriverRow: &row,
				OverrideFile: override, PGUser: "bloodhound", PGDatabase: "bloodhound"}.Save(dir)
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
			defer api.Close()

			project := func(files ...string) string {
				p := "docker compose --project-directory " + dir
				for _, f := range files {
					p += " -f " + filepath.Join(dir, f)
				}
				return p + " "
			}
			restoreRow := "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('pg')"
			fake := &dockerx.FakeRunner{Outputs: map[string][]byte{}}
			for _, files := range [][]string{{"docker-compose.yml"}, {"docker-compose.yml", "docker-compose.bloodtrail.yml"}, {"docker-compose.yml", "extra.yml"}, {"docker-compose.yml", "extra.yml", "docker-compose.bloodtrail.yml"}} {
				psql := project(files...) + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
				fake.Outputs[psql+restoreRow] = []byte("INSERT 0 1\n")
				fake.Outputs[project(files...)+"up -d"] = nil
				scriptLineageEnd(fake, psql)
			}
			var out bytes.Buffer
			if err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &out},
				Options{ComposeFile: composeFile, Yes: true, APIURL: api.URL, VerifyTimeout: time.Second}); err != nil {
				t.Fatalf("rollback: %v\n%s", err, out.String())
			}
			if got := readFile(t, envPath); got != withDir(c.want, dir) {
				t.Fatalf(".env = %q, want %q", got, withDir(c.want, dir))
			}
			if !fake.Called(project(c.restart...) + "up -d") {
				t.Fatalf("the restart did not use %v:\n%s", c.restart, strings.Join(fake.Calls, "\n"))
			}
		})
	}
}

// TestInstallThenRollbackGivesAnAbsoluteListBack pins the round trip on the
// .env, byte for byte.
func TestInstallThenRollbackGivesAnAbsoluteListBack(t *testing.T) {
	dir, composeFile := setupProject(t)
	envPath := filepath.Join(dir, ".env")
	original := "A=b\nexport COMPOSE_FILE=\"" + composeFile + "\" # mine\nC=d\n"
	if err := os.WriteFile(envPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
	if err := Install(context.Background(), Deps{Runner: scriptPGInstall(&dockerx.FakeRunner{}, base, overridePath, image), HTTP: api.Client(), Out: &bytes.Buffer{}}, opts); err != nil {
		t.Fatalf("install: %v", err)
	}
	if got, _ := os.ReadFile(envPath); !strings.Contains(string(got), ":"+overridePath+"\"") {
		t.Fatalf("the install did not add its override by its absolute path: %q", got)
	}

	installed := base + "-f " + overridePath + " "
	restoreRow := "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('pg')"
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		installed + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc " + restoreRow: []byte("INSERT 0 1\n"),
		base + "up -d": nil,
	}}
	scriptLineageEnd(fake, base+"exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc ")
	if err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, opts); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got, _ := os.ReadFile(envPath); string(got) != original {
		t.Fatalf("rollback left .env as %q, want %q", got, original)
	}
}

// requireComposeOracle skips the test unless docker compose is there and can
// read a trivial project. What the tests that call it compare the installer
// with is compose's own answer, so where there is none -- no docker, no
// compose plugin, nothing that can load a project -- they have nothing to say.
// `docker compose config`, all they ever run, only reads and prints a project;
// it starts nothing.
func requireComposeOracle(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "compose", "version").Run(); err != nil {
		t.Skipf("no docker compose to use as the oracle: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte("services:\n  s:\n    image: img:x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := dockerComposeConfig(dir); err != nil {
		t.Skipf("docker compose cannot read a trivial project here: %v", err)
	}
}

// dockerComposeConfig runs `docker compose config --format json` in cwd, with
// args in front, and returns its standard output.
func dockerComposeConfig(cwd string, args ...string) (string, error) {
	cmd := exec.Command("docker", append(append([]string{"compose"}, args...), "config", "--format", "json")...)
	cmd.Dir = cwd
	// The oracle must read what the project says, not what this shell says:
	// compose takes these from the environment over .env, empty ones included,
	// so they are left out rather than emptied.
	for _, kv := range os.Environ() {
		switch key, _, _ := strings.Cut(kv, "="); key {
		case "COMPOSE_FILE", "COMPOSE_PATH_SEPARATOR", "COMPOSE_ENV_FILES", "COMPOSE_DISABLE_ENV_FILE", "COMPOSE_PROJECT_NAME":
		default:
			cmd.Env = append(cmd.Env, kv)
		}
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if err != nil {
		err = &composeOracleError{err: err, stderr: strings.TrimSpace(stderr.String())}
	}
	return stdout.String(), err
}

type composeOracleError struct {
	err    error
	stderr string
}

func (e *composeOracleError) Error() string { return e.err.Error() + ": " + e.stderr }

// TestOverrideEntryLoadsFromAnyDirectoryComposeIsRunFrom uses docker compose
// itself as the oracle for why the override goes into a list of absolute paths
// by its absolute path: the project an install leaves is loaded by `docker
// compose --project-directory <dir> ...` run from another directory only when
// every name in COMPOSE_FILE is absolute. It runs the install over an .env
// that lists the compose file by its absolute path and asks compose for the
// result; `config` only reads and prints the project, nothing is started. The
// failure of the relative spelling is reported, not required: it is why the
// absolute one exists, and a compose that resolved relative names against the
// project directory would make that one merely unnecessary.
func TestOverrideEntryLoadsFromAnyDirectoryComposeIsRunFrom(t *testing.T) {
	requireComposeOracle(t)
	dir := t.TempDir()
	elsewhere := t.TempDir()
	composeFile := filepath.Join(dir, "docker-compose.yml")
	overrideFile := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	if err := os.WriteFile(composeFile, []byte("services:\n  bloodhound:\n    image: img:base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	envPath := filepath.Join(dir, ".env")
	if err := os.WriteFile(envPath, []byte("COMPOSE_FILE="+composeFile+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	fake := scriptPGInstall(&dockerx.FakeRunner{}, base, overrideFile, image)
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
	if err := Install(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, opts); err != nil {
		t.Fatalf("install: %v", err)
	}
	// What the install wrote is all compose gets to see of the override: its
	// image comes from the file the entry names, so only what compose loads
	// from it tells.
	if err := os.WriteFile(overrideFile, []byte("services:\n  bloodhound:\n    image: img:bt\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	written, _ := os.ReadFile(envPath)

	for _, cwd := range []string{dir, elsewhere} {
		out, err := dockerComposeConfig(cwd, "--project-directory", dir)
		if err != nil || !strings.Contains(out, `"image": "img:bt"`) {
			t.Errorf("run from %s, with .env %q: the project does not have the override's image (err %v):\n%s", cwd, written, err, out)
		}
	}

	relative := "COMPOSE_FILE=" + composeFile + ":docker-compose.bloodtrail.yml\n"
	if err := os.WriteFile(envPath, []byte(relative), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := dockerComposeConfig(elsewhere, "--project-directory", dir); err != nil {
		t.Logf("the relative spelling, run from another directory, fails as expected: %v", err)
	} else {
		t.Logf("this docker compose resolves the relative name against the project directory (override loaded: %v)", strings.Contains(out, "img:bt"))
	}
}
