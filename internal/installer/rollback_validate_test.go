// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
)

// rollbackWith rolls back an install whose manifest records no created entry,
// over the .env given, with override and tls.yml as files that exist and
// whatever else the .env lists as files that do not. It returns .env
// afterwards, what rollback said and how it ended.
func rollbackWith(t *testing.T, env string, tlsExists bool) (after, out string, err error) {
	t.Helper()
	dir, composeFile := setupProject(t)
	override := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	if writeErr := os.WriteFile(override, []byte("services: {}\n"), 0o644); writeErr != nil {
		t.Fatal(writeErr)
	}
	if tlsExists {
		if writeErr := os.WriteFile(filepath.Join(dir, "tls.yml"), []byte("services: {}\n"), 0o644); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	envPath := filepath.Join(dir, ".env")
	if writeErr := os.WriteFile(envPath, []byte(env), 0o644); writeErr != nil {
		t.Fatal(writeErr)
	}
	row := "pg"
	_ = manifest.Manifest{ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage, OriginalDriverRow: &row,
		OverrideFile: override, PGUser: "bloodhound", PGDatabase: "bloodhound"}.Save(dir)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer api.Close()

	restoreRow := "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('pg')"
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{}}
	tls := filepath.Join(dir, "tls.yml")
	for _, files := range [][]string{{composeFile}, {composeFile, override}, {composeFile, tls}, {composeFile, tls, override}, {composeFile, override, tls}} {
		project := "docker compose --project-directory " + dir
		for _, f := range files {
			project += " -f " + f
		}
		project += " "
		psql := project + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
		fake.Outputs[psql+restoreRow] = []byte("INSERT 0 1\n")
		fake.Outputs[project+"up -d"] = nil
		scriptLineageEnd(fake, psql)
	}
	var said bytes.Buffer
	err = Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &said},
		Options{ComposeFile: composeFile, Yes: true, APIURL: api.URL, VerifyTimeout: time.Second})
	data, _ := os.ReadFile(envPath)
	return string(data), said.String(), err
}

// TestRollbackTakesTheOverrideOutHoweverTheEntrySpellsIt covers an entry that
// names the override by a path that means the same file but is not spelled the
// way the install wrote it. Rollback deleted the override file and left the
// entry naming it -- and, its own commands skipping a file that is not there,
// reported success -- while the operator's plain `docker compose` stopped with
// "compose file ... is invalid: no such file". The override is recognised by
// the file it names, not by its spelling.
func TestRollbackTakesTheOverrideOutHoweverTheEntrySpellsIt(t *testing.T) {
	for _, c := range []struct{ name, env, want string }{
		{"a dot in front", "COMPOSE_FILE=docker-compose.yml:./docker-compose.bloodtrail.yml\n", "COMPOSE_FILE=docker-compose.yml\n"},
		{"a detour through a subdirectory", "COMPOSE_FILE=docker-compose.yml:sub/../docker-compose.bloodtrail.yml:tls.yml\n", "COMPOSE_FILE=docker-compose.yml:tls.yml\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			after, out, err := rollbackWith(t, c.env, true)
			if err != nil {
				t.Fatalf("rollback: %v\n%s", err, out)
			}
			if after != c.want {
				t.Errorf(".env afterwards = %q, want %q", after, c.want)
			}
		})
	}
}

// TestRollbackSaysWhenTheRestoredEntryNamesAFileThatIsGone covers what
// rollback cannot repair for the operator: its own restart skips a listed file
// that is not there (which keeps rollback working for the operator who deleted
// the override file and left its entry), so it succeeded and never said that
// the operator's own `docker compose` -- which stops on a listed file that is
// missing -- cannot load the project. It says so, naming the file.
func TestRollbackSaysWhenTheRestoredEntryNamesAFileThatIsGone(t *testing.T) {
	after, out, err := rollbackWith(t, "COMPOSE_FILE=docker-compose.yml:tls.yml:docker-compose.bloodtrail.yml\n", false)
	if err != nil {
		t.Fatalf("rollback: %v\n%s", err, out)
	}
	if after != "COMPOSE_FILE=docker-compose.yml:tls.yml\n" {
		t.Errorf(".env afterwards = %q", after)
	}
	if !strings.Contains(out, "tls.yml") || !strings.Contains(out, "does not exist") {
		t.Errorf("rollback did not say that the entry names a file that is gone:\n%s", out)
	}

	// Nothing to say when every listed file is there, or there is no entry.
	for _, env := range []string{"COMPOSE_FILE=docker-compose.yml:tls.yml:docker-compose.bloodtrail.yml\n", "A=b\n"} {
		if _, out, err := rollbackWith(t, env, true); err != nil || strings.Contains(out, "does not exist") {
			t.Errorf("rollback over %q: err %v, said:\n%s", env, err, out)
		}
	}
}
