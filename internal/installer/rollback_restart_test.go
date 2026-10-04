// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
)

// restartOutcome is what one rollback of an install did.
type restartOutcome struct {
	err          error
	out          string
	env          string   // the .env afterwards
	calls        []string // every command it ran
	restarted    bool     // whether it ran `up -d`
	rowRestored  bool     // whether it restored the driver row
	lineageEnded bool     // whether it ended the watermark lineage
	overrideGone bool
	manifestGone bool
	apiHits      int // requests the API of the deployment got (the wait for it after a restart)
}

// rollbackProject rolls back an install of the project whose compose file is
// composeName (by its name under the project directory, where the .env is),
// for a manifest that records the entry as created when created is set (the
// shape v0.1.0 to v0.1.2 wrote) and as the operator's own otherwise, over the
// .env given, with every plausible project scripted so that only what
// rollback does tells the outcomes apart.
func rollbackProject(t *testing.T, composeName, env string, created bool) restartOutcome {
	t.Helper()
	return rollbackProjectEndingLineage(t, composeName, env, created, nil)
}

// rollbackProjectEndingLineage is rollbackProject where ending the watermark
// lineage through the restored project fails with lineageErr, when it is not nil.
func rollbackProjectEndingLineage(t *testing.T, composeName, env string, created bool, lineageErr error) restartOutcome {
	t.Helper()
	root := t.TempDir()
	composeFile := writeProjectFile(t, root, composeName)
	override := filepath.Join(root, "docker-compose.bloodtrail.yml")
	if err := os.WriteFile(override, []byte("services: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	envPath := filepath.Join(root, ".env")
	if err := os.WriteFile(envPath, []byte(env), 0o600); err != nil {
		t.Fatal(err)
	}
	row := "pg"
	if err := (manifest.Manifest{ProjectDir: root, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage, OriginalDriverRow: &row,
		OverrideFile: override, PGUser: "bloodhound", PGDatabase: "bloodhound", EnvComposeFileCreated: created}).Save(root); err != nil {
		t.Fatal(err)
	}
	var apiHits atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer api.Close()

	restoreRow := "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('pg')"
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{}, Errors: map[string]error{}}
	for _, files := range [][]string{{composeFile}, {composeFile, override}} {
		project := "docker compose --project-directory " + root
		for _, f := range files {
			project += " -f " + f
		}
		project += " "
		psql := project + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
		fake.Outputs[psql+restoreRow] = []byte("INSERT 0 1\n")
		fake.Outputs[project+"up -d"] = nil
		scriptLineageEnd(fake, psql)
		if lineageErr != nil {
			fake.Errors[psql+endLineageSQL] = lineageErr
		}
	}
	var out bytes.Buffer
	err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &out},
		Options{ComposeFile: composeFile, ProjectDir: root, Yes: true, APIURL: api.URL, VerifyTimeout: time.Second})
	after, _ := os.ReadFile(envPath)

	res := restartOutcome{err: err, out: out.String(), env: string(after), calls: fake.Calls, apiHits: int(apiHits.Load())}
	res.manifestGone = !manifest.Exists(root)
	_, statErr := os.Stat(override)
	res.overrideGone = os.IsNotExist(statErr)
	for _, c := range fake.Calls {
		switch {
		case strings.HasSuffix(c, " up -d"):
			res.restarted = true
		case strings.HasSuffix(c, restoreRow):
			res.rowRestored = true
		case strings.HasSuffix(c, endLineageSQL):
			res.lineageEnded = true
		}
	}
	return res
}

// TestRollbackDoesNotRestartAProjectComposeLoadsFromAnotherDirectory covers
// rollback of an install whose restored project is one docker compose takes
// another project directory for than the .env's: the first file its
// COMPOSE_FILE lists (or, with no entry left, the compose file the install
// was given) is in a subdirectory. Rollback restarts with `--project-directory
// <the .env's directory>`, where relative paths in the compose files -- a
// bind mount such as ./pgdata -- resolve differently from the operator's own
// `docker compose up -d`, which is what recreated their database on an empty
// directory when install did the same. Rollback does everything else, does not
// restart, and says to.
func TestRollbackDoesNotRestartAProjectComposeLoadsFromAnotherDirectory(t *testing.T) {
	const entry = "COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=docker/compose.yml:docker-compose.bloodtrail.yml\n"
	for _, c := range []struct {
		name, composeName, env string
		created                bool
		want                   string // the .env afterwards
	}{
		{"an entry of the operator's own, first file in a subdirectory", "docker/compose.yml", entry, false,
			"COMPOSE_PROJECT_NAME=bh\nCOMPOSE_FILE=docker/compose.yml\n"},
		{"an entry the install created, which leaves none", "docker/compose.yml", entry, true,
			"COMPOSE_PROJECT_NAME=bh\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := rollbackProject(t, c.composeName, c.env, c.created)
			if res.err != nil {
				t.Fatalf("rollback: %v\ncalls:\n%s", res.err, strings.Join(res.calls, "\n"))
			}
			if res.restarted {
				t.Errorf("rollback restarted the project with --project-directory set to the .env's directory, where docker compose takes the compose file's:\n%s", strings.Join(res.calls, "\n"))
			}
			for what, done := range map[string]bool{
				"restore the driver row": res.rowRestored, "delete the override file": res.overrideGone,
				"end the watermark lineage": res.lineageEnded, "remove the manifest": res.manifestGone,
			} {
				if !done {
					t.Errorf("rollback did not %s", what)
				}
			}
			if res.env != c.want {
				t.Errorf(".env afterwards = %q, want %q", res.env, c.want)
			}
			for _, want := range []string{"your own `docker compose up -d`", "still running", "usual"} {
				if !strings.Contains(res.out, want) {
					t.Errorf("rollback did not tell the operator (%q missing):\n%s", want, res.out)
				}
			}
			// The lineage ends while BloodTrail still runs: a rebuild it adopts
			// before the restart reads the new lineage, and a file it saves
			// then stays adoptable after the stock image's uncounted writes.
			// The operator must end the lineage again once the original image
			// runs, or delete the file.
			for _, want := range []string{
				"ended BloodTrail's snapshot file watermark lineage",
				"Once the original image is running, end the lineage again",
				"`docker compose exec app-db psql -U bloodhound -d bloodhound -c 'update bloodtrail_watermark set lineage = gen_random_uuid() where id = 1'`",
				"delete BloodTrail's snapshot file",
				"any way other than `bloodtrail install`",
			} {
				if !strings.Contains(res.out, want) {
					t.Errorf("rollback did not tell the operator what the early lineage end leaves to them (%q missing):\n%s", want, res.out)
				}
			}
			// The API of a deployment that was not restarted is the old one's.
			if res.apiHits != 0 {
				t.Errorf("rollback waited for the API (%d requests) of a deployment it did not restart", res.apiHits)
			}
			if strings.Contains(res.out, "rolled back;") || !strings.Contains(res.out, "rolled back except for the restart") {
				t.Errorf("rollback reported itself finished without saying the restart is left:\n%s", res.out)
			}
		})
	}
}

// TestRollbackThatLeavesTheRestartKeepsItsManifestWhenTheLineageCannotBeEnded
// pins that the skipped restart changes nothing about what a rollback that
// could not finish leaves behind: the manifest stays, so the rerun the error
// asks for finds the installation.
func TestRollbackThatLeavesTheRestartKeepsItsManifestWhenTheLineageCannotBeEnded(t *testing.T) {
	res := rollbackProjectEndingLineage(t, "docker/compose.yml", "COMPOSE_FILE=docker/compose.yml:docker-compose.bloodtrail.yml\n", false, errors.New("psql: error: connection to server failed"))
	if res.err == nil || !strings.Contains(res.err.Error(), "watermark lineage") || !strings.Contains(res.err.Error(), "rerun `bloodtrail rollback`") {
		t.Fatalf("want the lineage failure with the rerun hint, got %v", res.err)
	}
	if strings.Contains(res.err.Error(), "running again") {
		t.Errorf("the error claims the original image runs although rollback did not restart it: %v", res.err)
	}
	if res.manifestGone {
		t.Error("the manifest is gone, so the rerun the error asks for would not find the installation")
	}
	if res.restarted {
		t.Error("rollback restarted the project")
	}
}

// TestRollbackRestartsAProjectComposeLoadsFromTheEnvDirectory is the case the
// one above must not touch: the restored project's directory is the .env's.
func TestRollbackRestartsAProjectComposeLoadsFromTheEnvDirectory(t *testing.T) {
	for _, c := range []struct {
		name, env string
		created   bool
		want      string
	}{
		{"an entry of the operator's own", "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n", false, "COMPOSE_FILE=docker-compose.yml\n"},
		{"an entry the install created", "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n", true, ""},
		{"no entry at all", "COMPOSE_PROJECT_NAME=bh\n", false, "COMPOSE_PROJECT_NAME=bh\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := rollbackProject(t, "docker-compose.yml", c.env, c.created)
			if res.err != nil {
				t.Fatalf("rollback: %v\ncalls:\n%s", res.err, strings.Join(res.calls, "\n"))
			}
			if !res.restarted || !res.lineageEnded || !res.manifestGone || res.apiHits == 0 {
				t.Errorf("rollback did not finish (restarted %v, lineage ended %v, manifest gone %v, API requests %d):\n%s", res.restarted, res.lineageEnded, res.manifestGone, res.apiHits, res.out)
			}
			if res.env != c.want {
				t.Errorf(".env afterwards = %q, want %q", res.env, c.want)
			}
			if strings.Contains(res.out, "your own `docker compose up -d`") {
				t.Errorf("rollback told the operator to restart although it did:\n%s", res.out)
			}
			if strings.Contains(res.out, "end the lineage again") {
				t.Errorf("rollback asked the operator to end the lineage although it ended it after the restart:\n%s", res.out)
			}
		})
	}
}
