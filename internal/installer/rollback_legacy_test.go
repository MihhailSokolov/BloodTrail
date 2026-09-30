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

// legacyRollback is one rollback of an install made by v0.1.0, v0.1.1 or
// v0.1.2: those recorded that they had created the COMPOSE_FILE entry
// (EnvComposeFileCreated) but not the list they wrote into it, which is what
// tells rollback later whether the operator has added to it. So the manifest
// says "created", and the .env says whatever it says now; backup, when not
// nil, is the .env the install copied into its backup directory before it
// changed anything.
type legacyRollback struct {
	env    string  // the .env when rollback runs
	backup *string // the .env the install backed up; nil: there is no backup directory to read
	// backupWithoutEnv makes the backup directory exist without an .env in it,
	// which is what an install of a project that had none leaves.
	backupWithoutEnv bool
}

// legacyRollbackResult is what a legacyRollback did.
type legacyRollbackResult struct {
	env      string   // the .env afterwards
	err      error    // what rollback failed with
	out      string   // what it said
	restart  []string // the files of the project it restarted, in order, by base name
	rowFiles []string // the files of the project it restored the driver row through
	calls    []string // every command it ran
}

// run saves the manifest and files that go with it and runs rollback with
// every plausible project scripted, so that only what rollback does tells the
// outcomes apart.
func (l legacyRollback) run(t *testing.T) legacyRollbackResult {
	t.Helper()
	dir, composeFile := setupProject(t)
	tls := filepath.Join(dir, "tls.yml")
	override := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	for _, f := range []string{tls, override} {
		if err := os.WriteFile(f, []byte("services: {}\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	envPath := filepath.Join(dir, ".env")
	if err := os.WriteFile(envPath, []byte(l.env), 0o600); err != nil {
		t.Fatal(err)
	}
	row := "neo4j"
	m := manifest.Manifest{InstallerVersion: "0.1.2", ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage,
		OriginalDriverRow: &row, OverrideFile: override, PGUser: "bloodhound", PGDatabase: "bloodhound", EnvComposeFileCreated: true}
	if l.backup != nil || l.backupWithoutEnv {
		m.BackupDir = filepath.Join(dir, ".bloodtrail", "backups", "20260902T000000Z")
		if err := os.MkdirAll(m.BackupDir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if l.backup != nil {
		if err := os.WriteFile(filepath.Join(m.BackupDir, ".env"), []byte(*l.backup), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Save(dir); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer api.Close()

	restoreRow := "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('neo4j')"
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{}}
	for _, files := range [][]string{{composeFile}, {composeFile, tls}, {composeFile, override}, {composeFile, tls, override}, {composeFile, override, tls}} {
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
	var out bytes.Buffer
	err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &out},
		Options{ComposeFile: composeFile, Yes: true, APIURL: api.URL, VerifyTimeout: time.Second})
	after, _ := os.ReadFile(envPath)

	res := legacyRollbackResult{env: string(after), err: err, out: out.String(), calls: fake.Calls}
	for _, c := range fake.Calls {
		switch {
		case strings.HasSuffix(c, " up -d"):
			res.restart = composeFilesOf(c, dir)
		case strings.HasSuffix(c, restoreRow):
			res.rowFiles = composeFilesOf(c, dir)
		}
	}
	return res
}

// composeFilesOf returns the base names of the -f files of a compose command.
func composeFilesOf(command, dir string) []string {
	var files []string
	fields := strings.Fields(command)
	for i, f := range fields {
		if f == "-f" && i+1 < len(fields) {
			files = append(files, strings.TrimPrefix(fields[i+1], dir+string(filepath.Separator)))
		}
	}
	return files
}

func strPtr(s string) *string { return &s }

// TestRollbackOfAnInstallThatDidNotRecordItsList is the rollback of what
// v0.1.0 to v0.1.2 left, in every state their manifests leave open. Those
// manifests say only that the install created the COMPOSE_FILE entry, and
// rollback then deleted the whole line whatever it listed: a file the operator
// had added since (tls.yml, say) silently dropped out of their project, and
// out of rollback's own restart. Now what the .env the install backed up said
// decides, and without it the line goes only while it names nothing beyond
// what those versions wrote -- the compose file and the override compose finds
// beside it -- and otherwise just the installer's override comes out.
func TestRollbackOfAnInstallThatDidNotRecordItsList(t *testing.T) {
	const installed = "docker-compose.yml:docker-compose.bloodtrail.yml"
	for _, c := range []struct {
		name    string
		rollbck legacyRollback
		want    string   // .env afterwards
		restart []string // the files of the project rollback restarts
		note    string   // what rollback says about the entry, when it does
	}{
		{"an entry nothing was added to, with a backup that had none",
			legacyRollback{env: "A=b\nCOMPOSE_FILE=" + installed + "\n", backup: strPtr("A=b\n")},
			"A=b\n", []string{"docker-compose.yml"}, ""},
		{"an entry nothing was added to, for a project that had no .env",
			legacyRollback{env: "COMPOSE_FILE=" + installed + "\n", backupWithoutEnv: true},
			"", []string{"docker-compose.yml"}, ""},
		{"an entry nothing was added to, without a backup to read",
			legacyRollback{env: "A=b\nCOMPOSE_FILE=" + installed + "\n"},
			"A=b\n", []string{"docker-compose.yml"}, ""},
		{"a file added after the override, with a backup that had no entry",
			legacyRollback{env: "COMPOSE_FILE=" + installed + ":tls.yml\n", backup: strPtr("A=b\n")},
			"COMPOSE_FILE=docker-compose.yml:tls.yml\n", []string{"docker-compose.yml", "tls.yml"}, "only docker-compose.bloodtrail.yml came out"},
		{"a file added before the override",
			legacyRollback{env: "COMPOSE_FILE=docker-compose.yml:tls.yml:docker-compose.bloodtrail.yml\n", backup: strPtr("A=b\n")},
			"COMPOSE_FILE=docker-compose.yml:tls.yml\n", []string{"docker-compose.yml", "tls.yml"}, "only docker-compose.bloodtrail.yml came out"},
		{"a file added, without a backup to read",
			legacyRollback{env: "A=b\nCOMPOSE_FILE=" + installed + ":tls.yml\nC=d\n"},
			"A=b\nCOMPOSE_FILE=docker-compose.yml:tls.yml\nC=d\n", []string{"docker-compose.yml", "tls.yml"}, "only docker-compose.bloodtrail.yml came out"},
		{"the override compose discovered, which the install wrote too",
			legacyRollback{env: "COMPOSE_FILE=docker-compose.yml:docker-compose.override.yml:docker-compose.bloodtrail.yml\n", backup: strPtr("")},
			"", []string{"docker-compose.yml"}, ""},

		// What v0.1.0 and v0.1.1 left over an entry they did not recognise:
		// they looked only for lines starting COMPOSE_FILE=, took the operator's
		// `export COMPOSE_FILE=...` for missing, recorded the entry as created and
		// appended a second line, which compose reads last. Rollback refused
		// ("keep one"); it now takes out the line the install appended.
		{"a line appended beside an exported entry, with the backup",
			legacyRollback{env: "export COMPOSE_FILE=docker-compose.yml:tls.yml\nCOMPOSE_FILE=" + installed + "\n", backup: strPtr("export COMPOSE_FILE=docker-compose.yml:tls.yml\n")},
			"export COMPOSE_FILE=docker-compose.yml:tls.yml\n", []string{"docker-compose.yml", "tls.yml"}, "removed the COMPOSE_FILE line an earlier install appended"},
		{"a line appended beside an exported entry, without a backup to read",
			legacyRollback{env: "A=b\nexport COMPOSE_FILE=docker-compose.yml:tls.yml\nC=d\nCOMPOSE_FILE=" + installed + "\n"},
			"A=b\nexport COMPOSE_FILE=docker-compose.yml:tls.yml\nC=d\n", []string{"docker-compose.yml", "tls.yml"}, "removed the COMPOSE_FILE line an earlier install appended"},
		{"a line appended before an exported entry that a later edit put after it",
			legacyRollback{env: "COMPOSE_FILE=" + installed + "\nCOMPOSE_FILE: docker-compose.yml:tls.yml\n"},
			"COMPOSE_FILE: docker-compose.yml:tls.yml\n", []string{"docker-compose.yml", "tls.yml"}, "removed the COMPOSE_FILE line an earlier install appended"},

		// The operator's answer to that refusal -- keep their own line -- and a
		// rerun: the manifest still says "created", and rollback used to delete
		// the operator's own line, tls.yml and all.
		{"the operator's own entry kept, with the backup",
			legacyRollback{env: "export COMPOSE_FILE=docker-compose.yml:tls.yml\n", backup: strPtr("export COMPOSE_FILE=docker-compose.yml:tls.yml\n")},
			"export COMPOSE_FILE=docker-compose.yml:tls.yml\n", []string{"docker-compose.yml", "tls.yml"}, ""},
		{"the operator's own entry kept, without a backup to read",
			legacyRollback{env: "export COMPOSE_FILE=docker-compose.yml:tls.yml\n"},
			"export COMPOSE_FILE=docker-compose.yml:tls.yml\n", []string{"docker-compose.yml", "tls.yml"}, ""},
		{"the operator's own entry, naming just the compose file, kept without a backup to read",
			legacyRollback{env: "export COMPOSE_FILE=docker-compose.yml\n"},
			"export COMPOSE_FILE=docker-compose.yml\n", []string{"docker-compose.yml"}, ""},

		// An entry the backup shows was the operator's, extended with the override.
		{"an entry the backup had, extended by the install",
			legacyRollback{env: "COMPOSE_FILE=docker-compose.yml:tls.yml:docker-compose.bloodtrail.yml\n", backup: strPtr("export COMPOSE_FILE=docker-compose.yml:tls.yml\n")},
			"COMPOSE_FILE=docker-compose.yml:tls.yml\n", []string{"docker-compose.yml", "tls.yml"}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := c.rollbck.run(t)
			if res.err != nil {
				t.Fatalf("rollback: %v\ncalls:\n%s", res.err, strings.Join(res.calls, "\n"))
			}
			if res.env != c.want {
				t.Errorf(".env afterwards = %q, want %q", res.env, c.want)
			}
			if got := strings.Join(res.restart, " "); got != strings.Join(c.restart, " ") {
				t.Errorf("rollback restarted the project made of %q, want %q", got, c.restart)
			}
			if c.note == "" && strings.Contains(res.out, "COMPOSE_FILE") {
				t.Errorf("rollback had something to say about the entry it should not have:\n%s", res.out)
			}
			if c.note != "" && !strings.Contains(res.out, c.note) {
				t.Errorf("rollback did not say %q:\n%s", c.note, res.out)
			}
		})
	}
}

// TestRollbackRefusesAnEnvItCannotTellHowToRestore covers what stays refused,
// and that it is refused before anything is changed: two COMPOSE_FILE lines of
// which the .env does not show which one an earlier install appended. The
// message says which lines and what to do.
func TestRollbackRefusesAnEnvItCannotTellHowToRestore(t *testing.T) {
	for _, c := range []struct{ name, env string }{
		{"neither line names the override", "COMPOSE_FILE=docker-compose.yml\nexport COMPOSE_FILE=docker-compose.yml:tls.yml\n"},
		{"both lines name the override", "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\nCOMPOSE_FILE=tls.yml:docker-compose.bloodtrail.yml\n"},
		{"the line that names it lists more than an install wrote", "export COMPOSE_FILE=tls.yml\nCOMPOSE_FILE=docker-compose.yml:tls.yml:docker-compose.bloodtrail.yml\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := legacyRollback{env: c.env}.run(t)
			if res.err == nil || !strings.Contains(res.err.Error(), "lines 1 and 2") || !strings.Contains(res.err.Error(), "docker-compose.bloodtrail.yml") {
				t.Fatalf("want a refusal naming the lines and the override, got %v", res.err)
			}
			if res.env != c.env {
				t.Errorf("a refused rollback changed .env to %q", res.env)
			}
			for _, call := range res.calls {
				if strings.Contains(call, " up -d") || strings.Contains(call, "delete from database_switch") {
					t.Errorf("a refused rollback changed the deployment: %s", call)
				}
			}
		})
	}
}
