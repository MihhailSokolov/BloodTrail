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
	"strings"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
	"github.com/MihhailSokolov/BloodTrail/internal/toolapi"
)

const upstreamImage = "docker.io/specterops/bloodhound:v9.6.0"

func composeConfigJSON(image, driver string) []byte {
	cfg := map[string]any{
		"name": "bh",
		"services": map[string]any{
			"app-db":     map[string]any{"image": "postgres:18", "environment": map[string]string{"POSTGRES_USER": "bloodhound", "POSTGRES_DB": "bloodhound"}},
			"graph-db":   map[string]any{"image": "neo4j:4.4.42", "environment": map[string]string{"NEO4J_AUTH": "neo4j/secret"}},
			"bloodhound": map[string]any{"image": image, "environment": map[string]string{"bhe_graph_driver": driver}},
		},
		"networks": map[string]any{"default": map[string]string{"name": "bh_default"}},
	}
	data, _ := json.Marshal(cfg)
	return data
}

// fakeToolAPI answers the migrator endpoints: one "migrating" poll, then idle.
func fakeToolAPI(t *testing.T) *httptest.Server {
	polls := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /pg-migration/neo-to-pg", "PUT /graph-db/switch/pg":
			w.WriteHeader(200)
		case "GET /pg-migration/status":
			polls++
			if polls == 1 {
				_, _ = w.Write([]byte(`{"state":"migrating"}`))
			} else {
				_, _ = w.Write([]byte(`{"state":"idle"}`))
			}
		default:
			t.Errorf("unexpected tool API call %s %s", r.Method, r.URL.Path)
		}
	}))
}

func setupProject(t *testing.T) (string, string) {
	dir := t.TempDir()
	composeFile := filepath.Join(dir, "docker-compose.yml")
	_ = os.WriteFile(composeFile, []byte("services: {}\n"), 0o644)
	return dir, composeFile
}

func TestInstallOnNeo4jDeployment(t *testing.T) {
	dir, composeFile := setupProject(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()
	tool := fakeToolAPI(t)
	defer tool.Close()

	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1":             []byte(""),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			base + "logs --no-color bloodhound":                             []byte("BloodTrail driver active version=test\n"),
		},
		Errors: map[string]error{},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)":         []byte("10|20\n"),
			psql + "create table if not exists database_switch": []byte("INSERT 0 1\n"),
			base + "exec -T graph-db cypher-shell":              []byte("count\n10\n"),
			"docker compose --project-directory " + dir + " -f " + composeFile + " -f " + filepath.Join(dir, "docker-compose.bloodtrail.yml") + " up -d": nil,
		},
	}
	var out bytes.Buffer
	deps := Deps{
		Runner:  fake,
		HTTP:    api.Client(),
		Out:     &out,
		Confirm: func(string) bool { t.Fatal("confirm must not be called with Yes"); return false },
		NewToolAPITransport: func(network string) toolapi.Transport {
			if network != "bh_default" {
				t.Errorf("network = %q", network)
			}
			return rewriteTransport{base: tool.URL, client: tool.Client()}
		},
	}
	opts := Options{ComposeFile: composeFile, Image: "ghcr.io/x/bt:v9.6.0-bt0.1.0", APIURL: api.URL, Yes: true,
		MigrationTimeout: time.Second, VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}

	if err := Install(context.Background(), deps, opts); err != nil {
		t.Fatalf("install failed: %v\noutput:\n%s", err, out.String())
	}

	// Override file and .env
	override, err := os.ReadFile(filepath.Join(dir, "docker-compose.bloodtrail.yml"))
	if err != nil || !strings.Contains(string(override), "image: ghcr.io/x/bt:v9.6.0-bt0.1.0") || !strings.Contains(string(override), "bhe_graph_driver=bloodtrail") {
		t.Fatalf("override not written correctly: %v\n%s", err, override)
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	if !strings.Contains(string(env), "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml") {
		t.Fatalf(".env = %q", env)
	}
	// Manifest
	m, err := manifest.Load(dir)
	if err != nil || m.OriginalImage != upstreamImage || m.OriginalDriverRow != nil || m.UpstreamTag != "v9.6.0" {
		t.Fatalf("manifest = %+v err=%v", m, err)
	}
	if _, err := os.Stat(filepath.Join(m.BackupDir, "app-db.dump")); err != nil {
		t.Fatalf("backup dump missing: %v", err)
	}
	// Order: backup before migration, migration before driver row, row before up
	idx := func(prefix string) int {
		for i, c := range fake.Calls {
			if strings.HasPrefix(c, prefix) {
				return i
			}
		}
		return -1
	}
	backupIdx := idx(base + "exec -T app-db pg_dump")
	driverRowIdx := idx(psql + "create table if not exists database_switch")
	upIdx := idx("docker compose --project-directory " + dir + " -f " + composeFile + " -f ")
	if backupIdx >= driverRowIdx || driverRowIdx >= upIdx {
		t.Fatalf("unexpected call order:\n%s", strings.Join(fake.Calls, "\n"))
	}
	if !strings.Contains(string(fake.Calls[len(fake.Calls)-1]), "logs --no-color bloodhound") {
		t.Fatalf("expected verification logs call last, got %v", fake.Calls[len(fake.Calls)-1])
	}
}

func TestInstallRefusesWhenAlreadyInstalled(t *testing.T) {
	dir, composeFile := setupProject(t)
	_ = manifest.Manifest{ProjectDir: dir}.Save(dir)
	err := Install(context.Background(), Deps{Runner: &dockerx.FakeRunner{}, Out: &bytes.Buffer{}}, Options{ComposeFile: composeFile, Yes: true})
	if err == nil || !strings.Contains(err.Error(), "already installed") {
		t.Fatalf("expected already-installed error, got %v", err)
	}
}

func TestRollbackRestoresEverything(t *testing.T) {
	dir, composeFile := setupProject(t)
	_ = os.WriteFile(filepath.Join(dir, "docker-compose.bloodtrail.yml"), []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, ".env"), []byte("COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"), 0o644)
	row := "neo4j"
	_ = manifest.Manifest{ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage, OriginalDriverRow: &row,
		OverrideFile: filepath.Join(dir, "docker-compose.bloodtrail.yml")}.Save(dir)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer api.Close()

	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                       composeConfigJSON(upstreamImage, "neo4j"),
			base + "up -d --remove-orphans":                     nil,
			psql + "select driver from database_switch limit 1": []byte("bloodtrail\n"),
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)":         []byte("10|20\n"),
			psql + "create table if not exists database_switch": []byte("INSERT 0 1\n"),
		},
	}
	if err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, Options{ComposeFile: composeFile, APIURL: api.URL, VerifyTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "docker-compose.bloodtrail.yml")); !os.IsNotExist(err) {
		t.Fatal("override file should be removed")
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	if strings.Contains(string(env), "bloodtrail") {
		t.Fatalf(".env still references the override: %q", env)
	}
	if manifest.Exists(dir) {
		t.Fatal("manifest should be removed after rollback")
	}
	if !fake.Called(psql + "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('neo4j')") {
		t.Fatalf("driver row not restored:\n%s", strings.Join(fake.Calls, "\n"))
	}
}

// rewriteTransport sends tool API calls meant for http://bloodhound:2112 to the test server.
type rewriteTransport struct {
	base   string
	client *http.Client
}

func (s rewriteTransport) Do(ctx context.Context, method, url string) (int, []byte, error) {
	path := url[strings.Index(url, "2112")+4:]
	return toolapi.HTTPTransport{Client: s.client}.Do(ctx, method, s.base+path)
}
