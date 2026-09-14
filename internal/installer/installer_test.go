// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// testNeo4jPassword is built from a plain identifier rather than written as
// a NEO4J_PASSWORD=literal or NEO4J_AUTH=neo4j/literal assignment anywhere
// below, so secret scanners do not mistake this fixture for a real
// credential.
const testNeo4jPassword = "test-only"

// neo4jNodeCount and neo4jEdgeCount are the exact commands the inventory runs
// to count the Neo4j graph, minus the compose prefix each test builds.
const (
	neo4jNodeCount = "exec -T -e NEO4J_PASSWORD=" + testNeo4jPassword + " graph-db cypher-shell -u neo4j --format plain MATCH (n) RETURN count(n)"
	neo4jEdgeCount = "exec -T -e NEO4J_PASSWORD=" + testNeo4jPassword + " graph-db cypher-shell -u neo4j --format plain MATCH ()-[r]->() RETURN count(r)"
)

func composeConfigJSON(image, driver string) []byte {
	cfg := map[string]any{
		"name": "bh",
		"services": map[string]any{
			"app-db":     map[string]any{"image": "postgres:18", "environment": map[string]string{"POSTGRES_USER": "bloodhound", "POSTGRES_DB": "bloodhound"}},
			"graph-db":   map[string]any{"image": "neo4j:4.4.42", "environment": map[string]string{"NEO4J_AUTH": "neo4j/" + testNeo4jPassword}},
			"bloodhound": map[string]any{"image": image, "environment": map[string]string{"bhe_graph_driver": driver}},
		},
		"networks": map[string]any{"default": map[string]string{"name": "bh_default"}},
	}
	data, _ := json.Marshal(cfg)
	return data
}

// fakeToolAPI answers the migrator endpoints, reporting "migrating" for the
// first migratingPolls status calls and idle after that. Each such poll costs
// a real migrationPoll wait, so only the test that has to prove the installer
// waits asks for one. *manifestExistsAtFirstCall records whether the install
// manifest already existed by the time the very first tool-API request
// arrived, proving the manifest is saved before migration begins.
func fakeToolAPI(t *testing.T, dir string, manifestExistsAtFirstCall *bool, migratingPolls int) *httptest.Server {
	polls := 0
	first := true
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first {
			*manifestExistsAtFirstCall = manifest.Exists(dir)
			first = false
		}
		switch r.Method + " " + r.URL.Path {
		case "PUT /pg-migration/neo-to-pg", "PUT /graph-db/switch/pg":
			w.WriteHeader(200)
		case "GET /pg-migration/status":
			polls++
			if polls <= migratingPolls {
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

// setRowSQL is the exact statement dbswitch.Store.Set sends to switch the
// active driver to "bloodtrail".
const setRowSQL = "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('bloodtrail')"

func TestInstallOnNeo4jDeployment(t *testing.T) {
	dir, composeFile := setupProject(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()
	var manifestExistsBeforeMigration bool
	tool := fakeToolAPI(t, dir, &manifestExistsBeforeMigration, 1)
	defer tool.Close()

	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1":             []byte(""),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			// Read once for migrator failures, before the override exists,
			// and again through the whole project when verifying.
			base + "logs --no-color bloodhound":                         []byte("BloodTrail driver active version=test\n"),
			base + "-f " + overridePath + " logs --no-color bloodhound": []byte("BloodTrail driver active version=test\n"),
			"docker image inspect " + image:                             []byte(""),
			psql + setRowSQL:                                            []byte("INSERT 0 1\n"),
			base + "-f " + overridePath + " up -d":                      nil,
			base + neo4jNodeCount:                                       []byte("count\n10\n"),
			base + neo4jEdgeCount:                                       []byte("count\n20\n"),
			// The migration reaches the tool API through a curl container,
			// which is fetched before anything is changed.
			"docker pull " + toolapi.CurlImage: nil,
		},
		Errors: map[string]error{
			"docker image inspect " + toolapi.CurlImage: errors.New("no such image"),
		},
		Sequences: map[string][][]byte{
			// PostgreSQL holds no graph before the migration and everything
			// Neo4j held after it.
			psql + "select (select count(*) from node)": {[]byte("0|0\n")},
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)": []byte("10|20\n"),
		},
	}
	scriptContainerEpoch(fake, base)
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
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		MigrationTimeout: time.Second, VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}

	if err := Install(context.Background(), deps, opts); err != nil {
		t.Fatalf("install failed: %v\noutput:\n%s", err, out.String())
	}

	if !manifestExistsBeforeMigration {
		t.Fatal("manifest must be saved before migration begins, so a failed migration can still be rolled back")
	}

	// Override file and .env
	override, err := os.ReadFile(filepath.Join(dir, "docker-compose.bloodtrail.yml"))
	if err != nil || !strings.Contains(string(override), "image: "+image) || !strings.Contains(string(override), "bhe_graph_driver=bloodtrail") {
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
	if m.PGUser != "bloodhound" || m.PGDatabase != "bloodhound" {
		t.Fatalf("manifest PG credentials = %+v", m)
	}
	if _, err := os.Stat(filepath.Join(m.BackupDir, "app-db.dump")); err != nil {
		t.Fatalf("backup dump missing: %v", err)
	}
	// Order: backup before driver row, row before up; logs last.
	idx := func(prefix string) int {
		for i, c := range fake.Calls {
			if strings.HasPrefix(c, prefix) {
				return i
			}
		}
		return -1
	}
	if curlIdx, backupIdx := idx("docker pull "+toolapi.CurlImage), idx(base+"exec -T app-db pg_dump"); curlIdx == -1 || curlIdx >= backupIdx {
		t.Fatalf("the curl image must be fetched before the backup:\n%s", strings.Join(fake.Calls, "\n"))
	}
	backupIdx := idx(base + "exec -T app-db pg_dump")
	driverRowIdx := idx(psql + setRowSQL)
	upIdx := idx("docker compose --project-directory " + dir + " -f " + composeFile + " -f ")
	if backupIdx >= driverRowIdx || driverRowIdx >= upIdx {
		t.Fatalf("unexpected call order:\n%s", strings.Join(fake.Calls, "\n"))
	}
	if !strings.Contains(string(fake.Calls[len(fake.Calls)-1]), "logs --no-color bloodhound") {
		t.Fatalf("expected verification logs call last, got %v", fake.Calls[len(fake.Calls)-1])
	}
}

// migrationFake scripts a Neo4j deployment through a migration that the tool
// API reports as clean. neo4j and pg are the "nodes|edges" the two databases
// report afterwards. preLogs and postLogs are what the bloodhound service's
// log held before and after the migration: the installer reads it once
// immediately before starting the migration and again afterwards, so both
// have to be scripted even when a test does not care about either.
func migrationFake(dir, composeFile, image, neo4jNodes, neo4jEdges, pg, preLogs, postLogs string) *dockerx.FakeRunner {
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1":             []byte(""),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			"docker image inspect " + image:                                 []byte(""),
			"docker image inspect " + toolapi.CurlImage:                     []byte(""),
			base + neo4jNodeCount:                                           []byte("count\n" + neo4jNodes + "\n"),
			base + neo4jEdgeCount:                                           []byte("count\n" + neo4jEdges + "\n"),
		},
		Sequences: map[string][][]byte{
			psql + "select (select count(*) from node)": {[]byte("0|0\n")},
			base + "logs --no-color bloodhound":         {[]byte(preLogs), []byte(postLogs)},
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)": []byte(pg + "\n"),
		},
	}
	scriptContainerEpoch(fake, base)
	return fake
}

// scriptContainerEpoch scripts the container-identity probe the installer
// runs on each side of the migration window: a stable container id and
// start time, i.e. no restart. A test proving restart detection overrides
// the inspect key with a Sequences entry instead.
func scriptContainerEpoch(fake *dockerx.FakeRunner, base string) {
	fake.Outputs[base+"ps --format json bloodhound"] = []byte(`{"ID":"cafe01","Image":"` + upstreamImage + `"}`)
	fake.Outputs["docker inspect -f {{.Id}} {{.State.StartedAt}} cafe01"] = []byte("cafe01 2026-09-02T00:00:00Z\n")
}

// runMigrationInstall drives Install through the migration with fake and
// returns whatever it failed with.
func runMigrationInstall(t *testing.T, dir, composeFile, image string, fake *dockerx.FakeRunner) error {
	t.Helper()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()
	tool := fakeToolAPI(t, dir, new(bool), 0)
	defer tool.Close()

	deps := Deps{
		Runner: fake,
		HTTP:   api.Client(),
		Out:    &bytes.Buffer{},
		NewToolAPITransport: func(string) toolapi.Transport {
			return rewriteTransport{base: tool.URL, client: tool.Client()}
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		MigrationTimeout: time.Second, VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
	return Install(context.Background(), deps, opts)
}

// TestInstallAbortsWhenNeo4jRecountFails pins the fail-closed verification:
// without a usable post-migration Neo4j count there is nothing to compare
// the migrated graph against, and the old behavior -- passing on any
// nonzero PostgreSQL node count -- silently blessed partial migrations. A
// failed recount is now itself the abort.
func TestInstallAbortsWhenNeo4jRecountFails(t *testing.T) {
	dir, composeFile := setupProject(t)
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	fake := migrationFake(dir, composeFile, image, "10", "20", "10|20", "", "")
	delete(fake.Outputs, base+neo4jNodeCount)
	delete(fake.Outputs, base+neo4jEdgeCount)

	err := runMigrationInstall(t, dir, composeFile, image, fake)
	if err == nil || !strings.Contains(err.Error(), "cannot be verified against its source") || !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("expected a fail-closed recount error mentioning rollback, got %v", err)
	}
	if !manifest.Exists(dir) {
		t.Fatal("manifest should still exist so `bloodtrail rollback` can undo the partial install")
	}
}

// TestInstallAbortsWhenBloodhoundRestartsDuringMigration: a bloodhound
// container restart resets its migrator to "idle", which WaitUntilIdle
// cannot tell apart from completion -- the installer compares the
// container's identity (id + start time) across the migration window and
// must refuse the reported completion when it changed.
func TestInstallAbortsWhenBloodhoundRestartsDuringMigration(t *testing.T) {
	dir, composeFile := setupProject(t)
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	fake := migrationFake(dir, composeFile, image, "10", "20", "10|20", "", "")
	// Same container id, different start time: a plain restart.
	delete(fake.Outputs, "docker inspect -f {{.Id}} {{.State.StartedAt}} cafe01")
	fake.Sequences["docker inspect -f {{.Id}} {{.State.StartedAt}} cafe01"] = [][]byte{
		[]byte("cafe01 2026-09-02T00:00:00Z\n"),
		[]byte("cafe01 2026-09-02T00:05:00Z\n"),
	}

	err := runMigrationInstall(t, dir, composeFile, image, fake)
	if err == nil || !strings.Contains(err.Error(), "restarted during the migration") || !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("expected a restart-detection error mentioning rollback, got %v", err)
	}
	if !manifest.Exists(dir) {
		t.Fatal("manifest should still exist so `bloodtrail rollback` can undo the partial install")
	}
}

// TestInstallAbortsWhenMigratedCountsDoNotMatchNeo4j covers the failure the
// zero-node check misses: the nodes arrive but the edges do not.
func TestInstallAbortsWhenMigratedCountsDoNotMatchNeo4j(t *testing.T) {
	dir, composeFile := setupProject(t)
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	fake := migrationFake(dir, composeFile, image, "10", "20", "10|0", "", "")

	err := runMigrationInstall(t, dir, composeFile, image, fake)
	if err == nil || !strings.Contains(err.Error(), "10 nodes and 0 edges") || !strings.Contains(err.Error(), "10 nodes and 20 edges") {
		t.Fatalf("expected an error naming both counts, got %v", err)
	}
	if !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("expected the rollback hint, got %v", err)
	}
}

// TestInstallSucceedsWhenPostgresMatchesFreshNeo4jCountsButNotStaleInventory
// covers BloodHound continuing to ingest into Neo4j between the inventory
// read (taken before the confirmation prompt and the backup) and the
// migration: comparing what arrived in PostgreSQL against that stale
// inventory count would refuse a clean migration just because Neo4j kept
// growing in the meantime, so the installer must re-read Neo4j immediately
// before migrating (a second, distinctly scripted cypher-shell call here)
// and compare against that instead.
func TestInstallSucceedsWhenPostgresMatchesFreshNeo4jCountsButNotStaleInventory(t *testing.T) {
	dir, composeFile := setupProject(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()

	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1":             []byte(""),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			base + "logs --no-color bloodhound":                             []byte(""),
			base + "-f " + overridePath + " logs --no-color bloodhound":     []byte("BloodTrail driver active version=test\n"),
			"docker image inspect " + image:                                 []byte(""),
			"docker image inspect " + toolapi.CurlImage:                     []byte(""),
			psql + setRowSQL:                       []byte("INSERT 0 1\n"),
			base + "-f " + overridePath + " up -d": nil,
		},
		Sequences: map[string][][]byte{
			psql + "select (select count(*) from node)": {[]byte("0|0\n")},
			// Neo4j gained 2 nodes and 5 edges between the inventory read
			// and the migration; PostgreSQL ends up with exactly what
			// Neo4j held right before the migration (12|25), which does
			// not match the stale inventory count (10|20) an equality
			// check against inv.Nodes/inv.Edges would have demanded.
			base + neo4jNodeCount: {[]byte("count\n10\n"), []byte("count\n12\n")},
			base + neo4jEdgeCount: {[]byte("count\n20\n"), []byte("count\n25\n")},
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)": []byte("12|25\n"),
		},
	}
	tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /pg-migration/neo-to-pg", "PUT /graph-db/switch/pg":
			w.WriteHeader(200)
		case "GET /pg-migration/status":
			_, _ = w.Write([]byte(`{"state":"idle"}`))
		default:
			t.Errorf("unexpected tool API call %s %s", r.Method, r.URL.Path)
		}
	}))
	defer tool.Close()

	scriptContainerEpoch(fake, base)
	deps := Deps{
		Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{},
		NewToolAPITransport: func(string) toolapi.Transport {
			return rewriteTransport{base: tool.URL, client: tool.Client()}
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		MigrationTimeout: time.Second, VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}

	if err := Install(context.Background(), deps, opts); err != nil {
		t.Fatalf("install should succeed once PostgreSQL meets the fresh Neo4j count, even though it differs from the stale inventory count: %v", err)
	}
}

// TestInstallRefusesWhenPostgresFallsBelowFreshNeo4jCounts is the other side
// of the same fix: exceeding the stale inventory count is not proof the
// migration is complete once Neo4j kept ingesting past it.
func TestInstallRefusesWhenPostgresFallsBelowFreshNeo4jCounts(t *testing.T) {
	dir, composeFile := setupProject(t)
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	// PostgreSQL (15|25) clears the stale inventory count (10|20) but falls
	// short of what Neo4j holds right before the migration (15|30).
	fake := migrationFake(dir, composeFile, image, "10", "20", "15|25", "", "")
	delete(fake.Outputs, base+neo4jNodeCount)
	delete(fake.Outputs, base+neo4jEdgeCount)
	fake.Sequences[base+neo4jNodeCount] = [][]byte{[]byte("count\n10\n"), []byte("count\n15\n")}
	fake.Sequences[base+neo4jEdgeCount] = [][]byte{[]byte("count\n20\n"), []byte("count\n30\n")}

	err := runMigrationInstall(t, dir, composeFile, image, fake)
	if err == nil || !strings.Contains(err.Error(), "15 nodes and 25 edges") || !strings.Contains(err.Error(), "15 nodes and 30 edges") {
		t.Fatalf("expected an error naming both the migrated and the fresh Neo4j counts, got %v", err)
	}
	if !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("expected the rollback hint, got %v", err)
	}
}

func TestInstallAbortsWhenTheMigratorLogsAFailure(t *testing.T) {
	for _, marker := range []string{"Failed importing", "Unable to migrate", "Unable to assert"} {
		t.Run(marker, func(t *testing.T) {
			dir, composeFile := setupProject(t)
			image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
			postLogs := "starting migration\n" + marker + " node objectid=S-1-5-21: duplicate key value\ndone\n"
			// The log holds nothing before the migration, so the whole marker
			// line is new; the installer must catch it.
			fake := migrationFake(dir, composeFile, image, "10", "20", "10|20", "", postLogs)

			err := runMigrationInstall(t, dir, composeFile, image, fake)
			if err == nil || !strings.Contains(err.Error(), marker) {
				t.Fatalf("expected the offending log line in the error, got %v", err)
			}
			if !strings.Contains(err.Error(), "rollback") {
				t.Fatalf("expected the rollback hint, got %v", err)
			}
			if fake.Called("docker compose --project-directory " + dir + " -f " + composeFile + " -f ") {
				t.Fatalf("the image must not be swapped in after a failed migration:\n%s", strings.Join(fake.Calls, "\n"))
			}
		})
	}
}

// TestInstallIgnoresAPreExistingMigratorFailureLog covers the case
// migratorFailureInLogs exists to avoid: the bloodhound service is not
// recreated by a failed install followed by a rollback, so its log can still
// carry a failure line from an earlier attempt when a later install runs the
// migration again. That earlier line must not block an install whose own
// migration logged nothing new.
func TestInstallIgnoresAPreExistingMigratorFailureLog(t *testing.T) {
	dir, composeFile := setupProject(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()

	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	preLogs := "starting up\nFailed importing node objectid=OLD: duplicate key value\nready\n"
	postLogs := preLogs + "migration progressing\nmigration complete\n"
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1":             []byte(""),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			base + "-f " + overridePath + " logs --no-color bloodhound":     []byte("BloodTrail driver active version=test\n"),
			"docker image inspect " + image:                                 []byte(""),
			"docker image inspect " + toolapi.CurlImage:                     []byte(""),
			psql + setRowSQL:                       []byte("INSERT 0 1\n"),
			base + "-f " + overridePath + " up -d": nil,
			base + neo4jNodeCount:                  []byte("count\n10\n"),
			base + neo4jEdgeCount:                  []byte("count\n20\n"),
		},
		Sequences: map[string][][]byte{
			// PostgreSQL holds no graph before the migration (so the
			// pre-existing log line is the only thing that could wrongly
			// block this install) and everything Neo4j held after it.
			psql + "select (select count(*) from node)": {[]byte("0|0\n")},
			base + "logs --no-color bloodhound":         {[]byte(preLogs), []byte(postLogs)},
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)": []byte("10|20\n"),
		},
	}
	tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /pg-migration/neo-to-pg", "PUT /graph-db/switch/pg":
			w.WriteHeader(200)
		case "GET /pg-migration/status":
			_, _ = w.Write([]byte(`{"state":"idle"}`))
		default:
			t.Errorf("unexpected tool API call %s %s", r.Method, r.URL.Path)
		}
	}))
	defer tool.Close()

	scriptContainerEpoch(fake, base)
	deps := Deps{
		Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{},
		NewToolAPITransport: func(string) toolapi.Transport {
			return rewriteTransport{base: tool.URL, client: tool.Client()}
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		MigrationTimeout: time.Second, VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}

	if err := Install(context.Background(), deps, opts); err != nil {
		t.Fatalf("a failure line already present before the migration must not block this install: %v", err)
	}
}

// TestInstallRefusesMigrationIntoPopulatedPostgres covers the case that makes
// a second install after a rollback dangerous: the graph an earlier migration
// left in PostgreSQL is still there, BloodHound's migrator would not replace
// it, and the installer would otherwise switch the deployment onto that stale
// copy.
func TestInstallRefusesMigrationIntoPopulatedPostgres(t *testing.T) {
	dir, composeFile := setupProject(t)
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1":             []byte(""),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			"docker image inspect " + image:                                 []byte(""),
			"docker image inspect " + toolapi.CurlImage:                     []byte(""),
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)":                                        []byte("10|20\n"),
			base + "exec -T -e NEO4J_PASSWORD=" + testNeo4jPassword + " graph-db cypher-shell": []byte("count\n10\n"),
		},
	}
	deps := Deps{
		Runner: fake, Out: &bytes.Buffer{},
		NewToolAPITransport: func(string) toolapi.Transport {
			t.Fatal("the migration must not be started when PostgreSQL already holds a graph")
			return nil
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, Yes: true,
		Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}

	err := Install(context.Background(), deps, opts)
	if err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("expected a refusal, got %v", err)
	}
	// Install itself refuses to run again while this install's manifest is
	// still on disk, so the way out has to start with a rollback, not a
	// straight rerun with --replace-postgres-graph.
	if !strings.Contains(err.Error(), "rollback") || !strings.Contains(err.Error(), "--replace-postgres-graph") {
		t.Fatalf("the refusal must tell the operator to roll back before replacing the graph, got %v", err)
	}
	if fake.Called(psql + "truncate") {
		t.Fatalf("nothing may be cleared without --replace-postgres-graph:\n%s", strings.Join(fake.Calls, "\n"))
	}
	if !manifest.Exists(dir) {
		t.Fatal("the manifest must survive so `bloodtrail rollback` can clear the backup this install took")
	}
}

func TestInstallReplacesPostgresGraphWhenAsked(t *testing.T) {
	dir, composeFile := setupProject(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()

	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1":             []byte(""),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			// Read once for migrator failures, before the override exists,
			// and again through the whole project when verifying.
			base + "logs --no-color bloodhound":                         []byte("BloodTrail driver active version=test\n"),
			base + "-f " + overridePath + " logs --no-color bloodhound": []byte("BloodTrail driver active version=test\n"),
			psql + "truncate table edge, node":                          []byte("TRUNCATE TABLE\n"),
			"docker image inspect " + image:                             []byte(""),
			"docker image inspect " + toolapi.CurlImage:                 []byte(""),
			psql + setRowSQL:                       []byte("INSERT 0 1\n"),
			base + "-f " + overridePath + " up -d": nil,
			base + neo4jNodeCount:                  []byte("count\n10\n"),
			base + neo4jEdgeCount:                  []byte("count\n20\n"),
		},
		Prefixes: map[string][]byte{
			// Populated before the migration (which is what triggers the
			// clear) and holding the same graph again afterwards.
			psql + "select (select count(*) from node)": []byte("10|20\n"),
		},
	}

	// The migrator must only be started once the stale graph is gone.
	clearedBeforeMigration := false
	firstToolCall := true
	tool := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if firstToolCall {
			clearedBeforeMigration = fake.Called(psql + "truncate table edge, node")
			firstToolCall = false
		}
		switch r.Method + " " + r.URL.Path {
		case "PUT /pg-migration/neo-to-pg", "PUT /graph-db/switch/pg":
			w.WriteHeader(200)
		case "GET /pg-migration/status":
			_, _ = w.Write([]byte(`{"state":"idle"}`))
		default:
			t.Errorf("unexpected tool API call %s %s", r.Method, r.URL.Path)
		}
	}))
	defer tool.Close()

	scriptContainerEpoch(fake, base)
	deps := Deps{
		Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{},
		NewToolAPITransport: func(string) toolapi.Transport {
			return rewriteTransport{base: tool.URL, client: tool.Client()}
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true, ReplacePostgresGraph: true,
		MigrationTimeout: time.Second, VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}

	if err := Install(context.Background(), deps, opts); err != nil {
		t.Fatalf("install failed: %v", err)
	}
	if !clearedBeforeMigration {
		t.Fatalf("the stale graph must be cleared before the migration starts:\n%s", strings.Join(fake.Calls, "\n"))
	}
}

// TestInstallKeepsTheOperatorsComposeFiles guards the fact that naming a file
// with -f makes docker compose ignore COMPOSE_FILE: every file the operator
// configured has to be named again or the installer reads, restarts and
// verifies a different project than the one that is running.
func TestInstallKeepsTheOperatorsComposeFiles(t *testing.T) {
	dir, composeFile := setupProject(t)
	extra := filepath.Join(dir, "extra.yml")
	_ = os.WriteFile(extra, []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, ".env"), []byte("COMPOSE_FILE=docker-compose.yml:extra.yml\n"), 0o644)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()

	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " -f " + extra + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	withOverride := base + "-f " + overridePath + " "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "pg"),
			psql + "select driver from database_switch limit 1":             []byte("pg\n"),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			"docker image inspect " + image:                                 []byte(""),
			psql + setRowSQL:                                                []byte("INSERT 0 1\n"),
			withOverride + "up -d":                                          nil,
			withOverride + "logs --no-color bloodhound":                     []byte("BloodTrail driver active version=test\n"),
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)": []byte("10|20\n"),
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
	if err := Install(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, opts); err != nil {
		t.Fatalf("install failed: %v", err)
	}
	for _, call := range fake.Calls {
		if strings.HasPrefix(call, "docker compose") && !strings.Contains(call, " -f "+extra+" ") {
			t.Fatalf("a compose call dropped the operator's own file: %s", call)
		}
	}
	if !fake.Called(withOverride + "up -d") {
		t.Fatalf("the restart must merge the override last:\n%s", strings.Join(fake.Calls, "\n"))
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	if strings.TrimSpace(string(env)) != "COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml" {
		t.Fatalf(".env = %q", env)
	}
}

func TestInstallRefusesUnknownImage(t *testing.T) {
	dir, composeFile := setupProject(t)
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                       composeConfigJSON(upstreamImage, "neo4j"),
			psql + "select driver from database_switch limit 1": []byte(""),
		},
		Errors: map[string]error{
			"docker image inspect " + image:    errors.New("no such image"),
			"docker manifest inspect " + image: errors.New("no such manifest"),
		},
		Prefixes: map[string][]byte{
			base + "exec -T -e NEO4J_PASSWORD=" + testNeo4jPassword + " graph-db cypher-shell": []byte("count\n10\n"),
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, Yes: true}
	err := Install(context.Background(), Deps{Runner: fake, Out: &bytes.Buffer{}}, opts)
	if err == nil || !strings.Contains(err.Error(), "neither present locally nor in a registry") {
		t.Fatalf("expected an unknown-image error, got %v", err)
	}
	if fake.Called(base + "exec -T app-db pg_dump") {
		t.Fatalf("pg_dump should not run before the image availability check: %v", fake.Calls)
	}
}

// TestInstallFallsBackToTheImageAlias covers a released CLI meeting a registry
// where the image for its own version has not been built yet: the moving alias
// for the same upstream release is the closest published thing.
func TestInstallFallsBackToTheImageAlias(t *testing.T) {
	dir, composeFile := setupProject(t)
	repo := "ghcr.io/x/bt"
	versioned, alias := repo+":v9.6.0-bt0.2.0", repo+":v9.6.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()

	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "pg"),
			psql + "select driver from database_switch limit 1":             []byte("pg\n"),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			"docker manifest inspect " + alias:                              []byte("{}"),
			psql + setRowSQL:                                                []byte("INSERT 0 1\n"),
			base + "-f " + overridePath + " up -d":                          nil,
			base + "-f " + overridePath + " logs --no-color bloodhound":     []byte("BloodTrail driver active version=test\n"),
		},
		Errors: map[string]error{
			"docker image inspect " + versioned:    errors.New("no such image"),
			"docker manifest inspect " + versioned: errors.New("manifest unknown"),
			"docker image inspect " + alias:        errors.New("no such image"),
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)": []byte("10|20\n"),
		},
	}
	var out bytes.Buffer
	opts := Options{ComposeFile: composeFile, ImageRepo: repo, DriverVersion: "0.2.0", APIURL: api.URL, Yes: true,
		VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
	if err := Install(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &out}, opts); err != nil {
		t.Fatalf("install failed: %v\noutput:\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "falling back to "+alias) {
		t.Fatalf("the fallback must be reported:\n%s", out.String())
	}
	override, _ := os.ReadFile(overridePath)
	if !strings.Contains(string(override), "image: "+alias) {
		t.Fatalf("override should install the alias, got %s", override)
	}
	m, err := manifest.Load(dir)
	if err != nil || m.TargetImage != alias {
		t.Fatalf("the manifest must record the image actually installed: %+v err=%v", m, err)
	}
}

func TestInstallRefusesWhenTheCurlImageIsUnavailable(t *testing.T) {
	dir, composeFile := setupProject(t)
	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	fake := migrationFake(dir, composeFile, image, "10", "20", "10|20", "", "")
	delete(fake.Outputs, "docker image inspect "+toolapi.CurlImage)
	fake.Errors = map[string]error{
		"docker image inspect " + toolapi.CurlImage: errors.New("no such image"),
		"docker pull " + toolapi.CurlImage:          errors.New("no route to host"),
	}

	err := runMigrationInstall(t, dir, composeFile, image, fake)
	if err == nil || !strings.Contains(err.Error(), toolapi.CurlImage) {
		t.Fatalf("expected an error naming the curl image, got %v", err)
	}
	if fake.Called(base + "exec -T app-db pg_dump") {
		t.Fatalf("nothing should be changed before the helper image is available:\n%s", strings.Join(fake.Calls, "\n"))
	}
}

func TestInstallRefusesWhenAlreadyInstalled(t *testing.T) {
	dir, composeFile := setupProject(t)
	_ = manifest.Manifest{ProjectDir: dir}.Save(dir)
	err := Install(context.Background(), Deps{Runner: &dockerx.FakeRunner{}, Out: &bytes.Buffer{}}, Options{ComposeFile: composeFile, Yes: true})
	if err == nil || !strings.Contains(err.Error(), "did not complete or is still installed") {
		t.Fatalf("expected already-installed error, got %v", err)
	}
}

func TestRollbackRestoresEverything(t *testing.T) {
	dir, composeFile := setupProject(t)
	_ = os.WriteFile(filepath.Join(dir, "docker-compose.bloodtrail.yml"), []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, ".env"), []byte("COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"), 0o644)
	row := "neo4j"
	_ = manifest.Manifest{ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage, OriginalDriverRow: &row,
		OverrideFile: filepath.Join(dir, "docker-compose.bloodtrail.yml"), PGUser: "bloodhound", PGDatabase: "bloodhound"}.Save(dir)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer api.Close()

	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	// While the installation stands, COMPOSE_FILE still lists the override, so
	// the project is addressed with both files; the restart afterwards is not.
	installed := base + "-f " + filepath.Join(dir, "docker-compose.bloodtrail.yml") + " "
	psql := installed + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "up -d": nil,
			psql + "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('neo4j')": []byte("INSERT 0 1\n"),
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

func TestRollbackDeletesRowWhenOriginalAbsent(t *testing.T) {
	dir, composeFile := setupProject(t)
	_ = os.WriteFile(filepath.Join(dir, "docker-compose.bloodtrail.yml"), []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, ".env"), []byte("COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"), 0o644)
	_ = manifest.Manifest{ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage,
		OverrideFile: filepath.Join(dir, "docker-compose.bloodtrail.yml"), PGUser: "bloodhound", PGDatabase: "bloodhound"}.Save(dir)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer api.Close()

	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	installed := base + "-f " + filepath.Join(dir, "docker-compose.bloodtrail.yml") + " "
	psql := installed + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "up -d":                       nil,
			psql + "delete from database_switch": nil,
		},
	}
	if err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, Options{ComposeFile: composeFile, APIURL: api.URL, VerifyTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if !fake.Called(psql + "delete from database_switch") {
		t.Fatalf("expected the row to be deleted:\n%s", strings.Join(fake.Calls, "\n"))
	}
	deleteIdx, upIdx := -1, -1
	for i, c := range fake.Calls {
		if strings.HasPrefix(c, psql+"delete from database_switch") && deleteIdx == -1 {
			deleteIdx = i
		}
		if strings.HasPrefix(c, base+"up -d") && upIdx == -1 {
			upIdx = i
		}
	}
	if deleteIdx == -1 || upIdx == -1 || deleteIdx >= upIdx {
		t.Fatalf("expected the driver row to be deleted before `up`, got calls:\n%s", strings.Join(fake.Calls, "\n"))
	}
}

func TestRollbackSaysWhatIsAlreadyRestoredWhenTheRestartFails(t *testing.T) {
	dir, composeFile := setupProject(t)
	_ = os.WriteFile(filepath.Join(dir, "docker-compose.bloodtrail.yml"), []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, ".env"), []byte("COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"), 0o644)
	_ = manifest.Manifest{ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage,
		OverrideFile: filepath.Join(dir, "docker-compose.bloodtrail.yml"), PGUser: "bloodhound", PGDatabase: "bloodhound"}.Save(dir)

	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	installed := base + "-f " + filepath.Join(dir, "docker-compose.bloodtrail.yml") + " "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			installed + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc delete from database_switch": nil,
		},
		Errors: map[string]error{base + "up -d": errors.New("port is already allocated")},
	}
	err := Rollback(context.Background(), Deps{Runner: fake, Out: &bytes.Buffer{}}, Options{ComposeFile: composeFile, VerifyTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "already restored") {
		t.Fatalf("expected an error saying what is already restored, got %v", err)
	}
	if !strings.Contains(err.Error(), "rerun `bloodtrail rollback`") {
		t.Fatalf("expected the error to say how to finish, got %v", err)
	}
	if !manifest.Exists(dir) {
		t.Fatal("the manifest must survive a failed restart so the rerun finds the installation")
	}
}

func TestInstallRollbackHintNamesTheGraphThatStays(t *testing.T) {
	for _, c := range []struct{ driver, want string }{
		{"neo4j", "Neo4j"},
		{"pg", "PostgreSQL"},
	} {
		t.Run(c.driver, func(t *testing.T) {
			if got := untouchedGraph(c.driver); !strings.Contains(got, c.want) {
				t.Fatalf("untouchedGraph(%q) = %q, want it to name %s", c.driver, got, c.want)
			}
		})
	}
	if strings.Contains(untouchedGraph("pg"), "Neo4j") {
		t.Fatal("a deployment that was already on PostgreSQL has no Neo4j graph to promise")
	}
}

func TestStatusReportsRunningImage(t *testing.T) {
	dir, composeFile := setupProject(t)
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	running := "ghcr.io/x/bt:v9.6.0-bt0.1.0"

	// A container still running the old image while the files already name the
	// new one is exactly what a half-finished install or rollback leaves.
	for _, c := range []struct {
		name, psOutput, want string
	}{
		{"array", `[{"Service":"bloodhound","Image":"` + running + `","State":"running"}]`, running},
		{"newline delimited", `{"Service":"bloodhound","Image":"` + running + `","State":"running"}` + "\n", running},
		{"nothing running", "[]\n", "not running"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fake := &dockerx.FakeRunner{
				Outputs: map[string][]byte{
					base + "config --format json":                       composeConfigJSON(upstreamImage, "bloodtrail"),
					psql + "select driver from database_switch limit 1": []byte("bloodtrail\n"),
					base + "ps --format json bloodhound":                []byte(c.psOutput),
				},
				Prefixes: map[string][]byte{
					psql + "select (select count(*) from node)": []byte("10|20\n"),
				},
			}
			var out bytes.Buffer
			if err := Status(context.Background(), Deps{Runner: fake, Out: &out}, Options{ComposeFile: composeFile}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "configured:    "+upstreamImage) {
				t.Fatalf("configured image missing from:\n%s", out.String())
			}
			if !strings.Contains(out.String(), "running:       "+c.want) {
				t.Fatalf("running image %q missing from:\n%s", c.want, out.String())
			}
		})
	}
}

func TestVerifyRunsChecks(t *testing.T) {
	dir, composeFile := setupProject(t)
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
	var apiHits int
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiHits++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer api.Close()

	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		base + "logs --no-color bloodhound": []byte("boot\nBloodTrail driver active version=0.1.0 mode=delegate backend=pg\n"),
	}}
	var out bytes.Buffer
	opts := Options{ComposeFile: composeFile, APIURL: api.URL, VerifyTimeout: time.Second}
	if err := Verify(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &out}, opts); err != nil {
		t.Fatalf("verify failed: %v\n%s", err, out.String())
	}
	if apiHits == 0 || !fake.Called(base+"logs --no-color bloodhound") {
		t.Fatalf("both checks must run: apiHits=%d calls=%v", apiHits, fake.Calls)
	}
	if !strings.Contains(out.String(), "driver log line present") || !strings.Contains(out.String(), "API answers at") {
		t.Fatalf("both checks must be reported:\n%s", out.String())
	}

	// The log line is the decisive check and the cheap one, so it comes first:
	// a missing driver must be reported without waiting on the API.
	fake = &dockerx.FakeRunner{Outputs: map[string][]byte{
		base + "logs --no-color bloodhound": []byte("boot\nready\n"),
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	unreachable := Options{ComposeFile: composeFile, APIURL: "http://127.0.0.1:1", VerifyTimeout: time.Second}
	if err := Verify(ctx, Deps{Runner: fake, Out: &bytes.Buffer{}}, unreachable); err == nil {
		t.Fatal("expected verification to fail when the driver never logged")
	}
	if fake.Calls == nil {
		t.Fatal("the log check must run before the API wait")
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

// TestInstallKeepsTheComposeFileDiscoveryWouldHaveLoaded covers the project
// shape that has no COMPOSE_FILE entry but does have the conventional
// override file beside the base one: docker compose loads that file on its
// own, and naming any file with -f switches that discovery off. Every compose
// call the installer makes has to name it, or `up -d` recreates the operator's
// services without whatever the override holds (published ports, bind mounts).
// The COMPOSE_FILE entry the install writes has to name it for the same
// reason: it replaces discovery for the operator's own commands too.
func TestInstallKeepsTheComposeFileDiscoveryWouldHaveLoaded(t *testing.T) {
	dir, composeFile := setupProject(t)
	discovered := filepath.Join(dir, "docker-compose.override.yml")
	_ = os.WriteFile(discovered, []byte("services: {}\n"), 0o644)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer api.Close()

	image := "ghcr.io/x/bt:v9.6.0-bt0.1.0"
	overridePath := filepath.Join(dir, "docker-compose.bloodtrail.yml")
	base := "docker compose --project-directory " + dir + " -f " + composeFile + " -f " + discovered + " "
	psql := base + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	withOverride := base + "-f " + overridePath + " "
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			base + "config --format json":                                   composeConfigJSON(upstreamImage, "pg"),
			psql + "select driver from database_switch limit 1":             []byte("pg\n"),
			base + "exec -T app-db pg_dump -Fc -U bloodhound -d bloodhound": []byte("PGDMP"),
			"docker image inspect " + image:                                 []byte(""),
			psql + setRowSQL:                                                []byte("INSERT 0 1\n"),
			withOverride + "up -d":                                          nil,
			withOverride + "logs --no-color bloodhound":                     []byte("BloodTrail driver active version=test\n"),
		},
		Prefixes: map[string][]byte{
			psql + "select (select count(*) from node)": []byte("10|20\n"),
		},
	}
	opts := Options{ComposeFile: composeFile, Image: image, APIURL: api.URL, Yes: true,
		VerifyTimeout: time.Second, Now: func() time.Time { return time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC) }}
	if err := Install(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}}, opts); err != nil {
		t.Fatalf("install failed: %v", err)
	}
	for _, call := range fake.Calls {
		if strings.HasPrefix(call, "docker compose") && !strings.Contains(call, " -f "+discovered+" ") {
			t.Fatalf("a compose call dropped the file compose would have discovered: %s", call)
		}
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	want := "COMPOSE_FILE=docker-compose.yml:docker-compose.override.yml:docker-compose.bloodtrail.yml"
	if strings.TrimSpace(string(env)) != want {
		t.Fatalf(".env = %q, want %q", strings.TrimSpace(string(env)), want)
	}
	m, err := manifest.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !m.EnvComposeFileCreated {
		t.Error("the manifest must record that this install created the COMPOSE_FILE entry")
	}
}

// TestRollbackRemovesAnEnvEntryItCreated pins that rollback restores the
// absence of the COMPOSE_FILE line, not a line naming the base file: any line
// at all keeps compose's own file discovery switched off, so leaving one
// behind would stop the operator's docker-compose.override.yml from being
// loaded by their own commands, for good, after a rollback that claims to
// have restored everything.
func TestRollbackRemovesAnEnvEntryItCreated(t *testing.T) {
	dir, composeFile := setupProject(t)
	discovered := filepath.Join(dir, "docker-compose.override.yml")
	_ = os.WriteFile(discovered, []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "docker-compose.bloodtrail.yml"), []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, ".env"),
		[]byte("A=b\nCOMPOSE_FILE=docker-compose.yml:docker-compose.override.yml:docker-compose.bloodtrail.yml\n"), 0o644)
	row := "pg"
	_ = manifest.Manifest{ProjectDir: dir, ComposeFile: composeFile, ProjectName: "bh", OriginalImage: upstreamImage, OriginalDriverRow: &row,
		OverrideFile: filepath.Join(dir, "docker-compose.bloodtrail.yml"), PGUser: "bloodhound", PGDatabase: "bloodhound",
		EnvComposeFileCreated: true}.Save(dir)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer api.Close()

	installed := "docker compose --project-directory " + dir + " -f " + composeFile +
		" -f " + discovered + " -f " + filepath.Join(dir, "docker-compose.bloodtrail.yml") + " "
	psql := installed + "exec -T app-db psql -v ON_ERROR_STOP=1 -U bloodhound -d bloodhound -tAc "
	restoreRow := "create table if not exists database_switch (driver text not null, primary key(driver)); delete from database_switch; insert into database_switch (driver) values ('pg')"
	fake := &dockerx.FakeRunner{
		Outputs: map[string][]byte{
			psql + restoreRow: []byte("INSERT 0 1\n"),
			// After the entry is gone, discovery is back on, so the restart
			// addresses the project as base file plus discovered override.
			"docker compose --project-directory " + dir + " -f " + composeFile + " -f " + discovered + " up -d": nil,
		},
	}
	if err := Rollback(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &bytes.Buffer{}},
		Options{ComposeFile: composeFile, APIURL: api.URL, VerifyTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	if strings.Contains(string(env), "COMPOSE_FILE") {
		t.Fatalf("rollback left a COMPOSE_FILE entry behind: %q", env)
	}
	if strings.TrimSpace(string(env)) != "A=b" {
		t.Fatalf("rollback changed more than the entry it created: %q", env)
	}
}

// TestComposeHandleSkipsAMissingExtraFile covers the state the override
// file's own header invites: the operator removes the file but leaves its
// COMPOSE_FILE entry. Passing a missing file to compose fails every command,
// which would wedge `bloodtrail rollback` behind an error telling the
// operator to rerun the command that cannot succeed.
func TestComposeHandleSkipsAMissingExtraFile(t *testing.T) {
	dir, composeFile := setupProject(t)
	present := filepath.Join(dir, "extra.yml")
	_ = os.WriteFile(present, []byte("services: {}\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, ".env"),
		[]byte("COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n"), 0o644)

	args := strings.Join(composeHandle(&dockerx.FakeRunner{}, composeFile, dir).Args("up", "-d"), " ")
	if !strings.Contains(args, " -f "+present) {
		t.Errorf("the file that is there was dropped: %s", args)
	}
	if strings.Contains(args, "docker-compose.bloodtrail.yml") {
		t.Errorf("the file that is gone was passed on anyway: %s", args)
	}
}
