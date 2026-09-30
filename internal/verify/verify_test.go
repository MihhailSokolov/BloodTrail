// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

func TestWaitForAPIRetriesUntilServerAnswers(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/version" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	if err := WaitForAPI(context.Background(), srv.Client(), srv.URL, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if hits.Load() < 3 {
		t.Fatalf("expected retries, got %d hits", hits.Load())
	}
}

func TestWaitForAPITimesOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	if err := WaitForAPI(context.Background(), srv.Client(), srv.URL, 50*time.Millisecond); err == nil {
		t.Fatal("expected timeout")
	}
}

func TestWaitForAPIAccepts200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"API":{"current_version":"v2"}}}`))
	}))
	defer srv.Close()
	if err := WaitForAPI(context.Background(), srv.Client(), srv.URL, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

// scriptRun scripts the bloodhound container of project as the current run
// of container id, started at started, whose log since then is logs and whose
// whole log, kept across restarts, is whole.
func scriptRun(project, id, started, logs, whole string) *dockerx.FakeRunner {
	return &dockerx.FakeRunner{Outputs: map[string][]byte{
		project + "ps --format json bloodhound":                        []byte(`[{"ID":"` + id + `"}]`),
		"docker inspect -f {{.Id}} {{.State.StartedAt}} " + id:         []byte(id + " " + started + "\n"),
		project + "logs --no-color --since " + started + " bloodhound": []byte(logs),
		project + "logs --no-color bloodhound":                         []byte(whole),
	}}
}

func TestLogsContain(t *testing.T) {
	const project = "docker compose --project-directory /p -f /p/docker-compose.yml "
	current := "boot\nBloodTrail driver active version=0.1.0 mode=delegate backend=pg\n"
	c := func(fake *dockerx.FakeRunner) dockerx.Compose {
		return dockerx.Compose{Runner: fake, File: "/p/docker-compose.yml", ProjectDir: "/p"}
	}

	ok, err := LogsContain(context.Background(), c(scriptRun(project, "cafe01", "2026-09-02T00:00:00.5Z", current, current)), "bloodhound", DriverActiveMarker)
	if err != nil || !ok {
		t.Fatalf("a marker logged by the current run: ok=%v err=%v", ok, err)
	}

	// Docker keeps a container's log across its restarts: the marker of a run
	// that has since been replaced is in the whole log but not in what the
	// current run has logged.
	stale := scriptRun(project, "cafe01", "2026-09-02T00:00:00.5Z", "Connecting to graph using PostgreSQL\n", current+"shutting down\nConnecting to graph using PostgreSQL\n")
	ok, err = LogsContain(context.Background(), c(stale), "bloodhound", DriverActiveMarker)
	if err != nil || ok {
		t.Fatalf("a marker from before the current start: ok=%v err=%v", ok, err)
	}
	if stale.Called(project + "logs --no-color bloodhound") {
		t.Fatalf("the whole log was read:\n%s", strings.Join(stale.Calls, "\n"))
	}
}

func TestLogsContainNeedsARunningContainer(t *testing.T) {
	const project = "docker compose --project-directory /p -f /p/docker-compose.yml "
	for _, ps := range []string{"[]\n", ""} {
		fake := &dockerx.FakeRunner{Outputs: map[string][]byte{project + "ps --format json bloodhound": []byte(ps)}}
		c := dockerx.Compose{Runner: fake, File: "/p/docker-compose.yml", ProjectDir: "/p"}
		if _, err := LogsContain(context.Background(), c, "bloodhound", DriverActiveMarker); !errors.Is(err, dockerx.ErrNoRunningContainer) {
			t.Fatalf("ps printed %q: err = %v, want ErrNoRunningContainer", ps, err)
		}
	}
}
