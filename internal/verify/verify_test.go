// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"net/http"
	"net/http/httptest"
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

func TestLogsContain(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		"docker compose --project-directory /p -f /p/docker-compose.yml logs --no-color bloodhound": []byte("boot\nBloodTrail driver active version=0.1.0 mode=delegate backend=pg\n"),
	}}
	c := dockerx.Compose{Runner: fake, File: "/p/docker-compose.yml", ProjectDir: "/p"}
	ok, err := LogsContain(context.Background(), c, "bloodhound", DriverActiveMarker)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}
