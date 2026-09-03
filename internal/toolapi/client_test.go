// SPDX-License-Identifier: Apache-2.0

package toolapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestMigrateNeoToPGHappyPath(t *testing.T) {
	var polls atomic.Int32
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch r.Method + " " + r.URL.Path {
		case "PUT /pg-migration/neo-to-pg":
			w.WriteHeader(http.StatusAccepted)
		case "GET /pg-migration/status":
			if polls.Add(1) < 3 {
				_, _ = w.Write([]byte(`{"state":"migrating"}`))
			} else {
				_, _ = w.Write([]byte(`{"state":"idle"}`))
			}
		case "PUT /graph-db/switch/pg":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := Client{BaseURL: srv.URL, Transport: HTTPTransport{Client: srv.Client()}}
	if err := c.MigrateNeoToPG(context.Background(), time.Millisecond, time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls[0] != "PUT /pg-migration/neo-to-pg" || calls[len(calls)-1] != "PUT /graph-db/switch/pg" {
		t.Fatalf("unexpected call order: %v", calls)
	}
	if polls.Load() < 3 {
		t.Fatalf("expected at least 3 polls, got %d", polls.Load())
	}
}

func TestMigrateFailsWhenStartRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"errors":[{"message":"migration already running"}]}`, http.StatusConflict)
	}))
	defer srv.Close()
	c := Client{BaseURL: srv.URL, Transport: HTTPTransport{Client: srv.Client()}}
	err := c.MigrateNeoToPG(context.Background(), time.Millisecond, time.Second)
	if err == nil || !contains(err.Error(), "409") {
		t.Fatalf("expected a 409 error, got %v", err)
	}
}

func TestWaitUntilIdleTimesOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"state":"migrating"}`))
	}))
	defer srv.Close()
	c := Client{BaseURL: srv.URL, Transport: HTTPTransport{Client: srv.Client()}}
	if err := c.WaitUntilIdle(context.Background(), time.Millisecond, 20*time.Millisecond); err == nil {
		t.Fatal("expected timeout error")
	}
}

func contains(s, sub string) bool { return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0) }
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
