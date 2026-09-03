// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSmokeRunHappyPath(t *testing.T) {
	var uploads, statusPolls, searchPolls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v2/login", func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["login_method"] != "secret" || req["username"] != "admin" || req["secret"] != "pw" {
			http.Error(w, "bad login", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"data":{"user_id":"u","auth_expired":false,"session_token":"tok"}}`))
	})
	auth := func(r *http.Request) bool { return r.Header.Get("Authorization") == "Bearer tok" }
	mux.HandleFunc("POST /api/v2/file-upload/start", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) {
			http.Error(w, "no", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"data":{"id":7,"status":0}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/7", func(w http.ResponseWriter, r *http.Request) {
		if !auth(r) || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-File-Upload-Name") == "" {
			http.Error(w, "bad upload", 400)
			return
		}
		uploads.Add(1)
		w.WriteHeader(202)
	})
	mux.HandleFunc("POST /api/v2/file-upload/7/end", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /api/v2/file-upload", func(w http.ResponseWriter, r *http.Request) {
		status := 1
		if statusPolls.Add(1) >= 2 {
			status = 2
		}
		_, _ = w.Write([]byte(`{"data":[{"id":7,"status":` + itoa(status) + `,"status_message":"","total_files":7,"failed_files":0}]}`))
	})
	mux.HandleFunc("GET /api/v2/datapipe/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"idle"}}`))
	})
	mux.HandleFunc("GET /api/v2/search", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("q") != "TESTLAB.LOCAL" {
			http.Error(w, "bad q", 400)
			return
		}
		if searchPolls.Add(1) < 3 {
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"objectid":"S-1-5-21-1","type":"Domain","name":"TESTLAB.LOCAL"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s := Smoke{Client: srv.Client(), BaseURL: srv.URL, User: "admin", Password: "pw", Poll: time.Millisecond}
	if err := s.Run(context.Background(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if uploads.Load() != 7 {
		t.Fatalf("expected 7 uploads, got %d", uploads.Load())
	}
	if searchPolls.Load() < 3 {
		t.Fatalf("expected search retries, got %d polls", searchPolls.Load())
	}
}

func TestSmokeFailsOnFailedJob(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v2/login", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"session_token":"tok"}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/start", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"data":{"id":1}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/1", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) })
	mux.HandleFunc("POST /api/v2/file-upload/1/end", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /api/v2/file-upload", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":1,"status":5,"status_message":"boom","failed_files":7}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	s := Smoke{Client: srv.Client(), BaseURL: srv.URL, User: "admin", Password: "pw", Poll: time.Millisecond}
	err := s.Run(context.Background(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected job failure with message, got %v", err)
	}
}

func TestSmokeSearchTimesOut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v2/login", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"session_token":"tok"}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/start", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"data":{"id":1}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/1", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) })
	mux.HandleFunc("POST /api/v2/file-upload/1/end", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /api/v2/file-upload", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":1,"status":2,"status_message":"","failed_files":0}]}`))
	})
	mux.HandleFunc("GET /api/v2/datapipe/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"idle"}}`))
	})
	mux.HandleFunc("GET /api/v2/search", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	s := Smoke{Client: srv.Client(), BaseURL: srv.URL, User: "admin", Password: "pw", Poll: 5 * time.Millisecond}
	err := s.Run(context.Background(), 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "no matching node") {
		t.Fatalf("expected search timeout with 'no matching node', got %v", err)
	}
}

func TestSmokeWaitForJobToleratesTransientErrors(t *testing.T) {
	var jobPolls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v2/login", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"session_token":"tok"}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/start", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"data":{"id":1}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/1", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) })
	mux.HandleFunc("POST /api/v2/file-upload/1/end", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /api/v2/file-upload", func(w http.ResponseWriter, r *http.Request) {
		if jobPolls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":1,"status":2,"status_message":"","failed_files":0}]}`))
	})
	mux.HandleFunc("GET /api/v2/datapipe/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"idle"}}`))
	})
	mux.HandleFunc("GET /api/v2/search", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"objectid":"S-1-5-21-1","type":"Domain","name":"TESTLAB.LOCAL"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	s := Smoke{Client: srv.Client(), BaseURL: srv.URL, User: "admin", Password: "pw", Poll: time.Millisecond}
	if err := s.Run(context.Background(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if jobPolls.Load() < 2 {
		t.Fatalf("expected at least 2 job polls (1 transient failure + 1 success), got %d", jobPolls.Load())
	}
}

func itoa(i int) string { return string(rune('0' + i)) }
