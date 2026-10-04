// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"context"
	"encoding/json"
	"errors"
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
		// The first search is the check, before the upload, for whether the
		// fixture is already there; the next two are the retries.
		if searchPolls.Add(1) < 4 {
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
	if got := searchPolls.Load(); got != 4 {
		t.Fatalf("got %d searches, want 4: the check before the upload, two empty retries after it, and the one that finds the fixture", got)
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
	mux.HandleFunc("GET /api/v2/search", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[]}`))
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
	var ended atomic.Bool
	mux.HandleFunc("POST /api/v2/file-upload/1", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202) })
	mux.HandleFunc("POST /api/v2/file-upload/1/end", func(w http.ResponseWriter, r *http.Request) {
		ended.Store(true)
		w.WriteHeader(200)
	})
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
	// The fixture is in the graph once the upload has been ended, not before:
	// a graph that already held it would make the run inconclusive.
	mux.HandleFunc("GET /api/v2/search", func(w http.ResponseWriter, r *http.Request) {
		if !ended.Load() {
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
	if jobPolls.Load() < 2 {
		t.Fatalf("expected at least 2 job polls (1 transient failure + 1 success), got %d", jobPolls.Load())
	}
}

// smokeAPI is a stand-in BloodHound API for Smoke: it accepts the login and
// the upload, finishes the job and the datapipe at once, and answers the
// search with the fixture domain from the start when fixtureAlreadyThere is
// set -- what an earlier smoke test leaves behind -- and otherwise only once
// the upload has been ended.
func smokeAPI(t *testing.T, fixtureAlreadyThere bool) (srv *httptest.Server, uploads *atomic.Int32) {
	t.Helper()
	return smokeAPIWithJob(t, fixtureAlreadyThere, `{"data":[{"id":1,"status":2,"status_message":"","failed_files":0}]}`)
}

// smokeAPIWithJob is smokeAPI with the answer to the ingest job listing of the
// caller's choosing, for a job that did not finish cleanly.
func smokeAPIWithJob(t *testing.T, fixtureAlreadyThere bool, jobs string) (srv *httptest.Server, uploads *atomic.Int32) {
	t.Helper()
	var ended atomic.Bool
	uploads = new(atomic.Int32)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v2/login", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"session_token":"tok"}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/start", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"data":{"id":1}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/1", func(w http.ResponseWriter, r *http.Request) {
		uploads.Add(1)
		w.WriteHeader(202)
	})
	mux.HandleFunc("POST /api/v2/file-upload/1/end", func(w http.ResponseWriter, r *http.Request) {
		ended.Store(true)
		w.WriteHeader(200)
	})
	mux.HandleFunc("GET /api/v2/file-upload", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(jobs))
	})
	mux.HandleFunc("GET /api/v2/datapipe/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"idle"}}`))
	})
	mux.HandleFunc("GET /api/v2/search", func(w http.ResponseWriter, r *http.Request) {
		if fixtureAlreadyThere || ended.Load() {
			_, _ = w.Write([]byte(`{"data":[{"objectid":"S-1-5-21-1","type":"Domain","name":"TESTLAB.LOCAL"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":[]}`))
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, uploads
}

// TestSmokeIsInconclusiveWhenTheFixtureIsAlreadyThere covers a repeat of the
// smoke test on a deployment that still holds the fixture domain an earlier
// run left (it is left on purpose). The search at the end then finds it whether
// or not this run's ingest reached the graph, so a pass would say nothing --
// and used to be reported all the same. The run still uploads and waits for its
// own job and the datapipe, which are evidence of this run and can fail it; but
// where they are all it has, it says the result is inconclusive.
func TestSmokeIsInconclusiveWhenTheFixtureIsAlreadyThere(t *testing.T) {
	srv, uploads := smokeAPI(t, true)
	s := Smoke{Client: srv.Client(), BaseURL: srv.URL, User: "admin", Password: "pw", Poll: time.Millisecond}
	err := s.Run(context.Background(), time.Second)
	if !errors.Is(err, ErrSmokeInconclusive) {
		t.Fatalf("Run over a graph that already holds the fixture = %v, want ErrSmokeInconclusive", err)
	}
	if !strings.Contains(err.Error(), fixtureDomain) {
		t.Errorf("the result does not name the fixture domain: %v", err)
	}
	if uploads.Load() != 7 {
		t.Errorf("the fixture was uploaded %d times, want 7: the run's own evidence is still gathered", uploads.Load())
	}

	// Where the fixture is not there, finding it is a pass.
	srv, _ = smokeAPI(t, false)
	s = Smoke{Client: srv.Client(), BaseURL: srv.URL, User: "admin", Password: "pw", Poll: time.Millisecond}
	if err := s.Run(context.Background(), time.Second); err != nil {
		t.Fatalf("Run over a graph without the fixture: %v", err)
	}
}

// TestSmokeFailsRatherThanInconclusiveWhenTheFixtureIsAlreadyThereAndTheJobFailed
// is the other half of the inconclusive result: it is what a run reports when
// everything it can check of its own held. Where this run's ingest job failed,
// or finished with failed files, that is evidence of this run however the
// search ends, so over a graph that already holds the fixture the run is a
// failure with the job's own reason, never ErrSmokeInconclusive -- which a
// caller may treat as acceptable.
func TestSmokeFailsRatherThanInconclusiveWhenTheFixtureIsAlreadyThereAndTheJobFailed(t *testing.T) {
	for _, c := range []struct {
		name string
		jobs string
		want string
	}{
		{"the job failed", `{"data":[{"id":1,"status":5,"status_message":"boom","failed_files":7}]}`, "boom"},
		{"the job finished with failed files", `{"data":[{"id":1,"status":2,"status_message":"two files","failed_files":2}]}`, "2 failed files"},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv, _ := smokeAPIWithJob(t, true, c.jobs)
			s := Smoke{Client: srv.Client(), BaseURL: srv.URL, User: "admin", Password: "pw", Poll: time.Millisecond}
			err := s.Run(context.Background(), time.Second)
			if err == nil {
				t.Fatal("Run passed over a failed ingest job")
			}
			if errors.Is(err, ErrSmokeInconclusive) {
				t.Fatalf("Run = %v: a failed job was reported as inconclusive, which a caller may accept", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("Run = %v, want the job's failure (%q)", err, c.want)
			}
		})
	}
}

func itoa(i int) string { return string(rune('0' + i)) }
