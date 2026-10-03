// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

// fakeBloodHoundForSmoke is a stand-in BloodHound API for the smoke test the
// verification can run: it answers /api/version, accepts the login and the
// upload, finishes the job and the datapipe at once, and answers the search
// with the fixture domain from the start when fixtureAlreadyThere is set --
// what an earlier smoke test leaves behind -- and otherwise only once the
// upload has been ended.
func fakeBloodHoundForSmoke(t *testing.T, fixtureAlreadyThere bool) *httptest.Server {
	t.Helper()
	var ended atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/version", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	mux.HandleFunc("POST /api/v2/login", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"session_token":"tok"}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/start", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"data":{"id":1}}`))
	})
	mux.HandleFunc("POST /api/v2/file-upload/1", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) })
	mux.HandleFunc("POST /api/v2/file-upload/1/end", func(w http.ResponseWriter, r *http.Request) {
		ended.Store(true)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /api/v2/file-upload", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":1,"status":2,"status_message":"","failed_files":0}]}`))
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
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestVerifyReportsASmokeTestOverAnExistingFixtureAsInconclusive covers the
// verification with --admin-password on a deployment that already holds the
// fixture domain an earlier run left. The smoke test's search then finds it
// whether or not this run's ingest reached the graph, and verification
// reported "found through the search API" as if it had shown that. It now says
// the smoke test is inconclusive -- a warning: everything else it checked
// stands -- and never the line that says it passed.
func TestVerifyReportsASmokeTestOverAnExistingFixtureAsInconclusive(t *testing.T) {
	const pass = "fixture ingested, analysed and found through the search API"
	for _, c := range []struct {
		name                string
		fixtureAlreadyThere bool
	}{
		{"the fixture is not there before the upload", false},
		{"the fixture is left by an earlier run", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir, composeFile := setupProject(t)
			base := "docker compose --project-directory " + dir + " -f " + composeFile + " "
			api := fakeBloodHoundForSmoke(t, c.fixtureAlreadyThere)
			fake := &dockerx.FakeRunner{}
			scriptRunningBloodhound(fake, base, "BloodTrail driver active version=0.1.2 mode=engine backend=pg\n", "bloodtrail")
			var out bytes.Buffer
			opts := Options{ComposeFile: composeFile, APIURL: api.URL, AdminPassword: "pw", VerifyTimeout: 5 * time.Second}
			if err := Verify(context.Background(), Deps{Runner: fake, HTTP: api.Client(), Out: &out}, opts); err != nil {
				t.Fatalf("verify failed: %v\n%s", err, out.String())
			}
			said := out.String()
			if c.fixtureAlreadyThere {
				if !strings.Contains(said, "INCONCLUSIVE") || !strings.Contains(said, "TESTLAB.LOCAL") {
					t.Errorf("verification did not report the smoke test as inconclusive:\n%s", said)
				}
				if strings.Contains(said, pass) {
					t.Errorf("verification reported a pass for a smoke test that showed nothing:\n%s", said)
				}
				return
			}
			if !strings.Contains(said, pass) || strings.Contains(said, "INCONCLUSIVE") {
				t.Errorf("verification did not report the smoke test as passed:\n%s", said)
			}
		})
	}
}
