// SPDX-License-Identifier: Apache-2.0

package toolapi

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

func TestCurlContainerTransportBuildsDockerRun(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		"docker run --rm --network bloodhound_default docker.io/curlimages/curl:8.10.1 -sS -o - -w \n%{http_code} --connect-timeout 15 --max-time 120 -X GET http://bloodhound:2112/pg-migration/status": []byte("{\"state\":\"idle\"}\n200"),
	}}
	tr := CurlContainerTransport{Runner: fake, Network: "bloodhound_default", Image: CurlImage}
	status, body, err := tr.Do(context.Background(), "GET", "http://bloodhound:2112/pg-migration/status")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != 200 || string(body) != `{"state":"idle"}` {
		t.Fatalf("status=%d body=%q", status, body)
	}
}

func TestCurlContainerTransportRejectsMalformedOutput(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		"docker run --rm --network n docker.io/curlimages/curl:8.10.1 -sS -o - -w \n%{http_code} --connect-timeout 15 --max-time 120 -X PUT http://x/y": []byte("no status code here"),
	}}
	tr := CurlContainerTransport{Runner: fake, Network: "n", Image: CurlImage}
	if _, _, err := tr.Do(context.Background(), "PUT", "http://x/y"); err == nil {
		t.Fatal("expected parse error")
	}
}

// TestCurlRequestsAreBounded pins that every request carries curl's own
// wall-clock limits. Nothing else bounds one: the installer's context has no
// deadline and WaitUntilIdle checks --migration-timeout only between polls,
// so a request without these flags can hang an install forever.
func TestCurlRequestsAreBounded(t *testing.T) {
	var got []string
	fake := argvRecorder{seen: &got}
	tr := CurlContainerTransport{Runner: fake, Network: "n", Image: CurlImage}
	_, _, _ = tr.Do(context.Background(), http.MethodGet, "http://x/y")
	argv := strings.Join(got, " ")
	for _, want := range []string{"--connect-timeout 15", "--max-time 120"} {
		if !strings.Contains(argv, want) {
			t.Errorf("curl argv %q is missing %q", argv, want)
		}
	}
}

type argvRecorder struct{ seen *[]string }

func (r argvRecorder) Run(_ context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	*r.seen = append([]string{name}, args...)
	return []byte("{}\n200"), nil
}
