// SPDX-License-Identifier: Apache-2.0

package toolapi

import (
	"context"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

func TestCurlContainerTransportBuildsDockerRun(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		"docker run --rm --network bloodhound_default docker.io/curlimages/curl:8.10.1 -sS -o - -w \n%{http_code} -X GET http://bloodhound:2112/pg-migration/status": []byte("{\"state\":\"idle\"}\n200"),
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
		"docker run --rm --network n docker.io/curlimages/curl:8.10.1 -sS -o - -w \n%{http_code} -X PUT http://x/y": []byte("no status code here"),
	}}
	tr := CurlContainerTransport{Runner: fake, Network: "n", Image: CurlImage}
	if _, _, err := tr.Do(context.Background(), "PUT", "http://x/y"); err == nil {
		t.Fatal("expected parse error")
	}
}
