// SPDX-License-Identifier: Apache-2.0

package installer

import "testing"

func TestUpstreamTagFromImage(t *testing.T) {
	cases := []struct {
		image string
		want  string
	}{
		{"docker.io/specterops/bloodhound:v9.6.0", "v9.6.0"},
		// Docker Hub publishes the application image without the "v" of the
		// GitHub source tag; BloodTrail images carry the "v", so the bare
		// number has to be normalised.
		{"docker.io/specterops/bloodhound:9.6.0", "v9.6.0"},
		{"docker.io/specterops/bloodhound:9.6.0-rc1", "v9.6.0-rc1"},
		// A moving tag names no particular release: refuse to guess.
		{"docker.io/specterops/bloodhound:latest", ""},
		{"registry.internal:5000/bloodhound", ""},
		{"repo@sha256:abc", ""},
		{"bloodhound", ""},
	}
	for _, c := range cases {
		if got := upstreamTagFromImage(c.image); got != c.want {
			t.Errorf("upstreamTagFromImage(%q) = %q, want %q", c.image, got, c.want)
		}
	}
}
