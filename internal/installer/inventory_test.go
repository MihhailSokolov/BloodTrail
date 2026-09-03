// SPDX-License-Identifier: Apache-2.0

package installer

import "testing"

func TestUpstreamTagFromImage(t *testing.T) {
	cases := []struct {
		image string
		want  string
	}{
		{"docker.io/specterops/bloodhound:v9.6.0", "v9.6.0"},
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
