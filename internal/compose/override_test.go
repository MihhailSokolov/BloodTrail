// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"strings"
	"testing"
)

func TestOverrideRender(t *testing.T) {
	o := Override{
		Service: "bloodhound",
		Image:   "ghcr.io/mihhailsokolov/bloodhound-bloodtrail:v9.6.0-bt0.1.0",
		Environment: map[string]string{
			"bhe_graph_driver":     "bloodtrail",
			"BLOODTRAIL_LOG_LEVEL": "info",
		},
	}
	got := o.Render()
	for _, want := range []string{
		"services:\n  bloodhound:\n    image: ghcr.io/mihhailsokolov/bloodhound-bloodtrail:v9.6.0-bt0.1.0\n",
		"    environment:\n",
		"      - BLOODTRAIL_LOG_LEVEL=info\n",
		"      - bhe_graph_driver=bloodtrail\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered override missing %q:\n%s", want, got)
		}
	}
	if !strings.HasPrefix(got, "# Written by bloodtrail") {
		t.Errorf("override should start with a marker comment:\n%s", got)
	}
}
