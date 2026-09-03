// SPDX-License-Identifier: Apache-2.0

package compose

import "testing"

const sampleConfig = `{
  "name": "bloodhound",
  "services": {
    "app-db": {"image": "docker.io/library/postgres:18", "environment": {"POSTGRES_USER": "bloodhound", "POSTGRES_DB": "bloodhound"}},
    "bloodhound": {"image": "docker.io/specterops/bloodhound:v9.6.0", "environment": {"bhe_graph_driver": "neo4j"}}
  },
  "networks": {"default": {"name": "bloodhound_default"}}
}`

func TestParseConfig(t *testing.T) {
	cfg, err := ParseConfig([]byte(sampleConfig))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Name != "bloodhound" {
		t.Errorf("Name = %q", cfg.Name)
	}
	if cfg.Services["bloodhound"].Image != "docker.io/specterops/bloodhound:v9.6.0" {
		t.Errorf("image = %q", cfg.Services["bloodhound"].Image)
	}
	if cfg.Services["app-db"].Environment["POSTGRES_USER"] != "bloodhound" {
		t.Errorf("POSTGRES_USER missing: %+v", cfg.Services["app-db"].Environment)
	}
	if cfg.DefaultNetworkName() != "bloodhound_default" {
		t.Errorf("network = %q", cfg.DefaultNetworkName())
	}
}

func TestDefaultNetworkNameFallsBackToProject(t *testing.T) {
	cfg := Config{Name: "proj"}
	if got := cfg.DefaultNetworkName(); got != "proj_default" {
		t.Fatalf("got %q", got)
	}
}

func TestParseConfigRejectsGarbage(t *testing.T) {
	if _, err := ParseConfig([]byte("not json")); err == nil {
		t.Fatal("expected error")
	}
}
