// SPDX-License-Identifier: Apache-2.0

package manifest

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	row := "neo4j"
	m := Manifest{
		InstallerVersion:  "0.1.0",
		InstalledAt:       "2026-09-02T12:00:00Z",
		ComposeFile:       filepath.Join(dir, "docker-compose.yml"),
		ProjectDir:        dir,
		ProjectName:       "bloodhound",
		OriginalImage:     "docker.io/specterops/bloodhound:v9.6.0",
		OriginalDriverRow: &row,
		BackupDir:         filepath.Join(dir, ".bloodtrail", "backups", "20260902T120000Z"),
		OverrideFile:      filepath.Join(dir, "docker-compose.bloodtrail.yml"),
		TargetImage:       "ghcr.io/mihhailsokolov/bloodtrail:v9.6.0-bt0.1.0",
		UpstreamTag:       "v9.6.0",
		PGUser:            "bloodhound",
		PGDatabase:        "bloodhound",

		EnvComposeFileCreated: true,
		EnvComposeFileWritten: []string{"docker-compose.yml", "docker-compose.bloodtrail.yml"},
	}
	if Exists(dir) {
		t.Fatal("manifest should not exist yet")
	}
	if err := m.Save(dir); err != nil {
		t.Fatalf("save: %v", err)
	}
	if !Exists(dir) {
		t.Fatal("manifest should exist after save")
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !reflect.DeepEqual(got, m) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, m)
	}
}

func TestLoadMissing(t *testing.T) {
	if _, err := Load(t.TempDir()); err == nil {
		t.Fatal("expected error for missing manifest")
	}
}
