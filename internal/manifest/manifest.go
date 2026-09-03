// SPDX-License-Identifier: Apache-2.0

// Package manifest records what an install changed so rollback can undo it.
package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	DirName  = ".bloodtrail"
	FileName = "manifest.json"
)

// Manifest is written next to the compose file after a successful install.
type Manifest struct {
	InstallerVersion  string  `json:"installer_version"`
	InstalledAt       string  `json:"installed_at"`
	ComposeFile       string  `json:"compose_file"`
	ProjectDir        string  `json:"project_dir"`
	ProjectName       string  `json:"project_name"`
	OriginalImage     string  `json:"original_image"`
	OriginalDriverRow *string `json:"original_driver_row"` // nil: the database_switch row was absent
	BackupDir         string  `json:"backup_dir"`
	OverrideFile      string  `json:"override_file"`
	TargetImage       string  `json:"target_image"`
	UpstreamTag       string  `json:"upstream_tag"`
	PGUser            string  `json:"pg_user"`
	PGDatabase        string  `json:"pg_database"`
}

// Path returns the manifest location for a compose project directory.
func Path(projectDir string) string {
	return filepath.Join(projectDir, DirName, FileName)
}

// Exists reports whether an install manifest is present.
func Exists(projectDir string) bool {
	_, err := os.Stat(Path(projectDir))
	return err == nil
}

// Save writes the manifest, creating .bloodtrail/ if needed.
func (m Manifest) Save(projectDir string) error {
	if err := os.MkdirAll(filepath.Dir(Path(projectDir)), 0o755); err != nil {
		return fmt.Errorf("creating manifest directory: %w", err)
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding manifest: %w", err)
	}
	return os.WriteFile(Path(projectDir), append(data, '\n'), 0o644)
}

// Load reads the manifest.
func Load(projectDir string) (Manifest, error) {
	data, err := os.ReadFile(Path(projectDir))
	if err != nil {
		return Manifest{}, fmt.Errorf("reading manifest: %w", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, fmt.Errorf("decoding manifest: %w", err)
	}
	return m, nil
}
