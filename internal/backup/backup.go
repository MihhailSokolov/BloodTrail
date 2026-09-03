// SPDX-License-Identifier: Apache-2.0

// Package backup takes the safety copies the installer makes before changing anything.
package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

// NewDir creates and returns .bloodtrail/backups/<timestamp> under projectDir.
func NewDir(projectDir string, now time.Time) (string, error) {
	dir := filepath.Join(projectDir, ".bloodtrail", "backups", now.UTC().Format("20060102T150405Z"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating backup directory: %w", err)
	}
	return dir, nil
}

// DumpDatabase runs pg_dump in custom format inside the database container and
// stores the output at destPath. An empty dump is treated as a failure.
func DumpDatabase(ctx context.Context, compose dockerx.Compose, service, user, database, destPath string) error {
	out, err := compose.Exec(ctx, service, nil, "pg_dump", "-Fc", "-U", user, "-d", database)
	if err != nil {
		return fmt.Errorf("pg_dump: %w", err)
	}
	if len(out) == 0 {
		return errors.New("pg_dump produced no output")
	}
	if err := os.WriteFile(destPath, out, 0o600); err != nil {
		return fmt.Errorf("writing dump: %w", err)
	}
	return nil
}

// CopyFiles copies each existing path into destDir, keeping the base name.
func CopyFiles(destDir string, paths ...string) error {
	for _, p := range paths {
		in, err := os.Open(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		out, err := os.Create(filepath.Join(destDir, filepath.Base(p)))
		if err != nil {
			_ = in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		_ = in.Close()
		if closeErr := out.Close(); copyErr == nil {
			copyErr = closeErr
		}
		if copyErr != nil {
			return fmt.Errorf("copying %s: %w", p, copyErr)
		}
	}
	return nil
}
