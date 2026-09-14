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
//
// The dump streams straight to the file rather than through a buffer: on a
// deployment whose graph already lives in PostgreSQL it is as large as the
// graph, and holding all of it (plus a second copy on the way out) in the
// installer's heap would risk being OOM-killed during the backup -- the step
// everything else is allowed to depend on having succeeded.
//
// Nothing is left at destPath unless the dump completed: a failed or empty
// dump removes the file, so a later restore cannot pick up a truncated one
// that still looks like a backup.
func DumpDatabase(ctx context.Context, compose dockerx.Compose, service, user, database, destPath string) error {
	f, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating dump: %w", err)
	}
	counted := &countingWriter{w: f}
	dumpErr := compose.ExecTo(ctx, counted, service, nil, "pg_dump", "-Fc", "-U", user, "-d", database)
	closeErr := f.Close()
	switch {
	case dumpErr != nil:
		_ = os.Remove(destPath)
		return fmt.Errorf("pg_dump: %w", dumpErr)
	case counted.n == 0:
		_ = os.Remove(destPath)
		return errors.New("pg_dump produced no output")
	case closeErr != nil:
		_ = os.Remove(destPath)
		return fmt.Errorf("writing dump: %w", closeErr)
	}
	return nil
}

// countingWriter counts what reached the file, so an empty dump is still
// recognised as one without stat-ing the result or buffering it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// CopyFiles copies each existing path into destDir, keeping the base name.
func CopyFiles(destDir string, paths ...string) error {
	for _, p := range paths {
		in, err := os.Open(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("opening %s: %w", p, err)
		}
		dest := filepath.Join(destDir, filepath.Base(p))
		out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			_ = in.Close()
			return fmt.Errorf("creating %s: %w", dest, err)
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
