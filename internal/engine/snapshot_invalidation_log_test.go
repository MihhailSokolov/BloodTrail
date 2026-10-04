// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// invalidationLogRecorder is a slog.Handler that keeps every record with its
// attributes, flattened to strings, for tests that check what an operator is
// told: not just that a line was logged but which path and reason it names.
type invalidationLogRecorder struct {
	mu      sync.Mutex
	records []invalidationLogRecord
}

type invalidationLogRecord struct {
	level   slog.Level
	message string
	attrs   map[string]string
}

func (r *invalidationLogRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *invalidationLogRecorder) Handle(_ context.Context, rec slog.Record) error {
	attrs := map[string]string{}
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key != "stack" { // a goroutine dump drowns a failure message
			attrs[a.Key] = a.Value.String()
		}
		return true
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, invalidationLogRecord{level: rec.Level, message: rec.Message, attrs: attrs})
	return nil
}

func (r *invalidationLogRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }

func (r *invalidationLogRecorder) WithGroup(string) slog.Handler { return r }

// find returns the records logged with exactly this message.
func (r *invalidationLogRecorder) find(message string) []invalidationLogRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	var found []invalidationLogRecord
	for _, rec := range r.records {
		if rec.message == message {
			found = append(found, rec)
		}
	}
	return found
}

// TestRemoveSnapshotFileNamesTheReasonItIsGiven pins the one removal every
// invalidation goes through: the "snapshot file invalidated" line carries the
// path it removed and the reason of whichever caller asked, rather than a
// reason fixed for the first of them.
func TestRemoveSnapshotFileNamesTheReasonItIsGiven(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph-1.btsnap")
	if err := os.WriteFile(path, []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	logs := &invalidationLogRecorder{}
	e := New(nil, nil, Config{SnapshotDir: dir, Log: slog.New(logs)})

	e.removeSnapshotFile(context.Background(), path, "a reason of the caller's own")

	lines := logs.find("bloodtrail: snapshot file invalidated")
	if len(lines) != 1 {
		t.Fatalf("%d \"snapshot file invalidated\" lines, want 1: %+v", len(lines), logs.records)
	}
	if got := lines[0].attrs["reason"]; got != "a reason of the caller's own" {
		t.Errorf("reason = %q, want the one the caller gave", got)
	}
	if got := lines[0].attrs["path"]; got != path {
		t.Errorf("path = %q, want %q", got, path)
	}
}

// TestInvalidateSnapshotFileWithNoPathSaysSoOnlyWhereAFileWasExpected pins the
// "snapshot file not invalidated" warning: an engine configured with a
// snapshot directory whose default graph has not resolved has no path to
// delete, and the operator is told the file may survive; an engine with no
// snapshot directory has no file to speak of and stays quiet.
func TestInvalidateSnapshotFileWithNoPathSaysSoOnlyWhereAFileWasExpected(t *testing.T) {
	for _, c := range []struct {
		name        string
		dir         string
		wantWarning bool
	}{
		{"a snapshot directory is configured", t.TempDir(), true},
		{"no snapshot directory is configured", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			logs := &invalidationLogRecorder{}
			e := New(nil, nil, Config{SnapshotDir: c.dir, Log: slog.New(logs)})

			e.invalidateSnapshotFile(context.Background(), "any reason")

			lines := logs.find("bloodtrail: snapshot file not invalidated")
			if got := len(lines) == 1; got != c.wantWarning {
				t.Fatalf("\"snapshot file not invalidated\" logged = %v (%+v), want %v", len(lines) > 0, lines, c.wantWarning)
			}
			if c.wantWarning && lines[0].level != slog.LevelWarn {
				t.Errorf("logged at %v, want Warn", lines[0].level)
			}
		})
	}
}

// TestSnapshotFileBootPanicWithNoPathIsLoggedWithoutOne covers the recovery
// of a panic in the file-boot attempt when the snapshot file's path cannot be
// worked out (the default graph has not resolved). The panic is still logged
// and still enters fallback, but no empty path is logged as if it were one,
// and the operator is told through the same "snapshot file not invalidated"
// warning every other invalidation uses that no file was deleted.
func TestSnapshotFileBootPanicWithNoPathIsLoggedWithoutOne(t *testing.T) {
	logs := &invalidationLogRecorder{}
	// Not Enabled: no recovery rebuild is launched, only the state changes.
	e := New(nil, nil, Config{SnapshotDir: t.TempDir(), Log: slog.New(logs)})

	adopted := true
	func() {
		defer e.recoverSnapshotFileBootPanic(context.Background(), &adopted)
		panic("a panic injected by the test")
	}()

	if adopted {
		t.Fatal("a panicked attempt reported the file adopted")
	}
	panics := logs.find("bloodtrail: snapshot file boot panicked")
	if len(panics) != 1 {
		t.Fatalf("%d \"snapshot file boot panicked\" lines, want 1: %+v", len(panics), logs.records)
	}
	if path, has := panics[0].attrs["path"]; has {
		t.Errorf("the panic line carries path=%q though no path was resolved", path)
	}
	if lines := logs.find("bloodtrail: snapshot file not invalidated"); len(lines) != 1 {
		t.Errorf("%d \"snapshot file not invalidated\" lines, want 1: %+v", len(lines), logs.records)
	}
}
