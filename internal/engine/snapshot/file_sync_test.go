// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// swapSyncParentDir replaces the directory sync WriteSnapshotFile and
// RemoveSnapshotFile run, for the duration of the test.
func swapSyncParentDir(t *testing.T, sync func(dir string) error) {
	t.Helper()
	prev := syncParentDir
	syncParentDir = sync
	t.Cleanup(func() { syncParentDir = prev })
}

// TestWriteSnapshotFileSyncsTheDirectoryAfterTheRename pins that the rename
// which puts a snapshot file in place is made durable: the file's own fsync
// covers its bytes, not the directory entry naming it, so a power loss after
// an unsynced rename can bring back whatever the directory held before.
func TestWriteSnapshotFileSyncsTheDirectoryAfterTheRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph-1.btsnap")

	var synced []string
	swapSyncParentDir(t, func(d string) error {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("directory synced before the file was renamed into place: %v", err)
		}
		synced = append(synced, d)
		return nil
	})

	if err := WriteSnapshotFile(path, buildFileFixture(t), Stamp{Watermark: 3}); err != nil {
		t.Fatalf("WriteSnapshotFile: %v", err)
	}
	if len(synced) != 1 || synced[0] != dir {
		t.Fatalf("directories synced = %v, want exactly the file's own directory %q", synced, dir)
	}
}

// TestWriteSnapshotFileSurfacesADirectorySyncFailure pins that a directory
// that could not be synced is reported, not swallowed: the file is complete
// and in place, but whether its name survives a power loss is unknown, and
// the caller has to hear about it (the engine's save still runs its own
// post-write checks either way).
func TestWriteSnapshotFileSurfacesADirectorySyncFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "graph-1.btsnap")
	failure := errors.New("simulated directory sync failure")
	swapSyncParentDir(t, func(string) error { return failure })

	err := WriteSnapshotFile(path, buildFileFixture(t), Stamp{Watermark: 3})
	if !errors.Is(err, failure) {
		t.Fatalf("WriteSnapshotFile error = %v, want it to wrap the directory sync failure", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("the renamed file is gone after the directory sync failed (stat: %v); its rename had already happened", statErr)
	}
	leftovers, globErr := filepath.Glob(filepath.Join(dir, ".snapshot-*.tmp"))
	if globErr != nil || len(leftovers) != 0 {
		t.Fatalf("temp files left behind = %v (glob error %v), want none", leftovers, globErr)
	}
}

// TestRemoveSnapshotFileSyncsTheDirectory pins the invalidation side: an
// unlink that is not synced can be undone by a power loss, bringing back a
// file that was taken out of play precisely because it can no longer be
// trusted. The directory is synced after a removal, and also when there was
// nothing to remove -- an earlier removal's unlink may not be durable yet --
// and a failure to sync is reported.
func TestRemoveSnapshotFileSyncsTheDirectory(t *testing.T) {
	t.Run("an existing file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "graph-1.btsnap")
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatalf("seed file: %v", err)
		}
		var synced []string
		swapSyncParentDir(t, func(d string) error {
			if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("directory synced before the file was removed (stat: %v)", err)
			}
			synced = append(synced, d)
			return nil
		})

		removed, err := RemoveSnapshotFile(path)
		if err != nil || !removed {
			t.Fatalf("RemoveSnapshotFile = (%v, %v), want (true, nil)", removed, err)
		}
		if len(synced) != 1 || synced[0] != dir {
			t.Fatalf("directories synced = %v, want exactly %q", synced, dir)
		}
	})

	t.Run("no file", func(t *testing.T) {
		dir := t.TempDir()
		var synced []string
		swapSyncParentDir(t, func(d string) error {
			synced = append(synced, d)
			return nil
		})

		removed, err := RemoveSnapshotFile(filepath.Join(dir, "graph-1.btsnap"))
		if err != nil || removed {
			t.Fatalf("RemoveSnapshotFile = (%v, %v), want (false, nil)", removed, err)
		}
		if len(synced) != 1 || synced[0] != dir {
			t.Fatalf("directories synced = %v, want exactly %q", synced, dir)
		}
	})

	t.Run("a sync failure", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "graph-1.btsnap")
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatalf("seed file: %v", err)
		}
		failure := errors.New("simulated directory sync failure")
		swapSyncParentDir(t, func(string) error { return failure })

		removed, err := RemoveSnapshotFile(path)
		if !removed || !errors.Is(err, failure) {
			t.Fatalf("RemoveSnapshotFile = (%v, %v), want (true, the sync failure)", removed, err)
		}
	})

	t.Run("no directory", func(t *testing.T) {
		removed, err := RemoveSnapshotFile(filepath.Join(t.TempDir(), "missing", "graph-1.btsnap"))
		if err != nil || removed {
			t.Fatalf("RemoveSnapshotFile in a directory that does not exist = (%v, %v), want (false, nil): there is nothing a power loss could bring back", removed, err)
		}
	})
}

// TestSyncDir pins the real directory sync: a directory syncs, and one that
// does not exist is an error the caller can recognize.
func TestSyncDir(t *testing.T) {
	if err := syncDir(t.TempDir()); err != nil {
		t.Fatalf("syncDir on a real directory: %v", err)
	}
	if err := syncDir(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("syncDir on a missing directory = %v, want an fs.ErrNotExist error", err)
	}
}

// TestDirSyncUnsupported pins which failures mean "this platform or
// filesystem cannot sync a directory at all" -- tolerated, since there is no
// stronger guarantee to be had there -- as opposed to a sync that failed.
func TestDirSyncUnsupported(t *testing.T) {
	wrap := func(errno syscall.Errno) error {
		return &os.PathError{Op: "sync", Path: "/snapshots", Err: errno}
	}
	cases := []struct {
		err  error
		want bool
	}{
		{wrap(syscall.EINVAL), true},
		{wrap(syscall.ENOTSUP), true},
		{wrap(syscall.EOPNOTSUPP), true},
		{wrap(syscall.EIO), false},
		{errors.New("some other failure"), false},
	}
	for _, tc := range cases {
		if got := dirSyncUnsupported(tc.err); got != tc.want {
			t.Errorf("dirSyncUnsupported(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
