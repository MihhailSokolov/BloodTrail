// SPDX-License-Identifier: Apache-2.0

//go:build unix

package installer

import (
	"errors"
	"os"
	"syscall"
)

// ownerOf returns the user and group that own the file info describes.
func ownerOf(info os.FileInfo) (uid, gid uint32, ok bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, false
	}
	return st.Uid, st.Gid, true
}

// syncDir flushes the directory dir, which is what makes a rename in it
// survive a crash. A file system that cannot flush a directory has nothing
// more to offer, and is not an error.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil && !errors.Is(syncErr, syscall.EINVAL) && !errors.Is(syncErr, errors.ErrUnsupported) {
		return syncErr
	}
	return closeErr
}
