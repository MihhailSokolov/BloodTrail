// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
)

// The project's .env holds the deployment's secrets, and install and rollback
// both rewrite it. Truncating it and writing it again in place -- what
// os.WriteFile does -- leaves it empty or cut short for anyone, or anything,
// that looks (or that stops the installer) in between: a crash, a full disk,
// a kill. It is replaced instead: the new contents are written, given the old
// file's mode and owner and flushed to disk in a scratch file beside it, and
// renamed over it, so what is there is always either the old file or the new
// one, whole.

// envReplacement is the replacement of one .env file.
type envReplacement struct {
	target string      // the file to replace: the path, with a symbolic link followed to the file it names
	info   os.FileInfo // what is there now; nil when there is no file yet
}

// planEnvReplacement checks that the file at path can be replaced -- and that
// the operator would let it be: a file this user could not have written in
// place is not one to swap out from under its owner either -- and returns the
// plan for it. It changes nothing.
func planEnvReplacement(path string) (envReplacement, error) {
	target := path
	if link, err := os.Lstat(path); err == nil && link.Mode()&os.ModeSymlink != 0 {
		// The operator keeps the file elsewhere and links it here: the link is
		// theirs, and stays; it is the file it leads to that changes.
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil {
			return envReplacement{}, fmt.Errorf("%s is a symbolic link that does not lead to a file: %w", path, err)
		}
		target = resolved
	}
	info, err := os.Stat(target)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return envReplacement{target: target}, nil
	case err != nil:
		return envReplacement{}, err
	case !info.Mode().IsRegular():
		return envReplacement{}, fmt.Errorf("%s is not a regular file", target)
	}
	f, err := os.OpenFile(target, os.O_WRONLY, 0)
	if err != nil {
		return envReplacement{}, fmt.Errorf("%s cannot be written by this user: %w", target, withoutPath(err))
	}
	_ = f.Close()
	return envReplacement{target: target, info: info}, nil
}

// withoutPath returns the reason inside a path error, for a message that
// names the path itself.
func withoutPath(err error) error {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}

// stage writes data into a new scratch file beside the target and returns its
// path: complete, flushed, with the target's mode and owner. It is left where
// it is for the caller to rename or remove; on failure it is gone already.
//
// The scratch file starts out private (0600) when it is going to hold the
// contents of an existing file, so its secrets are never readable by more
// than they were, and takes the old mode only once written. A file that does
// not exist yet gets what os.WriteFile gave it, 0644 less the umask.
func (r envReplacement) stage(data []byte) (string, error) {
	perm := os.FileMode(0o600)
	if r.info == nil {
		perm = 0o644
	}
	f, err := createScratchFile(filepath.Dir(r.target), perm)
	if err != nil {
		return "", fmt.Errorf("cannot create a file in %s to replace .env with: %w", filepath.Dir(r.target), withoutPath(err))
	}
	path := f.Name()
	if err := fillScratchFile(f, data, r.info); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

// fillScratchFile writes data into f, gives f the mode and owner of old (when
// there is an old file), flushes it to disk and closes it.
func fillScratchFile(f *os.File, data []byte, old os.FileInfo) error {
	if _, err := f.Write(data); err != nil {
		return err
	}
	if old != nil {
		if err := f.Chmod(old.Mode().Perm()); err != nil {
			return err
		}
		if err := copyOwner(f, old); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// createScratchFile creates a new file with a name that is not taken beside
// the one it is going to replace, like os.CreateTemp but with a mode of the
// caller's choosing (which the umask still narrows).
func createScratchFile(dir string, perm os.FileMode) (*os.File, error) {
	for attempt := 0; ; attempt++ {
		name := filepath.Join(dir, ".env-"+strconv.FormatUint(rand.Uint64(), 36)+".tmp")
		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
		if errors.Is(err, os.ErrExist) && attempt < 100 {
			continue
		}
		return f, err
	}
}

// copyOwner gives f the owner (user and group) of the file described by want,
// when that is not the owner it has already. Only root, or the owner itself,
// can hand a file on: anyone else could not replace the file without changing
// whose it is, which is refused rather than done.
func copyOwner(f *os.File, want os.FileInfo) error {
	uid, gid, ok := ownerOf(want)
	if !ok {
		return nil
	}
	if have, err := f.Stat(); err == nil {
		if haveUID, haveGID, ok := ownerOf(have); ok && haveUID == uid && haveGID == gid {
			return nil
		}
	}
	if err := f.Chown(int(uid), int(gid)); err != nil {
		return fmt.Errorf("the replacement cannot be given the owner of the .env it replaces (user %d, group %d): %w", uid, gid, err)
	}
	return nil
}

// writeEnvFile replaces the contents of the file at path with data, keeping
// its mode and owner, so that the file is at every moment the old one or the
// new one, whole. A file that does not exist is created. A symbolic link is
// followed: the file it leads to is replaced and the link stays.
func writeEnvFile(path string, data []byte) error {
	plan, err := planEnvReplacement(path)
	if err != nil {
		return err
	}
	scratch, err := plan.stage(data)
	if err != nil {
		return err
	}
	if err := os.Rename(scratch, plan.target); err != nil {
		_ = os.Remove(scratch)
		return err
	}
	// The rename is what makes the new file the .env; flushing the directory
	// makes that survive a crash as well.
	return syncDir(filepath.Dir(plan.target))
}

// checkEnvWritable reports whether writeEnvFile would be able to replace the
// file at path, going through the same steps -- including creating, and
// giving its owner to, the replacement -- and removing what it created. It
// changes nothing that is left behind, which lets a command find out that it
// cannot rewrite .env before it has changed anything else.
func checkEnvWritable(path string) error {
	plan, err := planEnvReplacement(path)
	if err != nil {
		return err
	}
	scratch, err := plan.stage(nil)
	if err != nil {
		return err
	}
	return os.Remove(scratch)
}
