// SPDX-License-Identifier: Apache-2.0

//go:build unix

package installer

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// otherOwner reports info as a file owned by another user and group.
type otherOwner struct{ os.FileInfo }

func (o otherOwner) Sys() any {
	st := *o.FileInfo.Sys().(*syscall.Stat_t)
	st.Uid++
	st.Gid++
	return &st
}

// TestStageRefusesToChangeWhoOwnsTheFile covers an .env that belongs to
// someone else, writable to this user through its group or mode bits: only
// root can give a new file another owner, so anyone else replacing it would
// end up owning it -- which the write refuses, before anything is replaced,
// and leaves nothing behind.
func TestStageRefusesToChangeWhoOwnsTheFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can hand the file on, which is the point of running as root")
	}
	path := writeEnvFixture(t, "OLD=1\n", 0o666)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	plan := envReplacement{target: path, info: otherOwner{info}}
	if scratch, err := plan.stage([]byte("NEW=2\n")); err == nil || !strings.Contains(err.Error(), "cannot be given the owner of the .env it replaces") {
		t.Fatalf("stage = %q, %v; want a refusal to change the owner", scratch, err)
	}
	if got, _ := os.ReadFile(path); string(got) != "OLD=1\n" {
		t.Fatalf(".env holds %q", got)
	}
	if names := dirNames(t, filepath.Dir(path)); len(names) != 1 {
		t.Fatalf("the refusal left files behind: %v", names)
	}
}

// TestWriteEnvFileGivesTheReplacementTheOwnerOfTheOriginal runs as root only,
// the one case in which the owner can differ from the user replacing the file.
func TestWriteEnvFileGivesTheReplacementTheOwnerOfTheOriginal(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("only root can give a file to another user")
	}
	path := writeEnvFixture(t, "OLD=1\n", 0o640)
	if err := os.Chown(path, 12345, 23456); err != nil {
		t.Fatal(err)
	}
	if err := writeEnvFile(path, []byte("NEW=2\n")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if uid, gid, _ := ownerOf(info); uid != 12345 || gid != 23456 {
		t.Fatalf("the replacement is owned by %d:%d, want 12345:23456", uid, gid)
	}
}
