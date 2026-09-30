// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package installer

import "os"

// ownerOf reports no owner: there is nothing to keep on this platform.
func ownerOf(os.FileInfo) (uid, gid uint32, ok bool) { return 0, 0, false }

// syncDir does nothing: directories cannot be flushed here.
func syncDir(string) error { return nil }
