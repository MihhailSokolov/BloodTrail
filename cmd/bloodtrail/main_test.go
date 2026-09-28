// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestParseFlagsRejectsLeftoverArguments pins that nothing after the flags
// is silently dropped: the flag package stops at the first non-flag, so
// `rollback ./bh --yes --compose-file /x.yml` parsed as a bare rollback of
// the default compose file with no --yes.
func TestParseFlagsRejectsLeftoverArguments(t *testing.T) {
	for _, args := range [][]string{
		{"./bh", "--yes", "--compose-file", "/x.yml"},
		{"--yes", "./bh"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var errOut bytes.Buffer
			_, err := parseFlags(args, &errOut)
			if !errors.Is(err, errUsage) {
				t.Fatalf("parseFlags(%q) error = %v, want errUsage", args, err)
			}
			if !strings.Contains(errOut.String(), `unexpected argument "./bh"`) {
				t.Fatalf("the leftover argument should be named:\n%s", errOut.String())
			}
		})
	}

	opts, err := parseFlags([]string{"--yes", "--compose-file", "/x.yml"}, io.Discard)
	if err != nil || !opts.Yes || opts.ComposeFile != "/x.yml" {
		t.Fatalf("flags alone: opts = %+v, err = %v", opts, err)
	}
}

func TestConfirmFrom(t *testing.T) {
	cases := []struct {
		name   string
		answer string
		want   bool
	}{
		{"y", "y\n", true},
		{"yes", "Yes\n", true},
		{"n", "n\n", false},
		{"anything else", "maybe\n", false},
		// A pipe that is already at end of file (nobody there to answer) is a
		// no, not a hang and not a yes.
		{"closed input", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out bytes.Buffer
			got := confirmFrom(strings.NewReader(c.answer), &out, "Proceed?")
			if got != c.want {
				t.Fatalf("confirmFrom(%q) = %v, want %v", c.answer, got, c.want)
			}
			if !strings.Contains(out.String(), "Proceed? [y/N]") {
				t.Fatalf("prompt = %q", out.String())
			}
		})
	}
}
