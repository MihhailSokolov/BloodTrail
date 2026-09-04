// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"strings"
	"testing"
)

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
