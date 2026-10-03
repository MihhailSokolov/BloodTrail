// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMain lets a test re-run this very binary as the oggen command, so the
// flags are exercised exactly as a user passes them.
func TestMain(m *testing.M) {
	if os.Getenv("OGGEN_TEST_RUN_AS_COMMAND") == "1" {
		main()
		return
	}
	os.Exit(m.Run())
}

// runOggen runs the command with args, killing it after limit.
func runOggen(t *testing.T, limit time.Duration, args ...string) (stderr string, exitCode int, timedOut bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], args...)
	cmd.Env = append(os.Environ(), "OGGEN_TEST_RUN_AS_COMMAND=1")
	var buf bytes.Buffer
	cmd.Stderr = &buf
	err := cmd.Run()
	if ctx.Err() != nil {
		return buf.String(), -1, true
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return buf.String(), exit.ExitCode(), false
	}
	if err != nil {
		t.Fatalf("run oggen %v: %v", args, err)
	}
	return buf.String(), 0, false
}

// TestChunkMustBePositive pins that -chunk 0 and a negative -chunk are
// refused up front. The file loops advance by chunk items at a time: with 0 they
// never reach the end of the data and write empty files until the disk is full,
// and with a negative size they slice out of range.
func TestChunkMustBePositive(t *testing.T) {
	for _, chunk := range []string{"0", "-1"} {
		t.Run("chunk="+chunk, func(t *testing.T) {
			out := t.TempDir()
			stderr, code, timedOut := runOggen(t, 10*time.Second, "-users", "20", "-out", out, "-chunk", chunk)
			if timedOut {
				t.Fatalf("oggen -chunk %s was still running after 10s (it loops forever); stderr: %q", chunk, stderr)
			}
			if code != 1 {
				t.Fatalf("oggen -chunk %s exited %d, want a clean refusal (exit 1); stderr: %q", chunk, code, stderr)
			}
			if !strings.Contains(stderr, "-chunk") {
				t.Fatalf("oggen -chunk %s did not say what was wrong with -chunk; stderr: %q", chunk, stderr)
			}
		})
	}
}

// TestChunkSplitsTheData pins that a valid -chunk still splits nodes and
// edges into files of at most that many items, nodes-* before rels-*.
func TestChunkSplitsTheData(t *testing.T) {
	out := t.TempDir()
	if stderr, code, timedOut := runOggen(t, 30*time.Second, "-users", "20", "-out", out, "-chunk", "7"); timedOut || code != 0 {
		t.Fatalf("oggen -chunk 7: exit %d (timed out: %v); stderr: %q", code, timedOut, stderr)
	}
	nodes, err := filepath.Glob(filepath.Join(out, "data", "nodes-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	rels, err := filepath.Glob(filepath.Join(out, "data", "rels-*.json"))
	if err != nil {
		t.Fatal(err)
	}
	total, _ := Generate(Config{Users: 20, Teams: 2, Repos: 4, Seed: 1})
	if want := (len(total) + 6) / 7; len(nodes) != want {
		t.Fatalf("got %d node files, want %d for %d nodes in chunks of 7", len(nodes), want, len(total))
	}
	if len(rels) < 2 {
		t.Fatalf("got %d edge files, want the edges split over several", len(rels))
	}
}
