// SPDX-License-Identifier: Apache-2.0

// Package dockerx runs docker and docker compose as subprocesses so the
// installer behaves exactly like the operator's own commands.
package dockerx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// Runner executes a command and returns its standard output.
type Runner interface {
	Run(ctx context.Context, stdin io.Reader, name string, args ...string) ([]byte, error)
}

// StreamRunner is a Runner whose output does not have to fit in memory. Run
// collects a command's whole standard output in a buffer, which is what every
// command the installer reads back wants -- they answer in kilobytes. A
// database dump does not: it is as large as the deployment's data, and on a
// deployment already using PostgreSQL for the graph that includes the graph,
// gigabytes at the scale BloodTrail exists for. Buffering that (twice, once
// in the runner and once in the writer) would risk the installer being
// OOM-killed during the backup, the one step that has to succeed before
// anything is changed.
//
// Callers reach this through Compose.ExecTo, which falls back to Run for
// runners that do not implement it (the test fake), so implementing it stays
// optional.
type StreamRunner interface {
	Runner
	RunTo(ctx context.Context, stdout io.Writer, stdin io.Reader, name string, args ...string) error
}

// ExecRunner is the real Runner. Stderr receives the child's standard error
// (defaults to os.Stderr) so operators see docker's own messages.
type ExecRunner struct {
	Stderr io.Writer
}

func (s ExecRunner) Run(ctx context.Context, stdin io.Reader, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	errSink := s.Stderr
	if errSink == nil {
		errSink = os.Stderr
	}
	cmd.Stderr = io.MultiWriter(&stderr, errSink)
	if err := cmd.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// RunTo streams the command's standard output to stdout instead of collecting
// it, so output larger than memory is fine. Stderr is still captured for the
// error message, exactly as Run does: it is a message, not a payload.
func (s ExecRunner) RunTo(ctx context.Context, stdout io.Writer, stdin io.Reader, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	var stderr bytes.Buffer
	errSink := s.Stderr
	if errSink == nil {
		errSink = os.Stderr
	}
	cmd.Stderr = io.MultiWriter(&stderr, errSink)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return nil
}
