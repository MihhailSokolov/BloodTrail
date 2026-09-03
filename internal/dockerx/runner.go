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
