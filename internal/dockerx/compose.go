// SPDX-License-Identifier: Apache-2.0

package dockerx

import (
	"context"
	"io"
)

// Compose addresses one compose project by file and project directory.
type Compose struct {
	Runner     Runner
	File       string
	ProjectDir string
}

// Args builds the docker compose argument list for a subcommand.
func (s Compose) Args(sub ...string) []string {
	args := []string{"compose", "--project-directory", s.ProjectDir, "-f", s.File}
	return append(args, sub...)
}

func (s Compose) run(ctx context.Context, stdin io.Reader, sub ...string) ([]byte, error) {
	return s.Runner.Run(ctx, stdin, "docker", s.Args(sub...)...)
}

// ConfigJSON returns the fully resolved project as JSON.
func (s Compose) ConfigJSON(ctx context.Context) ([]byte, error) {
	return s.run(ctx, nil, "config", "--format", "json")
}

// Up starts or updates the project in the background.
func (s Compose) Up(ctx context.Context) error {
	_, err := s.run(ctx, nil, "up", "-d", "--remove-orphans")
	return err
}

// Exec runs a command inside a running service container without a TTY.
func (s Compose) Exec(ctx context.Context, service string, stdin io.Reader, args ...string) ([]byte, error) {
	return s.run(ctx, stdin, append([]string{"exec", "-T", service}, args...)...)
}

// Logs returns the service's logs.
func (s Compose) Logs(ctx context.Context, service string) ([]byte, error) {
	return s.run(ctx, nil, "logs", "--no-color", service)
}
