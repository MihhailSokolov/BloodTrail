// SPDX-License-Identifier: Apache-2.0

package dockerx

import (
	"context"
	"io"
)

// Compose addresses one compose project by file and project directory.
// ExtraFiles are the further files the project is made of, in the order they
// must be merged after File: an explicit -f drops whatever COMPOSE_FILE says,
// so every file the operator configured has to be named again.
type Compose struct {
	Runner     Runner
	File       string
	ProjectDir string
	ExtraFiles []string
}

// Args builds the docker compose argument list for a subcommand.
func (s Compose) Args(sub ...string) []string {
	args := []string{"compose", "--project-directory", s.ProjectDir, "-f", s.File}
	for _, f := range s.ExtraFiles {
		args = append(args, "-f", f)
	}
	return append(args, sub...)
}

// WithExtraFile returns a copy of s that also merges file, unless it is
// already part of the project.
func (s Compose) WithExtraFile(file string) Compose {
	if file == s.File {
		return s
	}
	for _, f := range s.ExtraFiles {
		if f == file {
			return s
		}
	}
	s.ExtraFiles = append(append([]string(nil), s.ExtraFiles...), file)
	return s
}

func (s Compose) run(ctx context.Context, stdin io.Reader, sub ...string) ([]byte, error) {
	return s.Runner.Run(ctx, stdin, "docker", s.Args(sub...)...)
}

// ConfigJSON returns the fully resolved project as JSON.
func (s Compose) ConfigJSON(ctx context.Context) ([]byte, error) {
	return s.run(ctx, nil, "config", "--format", "json")
}

// Up starts or updates the project in the background. It deliberately does not
// pass --remove-orphans: the file list may not name every service the operator
// runs in this project, and removing what it does not know about is not the
// installer's business.
func (s Compose) Up(ctx context.Context) error {
	_, err := s.run(ctx, nil, "up", "-d")
	return err
}

// Exec runs a command inside a running service container without a TTY.
func (s Compose) Exec(ctx context.Context, service string, stdin io.Reader, args ...string) ([]byte, error) {
	return s.ExecEnv(ctx, service, nil, stdin, args...)
}

// ExecEnv is Exec with extra "NAME=value" environment entries set for the
// command inside the container, which keeps secrets off the command line of
// the process that reads them.
func (s Compose) ExecEnv(ctx context.Context, service string, env []string, stdin io.Reader, args ...string) ([]byte, error) {
	sub := []string{"exec", "-T"}
	for _, e := range env {
		sub = append(sub, "-e", e)
	}
	sub = append(sub, service)
	return s.run(ctx, stdin, append(sub, args...)...)
}

// Logs returns the service's logs.
func (s Compose) Logs(ctx context.Context, service string) ([]byte, error) {
	return s.run(ctx, nil, "logs", "--no-color", service)
}

// PS returns `docker compose ps` output for one service as JSON: one object
// per running container, either as a JSON array or as newline-delimited
// objects depending on the compose version.
func (s Compose) PS(ctx context.Context, service string) ([]byte, error) {
	return s.run(ctx, nil, "ps", "--format", "json", service)
}
