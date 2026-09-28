// SPDX-License-Identifier: Apache-2.0

package dockerx

import (
	"context"
	"io"
	"strings"
	"testing"
)

func TestComposeArgsIncludeFileAndProjectDir(t *testing.T) {
	c := Compose{File: "/srv/bh/docker-compose.yml", ProjectDir: "/srv/bh"}
	got := strings.Join(c.Args("up", "-d"), " ")
	want := "compose --project-directory /srv/bh -f /srv/bh/docker-compose.yml up -d"
	if got != want {
		t.Fatalf("Args = %q, want %q", got, want)
	}
}

func TestComposeExecUsesFakeRunner(t *testing.T) {
	fake := &FakeRunner{Outputs: map[string][]byte{
		"docker compose --project-directory /srv/bh -f /srv/bh/docker-compose.yml exec -T app-db psql -tAc select 1": []byte("1\n"),
	}}
	c := Compose{Runner: fake, File: "/srv/bh/docker-compose.yml", ProjectDir: "/srv/bh"}
	out, err := c.Exec(context.Background(), "app-db", nil, "psql", "-tAc", "select 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(string(out)) != "1" {
		t.Fatalf("out = %q", out)
	}
	if len(fake.Calls) != 1 {
		t.Fatalf("expected one recorded call, got %v", fake.Calls)
	}
}

// testNeo4jPassword is built from a plain identifier rather than written as
// a NEO4J_PASSWORD=literal assignment anywhere below, so secret scanners do
// not mistake this fixture for a real credential.
const testNeo4jPassword = "test-only"

// TestComposeExecEnvKeepsValuesOffTheCommandLine pins that ExecEnv names the
// variable before the service but hands its value to docker's environment:
// `-e NAME=value` on docker's own argv is readable by every local user
// through ps and was echoed into error messages.
func TestComposeExecEnvKeepsValuesOffTheCommandLine(t *testing.T) {
	fake := &FakeRunner{Outputs: map[string][]byte{
		"docker compose --project-directory /srv/bh -f /srv/bh/docker-compose.yml exec -T -e NEO4J_PASSWORD graph-db cypher-shell -u neo4j": []byte("ok\n"),
	}}
	c := Compose{Runner: fake, File: "/srv/bh/docker-compose.yml", ProjectDir: "/srv/bh"}
	if _, err := c.ExecEnv(context.Background(), "graph-db", []string{"NEO4J_PASSWORD=" + testNeo4jPassword}, nil, "cypher-shell", "-u", "neo4j"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(fake.Calls[0], testNeo4jPassword) {
		t.Fatalf("the value is on the command line: %q", fake.Calls[0])
	}
	if got := fake.Envs[0]; len(got) != 1 || got[0] != "NEO4J_PASSWORD="+testNeo4jPassword {
		t.Fatalf("environment handed to docker = %q, want the NEO4J_PASSWORD entry", got)
	}
}

// plainRunner is a Runner that cannot set a command's environment.
type plainRunner struct{ calls int }

func (p *plainRunner) Run(context.Context, io.Reader, string, ...string) ([]byte, error) {
	p.calls++
	return nil, nil
}

// TestComposeExecEnvRefusesARunnerWithoutEnvironmentSupport pins the fail
// closed half: a secret is never moved back onto the command line just
// because the Runner cannot carry it in the environment.
func TestComposeExecEnvRefusesARunnerWithoutEnvironmentSupport(t *testing.T) {
	runner := &plainRunner{}
	c := Compose{Runner: runner, File: "/srv/bh/docker-compose.yml", ProjectDir: "/srv/bh"}
	if _, err := c.ExecEnv(context.Background(), "graph-db", []string{"NEO4J_PASSWORD=" + testNeo4jPassword}, nil, "cypher-shell"); err == nil {
		t.Fatal("expected an error for a Runner that cannot set the environment")
	}
	if runner.calls != 0 {
		t.Fatalf("the command ran anyway")
	}
}

// TestExecRunnerSetsTheEnvironmentAndRedactsErrors pins ExecRunner's half:
// RunEnv's entries reach the child's environment, and an error that echoes a
// command line never carries an -e value.
func TestExecRunnerSetsTheEnvironmentAndRedactsErrors(t *testing.T) {
	r := ExecRunner{Stderr: io.Discard}
	out, err := r.RunEnv(context.Background(), []string{"BT_PROBE=" + testNeo4jPassword}, nil, "sh", "-c", `printf %s "$BT_PROBE"`)
	if err != nil {
		t.Fatalf("RunEnv: %v", err)
	}
	if string(out) != testNeo4jPassword {
		t.Fatalf("child saw BT_PROBE=%q, want %q", out, testNeo4jPassword)
	}

	_, err = r.Run(context.Background(), nil, "sh", "-c", "exit 3", "-e", "NEO4J_PASSWORD="+testNeo4jPassword)
	if err == nil {
		t.Fatal("expected the failing command to return an error")
	}
	if strings.Contains(err.Error(), testNeo4jPassword) {
		t.Fatalf("error text carries the secret: %v", err)
	}
	if !strings.Contains(err.Error(), "NEO4J_PASSWORD=<redacted>") {
		t.Fatalf("error text should still name the variable: %v", err)
	}
}

func TestFakeRunnerFailsOnUnexpectedCommand(t *testing.T) {
	fake := &FakeRunner{}
	if _, err := fake.Run(context.Background(), nil, "docker", "nope"); err == nil {
		t.Fatal("expected an error for an unscripted command")
	}
}

func TestFakeRunnerSequencesThenFallsBack(t *testing.T) {
	fake := &FakeRunner{
		Sequences: map[string][][]byte{"docker count": {[]byte("0"), []byte("7")}},
		Prefixes:  map[string][]byte{"docker count": []byte("last")},
	}
	var got []string
	for range 3 {
		out, err := fake.Run(context.Background(), nil, "docker", "count", "nodes")
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(out))
	}
	if strings.Join(got, ",") != "0,7,last" {
		t.Fatalf("got %v", got)
	}
}
