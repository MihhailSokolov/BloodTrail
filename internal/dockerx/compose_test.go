// SPDX-License-Identifier: Apache-2.0

package dockerx

import (
	"context"
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

func TestComposeExecEnvPassesEnvironmentBeforeTheService(t *testing.T) {
	fake := &FakeRunner{Outputs: map[string][]byte{
		"docker compose --project-directory /srv/bh -f /srv/bh/docker-compose.yml exec -T -e NEO4J_PASSWORD=s3cret graph-db cypher-shell -u neo4j": []byte("ok\n"),
	}}
	c := Compose{Runner: fake, File: "/srv/bh/docker-compose.yml", ProjectDir: "/srv/bh"}
	if _, err := c.ExecEnv(context.Background(), "graph-db", []string{"NEO4J_PASSWORD=s3cret"}, nil, "cypher-shell", "-u", "neo4j"); err != nil {
		t.Fatalf("unexpected error: %v", err)
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
