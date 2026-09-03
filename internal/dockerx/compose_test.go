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

func TestFakeRunnerFailsOnUnexpectedCommand(t *testing.T) {
	fake := &FakeRunner{}
	if _, err := fake.Run(context.Background(), nil, "docker", "nope"); err == nil {
		t.Fatal("expected an error for an unscripted command")
	}
}
