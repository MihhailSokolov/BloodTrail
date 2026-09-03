// SPDX-License-Identifier: Apache-2.0

package dbswitch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

const prefix = "docker compose --project-directory /p -f /p/docker-compose.yml exec -T app-db psql -v ON_ERROR_STOP=1 -U bh -d bhdb -tAc "

func newStore(fake *dockerx.FakeRunner) Store {
	return Store{
		Compose:  dockerx.Compose{Runner: fake, File: "/p/docker-compose.yml", ProjectDir: "/p"},
		Service:  "app-db",
		User:     "bh",
		Database: "bhdb",
	}
}

func TestReadPresent(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		prefix + readSQL: []byte("neo4j\n"),
	}}
	driver, present, err := newStore(fake).Read(context.Background())
	if err != nil || !present || driver != "neo4j" {
		t.Fatalf("got driver=%q present=%v err=%v", driver, present, err)
	}
}

func TestReadAbsentRowAndMissingTable(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{prefix + readSQL: []byte("")}}
	if _, present, err := newStore(fake).Read(context.Background()); err != nil || present {
		t.Fatalf("empty output should mean absent: present=%v err=%v", present, err)
	}
	fake = &dockerx.FakeRunner{Errors: map[string]error{
		prefix + readSQL: errors.New(`ERROR:  relation "database_switch" does not exist`),
	}}
	if _, present, err := newStore(fake).Read(context.Background()); err != nil || present {
		t.Fatalf("missing table should mean absent: present=%v err=%v", present, err)
	}
}

func TestSetUpserts(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		prefix + setSQL("bloodtrail"): []byte("INSERT 0 1\n"),
	}}
	if err := newStore(fake).Set(context.Background(), "bloodtrail"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(setSQL("bloodtrail"), "create table if not exists database_switch") {
		t.Fatalf("Set must create the table if missing: %s", setSQL("bloodtrail"))
	}
}

func TestSetRejectsUnsafeDriverName(t *testing.T) {
	if err := newStore(&dockerx.FakeRunner{}).Set(context.Background(), "x'; drop table users; --"); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestCountGraph(t *testing.T) {
	fake := &dockerx.FakeRunner{Outputs: map[string][]byte{
		prefix + countSQL: []byte("1234|56789\n"),
	}}
	nodes, edges, err := newStore(fake).CountGraph(context.Background())
	if err != nil || nodes != 1234 || edges != 56789 {
		t.Fatalf("got %d %d %v", nodes, edges, err)
	}
}
