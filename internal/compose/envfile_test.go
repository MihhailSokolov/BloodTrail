// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"strings"
	"testing"
)

func TestAddComposeFileToEmptyEnv(t *testing.T) {
	got := AddComposeFile("", "docker-compose.yml", OverrideFileName)
	if got != "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n" {
		t.Fatalf("got %q", got)
	}
}

func TestAddComposeFileExtendsExisting(t *testing.T) {
	env := "FOO=1\nCOMPOSE_FILE=docker-compose.yml:extra.yml\n"
	got := AddComposeFile(env, "docker-compose.yml", OverrideFileName)
	want := "FOO=1\nCOMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestAddComposeFileIsIdempotent(t *testing.T) {
	once := AddComposeFile("A=b\n", "docker-compose.yml", OverrideFileName)
	twice := AddComposeFile(once, "docker-compose.yml", OverrideFileName)
	if once != twice {
		t.Fatalf("second add changed the file: %q vs %q", once, twice)
	}
}

func TestRemoveComposeFile(t *testing.T) {
	env := "A=b\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"
	if got := RemoveComposeFile(env, OverrideFileName); got != "A=b\nCOMPOSE_FILE=docker-compose.yml\n" {
		t.Fatalf("got %q", got)
	}
	env = "COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n"
	if got := RemoveComposeFile(env, OverrideFileName); got != "COMPOSE_FILE=docker-compose.yml:extra.yml\n" {
		t.Fatalf("got %q", got)
	}
	if got := RemoveComposeFile("A=b\n", OverrideFileName); got != "A=b\n" {
		t.Fatalf("untouched file changed: %q", got)
	}
}

func TestAddComposeFileCRLFIdempotent(t *testing.T) {
	env := "A=b\r\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\r\n"
	got := AddComposeFile(env, "docker-compose.yml", OverrideFileName)
	if got != env {
		t.Fatalf("idempotency failed on CRLF file: got %q want %q", got, env)
	}
	if strings.Contains(got, "\r:") || strings.Contains(got, ":\r") {
		t.Fatalf("CRLF corruption: %q", got)
	}
}

func TestAddComposeFileToEmptyEnvCRLF(t *testing.T) {
	got := AddComposeFile("A=b\r\n", "docker-compose.yml", OverrideFileName)
	want := "A=b\r\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\r\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if strings.Contains(got, "\r:") || strings.Contains(got, ":\r") {
		t.Fatalf("CRLF corruption: %q", got)
	}
}

func TestRemoveComposeFileCRLF(t *testing.T) {
	env := "A=b\r\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\r\n"
	got := RemoveComposeFile(env, OverrideFileName)
	want := "A=b\r\nCOMPOSE_FILE=docker-compose.yml\r\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if !strings.Contains(got, "\r\n") {
		t.Fatalf("CRLF lost: %q", got)
	}
}
