// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"strings"
	"testing"
)

func TestAddComposeFileToEmptyEnv(t *testing.T) {
	got := AddComposeFile("", []string{"docker-compose.yml"}, OverrideFileName)
	if got != "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n" {
		t.Fatalf("got %q", got)
	}
}

func TestAddComposeFileExtendsExisting(t *testing.T) {
	env := "FOO=1\nCOMPOSE_FILE=docker-compose.yml:extra.yml\n"
	got := AddComposeFile(env, []string{"docker-compose.yml"}, OverrideFileName)
	want := "FOO=1\nCOMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestAddComposeFileIsIdempotent(t *testing.T) {
	once := AddComposeFile("A=b\n", []string{"docker-compose.yml"}, OverrideFileName)
	twice := AddComposeFile(once, []string{"docker-compose.yml"}, OverrideFileName)
	if once != twice {
		t.Fatalf("second add changed the file: %q vs %q", once, twice)
	}
}

func TestComposeFiles(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want string
	}{
		{"absent", "A=b\n", ""},
		{"single", "COMPOSE_FILE=docker-compose.yml\n", "docker-compose.yml"},
		{"several", "A=b\nCOMPOSE_FILE=docker-compose.yml:extra.yml:/srv/abs.yml\n", "docker-compose.yml,extra.yml,/srv/abs.yml"},
		{"crlf", "COMPOSE_FILE=docker-compose.yml:extra.yml\r\n", "docker-compose.yml,extra.yml"},
		{"empty entries dropped", "COMPOSE_FILE=docker-compose.yml::\n", "docker-compose.yml"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := strings.Join(ComposeFiles(c.env), ","); got != c.want {
				t.Fatalf("ComposeFiles(%q) = %q, want %q", c.env, got, c.want)
			}
		})
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
	got := AddComposeFile(env, []string{"docker-compose.yml"}, OverrideFileName)
	if got != env {
		t.Fatalf("idempotency failed on CRLF file: got %q want %q", got, env)
	}
	if strings.Contains(got, "\r:") || strings.Contains(got, ":\r") {
		t.Fatalf("CRLF corruption: %q", got)
	}
}

func TestAddComposeFileToEmptyEnvCRLF(t *testing.T) {
	got := AddComposeFile("A=b\r\n", []string{"docker-compose.yml"}, OverrideFileName)
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

// TestAddComposeFileNamesTheDiscoveredOverride pins the reason AddComposeFile
// takes a list: the entry it writes turns compose's own file discovery off, so
// it has to name the override file discovery would have loaded as well. A line
// naming only the base file would drop it from the operator's own commands.
func TestAddComposeFileNamesTheDiscoveredOverride(t *testing.T) {
	got := AddComposeFile("", []string{"docker-compose.yml", "docker-compose.override.yml"}, OverrideFileName)
	want := "COMPOSE_FILE=docker-compose.yml:docker-compose.override.yml:docker-compose.bloodtrail.yml\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

// TestRemoveComposeFileLine pins that the whole entry goes, whatever it lists:
// restoring a project that had no COMPOSE_FILE means restoring the absence of
// the line, since any line at all keeps discovery switched off.
func TestRemoveComposeFileLine(t *testing.T) {
	env := "A=b\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\nC=d\n"
	if got := RemoveComposeFileLine(env); got != "A=b\nC=d\n" {
		t.Fatalf("got %q", got)
	}
	if got := RemoveComposeFileLine("A=b\n"); got != "A=b\n" {
		t.Fatalf("untouched env changed: %q", got)
	}
	if got := RemoveComposeFileLine("COMPOSE_FILE=docker-compose.yml\r\n"); got != "" {
		t.Fatalf("CRLF sole entry: got %q", got)
	}
}

// TestAutoOverrideCandidates pins the spellings compose itself would look for
// beside a base file, same extension first, and that a base file which is not
// YAML at all has no such sibling.
func TestAutoOverrideCandidates(t *testing.T) {
	cases := []struct {
		base string
		want string
	}{
		{"docker-compose.yml", "docker-compose.override.yml,docker-compose.override.yaml"},
		{"docker-compose.yaml", "docker-compose.override.yaml,docker-compose.override.yml"},
		{"compose.yaml", "compose.override.yaml,compose.override.yml"},
		{"stack.yml", "stack.override.yml,stack.override.yaml"},
		{"docker-compose.json", ""},
		{"Makefile", ""},
	}
	for _, c := range cases {
		t.Run(c.base, func(t *testing.T) {
			if got := strings.Join(AutoOverrideCandidates(c.base), ","); got != c.want {
				t.Fatalf("AutoOverrideCandidates(%q) = %q, want %q", c.base, got, c.want)
			}
		})
	}
}
