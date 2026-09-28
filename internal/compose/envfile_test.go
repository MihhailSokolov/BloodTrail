// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"strings"
	"testing"
)

func mustAdd(t *testing.T, env string, baseFiles []string, overrideFile string) string {
	t.Helper()
	out, err := AddComposeFile(env, baseFiles, overrideFile)
	if err != nil {
		t.Fatalf("AddComposeFile(%q): %v", env, err)
	}
	return out
}

func mustRemove(t *testing.T, env, overrideFile string) string {
	t.Helper()
	out, err := RemoveComposeFile(env, overrideFile)
	if err != nil {
		t.Fatalf("RemoveComposeFile(%q): %v", env, err)
	}
	return out
}

func mustRemoveLine(t *testing.T, env string) string {
	t.Helper()
	out, err := RemoveComposeFileLine(env)
	if err != nil {
		t.Fatalf("RemoveComposeFileLine(%q): %v", env, err)
	}
	return out
}

func mustFiles(t *testing.T, env string) []string {
	t.Helper()
	files, err := ComposeFiles(env)
	if err != nil {
		t.Fatalf("ComposeFiles(%q): %v", env, err)
	}
	return files
}

// TestComposeFileEntrySpellingsAreReadAndRewrittenInPlace pins the dotenv
// shapes compose accepts for COMPOSE_FILE: quoted values, an export prefix,
// spaces around '=', a trailing comment. Each must be read as the same list,
// extended in place (keeping its spelling, never adding a second line, which
// compose would honour instead), and restored in place. These used to be
// missed entirely: the installer appended a second entry, and rollback left
// the operator's own line behind.
func TestComposeFileEntrySpellingsAreReadAndRewrittenInPlace(t *testing.T) {
	cases := []struct {
		name, line, added string
	}{
		{"double-quoted", `COMPOSE_FILE="docker-compose.yml:extra.yml"`, `COMPOSE_FILE="docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml"`},
		{"single-quoted", `COMPOSE_FILE='docker-compose.yml:extra.yml'`, `COMPOSE_FILE='docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml'`},
		{"exported", `export COMPOSE_FILE=docker-compose.yml:extra.yml`, `export COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml`},
		{"exported and quoted", `export COMPOSE_FILE="docker-compose.yml:extra.yml"`, `export COMPOSE_FILE="docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml"`},
		{"spaced", `COMPOSE_FILE = docker-compose.yml:extra.yml`, `COMPOSE_FILE = docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml`},
		{"commented", `COMPOSE_FILE=docker-compose.yml:extra.yml # ours`, `COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml # ours`},
		{"quoted and commented", `COMPOSE_FILE="docker-compose.yml:extra.yml"  # ours`, `COMPOSE_FILE="docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml"  # ours`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := "A=b\n" + c.line + "\nC=d\n"
			if got := strings.Join(mustFiles(t, env), ","); got != "docker-compose.yml,extra.yml" {
				t.Fatalf("ComposeFiles = %q", got)
			}
			added := mustAdd(t, env, []string{"docker-compose.yml"}, OverrideFileName)
			if want := "A=b\n" + c.added + "\nC=d\n"; added != want {
				t.Fatalf("AddComposeFile = %q, want %q", added, want)
			}
			if again := mustAdd(t, added, []string{"docker-compose.yml"}, OverrideFileName); again != added {
				t.Fatalf("second add changed the file: %q", again)
			}
			if removed := mustRemove(t, added, OverrideFileName); removed != env {
				t.Fatalf("RemoveComposeFile = %q, want the original %q", removed, env)
			}
			if gone := mustRemoveLine(t, added); gone != "A=b\nC=d\n" {
				t.Fatalf("RemoveComposeFileLine = %q", gone)
			}
		})
	}
}

// TestComposeFileEntryFailsClosed pins that an entry the parser cannot read
// with certainty is an error from every function, never a guess the
// installer then writes back.
func TestComposeFileEntryFailsClosed(t *testing.T) {
	for _, env := range []string{
		"COMPOSE_FILE=\"docker-compose.yml:extra.yml\n",        // unterminated quote
		"COMPOSE_FILE=\"docker-compose.yml:${EXTRA}\"\n",       // interpolation
		"COMPOSE_FILE=docker-compose.yml:$EXTRA\n",             // interpolation
		"COMPOSE_FILE=\"docker-compose.yml\\:extra.yml\"\n",    // escape
		"COMPOSE_FILE=\"docker-compose.yml\" extra.yml\n",      // text after the quote
		"COMPOSE_FILE=a.yml\nCOMPOSE_FILE=b.yml\n",             // two entries
		"export COMPOSE_FILE\n",                                // no value
		"COMPOSE_PATH_SEPARATOR=;\nCOMPOSE_FILE=a.yml;b.yml\n", // another separator
	} {
		t.Run(env, func(t *testing.T) {
			if _, err := ComposeFiles(env); err == nil {
				t.Errorf("ComposeFiles accepted %q", env)
			}
			if _, err := AddComposeFile(env, []string{"docker-compose.yml"}, OverrideFileName); err == nil {
				t.Errorf("AddComposeFile accepted %q", env)
			}
			if _, err := RemoveComposeFile(env, OverrideFileName); err == nil {
				t.Errorf("RemoveComposeFile accepted %q", env)
			}
			if _, err := RemoveComposeFileLine(env); err == nil {
				t.Errorf("RemoveComposeFileLine accepted %q", env)
			}
		})
	}

	// Look-alikes that are not the entry are left alone.
	for _, env := range []string{"# COMPOSE_FILE=a.yml\n", "COMPOSE_FILES=a.yml\n", "MY_COMPOSE_FILE=a.yml\n"} {
		if files := mustFiles(t, env); files != nil {
			t.Errorf("ComposeFiles(%q) = %q, want no entry", env, files)
		}
	}
}

func TestAddComposeFileToEmptyEnv(t *testing.T) {
	got := mustAdd(t, "", []string{"docker-compose.yml"}, OverrideFileName)
	if got != "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n" {
		t.Fatalf("got %q", got)
	}
}

func TestAddComposeFileExtendsExisting(t *testing.T) {
	env := "FOO=1\nCOMPOSE_FILE=docker-compose.yml:extra.yml\n"
	got := mustAdd(t, env, []string{"docker-compose.yml"}, OverrideFileName)
	want := "FOO=1\nCOMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestAddComposeFileIsIdempotent(t *testing.T) {
	once := mustAdd(t, "A=b\n", []string{"docker-compose.yml"}, OverrideFileName)
	twice := mustAdd(t, once, []string{"docker-compose.yml"}, OverrideFileName)
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
			if got := strings.Join(mustFiles(t, c.env), ","); got != c.want {
				t.Fatalf("ComposeFiles(%q) = %q, want %q", c.env, got, c.want)
			}
		})
	}
}

func TestRemoveComposeFile(t *testing.T) {
	env := "A=b\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"
	if got := mustRemove(t, env, OverrideFileName); got != "A=b\nCOMPOSE_FILE=docker-compose.yml\n" {
		t.Fatalf("got %q", got)
	}
	env = "COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n"
	if got := mustRemove(t, env, OverrideFileName); got != "COMPOSE_FILE=docker-compose.yml:extra.yml\n" {
		t.Fatalf("got %q", got)
	}
	if got := mustRemove(t, "A=b\n", OverrideFileName); got != "A=b\n" {
		t.Fatalf("untouched file changed: %q", got)
	}
}

func TestAddComposeFileCRLFIdempotent(t *testing.T) {
	env := "A=b\r\nCOMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\r\n"
	got := mustAdd(t, env, []string{"docker-compose.yml"}, OverrideFileName)
	if got != env {
		t.Fatalf("idempotency failed on CRLF file: got %q want %q", got, env)
	}
	if strings.Contains(got, "\r:") || strings.Contains(got, ":\r") {
		t.Fatalf("CRLF corruption: %q", got)
	}
}

func TestAddComposeFileToEmptyEnvCRLF(t *testing.T) {
	got := mustAdd(t, "A=b\r\n", []string{"docker-compose.yml"}, OverrideFileName)
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
	got := mustRemove(t, env, OverrideFileName)
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
	got := mustAdd(t, "", []string{"docker-compose.yml", "docker-compose.override.yml"}, OverrideFileName)
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
	if got := mustRemoveLine(t, env); got != "A=b\nC=d\n" {
		t.Fatalf("got %q", got)
	}
	if got := mustRemoveLine(t, "A=b\n"); got != "A=b\n" {
		t.Fatalf("untouched env changed: %q", got)
	}
	if got := mustRemoveLine(t, "COMPOSE_FILE=docker-compose.yml\r\n"); got != "" {
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
