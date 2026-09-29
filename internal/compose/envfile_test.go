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
// spaces around '=', a trailing comment, the YAML-style `KEY: value` its
// parser also takes, and a CRLF line in a file that otherwise ends lines
// with LF (compose reads every line whatever its ending). Each must be read
// as the same list, extended in place (keeping its spelling, never adding a
// second line, which compose would honour instead), and restored in place.
// These used to be missed entirely: the installer appended a second entry,
// and rollback left the operator's own line behind.
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
		{"yaml-style", `COMPOSE_FILE: docker-compose.yml:extra.yml`, `COMPOSE_FILE: docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml`},
		{"yaml-style unspaced", `COMPOSE_FILE:docker-compose.yml:extra.yml`, `COMPOSE_FILE:docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml`},
		{"CRLF line in an LF file", "COMPOSE_FILE=docker-compose.yml:extra.yml\r", "COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\r"},
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

// TestComposeFileEntryIsFoundWhateverTheFileAroundIt covers what surrounds
// the entry rather than its own spelling: a UTF-8 byte order mark, which
// Windows editors write and compose skips, an LF line in a file that
// otherwise ends lines with CRLF, and blank lines at the end. Compose reads
// the entry in each. The parser used to miss it after a byte order mark, and
// split a mixed file on CRLF alone -- missing the entry, or reading the next
// line into it -- so the installer appended a second entry; and every
// rewrite dropped the blank lines.
func TestComposeFileEntryIsFoundWhateverTheFileAroundIt(t *testing.T) {
	cases := []struct {
		name, env, added, gone string
	}{
		{"byte order mark",
			"\ufeffCOMPOSE_FILE=docker-compose.yml:extra.yml\nA=b\n",
			"\ufeffCOMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\nA=b\n",
			"\ufeffA=b\n"},
		{"LF line in a CRLF file",
			"A=b\r\nCOMPOSE_FILE=docker-compose.yml:extra.yml\nC=d\r\n",
			"A=b\r\nCOMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\nC=d\r\n",
			"A=b\r\nC=d\r\n"},
		{"trailing blank lines",
			"COMPOSE_FILE=docker-compose.yml:extra.yml\n\n",
			"COMPOSE_FILE=docker-compose.yml:extra.yml:docker-compose.bloodtrail.yml\n\n",
			"\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := strings.Join(mustFiles(t, c.env), ","); got != "docker-compose.yml,extra.yml" {
				t.Fatalf("ComposeFiles = %q", got)
			}
			added := mustAdd(t, c.env, []string{"docker-compose.yml"}, OverrideFileName)
			if added != c.added {
				t.Fatalf("AddComposeFile = %q, want %q", added, c.added)
			}
			if removed := mustRemove(t, added, OverrideFileName); removed != c.env {
				t.Fatalf("RemoveComposeFile = %q, want the original %q", removed, c.env)
			}
			if gone := mustRemoveLine(t, added); gone != c.gone {
				t.Fatalf("RemoveComposeFileLine = %q, want %q", gone, c.gone)
			}
		})
	}
}

// TestComposeFileLineInsideAnotherValueIsNotTheEntry covers a COMPOSE_FILE
// line inside another variable's quoted value, which compose reads on across
// lines to the closing quote -- a backslash-escaped quote does not close it.
// That line is part of the value, not an entry (confirmed with Compose
// v5.1.1), but used to be read as one: as the entry, or as a second entry
// beside the real one.
func TestComposeFileLineInsideAnotherValueIsNotTheEntry(t *testing.T) {
	for _, c := range []struct {
		name, env, files string
	}{
		{"double quotes", "CERT=\"-----BEGIN\nCOMPOSE_FILE=evil.yml\n-----END\"\n", ""},
		{"single quotes", "CERT='-----BEGIN\nCOMPOSE_FILE=evil.yml\n-----END'\n", ""},
		{"an escaped quote", "CERT=\"a\\\"\nCOMPOSE_FILE=evil.yml\n\"\n", ""},
		{"YAML-style key", "CERT: '-----BEGIN\nCOMPOSE_FILE=evil.yml\n-----END' # pem\n", ""},
		{"the entry after it", "CERT=\"-----BEGIN\nCOMPOSE_FILE=evil.yml\n-----END\"\nCOMPOSE_FILE=docker-compose.yml:extra.yml\n", "docker-compose.yml,extra.yml"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := strings.Join(mustFiles(t, c.env), ","); got != c.files {
				t.Fatalf("ComposeFiles = %q, want %q", got, c.files)
			}
			added := mustAdd(t, c.env, []string{"docker-compose.yml"}, OverrideFileName)
			want := c.env + "COMPOSE_FILE=docker-compose.yml:docker-compose.bloodtrail.yml\n"
			if c.files != "" {
				want = strings.Replace(c.env, ":extra.yml\n", ":extra.yml:docker-compose.bloodtrail.yml\n", 1)
			}
			if added != want {
				t.Fatalf("AddComposeFile = %q, want %q", added, want)
			}
		})
	}
}

// TestRestoreEmptyComposeFile pins what rollback puts back when an earlier
// install took an empty entry for none and wrote its override into it --
// `COMPOSE_FILE=docker-compose.bloodtrail.yml` in the operator's own spelling,
// or `COMPOSE_FILE=:docker-compose.bloodtrail.yml` from before entries were
// parsed at all: the empty entry, exactly as it was.
func TestRestoreEmptyComposeFile(t *testing.T) {
	for _, c := range []struct {
		env, want string
	}{
		{"A=b\nCOMPOSE_FILE=docker-compose.bloodtrail.yml\nC=d\n", "A=b\nCOMPOSE_FILE=\nC=d\n"},
		{"COMPOSE_FILE=\"docker-compose.bloodtrail.yml\"\n", "COMPOSE_FILE=\"\"\n"},
		{"export COMPOSE_FILE='docker-compose.bloodtrail.yml' # ours\n", "export COMPOSE_FILE='' # ours\n"},
		{"COMPOSE_FILE=:docker-compose.bloodtrail.yml\n", "COMPOSE_FILE=\n"},
		{"A=b\r\nCOMPOSE_FILE=docker-compose.bloodtrail.yml\r\n", "A=b\r\nCOMPOSE_FILE=\r\n"},
		{"COMPOSE_FILE=\n", "COMPOSE_FILE=\n"},
		{"A=b\n", "A=b\n"},
	} {
		if got, err := RestoreEmptyComposeFile(c.env, OverrideFileName); err != nil || got != c.want {
			t.Errorf("RestoreEmptyComposeFile(%q) = %q, %v; want %q", c.env, got, err, c.want)
		}
	}
}

// TestAddedLineComesOffWithoutATrace pins what rollback relies on when the
// install created the entry: adding the line and removing it again gives the
// file back byte for byte -- blank lines, byte order mark and line endings
// included.
func TestAddedLineComesOffWithoutATrace(t *testing.T) {
	for _, env := range []string{"", "A=b\n", "A=b\n\n", "\ufeffA=b\n", "A=b\r\n\r\n"} {
		added := mustAdd(t, env, []string{"docker-compose.yml"}, OverrideFileName)
		if gone := mustRemoveLine(t, added); gone != env {
			t.Errorf("adding to %q and removing the line again gave %q", env, gone)
		}
	}
}

// TestComposeFileEntryFailsClosed pins that an entry the parser cannot read
// with certainty is an error from every function, never a guess the
// installer then writes back -- rollback's readers included, since a misread
// line is one rollback would rewrite. That covers an assignment compose reads
// after another value's closing quote on the same line, which cannot be
// rewritten in place.
func TestComposeFileEntryFailsClosed(t *testing.T) {
	for _, env := range []string{
		"COMPOSE_FILE=\"docker-compose.yml:extra.yml\n",                         // unterminated quote
		"COMPOSE_FILE=\"docker-compose.yml:${EXTRA}\"\n",                        // interpolation
		"COMPOSE_FILE=docker-compose.yml:$EXTRA\n",                              // interpolation
		"COMPOSE_FILE=\"docker-compose.yml\\:extra.yml\"\n",                     // escape
		"COMPOSE_FILE='docker-compose.yml:extra.yml\\' # x'\n",                  // escaped quote, which compose honours in single quotes too
		"COMPOSE_FILE=\"docker-compose.yml\" extra.yml\n",                       // text after the quote
		"COMPOSE_FILE=a.yml\nCOMPOSE_FILE=b.yml\n",                              // two entries
		"COMPOSE_FILE=a.yml\nCOMPOSE_FILE: b.yml\n",                             // two entries, one YAML-style
		"export COMPOSE_FILE\n",                                                 // no value
		"COMPOSE_PATH_SEPARATOR=;\nCOMPOSE_FILE=a.yml;b.yml\n",                  // another separator
		"COMPOSE_PATH_SEPARATOR: ;\nCOMPOSE_FILE=a.yml;b.yml\n",                 // another separator, YAML-style
		"A=\"x\" COMPOSE_FILE=a.yml\n",                                          // after another value's closing quote
		"A='x\ny' COMPOSE_FILE=a.yml\n",                                         // after a multi-line value closes
		"A=\"x\" B=\"y\" export COMPOSE_PATH_SEPARATOR=;\nCOMPOSE_FILE=a.yml\n", // separator, two values along
	} {
		t.Run(env, func(t *testing.T) {
			if _, err := ComposeFiles(env); err == nil {
				t.Errorf("ComposeFiles accepted %q", env)
			}
			if _, err := ListedComposeFiles(env); err == nil {
				t.Errorf("ListedComposeFiles accepted %q", env)
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
			if _, err := RestoreEmptyComposeFile(env, OverrideFileName); err == nil {
				t.Errorf("RestoreEmptyComposeFile accepted %q", env)
			}
		})
	}

	// Look-alikes that are not the entry are left alone.
	for _, env := range []string{"# COMPOSE_FILE=a.yml\n", "COMPOSE_FILES=a.yml\n", "MY_COMPOSE_FILE=a.yml\n", "A=x COMPOSE_FILE=a.yml\n"} {
		if files := mustFiles(t, env); files != nil {
			t.Errorf("ComposeFiles(%q) = %q, want no entry", env, files)
		}
	}
}

// TestComposeFileListComposeCannotLoad covers lists compose itself fails to
// load: an empty name between separators, which it resolves to the project
// directory, and spaces around a name, which it keeps as part of it. These
// used to be tidied up -- dropped or trimmed -- so the installer ran
// against, and wrote back, a project compose never loads; the readers
// install goes by now refuse them. Rollback's readers take the names as
// written instead: rollback has to undo what earlier installs left, some of
// it in exactly these shapes, and only ever takes the installer's own file
// out of the list.
func TestComposeFileListComposeCannotLoad(t *testing.T) {
	for _, c := range []struct {
		env, listed string
	}{
		{"COMPOSE_FILE=docker-compose.yml::extra.yml\n", "docker-compose.yml,,extra.yml"},
		{"COMPOSE_FILE=docker-compose.yml:\n", "docker-compose.yml,"},
		{"COMPOSE_FILE=:docker-compose.yml\n", ",docker-compose.yml"},
		{"COMPOSE_FILE=docker-compose.yml : extra.yml\n", "docker-compose.yml , extra.yml"},
		{"COMPOSE_FILE=\" docker-compose.yml:extra.yml\"\n", " docker-compose.yml,extra.yml"},
	} {
		t.Run(c.env, func(t *testing.T) {
			if _, err := ComposeFiles(c.env); err == nil {
				t.Errorf("ComposeFiles accepted %q", c.env)
			}
			if _, err := AddComposeFile(c.env, []string{"docker-compose.yml"}, OverrideFileName); err == nil {
				t.Errorf("AddComposeFile accepted %q", c.env)
			}
			listed, err := ListedComposeFiles(c.env)
			if err != nil || strings.Join(listed, ",") != c.listed {
				t.Errorf("ListedComposeFiles(%q) = %q, %v; want %q", c.env, listed, err, c.listed)
			}
			added := strings.Replace(c.env, "extra.yml", "extra.yml:"+OverrideFileName, 1)
			if added != c.env {
				if got, err := RemoveComposeFile(added, OverrideFileName); err != nil || got != c.env {
					t.Errorf("RemoveComposeFile(%q) = %q, %v; want %q", added, got, err, c.env)
				}
			}
		})
	}
}

// TestComposeFileEntryListingNoFiles pins that an entry naming no file is
// refused by the readers install goes by, whatever its spelling, rather than
// read as no entry. Compose does not treat it as unset: the key alone
// switches its file discovery off, and the empty path resolves to the
// project directory, so every plain `docker compose` there fails to load the
// project. Read as no entry, it used to be rewritten to name the installer's
// override alone, dropping the base file and the conventional override from
// the operator's own commands. Rollback's readers see it as the entry it is,
// and leave it as it is.
func TestComposeFileEntryListingNoFiles(t *testing.T) {
	for _, env := range []string{
		"COMPOSE_FILE=\n",
		"COMPOSE_FILE=\"\"\n",
		"COMPOSE_FILE=''\n",
		"export COMPOSE_FILE=\n",
		"COMPOSE_FILE = \n",
		"COMPOSE_FILE=:\n",
		"COMPOSE_FILE= # set per host\n",
		"COMPOSE_FILE=\"\"  # set per host\n",
		"A=b\r\nCOMPOSE_FILE=\r\nC=d\r\n",
	} {
		t.Run(env, func(t *testing.T) {
			if files, err := ComposeFiles(env); err == nil || !strings.Contains(err.Error(), "COMPOSE_FILE lists no files") {
				t.Errorf("ComposeFiles(%q) = %q, %v; want an error saying the entry lists no files", env, files, err)
			}
			if out, err := AddComposeFile(env, []string{"docker-compose.yml", "docker-compose.override.yml"}, OverrideFileName); err == nil {
				t.Errorf("AddComposeFile accepted %q and wrote %q", env, out)
			}
			if files, err := ListedComposeFiles(env); err != nil || files == nil || strings.Join(files, "") != "" {
				t.Errorf("ListedComposeFiles(%q) = %q, %v; want the entry, naming nothing", env, files, err)
			}
			for name, remove := range map[string]func(string, string) (string, error){"RemoveComposeFile": RemoveComposeFile, "RestoreEmptyComposeFile": RestoreEmptyComposeFile} {
				if out, err := remove(env, OverrideFileName); err != nil || out != env {
					t.Errorf("%s(%q) = %q, %v; want it left as it is", name, env, out, err)
				}
			}
		})
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
		// Compose starts an inline comment only at " #": a '#' after a tab,
		// or inside a name, is part of the file name, and reading it any
		// other way would address a project compose does not load.
		{"comment after a space", "COMPOSE_FILE=docker-compose.yml:extra.yml # ours\n", "docker-compose.yml,extra.yml"},
		{"no comment after a tab", "COMPOSE_FILE=docker-compose.yml:extra.yml\t# ours\n", "docker-compose.yml,extra.yml\t# ours"},
		{"no comment inside a name", "COMPOSE_FILE=docker-compose.yml:extra#1.yml\n", "docker-compose.yml,extra#1.yml"},
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

// TestDefaultFileNamesFollowCompose pins compose's own discovery order
// (compose-go v2.10.1's cli.DefaultFileNames and DefaultOverrideFileNames).
// The override is not paired with the base file by name or extension: it
// used to be, which missed compose.override.* and preferred the wrong
// spelling when there were two.
func TestDefaultFileNamesFollowCompose(t *testing.T) {
	if got, want := strings.Join(DefaultFileNames(), ","), "compose.yaml,compose.yml,docker-compose.yml,docker-compose.yaml"; got != want {
		t.Errorf("DefaultFileNames = %s, want %s", got, want)
	}
	if got, want := strings.Join(DefaultOverrideFileNames(), ","), "compose.override.yml,compose.override.yaml,docker-compose.override.yml,docker-compose.override.yaml"; got != want {
		t.Errorf("DefaultOverrideFileNames = %s, want %s", got, want)
	}
}
