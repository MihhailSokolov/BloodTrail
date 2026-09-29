// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"fmt"
	"strings"
)

const (
	composeFileKey          = "COMPOSE_FILE"
	composePathSeparatorKey = "COMPOSE_PATH_SEPARATOR"
	utf8BOM                 = "\ufeff"
)

// composeFileEntry is the .env line that sets COMPOSE_FILE, taken apart so it
// can be rewritten in place: everything around the file list (an `export`
// prefix, spacing, the quote character, a trailing comment, the line's own
// ending) is kept exactly as the operator wrote it.
type composeFileEntry struct {
	index  int      // line number within envFile.lines
	prefix string   // the line up to where the value starts
	quote  string   // "", "'" or `"`
	files  []string // the list as compose reads it: never empty, no name empty or space-padded
	suffix string   // what follows the value and its closing quote
}

func (e composeFileEntry) render(files []string) string {
	return e.prefix + e.quote + strings.Join(files, ":") + e.quote + e.suffix
}

// findComposeFile locates and parses the COMPOSE_FILE entry, or returns nil
// when there is none.
//
// It reads the dotenv shapes compose itself accepts for this key -- an
// optional `export `, spaces around `=` (or around the `:` of the YAML-style
// `KEY: value` compose's parser also takes), a single- or double-quoted
// value, an unquoted value ending at a ` #` comment -- and fails closed on
// anything it cannot read with certainty, rather than guessing at a file list
// the installer then rewrites: a quoted value that does not close on its
// line, a value with escapes or `$` interpolation, a key with no value, more
// than one entry, or a COMPOSE_PATH_SEPARATOR setting (the list is split on
// ":"). Misreading used to be silent: a quoted, exported or YAML-style entry
// did not match at all, so the installer appended a second COMPOSE_FILE line
// (compose honours the last one) and rollback left the operator's entry
// behind.
//
// A list compose itself fails to load is refused as well, never tidied up
// into one it would load. An entry that lists no files (`COMPOSE_FILE=`,
// `COMPOSE_FILE=""`) is not unset to compose: the key alone switches its file
// discovery off, and the empty path resolves to the project directory, so
// every plain `docker compose` there fails. Taken for no entry, it used to be
// rewritten to name the installer's override alone, dropping the base file
// and the conventional override from the operator's own commands, and
// rollback then deleted the line outright. An empty name between separators
// fails the same way, and compose keeps spaces around a name as part of it.
func findComposeFile(lines []string) (*composeFileEntry, error) {
	var found *composeFileEntry
	for i, raw := range lines {
		line := strings.TrimRight(raw, "\r")
		cr := raw[len(line):]
		key, value, hasValue, ok := splitAssignment(line)
		if !ok {
			continue
		}
		switch key {
		case composePathSeparatorKey:
			return nil, fmt.Errorf(".env line %d sets %s, which this installer does not support; remove it or set COMPOSE_FILE by hand", i+1, composePathSeparatorKey)
		case composeFileKey:
		default:
			continue
		}
		if !hasValue {
			return nil, fmt.Errorf(".env line %d names %s without a value", i+1, composeFileKey)
		}
		if found != nil {
			return nil, fmt.Errorf(".env sets %s more than once (lines %d and %d); keep one", composeFileKey, found.index+1, i+1)
		}
		entry, err := parseComposeFileValue(line[:len(line)-len(value)], value)
		if err != nil {
			return nil, fmt.Errorf(".env line %d: %w", i+1, err)
		}
		entry.index = i
		entry.suffix += cr
		found = &entry
	}
	return found, nil
}

// splitAssignment splits a dotenv line into its key and the raw text after
// the `=`, or after the `:` of a YAML-style `KEY: value` -- whichever comes
// first, as in compose's own parser -- leading spaces included. ok is false
// for a blank line or a comment.
func splitAssignment(line string) (key, value string, hasValue, ok bool) {
	rest := strings.TrimLeft(line, " \t")
	if rest == "" || strings.HasPrefix(rest, "#") {
		return "", "", false, false
	}
	if after, isExport := strings.CutPrefix(rest, "export"); isExport && after != "" && (after[0] == ' ' || after[0] == '\t') {
		rest = strings.TrimLeft(after, " \t")
	}
	at := strings.IndexAny(rest, "=:")
	if at < 0 {
		return strings.TrimSpace(rest), "", false, true
	}
	return strings.TrimSpace(rest[:at]), rest[at+1:], true, true
}

// parseComposeFileValue parses the value part of a COMPOSE_FILE line; head is
// the line up to that part.
func parseComposeFileValue(head, value string) (composeFileEntry, error) {
	trimmed := strings.TrimLeft(value, " \t")
	entry := composeFileEntry{prefix: head + value[:len(value)-len(trimmed)]}

	var list string
	if trimmed != "" && (trimmed[0] == '\'' || trimmed[0] == '"') {
		entry.quote = trimmed[:1]
		end := strings.IndexByte(trimmed[1:], trimmed[0])
		if end < 0 {
			return entry, fmt.Errorf("%s: quoted value does not close on its line", composeFileKey)
		}
		list = trimmed[1 : 1+end]
		entry.suffix = trimmed[2+end:]
		if rest := strings.TrimLeft(entry.suffix, " \t"); rest != "" && !strings.HasPrefix(rest, "#") {
			return entry, fmt.Errorf("%s: unexpected text after the closing quote", composeFileKey)
		}
		// Compose honours a backslash before the closing quote in single
		// quotes too, so the value need not end where it seems to.
		if strings.Contains(list, `\`) || (entry.quote == `"` && strings.Contains(list, "$")) {
			return entry, fmt.Errorf("%s: escapes and $ interpolation are not supported", composeFileKey)
		}
	} else {
		list = trimmed
		if at := commentStart(trimmed); at >= 0 {
			list = trimmed[:at]
		}
		list = strings.TrimRight(list, " \t")
		entry.suffix = trimmed[len(list):]
		if strings.ContainsAny(list, "\\$\"'") {
			return entry, fmt.Errorf("%s: quotes, escapes and $ interpolation inside an unquoted value are not supported", composeFileKey)
		}
	}

	if strings.Trim(list, ": \t") == "" {
		return entry, fmt.Errorf("%s lists no files; docker compose does not read that as unset but fails to load the project, so delete the line to let compose find its files on its own, or list them", composeFileKey)
	}
	for _, f := range strings.Split(list, ":") {
		switch {
		case f == "":
			return entry, fmt.Errorf("%s has an empty entry, which docker compose reads as the project directory and fails on; remove the extra ':'", composeFileKey)
		case strings.TrimSpace(f) != f:
			return entry, fmt.Errorf("%s: docker compose keeps the spaces around %q as part of the file name; remove them", composeFileKey, f)
		}
		entry.files = append(entry.files, f)
	}
	return entry, nil
}

// commentStart returns where an unquoted value's inline comment begins, or
// -1. Compose starts one only at " #": after a tab, or inside a name, '#' is
// part of the file name. A '#' opening the value is taken as a comment as
// well, which leaves the entry listing no files and so refused -- compose
// reads it as the start of a file name, which it then fails to find.
func commentStart(v string) int {
	if strings.HasPrefix(v, "#") {
		return 0
	}
	if i := strings.Index(v, " #"); i >= 0 {
		return i + 1
	}
	return -1
}

// ComposeFiles returns the files listed in the .env contents' COMPOSE_FILE
// entry, in order, or nil when there is no such line. Paths are returned as
// written, so relative entries still have to be resolved against the compose
// project directory. An entry it cannot read with certainty, or one that
// lists no files, is an error (see findComposeFile), so nil always means
// there is no line.
func ComposeFiles(env string) ([]string, error) {
	entry, err := findComposeFile(parseEnvFile(env).lines)
	if err != nil || entry == nil {
		return nil, err
	}
	return entry.files, nil
}

// AddComposeFile ensures the .env contents make docker compose load the
// override after the files the project is already made of, so a plain
// `docker compose up -d` keeps it. An existing entry is extended in place,
// keeping its spelling.
//
// baseFiles is that existing project, in merge order, and is used only when
// there is no COMPOSE_FILE entry yet: writing one turns off compose's own
// file discovery for every later command, so the line has to name everything
// discovery would have found -- the base file AND the override compose loads
// beside it (DefaultOverrideFileNames). Naming only the base file would
// silently drop the operator's docker-compose.override.yml from their own
// commands, permanently and invisibly.
func AddComposeFile(env string, baseFiles []string, overrideFile string) (string, error) {
	f := parseEnvFile(env)
	entry, err := findComposeFile(f.lines)
	if err != nil {
		return "", err
	}
	if entry == nil {
		return f.appendLine(composeFileKey + "=" + strings.Join(append(append([]string(nil), baseFiles...), overrideFile), ":")).String(), nil
	}
	for _, file := range entry.files {
		if file == overrideFile {
			return env, nil
		}
	}
	f.lines[entry.index] = entry.render(append(entry.files, overrideFile))
	return f.String(), nil
}

// RemoveComposeFile drops the override from COMPOSE_FILE, removing the whole
// line only if the override was the sole entry. Use RemoveComposeFileLine
// instead when the install created the entry itself: what has to be restored
// then is the absence of the line, not a line naming the base file (see
// AddComposeFile for why a present line is not equivalent to no line).
func RemoveComposeFile(env, overrideFile string) (string, error) {
	f := parseEnvFile(env)
	entry, err := findComposeFile(f.lines)
	if err != nil || entry == nil {
		return env, err
	}
	var kept []string
	for _, file := range entry.files {
		if file != overrideFile {
			kept = append(kept, file)
		}
	}
	if len(kept) == 0 {
		return f.removeLine(entry.index).String(), nil
	}
	f.lines[entry.index] = entry.render(kept)
	return f.String(), nil
}

// RemoveComposeFileLine drops the COMPOSE_FILE entry entirely, whatever it
// lists. This is what restores a project that had no COMPOSE_FILE before the
// install wrote one: leaving a line behind would keep compose's own file
// discovery switched off for good, so the conventional override beside the
// base file would stop being loaded by the operator's own commands.
func RemoveComposeFileLine(env string) (string, error) {
	f := parseEnvFile(env)
	entry, err := findComposeFile(f.lines)
	if err != nil || entry == nil {
		return env, err
	}
	return f.removeLine(entry.index).String(), nil
}

// envFile is .env contents cut into lines at "\n" alone, each keeping its own
// "\r": compose reads every line whatever its ending, so a file that mixes
// the two has to be read line by line as well. A leading UTF-8 byte order
// mark, which compose skips, is held apart and written back as it was found.
// Joining the lines again gives back the contents byte for byte.
type envFile struct {
	bom   string
	lines []string // the last holds what follows the final "\n": "" when the file ends with one
}

func parseEnvFile(s string) envFile {
	var f envFile
	if rest, ok := strings.CutPrefix(s, utf8BOM); ok {
		f.bom, s = utf8BOM, rest
	}
	f.lines = strings.Split(s, "\n")
	return f
}

func (f envFile) String() string {
	return f.bom + strings.Join(f.lines, "\n")
}

// appendLine adds line at the end, ended -- like the file's last line, when
// that has no line break yet -- with CRLF if the file uses it anywhere.
func (f envFile) appendLine(line string) envFile {
	eol := ""
	for _, l := range f.lines[:len(f.lines)-1] {
		if strings.HasSuffix(l, "\r") {
			eol = "\r"
			break
		}
	}
	lines := append([]string(nil), f.lines...)
	if last := len(lines) - 1; lines[last] != "" {
		lines[last] += eol
		lines = append(lines, "")
	}
	lines[len(lines)-1] = line + eol
	return envFile{bom: f.bom, lines: append(lines, "")}
}

// removeLine drops line i together with its line break.
func (f envFile) removeLine(i int) envFile {
	lines := append(append([]string(nil), f.lines[:i]...), f.lines[i+1:]...)
	if len(lines) == 0 {
		lines = []string{""}
	}
	return envFile{bom: f.bom, lines: lines}
}

// DefaultFileNames names the files docker compose looks for when it is given
// neither -f nor COMPOSE_FILE, in its order of preference: it loads the first
// one that exists in the project directory as the base file (compose-go's
// cli.DefaultFileNames).
func DefaultFileNames() []string {
	return []string{"compose.yaml", "compose.yml", "docker-compose.yml", "docker-compose.yaml"}
}

// DefaultOverrideFileNames names the override files compose then looks for
// beside that base file, in its order of preference: it merges the first one
// that exists, whatever the base file itself is called (compose-go's
// cli.DefaultOverrideFileNames). Naming any file with -f, or through
// COMPOSE_FILE, switches both lookups off, so composeHandle and the entry
// AddComposeFile writes have to name that override explicitly, or the
// installer and the operator end up running two different projects.
func DefaultOverrideFileNames() []string {
	return []string{"compose.override.yml", "compose.override.yaml", "docker-compose.override.yml", "docker-compose.override.yaml"}
}
