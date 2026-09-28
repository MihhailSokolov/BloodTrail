// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"fmt"
	"path"
	"strings"
)

const (
	composeFileKey          = "COMPOSE_FILE"
	composePathSeparatorKey = "COMPOSE_PATH_SEPARATOR"
)

// composeFileEntry is the .env line that sets COMPOSE_FILE, taken apart so it
// can be rewritten in place: everything around the file list (an `export`
// prefix, spacing, the quote character, a trailing comment) is kept exactly
// as the operator wrote it.
type composeFileEntry struct {
	index  int      // line number within splitLines' result
	prefix string   // the line up to where the value starts
	quote  string   // "", "'" or `"`
	files  []string // the list, entries trimmed, empty ones dropped
	suffix string   // what follows the value and its closing quote
}

func (e composeFileEntry) render(files []string) string {
	return e.prefix + e.quote + strings.Join(files, ":") + e.quote + e.suffix
}

// findComposeFile locates and parses the COMPOSE_FILE entry, or returns nil
// when there is none.
//
// It reads the dotenv shapes compose itself accepts for this key -- an
// optional `export `, spaces around `=`, a single- or double-quoted value, an
// unquoted value ending at a ` #` comment -- and fails closed on anything it
// cannot read with certainty, rather than guessing at a file list the
// installer then rewrites: a quoted value that does not close on its line, a
// value with escapes or `$` interpolation, a key with no `=`, more than one
// entry, or a COMPOSE_PATH_SEPARATOR setting (the list is split on ":").
// Misreading used to be silent: a quoted or exported entry did not match at
// all, so the installer appended a second COMPOSE_FILE line (compose honours
// the last one) and rollback left the operator's entry behind.
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
// `=` (leading spaces included). ok is false for a blank line or a comment.
func splitAssignment(line string) (key, value string, hasValue, ok bool) {
	rest := strings.TrimLeft(line, " \t")
	if rest == "" || strings.HasPrefix(rest, "#") {
		return "", "", false, false
	}
	if after, isExport := strings.CutPrefix(rest, "export"); isExport && after != "" && (after[0] == ' ' || after[0] == '\t') {
		rest = strings.TrimLeft(after, " \t")
	}
	key, value, hasValue = strings.Cut(rest, "=")
	return strings.TrimSpace(key), value, hasValue, true
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
		if entry.quote == `"` && strings.ContainsAny(list, `\$`) {
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

	for _, f := range strings.Split(list, ":") {
		if f = strings.TrimSpace(f); f != "" {
			entry.files = append(entry.files, f)
		}
	}
	return entry, nil
}

// commentStart returns where an unquoted value's inline comment begins -- a
// '#' at the start or after a space or tab -- or -1.
func commentStart(v string) int {
	for i := 0; i < len(v); i++ {
		if v[i] == '#' && (i == 0 || v[i-1] == ' ' || v[i-1] == '\t') {
			return i
		}
	}
	return -1
}

// ComposeFiles returns the files listed in the .env contents' COMPOSE_FILE
// entry, in order, or nil when there is no such line. Paths are returned as
// written, so relative entries still have to be resolved against the compose
// project directory. An entry it cannot read with certainty is an error (see
// findComposeFile).
func ComposeFiles(env string) ([]string, error) {
	lines, _ := splitLines(env)
	entry, err := findComposeFile(lines)
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
// discovery would have found -- the base file AND the conventional override
// beside it (AutoOverrideCandidates). Naming only the base file would
// silently drop the operator's docker-compose.override.yml from their own
// commands, permanently and invisibly.
func AddComposeFile(env string, baseFiles []string, overrideFile string) (string, error) {
	lines, lineEnding := splitLines(env)
	entry, err := findComposeFile(lines)
	if err != nil {
		return "", err
	}
	if entry == nil {
		lines = append(lines, composeFileKey+"="+strings.Join(append(append([]string(nil), baseFiles...), overrideFile), ":"))
		return joinLines(lines, lineEnding), nil
	}
	for _, f := range entry.files {
		if f == overrideFile {
			return joinLines(lines, lineEnding), nil
		}
	}
	lines[entry.index] = entry.render(append(entry.files, overrideFile))
	return joinLines(lines, lineEnding), nil
}

// RemoveComposeFile drops the override from COMPOSE_FILE, removing the whole
// line only if the override was the sole entry. Use RemoveComposeFileLine
// instead when the install created the entry itself: what has to be restored
// then is the absence of the line, not a line naming the base file (see
// AddComposeFile for why a present line is not equivalent to no line).
func RemoveComposeFile(env, overrideFile string) (string, error) {
	lines, lineEnding := splitLines(env)
	entry, err := findComposeFile(lines)
	if err != nil || entry == nil {
		return env, err
	}
	var kept []string
	for _, f := range entry.files {
		if f != overrideFile {
			kept = append(kept, f)
		}
	}
	if len(kept) == 0 {
		return joinLines(append(lines[:entry.index:entry.index], lines[entry.index+1:]...), lineEnding), nil
	}
	lines[entry.index] = entry.render(kept)
	return joinLines(lines, lineEnding), nil
}

// RemoveComposeFileLine drops the COMPOSE_FILE entry entirely, whatever it
// lists. This is what restores a project that had no COMPOSE_FILE before the
// install wrote one: leaving a line behind would keep compose's own file
// discovery switched off for good, so the conventional override beside the
// base file would stop being loaded by the operator's own commands.
func RemoveComposeFileLine(env string) (string, error) {
	lines, lineEnding := splitLines(env)
	entry, err := findComposeFile(lines)
	if err != nil || entry == nil {
		return env, err
	}
	return joinLines(append(lines[:entry.index:entry.index], lines[entry.index+1:]...), lineEnding), nil
}

func splitLines(s string) ([]string, string) {
	// Detect line ending convention (CRLF vs LF)
	lineEnding := "\n"
	if strings.Contains(s, "\r\n") {
		lineEnding = "\r\n"
	}

	// Trim trailing line ending
	s = strings.TrimRight(s, "\r\n")
	if s == "" {
		return nil, lineEnding
	}

	return strings.Split(s, lineEnding), lineEnding
}

func joinLines(lines []string, lineEnding string) string {
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, lineEnding) + lineEnding
}

// AutoOverrideCandidates names the override files docker compose would load
// on its own beside baseFile, most preferred first. Compose pairs a base file
// with an "<name>.override.<ext>" sibling and loads it after the base without
// being told to; naming any file with -f (or through COMPOSE_FILE) switches
// that discovery off, so both composeHandle and AddComposeFile have to put
// the sibling back explicitly or the installer and the operator end up
// running two different projects.
//
// The same-extension spelling comes first, then the other one, matching
// compose's own preference. Callers resolve these against the base file's
// directory and keep the ones that exist.
func AutoOverrideCandidates(baseFile string) []string {
	ext := path.Ext(baseFile)
	if ext != ".yml" && ext != ".yaml" {
		return nil
	}
	stem := strings.TrimSuffix(baseFile, ext)
	other := ".yaml"
	if ext == ".yaml" {
		other = ".yml"
	}
	return []string{stem + ".override" + ext, stem + ".override" + other}
}
