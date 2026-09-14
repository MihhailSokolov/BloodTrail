// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"path"
	"strings"
)

const composeFileKey = "COMPOSE_FILE="

// ComposeFiles returns the files listed in the .env contents' COMPOSE_FILE
// entry, in order, or nil when there is no such line. Paths are returned as
// written, so relative entries still have to be resolved against the compose
// project directory.
func ComposeFiles(env string) []string {
	lines, _ := splitLines(env)
	for _, line := range lines {
		if !strings.HasPrefix(line, composeFileKey) {
			continue
		}
		var files []string
		for _, f := range strings.Split(strings.TrimPrefix(line, composeFileKey), ":") {
			if f = strings.TrimSpace(stripCR(f)); f != "" {
				files = append(files, f)
			}
		}
		return files
	}
	return nil
}

// AddComposeFile ensures the .env contents make docker compose load the
// override after the files the project is already made of, so a plain
// `docker compose up -d` keeps it.
//
// baseFiles is that existing project, in merge order, and is used only when
// there is no COMPOSE_FILE entry yet: writing one turns off compose's own
// file discovery for every later command, so the line has to name everything
// discovery would have found -- the base file AND the conventional override
// beside it (AutoOverrideCandidates). Naming only the base file would
// silently drop the operator's docker-compose.override.yml from their own
// commands, permanently and invisibly.
func AddComposeFile(env string, baseFiles []string, overrideFile string) string {
	lines, lineEnding := splitLines(env)
	for i, line := range lines {
		if strings.HasPrefix(line, composeFileKey) {
			files := strings.Split(strings.TrimPrefix(line, composeFileKey), ":")
			for _, f := range files {
				if stripCR(f) == overrideFile {
					return joinLines(lines, lineEnding)
				}
			}
			lines[i] = composeFileKey + strings.Join(append(files, overrideFile), ":")
			return joinLines(lines, lineEnding)
		}
	}
	lines = append(lines, composeFileKey+strings.Join(append(append([]string(nil), baseFiles...), overrideFile), ":"))
	return joinLines(lines, lineEnding)
}

// RemoveComposeFile drops the override from COMPOSE_FILE, removing the whole
// line only if the override was the sole entry. Use RemoveComposeFileLine
// instead when the install created the entry itself: what has to be restored
// then is the absence of the line, not a line naming the base file (see
// AddComposeFile for why a present line is not equivalent to no line).
func RemoveComposeFile(env, overrideFile string) string {
	lines, lineEnding := splitLines(env)
	out := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(line, composeFileKey) {
			var kept []string
			for _, f := range strings.Split(strings.TrimPrefix(line, composeFileKey), ":") {
				cleanedF := stripCR(f)
				if cleanedF != overrideFile && cleanedF != "" {
					kept = append(kept, cleanedF)
				}
			}
			if len(kept) == 0 {
				continue
			}
			line = composeFileKey + strings.Join(kept, ":")
		}
		out = append(out, line)
	}
	return joinLines(out, lineEnding)
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

func stripCR(s string) string {
	return strings.TrimRight(s, "\r")
}

// RemoveComposeFileLine drops the COMPOSE_FILE entry entirely, whatever it
// lists. This is what restores a project that had no COMPOSE_FILE before the
// install wrote one: leaving a line behind would keep compose's own file
// discovery switched off for good, so the conventional override beside the
// base file would stop being loaded by the operator's own commands.
func RemoveComposeFileLine(env string) string {
	lines, lineEnding := splitLines(env)
	out := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(line, composeFileKey) {
			continue
		}
		out = append(out, line)
	}
	return joinLines(out, lineEnding)
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
