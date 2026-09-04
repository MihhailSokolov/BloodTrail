// SPDX-License-Identifier: Apache-2.0

package compose

import "strings"

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
// override after the base file, so a plain `docker compose up -d` keeps it.
func AddComposeFile(env, baseFile, overrideFile string) string {
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
	lines = append(lines, composeFileKey+baseFile+":"+overrideFile)
	return joinLines(lines, lineEnding)
}

// RemoveComposeFile drops the override from COMPOSE_FILE, removing the whole
// line only if the override was the sole entry.
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
