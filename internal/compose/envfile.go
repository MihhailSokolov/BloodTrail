// SPDX-License-Identifier: Apache-2.0

package compose

import "strings"

const composeFileKey = "COMPOSE_FILE="

// AddComposeFile ensures the .env contents make docker compose load the
// override after the base file, so a plain `docker compose up -d` keeps it.
func AddComposeFile(env, baseFile, overrideFile string) string {
	lines := splitLines(env)
	for i, line := range lines {
		if strings.HasPrefix(line, composeFileKey) {
			files := strings.Split(strings.TrimPrefix(line, composeFileKey), ":")
			for _, f := range files {
				if f == overrideFile {
					return joinLines(lines)
				}
			}
			lines[i] = composeFileKey + strings.Join(append(files, overrideFile), ":")
			return joinLines(lines)
		}
	}
	lines = append(lines, composeFileKey+baseFile+":"+overrideFile)
	return joinLines(lines)
}

// RemoveComposeFile drops the override from COMPOSE_FILE, removing the whole
// line only if the override was the sole entry.
func RemoveComposeFile(env, overrideFile string) string {
	lines := splitLines(env)
	out := lines[:0]
	for _, line := range lines {
		if strings.HasPrefix(line, composeFileKey) {
			var kept []string
			for _, f := range strings.Split(strings.TrimPrefix(line, composeFileKey), ":") {
				if f != overrideFile && f != "" {
					kept = append(kept, f)
				}
			}
			if len(kept) == 0 {
				continue
			}
			line = composeFileKey + strings.Join(kept, ":")
		}
		out = append(out, line)
	}
	return joinLines(out)
}

func splitLines(s string) []string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func joinLines(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}
