// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/MihhailSokolov/BloodTrail/internal/compose"
	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
)

// priorEntry is what the .env an install copied into its backup directory
// said about COMPOSE_FILE: the one thing that tells rollback whether the line
// it finds now was the operator's own or the install's, when the manifest
// cannot.
type priorEntry int

const (
	priorUnknown priorEntry = iota // no readable copy of the .env from before the install
	priorAbsent                    // the copy has no COMPOSE_FILE entry, or the project had no .env
	priorPresent                   // the copy has one
)

// priorComposeEntry reads the .env copy in the backup directory the manifest
// names. Anything it cannot read with certainty is priorUnknown: a copy that
// is gone, or whose entry cannot be followed, says nothing.
func priorComposeEntry(m manifest.Manifest) priorEntry {
	if m.BackupDir == "" {
		return priorUnknown
	}
	data, err := os.ReadFile(filepath.Join(m.BackupDir, ".env"))
	if errors.Is(err, os.ErrNotExist) {
		// The install copies the .env when the project has one: none in a
		// backup directory that is there means none to copy.
		if info, statErr := os.Stat(m.BackupDir); statErr == nil && info.IsDir() {
			return priorAbsent
		}
		return priorUnknown
	}
	if err != nil {
		return priorUnknown
	}
	listed, err := compose.ListedComposeFiles(string(data))
	switch {
	case err != nil:
		return priorUnknown
	case listed == nil:
		return priorAbsent
	}
	return priorPresent
}

// restoreLegacyComposeFileEntry is restoreComposeFileEntry for the manifests
// of v0.1.0 to v0.1.2, which say that the install created the COMPOSE_FILE
// entry (EnvComposeFileCreated) but not the list it wrote into it. Rollback
// used to delete the whole line for those, whatever it listed, so a file the
// operator had added to it since silently dropped out of their project; and,
// since v0.1.0 and v0.1.1 looked only for lines that start `COMPOSE_FILE=`,
// an `export COMPOSE_FILE=...` of the operator's was taken for no entry and
// got a second line beside it, which rollback refused as ambiguous and, once
// the operator had kept their own line, deleted. The .env the install backed
// up now says whose the line is:
//
//   - The backup had an entry: it is the operator's, extended (or, for
//     v0.1.0 and v0.1.1, shadowed) by the install, so only the override comes
//     out of it.
//   - The backup had none, or the project had no .env: the line is the
//     install's. It goes whole while it lists just what those versions wrote --
//     the compose file, then the override compose loads beside it (legacyWrote)
//     -- and otherwise only the override comes out, with a note.
//   - There is no backup to read: the same, but the line has to name the
//     override to be taken for the install's; a line without it is not one
//     that can be told from the operator's own.
//
// An entry naming nothing besides the override is one of those installs took
// for empty and wrote their override into: it gets its empty entry back --
// or, when the backup shows there was none, goes.
func restoreLegacyComposeFileEntry(env string, m manifest.Manifest, prior priorEntry) (restored, note string, err error) {
	lines, err := compose.ComposeFileLines(env)
	if err != nil || len(lines) == 0 {
		return env, "", err
	}
	if len(lines) > 1 {
		return dropAppendedComposeFileLine(env, m, lines)
	}
	entry := installedOverrideEntry(m)
	listed := lines[0].Files
	others := withoutOverride(listed, entry)
	switch {
	case strings.Join(others, "") == "" && prior == priorAbsent:
		restored, err = compose.RemoveComposeFileLine(env)
	case strings.Join(others, "") == "":
		if restored, err = compose.RestoreEmptyOverrideEntry(env, entry); err == nil && restored != env {
			note = "put back the empty COMPOSE_FILE entry .env had before the install; docker compose does not read that as unset but fails to load the project, so delete the line to let compose find its files on its own, or list them"
		}
	case prior == priorPresent:
		restored, err = compose.RemoveOverrideEntry(env, entry)
	case legacyWrote(m, others) && (prior == priorAbsent || len(others) < len(listed)):
		restored, err = compose.RemoveComposeFileLine(env)
	default:
		if restored, err = compose.RemoveOverrideEntry(env, entry); err == nil && restored != env {
			note = fmt.Sprintf("COMPOSE_FILE in .env lists more than this install wrote, so only %s came out of it; delete the line if docker compose should find its files on its own again", compose.OverrideFileName)
		}
	}
	return restored, note, err
}

// dropAppendedComposeFileLine takes out of an .env that sets COMPOSE_FILE
// more than once the line an earlier install appended, and leaves the
// operator's own byte for byte. The appended one is the one that lists the
// override -- and only what those versions wrote besides it; when the .env
// does not show which line that is, rollback stops before it changes anything
// and says how to settle it.
func dropAppendedComposeFileLine(env string, m manifest.Manifest, lines []compose.EntryLine) (restored, note string, err error) {
	entry := installedOverrideEntry(m)
	var appended, kept []compose.EntryLine
	var numbers []string
	for _, l := range lines {
		numbers = append(numbers, fmt.Sprint(l.Line))
		if len(withoutOverride(l.Files, entry)) < len(l.Files) {
			appended = append(appended, l)
		} else {
			kept = append(kept, l)
		}
	}
	if len(appended) != 1 || len(kept) != 1 || !legacyWrote(m, withoutOverride(appended[0].Files, entry)) {
		return env, "", fmt.Errorf("COMPOSE_FILE is set on lines %s, and it is not clear which was added by an earlier install: it is the one that lists %s and nothing but the files that install wrote; "+
			"delete that line by hand (the other is the one docker compose should keep) and rerun `bloodtrail rollback`", joinAnd(numbers), compose.OverrideFileName)
	}
	if restored, err = compose.RemoveComposeFileLineAt(env, appended[0].Line); err != nil {
		return env, "", err
	}
	// The operator's line is numbered as it is now, without the one that went:
	// a line above it shifts it up.
	keptNow := kept[0].Line
	if appended[0].Line < keptNow {
		keptNow--
	}
	return restored, fmt.Sprintf("removed the COMPOSE_FILE line an earlier install appended (line %d); the entry now on line %d is in effect again", appended[0].Line, keptNow), nil
}

// joinAnd writes words as "1", "1 and 2" or "1, 2 and 3".
func joinAnd(words []string) string {
	if len(words) < 2 {
		return strings.Join(words, "")
	}
	return strings.Join(words[:len(words)-1], ", ") + " and " + words[len(words)-1]
}

// legacyWrote reports whether others -- what an entry lists besides the
// installer's override -- is what v0.1.0 to v0.1.2 wrote into an entry they
// created: the compose file, as a path relative to the project directory,
// and, when there was one beside it, the override file compose loads on its
// own. Those versions paired the compose file with `<name>.override.<ext>`;
// every spelling compose itself discovers is accepted as well, since which of
// them was there at install time is not recorded.
func legacyWrote(m manifest.Manifest, others []string) bool {
	base, err := filepath.Rel(m.ProjectDir, m.ComposeFile)
	if err != nil || len(others) == 0 || len(others) > 2 || others[0] != base {
		return false
	}
	if len(others) == 1 {
		return true
	}
	dir := filepath.Dir(m.ComposeFile)
	for _, name := range legacyOverrideNames(filepath.Base(m.ComposeFile)) {
		if rel, err := filepath.Rel(m.ProjectDir, filepath.Join(dir, name)); err == nil && others[1] == rel {
			return true
		}
	}
	return false
}

// legacyOverrideNames lists the files that could have been the override an
// install of v0.1.0 to v0.1.2 found beside the compose file called base:
// what compose discovers (compose.DefaultOverrideFileNames) and the
// `<name>.override.<ext>` pairing those versions used.
func legacyOverrideNames(base string) []string {
	names := compose.DefaultOverrideFileNames()
	if ext := path.Ext(base); ext == ".yml" || ext == ".yaml" {
		stem := strings.TrimSuffix(base, ext)
		other := ".yaml"
		if ext == ".yaml" {
			other = ".yml"
		}
		names = append(names, stem+".override"+ext, stem+".override"+other)
	}
	return names
}
