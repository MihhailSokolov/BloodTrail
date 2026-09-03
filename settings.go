// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/util/size"
)

// Environment variables the driver reads. BloodHound's configuration struct is
// never modified; everything BloodTrail needs comes from these.
const (
	EnvSnapshotDir = "BLOODTRAIL_SNAPSHOT_DIR"
	EnvMemoryLimit = "BLOODTRAIL_MEMORY_LIMIT"
	EnvLogLevel    = "BLOODTRAIL_LOG_LEVEL"
)

// Settings holds the driver's own configuration.
type Settings struct {
	// SnapshotDir is where replica snapshots will be written. Unused in the
	// delegating driver; parsed and kept so the setting is stable from day one.
	SnapshotDir string
	// MemoryLimit bounds the in-memory replica. Zero means unset.
	MemoryLimit size.Size
	// LogLevel for the driver's own logging.
	LogLevel slog.Level
}

// SettingsFromEnv builds Settings from an environment lookup function
// (normally os.LookupEnv). Missing variables take defaults; malformed values
// are errors so a misconfiguration fails at startup, not later.
func SettingsFromEnv(lookup func(string) (string, bool)) (Settings, error) {
	settings := Settings{LogLevel: slog.LevelInfo}

	if v, ok := lookup(EnvSnapshotDir); ok {
		settings.SnapshotDir = strings.TrimSpace(v)
	}

	if v, ok := lookup(EnvMemoryLimit); ok {
		limit, err := ParseSize(v)
		if err != nil {
			return Settings{}, fmt.Errorf("%s: %w", EnvMemoryLimit, err)
		}
		settings.MemoryLimit = limit
	}

	if v, ok := lookup(EnvLogLevel); ok {
		level, err := parseLogLevel(v)
		if err != nil {
			return Settings{}, fmt.Errorf("%s: %w", EnvLogLevel, err)
		}
		settings.LogLevel = level
	}

	return settings, nil
}

func parseLogLevel(text string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (want debug, info, warn or error)", text)
	}
}

var sizeUnits = map[string]size.Size{
	"":    1,
	"b":   1,
	"kib": size.Kibibyte,
	"mib": size.Mebibyte,
	"gib": size.Gibibyte,
	"tib": size.Tebibyte,
	"kb":  1_000,
	"mb":  1_000_000,
	"gb":  1_000_000_000,
	"tb":  1_000_000_000_000,
}

// ParseSize parses "4GiB", "512 MiB", "1GB" or a plain byte count.
func ParseSize(text string) (size.Size, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0, fmt.Errorf("empty size")
	}

	digits := 0
	for digits < len(text) && text[digits] >= '0' && text[digits] <= '9' {
		digits++
	}
	if digits == 0 {
		return 0, fmt.Errorf("size %q must start with a number", text)
	}

	value, err := strconv.ParseInt(text[:digits], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("size %q: %w", text, err)
	}

	unit := strings.ToLower(strings.TrimSpace(text[digits:]))
	factor, ok := sizeUnits[unit]
	if !ok {
		return 0, fmt.Errorf("size %q has unknown unit %q", text, unit)
	}

	return size.Size(value) * factor, nil
}
