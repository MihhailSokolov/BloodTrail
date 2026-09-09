// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	"github.com/specterops/dawgs/util/size"
)

// Environment variables the driver reads. BloodHound's configuration struct is
// never modified; everything BloodTrail needs comes from these.
const (
	EnvSnapshotDir = "BLOODTRAIL_SNAPSHOT_DIR"
	EnvMemoryLimit = "BLOODTRAIL_MEMORY_LIMIT"
	// EnvLogLevel sets the minimum level BloodTrail's own log lines are
	// guaranteed to be visible at, applied in Open via debugOverrideHandler
	// (driver.go): it can only add visibility on top of whatever already
	// configures slog.Default() (in production, BloodHound's own bhlog
	// package and its independent log-level config), never suppress it. Open
	// only installs that override when this variable is explicitly set
	// (Settings.LogLevelSet) -- leaving it unset is a true no-op, not an
	// implicit "widen to Info". "debug" is what surfaces e.g. "bloodtrail:
	// builder engine served".
	EnvLogLevel = "BLOODTRAIL_LOG_LEVEL"
	// EnvEngine toggles the in-memory path engine on or off. Accepts "on"
	// (default) or "off", as well as true/false/1/0 (case-insensitive).
	// When off, every read is delegated to PostgreSQL exactly as in the
	// pre-engine driver.
	EnvEngine = "BLOODTRAIL_ENGINE"
)

// Settings holds the driver's own configuration.
type Settings struct {
	// SnapshotDir enables the snapshot-file boot/save cycle
	// (BLOODTRAIL_SNAPSHOT_DIR) when non-empty: on boot, the engine tries
	// loading <SnapshotDir>/graph-<id>.btsnap before falling back to a full
	// PostgreSQL rebuild, gated on the file's embedded watermark exactly
	// matching PostgreSQL's own watermark counter at that moment -- a stale
	// or unreadable file is always rejected, never trusted (see
	// internal/engine/boot.go's tryLoadSnapshotFile). On a clean shutdown,
	// Driver.Close writes that same file back (internal/engine/persist.go's
	// SaveSnapshot), best-effort, after the engine itself has stopped.
	// Empty (the default) disables the feature outright: no file is ever
	// read or written.
	SnapshotDir string
	// MemoryLimit bounds the in-memory replica. Zero means unset.
	MemoryLimit size.Size
	// LogLevel for the driver's own logging. Defaults to slog.LevelInfo for
	// display purposes (e.g. printing the effective settings), but that
	// default carries no meaning on its own -- see LogLevelSet, which is
	// what Open actually checks before treating this as an explicit choice.
	LogLevel slog.Level
	// LogLevelSet reports whether EnvLogLevel was actually present (and
	// valid) in the environment, distinguishing "explicitly set to info"
	// from "left unset" -- both of which otherwise leave LogLevel at its
	// zero-adjacent default of slog.LevelInfo. Open (driver.go) only wraps
	// slog.Default()'s handler in debugOverrideHandler when this is true;
	// when false, BLOODTRAIL_LOG_LEVEL must be a complete no-op, even though
	// LogLevel itself still reads as Info.
	LogLevelSet bool
	// Engine gates whether the in-memory path engine ever attempts to serve
	// a query. Defaults to true (on); EnvEngine can turn it off.
	Engine bool
}

// SettingsFromEnv builds Settings from an environment lookup function
// (normally os.LookupEnv). Missing variables take defaults; malformed values
// are errors so a misconfiguration fails at startup, not later.
func SettingsFromEnv(lookup func(string) (string, bool)) (Settings, error) {
	settings := Settings{
		LogLevel: slog.LevelInfo,
		Engine:   true,
	}

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
		settings.LogLevelSet = true
	}

	if v, ok := lookup(EnvEngine); ok {
		enabled, err := parseEngineToggle(v)
		if err != nil {
			return Settings{}, fmt.Errorf("%s: %w", EnvEngine, err)
		}
		settings.Engine = enabled
	}

	return settings, nil
}

// parseEngineToggle parses EnvEngine's value: "on"/"off", or true/false/1/0,
// case-insensitively, with surrounding whitespace trimmed.
func parseEngineToggle(text string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "on", "true", "1":
		return true, nil
	case "off", "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("unknown value %q (want on, off, true, false, 1 or 0)", text)
	}
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

	if value > math.MaxInt64/int64(factor) {
		return 0, fmt.Errorf("size %q is too large", text)
	}

	return size.Size(value) * factor, nil
}
