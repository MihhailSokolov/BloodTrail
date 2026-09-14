// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"log/slog"
	"testing"

	"github.com/specterops/dawgs/util/size"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

func lookupFrom(m map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}

func TestSettingsFromEnvDefaults(t *testing.T) {
	s, err := SettingsFromEnv(lookupFrom(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.SnapshotDir != "" || s.MemoryLimit != 0 || s.LogLevel != slog.LevelInfo {
		t.Fatalf("unexpected defaults: %+v", s)
	}
	if s.LogLevelSet {
		t.Error("LogLevelSet = true with BLOODTRAIL_LOG_LEVEL unset, want false")
	}
	if s.Engine != true {
		t.Errorf("Engine default = %v, want true (on by default)", s.Engine)
	}
	if s.CompactEntries != engine.DefaultCompactEntries {
		t.Errorf("CompactEntries default = %d, want %d", s.CompactEntries, engine.DefaultCompactEntries)
	}
	if s.CompactBytes != engine.DefaultCompactBytes {
		t.Errorf("CompactBytes default = %d, want %d", s.CompactBytes, engine.DefaultCompactBytes)
	}
}

func TestSettingsFromEnvParsesAll(t *testing.T) {
	s, err := SettingsFromEnv(lookupFrom(map[string]string{
		EnvSnapshotDir:    "/var/lib/bloodtrail",
		EnvMemoryLimit:    "4GiB",
		EnvLogLevel:       "debug",
		EnvEngine:         "off",
		EnvCompactEntries: "42",
		EnvCompactBytes:   "8MiB",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.SnapshotDir != "/var/lib/bloodtrail" {
		t.Errorf("SnapshotDir = %q", s.SnapshotDir)
	}
	if s.MemoryLimit != 4*size.Gibibyte {
		t.Errorf("MemoryLimit = %d", s.MemoryLimit)
	}
	if s.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v", s.LogLevel)
	}
	if !s.LogLevelSet {
		t.Error("LogLevelSet = false with BLOODTRAIL_LOG_LEVEL=debug, want true")
	}
	if s.Engine != false {
		t.Errorf("Engine = %v, want false", s.Engine)
	}
	if s.CompactEntries != 42 {
		t.Errorf("CompactEntries = %d, want 42", s.CompactEntries)
	}
	if s.CompactBytes != 8*size.Mebibyte {
		t.Errorf("CompactBytes = %d, want %d", s.CompactBytes, 8*size.Mebibyte)
	}
}

// TestSettingsFromEnvCompactEntriesZeroMeansUnbounded pins the one
// zero-adjacent behavior worth a dedicated test: "0" is a valid,
// successfully-parsed entry count (unlike a negative one, rejected by
// TestSettingsFromEnvRejectsMalformed below), and it means "no bound on
// this dimension" -- the same convention EnvMemoryLimit already uses --
// not "compact on every write". See EnvCompactEntries' own doc.
func TestSettingsFromEnvCompactEntriesZeroMeansUnbounded(t *testing.T) {
	s, err := SettingsFromEnv(lookupFrom(map[string]string{EnvCompactEntries: "0"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.CompactEntries != 0 {
		t.Errorf("CompactEntries = %d, want 0", s.CompactEntries)
	}
}

// TestSettingsFromEnvLogLevelSetTracksExplicitness exercises the case most
// likely to regress silently: BLOODTRAIL_LOG_LEVEL=info parses to the exact
// same LogLevel as leaving it unset (both are slog.LevelInfo), so
// LogLevelSet -- not LogLevel -- is the only field Open (driver.go) can rely
// on to tell "explicitly asked for Info" apart from "never set it".
func TestSettingsFromEnvLogLevelSetTracksExplicitness(t *testing.T) {
	s, err := SettingsFromEnv(lookupFrom(map[string]string{EnvLogLevel: "info"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want Info", s.LogLevel)
	}
	if !s.LogLevelSet {
		t.Error("LogLevelSet = false with BLOODTRAIL_LOG_LEVEL=info explicitly set, want true")
	}
}

func TestSettingsFromEnvEngineToggleValues(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"on", true},
		{"On", true},
		{" ON ", true},
		{"true", true},
		{"TRUE", true},
		{"1", true},
		{"off", false},
		{"Off", false},
		{"false", false},
		{"FALSE", false},
		{"0", false},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			s, err := SettingsFromEnv(lookupFrom(map[string]string{EnvEngine: c.in}))
			if err != nil {
				t.Fatalf("SettingsFromEnv(%s=%q): unexpected error: %v", EnvEngine, c.in, err)
			}
			if s.Engine != c.want {
				t.Errorf("SettingsFromEnv(%s=%q).Engine = %v, want %v", EnvEngine, c.in, s.Engine, c.want)
			}
		})
	}
}

func TestSettingsFromEnvRejectsMalformed(t *testing.T) {
	cases := map[string]map[string]string{
		"bad size":                 {EnvMemoryLimit: "lots"},
		"bad unit":                 {EnvMemoryLimit: "4 parsecs"},
		"bad level":                {EnvLogLevel: "loud"},
		"bad engine toggle":        {EnvEngine: "maybe"},
		"bad compact entries":      {EnvCompactEntries: "lots"},
		"negative compact entries": {EnvCompactEntries: "-1"},
		"bad compact bytes":        {EnvCompactBytes: "4 parsecs"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := SettingsFromEnv(lookupFrom(env)); err == nil {
				t.Fatalf("expected an error for %v", env)
			}
		})
	}
}

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want size.Size
	}{
		{"0", 0},
		{"1024", 1024},
		{"512MiB", 512 * size.Mebibyte},
		{"2 GiB", 2 * size.Gibibyte},
		{"1gib", size.Gibibyte},
		{"1GB", 1_000_000_000},
		{"3KB", 3000},
	}
	for _, c := range cases {
		got, err := ParseSize(c.in)
		if err != nil {
			t.Errorf("ParseSize(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "-1", "1.5GiB", "GiB", "12XB", "9000000000000TiB"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("ParseSize(%q) expected error", bad)
		}
	}
}

// TestEmptyLogLevelIsAbsent pins that a declared-but-unset
// BLOODTRAIL_LOG_LEVEL leaves LogLevelSet false. The environment hands such
// a variable over as ("", true), and treating it as an explicit "info" makes
// Open install the debug override handler, which forces every BloodTrail
// Info line back on in a deployment that had turned its own logging down to
// Warn or Error -- the exact widening LogLevelSet exists to prevent.
func TestEmptyLogLevelIsAbsent(t *testing.T) {
	for _, value := range []string{"", "   ", "\t"} {
		settings, err := SettingsFromEnv(func(key string) (string, bool) {
			if key == EnvLogLevel {
				return value, true
			}
			return "", false
		})
		if err != nil {
			t.Fatalf("%q: %v", value, err)
		}
		if settings.LogLevelSet {
			t.Errorf("%q: LogLevelSet = true, want false", value)
		}
	}

	settings, err := SettingsFromEnv(func(key string) (string, bool) {
		if key == EnvLogLevel {
			return "warn", true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	if !settings.LogLevelSet || settings.LogLevel != slog.LevelWarn {
		t.Errorf("an explicit level must still be honored: set=%v level=%v", settings.LogLevelSet, settings.LogLevel)
	}
}
