// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"log/slog"
	"testing"

	"github.com/specterops/dawgs/util/size"
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
}

func TestSettingsFromEnvParsesAll(t *testing.T) {
	s, err := SettingsFromEnv(lookupFrom(map[string]string{
		EnvSnapshotDir: "/var/lib/bloodtrail",
		EnvMemoryLimit: "4GiB",
		EnvLogLevel:    "debug",
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
}

func TestSettingsFromEnvRejectsMalformed(t *testing.T) {
	cases := map[string]map[string]string{
		"bad size":  {EnvMemoryLimit: "lots"},
		"bad unit":  {EnvMemoryLimit: "4 parsecs"},
		"bad level": {EnvLogLevel: "loud"},
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
