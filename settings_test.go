// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"log/slog"
	"testing"
	"time"

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
	if s.Engine != true {
		t.Errorf("Engine default = %v, want true (on by default)", s.Engine)
	}
	if s.EnginePollInterval != defaultEnginePollInterval {
		t.Errorf("EnginePollInterval default = %v, want %v", s.EnginePollInterval, defaultEnginePollInterval)
	}
}

func TestSettingsFromEnvParsesAll(t *testing.T) {
	s, err := SettingsFromEnv(lookupFrom(map[string]string{
		EnvSnapshotDir:        "/var/lib/bloodtrail",
		EnvMemoryLimit:        "4GiB",
		EnvLogLevel:           "debug",
		EnvEngine:             "off",
		EnvEnginePollInterval: "250ms",
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
	if s.Engine != false {
		t.Errorf("Engine = %v, want false", s.Engine)
	}
	if s.EnginePollInterval != 250*time.Millisecond {
		t.Errorf("EnginePollInterval = %v, want 250ms", s.EnginePollInterval)
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

func TestSettingsFromEnvEnginePollIntervalValues(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		s, err := SettingsFromEnv(lookupFrom(map[string]string{EnvEnginePollInterval: "2s"}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.EnginePollInterval != 2*time.Second {
			t.Errorf("EnginePollInterval = %v, want 2s", s.EnginePollInterval)
		}
	})
	t.Run("default when unset", func(t *testing.T) {
		s, err := SettingsFromEnv(lookupFrom(nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.EnginePollInterval != defaultEnginePollInterval {
			t.Errorf("EnginePollInterval = %v, want default %v", s.EnginePollInterval, defaultEnginePollInterval)
		}
	})
	t.Run("malformed", func(t *testing.T) {
		if _, err := SettingsFromEnv(lookupFrom(map[string]string{EnvEnginePollInterval: "soon"})); err == nil {
			t.Fatal("expected an error for a malformed duration")
		}
	})
	t.Run("zero rejected", func(t *testing.T) {
		if _, err := SettingsFromEnv(lookupFrom(map[string]string{EnvEnginePollInterval: "0s"})); err == nil {
			t.Fatal("expected an error for a zero duration")
		}
	})
	t.Run("negative rejected", func(t *testing.T) {
		if _, err := SettingsFromEnv(lookupFrom(map[string]string{EnvEnginePollInterval: "-1s"})); err == nil {
			t.Fatal("expected an error for a negative duration")
		}
	})
}

func TestSettingsFromEnvRejectsMalformed(t *testing.T) {
	cases := map[string]map[string]string{
		"bad size":                 {EnvMemoryLimit: "lots"},
		"bad unit":                 {EnvMemoryLimit: "4 parsecs"},
		"bad level":                {EnvLogLevel: "loud"},
		"bad engine toggle":        {EnvEngine: "maybe"},
		"bad engine poll interval": {EnvEnginePollInterval: "soon"},
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
