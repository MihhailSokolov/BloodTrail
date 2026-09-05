// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/specterops/dawgs"
)

func TestDriverIsRegisteredUnderItsName(t *testing.T) {
	// Opening without a pool must fail with our own error, which proves the
	// registry routed the name to Open and that Open validates its input.
	_, err := dawgs.Open(context.Background(), DriverName, dawgs.Config{})
	if err == nil {
		t.Fatal("expected an error when no PostgreSQL pool is configured")
	}
	if !strings.Contains(err.Error(), "bloodtrail") || !strings.Contains(err.Error(), "pool") {
		t.Fatalf("error should mention bloodtrail and the missing pool, got: %v", err)
	}
}

func TestUnknownDriverNameStillFails(t *testing.T) {
	if _, err := dawgs.Open(context.Background(), "bloodtrail-does-not-exist", dawgs.Config{}); err == nil {
		t.Fatal("expected ErrDriverMissing for an unregistered name")
	}
}

func TestOpenRejectsMalformedSettingsBeforeConnecting(t *testing.T) {
	t.Setenv(EnvMemoryLimit, "many")
	_, err := dawgs.Open(context.Background(), DriverName, dawgs.Config{})
	if err == nil || !strings.Contains(err.Error(), EnvMemoryLimit) {
		t.Fatalf("expected a settings error naming %s, got: %v", EnvMemoryLimit, err)
	}
}

// TestDebugOverrideHandlerIsAdditiveOnly exercises Open's debugOverrideHandler
// (driver.go) directly, without going through dawgs.Open: it must only ever
// widen what the wrapped handler already accepts, never narrow it, so that
// BLOODTRAIL_LOG_LEVEL=debug surfaces BloodTrail's own Debug lines without
// disturbing whatever level already governs slog.Default() elsewhere (in
// production, BloodHound's own independently-configured logging; in the
// engine_serving_integration_test.go/staleness_integration_test.go suites,
// a Debug-level capture handler installed without ever touching
// BLOODTRAIL_LOG_LEVEL).
func TestDebugOverrideHandlerIsAdditiveOnly(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name         string
		innerLevel   slog.Level // the level the wrapped handler itself was built with
		settingsMin  slog.Level // Settings.LogLevel, i.e. BLOODTRAIL_LOG_LEVEL
		queriedLevel slog.Level
		want         bool
	}{
		{"both default to info: debug stays hidden", slog.LevelInfo, slog.LevelInfo, slog.LevelDebug, false},
		{"both default to info: info stays visible", slog.LevelInfo, slog.LevelInfo, slog.LevelInfo, true},
		{"inner already debug, settings unset: unchanged (still visible)", slog.LevelDebug, slog.LevelInfo, slog.LevelDebug, true},
		{"settings debug, inner info: override adds visibility", slog.LevelInfo, slog.LevelDebug, slog.LevelDebug, true},
		{"settings debug, inner warn: override still adds visibility", slog.LevelWarn, slog.LevelDebug, slog.LevelDebug, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: c.innerLevel})
			h := debugOverrideHandler{Handler: inner, level: c.settingsMin}
			if got := h.Enabled(ctx, c.queriedLevel); got != c.want {
				t.Errorf("Enabled(%v) with inner=%v settings=%v = %v, want %v", c.queriedLevel, c.innerLevel, c.settingsMin, got, c.want)
			}
		})
	}
}

// TestDebugOverrideHandlerSurvivesWithAttrsAndWithGroup guards against a
// future cfg.Log.With(...)/WithGroup(...) silently dropping
// BLOODTRAIL_LOG_LEVEL's effect: without overriding WithAttrs/WithGroup,
// the embedded slog.Handler's own implementations would return a plain,
// un-overridden handler for the derived logger.
func TestDebugOverrideHandlerSurvivesWithAttrsAndWithGroup(t *testing.T) {
	ctx := context.Background()
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := debugOverrideHandler{Handler: inner, level: slog.LevelDebug}

	t.Run("WithAttrs", func(t *testing.T) {
		derived := h.WithAttrs([]slog.Attr{slog.String("k", "v")})
		if _, ok := derived.(debugOverrideHandler); !ok {
			t.Fatalf("WithAttrs returned %T, want debugOverrideHandler", derived)
		}
		if !derived.Enabled(ctx, slog.LevelDebug) {
			t.Error("Debug not enabled after WithAttrs; the override was dropped")
		}
	})

	t.Run("WithGroup", func(t *testing.T) {
		derived := h.WithGroup("g")
		if _, ok := derived.(debugOverrideHandler); !ok {
			t.Fatalf("WithGroup returned %T, want debugOverrideHandler", derived)
		}
		if !derived.Enabled(ctx, slog.LevelDebug) {
			t.Error("Debug not enabled after WithGroup; the override was dropped")
		}
	})
}

// TestBuildLoggerOnlyWidensWhenExplicitlySet exercises buildLogger
// (driver.go), which Open calls to decide whether to install
// debugOverrideHandler at all. Settings.LogLevel defaults to slog.LevelInfo
// whether or not BLOODTRAIL_LOG_LEVEL was ever set, so LogLevelSet is the
// only signal buildLogger can use to tell "explicitly asked to widen"
// apart from "never configured" -- getting this wrong would mean a
// BloodHound deployment that turned its own logging down to Warn or Error
// still gets BloodTrail's Info lines on every served query, purely because
// Settings' zero-adjacent default happens to be Info.
func TestBuildLoggerOnlyWidensWhenExplicitlySet(t *testing.T) {
	ctx := context.Background()

	t.Run("unset BLOODTRAIL_LOG_LEVEL never widens an ambient warn-level handler", func(t *testing.T) {
		settings, err := SettingsFromEnv(lookupFrom(nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if settings.LogLevelSet {
			t.Fatalf("LogLevelSet = true with %s unset", EnvLogLevel)
		}

		ambient := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})
		logger := buildLogger(settings, ambient)
		if logger.Handler().Enabled(ctx, slog.LevelInfo) {
			t.Error("Info enabled with BLOODTRAIL_LOG_LEVEL unset and an ambient warn-level handler; unset must be a no-op")
		}
		if _, wrapped := logger.Handler().(debugOverrideHandler); wrapped {
			t.Error("handler was wrapped in debugOverrideHandler despite LogLevelSet being false")
		}
	})

	t.Run("explicit info widens an ambient warn-level handler", func(t *testing.T) {
		settings, err := SettingsFromEnv(lookupFrom(map[string]string{EnvLogLevel: "info"}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !settings.LogLevelSet {
			t.Fatalf("LogLevelSet = false with %s=info", EnvLogLevel)
		}

		ambient := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})
		logger := buildLogger(settings, ambient)
		if !logger.Handler().Enabled(ctx, slog.LevelInfo) {
			t.Error("Info not enabled despite explicit BLOODTRAIL_LOG_LEVEL=info widening an ambient warn-level handler")
		}
	})

	t.Run("explicit debug widens an ambient info-level handler", func(t *testing.T) {
		settings, err := SettingsFromEnv(lookupFrom(map[string]string{EnvLogLevel: "debug"}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ambient := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo})
		logger := buildLogger(settings, ambient)
		if !logger.Handler().Enabled(ctx, slog.LevelDebug) {
			t.Error("Debug not enabled despite explicit BLOODTRAIL_LOG_LEVEL=debug")
		}
	})

	t.Run("explicit warn never narrows an ambient debug-level handler", func(t *testing.T) {
		settings, err := SettingsFromEnv(lookupFrom(map[string]string{EnvLogLevel: "warn"}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		ambient := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
		logger := buildLogger(settings, ambient)
		if !logger.Handler().Enabled(ctx, slog.LevelDebug) {
			t.Error("Debug disabled despite explicit BLOODTRAIL_LOG_LEVEL=warn on top of an ambient debug-level handler; explicit settings must never narrow")
		}
	})
}
