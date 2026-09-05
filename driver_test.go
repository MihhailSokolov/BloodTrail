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
