// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"
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
