// SPDX-License-Identifier: Apache-2.0

// Package verify checks that a BloodHound deployment is up and running BloodTrail.
package verify

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

// DriverActiveMarker is the log line the driver prints once on a successful open.
const DriverActiveMarker = "BloodTrail driver active"

// apiPollInterval is the delay between GET /api/version retries in WaitForAPI.
const apiPollInterval = 500 * time.Millisecond

// WaitForAPI polls GET /api/version until it returns 200 or timeout passes.
func WaitForAPI(ctx context.Context, client *http.Client, baseURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := strings.TrimRight(baseURL, "/") + "/api/version"
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("API at %s not ready after %s: %w", baseURL, timeout, err)
			}
			return fmt.Errorf("API at %s not ready after %s: HTTP %d", baseURL, timeout, resp.StatusCode)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(apiPollInterval):
		}
	}
}

// LogsContain reports whether the service's logs include marker.
func LogsContain(ctx context.Context, compose dockerx.Compose, service, marker string) (bool, error) {
	logs, err := compose.Logs(ctx, service)
	if err != nil {
		return false, err
	}
	return bytes.Contains(logs, []byte(marker)), nil
}
