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

// WaitForAPI polls GET /api/version until the server answers with 200 (OK) or
// 401 (Unauthorized) or timeout passes. BloodHound registers /api/version
// behind RequireAuth, so an unauthenticated request against a live server
// normally gets a 401; that still proves the API is up and routing, so it
// counts as ready. Any other outcome — 5xx, other 4xx, or a connection error
// — keeps polling until the deadline.
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
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized {
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

// LogsContain reports whether marker is in what the service's container has
// logged since its current run started. Docker keeps a container's log across
// its restarts, so the whole log also holds the markers of every earlier run:
// a container that booted one driver once and has since been restarted onto
// another would still show the first one's marker there. It fails with
// dockerx.ErrNoRunningContainer when the service has no running container,
// which has no current run to ask about.
func LogsContain(ctx context.Context, compose dockerx.Compose, service, marker string) (bool, error) {
	container, err := compose.RunningContainer(ctx, service)
	if err != nil {
		return false, err
	}
	logs, err := compose.LogsSince(ctx, service, container.StartedAt)
	if err != nil {
		return false, err
	}
	return bytes.Contains(logs, []byte(marker)), nil
}
