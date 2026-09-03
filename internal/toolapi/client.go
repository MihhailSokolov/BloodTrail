// SPDX-License-Identifier: Apache-2.0

// Package toolapi talks to BloodHound's tool API (metrics port, default 2112):
// graph driver switching and the Neo4j <-> PostgreSQL migrator.
package toolapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Transport performs one HTTP request without a body and returns status and body.
type Transport interface {
	Do(ctx context.Context, method, url string) (status int, body []byte, err error)
}

// HTTPTransport uses net/http directly (tests, or when the port is published).
type HTTPTransport struct {
	Client *http.Client
}

func (s HTTPTransport) Do(ctx context.Context, method, url string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return 0, nil, err
	}
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

// Client drives the migrator endpoints.
type Client struct {
	BaseURL   string
	Transport Transport
}

const (
	StateIdle      = "idle"
	StateMigrating = "migrating"
	StateCanceling = "canceling"
)

func (c Client) do(ctx context.Context, method, path string) (int, []byte, error) {
	return c.Transport.Do(ctx, method, strings.TrimRight(c.BaseURL, "/")+path)
}

func (c Client) put(ctx context.Context, path string) error {
	status, body, err := c.do(ctx, http.MethodPut, path)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", path, err)
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("PUT %s: HTTP %d: %s", path, status, strings.TrimSpace(string(body)))
	}
	return nil
}

// StartNeoToPG asks BloodHound to migrate the graph from Neo4j to PostgreSQL.
func (c Client) StartNeoToPG(ctx context.Context) error {
	return c.put(ctx, "/pg-migration/neo-to-pg")
}

// SwitchToPG makes PostgreSQL the active graph driver (writes the database_switch row to "pg").
func (c Client) SwitchToPG(ctx context.Context) error {
	return c.put(ctx, "/graph-db/switch/pg")
}

// Status returns the migrator state: idle, migrating or canceling.
func (c Client) Status(ctx context.Context) (string, error) {
	status, body, err := c.do(ctx, http.MethodGet, "/pg-migration/status")
	if err != nil {
		return "", fmt.Errorf("GET /pg-migration/status: %w", err)
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("GET /pg-migration/status: HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	var payload struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", fmt.Errorf("decoding migration status %q: %w", body, err)
	}
	return payload.State, nil
}

// WaitUntilIdle polls Status until it reports idle or the timeout passes.
func (c Client) WaitUntilIdle(ctx context.Context, poll, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		state, err := c.Status(ctx)
		if err != nil {
			return err
		}
		if state == StateIdle {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("migration still %q after %s", state, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

// MigrateNeoToPG runs the whole flow: start, wait for idle, switch driver to pg.
// The migrator reports errors only through the server log, so after it returns
// to idle the caller must verify the graph is populated (the installer counts nodes).
func (c Client) MigrateNeoToPG(ctx context.Context, poll, timeout time.Duration) error {
	if err := c.StartNeoToPG(ctx); err != nil {
		return err
	}
	if err := c.WaitUntilIdle(ctx, poll, timeout); err != nil {
		return err
	}
	return c.SwitchToPG(ctx)
}
