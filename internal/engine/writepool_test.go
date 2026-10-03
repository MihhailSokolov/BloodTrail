// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// offlineMainPool returns a pool that stands in for BloodHound's own: valid
// configuration, a host nothing answers on, and MinConns 0, so it is created
// without a connection ever being made. get only reads its configuration.
func offlineMainPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	cfg, err := pgxpool.ParseConfig("postgres://bloodtrail@127.0.0.1:1/bloodtrail?sslmode=disable")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.MinConns, cfg.MinIdleConns = 0, 0

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestWritePathPoolLogsOnceWhenItFallsBackToTheMainPool pins that a write
// path that cannot get a pool of its own says so. The fallback is correct --
// a bump or read-back still has to run somewhere -- but it silently restores
// the hazard this pool exists to remove: those statements then draw a second
// connection from the pool their own caller may be holding one of, which is
// what made writers wait on each other at saturation. It is logged once, not
// once per statement, while the creation itself keeps being retried.
func TestWritePathPoolLogsOnceWhenItFallsBackToTheMainPool(t *testing.T) {
	main := offlineMainPool(t)

	createErr := errors.New("MaxSize must be >= 1")
	calls := 0
	original := newWritePathPool
	t.Cleanup(func() { newWritePathPool = original })
	newWritePathPool = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		calls++
		return nil, createErr
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var w writePathPool
	if got := w.get(main, log); got != main {
		t.Fatalf("get after a failed creation = %v, want the main pool", got)
	}
	if got := strings.Count(buf.String(), "the write path's own connection pool could not be created"); got != 1 {
		t.Fatalf("the failure was logged %d times, want 1:\n%s", got, buf.String())
	}
	if !strings.Contains(buf.String(), createErr.Error()) {
		t.Fatalf("the log line does not carry the error:\n%s", buf.String())
	}

	if got := w.get(main, log); got != main {
		t.Fatalf("second get after a failed creation = %v, want the main pool", got)
	}
	if got := strings.Count(buf.String(), "the write path's own connection pool could not be created"); got != 1 {
		t.Fatalf("the failure was logged %d times across two calls, want 1:\n%s", got, buf.String())
	}
	if calls != 2 {
		t.Fatalf("creation was attempted %d times, want one per call: a failure may be transient", calls)
	}
}

// TestWritePathPoolGetTolerance covers the two inputs get must survive with
// no pool of its own and nothing to log through: no main pool at all (a unit
// test's engine), and no logger.
func TestWritePathPoolGetTolerance(t *testing.T) {
	var w writePathPool
	if got := w.get(nil, slog.Default()); got != nil {
		t.Fatalf("get(nil main) = %v, want nil", got)
	}

	original := newWritePathPool
	t.Cleanup(func() { newWritePathPool = original })
	newWritePathPool = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, errors.New("boom")
	}

	main := offlineMainPool(t)
	if got := w.get(main, nil); got != main {
		t.Fatalf("get with a nil logger = %v, want the main pool", got)
	}
}
