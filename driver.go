// SPDX-License-Identifier: Apache-2.0

// Package bloodtrail is a DAWGS graph database driver for BloodHound CE.
//
// In this milestone the driver delegates every operation to the PostgreSQL
// driver it embeds. Later milestones override methods one at a time to serve
// reads from an in-memory replica while PostgreSQL stays the system of record.
package bloodtrail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/specterops/dawgs"
	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

// DriverName is the value BloodHound's graph_driver setting selects.
const DriverName = "bloodtrail"

// Version is stamped by the image build (see build/build-image.sh).
var Version = "dev"

// Driver wraps the PostgreSQL driver. Embedding the concrete type promotes every
// method, including the capability methods BloodHound discovers through
// graph.AsDriver (WipeGraph, DeleteNodesByKinds, DeleteRelationshipsByKinds,
// KindMapper, OptimizeStorage).
type Driver struct {
	*pg.Driver
	settings Settings
}

// Settings returns the driver's parsed configuration.
func (s *Driver) Settings() Settings {
	return s.settings
}

func init() {
	dawgs.Register(DriverName, Open)
}

// Open is the dawgs.DriverConstructor for BloodTrail. It requires the same
// dawgs.Config BloodHound builds for the PostgreSQL driver, pool included.
func Open(ctx context.Context, cfg dawgs.Config) (graph.Database, error) {
	settings, err := SettingsFromEnv(os.LookupEnv)
	if err != nil {
		return nil, fmt.Errorf("bloodtrail: %w", err)
	}

	if cfg.Pool == nil {
		return nil, errors.New("bloodtrail: a PostgreSQL connection pool is required (dawgs.Config.Pool is nil)")
	}

	backend, err := dawgs.Open(ctx, pg.DriverName, cfg)
	if err != nil {
		return nil, fmt.Errorf("bloodtrail: opening PostgreSQL backend: %w", err)
	}

	pgDriver, ok := backend.(*pg.Driver)
	if !ok {
		_ = backend.Close(ctx)
		return nil, fmt.Errorf("bloodtrail: unexpected PostgreSQL driver type %T", backend)
	}

	slog.InfoContext(ctx, "BloodTrail driver active",
		slog.String("version", Version),
		slog.String("mode", "delegate"),
		slog.String("backend", pg.DriverName),
	)

	return &Driver{Driver: pgDriver, settings: settings}, nil
}
