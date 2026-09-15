// SPDX-License-Identifier: Apache-2.0

//go:build integration

package bloodtrail

import "github.com/MihhailSokolov/BloodTrail/internal/engine"

// TestingEngine exposes a Driver's engine to the integration test package
// (integration/), which drives deterministic rebuilds (RebuildNow) and reads
// serving state (Fresh, ApplyCount, RebuildCount) around the public driver
// API it exercises. Compiled only under the integration build tag, so the
// package's ordinary build surface is unchanged; not for production use.
func TestingEngine(d *Driver) *engine.Engine {
	return d.engine
}
