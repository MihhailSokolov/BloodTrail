// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file covers two gaps in the watermark protocol's coverage: every
// existing watermark test either drove internal/engine's own primitives
// directly (internal/engine/watermark_integration_test.go's own doc on why --
// an import cycle, since the real observer/driver wiring lives up here) or
// never touched the watermark protocol at all (apply_integration_test.go's
// write-shape suite). Neither proves the wiring itself -- write_observer.go's
// ensureBumped/resolveAbandonedWrite, and driver.go's own call sites -- actually
// reaches BumpWatermark/Apply/AdvanceWatermark correctly when driven through
// the real *Driver, nor that a nested Driver.BatchOperation call issued from
// inside a WriteTransaction delegate (the shape BloodHound's own
// queries/graph.go uses, e.g. calling s.Graph.BatchOperation -- the driver,
// not the transaction -- for a batched node update while already inside a
// WriteTransaction) resolves both of its independently-bumped scopes.
//
// Lives in package bloodtrail (not bloodtrail_test), for the same reason
// apply_integration_test.go and staleness_integration_test.go do: it needs
// Driver's unexported engine field, here specifically for
// engine.ReadWatermark and the exported test-observability wrapper
// engine.WatermarkConverged (watermark.go's own doc on why that one is
// exported at all).
package bloodtrail

import (
	"errors"
	"fmt"
	"testing"

	"github.com/specterops/dawgs/graph"
)

// This file's fixture kinds, named distinctly from every other integration
// test's in this shared, long-lived database (see apply_integration_test.go's
// stalenessNodeKindA-style doc for why that matters).
var (
	watermarkWiringOuterKind = graph.StringKind("WatermarkWiringOuterNode")
	watermarkWiringInnerKind = graph.StringKind("WatermarkWiringInnerNode")
	watermarkWiringRunKind   = graph.StringKind("WatermarkWiringRunNode")
	watermarkWiringTxKind    = graph.StringKind("WatermarkWiringTxNode")
	watermarkWiringTxErrKind = graph.StringKind("WatermarkWiringTxErrNode")
)

// TestNestedBatchOperationInsideWriteTransactionResolvesBothScopes covers
// I3: a WriteTransaction delegate that itself calls the DRIVER's own
// BatchOperation (d.BatchOperation, not anything on tx) -- the shape
// BloodHound's queries/graph.go uses for a batched node update issued while
// already inside a transaction. The two calls build two entirely independent
// WriteScopes (driver.go's WriteTransaction constructs its own fresh
// observingTransaction per invocation; BatchOperation constructs its own
// observingBatch, both carrying engine.NewWriteScope()), so each bumps the
// watermark counter separately -- the outer transaction's own first
// mutating call (CreateNode), and the nested batch's own first (and only)
// mutating call (CreateNode) -- and each resolves separately too: the
// nested BatchOperation call's own Apply fires synchronously before it
// returns to the outer delegate, and the outer WriteTransaction's own Apply
// fires after the delegate returns and the outer transaction commits.
//
// Both writes must be reflected in the replica with no rebuild, and the
// watermark counter must advance by exactly 2 and reach convergence once
// both scopes have resolved.
func TestNestedBatchOperationInsideWriteTransactionResolvesBothScopes(t *testing.T) {
	d, bt, buf, ctx := openApplyDriver(t)

	if err := d.engine.RebuildNow(ctx, "manual_test"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}
	rebuilds := d.engine.RebuildCount()

	counterBefore, err := d.engine.ReadWatermark(ctx)
	if err != nil {
		t.Fatalf("ReadWatermark before: %v", err)
	}

	const innerObjectID = "WATERMARK-WIRING-NESTED-1"

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if _, err := tx.CreateNode(graph.NewProperties().Set("name", "outer"), watermarkWiringOuterKind); err != nil {
			return err
		}

		// Nested: the driver's own BatchOperation, called from inside this
		// WriteTransaction's delegate -- mirroring BloodHound's
		// queries/graph.go, which reaches s.Graph.BatchOperation (the
		// driver) rather than anything on tx for a batched node update.
		return d.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.CreateNode(graph.PrepareNode(
				graph.NewProperties().Set("objectid", innerObjectID).Set("name", "inner"),
				watermarkWiringInnerKind,
			))
		})
	}); err != nil {
		t.Fatalf("WriteTransaction (nested BatchOperation): %v", err)
	}

	counterAfter, err := d.engine.ReadWatermark(ctx)
	if err != nil {
		t.Fatalf("ReadWatermark after: %v", err)
	}
	if delta := counterAfter - counterBefore; delta != 2 {
		t.Fatalf("watermark counter advanced by %d, want exactly 2 (one bump for the outer transaction's own first mutating call, one for the nested batch's independently-bumped scope)", delta)
	}

	if _, converged := d.engine.WatermarkConverged(ctx); !converged {
		t.Fatalf("WatermarkConverged = false after both the outer transaction's and the nested batch's scopes should have resolved, want true")
	}
	if !d.engine.WatermarkTrusted(ctx) {
		t.Fatalf("WatermarkTrusted = false after two ordinary, fully resolved writes through the real wiring, want true")
	}

	requireMarkerDelta(t, buf, builderServedMarker, 1, "the outer transaction's own node is counted, served",
		func() int64 { return nodeCountByKind(t, ctx, bt, watermarkWiringOuterKind) }, 1)

	innerText := fmt.Sprintf(`MATCH (n:WatermarkWiringInnerNode) WHERE n.objectid = '%s' RETURN n.name`, innerObjectID)
	requireMarkerDelta(t, buf, cypherServedMarker, 1, "the nested batch's own node serves too, from the same replica",
		func() string { return cypherStringValue(t, ctx, bt, innerText) }, "inner")

	if got := d.engine.RebuildCount(); got != rebuilds {
		t.Fatalf("RebuildCount = %d, want %d -- both the outer and the nested write must be served without any rebuild", got, rebuilds)
	}
	assertNoFallback(t, buf)
}

// TestDriverWatermarkWiringAdvancesThroughRealPaths covers I4: three real
// driver-level write paths, each asserting that the watermark counter
// (d.engine.ReadWatermark) and convergence (d.engine.WatermarkConverged)
// advance through the REAL write_observer.go/driver.go wiring --
// ensureBumped's eager bump and Apply/resolveAbandonedWrite's resolution -- rather
// than internal/engine's own white-box simulation of that same sequence
// (watermark_integration_test.go's own doc on why it cannot drive the real
// wiring at all).
//
// Each subtest opens its own driver (openApplyDriver), so every counter
// assertion is a delta around that subtest's own call, immune to whatever
// value the shared, database-wide bloodtrail_watermark row already carries
// from earlier tests or earlier runs.
func TestDriverWatermarkWiringAdvancesThroughRealPaths(t *testing.T) {
	t.Run("Run", func(t *testing.T) {
		d, bt, _, ctx := openApplyDriver(t)

		var nodeID graph.ID
		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			n, err := tx.CreateNode(graph.NewProperties().Set("name", "before"), watermarkWiringRunKind)
			if err != nil {
				return err
			}
			nodeID = n.ID
			return nil
		}); err != nil {
			t.Fatalf("fixture setup WriteTransaction: %v", err)
		}

		before, err := d.engine.ReadWatermark(ctx)
		if err != nil {
			t.Fatalf("ReadWatermark before: %v", err)
		}

		// Driver.Run's own top-of-method ensureBumped call (driver.go), then
		// its own Apply once the raw statement lands -- see Run's own doc
		// for why an override is needed for this call to reach the engine
		// at all.
		if err := d.Run(ctx, fmt.Sprintf(`UPDATE node SET properties = properties || '{"name":"after"}'::jsonb WHERE id = %d`, nodeID), nil); err != nil {
			t.Fatalf("Run: %v", err)
		}

		after, err := d.engine.ReadWatermark(ctx)
		if err != nil {
			t.Fatalf("ReadWatermark after: %v", err)
		}
		if delta := after - before; delta != 1 {
			t.Fatalf("watermark counter advanced by %d through Driver.Run, want exactly 1 (ensureBumped's own top-of-method bump)", delta)
		}

		if _, converged := d.engine.WatermarkConverged(ctx); !converged {
			t.Fatalf("WatermarkConverged = false after Run's own scope resolved, want true")
		}
	})

	t.Run("WriteTransaction success", func(t *testing.T) {
		d, bt, _, ctx := openApplyDriver(t)

		before, err := d.engine.ReadWatermark(ctx)
		if err != nil {
			t.Fatalf("ReadWatermark before: %v", err)
		}

		if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			_, err := tx.CreateNode(graph.NewProperties(), watermarkWiringTxKind)
			return err
		}); err != nil {
			t.Fatalf("WriteTransaction: %v", err)
		}

		after, err := d.engine.ReadWatermark(ctx)
		if err != nil {
			t.Fatalf("ReadWatermark after: %v", err)
		}
		if delta := after - before; delta != 1 {
			t.Fatalf("watermark counter advanced by %d through a successful WriteTransaction, want exactly 1", delta)
		}

		if _, converged := d.engine.WatermarkConverged(ctx); !converged {
			t.Fatalf("WatermarkConverged = false after the transaction's own scope resolved via the success path (driver.go's own Apply call), want true")
		}
		if !d.engine.WatermarkTrusted(ctx) {
			t.Fatalf("WatermarkTrusted = false after an ordinary successful write, want true: no watermark failure was ever noted, the counters converged, and the engine is serving")
		}
	})

	// I4's own tx-error branch: a delegate that writes, then returns an
	// error, rolls the transaction back -- but the eager bump already
	// landed before that write was even attempted (BumpWatermark's own
	// ordering doc), so driver.go's WriteTransaction error branch must still
	// resolve it (resolveAbandonedWrite), reaching convergence despite there being
	// no committed effect at all for Apply to replay.
	t.Run("WriteTransaction error branch", func(t *testing.T) {
		d, bt, _, ctx := openApplyDriver(t)

		before, err := d.engine.ReadWatermark(ctx)
		if err != nil {
			t.Fatalf("ReadWatermark before: %v", err)
		}

		wantErr := errors.New("intentional rollback")
		txErr := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties(), watermarkWiringTxErrKind); err != nil {
				return err
			}
			return wantErr
		})
		if !errors.Is(txErr, wantErr) {
			t.Fatalf("WriteTransaction error = %v, want %v", txErr, wantErr)
		}

		after, err := d.engine.ReadWatermark(ctx)
		if err != nil {
			t.Fatalf("ReadWatermark after: %v", err)
		}
		if delta := after - before; delta != 1 {
			t.Fatalf("watermark counter advanced by %d through a rolled-back WriteTransaction, want exactly 1 (the eager bump still resolves via resolveAbandonedWrite despite the rollback)", delta)
		}

		if _, converged := d.engine.WatermarkConverged(ctx); !converged {
			t.Fatalf("WatermarkConverged = false after the rolled-back transaction's bump resolved via resolveAbandonedWrite, want true")
		}
		if !d.engine.WatermarkTrusted(ctx) {
			t.Fatalf("WatermarkTrusted = false after a rolled-back write whose bump succeeded, want true")
		}

		if got := nodeCountByKind(t, ctx, bt, watermarkWiringTxErrKind); got != 0 {
			t.Fatalf("node count = %d after a rolled-back WriteTransaction, want 0 (PostgreSQL rolled the write back)", got)
		}
	})
}
