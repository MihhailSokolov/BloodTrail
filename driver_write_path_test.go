// SPDX-License-Identifier: Apache-2.0

package bloodtrail

import (
	"context"
	"errors"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/engine"
)

// writePathPanicValueOf runs run and returns what it panicked with, nil if nothing.
func writePathPanicValueOf(run func()) (recovered any) {
	defer func() { recovered = recover() }()
	run()
	return nil
}

// TestSettleWriteTransactionPanicSplitsByWhereThePanicArose pins
// settleWriteTransactionPanic: a panic inside the delegate left nothing
// durable (the embedded driver rolls back), so it settles without an
// Apply; a panic after the delegate returned nil came from the embedded
// driver's own commit, whose outcome is unknown, so it records a fallback
// and Applies.
func TestSettleWriteTransactionPanicSplitsByWhereThePanicArose(t *testing.T) {
	cases := []struct {
		name             string
		noObserver       bool
		delegateReturned bool
		delegateErr      error
		wantApply        bool
	}{
		{name: "the delegate never ran", noObserver: true},
		{name: "the delegate panicked"},
		{name: "the delegate returned an error, then the driver panicked", delegateReturned: true, delegateErr: errors.New("delegate boom")},
		{name: "the delegate succeeded, then the driver panicked", delegateReturned: true, wantApply: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := disabledEngine()
			scope := engine.NewWriteScope()
			scope.Changes().RecordNodeID(7)
			var observer *observingTransaction
			if !tc.noObserver {
				observer = &observingTransaction{Transaction: &fakeTransaction{}, scope: scope, eng: eng}
			}

			applyCountBefore := eng.ApplyCount()
			settleWriteTransactionPanic(context.Background(), eng, observer, tc.delegateReturned, tc.delegateErr)

			applied := eng.ApplyCount() != applyCountBefore
			if applied != tc.wantApply {
				t.Fatalf("applied = %v, want %v", applied, tc.wantApply)
			}
			if hasFallback, _ := scope.Changes().HasFallback(); hasFallback != tc.wantApply {
				t.Fatalf("fallback recorded = %v, want %v", hasFallback, tc.wantApply)
			}
		})
	}
}

// TestDriverWriteTransactionPanicReachesTheCallerUnchanged: settling a
// panicking WriteTransaction must not swallow or alter the panic, and a
// panic from the delegate is a rollback, so nothing is applied.
func TestDriverWriteTransactionPanicReachesTheCallerUnchanged(t *testing.T) {
	eng := disabledEngine()
	d := &Driver{engine: eng, pgOverride: &fakePGBackend{tx: &fakeTransaction{createNodeReturn: &graph.Node{ID: 1}}}}

	recovered := writePathPanicValueOf(func() {
		_ = d.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties()); err != nil {
				return err
			}
			panic("delegate bug")
		})
	})
	if recovered != "delegate bug" {
		t.Fatalf("recovered %v, want the delegate's own panic", recovered)
	}
	if got := eng.ApplyCount(); got != 0 {
		t.Fatalf("ApplyCount = %d, want 0: a panicking delegate's transaction rolled back", got)
	}
}

// TestDriverReadTransactionPanicStillAppliesItsWrites: a write through a
// ReadTransaction is durable before the delegate panics, so it must still
// be applied, and the panic must still reach the caller unchanged.
func TestDriverReadTransactionPanicStillAppliesItsWrites(t *testing.T) {
	eng := disabledEngine()
	d := &Driver{engine: eng, pgOverride: &fakePGBackend{tx: &fakeTransaction{createNodeReturn: &graph.Node{ID: 1}}}}

	recovered := writePathPanicValueOf(func() {
		_ = d.ReadTransaction(context.Background(), func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties()); err != nil {
				return err
			}
			panic("delegate bug")
		})
	})
	if recovered != "delegate bug" {
		t.Fatalf("recovered %v, want the delegate's own panic", recovered)
	}
	if got := eng.ApplyCount(); got != 1 {
		t.Fatalf("ApplyCount = %d, want 1: the write before the panic never reached the engine", got)
	}
}
