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

// driverPanicValue is what panickingPGBackend and rawPanicTransaction panic
// with, so a test can assert the caller got that exact value back.
const driverPanicValue = "embedded driver bug"

// panickingPGBackend is a pgBackend that panics with driverPanicValue
// instead of returning: either before it runs the delegate at all (its own
// acquire or BEGIN) or right after the delegate returned (its own COMMIT) --
// the two places a panic can unwind through a driver-level write, and the
// two that differ in whether anything could be durable.
type panickingPGBackend struct {
	tx                 graph.Transaction
	panicAfterDelegate bool
}

func (b *panickingPGBackend) ReadTransaction(context.Context, graph.TransactionDelegate, ...graph.TransactionOption) error {
	panic(driverPanicValue)
}

func (b *panickingPGBackend) WriteTransaction(_ context.Context, txDelegate graph.TransactionDelegate, _ ...graph.TransactionOption) error {
	if !b.panicAfterDelegate {
		panic(driverPanicValue)
	}
	if err := txDelegate(b.tx); err != nil {
		return err
	}
	panic(driverPanicValue)
}

func (b *panickingPGBackend) SetDefaultGraph(context.Context, graph.Graph) error {
	panic(driverPanicValue)
}

func (b *panickingPGBackend) DeleteNodesByKinds(context.Context, graph.Kinds, graph.Kinds) error {
	panic(driverPanicValue)
}

func (b *panickingPGBackend) DeleteRelationshipsByKinds(context.Context, graph.Kinds) error {
	panic(driverPanicValue)
}

// rawPanicTransaction panics in Raw, which is how a panic reaches Run's or
// WipeGraph's own delegate -- before it has run the statement to completion,
// so the embedded driver's deferred Close rolls the transaction back.
type rawPanicTransaction struct {
	graph.Transaction
}

func (rawPanicTransaction) Raw(string, map[string]any) graph.Result {
	panic(driverPanicValue)
}

// TestDriverCapabilityWritePanicsSettleByWhereThePanicArose is the panic
// twin of TestDriverCapabilityWritesSettleByWhereTheErrorArose
// (wrapper_test.go): every driver-level write bumps the watermark counter at
// its top (ensureBumped), so a panic unwinding through one has to settle that
// bump on its way out, or it stays in flight for the life of the process and
// no snapshot file can ever be written again. The split is the same one the
// error paths make: a panic that proves nothing could be durable (the
// statement itself, rolled back) resolves without an Apply, and a panic that
// could have followed a durable write records a fallback and Applies, so the
// replica is rebuilt from what PostgreSQL actually holds. The panic itself is
// never recovered: it reaches the caller unchanged.
func TestDriverCapabilityWritePanicsSettleByWhereThePanicArose(t *testing.T) {
	okTx := func() graph.Transaction { return &fakeTransaction{rawResult: graph.NewErrorResult(nil)} }

	cases := []struct {
		name      string
		backend   *panickingPGBackend
		call      func(ctx context.Context, d *Driver) error
		wantApply bool
	}{
		{"Run: the statement panicked, rolled back", &panickingPGBackend{tx: rawPanicTransaction{}, panicAfterDelegate: true},
			func(ctx context.Context, d *Driver) error { return d.Run(ctx, "MATCH (n) DELETE n", nil) }, false},
		{"Run: never began", &panickingPGBackend{},
			func(ctx context.Context, d *Driver) error { return d.Run(ctx, "MATCH (n) DELETE n", nil) }, false},
		{"Run: panicked after the delegate, in the commit", &panickingPGBackend{tx: okTx(), panicAfterDelegate: true},
			func(ctx context.Context, d *Driver) error { return d.Run(ctx, "MATCH (n) DELETE n", nil) }, true},
		{"WipeGraph: the truncate panicked, rolled back", &panickingPGBackend{tx: rawPanicTransaction{}, panicAfterDelegate: true},
			func(ctx context.Context, d *Driver) error { return d.WipeGraph(ctx, nil) }, false},
		{"WipeGraph: panicked after the delegate, in the commit", &panickingPGBackend{tx: okTx(), panicAfterDelegate: true},
			func(ctx context.Context, d *Driver) error { return d.WipeGraph(ctx, nil) }, true},
		{"SetDefaultGraph: panicked", &panickingPGBackend{},
			func(ctx context.Context, d *Driver) error { return d.SetDefaultGraph(ctx, graph.Graph{Name: "g"}) }, true},
		{"DeleteNodesByKinds: panicked", &panickingPGBackend{},
			func(ctx context.Context, d *Driver) error {
				return d.DeleteNodesByKinds(ctx, graph.Kinds{graph.StringKind("A")}, nil)
			}, true},
		{"DeleteRelationshipsByKinds: panicked", &panickingPGBackend{},
			func(ctx context.Context, d *Driver) error {
				return d.DeleteRelationshipsByKinds(ctx, graph.Kinds{graph.StringKind("A")})
			}, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := disabledEngine()
			d := &Driver{engine: eng, pgOverride: tc.backend}
			before := eng.ApplyCount()

			recovered := panicValueOf(func() { _ = tc.call(context.Background(), d) })
			if recovered != driverPanicValue {
				t.Fatalf("recovered %v, want the embedded driver's own panic %q unchanged", recovered, driverPanicValue)
			}

			if applied := eng.ApplyCount() != before; applied != tc.wantApply {
				t.Fatalf("Apply called = %v, want %v", applied, tc.wantApply)
			}
		})
	}
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

// TestDriverWriteTransactionMidCommitPanicIsNotClassifiedAsARollback is the
// end-to-end half of observingTransaction.Commit's own panic settle
// (write_observer_test.go): a delegate that commits mid-transaction, where
// the inner commit panics. The panic unwinds through the delegate, so
// Driver.WriteTransaction's own settle reads it as "the delegate panicked",
// which means a rollback -- and for that scope it is not one, because pgx's
// Commit can panic after PostgreSQL made the write durable. Settling it as a
// rollback would advance the applied watermark past a committed write the
// replica never saw and let a snapshot file be stamped as matching
// PostgreSQL.
//
// Exactly one Apply: the Commit applies the scope it is settling and
// replaces it, so the outer settle finds a fresh, unbumped scope and has
// nothing of its own to resolve.
func TestDriverWriteTransactionMidCommitPanicIsNotClassifiedAsARollback(t *testing.T) {
	eng := disabledEngine()
	inner := &fakeTransaction{createNodeReturn: &graph.Node{ID: 1}}
	inner.commitHook = func() { panic(driverPanicValue) }
	d := &Driver{engine: eng, pgOverride: &fakePGBackend{tx: inner}}

	recovered := panicValueOf(func() {
		_ = d.WriteTransaction(context.Background(), func(tx graph.Transaction) error {
			if _, err := tx.CreateNode(graph.NewProperties()); err != nil {
				return err
			}
			return tx.Commit()
		})
	})
	if recovered != driverPanicValue {
		t.Fatalf("recovered %v, want the inner commit's own panic %q unchanged", recovered, driverPanicValue)
	}
	if got := eng.ApplyCount(); got != 1 {
		t.Fatalf("ApplyCount = %d after a mid-transaction commit panicked, want 1: the write may be durable", got)
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
