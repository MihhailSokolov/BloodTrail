// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestLoadNodesTurnsAStagingPanicIntoALoadError: loadNodes stages nodes on
// errgroup goroutines, and errgroup deliberately does not propagate a panic
// in one of them (x/sync v0.22.0, errgroup.Go's own comment) -- so a panic
// there ended the whole BloodHound process, despite rebuildOnce's recover
// (recoverRebuildPanic) and the fallback it leads to.
//
// A nil Builder stands in for any panic in the staging step: it makes the
// consumer goroutine's own builder.AddParsedNode call panic on its first
// row. The load must come back as an error -- which rebuildOnce turns into
// a failed rebuild and a retry -- and this test process must survive to
// assert it.
func TestLoadNodesTurnsAStagingPanicIntoALoadError(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	nodeKind := graph.StringKind("LoadPanicNode")
	if _, err := pgDriver.AssertKinds(ctx, graph.Kinds{nodeKind}); err != nil {
		t.Fatalf("assert kinds: %v", err)
	}
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().Set("name", "load-panic"), nodeKind)
		return err
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	graphModel, ok := pgDriver.DefaultGraph()
	if !ok {
		t.Fatalf("no default graph is set")
	}

	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	err = loadNodes(ctx, tx, graphModel.ID, nil)
	if err == nil {
		t.Fatalf("loadNodes with a panicking staging step returned no error")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("loadNodes error %q does not name the panic it recovered", err)
	}
}
