// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"testing"
)

// TestHydrateEdgePropsByIDEmptyNoQuery checks that an empty ids slice
// short-circuits before ever touching the pool: passing a nil *pgxpool.Pool
// here would panic on any real query, so a nil error together with a
// non-nil, empty map proves no query was attempted.
//
// This lives outside the integration build tag deliberately:
// hydrateEdgePropsByID has no production caller yet -- wiring it into the
// served pipeline comes later -- so without a reference from a
// plain-build test, golangci-lint's `unused` check (which the CI workflow
// runs without -tags integration) flags hydrateEdgePropsByID,
// hydrateEdgePropsByIDBatched, hydrateEdgePropsBatch, and dedupeUint64s as
// dead code. This mirrors the identical fix for edgePropsBatchSize
// (serve_cypher_test.go's TestCypherBudgetConstants).
func TestHydrateEdgePropsByIDEmptyNoQuery(t *testing.T) {
	got, err := hydrateEdgePropsByID(context.Background(), nil, 1, nil)
	if err != nil {
		t.Fatalf("hydrateEdgePropsByID(nil pool, empty ids): %v", err)
	}
	if got == nil {
		t.Fatalf("hydrateEdgePropsByID(nil pool, empty ids) = nil map, want a non-nil empty map")
	}
	if len(got) != 0 {
		t.Fatalf("hydrateEdgePropsByID(nil pool, empty ids) = %d entries, want 0", len(got))
	}
}

// TestDedupeUint64s pins dedupeUint64s' first-seen-order dedup contract that
// hydrateEdgePropsByIDBatched relies on for its deterministic missing-id
// error (the first vanished id reported is the first one named in the
// caller's own input order, once duplicates are collapsed).
func TestDedupeUint64s(t *testing.T) {
	if got := dedupeUint64s(nil); got != nil {
		t.Fatalf("dedupeUint64s(nil) = %v, want nil", got)
	}
	if got := dedupeUint64s([]uint64{}); got != nil {
		t.Fatalf("dedupeUint64s(empty) = %v, want nil", got)
	}

	got := dedupeUint64s([]uint64{5, 5, 3, 5, 3, 9})
	want := []uint64{5, 3, 9}
	if len(got) != len(want) {
		t.Fatalf("dedupeUint64s = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupeUint64s = %v, want %v", got, want)
		}
	}
}
