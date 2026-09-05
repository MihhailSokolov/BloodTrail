// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"testing"

	"github.com/specterops/dawgs/graph"
)

// TestDefaultValueMapperDoesNotHandleEngineNativeTypes pins down the exact
// failure mode mapRowIDValue/mapRowKindsValue/mapRowKindValue (rowresult.go)
// exist to work around: dawgs' own graph.NewValueMapper() -- with no
// MapFuncs of our own added, i.e. relying purely on its built-in
// defaultMapValue -- cannot map a graph.ID, graph.Kinds, or graph.Kind raw
// value (exactly what rowResult.Values() produces) into its matching
// pointer target.
//
// The root cause is the same for all three: defaultMapValue is written to
// unmarshal a live driver's raw, not-yet-typed scalar values (a bare
// uint64/int64/string for an id, a []any for a label list, a bare string
// for a kind name) into a caller's typed Go target. A rowResult row instead
// already holds fully-typed engine-native values -- there is no driver round
// trip in between to produce a raw scalar from -- so defaultMapValue's type
// switches (AsNumeric[ID], AsKinds via SliceOf[string], and the bare-string
// check for *Kind) never match rawValue's actual dynamic type here, and the
// mapper silently declines every one.
//
// If a future dawgs upgrade changes this, this test starts failing loudly,
// which is the point: rowResult's own explicit MapFuncs (mapRowIDValue/
// mapRowKindsValue/mapRowKindValue, appended ahead of the same
// defaultMapValue in rowResult.Mapper()) would then be redundant, not
// silently relied upon.
func TestDefaultValueMapperDoesNotHandleEngineNativeTypes(t *testing.T) {
	mapper := graph.NewValueMapper()

	var id graph.ID
	if mapper.Map(graph.ID(42), &id) {
		t.Fatalf("default ValueMapper unexpectedly mapped a graph.ID raw value into *graph.ID (id = %d) -- mapRowIDValue may no longer be needed", id)
	}

	var kinds graph.Kinds
	if mapper.Map(graph.Kinds{graph.StringKind("User")}, &kinds) {
		t.Fatalf("default ValueMapper unexpectedly mapped a graph.Kinds raw value into *graph.Kinds (kinds = %v) -- mapRowKindsValue may no longer be needed", kinds)
	}

	var kind graph.Kind
	if mapper.Map(graph.Kind(graph.StringKind("User")), &kind) {
		t.Fatalf("default ValueMapper unexpectedly mapped a graph.Kind raw value into *graph.Kind (kind = %v) -- mapRowKindValue may no longer be needed", kind)
	}
}

// TestRowResultMapperHandlesEngineNativeTypes is the positive counterpart to
// TestDefaultValueMapperDoesNotHandleEngineNativeTypes: rowResult's own
// Mapper() -- mapRowIDValue/mapRowKindsValue/mapRowKindValue, appended ahead
// of the same defaultMapValue the sibling test shows can't do this alone --
// must actually map all three raw value types graph.ScanNextResult ever
// hands it (see rowResult.Values()' doc for exactly which projections
// produce which types).
func TestRowResultMapperHandlesEngineNativeTypes(t *testing.T) {
	r := &rowResult{}
	mapper := r.Mapper()

	var id graph.ID
	if ok := mapper.Map(graph.ID(42), &id); !ok || id != 42 {
		t.Fatalf("rowResult Mapper(): Map(graph.ID(42), &id) = %v, id = %d, want (true, 42)", ok, id)
	}

	var kinds graph.Kinds
	want := graph.Kinds{graph.StringKind("User"), graph.StringKind("Computer")}
	if ok := mapper.Map(want, &kinds); !ok || len(kinds) != 2 || kinds[0].String() != "User" || kinds[1].String() != "Computer" {
		t.Fatalf("rowResult Mapper(): Map(%v, &kinds) = %v, kinds = %v, want (true, %v)", want, ok, kinds, want)
	}

	var kind graph.Kind
	if ok := mapper.Map(graph.Kind(graph.StringKind("AdminTo")), &kind); !ok || kind == nil || kind.String() != "AdminTo" {
		t.Fatalf("rowResult Mapper(): Map(AdminTo, &kind) = %v, kind = %v, want (true, AdminTo)", ok, kind)
	}
}
