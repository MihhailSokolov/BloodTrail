// SPDX-License-Identifier: Apache-2.0
package snapshot

import (
	"encoding/json"
	"reflect"
	"testing"
)

// mustAddNodeJSON stages a node with a JSON property bag via AddNode,
// failing the test immediately on error. It mirrors mustAddNode in
// builder_test.go but also passes propsJSON (as a string, for readable test
// fixtures) through to AddNode's propsJSON parameter.
func mustAddNodeJSON(t *testing.T, b *Builder, databaseID uint64, kinds []KindID, propsJSON string) {
	t.Helper()
	if err := b.AddNode(databaseID, kinds, []byte(propsJSON)); err != nil {
		t.Fatalf("AddNode(%d): %v", databaseID, err)
	}
}

// TestPropStorePropertyBagsAndObjectIDIndex is the brief's fixture: three
// nodes (10, 20, 30) with property bags exercising every JSON value kind,
// checked via Value, NodeMap, and the objectid index.
func TestPropStorePropertyBagsAndObjectIDIndex(t *testing.T) {
	const node0JSON = `{"objectid":"S-1-X","name":"A","enabled":true,"lastlogon":1725500000,"spns":["a/b","c/d"],"weird":null,"nested":{"a":1}}`

	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "User"})
	mustAddNodeJSON(t, b, 10, []KindID{1}, node0JSON)
	mustAddNodeJSON(t, b, 20, []KindID{1}, `{}`)
	mustAddNodeJSON(t, b, 30, []KindID{1}, `{"objectid":"S-1-Y","enabled":false}`)

	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if s.Props == nil {
		t.Fatal("Props is nil after Build, want non-nil")
	}

	// enabled: true on node 0, absent on node 1, false on node 2.
	enabledID, ok := s.Props.IDByName("enabled")
	if !ok {
		t.Fatal(`IDByName("enabled") not found`)
	}
	if v, ok := s.Props.Value(0, enabledID); !ok || v != true {
		t.Fatalf("Value(0, enabled) = (%v, %v), want (true, true)", v, ok)
	}
	if v, ok := s.Props.Value(1, enabledID); ok {
		t.Fatalf("Value(1, enabled) = (%v, %v), want ok=false", v, ok)
	}
	if v, ok := s.Props.Value(2, enabledID); !ok || v != false {
		t.Fatalf("Value(2, enabled) = (%v, %v), want (false, true)", v, ok)
	}

	// lastlogon: number, always decodes as float64.
	lastlogonID, ok := s.Props.IDByName("lastlogon")
	if !ok {
		t.Fatal(`IDByName("lastlogon") not found`)
	}
	if v, ok := s.Props.Value(0, lastlogonID); !ok || v != float64(1725500000) {
		t.Fatalf("Value(0, lastlogon) = (%v, %v), want (1.7255e9, true)", v, ok)
	}
	if _, isFloat := interface{}(mustValue(t, s, 0, lastlogonID)).(float64); !isFloat {
		t.Fatalf("Value(0, lastlogon) is not a float64: %T", mustValue(t, s, 0, lastlogonID))
	}

	// weird: stored JSON null returns (nil, true), distinct from absent.
	weirdID, ok := s.Props.IDByName("weird")
	if !ok {
		t.Fatal(`IDByName("weird") not found`)
	}
	if v, ok := s.Props.Value(0, weirdID); !ok || v != nil {
		t.Fatalf("Value(0, weird) = (%v, %v), want (nil, true)", v, ok)
	}

	// spns: JSON array decodes to []any.
	spnsID, ok := s.Props.IDByName("spns")
	if !ok {
		t.Fatal(`IDByName("spns") not found`)
	}
	wantSpns := []any{"a/b", "c/d"}
	if v, ok := s.Props.Value(0, spnsID); !ok || !reflect.DeepEqual(v, wantSpns) {
		t.Fatalf("Value(0, spns) = (%#v, %v), want (%#v, true)", v, ok, wantSpns)
	}

	// nested: JSON object decodes to map[string]any with float64 numbers.
	nestedID, ok := s.Props.IDByName("nested")
	if !ok {
		t.Fatal(`IDByName("nested") not found`)
	}
	wantNested := map[string]any{"a": 1.0}
	if v, ok := s.Props.Value(0, nestedID); !ok || !reflect.DeepEqual(v, wantNested) {
		t.Fatalf("Value(0, nested) = (%#v, %v), want (%#v, true)", v, ok, wantNested)
	}

	// NodeMap must deep-equal what json.Unmarshal of the original bag
	// produces, including JSON-null keys present with nil values.
	var wantMap map[string]any
	if err := json.Unmarshal([]byte(node0JSON), &wantMap); err != nil {
		t.Fatalf("reference json.Unmarshal: %v", err)
	}
	if got := s.Props.NodeMap(0); !reflect.DeepEqual(got, wantMap) {
		t.Fatalf("NodeMap(0) = %#v, want %#v", got, wantMap)
	}

	// Node 20 ({}) has an empty, non-nil property map.
	if got := s.Props.NodeMap(1); got == nil || len(got) != 0 {
		t.Fatalf("NodeMap(1) = %#v, want non-nil empty map", got)
	}

	// objectid index: exact match over string-valued objectid only.
	if id, ok := s.Props.NodeByObjectID("S-1-Y"); !ok || id != 2 {
		t.Fatalf(`NodeByObjectID("S-1-Y") = (%d, %v), want (2, true)`, id, ok)
	}
	if id, ok := s.Props.NodeByObjectID("S-1-X"); !ok || id != 0 {
		t.Fatalf(`NodeByObjectID("S-1-X") = (%d, %v), want (0, true)`, id, ok)
	}
	if _, ok := s.Props.NodeByObjectID("nope"); ok {
		t.Fatal(`NodeByObjectID("nope") found, want not found`)
	}

	// ApproxBytes grows when nodes/properties are added.
	if s.Props.ApproxBytes() == 0 {
		t.Fatal("ApproxBytes() = 0, want > 0")
	}
}

// mustValue is a small Value wrapper used only to re-check a value's dynamic
// type in TestPropStorePropertyBagsAndObjectIDIndex without repeating the
// (v, ok) unpack.
func mustValue(t *testing.T, s *Snapshot, n NodeID, id PropID) any {
	t.Helper()
	v, ok := s.Props.Value(n, id)
	if !ok {
		t.Fatalf("Value(%d, %d) not found", n, id)
	}
	return v
}

// TestPropStoreApproxBytesGrowsWithEntries checks ApproxBytes grows both
// with more staged nodes and with more properties per node, mirroring the
// KindTable ApproxBytes growth test.
func TestPropStoreApproxBytesGrowsWithEntries(t *testing.T) {
	b1 := NewBuilder(1)
	mustAddNodeJSON(t, b1, 10, []KindID{1}, `{}`)
	s1, err := b1.Build()
	if err != nil {
		t.Fatal(err)
	}

	b2 := NewBuilder(1)
	mustAddNodeJSON(t, b2, 10, []KindID{1}, `{"objectid":"S-1-X","name":"A","enabled":true}`)
	mustAddNodeJSON(t, b2, 20, []KindID{1}, `{"objectid":"S-1-Y","enabled":false}`)
	s2, err := b2.Build()
	if err != nil {
		t.Fatal(err)
	}

	if s1.Props.ApproxBytes() == 0 {
		t.Fatal("ApproxBytes() = 0 for one node with an empty bag, want > 0 (per-node overhead)")
	}
	if s2.Props.ApproxBytes() <= s1.Props.ApproxBytes() {
		t.Fatalf("ApproxBytes() with more nodes/properties (%d) not greater than fewer (%d)", s2.Props.ApproxBytes(), s1.Props.ApproxBytes())
	}
}

// TestAddNodePropsMalformedJSON checks that a JSON decode error during
// AddNode fails loudly and leaves the node unstaged (so a corrected retry
// with the same databaseID succeeds rather than tripping the
// strictly-ascending check).
func TestAddNodePropsMalformedJSON(t *testing.T) {
	b := NewBuilder(1)
	if err := b.AddNode(10, []KindID{1}, []byte(`{not valid json`)); err == nil {
		t.Fatal("AddNode with malformed properties JSON: want error, got nil")
	}
	if err := b.AddNode(10, []KindID{1}, nil); err != nil {
		t.Fatalf("AddNode(10) retry after a failed add: %v", err)
	}
}

// TestPropStoreNodeByObjectIDIgnoresNonStringValues checks that a numeric
// (or otherwise non-string) objectid value is not indexed, per the brief's
// "only indexes nodes whose objectid value is a string" requirement.
func TestPropStoreNodeByObjectIDIgnoresNonStringValues(t *testing.T) {
	b := NewBuilder(1)
	mustAddNodeJSON(t, b, 10, []KindID{1}, `{"objectid":12345}`)
	s, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Props.NodeByObjectID("12345"); ok {
		t.Fatal(`NodeByObjectID("12345") found for a numeric objectid, want not found`)
	}
}
