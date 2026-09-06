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

// TestPropStoreNodeByObjectIDDuplicateTieBreak checks that when two nodes
// share the same string objectid value, the index resolves to the node with
// the higher NodeID (the later-inserted node), per the strict ascending-id
// contract of AddNode and the single-assignment-wins semantics of the
// objectIndex map during buildPropStore's iteration.
func TestPropStoreNodeByObjectIDDuplicateTieBreak(t *testing.T) {
	b := NewBuilder(1)
	// Both nodes get the same objectid string value
	mustAddNodeJSON(t, b, 10, []KindID{1}, `{"objectid":"shared-id"}`)
	mustAddNodeJSON(t, b, 20, []KindID{1}, `{"objectid":"shared-id"}`)
	s, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	// The later-inserted node (NodeID 1, database ID 20) should win
	id, ok := s.Props.NodeByObjectID("shared-id")
	if !ok {
		t.Fatal(`NodeByObjectID("shared-id") not found, want found`)
	}
	if id != 1 {
		t.Fatalf(`NodeByObjectID("shared-id") = %d, want 1 (the later-inserted node)`, id)
	}
}

// TestParsePropsAddParsedNodeMatchesAddNode checks that staging nodes via
// ParseProps+AddParsedNode -- the split, worker-pool-friendly path
// loadNodes uses to parallelize property-bag parsing -- produces a
// semantically identical Snapshot to staging the same nodes via AddNode
// directly, covering every JSON value kind (string, bool, number, null,
// array, object) plus the objectid index.
//
// This compares NodeMap output (and GraphIDs/objectid-index membership)
// rather than the two Snapshots themselves via reflect.DeepEqual: property
// names are interned into PropIDs in the order parseNodeProps's internal
// map range happens to visit them, which Go deliberately randomizes per
// map, so two separately-built Snapshots holding equivalent property bags
// can legitimately assign the same name different PropIDs. That is a
// pre-existing property of parseNodeProps (shared by AddNode itself, not
// something ParseProps/AddParsedNode introduces), so the right equivalence
// check is "resolves to the same values," not "identical internal
// encoding."
func TestParsePropsAddParsedNodeMatchesAddNode(t *testing.T) {
	const node0JSON = `{"objectid":"S-1-X","name":"A","enabled":true,"lastlogon":1725500000,"spns":["a/b","c/d"],"weird":null,"nested":{"a":1}}`

	nodes := []struct {
		id        uint64
		propsJSON string
	}{
		{10, node0JSON},
		{20, `{}`},
		{30, `{"objectid":"S-1-Y","enabled":false}`},
	}

	bAddNode := NewBuilder(1)
	bAddNode.SetKinds(map[KindID]string{1: "User"})
	for _, n := range nodes {
		mustAddNodeJSON(t, bAddNode, n.id, []KindID{1}, n.propsJSON)
	}
	want, err := bAddNode.Build()
	if err != nil {
		t.Fatalf("Build (AddNode path): %v", err)
	}

	bParsed := NewBuilder(1)
	bParsed.SetKinds(map[KindID]string{1: "User"})
	for _, n := range nodes {
		parsed, err := ParseProps([]byte(n.propsJSON))
		if err != nil {
			t.Fatalf("ParseProps(%d): %v", n.id, err)
		}
		if err := bParsed.AddParsedNode(n.id, []KindID{1}, parsed); err != nil {
			t.Fatalf("AddParsedNode(%d): %v", n.id, err)
		}
	}
	got, err := bParsed.Build()
	if err != nil {
		t.Fatalf("Build (ParseProps/AddParsedNode path): %v", err)
	}

	if !reflect.DeepEqual(want.GraphIDs, got.GraphIDs) {
		t.Fatalf("GraphIDs differ: AddNode=%v AddParsedNode=%v", want.GraphIDs, got.GraphIDs)
	}
	for i := range nodes {
		n := NodeID(i)
		wantMap, gotMap := want.Props.NodeMap(n), got.Props.NodeMap(n)
		if !reflect.DeepEqual(wantMap, gotMap) {
			t.Fatalf("node %d: NodeMap differs: AddNode=%#v AddParsedNode=%#v", n, wantMap, gotMap)
		}
	}
	for _, objectID := range []string{"S-1-X", "S-1-Y"} {
		wantNode, wantOK := want.Props.NodeByObjectID(objectID)
		gotNode, gotOK := got.Props.NodeByObjectID(objectID)
		if wantOK != gotOK || wantNode != gotNode {
			t.Fatalf("NodeByObjectID(%q): AddNode=(%d,%t) AddParsedNode=(%d,%t)", objectID, wantNode, wantOK, gotNode, gotOK)
		}
	}
}

// TestParsePropsMalformedJSON checks that ParseProps surfaces a JSON decode
// error rather than panicking or silently dropping data, mirroring
// TestAddNodePropsMalformedJSON's coverage of AddNode's own parse half.
func TestParsePropsMalformedJSON(t *testing.T) {
	if _, err := ParseProps([]byte(`{not valid json`)); err == nil {
		t.Fatal("ParseProps with malformed properties JSON: want error, got nil")
	}
}

// TestAddParsedNodeOutOfOrder mirrors TestAddNodeOutOfOrder for the
// AddParsedNode path: a non-ascending databaseID is rejected, and a failed
// add leaves the Builder's ascending-id state untouched (a corrected retry
// with the same databaseID succeeds).
func TestAddParsedNodeOutOfOrder(t *testing.T) {
	b := NewBuilder(1)
	empty, err := ParseProps(nil)
	if err != nil {
		t.Fatalf("ParseProps(nil): %v", err)
	}
	if err := b.AddParsedNode(100, []KindID{1}, empty); err != nil {
		t.Fatalf("AddParsedNode(100) unexpected error: %v", err)
	}
	if err := b.AddParsedNode(100, []KindID{1}, empty); err == nil {
		t.Fatal("AddParsedNode(100) again: want error for non-ascending id, got nil")
	}
	if err := b.AddParsedNode(50, []KindID{1}, empty); err == nil {
		t.Fatal("AddParsedNode(50) after 100: want error for non-ascending id, got nil")
	}
	if err := b.AddParsedNode(200, []KindID{1}, empty); err != nil {
		t.Fatalf("AddParsedNode(200) retry after failed adds: %v", err)
	}
}
