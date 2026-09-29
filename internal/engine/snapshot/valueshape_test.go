// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"reflect"
	"strings"
	"testing"
)

// deltaShapeView builds a base of three nodes carrying `p` as a string and
// `l` as a list of strings -- so the base half of valueShape clears both --
// then a delta that rewrites node 3's `p` and `l` to the JSON value given.
// Whatever the view then reports for p and l comes from the delta half.
func deltaShapeView(t *testing.T, value string) *View {
	t.Helper()
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "Computer"})
	mustAddNodeJSON(t, b, 1, []KindID{1}, `{"p":"Windows 2000","l":["Windows"]}`)
	mustAddNodeJSON(t, b, 2, []KindID{1}, `{"p":"Linux","l":["Linux"]}`)
	mustAddNodeJSON(t, b, 3, []KindID{1}, `{"p":"Linux","l":["Linux"]}`)
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var sb SegmentBuilder
	sb.AddKind(1, "Computer")
	mustAddNodeState(t, &sb, 3, []KindID{1}, `{"p":`+value+`,"l":`+value+`}`)
	return NewView(s).WithSegment(sb.Build())
}

// TestValueShapeDeltaHalf pins the two valueShape flags as the DELTA reports
// them, for every JSON type a delta can write: a text predicate's index
// answer is complete only when no node carries the property as other than a
// string (or JSON null, which extracts to SQL NULL and matches nothing), an
// element lookup's only when none carries it as other than a list of strings.
// An empty list is a list of strings for the second question -- it has no
// element to miss -- and not a string for the first.
func TestValueShapeDeltaHalf(t *testing.T) {
	for _, tc := range []struct {
		value                  string
		stringsOnly, listsOnly bool
	}{
		{`"Windows XP"`, true, false},
		{`null`, true, true},
		{`7`, false, false},
		{`true`, false, false},
		{`{"k":"Windows"}`, false, false},
		{`[]`, false, true},
		{`["Windows"]`, false, true},
		{`["Windows",1]`, false, false},
		{`[{"k":"Windows"}]`, false, false},
		{`[["Windows"]]`, false, false},
	} {
		t.Run(tc.value, func(t *testing.T) {
			v := deltaShapeView(t, tc.value)
			if got := v.StringValuesOnly("p"); got != tc.stringsOnly {
				t.Errorf("StringValuesOnly(p) = %v, want %v", got, tc.stringsOnly)
			}
			if got := v.StringListValuesOnly("l"); got != tc.listsOnly {
				t.Errorf("StringListValuesOnly(l) = %v, want %v", got, tc.listsOnly)
			}
		})
	}
}

// TestValueShapeDeltaOnlyProperty: a property the base never interned is
// judged by the delta alone.
func TestValueShapeDeltaOnlyProperty(t *testing.T) {
	b := NewBuilder(1)
	b.SetKinds(map[KindID]string{1: "Computer"})
	mustAddNodeJSON(t, b, 1, []KindID{1}, `{"name":"a"}`)
	s, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var sb SegmentBuilder
	sb.AddKind(1, "Computer")
	mustAddNodeState(t, &sb, 1, []KindID{1}, `{"name":"a","q":{"k":1},"r":["x"]}`)
	v := NewView(s).WithSegment(sb.Build())

	if v.StringValuesOnly("q") {
		t.Error("StringValuesOnly(q) = true for a delta-only object value")
	}
	if !v.StringListValuesOnly("r") {
		t.Error("StringListValuesOnly(r) = false for a delta-only list of strings")
	}
	if !v.StringValuesOnly("never") || !v.StringListValuesOnly("never") {
		t.Error("a property no node carries must be trivially string-only")
	}
}

// TestNodesMatchingStringRefusesDeltaNonStrings: the string index answers a
// text predicate only while every value it could be asked about is a string.
// A delta that writes anything else must make it refuse, so the caller scans
// and the per-row evaluation declines the query to PostgreSQL -- whose `->>`
// renders the value as text the pattern may match. The delta side used to
// judge that from its postings rather than its valueShape flag, and an
// object (which has no key there) or an empty list (which has no element)
// slipped through: the delta node simply dropped out of the candidates, and
// the served answer came back short.
func TestNodesMatchingStringRefusesDeltaNonStrings(t *testing.T) {
	windows := func(s string) bool { return strings.Contains(s, "Windows") }

	for _, value := range []string{`{"k":"Windows"}`, `[]`, `[{"k":"Windows"}]`, `["Windows"]`, `7`, `true`} {
		t.Run(value, func(t *testing.T) {
			v := deltaShapeView(t, value)
			if ids, ok := v.NodesMatchingString("p", windows); ok {
				t.Fatalf("answered %v with a delta value of %s; want a refusal", ids, value)
			}
		})
	}

	for _, tc := range []struct {
		value string
		want  []uint64 // database ids
	}{
		{`null`, []uint64{1}},
		{`"Windows 10"`, []uint64{1, 3}},
		{`"Linux"`, []uint64{1}},
	} {
		t.Run(tc.value, func(t *testing.T) {
			v := deltaShapeView(t, tc.value)
			ids, ok := v.NodesMatchingString("p", windows)
			if !ok {
				t.Fatalf("refused a property whose values are all strings or null")
			}
			// A superset on an overlay: re-verify, as every caller does.
			var got []uint64
			for _, id := range ids {
				if val, present := v.PropValueByName(id, "p"); present {
					if s, isStr := val.(string); isStr && windows(s) {
						got = append(got, v.GraphID(id))
					}
				}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("matched %v, want %v", got, tc.want)
			}
		})
	}
}
