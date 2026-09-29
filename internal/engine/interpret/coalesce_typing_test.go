// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"reflect"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildUntypedCoalesceFixture holds, per kind, values an untyped coalesce()
// -- one with only property arguments -- picks and dawgs then casts to the
// type of whatever it is compared with (untypedCoalesce).
//
// Num: 1 a="1", 2 a=1, 3 b="1", 4 a=null b=1, 5 a=2, 6 nothing.
// Float: 7 a="1.5", 8 a=1.5, 9 b="2".
// Bool: 10 a="true", 11 a=true, 12 b=false.
// Dirty: 13 a="abc".
func buildUntypedCoalesceFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const (
		kiNum   snapshot.KindID = 1
		kiFloat snapshot.KindID = 2
		kiBool  snapshot.KindID = 3
		kiDirty snapshot.KindID = 4
	)
	kinds := map[snapshot.KindID]string{kiNum: "Num", kiFloat: "Float", kiBool: "Bool", kiDirty: "Dirty"}
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kiNum}, map[string]any{"a": "1"}},
		{2, []snapshot.KindID{kiNum}, map[string]any{"a": 1}},
		{3, []snapshot.KindID{kiNum}, map[string]any{"b": "1"}},
		{4, []snapshot.KindID{kiNum}, map[string]any{"a": nil, "b": 1}},
		{5, []snapshot.KindID{kiNum}, map[string]any{"a": 2}},
		{6, []snapshot.KindID{kiNum}, map[string]any{}},
		{7, []snapshot.KindID{kiFloat}, map[string]any{"a": "1.5"}},
		{8, []snapshot.KindID{kiFloat}, map[string]any{"a": 1.5}},
		{9, []snapshot.KindID{kiFloat}, map[string]any{"b": "2"}},
		{10, []snapshot.KindID{kiBool}, map[string]any{"a": "true"}},
		{11, []snapshot.KindID{kiBool}, map[string]any{"a": true}},
		{12, []snapshot.KindID{kiBool}, map[string]any{"b": false}},
		{13, []snapshot.KindID{kiDirty}, map[string]any{"a": "abc"}},
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestUntypedCoalesceComparedWithLiteralCastsLikeDawgs: dawgs casts an
// untyped coalesce to the type of the literal it is compared with --
// `coalesce(n.a, n.b) = 1` is `coalesce(p ->> 'a', p ->> 'b')::int8 = 1`.
// The engine kept the coalesced text, so '1' never equalled 1 and every
// numeric or boolean comparison came back FALSE: `= 1` served nothing where
// pg returns four nodes, and `<> 1` served all five where pg returns one.
// Every expected set here is PostgreSQL's own answer.
func TestUntypedCoalesceComparedWithLiteralCastsLikeDawgs(t *testing.T) {
	snap := buildUntypedCoalesceFixture(t)

	for _, tc := range []struct {
		query string
		want  []uint64
	}{
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) = 1 RETURN n`, []uint64{1, 2, 3, 4}},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) <> 1 RETURN n`, []uint64{5}},
		{`MATCH (n:Num) WHERE 1 = coalesce(n.a, n.b) RETURN n`, []uint64{1, 2, 3, 4}},
		{`MATCH (n:Num) WHERE NOT coalesce(n.a, n.b) = 1 RETURN n`, []uint64{5}},
		{`MATCH (n:Num) WHERE coalesce(n.a) = 1 RETURN n`, []uint64{1, 2}},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) = (1) RETURN n`, []uint64{1, 2, 3, 4}},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) = '1' RETURN n`, []uint64{1, 2, 3, 4}},
		{`MATCH (n:Float) WHERE coalesce(n.a, n.b) = 1.5 RETURN n`, []uint64{7, 8}},
		{`MATCH (n:Float) WHERE coalesce(n.a, n.b) = 2.0 RETURN n`, []uint64{9}},
		{`MATCH (n:Bool) WHERE coalesce(n.a, n.b) = true RETURN n`, []uint64{10, 11}},
		{`MATCH (n:Bool) WHERE coalesce(n.a, n.b) = false RETURN n`, []uint64{12}},
		{`MATCH (n:Bool) WHERE coalesce(n.a, n.b) <> true RETURN n`, []uint64{12}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, served := servedIDs(t, snap, tc.query)
			if !served {
				t.Fatalf("declined; this shape is served")
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("served %v, want %v (PostgreSQL's answer)", got, tc.want)
			}
		})
	}
}

// TestUntypedCoalesceCastFailureDeclines: a picked value the cast rejects is
// PostgreSQL's error (`invalid input syntax for type bigint: "abc"`), so the
// engine must decline rather than answer.
func TestUntypedCoalesceCastFailureDeclines(t *testing.T) {
	snap := buildUntypedCoalesceFixture(t)
	for _, query := range []string{
		`MATCH (n:Dirty) WHERE coalesce(n.a, n.b) = 1 RETURN n`,
		`MATCH (n:Dirty) WHERE coalesce(n.a, n.b) = 1.5 RETURN n`,
		`MATCH (n:Dirty) WHERE coalesce(n.a, n.b) = true RETURN n`,
	} {
		if err := execExpectErr(t, snap, query, generousBudget); !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("%s: got %v, want ErrRuntimeCast", query, err)
		}
	}
}

// TestComparisonTypingDeclines pins the comparisons whose SQL typing the
// evaluator does not reproduce, each of which it used to serve:
//
//   - text against a number or boolean -- a toLower()/toUpper() result, or
//     an untyped coalesce kept from taking its context's type by
//     parentheses -- is PostgreSQL's `operator does not exist: text =
//     integer`, where the engine answered FALSE for every row;
//   - an untyped coalesce next to anything but a literal or a text operand
//     takes a type this package does not derive;
//   - an untyped coalesce next to IN becomes the constant `false` (no row
//     matches, and under NOT every row does) or `x = any(<text>)`, an error;
//   - IS [NOT] NULL over anything but a plain property is dropped from the
//     SQL outright, so PostgreSQL returns every row.
//
// The controls are the shapes that stay served, the corpus's among them.
func TestComparisonTypingDeclines(t *testing.T) {
	snap := buildUntypedCoalesceFixture(t)

	for _, tc := range []struct {
		query  string
		served bool
	}{
		{`MATCH (n:Num) WHERE (coalesce(n.a, n.b)) = 1 RETURN n`, false},
		{`MATCH (n:Num) WHERE toLower(n.a) = 1 RETURN n`, false},
		{`MATCH (n:Num) WHERE toUpper(n.a) = true RETURN n`, false},
		{`MATCH (n:Num) WHERE toLower(n.a) = 1.5 RETURN n`, false},
		{`MATCH (n:Num) WHERE 1 <> toLower(n.a) RETURN n`, false},
		{`MATCH (n:Num) WHERE toLower(n.a) = id(n) RETURN n`, false},
		{`MATCH (n:Num) WHERE toLower(n.a) = n.b RETURN n`, false},
		{`MATCH (n:Num) WHERE coalesce(n.a, '') = n.b RETURN n`, false},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) = id(n) RETURN n`, false},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) = coalesce(n.x, 1) RETURN n`, false},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) = -1 RETURN n`, false},

		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) IN [1, 2] RETURN n`, false},
		{`MATCH (n:Num) WHERE NOT coalesce(n.a, n.b) IN [1, 2] RETURN n`, false},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) IN ['1', '2'] RETURN n`, false},
		{`MATCH (n:Num) WHERE (coalesce(n.a, n.b)) IN [1] RETURN n`, false},
		{`MATCH (n:Num) WHERE 1 IN coalesce(n.a, n.b) RETURN n`, false},

		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) IS NULL RETURN n`, false},
		{`MATCH (n:Num) WHERE coalesce(n.a, '') IS NOT NULL RETURN n`, false},
		{`MATCH (n:Num) WHERE id(n) IS NULL RETURN n`, false},
		{`MATCH (n:Num) WHERE (n.a) IS NULL RETURN n`, false},
		{`MATCH (n:Num) WHERE n IS NULL RETURN n`, false},
		{`MATCH (n:Num) WHERE toLower(n.a) IS NOT NULL RETURN n`, false},
		{`MATCH (n:Num) WHERE n.a = '1' AND id(n) IS NULL RETURN n`, false},

		{`MATCH (n:Num) WHERE n.a IS NULL RETURN n`, true},
		{`MATCH (n:Num) WHERE n.a IS NOT NULL RETURN n`, true},
		{`MATCH (n:Num) WHERE coalesce(n.a, n.b) = toLower(n.c) RETURN n`, true},
		{`MATCH (n:Num) WHERE (coalesce(n.a, n.b)) = '1' RETURN n`, true},
		{`MATCH (n:Num) WHERE toUpper(n.a) = toUpper(n.b) RETURN n`, true},
		{`MATCH (n:Num) WHERE toLower(n.a) = 'x' RETURN n`, true},
		{`MATCH (n:Num) WHERE coalesce(n.a, '') = 'x' RETURN n`, true},
		{`MATCH (n:Num) WHERE coalesce(n.a, false) = true RETURN n`, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			if _, ok := planNoFail(t, snap, tc.query); ok != tc.served {
				t.Fatalf("Plan() served = %v, want %v", ok, tc.served)
			}
		})
	}
}
