// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"reflect"
	"testing"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// buildPropertyCastFixture holds, per kind, values dawgs' translation casts
// before it compares or computes: `n.p IN [1, 3]` is `(p ->> 'p')::int8 =
// any(...)`, and `n.v + 1` is `(p ->> 'v')::int8 + 1`.
//
// In: 1 p=1, 2 p="1", 3 p=2, 4 nothing, 5 p=1.5 (float list only).
// Obj: 10 p={"k":"a"}, 11 p="a", 12 p="c", 13 p=[1], 14 p=["a"], 15 p=[].
// Arith: 20 v=5, 21 v="5", 22 v=6, 23 nothing.
// One node per dirty value, each its own kind, so a query declines on it
// alone: 30 "1.0", 31 "1e0", 32 1.5, 33 true, 34 [1] (all under int8).
func buildPropertyCastFixture(t *testing.T) *snapshot.View {
	t.Helper()
	const (
		kiIn snapshot.KindID = iota + 1
		kiFloat
		kiObj
		kiArith
		kiDirtyA
		kiDirtyB
		kiDirtyC
		kiDirtyD
		kiDirtyE
	)
	kinds := map[snapshot.KindID]string{
		kiIn: "In", kiFloat: "InFloat", kiObj: "Obj", kiArith: "Arith",
		kiDirtyA: "DirtyA", kiDirtyB: "DirtyB", kiDirtyC: "DirtyC", kiDirtyD: "DirtyD", kiDirtyE: "DirtyE",
	}
	nodes := []execNodeSpec{
		{1, []snapshot.KindID{kiIn}, map[string]any{"p": 1}},
		{2, []snapshot.KindID{kiIn}, map[string]any{"p": "1"}},
		{3, []snapshot.KindID{kiIn}, map[string]any{"p": 2}},
		{4, []snapshot.KindID{kiIn}, map[string]any{}},
		{5, []snapshot.KindID{kiFloat}, map[string]any{"p": 1.5}},
		{6, []snapshot.KindID{kiFloat}, map[string]any{"p": "2"}},
		{10, []snapshot.KindID{kiObj}, map[string]any{"p": map[string]any{"k": "a"}}},
		{11, []snapshot.KindID{kiObj}, map[string]any{"p": "a"}},
		{12, []snapshot.KindID{kiObj}, map[string]any{"p": "c"}},
		{13, []snapshot.KindID{kiObj}, map[string]any{"p": []any{1}}},
		{14, []snapshot.KindID{kiObj}, map[string]any{"p": []any{"a"}}},
		{15, []snapshot.KindID{kiObj}, map[string]any{"p": []any{}}},
		{20, []snapshot.KindID{kiArith}, map[string]any{"v": 5, "a": 2, "b": 3}},
		{21, []snapshot.KindID{kiArith}, map[string]any{"v": "5", "a": 4, "b": 1}},
		{22, []snapshot.KindID{kiArith}, map[string]any{"v": 6, "a": 1, "b": 1}},
		{23, []snapshot.KindID{kiArith}, map[string]any{}},
		{30, []snapshot.KindID{kiDirtyA}, map[string]any{"p": "1.0", "v": 5.5}},
		{31, []snapshot.KindID{kiDirtyB}, map[string]any{"p": "1e0", "v": "5.0"}},
		{32, []snapshot.KindID{kiDirtyC}, map[string]any{"p": 1.5}},
		{33, []snapshot.KindID{kiDirtyD}, map[string]any{"p": true}},
		{34, []snapshot.KindID{kiDirtyE}, map[string]any{"p": []any{1}}},
	}
	return buildExecSnapshot(t, kinds, nodes, nil)
}

// TestPropertyCastsLikeDawgs: a plain property compared against a list
// literal, or computed with, is cast to the other side's SQL type first.
// The engine used ParseFloat for IN (matching '1.0' and '1e0', answering
// FALSE for 1.5), answered NULL for an object and FALSE for a list against
// a string list (so `NOT ... IN` disagreed with pg), and computed a
// property's own float64 where pg casts to int8 -- declining the string '5'
// that pg reads as 5. Every expected set here is PostgreSQL's answer.
func TestPropertyCastsLikeDawgs(t *testing.T) {
	snap := buildPropertyCastFixture(t)

	for _, tc := range []struct {
		query string
		want  []uint64
	}{
		{`MATCH (n:In) WHERE n.p IN [1, 3] RETURN n`, []uint64{1, 2}},
		{`MATCH (n:In) WHERE NOT n.p IN [1, 3] RETURN n`, []uint64{3}},
		{`MATCH (n:InFloat) WHERE n.p IN [1.5, 2.0] RETURN n`, []uint64{5, 6}},
		{`MATCH (n:Obj) WHERE n.p IN ['a', 'b'] RETURN n`, []uint64{11}},
		{`MATCH (n:Obj) WHERE NOT n.p IN ['a', 'b'] RETURN n`, []uint64{10, 12, 13, 14, 15}},
		{`MATCH (n:Arith) WHERE n.v + 1 = 6 RETURN n`, []uint64{20, 21}},
		{`MATCH (n:Arith) WHERE 1 + n.v = 6 RETURN n`, []uint64{20, 21}},
		{`MATCH (n:Arith) WHERE n.v * 2 + 1 = 11 RETURN n`, []uint64{20, 21}},
		{`MATCH (n:Arith) WHERE n.v + 1.5 = 6.5 RETURN n`, []uint64{20, 21}},
		{`MATCH (n:Arith) WHERE 2 * n.v + n.a = 12 RETURN n`, []uint64{20}},
		{`MATCH (n:Arith) WHERE 1 * n.a * n.b = 6 RETURN n`, []uint64{20}},
		{`MATCH (n:Arith) WHERE n.v + 1 IN [6, 7] RETURN n`, []uint64{20, 21, 22}},
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

// TestPropertyCastFailuresDecline: every value the cast rejects is an error
// in PostgreSQL (`invalid input syntax for type bigint`), so the engine must
// decline -- it used to serve these, matching or not. A list or object
// against a string list whose elements could equal its jsonb rendering
// declines too: that rendering is not reproduced here.
func TestPropertyCastFailuresDecline(t *testing.T) {
	snap := buildPropertyCastFixture(t)
	for _, tc := range []struct {
		query string
		want  error
	}{
		{`MATCH (n:DirtyA) WHERE n.p IN [1, 3] RETURN n`, ErrRuntimeCast},
		{`MATCH (n:DirtyB) WHERE n.p IN [1, 3] RETURN n`, ErrRuntimeCast},
		{`MATCH (n:DirtyC) WHERE n.p IN [1, 3] RETURN n`, ErrRuntimeCast},
		{`MATCH (n:DirtyD) WHERE n.p IN [1, 3] RETURN n`, ErrRuntimeCast},
		{`MATCH (n:DirtyE) WHERE n.p IN [1, 3] RETURN n`, ErrRuntimeCast},
		{`MATCH (n:DirtyA) WHERE n.v + 1 = 6 RETURN n`, ErrRuntimeCast},
		{`MATCH (n:DirtyB) WHERE n.v + 1 = 6 RETURN n`, ErrRuntimeCast},
		{`MATCH (n:Obj) WHERE n.p IN ['[1]'] RETURN n`, ErrUnsupported},
		{`MATCH (n:Obj) WHERE n.p IN ['{"k": "a"}'] RETURN n`, ErrUnsupported},
	} {
		if err := execExpectErr(t, snap, tc.query, generousBudget); !errors.Is(err, tc.want) {
			t.Fatalf("%s: got %v, want %v", tc.query, err, tc.want)
		}
	}
}

// TestPropertyTypingDeclines pins the shapes PostgreSQL rejects for any
// data -- an operator between types it does not have -- which the engine
// used to answer: two properties under `- * / %` (`text * text`), a
// parenthesised property in arithmetic (dawgs casts only a plain one, so it
// stays text), a property under a unary sign (dawgs types it boolean), and
// a text operand IN a numeric list or a numeric operand IN a string list.
// The controls stay served.
func TestPropertyTypingDeclines(t *testing.T) {
	snap := buildPropertyCastFixture(t)

	for _, tc := range []struct {
		query  string
		served bool
	}{
		{`MATCH (n:Arith) WHERE n.a * n.b = 6 RETURN n`, false},
		{`MATCH (n:Arith) WHERE n.a - n.b = -1 RETURN n`, false},
		{`MATCH (n:Arith) WHERE n.a / n.b = 2.0 RETURN n`, false},
		{`MATCH (n:Arith) WHERE n.a * n.b * 1 = 6 RETURN n`, false},
		{`MATCH (n:Arith) WHERE (n.v) + 1 = 6 RETURN n`, false},
		{`MATCH (n:Arith) WHERE 1 + (n.v) = 6 RETURN n`, false},
		{`MATCH (n:Arith) WHERE -n.v = -5 RETURN n`, false},
		{`MATCH (n:Arith) WHERE -(n.v) = -5 RETURN n`, false},
		{`MATCH (n:In) WHERE id(n) IN ['1'] RETURN n`, false},
		{`MATCH (n:In) WHERE size(n.l) IN ['2'] RETURN n`, false},
		{`MATCH (n:In) WHERE n.p + 1 IN ['2'] RETURN n`, false},
		{`MATCH (n:In) WHERE toLower(n.p) IN [1] RETURN n`, false},
		{`MATCH (n:In) WHERE (n.p) IN [1] RETURN n`, false},

		{`MATCH (n:Arith) WHERE 1 * n.a * n.b = 6 RETURN n`, true},
		{`MATCH (n:Arith) WHERE -(n.v + 1) = -6 RETURN n`, true},
		{`MATCH (n:Arith) WHERE n.v + 'x' = '5x' RETURN n`, true},
		{`MATCH (n:In) WHERE (n.p) IN ['1'] RETURN n`, true},
		{`MATCH (n:In) WHERE id(n) IN [1] RETURN n`, true},
		{`MATCH (n:In) WHERE toLower(n.p) IN ['a'] RETURN n`, true},
		{`MATCH (n:In) WHERE n.p IN [1, 2] RETURN n`, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			if _, ok := planNoFail(t, snap, tc.query); ok != tc.served {
				t.Fatalf("Plan() served = %v, want %v", ok, tc.served)
			}
		})
	}
}
