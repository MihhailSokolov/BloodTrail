// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"errors"
	"regexp"
	"testing"
)

// --- Tri three-valued logic -------------------------------------------------

func TestTriNot(t *testing.T) {
	cases := []struct {
		in   Tri
		want Tri
	}{
		{TriFalse, TriTrue},
		{TriTrue, TriFalse},
		{TriNull, TriNull},
	}
	for _, c := range cases {
		t.Run(c.in.String(), func(t *testing.T) {
			if got := c.in.Not(); got != c.want {
				t.Fatalf("Not(%s) = %s, want %s", c.in, got, c.want)
			}
		})
	}
}

// sqlAnd/sqlOr/sqlXor are the SQL three-valued-logic truth tables, spelled
// out independently of the implementation so the table-driven tests below
// are an actual cross-check rather than a restatement of the code.
func sqlAnd(a, b Tri) Tri {
	if a == TriFalse || b == TriFalse {
		return TriFalse
	}
	if a == TriNull || b == TriNull {
		return TriNull
	}
	return TriTrue
}

func sqlOr(a, b Tri) Tri {
	if a == TriTrue || b == TriTrue {
		return TriTrue
	}
	if a == TriNull || b == TriNull {
		return TriNull
	}
	return TriFalse
}

func sqlXor(a, b Tri) Tri {
	if a == TriNull || b == TriNull {
		return TriNull
	}
	if a == b {
		return TriFalse
	}
	return TriTrue
}

func TestTriAndOrXorAllPairs(t *testing.T) {
	vals := []Tri{TriFalse, TriTrue, TriNull}
	for _, a := range vals {
		for _, b := range vals {
			t.Run(a.String()+"_"+b.String(), func(t *testing.T) {
				if got, want := a.And(b), sqlAnd(a, b); got != want {
					t.Errorf("And(%s,%s) = %s, want %s", a, b, got, want)
				}
				if got, want := a.Or(b), sqlOr(a, b); got != want {
					t.Errorf("Or(%s,%s) = %s, want %s", a, b, got, want)
				}
				if got, want := a.Xor(b), sqlXor(a, b); got != want {
					t.Errorf("Xor(%s,%s) = %s, want %s", a, b, got, want)
				}
			})
		}
	}
}

// Explicit named cases, in addition to the exhaustive sweep
// above: these pin the SQL corner cases that are easy to get backwards.
func TestTriNamedCases(t *testing.T) {
	cases := []struct {
		name string
		got  Tri
		want Tri
	}{
		{"false and null is false", TriFalse.And(TriNull), TriFalse},
		{"null and false is false", TriNull.And(TriFalse), TriFalse},
		{"true and null is null", TriTrue.And(TriNull), TriNull},
		{"true or null is true", TriTrue.Or(TriNull), TriTrue},
		{"null or true is true", TriNull.Or(TriTrue), TriTrue},
		{"false or null is null", TriFalse.Or(TriNull), TriNull},
		{"null xor anything is null", TriNull.Xor(TriTrue), TriNull},
		{"true xor false is true", TriTrue.Xor(TriFalse), TriTrue},
		{"true xor true is false", TriTrue.Xor(TriTrue), TriFalse},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("got %s, want %s", c.got, c.want)
			}
		})
	}
}

// --- String equality (jsonb_typeof guard) -----------------------------------

func TestStringEq(t *testing.T) {
	cases := []struct {
		name string
		got  Tri
		want Tri
	}{
		{"string eq guard: bool never equals 'true'", StringEq(true, true, "true"), TriFalse},
		{"string eq guard: number never equals a string literal", StringEq(float64(1234), true, "1234"), TriFalse},
		{"string eq: missing is null", StringEq(nil, false, "x"), TriNull},
		{"string eq: present json null is false, not null", StringEq(nil, true, "x"), TriFalse},
		{"string eq: matching string", StringEq("admin", true, "admin"), TriTrue},
		{"string eq: mismatched string", StringEq("admin", true, "user"), TriFalse},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("got %s, want %s", c.got, c.want)
			}
		})
	}
}

// --- String inequality (two-branch form) ------------------------------------

func TestStringNeq(t *testing.T) {
	cases := []struct {
		name string
		got  Tri
		want Tri
	}{
		{"string neq: number passes", StringNeq(float64(3), true, "3"), TriTrue},
		{"string neq: bool passes", StringNeq(true, true, "true"), TriTrue},
		{"string neq: present json null passes", StringNeq(nil, true, "x"), TriTrue},
		{"string neq: missing is null", StringNeq(nil, false, "x"), TriNull},
		{"string neq: matching string is false", StringNeq("admin", true, "admin"), TriFalse},
		{"string neq: mismatched string is true", StringNeq("admin", true, "user"), TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("got %s, want %s", c.got, c.want)
			}
		})
	}
}

// --- Scalar / property-vs-property equality ---------------------------------

func TestScalarEq(t *testing.T) {
	cases := []struct {
		name string
		got  Tri
		want Tri
	}{
		{"scalar eq: 1234 == 1234.0", ScalarEq(float64(1234), true, float64(1234.0)), TriTrue},
		{"scalar eq: '1234' != 1234", ScalarEq("1234", true, float64(1234)), TriFalse},
		{"scalar eq: bool true == true", ScalarEq(true, true, true), TriTrue},
		{"scalar eq: bool true != false", ScalarEq(true, true, false), TriFalse},
		{"scalar eq: missing is null", ScalarEq(nil, false, float64(1)), TriNull},
		{"scalar eq: string equal", ScalarEq("a", true, "a"), TriTrue},
		{"scalar eq: array vs array equal", ScalarEq([]any{float64(1), float64(2)}, true, []any{float64(1), float64(2)}), TriTrue},
		{"scalar eq: array order matters", ScalarEq([]any{float64(1), float64(2)}, true, []any{float64(2), float64(1)}), TriFalse},
		{"scalar eq: object key order irrelevant", ScalarEq(map[string]any{"a": float64(1), "b": float64(2)}, true, map[string]any{"b": float64(2), "a": float64(1)}), TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("got %s, want %s", c.got, c.want)
			}
		})
	}
}

func TestPropEq(t *testing.T) {
	cases := []struct {
		name string
		got  Tri
		want Tri
	}{
		{"both present equal", PropEq(float64(5), true, float64(5), true), TriTrue},
		{"both present unequal", PropEq(float64(5), true, float64(6), true), TriFalse},
		{"left missing", PropEq(nil, false, float64(6), true), TriNull},
		{"right missing", PropEq(float64(6), true, nil, false), TriNull},
		{"both missing", PropEq(nil, false, nil, false), TriNull},
		{"cross type false", PropEq("5", true, float64(5), true), TriFalse},
		{"structural deep equality on nested objects", PropEq(map[string]any{"x": []any{float64(1), float64(2)}}, true, map[string]any{"x": []any{float64(1), float64(2)}}, true), TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("got %s, want %s", c.got, c.want)
			}
		})
	}
}

// --- String predicates (STARTS WITH / ENDS WITH / CONTAINS / regex) --------

func TestStringPredicatePositive(t *testing.T) {
	cases := []struct {
		name    string
		op      StringOp
		val     any
		ok      bool
		needle  string
		want    Tri
		wantErr error
	}{
		{"starts with true", OpStartsWith, "hello world", true, "hello", TriTrue, nil},
		{"starts with false", OpStartsWith, "hello world", true, "world", TriFalse, nil},
		{"ends with true", OpEndsWith, "hello world", true, "world", TriTrue, nil},
		{"ends with false", OpEndsWith, "hello world", true, "hello", TriFalse, nil},
		{"contains true", OpContains, "hello world", true, "lo wo", TriTrue, nil},
		{"contains false", OpContains, "hello world", true, "xyz", TriFalse, nil},
		{"regex true", OpRegex, "abc123", true, `^[a-z]+[0-9]+$`, TriTrue, nil},
		{"regex false", OpRegex, "abc123", true, `^[0-9]+$`, TriFalse, nil},
		{"positive missing is null", OpContains, nil, false, "x", TriNull, nil},
		{"positive present json null is null", OpStartsWith, nil, true, "x", TriNull, nil},
		{"positive non-string number present is a runtime cast error", OpContains, float64(512), true, "5", TriFalse, ErrRuntimeCast},
		{"positive non-string bool present is a runtime cast error", OpStartsWith, true, true, "t", TriFalse, ErrRuntimeCast},
		{"positive non-string list present is a runtime cast error", OpContains, []any{float64(1)}, true, "1", TriFalse, ErrRuntimeCast},
		{"positive non-string map present is a runtime cast error", OpContains, map[string]any{"a": float64(1)}, true, "1", TriFalse, ErrRuntimeCast},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := StringPredicate(c.op, c.val, c.ok, c.needle, false)
			if c.wantErr != nil {
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("err = %v, want %v", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestStringPredicateNegated(t *testing.T) {
	cases := []struct {
		name    string
		op      StringOp
		val     any
		ok      bool
		needle  string
		want    Tri
		wantErr error
	}{
		{"negated contains: missing passes", OpContains, nil, false, "x", TriTrue, nil},
		{"negated starts with: missing passes", OpStartsWith, nil, false, "x", TriTrue, nil},
		{"negated ends with: missing passes", OpEndsWith, nil, false, "x", TriTrue, nil},
		{"negated regex: missing passes when pattern needs a char", OpRegex, nil, false, "^[0-9]+$", TriTrue, nil},
		{"negated contains: present json null passes (coalesced to empty string)", OpContains, nil, true, "x", TriTrue, nil},
		{"negated contains: real match inverts to false", OpContains, "hello", true, "ell", TriFalse, nil},
		{"negated contains: real non-match inverts to true", OpContains, "hello", true, "xyz", TriTrue, nil},
		{"negated starts with: non-string number present is a runtime cast error", OpContains, float64(512), true, "1", TriFalse, ErrRuntimeCast},
		{"negated starts with: non-string number present non-match is still a runtime cast error", OpContains, float64(512), true, "9", TriFalse, ErrRuntimeCast},
		{"negated regex matching empty string is false", OpRegex, nil, false, "^.*$", TriFalse, nil},
		{"negated non-string bool present is a runtime cast error", OpStartsWith, true, true, "t", TriFalse, ErrRuntimeCast},
		{"negated non-string list present is a runtime cast error", OpContains, []any{float64(1)}, true, "1", TriFalse, ErrRuntimeCast},
		{"negated non-string map present is a runtime cast error", OpContains, map[string]any{"a": float64(1)}, true, "1", TriFalse, ErrRuntimeCast},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := StringPredicate(c.op, c.val, c.ok, c.needle, true)
			if c.wantErr != nil {
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("err = %v, want %v", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

// --- IS NULL / IS NOT NULL ---------------------------------------------------

func TestIsNull(t *testing.T) {
	cases := []struct {
		name string
		got  Tri
		want Tri
	}{
		{"is null: json null == missing", IsNull(nil, true), TriTrue},
		{"is null: missing key", IsNull(nil, false), TriTrue},
		{"is null: present non-null value", IsNull("x", true), TriFalse},
		{"is null: present zero value is not null", IsNull(float64(0), true), TriFalse},
		{"is null: present false is not null", IsNull(false, true), TriFalse},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("got %s, want %s", c.got, c.want)
			}
		})
	}
}

func TestIsNotNull(t *testing.T) {
	cases := []struct {
		name string
		got  Tri
		want Tri
	}{
		{"is not null: json null == missing", IsNotNull(nil, true), TriFalse},
		{"is not null: missing key", IsNotNull(nil, false), TriFalse},
		{"is not null: present value", IsNotNull("x", true), TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.got != c.want {
				t.Fatalf("got %s, want %s", c.got, c.want)
			}
		})
	}
}

// --- IN --------------------------------------------------------------------

func TestIn(t *testing.T) {
	cases := []struct {
		name    string
		val     any
		ok      bool
		list    []any
		want    Tri
		wantErr error
	}{
		{"empty list false even when missing", nil, false, nil, TriFalse, nil},
		{"empty list false even when present", "x", true, []any{}, TriFalse, nil},
		{"lhs list is always false", []any{float64(1), float64(2)}, true, []any{float64(1)}, TriFalse, nil},
		{"missing lhs with nonempty list is null", nil, false, []any{"a", "b"}, TriNull, nil},
		{"present json null lhs extracts to sql null", nil, true, []any{"a", "b"}, TriNull, nil},

		// pg runs (p->>'k') = ANY(text[]) for a text list: the property's ->>
		// text extraction is compared, so a number property renders through
		// its JSON text form and can match a string element.
		{"text list: number property matches its text rendering", float64(512), true, []any{"512"}, TriTrue, nil},
		{"text list: bool property matches its text rendering", true, true, []any{"true", "false"}, TriTrue, nil},
		{"text list: string match", "admin", true, []any{"user", "admin"}, TriTrue, nil},
		{"text list: string no match", "admin", true, []any{"user", "guest"}, TriFalse, nil},
		{"text list: number property no match", float64(513), true, []any{"512"}, TriFalse, nil},

		// numeric lists cast the text extraction: (p->>'k')::int8/float8.
		{"numeric list: match", float64(5), true, []any{float64(4), float64(5), float64(6)}, TriTrue, nil},
		{"numeric list: no match", float64(7), true, []any{float64(4), float64(5), float64(6)}, TriFalse, nil},
		{"numeric list: non-numeric text fails the cast", "abc", true, []any{float64(4), float64(5)}, TriFalse, ErrRuntimeCast},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := In(c.val, c.ok, c.list)
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			}
			if c.wantErr == nil && got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

// --- Compare (cypher_value_compare port, used by ORDER BY) -----------------

func TestCompareTypeRanks(t *testing.T) {
	// object(1) < array(2) < string(3) < boolean(4) < number(5)
	rankOrdered := []any{
		map[string]any{"a": float64(1)},
		[]any{float64(1)},
		"s",
		true,
		float64(1),
	}
	for i := 0; i < len(rankOrdered); i++ {
		for j := i + 1; j < len(rankOrdered); j++ {
			t.Run(typeName(rankOrdered[i])+"_lt_"+typeName(rankOrdered[j]), func(t *testing.T) {
				got, err := Compare(rankOrdered[i], rankOrdered[j])
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got >= 0 {
					t.Fatalf("Compare(%v, %v) = %d, want < 0", rankOrdered[i], rankOrdered[j], got)
				}
				got2, err := Compare(rankOrdered[j], rankOrdered[i])
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got2 <= 0 {
					t.Fatalf("Compare(%v, %v) = %d, want > 0", rankOrdered[j], rankOrdered[i], got2)
				}
			})
		}
	}
}

func typeName(v any) string {
	switch v.(type) {
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case bool:
		return "bool"
	case float64:
		return "number"
	default:
		return "other"
	}
}

func TestCompareNullSortsLast(t *testing.T) {
	values := []any{
		map[string]any{"a": float64(1)},
		[]any{float64(1)},
		"s",
		true,
		float64(1),
	}
	for _, v := range values {
		t.Run(typeName(v), func(t *testing.T) {
			got, err := Compare(v, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got >= 0 {
				t.Fatalf("Compare(%v, nil) = %d, want < 0 (nil sorts last)", v, got)
			}
			got2, err := Compare(nil, v)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got2 <= 0 {
				t.Fatalf("Compare(nil, %v) = %d, want > 0 (nil sorts last)", v, got2)
			}
		})
	}
	if got, err := Compare(nil, nil); err != nil || got != 0 {
		t.Fatalf("Compare(nil, nil) = %d, %v, want 0, nil", got, err)
	}
}

func TestCompareStringVsStringIsCollationDependent(t *testing.T) {
	got, err := Compare("a", "b")
	if !errors.Is(err, ErrCollation) {
		t.Fatalf("err = %v, want ErrCollation", err)
	}
	_ = got
}

func TestCompareEqualStringsShortCircuitBeforeCollation(t *testing.T) {
	// This pins a load-bearing ordering inside Compare: the jsonbEqual
	// short-circuit at the top must run, and return, before the
	// string-vs-string branch that produces ErrCollation is ever reached.
	// Two equal strings are a raw-equality question ("are these the same
	// value"), not an ordering question ("which sorts first") -- pg's own
	// cypher_value_compare checks raw equality first for exactly this
	// reason, and if this package's port ever reordered those checks,
	// Compare("a","a") would incorrectly delegate via ErrCollation instead
	// of answering 0 locally.
	got, err := Compare("a", "a")
	if err != nil {
		t.Fatalf("Compare(\"a\",\"a\") returned err = %v, want nil", err)
	}
	if got != 0 {
		t.Fatalf("Compare(\"a\",\"a\") = %d, want 0", got)
	}
}

func TestCompareNumbers(t *testing.T) {
	cases := []struct {
		name string
		a, b float64
		want int
	}{
		{"equal", 1234, 1234.0, 0},
		{"less", 1, 2, -1},
		{"greater", 2, 1, 1},
		{"negative", -5, 5, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Compare(c.a, c.b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("Compare(%v,%v) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestCompareBooleans(t *testing.T) {
	if got, err := Compare(false, true); err != nil || got != -1 {
		t.Fatalf("Compare(false,true) = %d, %v, want -1, nil", got, err)
	}
	if got, err := Compare(true, false); err != nil || got != 1 {
		t.Fatalf("Compare(true,false) = %d, %v, want 1, nil", got, err)
	}
	if got, err := Compare(true, true); err != nil || got != 0 {
		t.Fatalf("Compare(true,true) = %d, %v, want 0, nil", got, err)
	}
}

func TestCompareArrays(t *testing.T) {
	cases := []struct {
		name string
		a, b []any
		want int
	}{
		{"equal", []any{float64(1), float64(2)}, []any{float64(1), float64(2)}, 0},
		{"elementwise less", []any{float64(1), float64(2)}, []any{float64(1), float64(3)}, -1},
		{"shorter is less when prefix equal", []any{float64(1), float64(2)}, []any{float64(1), float64(2), float64(3)}, -1},
		{"longer is greater when prefix equal", []any{float64(1), float64(2), float64(3)}, []any{float64(1), float64(2)}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Compare(c.a, c.b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("Compare(%v,%v) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestCompareArraysPropagateCollationError(t *testing.T) {
	_, err := Compare([]any{"a"}, []any{"b"})
	if !errors.Is(err, ErrCollation) {
		t.Fatalf("err = %v, want ErrCollation", err)
	}
}

func TestCompareObjects(t *testing.T) {
	cases := []struct {
		name string
		a, b map[string]any
		want int
	}{
		{"equal regardless of key order", map[string]any{"a": float64(1), "b": float64(2)}, map[string]any{"b": float64(2), "a": float64(1)}, 0},
		{"fewer keys is less", map[string]any{"a": float64(1)}, map[string]any{"a": float64(1), "b": float64(2)}, -1},
		{"more keys is greater", map[string]any{"a": float64(1), "b": float64(2)}, map[string]any{"a": float64(1)}, 1},
		{"same key count compares sorted key names", map[string]any{"a": float64(1)}, map[string]any{"b": float64(1)}, -1},
		{"same keys compares values", map[string]any{"a": float64(1)}, map[string]any{"a": float64(2)}, -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := Compare(c.a, c.b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("Compare(%v,%v) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

// --- OrderCompare (scalar <,>,<=,>= predicates) -----------------------------

func TestOrderCompareNumbers(t *testing.T) {
	cases := []struct {
		name string
		a, b float64
		want int
	}{
		{"equal", 5, 5, 0},
		{"less", 1, 2, -1},
		{"greater", 2, 1, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := OrderCompare(c.a, c.b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("OrderCompare(%v,%v) = %d, want %d", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestOrderCompareStringsAreCollationDependent(t *testing.T) {
	_, err := OrderCompare("a", "b")
	if !errors.Is(err, ErrCollation) {
		t.Fatalf("err = %v, want ErrCollation", err)
	}
}

// --- Defensive / edge-case paths, exercised so the documented fallback
// behavior is actually pinned by a test rather than only by comment. ---

func TestTriStringUnknownValue(t *testing.T) {
	if got := Tri(99).String(); got != "invalid" {
		t.Fatalf("Tri(99).String() = %q, want %q", got, "invalid")
	}
}

func TestInMixedTypeListFallsBackToStructuralEquality(t *testing.T) {
	// A mixed-type list literal (e.g. `x IN [1, 'a']`) doesn't correspond to
	// any single pg array type; In falls back to raw structural equality.
	got, err := In(float64(1), true, []any{float64(1), "a"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != TriTrue {
		t.Fatalf("got %s, want %s", got, TriTrue)
	}

	got, err = In(true, true, []any{true, "a"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != TriTrue {
		t.Fatalf("got %s, want %s", got, TriTrue)
	}

	got, err = In("z", true, []any{true, float64(1)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != TriFalse {
		t.Fatalf("got %s, want %s", got, TriFalse)
	}
}

func TestStringPredicateRegexInvalidPatternIsNoMatch(t *testing.T) {
	// A pattern that fails to compile is treated as no-match rather than
	// panicking; Cypher regex literals are expected to be validated before
	// a query reaches interpretation, so this is a defensive fallback only.
	got, err := StringPredicate(OpRegex, "abc", true, "(unclosed", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != TriFalse {
		t.Fatalf("got %s, want %s", got, TriFalse)
	}
	// Negated: coalesce still applies, and the inverted no-match is TriTrue.
	got, err = StringPredicate(OpRegex, "abc", true, "(unclosed", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != TriTrue {
		t.Fatalf("got %s, want %s", got, TriTrue)
	}
}

// TestRegexPredicate covers RegexPredicate, the pre-compiled-matcher variant
// of StringPredicate's OpRegex case (eval.go's regex compilation cache calls
// this instead of StringPredicate, so it can pass an *Env-cached
// *regexp.Regexp instead of a needle string that StringPredicate would
// recompile on every call). It must share StringPredicate's exact
// null/absent/non-string/negation policy -- that's the whole point of
// factoring both through one private core -- so these cases mirror
// TestStringPredicatePositive/TestStringPredicateNegated's regex rows.
func TestRegexPredicate(t *testing.T) {
	digits := regexp.MustCompile(`^[0-9]+$`)

	t.Run("absent negated is TriTrue", func(t *testing.T) {
		// Coalesce-to-empty-string then invert: "" doesn't match ^[0-9]+$, so
		// the negated result is TriTrue -- same as StringPredicate's negated
		// absent-value case.
		got, err := RegexPredicate(digits, nil, false, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != TriTrue {
			t.Fatalf("got %s, want %s", got, TriTrue)
		}
	})

	t.Run("absent positive is TriNull", func(t *testing.T) {
		got, err := RegexPredicate(digits, nil, false, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != TriNull {
			t.Fatalf("got %s, want %s", got, TriNull)
		}
	})

	t.Run("present non-string is ErrRuntimeCast", func(t *testing.T) {
		got, err := RegexPredicate(digits, float64(123), true, false)
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
		if got != TriFalse {
			t.Fatalf("got %s, want %s", got, TriFalse)
		}
	})

	t.Run("present string match", func(t *testing.T) {
		got, err := RegexPredicate(digits, "12345", true, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != TriTrue {
			t.Fatalf("got %s, want %s", got, TriTrue)
		}
	})

	t.Run("present string non-match, negated inverts to TriTrue", func(t *testing.T) {
		got, err := RegexPredicate(digits, "abc", true, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != TriTrue {
			t.Fatalf("got %s, want %s", got, TriTrue)
		}
	})

	t.Run("present string match, negated inverts to TriFalse", func(t *testing.T) {
		got, err := RegexPredicate(digits, "12345", true, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != TriFalse {
			t.Fatalf("got %s, want %s", got, TriFalse)
		}
	})

	t.Run("nil matcher (failed compile) behaves as no-match", func(t *testing.T) {
		got, err := RegexPredicate(nil, "12345", true, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != TriFalse {
			t.Fatalf("got %s, want %s", got, TriFalse)
		}
	})
}

func TestJSONTextExtractionOfCompositeValuesIsDefensive(t *testing.T) {
	// Arrays/objects are never valid ->> operands for the predicates this
	// package evaluates (IN's LHS-is-a-list case is rejected before
	// jsonText is reached), so this pins the defensive fallback rather than
	// a real pg behavior.
	if _, ok := jsonText([]any{float64(1)}); ok {
		t.Fatalf("expected jsonText of a composite value to report not-present")
	}
	if _, ok := jsonText(map[string]any{"a": float64(1)}); ok {
		t.Fatalf("expected jsonText of a composite value to report not-present")
	}
}

func TestOrderCompareMixedTypesAreNotComparable(t *testing.T) {
	cases := []struct {
		name string
		a, b any
	}{
		{"number vs string", float64(1), "1"},
		{"number vs bool", float64(1), true},
		{"bool vs bool", true, false},
		{"number vs nil", float64(1), nil},
		{"nil vs nil", nil, nil},
		{"array vs array", []any{float64(1)}, []any{float64(1)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := OrderCompare(c.a, c.b)
			if err == nil {
				t.Fatalf("expected a non-nil error for a non-orderable pair")
			}
			if errors.Is(err, ErrCollation) {
				t.Fatalf("expected a plain not-comparable error, not ErrCollation, for %v vs %v", c.a, c.b)
			}
		})
	}
}
