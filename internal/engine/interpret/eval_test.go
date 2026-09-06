// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/MihhailSokolov/BloodTrail/internal/engine/snapshot"
)

// --- test helpers: parsing -------------------------------------------------

// whereExprOf parses query (expected to contain exactly one MATCH with a
// WHERE clause) and returns the WHERE clause's single top-level expression,
// per the brief's suggested helper shape.
func whereExprOf(t *testing.T, query string) cypher.Expression {
	t.Helper()

	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		t.Fatalf("ParseCypher(%q): %v", query, err)
	}
	if parsed == nil || parsed.SingleQuery == nil || parsed.SingleQuery.SinglePartQuery == nil {
		t.Fatalf("ParseCypher(%q): not a single-part query", query)
	}
	readingClauses := parsed.SingleQuery.SinglePartQuery.ReadingClauses
	if len(readingClauses) != 1 || readingClauses[0] == nil || readingClauses[0].Match == nil {
		t.Fatalf("ParseCypher(%q): expected exactly one MATCH", query)
	}
	where := readingClauses[0].Match.Where
	if where == nil {
		t.Fatalf("ParseCypher(%q): expected a WHERE clause", query)
	}
	all := where.GetAll()
	if len(all) != 1 {
		t.Fatalf("ParseCypher(%q): WHERE has %d top-level expressions, want 1", query, len(all))
	}
	return all[0]
}

// returnExprOf parses query (expected to RETURN exactly one bare expression)
// and returns that expression, for isolating EvalValue-only cases (function
// calls, arithmetic, literals) without wrapping them in a boolean WHERE
// context.
func returnExprOf(t *testing.T, query string) cypher.Expression {
	t.Helper()

	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
	if err != nil {
		t.Fatalf("ParseCypher(%q): %v", query, err)
	}
	if parsed == nil || parsed.SingleQuery == nil || parsed.SingleQuery.SinglePartQuery == nil {
		t.Fatalf("ParseCypher(%q): not a single-part query", query)
	}
	ret := parsed.SingleQuery.SinglePartQuery.Return
	if ret == nil || ret.Projection == nil || len(ret.Projection.Items) != 1 {
		t.Fatalf("ParseCypher(%q): expected exactly one RETURN item", query)
	}
	item, isItem := ret.Projection.Items[0].(*cypher.ProjectionItem)
	if !isItem || item == nil {
		t.Fatalf("ParseCypher(%q): RETURN item is not a ProjectionItem", query)
	}
	return item.Expression
}

// --- test helpers: snapshot fixture -----------------------------------------

// fixture is a small, hand-built snapshot covering every scenario this
// file's tests need, plus a database-id -> dense-NodeID index so tests can
// bind Rows by the same ids they used to declare each node.
type fixture struct {
	snap  *snapshot.Snapshot
	dense map[uint64]snapshot.NodeID
}

// newFixture builds the shared test snapshot. Kind 1 = "User", kind 2 =
// "Computer", kind 10 = "MemberOf" (an edge kind). See each node's inline
// comment for which test(s) it exists for.
func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := snapshot.NewBuilder(1)
	b.SetKinds(map[snapshot.KindID]string{
		1:  "User",
		2:  "Computer",
		10: "MemberOf",
	})

	type nodeSpec struct {
		id    uint64
		kinds []snapshot.KindID
		props map[string]any
	}
	nodes := []nodeSpec{
		{100, []snapshot.KindID{1}, map[string]any{}},                                             // no "name" at all
		{200, []snapshot.KindID{1}, map[string]any{"name": "AZUREADKERBEROS.TEST.LOCAL"}},         // has "name"
		{300, []snapshot.KindID{1}, map[string]any{}},                                             // no "gmsa"
		{400, []snapshot.KindID{1}, map[string]any{"gmsa": true}},                                 // has "gmsa"
		{500, []snapshot.KindID{2}, map[string]any{"operatingsystem": "Windows Server 2019"}},     // OS contains SERVER (case-folded)
		{550, []snapshot.KindID{2}, map[string]any{"operatingsystem": "ubuntu"}},                  // OS does not
		{600, []snapshot.KindID{1}, map[string]any{"lastlogontimestamp": float64(1_000_000)}},     // ancient logon
		{620, []snapshot.KindID{1}, map[string]any{"lastlogontimestamp": float64(1_999_000_000)}}, // recent logon
		{650, []snapshot.KindID{1}, map[string]any{}},                                             // no logon timestamp
		{700, []snapshot.KindID{1}, map[string]any{"flag": true}},                                 // bool flag
		{750, []snapshot.KindID{1}, map[string]any{"flag": "yes"}},                                // non-bool flag
		{760, []snapshot.KindID{1}, map[string]any{"nullprop": nil}},                              // present JSON null
		{800, []snapshot.KindID{1}, map[string]any{"spns": []any{"a", "b", "c"}}},                 // list property
		{850, []snapshot.KindID{1}, map[string]any{"score": float64(42)}},                         // numeric property
		{900, []snapshot.KindID{1, 2}, map[string]any{}},                                          // User AND Computer
		{950, []snapshot.KindID{1}, map[string]any{}},                                             // User only; edge source
		{960, []snapshot.KindID{1}, map[string]any{"name": "beta"}},                               // paired with 200 for collation test
	}

	for _, n := range nodes {
		propsJSON, err := json.Marshal(n.props)
		if err != nil {
			t.Fatalf("marshal props for node %d: %v", n.id, err)
		}
		if err := b.AddNode(n.id, n.kinds, propsJSON); err != nil {
			t.Fatalf("AddNode(%d): %v", n.id, err)
		}
	}
	b.AddEdge(5000, 950, 900, 10)

	snap, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dense := make(map[uint64]snapshot.NodeID, len(nodes))
	for _, n := range nodes {
		id, ok := snap.Dense(n.id)
		if !ok {
			t.Fatalf("Dense(%d): not found after Build", n.id)
		}
		dense[n.id] = id
	}

	return &fixture{snap: snap, dense: dense}
}

// row builds a single-node-bound Row: sym is bound to the dense NodeID of
// the fixture node originally staged with database id dbid.
func (f *fixture) row(sym string, dbid uint64) *Row {
	r := NewRow()
	r.SetNode(sym, f.dense[dbid])
	return r
}

// tworow binds two node variables in one Row, for property-vs-property
// comparison tests.
func (f *fixture) tworow(symA string, dbidA uint64, symB string, dbidB uint64) *Row {
	r := NewRow()
	r.SetNode(symA, f.dense[dbidA])
	r.SetNode(symB, f.dense[dbidB])
	return r
}

func (f *fixture) env() *Env {
	return &Env{Snap: f.snap}
}

// --- decodeCypherStringLiteral -----------------------------------------------

func TestDecodeCypherStringLiteral(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"plain single-quoted", `'hello'`, "hello"},
		{"plain double-quoted", `"hello"`, "hello"},
		{"escaped single quote", `'it\'s'`, "it's"},
		{"escaped double quote", `"a\"b"`, `a"b`},
		{"escaped backslash", `'a\\b'`, `a\b`},
		{"escaped newline", `'a\nb'`, "a\nb"},
		{"escaped tab", `'a\tb'`, "a\tb"},
		{"empty", `''`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := decodeCypherStringLiteral(c.raw)
			if err != nil {
				t.Fatalf("decodeCypherStringLiteral(%q) error: %v", c.raw, err)
			}
			if got != c.want {
				t.Fatalf("decodeCypherStringLiteral(%q) = %q, want %q", c.raw, got, c.want)
			}
		})
	}

	t.Run("mismatched quotes error", func(t *testing.T) {
		if _, err := decodeCypherStringLiteral(`'unterminated"`); err == nil {
			t.Fatal("expected an error for mismatched quotes")
		}
	})
}

// --- EvalValue: literals -----------------------------------------------------

func TestEvalValueLiterals(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := NewRow()

	cases := []struct {
		name  string
		query string
		want  any
	}{
		{"integer", "MATCH (n) RETURN 5", float64(5)},
		{"float", "MATCH (n) RETURN 5.5", float64(5.5)},
		{"string", "MATCH (n) RETURN 'hi'", "hi"},
		{"escaped string", `MATCH (n) RETURN 'it\'s'`, "it's"},
		{"bool true", "MATCH (n) RETURN true", true},
		{"bool false", "MATCH (n) RETURN false", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			val, ok, err := EvalValue(env, row, returnExprOf(t, c.query))
			if err != nil {
				t.Fatalf("EvalValue: %v", err)
			}
			if !ok {
				t.Fatal("EvalValue: ok = false, want true")
			}
			if val != c.want {
				t.Fatalf("EvalValue = %#v, want %#v", val, c.want)
			}
		})
	}

	t.Run("null literal is present-null", func(t *testing.T) {
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN null"))
		if err != nil {
			t.Fatalf("EvalValue: %v", err)
		}
		if !ok || val != nil {
			t.Fatalf("EvalValue(null) = (%#v, %v), want (nil, true)", val, ok)
		}
	})
}

// --- EvalValue: property lookups ---------------------------------------------

func TestEvalValuePropertyLookup(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("present string", func(t *testing.T) {
		row := f.row("n", 200)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN n.name"))
		if err != nil || !ok || val != "AZUREADKERBEROS.TEST.LOCAL" {
			t.Fatalf("got (%#v, %v, %v)", val, ok, err)
		}
	})

	t.Run("absent property", func(t *testing.T) {
		row := f.row("n", 100)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN n.name"))
		if err != nil || ok || val != nil {
			t.Fatalf("got (%#v, %v, %v), want (nil, false, nil)", val, ok, err)
		}
	})

	t.Run("property name never seen anywhere in the snapshot", func(t *testing.T) {
		row := f.row("n", 200)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN n.totallyUnknownProperty"))
		if err != nil || ok || val != nil {
			t.Fatalf("got (%#v, %v, %v), want (nil, false, nil)", val, ok, err)
		}
	})

	t.Run("present JSON null", func(t *testing.T) {
		row := f.row("n", 760)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN n.nullprop"))
		if err != nil || !ok || val != nil {
			t.Fatalf("got (%#v, %v, %v), want (nil, true, nil)", val, ok, err)
		}
	})
}

// --- Supported functions -----------------------------------------------------

func TestEvalIDFunction(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("node", func(t *testing.T) {
		row := f.row("n", 850)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN id(n)"))
		if err != nil || !ok {
			t.Fatalf("EvalValue error/ok: %v %v", err, ok)
		}
		if id, isFloat := val.(float64); !isFloat || id != 850 {
			t.Fatalf("id(n) = %#v, want float64(850)", val)
		}
	})

	t.Run("edge", func(t *testing.T) {
		row := NewRow()
		fwd, found := f.snap.EdgeByID(5000)
		if !found {
			t.Fatal("EdgeByID(5000) not found")
		}
		row.SetEdge("r", EdgeRef{Fwd: fwd})
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH ()-[r]->() RETURN id(r)"))
		if err != nil || !ok {
			t.Fatalf("EvalValue error/ok: %v %v", err, ok)
		}
		if id, isFloat := val.(float64); !isFloat || id != 5000 {
			t.Fatalf("id(r) = %#v, want float64(5000)", val)
		}
	})
}

func TestEvalLabelsFunction(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := f.row("n", 900)

	val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN labels(n)"))
	if err != nil || !ok {
		t.Fatalf("EvalValue error/ok: %v %v", err, ok)
	}
	got, isList := val.([]any)
	if !isList || len(got) != 2 || got[0] != "User" || got[1] != "Computer" {
		t.Fatalf("labels(n) = %#v, want [User Computer] in kind_ids order", val)
	}
}

func TestEvalTypeFunction(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := NewRow()
	fwd, found := f.snap.EdgeByID(5000)
	if !found {
		t.Fatal("EdgeByID(5000) not found")
	}
	row.SetEdge("r", EdgeRef{Fwd: fwd})

	val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH ()-[r]->() RETURN type(r)"))
	if err != nil || !ok || val != "MemberOf" {
		t.Fatalf("type(r) = (%#v, %v, %v), want (\"MemberOf\", true, nil)", val, ok, err)
	}
}

func TestEvalToLowerToUpper(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("present string", func(t *testing.T) {
		row := f.row("n", 200)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN toLower(n.name)"))
		if err != nil || !ok || val != "azureadkerberos.test.local" {
			t.Fatalf("got (%#v, %v, %v)", val, ok, err)
		}
		val, ok, err = EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN toUpper(n.name)"))
		if err != nil || !ok || val != "AZUREADKERBEROS.TEST.LOCAL" {
			t.Fatalf("got (%#v, %v, %v)", val, ok, err)
		}
	})

	t.Run("absent property is NULL", func(t *testing.T) {
		row := f.row("n", 100)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN toUpper(n.name)"))
		if err != nil || ok || val != nil {
			t.Fatalf("got (%#v, %v, %v), want (nil, false, nil)", val, ok, err)
		}
	})

	t.Run("present JSON null behaves as absent, not a cast error", func(t *testing.T) {
		row := f.row("n", 760)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN toUpper(n.nullprop)"))
		if err != nil || ok || val != nil {
			t.Fatalf("got (%#v, %v, %v), want (nil, false, nil)", val, ok, err)
		}
	})

	t.Run("present non-string bails with ErrRuntimeCast", func(t *testing.T) {
		row := f.row("n", 850) // score: float64(42)
		_, _, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN toUpper(n.score)"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})
}

func TestEvalCoalesce(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("first non-null wins", func(t *testing.T) {
		row := f.row("u", 400) // gmsa: true
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (u) RETURN coalesce(u.gmsa, false)"))
		if err != nil || !ok || val != true {
			t.Fatalf("got (%#v, %v, %v)", val, ok, err)
		}
	})

	t.Run("falls through absent property to the default", func(t *testing.T) {
		row := f.row("u", 300) // no gmsa
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (u) RETURN coalesce(u.gmsa, false)"))
		if err != nil || !ok || val != false {
			t.Fatalf("got (%#v, %v, %v)", val, ok, err)
		}
	})

	t.Run("all-null arguments yield NULL", func(t *testing.T) {
		row := f.row("u", 300)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (u) RETURN coalesce(u.gmsa, u.alsoMissing)"))
		if err != nil || ok || val != nil {
			t.Fatalf("got (%#v, %v, %v), want (nil, false, nil)", val, ok, err)
		}
	})
}

// TestEvalCoalesceGuardIdiom pins the brief's named corpus-guard scenario:
// COALESCE(u.gmsa, false) = true on a node without gmsa evaluates to
// TriFalse, not TriNull -- coalesce always produces a definite (never
// "absent") value here, so the comparison is a plain, total boolean
// equality.
func TestEvalCoalesceGuardIdiom(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := f.row("u", 300) // no gmsa property

	got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (u) WHERE COALESCE(u.gmsa, false) = true RETURN u"))
	if err != nil {
		t.Fatalf("EvalPredicate: %v", err)
	}
	if got != TriFalse {
		t.Fatalf("got %s, want %s", got, TriFalse)
	}
}

func TestEvalSizeFunction(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("list property", func(t *testing.T) {
		row := f.row("n", 800)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN size(n.spns)"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		// The package's value model is float64-only for numbers (see value.go's
		// doc comment): every comparison/arithmetic primitive in value.go
		// type-switches on float64, so an int32 result here would silently
		// break `WHERE size(...) = 3`, `> `, and `IN` (they'd never match a
		// float64 literal). A later projection layer is expected to convert a
		// bare, top-level size() result to int32 for pg parity -- see
		// evalSizeFunction's doc comment.
		if n, isFloat64 := val.(float64); !isFloat64 || n != 3 {
			t.Fatalf("size(n.spns) = %#v, want float64(3)", val)
		}
	})

	t.Run("absent property is NULL", func(t *testing.T) {
		row := f.row("n", 100)
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN size(n.spns)"))
		if err != nil || ok || val != nil {
			t.Fatalf("got (%#v, %v, %v), want (nil, false, nil)", val, ok, err)
		}
	})

	t.Run("non-list operand is unsupported here (delegated at plan time)", func(t *testing.T) {
		row := f.row("n", 200) // name is a string, not a list
		_, _, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN size(n.name)"))
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("err = %v, want ErrUnsupported", err)
		}
	})

	// Regression coverage for the float64-vs-int32 bug: size() must compose
	// with every numeric comparison primitive in value.go (ScalarEq,
	// OrderCompare, In), all of which type-switch on float64 only. Before the
	// fix, evalSizeFunction returned a bare Go int32, so none of these three
	// ever matched a float64 numeric literal and each silently evaluated to
	// TriFalse instead of TriTrue.
	t.Run("size() = literal compares equal via ScalarEq", func(t *testing.T) {
		row := f.row("n", 800)
		got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE size(n.spns) = 3 RETURN n"))
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriTrue {
			t.Fatalf("got %s, want %s", got, TriTrue)
		}
	})

	t.Run("size() > literal compares via OrderCompare", func(t *testing.T) {
		row := f.row("n", 800)
		got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE size(n.spns) > 2 RETURN n"))
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriTrue {
			t.Fatalf("got %s, want %s", got, TriTrue)
		}
	})

	t.Run("size() IN literal list matches via In", func(t *testing.T) {
		row := f.row("n", 800)
		got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE size(n.spns) IN [3, 4] RETURN n"))
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriTrue {
			t.Fatalf("got %s, want %s", got, TriTrue)
		}
	})
}

func TestEvalSplitFunction(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := f.row("n", 200)

	val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN split(n.name, '.')"))
	if err != nil || !ok {
		t.Fatalf("got err=%v ok=%v", err, ok)
	}
	got, isList := val.([]any)
	want := []any{"AZUREADKERBEROS", "TEST", "LOCAL"}
	if !isList || len(got) != len(want) {
		t.Fatalf("split(...) = %#v, want %#v", val, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("split(...)[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestEvalDateTimeEpoch(t *testing.T) {
	f := newFixture(t)
	now := time.Unix(2_000_000_000, 123_000_000).UTC()
	env := &Env{Snap: f.snap, Now: now}
	row := NewRow()

	t.Run("epochseconds", func(t *testing.T) {
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN datetime().epochseconds"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if v, isFloat := val.(float64); !isFloat || v != float64(now.Unix()) {
			t.Fatalf("epochseconds = %#v, want %d", val, now.Unix())
		}
	})

	t.Run("epochmillis", func(t *testing.T) {
		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN datetime().epochmillis"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if v, isFloat := val.(float64); !isFloat || v != float64(now.UnixMilli()) {
			t.Fatalf("epochmillis = %#v, want %d", val, now.UnixMilli())
		}
	})
}

// --- Arithmetic ---------------------------------------------------------

func TestEvalArithmetic(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := NewRow()

	cases := []struct {
		query string
		want  float64
	}{
		{"MATCH (n) RETURN 2 + 3", 5},
		{"MATCH (n) RETURN 2 - 3", -1},
		{"MATCH (n) RETURN 2 * 3", 6},
		{"MATCH (n) RETURN 6 / 3", 2},
		{"MATCH (n) RETURN 7 % 3", 1},
		{"MATCH (n) RETURN 60 * 86400", 60 * 86400},
		{"MATCH (n) RETURN -5", -5},
		{"MATCH (n) RETURN 2 + 3 * 4", 14}, // precedence handled by the parser's own AST shape
	}
	for _, c := range cases {
		t.Run(c.query, func(t *testing.T) {
			val, ok, err := EvalValue(env, row, returnExprOf(t, c.query))
			if err != nil || !ok {
				t.Fatalf("got err=%v ok=%v", err, ok)
			}
			if val != c.want {
				t.Fatalf("got %#v, want %v", val, c.want)
			}
		})
	}

	t.Run("division by zero bails with ErrRuntimeCast", func(t *testing.T) {
		_, _, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN 1 / 0"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})

	t.Run("modulo by zero bails with ErrRuntimeCast", func(t *testing.T) {
		_, _, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN 1 % 0"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})
}

// TestEvalStringConcatenation pins gap (b)'s fix (task 16b) as corrected by
// this task's own review finding: `+` is disambiguated STATICALLY, from
// each operand's AST shape (classifyAddOperand), never from what it
// evaluates to at runtime -- applyAdd's own doc comment pins the full
// pg-parity investigation, the review finding that replaced the earlier
// runtime-sniffing implementation, and the safety argument for every case
// exercised here.
func TestEvalStringConcatenation(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("string-lit+string-prop concat", func(t *testing.T) {
		val, ok, err := EvalValue(env, f.row("n", 200), returnExprOf(t, "MATCH (n) RETURN 'CN=' + n.name"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != "CN=AZUREADKERBEROS.TEST.LOCAL" {
			t.Fatalf("got %#v, want %q", val, "CN=AZUREADKERBEROS.TEST.LOCAL")
		}
	})

	t.Run("string-prop+string-lit concat (operand order reversed)", func(t *testing.T) {
		val, ok, err := EvalValue(env, f.row("n", 200), returnExprOf(t, "MATCH (n) RETURN n.name + '-x'"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != "AZUREADKERBEROS.TEST.LOCAL-x" {
			t.Fatalf("got %#v, want %q", val, "AZUREADKERBEROS.TEST.LOCAL-x")
		}
	})

	// prop+prop: checkArithmetic (plan.go) rejects this shape at plan time
	// (TestPlanRejectMatrix's "arithmetic + both operands property lookups
	// rejected" pins that), so a served query never reaches this. Called
	// directly here anyway (bypassing Plan, as every other test in this
	// file does), applyAdd's defensive branch bails ErrUnsupported rather
	// than guess -- it must NOT silently concatenate (which the runtime
	// value happening to be two strings would previously have done) nor
	// silently add, since neither answer is provably the one pg would give
	// for every runtime shape this AST pairing can produce.
	t.Run("prop+prop bails ErrUnsupported (defensive; Plan rejects this shape)", func(t *testing.T) {
		row := f.tworow("n", 200, "m", 960) // 960's name = "beta"
		_, _, err := EvalValue(env, row, returnExprOf(t, "MATCH (n),(m) RETURN n.name + m.name"))
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("err = %v, want ErrUnsupported", err)
		}
	})

	t.Run("string-lit+absent property is NULL, not an error", func(t *testing.T) {
		// Node 100 has no "name" property at all -- mirrors applyArithmetic's
		// existing, unchanged absent-operand contract (a==nil||b==nil -> NULL)
		// which this fix's applyAdd is layered underneath, not around.
		val, ok, err := EvalValue(env, f.row("n", 100), returnExprOf(t, "MATCH (n) RETURN 'CN=' + n.name"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok || val != nil {
			t.Fatalf("got val=%#v ok=%v, want (nil, false)", val, ok)
		}
	})

	t.Run("string-lit+non-string bool-prop bails ErrRuntimeCast", func(t *testing.T) {
		_, _, err := EvalValue(env, f.row("n", 700), returnExprOf(t, "MATCH (n) RETURN 'x' + n.flag"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})

	t.Run("number-prop+string-lit bails ErrRuntimeCast (concat semantics, non-string number)", func(t *testing.T) {
		_, _, err := EvalValue(env, f.row("n", 850), returnExprOf(t, "MATCH (n) RETURN n.score + 'x'"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})

	t.Run("string-lit+number-prop bails ErrRuntimeCast (exact operand order)", func(t *testing.T) {
		_, _, err := EvalValue(env, f.row("n", 850), returnExprOf(t, "MATCH (n) RETURN 'x' + n.score"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})

	t.Run("number-prop+number-lit adds (neither operand statically Text)", func(t *testing.T) {
		val, ok, err := EvalValue(env, f.row("n", 850), returnExprOf(t, "MATCH (n) RETURN n.score + 1"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != float64(43) {
			t.Fatalf("got %#v, want 43", val)
		}
	})

	t.Run("1+number-prop adds (exact operand order)", func(t *testing.T) {
		val, ok, err := EvalValue(env, f.row("n", 850), returnExprOf(t, "MATCH (n) RETURN 1 + n.score"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != float64(43) {
			t.Fatalf("got %#v, want 43", val)
		}
	})

	// 1+string-prop: neither operand is statically Text (1 is a plain
	// numeric literal, n.name is a property lookup, not both are property
	// lookups), so this takes NUMERIC semantics -- and n.name evaluates to
	// a genuine runtime string, so it bails ErrRuntimeCast rather than
	// silently concatenate. This is the shape a purely runtime-sniffing
	// implementation risks getting wrong (grouping "not statically Text"
	// with "whatever it evaluates to" and treating two runtime strings as
	// automatic concatenation): pg's own `(properties->>'name')::numeric`
	// cast would itself raise a runtime error for this row, which this
	// bail mirrors instead of guessing.
	t.Run("1+string-prop bails ErrRuntimeCast (numeric semantics, runtime string)", func(t *testing.T) {
		_, _, err := EvalValue(env, f.row("n", 200), returnExprOf(t, "MATCH (n) RETURN 1 + n.name"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})

	// coalesce() (review finding, this task): classifyCoalesceOperand
	// (eval.go) derives coalesce's static addOperandKind from its own
	// arguments, mirroring dawgs' translateCoalesceFunction. A string
	// literal argument gives the whole call a known Text type
	// (addStaticText) -- PlanRejectMatrix's "coalesce with a
	// string-literal argument accepted" pins that this is accepted at plan
	// time; these two subtests pin the resulting eval-time CONCAT
	// semantics, in both directions:
	t.Run("coalesce(prop,'lit')+lit concatenates when coalesce evaluates to a string", func(t *testing.T) {
		val, ok, err := EvalValue(env, f.row("n", 200), returnExprOf(t, "MATCH (n) RETURN coalesce(n.name, 'default') + '!'"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != "AZUREADKERBEROS.TEST.LOCAL!" {
			t.Fatalf("got %#v, want %q", val, "AZUREADKERBEROS.TEST.LOCAL!")
		}
	})

	// The headline case the finding named directly: `coalesce(n.score,
	// 'default')` is statically Text in pg (the string-literal argument),
	// so `+ 1` takes CONCAT semantics -- and node 850's n.score is present
	// and numeric (42.0, not a string), so this must BAIL ErrRuntimeCast,
	// never silently add to 43 the way the old addOther classification
	// would have (numeric semantics, 42+1=43 -- exactly the wrong-answer
	// shape this fix closes).
	t.Run("coalesce(prop,'lit')+1 bails ErrRuntimeCast when coalesce evaluates to a number (the 42-score case)", func(t *testing.T) {
		_, _, err := EvalValue(env, f.row("n", 850), returnExprOf(t, "MATCH (n) RETURN coalesce(n.score, 'default') + 1"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast (got a served value instead of bailing -- exactly the silent-wrong-answer shape this fix closes)", err)
		}
	})

	// coalesce(prop, 5): the numeric-literal argument gives the whole call
	// a known non-Text type (addOther), so `+ 1` takes NUMERIC semantics --
	// correct whether the property is present (score=42, 850) or absent
	// (falls back to the literal 5, 100).
	t.Run("coalesce(prop,5)+1 adds numerically when the property is present", func(t *testing.T) {
		val, ok, err := EvalValue(env, f.row("n", 850), returnExprOf(t, "MATCH (n) RETURN coalesce(n.score, 5) + 1"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != float64(43) {
			t.Fatalf("got %#v, want 43", val)
		}
	})

	t.Run("coalesce(prop,5)+1 adds numerically when the property is absent (falls back to the literal)", func(t *testing.T) {
		val, ok, err := EvalValue(env, f.row("n", 100), returnExprOf(t, "MATCH (n) RETURN coalesce(n.score, 5) + 1"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != float64(6) {
			t.Fatalf("got %#v, want 6", val)
		}
	})

	// coalesce(n.a, m.b): every argument a bare property lookup, no
	// pg-known type anywhere (addUnresolved) -- TestPlanRejectMatrix's
	// "coalesce of all bare properties rejected" pins that Plan rejects
	// this shape outright, so a served query never reaches this. Called
	// directly here anyway (bypassing Plan, as every other test in this
	// function does), applyAdd's defensive addUnresolved branch bails
	// ErrUnsupported rather than guess -- see classifyCoalesceOperand's own
	// doc for why pg itself cannot safely resolve this shape either,
	// regardless of what it is paired with.
	t.Run("coalesce(allprops)+1 bails ErrUnsupported (defensive; Plan rejects this shape)", func(t *testing.T) {
		row := f.tworow("n", 200, "m", 960)
		_, _, err := EvalValue(env, row, returnExprOf(t, "MATCH (n),(m) RETURN coalesce(n.name, m.name) + 1"))
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("err = %v, want ErrUnsupported", err)
		}
	})

	// type() (audit, this task): EdgeTypeFunction is statically Text in pg
	// (function.go's `CastType: pgsql.Text`, the identical shape to
	// toLower()/toUpper()) -- classifyAddOperand previously fell through to
	// its addOther default for type(), which never served a WRONG numeric
	// answer (evalTypeFunction always returns a genuine Go string, and the
	// numeric branch already bails ErrRuntimeCast on any present runtime
	// string) but DID spuriously decline a query pg would concatenate
	// correctly. This is the concrete case that changes: both operands are
	// genuine runtime strings, so this must now succeed.
	t.Run("type()+string-prop concatenates (fix: type() is statically Text, not addOther)", func(t *testing.T) {
		row := NewRow()
		row.SetNode("n", f.dense[200])
		fwd, found := f.snap.EdgeByID(5000)
		if !found {
			t.Fatal("EdgeByID(5000) not found")
		}
		row.SetEdge("r", EdgeRef{Fwd: fwd})

		val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n)-[r]->() RETURN type(r) + n.name"))
		if err != nil || !ok {
			t.Fatalf("got err=%v ok=%v", err, ok)
		}
		if val != "MemberOfAZUREADKERBEROS.TEST.LOCAL" {
			t.Fatalf("got %#v, want %q", val, "MemberOfAZUREADKERBEROS.TEST.LOCAL")
		}
	})

	// split()/labels() (audit, this task): both are array-typed in pg
	// (TextArray), a third `+` semantics (list concatenation) this package
	// implements nowhere -- left classified addOther, safe by construction
	// rather than by a correct type mirror, since evalSplitFunction/
	// evalLabelsFunction always return a Go []any, which can satisfy
	// neither applyAdd's `.(string)` nor its `.(float64)` check. Both must
	// bail ErrUnsupported (numeric-default branch: neither operand is
	// statically Text, so the []any hits the final `.(float64)` check).
	t.Run("split()+number bails ErrUnsupported (array-typed operand, list concat not implemented)", func(t *testing.T) {
		_, _, err := EvalValue(env, f.row("n", 200), returnExprOf(t, "MATCH (n) RETURN split(n.name, '.') + 1"))
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("err = %v, want ErrUnsupported", err)
		}
	})

	t.Run("labels()+number bails ErrUnsupported (array-typed operand, list concat not implemented)", func(t *testing.T) {
		_, _, err := EvalValue(env, f.row("n", 900), returnExprOf(t, "MATCH (n) RETURN labels(n) + 1"))
		if !errors.Is(err, ErrUnsupported) {
			t.Fatalf("err = %v, want ErrUnsupported", err)
		}
	})

	// The corpus shape itself (selector/AdminSDHolder): a concatenation
	// result feeding an equality comparison. An absent right-hand property
	// must make the whole comparison NULL (the brief's own pinned rule:
	// "absent property -> NULL result -> comparison NULL -> row drops"),
	// not an error.
	t.Run("concatenation feeding an equality comparison: match", func(t *testing.T) {
		row := f.tworow("n", 200, "m", 960)
		got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n),(m) WHERE m.name = n.name + 'beta' RETURN m"))
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriFalse {
			t.Fatalf("got %s, want TriFalse (960's name is \"beta\", not \"AZUREADKERBEROS.TEST.LOCALbeta\")", got)
		}
	})

	t.Run("concatenation feeding an equality comparison: absent operand drops the row (NULL, not an error)", func(t *testing.T) {
		row := f.tworow("n", 100, "m", 960) // 100 has no "name" property
		got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n),(m) WHERE m.name = n.name + 'beta' RETURN m"))
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriNull {
			t.Fatalf("got %s, want TriNull", got)
		}
	})
}

// TestEvalLastLogonTimestampScenario pins the brief's named scenario:
// n.lastlogontimestamp < (datetime().epochseconds - (60 * 86400)) evaluated
// against a controlled Env.Now.
func TestEvalLastLogonTimestampScenario(t *testing.T) {
	f := newFixture(t)
	now := time.Unix(2_000_000_000, 0).UTC()
	env := &Env{Snap: f.snap, Now: now}
	query := "MATCH (n) WHERE n.lastlogontimestamp < (datetime().epochseconds - (60 * 86400)) RETURN n"
	expr := whereExprOf(t, query)

	t.Run("ancient logon is before the cutoff", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 600), expr)
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriTrue {
			t.Fatalf("got %s, want %s", got, TriTrue)
		}
	})

	t.Run("recent logon is after the cutoff", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 620), expr)
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriFalse {
			t.Fatalf("got %s, want %s", got, TriFalse)
		}
	})

	t.Run("absent timestamp is null", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 650), expr)
		if err != nil {
			t.Fatalf("EvalPredicate: %v", err)
		}
		if got != TriNull {
			t.Fatalf("got %s, want %s", got, TriNull)
		}
	})
}

// --- Comparison routing (=, <>, <,>,<=,>=) -----------------------------------

func TestEvalStringEqRouting(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("string literal vs matching property", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 200), whereExprOf(t, "MATCH (n) WHERE n.name = 'AZUREADKERBEROS.TEST.LOCAL' RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("string literal vs absent property is null", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 100), whereExprOf(t, "MATCH (n) WHERE n.name = 'X' RETURN n"))
		if err != nil || got != TriNull {
			t.Fatalf("got %s, %v, want TriNull", got, err)
		}
	})

	t.Run("bool property never equals a string literal (jsonb_typeof guard)", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 700), whereExprOf(t, "MATCH (n) WHERE n.flag = 'true' RETURN n"))
		if err != nil || got != TriFalse {
			t.Fatalf("got %s, %v, want TriFalse", got, err)
		}
	})

	t.Run("string <> absent is null", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 100), whereExprOf(t, "MATCH (n) WHERE n.name <> 'X' RETURN n"))
		if err != nil || got != TriNull {
			t.Fatalf("got %s, %v, want TriNull", got, err)
		}
	})
}

func TestEvalScalarEqRouting(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	cases := []struct {
		name  string
		dbid  uint64
		query string
		want  Tri
	}{
		{"numeric equal", 850, "MATCH (n) WHERE n.score = 42 RETURN n", TriTrue},
		{"numeric not equal", 850, "MATCH (n) WHERE n.score = 43 RETURN n", TriFalse},
		{"numeric absent is null", 100, "MATCH (n) WHERE n.score = 42 RETURN n", TriNull},
		{"numeric <> present", 850, "MATCH (n) WHERE n.score <> 43 RETURN n", TriTrue},
		{"bool equal", 700, "MATCH (n) WHERE n.flag = true RETURN n", TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EvalPredicate(env, f.row("n", c.dbid), whereExprOf(t, c.query))
			if err != nil {
				t.Fatalf("EvalPredicate: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestEvalPropEqRouting(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("two present equal properties", func(t *testing.T) {
		row := f.tworow("a", 850, "b", 850) // same underlying node -- score = score
		got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE a.score = b.score RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("one side absent is null", func(t *testing.T) {
		row := f.tworow("a", 850, "b", 100) // b has no score
		got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE a.score = b.score RETURN n"))
		if err != nil || got != TriNull {
			t.Fatalf("got %s, %v, want TriNull", got, err)
		}
	})
}

// --- evalIdentityEquality ----------------------------------------------

// TestEvalNodeIdentityEquality: bare node-variable `=`/`<>` must compare by
// identity (the same underlying dense NodeID), pre-empting the generic
// PropEq path entirely -- see evalIdentityEquality's own doc comment for why
// this matters: two *distinct* nodes carrying identical (here, both
// entirely absent) property bags would otherwise compare structurally
// "equal" via PropEq's jsonbEqual, exactly backward for what identity
// comparison means. Node 100 ("no name at all") and node 300 ("no gmsa")
// are deliberately chosen for this: both carry an empty property map, so a
// property-value comparison of the two would (wrongly) also come out
// TriTrue, masking a regression that reintroduced the value-comparison
// fallback for this shape -- two nodes with visibly different properties
// would not catch that class of bug at all.
func TestEvalNodeIdentityEquality(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	n1, n2 := f.dense[100], f.dense[300]
	row := func(a, b snapshot.NodeID) *Row {
		r := NewRow()
		r.SetNode("a", a)
		r.SetNode("b", b)
		return r
	}

	cases := []struct {
		name  string
		row   *Row
		query string
		want  Tri
	}{
		{"same node, =, TriTrue", row(n1, n1), "MATCH (n) WHERE a = b RETURN n", TriTrue},
		{"different nodes, =, TriFalse", row(n1, n2), "MATCH (n) WHERE a = b RETURN n", TriFalse},
		{"same node, <>, TriFalse", row(n1, n1), "MATCH (n) WHERE a <> b RETURN n", TriFalse},
		{"different nodes, <>, TriTrue", row(n1, n2), "MATCH (n) WHERE a <> b RETURN n", TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EvalPredicate(env, c.row, whereExprOf(t, c.query))
			if err != nil {
				t.Fatalf("EvalPredicate: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

// TestEvalEdgeIdentityEquality: the same identity-comparison shape as
// TestEvalNodeIdentityEquality, but for bare edge variables (EdgeRef.Fwd
// identity via Row.Edge) -- evalIdentityEquality's second branch, reached
// only once both operands fail the node check.
func TestEvalEdgeIdentityEquality(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	row := func(a, b EdgeRef) *Row {
		r := NewRow()
		r.SetEdge("r1", a)
		r.SetEdge("r2", b)
		return r
	}

	cases := []struct {
		name  string
		row   *Row
		query string
		want  Tri
	}{
		{"same edge, =, TriTrue", row(EdgeRef{Fwd: 5}, EdgeRef{Fwd: 5}), "MATCH (n) WHERE r1 = r2 RETURN n", TriTrue},
		{"different edges, =, TriFalse", row(EdgeRef{Fwd: 5}, EdgeRef{Fwd: 7}), "MATCH (n) WHERE r1 = r2 RETURN n", TriFalse},
		{"same edge, <>, TriFalse", row(EdgeRef{Fwd: 5}, EdgeRef{Fwd: 5}), "MATCH (n) WHERE r1 <> r2 RETURN n", TriFalse},
		{"different edges, <>, TriTrue", row(EdgeRef{Fwd: 5}, EdgeRef{Fwd: 7}), "MATCH (n) WHERE r1 <> r2 RETURN n", TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EvalPredicate(env, c.row, whereExprOf(t, c.query))
			if err != nil {
				t.Fatalf("EvalPredicate: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

// TestEvalScalarMapComparesByValueNotIdentity is a guard test: it proves
// evalIdentityEquality's shortcut applies only to a bare Variable that
// resolves to a bound Node or Edge in the Row (per its own doc comment),
// never to a scalar binding -- the shape a future WITH pipeline produces for
// a map-valued expression (`WITH {a: 1} AS m`). Two distinct Go map values
// with identical content must still compare equal via the generic PropEq/
// jsonbEqual path, and two with different content must not: if a future
// change ever widened the identity shortcut to scalars (or scalars were
// compared by some pointer/reference notion instead), the "identical
// content, different Go map instances" case below would wrongly flip to
// TriFalse.
func TestEvalScalarMapComparesByValueNotIdentity(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	row := NewRow()
	row.SetScalar("m1", map[string]any{"x": float64(1), "y": "a"})
	row.SetScalar("m2", map[string]any{"x": float64(1), "y": "a"}) // distinct map value, same content

	got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE m1 = m2 RETURN n"))
	if err != nil {
		t.Fatalf("EvalPredicate: %v", err)
	}
	if got != TriTrue {
		t.Fatalf("got %s, want TriTrue (maps with identical content must compare equal by value)", got)
	}

	row.SetScalar("m2", map[string]any{"x": float64(2), "y": "a"})
	got, err = EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE m1 = m2 RETURN n"))
	if err != nil {
		t.Fatalf("EvalPredicate: %v", err)
	}
	if got != TriFalse {
		t.Fatalf("got %s, want TriFalse (maps with different content must not compare equal)", got)
	}
}

func TestEvalOrderCompare(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	cases := []struct {
		name  string
		dbid  uint64
		query string
		want  Tri
	}{
		{"less than true", 850, "MATCH (n) WHERE n.score < 100 RETURN n", TriTrue},
		{"less than false", 850, "MATCH (n) WHERE n.score < 10 RETURN n", TriFalse},
		{"less-equal at boundary", 850, "MATCH (n) WHERE n.score <= 42 RETURN n", TriTrue},
		{"greater than true", 850, "MATCH (n) WHERE n.score > 10 RETURN n", TriTrue},
		{"greater-equal at boundary", 850, "MATCH (n) WHERE n.score >= 42 RETURN n", TriTrue},
		{"absent operand is null", 100, "MATCH (n) WHERE n.score < 100 RETURN n", TriNull},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EvalPredicate(env, f.row("n", c.dbid), whereExprOf(t, c.query))
			if err != nil {
				t.Fatalf("EvalPredicate: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}

	t.Run("string vs string bubbles ErrCollation", func(t *testing.T) {
		row := f.tworow("a", 200, "b", 960)
		_, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE a.name < b.name RETURN n"))
		if !errors.Is(err, ErrCollation) {
			t.Fatalf("err = %v, want ErrCollation", err)
		}
	})
}

// --- String predicates (STARTS WITH / ENDS WITH / CONTAINS / =~) ------------

func TestEvalToUpperContainsScenario(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	query := "MATCH (t) WHERE toUpper(t.operatingsystem) CONTAINS 'SERVER' RETURN t"
	expr := whereExprOf(t, query)

	got, err := EvalPredicate(env, f.row("t", 500), expr)
	if err != nil || got != TriTrue {
		t.Fatalf("got %s, %v, want TriTrue", got, err)
	}

	got, err = EvalPredicate(env, f.row("t", 550), expr)
	if err != nil || got != TriFalse {
		t.Fatalf("got %s, %v, want TriFalse", got, err)
	}
}

func TestEvalNotStartsWithMissingProperty(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	query := "MATCH (n) WHERE NOT n.name STARTS WITH 'AZUREADKERBEROS.' RETURN n"
	expr := whereExprOf(t, query)

	t.Run("property missing satisfies the negation", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 100), expr)
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("property present and matching negates to false", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 200), expr)
		if err != nil || got != TriFalse {
			t.Fatalf("got %s, %v, want TriFalse", got, err)
		}
	})
}

func TestEvalStringPredicates(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("ends with", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 200), whereExprOf(t, "MATCH (n) WHERE n.name ENDS WITH '.LOCAL' RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("regex match", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 200), whereExprOf(t, "MATCH (n) WHERE n.name =~ '^AZURE.*LOCAL$' RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("negated regex over an absent property is true", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 100), whereExprOf(t, "MATCH (n) WHERE NOT n.name =~ '^AZURE.*' RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("regex reused across rows against the same Env stays correct", func(t *testing.T) {
		// Exercises Env's regex compilation cache: the same *Env (and thus the
		// same cached *regexp.Regexp for this pattern) is evaluated against
		// two different rows.
		expr := whereExprOf(t, "MATCH (n) WHERE n.name =~ '^AZURE.*' RETURN n")
		if got, err := EvalPredicate(env, f.row("n", 200), expr); err != nil || got != TriTrue {
			t.Fatalf("row 1: got %s, %v, want TriTrue", got, err)
		}
		if got, err := EvalPredicate(env, f.row("n", 960), expr); err != nil || got != TriFalse {
			t.Fatalf("row 2: got %s, %v, want TriFalse", got, err)
		}
	})

	t.Run("present non-string bails with ErrRuntimeCast", func(t *testing.T) {
		_, err := EvalPredicate(env, f.row("n", 700), whereExprOf(t, "MATCH (n) WHERE n.flag STARTS WITH 'x' RETURN n"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})
}

// --- IN -----------------------------------------------------------------

func TestEvalIn(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	cases := []struct {
		name  string
		dbid  uint64
		query string
		want  Tri
	}{
		{"numeric literal list hit", 850, "MATCH (n) WHERE n.score IN [42, 43] RETURN n", TriTrue},
		{"numeric literal list miss", 850, "MATCH (n) WHERE n.score IN [1, 2] RETURN n", TriFalse},
		{"empty list is always false", 850, "MATCH (n) WHERE n.score IN [] RETURN n", TriFalse},
		{"absent lhs against non-empty list is null", 100, "MATCH (n) WHERE n.score IN [42] RETURN n", TriNull},
		{"list-valued property as the list", 800, "MATCH (n) WHERE 'a' IN n.spns RETURN n", TriTrue},
		{"list-valued property miss", 800, "MATCH (n) WHERE 'z' IN n.spns RETURN n", TriFalse},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EvalPredicate(env, f.row("n", c.dbid), whereExprOf(t, c.query))
			if err != nil {
				t.Fatalf("EvalPredicate: %v", err)
			}
			if got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

// --- IS NULL / IS NOT NULL ---------------------------------------------------

func TestEvalIsNull(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("absent property IS NULL", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 100), whereExprOf(t, "MATCH (n) WHERE n.name IS NULL RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("present property IS NOT NULL", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 200), whereExprOf(t, "MATCH (n) WHERE n.name IS NOT NULL RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})
}

// --- Kind matcher (n:Kind) ----------------------------------------------

func TestEvalKindMatcher(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("all labels present", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 900), whereExprOf(t, "MATCH (n) WHERE n:User:Computer RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("missing one of the labels", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 950), whereExprOf(t, "MATCH (n) WHERE n:User:Computer RETURN n"))
		if err != nil || got != TriFalse {
			t.Fatalf("got %s, %v, want TriFalse", got, err)
		}
	})

	t.Run("single label present", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 950), whereExprOf(t, "MATCH (n) WHERE n:User RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})
}

// --- Pattern predicates (gap (a), task 16b) ---------------------------------

// TestEvalPatternPredicate exercises evalPatternPredicate directly against
// the fixture's one real edge (950 -[:MemberOf]-> 900): both symbols
// already bound in the Row, per checkPatternPredicate's own plan-time
// restriction, so every case here binds "n"=950 and "m"=900 (or the
// reverse) via f.tworow and evaluates a WHERE-clause pattern predicate
// parsed from real Cypher text.
func TestEvalPatternPredicate(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := f.tworow("n", 950, "m", 900)

	cases := []struct {
		name  string
		query string
		want  Tri
	}{
		{"outbound match", "MATCH (n)-[:MemberOf]->(m) WHERE (n)-[:MemberOf]->(m) RETURN n", TriTrue},
		{"outbound reversed does not match", "MATCH (n)-[:MemberOf]->(m) WHERE (m)-[:MemberOf]->(n) RETURN n", TriFalse},
		{"inbound match (reversed direction arrow)", "MATCH (n)-[:MemberOf]->(m) WHERE (m)<-[:MemberOf]-(n) RETURN n", TriTrue},
		{"inbound no match", "MATCH (n)-[:MemberOf]->(m) WHERE (n)<-[:MemberOf]-(m) RETURN n", TriFalse},
		{"undirected matches the existing forward edge", "MATCH (n)-[:MemberOf]->(m) WHERE (n)-[:MemberOf]-(m) RETURN n", TriTrue},
		{"undirected matches regardless of which side is written first", "MATCH (n)-[:MemberOf]->(m) WHERE (m)-[:MemberOf]-(n) RETURN n", TriTrue},
		{"NOT on a true predicate is false", "MATCH (n)-[:MemberOf]->(m) WHERE NOT (n)-[:MemberOf]->(m) RETURN n", TriFalse},
		{"NOT on a false predicate is true", "MATCH (n)-[:MemberOf]->(m) WHERE NOT (m)-[:MemberOf]->(n) RETURN n", TriTrue},
		{"wrong kind does not match", "MATCH (n)-[:MemberOf]->(m) WHERE (n)-[:User]->(m) RETURN n", TriFalse},
		{"kind alternation matches via the second kind", "MATCH (n)-[:MemberOf]->(m) WHERE (n)-[:User|MemberOf]->(m) RETURN n", TriTrue},
		{"no kind restriction matches any kind", "MATCH (n)-[:MemberOf]->(m) WHERE (n)-->(m) RETURN n", TriTrue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := EvalPredicate(env, row, whereExprOf(t, c.query))
			if err != nil {
				t.Fatalf("EvalPredicate(%q): unexpected error: %v", c.query, err)
			}
			if got != c.want {
				t.Fatalf("EvalPredicate(%q) = %s, want %s", c.query, got, c.want)
			}
		})
	}
}

// TestEvalPatternPredicateSelfReferenceNoSelfLoop: `(n)-[:K]->(n)` (both
// pattern-predicate endpoints the same already-bound symbol) asks whether n
// has a self-loop of kind K -- node 950 has none (its only edge is to a
// distinct node, 900), so this must be false, not an error or a vacuous
// true from some accidental "n always relates to itself" shortcut.
func TestEvalPatternPredicateSelfReferenceNoSelfLoop(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := f.row("n", 950)

	got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n) WHERE (n)-[:MemberOf]->(n) RETURN n"))
	if err != nil {
		t.Fatalf("EvalPredicate: unexpected error: %v", err)
	}
	if got != TriFalse {
		t.Fatalf("EvalPredicate = %s, want TriFalse", got)
	}
}

// TestEvalPatternPredicateNeverNull pins the milestone brief's own rule
// directly: a pattern predicate's result is always TriTrue or TriFalse,
// never TriNull, regardless of any node property (there is none inspected
// here at all -- see evalPatternPredicate's own doc comment) -- unlike an
// ordinary property comparison, which propagates NULL for an absent
// property (TestEvalBooleanCombinators' own "AND with one absent operand"
// case, just above, pins that contrasting behavior for comparison).
func TestEvalPatternPredicateNeverNull(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := f.tworow("n", 100, "m", 300) // neither node has any properties at all, and no edge connects them

	got, err := EvalPredicate(env, row, whereExprOf(t, "MATCH (n)-[:MemberOf]->(m) WHERE (n)-[:MemberOf]->(m) RETURN n"))
	if err != nil {
		t.Fatalf("EvalPredicate: unexpected error: %v", err)
	}
	if got != TriFalse {
		t.Fatalf("EvalPredicate = %s, want TriFalse (never TriNull)", got)
	}
}

// --- Conjunction / Disjunction / XOR / NOT -----------------------------------

func TestEvalBooleanCombinators(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("AND with one absent operand is null, not false", func(t *testing.T) {
		// n.flag = true (TRUE) AND n.score = 42 (NULL, score absent on 700)
		got, err := EvalPredicate(env, f.row("n", 700), whereExprOf(t, "MATCH (n) WHERE n.flag = true AND n.score = 42 RETURN n"))
		if err != nil || got != TriNull {
			t.Fatalf("got %s, %v, want TriNull", got, err)
		}
	})

	t.Run("AND with a false operand is false even with a null operand", func(t *testing.T) {
		// n.flag = false (FALSE, since flag is actually true) AND n.score = 42 (NULL)
		got, err := EvalPredicate(env, f.row("n", 700), whereExprOf(t, "MATCH (n) WHERE n.flag = false AND n.score = 42 RETURN n"))
		if err != nil || got != TriFalse {
			t.Fatalf("got %s, %v, want TriFalse", got, err)
		}
	})

	t.Run("OR with a true operand is true even with a null operand", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 700), whereExprOf(t, "MATCH (n) WHERE n.flag = true OR n.score = 42 RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("plain NOT over an ordinary predicate", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 850), whereExprOf(t, "MATCH (n) WHERE NOT n.score = 43 RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("XOR", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 700), whereExprOf(t, "MATCH (n) WHERE n.flag = true XOR n.flag = false RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})
}

// --- Bare property truthiness ------------------------------------------------

func TestEvalBareTruthiness(t *testing.T) {
	f := newFixture(t)
	env := f.env()

	t.Run("present bool", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 700), whereExprOf(t, "MATCH (n) WHERE n.flag RETURN n"))
		if err != nil || got != TriTrue {
			t.Fatalf("got %s, %v, want TriTrue", got, err)
		}
	})

	t.Run("absent property is null", func(t *testing.T) {
		got, err := EvalPredicate(env, f.row("n", 100), whereExprOf(t, "MATCH (n) WHERE n.flag RETURN n"))
		if err != nil || got != TriNull {
			t.Fatalf("got %s, %v, want TriNull", got, err)
		}
	})

	t.Run("present non-bool bails with ErrRuntimeCast", func(t *testing.T) {
		_, err := EvalPredicate(env, f.row("n", 750), whereExprOf(t, "MATCH (n) WHERE n.flag RETURN n"))
		if !errors.Is(err, ErrRuntimeCast) {
			t.Fatalf("err = %v, want ErrRuntimeCast", err)
		}
	})
}
