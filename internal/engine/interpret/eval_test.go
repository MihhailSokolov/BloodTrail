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
		if n, isInt32 := val.(int32); !isInt32 || n != 3 {
			t.Fatalf("size(n.spns) = %#v, want int32(3)", val)
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
