// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"reflect"
	"testing"
)

// TestEvalSplitNullSeparatorSplitsCharacters pins split() with a missing or
// null separator to string_to_array(s, NULL), which dawgs emits for it: the
// string's characters, one element each -- not NULL.
func TestEvalSplitNullSeparatorSplitsCharacters(t *testing.T) {
	f := newFixture(t)
	env := f.env()
	row := f.row("n", 200)

	var want []any
	for _, r := range "AZUREADKERBEROS.TEST.LOCAL" {
		want = append(want, string(r))
	}
	for _, q := range []string{
		"MATCH (n) RETURN split(n.name, n.nosuchproperty)",
		"MATCH (n) RETURN split(n.name, null)",
	} {
		val, ok, err := EvalValue(env, row, returnExprOf(t, q))
		if err != nil || !ok {
			t.Fatalf("%s: err=%v ok=%v", q, err, ok)
		}
		if !reflect.DeepEqual(val, want) {
			t.Fatalf("%s = %#v, want %#v", q, val, want)
		}
	}

	// A missing string is still NULL, whatever the separator.
	val, ok, err := EvalValue(env, row, returnExprOf(t, "MATCH (n) RETURN split(n.nosuchproperty, null)"))
	if err != nil || ok || val != nil {
		t.Fatalf("split(<missing>, null) = %#v, ok=%v, err=%v; want NULL", val, ok, err)
	}
}
