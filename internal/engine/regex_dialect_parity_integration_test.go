// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"testing"
)

// TestTryCypherRegexDialectMatchesOracle compares `=~` patterns that Go's
// RE2 and PostgreSQL's regex engine read differently, and that
// PgRegexCompatible used to admit, with PostgreSQL:
//
//   - POSIX bracket expressions: [[:alpha:]] is ASCII-only in Go and
//     locale-aware in PostgreSQL (it matches 'José'); [[.a.]] and [[=a=]]
//     are collating elements there and bracket soup here.
//   - Bounds: PostgreSQL rejects a count above 255 and an unbalanced brace
//     (`a{1`), which Go reads as a literal; `a{01}` is a bound there and a
//     literal here.
//   - Case folding: under (?i) Go matches U+212A KELVIN SIGN to k and
//     U+017F LONG S to s (its Unicode fold orbits), and so leaves them out
//     of a negated class; PostgreSQL compares each character with the
//     pattern character's own upper and lower case only.
//
// The pre-built corpus's (?i) patterns and ordinary bounds stay served.
func TestTryCypherRegexDialectMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"RxPosix", map[string]any{"name": "José"}},
		{"RxPosix", map[string]any{"name": "a"}},
		{"RxPosix", map[string]any{"name": "a{1"}},
		{"RxPosix", map[string]any{"name": "aaa"}},
		{"RxFold", map[string]any{"name": "K"}},
		{"RxFold", map[string]any{"name": "k"}},
		{"RxFold", map[string]any{"name": "\u212a"}},
		{"RxFold", map[string]any{"name": "\u017f"}},
		{"RxFold", map[string]any{"name": "s"}},
		{"RxFold", map[string]any{"name": "Windows 2003 Server"}},
		{"RxFold", map[string]any{"name": "Window\u017f 2003 Server"}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:RxPosix) WHERE n.name =~ '^[[:alpha:]]+$' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ '[[:alpha:]][[:alpha:]][[:alpha:]][[:alpha:]]' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ '^[[.a.]]$' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ '^[[=a=]]$' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ '^[a[:digit:]]$' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ 'a{256}' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ 'a{1' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ 'a{1,' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ 'a{1,2' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ '^a{01}$' RETURN n`, false},
		{`MATCH (n:RxPosix) WHERE n.name =~ '(?i)^JOSÉ$' RETURN n`, false},
		{"MATCH (n:RxFold) WHERE n.name =~ '(?i)^\u212a$' RETURN n", false},

		{`MATCH (n:RxFold) WHERE n.name =~ '(?i)k' RETURN n`, true},
		{`MATCH (n:RxFold) WHERE n.name =~ '(?i).*K.*' RETURN n`, true},
		{`MATCH (n:RxFold) WHERE n.name =~ '(?i)^[^k]$' RETURN n`, true},
		{`MATCH (n:RxFold) WHERE n.name =~ '(?i)^[a-z]$' RETURN n`, true},
		{`MATCH (n:RxFold) WHERE n.name =~ '(?i)s' RETURN n`, true},
		{`MATCH (n:RxFold) WHERE NOT n.name =~ '(?i)^[s-t]$' RETURN n`, true},
		{`MATCH (n:RxFold) WHERE n.name =~ '(?i).*Windows.* (2000|2003|2008|2012|xp|vista|7|8|me|nt).*' RETURN n`, true},
		{`MATCH (n:RxPosix) WHERE n.name =~ '^a{2,3}$' RETURN n`, true},
		{`MATCH (n:RxPosix) WHERE n.name =~ '^a{1}$' RETURN n`, true},
		{`MATCH (n:RxPosix) WHERE n.name =~ 'a{' RETURN n`, true},
		{`MATCH (n:RxPosix) WHERE n.name =~ 'a{,3}' RETURN n`, true},
		{`MATCH (n:RxPosix) WHERE n.name =~ 'a{255}' RETURN n`, true},
	})
}

// TestTryCypherRegexQuantifiedAnchorMatchesOracle compares patterns that put
// a quantifier directly after an anchor with PostgreSQL, which rejects them
// ("quantifier operand invalid") while Go's RE2 repeats the empty-width
// assertion and matches. A quantified group around an anchor is valid in both.
func TestTryCypherRegexQuantifiedAnchorMatchesOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, []typedNode{
		{"RxAnchor", map[string]any{"name": "a"}},
		{"RxAnchor", map[string]any{"name": "ba"}},
	})

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (n:RxAnchor) WHERE n.name =~ '^*a' RETURN n`, false},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '$*a' RETURN n`, false},
		{`MATCH (n:RxAnchor) WHERE n.name =~ 'a|^+' RETURN n`, false},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '^?a' RETURN n`, false},
		{`MATCH (n:RxAnchor) WHERE n.name =~ 'a$?' RETURN n`, false},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '^{2}a' RETURN n`, false},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '(?i)^*A' RETURN n`, false},

		{`MATCH (n:RxAnchor) WHERE n.name =~ '^a.*' RETURN n`, true},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '.*a$' RETURN n`, true},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '(^)*a' RETURN n`, true},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '[^*]a' RETURN n`, true},
		{`MATCH (n:RxAnchor) WHERE n.name =~ '^{,2}a' RETURN n`, true},
	})
}
