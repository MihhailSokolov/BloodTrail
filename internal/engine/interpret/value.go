// SPDX-License-Identifier: Apache-2.0

// Package interpret evaluates Cypher expressions and predicates directly
// over an in-memory snapshot, without going through DAWGS' pgsql query
// translator. Its value model is the same "post-JSON" encoding
// snapshot.PropStore already returns for a property lookup: exactly one of
// nil (absent-or-JSON-null; a companion ok/present bool distinguishes the
// two), string, float64, bool, []any, or map[string]any -- the shapes
// encoding/json's default Unmarshal into `any` produces.
//
// The functions in this file are not "a reasonable interpretation of
// Cypher" -- they are a deliberate, bit-for-bit port of how DAWGS' pgsql
// driver (github.com/specterops/dawgs@v0.8.0) translates predicates over
// JSONB properties into SQL, including its surprising three-valued-logic
// corners (a stored bool never equals a string literal; a missing property
// makes a negated string predicate TRUE, not NULL; JSON null sorts last in
// every ORDER BY; string ordering is left to the database's collation and
// is refused here). Interpreting a query locally must reproduce exactly
// what pg would have returned, or the query has to be delegated back to pg
// -- so every rule below is cited against the DAWGS source it mirrors.
package interpret

import (
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Tri is a SQL three-valued-logic boolean: true, false, or null (unknown).
// Cypher WHERE clauses are boolean expressions built from JSONB property
// comparisons, and pg's NULL propagation through AND/OR/NOT is part of the
// observable semantics we must reproduce -- so Tri, not Go bool, is the
// result type for every predicate in this package.
type Tri int8

const (
	TriFalse Tri = iota
	TriTrue
	TriNull
)

// String renders a Tri for test failure messages and debugging.
func (t Tri) String() string {
	switch t {
	case TriFalse:
		return "false"
	case TriTrue:
		return "true"
	case TriNull:
		return "null"
	default:
		return "invalid"
	}
}

// And implements SQL's three-valued AND: FALSE dominates (FALSE AND NULL is
// FALSE, not NULL), otherwise NULL is sticky.
func (t Tri) And(o Tri) Tri {
	if t == TriFalse || o == TriFalse {
		return TriFalse
	}
	if t == TriNull || o == TriNull {
		return TriNull
	}
	return TriTrue
}

// Or implements SQL's three-valued OR: TRUE dominates, otherwise NULL is
// sticky.
func (t Tri) Or(o Tri) Tri {
	if t == TriTrue || o == TriTrue {
		return TriTrue
	}
	if t == TriNull || o == TriNull {
		return TriNull
	}
	return TriFalse
}

// Not implements SQL's three-valued NOT: NULL negates to NULL.
func (t Tri) Not() Tri {
	switch t {
	case TriFalse:
		return TriTrue
	case TriTrue:
		return TriFalse
	default:
		return TriNull
	}
}

// Xor is boolean inequality (!=) lowered from Cypher's XOR, NULL-propagating
// like every other SQL comparison: if either side is unknown, so is the
// result.
func (t Tri) Xor(o Tri) Tri {
	if t == TriNull || o == TriNull {
		return TriNull
	}
	if t == o {
		return TriFalse
	}
	return TriTrue
}

// boolToTri converts a definite Go boolean result into a Tri. It is never
// used for a case that might need TriNull -- callers decide absence/nullness
// before reaching for this helper.
func boolToTri(b bool) Tri {
	if b {
		return TriTrue
	}
	return TriFalse
}

// Sentinel errors returned by this package. Both signal "don't answer this
// locally" to the caller: ErrCollation means the answer depends on the
// database's collation (string ordering), and ErrRuntimeCast means pg would
// raise a genuine runtime cast error that we must reproduce, not swallow.
// A production query hitting either bails out to delegation to the real pg
// driver rather than serving a locally-computed (and potentially wrong)
// answer.
var (
	ErrCollation   = errors.New("interpret: collation-dependent comparison")
	ErrRuntimeCast = errors.New("interpret: runtime cast")

	// ErrNotComparable is returned by OrderCompare for any pair of operand
	// types that are neither both numbers nor both strings (e.g. a type
	// mismatch, or either operand being a bool/array/object/JSON-null).
	// Like the two above it declines the query. It once meant a Cypher NULL
	// -- Cypher's relational operators are defined only over like-typed
	// scalars -- but PostgreSQL does not answer NULL for such a pair: dawgs
	// casts the property to the other side's type, which raises an error on
	// a mismatch (castPropertyForOrder reproduces that), or compares two
	// jsonb values by type rank. No served shape reaches OrderCompare with
	// one (relationalComparisonSafe), so it marks a comparison this package
	// has no answer for.
	ErrNotComparable = errors.New("interpret: not comparable")
)

// StringEq implements Cypher/pg string equality's jsonb_typeof guard. DAWGS
// translates `n.prop = 'literal'` (where 'literal' typechecks as a string)
// into:
//
//	jsonb_typeof(n.properties -> 'prop') = 'string' and (n.properties ->> 'prop') = 'literal'
//
// (see dawgs@v0.8.0 cypher/models/pgsql/translate/predicate_test.go and
// expression_test.go, e.g. the `objectid`/`distinguishedname` cases). SQL
// AND does not short-circuit: both sides evaluate. A missing property makes
// `n.properties -> 'prop'` SQL NULL, so jsonb_typeof(NULL) is NULL and the
// AND of NULL with (also NULL) ->> comparison is NULL. A *present*
// non-string value makes the typeof guard a definite FALSE, and FALSE AND
// anything is FALSE (never NULL) even though the right-hand ->> comparison
// on a non-string still produces some text -- this is why a stored bool or
// number never equals a string literal, full stop, rather than sometimes
// coincidentally matching its own text rendering.
func StringEq(val any, ok bool, lit string) Tri {
	if !ok {
		return TriNull
	}
	s, isString := val.(string)
	if !isString {
		return TriFalse
	}
	return boolToTri(s == lit)
}

// StringNeq implements pg's two-branch translation of `n.prop <> 'literal'`:
//
//	jsonb_typeof(p) = 'string' and (p ->> 'k') <> 'literal'
//	  or
//	jsonb_typeof(p) <> 'string' and (p -> 'k') <> to_jsonb(('literal')::text)::jsonb
//
// (dawgs@v0.8.0 .../translate/expression_test.go, the `rank <> '1'` case).
// A missing property makes every sub-term NULL, so both branches are NULL
// and NULL OR NULL is NULL. A string value takes the first branch (plain
// text <>). Any other *present* value (including a present JSON null) takes
// the second branch: it is never structurally equal to a JSON string, so
// the branch is unconditionally TRUE -- a non-string value is always
// considered "not equal" to a string literal.
func StringNeq(val any, ok bool, lit string) Tri {
	if !ok {
		return TriNull
	}
	s, isString := val.(string)
	if !isString {
		return TriTrue
	}
	return boolToTri(s != lit)
}

// ScalarEq is raw JSONB equality (`n.prop = <literal>` for a non-string
// literal, and the equality half of property-vs-property comparisons):
// numbers compare numerically, booleans compare as booleans, and any
// type mismatch (including string vs. number) is a definite FALSE rather
// than NULL -- pg's jsonb `=` operator is total over present values. A
// missing property is the only source of NULL here.
func ScalarEq(val any, ok bool, rhs any) Tri {
	if !ok {
		return TriNull
	}
	return boolToTri(jsonbEqual(val, rhs))
}

// PropEq is ScalarEq generalized to two property lookups: structural deep
// equality when both sides are present, NULL if either is absent.
func PropEq(a any, aok bool, b any, bok bool) Tri {
	if !aok || !bok {
		return TriNull
	}
	return boolToTri(jsonbEqual(a, b))
}

// jsonbEqual is raw postgres jsonb `=` semantics over the decoded post-JSON
// value model: numbers compare as float64 (so 1234 and 1234.0 are equal,
// matching numeric's scale-insensitive equality), object key order is
// irrelevant (Go maps already have no order), but array element order and
// length matter. A JSON null is represented as untyped nil and only equals
// another JSON null.
func jsonbEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	switch av := a.(type) {
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonbEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			other, present := bv[k]
			if !present || !jsonbEqual(v, other) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// jsonText mirrors PostgreSQL's `->>` text extraction over the decoded
// post-JSON value model, for the scalars it can render: a string extracts to
// itself, a boolean to 'true'/'false', and a number to its shortest decimal
// form -- which is jsonb's own rendering for a number stored the way Go's
// encoding/json (and so BloodHound's ingest) writes it; jsonb keeps whatever
// spelling was stored, and a float64 cannot say whether that was 1 or 1.0.
// A JSON null (val == nil; PropStore.Value reports it present, with a nil
// value) extracts to SQL NULL, reported as (_, false).
//
// A list or an object is reported as (_, false) too, but it is not NULL:
// `->>` renders it as jsonb's JSON text (key order, spacing and number
// formatting included), which this package does not reproduce. Every caller
// therefore handles that case itself -- evalCoalesce and inTextElements
// decline, and inCastProperty reasons only about the rendering's first
// byte.
func jsonText(val any) (string, bool) {
	switch v := val.(type) {
	case nil:
		return "", false
	case string:
		return v, true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(v), true
	default:
		return "", false
	}
}

// StringOp identifies which Cypher string predicate to evaluate.
type StringOp uint8

const (
	OpStartsWith StringOp = iota
	OpEndsWith
	OpContains
	OpRegex
)

// StringPredicate evaluates STARTS WITH / ENDS WITH / CONTAINS / a regex
// match, positive or negated.
//
// Absent (ok=false) and present-JSON-null (val=nil, ok=true) behave
// identically in both forms, because pg's `->>` extraction of either is SQL
// NULL: positive form propagates that to TriNull. Negated form -- which
// evalNegation asks for only where dawgs rewrites the negation, a plain
// property under STARTS WITH/ENDS WITH/CONTAINS -- coalesces
// the NULL extraction to the empty string first (`coalesce(x ->> ..., ”)`)
// and runs the *positive* test against "" before inverting -- so the
// result is whatever the positive match against the empty string is, then
// flipped, not an unconditional constant. For STARTS WITH/ENDS
// WITH/CONTAINS with a non-empty needle this is always TriTrue (the empty
// string never starts with/ends with/contains a non-empty needle), which
// is the common case DAWGS' rewrite is aimed at -- but a regex needle that
// itself matches the empty string (e.g. `^.*$`) still inverts to TriFalse,
// exactly as coalesce-then-match-then-invert requires. (`NOT x STARTS WITH
// y` cannot be plain `NOT(x STARTS WITH y)` at the SQL level, since
// negating a NULL is still NULL and Cypher wants "no value" to satisfy the
// negation -- this is why DAWGS rewrites through coalesce instead.)
//
// A present *string* value is compared directly (or, negated, compared then
// inverted) -- no NULL or cast concern.
//
// A present *non-string, non-null* value (a number, bool, array, or object)
// still produces non-NULL text under pg's `->>` (its own JSON text
// rendering), so pg's LIKE/regex match runs against that rendering in both
// the positive and the coalesce-negated form. This package will not
// reproduce PostgreSQL's numeric/boolean text rendering in Go -- the
// renderings can diverge (e.g. numeric formatting edge cases), and a
// plausible-looking Go rendering that happens to disagree with pg would
// silently produce the wrong match/no-match rather than a loud error. Both
// forms therefore return ErrRuntimeCast for this case, so the caller bails
// the whole query to delegation and pg computes the real answer.
func StringPredicate(op StringOp, val any, ok bool, needle string, negated bool) (Tri, error) {
	return stringPredicateCore(func(s string) bool {
		return matchString(op, s, needle)
	}, val, ok, negated)
}

// RegexPredicate is StringPredicate's OpRegex case, parameterized over a
// pre-compiled *regexp.Regexp instead of a needle string that would be
// recompiled via regexp.Compile on every call (matchString's approach for
// OpRegex). It exists for callers -- currently eval.go's regex compilation
// cache, which amortizes compilation across an entire query's row loop --
// that already hold a compiled pattern and would otherwise have to either
// recompile it just to call StringPredicate, or reimplement the
// coalesce-then-match-then-invert policy themselves. re may be nil (a
// pattern that failed to compile): matched then treats every input as
// no-match, exactly like matchString's own compile-failure fallback,
// instead of panicking.
//
// This shares stringPredicateCore with StringPredicate, so the two can never
// drift on the null/absent/non-string/negation policy -- see that function's
// doc comment, and StringPredicate's, for the full three-valued-logic
// rationale (both apply here unchanged; only how "does s match" is decided
// differs).
func RegexPredicate(re *regexp.Regexp, val any, ok bool, negated bool) (Tri, error) {
	return stringPredicateCore(func(s string) bool {
		return re != nil && re.MatchString(s)
	}, val, ok, negated)
}

// RegexMatcherPredicate is RegexPredicate over a RegexMatcher, which answers
// the identical question and does it with a substring search when the pattern
// allows one (see RegexMatcher). m may be nil -- a pattern that failed to
// compile -- and then treats every input as no-match, exactly as
// RegexPredicate does for a nil *regexp.Regexp.
func RegexMatcherPredicate(m *RegexMatcher, val any, ok bool, negated bool) (Tri, error) {
	return stringPredicateCore(func(s string) bool {
		return m.MatchString(s)
	}, val, ok, negated)
}

// stringPredicateCore implements the coalesce-then-match-then-invert
// three-valued semantics shared by StringPredicate (STARTS WITH/ENDS
// WITH/CONTAINS/regex-by-needle-string) and RegexPredicate
// (regex-by-pre-compiled-matcher): absent/JSON-null propagates to TriNull
// unless negated, in which case the *positive* match against "" is computed
// and then inverted; a present non-string value is always ErrRuntimeCast
// (this package never reproduces pg's JSON-text rendering of numbers/
// bools/arrays/objects -- see StringPredicate's doc comment); a present
// string is matched (and, if negated, inverted). match is called with the
// coalesced-to-empty-string operand in the absent/null case and the actual
// string operand otherwise -- it never sees a non-string value. Holding this
// policy in exactly one place is the point: StringPredicate and
// RegexPredicate differ only in how they decide "does s match", never in
// what happens around that decision.
func stringPredicateCore(match func(s string) bool, val any, ok bool, negated bool) (Tri, error) {
	if !ok || val == nil {
		if !negated {
			return TriNull, nil
		}
		return boolToTri(!match("")), nil
	}
	s, isString := val.(string)
	if !isString {
		return TriFalse, ErrRuntimeCast
	}
	matched := match(s)
	if negated {
		matched = !matched
	}
	return boolToTri(matched), nil
}

// matchString runs the case-sensitive positive test for op. For OpRegex the
// pattern is compiled on every call: hoisting that compile to plan time
// (compile once, evaluate per row) is a performance concern for a future
// plan-execution loop, not a change to this function's observable result --
// MatchString's answer does not depend on when the pattern was compiled. A pattern that fails to
// compile is treated as no-match; Cypher regex literals are expected to be
// validated before a query reaches interpretation.
func matchString(op StringOp, s, needle string) bool {
	switch op {
	case OpStartsWith:
		return strings.HasPrefix(s, needle)
	case OpEndsWith:
		return strings.HasSuffix(s, needle)
	case OpContains:
		return strings.Contains(s, needle)
	case OpRegex:
		re, err := regexp.Compile(goRegexFor(needle))
		if err != nil {
			return false
		}
		return re.MatchString(s)
	default:
		return false
	}
}

// IsNull reports whether a property is IS NULL under Cypher semantics: a
// missing key and a present JSON null are indistinguishable to Cypher (both
// mean "no value"), so both are TriTrue. Unlike every other predicate in
// this file, IS NULL is total -- it never returns TriNull itself.
func IsNull(val any, ok bool) Tri {
	if !ok || val == nil {
		return TriTrue
	}
	return TriFalse
}

// IsNotNull is IsNull's inverse.
func IsNotNull(val any, ok bool) Tri {
	return IsNull(val, ok).Not()
}

// In evaluates Cypher's `val IN list` for a left-hand side that is NOT a
// plain property -- id(), size(), toLower(), arithmetic, a literal: a value
// with a SQL type of its own, which checkInOperands has already matched
// against the list literal's element type. A plain property is cast to that
// element type instead, the way dawgs translates it; see inCastProperty.
//
// Note on signature: the exact return type is (Tri, error) rather than a
// bare Tri, which a purely three-valued predicate would suggest, so the
// numeric branch can report ErrRuntimeCast for a value that is not a number
// -- the Tri returned alongside a non-nil error carries no meaning and
// should be ignored.
//
// Rules, in order:
//   - an empty list is always FALSE, even for a missing/NULL left-hand
//     side (dawgs lowers `x IN []` to the constant false).
//   - a left-hand side that is itself a list is always FALSE (Cypher does
//     not flatten nested lists for IN).
//   - a missing (or present JSON-null) left-hand side against a non-empty
//     list is NULL.
//   - otherwise a string list compares the value's text, and a numeric
//     list compares it as a number (ErrRuntimeCast when it is not one).
func In(val any, ok bool, list []any) (Tri, error) {
	if len(list) == 0 {
		return TriFalse, nil
	}
	if _, isList := val.([]any); isList {
		return TriFalse, nil
	}
	if !ok {
		return TriNull, nil
	}

	allString, allNumber := listElementKinds(list)

	switch {
	case allString:
		text, present := jsonText(val)
		if !present {
			return TriNull, nil
		}
		for _, el := range list {
			if text == el.(string) {
				return TriTrue, nil
			}
		}
		return TriFalse, nil

	case allNumber:
		text, present := jsonText(val)
		if !present {
			return TriNull, nil
		}
		n, err := parseFloat8Text(text)
		if err != nil {
			return TriFalse, err
		}
		for _, el := range list {
			if n == el.(float64) {
				return TriTrue, nil
			}
		}
		return TriFalse, nil

	default:
		// A mixed-type list literal (e.g. IN [1, 'a']) does not correspond
		// to any single pg array type, so DAWGS' planner would not produce
		// this shape for a locally-interpretable query. Fall back to raw
		// structural equality per element as the closest safe
		// approximation rather than guessing at a pg rewrite that doesn't
		// exist.
		for _, el := range list {
			if jsonbEqual(val, el) {
				return TriTrue, nil
			}
		}
		return TriFalse, nil
	}
}

// listElementKinds reports whether every element of list is a string, or
// every element is a number (float64). Both are false for an empty,
// mixed, or otherwise-typed (e.g. bool) list.
func listElementKinds(list []any) (allString, allNumber bool) {
	allString, allNumber = true, true
	for _, el := range list {
		switch el.(type) {
		case string:
			allNumber = false
		case float64:
			allString = false
		default:
			allString, allNumber = false, false
		}
	}
	return allString, allNumber
}

// typeRank orders JSON types the way DAWGS' cypher_jsonb_type_rank SQL
// function does (dawgs@v0.8.0
// drivers/pg/query/sql/schema_up.sql:375-392): object < array < string <
// boolean < number. JSON null ranks last there too (6), but Compare never
// consults typeRank for a nil operand -- it special-cases null before
// reaching the rank comparison, mirroring the SQL function's own caller
// (cypher_value_compare) doing the same.
func typeRank(v any) int {
	switch v.(type) {
	case map[string]any:
		return 1
	case []any:
		return 2
	case string:
		return 3
	case bool:
		return 4
	case float64:
		return 5
	default:
		return 6
	}
}

// Compare is a port of DAWGS' public.cypher_value_compare plpgsql function
// (dawgs@v0.8.0 drivers/pg/query/sql/schema_up.sql:394-547), used to order
// ORDER BY results and MIN/MAX aggregates over arbitrary JSONB values. Its
// structure follows the SQL line for line:
//
//  1. raw equality short-circuits to 0 (this subsumes both-null: 'null' =
//     'null' is true, so two JSON nulls are already equal here).
//  2. left null sorts after right (return 1); right null sorts after left
//     (return -1) -- JSON null sorts last, unconditionally, regardless of
//     what it's being compared against.
//  3. otherwise, a type mismatch is decided purely by typeRank.
//  4. same-type values compare per-type: numbers numerically, strings
//     lexically (but see below), booleans false<true, arrays element-wise
//     then by length, objects by key count then sorted key names then
//     values.
//
// String-vs-string is the one case Compare refuses to answer locally:
// pg's `<` over text depends on the database's collation, which this
// package has no access to and must not guess at (guessing wrong would
// silently corrupt result ordering). It returns ErrCollation instead,
// which the caller is expected to treat as "delegate this query to pg".
func Compare(a, b any) (int, error) {
	if jsonbEqual(a, b) {
		return 0, nil
	}
	if a == nil {
		return 1, nil
	}
	if b == nil {
		return -1, nil
	}

	aRank, bRank := typeRank(a), typeRank(b)
	if aRank != bRank {
		if aRank < bRank {
			return -1, nil
		}
		return 1, nil
	}

	switch av := a.(type) {
	case float64:
		return compareFloat(av, b.(float64)), nil

	case string:
		return 0, ErrCollation

	case bool:
		bv := b.(bool)
		switch {
		case av == bv:
			return 0, nil
		case !av && bv:
			return -1, nil
		default:
			return 1, nil
		}

	case []any:
		bv := b.([]any)
		n := len(av)
		if len(bv) < n {
			n = len(bv)
		}
		for i := 0; i < n; i++ {
			c, err := Compare(av[i], bv[i])
			if err != nil {
				return 0, err
			}
			if c != 0 {
				return c, nil
			}
		}
		return compareInt(len(av), len(bv)), nil

	case map[string]any:
		bv := b.(map[string]any)
		if len(av) != len(bv) {
			return compareInt(len(av), len(bv)), nil
		}

		aKeys, bKeys := sortedKeys(av), sortedKeys(bv)
		for i := range aKeys {
			if aKeys[i] != bKeys[i] {
				return compareString(aKeys[i], bKeys[i]), nil
			}
		}
		for i := range aKeys {
			c, err := Compare(av[aKeys[i]], bv[bKeys[i]])
			if err != nil {
				return 0, err
			}
			if c != 0 {
				return c, nil
			}
		}
		return 0, nil

	default:
		// Unreachable: typeRank places every value this package's value
		// model produces (map[string]any, []any, string, bool, float64)
		// into ranks 1-5, and aRank == bRank is guaranteed above (a mismatch
		// already returned), so a and b are one of the five types the cases
		// above already handle. Kept, like typeRank's own default, only so
		// the port stays visibly total against the SQL source's own
		// case/else structure.
		return 0, nil
	}
}

// compareFloat orders two float64s numerically. NaN and +/-Inf never need a
// defined answer here: every float64 in this package's value model was
// decoded by encoding/json's default Unmarshal into `any` (see the package
// doc comment), and JSON's number grammar has no literal for NaN or
// infinity, so encoding/json cannot produce either -- these values are
// unreachable, not merely assumed absent.
func compareFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// compareString is only ever used to order object key *names* (never
// property string values), which is a Go-native comparison over identifiers
// DAWGS' plpgsql performs with plain `<`/`>` on `text[]` array elements
// already sorted by `array_agg(key order by key)` -- i.e. under whatever
// collation `ORDER BY key` used to build that array, which for a fixed
// system/database collation is unambiguous. This is deliberately not routed
// through ErrCollation: object key comparison is an internal bookkeeping
// step to align two objects' keys, not a user-observable string ordering
// predicate.
func compareString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// OrderCompare is Compare's counterpart for the scalar relational operators
// (<, >, <=, >=) over two values the caller has already cast the way
// PostgreSQL does (evalOrder, castPropertyForOrder). Numbers compare
// numerically; strings are refused with ErrCollation for the same reason as
// Compare. Every other combination (a type mismatch, a bool, an array, an
// object, or a JSON null on either side) returns ErrNotComparable, which
// declines the query -- see its doc for why that is not a NULL.
func OrderCompare(a, b any) (int, error) {
	if af, aok := a.(float64); aok {
		if bf, bok := b.(float64); bok {
			return compareFloat(af, bf), nil
		}
	}
	if _, aok := a.(string); aok {
		if _, bok := b.(string); bok {
			return 0, ErrCollation
		}
	}
	return 0, ErrNotComparable
}

// likePattern is a PostgreSQL LIKE pattern split into its parts: literal
// runs of text and the wildcards between them, % (any sequence) and _ (any
// one character), with the default escape character, backslash, already
// applied -- `a\_b%` is the literal run "a_b" and a trailing %.
type likePattern struct {
	parts []likePart
}

type likePart struct {
	literal  string // the run's text when wildcard is 0
	wildcard byte   // '%' or '_', or 0 for a literal run
}

// parseLikePattern splits pattern the way PostgreSQL's LIKE reads it. A
// pattern that ends in an unpaired escape is an error there ("LIKE pattern
// must not end with escape character"), and so here.
func parseLikePattern(pattern string) (likePattern, error) {
	var (
		lp  likePattern
		run strings.Builder
	)
	flush := func() {
		if run.Len() > 0 {
			lp.parts = append(lp.parts, likePart{literal: run.String()})
			run.Reset()
		}
	}
	for i := 0; i < len(pattern); i++ {
		switch c := pattern[i]; c {
		case '\\':
			if i+1 == len(pattern) {
				return likePattern{}, ErrLikePattern
			}
			i++
			run.WriteByte(pattern[i])
		case '%', '_':
			flush()
			lp.parts = append(lp.parts, likePart{wildcard: c})
		default:
			run.WriteByte(c)
		}
	}
	flush()
	return lp, nil
}

// ErrLikePattern is PostgreSQL's error for a LIKE pattern that ends in the
// escape character. Like ErrRuntimeCast it declines the query: pg raises it
// before returning a row.
var ErrLikePattern = errors.New("interpret: LIKE pattern ends with the escape character")

// regexp compiles the pattern to an anchored Go regexp that matches exactly
// the strings PostgreSQL's (case-sensitive) LIKE does. _ is one character,
// not one byte, and matches a newline too, as it does in pg.
func (lp likePattern) regexp() *regexp.Regexp {
	var b strings.Builder
	b.WriteString(`(?s)\A`)
	for _, p := range lp.parts {
		switch p.wildcard {
		case '%':
			b.WriteString(`.*`)
		case '_':
			b.WriteString(`.`)
		default:
			b.WriteString(regexp.QuoteMeta(p.literal))
		}
	}
	b.WriteString(`\z`)
	return regexp.MustCompile(b.String())
}

// leadingLiteral, trailingLiteral and longestLiteral return a literal run
// every string matching the pattern must begin with, end with, or contain --
// the text a string index can narrow on -- and false when the pattern has
// none in that position.
func (lp likePattern) leadingLiteral() (string, bool) {
	if len(lp.parts) == 0 || lp.parts[0].wildcard != 0 {
		return "", false
	}
	return lp.parts[0].literal, true
}

func (lp likePattern) trailingLiteral() (string, bool) {
	if len(lp.parts) == 0 || lp.parts[len(lp.parts)-1].wildcard != 0 {
		return "", false
	}
	return lp.parts[len(lp.parts)-1].literal, true
}

func (lp likePattern) longestLiteral() (string, bool) {
	best := ""
	for _, p := range lp.parts {
		if p.wildcard == 0 && len(p.literal) > len(best) {
			best = p.literal
		}
	}
	return best, best != ""
}

// likePatternFor is the LIKE pattern dawgs builds for a string predicate
// whose literal needle it does not escape (plan.go's likeNeedleServed):
// the needle as written, with % appended, prepended, or both.
func likePatternFor(op StringOp, needle string) string {
	switch op {
	case OpStartsWith:
		return needle + "%"
	case OpEndsWith:
		return "%" + needle
	default:
		return "%" + needle + "%"
	}
}
