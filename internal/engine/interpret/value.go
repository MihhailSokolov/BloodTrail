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
	// Unlike ErrCollation and ErrRuntimeCast this is not a delegation
	// signal: Cypher's scalar relational operators (<, >, <=, >=) are only
	// defined over like-typed orderable scalars, so a type mismatch is a
	// genuine, locally-answerable Cypher NULL. It is exported (rather than
	// folded into a bare non-nil error) precisely so callers can tell the
	// two apart with errors.Is: ErrCollation must delegate, ErrNotComparable
	// must become TriNull.
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

// jsonText mirrors postgres' `->>` text-extraction operator over the
// decoded post-JSON value model: a JSON string extracts to itself, a
// number/bool extracts to its textual JSON rendering, and a JSON null
// (represented here as ok=false, matching PropStore.Value's own contract
// that absence and JSON null are only distinguished by the caller's ok/present
// flag) extracts to SQL NULL -- reported as (_, false). This is the
// extraction pg's IN-list and negated-string-predicate rewrites both key
// off of.
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
		// Arrays/objects are never valid ->> operands in the predicates this
		// package evaluates (IN's LHS-is-a-list case is rejected before
		// jsonText is ever reached; see In below), so this path is not
		// expected to be exercised. Included only so the function is total.
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
// Positive form: DAWGS translates these into a LIKE (or a regex match)
// directly against the property, which is only well-typed for a string
// property; a missing or non-string property yields Cypher NULL, matching
// the same "predicates over properties are only defined for matching JSON
// types" spirit as StringEq's typeof guard.
//
// Negated form: `NOT x STARTS WITH y` is not simply `NOT(x STARTS WITH y)`
// at the SQL level, because negating a NULL is still NULL and Cypher wants
// "no value" to satisfy the negation. DAWGS' rewrite instead runs the
// positive test against coalesce(x ->> ..., ”) and negates that: a missing
// property (or a present JSON null, since ->> on JSON null is also SQL
// NULL) coalesces to the empty string, which never starts with, ends with,
// or contains a non-empty needle, so the negation is unconditionally TRUE.
// A present non-string value coalesces to nothing (its ->> rendering is
// already non-NULL text), so it is compared by its own text rendering,
// consistent with the coalesce being a no-op whenever the extraction wasn't
// NULL to begin with.
func StringPredicate(op StringOp, val any, ok bool, needle string, negated bool) Tri {
	if negated {
		text, present := jsonText(val)
		if !ok || !present {
			text = ""
		}
		return boolToTri(!matchString(op, text, needle))
	}

	if !ok {
		return TriNull
	}
	s, isString := val.(string)
	if !isString {
		return TriNull
	}
	return boolToTri(matchString(op, s, needle))
}

// matchString runs the case-sensitive positive test for op. For OpRegex the
// pattern is compiled on every call: the brief's note that "regex is
// compiled by the caller (plan-time)" is a performance concern for a future
// milestone's plan-execution loop (compile once, evaluate per row), not a
// change to this function's observable result -- MatchString's answer does
// not depend on when the pattern was compiled. A pattern that fails to
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
		re, err := regexp.Compile(needle)
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

// In evaluates Cypher's `val IN list`.
//
// Note on signature: the exact return type is (Tri, error) rather than a
// bare Tri, even though the brief's header line elides the error. The
// numeric-list branch below must be able to signal ErrRuntimeCast (a
// non-numeric property text failing pg's `::int8`/`::float8` cast), and
// that can only reach the caller through a second return value -- the Tri
// returned alongside a non-nil error carries no meaning and should be
// ignored.
//
// Rules, in order:
//   - an empty list is always FALSE, even for a missing/NULL left-hand
//     side (pg: `x = ANY('{}')` is FALSE, not NULL, regardless of x).
//   - a left-hand side that is itself a list is always FALSE (Cypher does
//     not flatten nested lists for IN).
//   - a missing (or present JSON-null) left-hand side against a non-empty
//     list is NULL.
//   - otherwise, list membership is decided by how DAWGS types the list at
//     plan time. A list of Cypher string literals becomes a pg text[], and
//     membership is `(p ->> 'k') = ANY(text[])` -- the property's ->> text
//     extraction, so a non-string property (number/bool) still matches by
//     its textual rendering. A list of Cypher numeric literals becomes an
//     int8[]/float8[], and membership is `(p ->> 'k')::int8 = ANY(...)` --
//     casting the text extraction to a number, which raises a genuine
//     runtime error in pg if the text isn't numeric; we reproduce that as
//     ErrRuntimeCast rather than silently returning FALSE, so the caller
//     can bail the whole query to delegation.
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
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return TriFalse, ErrRuntimeCast
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
		return 0, nil
	}
}

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
// (<, >, <=, >=), which in Cypher are only ever defined over two
// like-typed, orderable scalars -- unlike ORDER BY, there is no cross-type
// total order to fall back on. Numbers compare numerically; strings are
// refused with ErrCollation for the same reason as Compare. Every other
// combination (a type mismatch, a bool, an array, an object, or a JSON
// null on either side) is not a type pg's `<`/`>` defines over JSONB in a
// collation-independent way, or is not orderable at all in Cypher -- those
// return ErrNotComparable, which the caller (holding the ok/present flags
// this function does not see) is expected to turn into TriNull rather than
// delegate.
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
