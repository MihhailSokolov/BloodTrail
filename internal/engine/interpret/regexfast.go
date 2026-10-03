// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"regexp"
	"regexp/syntax"
	"strings"
)

// RegexMatcher is a compiled Cypher regex together with, when the pattern
// allows it, a substring plan that answers the identical question far faster.
//
// This exists because a regex predicate is the one shape neither engine has
// an index for -- BloodHound's schema carries no index on `properties` -- so
// both sides scan, and the query reduces to raw per-row matching speed
// against PostgreSQL's C regex engine. Go's regexp is a fine general matcher
// and a poor way to ask "does this string contain SMITH", which is what
// BloodHound's regex predicates overwhelmingly are:
//
//	MATCH (u:User) WHERE u.name =~ '(?i).*SMITH.*'
//	MATCH (n:AZDevice) WHERE n.operatingsystemversion =~ '(10.0.19044|10.0.22000|...)'
//
// Cypher's `=~` is evaluated here with MatchString, which SEARCHES rather
// than anchoring, so `.*X.*` and a bare `X` ask the same question: does s
// contain X. That equivalence is what makes the rewrite safe.
type RegexMatcher struct {
	re *regexp.Regexp

	// literals, when non-empty, are the alternatives whose presence anywhere
	// in the subject is exactly EQUIVALENT to the pattern matching. Held
	// ASCII-lowercased when fold is set.
	literals []string

	// prefilter is a conjunction of requirements: each entry is a set of
	// which at least one member MUST appear for the pattern to match. It is
	// necessary, not sufficient -- subjects failing any entry are rejected
	// without running the engine, and the rest are matched normally.
	//
	// `(10.0.19044|10.0.22000|6.1.7601)` yields one entry: the dots are
	// wildcards so no arm is a plain substring, but every arm requires a
	// literal, and a version carrying none of them cannot match whatever the
	// wildcards do. `(?i).*Windows.* (2000|2003|xp).*` yields three -- the
	// two literals and the version alternation -- and it is that last one
	// that does the filtering, which a single "longest required literal"
	// would have missed by picking "windows".
	prefilter [][]string

	fold bool

	// foldsCase is set when any part of the pattern matches case-
	// insensitively, which decides how a non-ASCII subject is matched (see
	// MatchString).
	foldsCase bool
}

// NewRegexMatcher compiles pattern and derives its substring plan, if any.
// The pattern is compiled as goRegexFor renders it -- with `.` matching a
// newline, as PostgreSQL's does -- so every consumer asks the same question.
func NewRegexMatcher(pattern string) (*RegexMatcher, error) {
	pattern = goRegexFor(pattern)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	m := &RegexMatcher{re: re}
	if parsed, err := syntax.Parse(pattern, syntax.Perl); err == nil {
		m.foldsCase = foldsCase(parsed)
		simplified := parsed.Simplify()
		if lits, fold, ok := searchEquivalentLiterals(simplified); ok {
			m.literals, m.fold = lits, fold
		} else if sets, fold, ok := prefilterSets(simplified); ok {
			m.prefilter, m.fold = sets, fold
		}
	}
	return m, nil
}

// foldsCase reports whether any node of re matches case-insensitively.
func foldsCase(re *syntax.Regexp) bool {
	if re.Flags&syntax.FoldCase != 0 {
		return true
	}
	for _, sub := range re.Sub {
		if foldsCase(sub) {
			return true
		}
	}
	return false
}

// pgFoldSubject prepares a subject for a case-insensitive Go match that must
// answer as PostgreSQL's does. Go folds case along Unicode's simple-fold
// orbits, which give two ASCII letters a non-ASCII member: U+212A KELVIN
// SIGN (k, K) and U+017F LATIN SMALL LETTER LONG S (s, S). PostgreSQL
// compares a character only with the pattern character's own upper and lower
// case, so for it neither rune is a letter of an ASCII pattern: `(?i)k`
// misses the Kelvin sign, and `(?i)[^k]` matches it -- the opposite of Go on
// both. Replacing the two with U+FFFD, which folds onto nothing and occurs in
// no pattern PgRegexCompatible admits under (?i) (those are ASCII), makes Go
// give PostgreSQL's answer for every subject.
func pgFoldSubject(s string) string {
	if !strings.ContainsRune(s, '\u212a') && !strings.ContainsRune(s, '\u017f') {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r == '\u212a' || r == '\u017f' {
			return '\ufffd'
		}
		return r
	}, s)
}

// MatchString reports whether the pattern matches anywhere in s, as
// PostgreSQL's `~` does for the patterns PgRegexCompatible admits: exactly
// as the compiled regex would, except that a case-insensitive pattern
// matches a non-ASCII subject the way PostgreSQL folds case (pgFoldSubject).
func (m *RegexMatcher) MatchString(s string) bool {
	if m == nil || m.re == nil {
		return false
	}
	if m.foldsCase && !isASCII(s) {
		// The substring plans below never decide a non-ASCII subject under
		// case folding anyway; the engine answers it.
		return m.re.MatchString(pgFoldSubject(s))
	}
	if len(m.literals) == 0 {
		for _, required := range m.prefilter {
			if !m.carriesAny(s, required) {
				// A requirement the subject cannot satisfy, so no match is
				// possible however the rest of the pattern behaves.
				return false
			}
		}
		return m.re.MatchString(s)
	}
	if !m.fold {
		return containsAnyLiteral(s, m.literals)
	}
	// Case-insensitive matching is only delegated to the ASCII fast path for
	// an ASCII subject. Go's FoldCase uses Unicode simple folding, under
	// which U+212A KELVIN SIGN and U+017F LATIN SMALL LETTER LONG S fold onto
	// ASCII letters; neither can occur in an all-ASCII string, so for one the
	// two agree exactly, and for anything else the real engine answers.
	if !isASCII(s) {
		return m.re.MatchString(s)
	}
	return containsAnyLiteralFoldASCII(s, m.literals)
}

// searchEquivalentLiterals reports the alternatives whose presence in the
// subject is EQUIVALENT to re matching under search semantics, for the shapes
// where such a set exists.
//
// Recognized, and nothing else:
//
//   - a bare literal, which under a search means "contains it";
//   - a concatenation of `.*`/`(?s).*` around exactly one literal, which asks
//     the same thing (the stars are redundant to a search);
//   - an alternation of the above, which asks it of each arm;
//   - a capture group around any of them.
//
// Every literal must be ASCII when the pattern folds case, and all arms must
// agree on folding -- a mixed-fold alternation would need per-arm handling
// the caller does not have.
func searchEquivalentLiterals(re *syntax.Regexp) (lits []string, fold bool, ok bool) {
	switch re.Op {
	case syntax.OpCapture:
		if len(re.Sub) != 1 {
			return nil, false, false
		}
		return searchEquivalentLiterals(re.Sub[0])

	case syntax.OpLiteral:
		f := re.Flags&syntax.FoldCase != 0
		s := string(re.Rune)
		if f {
			if !isASCII(s) {
				return nil, false, false
			}
			s = strings.ToLower(s)
		}
		return []string{s}, f, true

	case syntax.OpConcat:
		var lit *syntax.Regexp
		for _, sub := range re.Sub {
			if isAnyStar(sub) {
				continue
			}
			if lit != nil {
				// More than one required piece: the presence of any single
				// one is necessary but not sufficient, so there is no
				// equivalent substring test.
				return nil, false, false
			}
			lit = sub
		}
		if lit == nil {
			return nil, false, false
		}
		return searchEquivalentLiterals(lit)

	case syntax.OpAlternate:
		var all []string
		first := true
		for _, sub := range re.Sub {
			subLits, subFold, subOK := searchEquivalentLiterals(sub)
			if !subOK {
				return nil, false, false
			}
			if first {
				fold, first = subFold, false
			} else if subFold != fold {
				return nil, false, false
			}
			all = append(all, subLits...)
		}
		if len(all) == 0 {
			return nil, false, false
		}
		return all, fold, true

	default:
		return nil, false, false
	}
}

// isAnyStar reports whether sub is `.*` in either of its forms -- with or
// without the newline-matching flag. Both are redundant around a literal when
// the match is a search, which is why they can be dropped.
func isAnyStar(sub *syntax.Regexp) bool {
	if sub.Op != syntax.OpStar || len(sub.Sub) != 1 {
		return false
	}
	switch sub.Sub[0].Op {
	case syntax.OpAnyChar, syntax.OpAnyCharNotNL:
		return true
	default:
		return false
	}
}

// carriesAny reports whether s contains any of lits, honouring the matcher's
// fold setting and falling back to a case-sensitive test for a non-ASCII
// subject -- which can only ever admit MORE subjects to the real engine, so a
// prefilter can never reject something that would have matched.
func (m *RegexMatcher) carriesAny(s string, lits []string) bool {
	if !m.fold || !isASCII(s) {
		if m.fold {
			// A non-ASCII subject under case folding: do not attempt to
			// decide it here, let the engine answer.
			return true
		}
		return containsAnyLiteral(s, lits)
	}
	return containsAnyLiteralFoldASCII(s, lits)
}

// prefilterSets returns requirements re cannot match without: a conjunction
// of alternatives, each of which must have at least one member present.
// Unlike searchEquivalentLiterals these are NECESSARY conditions only, so the
// caller still runs the real engine on whatever survives.
//
// A concatenation contributes every requirement of its parts, because all of
// them must be traversed. An alternation contributes ONE requirement -- the
// union of what each arm needs -- and only when every arm needs something,
// since an arm with no requirement can match anything. A repetition or an
// optional group contributes nothing: the pattern matches without it.
func prefilterSets(re *syntax.Regexp) (sets [][]string, fold bool, ok bool) {
	foldSet := false
	seenFold := false

	var walk func(*syntax.Regexp) bool
	walk = func(node *syntax.Regexp) bool {
		switch node.Op {
		case syntax.OpCapture:
			if len(node.Sub) != 1 {
				return true
			}
			return walk(node.Sub[0])

		case syntax.OpConcat:
			for _, sub := range node.Sub {
				if !walk(sub) {
					return false
				}
			}
			return true

		case syntax.OpLiteral:
			lit, f, litOK := literalText(node)
			if !litOK {
				return true // contributes no requirement, which is safe
			}
			if seenFold && f != foldSet {
				return false
			}
			foldSet, seenFold = f, true
			sets = append(sets, []string{lit})
			return true

		case syntax.OpAlternate:
			var union []string
			for _, arm := range node.Sub {
				lit, f, armOK := requiredLiteral(arm)
				if !armOK {
					// One arm requires nothing, so the alternation as a whole
					// requires nothing.
					return true
				}
				if seenFold && f != foldSet {
					return false
				}
				foldSet, seenFold = f, true
				union = append(union, lit)
			}
			if len(union) > 0 {
				sets = append(sets, union)
			}
			return true

		default:
			return true
		}
	}

	if !walk(re) || len(sets) == 0 {
		return nil, false, false
	}
	return sets, foldSet, true
}

// literalText renders an OpLiteral, ASCII-lowercased when it folds case, or
// reports that it cannot be used (a non-ASCII folding literal, or an empty
// one).
func literalText(node *syntax.Regexp) (string, bool, bool) {
	f := node.Flags&syntax.FoldCase != 0
	s := string(node.Rune)
	if s == "" {
		return "", false, false
	}
	if f {
		if !isASCII(s) {
			return "", false, false
		}
		s = strings.ToLower(s)
	}
	return s, f, true
}

// requiredLiteral returns the longest literal re cannot match without, or
// false when it has none. Only pieces that MUST be traversed count: a
// repetition or an optional group contributes nothing, because the pattern
// matches without it.
func requiredLiteral(re *syntax.Regexp) (lit string, fold bool, ok bool) {
	switch re.Op {
	case syntax.OpCapture:
		if len(re.Sub) != 1 {
			return "", false, false
		}
		return requiredLiteral(re.Sub[0])
	case syntax.OpLiteral:
		f := re.Flags&syntax.FoldCase != 0
		s := string(re.Rune)
		if f {
			if !isASCII(s) {
				return "", false, false
			}
			s = strings.ToLower(s)
		}
		if s == "" {
			return "", false, false
		}
		return s, f, true
	case syntax.OpConcat:
		best := ""
		bestFold := false
		for _, sub := range re.Sub {
			s, f, subOK := requiredLiteral(sub)
			if !subOK {
				continue
			}
			if len(s) > len(best) {
				best, bestFold = s, f
			}
		}
		if best == "" {
			return "", false, false
		}
		return best, bestFold, true
	default:
		return "", false, false
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

func containsAnyLiteral(s string, lits []string) bool {
	for _, lit := range lits {
		if strings.Contains(s, lit) {
			return true
		}
	}
	return false
}

// containsAnyLiteralFoldASCII searches s for any of lits, which are already
// ASCII-lowercased, comparing ASCII-case-insensitively and without
// lowercasing s into a fresh allocation per call.
func containsAnyLiteralFoldASCII(s string, lits []string) bool {
	for _, lit := range lits {
		if containsFoldASCII(s, lit) {
			return true
		}
	}
	return false
}

func containsFoldASCII(s, lit string) bool {
	n, m := len(s), len(lit)
	if m == 0 {
		return true
	}
	if m > n {
		return false
	}
	first := lit[0]
	for i := 0; i <= n-m; i++ {
		if asciiLower(s[i]) != first {
			continue
		}
		j := 1
		for j < m && asciiLower(s[i+j]) == lit[j] {
			j++
		}
		if j == m {
			return true
		}
	}
	return false
}

func asciiLower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

// goRegexFor renders a Cypher regex literal as the Go pattern that matches
// what PostgreSQL's `~` matches for it. The one rewrite is the dot: pg's
// advanced regexes are not newline-sensitive by default, so `.` matches
// '\n' there and not in Go unless the s flag is set. Only patterns
// PgRegexCompatible admits reach this; for them, that is the whole
// difference.
func goRegexFor(pattern string) string {
	return "(?s)" + pattern
}

// PgRegexCompatible reports whether pattern means the same thing to Go's
// regexp as it does to PostgreSQL after dawgs translates `x =~ pattern`.
// Declining everything else is what keeps a served regex answer pg's:
//
//   - No backslash at all. dawgs escapes a property-side regex literal for
//     LIKE before handing it to `~` (rewriteStringWildCardLiteral doubles
//     every backslash), so `\.` reaches pg as "a literal backslash, then any
//     character"; and where it is not doubled, pg's escapes differ from
//     Go's anyway (`\b` is a backspace in pg, a word boundary in Go).
//   - No embedded flag group other than one leading `(?i)`. pg accepts
//     options only at the very start, and gives some letters different
//     meanings (`m` is newline-sensitivity there, multi-line here); Go also
//     accepts scoped `(?i:...)` and named groups, which pg rejects. A plain
//     non-capturing `(?:...)` means the same in both.
//   - Under `(?i)`, nothing but ASCII. pg matches a character against the
//     pattern character's own upper and lower case; Go against its whole
//     Unicode fold orbit, which for σ also takes ς, for µ also μ, for the
//     Kelvin sign also K. For an ASCII pattern the only difference is in two
//     subject runes, which RegexMatcher.MatchString handles (pgFoldSubject).
//   - No POSIX bracket construct -- `[:alpha:]`, `[.a.]`, `[=a=]` inside a
//     bracket expression. pg's character classes follow the database locale
//     (`[[:alpha:]]` matches 'é'), Go's are ASCII-only, and Go has no
//     collating elements or equivalence classes at all.
//   - Every bound pg would read as one, read the same way by Go (pgBoundsAgree).
func PgRegexCompatible(pattern string) bool {
	if strings.ContainsRune(pattern, '\\') {
		return false
	}
	rest := strings.TrimPrefix(pattern, "(?i)")
	if len(rest) != len(pattern) && !isASCII(rest) {
		return false
	}
	if !pgBracketsAndBoundsAgree(rest) {
		return false
	}
	for i := strings.Index(rest, "(?"); i >= 0; {
		if i+2 >= len(rest) || rest[i+2] != ':' {
			return false
		}
		next := strings.Index(rest[i+2:], "(?")
		if next < 0 {
			break
		}
		i += 2 + next
	}
	return true
}

// pgMaxRepeat is PostgreSQL's largest repetition count (RE_DUPMAX); a bigger
// one is "invalid repetition count(s)" there, while Go accepts up to 1000.
const pgMaxRepeat = 255

// pgBracketsAndBoundsAgree walks pattern's bracket expressions and braces
// (no backslash can occur; PgRegexCompatible has refused those) and reports
// whether both mean the same to PostgreSQL and Go:
//
//   - Inside a bracket expression, `[:`, `[.` and `[=` open a POSIX class,
//     collating element or equivalence class in pg, none of which Go reads
//     the same way (see PgRegexCompatible). An unterminated bracket is an
//     error in both.
//   - Outside one, a `{` followed by a digit is a bound in pg, which must be
//     `{m}`, `{m,}` or `{m,n}` with counts of at most 255, or the pattern is
//     an error there. Go reads a malformed one as literal text (`a{1`, `a{1,`,
//     `a{2x}`), and a count with a leading zero too (`a{01}`, a bound in pg).
//     A `{` followed by anything else is literal text in both.
//   - A quantifier directly after an anchor -- `^*a`, `a|^+`, `a$?`,
//     `^{2}a` -- has no operand to pg ("quantifier operand invalid"), while
//     Go repeats the empty-width assertion and matches. A quantified group
//     around one, `(^)*a`, is valid in both.
func pgBracketsAndBoundsAgree(pattern string) bool {
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '^', '$':
			if i+1 < len(pattern) {
				switch next := pattern[i+1]; {
				case next == '*' || next == '+' || next == '?':
					return false
				case next == '{' && i+2 < len(pattern) && isDecimalDigit(pattern[i+2]):
					return false
				}
			}
		case '[':
			j := i + 1
			if j < len(pattern) && pattern[j] == '^' {
				j++
			}
			if j < len(pattern) && pattern[j] == ']' {
				j++ // a leading ']' is a member, not the end
			}
			for ; j < len(pattern) && pattern[j] != ']'; j++ {
				if pattern[j] == '[' && j+1 < len(pattern) && strings.IndexByte(":.=", pattern[j+1]) >= 0 {
					return false
				}
			}
			if j >= len(pattern) {
				return false
			}
			i = j
		case '{':
			if i+1 < len(pattern) && isDecimalDigit(pattern[i+1]) {
				n, ok := pgBoundLength(pattern[i+1:])
				if !ok {
					return false
				}
				i += n
			}
		}
	}
	return true
}

// pgBoundLength reads the rest of a bound after its `{` -- `m}`, `m,}` or
// `m,n}` -- and returns how many bytes it spans, or false for anything pg
// would reject or Go would not read as the same bound.
func pgBoundLength(s string) (int, bool) {
	lo, i, ok := pgBoundCount(s, 0)
	if !ok {
		return 0, false
	}
	if i < len(s) && s[i] == '}' {
		return i + 1, true
	}
	if i >= len(s) || s[i] != ',' {
		return 0, false
	}
	i++
	if i < len(s) && s[i] == '}' {
		return i + 1, true
	}
	hi, i, ok := pgBoundCount(s, i)
	if !ok || hi < lo || i >= len(s) || s[i] != '}' {
		return 0, false
	}
	return i + 1, true
}

// pgBoundCount reads one repetition count starting at s[i]: decimal digits
// without a leading zero (Go refuses those), at most pgMaxRepeat.
func pgBoundCount(s string, i int) (count, next int, ok bool) {
	start := i
	for i < len(s) && isDecimalDigit(s[i]) {
		count = count*10 + int(s[i]-'0')
		if count > pgMaxRepeat {
			return 0, 0, false
		}
		i++
	}
	if i == start || (s[start] == '0' && i-start > 1) {
		return 0, 0, false
	}
	return count, i, true
}

func isDecimalDigit(c byte) bool {
	return c >= '0' && c <= '9'
}
