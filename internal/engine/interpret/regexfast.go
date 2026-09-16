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

	// prefilter, when non-empty, is a set of which at least one MUST appear
	// for the pattern to match -- necessary, not sufficient. Subjects that
	// carry none are rejected without running the engine; the rest are
	// matched normally. `(10.0.19044|10.0.22000|6.1.7601)` is the shape:
	// the dots are wildcards so no arm is a plain substring, but every arm
	// still requires one, and a version string carrying none of them cannot
	// match whatever the wildcards do.
	prefilter []string

	fold bool
}

// NewRegexMatcher compiles pattern and derives its substring plan, if any.
func NewRegexMatcher(pattern string) (*RegexMatcher, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	m := &RegexMatcher{re: re}
	if parsed, err := syntax.Parse(pattern, syntax.Perl); err == nil {
		simplified := parsed.Simplify()
		if lits, fold, ok := searchEquivalentLiterals(simplified); ok {
			m.literals, m.fold = lits, fold
		} else if lits, fold, ok := prefilterLiterals(simplified); ok {
			m.prefilter, m.fold = lits, fold
		}
	}
	return m, nil
}

// MatchString reports whether the pattern matches anywhere in s, exactly as
// the compiled regex would.
func (m *RegexMatcher) MatchString(s string) bool {
	if m == nil || m.re == nil {
		return false
	}
	if len(m.literals) == 0 {
		if len(m.prefilter) > 0 && !m.carriesAny(s, m.prefilter) {
			// No required literal present, so no arm can match.
			return false
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

// prefilterLiterals returns a set of which at least one must appear for re to
// match. Unlike searchEquivalentLiterals this is a NECESSARY condition only,
// so the caller still runs the real engine on whatever survives.
//
// Derived from a top-level alternation: every arm must yield a literal it
// requires, and a match means some arm matched, so it means that arm's
// literal is present. One arm without a required literal (`.*`, a character
// class) and there is nothing to filter on.
func prefilterLiterals(re *syntax.Regexp) (lits []string, fold bool, ok bool) {
	switch re.Op {
	case syntax.OpCapture:
		if len(re.Sub) != 1 {
			return nil, false, false
		}
		return prefilterLiterals(re.Sub[0])
	case syntax.OpAlternate:
		first := true
		for _, sub := range re.Sub {
			lit, subFold, subOK := requiredLiteral(sub)
			if !subOK {
				return nil, false, false
			}
			if first {
				fold, first = subFold, false
			} else if subFold != fold {
				return nil, false, false
			}
			lits = append(lits, lit)
		}
		if len(lits) == 0 {
			return nil, false, false
		}
		return lits, fold, true
	default:
		return nil, false, false
	}
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
