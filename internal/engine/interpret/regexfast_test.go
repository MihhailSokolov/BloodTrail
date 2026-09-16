// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"testing"
)

// TestRegexMatcherAgreesWithRegexp is the whole safety argument for the
// substring fast path, made by differential rather than by reasoning: for
// every pattern and every subject, the matcher must answer exactly what the
// compiled regex answers. A fast path that disagreed anywhere would be a
// SERVED WRONG ANSWER, which is the one failure mode a decline cannot cover.
//
// The subjects deliberately include the cases the ASCII restriction exists
// for: U+212A KELVIN SIGN and U+017F LATIN SMALL LETTER LONG S both fold onto
// ASCII letters under Go's rules, so a naive ASCII-only comparison would
// disagree on them -- the matcher hands those to the real engine.
func TestRegexMatcherAgreesWithRegexp(t *testing.T) {
	patterns := []string{
		// The shipped corpus shapes.
		`(?i).*SMITH.*`,
		`(10.0.19044|10.0.22000|10.0.19043|6.1.7601|5.1.2600)`,
		`(?i)^(Global Administrator|User Administrator|Exchange Administrator).*$`,
		`(?i).*Windows.* (2000|2003|2008|2012|xp|vista|7|8|me|nt).*`,
		// Shapes the fast path should take.
		`.*abc.*`,
		`abc`,
		`(?s).*abc.*`,
		`(abc|def)`,
		`(?i)(abc|DEF)`,
		`.*(abc|def).*`,
		// Shapes it must refuse.
		`^abc`,
		`abc$`,
		`a.c`,
		`a+bc`,
		`(?i)abc|de.`,
		`[abc]+`,
		`\d+`,
		`(?i).*k.*`,
		`(?i)stra.e`,
		``,
	}
	subjects := []string{
		"", "a", "abc", "ABC", "AbC", "xxabcxx", "xxABCxx", "def", "DEF",
		"SMITH", "smith", "John Smith", "JOHN SMITH", "smyth",
		"Windows Server 2012 R2", "windows xp", "Windows 10", "Linux",
		"10.0.19044", "10.0.99999", "6.1.7601", "Global Administrator",
		"global administrator", "Helpdesk Administrator",
		// Non-ASCII, including the two runes that fold onto ASCII letters.
		"K", "xKx", "KELVIN", "kelvin", "ſ", "straſe",
		"straße", "ÄÖÜ", "日本語", "abcK", "éabc",
	}

	for _, pattern := range patterns {
		t.Run(fmt.Sprintf("%q", pattern), func(t *testing.T) {
			want, err := regexp.Compile(pattern)
			if err != nil {
				t.Skipf("pattern does not compile: %v", err)
			}
			got, err := NewRegexMatcher(pattern)
			if err != nil {
				t.Fatalf("NewRegexMatcher: %v", err)
			}
			for _, s := range subjects {
				if got.MatchString(s) != want.MatchString(s) {
					t.Fatalf("MatchString(%q) = %v, want %v (fast literals=%q fold=%v)",
						s, got.MatchString(s), want.MatchString(s), got.literals, got.fold)
				}
			}
		})
	}
}

// TestRegexMatcherAgreesOnRandomSubjects widens the same differential over
// generated strings, so agreement does not rest on a subject list I chose.
func TestRegexMatcherAgreesOnRandomSubjects(t *testing.T) {
	alphabet := []rune("abcABC .0129xyzXYZKſé")
	rng := rand.New(rand.NewSource(20260916))

	patterns := []string{`(?i).*abc.*`, `(abc|xyz)`, `(?i)(abc|XYZ)`, `.*abc.*`, `abc`}
	compiled := make([]*regexp.Regexp, len(patterns))
	matchers := make([]*RegexMatcher, len(patterns))
	for i, p := range patterns {
		var err error
		if compiled[i], err = regexp.Compile(p); err != nil {
			t.Fatal(err)
		}
		if matchers[i], err = NewRegexMatcher(p); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 20000; i++ {
		var b strings.Builder
		for j := rng.Intn(12); j > 0; j-- {
			b.WriteRune(alphabet[rng.Intn(len(alphabet))])
		}
		s := b.String()
		for k := range patterns {
			if matchers[k].MatchString(s) != compiled[k].MatchString(s) {
				t.Fatalf("pattern %q, subject %q: matcher = %v, regexp = %v",
					patterns[k], s, matchers[k].MatchString(s), compiled[k].MatchString(s))
			}
		}
	}
}

// TestRegexMatcherTakesTheFastPathWhereExpected pins WHICH plan each pattern
// gets -- the differential above proves agreement whichever it is, so without
// this a plan could silently stop firing and show up only as a performance
// regression nobody attributes.
//
// "equivalent" replaces the engine outright; "prefilter" only rejects
// subjects that provably cannot match and still runs the engine on the rest.
func TestRegexMatcherTakesTheFastPathWhereExpected(t *testing.T) {
	const (
		none       = "none"
		equivalent = "equivalent"
		prefilter  = "prefilter"
	)
	for _, tc := range []struct {
		pattern string
		want    string
	}{
		{`(?i).*SMITH.*`, equivalent},
		{`.*abc.*`, equivalent},
		{`abc`, equivalent},
		{`(abc|def)`, equivalent},
		// The dots are WILDCARDS, so no arm is a plain substring -- but every
		// arm still requires one, which is a sound prefilter.
		{`(10.0.19044|10.0.22000|6.1.7601)`, prefilter},
		{`^abc`, none},
		{`abc$`, none},
		{`a.c`, none},
		{`(?i).*Windows.* (2000|2003).*`, none},
		{`[abc]+`, none},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			m, err := NewRegexMatcher(tc.pattern)
			if err != nil {
				t.Fatal(err)
			}
			got := none
			switch {
			case len(m.literals) > 0:
				got = equivalent
			case len(m.prefilter) > 0:
				got = prefilter
			}
			if got != tc.want {
				t.Fatalf("plan = %s, want %s (literals=%q prefilter=%q)",
					got, tc.want, m.literals, m.prefilter)
			}
		})
	}
}

func BenchmarkRegexContainsFold(b *testing.B) {
	const pattern = `(?i).*SMITH.*`
	subjects := make([]string, 1000)
	for i := range subjects {
		subjects[i] = fmt.Sprintf("USER%06d@CORP.LOCAL", i)
	}
	subjects[len(subjects)-1] = "JOHN SMITH@CORP.LOCAL"

	b.Run("regexp", func(b *testing.B) {
		re := regexp.MustCompile(pattern)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, s := range subjects {
				_ = re.MatchString(s)
			}
		}
	})
	b.Run("matcher", func(b *testing.B) {
		m, _ := NewRegexMatcher(pattern)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, s := range subjects {
				_ = m.MatchString(s)
			}
		}
	})
}
