// SPDX-License-Identifier: Apache-2.0

package interpret

import (
	"testing"
)

// TestPgRegexCompatibleRefusesDialectDifferences pins which patterns the
// gate admits. Each refused one means something else to PostgreSQL than to
// Go (pinned against PostgreSQL by TestTryCypherRegexDialectMatchesOracle):
// a POSIX bracket construct, a bound pg rejects or reads differently, and
// non-ASCII text under (?i).
func TestPgRegexCompatibleRefusesDialectDifferences(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		want    bool
	}{
		{`^[[:alpha:]]+$`, false},
		{`^[a[:digit:]]$`, false},
		{`^[[.a.]]$`, false},
		{`^[[=a=]]$`, false},
		{`^[^[:space:]]$`, false},
		{`[abc`, false},
		{`a{256}`, false},
		{`a{0,256}`, false},
		{`a{1`, false},
		{`a{1,`, false},
		{`a{1,2`, false},
		{`a{2x}`, false},
		{`a{01}`, false},
		{`a{3,2}`, false},
		{`(?i)^JOSÉ$`, false},
		{`^*a`, false},
		{`$*a`, false},
		{`a|^+`, false},
		{`a$?`, false},
		{`^{2}a`, false},
		{`(?i)^*a`, false},
		{"(?i)\u212a", false},

		{`^a{255}$`, true},
		{`^a{0}$`, true},
		{`^a{1,}$`, true},
		{`^a{2,3}$`, true},
		{`a{`, true},
		{`a{,3}`, true},
		{`a{x}`, true},
		{`{`, true},
		{`[{1]`, true},
		{`[]a]`, true},
		{`[^]a]`, true},
		{`10[.]0`, true},
		{`[:a]`, true},
		{`^JOSÉ$`, true},
		{`(^)*a`, true},
		{`[^*]a`, true},
		{`^{,2}a`, true},
		{`^a$`, true},
		{`(?i).*Windows.* (2000|2003|2008|2012|xp|vista|7|8|me|nt).*`, true},
		{`(10.0.19044|10.0.22000|6.1.7601).?.*`, true},
	} {
		if got := PgRegexCompatible(tc.pattern); got != tc.want {
			t.Errorf("PgRegexCompatible(%q) = %v, want %v", tc.pattern, got, tc.want)
		}
	}
}

// TestRegexMatcherFoldsCaseAsPostgres checks the answers PostgreSQL gives
// for the two non-ASCII runes Go's case folding maps onto ASCII letters.
func TestRegexMatcherFoldsCaseAsPostgres(t *testing.T) {
	for _, tc := range []struct {
		pattern, subject string
		want             bool
	}{
		{`(?i)k`, "\u212a", false},
		{`(?i)k`, "K", true},
		{`(?i)^[^k]$`, "\u212a", true},
		{`(?i)^[^k]$`, "K", false},
		{`(?i)^[a-z]$`, "\u017f", false},
		{`(?i)s`, "\u017f", false},
		{`(?i).*windows.*`, "Window\u017f", false},
		{`(?i).*windows.*`, "WINDOWS é", true},
		{`k`, "\u212a", false},
	} {
		m, err := NewRegexMatcher(tc.pattern)
		if err != nil {
			t.Fatalf("NewRegexMatcher(%q): %v", tc.pattern, err)
		}
		if got := m.MatchString(tc.subject); got != tc.want {
			t.Errorf("%q =~ %q = %v, want %v", tc.subject, tc.pattern, got, tc.want)
		}
	}
}
