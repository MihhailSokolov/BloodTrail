// SPDX-License-Identifier: Apache-2.0

//go:build integration

// This file is Task 14 of the milestone-4 plan: count/shape assertions over
// the pre-built Cypher query corpus extracted from an upstream BloodHound CE
// checkout (scripts/extract-prebuilt-queries.go) into testdata/prebuilt/
// {agt,agi,selectors}.json -- see testdata/prebuilt/NOTICE for provenance
// and that script's package doc for exactly what each file holds and why.
//
// The loader helpers below (loadCommonSearches/loadSelectors and the
// commonSearchEntry/selectorEntry types) are deliberately factored out for
// Task 16's differential suite to build on top of: this file only asserts
// the corpus' *shape* -- exact counts, the disabled/probe markers, the
// absence of Cypher bind parameters (this is a fixed, canned corpus of demo
// queries, never caller-parameterized), and syntactic validity via the same
// frontend.ParseCypher the engine's own interpreter uses (see
// write_observer.go's parseCypherFrontend). Task 16 will additionally run
// every enabled query against a live engine/oracle pair.
//
// File placement mirrors dawgs_corpus_integration_test.go and
// builder_differential_matrix_integration_test.go: package bloodtrail, not
// bloodtrail_test, even though this file itself never touches Driver's
// unexported engine field -- Task 16's differential suite almost certainly
// will (to force a deterministic snapshot rebuild the way those two files
// do), and there is no benefit to churning the package declaration once
// this file already has line-item history under the other name.
package bloodtrail

import (
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
)

// commonSearchEntry mirrors one entry of testdata/prebuilt/agt.json or
// agi.json -- see scripts/extract-prebuilt-queries.go's identically shaped
// type for the extraction side.
type commonSearchEntry struct {
	Subheader string `json:"subheader"`
	Name      string `json:"name"`
	Query     string `json:"query"`
	Disabled  bool   `json:"disabled"`
	Probe     bool   `json:"probe,omitempty"`
}

// selectorEntry mirrors one entry of testdata/prebuilt/selectors.json.
type selectorEntry struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

// loadCommonSearches loads one of testdata/prebuilt/{agt,agi}.json.
func loadCommonSearches(t *testing.T, path string) []commonSearchEntry {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var entries []commonSearchEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return entries
}

// loadSelectors loads testdata/prebuilt/selectors.json.
func loadSelectors(t *testing.T, path string) []selectorEntry {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var entries []selectorEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return entries
}

// cypherParamPattern matches genuine Cypher bind-parameter syntax ("$" then
// an identifier character), as opposed to a bare "$" appearing incidentally
// inside a string literal. Two corpus queries end a regex pattern with a
// literal "$" end-anchor (e.g. an AGT/AGI query matching high-privileged
// role display names via '(?i)(...|Privileged Role Administrator).*$'), and
// one selector's cypher matches the literal AD computer-account suffix
// 'AZUREADSSOACC$' -- neither is a parameter reference, and a naive
// substring check for "$" would misflag both.
var cypherParamPattern = regexp.MustCompile(`\$[A-Za-z_]`)

// assertNoParams fails the test if any query in queries references a Cypher
// bind parameter -- this corpus is a fixed, canned set of demo queries with
// no caller-supplied parameters, by design.
func assertNoParams(t *testing.T, label string, queries []string) {
	t.Helper()

	for i, q := range queries {
		if cypherParamPattern.MatchString(q) {
			t.Errorf("%s[%d]: query contains a Cypher parameter reference: %s", label, i, q)
		}
	}
}

// assertParses fails the test if query does not parse as valid Cypher.
func assertParses(t *testing.T, label, query string) {
	t.Helper()

	if _, err := frontend.ParseCypher(frontend.DefaultCypherContext(), query); err != nil {
		t.Errorf("%s: expected query to parse, got error: %v\nquery: %s", label, err, query)
	}
}

// assertFailsToParse fails the test if query parses successfully -- used
// for the one deliberately malformed "probe" entry (UncommonSearches'
// "Query Parse Error", carried in agt.json with Probe: true).
func assertFailsToParse(t *testing.T, label, query string) {
	t.Helper()

	if _, err := frontend.ParseCypher(frontend.DefaultCypherContext(), query); err == nil {
		t.Errorf("%s: expected query to fail to parse, but it parsed: %s", label, query)
	}
}

// TestPrebuiltCorpusCounts is milestone 4's task-14 exit criterion: the
// pre-built query corpus extracted from upstream BloodHound CE has the
// exact shape task-14's brief documents --
//
//   - agt.json: 92 entries total -- the 91-entry CommonSearches export (one
//     disabled, the many-to-many "Shortest paths from Owned objects to Tier
//     Zero" query) plus the single UncommonSearches probe entry folded in
//     alongside them;
//   - agi.json: 91 entries, one disabled (the same many-to-many query);
//   - selectors.json: 42 seeded cypher tier-zero selectors --
//
// no query anywhere in the corpus references a Cypher bind parameter, and
// every non-disabled query parses as valid Cypher via frontend.ParseCypher
// except the probe, which must fail to parse.
func TestPrebuiltCorpusCounts(t *testing.T) {
	agt := loadCommonSearches(t, "testdata/prebuilt/agt.json")
	agi := loadCommonSearches(t, "testdata/prebuilt/agi.json")
	selectors := loadSelectors(t, "testdata/prebuilt/selectors.json")

	t.Run("agt", func(t *testing.T) {
		const wantTotal = 92 // 91 CommonSearches (1 disabled) + 1 probe
		if len(agt) != wantTotal {
			t.Fatalf("got %d entries, want %d", len(agt), wantTotal)
		}

		var (
			disabled, probes int
			queries          []string
		)
		for _, e := range agt {
			queries = append(queries, e.Query)
			if e.Disabled {
				disabled++
			}
			if e.Probe {
				probes++
			}
		}
		if disabled != 1 {
			t.Errorf("got %d disabled entries, want 1", disabled)
		}
		if probes != 1 {
			t.Errorf("got %d probe entries, want 1", probes)
		}

		assertNoParams(t, "agt", queries)

		for _, e := range agt {
			switch {
			case e.Probe:
				assertFailsToParse(t, "agt probe "+e.Name, e.Query)
			case e.Disabled:
				// Extracted for the record, never executed or parse-checked:
				// its query text is a block of "// ..." comment lines, not
				// valid Cypher on its own (see extract-prebuilt-queries.go's
				// isCommentedOut).
			default:
				assertParses(t, "agt "+e.Name, e.Query)
			}
		}
	})

	t.Run("agi", func(t *testing.T) {
		const wantTotal = 91
		if len(agi) != wantTotal {
			t.Fatalf("got %d entries, want %d", len(agi), wantTotal)
		}

		var (
			disabled int
			queries  []string
		)
		for _, e := range agi {
			queries = append(queries, e.Query)
			if e.Disabled {
				disabled++
			}
			if e.Probe {
				t.Errorf("agi.json entry %q unexpectedly marked probe", e.Name)
			}
		}
		if disabled != 1 {
			t.Errorf("got %d disabled entries, want 1", disabled)
		}

		assertNoParams(t, "agi", queries)

		for _, e := range agi {
			if e.Disabled {
				continue
			}
			assertParses(t, "agi "+e.Name, e.Query)
		}
	})

	t.Run("selectors", func(t *testing.T) {
		const want = 42
		if len(selectors) != want {
			t.Fatalf("got %d selectors, want %d", len(selectors), want)
		}

		var queries []string
		for _, s := range selectors {
			queries = append(queries, s.Query)
		}
		assertNoParams(t, "selectors", queries)

		for _, s := range selectors {
			assertParses(t, "selector "+s.Name, s.Query)
		}
	})
}
