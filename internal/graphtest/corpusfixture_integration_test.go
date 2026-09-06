// SPDX-License-Identifier: Apache-2.0

//go:build integration

package graphtest

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

// corpusQueryHasRow runs cypherText against d through a read transaction
// and reports whether it returned at least one row, failing the test
// outright on any query error (a translation/execution error here means
// the fixture is missing a kind declaration or is otherwise broken, not
// that the predicate legitimately found zero rows).
func corpusQueryHasRow(t *testing.T, ctx context.Context, d *pg.Driver, cypherText string) bool {
	t.Helper()

	found := false
	if err := d.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypherText, nil)
		defer result.Close()
		if result.Next() {
			found = true
		}
		return result.Error()
	}); err != nil {
		t.Fatalf("corpus fixture self-check: query %q: %v", cypherText, err)
	}
	return found
}

// TestLoadCorpusFixtureSelfCheck is task-15's own exit criterion: seed the
// corpus fixture, then confirm -- straight through the pg driver, the same
// oracle Task 16's differential suite will compare the engine against --
// that each of five representative predicates drawn from the major corpus
// families returns at least one row. This does not attempt to validate the
// whole corpus (that is Task 16's job); it exists so a broken fixture (a
// missing kind declaration, a property typed wrong, a WHERE clause that
// silently matches nothing) fails fast and close to the code that caused
// it, rather than surfacing 100+ query mismatches downstream.
func TestLoadCorpusFixtureSelfCheck(t *testing.T) {
	dsn := PGAvailable(t)
	d, _ := OpenPG(t, dsn)
	WipeGraph(t, d)

	fixture := LoadCorpusFixture(t, d)
	if len(fixture.IDs) == 0 {
		t.Fatal("corpus fixture self-check: LoadCorpusFixture returned no id handles")
	}

	ctx := context.Background()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "a -512 group",
			query: `MATCH (g:Group) WHERE g.objectid ENDS WITH '-512' RETURN g LIMIT 1`,
		},
		{
			name:  "a hasspn+enabled user",
			query: `MATCH (u:User) WHERE u.hasspn = true AND u.enabled = true RETURN u LIMIT 1`,
		},
		{
			// Mirrors agt.json/agi.json's "Enrollment rights on published
			// ESC2 certificate templates" WHERE clause (minus its
			// '2.5.29.37.0' IN effectiveekus branch, which this fixture's
			// certtemplate:esc node doesn't need to satisfy separately).
			name: "a vulnerable CertTemplate",
			query: `MATCH (c:CertTemplate)
WHERE c.requiresmanagerapproval = false
AND (c.effectiveekus = [''] OR c.effectiveekus IS NULL)
AND (c.authorizedsignatures = 0 OR c.schemaversion = 1)
RETURN c LIMIT 1`,
		},
		{
			name:  "a Tag_Tier_Zero node",
			query: `MATCH (n:Tag_Tier_Zero) RETURN n LIMIT 1`,
		},
		{
			// The exact regex agt.json/agi.json's "Computers with
			// unsupported operating systems" query uses.
			name:  "a legacy-OS computer",
			query: `MATCH (c:Computer) WHERE c.operatingsystem =~ '(?i).*Windows.* (2000|2003|2008|2012|xp|vista|7|8|me|nt).*' RETURN c LIMIT 1`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if !corpusQueryHasRow(t, ctx, d, tc.query) {
				t.Errorf("expected at least one row, got none (query: %s)", tc.query)
			}
		})
	}

	// Belt-and-suspenders: the fixture's own id-handle map should agree
	// with the driver-level results above for a sample of well-known keys.
	for _, key := range []string{d1DomainAdmins, d1DomainUsers, "domain1", "domain2", "certtemplate:esc"} {
		if _, ok := fixture.IDs[key]; !ok {
			t.Errorf("fixture.IDs missing expected key %q", key)
		}
	}
}
