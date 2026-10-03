// SPDX-License-Identifier: Apache-2.0

//go:build integration

package interpret

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestReservedWordListCoversTheServer pins pgReservedWords to the PostgreSQL
// the suite runs against. orderByNameMisresolved declines an ORDER BY whose
// name is a fully reserved key word because dawgs emits the name unquoted and
// PostgreSQL then rejects it (42601) or reads it as a constant; the list was
// derived from a PostgreSQL 14 catalog, while CI and real deployments run
// newer servers, where a word can become reserved (system_user did in 16).
//
// The two directions are not symmetric. A word the server reserves that the
// map lacks is a wrong serve: `... ORDER BY <word>` is answered by the engine
// where PostgreSQL raises a syntax error, so the test fails and names it. A
// word the map holds that the server does not reserve only over-declines,
// which is safe; the test logs it so drift stays visible without breaking
// the build.
//
// The query is a single catalog read: no fixtures, no graph writes, and no
// schema assertion.
func TestReservedWordListCoversTheServer(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// catcode R is "reserved", T is "reserved (can be function or type)":
	// the two categories whose members are not accepted as a column
	// reference.
	rows, err := pool.Query(ctx, `select word from pg_get_keywords() where catcode in ('R','T')`)
	if err != nil {
		t.Fatalf("read pg_get_keywords(): %v", err)
	}
	defer rows.Close()

	reserved := map[string]bool{}
	for rows.Next() {
		var word string
		if err := rows.Scan(&word); err != nil {
			t.Fatalf("scan keyword: %v", err)
		}
		reserved[word] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read keywords: %v", err)
	}
	if len(reserved) == 0 {
		t.Fatal("pg_get_keywords() returned no reserved words; the catalog query no longer describes the server")
	}

	var missing []string
	for word := range reserved {
		if !pgReservedWords[word] {
			missing = append(missing, word)
		}
	}
	sort.Strings(missing)
	for _, word := range missing {
		t.Errorf("PostgreSQL reserves %q but pgReservedWords does not list it: "+
			"`ORDER BY %s` on such an alias would be served where PostgreSQL raises a syntax error, "+
			"so add %q to pgReservedWords in plan.go", word, word, word)
	}

	var extra []string
	for word := range pgReservedWords {
		if !reserved[word] {
			extra = append(extra, word)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Logf("pgReservedWords holds %d word(s) this server does not reserve (a harmless over-decline): %s",
			len(extra), strings.Join(extra, ", "))
	}
	t.Logf("server reserves %d words; pgReservedWords holds %d", len(reserved), len(pgReservedWords))
}
