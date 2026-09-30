// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
)

// TestTryCypherTextColumnsMatchOracle compares the columns PostgreSQL types
// text with the served ones. dawgs' pg driver decodes only jsonb and json
// columns, so a text column comes back exactly as stored even when its text
// looks like JSON: type(r) is `kind_name(..)::text`, and a string constant
// carried through WITH stays text (`select '[1]' as i0`) however it is
// projected, parenthesized, renamed or de-duplicated. Serving either through
// the jsonb double-decode returned a list, an object or an unquoted string.
// The property-lookup cases at the end keep the decode where PostgreSQL
// applies it (a jsonb column), including next to a carried constant.
func TestTryCypherTextColumnsMatchOracle(t *testing.T) {
	pgDriver, eng := seedTypedGraph(t, nil)
	ctx := context.Background()

	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		nodeKind := graph.StringKind("TextColNode")
		a, err := tx.CreateNode(graph.NewProperties().Set("name", "u0").Set("js", `"quoted"`).Set("jl", `[1,2]`), nodeKind)
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(graph.NewProperties().Set("name", "u1").Set("js", "plain").Set("jl", `[x`), nodeKind)
		if err != nil {
			return err
		}
		// Relationship kinds whose names are valid JSON text.
		for _, kindName := range []string{`"RevQuoted"`, `[7]`, `{"rev":1}`} {
			if _, err := tx.CreateRelationshipByIDs(a.ID, b.ID, graph.StringKind(kindName), graph.NewProperties()); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	assertTypedCasesMatchOracle(t, pgDriver, eng, []typedCase{
		{`MATCH (a:TextColNode)-[r]->(b:TextColNode) RETURN type(r) AS t`, true},
		{`MATCH (a:TextColNode)-[r]->(b:TextColNode) RETURN labels(a) AS l, type(r) AS t`, true},
		{`WITH '[1]' AS s MATCH (m:TextColNode) RETURN s`, true},
		{`WITH '"q"' AS s MATCH (m:TextColNode) RETURN s, m.name`, true},
		{`WITH '{"a":1}' AS s MATCH (m:TextColNode) RETURN m.name, s`, true},
		{`WITH '[1]' AS s MATCH (m:TextColNode) WHERE m.name = 'u0' RETURN s`, true},
		{`MATCH (n:TextColNode) WITH n, '{"a":1}' AS s RETURN n.name, s`, true},
		{`MATCH (n:TextColNode) WITH n, '"q"' AS s RETURN n.name, s`, true},
		{`MATCH (n:TextColNode) WITH n, '[1,2]' AS s RETURN n.name, s`, true},
		{`MATCH (n:TextColNode) WITH n, ' {"a":1}' AS s RETURN n.name, s`, true},
		{`MATCH (n:TextColNode) WITH n, '[1,2]' AS s RETURN DISTINCT s`, true},
		{`MATCH (n:TextColNode) WITH n, '[1,2]' AS s RETURN s AS t`, true},
		{`MATCH (n:TextColNode) WITH n, '[1,2]' AS s RETURN (s)`, true},
		{`MATCH (n:TextColNode) WITH n, '[1,2]' AS s RETURN n.jl, s`, true},
		{`MATCH (n:TextColNode) RETURN n.js`, true},
		{`MATCH (n:TextColNode) RETURN n.jl`, true},
	})
}
