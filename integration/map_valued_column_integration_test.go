// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"reflect"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/ops"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

var mapValuedNodeKind = graph.StringKind("MapValuedNode")

// TestMapValuedColumnsClassifyLikePostgreSQL runs map-valued RETURN columns
// through ops.FetchByQuery, BloodHound's cypher endpoint consumer, on the
// PostgreSQL driver and on the wrapped one. dawgs' pg mapper turns any
// map[string]any whose "nodes" and "edges" keys (if present) are empty lists
// into an empty graph.Path, so FetchByQuery files such a column under Paths,
// not Literals; a map that fails the path composite (a non-list "nodes", or
// a list of node composites, which decoded JSON can never satisfy) stays a
// literal. The served result must classify every one of them the same way.
func TestMapValuedColumnsClassifyLikePostgreSQL(t *testing.T) {
	_, bt, buf, ctx := openApplyDriver(t)

	if err := bt.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateNode(graph.NewProperties().
			Set("name", "m1").
			Set("obj", map[string]any{"a": 1}).
			Set("jsontext", `{"b": 2}`).
			Set("emptyobj", map[string]any{}).
			Set("emptyparts", map[string]any{"nodes": []any{}, "edges": []any{}}).
			Set("nodesstring", map[string]any{"nodes": "x"}).
			Set("nodesnull", map[string]any{"nodes": nil}).
			Set("edgeslist", map[string]any{"edges": []any{1}}).
			Set("nodecomposite", map[string]any{"nodes": []any{map[string]any{"id": 1, "kind_ids": []any{}, "properties": map[string]any{}}}}).
			Set("listofobjects", []any{map[string]any{"c": 3}}), mapValuedNodeKind)
		return err
	}); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	oracle, _ := graphtest.OpenPG(t, graphtest.PGAvailable(t))

	for _, text := range []string{
		`MATCH (n:MapValuedNode) RETURN n.obj`,
		`MATCH (n:MapValuedNode) RETURN n.jsontext`,
		`MATCH (n:MapValuedNode) RETURN n.name, n.obj`,
		`MATCH (n:MapValuedNode) RETURN n.emptyobj`,
		`MATCH (n:MapValuedNode) RETURN n.emptyparts`,
		`MATCH (n:MapValuedNode) RETURN n.nodesstring`,
		`MATCH (n:MapValuedNode) RETURN n.nodesnull`,
		`MATCH (n:MapValuedNode) RETURN n.edgeslist`,
		`MATCH (n:MapValuedNode) RETURN n.nodecomposite`,
		`MATCH (n:MapValuedNode) RETURN n.listofobjects`,
	} {
		t.Run(text, func(t *testing.T) {
			want, err := fetchByQueryThrough(ctx, oracle, text)
			if err != nil {
				t.Fatalf("postgresql: %v", err)
			}
			before := markerCount(buf, cypherServedMarker)
			got, err := fetchByQueryThrough(ctx, bt, text)
			if err != nil {
				t.Fatalf("bloodtrail: %v", err)
			}
			if served := markerCount(buf, cypherServedMarker) - before; served != 1 {
				t.Fatalf("cypher engine served the query %d times, want 1 (a decline compares nothing)", served)
			}
			if !reflect.DeepEqual(got.Paths, want.Paths) {
				t.Errorf("paths differ:\n  bloodtrail: %#v\n  postgresql: %#v", got.Paths, want.Paths)
			}
			if !reflect.DeepEqual(got.Literals, want.Literals) {
				t.Errorf("literals differ:\n  bloodtrail: %#v\n  postgresql: %#v", got.Literals, want.Literals)
			}
		})
	}
}

// fetchByQueryThrough runs text through ops.FetchByQuery on db.
func fetchByQueryThrough(ctx context.Context, db graph.Database, text string) (ops.QueryResult, error) {
	var result ops.QueryResult
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		result, err = ops.FetchByQuery(tx, text)
		return err
	})
	return result, err
}
