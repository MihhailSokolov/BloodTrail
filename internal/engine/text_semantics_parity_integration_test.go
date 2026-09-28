// SPDX-License-Identifier: Apache-2.0

//go:build integration

package engine

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/specterops/dawgs/graph"

	"github.com/MihhailSokolov/BloodTrail/internal/graphtest"
)

// TestTryCypherMixedTypeTextSemanticsMatchOracle compares served answers
// with PostgreSQL over properties whose values span JSON types. dawgs runs
// the string predicates, `p IN [...]`, `'x' IN n.list`, coalesce() and
// relational comparisons over the `->>` TEXT of a value (cast where a
// literal asks for it), so a number, boolean or list can match a string
// predicate there. The engine's string and element indexes held only
// string values and served short answers; its per-row coalesce, IN and
// relational evaluation typed values the jsonb way. Each query here must be
// either declined -- in which case PostgreSQL's own answer, or error,
// stands -- or served with exactly PostgreSQL's rows. PostgreSQL erroring
// on a query the engine serves is a failure too.
func TestTryCypherMixedTypeTextSemanticsMatchOracle(t *testing.T) {
	dsn := graphtest.PGAvailable(t)
	ctx := context.Background()

	pgDriver, pool := graphtest.OpenPG(t, dsn)
	graphtest.WipeGraph(t, pgDriver)

	kind := graph.StringKind("MixUser")
	if err := pgDriver.WriteTransaction(ctx, func(tx graph.Transaction) error {
		// Enough plain nodes that every property-index anchor is worth
		// taking, so the index route -- not only the per-row one -- is what
		// gets compared.
		for i := 0; i < 400; i++ {
			props := graph.NewProperties().Set("name", fmt.Sprintf("U%d", i))
			switch i {
			case 1:
				props.Set("tag", 12345)
			case 2:
				props.Set("tag", "12999")
			case 3:
				props.Set("os", []string{"Windows 2000"})
			case 4:
				props.Set("os", "Windows 2003")
			case 5:
				props.Set("arr", []int{1, 2})
			case 6:
				props.Set("arr", []string{"1", "x"})
			case 7:
				props.Set("flag", true)
			case 8:
				props.Set("flag", "false")
			case 9:
				props.Set("arr2", []bool{true})
			case 10:
				props.Set("num", 7)
			case 11:
				props.Set("num", "7")
			case 12:
				props.Set("frac", 7.5)
			case 13:
				props.Set("name", "a\nb")
			}
			if _, err := tx.CreateNode(props, kind); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	eng := New(pgDriver, pool, Config{Enabled: true, Log: testEngineLogger()})
	if err := eng.RebuildNow(ctx, "manual"); err != nil {
		t.Fatalf("RebuildNow: %v", err)
	}

	names := func(result graph.Result) ([]string, error) {
		defer result.Close()
		var out []string
		for result.Next() {
			var node graph.Node
			if !result.Mapper().Map(result.Values()[0], &node) {
				return nil, fmt.Errorf("column did not map to a node: %#v", result.Values()[0])
			}
			name, _ := node.Properties.Get("name").String()
			out = append(out, name)
		}
		if err := result.Error(); err != nil {
			return nil, err
		}
		sort.Strings(out)
		return out, nil
	}

	for _, query := range []string{
		`MATCH (u:MixUser) WHERE u.tag STARTS WITH '12' RETURN u`,
		`MATCH (u:MixUser) WHERE u.tag ENDS WITH '45' RETURN u`,
		`MATCH (u:MixUser) WHERE u.tag CONTAINS '234' RETURN u`,
		`MATCH (u:MixUser) WHERE u.os =~ '.*2000.*' RETURN u`,
		`MATCH (u:MixUser) WHERE u.tag IN ['12345'] RETURN u`,
		`MATCH (u:MixUser) WHERE u.flag IN ['true'] RETURN u`,
		`MATCH (u:MixUser) WHERE coalesce(u.tag, '') = '12345' RETURN u`,
		`MATCH (u:MixUser) WHERE '1' IN u.arr RETURN u`,
		`MATCH (u:MixUser) WHERE 'true' IN u.arr2 RETURN u`,
		`MATCH (u:MixUser) WHERE '1.0' IN u.arr RETURN u`,
		`MATCH (u:MixUser) WHERE 1 IN u.arr RETURN u`,
		`MATCH (u:MixUser) WHERE coalesce(u.num, 0) = 7 RETURN u`,
		`MATCH (u:MixUser) WHERE coalesce(u.num, '') = '7' RETURN u`,
		`MATCH (u:MixUser) WHERE u.num > 5 RETURN u`,
		`MATCH (u:MixUser) WHERE u.frac > 5 RETURN u`,
		`MATCH (u:MixUser) WHERE u.frac > 5.0 RETURN u`,
		`MATCH (u:MixUser) WHERE u.tag = '12345' RETURN u`,
		`MATCH (u:MixUser) WHERE u.tag = 12345 RETURN u`,
		`MATCH (u:MixUser) WHERE u.name =~ 'a.b' RETURN u`,
		`MATCH (u:MixUser) WHERE u.name =~ 'U1\\d' RETURN u`,
	} {
		t.Run(query, func(t *testing.T) {
			var (
				engineRows []string
				engineErr  error
				served     bool
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				var result graph.Result
				if result, served = eng.TryCypher(ctx, tx, query, nil); served {
					engineRows, engineErr = names(result)
				}
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (engine): %v", err)
			}
			if !served {
				return // PostgreSQL answers, and is right by definition
			}
			if engineErr != nil {
				t.Fatalf("served result failed: %v", engineErr)
			}

			var (
				oracleRows []string
				oracleErr  error
			)
			if err := pgDriver.ReadTransaction(ctx, func(tx graph.Transaction) error {
				oracleRows, oracleErr = names(tx.Query(query, map[string]any{}))
				return nil
			}); err != nil {
				t.Fatalf("ReadTransaction (oracle): %v", err)
			}
			if oracleErr != nil {
				t.Fatalf("engine served %v; PostgreSQL raises an error: %v", engineRows, oracleErr)
			}
			if !reflect.DeepEqual(engineRows, oracleRows) {
				t.Fatalf("engine served %v, PostgreSQL returns %v", engineRows, oracleRows)
			}
		})
	}
}
