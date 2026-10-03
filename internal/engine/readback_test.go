// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/specterops/dawgs/graph"
)

// catalogKindTable is a kindCatalog over a fixed name->id catalog, keeping
// pgKindCatalog's own contract: a name (or id) the catalog lacks is simply
// absent from the result, the way a kind-table query returns no row for it,
// and a read with a done ctx fails as that query would. asked records every
// name either direction was handed, read through kind.String() -- so a nil
// kind reaching it panics, as it would against a real query's parameter
// list.
type catalogKindTable struct {
	ids   map[string]int16
	asked []string
}

func (m *catalogKindTable) idsByName(ctx context.Context, names []string) (map[string]int16, error) {
	out := make(map[string]int16, len(names))
	for _, name := range names {
		m.asked = append(m.asked, name)
		if id, ok := m.ids[name]; ok {
			out[name] = id
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("catalogKindTable: query kinds by name: %w", err)
	}
	return out, nil
}

func (m *catalogKindTable) namesByID(ctx context.Context, ids []int16) (map[int16]string, error) {
	out := make(map[int16]string, len(ids))
	for _, id := range ids {
		m.asked = append(m.asked, strconv.Itoa(int(id)))
		for name, known := range m.ids {
			if known == id {
				out[id] = name
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("catalogKindTable: query kinds by id: %w", err)
	}
	return out, nil
}

// askedSet returns the distinct names m was asked about, sorted.
func (m *catalogKindTable) askedSet() []string {
	seen := make(map[string]struct{}, len(m.asked))
	var out []string
	for _, name := range m.asked {
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// criteriaChangeSet records one node criteria and one relationship criteria
// naming both kinds buildApplyView's table knows (User, Tag, AdminTo) and
// kinds it does not (RowlessA, RowlessB, RowlessEdge).
func criteriaChangeSet() *ChangeSet {
	cs := &ChangeSet{}
	cs.RecordDeleteNodesByKinds(
		graph.Kinds{graph.StringKind("User"), graph.StringKind("RowlessA")},
		graph.Kinds{graph.StringKind("Tag"), graph.StringKind("RowlessB")},
	)
	cs.RecordDeleteRelationshipsByKinds(graph.Kinds{graph.StringKind("AdminTo"), graph.StringKind("RowlessEdge")})
	return cs
}

// rowlessCatalog is a catalog knowing every kind criteriaChangeSet names,
// the View-known ones under the ids buildApplyView uses.
func rowlessCatalog() *catalogKindTable {
	return &catalogKindTable{ids: map[string]int16{
		"User": applyKindUser, "Tag": applyKindTag, "AdminTo": applyKindAdminTo,
		"RowlessA": 40, "RowlessB": 41, "RowlessEdge": 43,
	}}
}

// TestResolveCriteriaKindsResolvesOnlyKindsTheViewLacks pins the unknown-only
// rule: a criteria kind the View already names never reaches the kind table,
// and every one it lacks comes back id->name, include, exclude and
// relationship kinds alike.
func TestResolveCriteriaKindsResolvesOnlyKindsTheViewLacks(t *testing.T) {
	catalog := rowlessCatalog()

	got, err := resolveCriteriaKinds(context.Background(), catalog, buildApplyView(t), criteriaChangeSet())
	if err != nil {
		t.Fatalf("resolveCriteriaKinds: %v", err)
	}

	want := map[int16]string{40: "RowlessA", 41: "RowlessB", 43: "RowlessEdge"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveCriteriaKinds = %v, want %v", got, want)
	}
	if asked, want := catalog.askedSet(), []string{"RowlessA", "RowlessB", "RowlessEdge"}; !reflect.DeepEqual(asked, want) {
		t.Fatalf("the kind table was asked about %v, want only the kinds the view lacks: %v", asked, want)
	}
}

// TestResolveCriteriaKindsNilViewResolvesEveryKind covers the boot-time
// replay, which reads back before any View is published: with nothing to
// consult, every criteria kind is resolved, as resolveUnknownKinds does for
// row kinds.
func TestResolveCriteriaKindsNilViewResolvesEveryKind(t *testing.T) {
	catalog := rowlessCatalog()

	got, err := resolveCriteriaKinds(context.Background(), catalog, nil, criteriaChangeSet())
	if err != nil {
		t.Fatalf("resolveCriteriaKinds: %v", err)
	}

	want := map[int16]string{
		applyKindUser: "User", applyKindTag: "Tag", applyKindAdminTo: "AdminTo",
		40: "RowlessA", 41: "RowlessB", 43: "RowlessEdge",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveCriteriaKinds = %v, want %v", got, want)
	}
}

// TestResolveCriteriaKindsLeavesUnresolvableKindsOut covers a name the kind
// table holds no row for: it is left out rather than reported (what that
// means is buildApplySegment's call), and costs the other names in the same
// lookup nothing.
func TestResolveCriteriaKindsLeavesUnresolvableKindsOut(t *testing.T) {
	catalog := rowlessCatalog()
	delete(catalog.ids, "RowlessB")

	got, err := resolveCriteriaKinds(context.Background(), catalog, buildApplyView(t), criteriaChangeSet())
	if err != nil {
		t.Fatalf("resolveCriteriaKinds: %v", err)
	}

	want := map[int16]string{40: "RowlessA", 43: "RowlessEdge"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveCriteriaKinds = %v, want %v", got, want)
	}
}

// TestResolveCriteriaKindsCancelledContextIsAHardError keeps resolveKindIDs'
// ctx rule for criteria kinds: a kind-table read that failed on a cancelled
// ctx says nothing about whether the kind exists, so it is returned (and
// Apply falls back) rather than read as "unresolved".
func TestResolveCriteriaKindsCancelledContextIsAHardError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := resolveCriteriaKinds(ctx, rowlessCatalog(), buildApplyView(t), criteriaChangeSet())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("resolveCriteriaKinds with a cancelled ctx = %v, want an error wrapping context.Canceled", err)
	}
}

// TestResolveCriteriaKindsNothingToResolve covers the two ways there is
// nothing to ask: no criteria at all, and criteria naming only kinds the
// View already knows. Neither reaches the kind table.
func TestResolveCriteriaKindsNothingToResolve(t *testing.T) {
	known := &ChangeSet{}
	known.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("User")}, graph.Kinds{graph.StringKind("Tag")})
	known.RecordDeleteRelationshipsByKinds(graph.Kinds{graph.StringKind("AdminTo")})

	for name, cs := range map[string]*ChangeSet{"no criteria": {}, "only known kinds": known} {
		catalog := rowlessCatalog()

		got, err := resolveCriteriaKinds(context.Background(), catalog, buildApplyView(t), cs)
		if err != nil {
			t.Fatalf("%s: resolveCriteriaKinds: %v", name, err)
		}
		if got != nil {
			t.Fatalf("%s: resolveCriteriaKinds = %v, want nil", name, got)
		}
		if len(catalog.asked) != 0 {
			t.Fatalf("%s: the kind table was asked about %v, want no lookups", name, catalog.asked)
		}
	}
}

// TestResolveKindIDsSkipsNilKinds pins the nil guard: a nil kind has no name
// to resolve and must never reach the kind table, whose lookup reads
// String() off every kind it was handed.
func TestResolveKindIDsSkipsNilKinds(t *testing.T) {
	catalog := &catalogKindTable{ids: map[string]int16{"Known": 7}}

	got, err := resolveKindIDs(context.Background(), catalog, nil, graph.Kinds{nil, graph.StringKind("Known"), nil})
	if err != nil {
		t.Fatalf("resolveKindIDs: %v", err)
	}
	if want := map[string]int16{"Known": 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveKindIDs = %v, want %v", got, want)
	}

	got, err = resolveKindIDs(context.Background(), catalog, nil, graph.Kinds{nil})
	if err != nil {
		t.Fatalf("resolveKindIDs(nil only): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("resolveKindIDs(nil only) = %v, want empty", got)
	}
}
