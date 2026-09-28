// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
)

// catalogKindMapper is a pg.KindMapper over a fixed name->id catalog that
// keeps dawgs' own contract (drivers/pg/manager.go, v0.8.0): MapKinds is
// all-or-nothing, MapKind fails for a name the catalog lacks, and both fail
// once ctx is done. asked records every name either method was handed,
// read through kind.String() exactly as dawgs formats its own mapping
// errors -- so a nil kind reaching it panics, as it would against dawgs.
type catalogKindMapper struct {
	ids   map[string]int16
	asked []string
}

func (m *catalogKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	ids := make([]int16, 0, len(kinds))
	var missing []string
	for _, kind := range kinds {
		name := kind.String()
		m.asked = append(m.asked, name)
		if id, ok := m.ids[name]; ok {
			ids = append(ids, id)
		} else {
			missing = append(missing, name)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("unable to map kinds: %s", strings.Join(missing, ", "))
	}
	return ids, nil
}

func (m *catalogKindMapper) MapKind(ctx context.Context, kind graph.Kind) (int16, error) {
	name := kind.String()
	m.asked = append(m.asked, name)
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	if id, ok := m.ids[name]; ok {
		return id, nil
	}
	return -1, fmt.Errorf("unable to map kind: %s", name)
}

func (m *catalogKindMapper) MapKindID(context.Context, int16) (graph.Kind, error) {
	return nil, errors.New("catalogKindMapper: MapKindID is not used by name resolution")
}

func (m *catalogKindMapper) MapKindIDs(context.Context, []int16) (graph.Kinds, error) {
	return nil, errors.New("catalogKindMapper: MapKindIDs is not used by name resolution")
}

func (m *catalogKindMapper) AssertKinds(context.Context, graph.Kinds) ([]int16, error) {
	return nil, errors.New("catalogKindMapper: read-back never asserts kinds")
}

// askedSet returns the distinct names m was asked about, sorted.
func (m *catalogKindMapper) askedSet() []string {
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
func rowlessCatalog() *catalogKindMapper {
	return &catalogKindMapper{ids: map[string]int16{
		"User": applyKindUser, "Tag": applyKindTag, "AdminTo": applyKindAdminTo,
		"RowlessA": 40, "RowlessB": 41, "RowlessEdge": 43,
	}}
}

// TestResolveCriteriaKindsResolvesOnlyKindsTheViewLacks pins the unknown-only
// rule: a criteria kind the View already names never reaches the mapper, and
// every one it lacks comes back id->name, include, exclude and relationship
// kinds alike.
func TestResolveCriteriaKindsResolvesOnlyKindsTheViewLacks(t *testing.T) {
	mapper := rowlessCatalog()

	got, err := resolveCriteriaKinds(context.Background(), mapper, buildApplyView(t), criteriaChangeSet())
	if err != nil {
		t.Fatalf("resolveCriteriaKinds: %v", err)
	}

	want := map[int16]string{40: "RowlessA", 41: "RowlessB", 43: "RowlessEdge"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveCriteriaKinds = %v, want %v", got, want)
	}
	if asked, want := mapper.askedSet(), []string{"RowlessA", "RowlessB", "RowlessEdge"}; !reflect.DeepEqual(asked, want) {
		t.Fatalf("mapper was asked about %v, want only the kinds the view lacks: %v", asked, want)
	}
}

// TestResolveCriteriaKindsNilViewResolvesEveryKind covers the boot-time
// replay, which reads back before any View is published: with nothing to
// consult, every criteria kind is resolved, as resolveUnknownKinds does for
// row kinds.
func TestResolveCriteriaKindsNilViewResolvesEveryKind(t *testing.T) {
	mapper := rowlessCatalog()

	got, err := resolveCriteriaKinds(context.Background(), mapper, nil, criteriaChangeSet())
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

// TestResolveCriteriaKindsLeavesUnresolvableKindsOut covers a name the
// mapper cannot resolve: it is left out rather than reported (what that
// means is buildApplySegment's call), and the batch failure it causes does
// not cost the other names their resolution.
func TestResolveCriteriaKindsLeavesUnresolvableKindsOut(t *testing.T) {
	mapper := rowlessCatalog()
	delete(mapper.ids, "RowlessB")

	got, err := resolveCriteriaKinds(context.Background(), mapper, buildApplyView(t), criteriaChangeSet())
	if err != nil {
		t.Fatalf("resolveCriteriaKinds: %v", err)
	}

	want := map[int16]string{40: "RowlessA", 43: "RowlessEdge"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveCriteriaKinds = %v, want %v", got, want)
	}
}

// TestResolveCriteriaKindsCancelledContextIsAHardError keeps resolveKindIDs'
// ctx rule for criteria kinds: a mapper failure caused by a cancelled ctx
// says nothing about whether the kind exists, so it is returned (and Apply
// falls back) rather than read as "unresolved".
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
// View already knows. Neither reaches the mapper.
func TestResolveCriteriaKindsNothingToResolve(t *testing.T) {
	known := &ChangeSet{}
	known.RecordDeleteNodesByKinds(graph.Kinds{graph.StringKind("User")}, graph.Kinds{graph.StringKind("Tag")})
	known.RecordDeleteRelationshipsByKinds(graph.Kinds{graph.StringKind("AdminTo")})

	for name, cs := range map[string]*ChangeSet{"no criteria": {}, "only known kinds": known} {
		mapper := rowlessCatalog()

		got, err := resolveCriteriaKinds(context.Background(), mapper, buildApplyView(t), cs)
		if err != nil {
			t.Fatalf("%s: resolveCriteriaKinds: %v", name, err)
		}
		if got != nil {
			t.Fatalf("%s: resolveCriteriaKinds = %v, want nil", name, got)
		}
		if len(mapper.asked) != 0 {
			t.Fatalf("%s: mapper was asked about %v, want no calls", name, mapper.asked)
		}
	}
}

// TestResolveKindIDsSkipsNilKinds pins the nil guard: a nil kind has no name
// to resolve and must never reach the mapper, whose error formatting calls
// String() on every kind it was handed.
func TestResolveKindIDsSkipsNilKinds(t *testing.T) {
	mapper := &catalogKindMapper{ids: map[string]int16{"Known": 7}}

	got, err := resolveKindIDs(context.Background(), mapper, graph.Kinds{nil, graph.StringKind("Known"), nil})
	if err != nil {
		t.Fatalf("resolveKindIDs: %v", err)
	}
	if want := map[string]int16{"Known": 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resolveKindIDs = %v, want %v", got, want)
	}

	got, err = resolveKindIDs(context.Background(), mapper, graph.Kinds{nil})
	if err != nil {
		t.Fatalf("resolveKindIDs(nil only): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("resolveKindIDs(nil only) = %v, want empty", got)
	}
}
