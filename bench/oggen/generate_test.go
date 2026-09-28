// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"testing"
)

func testConfig() Config {
	return Config{Users: 500, Teams: 50, Repos: 100, Seed: 7, ADUsers: []string{"S-1-5-21-1-2-3-1105", "S-1-5-21-1-2-3-1106"}}
}

func TestGenerateIsDeterministic(t *testing.T) {
	n1, e1 := Generate(testConfig())
	n2, e2 := Generate(testConfig())
	if !reflect.DeepEqual(n1, n2) || !reflect.DeepEqual(e1, e2) {
		t.Fatal("the same Config produced different output")
	}
	other := testConfig()
	other.Seed = 8
	if _, e3 := Generate(other); reflect.DeepEqual(e1, e3) {
		t.Fatal("a different seed produced identical edges")
	}
}

// TestNoSelfLoopEdges pins what the benchmark relies on: a self-loop of a
// traversed kind would make BloodTrail delegate every variable-length
// query over it, for reasons unrelated to OpenGraph.
func TestNoSelfLoopEdges(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		cfg := testConfig()
		cfg.Seed = seed
		_, edges := Generate(cfg)
		for _, e := range edges {
			if e.Start.Value == e.End.Value {
				t.Fatalf("seed %d: self-loop %+v", seed, e)
			}
		}
	}
}

// TestEveryEndpointIsANode pins that no edge creates a stub: every
// endpoint but a synced AD user is a generated node.
func TestEveryEndpointIsANode(t *testing.T) {
	cfg := testConfig()
	nodes, edges := Generate(cfg)
	ids := map[string]bool{}
	for _, n := range nodes {
		ids[n.ID] = true
	}
	ad := map[string]bool{}
	for _, u := range cfg.ADUsers {
		ad[u] = true
	}
	for _, e := range edges {
		for _, end := range []Endpoint{e.Start, e.End} {
			if !ids[end.Value] && !ad[end.Value] {
				t.Fatalf("edge %+v names %s, which is not a node", e, end.Value)
			}
		}
	}
}

// TestNodesFitBloodHoundsKindLimit pins graphify.validateNodeKinds' limit:
// with the source kind prepended, a node may carry at most three kinds.
func TestNodesFitBloodHoundsKindLimit(t *testing.T) {
	nodes, _ := Generate(testConfig())
	for _, n := range nodes {
		if len(n.Kinds)+1 > 3 {
			t.Fatalf("node %s has %d kinds plus the source kind", n.ID, len(n.Kinds))
		}
	}
}

func TestSeededPathsToRepoZero(t *testing.T) {
	cfg := testConfig()
	_, edges := Generate(cfg)
	has := func(start, kind, end string) bool {
		for _, e := range edges {
			if e.Start.Value == start && e.Kind == kind && e.End.Value == end {
				return true
			}
		}
		return false
	}
	for _, want := range [][3]string{
		{userID(1), edgeHasRole, roleID(0, "write")},
		{roleID(0, "write"), edgeCanWrite, repoID(0)},
		{userID(2), edgeMemberOf, teamID(0)},
		{teamID(0), edgeHasRole, roleID(0, "admin")},
		{roleID(0, "admin"), edgeCanAdmin, repoID(0)},
		{userID(0), edgeMemberOf, teamID(1)},
		{teamID(1), edgeMemberOf, teamID(0)},
		{cfg.ADUsers[0], edgeSyncedTo, userID(0)},
		{cfg.ADUsers[1], edgeSyncedTo, userID(1)},
	} {
		if !has(want[0], want[1], want[2]) {
			t.Errorf("missing seeded edge %s -%s-> %s", want[0], want[1], want[2])
		}
	}
}
