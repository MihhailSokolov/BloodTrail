// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"strings"
	"testing"
)

// TestGenerateDeterministic asserts that two calls to Generate with the same
// Spec produce byte-for-byte (structurally) identical graphs: same node
// order, same objectids, same edges. Generate must not rely on map
// iteration order or any other nondeterministic source when it builds the
// Nodes/Edges slices.
func TestGenerateDeterministic(t *testing.T) {
	spec := Spec{Users: 1000, Seed: 1}

	a := Generate(spec)
	b := Generate(spec)

	if !reflect.DeepEqual(a, b) {
		t.Fatalf("Generate(%+v) is not deterministic: two runs produced different graphs", spec)
	}
}

// TestGenerateSeedVarianceProducesDifferentGraphs asserts that changing the
// seed (with the same Users) changes the generated graph, so Seed actually
// participates in generation rather than being ignored.
func TestGenerateSeedVarianceProducesDifferentGraphs(t *testing.T) {
	a := Generate(Spec{Users: 1000, Seed: 1})
	b := Generate(Spec{Users: 1000, Seed: 2})

	if reflect.DeepEqual(a, b) {
		t.Fatalf("Generate produced identical graphs for different seeds (1 vs 2); Seed is not affecting generation")
	}
}

// TestGenerateCounts pins down the deterministic (non-random) node/edge
// count arithmetic described in the task brief for Users=1000: 1 domain (<
// 50k users), Computers = Users/2, Groups = Users/5 (>=2 to hold the two
// well-known groups), plus bounds on the edge count that hold for ANY seed
// because only the group-nesting category (p=0.3 per non-special group) has
// seed-dependent count; every other edge category has a fixed, seed-
// independent count for a given Users value.
func TestGenerateCounts(t *testing.T) {
	const users = 1000
	g := Generate(Spec{Users: users, Seed: 7})

	const (
		wantUsers     = users
		wantComputers = users / 2
		wantGroups    = users / 5 // includes the 2 well-known groups
		wantNodes     = wantUsers + wantComputers + wantGroups
	)
	if len(g.Nodes) != wantNodes {
		t.Fatalf("len(Nodes) = %d, want %d (users=%d computers=%d groups=%d)", len(g.Nodes), wantNodes, wantUsers, wantComputers, wantGroups)
	}

	var users_, computers, groups int
	for _, n := range g.Nodes {
		hasBase := false
		for _, k := range n.Kinds {
			if k == "Base" {
				hasBase = true
			}
		}
		if !hasBase {
			t.Fatalf("node %q missing Base kind: %v", n.ObjectID, n.Kinds)
		}
		switch {
		case containsKind(n.Kinds, "User"):
			users_++
		case containsKind(n.Kinds, "Computer"):
			computers++
		case containsKind(n.Kinds, "Group"):
			groups++
		default:
			t.Fatalf("node %q has neither User, Computer, nor Group kind: %v", n.ObjectID, n.Kinds)
		}
		if n.Props["objectid"] != n.ObjectID {
			t.Fatalf("node %q: props[objectid] = %v, want %v", n.ObjectID, n.Props["objectid"], n.ObjectID)
		}
		if n.Props["name"] == "" || n.Props["name"] == nil {
			t.Fatalf("node %q: props[name] is empty", n.ObjectID)
		}
	}
	if users_ != wantUsers {
		t.Errorf("User nodes = %d, want %d", users_, wantUsers)
	}
	if computers != wantComputers {
		t.Errorf("Computer nodes = %d, want %d", computers, wantComputers)
	}
	if groups != wantGroups {
		t.Errorf("Group nodes = %d, want %d", groups, wantGroups)
	}

	// Upper bound from the pre-dedup edge categories for users=1000,
	// domains=1 (Generate dedups identical (start,end,kind) triples before
	// returning, since the database enforces that uniqueness -- see
	// dedupEdges's doc comment -- so this is an upper bound, not exact):
	//   MemberOf hub:    users              = 1000
	//   extra MemberOf:  3 * users          = 3000
	//   AdminTo:         round(0.3*C)       =  150
	//   HasSession:      round(0.1*C)       =   50
	//   ACL:             round(1.5*users)+2 = 1502
	//   nesting:         seed-dependent, in [0, groups-2]
	const fixedEdges = wantUsers + 3*wantUsers + 150 + 50 + (1502)
	maxEdges := fixedEdges + (wantGroups - 2)
	// Lower bound: every user's MemberOf-to-hub edge is unique by
	// construction (distinct StartIdx per user, same EndIdx/Kind), so
	// dedup can never remove any of those.
	minEdges := wantUsers
	if len(g.Edges) < minEdges || len(g.Edges) > maxEdges {
		t.Fatalf("len(Edges) = %d, want in [%d, %d]", len(g.Edges), minEdges, maxEdges)
	}
}

func containsKind(kinds []string, k string) bool {
	for _, kind := range kinds {
		if kind == k {
			return true
		}
	}
	return false
}

// TestGenerateOneDomainAdminsGroupPerDomain asserts that every domain has
// exactly one group whose objectid carries the well-known Domain Admins RID
// suffix "-512".
func TestGenerateOneDomainAdminsGroupPerDomain(t *testing.T) {
	g := Generate(Spec{Users: 1000, Seed: 3})

	domains := domainCount(1000)

	count := 0
	for _, n := range g.Nodes {
		if strings.HasSuffix(n.ObjectID, "-512") {
			count++
		}
	}
	if count != domains {
		t.Fatalf("found %d nodes with a -512 objectid suffix, want exactly %d (one per domain)", count, domains)
	}
}

// TestGenerateEdgesReferenceValidIndices asserts every edge's StartIdx and
// EndIdx address a node actually present in Nodes.
func TestGenerateEdgesReferenceValidIndices(t *testing.T) {
	g := Generate(Spec{Users: 1000, Seed: 9})

	n := len(g.Nodes)
	for i, e := range g.Edges {
		if e.StartIdx < 0 || e.StartIdx >= n {
			t.Fatalf("edge[%d] StartIdx=%d out of range [0,%d)", i, e.StartIdx, n)
		}
		if e.EndIdx < 0 || e.EndIdx >= n {
			t.Fatalf("edge[%d] EndIdx=%d out of range [0,%d)", i, e.EndIdx, n)
		}
		if e.Kind == "" {
			t.Fatalf("edge[%d] has an empty Kind", i)
		}
	}
}

// TestGenerateHasPathFromUserToDomainAdmins asserts that the generated graph
// always contains at least one directed, multi-hop path from some User node
// to some Domain Admins ("-512") group node, found via a plain BFS over the
// generated edge slice (forward direction: StartIdx -> EndIdx).
func TestGenerateHasPathFromUserToDomainAdmins(t *testing.T) {
	g := Generate(Spec{Users: 1000, Seed: 11})

	adj := make(map[int][]int, len(g.Nodes))
	for _, e := range g.Edges {
		adj[e.StartIdx] = append(adj[e.StartIdx], e.EndIdx)
	}

	isAdmins := make([]bool, len(g.Nodes))
	for i, n := range g.Nodes {
		if strings.HasSuffix(n.ObjectID, "-512") {
			isAdmins[i] = true
		}
	}

	found := false
	for start, n := range g.Nodes {
		if !containsKind(n.Kinds, "User") {
			continue
		}

		visited := make([]bool, len(g.Nodes))
		visited[start] = true
		queue := []int{start}
		depth := map[int]int{start: 0}

		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]

			if isAdmins[cur] && depth[cur] > 0 {
				if depth[cur] >= 2 {
					found = true
				}
				break
			}

			for _, next := range adj[cur] {
				if !visited[next] {
					visited[next] = true
					depth[next] = depth[cur] + 1
					queue = append(queue, next)
				}
			}
		}

		if found {
			break
		}
	}

	if !found {
		t.Fatalf("no multi-hop path found from any User node to a Domain Admins (-512) group node")
	}
}
