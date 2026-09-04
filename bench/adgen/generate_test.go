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
	//   MemberOf hub:    users                              = 1000
	//   extra MemberOf:  extraMemberOfPerUser * users        = 8000
	//   AdminTo:         round(adminToCoverage*C)            =  250
	//   HasSession:      round(hasSessionCoverage*C)         =  150
	//   ACL:             round(aclDensityPerUser*users)+2    = 8002
	//   nesting:         seed-dependent, in [0, groups-2]
	const fixedEdges = wantUsers + extraMemberOfPerUser*wantUsers + 250 + 150 + (8002)
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

// TestGenerateDefaultEdgeDensityNear10 asserts that a *default* run (no
// flags/knobs beyond -users) lands close to the milestone's ~10-edges-per-
// node design point (5M nodes / ~50M edges at -users 2800000, see the
// task-15 brief and README.md). Users=100000 (2 domains, matching the
// same per-domain proportions the -users 2800000 reference scale uses --
// see README.md's worked example) is used as a fast proxy for that large
// run. The tolerance band is deliberately generous (8-12) since the brief
// only asks for "roughly 10 edges/node", not an exact ratio.
func TestGenerateDefaultEdgeDensityNear10(t *testing.T) {
	g := Generate(Spec{Users: 100_000, Seed: 1})

	ratio := float64(len(g.Edges)) / float64(len(g.Nodes))
	if ratio < 8 || ratio > 12 {
		t.Fatalf("edges/node ratio = %.2f (%d edges / %d nodes), want in [8, 12]", ratio, len(g.Edges), len(g.Nodes))
	}
}

// TestGenerateMultiDomainIsolation exercises the domainCount > 1 path,
// which every other test in this file avoids (they all use Users < 50000,
// i.e. a single domain). Users=150000 is the smallest input that produces
// exactly 3 domains under domainCount's floor-based formula (the task-15
// brief's illustrative "Users=120000 (3 domains)" example does not hold
// against that formula: 120000/50000 floors to 2, not 3 -- 150000 is the
// smallest exact-3-domains input and also partitions evenly, 50000 users
// per domain).
//
// It asserts:
//   - domainCount(150000) == 3 domains actually got generated (via the
//     -512 suffix count, same technique as
//     TestGenerateOneDomainAdminsGroupPerDomain);
//   - exactly one -512 (Domain Admins) group per domain;
//   - no cross-domain edge-index leakage: Generate's edge-wiring
//     (generateDomainEdges) only ever consumes node indices from the one
//     domain's own index lists (info.userIdx/compIdx/groupIdx), so by
//     construction every edge is intra-domain -- this test verifies that
//     invariant actually holds by recomputing each domain's node-index
//     range the same way Generate lays nodes out (groups, then users,
//     then computers, per domain, domains back-to-back) and checking
//     every edge's endpoints fall in the same range;
//   - the -513 (Domain Users) hub membership per domain: every user in
//     domain d has a MemberOf edge to *that* domain's hub, never another
//     domain's.
func TestGenerateMultiDomainIsolation(t *testing.T) {
	const users = 150_000
	wantDomains := domainCount(users)
	if wantDomains != 3 {
		t.Fatalf("test setup: domainCount(%d) = %d, want 3 (adjust the users constant)", users, wantDomains)
	}

	g := Generate(Spec{Users: users, Seed: 5})

	// Recompute each domain's node-index range exactly as Generate lays
	// nodes out: for each domain d, usersPerDomain[d] users, u/2
	// computers, max(2, u/5) groups, emitted groups-then-users-then-
	// computers, domains back-to-back. userRanges additionally isolates
	// the users sub-range within each domain, needed below to distinguish
	// "a user MemberOf the hub" from "a nested group MemberOf the hub"
	// (group nesting, category 2, can and does pick the hub as its random
	// earlier-group nesting target since it's just groups[1]).
	usersPerDomain := partitionInts(users, wantDomains)
	type nodeRange struct{ start, end int }
	ranges := make([]nodeRange, wantDomains)
	userRanges := make([]nodeRange, wantDomains)
	offset := 0
	for d := 0; d < wantDomains; d++ {
		u := usersPerDomain[d]
		c := u / 2
		gr := u / 5
		if gr < 2 {
			gr = 2
		}
		size := gr + u + c
		ranges[d] = nodeRange{start: offset, end: offset + size}
		userRanges[d] = nodeRange{start: offset + gr, end: offset + gr + u}
		offset += size
	}
	if offset != len(g.Nodes) {
		t.Fatalf("recomputed total node count = %d, want %d (len(g.Nodes)); domain layout assumption is stale", offset, len(g.Nodes))
	}

	domainOf := func(nodeIdx int) int {
		for d, r := range ranges {
			if nodeIdx >= r.start && nodeIdx < r.end {
				return d
			}
		}
		t.Fatalf("node index %d not in any recomputed domain range", nodeIdx)
		return -1
	}

	// Exactly one -512 group per domain, and it must fall inside that
	// domain's own recomputed range.
	adminsPerDomain := make([]int, wantDomains)
	usersHubIdxByDomain := make([]int, wantDomains)
	for i, n := range g.Nodes {
		d := domainOf(i)
		if strings.HasSuffix(n.ObjectID, "-512") {
			adminsPerDomain[d]++
		}
		if strings.HasSuffix(n.ObjectID, "-513") {
			usersHubIdxByDomain[d] = i
		}
	}
	for d, count := range adminsPerDomain {
		if count != 1 {
			t.Fatalf("domain %d: found %d nodes with a -512 objectid suffix, want exactly 1", d, count)
		}
	}

	// No cross-domain edge-index leakage: every edge's Start/End indices
	// fall in the same recomputed domain range (Generate wires up edges
	// strictly within one domain's own index lists -- there is no
	// intentionally cross-domain edge kind).
	for i, e := range g.Edges {
		sd := domainOf(e.StartIdx)
		ed := domainOf(e.EndIdx)
		if sd != ed {
			t.Fatalf("edge[%d] (%s) crosses domains: start node %d is in domain %d, end node %d is in domain %d",
				i, e.Kind, e.StartIdx, sd, e.EndIdx, ed)
		}
	}

	// -513 hub membership per domain: every user in domain d has a
	// MemberOf edge to domain d's own hub (never another domain's hub).
	// Restricted to edges whose *source* is a user (per userRanges) so
	// that group-nesting edges which happen to nest a group under the hub
	// (category 2 can pick groups[1], the hub, as its random earlier-group
	// target) don't get counted as user membership.
	isUserIdx := func(nodeIdx, d int) bool {
		r := userRanges[d]
		return nodeIdx >= r.start && nodeIdx < r.end
	}
	hubMemberCount := make([]int, wantDomains)
	for _, e := range g.Edges {
		if e.Kind != EdgeMemberOf {
			continue
		}
		for d, hubIdx := range usersHubIdxByDomain {
			if e.EndIdx == hubIdx && isUserIdx(e.StartIdx, d) {
				hubMemberCount[d]++
			}
		}
	}
	for d := 0; d < wantDomains; d++ {
		wantMembers := usersPerDomain[d]
		if hubMemberCount[d] != wantMembers {
			t.Fatalf("domain %d: %d MemberOf edges into the -513 hub, want exactly %d (one per user in the domain)", d, hubMemberCount[d], wantMembers)
		}
	}
}
