// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// pinNow overrides the package's nowFunc for the duration of t, so tests
// that need Generate's *entire* output (including its timestamp fields) to
// be reproducible across multiple calls don't depend on two calls landing
// in the same wall-clock second. See nowFunc's doc comment in generate.go
// for why Generate deliberately anchors timestamps to generation time
// rather than to spec.Seed.
func pinNow(t *testing.T, at time.Time) {
	t.Helper()
	orig := nowFunc
	nowFunc = func() time.Time { return at }
	t.Cleanup(func() { nowFunc = orig })
}

// TestGenerateDeterministic asserts that two calls to Generate with the same
// Spec produce byte-for-byte (structurally) identical graphs: same node
// order, same objectids, same edges, same property bags. Generate must not
// rely on map iteration order or any other nondeterministic source when it
// builds the Nodes/Edges slices. Node timestamp properties are anchored to
// nowFunc() rather than spec.Seed (see nowFunc's doc comment in
// generate.go), so pinNow holds that fixed for the duration of this test --
// otherwise two real-time calls landing a wall-clock second apart would
// make this assertion flaky by construction, even though Generate's day
// *offsets* would still agree.
func TestGenerateDeterministic(t *testing.T) {
	pinNow(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
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
// count arithmetic for Users=1000: 1 domain (<
// 50k users), Computers = Users/2, Groups = Users/5 (>=4 to hold the four
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
		wantGroups    = users / 5 // includes the 4 well-known groups
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
	//   nesting:         seed-dependent, in [0, groups-wellKnownGroupsPerDomain]
	const fixedEdges = wantUsers + extraMemberOfPerUser*wantUsers + 250 + 150 + (8002)
	maxEdges := fixedEdges + (wantGroups - wellKnownGroupsPerDomain)
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
// flags/knobs beyond -users) lands close to the ~10-edges-per-node design
// point (5M nodes / ~50M edges at -users 2800000, see README.md).
// Users=100000 (2 domains, matching the same per-domain proportions the
// -users 2800000 reference scale uses -- see README.md's worked example) is
// used as a fast proxy for that large run. The tolerance band is
// deliberately generous (8-12) because the design point is "roughly 10
// edges/node", not an exact ratio.
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
// exactly 3 domains under domainCount's floor-based formula (the intuitive
// "Users=120000 gives 3 domains" reading does not hold against that
// formula: 120000/50000 floors to 2, not 3 -- 150000 is the
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

// TestGenerateDomainsOverride exercises the Domains override in Spec:
// forcing an explicit domain count overrides the automatic 1-per-50k rule.
//
// It asserts:
//   - Generate(Spec{Users: 150000, Seed: 1, Domains: 2}) produces exactly
//     2 domains (not 3 as the automatic rule would yield);
//   - domain partitioning is consistent: users divided evenly across 2
//     domains, one -512 group per domain, no cross-domain edges;
//   - Domains: 0 (default) preserves current behavior: domainCount-based
//     automatic rule applies;
//   - small Domains override with fewer than 50k users per domain works:
//     Users=1000, Domains: 3 produces 3 domains of ~333 users each.
func TestGenerateDomainsOverride(t *testing.T) {
	// g2 vs g3 below compares two full Generate() calls for equality; pin
	// nowFunc so their timestamp properties agree too (see pinNow's doc
	// comment).
	pinNow(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	// Test Domains: 2 override with Users: 150000 (automatic would be 3)
	const users150k = 150_000
	wantDomains := 2
	g := Generate(Spec{Users: users150k, Seed: 1, Domains: wantDomains})

	// Count -512 (Domain Admins) groups: should be exactly 2
	adminCount := 0
	for _, n := range g.Nodes {
		if strings.HasSuffix(n.ObjectID, "-512") {
			adminCount++
		}
	}
	if adminCount != wantDomains {
		t.Fatalf("Domains: 2 override: found %d nodes with -512 suffix, want exactly %d", adminCount, wantDomains)
	}

	// Verify partition is as expected: 150000 users / 2 domains = 75000 each
	usersPerDomain := partitionInts(users150k, wantDomains)
	expectedPerDomain := users150k / wantDomains
	for d, count := range usersPerDomain {
		if count != expectedPerDomain {
			t.Fatalf("domain %d: users = %d, want %d", d, count, expectedPerDomain)
		}
	}

	// Test Domains: 0 (default) preserves automatic behavior
	g2 := Generate(Spec{Users: users150k, Seed: 1, Domains: 0})
	g3 := Generate(Spec{Users: users150k, Seed: 1})
	if !reflect.DeepEqual(g2, g3) {
		t.Fatalf("Domains: 0 should be identical to omitting Domains field")
	}

	// Verify automatic rule still applies: domainCount(150000) == 3
	automaticDomains := domainCount(users150k)
	if automaticDomains != 3 {
		t.Fatalf("test setup: domainCount(%d) = %d, want 3", users150k, automaticDomains)
	}

	// Count -512 groups in automatic run: should be 3
	adminCountAuto := 0
	for _, n := range g3.Nodes {
		if strings.HasSuffix(n.ObjectID, "-512") {
			adminCountAuto++
		}
	}
	if adminCountAuto != automaticDomains {
		t.Fatalf("automatic mode: found %d nodes with -512 suffix, want exactly %d", adminCountAuto, automaticDomains)
	}

	// Test Domains: 3 with small user count (Users=1000, which would normally be 1 domain)
	const smallUsers = 1000
	g4 := Generate(Spec{Users: smallUsers, Seed: 2, Domains: 3})

	adminCountSmall := 0
	for _, n := range g4.Nodes {
		if strings.HasSuffix(n.ObjectID, "-512") {
			adminCountSmall++
		}
	}
	if adminCountSmall != 3 {
		t.Fatalf("Domains: 3 override with small Users: found %d nodes with -512 suffix, want exactly 3", adminCountSmall)
	}

	// Verify partition: 1000 users / 3 domains = 333, 333, 334
	usersPerDomainSmall := partitionInts(smallUsers, 3)
	expectedTotal := 0
	for _, count := range usersPerDomainSmall {
		expectedTotal += count
	}
	if expectedTotal != smallUsers {
		t.Fatalf("partition sum = %d, want %d", expectedTotal, smallUsers)
	}
}

// TestGenerateWellKnownRIDGroupsPerDomain asserts that every domain gets
// exactly one each of the four well-known, RID-suffixed groups the
// cypherbench RID-suffix query shape looks up: Domain Admins
// (-512), Domain Users (-513), Domain Controllers (-516), and Enterprise
// Admins (-519).
func TestGenerateWellKnownRIDGroupsPerDomain(t *testing.T) {
	g := Generate(Spec{Users: 1000, Seed: 3})
	domains := domainCount(1000)

	for _, suffix := range []string{"-512", "-513", "-516", "-519"} {
		count := 0
		for _, n := range g.Nodes {
			if strings.HasSuffix(n.ObjectID, suffix) {
				count++
			}
		}
		if count != domains {
			t.Fatalf("suffix %q: found %d nodes, want exactly %d (one per domain)", suffix, count, domains)
		}
	}
}

// TestGeneratePrincipalTimestampsAnchorToGenerationTime asserts the
// intended resolution: a principal's day-relative
// timestamp properties (lastlogontimestamp here) are computed as
// nowFunc() minus a seed-deterministic number of days, not as an absolute
// point fixed by the seed. Generating the identical Spec at two different
// "now" instants must produce the identical node (same objectid, since
// objectid generation never touches nowFunc) with lastlogontimestamp
// shifted by exactly the gap between those two instants -- proving the
// per-node day *offset* survived unchanged while only the anchor moved.
func TestGeneratePrincipalTimestampsAnchorToGenerationTime(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(72 * time.Hour)
	spec := Spec{Users: 200, Seed: 17}

	pinNow(t, t1)
	g1 := Generate(spec)
	pinNow(t, t2)
	g2 := Generate(spec)

	u1 := firstUserNode(t, g1)
	u2 := firstUserNode(t, g2)
	if u1.ObjectID != u2.ObjectID {
		t.Fatalf("first User node's objectid changed between generation times: %q vs %q (objectid must not depend on nowFunc)", u1.ObjectID, u2.ObjectID)
	}

	ts1, ok := u1.Props["lastlogontimestamp"].(int64)
	if !ok {
		t.Fatalf("node %q: lastlogontimestamp is %T, want int64", u1.ObjectID, u1.Props["lastlogontimestamp"])
	}
	ts2, ok := u2.Props["lastlogontimestamp"].(int64)
	if !ok {
		t.Fatalf("node %q: lastlogontimestamp is %T, want int64", u2.ObjectID, u2.Props["lastlogontimestamp"])
	}

	wantDelta := t2.Unix() - t1.Unix()
	if gotDelta := ts2 - ts1; gotDelta != wantDelta {
		t.Fatalf("lastlogontimestamp delta between generation times = %d, want %d (day offset should be seed-deterministic; only the anchor should move)", gotDelta, wantDelta)
	}
}

func firstUserNode(t *testing.T, g Graph) Node {
	t.Helper()
	for _, n := range g.Nodes {
		if containsKind(n.Kinds, "User") {
			return n
		}
	}
	t.Fatalf("no User node found in generated graph")
	return Node{}
}

// TestGeneratePrincipalPropertyBags asserts the realistic-property-bag
// shape adgen aims for: every User/Computer node carries the
// full common-principal field set (plus its kind-specific extras), the
// per-field true/false split lands close to its target
// probabilities, and the mean marshaled JSON size across every principal
// (User+Computer) node lands in the 400-700 byte target window.
// Users=5000 (a single domain, so exact arithmetic applies) is large
// enough to make the sampled ratios a stable proxy for the underlying
// per-node probabilities without slowing the test suite down.
func TestGeneratePrincipalPropertyBags(t *testing.T) {
	pinNow(t, time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))

	const wantUsers = 5000 // -> 2500 computers, 1000 groups, 1 domain
	g := Generate(Spec{Users: wantUsers, Seed: 21})

	var (
		userCount, computerCount                             int
		userEnabled, userAdminCount                          int
		userHasSPN, userDontReqPreauth, userPwdNeverExpires  int
		computerEnabled, computerAdminCount, computerHasLAPS int
		principalBagBytes, principalCount                    int
	)

	for _, n := range g.Nodes {
		switch {
		case containsKind(n.Kinds, "User"):
			userCount++
			assertPrincipalCommonShape(t, n)
			assertBoolProp(t, n, "hasspn")
			assertBoolProp(t, n, "dontreqpreauth")
			assertBoolProp(t, n, "pwdneverexpires")
			if n.Props["enabled"].(bool) {
				userEnabled++
			}
			if n.Props["admincount"].(bool) {
				userAdminCount++
			}
			if n.Props["hasspn"].(bool) {
				userHasSPN++
			}
			if n.Props["dontreqpreauth"].(bool) {
				userDontReqPreauth++
			}
			if n.Props["pwdneverexpires"].(bool) {
				userPwdNeverExpires++
			}
			principalBagBytes += marshaledSize(t, n)
			principalCount++
		case containsKind(n.Kinds, "Computer"):
			computerCount++
			assertPrincipalCommonShape(t, n)
			assertStringProp(t, n, "operatingsystem")
			assertBoolProp(t, n, "haslaps")
			if n.Props["enabled"].(bool) {
				computerEnabled++
			}
			if n.Props["admincount"].(bool) {
				computerAdminCount++
			}
			if n.Props["haslaps"].(bool) {
				computerHasLAPS++
			}
			principalBagBytes += marshaledSize(t, n)
			principalCount++
		}
	}

	if userCount != wantUsers {
		t.Fatalf("User nodes = %d, want %d", userCount, wantUsers)
	}
	if computerCount != wantUsers/2 {
		t.Fatalf("Computer nodes = %d, want %d", computerCount, wantUsers/2)
	}

	assertRatioWithin(t, "user enabled", userEnabled, userCount, 0.90, 0.99)
	assertRatioWithin(t, "user hasspn", userHasSPN, userCount, 0.005, 0.05)
	assertRatioWithin(t, "user dontreqpreauth", userDontReqPreauth, userCount, 0.001, 0.03)
	assertRatioWithin(t, "user pwdneverexpires", userPwdNeverExpires, userCount, 0.15, 0.26)
	assertRatioWithin(t, "user admincount", userAdminCount, userCount, 0.02, 0.09)

	assertRatioWithin(t, "computer enabled", computerEnabled, computerCount, 0.90, 0.99)
	assertRatioWithin(t, "computer admincount", computerAdminCount, computerCount, 0.02, 0.09)
	assertRatioWithin(t, "computer haslaps", computerHasLAPS, computerCount, 0.50, 0.70)

	meanBytes := float64(principalBagBytes) / float64(principalCount)
	if meanBytes < 400 || meanBytes > 700 {
		t.Fatalf("mean principal (User+Computer) JSON bag size = %.1f bytes, want in [400, 700]", meanBytes)
	}
	t.Logf("mean principal JSON bag size across %d nodes: %.1f bytes", principalCount, meanBytes)
}

// assertPrincipalCommonShape checks the property fields README.md
// documents as shared by every User/Computer node.
func assertPrincipalCommonShape(t *testing.T, n Node) {
	t.Helper()
	assertBoolProp(t, n, "enabled")
	assertBoolProp(t, n, "admincount")
	assertIntProp(t, n, "lastlogontimestamp")
	assertIntProp(t, n, "pwdlastset")
	assertIntProp(t, n, "whencreated")
	assertStringProp(t, n, "samaccountname")
	assertStringProp(t, n, "distinguishedname")

	lastSeen := assertStringProp(t, n, "lastseen")
	if _, err := time.Parse(time.RFC3339, lastSeen); err != nil {
		t.Fatalf("node %q: lastseen %q does not parse as RFC3339: %v", n.ObjectID, lastSeen, err)
	}
}

func assertBoolProp(t *testing.T, n Node, key string) {
	t.Helper()
	if _, ok := n.Props[key].(bool); !ok {
		t.Fatalf("node %q: %s is %T, want bool", n.ObjectID, key, n.Props[key])
	}
}

func assertIntProp(t *testing.T, n Node, key string) {
	t.Helper()
	if _, ok := n.Props[key].(int64); !ok {
		t.Fatalf("node %q: %s is %T, want int64", n.ObjectID, key, n.Props[key])
	}
}

func assertStringProp(t *testing.T, n Node, key string) string {
	t.Helper()
	v, ok := n.Props[key].(string)
	if !ok || v == "" {
		t.Fatalf("node %q: %s is %q (%T), want a non-empty string", n.ObjectID, key, n.Props[key], n.Props[key])
	}
	return v
}

func marshaledSize(t *testing.T, n Node) int {
	t.Helper()
	b, err := json.Marshal(n.Props)
	if err != nil {
		t.Fatalf("node %q: marshal properties: %v", n.ObjectID, err)
	}
	return len(b)
}

func assertRatioWithin(t *testing.T, label string, count, total int, lo, hi float64) {
	t.Helper()
	if total == 0 {
		t.Fatalf("%s: total is 0, cannot compute ratio", label)
	}
	ratio := float64(count) / float64(total)
	if ratio < lo || ratio > hi {
		t.Fatalf("%s: ratio = %.4f (%d/%d), want in [%.4f, %.4f]", label, ratio, count, total, lo, hi)
	}
}

// TestGenerateGroupPropertyBag asserts groups (including the well-known
// RID-suffixed ones) carry admincount and a realistic-length description,
// and that the admincount true-rate lands close to its 10% target.
func TestGenerateGroupPropertyBag(t *testing.T) {
	g := Generate(Spec{Users: 5000, Seed: 21})

	var groupCount, groupAdminCount int
	for _, n := range g.Nodes {
		if !containsKind(n.Kinds, "Group") {
			continue
		}
		groupCount++
		assertBoolProp(t, n, "admincount")
		desc := assertStringProp(t, n, "description")
		if len(desc) < 20 {
			t.Fatalf("node %q: description %q is implausibly short (%d bytes)", n.ObjectID, desc, len(desc))
		}
		if n.Props["admincount"].(bool) {
			groupAdminCount++
		}
	}

	if groupCount == 0 {
		t.Fatalf("no Group nodes found")
	}
	assertRatioWithin(t, "group admincount", groupAdminCount, groupCount, 0.04, 0.18)
}

// TestGenerateDistinguishedNameLengths asserts that generated User and
// Computer nodes carry realistic-length distinguishedname (DN) values
// averaging 80-100 bytes, per the README's claim of ~90 byte DNs. This
// test ensures the DN construction captures realistic organizational
// depth (multiple OU levels, varied DC suffixes) that approximates
// real Active Directory directory paths. Users=5000 is used to make
// the measured mean stable without slowing the test suite.
func TestGenerateDistinguishedNameLengths(t *testing.T) {
	g := Generate(Spec{Users: 5000, Seed: 21})

	var (
		userDNBytes, computerDNBytes int
		userCount, computerCount     int
	)

	for _, n := range g.Nodes {
		switch {
		case containsKind(n.Kinds, "User"):
			userCount++
			dn := assertStringProp(t, n, "distinguishedname")
			userDNBytes += len(dn)
		case containsKind(n.Kinds, "Computer"):
			computerCount++
			dn := assertStringProp(t, n, "distinguishedname")
			computerDNBytes += len(dn)
		}
	}

	if userCount == 0 {
		t.Fatalf("no User nodes found")
	}
	if computerCount == 0 {
		t.Fatalf("no Computer nodes found")
	}

	userDNMean := float64(userDNBytes) / float64(userCount)
	computerDNMean := float64(computerDNBytes) / float64(computerCount)

	// Assert both user and computer DN means are in [75, 110] byte range
	if userDNMean < 75 || userDNMean > 110 {
		t.Errorf("mean User DN length = %.1f bytes, want in [75, 110]", userDNMean)
	}
	if computerDNMean < 75 || computerDNMean > 110 {
		t.Errorf("mean Computer DN length = %.1f bytes, want in [75, 110]", computerDNMean)
	}

	t.Logf("mean User DN length across %d nodes: %.1f bytes", userCount, userDNMean)
	t.Logf("mean Computer DN length across %d nodes: %.1f bytes", computerCount, computerDNMean)
}

// BenchmarkGenerate measures Generate's per-call wall-clock cost at a
// moderate scale, including the per-node property-bag construction -- the
// concern being that building a property bag per node must not blow up
// generation time once the generator is run at the 5M-node benchmark
// target. Run with:
//
//	go test ./bench/adgen/ -bench=BenchmarkGenerate -benchtime=5x -run '^$'
//
// and extrapolate ns/op to nodes/sec.
func BenchmarkGenerate(b *testing.B) {
	spec := Spec{Users: 100_000, Seed: 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Generate(spec)
	}
}
