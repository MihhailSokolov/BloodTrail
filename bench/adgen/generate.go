// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"math"
	"math/rand"
)

// Spec parameterizes a synthetic AD-shaped graph: how many user principals
// to generate (which drives every other count, see Generate's doc comment),
// the seed a run is reproducible from, and optionally the number of domains.
type Spec struct {
	Users   int
	Seed    int64
	Domains int // 0 (default) keeps automatic 1-per-50k rule; N > 0 forces exactly N domains
}

// Node is a generated graph vertex. ObjectID is the synthetic AD objectid
// (a SID-like string); Kinds always includes "Base" plus exactly one of
// "User", "Computer", or "Group"; Props always carries "objectid" and
// "name".
type Node struct {
	ObjectID string
	Kinds    []string
	Props    map[string]any
}

// Edge is a generated graph relationship, referencing its endpoints by
// index into the Graph's Nodes slice.
type Edge struct {
	StartIdx int
	EndIdx   int
	Kind     string
}

// Graph is the in-memory result of Generate: every node and edge to be
// loaded into the target database.
type Graph struct {
	Nodes []Node
	Edges []Edge
}

// Node/edge kind name constants. These are the exact strings adgen writes
// as graph.StringKind kinds through the pg driver's KindMapper.
const (
	KindBase     = "Base"
	KindUser     = "User"
	KindComputer = "Computer"
	KindGroup    = "Group"

	EdgeMemberOf   = "MemberOf"
	EdgeAdminTo    = "AdminTo"
	EdgeHasSession = "HasSession"
	EdgeGenericAll = "GenericAll"
	EdgeWriteDacl  = "WriteDacl"
	EdgeAddMember  = "AddMember"
)

// NodeKinds and EdgeKinds list every kind string Generate can emit, in a
// fixed order, so callers (main.go's write path) can pre-assert the full
// kind catalog once instead of discovering it by scanning the graph.
var (
	NodeKinds = []string{KindBase, KindUser, KindComputer, KindGroup}
	EdgeKinds = []string{EdgeMemberOf, EdgeAdminTo, EdgeHasSession, EdgeGenericAll, EdgeWriteDacl, EdgeAddMember}
)

// usersPerDomainCap is the scale factor from the task brief: one domain per
// 50k users, minimum one domain.
const usersPerDomainCap = 50_000

// Edge-density budgets. These are deliberately linear in Users/Computers
// (never the product of two counts that both grow with -users, which would
// make large runs quadratic and computationally infeasible -- see the
// AdminTo design note below) but are sized so that a default run (no flags
// beyond -users) lands close to the milestone's ~10-edges-per-node design
// point -- see Generate's doc comment and README.md for the worked
// numbers. Raised from an earlier, much sparser pass (extraMemberOfPerUser
// 3, aclDensityPerUser 1.5, adminToCoverage 0.3, hasSessionCoverage 0.1)
// that only reached ~3.4 edges/node.
const (
	// extraMemberOfPerUser is the number of additional MemberOf edges each
	// user gets to random groups, on top of the one guaranteed MemberOf
	// edge to the domain's Domain Users hub.
	extraMemberOfPerUser = 8

	// adminToCoverage is the fraction of a domain's computers that receive
	// an AdminTo edge from the domain's admin-group pool.
	adminToCoverage = 0.5

	// hasSessionCoverage is the fraction of a domain's computers that get
	// a HasSession edge to a random user.
	hasSessionCoverage = 0.3

	// aclDensityPerUser is the number of ACL edges (GenericAll, WriteDacl,
	// AddMember) generated per user in the domain.
	aclDensityPerUser = 8.0
)

// domainCount returns the number of domains Generate splits users users
// across for the given Spec.Users value: floor(users/50000), minimum 1.
func domainCount(users int) int {
	if users < 0 {
		users = 0
	}
	d := users / usersPerDomainCap
	if d < 1 {
		d = 1
	}
	return d
}

// partitionInts splits total as evenly as possible across parts buckets,
// deterministically: the first (total % parts) buckets get one extra unit.
func partitionInts(total, parts int) []int {
	out := make([]int, parts)
	base := total / parts
	rem := total % parts
	for i := 0; i < parts; i++ {
		out[i] = base
		if i < rem {
			out[i]++
		}
	}
	return out
}

// domain holds the node indices (into Graph.Nodes) belonging to one
// generated AD domain, used while wiring up edges after every domain's
// nodes have been emitted.
type domain struct {
	adminsIdx  int   // index of the Domain Admins group (-512)
	usersHubIx int   // index of the Domain Users group (-513)
	groupIdx   []int // every group in the domain, groupIdx[0]==adminsIdx, groupIdx[1]==usersHubIx
	userIdx    []int
	compIdx    []int
}

// Generate deterministically builds a synthetic Active-Directory-shaped
// graph from spec: same Spec always produces byte-for-byte the same Graph
// (Generate never consults map iteration order, wall-clock time, or any
// other nondeterministic input -- the only randomness is a *rand.Rand
// seeded from spec.Seed, consumed in a fixed order that depends only on
// spec.Users).
//
// Shape (see the task-15 brief for the source of these ratios):
//
//   - domains = spec.Domains if > 0, else max(1, Users/50000); Users,
//     Computers (=Users/2 overall) and Groups (=Users/5 overall, floor,
//     minimum 2 to hold the two well-known groups below) are partitioned
//     across domains as evenly as possible.
//   - every domain has exactly one Domain Admins group (objectid suffix
//     "-512") and one Domain Users hub (objectid suffix "-513").
//   - every user MemberOf the domain's Domain Users hub.
//   - groups nest into an earlier group in the same domain with p=0.3,
//     capped at nesting depth 5.
//   - every user gets extraMemberOfPerUser (8) extra MemberOf edges to
//     random groups.
//   - a small set of "admin groups" (~2% of a domain's non-special groups)
//     hold AdminTo over adminToCoverage (50%) of the domain's computers.
//   - hasSessionCoverage (30%) of computers HasSession to a random user.
//   - ACL edges (GenericAll, WriteDacl, AddMember) run from random
//     users/groups to random groups, combined density aclDensityPerUser
//     (8.0) per user, with one edge pair per domain deliberately chained
//     (User -GenericAll-> group -AddMember-> Domain Admins) so a
//     multi-hop path to Domain Admins always exists.
//
// These per-user/per-computer budgets are deliberately kept linear in
// Users (see the AdminTo/HasSession design note below) but were sized so
// that a *default* run (no flags beyond -users) lands close to the
// milestone's ~10-edges-per-node design point: at Users=100000 (2
// domains) this produces a ~10.3 edges/node graph (see
// TestGenerateDefaultEdgeDensityNear10), and the same per-domain
// proportions carry through to -users 2800000 (56 domains of 50,000
// users each) -- see README.md for the worked-out formula numbers at
// that scale.
//
// Node kinds are always ["Base", <User|Computer|Group>]; properties are
// always {"objectid": <ObjectID>, "name": <display name>}.
func Generate(spec Spec) Graph {
	users := spec.Users
	if users < 0 {
		users = 0
	}

	domains := spec.Domains
	if domains <= 0 {
		domains = domainCount(users)
	}
	usersPerDomain := partitionInts(users, domains)

	rng := rand.New(rand.NewSource(spec.Seed))

	var nodes []Node
	var edges []Edge

	domainInfo := make([]domain, domains)

	for d := 0; d < domains; d++ {
		u := usersPerDomain[d]
		c := u / 2
		g := u / 5
		if g < 2 {
			g = 2
		}

		sid := fmt.Sprintf("S-1-5-21-%d-%d-%d", rng.Uint32(), rng.Uint32(), rng.Uint32())
		rid := 1000

		info := domain{}

		info.adminsIdx = len(nodes)
		nodes = append(nodes, newNode(sid+"-512", KindGroup, fmt.Sprintf("DOMAIN ADMINS@DOMAIN%d.SYNTH", d)))

		info.usersHubIx = len(nodes)
		nodes = append(nodes, newNode(sid+"-513", KindGroup, fmt.Sprintf("DOMAIN USERS@DOMAIN%d.SYNTH", d)))

		info.groupIdx = []int{info.adminsIdx, info.usersHubIx}
		for gi := 2; gi < g; gi++ {
			oid := fmt.Sprintf("%s-%d", sid, rid)
			rid++
			idx := len(nodes)
			nodes = append(nodes, newNode(oid, KindGroup, fmt.Sprintf("GROUP%d@DOMAIN%d.SYNTH", gi, d)))
			info.groupIdx = append(info.groupIdx, idx)
		}

		info.userIdx = make([]int, 0, u)
		for ui := 0; ui < u; ui++ {
			oid := fmt.Sprintf("%s-%d", sid, rid)
			rid++
			idx := len(nodes)
			nodes = append(nodes, newNode(oid, KindUser, fmt.Sprintf("USER%d@DOMAIN%d.SYNTH", ui, d)))
			info.userIdx = append(info.userIdx, idx)
		}

		info.compIdx = make([]int, 0, c)
		for ci := 0; ci < c; ci++ {
			oid := fmt.Sprintf("%s-%d", sid, rid)
			rid++
			idx := len(nodes)
			nodes = append(nodes, newNode(oid, KindComputer, fmt.Sprintf("COMPUTER%d.DOMAIN%d.SYNTH", ci, d)))
			info.compIdx = append(info.compIdx, idx)
		}

		domainInfo[d] = info
	}

	for d := 0; d < domains; d++ {
		edges = generateDomainEdges(edges, domainInfo[d], rng)
	}

	return Graph{Nodes: nodes, Edges: dedupEdges(edges)}
}

// edgeKey identifies an edge by its (start, end, kind) triple -- exactly
// the tuple the database's edge table enforces uniqueness over (unique
// (start_id, end_id, kind_id, graph_id), see schema_up.sql). The random
// edge categories above can and do collide (e.g. a user's "extra MemberOf"
// draw landing on the same group as another category), so Generate must
// dedup before returning.
type edgeKey struct {
	start, end int
	kind       string
}

// dedupEdges removes duplicate (StartIdx, EndIdx, Kind) edges, keeping the
// first occurrence in place. It never consults map iteration order -- the
// map here is purely a seen-set, and the surviving edges keep their
// original relative order -- so it does not affect Generate's
// determinism.
func dedupEdges(edges []Edge) []Edge {
	seen := make(map[edgeKey]struct{}, len(edges))
	out := edges[:0]
	for _, e := range edges {
		k := edgeKey{e.StartIdx, e.EndIdx, e.Kind}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, e)
	}
	return out
}

func newNode(objectID, kind, name string) Node {
	return Node{
		ObjectID: objectID,
		Kinds:    []string{KindBase, kind},
		Props: map[string]any{
			"objectid": objectID,
			"name":     name,
		},
	}
}

// generateDomainEdges appends every edge belonging to one domain to edges
// and returns the extended slice. It is the single place that consumes rng,
// always in the same fixed order for a given domain's node counts, which is
// what makes Generate reproducible.
func generateDomainEdges(edges []Edge, info domain, rng *rand.Rand) []Edge {
	users := info.userIdx
	comps := info.compIdx
	groups := info.groupIdx
	extraGroups := groups[2:] // every group except Domain Admins/Domain Users

	// 1. every user MemberOf the Domain Users hub.
	for _, u := range users {
		edges = append(edges, Edge{StartIdx: u, EndIdx: info.usersHubIx, Kind: EdgeMemberOf})
	}

	// 2. group nesting: each non-special group, with p=0.3, MemberOf a
	// random earlier group in the same domain, capped at nesting depth 5.
	depth := make([]int, len(groups))
	for gi := 2; gi < len(groups); gi++ {
		if rng.Float64() >= 0.3 {
			continue
		}
		cand := rng.Intn(gi)
		if depth[cand] >= 5 {
			continue
		}
		edges = append(edges, Edge{StartIdx: groups[gi], EndIdx: groups[cand], Kind: EdgeMemberOf})
		depth[gi] = depth[cand] + 1
	}

	// 3. extraMemberOfPerUser extra MemberOf per user, to a random group in
	// the domain.
	for _, u := range users {
		for k := 0; k < extraMemberOfPerUser; k++ {
			target := groups[rng.Intn(len(groups))]
			edges = append(edges, Edge{StartIdx: u, EndIdx: target, Kind: EdgeMemberOf})
		}
	}

	// 4. AdminTo: ~2% of the domain's non-special groups act as "admin
	// groups"; together they hold AdminTo over adminToCoverage of the
	// domain's computers (one admin group per targeted computer -- see the
	// package doc's AdminTo design note for why this isn't the full
	// admins x computers cross product).
	if len(extraGroups) > 0 && len(comps) > 0 {
		adminCount := round(0.02 * float64(len(extraGroups)))
		if adminCount < 1 {
			adminCount = 1
		}
		if adminCount > len(extraGroups) {
			adminCount = len(extraGroups)
		}
		adminPerm := rng.Perm(len(extraGroups))
		adminPool := make([]int, adminCount)
		for i := 0; i < adminCount; i++ {
			adminPool[i] = extraGroups[adminPerm[i]]
		}

		targetCount := round(adminToCoverage * float64(len(comps)))
		if targetCount > len(comps) {
			targetCount = len(comps)
		}
		compPerm := rng.Perm(len(comps))
		for i := 0; i < targetCount; i++ {
			comp := comps[compPerm[i]]
			group := adminPool[rng.Intn(len(adminPool))]
			edges = append(edges, Edge{StartIdx: group, EndIdx: comp, Kind: EdgeAdminTo})
		}
	}

	// 5. HasSession: hasSessionCoverage of computers each have a session of
	// a random user.
	if len(comps) > 0 && len(users) > 0 {
		sessionCount := round(hasSessionCoverage * float64(len(comps)))
		if sessionCount > len(comps) {
			sessionCount = len(comps)
		}
		compPerm := rng.Perm(len(comps))
		for i := 0; i < sessionCount; i++ {
			comp := comps[compPerm[i]]
			user := users[rng.Intn(len(users))]
			edges = append(edges, Edge{StartIdx: comp, EndIdx: user, Kind: EdgeHasSession})
		}
	}

	// 6. ACL edges: combined density aclDensityPerUser per user, source is
	// a random user or group, target is a random group, kind is one of
	// GenericAll, WriteDacl, AddMember.
	aclKinds := [...]string{EdgeGenericAll, EdgeWriteDacl, EdgeAddMember}
	aclCount := round(aclDensityPerUser * float64(len(users)))
	for i := 0; i < aclCount; i++ {
		var src int
		if len(users) > 0 && rng.Intn(2) == 0 {
			src = users[rng.Intn(len(users))]
		} else {
			src = groups[rng.Intn(len(groups))]
		}
		tgt := groups[rng.Intn(len(groups))]
		kind := aclKinds[rng.Intn(len(aclKinds))]
		edges = append(edges, Edge{StartIdx: src, EndIdx: tgt, Kind: kind})
	}

	// 7. Guarantee at least one multi-hop path from a User to Domain
	// Admins, biasing the otherwise-random ACL noise above toward actually
	// reaching Domain Admins via a small set of paths, per the brief.
	if len(users) > 0 {
		if len(extraGroups) > 0 {
			edges = append(edges,
				Edge{StartIdx: users[0], EndIdx: extraGroups[0], Kind: EdgeGenericAll},
				Edge{StartIdx: extraGroups[0], EndIdx: info.adminsIdx, Kind: EdgeAddMember},
			)
		} else {
			edges = append(edges, Edge{StartIdx: users[0], EndIdx: info.adminsIdx, Kind: EdgeGenericAll})
		}
	}

	return edges
}

// round rounds x to the nearest integer using round-half-away-from-zero,
// matching the "~N%" ratios described in the task brief closely enough that
// precise reproduction isn't required (the brief says as much explicitly).
func round(x float64) int {
	return int(math.Round(x))
}
