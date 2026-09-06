// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
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
// "name" plus a realistic bag of AD-shaped properties sized and
// distributed to approximate the fixture-measured upstream reality -- see
// README.md's "Property bags" section for the full shape table and the
// determinism/timestamp caveat.
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

// nowFunc returns the reference wall-clock time every generated node's
// day-relative timestamp properties (lastlogontimestamp/pwdlastset/
// whencreated/lastseen) are anchored to. It is a variable -- not a bare
// time.Now() call inlined into Generate -- purely so tests can pin it to a
// fixed instant and assert that Generate's *day offsets* are seed-
// deterministic without also asserting exact wall-clock values (which
// would make such an assertion flaky by construction). Production callers
// (main.go) never touch this.
//
// This deliberately breaks with Generate's otherwise-total determinism
// (see Generate's doc comment): real AD's own timestamp properties are
// epoch seconds compared against datetime().epochseconds - N*86400 at
// *query* time (see internal/graphtest/corpusfixture.go's "Time-dependent
// predicates" section for the same tradeoff made by the milestone-4 corpus
// fixture), so a graph whose timestamps were anchored to a seed-fixed
// point in time would silently age out of any such window the longer it
// sits unqueried after generation. Anchoring to generation time instead
// means: the same -seed and -users always produce the same *offsets*
// (which node is "recently active" vs. "stale" relative to every other
// node, and by how many days), but the *absolute* timestamps differ run to
// run by however long has passed between generation runs.
var nowFunc = time.Now

// wellKnownGroupsPerDomain is the number of well-known, RID-suffixed
// groups every domain gets up front -- Domain Admins ("-512"), Domain
// Users ("-513"), Domain Controllers ("-516"), and Enterprise Admins
// ("-519"), the four RID suffixes the cypherbench query shape (task 21)
// looks up -- before any synthetic group.
const wellKnownGroupsPerDomain = 4

// maxTimestampOffsetDays bounds how far into the past a principal's
// lastlogontimestamp/pwdlastset/whencreated can be dated, relative to
// nowFunc(): "epoch numbers spread over 0-400 days back" per the task
// brief.
const maxTimestampOffsetDays = 400

// day is a calendar day, used only to convert maxTimestampOffsetDays (and
// individual random day offsets) into a time.Duration.
const day = 24 * time.Hour

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
//     minimum 4 to hold the four well-known groups below) are partitioned
//     across domains as evenly as possible.
//   - every domain has exactly one each of four well-known, RID-suffixed
//     groups: Domain Admins ("-512"), Domain Users ("-513", every user's
//     hub), Domain Controllers ("-516"), and Enterprise Admins ("-519").
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
// Node kinds are always ["Base", <User|Computer|Group>]; properties always
// include "objectid" and "name" plus a realistic, deterministically
// generated property bag (see README.md's "Property bags" section for the
// full shape table and the timestamp-anchoring caveat covered by nowFunc's
// doc comment above).
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

	// now anchors every generated node's timestamp properties for this
	// entire call: one wall-clock read for the whole graph, not one per
	// node (which would be needlessly slow and would make same-run nodes
	// drift relative to each other for no reason). See nowFunc's doc
	// comment for why this, rather than spec.Seed, is the timestamp
	// anchor.
	now := nowFunc()

	for d := 0; d < domains; d++ {
		u := usersPerDomain[d]
		c := u / 2
		g := u / 5
		if g < wellKnownGroupsPerDomain {
			g = wellKnownGroupsPerDomain
		}

		sid := fmt.Sprintf("S-1-5-21-%d-%d-%d", rng.Uint32(), rng.Uint32(), rng.Uint32())
		domainDN := fmt.Sprintf("DC=DOMAIN%d,DC=SYNTH", d)
		rid := 1000

		info := domain{}

		info.adminsIdx = len(nodes)
		nodes = append(nodes, newGroupNode(sid+"-512", fmt.Sprintf("DOMAIN ADMINS@DOMAIN%d.SYNTH", d), rng))

		info.usersHubIx = len(nodes)
		nodes = append(nodes, newGroupNode(sid+"-513", fmt.Sprintf("DOMAIN USERS@DOMAIN%d.SYNTH", d), rng))

		domainControllersIdx := len(nodes)
		nodes = append(nodes, newGroupNode(sid+"-516", fmt.Sprintf("DOMAIN CONTROLLERS@DOMAIN%d.SYNTH", d), rng))

		enterpriseAdminsIdx := len(nodes)
		nodes = append(nodes, newGroupNode(sid+"-519", fmt.Sprintf("ENTERPRISE ADMINS@DOMAIN%d.SYNTH", d), rng))

		info.groupIdx = []int{info.adminsIdx, info.usersHubIx, domainControllersIdx, enterpriseAdminsIdx}
		for gi := wellKnownGroupsPerDomain; gi < g; gi++ {
			oid := fmt.Sprintf("%s-%d", sid, rid)
			rid++
			idx := len(nodes)
			nodes = append(nodes, newGroupNode(oid, fmt.Sprintf("GROUP%d@DOMAIN%d.SYNTH", gi, d), rng))
			info.groupIdx = append(info.groupIdx, idx)
		}

		info.userIdx = make([]int, 0, u)
		for ui := 0; ui < u; ui++ {
			oid := fmt.Sprintf("%s-%d", sid, rid)
			rid++
			idx := len(nodes)
			name := fmt.Sprintf("USER%d@DOMAIN%d.SYNTH", ui, d)
			nodes = append(nodes, newUserNode(oid, name, domainDN, rng, now))
			info.userIdx = append(info.userIdx, idx)
		}

		info.compIdx = make([]int, 0, c)
		for ci := 0; ci < c; ci++ {
			oid := fmt.Sprintf("%s-%d", sid, rid)
			rid++
			idx := len(nodes)
			name := fmt.Sprintf("COMPUTER%d.DOMAIN%d.SYNTH", ci, d)
			nodes = append(nodes, newComputerNode(oid, name, domainDN, rng, now))
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

// randomPastEpoch returns a Unix epoch-seconds timestamp somewhere in
// [0, maxTimestampOffsetDays] days before now, chosen from rng. Every call
// consumes exactly one rng.Intn draw, in the fixed order its callers make
// them, which is what keeps Generate's day *offsets* seed-deterministic
// even though now (and therefore the absolute timestamp this returns)
// varies run to run -- see nowFunc's doc comment.
func randomPastEpoch(rng *rand.Rand, now time.Time) int64 {
	offsetDays := rng.Intn(maxTimestampOffsetDays + 1)
	return now.Add(-time.Duration(offsetDays) * day).Unix()
}

// localPart returns the portion of name before its first occurrence of
// sep, or name unchanged if sep does not appear -- e.g.
// localPart("USER1@DOMAIN0.SYNTH", "@") == "USER1",
// localPart("COMPUTER1.DOMAIN0.SYNTH", ".") == "COMPUTER1".
func localPart(name, sep string) string {
	if i := strings.Index(name, sep); i >= 0 {
		return name[:i]
	}
	return name
}

// principalCommonProps builds the property-bag fields shared by User and
// Computer nodes: the fixture-measured shape (see README.md's "Property
// bags" section) of enabled/admincount flags, three day-offset timestamps,
// and the samaccountname/distinguishedname/lastseen identity fields every
// AD principal carries. Callers add their own kind-specific fields
// (hasspn etc. for User; operatingsystem/haslaps for Computer) to the
// returned map.
func principalCommonProps(objectID, name, dn, sam string, rng *rand.Rand, now time.Time) map[string]any {
	enabled := rng.Float64() < 0.95
	adminCount := rng.Float64() < 0.05
	lastLogon := randomPastEpoch(rng, now)
	pwdLastSet := randomPastEpoch(rng, now)
	whenCreated := randomPastEpoch(rng, now)

	return map[string]any{
		"objectid":           objectID,
		"name":               name,
		"enabled":            enabled,
		"admincount":         adminCount,
		"lastlogontimestamp": lastLogon,
		"pwdlastset":         pwdLastSet,
		"whencreated":        whenCreated,
		"samaccountname":     sam,
		"distinguishedname":  dn,
		"lastseen":           now.Format(time.RFC3339),
	}
}

// userTierOUs provides organizational variety in User node DN construction:
// selecting from these tier OUs (seeded randomly) creates realistic depth in
// the DN while keeping all users' DNs in a consistent namespace.
var userTierOUs = []string{
	"OU=Tier1,OU=Managed Accounts",
	"OU=Tier2,OU=Managed Accounts",
	"OU=Tier3,OU=Standard Accounts",
	"OU=Tier1,OU=Service Accounts",
	"OU=Tier2,OU=Service Accounts",
}

// newUserNode builds a User node: the shared principal bag (see
// principalCommonProps) plus the three Kerberos/password-policy flags the
// task brief calls out specifically for users.
func newUserNode(objectID, name, domainDN string, rng *rand.Rand, now time.Time) Node {
	local := localPart(name, "@")
	tierOU := userTierOUs[rng.Intn(len(userTierOUs))]
	dn := fmt.Sprintf("CN=%s,OU=Users,%s,OU=Corp Accounts,%s", local, tierOU, domainDN)
	props := principalCommonProps(objectID, name, dn, strings.ToLower(local), rng, now)
	props["hasspn"] = rng.Float64() < 0.02
	props["dontreqpreauth"] = rng.Float64() < 0.01
	props["pwdneverexpires"] = rng.Float64() < 0.20

	return Node{ObjectID: objectID, Kinds: []string{KindBase, KindUser}, Props: props}
}

// computerTierOUs provides organizational variety in Computer node DN construction:
// selecting from these tier OUs (seeded randomly) creates realistic depth in
// the DN while keeping all computers' DNs in a consistent namespace.
var computerTierOUs = []string{
	"OU=Tier1,OU=Workstations",
	"OU=Tier2,OU=Workstations",
	"OU=Tier3,OU=Workstations",
	"OU=Tier1,OU=Servers",
	"OU=Tier2,OU=Servers",
}

// newComputerNode builds a Computer node: the shared principal bag plus
// operatingsystem/haslaps.
func newComputerNode(objectID, name, domainDN string, rng *rand.Rand, now time.Time) Node {
	local := localPart(name, ".")
	tierOU := computerTierOUs[rng.Intn(len(computerTierOUs))]
	dn := fmt.Sprintf("CN=%s,OU=Computers,%s,OU=Corp Assets,%s", local, tierOU, domainDN)
	sam := strings.ToUpper(local) + "$"
	props := principalCommonProps(objectID, name, dn, sam, rng, now)
	props["operatingsystem"] = pickOperatingSystem(rng)
	props["haslaps"] = rng.Float64() < 0.60

	return Node{ObjectID: objectID, Kinds: []string{KindBase, KindComputer}, Props: props}
}

// newGroupNode builds a Group node: objectid/name plus admincount (10%)
// and a realistic-length description, per the task brief. Well-known
// groups (Domain Admins, Domain Users, Domain Controllers, Enterprise
// Admins) use this same constructor as ordinary synthetic groups -- only
// their objectid's RID suffix and name distinguish them.
func newGroupNode(objectID, name string, rng *rand.Rand) Node {
	return Node{
		ObjectID: objectID,
		Kinds:    []string{KindBase, KindGroup},
		Props: map[string]any{
			"objectid":    objectID,
			"name":        name,
			"admincount":  rng.Float64() < 0.10,
			"description": pickGroupDescription(rng),
		},
	}
}

// weightedOS is one entry in operatingSystems' cumulative-weight
// distribution.
type weightedOS struct {
	os     string
	weight float64
}

// operatingSystems is Computer nodes' operatingsystem distribution: mostly
// modern builds, with a legacy tail whose strings are deliberately drawn
// to match the pre-built Cypher corpus' own legacy-OS regex
// ("(?i).*Windows.* (2000|2003|2008|2012|xp|vista|7|8|me|nt).*", see
// testdata/prebuilt/agt.json and internal/graphtest/corpusfixture.go's
// legacyOS pool) so cypherbench's legacy-computer queries (task 21) have
// real rows to match in a generated benchmark graph, not only in the
// smaller corpus fixture. Weights sum to 1.0.
var operatingSystems = []weightedOS{
	{"WINDOWS SERVER 2019 DATACENTER", 0.35},
	{"WINDOWS 11 ENTERPRISE", 0.30},
	{"WINDOWS SERVER 2022 DATACENTER", 0.15},
	{"WINDOWS 10 ENTERPRISE", 0.10},
	{"WINDOWS SERVER 2008 R2 STANDARD", 0.06},
	{"WINDOWS 7 PROFESSIONAL", 0.04},
}

// pickOperatingSystem draws one operatingsystem value from operatingSystems
// per its cumulative weights.
func pickOperatingSystem(rng *rand.Rand) string {
	r := rng.Float64()
	cumulative := 0.0
	for _, w := range operatingSystems {
		cumulative += w.weight
		if r < cumulative {
			return w.os
		}
	}
	return operatingSystems[len(operatingSystems)-1].os
}

// groupDescriptions are realistic-length (~60 byte) AD group description
// strings; pickGroupDescription draws one uniformly at random.
var groupDescriptions = []string{
	"Members of this group have full administrative control.",
	"Standard security group used for resource access control.",
	"Distribution list synchronized from the on-premises directory.",
	"Delegated group scoped to a single organizational unit.",
}

func pickGroupDescription(rng *rand.Rand) string {
	return groupDescriptions[rng.Intn(len(groupDescriptions))]
}

// generateDomainEdges appends every edge belonging to one domain to edges
// and returns the extended slice. It is the single place that consumes rng,
// always in the same fixed order for a given domain's node counts, which is
// what makes Generate reproducible.
func generateDomainEdges(edges []Edge, info domain, rng *rand.Rand) []Edge {
	users := info.userIdx
	comps := info.compIdx
	groups := info.groupIdx
	extraGroups := groups[wellKnownGroupsPerDomain:] // every group except the four well-known ones

	// 1. every user MemberOf the Domain Users hub.
	for _, u := range users {
		edges = append(edges, Edge{StartIdx: u, EndIdx: info.usersHubIx, Kind: EdgeMemberOf})
	}

	// 2. group nesting: each non-special group, with p=0.3, MemberOf a
	// random earlier group in the same domain, capped at nesting depth 5.
	depth := make([]int, len(groups))
	for gi := wellKnownGroupsPerDomain; gi < len(groups); gi++ {
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
