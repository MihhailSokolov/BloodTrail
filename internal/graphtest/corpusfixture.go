// SPDX-License-Identifier: Apache-2.0

//go:build integration

package graphtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/specterops/dawgs/drivers/pg"
	"github.com/specterops/dawgs/graph"
)

// This file seeds a deterministic BloodHound-shaped graph through the pg
// driver's write/batch APIs so that milestone 4's pre-built Cypher query
// corpus (testdata/prebuilt/{agt,agi,selectors}.json, see task-14) has
// something to match against: Task 16's differential suite runs every
// enabled corpus query against both the bloodtrail engine and the plain pg
// driver oracle and compares results, and an empty graph makes every query
// trivially agree (0 rows == 0 rows) without ever exercising a real
// traversal, aggregate or property predicate. LoadCorpusFixture exists to
// give the *major* query families real rows to disagree over.
//
// # Every referenced kind must be declared, not just the ones this file
// materializes
//
// dawgs' pgsql translator maps every Kind referenced by a MATCH pattern --
// node label, relationship type, and WHERE-clause "n:Label" kind matchers
// alike -- through pgsql.KindMapper.MapKinds before it will translate the
// query at all (cypher/models/pgsql/translate/{node,relationship,kind}.go
// in specterops/dawgs), and the pg driver's SchemaManager.MapKinds fails
// the *entire* call ("unable to map kinds: ...") if even one of the
// requested kinds was never asserted into the schema -- it does not
// degrade to "no rows for the unknown kind." Concretely: the corpus's many
// shortestPath queries alternate over the same ~64-member Active Directory
// (and ~42-member Azure) pathfinding-edge kind lists BloodHound's own UI
// builds from graphSchema.ts (see scripts/extract-prebuilt-queries.go,
// which ports those exact lists) -- if this fixture only declared the
// handful of edge kinds it actually creates relationships for, every one
// of those queries would error out during translation rather than simply
// return few or no rows. corpusNodeKindNames/corpusEdgeKindNames below are
// therefore the complete closure of every node and edge kind referenced
// anywhere in the three corpus files' *enabled* queries (the one "probe"
// entry is deliberately malformed and never parses; the one disabled
// many-to-many entry is a comment block) -- verified by parsing every
// query with the real cypher frontend and walking NodePattern/
// RelationshipPattern/KindMatcher nodes, not by hand-transcription. Most
// of those kinds are declared but never instantiated by this file: that is
// fine and intentional (see "breadth over perfection" below) -- an
// undeclared kind breaks the query outright, a declared-but-empty kind
// just means that particular edge alternative contributes no rows.
//
// # Breadth over perfection
//
// This fixture aims to make the *major* corpus families -- well-known RID
// groups, Kerberoastable/AS-REP-roastable users, legacy/LAPS computers,
// the core ACL edges, an ADCS enrollment chain, tier-zero tagging (both
// the AGT kind-based and AGI property-based conventions), and a minimal
// Azure corner -- return real rows, organized as one seeding helper per
// family (seedDomainsAndWellKnownPrincipals, seedUsers, seedComputers,
// ...) so a gap Task 16 finds is a small, localized addition to one
// helper rather than a rewrite. It does not attempt every query in the
// corpus (some, e.g. GPO-linking or the narrower selector-only well-known
// SIDs, are left for whoever extends this file once Task 16's
// differential run shows which families still come back empty).
//
// # Time-dependent predicates
//
// Several hygiene queries compare a property against
// `datetime().epochseconds - (N * 86400)` for N in {60, 90, 365} evaluated
// at *query* time, which is necessarily some moments after this fixture
// seeds its data. Rather than trying to straddle those windows precisely
// (and risk a value that was "old enough" at seed time reading as
// "too new" after a slow CI run, or vice versa), every timestamp property
// this fixture sets is pinned to one of two points fixed at seed time and
// held many months from every window boundary: corpusSeed.old (~400 days
// before now, past all three windows) and corpusSeed.recent (~5 days
// before now, inside all three) -- see corpusSeed's doc.

// corpusNodeKindNames and corpusEdgeKindNames are declared into the schema
// verbatim (see the package doc above for why the full closure, not just
// what this file instantiates, is required). Order is alphabetical; it has
// no semantic meaning, it just makes the list diffable.
var corpusNodeKindNames = []string{
	"AIACA", "AZApp", "AZBase", "AZDevice",
	"AZGroup", "AZRole", "AZServicePrincipal", "AZSubscription",
	"AZTenant", "AZUser", "Base", "CertTemplate",
	"Computer", "Container", "Domain", "EnterpriseCA",
	"Group", "IssuancePolicy", "NTAuthStore", "OU",
	"RootCA", "Tag_Owned", "Tag_Tier_Zero", "User",
}

var corpusEdgeKindNames = []string{
	"ADCSESC1", "ADCSESC10a", "ADCSESC10b", "ADCSESC13",
	"ADCSESC3", "ADCSESC4", "ADCSESC6a", "ADCSESC6b",
	"ADCSESC9a", "ADCSESC9b", "AZAKSContributor", "AZAddMembers",
	"AZAddOwner", "AZAddSecret", "AZAppAdmin", "AZAuthenticatesTo",
	"AZAutomationContributor", "AZAvereContributor", "AZCloudAppAdmin", "AZContains",
	"AZContributor", "AZExecuteCommand", "AZGetCertificates", "AZGetKeys",
	"AZGetSecrets", "AZGlobalAdmin", "AZGrant", "AZGrantSelf",
	"AZHasRole", "AZKeyVaultContributor", "AZLogicAppContributor", "AZMGAddMember",
	"AZMGAddOwner", "AZMGAddSecret", "AZMGAppRoleAssignment_ReadWrite_All", "AZMGApplication_ReadWrite_All",
	"AZMGDirectory_ReadWrite_All", "AZMGGrantAppRoles", "AZMGGrantRole", "AZMGGroupMember_ReadWrite_All",
	"AZMGGroup_ReadWrite_All", "AZMGRoleManagement_ReadWrite_Directory", "AZMGServicePrincipalEndpoint_ReadWrite_All", "AZManagedIdentity",
	"AZMemberOf", "AZNodeResourceGroup", "AZOwner", "AZOwns",
	"AZPrivilegedAuthAdmin", "AZPrivilegedRoleAdmin", "AZResetPassword", "AZRoleApprover",
	"AZRoleEligible", "AZRunsAs", "AZUserAccessAdministrator", "AZVMAdminLogin",
	"AZVMContributor", "AZWebsiteContributor", "AbuseTGTDelegation", "AddAllowedToAct",
	"AddKeyCredentialLink", "AddMember", "AddSelf", "AdminTo",
	"AllExtendedRights", "AllowedToAct", "AllowedToDelegate", "CanApplyGPO",
	"CanPSRemote", "CanRDP", "ClaimSpecialIdentity", "CoerceAndRelayNTLMToADCS",
	"CoerceAndRelayNTLMToLDAP", "CoerceAndRelayNTLMToLDAPS", "CoerceAndRelayNTLMToSMB", "CoerceToTGT",
	"Contains", "ContainsIdentity", "CrossForestTrust", "DCFor",
	"DCSync", "DumpSMSAPassword", "Enroll", "EnterpriseCAFor",
	"ExecuteDCOM", "ExtendedByPolicy", "ForceChangePassword", "GPLink",
	"GPOAppliesTo", "GenericAll", "GenericWrite", "GoldenCert",
	"HasSIDHistory", "HasSession", "HasTrustKeys", "HostsCAService",
	"IssuedSignedBy", "ManageCA", "ManageCertificates", "MemberOf",
	"NTAuthStoreFor", "OIDGroupLink", "Owns", "OwnsLimitedRights",
	"PropagatesACEsTo", "ProtectAdminGroups", "PublishedTo", "ReadGMSAPassword",
	"ReadLAPSPassword", "RootCAFor", "SQLAdmin", "SameForestTrust",
	"SpoofSIDHistory", "SyncLAPSPassword", "SyncedToADUser", "SyncedToEntraUser",
	"TrustedForNTAuth", "WriteAccountRestrictions", "WriteAltSecurityIdentities", "WriteDacl",
	"WriteGPLink", "WriteOwner", "WriteOwnerLimitedRights", "WritePublicInformation",
	"WriteSPN",
}

func corpusKinds(names []string) graph.Kinds {
	kinds := make(graph.Kinds, len(names))
	for i, name := range names {
		kinds[i] = graph.StringKind(name)
	}
	return kinds
}

// Kinds this file actually instantiates nodes or relationships with. Every
// name here also appears in corpusNodeKindNames/corpusEdgeKindNames above;
// keeping typed vars only for the kinds in active use (rather than one per
// declared name) keeps this file's signal-to-noise ratio sane against the
// 149-kind declared alphabet.
var (
	kindBase           = graph.StringKind("Base")
	kindUser           = graph.StringKind("User")
	kindComputer       = graph.StringKind("Computer")
	kindGroup          = graph.StringKind("Group")
	kindDomain         = graph.StringKind("Domain")
	kindOU             = graph.StringKind("OU")
	kindContainer      = graph.StringKind("Container")
	kindCertTemplate   = graph.StringKind("CertTemplate")
	kindEnterpriseCA   = graph.StringKind("EnterpriseCA")
	kindRootCA         = graph.StringKind("RootCA")
	kindAIACA          = graph.StringKind("AIACA")
	kindNTAuthStore    = graph.StringKind("NTAuthStore")
	kindIssuancePolicy = graph.StringKind("IssuancePolicy")
	kindTagTierZero    = graph.StringKind("Tag_Tier_Zero")
	kindTagOwned       = graph.StringKind("Tag_Owned")

	kindAZBase             = graph.StringKind("AZBase")
	kindAZUser             = graph.StringKind("AZUser")
	kindAZGroup            = graph.StringKind("AZGroup")
	kindAZRole             = graph.StringKind("AZRole")
	kindAZApp              = graph.StringKind("AZApp")
	kindAZServicePrincipal = graph.StringKind("AZServicePrincipal")
	kindAZDevice           = graph.StringKind("AZDevice")
	kindAZTenant           = graph.StringKind("AZTenant")
	kindAZSubscription     = graph.StringKind("AZSubscription")

	kindMemberOf         = graph.StringKind("MemberOf")
	kindAdminTo          = graph.StringKind("AdminTo")
	kindHasSession       = graph.StringKind("HasSession")
	kindGenericAll       = graph.StringKind("GenericAll")
	kindOwns             = graph.StringKind("Owns")
	kindWriteDacl        = graph.StringKind("WriteDacl")
	kindCanRDP           = graph.StringKind("CanRDP")
	kindReadLAPSPassword = graph.StringKind("ReadLAPSPassword")
	kindDCSync           = graph.StringKind("DCSync")
	kindDCFor            = graph.StringKind("DCFor")
	kindContains         = graph.StringKind("Contains")

	kindHostsCAService     = graph.StringKind("HostsCAService")
	kindIssuedSignedBy     = graph.StringKind("IssuedSignedBy")
	kindEnterpriseCAFor    = graph.StringKind("EnterpriseCAFor")
	kindRootCAFor          = graph.StringKind("RootCAFor")
	kindTrustedForNTAuth   = graph.StringKind("TrustedForNTAuth")
	kindNTAuthStoreFor     = graph.StringKind("NTAuthStoreFor")
	kindPublishedTo        = graph.StringKind("PublishedTo")
	kindEnroll             = graph.StringKind("Enroll")
	kindExtendedByPolicy   = graph.StringKind("ExtendedByPolicy")
	kindOIDGroupLink       = graph.StringKind("OIDGroupLink")
	kindManageCA           = graph.StringKind("ManageCA")
	kindManageCertificates = graph.StringKind("ManageCertificates")
	kindProtectAdminGroups = graph.StringKind("ProtectAdminGroups")
	kindCrossForestTrust   = graph.StringKind("CrossForestTrust")
	kindSpoofSIDHistory    = graph.StringKind("SpoofSIDHistory")
	kindCoerceRelaySMB     = graph.StringKind("CoerceAndRelayNTLMToSMB")
	kindSyncedToADUser     = graph.StringKind("SyncedToADUser")
	kindSyncedToEntraUser  = graph.StringKind("SyncedToEntraUser")

	kindAZContains        = graph.StringKind("AZContains")
	kindAZMemberOf        = graph.StringKind("AZMemberOf")
	kindAZHasRole         = graph.StringKind("AZHasRole")
	kindAZRoleEligible    = graph.StringKind("AZRoleEligible")
	kindAZRoleApprover    = graph.StringKind("AZRoleApprover")
	kindAZGlobalAdmin     = graph.StringKind("AZGlobalAdmin")
	kindAZOwner           = graph.StringKind("AZOwner")
	kindAZOwns            = graph.StringKind("AZOwns")
	kindAZContributor     = graph.StringKind("AZContributor")
	kindAZMGGrantAppRoles = graph.StringKind("AZMGGrantAppRoles")
	kindAZMGAppRoleAssgn  = graph.StringKind("AZMGAppRoleAssignment_ReadWrite_All")
)

// Domain SIDs/names/DNs. domain1 is the "home" domain almost everything
// below hangs off; domain2 exists so the fixture has a real second domain
// for cross-domain membership ("Principals with foreign domain group
// membership") and trust-family queries.
const (
	domain1SID  = "S-1-5-21-1111111111-2222222222-3333333333"
	domain1DN   = "DC=CORP,DC=LOCAL"
	domain1Name = "CORP.LOCAL"

	domain2SID  = "S-1-5-21-4444444444-5555555555-6666666666"
	domain2DN   = "DC=CHILD,DC=CORP,DC=LOCAL"
	domain2Name = "CHILD.CORP.LOCAL"
)

// Frequently-referenced well-known object keys (== their objectid, see
// corpusSeed.node's doc on keys). Declared as consts, not computed inline
// at every use site, so a typo in a RID becomes a compile-time constant
// mismatch against addWellKnownGroups' own construction instead of a
// silent dangling-edge-key runtime failure.
const (
	d1DomainAdmins    = domain1SID + "-512"
	d1DomainUsers     = domain1SID + "-513"
	d1DomainControlrs = domain1SID + "-516"
	d1CertPublishers  = domain1SID + "-517"
	d1ReadOnlyDCs     = domain1SID + "-521"
	d1ProtectedUsers  = domain1SID + "-525"
)

// CorpusFixture is returned by LoadCorpusFixture: id handles for the
// fixture's principal objects, for tests that want to assert on specific
// nodes rather than just "the query returned at least one row."
type CorpusFixture struct {
	// IDs maps every fixture node's lookup key to the database ID the
	// driver assigned it. The key is the node's objectid for every AD/Azure
	// principal (RID-suffixed SIDs, well-known SIDs, and the synthetic
	// objectids this file mints for generated principals), or a short
	// logical name (e.g. "domain1", "certtemplate:esc") for objects that
	// don't carry an objectid in this fixture (domains, OUs, containers,
	// ADCS chain nodes).
	IDs map[string]graph.ID

	// Now is the fixed reference time every day-window property
	// (lastlogontimestamp/pwdlastset/whencreated, etc.) was pinned against
	// at seed time. Callers building additional predicates against the
	// corpus's 60/90/365-day windows should compare against this, not
	// time.Now(), to stay consistent with what the fixture actually wrote.
	Now time.Time
}

// nodeSpec and edgeSpec are the fixture's intermediate representation: every
// seedXxx family helper below appends to a corpusSeed's nodes/edges slices,
// and LoadCorpusFixture flushes the whole thing in two passes (all nodes
// via a write transaction, so their assigned ids exist; then all edges via
// a batch operation, resolving each edgeSpec's string keys against the ids
// the node pass produced). This decouples authoring order from creation
// order: a family helper can reference another family's node key before or
// after that family runs, since edges are only resolved at flush time.
type nodeSpec struct {
	key   string
	kinds graph.Kinds
	props *graph.Properties
}

type edgeSpec struct {
	start, end string
	kind       graph.Kind
	props      *graph.Properties
}

// corpusSeed accumulates a LoadCorpusFixture graph before it is flushed
// through the driver. old and recent are the two fixed points every
// timestamp property in this fixture is pinned to -- see the package doc's
// "Time-dependent predicates" section for why margins of months rather
// than a value computed against each query's own window.
//
// The exported-looking fields below the accumulator fields (kerberoastableAdmin,
// dcComputer, etc.) are the node keys one seed helper hands to another --
// e.g. seedUsers assigns SyncedUser, and seedAzure later wires it to the
// Azure corner via SyncedToEntraUser/SyncedToADUser. Threading them through
// this struct (rather than package-level vars) keeps LoadCorpusFixture
// re-entrant and avoids any shared mutable package state.
type corpusSeed struct {
	now    time.Time
	old    time.Time
	recent time.Time

	nodes []nodeSpec
	edges []edgeSpec
	seen  map[string]struct{}

	KerberoastableAdmin        string
	KerberoastablePlain        string
	SmartcardRequiredUser      string
	AdminSDHolderProtectedUser string
	SyncedUser                 string

	DCComputer                string
	LegacyServerComputer      string
	LegacyWorkstationComputer string
	ModernServerComputer      string
	ModernWorkstationComputer string
}

func newCorpusSeed(now time.Time) *corpusSeed {
	const day = 24 * time.Hour
	return &corpusSeed{
		now:    now,
		old:    now.Add(-400 * day),
		recent: now.Add(-5 * day),
		seen:   map[string]struct{}{},
	}
}

// node registers a node to be created. key is used both for de-duplication
// (a repeated key is a fixture bug -- two different objects would silently
// collide onto one id -- and panics immediately rather than producing a
// hard-to-diagnose wrong-row-count failure later) and as the lookup handle
// edge() and CorpusFixture.IDs use. Pass "" for a node no edge or assertion
// needs to address by key.
func (s *corpusSeed) node(key string, props *graph.Properties, kinds ...graph.Kind) {
	if key != "" {
		if _, dup := s.seen[key]; dup {
			panic("graphtest: LoadCorpusFixture: duplicate node key " + key)
		}
		s.seen[key] = struct{}{}
	}
	s.nodes = append(s.nodes, nodeSpec{key: key, kinds: kinds, props: props})
}

// edge registers a relationship between two node keys with arbitrary
// properties.
func (s *corpusSeed) edge(start, end string, kind graph.Kind, props *graph.Properties) {
	s.edges = append(s.edges, edgeSpec{start: start, end: end, kind: kind, props: props})
}

// flaggedEdge registers a relationship carrying the {"isacl", "lastseen"}
// shape the brief calls out for the six core BloodHound relationship kinds
// (MemberOf/AdminTo/HasSession/GenericAll/Owns/WriteDacl): isACL marks
// whether the edge represents a materialized ACE (GenericAll/Owns/
// WriteDacl -- true) versus a structural/derived relationship (MemberOf/
// AdminTo/HasSession -- false), matching real BloodHound's own edge-source
// convention. lastseen is stamped at fixture-seed time.
func (s *corpusSeed) flaggedEdge(start, end string, kind graph.Kind, isACL bool) {
	s.edge(start, end, kind, graph.NewProperties().
		Set("isacl", isACL).
		Set("lastseen", s.now.Format(time.RFC3339)))
}

// plainEdge registers a relationship with no properties, for edge kinds
// the corpus only matches by existence (ADCS spine, trusts, Azure spine,
// etc.).
func (s *corpusSeed) plainEdge(start, end string, kind graph.Kind) {
	s.edge(start, end, kind, graph.NewProperties())
}

func epoch(t time.Time) int64 { return t.Unix() }

// wellKnownGroup is one row of a well-known-SID group table (RID-suffixed
// to a domain SID, or literal for domain-agnostic builtin SIDs).
type wellKnownGroup struct {
	ridOrSuffix string
	name        string
	tier0       bool
}

// domain1RIDGroups/domain2RIDGroups are the RID-suffixed well-known groups
// the brief calls out (-512/-513/-516/-519/-525) plus a handful more the
// corpus' selectors.json also names (-517/-518/-521/-526/-527/-557), which
// cost nothing extra to seed alongside them.
var domain1RIDGroups = []wellKnownGroup{
	{"512", "DOMAIN ADMINS", true},
	{"513", "DOMAIN USERS", false},
	{"516", "DOMAIN CONTROLLERS", false},
	{"517", "CERT PUBLISHERS", false},
	{"518", "SCHEMA ADMINS", true},
	{"519", "ENTERPRISE ADMINS", true},
	{"521", "READ-ONLY DOMAIN CONTROLLERS", false},
	{"525", "PROTECTED USERS", false},
	{"526", "KEY ADMINS", false},
	{"527", "ENTERPRISE KEY ADMINS", false},
	{"557", "INCOMING FOREST TRUST BUILDERS", false},
}

var domain2RIDGroups = []wellKnownGroup{
	{"512", "DOMAIN ADMINS", true},
	{"513", "DOMAIN USERS", false},
	{"516", "DOMAIN CONTROLLERS", false},
	{"519", "ENTERPRISE ADMINS", true},
}

// builtinGroups are the domain-agnostic "S-1-5-32-*" local groups
// selectors.json and the brief both name; their objectid is the bare
// well-known SID (no domain prefix), matching how BloodHound and the
// corpus' own "ENDS WITH 'S-1-5-32-...'" predicates expect them.
var builtinGroups = []wellKnownGroup{
	{"544", "ADMINISTRATORS", true},
	{"548", "ACCOUNT OPERATORS", false},
	{"549", "SERVER OPERATORS", false},
	{"550", "PRINT OPERATORS", false},
	{"551", "BACKUP OPERATORS", false},
	{"556", "NETWORK CONFIGURATION OPERATORS", false},
	{"559", "PERFORMANCE LOG USERS", false},
	{"562", "DISTRIBUTED COM USERS", false},
	{"569", "CRYPTOGRAPHIC OPERATORS", false},
}

// addWellKnownGroups seeds one SID family (either RID-suffixed to
// sidPrefix, or a literal "S-1-5-32" prefix for builtin groups -- the
// function doesn't care which, it just concatenates sidPrefix+"-"+rid).
// tier0 groups get both the AGT-style Tag_Tier_Zero kind and the AGI-style
// system_tags property, on the same node, per the brief's "both ways"
// requirement.
func addWellKnownGroups(s *corpusSeed, sidPrefix string, groups []wellKnownGroup) {
	for _, g := range groups {
		objectID := sidPrefix + "-" + g.ridOrSuffix
		props := graph.NewProperties().
			Set("objectid", objectID).
			Set("name", g.name).
			Set("domainsid", sidPrefix)
		kinds := graph.Kinds{kindBase, kindGroup}
		if g.tier0 {
			kinds = append(kinds, kindTagTierZero)
			props.Set("system_tags", "admin_tier_0")
		}
		s.node(objectID, props, kinds...)
	}
}

// seedDomainsAndWellKnownPrincipals seeds the two domains, every
// well-known RID/builtin-SID group, the special universal "Enterprise
// Domain Controllers" SID, the two well-known users (Administrator/
// KRBTGT), and one ordinary nested group used by the "Nested groups within
// Tier Zero" family.
func seedDomainsAndWellKnownPrincipals(s *corpusSeed) {
	s.node("domain1", graph.NewProperties().
		Set("objectid", domain1SID).
		Set("name", domain1Name).
		Set("distinguishedname", domain1DN).
		Set("domainsid", domain1SID).
		Set("machineaccountquota", 10).
		Set("expirepasswordsonsmartcardonlyaccounts", false),
		kindBase, kindDomain)

	s.node("domain2", graph.NewProperties().
		Set("objectid", domain2SID).
		Set("name", domain2Name).
		Set("distinguishedname", domain2DN).
		Set("domainsid", domain2SID).
		Set("machineaccountquota", 0).
		Set("expirepasswordsonsmartcardonlyaccounts", true),
		kindBase, kindDomain)

	addWellKnownGroups(s, domain1SID, domain1RIDGroups)
	addWellKnownGroups(s, domain2SID, domain2RIDGroups)
	addWellKnownGroups(s, "S-1-5-32", builtinGroups)

	// Domain1 directly Contains its Tier Zero group -- "Locations of Tier
	// Zero / High Value objects" needs at least one Tag_Tier_Zero (AGT) /
	// system_tags-carrying (AGI) node reachable from a Domain via
	// Contains*1...
	s.plainEdge("domain1", d1DomainAdmins, kindContains)

	// A handful of name-STARTS-WITH ("NAME@DOMAIN", BloodHound's own name
	// convention) groups selectors.json probes ("Exchange Trusted
	// Subsystem", "Exchange Windows Permissions", "DNS Admins") -- cheap
	// breadth beyond the RID/builtin families above.
	for i, prefix := range []string{"EXCHANGE TRUSTED SUBSYSTEM", "EXCHANGE WINDOWS PERMISSIONS", "DNSADMINS"} {
		objectID := fmt.Sprintf("%s-8%03d", domain1SID, i+1)
		s.node(objectID, graph.NewProperties().
			Set("objectid", objectID).
			Set("name", prefix+"@"+domain1Name).
			Set("domainsid", domain1SID),
			kindBase, kindGroup)
	}

	// Enterprise Domain Controllers: a universal well-known SID, not
	// domain-relative, but the corpus still probes it via
	// "ENDS WITH '-1-5-9'" (selectors.json).
	s.node("S-1-5-9", graph.NewProperties().
		Set("objectid", "S-1-5-9").
		Set("name", "ENTERPRISE DOMAIN CONTROLLERS"),
		kindBase, kindGroup)

	// Administrator (RID 500) and KRBTGT (RID 502): both conventionally
	// Tier Zero, and both explicitly excluded by RID suffix from several
	// hygiene queries (inactivity, disabled-principal) that would
	// otherwise misfire on them.
	s.node(domain1SID+"-500", graph.NewProperties().
		Set("objectid", domain1SID+"-500").
		Set("name", "ADMINISTRATOR").
		Set("domainsid", domain1SID).
		Set("enabled", true).
		Set("system_tags", "admin_tier_0"),
		kindBase, kindUser, kindTagTierZero)

	s.node(domain1SID+"-502", graph.NewProperties().
		Set("objectid", domain1SID+"-502").
		Set("name", "KRBTGT").
		Set("domainsid", domain1SID).
		Set("enabled", false).
		Set("system_tags", "admin_tier_0"),
		kindBase, kindUser, kindTagTierZero)

	// A plain (non-well-known) group nested one level into Domain Admins,
	// for "Nested groups within Tier Zero / High Value" -- that query
	// explicitly excludes -512/-519 themselves, so it needs a *different*
	// group one hop below a tier-zero group to return a row. Also used as
	// the source of a GenericAll/Enroll web into the ADCS chain below.
	s.node(domain1SID+"-3001", graph.NewProperties().
		Set("objectid", domain1SID+"-3001").
		Set("name", "IT SUPPORT").
		Set("domainsid", domain1SID),
		kindBase, kindGroup)
	s.flaggedEdge(domain1SID+"-3001", d1DomainAdmins, kindMemberOf, false)
}

// seedUsers seeds every purpose-built user the corpus' Kerberos-interaction
// and Active-Directory-Hygiene families need: Kerberoastable/AS-REP
// roastable combinations, smart-card and password-policy hygiene
// violations, tier-zero tagging both ways, an inactive-for-60-days tier
// zero principal, and a foreign-domain user for the cross-domain
// membership family. Populates several of corpusSeed's cross-family key
// fields (KerberoastableAdmin, SyncedUser, ...) for seedOUsAndContainers/
// seedAzure/seedMembershipAndACLs to wire up later.
func seedUsers(s *corpusSeed) {
	u := func(rid int, name string, tier0 bool, configure func(*graph.Properties)) string {
		objectID := fmt.Sprintf("%s-%d", domain1SID, rid)
		props := graph.NewProperties().
			Set("objectid", objectID).
			Set("name", name).
			Set("domainsid", domain1SID).
			Set("gmsa", false).
			Set("msa", false)
		kinds := graph.Kinds{kindBase, kindUser}
		if tier0 {
			kinds = append(kinds, kindTagTierZero)
			props.Set("system_tags", "admin_tier_0")
		}
		configure(props)
		s.node(objectID, props, kinds...)
		return objectID
	}

	// Kerberoastable, Tier Zero, active recently -- the positive case for
	// "Kerberoastable members of Tier Zero / High Value groups" as well as
	// the plain "All Kerberoastable users"/"most admin privileges" family.
	s.KerberoastableAdmin = u(1101, "SVC-SQLADMIN", true, func(p *graph.Properties) {
		p.Set("hasspn", true).Set("enabled", true).Set("admincount", true).
			Set("pwdneverexpires", false).
			Set("lastlogontimestamp", epoch(s.recent)).
			Set("pwdlastset", epoch(s.recent)).
			Set("whencreated", epoch(s.old))
	})
	s.flaggedEdge(s.KerberoastableAdmin, d1DomainAdmins, kindMemberOf, false)

	// Kerberoastable but not Tier Zero, and tagged Tag_Owned/"owned" --
	// keeps "All Kerberoastable users" from being satisfied by the
	// tier-zero case alone, and gives "Shortest paths from Owned objects"
	// a starting point (it already gets Owns/WriteDacl edges out, below).
	s.KerberoastablePlain = u(1102, "SVC-BACKUP", false, func(p *graph.Properties) {
		p.Set("hasspn", true).Set("enabled", true).Set("admincount", false).
			Set("system_tags", "owned")
	})
	s.nodes[len(s.nodes)-1].kinds = append(s.nodes[len(s.nodes)-1].kinds, kindTagOwned)

	// AS-REP roastable.
	u(1103, "SVC-LEGACYAPP", false, func(p *graph.Properties) {
		p.Set("dontreqpreauth", true).Set("enabled", true)
	})

	// Negative controls: excluded from the Kerberoastable families by the
	// gMSA flag and by being disabled, mirroring
	// internal/engine/cypher_integration_test.go's badUserGMSA/
	// badUserDisabled precedent.
	u(1104, "SVC-GMSA", false, func(p *graph.Properties) {
		p.Set("hasspn", true).Set("enabled", true).Set("gmsa", true)
	})
	u(1105, "SVC-DISABLED", false, func(p *graph.Properties) {
		p.Set("hasspn", true).Set("enabled", false)
	})

	// Tier Zero, enabled, not requiring a smart card.
	u(1106, "T0-NOSMARTCARD", true, func(p *graph.Properties) {
		p.Set("enabled", true).Set("smartcardrequired", false)
	})

	// Enabled, smart-card-required -- contained under domain1 (whose
	// expirepasswordsonsmartcardonlyaccounts is false) via ou1, for
	// "Accounts with smart card required in domains where smart account
	// passwords do not expire".
	s.SmartcardRequiredUser = u(1107, "SMARTCARD-USER", false, func(p *graph.Properties) {
		p.Set("enabled", true).Set("smartcardrequired", true)
	})

	u(1108, "SVC-NOPASSWORD", false, func(p *graph.Properties) {
		p.Set("passwordnotreqd", true).Set("enabled", true)
	})

	u(1109, "STALE-PWD-USER", false, func(p *graph.Properties) {
		p.Set("enabled", true).Set("pwdlastset", epoch(s.old))
	})

	u(1110, "LEGACY-REVENC-USER", false, func(p *graph.Properties) {
		p.Set("encryptedtextpwdallowed", true).Set("enabled", true)
	})

	u(1111, "LEGACY-DESONLY-USER", false, func(p *graph.Properties) {
		p.Set("usedeskeyonly", true).Set("enabled", true)
	})

	u(1112, "LEGACY-RC4-USER", false, func(p *graph.Properties) {
		p.Set("supportedencryptiontypes", []string{"RC4-HMAC-MD5"}).Set("enabled", true)
	})

	u(1113, "T0-PWDNEVEREXPIRES", true, func(p *graph.Properties) {
		p.Set("enabled", true).Set("pwdneverexpires", true)
	})

	u(1114, "T0-NOSDHOLDER", true, func(p *graph.Properties) {
		p.Set("enabled", true).Set("adminsdholderprotected", false)
	})

	// Contained under domain1 via ou1, adminsdholderprotected=true, for
	// "Location of AdminSDHolder Protected objects".
	s.AdminSDHolderProtectedUser = u(1115, "SDHOLDER-PROTECTED-USER", false, func(p *graph.Properties) {
		p.Set("enabled", true).Set("adminsdholderprotected", true)
	})

	// Disabled Tier Zero principal, not one of the well-known excluded
	// RIDs (-500/-502).
	u(1116, "T0-DISABLED", true, func(p *graph.Properties) {
		p.Set("enabled", false)
	})

	// Enabled, Tier Zero, inactive for well over 60 days on every one of
	// the three properties the query inspects.
	u(1117, "SVC-OLDADMIN", true, func(p *graph.Properties) {
		p.Set("enabled", true).
			Set("lastlogontimestamp", epoch(s.old)).
			Set("lastlogon", epoch(s.old)).
			Set("whencreated", epoch(s.old))
	})

	// Tier Zero, synced to an Entra ID user (see seedAzure), and a member
	// of Domain Admins -- also doubles as the HasSession target for the
	// "Domain Admins logons to non-Domain Controllers" family and the
	// source of a DCSync edge for "Principals with DCSync privileges".
	s.SyncedUser = u(1118, "SVC-HYBRIDADMIN", true, func(p *graph.Properties) {
		p.Set("enabled", true)
	})
	s.flaggedEdge(s.SyncedUser, d1DomainAdmins, kindMemberOf, false)
	s.plainEdge(s.SyncedUser, "domain1", kindDCSync)

	// Foreign-domain user: MemberOf a domain1 group despite belonging to
	// domain2, for "Principals with foreign domain group membership".
	foreignDomainUser := fmt.Sprintf("%s-1201", domain2SID)
	s.node(foreignDomainUser, graph.NewProperties().
		Set("objectid", foreignDomainUser).
		Set("name", "CHILD-USER").
		Set("domainsid", domain2SID).
		Set("enabled", true),
		kindBase, kindUser)
	s.flaggedEdge(foreignDomainUser, d1DomainAdmins, kindMemberOf, false)
}

// seedComputers seeds the DC and a small pool of purpose-built
// workstations/servers spanning legacy/modern operating systems, LAPS
// both ways, unconstrained delegation, NTLM/SMB hygiene flags, and a
// read-only DC. Populates corpusSeed's computer key fields.
func seedComputers(s *corpusSeed) {
	c := func(rid int, name string, configure func(*graph.Properties)) string {
		objectID := fmt.Sprintf("%s-%d", domain1SID, rid)
		props := graph.NewProperties().
			Set("objectid", objectID).
			Set("name", name).
			Set("domainsid", domain1SID).
			Set("enabled", true)
		configure(props)
		s.node(objectID, props, kindBase, kindComputer)
		return objectID
	}

	s.DCComputer = c(2001, "DC01", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows Server 2019 Datacenter").
			Set("haslaps", false).
			Set("unconstraineddelegation", false).
			Set("strongcertificatebindingenforcementraw", 1).
			Set("certificatemappingmethodsraw", 4).
			Set("ldapavailable", true).
			Set("ldapsigning", false).
			Set("ldapsavailable", true).
			Set("ldapsepa", true).
			Set("restrictoutboundntlm", false).
			Set("webclientrunning", false).
			Set("smbsigning", true).
			Set("isReadOnlyDC", false)
	})
	s.flaggedEdge(s.DCComputer, d1DomainControlrs, kindMemberOf, false)
	s.plainEdge(s.DCComputer, "domain1", kindDCFor)

	s.LegacyServerComputer = c(2002, "FILESRV01", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows Server 2008 R2 Standard").
			Set("haslaps", false).
			Set("unconstraineddelegation", true).
			Set("restrictoutboundntlm", false).
			Set("webclientrunning", true).
			Set("smbsigning", false)
	})

	s.LegacyWorkstationComputer = c(2003, "WKS-XP01", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows 7 Professional").
			Set("haslaps", true)
	})

	s.ModernServerComputer = c(2004, "APPSRV01", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows Server 2022 Datacenter").
			Set("haslaps", true).
			Set("restrictoutboundntlm", true).
			Set("smbsigning", true)
	})

	s.ModernWorkstationComputer = c(2005, "WKS-WIN11-01", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows 11 Enterprise").
			Set("haslaps", true)
	})

	readOnlyDC := c(2006, "RODC01", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows Server 2022 Datacenter").
			Set("isReadOnlyDC", true)
	})
	s.flaggedEdge(readOnlyDC, d1ReadOnlyDCs, kindMemberOf, false)

	coerceTarget := c(2007, "PRINTSRV01", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows Server 2019 Datacenter")
	})
	s.plainEdge(s.LegacyServerComputer, coerceTarget, kindCoerceRelaySMB)

	// The synthetic computer object Entra Connect's Seamless SSO feature
	// creates in AD -- selectors.json's "AZUREADSSOACC object" matches it
	// by samaccountname.
	c(2008, "AZUREADSSOACC", func(p *graph.Properties) {
		p.Set("operatingsystem", "Windows Server 2019 Datacenter").
			Set("samaccountname", "AZUREADSSOACC$")
	})
}

// seedOUsAndContainers seeds one OU (hanging one hop off domain1 for "Map
// OU structure" and to anchor the smart-card/AdminSDHolder-contained-object
// queries) and the two well-known containers (AdminSDHolder, Public Key
// Services) the corpus matches by distinguishedname.
func seedOUsAndContainers(s *corpusSeed) {
	const ou1 = "ou1"
	s.node(ou1, graph.NewProperties().
		Set("distinguishedname", "OU=Workstations,"+domain1DN).
		Set("name", "WORKSTATIONS"),
		kindBase, kindOU)
	s.plainEdge("domain1", ou1, kindContains)
	s.plainEdge(ou1, s.SmartcardRequiredUser, kindContains)
	s.plainEdge(ou1, s.AdminSDHolderProtectedUser, kindContains)

	s.node("container:adminsdholder", graph.NewProperties().
		Set("distinguishedname", "CN=ADMINSDHOLDER,CN=SYSTEM,"+domain1DN).
		Set("name", "ADMINSDHOLDER"),
		kindBase, kindContainer)
	s.plainEdge("container:adminsdholder", d1DomainAdmins, kindProtectAdminGroups)

	s.node("container:pki", graph.NewProperties().
		Set("distinguishedname", "CN=PUBLIC KEY SERVICES,CN=SERVICES,CN=CONFIGURATION,"+domain1DN).
		Set("name", "PUBLIC KEY SERVICES"),
		kindBase, kindContainer)
	// certtemplate:esc is seeded by seedADCS; edge resolution happens at
	// flush time so referencing its key here, before seedADCS runs, is
	// fine -- see edgeSpec's doc.
	s.plainEdge("container:pki", "certtemplate:esc", kindContains)
}

// seedMembershipAndACLs wires the dangerous-privilege and RDP/session
// families around the well-known "Domain Users"/"Domain Admins" groups and
// the computers/users seeded above.
func seedMembershipAndACLs(s *corpusSeed) {
	s.flaggedEdge(d1DomainUsers, s.ModernServerComputer, kindAdminTo, false)
	s.flaggedEdge(s.KerberoastableAdmin, s.ModernWorkstationComputer, kindAdminTo, false)
	s.flaggedEdge(s.ModernWorkstationComputer, s.SyncedUser, kindHasSession, false)

	s.flaggedEdge(d1DomainUsers, s.LegacyWorkstationComputer, kindCanRDP, false)
	s.flaggedEdge(d1DomainUsers, s.ModernServerComputer, kindCanRDP, false)
	s.flaggedEdge(d1DomainUsers, s.ModernServerComputer, kindReadLAPSPassword, false)

	s.flaggedEdge(d1DomainUsers, s.LegacyServerComputer, kindGenericAll, true)
	s.flaggedEdge(s.KerberoastablePlain, s.ModernWorkstationComputer, kindOwns, true)
	s.flaggedEdge(s.KerberoastablePlain, s.ModernServerComputer, kindWriteDacl, true)

	// A (synthetic, deliberately severe) GenericAll from Domain Users onto
	// Domain Admins: gives the "Paths/Shortest paths from Domain Users to
	// Tier Zero / High Value targets" family a real edge to traverse.
	s.flaggedEdge(d1DomainUsers, d1DomainAdmins, kindGenericAll, true)

	// "All members of Protected Users".
	s.flaggedEdge(s.AdminSDHolderProtectedUser, d1ProtectedUsers, kindMemberOf, false)
}

// seedADCS seeds a minimal but complete PKI chain: a root CA, an AIA CA, an
// NTAuth store, one enterprise CA (flagged vulnerable two ways), and three
// certificate templates covering the ESC1/ESC2 shape, the enrollment-agent
// EKU, and the no-security-extension flag -- plus an IssuancePolicy with an
// OIDGroupLink for the last ADCS selector query.
func seedADCS(s *corpusSeed) {
	// BloodHound stores distinguishedname uppercase, and one corpus query
	// (ESC5, "Compromising permissions on ADCS nodes") does a
	// case-sensitive CONTAINS "PUBLIC KEY SERVICES" check against it.
	const pkiSuffix = "CN=PUBLIC KEY SERVICES,CN=SERVICES,CN=CONFIGURATION," + domain1DN
	const enterpriseCA1 = "enterpriseca1"
	const certTemplateESC = "certtemplate:esc"

	s.node("rootca1", graph.NewProperties().
		Set("distinguishedname", "CN=CORP ROOT CA,CN=CERTIFICATION AUTHORITIES,"+pkiSuffix).
		Set("name", "CORP ROOT CA"),
		kindBase, kindRootCA)
	s.plainEdge("rootca1", "domain1", kindRootCAFor)

	s.node("aiaca1", graph.NewProperties().
		Set("name", "CORP AIA CA"),
		kindBase, kindAIACA)
	s.plainEdge("aiaca1", enterpriseCA1, kindEnterpriseCAFor)

	s.node("ntauthstore1", graph.NewProperties().
		Set("name", "NTAUTHCERTIFICATES"),
		kindBase, kindNTAuthStore)
	s.plainEdge("ntauthstore1", "domain1", kindNTAuthStoreFor)

	s.node(enterpriseCA1, graph.NewProperties().
		Set("distinguishedname", "CN=CORP ISSUING CA,CN=ENROLLMENT SERVICES,"+pkiSuffix).
		Set("name", "CORP ISSUING CA").
		Set("isuserspecifiessanenabled", true).
		Set("hasvulnerableendpoint", true),
		kindBase, kindEnterpriseCA)
	s.plainEdge(s.DCComputer, enterpriseCA1, kindHostsCAService)
	s.plainEdge(enterpriseCA1, "rootca1", kindIssuedSignedBy)
	s.plainEdge(enterpriseCA1, "ntauthstore1", kindTrustedForNTAuth)
	s.flaggedEdge(d1CertPublishers, enterpriseCA1, kindGenericAll, true)
	s.plainEdge(d1CertPublishers, enterpriseCA1, kindManageCA)
	s.plainEdge(d1CertPublishers, enterpriseCA1, kindManageCertificates)

	s.node(certTemplateESC, graph.NewProperties().
		Set("distinguishedname", "CN=USERESC,CN=CERTIFICATE TEMPLATES,"+pkiSuffix).
		Set("name", "USERESC").
		Set("requiresmanagerapproval", false).
		Set("authorizedsignatures", 0).
		Set("schemaversion", 1).
		Set("enrolleesuppliessubject", true).
		Set("authenticationenabled", true).
		Set("effectiveekus", []string{""}),
		kindBase, kindCertTemplate)
	s.plainEdge(certTemplateESC, enterpriseCA1, kindPublishedTo)
	s.flaggedEdge(d1DomainUsers, certTemplateESC, kindGenericAll, true)
	s.plainEdge(domain1SID+"-3001", certTemplateESC, kindEnroll)
	s.plainEdge(certTemplateESC, "issuancepolicy1", kindExtendedByPolicy)

	s.node("certtemplate:enrollagent", graph.NewProperties().
		Set("name", "ENROLLMENTAGENT").
		Set("effectiveekus", []string{"1.3.6.1.4.1.311.20.2.1"}),
		kindBase, kindCertTemplate)
	s.plainEdge("certtemplate:enrollagent", enterpriseCA1, kindPublishedTo)
	s.plainEdge(d1DomainUsers, "certtemplate:enrollagent", kindEnroll)

	s.node("certtemplate:nosecext", graph.NewProperties().
		Set("name", "NOSECEXT").
		Set("nosecurityextension", true),
		kindBase, kindCertTemplate)
	s.plainEdge("certtemplate:nosecext", enterpriseCA1, kindPublishedTo)
	s.plainEdge(d1DomainUsers, "certtemplate:nosecext", kindEnroll)

	s.node("issuancepolicy1", graph.NewProperties().
		Set("name", "ISSUANCEPOLICY1"),
		kindBase, kindIssuancePolicy)
	s.flaggedEdge(domain1SID+"-3001", "issuancepolicy1", kindGenericAll, true)
	s.plainEdge("issuancepolicy1", "S-1-5-9", kindOIDGroupLink)
}

// seedTrustsAndRelay wires a cross-forest trust with an abusable
// SpoofSIDHistory edge between the two domains (satisfying both the plain
// "Map domain trusts" query and the "abusable configuration" one, which
// needs the two edges to coexist on the same domain pair).
func seedTrustsAndRelay(s *corpusSeed) {
	s.plainEdge("domain1", "domain2", kindCrossForestTrust)
	s.plainEdge("domain1", "domain2", kindSpoofSIDHistory)
}

// seedAzure seeds a minimal Azure/Entra corner: one tenant, one
// high-privileged role ("Global Administrator", matching the corpus'
// highPrivilegedRoleDisplayNameRegex), a user/group/app/service-principal
// spine reaching it through both direct and group-delegated paths, a
// device with a legacy build number, a subscription, and the two
// cross-platform sync edges linking back to the AD side's SyncedUser.
func seedAzure(s *corpusSeed) {
	const (
		azTenant1           = "aztenant1"
		azUser1             = "azuser1"
		azServicePrincipal1 = "azsp1"
	)

	s.node(azTenant1, graph.NewProperties().
		Set("objectid", "11111111-1111-1111-1111-111111111111").
		Set("name", "corp.onmicrosoft.com"),
		kindAZBase, kindAZTenant)

	// GUIDs uppercase throughout this section, matching BloodHound's own
	// Azure objectid convention ("<UPPERCASE ROLE TEMPLATE GUID>@<tenant
	// id>") -- selectors.json's role selectors do an exact-case STARTS
	// WITH against it.
	s.node("azrole:ga", graph.NewProperties().
		Set("objectid", "62E90394-69F5-4237-9190-012177145E10@11111111-1111-1111-1111-111111111111").
		Set("name", "Global Administrator"),
		kindAZBase, kindAZRole)

	// A handful more high-privileged AZRole nodes, keyed by the exact role
	// template GUIDs selectors.json's remaining role selectors match --
	// cheap breadth beyond the Global Administrator role above.
	for _, role := range []struct{ guid, name string }{
		{"9B895D92-2CD3-44C7-9D02-A6AC2D5EA5C3", "Application Administrator"},
		{"B5A8DCF3-09D5-43A9-A639-8E29EF291470", "Knowledge Administrator"},
		{"E00E864A-17C5-4A4B-9C06-F5B95A8D5BD8", "Partner Tier2 Support"},
		{"7BE44C8A-ADAF-4E2A-84D6-AB2649E08A13", "Privileged Authentication Administrator"},
		{"E8611AB8-C189-46E8-94E1-60213AB1F814", "Privileged Role Administrator"},
		{"3A2C62DB-5318-420D-8D74-23AFFEE5D9D5", "Intune Administrator"},
		{"194AE4CB-B126-40B2-BD5B-6091B380977D", "Security Administrator"},
	} {
		s.node("azrole:"+role.guid, graph.NewProperties().
			Set("objectid", role.guid+"@11111111-1111-1111-1111-111111111111").
			Set("name", role.name),
			kindAZBase, kindAZRole)
	}

	s.node(azUser1, graph.NewProperties().
		Set("objectid", "22222222-2222-2222-2222-222222222222").
		Set("name", "admin@corp.onmicrosoft.com").
		Set("enabled", true).
		Set("onpremsyncenabled", true).
		Set("onpremid", s.SyncedUser).
		Set("system_tags", "admin_tier_0"),
		kindAZBase, kindAZUser, kindTagTierZero)

	s.node("azgroup1", graph.NewProperties().
		Set("objectid", "33333333-3333-3333-3333-333333333333").
		Set("name", "Azure Admins"),
		kindAZBase, kindAZGroup)

	s.node("azapp1", graph.NewProperties().
		Set("objectid", "44444444-4444-4444-4444-444444444444").
		Set("name", "Corp App"),
		kindAZBase, kindAZApp)

	s.node(azServicePrincipal1, graph.NewProperties().
		Set("objectid", "55555555-5555-5555-5555-555555555555").
		Set("name", "Corp Service Principal").
		Set("appownerorganizationid", "99999999-9999-9999-9999-999999999999").
		Set("tenantid", "11111111-1111-1111-1111-111111111111").
		Set("system_tags", "admin_tier_0"),
		kindAZBase, kindAZServicePrincipal, kindTagTierZero)

	s.node("azsp2", graph.NewProperties().
		Set("objectid", "66666666-6666-6666-6666-666666666666").
		Set("name", "Corp Service Principal 2"),
		kindAZBase, kindAZServicePrincipal)

	s.node("azdevice1", graph.NewProperties().
		Set("objectid", "77777777-7777-7777-7777-777777777777").
		Set("name", "LEGACY-LAPTOP").
		Set("operatingsystem", "WINDOWS 10 ENTERPRISE").
		Set("operatingsystemversion", "10.0.14393"),
		kindAZBase, kindAZDevice)

	s.node("azsubscription1", graph.NewProperties().
		Set("objectid", "88888888-8888-8888-8888-888888888888").
		Set("name", "Corp Subscription"),
		kindAZBase, kindAZSubscription)

	// External (guest) and disabled, on top of Tier Zero -- "Tier Zero /
	// High Value external Entra ID users" and the Azure "Disabled Tier
	// Zero / High Value principals" hygiene query both need a node like
	// this; combining both traits onto one node is cheap and each
	// predicate is independent of the other anyway.
	s.node("azuser2", graph.NewProperties().
		Set("objectid", "99999999-2222-2222-2222-222222222222").
		Set("name", "guest_partner.com#EXT#@corp.onmicrosoft.com").
		Set("enabled", false).
		Set("system_tags", "admin_tier_0"),
		kindAZBase, kindAZUser, kindTagTierZero)

	s.plainEdge(azTenant1, azUser1, kindAZContains)
	s.plainEdge(azTenant1, "azgroup1", kindAZContains)
	s.plainEdge(azUser1, "azgroup1", kindAZMemberOf)
	s.plainEdge(azUser1, "azrole:ga", kindAZHasRole)
	s.plainEdge("azgroup1", "azrole:ga", kindAZHasRole)
	s.plainEdge(azUser1, "azrole:ga", kindAZRoleEligible)
	s.plainEdge(azUser1, "azrole:ga", kindAZRoleApprover)
	// Group-delegated eligibility/approval, and a subscription-level RM
	// role via the group -- gives the four "... group delegated ..."
	// corpus queries (Entra/Cross-Platform families) a path through
	// azgroup1 rather than only azUser1's direct edges above.
	s.plainEdge("azgroup1", "azrole:ga", kindAZRoleEligible)
	s.plainEdge("azgroup1", "azrole:ga", kindAZRoleApprover)
	s.plainEdge("azgroup1", "azsubscription1", kindAZContributor)
	s.plainEdge(azUser1, azTenant1, kindAZGlobalAdmin)
	s.plainEdge("azapp1", azUser1, kindAZOwner)
	// azUser1 owns azapp1 and (separately) a *different* Tier Zero AZBase
	// node -- "On-Prem Users synced to Entra Users that Own Entra Objects"
	// and "Shortest paths from Entra Users to Tier Zero" both need this
	// (the latter needs s<>t, so azUser1's own Tag_Tier_Zero tag doesn't
	// satisfy it by itself).
	s.plainEdge(azUser1, "azapp1", kindAZOwns)
	s.plainEdge(azUser1, azServicePrincipal1, kindAZOwner)
	s.plainEdge(azUser1, "azsubscription1", kindAZContributor)
	s.plainEdge(azServicePrincipal1, azTenant1, kindAZMGGrantAppRoles)
	s.plainEdge(azServicePrincipal1, "azsp2", kindAZMGAppRoleAssgn)

	s.plainEdge(s.SyncedUser, azUser1, kindSyncedToEntraUser)
	s.plainEdge(azUser1, s.SyncedUser, kindSyncedToADUser)
}

// seedFillerPopulation adds a deterministic pool of ordinary users,
// computers and groups around the curated fixture above: 150 users (Domain
// Users members with varied enabled/admincount/hasspn/pwdneverexpires
// flags and old/recent timestamp splits), 45 computers (split across a
// legacy/modern operating-system pool with varied LAPS), and 20 plain
// groups nested under Domain Users. None of it is load-bearing for any
// single corpus predicate -- it exists so shortestPath/variable-length
// queries and the "All Domain Admins"/general population style queries
// operate over something closer to a real environment's density than the
// curated core alone would give them, and so the fixture's total size
// approaches the brief's "~300 nodes" target.
func seedFillerPopulation(s *corpusSeed) {
	legacyOS := []string{"Windows XP Professional", "Windows Server 2003 Standard", "Windows Server 2012 R2 Datacenter", "Windows 8.1 Pro"}
	modernOS := []string{"Windows Server 2019 Datacenter", "Windows 10 Enterprise", "Windows Server 2022 Datacenter", "Windows 11 Enterprise"}

	const numComputers = 45
	computerKey := func(i int) string {
		return fmt.Sprintf("%s-6%03d", domain1SID, i)
	}
	for i := 1; i <= numComputers; i++ {
		var os string
		if i%2 == 0 {
			os = legacyOS[i%len(legacyOS)]
		} else {
			os = modernOS[i%len(modernOS)]
		}
		key := computerKey(i)
		s.node(key, graph.NewProperties().
			Set("objectid", key).
			Set("name", fmt.Sprintf("WKSTN%03d", i)).
			Set("domainsid", domain1SID).
			Set("enabled", true).
			Set("operatingsystem", os).
			Set("haslaps", i%2 == 0),
			kindBase, kindComputer)
		if i%5 == 0 {
			s.flaggedEdge(d1DomainUsers, key, kindAdminTo, false)
		}
	}

	const numUsers = 150
	for i := 1; i <= numUsers; i++ {
		key := fmt.Sprintf("%s-5%03d", domain1SID, i)
		props := graph.NewProperties().
			Set("objectid", key).
			Set("name", fmt.Sprintf("USER%03d", i)).
			Set("domainsid", domain1SID).
			Set("enabled", i%10 != 0).
			Set("admincount", i%20 == 0).
			Set("hasspn", i%15 == 0).
			Set("pwdneverexpires", i%7 == 0).
			Set("gmsa", false).
			Set("msa", false)
		if i%3 == 0 {
			props.Set("lastlogontimestamp", epoch(s.old)).
				Set("pwdlastset", epoch(s.old)).
				Set("whencreated", epoch(s.old))
		} else {
			props.Set("lastlogontimestamp", epoch(s.recent)).
				Set("pwdlastset", epoch(s.recent)).
				Set("whencreated", epoch(s.recent))
		}
		s.node(key, props, kindBase, kindUser)
		s.flaggedEdge(key, d1DomainUsers, kindMemberOf, false)
		if i%8 == 0 {
			compIdx := (i/8-1)%numComputers + 1
			s.flaggedEdge(computerKey(compIdx), key, kindHasSession, false)
		}
	}

	const numDeptGroups = 20
	for i := 1; i <= numDeptGroups; i++ {
		key := fmt.Sprintf("%s-7%03d", domain1SID, i)
		s.node(key, graph.NewProperties().
			Set("objectid", key).
			Set("name", fmt.Sprintf("DEPT%02d", i)).
			Set("domainsid", domain1SID),
			kindBase, kindGroup)
		s.flaggedEdge(key, d1DomainUsers, kindMemberOf, false)
	}
}

// LoadCorpusFixture seeds a deterministic BloodHound-shaped graph into d's
// default graph through the pg driver's write-transaction (nodes) and
// batch (relationships) APIs, covering the major families of
// testdata/prebuilt/{agt,agi,selectors}.json's pre-built Cypher query
// corpus -- see this file's package doc for the design rationale (why
// every referenced kind, not just the ones instantiated here, must be
// declared; why timestamps are pinned to fixed old/recent points; and the
// "breadth over perfection" scope this fixture targets).
//
// It does not call WipeGraph itself; callers that need a clean slate are
// responsible for that, matching LoadDataset/LoadRandom's precedent of
// leaving lifecycle management to the caller.
func LoadCorpusFixture(t testing.TB, d *pg.Driver) CorpusFixture {
	t.Helper()
	ctx := context.Background()

	seed := newCorpusSeed(time.Now().UTC())

	// Order here only affects s.nodes'/s.edges' slice order (and hence
	// creation order within each flush pass below), not correctness: edges
	// resolve their start/end keys against the ids map built from *every*
	// node this pass produced, regardless of which seed helper defined
	// which key first. See nodeSpec/edgeSpec's doc. seedUsers/seedComputers
	// must still run before any helper that reads their cross-family
	// corpusSeed fields (SyncedUser, DCComputer, ...), since those are
	// plain Go string fields resolved immediately, not deferred keys.
	seedDomainsAndWellKnownPrincipals(seed)
	seedUsers(seed)
	seedComputers(seed)
	seedOUsAndContainers(seed)
	seedMembershipAndACLs(seed)
	seedADCS(seed)
	seedTrustsAndRelay(seed)
	seedAzure(seed)
	seedFillerPopulation(seed)

	schema := graph.Schema{Graphs: []graph.Graph{{
		Name:  GraphName,
		Nodes: corpusKinds(corpusNodeKindNames),
		Edges: corpusKinds(corpusEdgeKindNames),
	}}}
	if err := d.AssertSchema(ctx, schema); err != nil {
		t.Fatalf("graphtest: LoadCorpusFixture: assert schema: %v", err)
	}

	ids := make(map[string]graph.ID, len(seed.nodes))
	if err := d.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, n := range seed.nodes {
			node, err := tx.CreateNode(n.props, n.kinds...)
			if err != nil {
				return err
			}
			if n.key != "" {
				ids[n.key] = node.ID
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("graphtest: LoadCorpusFixture: create nodes: %v", err)
	}

	if err := d.BatchOperation(ctx, func(batch graph.Batch) error {
		for _, e := range seed.edges {
			startID, ok := ids[e.start]
			if !ok {
				return fmt.Errorf("unknown edge start key %q", e.start)
			}
			endID, ok := ids[e.end]
			if !ok {
				return fmt.Errorf("unknown edge end key %q", e.end)
			}
			if err := batch.CreateRelationshipByIDs(startID, endID, e.kind, e.props); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("graphtest: LoadCorpusFixture: create edges: %v", err)
	}

	return CorpusFixture{IDs: ids, Now: seed.now}
}
