// SPDX-License-Identifier: Apache-2.0

// generate.go builds a fictitious corporate Active Directory forest
// (MEGACORP.LOCAL and, with -domains > 1, child domains) as SharpHound v6
// collection files. It is fully index-deterministic: every object's SID, and
// every cross-reference to another object, is computed from the object's
// index and the seed through a hash, never drawn from carried RNG state --
// so generation streams one object at a time with memory flat regardless of
// scale, and the same (-seed, counts) always produce byte-identical output.
//
// Referential integrity is a build-time invariant, not a hope: every SID a
// reference emits is one deriveXxxSID would produce for an index inside the
// generated range, and generate_test.go walks the output proving no edge
// dangles. A deterministic set of attack paths is seeded on top of the
// realistic noise -- see the attackPath* helpers and their tests.
package main

import (
	"encoding/binary"
	"fmt"
)

// config is one generation request, from the flags main.go parses.
type config struct {
	Domain    string // forest root domain name, e.g. "MEGACORP.LOCAL"
	Users     int    // total User principals across all domains
	Computers int    // total Computer principals across all domains
	Groups    int    // total non-well-known Group principals across all domains
	Domains   int    // number of domains (>= 1); domain 0 is the forest root
	Seed      int64

	// Azure/Entra tenant (see emit_azure.go). AZUsers == 0 disables the
	// whole azure side, keeping pre-hybrid invocations byte-identical.
	AZUsers    int // AZUser principals in the tenant
	AZGroups   int // AZGroup security groups
	AZApps     int // AZApp registrations (each with a matching AZServicePrincipal)
	AZDevices  int // AZDevice entries
	AZVMs      int // AZVM virtual machines, spread over the resource groups
	AZKeyVault int // AZKeyVault vaults, spread over the resource groups
	AZSubs     int // AZSubscription subscriptions under the tenant
	AZSyncPct  int // percent of AZUsers hybrid-synced to an AD user (onprem SID)
}

// applyAzureDefaults resolves the -1 "derive from az-users" azure sizing
// flags. With AZUsers == 0 everything azure stays zero and no azure file is
// emitted at all.
func (c *config) applyAzureDefaults() {
	if c.AZUsers <= 0 {
		c.AZUsers, c.AZGroups, c.AZApps, c.AZDevices, c.AZVMs, c.AZKeyVault, c.AZSubs = 0, 0, 0, 0, 0, 0, 0
		return
	}
	if c.AZUsers < azAttackUserReserve {
		c.AZUsers = azAttackUserReserve
	}
	def := func(v *int, fallback, floor int) {
		if *v < 0 {
			*v = fallback
		}
		if *v < floor {
			*v = floor
		}
	}
	def(&c.AZGroups, c.AZUsers/20, azAttackGroupReserve)
	def(&c.AZApps, c.AZUsers/100, azAttackAppReserve)
	def(&c.AZDevices, c.AZUsers/3, 1)
	def(&c.AZVMs, 200, 1)
	def(&c.AZKeyVault, 50, 1)
	def(&c.AZSubs, 2, 1)
	if c.AZSyncPct < 0 {
		c.AZSyncPct = 0
	}
	if c.AZSyncPct > 100 {
		c.AZSyncPct = 100
	}
}

// splitmix64 hashes an arbitrary tuple of ints (mixed with the seed) into a
// uint64, the one source of every pseudo-random-but-reproducible decision in
// this generator. Being a pure function of its inputs is what lets an object
// emitted in isolation agree with a reference to it emitted elsewhere,
// without either side consulting shared state.
func (c config) hash(parts ...int64) uint64 {
	x := uint64(c.Seed) + 0x9e3779b97f4a7c15
	for _, p := range parts {
		x += uint64(p) * 0x9e3779b97f4a7c15
		x ^= x >> 30
		x *= 0xbf58476d1ce4e5b9
		x ^= x >> 27
		x *= 0x94d049bb133111eb
		x ^= x >> 31
	}
	return x
}

// intn returns a deterministic value in [0, n) from the hashed tuple.
func (c config) intn(n int, parts ...int64) int {
	if n <= 0 {
		return 0
	}
	return int(c.hash(parts...) % uint64(n))
}

// --- domain and RID layout -------------------------------------------------

// domainCount is the number of domains, floored at 1.
func (c config) domainCount() int {
	if c.Domains < 1 {
		return 1
	}
	return c.Domains
}

// perDomain splits total as evenly as possible across the domains, giving the
// remainder to the earliest domains, with an optional floor for a domain that
// must hold a minimum (groups must hold the well-known groups' siblings).
func (c config) perDomain(total, domain, floor int) int {
	d := c.domainCount()
	base := total / d
	if domain < total%d {
		base++
	}
	if base < floor {
		base = floor
	}
	return base
}

func (c config) usersInDomain(d int) int     { return c.perDomain(c.Users, d, 2) } // >= Administrator + krbtgt
func (c config) computersInDomain(d int) int { return c.perDomain(c.Computers, d, 0) }
func (c config) groupsInDomain(d int) int    { return c.perDomain(c.Groups, d, attackGroupReserve) }

// domainName returns domain d's DNS name: the root for d == 0, else a child.
func (c config) domainName(d int) string {
	if d == 0 {
		return c.Domain
	}
	return fmt.Sprintf("DIV%02d.%s", d, c.Domain)
}

// domainSID derives domain d's `S-1-5-21-a-b-c` machine SID from the seed, so
// every domain's SID is distinct and stable across runs.
func (c config) domainSID(d int) string {
	h := c.hash(1, int64(d))
	a := uint32(h)
	b := uint32(h >> 32)
	cc := uint32(c.hash(2, int64(d)))
	return fmt.Sprintf("S-1-5-21-%d-%d-%d", a, b, cc)
}

// Well-known RID suffixes, per real Active Directory.
const (
	ridAdministrator   = 500
	ridKrbtgt          = 502
	ridDomainAdmins    = 512
	ridDomainUsers     = 513
	ridDomainComputers = 515
	ridDomainCtrls     = 516
	ridEnterpriseAdm   = 519

	// RID base offsets keep the three principal classes in disjoint ranges
	// so an index in one never collides with another's, and neither with a
	// well-known RID. AD RIDs are 32-bit, but SharpHound/BloodHound treat a
	// SID as an opaque string, so these oversized bases are only ever
	// compared for equality, never interpreted.
	ridGroupBase    = 1000
	ridUserBase     = 100_000_000
	ridComputerBase = 200_000_000
)

func (c config) groupSID(d, gi int) string {
	return fmt.Sprintf("%s-%d", c.domainSID(d), ridGroupBase+gi)
}
func (c config) userSID(d, ui int) string {
	return fmt.Sprintf("%s-%d", c.domainSID(d), ridUserBase+ui)
}
func (c config) computerSID(d, ci int) string {
	return fmt.Sprintf("%s-%d", c.domainSID(d), ridComputerBase+ci)
}
func (c config) wellKnownSID(d, rid int) string {
	return fmt.Sprintf("%s-%d", c.domainSID(d), rid)
}

// guid derives a stable GUID string for a non-principal object (OU, GPO,
// container) from its kind tag and index.
func (c config) guid(kind, d, i int) string {
	h1 := c.hash(3, int64(kind), int64(d), int64(i))
	h2 := c.hash(4, int64(kind), int64(d), int64(i))
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], h1)
	binary.BigEndian.PutUint64(b[8:16], h2)
	return fmt.Sprintf("%08X-%04X-%04X-%04X-%012X",
		binary.BigEndian.Uint32(b[0:4]), binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]), binary.BigEndian.Uint16(b[8:10]), b[10:16])
}

// --- realistic noise knobs -------------------------------------------------

const (
	// extraGroupMembers is the mean number of user members a non-well-known
	// group lists beyond primary-group (Domain Users) membership, giving the
	// ~8-memberships-per-user density adgen's README calibrated as realistic
	// once amortized across groups.
	extraGroupMembers = 6

	// groupNestChance (out of 100) that a group nests into an earlier group
	// in the same domain, and groupNestDepthCap bounding the chain.
	groupNestChance   = 25
	groupNestDepthCap = 5

	// sessionChance (out of 100) that a computer carries a non-privileged
	// user session, and aclFanout the mean number of ACL ACEs a group
	// carries from user/group principals.
	sessionChance = 30
	aclFanout     = 4

	// kerberoastPct / asrepPct / unconstrainedPct are the fractions (out of
	// 100) of users that are kerberoastable (carry an SPN), AS-REP roastable
	// (do not require pre-auth), and of computers trusted for unconstrained
	// delegation, respectively -- realistic single-digit exposure rates,
	// each an independent attack primitive BloodHound surfaces.
	kerberoastPct    = 4
	asrepPct         = 3
	unconstrainedPct = 2
)

// baseTime is a fixed epoch (2023-01-01) the generated whencreated/lastlogon
// timestamps hang off, so a graph is reproducible and its
// datetime()-relative hygiene queries have realistic recent data to match.
const baseTime int64 = 1672531200

// --- attack-path layout ----------------------------------------------------
//
// The lowest few indices in every domain are RESERVED for a fixed set of
// attack paths so a test can assert each one exists by walking known SIDs.
// Noise generation must not disturb these reserved objects' seeded edges;
// it only ever ADDS to them.

const (
	// Reserved group indices per domain (see attackGroupReserve):
	//   0 = "Server Admins"  (member of Domain Admins)
	//   1 = "IT Admins"      (member of Server Admins)
	//   2 = "Helpdesk"       (member of IT Admins)
	//   3 = "App Owners"     (AddMember over Domain Admins via an ACL)
	agServerAdmins = 0
	agITAdmins     = 1
	agHelpdesk     = 2
	agAppOwners    = 3

	attackGroupReserve = 4

	// Reserved user indices per domain:
	//   0 = nested-membership foothold (member of Helpdesk)
	//   1 = ACL foothold (GenericAll over App Owners)
	//   2 = kerberoastable account with AdminTo over reserved computer 0
	auNestedFoothold = 0
	auACLFoothold    = 1
	auKerberoastable = 2

	// Reserved computer indices per domain:
	//   0 = holds a Domain Admin (Administrator) session; reserved user 2 is AdminTo it
	//   1 = trusted for unconstrained delegation, also holds a DA session
	acDASessionHost = 0
	acUnconstrained = 1
)

// attackComputerReserve is how many computers the attack paths need present
// in a domain; a domain generated with fewer simply omits the paths that
// need the missing computers (the tests scale their expectations to the
// counts, and main.go warns).
const attackComputerReserve = 2

// --- azure id layout ---------------------------------------------------------
//
// Azure object identifiers are GUIDs, derived exactly like the AD side's
// guid(): pure functions of (seed, kind tag, index), so any emitter can
// reference any object without shared state. Distinct kind tags keep the
// classes collision-free. Emitted uppercase throughout: BloodHound joins
// azure edges to nodes by exact objectid string, so one consistent case is
// the whole requirement, and uppercase matches how ingest builds composite
// ids (e.g. an AZRole's ROLEDEFINITIONID@TENANTID).
const (
	azgTenant  = 100
	azgUser    = 101
	azgGroup   = 102
	azgApp     = 103 // the app REGISTRATION's directory object id
	azgAppID   = 104 // the app's appId (client id) -- a distinct GUID in AAD
	azgSP      = 105
	azgDevice  = 106
	azgSub     = 107
	azgRG      = 108
	azgVM      = 109
	azgVault   = 110
	azgGraphSP = 111 // the tenant's own "Microsoft Graph" service principal
)

func (c config) azTenantID() string         { return c.guid(azgTenant, 0, 0) }
func (c config) azUserID(i int) string      { return c.guid(azgUser, 0, i) }
func (c config) azGroupID(i int) string     { return c.guid(azgGroup, 0, i) }
func (c config) azAppObjectID(i int) string { return c.guid(azgApp, 0, i) }
func (c config) azAppClientID(i int) string { return c.guid(azgAppID, 0, i) }
func (c config) azSPID(i int) string        { return c.guid(azgSP, 0, i) }
func (c config) azDeviceID(i int) string    { return c.guid(azgDevice, 0, i) }
func (c config) azSubID(i int) string       { return c.guid(azgSub, 0, i) }
func (c config) azRGID(i int) string        { return c.guid(azgRG, 0, i) }
func (c config) azVMID(i int) string        { return c.guid(azgVM, 0, i) }
func (c config) azVaultID(i int) string     { return c.guid(azgVault, 0, i) }
func (c config) azGraphSPID() string        { return c.guid(azgGraphSP, 0, 0) }

// azSyncedUsers is how many AZUsers carry an on-prem identity; azure user i
// (for i < that count) syncs to AD user index i in domain i%domains, so the
// on-prem SID always resolves to an emitted AD user.
func (c config) azSyncedUsers() int {
	return c.AZUsers * c.AZSyncPct / 100
}

// Reserved azure indices, mirroring the AD side's reserved-attack-path
// convention: noise never disturbs these, tests assert them by exact id.
const (
	// AZUsers:
	//   0 = Global Administrator (also the hybrid target: AD user 0 of
	//       domain 0 syncs INTO it when AZSyncPct > 0)
	//   1 = eligible (PIM) for Privileged Role Administrator
	//   2 = owner of app 0 (whose SP holds RoleManagement.ReadWrite.Directory
	//       on MS Graph -- the AZMGGrantRole escalation)
	//   3 = member of group 0 (the role-assignable tier-zero group)
	//   4 = GetSecrets/GetKeys/GetCertificates on key vault 0
	//   5 = AZVMAdminLogin on VM 0
	azuGlobalAdmin = 0
	azuPIMEligible = 1
	azuAppOwner    = 2
	azuGroupMember = 3
	azuVaultReader = 4
	azuVMAdmin     = 5
	//   6 = Intune Administrator (post-processing: AZExecuteCommand to devices)
	//   7 = Application Administrator scoped to app 1 (AZAppAdmin at ingest)
	//   8 = approver for the Global Administrator role (AZRoleApprover)
	//   9 = Privileged Authentication Administrator holder
	//  10 = Application Administrator at tenant scope (AZHasRole; the role
	//       node then fans AZAddSecret out to every app/SP in analysis)
	azuIntuneAdmin      = 6
	azuScopedAppAdmin   = 7
	azuRoleApprover     = 8
	azuPAAHolder        = 9
	azuUnscopedAppAdmin = 10

	// azAttackUserReserve is the AZUsers floor when the azure side is
	// enabled at all -- indices 0-10 above carry the seeded paths.
	azAttackUserReserve = 11

	// AZGroups: 0 = role-assignable group holding Privileged Role Admin.
	azgTierZeroGroup = 0

	azAttackGroupReserve = 1
	// AZApps: 0 = the escalation app (owner: user 2; SP 0 has the MS Graph
	// role grant).
	azAttackAppReserve = 1
)
