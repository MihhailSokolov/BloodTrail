// SPDX-License-Identifier: Apache-2.0

// emit.go turns a config into SharpHound v6 collection files. Each object
// type is streamed through a chunkWriter -- objects are generated and
// written one at a time, never accumulated -- so peak memory is one object
// plus a JSON buffer regardless of how many hundreds of thousands the run
// produces. Every reference an object emits is computed from indices via
// config's deriveXxxSID helpers, so it always names a real emitted object.
package main

import (
	"fmt"
	"path/filepath"
)

// generate writes the whole forest into dir, returning the files written and
// the per-type object totals. It is the one entry point emit.go exposes; the
// per-type emitters below are ordinary methods on generator.
func generate(cfg config, dir string, chunkSize int) (*summary, error) {
	g := &generator{cfg: cfg, dir: dir, chunk: chunkSize}
	sum := &summary{}
	for _, step := range []struct {
		typ string
		fn  func(*summary) error
	}{
		{"users", g.emitUsers},
		{"computers", g.emitComputers},
		{"groups", g.emitGroups},
		{"gpos", g.emitGPOs},
		{"ous", g.emitOUs},
		{"containers", g.emitContainers},
		{"domains", g.emitDomains},
		{"azure", g.emitAzure},
	} {
		if err := step.fn(sum); err != nil {
			return nil, fmt.Errorf("emit %s: %w", step.typ, err)
		}
	}
	sum.Files = g.files
	return sum, nil
}

type generator struct {
	cfg   config
	dir   string
	chunk int
	files []string
}

type summary struct {
	Users, Computers, Groups, GPOs, OUs, Containers, Domains int
	AZObjects                                                int
	Files                                                    []string
}

// open starts a chunkWriter for one object type, recording its files.
func (g *generator) open(typ string) (*chunkWriter, error) {
	prefix := filepath.Join(g.dir, sanitizeDomain(g.cfg.Domain))
	w, err := newChunkWriter(prefix, typ, metaType(typ), g.chunk)
	if err != nil {
		return nil, err
	}
	w.onClose = func(path string) { g.files = append(g.files, path) }
	return w, nil
}

// --- users -----------------------------------------------------------------

func (g *generator) emitUsers(sum *summary) error {
	w, err := g.open("users")
	if err != nil {
		return err
	}
	c := g.cfg
	for d := 0; d < c.domainCount(); d++ {
		dsid, dname := c.domainSID(d), c.domainName(d)
		// Administrator (RID 500) and krbtgt (RID 502) are seeded first, as
		// real domains carry them; Administrator is the Domain Admin every
		// tier-0 path terminates at.
		if err := w.write(g.adminUser(d, dsid, dname)); err != nil {
			return err
		}
		if err := w.write(g.krbtgtUser(d, dsid, dname)); err != nil {
			return err
		}
		sum.Users += 2
		for ui := 0; ui < c.usersInDomain(d); ui++ {
			if err := w.write(g.user(d, ui, dsid, dname)); err != nil {
				return err
			}
			sum.Users++
		}
	}
	return w.close()
}

func (g *generator) adminUser(d int, dsid, dname string) User {
	return User{
		Properties: map[string]any{
			"domain": dname, "name": "ADMINISTRATOR@" + dname,
			"distinguishedname": "CN=ADMINISTRATOR,CN=USERS," + domainDN(dname),
			"domainsid":         dsid, "enabled": true, "admincount": true,
			"description": "Built-in account for administering the domain",
			"whencreated": baseTime, "pwdlastset": baseTime, "lastlogon": baseTime + 86400,
			"pwdneverexpires": true, "hasspn": false, "dontreqpreauth": false,
			"unconstraineddelegation": false, "sensitive": false, "passwordnotreqd": false,
			"serviceprincipalnames": []string{},
		},
		PrimaryGroupSID:   g.cfg.wellKnownSID(d, ridDomainUsers),
		AllowedToDelegate: []TypedPrincipal{}, HasSIDHistory: []TypedPrincipal{}, SpnTargets: []TypedPrincipal{},
		Aces:             []ACE{},
		ObjectIdentifier: g.cfg.wellKnownSID(d, ridAdministrator),
	}
}

func (g *generator) krbtgtUser(d int, dsid, dname string) User {
	return User{
		Properties: map[string]any{
			"domain": dname, "name": "KRBTGT@" + dname,
			"distinguishedname": "CN=KRBTGT,CN=USERS," + domainDN(dname),
			"domainsid":         dsid, "enabled": false, "admincount": true,
			"description": "Key Distribution Center Service Account",
			"whencreated": baseTime, "pwdlastset": baseTime, "lastlogon": int64(0),
			"hasspn": true, "dontreqpreauth": false, "unconstraineddelegation": false,
			"serviceprincipalnames": []string{"kadmin/changepw"},
		},
		PrimaryGroupSID:   g.cfg.wellKnownSID(d, ridDomainUsers),
		AllowedToDelegate: []TypedPrincipal{}, HasSIDHistory: []TypedPrincipal{},
		SpnTargets: []TypedPrincipal{}, Aces: []ACE{},
		ObjectIdentifier: g.cfg.wellKnownSID(d, ridKrbtgt),
	}
}

func (g *generator) user(d, ui int, dsid, dname string) User {
	c := g.cfg
	kerberoast := c.intn(100, 10, int64(d), int64(ui)) < kerberoastPct || ui == auKerberoastable
	asrep := c.intn(100, 11, int64(d), int64(ui)) < asrepPct
	name := fmt.Sprintf("%s@%s", userSam(d, ui), dname)
	spns := []string{}
	if kerberoast {
		spns = []string{fmt.Sprintf("MSSQLSvc/db%02d-%d.%s:1433", ui%64, d, dname)}
	}
	// A slice of accounts have a stale password (last set ~2 years before
	// baseTime), realistic hygiene-query fodder.
	pwdlastset := baseTime - int64(c.intn(90, 12, int64(d), int64(ui)))*86400
	if c.intn(100, 13, int64(d), int64(ui)) < 15 {
		pwdlastset = baseTime - int64(730+c.intn(400, 14, int64(d), int64(ui)))*86400
	}
	u := User{
		Properties: map[string]any{
			"domain": dname, "name": name,
			"distinguishedname": fmt.Sprintf("CN=%s,OU=EMPLOYEES,%s", userSam(d, ui), domainDN(dname)),
			"domainsid":         dsid, "enabled": true,
			"description": fmt.Sprintf("%s in %s", jobTitle(c.intn(len(jobTitles), 15, int64(d), int64(ui))), dname),
			"whencreated": baseTime - int64(c.intn(1000, 16, int64(d), int64(ui)))*86400,
			"pwdlastset":  pwdlastset,
			"lastlogon":   baseTime + int64(c.intn(200, 17, int64(d), int64(ui)))*3600,
			"hasspn":      kerberoast, "dontreqpreauth": asrep,
			"unconstraineddelegation": false, "pwdneverexpires": c.intn(100, 18, int64(d), int64(ui)) < 20,
			"sensitive": false, "passwordnotreqd": false, "admincount": false,
			"serviceprincipalnames": spns,
		},
		PrimaryGroupSID:   c.wellKnownSID(d, ridDomainUsers),
		AllowedToDelegate: []TypedPrincipal{}, HasSIDHistory: []TypedPrincipal{}, SpnTargets: []TypedPrincipal{},
		Aces:             []ACE{},
		ObjectIdentifier: c.userSID(d, ui),
	}
	return u
}

// --- computers -------------------------------------------------------------

func (g *generator) emitComputers(sum *summary) error {
	w, err := g.open("computers")
	if err != nil {
		return err
	}
	c := g.cfg
	for d := 0; d < c.domainCount(); d++ {
		dsid, dname := c.domainSID(d), c.domainName(d)
		for ci := 0; ci < c.computersInDomain(d); ci++ {
			if err := w.write(g.computer(d, ci, dsid, dname)); err != nil {
				return err
			}
			sum.Computers++
		}
	}
	return w.close()
}

func (g *generator) computer(d, ci int, dsid, dname string) Computer {
	c := g.cfg
	unconstrained := c.intn(100, 20, int64(d), int64(ci)) < unconstrainedPct || ci == acUnconstrained
	host := fmt.Sprintf("HOST%06d", ci)
	comp := Computer{
		Properties: map[string]any{
			"domain": dname, "name": host + "." + dname,
			"distinguishedname": fmt.Sprintf("CN=%s,OU=COMPUTERS,%s", host, domainDN(dname)),
			"domainsid":         dsid, "enabled": true, "haslaps": c.intn(100, 21, int64(d), int64(ci)) < 60,
			"whencreated":             baseTime - int64(c.intn(900, 22, int64(d), int64(ci)))*86400,
			"lastlogontimestamp":      baseTime + int64(c.intn(200, 23, int64(d), int64(ci)))*3600,
			"operatingsystem":         osName(c.intn(len(osNames), 24, int64(d), int64(ci))),
			"unconstraineddelegation": unconstrained,
			"trustedtoauth":           false,
			"serviceprincipalnames":   []string{"HOST/" + host, "HOST/" + host + "." + dname},
		},
		PrimaryGroupSID:   c.wellKnownSID(d, ridDomainComputers),
		AllowedToDelegate: []TypedPrincipal{}, AllowedToAct: []TypedPrincipal{}, HasSIDHistory: []TypedPrincipal{},
		Sessions: emptySessions(), PrivilegedSessions: emptySessions(), RegistrySessions: emptySessions(),
		LocalGroups:      []LocalGroupAPIResult{},
		Aces:             []ACE{},
		ObjectIdentifier: c.computerSID(d, ci),
	}

	var sessions []Session
	// Reserved computer 0 holds a Domain Admin (Administrator) session --
	// the terminal hop of the kerberoast->AdminTo->session path -- and
	// reserved computer 1 (unconstrained delegation) does too, so cracking
	// or coercing it reaches a DA ticket.
	if ci == acDASessionHost || ci == acUnconstrained {
		sessions = append(sessions, Session{UserSID: c.wellKnownSID(d, ridAdministrator), ComputerSID: comp.ObjectIdentifier})
	}
	// A fraction of ordinary workstations carry a random user's session
	// (privileged if that user happens to sit in an admin group -- BloodHound
	// works that out from the graph).
	if c.intn(100, 25, int64(d), int64(ci)) < sessionChance && c.usersInDomain(d) > 0 {
		ui := c.intn(c.usersInDomain(d), 26, int64(d), int64(ci))
		sessions = append(sessions, Session{UserSID: c.userSID(d, ui), ComputerSID: comp.ObjectIdentifier})
	}
	if len(sessions) > 0 {
		comp.Sessions.Results = sessions
	}

	// Every computer's local Administrators group: Domain Admins always (the
	// realistic baseline), the IT Admins tier group on a stride of machines
	// (broad tier-1 AdminTo coverage), and -- on reserved computer 0 only --
	// the kerberoastable reserved user, so cracking that account yields local
	// admin on the very box holding a DA session.
	admins := []TypedPrincipal{{ObjectIdentifier: c.wellKnownSID(d, ridDomainAdmins), ObjectType: "Group"}}
	if ci%5 == 0 && c.groupsInDomain(d) > agITAdmins {
		admins = append(admins, TypedPrincipal{ObjectIdentifier: c.groupSID(d, agITAdmins), ObjectType: "Group"})
	}
	if ci == acDASessionHost && c.usersInDomain(d) > auKerberoastable {
		admins = append(admins, TypedPrincipal{ObjectIdentifier: c.userSID(d, auKerberoastable), ObjectType: "User"})
	}
	comp.LocalGroups = []LocalGroupAPIResult{{
		Collected: true, Name: "ADMINISTRATORS@" + host + "." + dname,
		ObjectIdentifier: comp.ObjectIdentifier + "-544",
		LocalName:        []string{},
		Results:          admins,
	}}
	return comp
}

// --- groups ----------------------------------------------------------------

func (g *generator) emitGroups(sum *summary) error {
	w, err := g.open("groups")
	if err != nil {
		return err
	}
	c := g.cfg
	for d := 0; d < c.domainCount(); d++ {
		for _, wk := range g.wellKnownGroups(d) {
			if err := w.write(wk); err != nil {
				return err
			}
			sum.Groups++
		}
		for gi := 0; gi < c.groupsInDomain(d); gi++ {
			if err := w.write(g.group(d, gi)); err != nil {
				return err
			}
			sum.Groups++
		}
	}
	return w.close()
}

// wellKnownGroups returns a domain's RID-suffixed groups, their Members and
// Aces carrying the tier-0 attack-path terminals.
func (g *generator) wellKnownGroups(d int) []Group {
	c := g.cfg
	dsid, dname := c.domainSID(d), c.domainName(d)
	mk := func(rid int, label, desc string, members []TypedPrincipal, aces []ACE) Group {
		return Group{
			Properties: map[string]any{
				"domain": dname, "name": label + "@" + dname,
				"distinguishedname": fmt.Sprintf("CN=%s,CN=USERS,%s", label, domainDN(dname)),
				"domainsid":         dsid, "admincount": true,
				"description": desc, "whencreated": baseTime,
			},
			Members: members, Aces: aces,
			ObjectIdentifier: c.wellKnownSID(d, rid),
		}
	}
	// Domain Admins: Administrator, the nested "Server Admins" chain head, and
	// the App Owners group's AddMember ACL -- three independent paths in.
	da := mk(ridDomainAdmins, "DOMAIN ADMINS", "Designated administrators of the domain",
		[]TypedPrincipal{
			{ObjectIdentifier: c.wellKnownSID(d, ridAdministrator), ObjectType: "User"},
			{ObjectIdentifier: c.groupSID(d, agServerAdmins), ObjectType: "Group"},
		},
		[]ACE{{PrincipalSID: c.groupSID(d, agAppOwners), PrincipalType: "Group", RightName: "GenericAll", IsInherited: false}},
	)
	groups := []Group{
		da,
		mk(ridDomainUsers, "DOMAIN USERS", "All domain users", nil, nil),
		mk(ridDomainComputers, "DOMAIN COMPUTERS", "All workstations and servers joined to the domain", nil, nil),
		mk(ridDomainCtrls, "DOMAIN CONTROLLERS", "All domain controllers", nil, nil),
	}
	if d == 0 {
		groups = append(groups, mk(ridEnterpriseAdm, "ENTERPRISE ADMINS", "Designated administrators of the enterprise",
			[]TypedPrincipal{{ObjectIdentifier: c.wellKnownSID(0, ridAdministrator), ObjectType: "User"}}, nil))
	}
	return groups
}

func (g *generator) group(d, gi int) Group {
	c := g.cfg
	dsid, dname := c.domainSID(d), c.domainName(d)
	label := groupName(d, gi)
	grp := Group{
		Properties: map[string]any{
			"domain": dname, "name": label + "@" + dname,
			"distinguishedname": fmt.Sprintf("CN=%s,OU=GROUPS,%s", label, domainDN(dname)),
			"domainsid":         dsid, "admincount": false,
			"description": "Security group " + label, "whencreated": baseTime - int64(c.intn(900, 30, int64(d), int64(gi)))*86400,
		},
		Members:          g.groupMembers(d, gi),
		Aces:             g.groupAces(d, gi),
		ObjectIdentifier: c.groupSID(d, gi),
	}
	return grp
}

// groupMembers builds group gi's Members: its seeded attack-path members
// first (so they are present regardless of noise), then a deterministic
// sample of ordinary user members.
func (g *generator) groupMembers(d, gi int) []TypedPrincipal {
	c := g.cfg
	var members []TypedPrincipal
	// The nested-admin chain: Server Admins <- IT Admins <- Helpdesk <- user0.
	switch gi {
	case agServerAdmins:
		members = append(members, TypedPrincipal{ObjectIdentifier: c.groupSID(d, agITAdmins), ObjectType: "Group"})
	case agITAdmins:
		members = append(members, TypedPrincipal{ObjectIdentifier: c.groupSID(d, agHelpdesk), ObjectType: "Group"})
	case agHelpdesk:
		if c.usersInDomain(d) > auNestedFoothold {
			members = append(members, TypedPrincipal{ObjectIdentifier: c.userSID(d, auNestedFoothold), ObjectType: "User"})
		}
	}
	// Group nesting: gi may nest into an earlier ordinary group (adds this
	// group as a member of that one -- recorded on the PARENT, so emitted
	// here means "gi's own members include a deeper group"). To keep it
	// streaming and acyclic, gi lists a deterministic later group as member
	// only within the depth cap.
	if gi >= attackGroupReserve && c.intn(100, 31, int64(d), int64(gi)) < groupNestChance {
		depth := c.intn(groupNestDepthCap, 32, int64(d), int64(gi)) + 1
		child := gi + depth
		if child < c.groupsInDomain(d) {
			members = append(members, TypedPrincipal{ObjectIdentifier: c.groupSID(d, child), ObjectType: "Group"})
		}
	}
	// Ordinary user members: a deterministic sample.
	ud := c.usersInDomain(d)
	n := c.intn(extraGroupMembers*2, 33, int64(d), int64(gi))
	for k := 0; k < n && ud > 0; k++ {
		ui := c.intn(ud, 34, int64(d), int64(gi), int64(k))
		members = append(members, TypedPrincipal{ObjectIdentifier: c.userSID(d, ui), ObjectType: "User"})
	}
	return members
}

// groupAces builds group gi's inbound ACLs: the seeded ACL-abuse path first,
// then deterministic noise from user/group principals.
func (g *generator) groupAces(d, gi int) []ACE {
	c := g.cfg
	var aces []ACE
	// App Owners: reserved user 1 has GenericAll over it (and App Owners has
	// AddMember over Domain Admins, set on the DA group), so user1 -> App
	// Owners -> Domain Admins.
	if gi == agAppOwners && c.usersInDomain(d) > auACLFoothold {
		aces = append(aces, ACE{PrincipalSID: c.userSID(d, auACLFoothold), PrincipalType: "User", RightName: "GenericAll", IsInherited: false})
	}
	n := c.intn(aclFanout*2, 35, int64(d), int64(gi))
	ud, gd := c.usersInDomain(d), c.groupsInDomain(d)
	for k := 0; k < n; k++ {
		right := aclRights[c.intn(len(aclRights), 36, int64(d), int64(gi), int64(k))]
		if c.intn(2, 37, int64(d), int64(gi), int64(k)) == 0 && ud > 0 {
			aces = append(aces, ACE{PrincipalSID: c.userSID(d, c.intn(ud, 38, int64(d), int64(gi), int64(k))), PrincipalType: "User", RightName: right})
		} else if gd > 0 {
			other := c.intn(gd, 39, int64(d), int64(gi), int64(k))
			// Never let a group hold an ACL over ITSELF. Such an edge is a
			// self-loop, and a self-loop of an admitted kind makes the
			// engine's variable-length executor decline the whole pattern
			// (internal/engine/interpret/expand.go's SELF-LOOPS rule, which
			// exists because PostgreSQL's own recursive CTE places its
			// self-loop guard on whichever side it seeds from). One stray
			// self-ACE would therefore push every `[*1..]` query in a
			// benchmark onto the delegating path and understate the engine
			// for a reason that has nothing to do with the engine -- and it
			// is not realistic AD data either. Skipped rather than remapped
			// so the remaining draws keep their own indices.
			if other != gi {
				aces = append(aces, ACE{PrincipalSID: c.groupSID(d, other), PrincipalType: "Group", RightName: right})
			}
		}
	}
	return aces
}

// --- gpos, ous, containers, domains ---------------------------------------

func (g *generator) emitGPOs(sum *summary) error {
	w, err := g.open("gpos")
	if err != nil {
		return err
	}
	c := g.cfg
	for d := 0; d < c.domainCount(); d++ {
		dsid, dname := c.domainSID(d), c.domainName(d)
		for i := 0; i < gposPerDomain; i++ {
			if err := w.write(GPO{
				Properties: map[string]any{
					"domain": dname, "name": fmt.Sprintf("POLICY-%02d@%s", i, dname),
					"distinguishedname": fmt.Sprintf("CN={%s},CN=POLICIES,CN=SYSTEM,%s", c.guid(gpoKind, d, i), domainDN(dname)),
					"domainsid":         dsid, "whencreated": baseTime,
					"gpcpath": fmt.Sprintf("\\\\%s\\SYSVOL\\%s\\POLICIES\\{%s}", dname, dname, c.guid(gpoKind, d, i)),
				},
				Aces:             []ACE{},
				ObjectIdentifier: c.guid(gpoKind, d, i),
			}); err != nil {
				return err
			}
			sum.GPOs++
		}
	}
	return w.close()
}

func (g *generator) emitOUs(sum *summary) error {
	w, err := g.open("ous")
	if err != nil {
		return err
	}
	c := g.cfg
	for d := 0; d < c.domainCount(); d++ {
		dsid, dname := c.domainSID(d), c.domainName(d)
		for i, name := range ouNames {
			if err := w.write(OU{
				GPOChanges: emptyGPOChanges(),
				Properties: map[string]any{
					"domain": dname, "name": name + "@" + dname,
					"distinguishedname": fmt.Sprintf("OU=%s,%s", name, domainDN(dname)),
					"domainsid":         dsid, "whencreated": baseTime,
				},
				Links:            []GPLink{{IsEnforced: false, GUID: c.guid(gpoKind, d, 0)}},
				Aces:             []ACE{},
				ObjectIdentifier: c.guid(ouKind, d, i),
			}); err != nil {
				return err
			}
			sum.OUs++
		}
	}
	return w.close()
}

func (g *generator) emitContainers(sum *summary) error {
	w, err := g.open("containers")
	if err != nil {
		return err
	}
	c := g.cfg
	for d := 0; d < c.domainCount(); d++ {
		dsid, dname := c.domainSID(d), c.domainName(d)
		for i, name := range containerNames {
			if err := w.write(Container{
				Properties: map[string]any{
					"domain": dname, "name": name + "@" + dname,
					"distinguishedname": fmt.Sprintf("CN=%s,%s", name, domainDN(dname)),
					"domainsid":         dsid,
				},
				Aces:             []ACE{},
				ObjectIdentifier: c.guid(containerKind, d, i),
			}); err != nil {
				return err
			}
			sum.Containers++
		}
	}
	return w.close()
}

func (g *generator) emitDomains(sum *summary) error {
	w, err := g.open("domains")
	if err != nil {
		return err
	}
	c := g.cfg
	for d := 0; d < c.domainCount(); d++ {
		dsid, dname := c.domainSID(d), c.domainName(d)
		dom := Domain{
			Properties: map[string]any{
				"domain": dname, "name": dname, "distinguishedname": domainDN(dname),
				"domainsid": dsid, "collected": true, "whencreated": baseTime, "functionallevel": "2016",
			},
			Trusts:           g.domainTrusts(d),
			Links:            []GPLink{{IsEnforced: false, GUID: c.guid(gpoKind, d, 0)}},
			Aces:             []ACE{},
			ObjectIdentifier: dsid,
		}
		if err := w.write(dom); err != nil {
			return err
		}
		sum.Domains++
	}
	return w.close()
}

// domainTrusts links every child domain bidirectionally to the forest root,
// so with -domains > 1 a principal in one domain can reach another across
// the trust (the cross-domain attack surface).
func (g *generator) domainTrusts(d int) []Trust {
	c := g.cfg
	if c.domainCount() < 2 {
		return []Trust{}
	}
	trust := func(other int) Trust {
		return Trust{
			TargetDomainSid: c.domainSID(other), TargetDomainName: c.domainName(other),
			IsTransitive: true, SidFilteringEnabled: false, TGTDelegationEnabled: true,
			TrustDirection: "Bidirectional", TrustType: "ParentChild",
		}
	}
	if d == 0 {
		var ts []Trust
		for other := 1; other < c.domainCount(); other++ {
			ts = append(ts, trust(other))
		}
		return ts
	}
	return []Trust{trust(0)}
}

func emptySessions() SessionAPIResult {
	return SessionAPIResult{Results: []Session{}, Collected: true}
}
func emptyGPOChanges() GPOChanges {
	return GPOChanges{LocalAdmins: []TypedPrincipal{}, RemoteDesktopUsers: []TypedPrincipal{}, DcomUsers: []TypedPrincipal{}, PSRemoteUsers: []TypedPrincipal{}, AffectedComputers: []TypedPrincipal{}}
}
