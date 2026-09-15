// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// testConfig is a two-domain forest big enough to exercise every reserved
// attack path and the chunk roll-over, small enough to walk in memory.
func testConfig() config {
	return config{Domain: "MEGACORP.LOCAL", Users: 800, Computers: 300, Groups: 60, Domains: 2, Seed: 1}
}

// parsedForest indexes one generated output directory for the assertions.
type parsedForest struct {
	objects map[string]string // ObjectIdentifier -> type
	users   []User
	groups  []Group
	comps   []Computer
	domains []Domain
	ous     []OU
	metas   []Meta
	counts  map[string][]int // type -> per-file data lengths
}

func generateAndParse(t *testing.T, cfg config, chunk int) *parsedForest {
	t.Helper()
	dir := t.TempDir()
	if _, err := generate(cfg, dir, chunk); err != nil {
		t.Fatalf("generate: %v", err)
	}
	return parseDir(t, dir)
}

func parseDir(t *testing.T, dir string) *parsedForest {
	t.Helper()
	f := &parsedForest{objects: map[string]string{}, counts: map[string][]int{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var envelope struct {
			Data json.RawMessage `json:"data"`
			Meta Meta            `json:"meta"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		f.metas = append(f.metas, envelope.Meta)
		record := func(oid, typ string) {
			if oid == "" {
				t.Fatalf("%s: object with empty ObjectIdentifier", e.Name())
			}
			if prev, dup := f.objects[oid]; dup {
				t.Fatalf("%s: duplicate ObjectIdentifier %s (%s and %s)", e.Name(), oid, prev, typ)
			}
			f.objects[oid] = typ
		}
		n := 0
		switch envelope.Meta.Type {
		case "users":
			var xs []User
			mustParse(t, e.Name(), envelope.Data, &xs)
			for _, x := range xs {
				record(x.ObjectIdentifier, "User")
			}
			f.users = append(f.users, xs...)
			n = len(xs)
		case "groups":
			var xs []Group
			mustParse(t, e.Name(), envelope.Data, &xs)
			for _, x := range xs {
				record(x.ObjectIdentifier, "Group")
			}
			f.groups = append(f.groups, xs...)
			n = len(xs)
		case "computers":
			var xs []Computer
			mustParse(t, e.Name(), envelope.Data, &xs)
			for _, x := range xs {
				record(x.ObjectIdentifier, "Computer")
			}
			f.comps = append(f.comps, xs...)
			n = len(xs)
		case "domains":
			var xs []Domain
			mustParse(t, e.Name(), envelope.Data, &xs)
			for _, x := range xs {
				record(x.ObjectIdentifier, "Domain")
			}
			f.domains = append(f.domains, xs...)
			n = len(xs)
		case "ous":
			var xs []OU
			mustParse(t, e.Name(), envelope.Data, &xs)
			for _, x := range xs {
				record(x.ObjectIdentifier, "OU")
			}
			f.ous = append(f.ous, xs...)
			n = len(xs)
		case "containers":
			var xs []Container
			mustParse(t, e.Name(), envelope.Data, &xs)
			for _, x := range xs {
				record(x.ObjectIdentifier, "Container")
			}
			n = len(xs)
		case "gpos":
			var xs []GPO
			mustParse(t, e.Name(), envelope.Data, &xs)
			for _, x := range xs {
				record(x.ObjectIdentifier, "GPO")
			}
			n = len(xs)
		default:
			t.Fatalf("%s: unknown meta type %q", e.Name(), envelope.Meta.Type)
		}
		if envelope.Meta.Count != n {
			t.Fatalf("%s: meta.count = %d, data holds %d", e.Name(), envelope.Meta.Count, n)
		}
		f.counts[envelope.Meta.Type] = append(f.counts[envelope.Meta.Type], n)
	}
	return f
}

func mustParse(t *testing.T, name string, raw json.RawMessage, into any) {
	t.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("parse %s data: %v", name, err)
	}
}

func hasACE(aces []ACE, principal, right string) bool {
	for _, a := range aces {
		if a.PrincipalSID == principal && a.RightName == right {
			return true
		}
	}
	return false
}

// TestMetaMatchesFixtureFormat pins the SharpHound v6 envelope every file
// carries: the fixture-proven methods bitmask, version 6, and a per-file
// count equal to its data length (checked during parsing above).
func TestMetaMatchesFixtureFormat(t *testing.T) {
	f := generateAndParse(t, testConfig(), 50000)
	for _, m := range f.metas {
		if m.Version != 6 || m.Methods != 46067 {
			t.Fatalf("meta = %+v, want version 6 / methods 46067", m)
		}
	}
}

// TestReferentialIntegrity is the load-bearing pin: every reference any
// object emits must name another emitted object, or BloodHound's ingest
// silently drops the edge and the graph under test is not the graph
// documented.
func TestReferentialIntegrity(t *testing.T) {
	f := generateAndParse(t, testConfig(), 50000)

	require := func(where, oid string) {
		t.Helper()
		if _, ok := f.objects[oid]; !ok {
			t.Fatalf("%s references %s, which no emitted object carries", where, oid)
		}
	}

	for _, u := range f.users {
		require("user "+u.ObjectIdentifier+" PrimaryGroupSID", u.PrimaryGroupSID)
		for _, a := range u.Aces {
			require("user "+u.ObjectIdentifier+" ACE", a.PrincipalSID)
		}
	}
	for _, g := range f.groups {
		for _, m := range g.Members {
			require("group "+g.ObjectIdentifier+" member", m.ObjectIdentifier)
		}
		for _, a := range g.Aces {
			require("group "+g.ObjectIdentifier+" ACE", a.PrincipalSID)
		}
	}
	for _, cmp := range f.comps {
		require("computer "+cmp.ObjectIdentifier+" PrimaryGroupSID", cmp.PrimaryGroupSID)
		for _, s := range cmp.Sessions.Results {
			require("computer "+cmp.ObjectIdentifier+" session user", s.UserSID)
			if s.ComputerSID != cmp.ObjectIdentifier {
				t.Fatalf("computer %s session names ComputerSID %s", cmp.ObjectIdentifier, s.ComputerSID)
			}
		}
		for _, lg := range cmp.LocalGroups {
			for _, r := range lg.Results {
				require("computer "+cmp.ObjectIdentifier+" local group member", r.ObjectIdentifier)
			}
		}
	}
	for _, d := range f.domains {
		for _, tr := range d.Trusts {
			require("domain "+d.ObjectIdentifier+" trust", tr.TargetDomainSid)
		}
		for _, l := range d.Links {
			require("domain "+d.ObjectIdentifier+" gplink", l.GUID)
		}
	}
	for _, ou := range f.ous {
		for _, l := range ou.Links {
			require("ou "+ou.ObjectIdentifier+" gplink", l.GUID)
		}
	}
}

// TestSeededAttackPaths walks the generated data itself, proving each
// documented attack path exists in every domain.
func TestSeededAttackPaths(t *testing.T) {
	cfg := testConfig()
	f := generateAndParse(t, cfg, 50000)

	groupsBySID := map[string]Group{}
	for _, g := range f.groups {
		groupsBySID[g.ObjectIdentifier] = g
	}
	compsBySID := map[string]Computer{}
	for _, cmp := range f.comps {
		compsBySID[cmp.ObjectIdentifier] = cmp
	}
	usersBySID := map[string]User{}
	for _, u := range f.users {
		usersBySID[u.ObjectIdentifier] = u
	}

	hasMember := func(g Group, oid string) bool {
		for _, m := range g.Members {
			if m.ObjectIdentifier == oid {
				return true
			}
		}
		return false
	}

	for d := 0; d < cfg.domainCount(); d++ {
		da := groupsBySID[cfg.wellKnownSID(d, ridDomainAdmins)]

		// Path 1: nested membership. user0 -> Helpdesk -> IT Admins ->
		// Server Admins -> Domain Admins.
		if !hasMember(groupsBySID[cfg.groupSID(d, agHelpdesk)], cfg.userSID(d, auNestedFoothold)) {
			t.Fatalf("domain %d: Helpdesk does not hold the nested foothold user", d)
		}
		if !hasMember(groupsBySID[cfg.groupSID(d, agITAdmins)], cfg.groupSID(d, agHelpdesk)) {
			t.Fatalf("domain %d: IT Admins does not hold Helpdesk", d)
		}
		if !hasMember(groupsBySID[cfg.groupSID(d, agServerAdmins)], cfg.groupSID(d, agITAdmins)) {
			t.Fatalf("domain %d: Server Admins does not hold IT Admins", d)
		}
		if !hasMember(da, cfg.groupSID(d, agServerAdmins)) {
			t.Fatalf("domain %d: Domain Admins does not hold Server Admins", d)
		}

		// Path 2: ACL chain. user1 GenericAll over App Owners; App Owners
		// GenericAll over Domain Admins.
		appOwners := groupsBySID[cfg.groupSID(d, agAppOwners)]
		if !hasACE(appOwners.Aces, cfg.userSID(d, auACLFoothold), "GenericAll") {
			t.Fatalf("domain %d: App Owners lacks the foothold user's GenericAll ACE", d)
		}
		if !hasACE(da.Aces, cfg.groupSID(d, agAppOwners), "GenericAll") {
			t.Fatalf("domain %d: Domain Admins lacks App Owners' GenericAll ACE", d)
		}

		// Path 3: kerberoast. The reserved SVC-SQL user carries an SPN, sits
		// in reserved computer 0's local Administrators (AdminTo), and that
		// computer holds a Domain Admin (Administrator) session.
		svc := usersBySID[cfg.userSID(d, auKerberoastable)]
		if hasspn, _ := svc.Properties["hasspn"].(bool); !hasspn {
			t.Fatalf("domain %d: reserved kerberoastable user lacks hasspn", d)
		}
		host := compsBySID[cfg.computerSID(d, acDASessionHost)]
		foundAdminTo := false
		for _, lg := range host.LocalGroups {
			for _, r := range lg.Results {
				if r.ObjectIdentifier == svc.ObjectIdentifier {
					foundAdminTo = true
				}
			}
		}
		if !foundAdminTo {
			t.Fatalf("domain %d: DA-session host's local Administrators lack the kerberoastable user", d)
		}
		foundDASession := false
		for _, s := range host.Sessions.Results {
			if s.UserSID == cfg.wellKnownSID(d, ridAdministrator) {
				foundDASession = true
			}
		}
		if !foundDASession {
			t.Fatalf("domain %d: DA-session host holds no Administrator session", d)
		}

		// Path 4: unconstrained delegation host with a DA session.
		uncon := compsBySID[cfg.computerSID(d, acUnconstrained)]
		if v, _ := uncon.Properties["unconstraineddelegation"].(bool); !v {
			t.Fatalf("domain %d: reserved delegation host is not unconstrained", d)
		}
		foundDASession = false
		for _, s := range uncon.Sessions.Results {
			if s.UserSID == cfg.wellKnownSID(d, ridAdministrator) {
				foundDASession = true
			}
		}
		if !foundDASession {
			t.Fatalf("domain %d: unconstrained host holds no Administrator session", d)
		}
	}

	// Path 5: at least one AS-REP roastable user exists at this scale.
	asrep := 0
	for _, u := range f.users {
		if v, _ := u.Properties["dontreqpreauth"].(bool); v {
			asrep++
		}
	}
	if asrep == 0 {
		t.Fatal("no AS-REP roastable users generated")
	}

	// Path 6: with two domains, the trusts are mutual root<->child.
	if len(f.domains) != 2 {
		t.Fatalf("domains = %d, want 2", len(f.domains))
	}
	for _, d := range f.domains {
		if len(d.Trusts) != 1 {
			t.Fatalf("domain %s carries %d trusts, want 1", d.ObjectIdentifier, len(d.Trusts))
		}
	}
	if f.domains[0].Trusts[0].TargetDomainSid == f.domains[1].Trusts[0].TargetDomainSid {
		t.Fatal("both domains trust the same target; want mutual root<->child")
	}
}

// TestNoSelfLoopEdges pins a property the generated forest must hold at
// every scale and seed: no object references ITSELF. Such an edge is a
// self-loop, and a self-loop of an admitted kind makes BloodTrail's
// variable-length executor decline the whole pattern and delegate
// (internal/engine/interpret/expand.go's SELF-LOOPS rule) -- so one stray
// self-ACE would push every `[*1..]` query in a benchmark onto PostgreSQL
// and understate the engine for a generator-side reason. It is also not
// realistic AD data. Several seeds and shapes are probed because the noise
// draw that used to produce these hit only at some (groups, seed)
// combinations: a single-config test would have passed while the bug was
// live.
func TestNoSelfLoopEdges(t *testing.T) {
	for _, cfg := range []config{
		{Domain: "MEGACORP.LOCAL", Users: 500, Computers: 100, Groups: 10, Domains: 1, Seed: 1},
		{Domain: "MEGACORP.LOCAL", Users: 5000, Computers: 500, Groups: 40, Domains: 1, Seed: 7},
		{Domain: "MEGACORP.LOCAL", Users: 20000, Computers: 2000, Groups: 1000, Domains: 2, Seed: 3},
		{Domain: "MEGACORP.LOCAL", Users: 800, Computers: 300, Groups: 60, Domains: 2, Seed: 11},
	} {
		g := &generator{cfg: cfg}
		for d := 0; d < cfg.domainCount(); d++ {
			for gi := 0; gi < cfg.groupsInDomain(d); gi++ {
				sid := cfg.groupSID(d, gi)
				for _, a := range g.groupAces(d, gi) {
					if a.PrincipalSID == sid {
						t.Fatalf("seed %d, domain %d, group %d: ACL edge references itself (%s)", cfg.Seed, d, gi, sid)
					}
				}
				for _, m := range g.groupMembers(d, gi) {
					if m.ObjectIdentifier == sid {
						t.Fatalf("seed %d, domain %d, group %d: membership edge references itself (%s)", cfg.Seed, d, gi, sid)
					}
				}
			}
			for ci := 0; ci < cfg.computersInDomain(d) && ci < 500; ci++ {
				cmp := g.computer(d, ci, cfg.domainSID(d), cfg.domainName(d))
				for _, lg := range cmp.LocalGroups {
					for _, r := range lg.Results {
						if r.ObjectIdentifier == cmp.ObjectIdentifier {
							t.Fatalf("seed %d, domain %d, computer %d: local-group edge references itself", cfg.Seed, d, ci)
						}
					}
				}
			}
		}
	}
}

// TestChunkingRollsFiles pins that a small -chunk splits a type across
// several files, each independently valid with its own correct meta.count
// (parseDir already validates each file), and that the union carries every
// object exactly once (the duplicate check in record).
func TestChunkingRollsFiles(t *testing.T) {
	f := generateAndParse(t, testConfig(), 100)
	if files := len(f.counts["users"]); files < 2 {
		t.Fatalf("users split into %d files at chunk 100, want several", files)
	}
	for typ, ns := range f.counts {
		for i, n := range ns {
			if n > 100 {
				t.Fatalf("%s file %d holds %d objects, above the chunk cap", typ, i, n)
			}
		}
	}
}

// TestDeterministicOutput: the same seed and counts produce byte-identical
// files; a different seed does not.
func TestDeterministicOutput(t *testing.T) {
	digest := func(cfg config) string {
		t.Helper()
		dir := t.TempDir()
		if _, err := generate(cfg, dir, 50000); err != nil {
			t.Fatalf("generate: %v", err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		sort.Strings(names)
		h := sha256.New()
		for _, name := range names {
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = fmt.Fprintf(h, "%s\n", name)
			h.Write(b)
		}
		return fmt.Sprintf("%x", h.Sum(nil))
	}

	cfg := testConfig()
	first, second := digest(cfg), digest(cfg)
	if first != second {
		t.Fatal("two runs with the same seed differ")
	}
	cfg.Seed = 2
	if digest(cfg) == first {
		t.Fatal("a different seed produced identical output")
	}
}

// TestNamesAreFictitious guards the corporate-fiction contract: every name
// lives under the configured forest (default MEGACORP.LOCAL) and nothing
// leaks the repository's other fixture domain.
func TestNamesAreFictitious(t *testing.T) {
	f := generateAndParse(t, testConfig(), 50000)
	for _, u := range f.users {
		name, _ := u.Properties["name"].(string)
		if !strings.HasSuffix(name, "MEGACORP.LOCAL") {
			t.Fatalf("user name %q is outside the forest", name)
		}
		if strings.Contains(name, "TESTLAB") {
			t.Fatalf("user name %q leaks the verify fixture's domain", name)
		}
	}
}
