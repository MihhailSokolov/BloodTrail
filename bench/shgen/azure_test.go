// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func azTestConfig() config {
	c := config{Domain: "MEGACORP.LOCAL", Users: 400, Computers: 100, Groups: 40, Domains: 2, Seed: 1,
		AZUsers: 120, AZGroups: -1, AZApps: 3, AZDevices: -1, AZVMs: 12, AZKeyVault: 6, AZSubs: -1, AZSyncPct: 60}
	c.applyAzureDefaults()
	return c
}

// parsedAzure indexes the azure item stream by kind for the assertions.
type parsedAzure struct {
	tenants     []AZTenantData
	users       []AZUserData
	groups      []AZGroupData
	members     []AZGroupMembersData
	roles       []AZRoleData
	assignments []AZRoleAssignmentsData
	apps        []AZAppData
	appOwners   []AZAppOwnersData
	sps         []AZServicePrincipalData
	appRoles    []AZAppRoleAssignmentData
	devices     []AZDeviceData
	subs        []AZSubscriptionData
	rgs         []AZResourceGroupData
	vms         []AZVMData
	vmAdmins    []AZVMAdminLoginsData
	vaults      []AZKeyVaultData
	policies    []AZKeyVaultAccessPolicyData
	eligibility []AZRoleEligibilityData
	rmpas       []AZRoleManagementPolicyAssignmentData

	// ids is every azure object identifier a reference may point at.
	ids map[string]string // id -> kind
}

func generateAndParseAzure(t *testing.T, cfg config, chunk int) (*parsedAzure, *parsedForest, string) {
	t.Helper()
	dir := t.TempDir()
	if _, err := generate(cfg, dir, chunk); err != nil {
		t.Fatalf("generate: %v", err)
	}
	az := &parsedAzure{ids: map[string]string{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	for _, e := range entries {
		if !strings.Contains(e.Name(), "_azure_") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		var envelope struct {
			Data []struct {
				Kind string          `json:"kind"`
				Data json.RawMessage `json:"data"`
			} `json:"data"`
			Meta Meta `json:"meta"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		if envelope.Meta.Type != "azure" {
			t.Fatalf("%s meta.type = %q, want azure", e.Name(), envelope.Meta.Type)
		}
		if envelope.Meta.Count != len(envelope.Data) {
			t.Fatalf("%s meta.count = %d, want %d", e.Name(), envelope.Meta.Count, len(envelope.Data))
		}
		for _, item := range envelope.Data {
			switch item.Kind {
			case "AZTenant":
				var v AZTenantData
				mustParse(t, item.Kind, item.Data, &v)
				az.tenants = append(az.tenants, v)
				az.ids[strings.ToUpper(v.TenantID)] = item.Kind
			case "AZUser":
				var v AZUserData
				mustParse(t, item.Kind, item.Data, &v)
				az.users = append(az.users, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZGroup":
				var v AZGroupData
				mustParse(t, item.Kind, item.Data, &v)
				az.groups = append(az.groups, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZGroupMember":
				var v AZGroupMembersData
				mustParse(t, item.Kind, item.Data, &v)
				az.members = append(az.members, v)
			case "AZGroupOwner":
				// owners only referenced, nothing indexed
			case "AZRole":
				var v AZRoleData
				mustParse(t, item.Kind, item.Data, &v)
				az.roles = append(az.roles, v)
				az.ids[strings.ToUpper(v.ID+"@"+v.TenantID)] = item.Kind
			case "AZRoleAssignment":
				var v AZRoleAssignmentsData
				mustParse(t, item.Kind, item.Data, &v)
				az.assignments = append(az.assignments, v)
			case "AZApp":
				var v AZAppData
				mustParse(t, item.Kind, item.Data, &v)
				az.apps = append(az.apps, v)
				az.ids[strings.ToUpper(v.AppID)] = item.Kind
			case "AZAppOwner":
				var v AZAppOwnersData
				mustParse(t, item.Kind, item.Data, &v)
				az.appOwners = append(az.appOwners, v)
			case "AZServicePrincipal":
				var v AZServicePrincipalData
				mustParse(t, item.Kind, item.Data, &v)
				az.sps = append(az.sps, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZAppRoleAssignment":
				var v AZAppRoleAssignmentData
				mustParse(t, item.Kind, item.Data, &v)
				az.appRoles = append(az.appRoles, v)
			case "AZDevice":
				var v AZDeviceData
				mustParse(t, item.Kind, item.Data, &v)
				az.devices = append(az.devices, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZSubscription":
				var v AZSubscriptionData
				mustParse(t, item.Kind, item.Data, &v)
				az.subs = append(az.subs, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZResourceGroup":
				var v AZResourceGroupData
				mustParse(t, item.Kind, item.Data, &v)
				az.rgs = append(az.rgs, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZVM":
				var v AZVMData
				mustParse(t, item.Kind, item.Data, &v)
				az.vms = append(az.vms, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZVMAdminLogin":
				var v AZVMAdminLoginsData
				mustParse(t, item.Kind, item.Data, &v)
				az.vmAdmins = append(az.vmAdmins, v)
			case "AZKeyVault":
				var v AZKeyVaultData
				mustParse(t, item.Kind, item.Data, &v)
				az.vaults = append(az.vaults, v)
				az.ids[strings.ToUpper(v.ID)] = item.Kind
			case "AZKeyVaultAccessPolicy":
				var v AZKeyVaultAccessPolicyData
				mustParse(t, item.Kind, item.Data, &v)
				az.policies = append(az.policies, v)
			case "AZRoleEligibilityScheduleInstance":
				var v AZRoleEligibilityData
				mustParse(t, item.Kind, item.Data, &v)
				az.eligibility = append(az.eligibility, v)
			case "AZRoleManagementPolicyAssignment":
				var v AZRoleManagementPolicyAssignmentData
				mustParse(t, item.Kind, item.Data, &v)
				az.rmpas = append(az.rmpas, v)
			default:
				t.Fatalf("unknown azure item kind %q", item.Kind)
			}
		}
	}
	return az, parseDir(t, dir), dir
}

// TestAzureReferentialIntegrity walks every azure cross-reference and proves
// it resolves to an emitted object -- the same build-time invariant the AD
// side pins, extended to the tenant: dangling references silently create no
// edge at ingest, which would quietly hollow out the seeded paths.
func TestAzureReferentialIntegrity(t *testing.T) {
	cfg := azTestConfig()
	az, forest, _ := generateAndParseAzure(t, cfg, 50000)

	if len(az.tenants) != 1 {
		t.Fatalf("got %d tenants, want 1", len(az.tenants))
	}
	tid := az.tenants[0].TenantID

	requireID := func(ctx, id string) {
		t.Helper()
		if _, ok := az.ids[strings.ToUpper(id)]; !ok {
			t.Fatalf("%s references %q, which no emitted azure object carries", ctx, id)
		}
	}

	for _, u := range az.users {
		if u.TenantID != tid {
			t.Fatalf("user %s carries tenant %s, want %s", u.ID, u.TenantID, tid)
		}
		if u.OnPremisesSyncEnabled {
			if u.OnPremisesSecurityIdentifier == "" {
				t.Fatalf("user %s sync-enabled with empty onprem SID", u.ID)
			}
			if typ, ok := forest.objects[u.OnPremisesSecurityIdentifier]; !ok || typ != "User" {
				t.Fatalf("user %s onprem SID %s does not resolve to an emitted AD User (got %q)",
					u.ID, u.OnPremisesSecurityIdentifier, typ)
			}
		}
	}
	for _, m := range az.members {
		requireID("group-members groupId", m.GroupID)
		for _, e := range m.Members {
			requireID("group member", e.Member.ID)
		}
	}
	for _, a := range az.assignments {
		requireID("role assignment role", strings.ToUpper(a.RoleDefinitionID+"@"+a.TenantID))
		for _, e := range a.RoleAssignments {
			requireID("role assignment principal", e.PrincipalID)
			if e.DirectoryScopeID == "" || !strings.HasPrefix(e.DirectoryScopeID, "/") {
				t.Fatalf("role assignment %s has scope %q; must start with / (empty panics BloodHound's convertor)", e.ID, e.DirectoryScopeID)
			}
			if e.DirectoryScopeID != "/" {
				requireID("scoped role assignment target", e.DirectoryScopeID[1:])
			}
		}
	}
	for _, ao := range az.appOwners {
		requireID("app owner app", ao.AppID)
		for _, o := range ao.Owners {
			requireID("app owner", o.Owner.ID)
		}
	}
	for _, ar := range az.appRoles {
		if ar.AppID != msGraphAppID {
			t.Fatalf("app role assignment appId %q; BloodHound ignores anything but the MS Graph id", ar.AppID)
		}
		if ar.PrincipalType != "ServicePrincipal" {
			t.Fatalf("app role assignment principalType %q; BloodHound ignores anything else", ar.PrincipalType)
		}
		requireID("app role principal", ar.PrincipalID)
		requireID("app role resource", ar.ResourceID)
	}
	for _, rg := range az.rgs {
		requireID("resource group subscription", rg.SubscriptionID)
	}
	for _, vm := range az.vms {
		requireID("vm resource group", vm.ResourceGroupID)
	}
	for _, v := range az.vaults {
		requireID("vault resource group", v.ResourceGroup)
	}
	for _, p := range az.policies {
		requireID("vault policy vault", p.KeyVaultID)
		requireID("vault policy principal", p.ObjectID)
	}
	for _, va := range az.vmAdmins {
		requireID("vm admin vm", va.VirtualMachineID)
		for _, e := range va.AdminLogins {
			requireID("vm admin principal", e.AdminLogin.Properties.PrincipalID)
		}
	}
	for _, el := range az.eligibility {
		requireID("eligibility principal", el.PrincipalID)
		requireID("eligibility role", strings.ToUpper(el.RoleDefinitionID)+"@"+tid)
	}
	for _, r := range az.rmpas {
		requireID("policy assignment role", strings.ToUpper(r.RoleDefinitionID)+"@"+tid)
		for _, a := range r.EndUserAssignmentUserApprovers {
			requireID("policy user approver", a)
		}
	}
}

// TestAzureSeededPaths asserts each seeded azure attack path is present in
// the emitted data, by exact reserved id -- the ingest/analysis recipes then
// turn these into the derived edges the prebuilt queries traverse.
func TestAzureSeededPaths(t *testing.T) {
	cfg := azTestConfig()
	az, _, _ := generateAndParseAzure(t, cfg, 50000)

	hasAssignment := func(template, principal, scope string) bool {
		for _, a := range az.assignments {
			for _, e := range a.RoleAssignments {
				if e.RoleDefinitionID == template && e.PrincipalID == principal && e.DirectoryScopeID == scope {
					return true
				}
			}
		}
		return false
	}

	if !hasAssignment(azRoles[azRoleGA].template, cfg.azUserID(azuGlobalAdmin), "/") {
		t.Fatal("missing: Global Administrator assignment for reserved user 0")
	}
	if !hasAssignment(azRoles[azRolePRA].template, cfg.azGroupID(azgTierZeroGroup), "/") {
		t.Fatal("missing: Privileged Role Administrator assignment for the tier-zero group")
	}
	if !hasAssignment(azRoles[azRoleIntune].template, cfg.azUserID(azuIntuneAdmin), "/") {
		t.Fatal("missing: Intune Administrator assignment (AZExecuteCommand precondition)")
	}
	if !hasAssignment(azRoles[azRoleAppAdmin].template, cfg.azUserID(azuScopedAppAdmin), "/"+cfg.azAppClientID(1)) {
		t.Fatal("missing: app-scoped Application Administrator (AZAppAdmin precondition)")
	}

	// Hybrid: the Global Administrator azure user must sync from AD user 0
	// of domain 0 -- the cross-cloud path's whole point.
	gaSynced := false
	for _, u := range az.users {
		if u.ID == cfg.azUserID(azuGlobalAdmin) {
			gaSynced = u.OnPremisesSyncEnabled && u.OnPremisesSecurityIdentifier == cfg.userSID(0, 0)
		}
	}
	if !gaSynced {
		t.Fatal("missing: hybrid sync of AD user (0,0) into the azure Global Administrator")
	}

	// Tier-zero group: role-assignable, contains reserved user 3.
	foundTZMember := false
	for _, m := range az.members {
		if m.GroupID != cfg.azGroupID(azgTierZeroGroup) {
			continue
		}
		for _, e := range m.Members {
			if e.Member.ID == cfg.azUserID(azuGroupMember) {
				foundTZMember = true
			}
		}
	}
	if !foundTZMember {
		t.Fatal("missing: reserved user 3's membership in the tier-zero group")
	}
	for _, g := range az.groups {
		if g.ID == cfg.azGroupID(azgTierZeroGroup) && !g.IsAssignableToRole {
			t.Fatal("tier-zero group must be role-assignable (single-hop role inheritance)")
		}
		if g.ID != cfg.azGroupID(azgTierZeroGroup) && g.IsAssignableToRole {
			t.Fatalf("group %s unexpectedly role-assignable", g.ID)
		}
	}

	// App-owner escalation: user 2 owns app 0, and app 0's SP holds
	// RoleManagement.ReadWrite.Directory on the tenant's MS Graph SP.
	ownsApp0 := false
	for _, ao := range az.appOwners {
		if ao.AppID == cfg.azAppClientID(0) {
			for _, o := range ao.Owners {
				if o.Owner.ID == cfg.azUserID(azuAppOwner) {
					ownsApp0 = true
				}
			}
		}
	}
	if !ownsApp0 {
		t.Fatal("missing: reserved user 2's ownership of app 0")
	}
	graphGrant := false
	for _, ar := range az.appRoles {
		if ar.PrincipalID == cfg.azSPID(0) && ar.ResourceID == cfg.azGraphSPID() && ar.AppRoleID == appRoleRoleManagementRWDir {
			graphGrant = true
		}
	}
	if !graphGrant {
		t.Fatal("missing: SP 0's RoleManagement.ReadWrite.Directory grant on the MS Graph SP")
	}

	// Key vault, VM, PIM, approver.
	vaultRead := false
	for _, p := range az.policies {
		if p.KeyVaultID == cfg.azVaultPath(0) && p.ObjectID == cfg.azUserID(azuVaultReader) {
			for _, perm := range [][]string{p.Permissions.Keys, p.Permissions.Secrets, p.Permissions.Certificates} {
				found := false
				for _, v := range perm {
					if v == "Get" {
						found = true
					}
				}
				if !found {
					t.Fatalf("vault-0 policy for reserved user 4 lacks a literal \"Get\" in %v", perm)
				}
			}
			vaultRead = true
		}
	}
	if !vaultRead {
		t.Fatal("missing: reserved user 4's Get policy on vault 0")
	}
	vmAdmin := false
	for _, va := range az.vmAdmins {
		if va.VirtualMachineID == cfg.azVMPath(0) {
			for _, e := range va.AdminLogins {
				if e.AdminLogin.Properties.PrincipalID == cfg.azUserID(azuVMAdmin) {
					vmAdmin = true
				}
			}
		}
	}
	if !vmAdmin {
		t.Fatal("missing: reserved user 5's AZVMAdminLogin on VM 0")
	}
	pim := false
	for _, el := range az.eligibility {
		if el.PrincipalID == cfg.azUserID(azuPIMEligible) && el.RoleDefinitionID == azRoles[azRolePRA].template && el.DirectoryScopeID == "/" {
			pim = true
		}
	}
	if !pim {
		t.Fatal("missing: reserved user 1's PIM eligibility for Privileged Role Administrator")
	}
	approver := false
	for _, r := range az.rmpas {
		if r.RoleDefinitionID == azRoles[azRoleGA].template && r.EndUserAssignmentRequiresApproval {
			for _, a := range r.EndUserAssignmentUserApprovers {
				if a == cfg.azUserID(azuRoleApprover) {
					approver = true
				}
			}
			if r.EndUserAssignmentUserApprovers == nil || r.EndUserAssignmentGroupApprovers == nil {
				t.Fatal("GA policy assignment must carry PRESENT approver arrays (absent ones are skipped by analysis)")
			}
		}
	}
	if !approver {
		t.Fatal("missing: reserved user 8 as Global Administrator role approver")
	}

	// Devices must all run something matching "windows" case-insensitively:
	// a tenant with zero Windows devices aborts azure post-processing for
	// every following tenant in BloodHound v9.7.0.
	if len(az.devices) == 0 {
		t.Fatal("no devices emitted; azure post-processing requires at least one")
	}
	for _, d := range az.devices {
		if !strings.Contains(strings.ToLower(d.OperatingSystem), "windows") {
			t.Fatalf("device %s runs %q; every seeded device must be Windows-family", d.ID, d.OperatingSystem)
		}
	}
}

// TestAzureDisabledLeavesADOutputIdentical pins backwards compatibility:
// with the azure side off, no azure file appears and the AD files are
// byte-identical to what the flag-less generator produced.
func TestAzureDisabledLeavesADOutputIdentical(t *testing.T) {
	base := testConfig()
	dirA, dirB := t.TempDir(), t.TempDir()
	if _, err := generate(base, dirA, 50000); err != nil {
		t.Fatalf("generate A: %v", err)
	}
	withAzureOff := base
	withAzureOff.AZUsers = 0
	withAzureOff.applyAzureDefaults()
	if _, err := generate(withAzureOff, dirB, 50000); err != nil {
		t.Fatalf("generate B: %v", err)
	}
	entriesA, _ := os.ReadDir(dirA)
	entriesB, _ := os.ReadDir(dirB)
	if len(entriesA) != len(entriesB) {
		t.Fatalf("file count changed: %d vs %d", len(entriesA), len(entriesB))
	}
	for _, e := range entriesA {
		if strings.Contains(e.Name(), "_azure_") {
			t.Fatalf("azure file %s emitted with the azure side disabled", e.Name())
		}
		a, _ := os.ReadFile(filepath.Join(dirA, e.Name()))
		b, err := os.ReadFile(filepath.Join(dirB, e.Name()))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("AD file %s differs with the azure side disabled", e.Name())
		}
	}
}

// TestAzureDeterministicOutput: same seed and counts, byte-identical azure
// stream -- the same guarantee the AD side pins.
func TestAzureDeterministicOutput(t *testing.T) {
	cfg := azTestConfig()
	dirA, dirB := t.TempDir(), t.TempDir()
	for _, dir := range []string{dirA, dirB} {
		if _, err := generate(cfg, dir, 50000); err != nil {
			t.Fatalf("generate: %v", err)
		}
	}
	entries, _ := os.ReadDir(dirA)
	compared := 0
	for _, e := range entries {
		if !strings.Contains(e.Name(), "_azure_") {
			continue
		}
		a, _ := os.ReadFile(filepath.Join(dirA, e.Name()))
		b, err := os.ReadFile(filepath.Join(dirB, e.Name()))
		if err != nil || !bytes.Equal(a, b) {
			t.Fatalf("azure file %s differs between identical runs", e.Name())
		}
		compared++
	}
	if compared == 0 {
		t.Fatal("no azure files compared")
	}
}
