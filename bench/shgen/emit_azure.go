// SPDX-License-Identifier: Apache-2.0

// emit_azure.go generates the Entra/Azure half of a hybrid collection: one
// tenant, its directory (users, groups, roles, apps, service principals,
// devices) and an Azure resource tree (subscriptions, resource groups, VMs,
// key vaults), as AzureHound-format items BloodHound CE ingests through the
// same file-upload pipeline as the AD files. Reserved indices 0-10 of the
// AZUser range (and index 0 of groups/apps) carry a fixed set of seeded
// attack paths -- hybrid sync into a Global Administrator, PIM eligibility,
// app-owner -> MS-Graph-role escalation, key-vault and VM access, Intune,
// scoped app admin, and a role approver -- each pinned by azure_test.go.
//
// Everything here follows generate.go's index-determinism rule: ids are pure
// functions of (seed, kind tag, index), so any item can reference any other
// without shared state, and referential integrity is provable by re-deriving.
package main

import (
	"fmt"
	"strings"
)

func (c config) azTenantName() string {
	root := strings.ToLower(c.Domain)
	if i := strings.IndexByte(root, '.'); i > 0 {
		root = root[:i]
	}
	return root + ".onmicrosoft.com"
}

func (c config) azSubPath(i int) string {
	return "/subscriptions/" + c.azSubID(i)
}

func (c config) azRGCount() int { return c.AZSubs * 3 }

func (c config) azRGPath(j int) string {
	return fmt.Sprintf("%s/resourceGroups/RG%03d", c.azSubPath(j%c.AZSubs), j)
}

func (c config) azVMPath(k int) string {
	return fmt.Sprintf("%s/providers/Microsoft.Compute/virtualMachines/VM%05d", c.azRGPath(k%c.azRGCount()), k)
}

func (c config) azVaultPath(k int) string {
	return fmt.Sprintf("%s/providers/Microsoft.KeyVault/vaults/VAULT%04d", c.azRGPath(k%c.azRGCount()), k)
}

// azSyncTarget maps azure user i to the AD user its on-prem identity points
// at, round-robin across domains; ok is false when i is outside the synced
// range or the mapped AD index does not exist at the current sizing.
func (c config) azSyncTarget(i int) (sid string, ok bool) {
	if i >= c.azSyncedUsers() {
		return "", false
	}
	d := i % c.domainCount()
	ui := i / c.domainCount()
	if ui >= c.usersInDomain(d) {
		return "", false
	}
	return c.userSID(d, ui), true
}

// emitAzure writes the whole azure item stream. Ordering inside the stream
// is irrelevant to BloodHound (each item is independently converted, and
// edges reference objectids, not prior items), so items are grouped by kind
// for readability of the output files.
func (g *generator) emitAzure(sum *summary) error {
	c := g.cfg
	if c.AZUsers == 0 {
		return nil
	}
	w, err := newAzureChunkWriter(g.dir+"/"+sanitizeDomain(c.Domain), g.chunk)
	if err != nil {
		return err
	}
	w.onClose = func(path string) { g.files = append(g.files, path) }
	put := func(kind string, data any) error {
		sum.AZObjects++
		return w.write(AZItem{Kind: kind, Data: data})
	}

	tid := c.azTenantID()
	tname := c.azTenantName()

	// -- tenant ---------------------------------------------------------
	if err := put("AZTenant", AZTenantData{
		TenantID: tid, ID: "/tenants/" + tid, DisplayName: strings.ToUpper(tname[:strings.IndexByte(tname, '.')]), Collected: true,
	}); err != nil {
		return err
	}

	// -- directory roles -------------------------------------------------
	// Every seeded role uses id == templateId, the shape AAD's built-in
	// role definitions take, so an assignment's roleDefinitionId matches
	// the role node's own id half.
	for _, r := range azRoles {
		if err := put("AZRole", AZRoleData{
			ID: r.template, DisplayName: r.name, TemplateID: r.template,
			IsBuiltIn: true, IsEnabled: true, TenantID: tid, TenantName: tname,
		}); err != nil {
			return err
		}
	}

	// -- users ------------------------------------------------------------
	for i := 0; i < c.AZUsers; i++ {
		u := AZUserData{
			ID:                c.azUserID(i),
			UserPrincipalName: fmt.Sprintf("azuser%07d@%s", i, tname),
			DisplayName:       fmt.Sprintf("AZ User %07d", i),
			AccountEnabled:    i < azAttackUserReserve || c.intn(100, 20, int64(i)) >= 2,
			JobTitle:          jobTitle(i),
			UserType:          "Member",
			TenantID:          tid, TenantName: tname,
		}
		if sid, ok := c.azSyncTarget(i); ok {
			u.OnPremisesSecurityIdentifier = sid
			u.OnPremisesSyncEnabled = true
		}
		if err := put("AZUser", u); err != nil {
			return err
		}
	}

	// -- groups, members, owners -----------------------------------------
	for gi := 0; gi < c.AZGroups; gi++ {
		grp := AZGroupData{
			ID:                 c.azGroupID(gi),
			DisplayName:        fmt.Sprintf("AZ-GROUP-%05d", gi),
			IsAssignableToRole: gi == azgTierZeroGroup,
			SecurityEnabled:    true,
			TenantID:           tid, TenantName: tname,
		}
		if gi == azgTierZeroGroup {
			grp.DisplayName = "TIER ZERO ADMINS"
		}
		if err := put("AZGroup", grp); err != nil {
			return err
		}

		var members []AZGroupMemberEntry
		addUser := func(ui int) {
			members = append(members, AZGroupMemberEntry{GroupID: grp.ID,
				Member: AZDirectoryObject{ID: c.azUserID(ui), OType: odataUser}})
		}
		if gi == azgTierZeroGroup {
			addUser(azuGroupMember)
		}
		for m := 0; m < 4; m++ {
			addUser(c.intn(c.AZUsers, 21, int64(gi), int64(m)))
		}
		// Occasional group nesting, always toward a lower index so chains
		// terminate; the tier-zero group is never a member of anything.
		if gi >= 1 && c.intn(100, 22, int64(gi)) < 20 {
			nest := c.intn(gi, 23, int64(gi))
			if nest != azgTierZeroGroup {
				members = append(members, AZGroupMemberEntry{GroupID: grp.ID,
					Member: AZDirectoryObject{ID: c.azGroupID(nest), OType: odataGroup}})
			}
		}
		if err := put("AZGroupMember", AZGroupMembersData{GroupID: grp.ID, Members: members}); err != nil {
			return err
		}

		if gi%7 == 3 {
			owner := c.intn(c.AZUsers, 24, int64(gi))
			if err := put("AZGroupOwner", AZGroupOwnersData{GroupID: grp.ID, Owners: []AZGroupOwnerEntry{
				{GroupID: grp.ID, Owner: AZDirectoryObject{ID: c.azUserID(owner), OType: odataUser}},
			}}); err != nil {
				return err
			}
		}
	}

	// -- directory role assignments --------------------------------------
	// One AZRoleAssignment item per role, carrying that role's principals.
	assign := func(roleIdx int, entries ...AZRoleAssignmentEntry) error {
		if len(entries) == 0 {
			return nil
		}
		return put("AZRoleAssignment", AZRoleAssignmentsData{
			RoleDefinitionID: azRoles[roleIdx].template, TenantID: tid, RoleAssignments: entries,
		})
	}
	entry := func(roleIdx, n int, principal, scope string) AZRoleAssignmentEntry {
		return AZRoleAssignmentEntry{
			ID:               fmt.Sprintf("ra-%s-%d", azRoles[roleIdx].template[:8], n),
			RoleDefinitionID: azRoles[roleIdx].template,
			PrincipalID:      principal,
			DirectoryScopeID: scope,
		}
	}
	if err := assign(azRoleGA, entry(azRoleGA, 0, c.azUserID(azuGlobalAdmin), "/")); err != nil {
		return err
	}
	if err := assign(azRolePRA, entry(azRolePRA, 0, c.azGroupID(azgTierZeroGroup), "/")); err != nil {
		return err
	}
	if err := assign(azRolePAA, entry(azRolePAA, 0, c.azUserID(azuPAAHolder), "/")); err != nil {
		return err
	}
	if err := assign(azRoleIntune, entry(azRoleIntune, 0, c.azUserID(azuIntuneAdmin), "/")); err != nil {
		return err
	}
	// Application Administrator, both shapes: tenant-scoped ("/", becomes
	// AZHasRole and, in analysis, fans AZAddSecret out from the role node)
	// and object-scoped (becomes an ingest-time AZAppAdmin straight onto
	// app 1, when it exists).
	appAdmins := []AZRoleAssignmentEntry{entry(azRoleAppAdmin, 0, c.azUserID(azuUnscopedAppAdmin), "/")}
	if c.AZApps > 1 {
		appAdmins = append(appAdmins, entry(azRoleAppAdmin, 1, c.azUserID(azuScopedAppAdmin), "/"+c.azAppClientID(1)))
	}
	if err := assign(azRoleAppAdmin, appAdmins...); err != nil {
		return err
	}
	// Noise: a few helpdesk/user-admin style holders outside the reserved
	// range, so the reset-password matrix has non-trivial member sets.
	for _, roleIdx := range []int{azRoleHelpdesk, azRoleUserAdmin} {
		var entries []AZRoleAssignmentEntry
		for n := 0; n < 3 && azAttackUserReserve+n < c.AZUsers; n++ {
			ui := azAttackUserReserve + c.intn(c.AZUsers-azAttackUserReserve, 25, int64(roleIdx), int64(n))
			entries = append(entries, entry(roleIdx, n, c.azUserID(ui), "/"))
		}
		if err := assign(roleIdx, entries...); err != nil {
			return err
		}
	}

	// -- apps, owners, service principals, MS Graph -----------------------
	for ai := 0; ai < c.AZApps; ai++ {
		if err := put("AZApp", AZAppData{
			AppID: c.azAppClientID(ai), DisplayName: fmt.Sprintf("APP-%04d", ai),
			PublisherDomain: tname, SignInAudience: "AzureADMyOrg",
			TenantID: tid, TenantName: tname,
		}); err != nil {
			return err
		}
		if err := put("AZServicePrincipal", AZServicePrincipalData{
			ID: c.azSPID(ai), AppID: c.azAppClientID(ai),
			DisplayName: fmt.Sprintf("APP-%04d SP", ai), AppDisplayName: fmt.Sprintf("APP-%04d", ai),
			AccountEnabled: true, AppOwnerOrgID: tid, ServicePrincipalType: "Application",
			TenantID: tid, TenantName: tname,
		}); err != nil {
			return err
		}
	}
	if err := put("AZAppOwner", AZAppOwnersData{AppID: c.azAppClientID(0), Owners: []AZAppOwnerEntry{
		{AppID: c.azAppClientID(0), Owner: AZDirectoryObject{ID: c.azUserID(azuAppOwner), OType: odataUser}},
	}}); err != nil {
		return err
	}
	// The tenant's Microsoft Graph service principal: the resource every
	// AZMG* grant points at. Its objectid is tenant-local (post-processing
	// only requires the node be tenant-contained); its appId is the
	// universal MS Graph application id.
	if err := put("AZServicePrincipal", AZServicePrincipalData{
		ID: c.azGraphSPID(), AppID: msGraphAppID,
		DisplayName: "Microsoft Graph", AppDisplayName: "Microsoft Graph",
		AccountEnabled: true, AppOwnerOrgID: tid, ServicePrincipalType: "Application",
		TenantID: tid, TenantName: tname,
	}); err != nil {
		return err
	}
	grants := []struct {
		sp      int
		appRole string
	}{{0, appRoleRoleManagementRWDir}}
	if c.AZApps > 1 {
		grants = append(grants, struct {
			sp      int
			appRole string
		}{1, appRoleApplicationRWAll})
	}
	if c.AZApps > 2 {
		grants = append(grants, struct {
			sp      int
			appRole string
		}{2, appRoleGroupRWAll})
	}
	for _, gr := range grants {
		if err := put("AZAppRoleAssignment", AZAppRoleAssignmentData{
			AppID: msGraphAppID, TenantID: tid, PrincipalType: "ServicePrincipal",
			AppRoleID: gr.appRole, PrincipalID: c.azSPID(gr.sp), ResourceID: c.azGraphSPID(),
			PrincipalDisplayName: fmt.Sprintf("APP-%04d SP", gr.sp), ResourceDisplayName: "Microsoft Graph",
		}); err != nil {
			return err
		}
	}

	// -- devices ----------------------------------------------------------
	for di := 0; di < c.AZDevices; di++ {
		osName, osVer := "Windows", "10.0.19045"
		if c.intn(100, 26, int64(di)) < 8 {
			osName, osVer = "Windows 7", "6.1.7601" // unsupported-OS hygiene hits
		}
		if err := put("AZDevice", AZDeviceData{
			ID: c.azDeviceID(di), DisplayName: fmt.Sprintf("AZDEV%06d", di),
			DeviceID: c.guid(azgDevice, 1, di), OperatingSystem: osName, OperatingSystemVersion: osVer,
			TrustType: "AzureAd", TenantID: tid, TenantName: tname,
		}); err != nil {
			return err
		}
	}

	// -- azure resources ---------------------------------------------------
	for si := 0; si < c.AZSubs; si++ {
		if err := put("AZSubscription", AZSubscriptionData{
			ID: c.azSubPath(si), SubscriptionID: c.azSubID(si),
			DisplayName: fmt.Sprintf("SUB-%02d", si), TenantID: tid,
		}); err != nil {
			return err
		}
	}
	for ri := 0; ri < c.azRGCount(); ri++ {
		if err := put("AZResourceGroup", AZResourceGroupData{
			ID: c.azRGPath(ri), Name: fmt.Sprintf("RG%03d", ri),
			SubscriptionID: c.azSubPath(ri % c.AZSubs), TenantID: tid,
		}); err != nil {
			return err
		}
	}
	for vi := 0; vi < c.AZVMs; vi++ {
		if err := put("AZVM", AZVMData{
			ID: c.azVMPath(vi), Name: fmt.Sprintf("VM%05d", vi),
			Properties:      AZVMProperties{VMID: c.azVMID(vi), StorageProfile: AZStorageProfile{OSDisk: AZOSDisk{OSType: "Windows"}}},
			ResourceGroupID: c.azRGPath(vi % c.azRGCount()), SubscriptionID: c.azSubPath(vi % c.AZSubs), TenantID: tid,
		}); err != nil {
			return err
		}
	}
	if c.AZVMs > 0 {
		if err := put("AZVMAdminLogin", AZVMAdminLoginsData{
			VirtualMachineID: c.azVMPath(0),
			AdminLogins: []AZVMAdminLoginEntry{{AdminLogin: AZRoleAssignmentARM{
				Properties: AZARMAssignmentProps{PrincipalID: c.azUserID(azuVMAdmin), Scope: c.azVMPath(0)},
			}}},
		}); err != nil {
			return err
		}
	}
	for ki := 0; ki < c.AZKeyVault; ki++ {
		if err := put("AZKeyVault", AZKeyVaultData{
			ID: c.azVaultPath(ki), Name: fmt.Sprintf("VAULT%04d", ki),
			Properties:    AZKeyVaultProperties{EnableRbacAuthorization: false},
			ResourceGroup: c.azRGPath(ki % c.azRGCount()), SubscriptionID: c.azSubPath(ki % c.AZSubs), TenantID: tid,
		}); err != nil {
			return err
		}
		policy := AZKeyVaultAccessPolicyData{KeyVaultID: c.azVaultPath(ki)}
		if ki == 0 {
			policy.ObjectID = c.azUserID(azuVaultReader)
			policy.Permissions = AZKeyVaultPermissions{Keys: []string{"Get", "List"}, Secrets: []string{"Get"}, Certificates: []string{"Get"}}
		} else {
			reader := c.intn(c.AZUsers, 27, int64(ki))
			policy.ObjectID = c.azUserID(reader)
			policy.Permissions = AZKeyVaultPermissions{Keys: []string{"List"}, Secrets: []string{"Get"}, Certificates: []string{}}
		}
		if err := put("AZKeyVaultAccessPolicy", policy); err != nil {
			return err
		}
	}

	// -- PIM: eligibility + approval policy --------------------------------
	if err := put("AZRoleEligibilityScheduleInstance", AZRoleEligibilityData{
		ID: "resi-0", RoleDefinitionID: azRoles[azRolePRA].template,
		PrincipalID: c.azUserID(azuPIMEligible), DirectoryScopeID: "/", TenantID: tid,
	}); err != nil {
		return err
	}
	if err := put("AZRoleManagementPolicyAssignment", AZRoleManagementPolicyAssignmentData{
		ID: "rmpa-ga", RoleDefinitionID: azRoles[azRoleGA].template, TenantID: tid,
		EndUserAssignmentRequiresApproval: true,
		EndUserAssignmentUserApprovers:    []string{c.azUserID(azuRoleApprover)},
		EndUserAssignmentGroupApprovers:   []string{},
		EndUserAssignmentRequiresMFA:      true,
	}); err != nil {
		return err
	}
	// Empty-but-present approver lists: analysis falls back to the tenant's
	// GA and PRA role nodes as the default approvers.
	if err := put("AZRoleManagementPolicyAssignment", AZRoleManagementPolicyAssignmentData{
		ID: "rmpa-pra", RoleDefinitionID: azRoles[azRolePRA].template, TenantID: tid,
		EndUserAssignmentRequiresApproval: true,
		EndUserAssignmentUserApprovers:    []string{},
		EndUserAssignmentGroupApprovers:   []string{},
	}); err != nil {
		return err
	}

	return w.close()
}
