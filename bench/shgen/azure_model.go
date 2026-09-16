// SPDX-License-Identifier: Apache-2.0

// azure_model.go defines the AzureHound collection-item shapes shgen emits.
// Every struct mirrors, field for field and JSON tag for JSON tag, exactly
// the subset of the AzureHound v2.12.1 model that BloodHound CE v9.7.0's
// azure convertors (services/graphify/azure_convertors.go -> ein/azure.go)
// actually consume -- traced from source, not guessed; the trace lives in
// the internal notes. Every azure file is the same `{"data":[...],"meta":
// {...}}` envelope as the AD side with `meta.type: "azure"`, and each data
// item is `{"kind": "<AZKind>", "data": {...}}`.
package main

// AZItem is one azure data-array entry: the kind string BloodHound
// dispatches its convertor on, and the kind-specific payload.
type AZItem struct {
	Kind string `json:"kind"`
	Data any    `json:"data"`
}

// AZDirectoryObject is the member/owner reference shape; BloodHound
// discriminates the principal's kind on the `@odata.type` value.
type AZDirectoryObject struct {
	ID    string `json:"id"`
	OType string `json:"@odata.type"`
}

const (
	odataUser             = "#microsoft.graph.user"
	odataGroup            = "#microsoft.graph.group"
	odataServicePrincipal = "#microsoft.graph.servicePrincipal"
)

type AZTenantData struct {
	TenantID    string `json:"tenantId"`
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Collected   bool   `json:"collected"`
}

type AZUserData struct {
	ID                string `json:"id"`
	UserPrincipalName string `json:"userPrincipalName"`
	DisplayName       string `json:"displayName"`
	AccountEnabled    bool   `json:"accountEnabled"`
	JobTitle          string `json:"jobTitle,omitempty"`
	// OnPremisesSecurityIdentifier + OnPremisesSyncEnabled are what
	// hybrid post-processing keys the SyncedToEntraUser/SyncedToADUser
	// pair on: the SID must equal an AD User's objectid exactly
	// (case-sensitive), and the sync flag must be a true BOOL.
	OnPremisesSecurityIdentifier string `json:"onPremisesSecurityIdentifier,omitempty"`
	OnPremisesSyncEnabled        bool   `json:"onPremisesSyncEnabled,omitempty"`
	UserType                     string `json:"userType,omitempty"`
	TenantID                     string `json:"tenantId"`
	TenantName                   string `json:"tenantName"`
}

type AZGroupData struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Description string `json:"description,omitempty"`
	// Always emitted explicitly (no omitempty): azure post-processing
	// distinguishes bool true (single-hop role inheritance through the
	// group), bool false (AZMG*/AddMembers extended-role target), and
	// ABSENT (neither) -- a string would silently disable both behaviors.
	IsAssignableToRole bool   `json:"isAssignableToRole"`
	SecurityEnabled    bool   `json:"securityEnabled"`
	TenantID           string `json:"tenantId"`
	TenantName         string `json:"tenantName"`
}

type AZGroupMemberEntry struct {
	Member  AZDirectoryObject `json:"member"`
	GroupID string            `json:"groupId"`
}

type AZGroupMembersData struct {
	Members []AZGroupMemberEntry `json:"members"`
	GroupID string               `json:"groupId"`
}

type AZGroupOwnerEntry struct {
	Owner   AZDirectoryObject `json:"owner"`
	GroupID string            `json:"groupId"`
}

type AZGroupOwnersData struct {
	Owners  []AZGroupOwnerEntry `json:"owners"`
	GroupID string              `json:"groupId"`
}

type AZRoleData struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	TemplateID  string `json:"templateId"`
	IsBuiltIn   bool   `json:"isBuiltIn"`
	IsEnabled   bool   `json:"isEnabled"`
	TenantID    string `json:"tenantId"`
	TenantName  string `json:"tenantName"`
}

type AZRoleAssignmentEntry struct {
	ID               string `json:"id"`
	RoleDefinitionID string `json:"roleDefinitionId"`
	PrincipalID      string `json:"principalId"`
	// Required, must begin with "/": "/" scopes to the tenant (AZHasRole);
	// "/<objectid>" scopes to that object, which for the App/CloudApp
	// Administrator roles becomes an AZAppAdmin/AZCloudAppAdmin edge.
	// BloodHound's convertor slices [1:] unconditionally on the non-"/"
	// path, so an empty value would panic the ingest worker.
	DirectoryScopeID string `json:"directoryScopeId"`
}

type AZRoleAssignmentsData struct {
	RoleAssignments  []AZRoleAssignmentEntry `json:"roleAssignments"`
	RoleDefinitionID string                  `json:"roleDefinitionId"`
	TenantID         string                  `json:"tenantId"`
}

type AZAppData struct {
	AppID           string `json:"appId"`
	DisplayName     string `json:"displayName"`
	PublisherDomain string `json:"publisherDomain"`
	SignInAudience  string `json:"signInAudience"`
	TenantID        string `json:"tenantId"`
	TenantName      string `json:"tenantName"`
}

type AZAppOwnerEntry struct {
	Owner AZDirectoryObject `json:"owner"`
	AppID string            `json:"appId"`
}

type AZAppOwnersData struct {
	Owners []AZAppOwnerEntry `json:"owners"`
	AppID  string            `json:"appId"`
}

type AZServicePrincipalData struct {
	ID                   string `json:"id"`
	AppID                string `json:"appId"`
	DisplayName          string `json:"displayName"`
	AppDisplayName       string `json:"appDisplayName"`
	AccountEnabled       bool   `json:"accountEnabled"`
	AppOwnerOrgID        string `json:"appOwnerOrganizationId"`
	ServicePrincipalType string `json:"servicePrincipalType"`
	TenantID             string `json:"tenantId"`
	TenantName           string `json:"tenantName"`
}

type AZAppRoleAssignmentData struct {
	// AppID must be the Microsoft Graph universal app id and PrincipalType
	// exactly "ServicePrincipal", or BloodHound skips the record entirely.
	// AppRoleID/PrincipalID unmarshal as UUIDs upstream: a malformed GUID
	// drops the whole item.
	AppID                string `json:"appId"`
	TenantID             string `json:"tenantId"`
	PrincipalType        string `json:"principalType"`
	AppRoleID            string `json:"appRoleId"`
	PrincipalID          string `json:"principalId"`
	ResourceID           string `json:"resourceId"`
	PrincipalDisplayName string `json:"principalDisplayName"`
	ResourceDisplayName  string `json:"resourceDisplayName"`
}

type AZDeviceData struct {
	ID                     string `json:"id"`
	DisplayName            string `json:"displayName"`
	DeviceID               string `json:"deviceId"`
	OperatingSystem        string `json:"operatingSystem"`
	OperatingSystemVersion string `json:"operatingSystemVersion"`
	TrustType              string `json:"trustType"`
	TenantID               string `json:"tenantId"`
	TenantName             string `json:"tenantName"`
}

type AZSubscriptionData struct {
	ID             string `json:"id"`
	SubscriptionID string `json:"subscriptionId"`
	DisplayName    string `json:"displayName"`
	TenantID       string `json:"tenantId"`
}

type AZResourceGroupData struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	SubscriptionID string `json:"subscriptionId"`
	TenantID       string `json:"tenantId"`
}

type AZVMProperties struct {
	VMID           string           `json:"vmId"`
	StorageProfile AZStorageProfile `json:"storageProfile"`
}

type AZStorageProfile struct {
	OSDisk AZOSDisk `json:"osDisk"`
}

type AZOSDisk struct {
	OSType string `json:"osType"`
}

type AZVMData struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Properties      AZVMProperties `json:"properties"`
	ResourceGroupID string         `json:"resourceGroupId"`
	SubscriptionID  string         `json:"subscriptionId"`
	TenantID        string         `json:"tenantId"`
}

type AZVMAdminLoginEntry struct {
	AdminLogin AZRoleAssignmentARM `json:"adminLogin"`
}

type AZRoleAssignmentARM struct {
	Properties AZARMAssignmentProps `json:"properties"`
}

type AZARMAssignmentProps struct {
	PrincipalID string `json:"principalId"`
	Scope       string `json:"scope"`
}

type AZVMAdminLoginsData struct {
	AdminLogins      []AZVMAdminLoginEntry `json:"adminLogins"`
	VirtualMachineID string                `json:"virtualMachineId"`
}

type AZKeyVaultProperties struct {
	EnableRbacAuthorization bool `json:"enableRbacAuthorization"`
}

type AZKeyVaultData struct {
	ID             string               `json:"id"`
	Name           string               `json:"name"`
	Properties     AZKeyVaultProperties `json:"properties"`
	ResourceGroup  string               `json:"resourceGroup"`
	SubscriptionID string               `json:"subscriptionId"`
	TenantID       string               `json:"tenantId"`
}

type AZKeyVaultPermissions struct {
	// Only the exact string "Get" arms the AZGetKeys/AZGetSecrets/
	// AZGetCertificates edges; other verbs are stored nowhere.
	Keys         []string `json:"keys"`
	Secrets      []string `json:"secrets"`
	Certificates []string `json:"certificates"`
}

type AZKeyVaultAccessPolicyData struct {
	KeyVaultID  string                `json:"keyVaultId"`
	ObjectID    string                `json:"objectId"`
	Permissions AZKeyVaultPermissions `json:"permissions"`
}

type AZRoleEligibilityData struct {
	ID               string `json:"id"`
	RoleDefinitionID string `json:"roleDefinitionId"`
	PrincipalID      string `json:"principalId"`
	// Only "/" produces an AZRoleEligible edge; any other scope is skipped.
	DirectoryScopeID string `json:"directoryScopeId"`
	TenantID         string `json:"tenantId"`
}

type AZRoleManagementPolicyAssignmentData struct {
	ID                                string `json:"id"`
	RoleDefinitionID                  string `json:"roleDefinitionId"`
	TenantID                          string `json:"tenantId"`
	EndUserAssignmentRequiresApproval bool   `json:"endUserAssignmentRequiresApproval"`
	// Both approver lists are emitted even when empty: analysis skips a
	// role whose approver properties are ABSENT, while present-but-empty
	// falls back to the tenant's Privileged Role Administrator / Global
	// Administrator role nodes as default approvers.
	EndUserAssignmentUserApprovers  []string `json:"endUserAssignmentUserApprovers"`
	EndUserAssignmentGroupApprovers []string `json:"endUserAssignmentGroupApprovers"`
	EndUserAssignmentRequiresMFA    bool     `json:"endUserAssignmentRequiresMFA"`
}

// msGraphAppID is the Microsoft Graph universal application id -- the ingest
// gate for AZAppRoleAssignment records, and the appId of the tenant's own
// "Microsoft Graph" service principal.
const msGraphAppID = "00000003-0000-0000-c000-000000000000"

// MS Graph app-role ids that BloodHound maps to AZMG* edges.
const (
	appRoleRoleManagementRWDir = "9e3f62cf-ca93-4989-b6ce-bf83c28f9fe8"
	appRoleApplicationRWAll    = "1bfefb4e-e0b5-418b-a88f-73c46d2cc8e9"
	appRoleGroupRWAll          = "62a82d76-70ea-41e2-9197-370581804d09"
)

// azRole is one built-in Entra directory role this generator seeds; the
// template ids are the real, well-known AAD role-template GUIDs BloodHound's
// post-processing matches on (lowercase, exact).
type azRole struct {
	template string
	name     string
}

var azRoles = []azRole{
	{"62e90394-69f5-4237-9190-012177145e10", "Global Administrator"},
	{"e8611ab8-c189-46e8-94e1-60213ab1f814", "Privileged Role Administrator"},
	{"7be44c8a-adaf-4e2a-84d6-ab2649e08a13", "Privileged Authentication Administrator"},
	{"e00e864a-17c5-4a4b-9c06-f5b95a8d5bd8", "Partner Tier2 Support"},
	{"4ba39ca4-527c-499a-b93d-d9b492c50246", "Partner Tier1 Support"},
	{"729827e3-9c14-49f7-bb1b-9608f156bbb8", "Helpdesk Administrator"},
	{"c4e39bd9-1100-46d3-8c65-fb160da0071f", "Authentication Administrator"},
	{"fe930be7-5e62-47db-91af-98c3a49a38b1", "User Administrator"},
	{"966707d0-3269-4727-9be2-8c3a10f19b9d", "Password Administrator"},
	{"9b895d92-2cd3-44c7-9d02-a6ac2d5ea5c3", "Application Administrator"},
	{"158c047a-c907-4556-b7ef-446551a6b5f7", "Cloud Application Administrator"},
	{"8ac3fc64-6eca-42ea-9e69-59f4c7b60eb2", "Hybrid Identity Administrator"},
	{"fdd7a751-b60b-444a-984c-02652fe8fa1c", "Groups Administrator"},
	{"9360feb5-f418-4baa-8175-e2a00bac4301", "Directory Writers"},
	{"3a2c62db-5318-420d-8d74-23affee5d9d5", "Intune Administrator"},
	{"b5a8dcf3-09d5-43a9-a639-8e29ef291470", "Knowledge Administrator"},
	{"744ec460-397e-42ad-a462-8b3f9747a02c", "Knowledge Manager"},
	{"194ae4cb-b126-40b2-bd5b-6091b380977d", "Security Administrator"},
	{"88d8e3e3-8f55-4a1e-953a-9b9898b8876b", "Directory Readers"},
	{"95e79109-95c0-4d8e-aee3-d01accf2d47b", "Guest Inviter"},
}

// Indices into azRoles for the seeded assignments.
const (
	azRoleGA = iota
	azRolePRA
	azRolePAA
	_ // Partner Tier2
	_ // Partner Tier1
	azRoleHelpdesk
	_ // Auth Admin
	azRoleUserAdmin
	_ // Password Admin
	azRoleAppAdmin
	_ // Cloud App Admin
	_ // Hybrid Identity
	_ // Groups Admin
	_ // Directory Writers
	azRoleIntune
)
