// SPDX-License-Identifier: Apache-2.0

// model.go defines the SharpHound v6 collection-file shapes shgen emits,
// mirrored field for field from internal/verify/fixture (a collection this
// repository already proves BloodHound CE ingests): every file is
// `{"data":[...],"meta":{...}}` with meta carrying the collection methods
// bitmask, the object type, the object count and the format version, and
// every cross-object reference travels as a SID/GUID string that must
// resolve to another emitted object for BloodHound's ingest to create the
// corresponding edge (see generate_test.go's referential-integrity pin).
package main

// metaMethods is the SharpHound collection-methods bitmask the fixture
// carries; BloodHound reads it as "which collectors ran" and gates some
// post-processing on it, so shgen reuses the fixture's proven value.
const metaMethods = 46067

// metaVersion is the SharpHound JSON format version this generator emits.
const metaVersion = 6

// Meta is the per-file trailer.
type Meta struct {
	Methods int    `json:"methods"`
	Type    string `json:"type"`
	Count   int    `json:"count"`
	Version int    `json:"version"`
}

// TypedPrincipal references another object by identifier and kind -- the
// shape group members, delegation targets and GPO-changes lists use.
type TypedPrincipal struct {
	ObjectIdentifier string `json:"ObjectIdentifier"`
	ObjectType       string `json:"ObjectType"`
}

// ACE is one entry of an object's access-control list; BloodHound turns
// each into an edge from PrincipalSID to the owning object.
type ACE struct {
	PrincipalSID  string `json:"PrincipalSID"`
	PrincipalType string `json:"PrincipalType"`
	RightName     string `json:"RightName"`
	IsInherited   bool   `json:"IsInherited"`
}

// Session is one logged-on-user observation on a computer.
type Session struct {
	UserSID     string `json:"UserSID"`
	ComputerSID string `json:"ComputerSID"`
}

// SessionAPIResult wraps a session list the way SharpHound records
// collection success per computer.
type SessionAPIResult struct {
	Results       []Session `json:"Results"`
	Collected     bool      `json:"Collected"`
	FailureReason *string   `json:"FailureReason"`
}

// LocalGroupAPIResult is one local group on a computer; an entry named
// ADMINISTRATORS@<host> whose Results list domain principals is what
// BloodHound turns into AdminTo edges.
type LocalGroupAPIResult struct {
	Collected        bool             `json:"Collected"`
	FailureReason    string           `json:"FailureReason"`
	Results          []TypedPrincipal `json:"Results"`
	LocalName        []string         `json:"LocalName"`
	Name             string           `json:"Name"`
	ObjectIdentifier string           `json:"ObjectIdentifier"`
}

// User is one users-file entry.
type User struct {
	Properties        map[string]any   `json:"Properties"`
	AllowedToDelegate []TypedPrincipal `json:"AllowedToDelegate"`
	PrimaryGroupSID   string           `json:"PrimaryGroupSID"`
	HasSIDHistory     []TypedPrincipal `json:"HasSIDHistory"`
	SpnTargets        []TypedPrincipal `json:"SpnTargets"`
	Aces              []ACE            `json:"Aces"`
	ObjectIdentifier  string           `json:"ObjectIdentifier"`
	IsDeleted         bool             `json:"IsDeleted"`
	IsACLProtected    bool             `json:"IsACLProtected"`
}

// Group is one groups-file entry.
type Group struct {
	Properties       map[string]any   `json:"Properties"`
	Members          []TypedPrincipal `json:"Members"`
	Aces             []ACE            `json:"Aces"`
	ObjectIdentifier string           `json:"ObjectIdentifier"`
	IsDeleted        bool             `json:"IsDeleted"`
	IsACLProtected   bool             `json:"IsACLProtected"`
}

// Computer is one computers-file entry.
type Computer struct {
	PrimaryGroupSID    string                `json:"PrimaryGroupSID"`
	AllowedToDelegate  []TypedPrincipal      `json:"AllowedToDelegate"`
	AllowedToAct       []TypedPrincipal      `json:"AllowedToAct"`
	HasSIDHistory      []TypedPrincipal      `json:"HasSIDHistory"`
	Sessions           SessionAPIResult      `json:"Sessions"`
	PrivilegedSessions SessionAPIResult      `json:"PrivilegedSessions"`
	RegistrySessions   SessionAPIResult      `json:"RegistrySessions"`
	LocalGroups        []LocalGroupAPIResult `json:"LocalGroups"`
	Aces               []ACE                 `json:"Aces"`
	ObjectIdentifier   string                `json:"ObjectIdentifier"`
	IsDeleted          bool                  `json:"IsDeleted"`
	IsACLProtected     bool                  `json:"IsACLProtected"`
	Properties         map[string]any        `json:"Properties"`
	ContainedBy        *TypedPrincipal       `json:"ContainedBy,omitempty"`
}

// Trust is one inter-domain trust as recorded on a domains-file entry.
type Trust struct {
	TargetDomainSid      string `json:"TargetDomainSid"`
	TargetDomainName     string `json:"TargetDomainName"`
	IsTransitive         bool   `json:"IsTransitive"`
	SidFilteringEnabled  bool   `json:"SidFilteringEnabled"`
	TGTDelegationEnabled bool   `json:"TGTDelegationEnabled"`
	TrustDirection       string `json:"TrustDirection"`
	TrustType            string `json:"TrustType"`
}

// GPLink links a GPO to the domain or OU carrying it.
type GPLink struct {
	IsEnforced bool   `json:"IsEnforced"`
	GUID       string `json:"GUID"`
}

// Domain is one domains-file entry.
type Domain struct {
	Properties       map[string]any `json:"Properties"`
	Trusts           []Trust        `json:"Trusts"`
	Links            []GPLink       `json:"Links"`
	Aces             []ACE          `json:"Aces"`
	ObjectIdentifier string         `json:"ObjectIdentifier"`
	IsDeleted        bool           `json:"IsDeleted"`
	IsACLProtected   bool           `json:"IsACLProtected"`
}

// GPOChanges carries an OU's GPO-derived local-group/computer effects;
// shgen emits it with empty (non-nil) lists, matching an OU whose linked
// GPOs push no principals.
type GPOChanges struct {
	LocalAdmins        []TypedPrincipal `json:"LocalAdmins"`
	RemoteDesktopUsers []TypedPrincipal `json:"RemoteDesktopUsers"`
	DcomUsers          []TypedPrincipal `json:"DcomUsers"`
	PSRemoteUsers      []TypedPrincipal `json:"PSRemoteUsers"`
	AffectedComputers  []TypedPrincipal `json:"AffectedComputers"`
}

// OU is one ous-file entry.
type OU struct {
	GPOChanges       GPOChanges     `json:"GPOChanges"`
	Properties       map[string]any `json:"Properties"`
	Links            []GPLink       `json:"Links"`
	Aces             []ACE          `json:"Aces"`
	ObjectIdentifier string         `json:"ObjectIdentifier"`
	IsDeleted        bool           `json:"IsDeleted"`
	IsACLProtected   bool           `json:"IsACLProtected"`
}

// Container is one containers-file entry.
type Container struct {
	Properties       map[string]any `json:"Properties"`
	Aces             []ACE          `json:"Aces"`
	ObjectIdentifier string         `json:"ObjectIdentifier"`
	IsDeleted        bool           `json:"IsDeleted"`
	IsACLProtected   bool           `json:"IsACLProtected"`
}

// GPO is one gpos-file entry.
type GPO struct {
	Properties       map[string]any `json:"Properties"`
	Aces             []ACE          `json:"Aces"`
	ObjectIdentifier string         `json:"ObjectIdentifier"`
	IsDeleted        bool           `json:"IsDeleted"`
	IsACLProtected   bool           `json:"IsACLProtected"`
}
