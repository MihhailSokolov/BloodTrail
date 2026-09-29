// SPDX-License-Identifier: Apache-2.0

package main

import "encoding/json"

// extensionSchema is the OpenGraph extension (PUT /api/v2/extensions) that
// declares the organization's kinds and marks every edge kind but sc_Owns
// traversable, so pathfinding with only_traversable=true walks them.
func extensionSchema() []byte {
	info := func(text string) map[string]any {
		return map[string]any{"overview": map[string]any{"title": "Overview", "position": 1, "markdown": map[string]any{"content": text}}}
	}
	nodeKind := func(name, display, icon, color, text string) map[string]any {
		return map[string]any{"name": name, "display_name": display, "description": text, "is_display_kind": true, "icon": icon, "color": color, "info": info(text)}
	}
	edgeKind := func(name string, traversable bool, text string) map[string]any {
		return map[string]any{"name": name, "description": text, "is_traversable": traversable, "info": info(text)}
	}
	schema := map[string]any{
		"schema": map[string]any{"name": "SC_Bench_Extension", "display_name": "Source Control (benchmark)", "version": "v1.0.0", "namespace": "sc"},
		"node_kinds": []any{
			nodeKind(kindOrg, "Organization", "building", "#455A64", "A source-control organization"),
			nodeKind(kindUser, "User", "user", "#1976D2", "A user account"),
			nodeKind(kindOwner, "Org Owner", "crown", "#F9A825", "A user who owns the organization"),
			nodeKind(kindTeam, "Team", "users", "#388E3C", "A team of users"),
			nodeKind(kindRepo, "Repository", "book", "#6A1B9A", "A source repository"),
			nodeKind(kindRole, "Repo Role", "key", "#C62828", "A permission level on one repository"),
		},
		"relationship_kinds": []any{
			edgeKind(edgeMemberOf, true, "Principal is a member of a team"),
			edgeKind(edgeHasRole, true, "Principal holds a repository role"),
			edgeKind(edgeCanAdmin, true, "Role administers the repository"),
			edgeKind(edgeCanWrite, true, "Role can push to the repository"),
			edgeKind(edgeCanRead, true, "Role can read the repository"),
			edgeKind(edgeOrgAdmin, true, "User administers the organization"),
			edgeKind(edgeSyncedTo, true, "Directory identity is synced to this account"),
			edgeKind(edgeOwns, false, "Organization owns the repository (not an attack path)"),
		},
		"environments": []any{map[string]any{"environment_kind": kindOrg, "source_kind": sourceKind, "principal_kinds": []string{kindUser, kindTeam}}},
		"relationship_findings": []any{map[string]any{
			"name": "sc_FindRepoAdmins", "display_name": "Principal Can Administer Repository", "relationship_kind": edgeCanAdmin, "environment_kind": kindOrg,
			"remediation": map[string]any{
				"short_description": "A role can administer a repository", "long_description": "A role holds admin on a repository.",
				"short_remediation": "Review repository admins", "long_remediation": "Confirm every admin grant is intended.",
			},
		}},
	}
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		panic(err)
	}
	return data
}
