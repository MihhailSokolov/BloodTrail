// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// The activeDirectoryPathfindingEdges/azurePathfindingEdges kind lists and
// the interpolations map below are ported verbatim from SpecterOps'
// BloodHound CE (packages/javascript/bh-shared-ui/src/graphSchema.ts and
// constants.ts, both Apache-2.0), hence the header above; see testdata/
// prebuilt/NOTICE for the full provenance of everything this tool extracts.
//
// Command extract-prebuilt-queries pulls BloodHound CE's pre-built Cypher
// query corpus out of an upstream checkout and writes it as JSON fixtures
// under testdata/prebuilt/, for the Cypher interpreter's differential
// testing against these exact real-world queries.
//
// Three sources, all read from the checkout root passed as the single
// command-line argument:
//
//   - packages/javascript/bh-shared-ui/src/commonSearchesAGT.ts -- the
//     "Common Searches" list the Explore UI shows for an AGT-model graph.
//     Its CommonSearches export (91 entries, one of which -- "Shortest
//     paths from Owned objects to Tier Zero" -- is fully commented out
//     because many-to-many shortestPath queries are too expensive to run
//     by default) becomes testdata/prebuilt/agt.json. Its UncommonSearches
//     export carries one more entry, a deliberately malformed Cypher
//     fragment BloodHound's own frontend test suite uses to assert the UI
//     surfaces a parse error; it is folded into agt.json too, marked
//     "probe": true, since it belongs to the same corpus and Task 16's
//     differential suite needs a known-bad query to assert parse failure
//     against.
//   - packages/javascript/bh-shared-ui/src/commonSearchesAGI.ts -- the
//     AGI-model equivalent (91 entries, same single disabled many-to-many
//     query), written to testdata/prebuilt/agi.json. It has no
//     UncommonSearches export.
//   - cmd/api/src/database/migration/migrations/00000000000001_init.sql --
//     the cypher-type (type = 2) asset_group_tag_selector_seeds rows the
//     initial migration seeds, written to testdata/prebuilt/selectors.json.
//     There are three separate INSERT statements that seed a type-2 seed
//     row: a 40-row VALUES block and a 1-row VALUES block, each delimited
//     by a "-- START"/"-- END" comment pair, plus a single-row literal
//     INSERT ("Incoming Forest Trust Builders") that carries no such
//     comment markers -- 40 + 1 + 1 = 42 selectors total. See
//     extractSelectors' doc for how each is found.
//
// Both TypeScript files build several of their query strings via `${...}`
// template interpolation, resolved at import time from two functions in
// graphSchema.ts (ActiveDirectoryPathfindingEdges, AzurePathfindingEdges,
// 64 and 42 relationship kinds respectively) and a handful of plain string
// constants (two from constants.ts, one file-local regex). This tool
// doesn't parse graphSchema.ts/constants.ts as TypeScript; it ports every
// value those interpolations resolve to verbatim as Go data (see
// activeDirectoryPathfindingEdges, azurePathfindingEdges and
// interpolations below), pinned to upstream v9.6.0, and fails loudly if a
// query references an interpolation this tool doesn't know about --
// insurance against upstream adding a new one silently changing the
// extracted text out from under us.
//
// Usage (from the repository root, output paths are relative to it):
//
//	go run ./scripts/extract-prebuilt-queries.go <upstream-checkout-root>
//
// Example:
//
//	go run ./scripts/extract-prebuilt-queries.go .build/upstream-v9.6.0
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// backtick lets the regexes below embed a literal backtick without ending
// their enclosing raw string literal.
const backtick = "`"

// activeDirectoryPathfindingEdges is graphSchema.ts'
// ActiveDirectoryPathfindingEdges(), ported verbatim as the relationship
// kinds' *string values* (ActiveDirectoryRelationshipKind's TS enum keys
// mostly match their string values, but not always -- WriteDACL's value is
// 'WriteDacl', the one case in this list where they differ).
var activeDirectoryPathfindingEdges = []string{
	"Owns", "GenericAll", "GenericWrite", "WriteOwner", "WriteDacl", "MemberOf",
	"ForceChangePassword", "AllExtendedRights", "AddMember", "HasSession",
	"GPLink", "AllowedToDelegate", "CoerceToTGT", "AllowedToAct", "AdminTo",
	"CanPSRemote", "CanRDP", "ExecuteDCOM", "HasSIDHistory", "AddSelf", "DCSync",
	"ReadLAPSPassword", "ReadGMSAPassword", "DumpSMSAPassword", "SQLAdmin",
	"AddAllowedToAct", "WriteSPN", "AddKeyCredentialLink", "SyncLAPSPassword",
	"WriteAccountRestrictions", "WriteGPLink", "GoldenCert", "ADCSESC1",
	"ADCSESC3", "ADCSESC4", "ADCSESC6a", "ADCSESC6b", "ADCSESC9a", "ADCSESC9b",
	"ADCSESC10a", "ADCSESC10b", "ADCSESC13", "SyncedToADUser",
	"CoerceAndRelayNTLMToSMB", "CoerceAndRelayNTLMToADCS",
	"WriteOwnerLimitedRights", "OwnsLimitedRights", "ClaimSpecialIdentity",
	"CoerceAndRelayNTLMToLDAP", "CoerceAndRelayNTLMToLDAPS", "ContainsIdentity",
	"PropagatesACEsTo", "GPOAppliesTo", "CanApplyGPO", "HasTrustKeys",
	"WriteAltSecurityIdentities", "WritePublicInformation", "ManageCA",
	"ManageCertificates", "Contains", "DCFor", "SameForestTrust",
	"SpoofSIDHistory", "AbuseTGTDelegation",
}

// azurePathfindingEdges is graphSchema.ts' AzurePathfindingEdges(), ported
// verbatim as the relationship kinds' string values (for AzureRelationshipKind
// every TS enum key's string value already carries the "AZ" prefix or
// otherwise matches, so there is no WriteDACL-style divergence here).
var azurePathfindingEdges = []string{
	"AZAvereContributor", "AZContributor", "AZGetCertificates", "AZGetKeys",
	"AZGetSecrets", "AZHasRole", "AZMemberOf", "AZOwner", "AZRunsAs",
	"AZVMContributor", "AZAutomationContributor", "AZKeyVaultContributor",
	"AZVMAdminLogin", "AZAddMembers", "AZAddSecret", "AZExecuteCommand",
	"AZGlobalAdmin", "AZPrivilegedAuthAdmin", "AZGrant", "AZGrantSelf",
	"AZPrivilegedRoleAdmin", "AZResetPassword", "AZUserAccessAdministrator",
	"AZOwns", "AZCloudAppAdmin", "AZAppAdmin", "AZAddOwner", "AZManagedIdentity",
	"AZAKSContributor", "AZNodeResourceGroup", "AZWebsiteContributor",
	"AZLogicAppContributor", "AZMGAddMember", "AZMGAddOwner", "AZMGAddSecret",
	"AZMGGrantAppRoles", "AZMGGrantRole", "SyncedToEntraUser", "AZRoleEligible",
	"AZRoleApprover", "AZContains", "AZAuthenticatesTo",
}

// interpolations resolves every `${identifier}` this tool has found used in
// commonSearchesAGT.ts/commonSearchesAGI.ts, ported verbatim from their
// definitions: TAG_TIER_ZERO_AGT/TAG_OWNED_AGT/TIER_ZERO_TAG/OWNED_OBJECT_TAG
// from constants.ts, highPrivilegedRoleDisplayNameRegex from a file-local
// const each file defines identically, and the two pathfinding-edge joins
// from the kind lists above (both files compute
// `AzurePathfindingEdges().join('|')` / `ActiveDirectoryPathfindingEdges()
// .join('|')` into a local const before using it).
var interpolations = map[string]string{
	"TAG_TIER_ZERO_AGT": "Tag_Tier_Zero",
	"TAG_OWNED_AGT":     "Tag_Owned",
	"TIER_ZERO_TAG":     "admin_tier_0",
	"OWNED_OBJECT_TAG":  "owned",
	"highPrivilegedRoleDisplayNameRegex": "^(Global Administrator|User Administrator|Cloud Application Administrator|" +
		"Authentication Policy Administrator|Exchange Administrator|Helpdesk Administrator|" +
		"Privileged Authentication Administrator|Privileged Role Administrator).*$",
	"adTransitEdgeTypes":    strings.Join(activeDirectoryPathfindingEdges, "|"),
	"azureTransitEdgeTypes": strings.Join(azurePathfindingEdges, "|"),
}

func init() {
	// Self-check the ported kind lists against graphSchema.ts' own documented
	// counts, so a transcription slip fails immediately rather than silently
	// shipping a wrong edge list.
	if got := len(activeDirectoryPathfindingEdges); got != 64 {
		panic(fmt.Sprintf("activeDirectoryPathfindingEdges: got %d kinds, want 64", got))
	}
	if got := len(azurePathfindingEdges); got != 42 {
		panic(fmt.Sprintf("azurePathfindingEdges: got %d kinds, want 42", got))
	}
}

// commonSearchEntry mirrors one entry from a CommonSearches/UncommonSearches
// array (see the CommonSearchType TS type), plus the "disabled"/"probe"
// markers this tool derives.
type commonSearchEntry struct {
	Subheader string `json:"subheader"`
	Name      string `json:"name"`
	Query     string `json:"query"`
	Disabled  bool   `json:"disabled"`
	Probe     bool   `json:"probe,omitempty"`
}

// selectorEntry mirrors one seeded cypher (type = 2)
// asset_group_tag_selector_seeds row.
type selectorEntry struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

var (
	// subheaderPattern matches a group's `subheader: '...'` field. Group
	// values are always plain text in both source files, but the escape
	// alternative keeps this correct if that ever stops being true.
	subheaderPattern = regexp.MustCompile(`subheader:\s*'((?:\\.|[^'\\])*)'`)

	// entryPattern matches one `{ name: '...', description: '', query:
	// `...`, },` object literal. description is always the empty string in
	// both files (verified against the checkout below), so it is matched
	// literally rather than captured.
	entryPattern = regexp.MustCompile(
		`\{\s*name:\s*'((?:\\.|[^'\\])*)',\s*description:\s*'',\s*query:\s*` +
			backtick + `((?:[^` + backtick + `\\]|\\.)*)` + backtick + `,\s*\},`,
	)

	// interpolationPattern finds `${identifier}` placeholders inside an
	// extracted query string.
	interpolationPattern = regexp.MustCompile(`\$\{([A-Za-z0-9_]+)\}`)

	// sqlStringPattern matches the contents of a single-quoted or E'...'
	// SQL string literal, using the standard trick for literals that may
	// contain doubled ('') escaped quotes: alternate "any non-quote" with
	// "a doubled quote" so the match only ends at a lone, unescaped quote.
	sqlStringPattern = `'(?:[^']|'')*'`

	// valuesRowPattern matches one row of the seeded-selector VALUES
	// blocks: (name, enabled, allow_disable, E'cypher', E'description|.
	valuesRowPattern = regexp.MustCompile(
		`\(\s*'((?:[^']|'')*)'\s*,\s*(?:true|false)\s*,\s*(?:true|false)\s*,\s*E'((?:[^']|'')*)'\s*,\s*E` + sqlStringPattern + `\s*\)`,
	)
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run ./scripts/extract-prebuilt-queries.go <upstream-checkout-root>")
		os.Exit(1)
	}
	root := os.Args[1]

	agtPath := filepath.Join(root, "packages", "javascript", "bh-shared-ui", "src", "commonSearchesAGT.ts")
	agiPath := filepath.Join(root, "packages", "javascript", "bh-shared-ui", "src", "commonSearchesAGI.ts")
	sqlPath := filepath.Join(root, "cmd", "api", "src", "database", "migration", "migrations", "00000000000001_init.sql")

	agt, err := extractCommonSearches(agtPath, true)
	if err != nil {
		fatalf("extract %s: %v", agtPath, err)
	}
	agi, err := extractCommonSearches(agiPath, false)
	if err != nil {
		fatalf("extract %s: %v", agiPath, err)
	}
	selectors, err := extractSelectors(sqlPath)
	if err != nil {
		fatalf("extract %s: %v", sqlPath, err)
	}

	if err := checkCounts(agt, agi, selectors); err != nil {
		fatalf("%v", err)
	}

	if err := writeJSON(filepath.Join("testdata", "prebuilt", "agt.json"), agt); err != nil {
		fatalf("%v", err)
	}
	if err := writeJSON(filepath.Join("testdata", "prebuilt", "agi.json"), agi); err != nil {
		fatalf("%v", err)
	}
	if err := writeJSON(filepath.Join("testdata", "prebuilt", "selectors.json"), selectors); err != nil {
		fatalf("%v", err)
	}

	fmt.Printf("wrote %d AGT entries, %d AGI entries, %d selectors\n", len(agt), len(agi), len(selectors))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// checkCounts asserts the corpus' documented shape, mirroring (and running
// ahead of) the integration test's own count assertions --
// catching an extraction mistake here, at generation time, rather than only
// at `go test`. agt.json holds 92 entries total: the 91-entry CommonSearches
// export (one disabled) plus the 1-entry UncommonSearches probe, folded in
// alongside it (see extractCommonSearches' doc).
func checkCounts(agt, agi []commonSearchEntry, selectors []selectorEntry) error {
	if len(agt) != 92 {
		return fmt.Errorf("agt.json: got %d entries, want 92 (91 CommonSearches + 1 probe)", len(agt))
	}
	if n := countDisabled(agt); n != 1 {
		return fmt.Errorf("agt.json: got %d disabled entries, want 1", n)
	}
	if n := countProbes(agt); n != 1 {
		return fmt.Errorf("agt.json: got %d probe entries, want 1", n)
	}

	if len(agi) != 91 {
		return fmt.Errorf("agi.json: got %d entries, want 91", len(agi))
	}
	if n := countDisabled(agi); n != 1 {
		return fmt.Errorf("agi.json: got %d disabled entries, want 1", n)
	}
	if n := countProbes(agi); n != 0 {
		return fmt.Errorf("agi.json: got %d probe entries, want 0", n)
	}

	if len(selectors) != 42 {
		return fmt.Errorf("selectors.json: got %d entries, want 42", len(selectors))
	}

	return nil
}

func countDisabled(entries []commonSearchEntry) int {
	n := 0
	for _, e := range entries {
		if e.Disabled {
			n++
		}
	}
	return n
}

func countProbes(entries []commonSearchEntry) int {
	n := 0
	for _, e := range entries {
		if e.Probe {
			n++
		}
	}
	return n
}

// extractCommonSearches parses one commonSearchesAG{T,I}.ts file's
// CommonSearches export, and -- when includeUncommon is set -- its
// UncommonSearches export too (only commonSearchesAGT.ts has one), tagging
// every entry pulled from UncommonSearches with Probe: true.
func extractCommonSearches(path string, includeUncommon bool) ([]commonSearchEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	src := string(raw)

	commonStart := strings.Index(src, "export const CommonSearches")
	if commonStart < 0 {
		return nil, fmt.Errorf("CommonSearches export not found")
	}

	uncommonStart := strings.Index(src, "export const UncommonSearches")

	commonEnd := len(src)
	if uncommonStart >= 0 {
		commonEnd = uncommonStart
	}

	entries, err := extractSection(src[commonStart:commonEnd], false)
	if err != nil {
		return nil, fmt.Errorf("CommonSearches: %w", err)
	}

	if includeUncommon {
		if uncommonStart < 0 {
			return nil, fmt.Errorf("UncommonSearches export not found")
		}
		probeEntries, err := extractSection(src[uncommonStart:], true)
		if err != nil {
			return nil, fmt.Errorf("UncommonSearches: %w", err)
		}
		entries = append(entries, probeEntries...)
	}

	return entries, nil
}

// extractSection extracts every `{ name, description, query }` entry from
// one CommonSearches-shaped array literal's source text, attaching each
// entry to the nearest preceding `subheader: '...'`.
func extractSection(section string, markProbe bool) ([]commonSearchEntry, error) {
	subMatches := subheaderPattern.FindAllStringSubmatchIndex(section, -1)
	entryMatches := entryPattern.FindAllStringSubmatchIndex(section, -1)

	if len(entryMatches) == 0 {
		return nil, fmt.Errorf("no entries found")
	}

	subheaderAt := func(pos int) (string, error) {
		found := false
		var subheader string
		for _, m := range subMatches {
			if m[0] > pos {
				break
			}
			subheader = section[m[2]:m[3]]
			found = true
		}
		if !found {
			return "", fmt.Errorf("no subheader precedes offset %d", pos)
		}
		return subheader, nil
	}

	entries := make([]commonSearchEntry, 0, len(entryMatches))
	for _, m := range entryMatches {
		name := unescapeJS(section[m[2]:m[3]])
		queryRaw := section[m[4]:m[5]]

		subheader, err := subheaderAt(m[0])
		if err != nil {
			return nil, fmt.Errorf("entry %q: %w", name, err)
		}

		// `\n` (two characters, backslash-n) is the only escape either
		// file uses inside a query template literal (verified against the
		// checkout); real, unescaped newlines also appear directly in a
		// couple of entries (e.g. AGT/AGI's ESC5 query) and need no
		// conversion.
		normalized := strings.ReplaceAll(queryRaw, `\n`, "\n")

		// A `${...}` placeholder is resolved by the JS runtime wherever it
		// textually appears inside the backtick string, including inside
		// the fully-commented-out many-to-many query below -- so
		// disabled-ness is determined on the pre-resolution text (every
		// non-blank line starting with "//"), but the *stored* query
		// still gets its placeholders resolved like any other entry.
		disabled := isCommentedOut(normalized)

		resolved, err := resolveInterpolations(normalized)
		if err != nil {
			return nil, fmt.Errorf("entry %q: %w", name, err)
		}

		entries = append(entries, commonSearchEntry{
			Subheader: subheader,
			Name:      name,
			Query:     resolved,
			Disabled:  disabled,
			Probe:     markProbe,
		})
	}

	return entries, nil
}

// isCommentedOut reports whether every non-blank line of a (newline
// -normalized, pre-interpolation) query is a `//` comment -- the shape of
// the one many-to-many shortestPath query both files ship disabled by
// default.
func isCommentedOut(query string) bool {
	nonBlank := 0
	for _, line := range strings.Split(query, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		nonBlank++
		if !strings.HasPrefix(trimmed, "//") {
			return false
		}
	}
	return nonBlank > 0
}

// resolveInterpolations replaces every `${identifier}` in s with its value
// from interpolations, failing loudly if s references an identifier this
// tool doesn't know how to resolve (see interpolations' doc).
func resolveInterpolations(s string) (string, error) {
	var missing []string
	resolved := interpolationPattern.ReplaceAllStringFunc(s, func(match string) string {
		name := interpolationPattern.FindStringSubmatch(match)[1]
		value, ok := interpolations[name]
		if !ok {
			missing = append(missing, name)
			return match
		}
		return value
	})

	if len(missing) > 0 {
		return "", fmt.Errorf("unresolved interpolation(s): %s (add to the interpolations map)", strings.Join(missing, ", "))
	}
	return resolved, nil
}

// unescapeJS undoes the two escapes entryPattern's name capture group can
// contain: `\'` and `\\`. Neither actually appears in either file's entry
// names today (all plain text), but the capture group allows for them.
func unescapeJS(s string) string {
	s = strings.ReplaceAll(s, `\'`, "'")
	s = strings.ReplaceAll(s, `\\`, `\`)
	return s
}

// unescapeSQL undoes the standard SQL doubled-single-quote escape (a pair of
// single quotes standing for one literal quote) in a string already
// extracted by sqlStringPattern/valuesRowPattern.
func unescapeSQL(s string) string {
	return strings.ReplaceAll(s, "''", "'")
}

// decodeSQLEString decodes the content of a Postgres E'...' extended
// string literal: undo the SQL doubled-quote escape, then the one backslash
// escape these files actually use, \n -> newline (verified against the
// checkout: no other backslash escape appears in any selector name/cypher
// field).
func decodeSQLEString(s string) string {
	return strings.ReplaceAll(unescapeSQL(s), `\n`, "\n")
}

// extractSelectors parses the three INSERT statements in
// 00000000000001_init.sql that seed a cypher-type (type = 2)
// asset_group_tag_selector_seeds row: a 40-row VALUES block and a 1-row
// VALUES block, each wrapped in a "-- START"/"-- END" comment pair (found
// positionally, not by line number, since upstream line numbers drift
// across versions), plus a single literal-valued INSERT for "Incoming
// Forest Trust Builders" that carries no such comment markers. See this
// file's package doc for why these three, together, total 42.
func extractSelectors(path string) ([]selectorEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	src := string(raw)

	block1, afterBlock1, err := sliceBetweenMarkers(src, "-- START", "-- END", 0)
	if err != nil {
		return nil, fmt.Errorf("first -- START/-- END block: %w", err)
	}
	block2, _, err := sliceBetweenMarkers(src, "-- START", "-- END", afterBlock1)
	if err != nil {
		return nil, fmt.Errorf("second -- START/-- END block: %w", err)
	}

	selectors := extractValuesRowSelectors(block1)
	selectors = append(selectors, extractValuesRowSelectors(block2)...)

	incoming, err := extractIncomingForestTrustBuildersSelector(src)
	if err != nil {
		return nil, fmt.Errorf("incoming forest trust builders selector: %w", err)
	}
	selectors = append(selectors, incoming)

	return selectors, nil
}

// sliceBetweenMarkers returns the text strictly between the first
// occurrence of startMarker at or after fromIndex and the following
// endMarker, plus the offset immediately after that endMarker (so callers
// can chain calls to find a second, later block).
func sliceBetweenMarkers(src, startMarker, endMarker string, fromIndex int) (slice string, nextIndex int, err error) {
	startRel := strings.Index(src[fromIndex:], startMarker)
	if startRel < 0 {
		return "", 0, fmt.Errorf("start marker %q not found", startMarker)
	}
	start := fromIndex + startRel + len(startMarker)

	endRel := strings.Index(src[start:], endMarker)
	if endRel < 0 {
		return "", 0, fmt.Errorf("end marker %q not found after offset %d", endMarker, start)
	}
	end := start + endRel

	return src[start:end], end + len(endMarker), nil
}

// extractValuesRowSelectors extracts every (name, enabled, allow_disable,
// E'cypher', E'description') row from a VALUES block's source text.
func extractValuesRowSelectors(block string) []selectorEntry {
	var out []selectorEntry
	for _, m := range valuesRowPattern.FindAllStringSubmatch(block, -1) {
		out = append(out, selectorEntry{
			Name:  unescapeSQL(m[1]),
			Query: decodeSQLEString(m[2]),
		})
	}
	return out
}

// extractIncomingForestTrustBuildersSelector extracts the one seeded cypher
// selector added as a standalone literal-valued INSERT rather than a VALUES
// row (see this file's package doc). It is found by two structural
// anchors, neither of which is the entry's own name text: the
// `WHERE NOT EXISTS(SELECT 1 FROM asset_group_tag_selectors WHERE name =
// '...')` clause that (uniquely, in this file) compares name against a
// string literal instead of a joined column, for the name; and the
// `SELECT\n\ts.id,\n\t2,\n\tE'...'\nFROM s;` shape -- literal E'...' cypher
// text rather than a `d.cypher` column reference -- for the query, which
// (uniquely, among the three type = 2 seed INSERTs) is this one's shape.
func extractIncomingForestTrustBuildersSelector(src string) (selectorEntry, error) {
	nameMatch := regexp.MustCompile(
		`WHERE NOT EXISTS\(SELECT 1 FROM asset_group_tag_selectors WHERE name = '((?:[^']|'')*)'\)`,
	).FindStringSubmatch(src)
	if nameMatch == nil {
		return selectorEntry{}, fmt.Errorf("name anchor not found")
	}

	cypherMatch := regexp.MustCompile(
		`SELECT\s*\n\s*s\.id\s*,\s*\n\s*2\s*,\s*\n\s*E'((?:[^']|'')*)'\s*\n\s*FROM s;`,
	).FindStringSubmatch(src)
	if cypherMatch == nil {
		return selectorEntry{}, fmt.Errorf("cypher anchor not found")
	}

	return selectorEntry{
		Name:  unescapeSQL(nameMatch[1]),
		Query: decodeSQLEString(cypherMatch[1]),
	}, nil
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}

	encoded, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	encoded = append(encoded, '\n')

	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
