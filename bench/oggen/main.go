// SPDX-License-Identifier: Apache-2.0

// Command oggen generates a fictitious source-control organization as
// BloodHound OpenGraph JSON, for ingesting through BloodHound's ordinary
// file-upload pipeline and comparing a deployment with and without
// BloodTrail on OpenGraph data. See README.md.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Kinds, all in the extension's "sc" namespace. sc_Base is every upload's
// metadata.source_kind.
const (
	sourceKind = "sc_Base"

	kindOrg   = "sc_Organization"
	kindUser  = "sc_User"
	kindOwner = "sc_OrgOwner"
	kindTeam  = "sc_Team"
	kindRepo  = "sc_Repository"
	kindRole  = "sc_RepoRole"

	edgeMemberOf = "sc_MemberOf"
	edgeHasRole  = "sc_HasRole"
	edgeCanAdmin = "sc_CanAdmin"
	edgeCanWrite = "sc_CanWrite"
	edgeCanRead  = "sc_CanRead"
	edgeOrgAdmin = "sc_OrgAdmin"
	edgeOwns     = "sc_Owns"
	edgeSyncedTo = "sc_SyncedTo"
)

// Config sizes one organization.
type Config struct {
	Users, Teams, Repos int
	Seed                int64
	// ADUsers are AD user objectids (from a bench/shgen forest) to sync to
	// the first users of the organization, one each.
	ADUsers []string
}

// Node and Edge are OpenGraph's JSON shapes.
type Node struct {
	ID         string         `json:"id"`
	Kinds      []string       `json:"kinds"`
	Properties map[string]any `json:"properties"`
}

type Endpoint struct {
	MatchBy string `json:"match_by"`
	Value   string `json:"value"`
	Kind    string `json:"kind,omitempty"`
}

type Edge struct {
	Start      Endpoint       `json:"start"`
	End        Endpoint       `json:"end"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties,omitempty"`
}

func userID(i int) string { return fmt.Sprintf("SC_USER_%07d", i) }
func teamID(i int) string { return fmt.Sprintf("SC_TEAM_%06d", i) }
func repoID(i int) string { return fmt.Sprintf("SC_REPO_%06d", i) }
func roleID(repo int, level string) string {
	return fmt.Sprintf("SC_ROLE_%06d_%s", repo, strings.ToUpper(level))
}

const orgID = "SC_ORG"

var (
	roleLevels = []string{"admin", "write", "read"}
	roleEdges  = map[string]string{"admin": edgeCanAdmin, "write": edgeCanWrite, "read": edgeCanRead}
	topics     = []string{"infra", "terraform", "web", "api", "ml", "mobile", "docs", "security", "data", "ci"}
	languages  = []string{"Go", "Python", "TypeScript", "Java", "Rust", "C#"}
)

func id(value string) Endpoint { return Endpoint{MatchBy: "id", Value: value} }

// Generate builds the organization. The same Config always yields the same
// nodes and edges, in the same order. Beyond random membership and role
// noise, repository 0 is reachable along fixed paths of different lengths:
//
//	user 1 -HasRole-> repo 0 write role -CanWrite-> repo 0         (2 hops)
//	user 2 -MemberOf-> team 0 -HasRole-> repo 0 admin role -> repo (3 hops)
//	user 0 -MemberOf-> team 1 -MemberOf-> team 0 -> ... -> repo 0  (4 hops)
//	ADUsers[0] -SyncedTo-> user 0                                   (5 hops)
//
// No edge joins a node to itself: a self-loop of a traversed kind makes
// BloodTrail's variable-length executor delegate the whole pattern.
func Generate(cfg Config) ([]Node, []Edge) {
	rng := rand.New(rand.NewSource(cfg.Seed))
	var (
		nodes []Node
		edges []Edge
	)

	nodes = append(nodes, Node{ID: orgID, Kinds: []string{kindOrg}, Properties: map[string]any{"name": "acme", "plan": "enterprise"}})

	owners := max(1, cfg.Users/1000)
	for i := 0; i < cfg.Users; i++ {
		kinds := []string{kindUser}
		if i < owners {
			kinds = append(kinds, kindOwner)
		}
		nodes = append(nodes, Node{ID: userID(i), Kinds: kinds, Properties: map[string]any{
			"name":          fmt.Sprintf("user%07d", i),
			"login":         fmt.Sprintf("user%07d", i),
			"mfa_enabled":   rng.Intn(10) < 8,
			"site_admin":    rng.Intn(1000) == 0,
			"contributions": rng.Intn(5000),
			"emails":        []string{fmt.Sprintf("user%07d@acme.test", i)},
		}})
		if i < owners {
			edges = append(edges, Edge{Start: id(userID(i)), End: id(orgID), Kind: edgeOrgAdmin})
		}
	}

	for t := 0; t < cfg.Teams; t++ {
		privacy := "closed"
		if rng.Intn(4) == 0 {
			privacy = "secret"
		}
		nodes = append(nodes, Node{ID: teamID(t), Kinds: []string{kindTeam}, Properties: map[string]any{
			"name": fmt.Sprintf("team%06d", t), "privacy": privacy,
		}})
		// Nesting: most teams sit under an older one, so chains stay
		// logarithmic in depth.
		if t > 1 && rng.Intn(10) < 6 {
			edges = append(edges, Edge{Start: id(teamID(t)), End: id(teamID(t / (2 + rng.Intn(3)))), Kind: edgeMemberOf})
		}
	}

	for r := 0; r < cfg.Repos; r++ {
		visibility := "private"
		if rng.Intn(10) < 3 {
			visibility = "public"
		}
		repoTopics := []string{topics[rng.Intn(len(topics))]}
		if rng.Intn(2) == 0 {
			if extra := topics[rng.Intn(len(topics))]; extra != repoTopics[0] {
				repoTopics = append(repoTopics, extra)
			}
		}
		nodes = append(nodes, Node{ID: repoID(r), Kinds: []string{kindRepo}, Properties: map[string]any{
			"name":       fmt.Sprintf("repo%06d", r),
			"full_name":  fmt.Sprintf("acme/repo%06d", r),
			"visibility": visibility,
			"archived":   rng.Intn(20) == 0,
			"stars":      rng.Intn(2000),
			"score":      float64(rng.Intn(1000)) / 100,
			"topics":     repoTopics,
			"language":   languages[rng.Intn(len(languages))],
		}})
		edges = append(edges, Edge{Start: id(orgID), End: id(repoID(r)), Kind: edgeOwns})
		for _, level := range roleLevels {
			nodes = append(nodes, Node{ID: roleID(r, level), Kinds: []string{kindRole}, Properties: map[string]any{
				"name": fmt.Sprintf("repo%06d-%s", r, level), "permission": level,
			}})
			edges = append(edges, Edge{Start: id(roleID(r, level)), End: id(repoID(r)), Kind: roleEdges[level], Properties: map[string]any{"permission": level}})
		}
	}

	pickLevel := func() string {
		switch n := rng.Intn(10); {
		case n == 0:
			return "admin"
		case n < 6:
			return "write"
		default:
			return "read"
		}
	}

	if cfg.Teams > 0 && cfg.Repos > 0 {
		for t := 0; t < cfg.Teams; t++ {
			for n := 1 + rng.Intn(8); n > 0; n-- {
				edges = append(edges, Edge{Start: id(teamID(t)), End: id(roleID(rng.Intn(cfg.Repos), pickLevel())), Kind: edgeHasRole})
			}
		}
	}

	if cfg.Teams > 0 {
		for u := 0; u < cfg.Users; u++ {
			seen := map[int]bool{}
			for n := 1 + rng.Intn(3); n > 0; n-- {
				t := rng.Intn(cfg.Teams)
				if seen[t] {
					continue
				}
				seen[t] = true
				edges = append(edges, Edge{Start: id(userID(u)), End: id(teamID(t)), Kind: edgeMemberOf, Properties: map[string]any{"role": "member"}})
			}
		}
	}

	if cfg.Repos > 0 {
		for u := 0; u < cfg.Users; u++ {
			if rng.Intn(5) == 0 {
				edges = append(edges, Edge{Start: id(userID(u)), End: id(roleID(rng.Intn(cfg.Repos), pickLevel())), Kind: edgeHasRole, Properties: map[string]any{"direct": true}})
			}
		}
	}

	// The seeded paths to repository 0 (see the doc comment).
	if cfg.Users >= 3 && cfg.Teams >= 2 && cfg.Repos >= 1 {
		edges = append(edges,
			Edge{Start: id(userID(1)), End: id(roleID(0, "write")), Kind: edgeHasRole, Properties: map[string]any{"direct": true}},
			Edge{Start: id(userID(2)), End: id(teamID(0)), Kind: edgeMemberOf, Properties: map[string]any{"role": "maintainer"}},
			Edge{Start: id(teamID(0)), End: id(roleID(0, "admin")), Kind: edgeHasRole},
			Edge{Start: id(userID(0)), End: id(teamID(1)), Kind: edgeMemberOf, Properties: map[string]any{"role": "member"}},
			Edge{Start: id(teamID(1)), End: id(teamID(0)), Kind: edgeMemberOf},
		)
	}

	for i, adUser := range cfg.ADUsers {
		if i >= cfg.Users {
			break
		}
		edges = append(edges, Edge{Start: Endpoint{MatchBy: "id", Value: adUser, Kind: "User"}, End: id(userID(i)), Kind: edgeSyncedTo, Properties: map[string]any{"via": "saml"}})
	}

	return nodes, edges
}

// file is one OpenGraph upload file.
type file struct {
	Metadata map[string]string `json:"metadata"`
	Graph    struct {
		Nodes []Node `json:"nodes"`
		Edges []Edge `json:"edges"`
	} `json:"graph"`
}

func writeFile(path string, nodes []Node, edges []Edge) error {
	var f file
	f.Metadata = map[string]string{"source_kind": sourceKind}
	f.Graph.Nodes, f.Graph.Edges = nodes, edges
	if f.Graph.Nodes == nil {
		f.Graph.Nodes = []Node{}
	}
	if f.Graph.Edges == nil {
		f.Graph.Edges = []Edge{}
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// readADUsers returns the objectids of the User objects in a bench/shgen
// output directory, in file and object order.
func readADUsers(dir string) ([]string, error) {
	var paths []string
	if err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.Contains(filepath.Base(path), "users") && strings.HasSuffix(path, ".json") {
			paths = append(paths, path)
		}
		return err
	}); err != nil {
		return nil, err
	}
	sort.Strings(paths)
	var ids []string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var collection struct {
			Data []struct {
				ObjectIdentifier string
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &collection); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		for _, user := range collection.Data {
			ids = append(ids, user.ObjectIdentifier)
		}
	}
	if len(ids) == 0 {
		return nil, errors.New("no AD users found")
	}
	return ids, nil
}

func main() {
	var (
		out     = flag.String("out", "oggen-out", "directory to write extension.json and data/*.json into (created if missing)")
		users   = flag.Int("users", 100000, "sc_User accounts")
		teams   = flag.Int("teams", 0, "sc_Team teams; 0 means users/10")
		repos   = flag.Int("repos", 0, "sc_Repository repositories, each with admin/write/read roles; 0 means users/5")
		seed    = flag.Int64("seed", 1, "deterministic seed")
		chunk   = flag.Int("chunk", 100000, "maximum nodes or edges per data file")
		adDir   = flag.String("ad", "", "a bench/shgen output directory; its users are synced to the first sc_Users")
		syncPct = flag.Int("sync-pct", 10, "with -ad, percent of sc_Users synced to an AD user")
	)
	flag.Parse()

	// The file loops below advance by chunk items: 0 never reaches the end of
	// the data (an endless run of empty files) and a negative size slices
	// out of range.
	if *chunk <= 0 {
		fatal("-chunk must be at least 1, got %d", *chunk)
	}

	cfg := Config{Users: *users, Teams: *teams, Repos: *repos, Seed: *seed}
	if cfg.Teams == 0 {
		cfg.Teams = max(2, cfg.Users/10)
	}
	if cfg.Repos == 0 {
		cfg.Repos = max(1, cfg.Users/5)
	}
	if *adDir != "" {
		adUsers, err := readADUsers(*adDir)
		if err != nil {
			fatal("read AD users: %v", err)
		}
		cfg.ADUsers = adUsers[:min(len(adUsers), cfg.Users**syncPct/100)]
	}

	nodes, edges := Generate(cfg)

	dataDir := filepath.Join(*out, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fatal("%v", err)
	}
	// Nodes first, then edges: BloodHound ingests an upload's files in
	// name order, and "nodes-" sorts before "rels-".
	for i := 0; i*(*chunk) < len(nodes); i++ {
		part := nodes[i**chunk : min(len(nodes), (i+1)**chunk)]
		if err := writeFile(filepath.Join(dataDir, fmt.Sprintf("nodes-%04d.json", i)), part, nil); err != nil {
			fatal("%v", err)
		}
	}
	for i := 0; i*(*chunk) < len(edges); i++ {
		part := edges[i**chunk : min(len(edges), (i+1)**chunk)]
		if err := writeFile(filepath.Join(dataDir, fmt.Sprintf("rels-%04d.json", i)), nil, part); err != nil {
			fatal("%v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(*out, "extension.json"), extensionSchema(), 0o644); err != nil {
		fatal("%v", err)
	}
	fmt.Printf("oggen: %d nodes, %d edges (%d synced AD users) -> %s\n", len(nodes), len(edges), len(cfg.ADUsers), *out)
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "oggen: "+format+"\n", a...)
	os.Exit(1)
}
