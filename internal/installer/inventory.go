// SPDX-License-Identifier: Apache-2.0

package installer

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/MihhailSokolov/BloodTrail/internal/compose"
	"github.com/MihhailSokolov/BloodTrail/internal/dbswitch"
	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

const (
	bloodhoundService = "bloodhound"
	appDBService      = "app-db"
	graphDBService    = "graph-db"
	defaultPGUser     = "bloodhound"
	defaultPGDatabase = "bloodhound"
)

// upstreamTagPattern matches a plausible Docker image tag.
var upstreamTagPattern = regexp.MustCompile(`^v?[0-9A-Za-z._-]+$`)

// versionTagPattern matches a tag that starts with a bare release number.
// SpecterOps publishes the application image on Docker Hub without the "v"
// of the GitHub source tag ("9.6.0"), while BloodTrail images are tagged
// after the source tag ("v9.6.0", "v9.6.0-bt0.1.0"), so a derived tag has to
// be normalised back to the "v" form before it names a BloodTrail image.
var versionTagPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+`)

// upstreamTagFromImage derives the version tag BloodTrail images are built
// against from the final path segment of image (so a registry host:port
// prefix is never mistaken for a tag). It refuses digest references ("@"),
// untagged images and the moving "latest" tag, returning "" so the caller
// requires --image instead of guessing.
func upstreamTagFromImage(image string) string {
	seg := image
	if i := strings.LastIndex(image, "/"); i >= 0 {
		seg = image[i+1:]
	}
	if strings.Contains(seg, "@") {
		return ""
	}
	i := strings.LastIndex(seg, ":")
	if i < 0 {
		return ""
	}
	tag := seg[i+1:]
	if tag == "" || tag == "latest" {
		return ""
	}
	if !upstreamTagPattern.MatchString(tag) {
		return ""
	}
	if versionTagPattern.MatchString(tag) {
		return "v" + tag
	}
	return tag
}

// inventory is everything Install learns before changing anything.
type inventory struct {
	Config        compose.Config
	Image         string // current bloodhound image
	UpstreamTag   string // derived from the last path segment of Image
	DriverRow     *string
	ActiveDriver  string // row if present, else bhe_graph_driver, else "neo4j"
	PGUser, PGDB  string
	Nodes, Edges  int64 // from PostgreSQL when the graph is there, else Neo4j best effort, else -1
	CountSource   string
	HostMemoryGiB float64 // 0 when unknown
}

func takeInventory(ctx context.Context, c dockerx.Compose) (inventory, dbswitch.Store, error) {
	raw, err := c.ConfigJSON(ctx)
	if err != nil {
		return inventory{}, dbswitch.Store{}, fmt.Errorf("reading compose config: %w", err)
	}
	cfg, err := compose.ParseConfig(raw)
	if err != nil {
		return inventory{}, dbswitch.Store{}, err
	}
	svc, ok := cfg.Services[bloodhoundService]
	if !ok {
		return inventory{}, dbswitch.Store{}, fmt.Errorf("compose project %q has no %q service", cfg.Name, bloodhoundService)
	}
	if _, ok := cfg.Services[appDBService]; !ok {
		return inventory{}, dbswitch.Store{}, fmt.Errorf("compose project %q has no %q service", cfg.Name, appDBService)
	}

	inv := inventory{
		Config: cfg, Image: svc.Image, UpstreamTag: upstreamTagFromImage(svc.Image),
		PGUser: defaultPGUser, PGDB: defaultPGDatabase, Nodes: -1, Edges: -1, CountSource: "unknown",
	}
	if u := cfg.Services[appDBService].Environment["POSTGRES_USER"]; u != "" {
		inv.PGUser = u
	}
	if d := cfg.Services[appDBService].Environment["POSTGRES_DB"]; d != "" {
		inv.PGDB = d
	}

	store := dbswitch.Store{Compose: c, Service: appDBService, User: inv.PGUser, Database: inv.PGDB}
	row, present, err := store.Read(ctx)
	if err != nil {
		return inventory{}, store, fmt.Errorf("reading database_switch: %w", err)
	}
	if present {
		inv.DriverRow = &row
		inv.ActiveDriver = row
	} else if d := svc.Environment["bhe_graph_driver"]; d != "" {
		inv.ActiveDriver = d
	} else {
		inv.ActiveDriver = "neo4j"
	}

	switch inv.ActiveDriver {
	case "pg", "bloodtrail":
		if n, e, err := store.CountGraph(ctx); err == nil {
			inv.Nodes, inv.Edges, inv.CountSource = n, e, "postgresql"
		}
	case "neo4j":
		if n, e, err := neo4jCounts(ctx, c, cfg); err == nil {
			inv.Nodes, inv.Edges, inv.CountSource = n, e, "neo4j"
		}
	}
	inv.HostMemoryGiB = hostMemoryGiB()
	return inv, store, nil
}

// neo4jCounts is best effort: it needs cypher-shell inside graph-db and NEO4J_AUTH.
func neo4jCounts(ctx context.Context, c dockerx.Compose, cfg compose.Config) (int64, int64, error) {
	auth := cfg.Services[graphDBService].Environment["NEO4J_AUTH"]
	user, pass, ok := strings.Cut(auth, "/")
	if !ok {
		return 0, 0, fmt.Errorf("NEO4J_AUTH not set on %s", graphDBService)
	}
	count := func(query string) (int64, error) {
		out, err := c.Exec(ctx, graphDBService, nil, "cypher-shell", "-u", user, "-p", pass, "--format", "plain", query)
		if err != nil {
			return 0, err
		}
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		return strconv.ParseInt(strings.TrimSpace(lines[len(lines)-1]), 10, 64)
	}
	n, err := count("MATCH (n) RETURN count(n)")
	if err != nil {
		return 0, 0, err
	}
	e, err := count("MATCH ()-[r]->() RETURN count(r)")
	if err != nil {
		return 0, 0, err
	}
	return n, e, nil
}

// hostMemoryGiB returns total physical memory, 0 if it cannot be determined.
func hostMemoryGiB() float64 {
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err != nil {
			return 0
		}
		b, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		return b / (1 << 30)
	case "linux":
		out, err := exec.Command("sh", "-c", "grep MemTotal /proc/meminfo | awk '{print $2}'").Output()
		if err != nil {
			return 0
		}
		kb, _ := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
		return kb / (1 << 20)
	}
	return 0
}
