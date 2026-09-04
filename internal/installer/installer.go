// SPDX-License-Identifier: Apache-2.0

// Package installer upgrades a BloodHound CE compose deployment to BloodTrail
// and can report on, verify, or roll back that change.
package installer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/backup"
	"github.com/MihhailSokolov/BloodTrail/internal/compose"
	"github.com/MihhailSokolov/BloodTrail/internal/dbswitch"
	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
	"github.com/MihhailSokolov/BloodTrail/internal/manifest"
	"github.com/MihhailSokolov/BloodTrail/internal/toolapi"
	"github.com/MihhailSokolov/BloodTrail/internal/verify"
)

const (
	DefaultImageRepo  = "ghcr.io/mihhailsokolov/bloodhound-bloodtrail"
	DefaultAPIURL     = "http://127.0.0.1:8080"
	DefaultAdminUser  = "admin"
	toolAPIBaseURL    = "http://bloodhound:2112"
	driverName        = "bloodtrail"
	migrationPoll     = 5 * time.Second
	logMarkerAttempts = 24 // x 5s = 2 minutes
)

// Options are the user-facing knobs; zero values take the defaults above.
type Options struct {
	ComposeFile          string
	ProjectDir           string
	Image                string
	ImageRepo            string
	DriverVersion        string
	APIURL               string
	AdminUser            string
	AdminPassword        string
	Yes                  bool
	ReplacePostgresGraph bool
	MigrationTimeout     time.Duration
	VerifyTimeout        time.Duration
	Now                  func() time.Time
}

// Deps are the side-effecting collaborators, replaceable in tests.
type Deps struct {
	Runner              dockerx.Runner
	HTTP                *http.Client
	Out                 io.Writer
	Confirm             func(prompt string) bool
	NewToolAPITransport func(network string) toolapi.Transport
	InstallerVersion    string
}

func (o *Options) defaults() error {
	if o.ComposeFile == "" {
		o.ComposeFile = "docker-compose.yml"
	}
	abs, err := filepath.Abs(o.ComposeFile)
	if err != nil {
		return err
	}
	o.ComposeFile = abs
	if o.ProjectDir == "" {
		o.ProjectDir = filepath.Dir(abs)
	}
	if o.ImageRepo == "" {
		o.ImageRepo = DefaultImageRepo
	}
	if o.APIURL == "" {
		o.APIURL = DefaultAPIURL
	}
	if o.AdminUser == "" {
		o.AdminUser = DefaultAdminUser
	}
	if o.MigrationTimeout == 0 {
		o.MigrationTimeout = 6 * time.Hour
	}
	if o.VerifyTimeout == 0 {
		o.VerifyTimeout = 20 * time.Minute
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return nil
}

func (d *Deps) defaults() {
	if d.Runner == nil {
		d.Runner = dockerx.ExecRunner{}
	}
	if d.HTTP == nil {
		d.HTTP = &http.Client{Timeout: 30 * time.Second}
	}
	if d.Out == nil {
		d.Out = io.Discard
	}
	if d.Confirm == nil {
		d.Confirm = func(string) bool { return false }
	}
	if d.NewToolAPITransport == nil {
		runner := d.Runner
		d.NewToolAPITransport = func(network string) toolapi.Transport {
			return toolapi.CurlContainerTransport{Runner: runner, Network: network}
		}
	}
}

func (o Options) compose(runner dockerx.Runner) dockerx.Compose {
	return composeHandle(runner, o.ComposeFile, o.ProjectDir)
}

// composeHandle addresses the project as the operator configured it. Naming a
// file with -f makes docker compose ignore COMPOSE_FILE entirely, so the other
// files that variable lists have to be passed along too or the project the
// installer reads and restarts is not the project the operator runs.
func composeHandle(runner dockerx.Runner, composeFile, projectDir string) dockerx.Compose {
	c := dockerx.Compose{Runner: runner, File: composeFile, ProjectDir: projectDir}
	envData, err := os.ReadFile(filepath.Join(projectDir, ".env"))
	if err != nil {
		return c
	}
	for _, f := range compose.ComposeFiles(string(envData)) {
		if !filepath.IsAbs(f) {
			f = filepath.Join(projectDir, f)
		}
		c = c.WithExtraFile(f)
	}
	return c
}

// imageExists reports whether the image can be found locally or in a registry
// without changing anything, so the check can run before the backup. It
// discards both commands' stdout; only success/failure matters.
func imageExists(ctx context.Context, runner dockerx.Runner, image string) bool {
	if _, err := runner.Run(ctx, nil, "docker", "image", "inspect", image); err == nil {
		return true
	}
	_, err := runner.Run(ctx, nil, "docker", "manifest", "inspect", image)
	return err == nil
}

// resolveImage confirms the target image exists, falling back to the moving
// alias for the same upstream release when the version-stamped tag is not
// published. A CLI release always names an image built for its own version,
// but the images are built on their own schedule, so that tag can lag behind.
// The alias carries the newest driver build for the upstream release, which is
// the closest thing to what was asked for.
func resolveImage(ctx context.Context, deps Deps, target, alias string) (string, error) {
	if imageExists(ctx, deps.Runner, target) {
		return target, nil
	}
	if alias != "" && alias != target && imageExists(ctx, deps.Runner, alias) {
		_, _ = fmt.Fprintf(deps.Out, "    %s is not published; falling back to %s, which holds the newest driver build for this release\n", target, alias)
		return alias, nil
	}
	return "", fmt.Errorf("target image %s is neither present locally nor in a registry; build or pull it first", target)
}

// untouchedGraph names the copy of the graph a rollback returns to. Only a
// deployment that was on Neo4j has one to return to: an install that started
// on PostgreSQL never migrated anything, so the graph it goes back to is the
// PostgreSQL one it has been using all along.
func untouchedGraph(originalDriver string) string {
	if originalDriver == "neo4j" {
		return "the Neo4j graph this install migrates from stays as it is"
	}
	return "the PostgreSQL graph is not touched"
}

// ensureCurlImage puts the throwaway container the tool API is reached through
// on the host before anything is changed. It is otherwise pulled in the middle
// of the install, so a host without registry access finds out only once the
// migration is due, with the backup already taken.
func ensureCurlImage(ctx context.Context, runner dockerx.Runner) error {
	if _, err := runner.Run(ctx, nil, "docker", "image", "inspect", toolapi.CurlImage); err == nil {
		return nil
	}
	if _, err := runner.Run(ctx, nil, "docker", "pull", toolapi.CurlImage); err != nil {
		return fmt.Errorf("the migration reaches BloodHound's tool API through %s, which is not on this host and could not be pulled: %w", toolapi.CurlImage, err)
	}
	return nil
}

// Install performs inventory, backup, migration if needed, image swap, driver switch and verification.
func Install(ctx context.Context, deps Deps, opts Options) error {
	if err := opts.defaults(); err != nil {
		return err
	}
	deps.defaults()
	say := func(format string, a ...any) { _, _ = fmt.Fprintf(deps.Out, format+"\n", a...) }

	if manifest.Exists(opts.ProjectDir) {
		return fmt.Errorf("a previous install did not complete or is still installed in %s; run `bloodtrail rollback` first", opts.ProjectDir)
	}

	c := opts.compose(deps.Runner)
	say("==> Inventory")
	inv, store, err := takeInventory(ctx, c)
	if err != nil {
		return err
	}
	// alias is the moving tag for this upstream release, used only as a
	// fallback for a derived target: an explicit --image is taken literally.
	target, alias := opts.Image, ""
	if target == "" {
		if inv.UpstreamTag == "" {
			return fmt.Errorf("the deployment runs an unpinned or unrecognised image tag %q; set BLOODHOUND_TAG in .env or pass --image", inv.Image)
		}
		alias = opts.ImageRepo + ":" + inv.UpstreamTag
		target = alias
		if opts.DriverVersion != "" {
			target = alias + "-bt" + opts.DriverVersion
		}
	}
	say("    project:        %s", inv.Config.Name)
	say("    current image:  %s", inv.Image)
	say("    target image:   %s", target)
	say("    active driver:  %s (row present: %v)", inv.ActiveDriver, inv.DriverRow != nil)
	say("    graph size:     %d nodes, %d edges (from %s)", inv.Nodes, inv.Edges, inv.CountSource)
	say("    host memory:    %.1f GiB", inv.HostMemoryGiB)
	if inv.ActiveDriver == "neo4j" {
		say("    the graph will be migrated from Neo4j to PostgreSQL (this can take a long time on large graphs)")
	}
	if !opts.Yes && !deps.Confirm("Proceed with the installation?") {
		return errors.New("aborted by user")
	}

	say("==> Checking target image availability")
	if target, err = resolveImage(ctx, deps, target, alias); err != nil {
		return err
	}
	if inv.ActiveDriver == "neo4j" {
		if err := ensureCurlImage(ctx, deps.Runner); err != nil {
			return err
		}
	}

	say("==> Backup")
	backupDir, err := backup.NewDir(opts.ProjectDir, opts.Now())
	if err != nil {
		return err
	}
	if err := backup.DumpDatabase(ctx, c, appDBService, inv.PGUser, inv.PGDB, filepath.Join(backupDir, "app-db.dump")); err != nil {
		return err
	}
	if err := backup.CopyFiles(backupDir, opts.ComposeFile, filepath.Join(opts.ProjectDir, ".env")); err != nil {
		return err
	}
	say("    written to %s", backupDir)

	// The manifest is saved now, with everything already known, so that a
	// failure anywhere from here on (including inside the migration, which
	// may already have flipped the live driver to pg) leaves `bloodtrail
	// rollback` able to find an installation to undo.
	overridePath := filepath.Join(opts.ProjectDir, compose.OverrideFileName)
	m := manifest.Manifest{
		InstallerVersion: deps.InstallerVersion, InstalledAt: opts.Now().UTC().Format(time.RFC3339),
		ComposeFile: opts.ComposeFile, ProjectDir: opts.ProjectDir, ProjectName: inv.Config.Name,
		OriginalImage: inv.Image, OriginalDriverRow: inv.DriverRow, BackupDir: backupDir,
		OverrideFile: overridePath, TargetImage: target, UpstreamTag: inv.UpstreamTag,
		PGUser: inv.PGUser, PGDatabase: inv.PGDB,
	}
	if err := m.Save(opts.ProjectDir); err != nil {
		return fmt.Errorf("saving manifest: %w", err)
	}
	rollbackHint := fmt.Sprintf("run `bloodtrail rollback` to restore the original driver and image (%s); backup in %s",
		untouchedGraph(inv.ActiveDriver), backupDir)

	if inv.ActiveDriver == "neo4j" {
		// BloodHound's migrator only inserts: run against a PostgreSQL graph
		// an earlier migration already filled, it collides with the unique
		// object id index at the first node, logs the failure and returns to
		// idle. The node count would still be non-zero from that earlier run,
		// so without this check the installer would happily switch the
		// deployment onto a stale graph and lose everything ingested into
		// Neo4j since.
		if err := checkPostgresGraphEmpty(ctx, deps, opts, store, backupDir, rollbackHint); err != nil {
			return err
		}
		say("==> Migrating graph from Neo4j to PostgreSQL")
		client := toolapi.Client{BaseURL: toolAPIBaseURL, Transport: deps.NewToolAPITransport(inv.Config.DefaultNetworkName())}
		if err := client.MigrateNeoToPG(ctx, migrationPoll, opts.MigrationTimeout); err != nil {
			return fmt.Errorf("the migration reported an error: %w; BloodHound's migrator may already have switched the active driver to pg; %s", err, rollbackHint)
		}
		nodes, edges, err := store.CountGraph(ctx)
		if err != nil {
			return fmt.Errorf("counting migrated graph: %w; %s", err, rollbackHint)
		}
		// The migrator reports per-object failures to the server log only and
		// still returns to idle, so a run that imported the nodes but dropped
		// every edge looks identical to a clean one from the API. Check what
		// arrived against what Neo4j held, then read the log.
		switch {
		case inv.CountSource == "neo4j" && (nodes != inv.Nodes || edges != inv.Edges):
			return fmt.Errorf("the migration moved %d nodes and %d edges into PostgreSQL but Neo4j holds %d nodes and %d edges; "+
				"BloodHound's migrator reports failures to its log only; %s", nodes, edges, inv.Nodes, inv.Edges, rollbackHint)
		case inv.CountSource != "neo4j" && nodes == 0:
			return fmt.Errorf("the migration finished with zero nodes in PostgreSQL; BloodHound's migrator may already have switched the active driver to pg; %s", rollbackHint)
		}
		if line, err := migratorFailureInLogs(ctx, c); err != nil {
			return fmt.Errorf("reading %s logs after the migration: %w; %s", bloodhoundService, err, rollbackHint)
		} else if line != "" {
			return fmt.Errorf("the migration logged a failure: %q; %s", line, rollbackHint)
		}
		say("    PostgreSQL now holds %d nodes and %d edges", nodes, edges)
	}

	say("==> Switching to the BloodTrail image and driver")
	override := compose.Override{Service: bloodhoundService, Image: target, Environment: map[string]string{"bhe_graph_driver": driverName}}
	if err := os.WriteFile(overridePath, []byte(override.Render()), 0o644); err != nil {
		return fmt.Errorf("writing override: %w; %s", err, rollbackHint)
	}
	envPath := filepath.Join(opts.ProjectDir, ".env")
	envData, err := os.ReadFile(envPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("reading .env: %w; %s", err, rollbackHint)
	}
	baseRel, _ := filepath.Rel(opts.ProjectDir, opts.ComposeFile)
	if err := os.WriteFile(envPath, []byte(compose.AddComposeFile(string(envData), baseRel, compose.OverrideFileName)), 0o644); err != nil {
		return fmt.Errorf("writing .env: %w; %s", err, rollbackHint)
	}
	if err := store.Set(ctx, driverName); err != nil {
		return fmt.Errorf("setting database_switch: %w; %s", err, rollbackHint)
	}
	c = c.WithExtraFile(overridePath)
	if err := c.Up(ctx); err != nil {
		return fmt.Errorf("docker compose up: %w; %s", err, rollbackHint)
	}

	say("==> Verifying")
	if err := runVerification(ctx, deps, opts, c); err != nil {
		return fmt.Errorf("the image and driver switch completed, but verification failed: %w; run `bloodtrail rollback` to revert if needed", err)
	}
	return nil
}

// migratorFailureMarkers are the phrases BloodHound's migrator logs when it
// cannot import an object, migrate a batch or assert a kind. It carries on and
// still returns to idle afterwards, so the log is the only evidence.
var migratorFailureMarkers = []string{"Failed importing", "Unable to migrate", "Unable to assert"}

// migratorFailureInLogs returns the first log line reporting a migration
// failure, or "" when there is none.
func migratorFailureInLogs(ctx context.Context, c dockerx.Compose) (string, error) {
	logs, err := c.Logs(ctx, bloodhoundService)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(logs), "\n") {
		for _, marker := range migratorFailureMarkers {
			if strings.Contains(line, marker) {
				return strings.TrimSpace(line), nil
			}
		}
	}
	return "", nil
}

// checkPostgresGraphEmpty refuses to start a migration when PostgreSQL already
// holds a graph, unless the caller asked for that graph to be replaced. Install
// refuses to run at all while a manifest from this install already sits in
// opts.ProjectDir, so an operator who hits the refusal below cannot simply
// rerun with --replace-postgres-graph: they have to roll back the install
// that saved it first.
func checkPostgresGraphEmpty(ctx context.Context, deps Deps, opts Options, store dbswitch.Store, backupDir, rollbackHint string) error {
	nodes, edges, err := store.CountGraph(ctx)
	if err != nil {
		return fmt.Errorf("counting the PostgreSQL graph before migrating: %w; %s", err, rollbackHint)
	}
	if nodes == 0 {
		return nil
	}
	if !opts.ReplacePostgresGraph {
		return fmt.Errorf("PostgreSQL already holds %d nodes and %d edges from an earlier migration or install; "+
			"refusing to migrate on top of them; run `bloodtrail rollback` first, then rerun with --replace-postgres-graph "+
			"to clear them (the backup in %s holds the current state)", nodes, edges, backupDir)
	}
	_, _ = fmt.Fprintf(deps.Out, "    clearing %d nodes and %d edges left in PostgreSQL by an earlier migration\n", nodes, edges)
	if err := store.ClearGraph(ctx); err != nil {
		return fmt.Errorf("clearing the PostgreSQL graph: %w; %s", err, rollbackHint)
	}
	return nil
}

// runVerification checks the cheap and decisive things first: the driver log
// line says the right image booted with the right driver, and it appears
// within seconds. The smoke test goes last because it ingests a fixture and
// triggers a full analysis, which on a real graph can run for a long time —
// there is no sense paying for it to learn what the log already said.
func runVerification(ctx context.Context, deps Deps, opts Options, c dockerx.Compose) error {
	say := func(format string, a ...any) { _, _ = fmt.Fprintf(deps.Out, format+"\n", a...) }
	if err := waitForDriverLogLine(ctx, c); err != nil {
		return err
	}
	say("    driver log line present: %q", verify.DriverActiveMarker)
	if err := verify.WaitForAPI(ctx, deps.HTTP, opts.APIURL, opts.VerifyTimeout); err != nil {
		return err
	}
	say("    API answers at %s", opts.APIURL)
	if opts.AdminPassword != "" {
		smoke := verify.Smoke{Client: deps.HTTP, BaseURL: opts.APIURL, User: opts.AdminUser, Password: opts.AdminPassword}
		if err := smoke.Run(ctx, opts.VerifyTimeout); err != nil {
			return fmt.Errorf("smoke test: %w", err)
		}
		say("    fixture ingested, analysed and found through the search API")
	}
	return nil
}

func waitForDriverLogLine(ctx context.Context, c dockerx.Compose) error {
	for attempt := 0; attempt < logMarkerAttempts; attempt++ {
		ok, err := verify.LogsContain(ctx, c, bloodhoundService, verify.DriverActiveMarker)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("the %s service never logged %q; the image may not contain the driver", bloodhoundService, verify.DriverActiveMarker)
}

// Rollback restores the original image and driver recorded in the manifest.
// It trusts the manifest (not opts) for the compose file, project directory
// and PostgreSQL credentials, so a rollback invoked from elsewhere still
// targets the recorded installation.
func Rollback(ctx context.Context, deps Deps, opts Options) error {
	if err := opts.defaults(); err != nil {
		return err
	}
	deps.defaults()
	say := func(format string, a ...any) { _, _ = fmt.Fprintf(deps.Out, format+"\n", a...) }

	m, err := manifest.Load(opts.ProjectDir)
	if err != nil {
		return fmt.Errorf("no BloodTrail installation found in %s: %w", opts.ProjectDir, err)
	}
	c := composeHandle(deps.Runner, m.ComposeFile, m.ProjectDir)
	store := dbswitch.Store{Compose: c, Service: appDBService, User: m.PGUser, Database: m.PGDatabase}

	say("==> Restoring the graph driver setting")
	if m.OriginalDriverRow != nil {
		if err := store.Set(ctx, *m.OriginalDriverRow); err != nil {
			return fmt.Errorf("restoring database_switch: %w (the override file is still in place; fix the database and rerun rollback)", err)
		}
	} else if err := store.Delete(ctx); err != nil {
		return fmt.Errorf("restoring database_switch: %w (the override file is still in place; fix the database and rerun rollback)", err)
	}

	say("==> Restoring compose configuration")
	if err := os.Remove(m.OverrideFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	envPath := filepath.Join(m.ProjectDir, ".env")
	envData, err := os.ReadFile(envPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("reading .env: %w", err)
	}
	if err == nil {
		if err := os.WriteFile(envPath, []byte(compose.RemoveComposeFile(string(envData), compose.OverrideFileName)), 0o644); err != nil {
			return err
		}
	}

	say("==> Restarting with the original image %s", m.OriginalImage)
	// Everything that makes the deployment BloodTrail is undone by this point,
	// so a failure from here on is about the deployment coming back up, not
	// about the rollback being incomplete. Say so: the manifest is still
	// there, and rerunning rollback picks up where this left off.
	restored := "the driver row and the compose files are already restored; rerun `bloodtrail rollback` once the deployment can start"
	if err := composeHandle(deps.Runner, m.ComposeFile, m.ProjectDir).Up(ctx); err != nil {
		return fmt.Errorf("restarting with the original image: %w (%s)", err, restored)
	}
	if err := verify.WaitForAPI(ctx, deps.HTTP, opts.APIURL, opts.VerifyTimeout); err != nil {
		return fmt.Errorf("waiting for the API after the restart: %w (%s; check the %s logs)", err, restored, bloodhoundService)
	}
	if err := os.Remove(manifest.Path(m.ProjectDir)); err != nil {
		return err
	}
	say("    rolled back; backups kept in %s", m.BackupDir)
	return nil
}

// runningImage reports the image of the service's container. The two answers
// differ whenever the compose files on disk have moved on from what is running
// — which is exactly the state a half-finished install or rollback leaves — so
// status has to read the container, not just the files.
func runningImage(ctx context.Context, c dockerx.Compose, service string) string {
	out, err := c.PS(ctx, service)
	if err != nil {
		return "not running"
	}
	// Compose v2 prints a JSON array in recent versions and one object per
	// line in older ones. A decoder reading the stream handles both: it takes
	// the first value, array or object, and an array's first element after
	// stepping past the bracket.
	dec := json.NewDecoder(bytes.NewReader(out))
	if tok, err := dec.Token(); err != nil {
		return "not running"
	} else if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		dec = json.NewDecoder(bytes.NewReader(out))
	}
	var container struct {
		Image string `json:"Image"`
	}
	if err := dec.Decode(&container); err != nil || container.Image == "" {
		return "not running"
	}
	return container.Image
}

// Status prints the manifest, the driver row and the running image.
func Status(ctx context.Context, deps Deps, opts Options) error {
	if err := opts.defaults(); err != nil {
		return err
	}
	deps.defaults()
	c := opts.compose(deps.Runner)
	inv, _, err := takeInventory(ctx, c)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(deps.Out, "project:       %s\nconfigured:    %s\nrunning:       %s\nactive driver: %s\n",
		inv.Config.Name, inv.Image, runningImage(ctx, c, bloodhoundService), inv.ActiveDriver)
	if m, err := manifest.Load(opts.ProjectDir); err == nil {
		_, _ = fmt.Fprintf(deps.Out, "bloodtrail:    installed %s, image %s, backups %s\n", m.InstalledAt, m.TargetImage, m.BackupDir)
	} else {
		_, _ = fmt.Fprintln(deps.Out, "bloodtrail:    not installed")
	}
	return nil
}

// Verify runs the post-install checks on their own.
func Verify(ctx context.Context, deps Deps, opts Options) error {
	if err := opts.defaults(); err != nil {
		return err
	}
	deps.defaults()
	return runVerification(ctx, deps, opts, opts.compose(deps.Runner))
}
