// SPDX-License-Identifier: Apache-2.0

// Package installer upgrades a BloodHound CE compose deployment to BloodTrail
// and can report on, verify, or roll back that change.
package installer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	ComposeFile      string
	ProjectDir       string
	Image            string
	ImageRepo        string
	DriverVersion    string
	APIURL           string
	AdminUser        string
	AdminPassword    string
	Yes              bool
	MigrationTimeout time.Duration
	VerifyTimeout    time.Duration
	Now              func() time.Time
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
	return dockerx.Compose{Runner: runner, File: o.ComposeFile, ProjectDir: o.ProjectDir}
}

// checkImageAvailable confirms the target image can be found locally or in a
// registry without changing anything, so the check can run before the
// backup. It discards both commands' stdout; only success/failure matters.
func checkImageAvailable(ctx context.Context, runner dockerx.Runner, image string) error {
	if _, err := runner.Run(ctx, nil, "docker", "image", "inspect", image); err == nil {
		return nil
	}
	if _, err := runner.Run(ctx, nil, "docker", "manifest", "inspect", image); err == nil {
		return nil
	}
	return fmt.Errorf("target image %s is neither present locally nor in a registry; build or pull it first", image)
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
	target := opts.Image
	if target == "" {
		if inv.UpstreamTag == "" {
			return fmt.Errorf("the deployment runs an unpinned or unrecognised image tag %q; set BLOODHOUND_TAG in .env or pass --image", inv.Image)
		}
		if opts.DriverVersion == "" {
			target = opts.ImageRepo + ":" + inv.UpstreamTag
		} else {
			target = opts.ImageRepo + ":" + inv.UpstreamTag + "-bt" + opts.DriverVersion
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
	if err := checkImageAvailable(ctx, deps.Runner, target); err != nil {
		return err
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
	rollbackHint := fmt.Sprintf("run `bloodtrail rollback` to restore the original driver and image (Neo4j data is intact); backup in %s", backupDir)

	if inv.ActiveDriver == "neo4j" {
		say("==> Migrating graph from Neo4j to PostgreSQL")
		client := toolapi.Client{BaseURL: toolAPIBaseURL, Transport: deps.NewToolAPITransport(inv.Config.DefaultNetworkName())}
		if err := client.MigrateNeoToPG(ctx, migrationPoll, opts.MigrationTimeout); err != nil {
			return fmt.Errorf("the migration reported an error: %w; BloodHound's migrator may already have switched the active driver to pg; %s", err, rollbackHint)
		}
		nodes, edges, err := store.CountGraph(ctx)
		if err != nil {
			return fmt.Errorf("counting migrated graph: %w; %s", err, rollbackHint)
		}
		if nodes == 0 {
			return fmt.Errorf("the migration finished with zero nodes in PostgreSQL; BloodHound's migrator may already have switched the active driver to pg; %s", rollbackHint)
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
	if _, err := deps.Runner.Run(ctx, nil, "docker", "compose", "--project-directory", opts.ProjectDir,
		"-f", opts.ComposeFile, "-f", overridePath, "up", "-d", "--remove-orphans"); err != nil {
		return fmt.Errorf("docker compose up: %w; %s", err, rollbackHint)
	}

	say("==> Verifying")
	if err := runVerification(ctx, deps, opts, c); err != nil {
		return fmt.Errorf("the image and driver switch completed, but verification failed: %w; run `bloodtrail rollback` to revert if needed", err)
	}
	return nil
}

func runVerification(ctx context.Context, deps Deps, opts Options, c dockerx.Compose) error {
	say := func(format string, a ...any) { _, _ = fmt.Fprintf(deps.Out, format+"\n", a...) }
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
	for attempt := 0; attempt < logMarkerAttempts; attempt++ {
		ok, err := verify.LogsContain(ctx, c, bloodhoundService, verify.DriverActiveMarker)
		if err != nil {
			return err
		}
		if ok {
			say("    driver log line present: %q", verify.DriverActiveMarker)
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
	c := dockerx.Compose{Runner: deps.Runner, File: m.ComposeFile, ProjectDir: m.ProjectDir}
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
	if err := c.Up(ctx); err != nil {
		return err
	}
	if err := verify.WaitForAPI(ctx, deps.HTTP, opts.APIURL, opts.VerifyTimeout); err != nil {
		return err
	}
	if err := os.Remove(manifest.Path(m.ProjectDir)); err != nil {
		return err
	}
	say("    rolled back; backups kept in %s", m.BackupDir)
	return nil
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
	_, _ = fmt.Fprintf(deps.Out, "project:       %s\nconfigured:    %s\nactive driver: %s\n", inv.Config.Name, inv.Image, inv.ActiveDriver)
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
