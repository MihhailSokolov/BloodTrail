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
	"slices"
	"strconv"
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
	DefaultImageRepo  = "ghcr.io/mihhailsokolov/bloodtrail"
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
	// Absolute like the compose file: the manifest records it, and a later
	// rollback run from another directory has to find the same project.
	if o.ProjectDir, err = filepath.Abs(o.ProjectDir); err != nil {
		return err
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

func (o Options) compose(runner dockerx.Runner) (dockerx.Compose, error) {
	return composeHandle(runner, o.ComposeFile, o.ProjectDir)
}

// composeHandle addresses the project as the operator configured it. Naming a
// file with -f makes docker compose ignore COMPOSE_FILE entirely AND switches
// off its own discovery of the conventional override file, so both have to be
// passed along or the project the installer reads and restarts is not the
// project the operator runs -- and `up -d` would then recreate services
// without the settings the operator keeps in those files.
//
// This is how status, verify and rollback read the project, and it is
// forgiving (projectFiles): those commands have to keep working on whatever
// an install -- this version or an earlier one -- left behind, so it fails
// only when the .env cannot be read with certainty at all. Install, which
// changes the deployment on the strength of its reading, goes by the strict
// one instead (installHandle).
func composeHandle(runner dockerx.Runner, composeFile, projectDir string) (dockerx.Compose, error) {
	return handle(runner, composeFile, projectDir, false)
}

// installHandle is composeHandle for install, which refuses whatever it
// cannot be sure matches the project compose loads for the operator.
func installHandle(runner dockerx.Runner, composeFile, projectDir string) (dockerx.Compose, error) {
	return handle(runner, composeFile, projectDir, true)
}

func handle(runner dockerx.Runner, composeFile, projectDir string, strict bool) (dockerx.Compose, error) {
	c := dockerx.Compose{Runner: runner, File: composeFile, ProjectDir: projectDir}
	files, err := projectFiles(composeFile, projectDir, strict)
	if err != nil {
		return c, err
	}
	// Set, not added one by one with WithExtraFile: a file listed twice is
	// merged twice by compose, and has to be here too.
	c.File, c.ExtraFiles = files[0], files[1:]
	return c, nil
}

// projectFiles resolves the files the project is made of, in the order
// compose merges them. COMPOSE_FILE wins when it is set, because compose then
// loads exactly what it lists, in that order, repeats included, and
// discovers nothing. With no such entry compose finds the project on its own
// (discoveredProject): the base file and, when there is one, the override
// beside it. A compose file discovery does not pick is one the operator runs
// with -f, which merges no override either.
//
// Strict, for install, it refuses anything it cannot be sure of: an entry
// compose itself cannot load (compose.ComposeFiles); one that does not list
// the compose file given; one listing a file that is not on disk -- compose
// fails to load the project over it, so the project without it is a guess,
// and `up -d` would recreate the operator's services without whatever that
// file held; and, with no entry, a compose file other than the one discovery
// picks, which is not what the operator's own commands run. Only the
// installer's own override may be missing: a leftover entry may still name
// it, and the install is about to write it.
//
// Otherwise it takes the project as best it can, for the commands that have
// to keep working on whatever an install -- this version or an earlier one --
// left behind: the listed files that are on disk, in their order, with the
// compose file put first where the entry leaves it out (an earlier install
// wrote its override alone into an empty entry, and compose's reading of
// that has none of the project's services); discovery for an entry that
// names no file, which is how earlier installs read one; and the compose file
// given where discovery would pick another. Skipping what is not on disk also
// keeps `bloodtrail rollback` working for the operator who deleted the
// override file but left its COMPOSE_FILE entry behind -- exactly what the
// override file's own header invites.
//
// A .env that exists but cannot be read, or whose COMPOSE_FILE entry cannot
// be read with certainty, is an error either way rather than "no entry":
// guessing there would address -- and later rewrite -- a different project
// from the one the operator runs.
func projectFiles(composeFile, projectDir string, strict bool) ([]string, error) {
	envPath := filepath.Join(projectDir, ".env")
	envData, err := os.ReadFile(envPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading %s: %w", envPath, err)
	}
	read := compose.ListedComposeFiles
	if strict {
		read = compose.ComposeFiles
	}
	listed, err := read(string(envData))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", envPath, err)
	}
	if strings.Join(listed, "") != "" {
		var files []string
		for _, f := range listed {
			if f == "" {
				continue
			}
			if !filepath.IsAbs(f) {
				f = filepath.Join(projectDir, f)
			}
			switch {
			case isFile(f):
				files = append(files, f)
			case strict && f != filepath.Join(projectDir, compose.OverrideFileName):
				return nil, fmt.Errorf("%s: COMPOSE_FILE lists %s, which does not exist, so docker compose fails to load the project; restore the file or take it out of the list", envPath, f)
			}
		}
		switch {
		case slices.Contains(files, composeFile):
			return files, nil
		case strict:
			return nil, fmt.Errorf("%s: COMPOSE_FILE does not list %s, the compose file given, so docker compose does not load it; pass --compose-file for the one the deployment runs", envPath, composeFile)
		}
		return append([]string{composeFile}, files...), nil
	}
	base, override := discoveredProject(projectDir)
	switch {
	case base == composeFile && override != "":
		return []string{composeFile, override}, nil
	case base != composeFile && base != "" && strict:
		return nil, fmt.Errorf("%s sets no COMPOSE_FILE, so docker compose loads %s there on its own, not %s; pass --compose-file for the file the deployment runs, or list its files in COMPOSE_FILE", envPath, base, composeFile)
	}
	return []string{composeFile}, nil
}

// checkComposeEnvironment refuses what the installer's own environment would
// change about how docker compose reads the project. Compose takes
// COMPOSE_FILE and COMPOSE_PATH_SEPARATOR from the shell over .env, so with
// COMPOSE_FILE set there (even empty) the operator's own `docker compose up
// -d` from this shell ignores the entry the install writes to .env -- and
// boots the upstream image against the `bloodtrail` driver setting -- while a
// separator other than ':' splits that entry into names that do not exist.
// COMPOSE_ENV_FILES (when not empty) and a true COMPOSE_DISABLE_ENV_FILE make
// compose leave the project's .env unread altogether, which has the same
// effect on the entry. The installer's own commands name every file with -f,
// which compose honours over all of them, so they would never show it.
//
// Compose reads COMPOSE_DISABLE_ENV_FILE with strconv.ParseBool and stops on a
// value that is not a boolean, empty included; so does every command the
// installer would run, and it says why here instead.
func checkComposeEnvironment() error {
	if _, ok := os.LookupEnv("COMPOSE_FILE"); ok {
		return errors.New("COMPOSE_FILE is set in this shell's environment, where docker compose takes it over the COMPOSE_FILE entry in .env: a plain `docker compose up -d` from here would not load the override this install adds there; unset it (moving the setting into .env if the project needs it) and rerun")
	}
	if sep, ok := os.LookupEnv("COMPOSE_PATH_SEPARATOR"); ok && sep != "" && sep != ":" {
		return fmt.Errorf("COMPOSE_PATH_SEPARATOR is set in this shell's environment to %q, so docker compose would split COMPOSE_FILE on it rather than on the ':' this installer writes; unset it and rerun", sep)
	}
	if files := os.Getenv("COMPOSE_ENV_FILES"); files != "" {
		return fmt.Errorf("COMPOSE_ENV_FILES is set in this shell's environment to %q, so docker compose reads those env files instead of the project's .env: a plain `docker compose up -d` from here would not see the COMPOSE_FILE entry this install adds there, and would boot the upstream image against the `bloodtrail` driver setting; unset it and rerun", files)
	}
	if v, ok := os.LookupEnv("COMPOSE_DISABLE_ENV_FILE"); ok {
		disabled, err := strconv.ParseBool(v)
		switch {
		case err != nil:
			return fmt.Errorf("COMPOSE_DISABLE_ENV_FILE is set in this shell's environment to %q, which docker compose does not accept as a boolean -- it stops with an error on it, and would on every command this installer runs; unset it (or set it to false) and rerun", v)
		case disabled:
			return fmt.Errorf("COMPOSE_DISABLE_ENV_FILE is set in this shell's environment to %q, so docker compose skips the project's .env: a plain `docker compose up -d` from here would not see the COMPOSE_FILE entry this install adds there, and would boot the upstream image against the `bloodtrail` driver setting; unset it (or set it to false) and rerun", v)
		}
	}
	return nil
}

// discoveredProject returns the files docker compose loads on its own in dir
// when it is given neither -f nor COMPOSE_FILE: the first of
// compose.DefaultFileNames that exists there as the base file, then the first
// of compose.DefaultOverrideFileNames beside it -- "" for either that is not
// there. (With no base file in dir compose goes on to look in the parent
// directories, but whatever it finds there is not this project either.)
func discoveredProject(dir string) (base, override string) {
	if base = firstExisting(dir, compose.DefaultFileNames()); base != "" {
		override = firstExisting(dir, compose.DefaultOverrideFileNames())
	}
	return base, override
}

func firstExisting(dir string, names []string) string {
	for _, name := range names {
		if p := filepath.Join(dir, name); fileExists(p) {
			return p
		}
	}
	return ""
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// isFile reports whether path is something compose can load as a file:
// there, and not a directory.
func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
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

// resolveImage confirms the target image exists. It never substitutes the
// moving alias for the same upstream release (alias) when a version-stamped
// target is missing: the alias is republished by the weekly image builds from
// main, so it can hold a driver no release ever shipped, and a released CLI
// that silently installed it would put an untested build on the operator's
// deployment under a version it does not match. The error names the alias
// instead, so taking it is an explicit --image choice.
func resolveImage(ctx context.Context, deps Deps, target, alias string) (string, error) {
	if imageExists(ctx, deps.Runner, target) {
		return target, nil
	}
	if alias != "" && alias != target {
		return "", fmt.Errorf("target image %s, built for this installer's version, is neither present locally nor in a registry; "+
			"its release may not be published for this upstream version yet. %s holds the newest driver build for this upstream "+
			"release, which may be unreleased; pass --image %s to install it anyway", target, alias, alias)
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
	if err := checkComposeEnvironment(); err != nil {
		return err
	}

	c, err := installHandle(deps.Runner, opts.ComposeFile, opts.ProjectDir)
	if err != nil {
		return err
	}
	say("==> Inventory")
	inv, store, err := takeInventory(ctx, c)
	if err != nil {
		return err
	}
	// The install checks the migrated graph against a count of the Neo4j one
	// (below), and stops if it cannot take that count. Finding out only then
	// meant a backup, a manifest and hours of migration had gone into a graph
	// that could not be verified, with the driver row already switched: so
	// what the migration is checked against has to be countable before
	// anything is asked or changed.
	if inv.ActiveDriver == "neo4j" && inv.CountErr != nil {
		return fmt.Errorf("counting the Neo4j graph: %w; the install checks the migrated graph against this count, so it needs cypher-shell and NEO4J_AUTH (as <user>/<password>) in the %s service; it stopped before changing anything", inv.CountErr, graphDBService)
	}
	// alias is the moving tag for this upstream release, named only in the
	// error for a derived target that is missing (resolveImage's doc): an
	// explicit --image is taken literally.
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
	// Read now whether the project already pins its file list: the install
	// is about to write a COMPOSE_FILE entry if it does not, and only
	// rollback's own record can tell it afterwards whether the line was
	// there before (compose.RemoveComposeFileLine). Nothing else writes
	// .env between here and that write, and an unreadable or absent file
	// means no entry, which the write below handles on its own.
	// ComposeFiles refuses an entry that lists no files, so an empty result
	// here means there is no line at all.
	envProbe, _ := os.ReadFile(filepath.Join(opts.ProjectDir, ".env"))
	probed, err := compose.ComposeFiles(string(envProbe))
	if err != nil {
		return fmt.Errorf(".env: %w", err)
	}
	envComposeFileCreated := len(probed) == 0
	// An entry the install writes, where there was none, replaces compose's
	// own file discovery, so it has to name what discovery found and the
	// installer has been addressing since: the base file and, when there is
	// one, the override beside it. Listing only the base file would drop that
	// override out of the operator's own `docker compose` commands from here
	// on. The manifest records the list, so rollback can tell whether the
	// line still names just that.
	var baseFiles, envComposeFileWritten []string
	for _, f := range append([]string{c.File}, c.ExtraFiles...) {
		rel, _ := filepath.Rel(opts.ProjectDir, f)
		baseFiles = append(baseFiles, rel)
	}
	if envComposeFileCreated {
		envComposeFileWritten = append(append([]string(nil), baseFiles...), compose.OverrideFileName)
	}
	m := manifest.Manifest{
		InstallerVersion: deps.InstallerVersion, InstalledAt: opts.Now().UTC().Format(time.RFC3339),
		ComposeFile: opts.ComposeFile, ProjectDir: opts.ProjectDir, ProjectName: inv.Config.Name,
		OriginalImage: inv.Image, OriginalDriverRow: inv.DriverRow, BackupDir: backupDir,
		OverrideFile: overridePath, TargetImage: target, UpstreamTag: inv.UpstreamTag,
		PGUser: inv.PGUser, PGDatabase: inv.PGDB,
		EnvComposeFileCreated: envComposeFileCreated, EnvComposeFileWritten: envComposeFileWritten,
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
		// The bloodhound service is not recreated by a failed install followed
		// by a rollback, so its log can already carry a failure line from an
		// earlier attempt; remember how much log there was before this
		// migration so only what it appends gets scanned for one.
		preLogs, err := c.Logs(ctx, bloodhoundService)
		if err != nil {
			return fmt.Errorf("reading %s logs before the migration: %w; %s", bloodhoundService, err, rollbackHint)
		}
		// The migrator runs inside the bloodhound server, and its state
		// machine resets to "idle" if that container restarts -- which
		// WaitUntilIdle cannot tell apart from completion. Capture the
		// container's identity (id + start time) before starting, so a
		// restart anywhere inside the migration window is detected instead
		// of read as success.
		epochBefore, err := bloodhoundContainerEpoch(ctx, deps, c)
		if err != nil {
			return fmt.Errorf("reading the %s container's identity before the migration: %w; %s", bloodhoundService, err, rollbackHint)
		}
		client := toolapi.Client{BaseURL: toolAPIBaseURL, Transport: deps.NewToolAPITransport(inv.Config.DefaultNetworkName())}
		if err := client.MigrateNeoToPG(ctx, migrationPoll, opts.MigrationTimeout); err != nil {
			return fmt.Errorf("the migration reported an error: %w; BloodHound's migrator may already have switched the active driver to pg; %s", err, rollbackHint)
		}
		epochAfter, err := bloodhoundContainerEpoch(ctx, deps, c)
		if err != nil {
			return fmt.Errorf("reading the %s container's identity after the migration: %w; %s", bloodhoundService, err, rollbackHint)
		}
		if epochAfter != epochBefore {
			return fmt.Errorf("the %s container restarted during the migration; its migrator resets to idle on boot, so the reported completion cannot be trusted; %s", bloodhoundService, rollbackHint)
		}
		// Count Neo4j AFTER the migration finished, not before: BloodHound
		// keeps ingesting into Neo4j right up to the driver switch, so only
		// a post-migration count also covers whatever landed mid-migration
		// (which the migrator may not have carried over). And the count is
		// required, not best-effort: without it a partial migration has no
		// backstop at all -- a failed recount used to fall through with any
		// nonzero node count and silently bless whatever arrived.
		freshNodes, freshEdges, freshErr := neo4jCounts(ctx, c, inv.Config)
		if freshErr != nil {
			return fmt.Errorf("counting the Neo4j graph after the migration: %w; the migrated graph cannot be verified against its source (cypher-shell and NEO4J_AUTH must be available in the graph-db service); %s", freshErr, rollbackHint)
		}
		nodes, edges, err := store.CountGraph(ctx)
		if err != nil {
			return fmt.Errorf("counting migrated graph: %w; %s", err, rollbackHint)
		}
		// The migrator reports per-object failures to the server log only and
		// still returns to idle, so a run that imported the nodes but dropped
		// every edge looks identical to a clean one from the API. Check what
		// arrived against what Neo4j holds now, then read the log.
		if nodes < freshNodes || edges < freshEdges {
			return fmt.Errorf("the migration moved %d nodes and %d edges into PostgreSQL but Neo4j now holds %d nodes and %d edges "+
				"(data ingested during the migration is not carried over -- pause ingestion for the install window); "+
				"BloodHound's migrator reports failures to its log only; %s", nodes, edges, freshNodes, freshEdges, rollbackHint)
		}
		if line, err := migratorFailureInLogs(ctx, c, len(preLogs)); err != nil {
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
	newEnv, err := compose.AddComposeFile(string(envData), baseFiles, compose.OverrideFileName)
	if err != nil {
		return fmt.Errorf(".env: %w; %s", err, rollbackHint)
	}
	if err := os.WriteFile(envPath, []byte(newEnv), 0o644); err != nil {
		return fmt.Errorf("writing .env: %w; %s", err, rollbackHint)
	}
	if err := store.Set(ctx, driverName); err != nil {
		return fmt.Errorf("setting database_switch: %w; %s", err, rollbackHint)
	}
	// BloodTrail's first boot may adopt a snapshot file an earlier install
	// saved, and every writer the graph has had since -- the stock image
	// after a rollback, this install's own migration -- wrote it without the
	// watermark counter that file's proof rests on. Ending the lineage now,
	// with no BloodTrail process running and before the one `up` starts has
	// loaded anything, leaves every such file naming a lineage PostgreSQL
	// has left. The stock image may keep writing until `up` replaces it;
	// that is harmless, since it stops before BloodTrail loads.
	if err := store.EndWatermarkLineage(ctx); err != nil {
		return fmt.Errorf("ending the snapshot file watermark lineage: %w; %s", err, rollbackHint)
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
// failure among the bytes the bloodhound service's log gained since preLen,
// or "" when there is none. The bloodhound container is not recreated by a
// failed install followed by a rollback, so its log can carry a failure line
// from an earlier migration attempt; scanning only what was appended keeps
// that stale line from blocking every later install. A log shorter than
// preLen means the container was recreated after all (nothing here
// guarantees it will not be), so the whole thing is scanned in that case.
func migratorFailureInLogs(ctx context.Context, c dockerx.Compose, preLen int) (string, error) {
	logs, err := c.Logs(ctx, bloodhoundService)
	if err != nil {
		return "", err
	}
	if preLen > len(logs) {
		preLen = 0
	}
	for _, line := range strings.Split(string(logs[preLen:]), "\n") {
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
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no BloodTrail installation found in %s: %w", opts.ProjectDir, err)
	} else if err != nil {
		return fmt.Errorf("the install manifest %s cannot be used: %w", manifest.Path(opts.ProjectDir), err)
	}

	// Rollback changes the deployment exactly as much as install does -- the
	// driver row, the compose files, a restart onto another image -- so it
	// asks the same way, and --yes skips the question the same way.
	say("    project:        %s (%s)", m.ProjectName, m.ProjectDir)
	say("    restore image:  %s", m.OriginalImage)
	if m.OriginalDriverRow != nil {
		say("    restore driver: %s", *m.OriginalDriverRow)
	} else {
		say("    restore driver: none (the row is deleted)")
	}
	if !opts.Yes && !deps.Confirm("Proceed with the rollback?") {
		return errors.New("aborted by user")
	}

	c, err := composeHandle(deps.Runner, m.ComposeFile, m.ProjectDir)
	if err != nil {
		return err
	}
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
		restoredEnv, note, err := restoreComposeFileEntry(string(envData), m)
		if err != nil {
			return fmt.Errorf(".env: %w", err)
		}
		if note != "" {
			say("    %s", note)
		}
		if err := os.WriteFile(envPath, []byte(restoredEnv), 0o644); err != nil {
			return err
		}
	}

	say("==> Restarting with the original image %s", m.OriginalImage)
	// Everything that makes the deployment BloodTrail is undone by this point,
	// so a failure from here on is about the deployment coming back up, not
	// about the rollback being incomplete. Say so: the manifest is still
	// there, and rerunning rollback picks up where this left off.
	restored := "the driver row and the compose files are already restored; rerun `bloodtrail rollback` once the deployment can start"
	restarted, err := composeHandle(deps.Runner, m.ComposeFile, m.ProjectDir)
	if err != nil {
		return fmt.Errorf("%w (%s)", err, restored)
	}
	// The stock image owns the graph from here on and writes it without the
	// watermark counter. BloodTrail has stopped -- its last snapshot file
	// save is behind it -- so ending the lineage now leaves any file it
	// saved unadoptable by whatever brings BloodTrail back: a reinstall ends
	// the lineage again anyway, but a BloodTrail started any other way would
	// otherwise find the counter exactly where the file left it. Addressed
	// through the restored project, since the override file is gone.
	restoredStore := dbswitch.Store{Compose: restarted, Service: appDBService, User: m.PGUser, Database: m.PGDatabase}
	if err := restarted.Up(ctx); err != nil {
		// A failed up can still have started the stock image -- compose
		// reports the first service that failed, not the ones it already
		// started -- and nothing says the operator reruns rollback before
		// it writes. Ending the lineage costs only a rebuild if it did not.
		if lineageErr := restoredStore.EndWatermarkLineage(ctx); lineageErr != nil {
			return fmt.Errorf("restarting with the original image: %w (%s; ending the snapshot file watermark lineage failed too: %v)", err, restored, lineageErr)
		}
		return fmt.Errorf("restarting with the original image: %w (%s)", err, restored)
	}
	if err := restoredStore.EndWatermarkLineage(ctx); err != nil {
		return fmt.Errorf("ending the snapshot file watermark lineage: %w (the original image is running again; rerun `bloodtrail rollback` to finish)", err)
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

// restoreComposeFileEntry undoes what the install m records did to the .env
// contents' COMPOSE_FILE entry, and says what the operator should know about
// the result, if anything.
//
// An entry the install created goes away entirely: leaving a line behind
// would keep compose's file discovery off, so the operator's conventional
// override file would stay unloaded by their own commands even after the
// rollback. That holds while the line names just what the install wrote,
// though: a file the operator has added to it since would drop out of their
// project with it, so then only the installer's own override comes out.
//
// Installs from before the written list was recorded also took an empty
// entry for none, recorded it as created and wrote their override into it.
// An entry naming nothing besides that override is one of those: it gets
// its empty entry back, spelled as the operator wrote it -- or keeps it, when
// the install stopped before its write -- rather than losing the line.
func restoreComposeFileEntry(env string, m manifest.Manifest) (restored, note string, err error) {
	listed, err := compose.ListedComposeFiles(env)
	if err != nil || listed == nil {
		return env, "", err
	}
	others := withoutOverride(listed)
	switch {
	case !m.EnvComposeFileCreated:
		restored, err = compose.RemoveComposeFile(env, compose.OverrideFileName)
	case m.EnvComposeFileWritten == nil && strings.Join(others, "") == "":
		if restored, err = compose.RestoreEmptyComposeFile(env, compose.OverrideFileName); err == nil && restored != env {
			note = "put back the empty COMPOSE_FILE entry .env had before the install; docker compose does not read that as unset but fails to load the project, so delete the line to let compose find its files on its own, or list them"
		}
	case m.EnvComposeFileWritten != nil && !slices.Equal(others, withoutOverride(m.EnvComposeFileWritten)):
		if restored, err = compose.RemoveComposeFile(env, compose.OverrideFileName); err == nil && restored != env {
			note = fmt.Sprintf("COMPOSE_FILE in .env has changed since the install, so only %s came out of it", compose.OverrideFileName)
		}
	default:
		restored, err = compose.RemoveComposeFileLine(env)
	}
	return restored, note, err
}

// withoutOverride returns files without the installer's own override.
func withoutOverride(files []string) []string {
	var out []string
	for _, f := range files {
		if f != compose.OverrideFileName {
			out = append(out, f)
		}
	}
	return out
}

// bloodhoundContainerEpoch identifies the current incarnation of the
// bloodhound service's container: its full container id plus its
// docker-reported start time. A plain restart keeps the id and changes
// StartedAt; a recreate changes the id; either one invalidates anything
// observed across the migration window (the migrator's own state lives in
// that process and resets to idle on boot).
func bloodhoundContainerEpoch(ctx context.Context, deps Deps, c dockerx.Compose) (string, error) {
	out, err := c.PS(ctx, bloodhoundService)
	if err != nil {
		return "", fmt.Errorf("docker compose ps %s: %w", bloodhoundService, err)
	}
	// Same dual-shape parse as runningImage below: compose v2 prints a JSON
	// array in recent versions and one object per line in older ones.
	dec := json.NewDecoder(bytes.NewReader(out))
	if tok, err := dec.Token(); err != nil {
		return "", fmt.Errorf("parsing docker compose ps %s output: %w", bloodhoundService, err)
	} else if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		dec = json.NewDecoder(bytes.NewReader(out))
	}
	var container struct {
		ID string `json:"ID"`
	}
	if err := dec.Decode(&container); err != nil || container.ID == "" {
		return "", fmt.Errorf("the %s service has no running container", bloodhoundService)
	}
	started, err := deps.Runner.Run(ctx, nil, "docker", "inspect", "-f", "{{.Id}} {{.State.StartedAt}}", container.ID)
	if err != nil {
		return "", fmt.Errorf("docker inspect %s: %w", container.ID, err)
	}
	return strings.TrimSpace(string(started)), nil
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
	c, err := opts.compose(deps.Runner)
	if err != nil {
		return err
	}
	inv, _, err := takeInventory(ctx, c)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(deps.Out, "project:       %s\nconfigured:    %s\nrunning:       %s\nactive driver: %s\n",
		inv.Config.Name, inv.Image, runningImage(ctx, c, bloodhoundService), inv.ActiveDriver)
	// Only a missing manifest means "not installed". One that is there but
	// unreadable or corrupt belongs to an installation rollback cannot undo
	// until it is repaired, and reporting it as "not installed" would invite
	// a second install over the first.
	m, err := manifest.Load(opts.ProjectDir)
	switch {
	case err == nil:
		_, _ = fmt.Fprintf(deps.Out, "bloodtrail:    installed %s, image %s, backups %s\n", m.InstalledAt, m.TargetImage, m.BackupDir)
	case errors.Is(err, os.ErrNotExist):
		_, _ = fmt.Fprintln(deps.Out, "bloodtrail:    not installed")
	default:
		_, _ = fmt.Fprintln(deps.Out, "bloodtrail:    install manifest present but unreadable")
		return fmt.Errorf("the install manifest %s cannot be used: %w", manifest.Path(opts.ProjectDir), err)
	}
	return nil
}

// Verify runs the post-install checks on their own.
func Verify(ctx context.Context, deps Deps, opts Options) error {
	if err := opts.defaults(); err != nil {
		return err
	}
	deps.defaults()
	c, err := opts.compose(deps.Runner)
	if err != nil {
		return err
	}
	return runVerification(ctx, deps, opts, c)
}
