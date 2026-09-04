// SPDX-License-Identifier: Apache-2.0

// Command bloodtrail installs the BloodTrail driver into an existing
// BloodHound CE compose deployment, and can verify, report on, or roll it back.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/installer"
)

var version = "dev" // set with -ldflags "-X main.version=…"

const usage = `bloodtrail %s

Usage:
  bloodtrail install  [flags]   upgrade a BloodHound CE compose deployment to BloodTrail
  bloodtrail status   [flags]   show what is installed
  bloodtrail verify   [flags]   run the post-install checks
  bloodtrail rollback [flags]   restore the original image and driver

Run "bloodtrail <command> -h" for the flags of a command.
`

// errUsage signals a flag-parsing failure that the flag package has already
// reported on stderr; main exits 2 without printing it a second time.
var errUsage = errors.New("usage error")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, version)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "install":
		err = run(ctx, os.Args[2:], installer.Install)
	case "status":
		err = run(ctx, os.Args[2:], installer.Status)
	case "verify":
		err = run(ctx, os.Args[2:], installer.Verify)
	case "rollback":
		err = run(ctx, os.Args[2:], installer.Rollback)
	case "-h", "--help", "help":
		fmt.Printf(usage, version)
		return
	case "version", "--version":
		fmt.Println(version)
		return
	default:
		fmt.Fprintf(os.Stderr, usage, version)
		os.Exit(2)
	}

	if err != nil {
		if errors.Is(err, errUsage) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, cmd func(context.Context, installer.Deps, installer.Options) error) error {
	fs := flag.NewFlagSet("bloodtrail", flag.ContinueOnError)
	var opts installer.Options
	fs.StringVar(&opts.ComposeFile, "compose-file", "docker-compose.yml", "path to the BloodHound compose file")
	fs.StringVar(&opts.ProjectDir, "project-dir", "", "compose project directory (default: directory of the compose file)")
	fs.StringVar(&opts.Image, "image", "", "BloodTrail image to install (default: derived from the running upstream tag)")
	fs.StringVar(&opts.ImageRepo, "image-repo", installer.DefaultImageRepo, "image repository used to derive the default image")
	fs.StringVar(&opts.APIURL, "api-url", installer.DefaultAPIURL, "BloodHound API base URL as reachable from this machine")
	fs.StringVar(&opts.AdminUser, "admin-user", installer.DefaultAdminUser, "admin user for the authenticated smoke test")
	fs.StringVar(&opts.AdminPassword, "admin-password", "", "admin password; enables the ingest-and-search smoke test (or set BLOODTRAIL_ADMIN_PASSWORD)")
	fs.BoolVar(&opts.Yes, "yes", false, "do not ask for confirmation")
	fs.BoolVar(&opts.ReplacePostgresGraph, "replace-postgres-graph", false,
		"clear a graph an earlier migration left in PostgreSQL instead of refusing to migrate on top of it")
	fs.DurationVar(&opts.MigrationTimeout, "migration-timeout", 6*time.Hour, "how long to wait for the Neo4j to PostgreSQL migration")
	fs.DurationVar(&opts.VerifyTimeout, "verify-timeout", 20*time.Minute, "how long to wait for the API and the smoke test")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errUsage
	}
	if opts.AdminPassword == "" {
		opts.AdminPassword = os.Getenv("BLOODTRAIL_ADMIN_PASSWORD")
	}
	opts.DriverVersion = strings.TrimPrefix(version, "v")
	if opts.DriverVersion == "dev" {
		opts.DriverVersion = ""
	}
	deps := installer.Deps{
		Out:              os.Stdout,
		InstallerVersion: version,
		Confirm:          confirm,
	}
	return cmd(ctx, deps, opts)
}

// confirm asks the question on stderr and reads the answer from the terminal.
// The usual way to run this is `curl … | sh -s -- install`, where stdin is the
// pipe the shell is reading the script from: reading the answer from there
// consumes the script and never sees the operator. /dev/tty is the terminal
// whatever stdin happens to be, and stderr keeps the prompt out of anything
// that pipes the installer's output. Both fall back when they are not there,
// so a truly non-interactive run still gets a "no" rather than a hang; --yes
// is the way to say yes without a terminal.
func confirm(prompt string) bool {
	in := io.Reader(os.Stdin)
	if tty, err := os.Open("/dev/tty"); err == nil {
		defer func() { _ = tty.Close() }()
		in = tty
	}
	return confirmFrom(in, os.Stderr, prompt)
}

func confirmFrom(in io.Reader, out io.Writer, prompt string) bool {
	_, _ = fmt.Fprintf(out, "%s [y/N] ", prompt)
	var answer string
	if _, err := fmt.Fscanln(in, &answer); err != nil {
		return false
	}
	return strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes")
}
