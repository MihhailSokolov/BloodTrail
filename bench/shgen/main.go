// SPDX-License-Identifier: Apache-2.0

// shgen generates a fictitious corporate Active Directory forest as
// SharpHound v6 collection JSON, sized by flags, for ingesting through
// BloodHound's ordinary file-upload pipeline -- the way to benchmark a
// deployment end to end (ingest, analysis, queries) with and without the
// BloodTrail driver on data BloodHound treats exactly like a real
// SharpHound collection. See README.md for the seeded attack paths and the
// measurement recipe.
package main

import (
	"archive/zip"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func main() {
	var (
		out       = flag.String("out", "shgen-out", "directory to write the collection JSON files into (created if missing)")
		users     = flag.Int("users", 100000, "total User principals across all domains (Administrator and krbtgt come on top, per domain)")
		computers = flag.Int("computers", 0, "total Computer principals; 0 means users/2")
		groups    = flag.Int("groups", 0, "total non-well-known Group principals; 0 means users/20 (minimum 4 per domain for the seeded attack paths)")
		domains   = flag.Int("domains", 0, "number of domains in the forest; 0 means 1 per 250k users (minimum 1)")
		seed      = flag.Int64("seed", 1, "deterministic seed; the same seed and counts always produce identical output")
		chunk     = flag.Int("chunk", 50000, "maximum objects per output file (BloodHound accepts any number of files per upload)")
		zipPath   = flag.String("zip", "", "also bundle every generated file into this zip (handy for UI drag-and-drop upload)")
		domain    = flag.String("domain", "MEGACORP.LOCAL", "forest root domain name (children become DIVNN.<root>)")

		azUsers    = flag.Int("az-users", 0, "AZUser principals in the Entra tenant; 0 disables the whole azure side")
		azGroups   = flag.Int("az-groups", -1, "AZGroup security groups; -1 means az-users/20")
		azApps     = flag.Int("az-apps", -1, "AZApp registrations, each with a service principal; -1 means az-users/100 (minimum covers the seeded paths)")
		azDevices  = flag.Int("az-devices", -1, "AZDevice entries; -1 means az-users/3")
		azVMs      = flag.Int("az-vms", -1, "AZVM virtual machines; -1 means 200")
		azKeyVault = flag.Int("az-keyvaults", -1, "AZKeyVault vaults; -1 means 50")
		azSubs     = flag.Int("az-subs", -1, "AZSubscription subscriptions; -1 means 2")
		azSyncPct  = flag.Int("az-sync-pct", 60, "percent of AZUsers hybrid-synced to an AD user")
	)
	flag.Parse()

	cfg := config{Domain: *domain, Users: *users, Computers: *computers, Groups: *groups, Domains: *domains, Seed: *seed,
		AZUsers: *azUsers, AZGroups: *azGroups, AZApps: *azApps, AZDevices: *azDevices,
		AZVMs: *azVMs, AZKeyVault: *azKeyVault, AZSubs: *azSubs, AZSyncPct: *azSyncPct}
	cfg.applyAzureDefaults()
	if cfg.Computers == 0 {
		cfg.Computers = cfg.Users / 2
	}
	if cfg.Groups == 0 {
		cfg.Groups = cfg.Users / 20
	}
	if cfg.Domains == 0 {
		cfg.Domains = cfg.Users/250000 + 1
	}
	if cfg.Users < 3*cfg.Domains {
		fmt.Fprintf(os.Stderr, "warning: fewer than 3 users per domain; some seeded attack paths need reserved users 0-2 and will be partially absent\n")
	}
	if cfg.Computers < attackComputerReserve*cfg.Domains {
		fmt.Fprintf(os.Stderr, "warning: fewer than %d computers per domain; the session/delegation attack paths will be partially absent\n", attackComputerReserve)
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatal("creating %s: %v", *out, err)
	}

	start := time.Now()
	sum, err := generate(cfg, *out, *chunk)
	if err != nil {
		fatal("generate: %v", err)
	}

	fmt.Printf("generated %s in %s (%d files in %s)\n", cfg.Domain, time.Since(start).Round(time.Millisecond), len(sum.Files), *out)
	fmt.Printf("  domains: %d   users: %d   computers: %d   groups: %d   ous: %d   gpos: %d   containers: %d\n",
		sum.Domains, sum.Users, sum.Computers, sum.Groups, sum.OUs, sum.GPOs, sum.Containers)
	if sum.AZObjects > 0 {
		fmt.Printf("  azure: %d items (tenant, %d users, %d groups, %d apps+SPs, %d devices, %d VMs, %d vaults, %d subscriptions)\n",
			sum.AZObjects, cfg.AZUsers, cfg.AZGroups, cfg.AZApps, cfg.AZDevices, cfg.AZVMs, cfg.AZKeyVault, cfg.AZSubs)
		fmt.Printf("  seeded azure paths: hybrid sync into a Global Admin; PIM eligibility; app owner -> MS Graph role grant;\n")
		fmt.Printf("  role-assignable group -> Privileged Role Admin; key-vault reader; VM admin login; Intune -> devices\n")
	}
	fmt.Printf("  seeded attack paths per domain: nested-membership chain to DOMAIN ADMINS; GenericAll ACL chain via APP OWNERS;\n")
	fmt.Printf("  kerberoastable SVC-SQL* with AdminTo over a host holding a DA session; AS-REP roastables; unconstrained delegation\n")

	if *zipPath != "" {
		if err := writeZip(*zipPath, sum.Files); err != nil {
			fatal("writing %s: %v", *zipPath, err)
		}
		fmt.Printf("  bundled into %s\n", *zipPath)
	}
}

func writeZip(path string, files []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	zw := zip.NewWriter(f)
	for _, src := range files {
		in, err := os.Open(src)
		if err != nil {
			return err
		}
		w, err := zw.Create(filepath.Base(src))
		if err == nil {
			_, err = io.Copy(w, in)
		}
		_ = in.Close()
		if err != nil {
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return f.Close()
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "shgen: "+format+"\n", a...)
	os.Exit(1)
}
