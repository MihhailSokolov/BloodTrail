// SPDX-License-Identifier: Apache-2.0

// Command bloodtrail installs the BloodTrail driver into an existing
// BloodHound CE compose deployment, and can verify, report on, or roll it back.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	_ = ctx // will be used by Task 14

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, usage, version)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "install", "status", "verify", "rollback":
		err = fmt.Errorf("%s: not implemented yet", os.Args[1]) //nolint:staticcheck
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

	if err != nil { //nolint:staticcheck
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
