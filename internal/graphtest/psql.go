// SPDX-License-Identifier: Apache-2.0

//go:build integration

package graphtest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PSQLRunner stands in for the docker CLI in tests of the installer's own
// database statements (internal/dbswitch): it answers the `docker compose
// ... exec ... psql ... -tAc <sql>` command a dbswitch.Store issues by
// running <sql> against the test database instead of inside a container.
// The string goes over the simple query protocol, as psql -c sends it, so a
// string of several statements runs as one transaction exactly as it would
// in production, and rows come back the way -tA prints them: fields joined
// by "|", one row per line.
//
// It runs nothing else: any other command is an error, so a test cannot
// quietly depend on a docker command it never actually ran.
type PSQLRunner struct {
	Pool *pgxpool.Pool
}

// Run implements the installer's dockerx.Runner.
func (r PSQLRunner) Run(ctx context.Context, _ io.Reader, name string, args ...string) ([]byte, error) {
	sql, ok := psqlCommand(args)
	if name != "docker" || !ok {
		return nil, fmt.Errorf("graphtest: PSQLRunner runs psql -c commands only, got %s %s", name, strings.Join(args, " "))
	}

	conn, err := r.Pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("graphtest: PSQLRunner: acquire: %w", err)
	}
	defer conn.Release()

	results, err := conn.Conn().PgConn().Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	for _, result := range results {
		for _, row := range result.Rows {
			for i, field := range row {
				if i > 0 {
					out.WriteByte('|')
				}
				out.Write(field)
			}
			out.WriteByte('\n')
		}
	}
	return out.Bytes(), nil
}

// psqlCommand finds the SQL a psql invocation inside args carries: the
// argument after its first short option cluster ending in "c" (dbswitch
// passes "-tAc").
func psqlCommand(args []string) (string, bool) {
	for i, arg := range args {
		if arg != "psql" {
			continue
		}
		for j := i + 1; j+1 < len(args); j++ {
			if opt := args[j]; strings.HasPrefix(opt, "-") && !strings.HasPrefix(opt, "--") && strings.HasSuffix(opt, "c") {
				return args[j+1], true
			}
		}
	}
	return "", false
}
