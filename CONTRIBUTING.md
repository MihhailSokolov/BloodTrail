# Contributing

Thanks for looking at BloodTrail. It is a small, correctness-obsessed codebase;
the notes below are what keep it that way.

## Development setup

- Go: the version pinned in [go.mod](go.mod).
- The integration suite needs a disposable PostgreSQL:

      docker compose -f docker-compose.test.yml up -d
      export BLOODTRAIL_TEST_PG='postgresql://bloodtrail:bloodtrail@127.0.0.1:55432/bloodtrail'

  The suite wipes and reseeds that database freely.

## Before opening a pull request

CI runs exactly these; green locally means green there:

    make test          # unit suite (add -race locally when touching concurrency)
    make lint          # golangci-lint, the pinned CI version
    make integration   # the full -tags integration suite against BLOODTRAIL_TEST_PG

## Ground rules the codebase follows

- **Decline over guess.** The engine serves a query only when its behavior is
  pinned to what the PostgreSQL driver would return; anything uncertain
  delegates. A change that widens serving needs differential evidence against
  the live pg oracle (see the `*_integration_test.go` differential suites) --
  not an argument.
- **Every behavior change lands with a test that fails without it.**
- **Measured numbers, not invented ones.** Enforced thresholds in the benches
  are derived as measured-worst × ~1.75 and pinned by a `TestMeasured*` test;
  don't move a bar without a fresh measurement (each bench README records the
  derivation history).
- New Go files start with `// SPDX-License-Identifier: Apache-2.0` (files
  derived from upstream carry upstream's full header instead).
- No local paths, usernames, or machine-specific details in code, comments, or
  committed output.

## Security issues

See [SECURITY.md](SECURITY.md) -- please do not open public issues for
vulnerabilities.
