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

Run these locally:

    make test          # unit suite
    make lint          # golangci-lint, integration-tagged code included; refuses any
                       # version but the one CI pins
    make integration   # the full -tags integration suite against BLOODTRAIL_TEST_PG
    make test-bench-scripts test-build-scripts  # when touching bench/ or build/ scripts

CI (`.github/workflows/`) runs more than that, so green locally is necessary but not
sufficient:

- `ci.yml` runs the unit and integration suites with `-race` (`go test -race ./...`, and
  `go test -race -tags integration -count=1 -p 1 ./...`) -- add `-race` locally when
  touching concurrency; golangci-lint with `--build-tags integration`, so the
  integration-tagged code is linted too; `go vet` and `go test` in `bench/csrbench`, a
  separate module the `./...` patterns above never reach; the benchmark and build
  scripts' own tests (`make test-bench-scripts`, `make test-build-scripts`); a
  patch-guard job that checks `patches/bloodhound-driver.patch` still applies
  (`git apply --check`) to every supported upstream tag (`build/upstream-tags.sh`: each
  stable BloodHound CE release from v9.6.0 on, read from upstream's release list -- so a
  new upstream release can turn this job red on a change that did not touch the patch;
  that is the signal to update the patch, not noise); and a `dawgs` job
  (`build/dawgs-suites.sh`) that resolves the dawgs version each supported release's
  image would ship and runs the unit and integration suites against every one other
  than `go.mod`'s, failing for a version that is neither `go.mod`'s nor listed as tested
  in `build/build-image.sh`.
- `e2e.yml` builds the patched image and runs `build/e2e.sh v9.6.0`, the full
  install/verify/rollback cycle against a live compose stack (needs Docker; see
  [build/README.md](build/README.md#end-to-end-test)).
- Each workflow ends in an aggregate job, `ci-ok` and `e2e-ok`, that fails unless every
  job before it succeeded; those two are the checks to require before merging (see
  [build/README.md](build/README.md#merge-gate)). A job added to a workflow belongs in
  its gate's `needs`.

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
