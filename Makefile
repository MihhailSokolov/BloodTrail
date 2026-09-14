.PHONY: test lint integration build-image tidy bench-path bench-builder bench-cypher bench-apply

test:
	go test ./...

lint:
	golangci-lint run ./...

# Requires a PostgreSQL reachable at BLOODTRAIL_TEST_PG, see docker-compose.test.yml
# Integration packages share one database, so packages must not run in parallel.
integration:
	go test -tags integration -count=1 -p 1 ./...

tidy:
	go mod tidy

# Requires a PostgreSQL reachable at BLOODTRAIL_TEST_PG, already loaded with a
# graph (see bench/adgen). -enforce fails the build if the path engine misses
# its latency targets; never pass -enforce in CI (see bench/pathbench).
bench-path: ## run the path-engine benchmark against BLOODTRAIL_TEST_PG
	go run ./bench/pathbench -dsn "$(BLOODTRAIL_TEST_PG)" -enforce

# Requires a PostgreSQL reachable at BLOODTRAIL_TEST_PG, already loaded with a
# graph (see bench/adgen). -enforce fails the build if builder-query serving
# isn't at least 5x faster than delegating to PostgreSQL; an engine-side (bt)
# call that runs past -bt-cap (15m default) ABORTS the whole run (nonzero
# exit, never a data point); never pass -enforce in CI (see bench/builderbench).
bench-builder: ## run the builder-query benchmark against BLOODTRAIL_TEST_PG
	go run ./bench/builderbench -dsn "$(BLOODTRAIL_TEST_PG)" -enforce

# Requires a PostgreSQL reachable at BLOODTRAIL_TEST_PG, already loaded with a
# graph (see bench/adgen). -enforce fails the build if Cypher serving isn't
# at least 5x faster than delegating to PostgreSQL (1.5x for rid_suffix_scan,
# 1x for the objectid point lookup); a pg baseline that runs past -pg-cap
# (120s default) is capped and judged on the engine's own absolute p50
# instead; an engine-side (bt) call that runs past -bt-cap (15m default)
# ABORTS the whole run (nonzero exit, never a data point) -- it almost
# certainly means the engine declined and silently delegated to an unbounded
# PostgreSQL query; never pass -enforce in CI (see bench/cypherbench).
bench-cypher: ## run the cypher-interpreter benchmark against BLOODTRAIL_TEST_PG
	go run ./bench/cypherbench -dsn "$(BLOODTRAIL_TEST_PG)" -enforce

# Generates a fresh graph via bench/adgen (ARGS forwarded to adgen -- e.g.
# ARGS='-users 50000') and then runs the write-through apply benchmark
# (sustained apply throughput, query latency during active ingest,
# compaction duration, snapshot file save/load/boot duration) against it.
# NOTE: -wipe is passed unconditionally, so this target TRUNCATES the
# node/edge tables of EVERY graph in BLOODTRAIL_TEST_PG -- with no ARGS at
# all too, which then regenerates at adgen's own default size. Point it only
# at a disposable test database.
# -enforce fails the build if apply overhead or during-ingest p95 miss their
# measured, evidence-derived bars (see bench/applybench/README.md); every phase is bounded
# by applybench's own -cap watchdog (default 10m per operation), which
# ABORTS THE WHOLE RUN on a trip rather than ever waiting unbounded; never
# pass -enforce in CI (see bench/applybench).
bench-apply: ## generate a graph then run the write-through apply benchmark against BLOODTRAIL_TEST_PG
	go run ./bench/adgen -dsn "$(BLOODTRAIL_TEST_PG)" -wipe $(ARGS)
	go run ./bench/applybench -dsn "$(BLOODTRAIL_TEST_PG)" -enforce

# Usage: make build-image UPSTREAM_TAG=v9.6.0
UPSTREAM_TAG ?= v9.6.0
build-image:
	./build/build-image.sh $(UPSTREAM_TAG)
