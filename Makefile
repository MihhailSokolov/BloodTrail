.PHONY: test lint integration build-image tidy bench-path bench-builder bench-cypher

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
# isn't at least 5x faster than delegating to PostgreSQL; never pass -enforce
# in CI (see bench/builderbench).
bench-builder: ## run the builder-query benchmark against BLOODTRAIL_TEST_PG
	go run ./bench/builderbench -dsn "$(BLOODTRAIL_TEST_PG)" -enforce

# Requires a PostgreSQL reachable at BLOODTRAIL_TEST_PG, already loaded with a
# graph (see bench/adgen). -enforce fails the build if Cypher serving isn't
# at least 5x faster than delegating to PostgreSQL (1.5x for rid_suffix_scan,
# 1x for the objectid point lookup); a pg baseline that runs past -pg-cap
# (120s default) is capped and judged on the engine's own absolute p50
# instead; never pass -enforce in CI (see bench/cypherbench).
bench-cypher: ## run the cypher-interpreter benchmark against BLOODTRAIL_TEST_PG
	go run ./bench/cypherbench -dsn "$(BLOODTRAIL_TEST_PG)" -enforce

# Usage: make build-image UPSTREAM_TAG=v9.6.0
UPSTREAM_TAG ?= v9.6.0
build-image:
	./build/build-image.sh $(UPSTREAM_TAG)
