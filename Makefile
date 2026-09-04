.PHONY: test lint integration build-image tidy

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

# Usage: make build-image UPSTREAM_TAG=v9.6.0
UPSTREAM_TAG ?= v9.6.0
build-image:
	./build/build-image.sh $(UPSTREAM_TAG)
