#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# End-to-end: upstream compose stack on Neo4j -> bloodtrail install -> checks -> rollback -> checks.
set -euo pipefail
TAG="${1:-v9.6.0}"
# specterops/bloodhound on Docker Hub tags its pre-built app images without
# the "v" prefix used by the GitHub source tag (e.g. "9.6.0", not "v9.6.0");
# "latest" currently resolves to the same manifest digest as the bare-number
# tag for the current stable release, confirming the convention.
DOCKERHUB_TAG="${TAG#v}"
IMAGE="${IMAGE:-ghcr.io/mihhailsokolov/bloodhound-bloodtrail:$TAG-bte2e}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$ROOT/.build/e2e"
rm -rf "$WORK" && mkdir -p "$WORK"

# BloodHound registers GET /api/version behind auth, so an unauthenticated
# request against a live server normally answers 401, not 200; that still
# proves the API is up and routing, so it counts as ready here too (matching
# internal/verify.WaitForAPI's semantics).
api_ready() {
  code="$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8080/api/version)"
  [ "$code" = "200" ] || [ "$code" = "401" ]
}

echo "==> Building image $IMAGE"
"$ROOT/build/build-image.sh" "$TAG" e2e --platform "${PLATFORM:-linux/amd64}"

echo "==> Starting the upstream stack"
cp "$ROOT/.build/upstream-$TAG/examples/docker-compose/docker-compose.yml" "$WORK/"
printf 'BLOODHOUND_TAG=%s\n' "$DOCKERHUB_TAG" > "$WORK/.env"
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" up -d
for _ in $(seq 1 90); do api_ready && break; sleep 5; done
api_ready
PASSWORD="$(docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" logs bloodhound | grep -o 'Initial Password Set To: *[^ #]*' | awk '{print $NF}' | tail -1)"
[ -n "$PASSWORD" ] || { echo "could not read the initial admin password from the logs" >&2; exit 1; }

echo "==> Installing BloodTrail"
(cd "$ROOT" && go run ./cmd/bloodtrail install --compose-file "$WORK/docker-compose.yml" --image "$IMAGE" --admin-password "$PASSWORD" --migration-timeout 30m --yes)
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" ps --format json bloodhound | grep -q "$IMAGE"
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" exec -T app-db psql -U bloodhound -d bloodhound -tAc 'select driver from database_switch' | grep -qx bloodtrail

echo "==> Rolling back"
(cd "$ROOT" && go run ./cmd/bloodtrail rollback --compose-file "$WORK/docker-compose.yml")
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" ps --format json bloodhound | grep -q "specterops/bloodhound:$DOCKERHUB_TAG"
api_ready
echo "==> e2e passed"
