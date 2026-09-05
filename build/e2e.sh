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
# Written to a file rather than piped into grep: `grep -q` exits at the first
# match and the writer then dies of SIGPIPE, which `go run` reports as a
# failure.
(cd "$ROOT" && go run ./cmd/bloodtrail status --compose-file "$WORK/docker-compose.yml") > "$WORK/status.txt"
grep -q "running:.*$IMAGE" "$WORK/status.txt"

echo "==> Waiting for the path engine's snapshot to build from the ingested fixture"
# Written to a file rather than piped into grep, for the same reason as the
# status check above: `docker compose logs` keeps writing after `grep -q`
# finds its match and closes the pipe, dies of SIGPIPE, and (with pipefail)
# turns a successful match into a failed pipeline.
bh_logs() { docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" logs bloodhound > "$WORK/bloodhound-logs.txt" 2>&1; }
snapshot_rebuilt=false
for _ in $(seq 1 24); do
  bh_logs
  if grep -q "snapshot rebuilt" "$WORK/bloodhound-logs.txt"; then snapshot_rebuilt=true; break; fi
  sleep 5
done
[ "$snapshot_rebuilt" = true ] || { echo "the path engine never logged a rebuilt snapshot within 120s" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }

echo "==> Querying the path engine directly"
# The fixture's built-in Administrator (RID 500) is a direct MemberOf member
# of Domain Admins (RID 512) -- see internal/verify/fixture/groups.json's
# "S-1-5-...-512" entry and users.json's "S-1-5-...-500" entry.
DOMAIN_SID="S-1-5-21-3130019616-2776909439-2417379446"
USER_SID="$DOMAIN_SID-500"
GROUP_SID="$DOMAIN_SID-512"

LOGIN_BODY="$(jq -n --arg u admin --arg p "$PASSWORD" '{login_method:"secret", username:$u, secret:$p}')"
TOKEN="$(curl -s -X POST http://127.0.0.1:8080/api/v2/login -H 'Content-Type: application/json' -d "$LOGIN_BODY" | jq -r '.data.session_token // empty')"
[ -n "$TOKEN" ] || { echo "could not obtain a session token for the engine phase" >&2; exit 1; }

bh_logs
served_before="$(grep -c "path engine served" "$WORK/bloodhound-logs.txt" || true)"

sp_code="$(curl -s -o "$WORK/shortest-path.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/api/v2/graphs/shortest-path?start_node=$USER_SID&end_node=$GROUP_SID")"
[ "$sp_code" = "200" ] || { echo "GET /api/v2/graphs/shortest-path returned HTTP $sp_code" >&2; cat "$WORK/shortest-path.json" >&2; exit 1; }
node_count="$(jq '.data.nodes | length' "$WORK/shortest-path.json")"
[ "$node_count" -gt 0 ] || { echo "GET /api/v2/graphs/shortest-path returned no nodes" >&2; cat "$WORK/shortest-path.json" >&2; exit 1; }

cypher="MATCH p=shortestPath((s)-[:MemberOf*1..]->(t:Group)) WHERE s.objectid = '$USER_SID' AND t.objectid ENDS WITH '-512' AND s<>t RETURN p LIMIT 10"
CYPHER_BODY="$(jq -n --arg q "$cypher" '{query:$q}')"
cypher_code="$(curl -s -o "$WORK/cypher.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$CYPHER_BODY" http://127.0.0.1:8080/api/v2/graphs/cypher)"
[ "$cypher_code" = "200" ] || { echo "POST /api/v2/graphs/cypher returned HTTP $cypher_code" >&2; cat "$WORK/cypher.json" >&2; exit 1; }

bh_logs
served_after="$(grep -c "path engine served" "$WORK/bloodhound-logs.txt" || true)"
served_delta=$((served_after - served_before))
[ "$served_delta" -ge 2 ] || { echo "engine did not serve both GET /api/v2/graphs/shortest-path and POST /api/v2/graphs/cypher (\"path engine served\" count $served_before -> $served_after, delta $served_delta); PostgreSQL answered instead" >&2; exit 1; }

echo "==> Rolling back"
(cd "$ROOT" && go run ./cmd/bloodtrail rollback --compose-file "$WORK/docker-compose.yml")
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" ps --format json bloodhound | grep -q "specterops/bloodhound:$DOCKERHUB_TAG"
api_ready

# A rollback leaves the migrated graph in PostgreSQL: BloodHound's migrator
# only inserts, so a second install would silently switch onto that stale copy
# instead of migrating whatever Neo4j holds now.
echo "==> Refusing a second install onto the graph the first one left in PostgreSQL"
if (cd "$ROOT" && go run ./cmd/bloodtrail install --compose-file "$WORK/docker-compose.yml" --image "$IMAGE" --migration-timeout 30m --yes) > "$WORK/second-install.log" 2>&1; then
  echo "the second install should have refused to migrate onto the existing PostgreSQL graph" >&2
  cat "$WORK/second-install.log" >&2
  exit 1
fi
grep -q "refusing to migrate on top of them" "$WORK/second-install.log"
# The refused install took a backup and wrote its manifest before finding the
# stale graph; rollback clears both without touching the deployment.
(cd "$ROOT" && go run ./cmd/bloodtrail rollback --compose-file "$WORK/docker-compose.yml")

echo "==> Reinstalling with --replace-postgres-graph"
(cd "$ROOT" && go run ./cmd/bloodtrail install --compose-file "$WORK/docker-compose.yml" --image "$IMAGE" --migration-timeout 30m --replace-postgres-graph --yes)
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" ps --format json bloodhound | grep -q "$IMAGE"
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" exec -T app-db psql -U bloodhound -d bloodhound -tAc 'select driver from database_switch' | grep -qx bloodtrail

echo "==> Rolling back again"
(cd "$ROOT" && go run ./cmd/bloodtrail rollback --compose-file "$WORK/docker-compose.yml")
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" ps --format json bloodhound | grep -q "specterops/bloodhound:$DOCKERHUB_TAG"
api_ready
echo "==> e2e passed"
