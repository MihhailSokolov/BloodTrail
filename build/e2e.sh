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

echo "==> Waiting for the path engine's snapshot to rebuild from the ingested fixture"
# Written to a file rather than piped into grep, for the same reason as the
# status check above: `docker compose logs` keeps writing after `grep -q`
# finds its match and closes the pipe, dies of SIGPIPE, and (with pipefail)
# turns a successful match into a failed pipeline.
#
# "snapshot rebuilt" alone also matches the startup-triggered rebuild, which
# runs against the pre-ingest 1-node graph before the fixture is loaded --
# that line appears in the logs almost immediately and would let the loop
# fall through while the snapshot is still stale, so the GET below would hit
# a declining engine and PostgreSQL would answer instead (see
# internal/engine/engine.go's "bloodtrail: snapshot rebuilt" log call and
# internal/engine/poller.go's triggerStartup/triggerAnalysis constants). Wait
# specifically for the analysis-triggered rebuild, which fires once the
# fixture's ingest+analysis run completes and is the one that reflects the
# ingested graph.
bh_logs() { docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" logs bloodhound > "$WORK/bloodhound-logs.txt" 2>&1; }
snapshot_rebuilt=false
for _ in $(seq 1 24); do
  bh_logs
  if grep "snapshot rebuilt" "$WORK/bloodhound-logs.txt" | grep -q '"trigger":"analysis"'; then snapshot_rebuilt=true; break; fi
  sleep 5
done
[ "$snapshot_rebuilt" = true ] || { echo "the path engine never logged an analysis-triggered rebuilt snapshot within 120s" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }

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

bh_logs
served_after="$(grep -c "path engine served" "$WORK/bloodhound-logs.txt" || true)"
served_delta=$((served_after - served_before))
[ "$served_delta" -ge 1 ] || { echo "the path engine did not serve GET /api/v2/graphs/shortest-path (\"path engine served\" count $served_before -> $served_after, delta $served_delta); PostgreSQL answered instead" >&2; exit 1; }

# The POST below is exercised here too (proving the endpoint is live before
# the debug-logging restart further down), but its own serving marker is
# deliberately not asserted at this point: milestone 4 rewired the cypher
# endpoint off servePathQuery entirely, so it no longer touches the Info
# "bloodtrail: path engine served" line checked above, and instead logs its
# own "bloodtrail: cypher engine served" (see internal/engine/engine.go's
# TryCypher/cypherServedLogMessage) at Debug -- which stays invisible until
# BLOODTRAIL_LOG_LEVEL=debug reaches the container, and that only happens at
# the restart below (see its own comment for why it can't simply move
# earlier: doing so before the "trigger":"analysis" wait above would drop
# the pre-recreate log history and swap that wait's required trigger for a
# "startup" one). The dedicated "Querying the Cypher interpreter directly"
# phase further down -- which runs after that restart, with debug logging
# already active -- is what actually asserts the cypher engine served this
# endpoint instead of PostgreSQL.
cypher="MATCH p=shortestPath((s)-[:MemberOf*1..]->(t:Group)) WHERE s.objectid = '$USER_SID' AND t.objectid ENDS WITH '-512' AND s<>t RETURN p LIMIT 10"
CYPHER_BODY="$(jq -n --arg q "$cypher" '{query:$q}')"
cypher_code="$(curl -s -o "$WORK/cypher.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$CYPHER_BODY" http://127.0.0.1:8080/api/v2/graphs/cypher)"
[ "$cypher_code" = "200" ] || { echo "POST /api/v2/graphs/cypher returned HTTP $cypher_code" >&2; cat "$WORK/cypher.json" >&2; exit 1; }
cypher_node_count="$(jq '.data.nodes | length' "$WORK/cypher.json")"
[ "$cypher_node_count" -gt 0 ] || { echo "POST /api/v2/graphs/cypher returned no nodes" >&2; cat "$WORK/cypher.json" >&2; exit 1; }

echo "==> Enabling debug logging for the builder-served log line"
# internal/engine/serve_builder.go's servedOp logs "bloodtrail: builder
# engine served" one level quieter (Debug) than the path engine's Info
# "bloodtrail: path engine served" used above, deliberately, since a
# structural node/relationship query is expected to run far more often than
# a shortest-path one. Debug only surfaces once BLOODTRAIL_LOG_LEVEL=debug
# reaches the bloodhound service, and settings.go's SettingsFromEnv (called
# once from driver.go at driver construction) only ever reads that
# environment variable at process start, so it takes a container recreate to
# take effect -- there is no live-reconfigure path and no `bloodtrail
# install` flag for it (see cmd/bloodtrail/main.go's flag set). The smallest
# mechanism is editing the override file `bloodtrail install` already wrote
# (compose.OverrideFileName, rendered by compose.Override.Render with the
# `bhe_graph_driver` entry already in it) and reapplying it with the same
# two -f files the installer itself merges via dockerx.Compose.WithExtraFile
# -- `bloodtrail rollback` only ever os.Remove()s this file wholesale, so
# editing its contents here does not confuse it.
#
# This restart is deliberately placed here, after the path phase, rather
# than right after `install` returns (which would be earlier, and was
# considered): the fixture's ingest+analysis already ran as part of
# `install --admin-password`'s own smoke test (internal/installer's
# runVerification), inside the single container instance install itself
# started, and that run is what drives the "trigger":"analysis" snapshot
# rebuild the wait loop above requires. Recreating the container between
# install and that wait would both drop the pre-recreate container's log
# history (docker compose logs only ever shows the current container
# instance) and replace the required "analysis" trigger with a "startup"
# one (rule (a) in internal/engine/poller.go's decideRebuild -- no snapshot
# exists yet after a recreate), breaking the existing assertion above.
# Restarting now, once that wait and the path-phase queries it fed have
# already passed, costs only one more short wait for the API and the
# engine's own (now startup-triggered) snapshot rebuild before the builder
# phase queries it.
OVERRIDE_FILE="$WORK/docker-compose.bloodtrail.yml"
awk '{print} /^    environment:$/ { print "      - BLOODTRAIL_LOG_LEVEL=debug" }' "$OVERRIDE_FILE" > "$OVERRIDE_FILE.tmp"
mv "$OVERRIDE_FILE.tmp" "$OVERRIDE_FILE"
grep -q "BLOODTRAIL_LOG_LEVEL=debug" "$OVERRIDE_FILE"
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" -f "$OVERRIDE_FILE" up -d
for _ in $(seq 1 90); do api_ready && break; sleep 5; done
api_ready

echo "==> Waiting for the path engine's snapshot to rebuild after the debug-logging restart"
snapshot_rebuilt=false
for _ in $(seq 1 24); do
  bh_logs
  if grep -q "snapshot rebuilt" "$WORK/bloodhound-logs.txt"; then snapshot_rebuilt=true; break; fi
  sleep 5
done
[ "$snapshot_rebuilt" = true ] || { echo "the path engine never logged a rebuilt snapshot within 120s of the debug-logging restart" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }

echo "==> Querying the builder engine directly"
# The fixture's Domain Admins group (RID 512) lists the built-in
# Administrator (RID 500) as a direct member -- see
# internal/verify/fixture/groups.json's "-512" entry's "Members" array --
# the same relationship the path phase above walked, this time served (or
# not) through the builder-serving path (internal/engine/serve_builder.go)
# instead of a shortest-path/cypher query.
#
# The route and its {object_id} semantics are pinned from upstream source,
# not guessed: .build/upstream-v9.6.0/cmd/api/src/api/registration/v2.go
# registers "GET /api/v2/groups/{object_id}/members" -> resources.
# ListADGroupMembers; ad_related_entity.go's handleAdRelatedEntityQuery calls
# queries.BuildEntityQueryParams, whose GetEntityObjectIDFromRequestPath
# reads {object_id} as a plain string and GetEntityByObjectId matches it
# straight against the node's objectid property -- so {object_id} is the AD
# SID string, exactly what GROUP_SID already holds, not a database row id.
#
# The session token from the login above should still be valid (BloodHound
# records sessions in PostgreSQL, not in the bloodhound process the restart
# above recreated), but re-authenticate anyway rather than lean on that.
LOGIN_BODY="$(jq -n --arg u admin --arg p "$PASSWORD" '{login_method:"secret", username:$u, secret:$p}')"
TOKEN="$(curl -s -X POST http://127.0.0.1:8080/api/v2/login -H 'Content-Type: application/json' -d "$LOGIN_BODY" | jq -r '.data.session_token // empty')"
[ -n "$TOKEN" ] || { echo "could not obtain a session token for the builder phase" >&2; exit 1; }

bh_logs
served_before="$(grep -c "builder engine served" "$WORK/bloodhound-logs.txt" || true)"

members_code="$(curl -s -o "$WORK/group-members.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/api/v2/groups/$GROUP_SID/members")"
[ "$members_code" = "200" ] || { echo "GET /api/v2/groups/\$GROUP_SID/members returned HTTP $members_code" >&2; cat "$WORK/group-members.json" >&2; exit 1; }
# The response envelope is {"data":[{"objectID":...,"name":...,"label":...,
# "kinds":[...]}], "count", "limit", "skip"} -- see cmd/api/src/model/
# model.go's PagedNodeListEntry (json tag "objectID", not "objectid") and
# marshalling.go's ResponseWrapper.
member_found="$(jq --arg sid "$USER_SID" '[.data[] | select(.objectID == $sid)] | length' "$WORK/group-members.json")"
[ "$member_found" -ge 1 ] || { echo "GET /api/v2/groups/\$GROUP_SID/members did not include the RID-500 user $USER_SID" >&2; cat "$WORK/group-members.json" >&2; exit 1; }

bh_logs
served_after="$(grep -c "builder engine served" "$WORK/bloodhound-logs.txt" || true)"
builder_served_delta=$((served_after - served_before))
[ "$builder_served_delta" -ge 1 ] || { echo "the builder engine did not serve GET /api/v2/groups/\$GROUP_SID/members (\"builder engine served\" count $served_before -> $served_after, delta $builder_served_delta); PostgreSQL answered instead" >&2; exit 1; }

echo "==> Querying the Cypher interpreter directly"
# Milestone 4 extends the same in-memory replica to a Cypher interpreter
# (internal/engine.TryCypher), reached through POST /api/v2/graphs/cypher
# exactly as the path phase above already exercised once (with a
# hand-written shortestPath text). This phase instead sends two queries
# copied verbatim from the milestone's own pre-built corpus
# (testdata/prebuilt/{selectors,agt}.json), one of each required shape:
#
#   - a plain property MATCH (no path, no traversal): selectors.json's
#     "Domain Admins" selector query, matching the fixture's Domain Admins
#     group (RID 512) by its objectid suffix.
#   - a shortestPath: agt.json's "Shortest paths to Domain Admins" query,
#     BloodHound's own richest pre-built shortestPath search -- an
#     unconstrained root alternated over its full ~64-member Active
#     Directory pathfinding edge-kind list (see bench/cypherbench's
#     identical shape4Text for the same text and why every one of those 64
#     kinds must already be registered in the `kind` table before the query
#     can run at all: upstream BloodHound's own migration
#     (database/migration/extensions/ad_graph_schema.sql) bulk-inserts the
#     complete AD kind vocabulary at schema-creation time, unlike
#     bench/adgen's synthetic graph, which only defines the handful of kinds
#     it actually writes edges for).
#
# The fixture's built-in Administrator (RID 500) is a direct MemberOf member
# of Domain Admins (RID 512, see the path phase's own comment above), so the
# shortestPath query is guaranteed at least that one trivial one-hop match.
#
# BLOODTRAIL_LOG_LEVEL=debug is already active from the restart above (the
# builder phase's own debug-logging line only surfaces at that level, and
# the just-completed builder phase already relied on it), so
# cypherServedLogMessage's Debug line ("bloodtrail: cypher engine served",
# see internal/engine/engine.go) is visible here too without another
# restart. The session token from the builder phase above is still valid
# (no restart happened in between).
CYPHER_PLAIN_MATCH="$(cat <<'CYPHER_EOF'
MATCH (n:Group)
WHERE n.objectid ENDS WITH '-512'
RETURN n;
CYPHER_EOF
)"
CYPHER_SHORTEST_PATH="$(cat <<'CYPHER_EOF'
MATCH p=shortestPath((t:Group)<-[:Owns|GenericAll|GenericWrite|WriteOwner|WriteDacl|MemberOf|ForceChangePassword|AllExtendedRights|AddMember|HasSession|GPLink|AllowedToDelegate|CoerceToTGT|AllowedToAct|AdminTo|CanPSRemote|CanRDP|ExecuteDCOM|HasSIDHistory|AddSelf|DCSync|ReadLAPSPassword|ReadGMSAPassword|DumpSMSAPassword|SQLAdmin|AddAllowedToAct|WriteSPN|AddKeyCredentialLink|SyncLAPSPassword|WriteAccountRestrictions|WriteGPLink|GoldenCert|ADCSESC1|ADCSESC3|ADCSESC4|ADCSESC6a|ADCSESC6b|ADCSESC9a|ADCSESC9b|ADCSESC10a|ADCSESC10b|ADCSESC13|SyncedToADUser|CoerceAndRelayNTLMToSMB|CoerceAndRelayNTLMToADCS|WriteOwnerLimitedRights|OwnsLimitedRights|ClaimSpecialIdentity|CoerceAndRelayNTLMToLDAP|CoerceAndRelayNTLMToLDAPS|ContainsIdentity|PropagatesACEsTo|GPOAppliesTo|CanApplyGPO|HasTrustKeys|WriteAltSecurityIdentities|WritePublicInformation|ManageCA|ManageCertificates|Contains|DCFor|SameForestTrust|SpoofSIDHistory|AbuseTGTDelegation*1..]-(s:Base))
WHERE t.objectid ENDS WITH '-512' AND s<>t
RETURN p
LIMIT 1000
CYPHER_EOF
)"

bh_logs
served_before="$(grep -c "cypher engine served" "$WORK/bloodhound-logs.txt" || true)"

match_body="$(jq -n --arg q "$CYPHER_PLAIN_MATCH" '{query:$q}')"
match_code="$(curl -s -o "$WORK/cypher-match.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$match_body" http://127.0.0.1:8080/api/v2/graphs/cypher)"
[ "$match_code" = "200" ] || { echo "POST /api/v2/graphs/cypher (plain property MATCH) returned HTTP $match_code" >&2; cat "$WORK/cypher-match.json" >&2; exit 1; }
match_nodes="$(jq '.data.nodes | length' "$WORK/cypher-match.json")"
[ "$match_nodes" -gt 0 ] || { echo "POST /api/v2/graphs/cypher (plain property MATCH) returned no nodes" >&2; cat "$WORK/cypher-match.json" >&2; exit 1; }

sp_body="$(jq -n --arg q "$CYPHER_SHORTEST_PATH" '{query:$q}')"
sp_code="$(curl -s -o "$WORK/cypher-shortest-path.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$sp_body" http://127.0.0.1:8080/api/v2/graphs/cypher)"
[ "$sp_code" = "200" ] || { echo "POST /api/v2/graphs/cypher (shortestPath) returned HTTP $sp_code" >&2; cat "$WORK/cypher-shortest-path.json" >&2; exit 1; }
sp_nodes="$(jq '.data.nodes | length' "$WORK/cypher-shortest-path.json")"
[ "$sp_nodes" -gt 0 ] || { echo "POST /api/v2/graphs/cypher (shortestPath) returned no nodes" >&2; cat "$WORK/cypher-shortest-path.json" >&2; exit 1; }

bh_logs
served_after="$(grep -c "cypher engine served" "$WORK/bloodhound-logs.txt" || true)"
cypher_served_delta=$((served_after - served_before))
[ "$cypher_served_delta" -ge 2 ] || { echo "the cypher interpreter did not serve both corpus queries (\"cypher engine served\" count $served_before -> $served_after, delta $cypher_served_delta); PostgreSQL answered instead" >&2; exit 1; }

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
