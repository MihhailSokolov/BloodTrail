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
IMAGE="${IMAGE:-ghcr.io/mihhailsokolov/bloodtrail:$TAG-bte2e}"
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
# BLOODTRAIL_LOG_LEVEL=debug is stamped into the bloodhound service's own
# environment list here, on the base compose file, rather than later on the
# override file `bloodtrail install` writes: settings.go's SettingsFromEnv
# only ever reads it once, at driver-construction time, so it must already
# be in place before the FIRST BloodTrail-driver boot -- the very run this
# fixture's ingest+analysis happens in -- for Debug lines like "write-through
# applied" to be observable from that boot onward. Scoped to the bloodhound
# service specifically (the awk state machine below) so app-db/graph-db's
# own "    environment:" blocks, at the same indent, are left alone.
awk '
  $0 ~ /^  [A-Za-z0-9_-]+:$/ { in_bh = ($0 == "  bloodhound:") }
  { print }
  in_bh && $0 == "    environment:" { print "      - BLOODTRAIL_LOG_LEVEL=debug" }
' "$WORK/docker-compose.yml" > "$WORK/docker-compose.yml.tmp"
mv "$WORK/docker-compose.yml.tmp" "$WORK/docker-compose.yml"
grep -q "BLOODTRAIL_LOG_LEVEL=debug" "$WORK/docker-compose.yml"
printf 'BLOODHOUND_TAG=%s\n' "$DOCKERHUB_TAG" > "$WORK/.env"
# Tear down whatever a previous local run left behind FIRST -- containers
# and, critically, volumes. `rm -rf $WORK` above wipes the compose file but
# not the docker project it named, so a rerun on a developer machine would
# otherwise attach to days-old app-db/graph-db volumes: the admin user then
# already exists, no "Initial Password Set To" line is ever printed, and the
# run dies at the password extraction below. A no-op on a fresh CI runner.
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" down -v --remove-orphans 2>/dev/null || true
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" up -d
for _ in $(seq 1 90); do api_ready && break; sleep 5; done
api_ready
# `|| true` on the extraction pipeline: with pipefail, a log with no match
# makes grep's own exit status kill the whole script SILENTLY -- before the
# guard below can say what actually went wrong.
PASSWORD="$(docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" logs bloodhound | grep -o 'Initial Password Set To: *[^ #]*' | awk '{print $NF}' | tail -1 || true)"
[ -n "$PASSWORD" ] || { echo "could not read the initial admin password from the logs -- was this stack's app-db volume left over from an earlier run?" >&2; exit 1; }

echo "==> Installing BloodTrail"
(cd "$ROOT" && go run ./cmd/bloodtrail install --compose-file "$WORK/docker-compose.yml" --image "$IMAGE" --admin-password "$PASSWORD" --migration-timeout 30m --yes)
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" ps --format json bloodhound | grep -q "$IMAGE"
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" exec -T app-db psql -U bloodhound -d bloodhound -tAc 'select driver from database_switch' | grep -qx bloodtrail
# Written to a file rather than piped into grep: `grep -q` exits at the first
# match and the writer then dies of SIGPIPE, which `go run` reports as a
# failure.
(cd "$ROOT" && go run ./cmd/bloodtrail status --compose-file "$WORK/docker-compose.yml") > "$WORK/status.txt"
grep -q "running:.*$IMAGE" "$WORK/status.txt"

bh_logs() { docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" logs bloodhound > "$WORK/bloodhound-logs.txt" 2>&1; }

echo "==> Asserting write-through replicated the ingested-and-analyzed fixture with no post-boot rebuild"
# The poller that used to keep re-rebuilding a snapshot on a timer is gone
# (see internal/engine/boot.go's own doc on Start): every committed write is
# now replayed directly into the in-memory replica by Apply
# (write_observer.go's driver hooks), so the only rebuild a healthy
# container logs across its whole lifetime is the one-shot boot load
# Start's own goroutine runs before it can serve anything at all
# (triggerStartup, boot.go) -- there is no periodic rebuild left to wait
# for. `bloodtrail install --admin-password`'s own smoke test
# (internal/verify.Smoke.Run, which the install call above already ran to
# completion, including its own GET /api/v2/search poll) is therefore
# already all the synchronization this phase needs: by the time install
# returned, the fixture's ingest AND analysis had both committed to
# PostgreSQL and been replayed into the engine, with no lag left to poll
# out.
#
# Written to a file rather than piped into grep, for the same reason as the
# status check above: `docker compose logs` keeps writing after `grep -q`
# finds its match and closes the pipe, dies of SIGPIPE, and (with pipefail)
# turns a successful match into a failed pipeline.
bh_logs
rebuilt_after_ingest="$(grep -c "bloodtrail: snapshot rebuilt" "$WORK/bloodhound-logs.txt" || true)"
applied_after_ingest="$(grep -c "bloodtrail: write-through applied" "$WORK/bloodhound-logs.txt" || true)"
# Measured against this real deployment's own ingest+analysis pipeline
# (SharpHound-shaped JSON through the same /api/v2/file-upload path every
# real collector uses, followed by the same analysis pass a production
# deployment runs) rather than a synthetic corpus: this container logs
# exactly one rebuild -- the boot load (trigger=startup), which ran against
# the freshly created container's empty graph before the fixture upload
# even began -- with every write the fixture's ingest+analysis produced
# replayed by write-through instead of tripping a fallback rebuild.
[ "$rebuilt_after_ingest" -eq 1 ] || { echo "expected exactly 1 \"snapshot rebuilt\" line (the boot load) after install+ingest+analysis, found $rebuilt_after_ingest" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }
grep "bloodtrail: snapshot rebuilt" "$WORK/bloodhound-logs.txt" | grep -q '"trigger":"startup"' || { echo "the one rebuild logged was not the boot load (trigger=startup)" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }
[ "$applied_after_ingest" -ge 1 ] || { echo "no \"write-through applied\" lines after install+ingest+analysis; every write should have replayed directly into the engine" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }

echo "==> Querying for a node analysis itself creates, to prove analysis-phase writes reached the engine"
# TESTLAB.LOCAL-S-1-1-0 is the domain's well-known "Everyone" principal --
# not part of the SharpHound fixture at all (internal/verify/fixture has no
# such object) -- but created by analysis post-processing itself
# (upstream packages/go/analysis/ad/post.go's Post -> LinkWellKnownNodes ->
# createWellKnownNodesForDomain -> getOrCreateWellKnownGroup, which
# CreateNodes it when absent) as part of the same analysis run install's
# smoke test already waited out above. Its objectid is deterministic:
# wellknown.EveryoneSIDSuffix ("-S-1-1-0") appended to the domain's FQDN,
# not its SID (getOrCreateWellKnownGroup branches on which well-known
# principal it is building; Everyone and Authenticated Users take the
# domain-name branch, unlike Domain Users/Computers). Finding it here is
# proof that this specific analysis-phase CREATE -- not just the raw
# ingest that came before it -- was replayed into the in-memory replica by
# write-through, not merely served by the boot rebuild counted above.
LOGIN_BODY="$(jq -n --arg u admin --arg p "$PASSWORD" '{login_method:"secret", username:$u, secret:$p}')"
TOKEN="$(curl -s -X POST http://127.0.0.1:8080/api/v2/login -H 'Content-Type: application/json' -d "$LOGIN_BODY" | jq -r '.data.session_token // empty')"
[ -n "$TOKEN" ] || { echo "could not obtain a session token for the zero-rebuild phase" >&2; exit 1; }

EVERYONE_OID="TESTLAB.LOCAL-S-1-1-0"
everyone_query="MATCH (n) WHERE n.objectid = '$EVERYONE_OID' RETURN n LIMIT 1"
everyone_body="$(jq -n --arg q "$everyone_query" '{query:$q}')"
everyone_code="$(curl -s -o "$WORK/everyone.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$everyone_body" http://127.0.0.1:8080/api/v2/graphs/cypher)"
[ "$everyone_code" = "200" ] || { echo "POST /api/v2/graphs/cypher (well-known Everyone lookup) returned HTTP $everyone_code" >&2; cat "$WORK/everyone.json" >&2; exit 1; }
everyone_nodes="$(jq '.data.nodes | length' "$WORK/everyone.json")"
[ "$everyone_nodes" -ge 1 ] || { echo "the domain's well-known Everyone principal ($EVERYONE_OID), created by analysis post-processing, was not found" >&2; cat "$WORK/everyone.json" >&2; exit 1; }

bh_logs
rebuilt_after_query="$(grep -c "bloodtrail: snapshot rebuilt" "$WORK/bloodhound-logs.txt" || true)"
[ "$rebuilt_after_query" -eq "$rebuilt_after_ingest" ] || { echo "a rebuild was logged between ingest completion and the analysis-created-node query (count $rebuilt_after_ingest -> $rebuilt_after_query); write-through's zero-rebuild guarantee did not hold" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }

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

# The POST below is exercised here too (proving the endpoint is live), but
# its own serving marker is deliberately not asserted at this point:
# the cypher endpoint is no longer wired through servePathQuery at all, so
# it no longer touches the Info "bloodtrail: path engine served" line
# checked above, and instead logs its own "bloodtrail: cypher engine served"
# (see internal/engine/engine.go's TryCypher/cypherServedLogMessage) at Debug.
# BLOODTRAIL_LOG_LEVEL=debug has been active since the container's very
# first boot (stamped into the base compose file above), so that line is
# already being written -- but the dedicated "Querying the Cypher
# interpreter directly" phase further down is what actually asserts it,
# via its own before/after delta around its own calls; asserting it here
# too would double-count against that phase's delta.
cypher="MATCH p=shortestPath((s)-[:MemberOf*1..]->(t:Group)) WHERE s.objectid = '$USER_SID' AND t.objectid ENDS WITH '-512' AND s<>t RETURN p LIMIT 10"
CYPHER_BODY="$(jq -n --arg q "$cypher" '{query:$q}')"
cypher_code="$(curl -s -o "$WORK/cypher.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "$CYPHER_BODY" http://127.0.0.1:8080/api/v2/graphs/cypher)"
[ "$cypher_code" = "200" ] || { echo "POST /api/v2/graphs/cypher returned HTTP $cypher_code" >&2; cat "$WORK/cypher.json" >&2; exit 1; }
cypher_node_count="$(jq '.data.nodes | length' "$WORK/cypher.json")"
[ "$cypher_node_count" -gt 0 ] || { echo "POST /api/v2/graphs/cypher returned no nodes" >&2; cat "$WORK/cypher.json" >&2; exit 1; }

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
# The session token from the login above should still be valid (no restart
# has happened since), but re-authenticate anyway rather than lean on that.
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
# The same in-memory replica also backs a Cypher interpreter
# (internal/engine.TryCypher), reached through POST /api/v2/graphs/cypher
# exactly as the path phase above already exercised once (with a
# hand-written shortestPath text). This phase instead sends two queries
# copied verbatim from BloodHound's own pre-built query corpus
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
# BLOODTRAIL_LOG_LEVEL=debug has been active since the container's very
# first boot (the builder phase above already relied on this for its own
# Debug-level marker), so cypherServedLogMessage's Debug line ("bloodtrail:
# cypher engine served", see internal/engine/engine.go) is visible here
# too. The session token from the builder phase above is still valid (no
# restart happened in between).
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

echo "==> Enabling the snapshot file for a graceful-restart persistence check"
# BLOODTRAIL_SNAPSHOT_DIR (settings.go's EnvSnapshotDir) turns on the
# snapshot-file boot/save cycle: on a clean shutdown, Driver.Close saves a
# fold of the engine's current View to <dir>/graph-<id>.btsnap
# (internal/engine/persist.go's SaveSnapshot), and a later Start's boot-load
# goroutine tries reading that file back before spending a full PostgreSQL
# rebuild (internal/engine/boot.go's tryLoadSnapshotFile) -- gated on the
# file's embedded watermark exactly matching PostgreSQL's own watermark
# counter at that moment (63906a9 fixed the boot-side half of this: the
# attempt used to run before the driver's default graph could possibly be
# known, making the file unreachable in production regardless of what it
# held; 1f57046 fixed the save-side half, below).
#
# A bind mount onto the host filesystem, not the container's own writable
# layer, is required here: this override-file edit is applied below with
# `docker compose ... up -d`, and `up -d` after a compose config change
# RECREATES the container -- discarding its writable layer -- whereas the
# `docker compose restart` this phase actually means to test, further
# down, does not. Without a mount surviving that recreate, the very first
# boot under the new config would find no file (same as every boot
# before it), and the restart that follows would too, making the
# assertions below vacuous.
#
# OVERRIDE_FILE is the file `bloodtrail install` already wrote
# (compose.OverrideFileName, rendered by compose.Override.Render with the
# `bhe_graph_driver` entry already in it, so it always has an
# "environment:" section to extend) and reapplies with the same two -f
# files the installer itself merges via dockerx.Compose.WithExtraFile --
# `bloodtrail rollback` only ever os.Remove()s this file wholesale, so
# editing its contents here does not confuse it.
OVERRIDE_FILE="$WORK/docker-compose.bloodtrail.yml"
SNAPSHOT_HOST_DIR="$WORK/snapshot-data"
SNAPSHOT_CONTAINER_DIR="/data/bloodtrail-snapshots"
mkdir -p "$SNAPSHOT_HOST_DIR"
awk -v hostdir="$SNAPSHOT_HOST_DIR" -v ctdir="$SNAPSHOT_CONTAINER_DIR" '
  { print }
  /^    image:/ { print "    volumes:"; print "      - " hostdir ":" ctdir }
  /^    environment:$/ { print "      - BLOODTRAIL_SNAPSHOT_DIR=" ctdir }
' "$OVERRIDE_FILE" > "$OVERRIDE_FILE.tmp"
mv "$OVERRIDE_FILE.tmp" "$OVERRIDE_FILE"
grep -q "BLOODTRAIL_SNAPSHOT_DIR=$SNAPSHOT_CONTAINER_DIR" "$OVERRIDE_FILE"
grep -q "$SNAPSHOT_HOST_DIR:$SNAPSHOT_CONTAINER_DIR" "$OVERRIDE_FILE"
docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" -f "$OVERRIDE_FILE" up -d
for _ in $(seq 1 90); do api_ready && break; sleep 5; done
api_ready

echo "==> Restarting the API container to prove the snapshot file survives it"
# The point of this phase is narrower than "serves correctly after a
# restart": a boot that silently fell back to a full PostgreSQL rebuild
# would still answer the query below correctly, which would make a check
# that stopped there pass vacuously. So the shutdown-side write is asserted
# unconditionally, and the boot side is then required to land on exactly one
# of two named outcomes -- ADOPTED or SUPERSEDED -- with everything else
# still a failure.
#
# ADOPTED is the outcome the feature exists for: "snapshot file loaded" with
# zero new "snapshot rebuilt" lines. tryLoadSnapshotFile's successful path
# returns before rebuildOnce is ever called (internal/engine/boot.go), so a
# genuine file load costs exactly zero rebuilds, by construction -- which is
# what makes the zero-rebuild half of that assertion proof the FILE, and not
# a rebuild that happened to produce the same answer, is what the boot served
# from.
#
# ADOPTED is also what the boot gap buffer (internal/engine/bootgap.go)
# makes of the write that used to force the other outcome: BloodHound queues
# a full analysis request on every boot and starts its Data Pipe Daemon with
# a zero start delay, so AD post-processing (FixWellKnownNodeTypes,
# RunDomainAssociations, LinkWellKnownNodes) writes to the graph within
# milliseconds of "Server started successfully" -- on every boot, with no
# new ingest and nothing to do. Those writes are recognized, so the boot
# buffers them while the file loads, proves via the watermark counters that
# they are exactly what landed since the file was stamped (bootGapCovered),
# and replays them onto the loaded snapshot before publishing -- the
# "snapshot file loaded" marker then carries a nonzero replayed_writes.
# ADOPTED is therefore the EXPECTED outcome now, with or without boot-time
# writes; it stays asserted as one of two, because supersession is still
# legitimate.
#
# SUPERSEDED is what a boot-time write the replay cannot account for
# produces, and it is equally correct -- not a flake being tolerated. The
# watermark protocol (internal/engine/watermark.go) is what makes that so:
# every mutating driver call bumps the single-row `bloodtrail_watermark`
# counter EAGERLY, before its own effect reaches PostgreSQL, and SaveSnapshot
# stamps the file with the counter that was live at the moment it folded.
# Two legitimate shapes:
#
#   - a counter advance nothing in this boot accounted for -- a write from
#     the previous process's crash window, or one whose Apply had not yet
#     been observed when the adoption decision ran: the boot logs
#     reason="boot gap not covered by buffered writes" with
#     pg_watermark > file_watermark.
#   - a fallback-shaped write (raw Cypher, a wipe -- the same closed list
#     ordinary write-through falls back on) landing during boot: its effect
#     cannot be replayed, the engine enters fallback before any view exists,
#     and the boot logs reason="the engine entered fallback while the file
#     was loading".
#
# Both are accepted here; the uncovered-gap form additionally requires pg to
# be AHEAD (a file ahead of pg is impossible under a monotonic counter and
# stays a failure), and requires the file the boot read to be the very file
# this phase's own shutdown just wrote, compared by stamped watermark. Either
# way the boot must fall back to a genuine PostgreSQL rebuild, and the query
# in the next phase must still be correct.
#
# Everything else still fails, and these are the cases that would be real
# defects: a rejection for a corrupt or wrong-version file, a failed pg
# watermark read, an over-memory-limit file, a mismatch in the impossible
# direction, a boot that read a DIFFERENT file than the one just written, or
# no file attempt at all (both deltas zero -- what a silently unreachable
# file boot looks like, which is exactly the defect this phase was added for).
#
# `docker compose restart` -- unlike the `up -d` calls above and below,
# which recreate the container because the compose config just changed --
# sends SIGTERM to the same container's still-running process and starts
# it again in place once it exits, with no config change to react to. A
# generous --timeout keeps a slow fold+write from ever racing SIGKILL on
# this tiny fixture graph, though the graph is small enough that this
# should never bind in practice: the save this phase measures folds and
# writes 118 nodes / 974 edges in about 7ms, four orders of magnitude
# inside even the 10s default.
#
# The shutdown-side assertion is what earns this phase a real container
# stop rather than a driver-level test. The save runs inside Driver.Close,
# on the context BloodHound's own shutdown hands it -- which is always
# ALREADY CANCELLED, because that cancellation is precisely what releases
# the wait that reaches the deferred Close at all. A save reading
# PostgreSQL on such a context can never converge, and declines with a
# single Debug line; nothing errors, nothing warns, and the next boot
# rebuilds exactly as if the feature were switched off. That is a defect
# only a signal-driven shutdown reproduces -- every driver test that
# closed with a live context passed throughout -- so this phase, which
# stops the container the way an operator does, is the end-to-end guard
# for it. See driver.go's Close for the fix and its own regression test.
# The two superseded shapes, each matched as one pattern spanning message and
# reason (the JSON line always puts "message" first) so neither can ever be
# satisfied by some other line that happens to share a phrase -- in
# particular the Debug "snapshot rebuild not adopted: a write was applied
# while it loaded", which is a different message with different wording.
GAP_REJECTION='bloodtrail: snapshot file rejected.*"reason":"boot gap not covered by buffered writes"'
FALLBACK_REJECTION='bloodtrail: snapshot file rejected.*"reason":"the engine entered fallback while the file was loading"'

# json_num prints the value of a numeric JSON field from the LAST log line
# carrying the given message: $1 message, $2 field. Field names are matched
# with their opening quote, so asking for "watermark" never matches
# "file_watermark" or "pg_watermark".
json_num() {
  grep -o "\"message\":\"$1\"[^}]*\"$2\":[0-9]*" "$WORK/bloodhound-logs.txt" | tail -1 | sed 's/.*:\([0-9]*\)$/\1/'
}

bh_logs
written_before="$(grep -c "bloodtrail: snapshot file written" "$WORK/bloodhound-logs.txt" || true)"
loaded_before="$(grep -c "bloodtrail: snapshot file loaded" "$WORK/bloodhound-logs.txt" || true)"
rejected_before="$(grep -c "bloodtrail: snapshot file rejected" "$WORK/bloodhound-logs.txt" || true)"
gap_before="$(grep -c "$GAP_REJECTION" "$WORK/bloodhound-logs.txt" || true)"
fellback_before="$(grep -c "$FALLBACK_REJECTION" "$WORK/bloodhound-logs.txt" || true)"
rebuilt_before="$(grep -c "bloodtrail: snapshot rebuilt" "$WORK/bloodhound-logs.txt" || true)"

docker compose --project-directory "$WORK" -f "$WORK/docker-compose.yml" -f "$OVERRIDE_FILE" restart --timeout 30 bloodhound
for _ in $(seq 1 90); do api_ready && break; sleep 5; done
api_ready

bh_logs
written_after="$(grep -c "bloodtrail: snapshot file written" "$WORK/bloodhound-logs.txt" || true)"
loaded_after="$(grep -c "bloodtrail: snapshot file loaded" "$WORK/bloodhound-logs.txt" || true)"
rejected_after="$(grep -c "bloodtrail: snapshot file rejected" "$WORK/bloodhound-logs.txt" || true)"
gap_after="$(grep -c "$GAP_REJECTION" "$WORK/bloodhound-logs.txt" || true)"
fellback_after="$(grep -c "$FALLBACK_REJECTION" "$WORK/bloodhound-logs.txt" || true)"
rebuilt_after="$(grep -c "bloodtrail: snapshot rebuilt" "$WORK/bloodhound-logs.txt" || true)"

written_delta=$((written_after - written_before))
loaded_delta=$((loaded_after - loaded_before))
rejected_delta=$((rejected_after - rejected_before))
gap_delta=$((gap_after - gap_before))
fellback_delta=$((fellback_after - fellback_before))
rebuilt_delta=$((rebuilt_after - rebuilt_before))

# The shutdown side is unconditional: whichever outcome the boot lands on,
# this phase is only meaningful if the file path was genuinely exercised, and
# the save running at all is the half that never races anything.
[ "$written_delta" -ge 1 ] || { echo "the shutdown triggered by \"docker compose restart\" never logged \"snapshot file written\" (count $written_before -> $written_after); the graceful-shutdown save did not run" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }
ls "$SNAPSHOT_HOST_DIR"/graph-*.btsnap >/dev/null 2>&1 || { echo "no graph-*.btsnap file found on the bind-mounted host directory $SNAPSHOT_HOST_DIR after the restart" >&2; exit 1; }

# The watermark stamped into the file this phase's own shutdown just wrote.
# Both accepted outcomes below compare the boot's view of the file against
# it, so neither can be satisfied by a boot that read some other, older file.
saved_watermark="$(json_num "bloodtrail: snapshot file written" "watermark")"
[ -n "$saved_watermark" ] || { echo "could not read the watermark stamped into the snapshot file at shutdown" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }

# Exactly one attempt is made per boot (tryLoadSnapshotFile is one-shot), so
# the outcomes below are mutually exclusive by construction. "No file attempt
# at all" -- every delta zero -- falls through to the failure branch, as does
# any rejection whose reason is not one of the two boot-write shapes.
if [ "$loaded_delta" -ge 1 ] && [ "$rejected_delta" -eq 0 ] && [ "$rebuilt_delta" -eq 0 ]; then
  loaded_watermark="$(json_num "bloodtrail: snapshot file loaded" "watermark")"
  [ "$loaded_watermark" = "$saved_watermark" ] || { echo "the boot loaded a snapshot file stamped $loaded_watermark, but the shutdown wrote $saved_watermark; it did not read the file this phase produced" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }
  replayed_writes="$(json_num "bloodtrail: snapshot file loaded" "replayed_writes")"
  echo "    restart outcome: ADOPTED -- the boot loaded the snapshot file (watermark $loaded_watermark) and rebuilt nothing, replaying ${replayed_writes:-0} boot-time write(s) onto it"
elif [ "$loaded_delta" -eq 0 ] && [ "$rejected_delta" -ge 1 ] && [ "$((gap_delta + fellback_delta))" -eq "$rejected_delta" ] && [ "$rebuilt_delta" -ge 1 ]; then
  if [ "$gap_delta" -ge 1 ]; then
    file_watermark="$(json_num "bloodtrail: snapshot file rejected" "file_watermark")"
    pg_watermark="$(json_num "bloodtrail: snapshot file rejected" "pg_watermark")"
    [ "$file_watermark" = "$saved_watermark" ] || { echo "the boot rejected a snapshot file stamped $file_watermark, but the shutdown wrote $saved_watermark; it did not read the file this phase produced" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }
    [ "$pg_watermark" -gt "$file_watermark" ] || { echo "the boot rejected the file for an uncovered gap with PostgreSQL at $pg_watermark and the file at $file_watermark; only PostgreSQL being AHEAD is a boot-time write superseding the file, and the counter is monotonic, so this is a real defect" >&2; cat "$WORK/bloodhound-logs.txt" >&2; exit 1; }
    echo "    restart outcome: SUPERSEDED -- a counter advance nothing in the boot accounted for (PostgreSQL at $pg_watermark, file at $file_watermark), so the file was correctly refused and the boot rebuilt from PostgreSQL"
  else
    echo "    restart outcome: SUPERSEDED -- a fallback-shaped write landed during the restart's own boot, so the file was correctly refused and the boot rebuilt from PostgreSQL"
  fi
else
  {
    echo "the restart's boot landed on neither accepted outcome."
    echo "  deltas: snapshot file loaded=$loaded_delta, snapshot file rejected=$rejected_delta (uncovered gap=$gap_delta, boot fallback=$fellback_delta), snapshot rebuilt=$rebuilt_delta"
    echo "  accepted: ADOPTED    (loaded>=1, rejected=0, rebuilt=0; boot-time writes are buffered and replayed, so they no longer supersede the file)"
    echo "         or SUPERSEDED (loaded=0, rejected>=1 and every one of them a legitimate boot-write shape, rebuilt>=1)"
    echo "  a rejection for corruption, a bad version, a failed watermark read or the memory limit is a real defect, as is no file attempt at all."
  } >&2
  cat "$WORK/bloodhound-logs.txt" >&2
  exit 1
fi

echo "==> Querying after the restart to confirm the engine serves correctly either way"
# Asserted for BOTH accepted outcomes above, deliberately: an adopted file and
# a rebuild that superseded it must be indistinguishable to a caller, and this
# is what proves it. On the SUPERSEDED path it is also the check that the
# fallback actually recovered rather than leaving the engine unable to serve.
LOGIN_BODY="$(jq -n --arg u admin --arg p "$PASSWORD" '{login_method:"secret", username:$u, secret:$p}')"
TOKEN="$(curl -s -X POST http://127.0.0.1:8080/api/v2/login -H 'Content-Type: application/json' -d "$LOGIN_BODY" | jq -r '.data.session_token // empty')"
[ -n "$TOKEN" ] || { echo "could not obtain a session token after the restart" >&2; exit 1; }
restart_code="$(curl -s -o "$WORK/restart-shortest-path.json" -w '%{http_code}' \
  -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/api/v2/graphs/shortest-path?start_node=$USER_SID&end_node=$GROUP_SID")"
[ "$restart_code" = "200" ] || { echo "GET /api/v2/graphs/shortest-path after the restart returned HTTP $restart_code" >&2; cat "$WORK/restart-shortest-path.json" >&2; exit 1; }
restart_node_count="$(jq '.data.nodes | length' "$WORK/restart-shortest-path.json")"
[ "$restart_node_count" -gt 0 ] || { echo "GET /api/v2/graphs/shortest-path after the restart returned no nodes" >&2; cat "$WORK/restart-shortest-path.json" >&2; exit 1; }

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
