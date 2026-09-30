#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# OpenGraph phase of build/e2e.sh. Registers an extension schema, uploads
# testdata/opengraph through BloodHound's own file-upload API, and checks
# what BloodHound answers from the BloodTrail driver: Cypher queries, the
# pathfinding endpoint, and "clear database" by source kind. Every expected
# value below is what stock BloodHound v9.6.0 on PostgreSQL returns for the
# same uploads. It also checks that BloodTrail served each answer from
# memory, with no fallback and no rebuild.
#
# It runs against any BloodHound using the BloodTrail driver:
#
#   BASE_URL  the API root (default http://127.0.0.1:8080)
#   PASSWORD  the admin password
#   LOGS_CMD  a shell command printing the server's log so far
#   WORK      a scratch directory
#
# CHECK_SERVED=0 skips the serve, fallback and rebuild checks, for running
# the same expectations against stock BloodHound on PostgreSQL.
#
# graph.json's hybrid edge starts at a user from BloodTrail's SharpHound
# fixture (internal/verify/fixture), which `bloodtrail install` uploads
# before e2e.sh reaches this phase.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURES="$ROOT/testdata/opengraph"
BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
: "${PASSWORD:?PASSWORD must be set}"
: "${WORK:?WORK must be set}"
CHECK_SERVED="${CHECK_SERVED:-1}"
[ "$CHECK_SERVED" = "0" ] || : "${LOGS_CMD:?LOGS_CMD must be set}"
mkdir -p "$WORK"

AD_USER="S-1-5-21-3130019616-2776909439-2417379446-2106" # ESID@TESTLAB.LOCAL
EDGES="ghx_MemberOf|ghx_HasRole|ghx_CanWrite|ghx_CanAdmin"
RESPONSE="$WORK/og-response.json"

fail() { echo "opengraph: $*" >&2; exit 1; }

# Every request carries a connect and a total time limit. Without one, a
# wedged API or proxy that accepts the connection and never answers holds the
# run until the workflow's own 60-minute cap, with nothing saying where. Every
# call here answers in well under a second, so the limits are generous;
# CURL_MAX_TIME overrides the total for a slow host.
CURL_MAX_TIME="${CURL_MAX_TIME:-60}"
bounded_curl() {
  local rc=0 arg url=""
  curl --connect-timeout 5 --max-time "$CURL_MAX_TIME" "$@" || rc=$?
  if [ "$rc" -ne 0 ]; then
    # Only the URL is reported: the other arguments carry the bearer token or the login body.
    for arg in "$@"; do case "$arg" in http://* | https://*) url="$arg" ;; esac; done
    echo "opengraph: curl exited $rc (28 means it timed out) for $url" >&2
  fi
  return "$rc"
}

LOGIN_BODY="$(jq -n --arg p "$PASSWORD" '{login_method:"secret", username:"admin", secret:$p}')"
TOKEN="$(bounded_curl -s -X POST "$BASE_URL/api/v2/login" -H 'Content-Type: application/json' -d "$LOGIN_BODY" | jq -r '.data.session_token // empty')"
[ -n "$TOKEN" ] || fail "could not obtain a session token"

# req METHOD PATH [BODY_FILE [CONTENT_TYPE [HEADER]]] makes one API call,
# leaves the body in $RESPONSE and prints the HTTP status. It rides out
# BloodHound's rate limiter (HTTP 429).
req() {
  local method="$1" path="$2" body="${3:-}" ctype="${4:-application/json}" header="${5:-}"
  local args=(-s -o "$RESPONSE" -w '%{http_code}' -X "$method" -H "Authorization: Bearer $TOKEN")
  if [ -n "$body" ]; then args+=(-H "Content-Type: $ctype" --data-binary "@$body"); fi
  if [ -n "$header" ]; then args+=(-H "$header"); fi
  local code delay=1
  for _ in 1 2 3 4 5 6 7 8; do
    code="$(bounded_curl "${args[@]}" "$BASE_URL$path")"
    [ "$code" = "429" ] || break
    sleep "$delay"
    delay=$((delay * 2))
  done
  echo "$code"
}

# marker PATTERN prints how many server log lines match PATTERN. The log is
# written to a file first: grep -q closing a pipe early would kill the
# writer with SIGPIPE, and pipefail would turn a match into a failure.
marker() {
  [ "$CHECK_SERVED" = "0" ] && { echo 0; return; }
  bash -c "$LOGS_CMD" > "$WORK/og-logs.txt" 2>&1
  grep -c "$1" "$WORK/og-logs.txt" || true
}

# logged_since PATTERN BEFORE succeeds once PATTERN's count has passed
# BEFORE. It polls for a few seconds: the container's log reaches `docker
# compose logs` asynchronously, so a line can trail the HTTP response it
# belongs to.
logged_since() {
  [ "$CHECK_SERVED" = "0" ] && return 0
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    [ "$(marker "$1")" -gt "$2" ] && return 0
    sleep 1
  done
  return 1
}

# wait_idle waits for the datapipe to report idle twice in a row.
wait_idle() {
  local idle=0
  for _ in $(seq 1 900); do
    if [ "$(req GET /api/v2/datapipe/status)" = "200" ] && [ "$(jq -r '.data.status' "$RESPONSE")" = "idle" ]; then
      idle=$((idle + 1))
      [ "$idle" -ge 2 ] && return 0
    else
      idle=0
    fi
    sleep 1
  done
  fail "the datapipe did not go idle within 15 minutes"
}

# upload FILE WANT_STATUS runs FILE as one ingest job and requires it to
# end in WANT_STATUS (model/jobs.go: 2 complete, 5 failed, 8 partially
# complete; -1, 3 and 4 are the other terminal ones). The job's warnings and
# errors are left in $WORK/og-tasks.json.
upload() {
  local file="$1" want="$2" code job status=""
  code="$(req POST /api/v2/file-upload/start)"
  [ "$code" = "201" ] || [ "$code" = "200" ] || fail "file-upload/start: HTTP $code: $(cat "$RESPONSE")"
  job="$(jq -r '.data.id' "$RESPONSE")"
  code="$(req POST "/api/v2/file-upload/$job" "$file" application/json "X-File-Upload-Name: $(basename "$file")")"
  [ "$code" = "202" ] || [ "$code" = "200" ] || fail "upload $(basename "$file"): HTTP $code: $(cat "$RESPONSE")"
  code="$(req POST "/api/v2/file-upload/$job/end")"
  [ "$code" = "200" ] || [ "$code" = "201" ] || [ "$code" = "202" ] || fail "file-upload/$job/end: HTTP $code: $(cat "$RESPONSE")"
  for _ in $(seq 1 900); do
    if [ "$(req GET "/api/v2/file-upload?sort_by=-id&limit=50")" = "200" ]; then
      status="$(jq -r --argjson id "$job" '.data[] | select(.id == $id) | .status' "$RESPONSE")"
      case "$status" in -1 | 2 | 3 | 4 | 5 | 8) break ;; esac
    fi
    sleep 1
  done
  [ "$status" = "$want" ] || fail "$(basename "$file"): ingest job $job ended in status ${status:-none}, want $want"
  wait_idle
  code="$(req GET "/api/v2/file-upload/$job/completed-tasks")"
  [ "$code" = "200" ] || fail "file-upload/$job/completed-tasks: HTTP $code"
  cp "$RESPONSE" "$WORK/og-tasks.json"
  echo "    $(basename "$file"): job $job status $status"
}

# cypher LABEL QUERY JQ WANT sends QUERY to POST /api/v2/graphs/cypher and
# requires HTTP 200, JQ's value of the response to equal WANT, and
# BloodTrail to log a Cypher serve for it.
cypher() {
  local label="$1" query="$2" expr="$3" want="$4" code got before
  jq -n --arg q "$query" '{query:$q, include_properties:true}' > "$WORK/og-query.json"
  before="$(marker "bloodtrail: cypher engine served")"
  code="$(req POST /api/v2/graphs/cypher "$WORK/og-query.json")"
  [ "$code" = "200" ] || fail "$label: HTTP $code: $(cat "$RESPONSE")"
  got="$(jq -c "$expr" "$RESPONSE")"
  [ "$got" = "$want" ] || fail "$label: got $got, want $want: $(cat "$RESPONSE")"
  logged_since "bloodtrail: cypher engine served" "$before" ||
    fail "$label: PostgreSQL answered, not the BloodTrail replica (\"cypher engine served\" stayed at $before)"
  echo "    $label: $got"
}

# pathfind LABEL START END WANT_CODE WANT asks the pathfinding endpoint for
# START -> END over traversable edges and requires WANT_CODE, [nodes, edges]
# equal to WANT on a 200, and a path engine serve.
pathfind() {
  local label="$1" start="$2" end="$3" want_code="$4" want="$5" code got before
  before="$(marker "bloodtrail: path engine served")"
  code="$(req GET "/api/v2/graphs/shortest-path?start_node=$start&end_node=$end&only_traversable=true")"
  [ "$code" = "$want_code" ] || fail "$label: HTTP $code, want $want_code: $(cat "$RESPONSE")"
  if [ "$code" = "200" ]; then
    got="$(jq -c '[(.data.nodes | length), (.data.edges | length)]' "$RESPONSE")"
    [ "$got" = "$want" ] || fail "$label: got $got, want $want: $(cat "$RESPONSE")"
  fi
  logged_since "bloodtrail: path engine served" "$before" ||
    fail "$label: PostgreSQL answered, not the BloodTrail replica (\"path engine served\" stayed at $before)"
  echo "    $label: HTTP $code${got:+ $got}"
}

# clear_database BODY queues a "clear database" request and waits for the
# datapipe to run it, which it does on a later tick followed by a fresh
# analysis: last_complete_analysis_at moving is the signal.
clear_database() {
  local body="$1" code before now
  [ "$(req GET /api/v2/datapipe/status)" = "200" ] || fail "datapipe/status: HTTP error"
  before="$(jq -r '.data.last_complete_analysis_at' "$RESPONSE")"
  printf '%s' "$body" > "$WORK/og-clear.json"
  code="$(req POST /api/v2/clear-database "$WORK/og-clear.json")"
  [ "$code" = "204" ] || [ "$code" = "200" ] || [ "$code" = "202" ] || fail "clear-database $body: HTTP $code: $(cat "$RESPONSE")"
  for _ in $(seq 1 900); do
    if [ "$(req GET /api/v2/datapipe/status)" = "200" ]; then
      now="$(jq -r '.data.last_complete_analysis_at' "$RESPONSE")"
      if [ "$now" != "$before" ] && [ "$(jq -r '.data.status' "$RESPONSE")" = "idle" ]; then
        return 0
      fi
    fi
    sleep 1
  done
  fail "clear-database $body did not complete within 15 minutes"
}

rebuilt_before="$(marker "bloodtrail: snapshot rebuilt")"
fallback_before="$(marker "bloodtrail: fallback entered")"

echo "    registering the extension schema"
code="$(req PUT /api/v2/extensions "$FIXTURES/extension.json")"
[ "$code" = "201" ] || [ "$code" = "200" ] || fail "PUT /api/v2/extensions: HTTP $code: $(cat "$RESPONSE")"

echo "    uploading"
upload "$FIXTURES/graph.json" 2
# The third edge names a user no node matches; BloodHound drops it with a
# warning and finishes the job as partially complete.
upload "$FIXTURES/links.json" 8
jq -e '[.data[].warnings // [] | .[] | select(test("nobody"))] | length == 1' "$WORK/og-tasks.json" > /dev/null ||
  fail "links.json: want one warning about the unresolvable \"nobody\" endpoint: $(cat "$WORK/og-tasks.json")"
# A node with four kinds fails validation after the upload registered its
# source kind, leaving a source kind no row carries.
upload "$FIXTURES/failed.json" 5

echo "    querying"
NODES_EDGES='[(.data.nodes | length), (.data.edges | length)]'
cypher "label scan, stub endpoint included" "MATCH (n:ghx_User) RETURN n" "$NODES_EDGES" "[5,0]"
cypher "property filter" "MATCH (n:ghx_Repository) WHERE n.visibility = 'private' RETURN n" "$NODES_EDGES" "[1,0]"
cypher "objectid lookup" "MATCH (n) WHERE n.objectid = 'GHX_USER_4' RETURN n LIMIT 1" "$NODES_EDGES" "[1,0]"
cypher "second kind" "MATCH (n:ghx_OrgOwner) RETURN n" "$NODES_EDGES" "[1,0]"
cypher "AD user gained the source kind" "MATCH (n:ghx_Base) WHERE n.objectid = '$AD_USER' RETURN n" "$NODES_EDGES" "[1,0]"
cypher "team closure" "MATCH p=(u:ghx_User)-[:ghx_MemberOf*1..]->(t:ghx_Team) WHERE u.objectid = 'GHX_USER_2' RETURN p" "$NODES_EDGES" "[3,2]"
cypher "name-matched edge" "MATCH p=(u:ghx_User)-[:ghx_HasRole]->(r:ghx_RepoRole) WHERE u.objectid = 'GHX_USER_3' RETURN p" "$NODES_EDGES" "[2,1]"
cypher "property-matched edge" "MATCH p=(:ghx_RepoRole)-[:ghx_CanWrite]->(:ghx_Repository) RETURN p" "$NODES_EDGES" "[2,1]"
cypher "shortestPath" "MATCH p=shortestPath((u:ghx_User)-[:$EDGES*1..]->(r:ghx_Repository)) WHERE u.objectid = 'GHX_USER_2' AND r.objectid = 'GHX_REPO_1' RETURN p" "$NODES_EDGES" "[5,4]"
# Users at 2, 3 and 4 hops from the repository: PostgreSQL keeps only the
# overall shortest paths here, but every pair's own shortest path when both
# endpoints carry a property constraint.
cypher "allShortestPaths, overall shortest" "MATCH p=allShortestPaths((u:ghx_User)-[:$EDGES*1..]->(r:ghx_Repository)) WHERE r.objectid = 'GHX_REPO_1' RETURN p" "$NODES_EDGES" "[3,2]"
cypher "allShortestPaths, per pair" "MATCH p=allShortestPaths((u:ghx_User)-[:$EDGES*1..]->(r:ghx_Repository)) WHERE u.objectid IN ['GHX_USER_1', 'GHX_USER_2', 'GHX_USER_3'] AND r.objectid = 'GHX_REPO_1' RETURN p" "$NODES_EDGES" "[8,7]"
cypher "hybrid AD-to-OpenGraph path" "MATCH p=shortestPath((u:User)-[:ghx_SyncedTo|$EDGES*1..]->(r:ghx_Repository)) WHERE u.objectid = '$AD_USER' AND r.objectid = 'GHX_REPO_1' RETURN p" "$NODES_EDGES" "[6,5]"
cypher "source kind count" "MATCH (n:ghx_Base) RETURN count(n) AS nodes" '.data.literals[0].value' "14"
cypher "edge kind count" "MATCH ()-[r:ghx_MemberOf]->() RETURN count(r) AS edges" '.data.literals[0].value' "4"

pathfind "pathfinding" GHX_USER_2 GHX_REPO_1 200 "[5,4]"
pathfind "pathfinding, AD to OpenGraph" "$AD_USER" GHX_REPO_1 200 "[6,5]"
pathfind "pathfinding, non-traversable edge only" GHX_ORG_1 GHX_REPO_2 404 ""

echo "    clearing sourceless data, then the source kind"
jq -n '{query:"MATCH (n) RETURN count(n) AS nodes"}' > "$WORK/og-query.json"
[ "$(req POST /api/v2/graphs/cypher "$WORK/og-query.json")" = "200" ] || fail "whole-graph count: HTTP error: $(cat "$RESPONSE")"
total="$(jq -r '.data.literals[0].value' "$RESPONSE")"
clear_database '{"deleteSourceKinds":[0]}'
cypher "sourceless delete keeps every node with a source kind" "MATCH (n) RETURN count(n) AS nodes" '.data.literals[0].value' "$total"
[ "$(req GET /api/v2/graphs/source-kinds)" = "200" ] || fail "graphs/source-kinds: HTTP error"
ghx_id="$(jq -r '.data.kinds[] | select(.name == "ghx_Base") | .id' "$RESPONSE")"
[ -n "$ghx_id" ] || fail "ghx_Base is not a registered source kind: $(cat "$RESPONSE")"
clear_database "{\"deleteSourceKinds\":[$ghx_id]}"
cypher "source kind delete" "MATCH (n:ghx_Base) RETURN count(n) AS nodes" '.data.literals[0].value' "0"
cypher "the AD user that gained the source kind went with it" "MATCH (n:User) WHERE n.objectid = '$AD_USER' RETURN count(n) AS nodes" '.data.literals[0].value' "0"

rebuilt_after="$(marker "bloodtrail: snapshot rebuilt")"
fallback_after="$(marker "bloodtrail: fallback entered")"
[ "$rebuilt_after" -eq "$rebuilt_before" ] || fail "the snapshot was rebuilt during the OpenGraph phase ($rebuilt_before -> $rebuilt_after)"
[ "$fallback_after" -eq "$fallback_before" ] || fail "the engine entered fallback during the OpenGraph phase ($fallback_before -> $fallback_after)"
[ "$CHECK_SERVED" = "0" ] || echo "    no rebuild, no fallback"
