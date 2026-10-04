#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# OpenGraph phase of build/e2e.sh. Registers an extension schema, uploads
# testdata/opengraph through BloodHound's own file-upload API, and checks
# what BloodHound answers from the BloodTrail driver: Cypher queries, the
# pathfinding endpoint, and "clear database" by source kind. The expected
# answers are derived from the fixtures and pinned here, not read back from the
# engine under test while the script runs, and a graph answer is compared by
# content: the sorted objectIds of its nodes and the sorted (source objectId,
# target objectId, kind) triples of its edges, not just how many there are.
# Their sizes are what stock BloodHound v9.6.0 on PostgreSQL returns for the
# same uploads, with one exception: the whole-graph count after the sourceless
# delete (131, below) came from BloodTrail's own answer in an earlier CI e2e
# run, not from stock PostgreSQL, and is specific to v9.6.0. It also checks
# that BloodTrail served each answer from memory, with no fallback and no
# rebuild.
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

# ANSWER is the jq program that reduces a graph response to what the checks
# compare: the sorted objectIds of its nodes and the sorted [source objectId,
# target objectId, kind] triples of its edges (an edge names its endpoints by
# the response's node keys, which are resolved through .data.nodes).
ANSWER='. as $r | {nodes: ([($r.data.nodes // {})[].objectId] | sort), edges: ([($r.data.edges // [])[] | [$r.data.nodes[.source].objectId, $r.data.nodes[.target].objectId, .kind]] | sort)}'

# graph_answer "NODE..." "SOURCE:KIND:TARGET..." prints, in the same compact
# form ANSWER produces, the answer for a graph with those objectIds and edges.
graph_answer() {
  jq -n -c --arg nodes "$1" --arg edges "$2" '
    {nodes: ($nodes | split(" ") | map(select(. != "")) | sort),
     edges: ($edges | split(" ") | map(select(. != "") | split(":") | [.[0], .[2], .[1]]) | sort)}'
}

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

# login_token BODY prints a session token, or nothing if there is none to get.
# BloodHound's rate limiter (HTTP 429) covers the login endpoint too, so a login
# that lands just after a burst of requests waits and tries again instead of
# failing the phase with an empty token.
login_token() {
  local body="$1" out code delay=1 _
  out="$(mktemp)"
  for _ in 1 2 3 4 5 6; do
    code="$(bounded_curl -s -o "$out" -w '%{http_code}' -X POST "$BASE_URL/api/v2/login" -H 'Content-Type: application/json' -d "$body")" || code=000
    [ "$code" = "429" ] || break
    sleep "$delay"
    delay=$((delay * 2))
  done
  if [ "$code" = "200" ]; then jq -r '.data.session_token // empty' "$out"; fi
  rm -f "$out"
}

LOGIN_BODY="$(jq -n --arg p "$PASSWORD" '{login_method:"secret", username:"admin", secret:$p}')"
TOKEN="$(login_token "$LOGIN_BODY")"
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
# START -> END over traversable edges and requires WANT_CODE, the graph
# answer (see ANSWER) equal to WANT on a 200, and a path engine serve.
pathfind() {
  local label="$1" start="$2" end="$3" want_code="$4" want="$5" code got before
  before="$(marker "bloodtrail: path engine served")"
  code="$(req GET "/api/v2/graphs/shortest-path?start_node=$start&end_node=$end&only_traversable=true")"
  [ "$code" = "$want_code" ] || fail "$label: HTTP $code, want $want_code: $(cat "$RESPONSE")"
  if [ "$code" = "200" ]; then
    got="$(jq -c "$ANSWER" "$RESPONSE")"
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
# What the fixtures make of these uploads: graph.json's twelve nodes, the stub
# GHX_USER_9 that one of its edges creates, the AD user that its hybrid edge
# gives the source kind, and its eleven edges; links.json resolves two more
# and drops its third, the "nobody" edge. The path answers are built from
# these edges, written SOURCE:KIND:TARGET.
U1_MEMBER_T1="GHX_USER_1:ghx_MemberOf:GHX_TEAM_1"
U2_MEMBER_T2="GHX_USER_2:ghx_MemberOf:GHX_TEAM_2"
T2_MEMBER_T1="GHX_TEAM_2:ghx_MemberOf:GHX_TEAM_1"
T1_HASROLE_R2="GHX_TEAM_1:ghx_HasRole:GHX_ROLE_2"
R2_CANWRITE_REPO1="GHX_ROLE_2:ghx_CanWrite:GHX_REPO_1"
U3_HASROLE_R1="GHX_USER_3:ghx_HasRole:GHX_ROLE_1"
R1_CANADMIN_REPO1="GHX_ROLE_1:ghx_CanAdmin:GHX_REPO_1"
AD_SYNCED_U2="$AD_USER:ghx_SyncedTo:GHX_USER_2"
# GHX_USER_2 reaches the repository over four edges, and the AD user over five.
USER2_PATH_NODES="GHX_USER_2 GHX_TEAM_2 GHX_TEAM_1 GHX_ROLE_2 GHX_REPO_1"
USER2_PATH_EDGES="$U2_MEMBER_T2 $T2_MEMBER_T1 $T1_HASROLE_R2 $R2_CANWRITE_REPO1"

cypher "label scan, stub endpoint included" "MATCH (n:ghx_User) RETURN n" "$ANSWER" \
  "$(graph_answer "GHX_USER_1 GHX_USER_2 GHX_USER_3 GHX_USER_4 GHX_USER_9" "")"
cypher "property filter" "MATCH (n:ghx_Repository) WHERE n.visibility = 'private' RETURN n" "$ANSWER" \
  "$(graph_answer "GHX_REPO_1" "")"
cypher "objectid lookup" "MATCH (n) WHERE n.objectid = 'GHX_USER_4' RETURN n LIMIT 1" "$ANSWER" \
  "$(graph_answer "GHX_USER_4" "")"
cypher "second kind" "MATCH (n:ghx_OrgOwner) RETURN n" "$ANSWER" \
  "$(graph_answer "GHX_USER_4" "")"
cypher "AD user gained the source kind" "MATCH (n:ghx_Base) WHERE n.objectid = '$AD_USER' RETURN n" "$ANSWER" \
  "$(graph_answer "$AD_USER" "")"
cypher "team closure" "MATCH p=(u:ghx_User)-[:ghx_MemberOf*1..]->(t:ghx_Team) WHERE u.objectid = 'GHX_USER_2' RETURN p" "$ANSWER" \
  "$(graph_answer "GHX_USER_2 GHX_TEAM_2 GHX_TEAM_1" "$U2_MEMBER_T2 $T2_MEMBER_T1")"
cypher "name-matched edge" "MATCH p=(u:ghx_User)-[:ghx_HasRole]->(r:ghx_RepoRole) WHERE u.objectid = 'GHX_USER_3' RETURN p" "$ANSWER" \
  "$(graph_answer "GHX_USER_3 GHX_ROLE_1" "$U3_HASROLE_R1")"
cypher "property-matched edge" "MATCH p=(:ghx_RepoRole)-[:ghx_CanWrite]->(:ghx_Repository) RETURN p" "$ANSWER" \
  "$(graph_answer "GHX_ROLE_2 GHX_REPO_1" "$R2_CANWRITE_REPO1")"
cypher "shortestPath" "MATCH p=shortestPath((u:ghx_User)-[:$EDGES*1..]->(r:ghx_Repository)) WHERE u.objectid = 'GHX_USER_2' AND r.objectid = 'GHX_REPO_1' RETURN p" "$ANSWER" \
  "$(graph_answer "$USER2_PATH_NODES" "$USER2_PATH_EDGES")"
# Users at 2, 3 and 4 hops from the repository: PostgreSQL keeps only the
# overall shortest paths here (GHX_USER_3's two edges), but every pair's own
# shortest path when both endpoints carry a property constraint.
cypher "allShortestPaths, overall shortest" "MATCH p=allShortestPaths((u:ghx_User)-[:$EDGES*1..]->(r:ghx_Repository)) WHERE r.objectid = 'GHX_REPO_1' RETURN p" "$ANSWER" \
  "$(graph_answer "GHX_USER_3 GHX_ROLE_1 GHX_REPO_1" "$U3_HASROLE_R1 $R1_CANADMIN_REPO1")"
cypher "allShortestPaths, per pair" "MATCH p=allShortestPaths((u:ghx_User)-[:$EDGES*1..]->(r:ghx_Repository)) WHERE u.objectid IN ['GHX_USER_1', 'GHX_USER_2', 'GHX_USER_3'] AND r.objectid = 'GHX_REPO_1' RETURN p" "$ANSWER" \
  "$(graph_answer "GHX_USER_1 GHX_USER_2 GHX_USER_3 GHX_TEAM_1 GHX_TEAM_2 GHX_ROLE_1 GHX_ROLE_2 GHX_REPO_1" "$U1_MEMBER_T1 $U2_MEMBER_T2 $T2_MEMBER_T1 $T1_HASROLE_R2 $R2_CANWRITE_REPO1 $U3_HASROLE_R1 $R1_CANADMIN_REPO1")"
cypher "hybrid AD-to-OpenGraph path" "MATCH p=shortestPath((u:User)-[:ghx_SyncedTo|$EDGES*1..]->(r:ghx_Repository)) WHERE u.objectid = '$AD_USER' AND r.objectid = 'GHX_REPO_1' RETURN p" "$ANSWER" \
  "$(graph_answer "$AD_USER $USER2_PATH_NODES" "$AD_SYNCED_U2 $USER2_PATH_EDGES")"
cypher "source kind count" "MATCH (n:ghx_Base) RETURN count(n) AS nodes" '.data.literals[0].value' "14"
cypher "edge kind count" "MATCH ()-[r:ghx_MemberOf]->() RETURN count(r) AS edges" '.data.literals[0].value' "4"

pathfind "pathfinding" GHX_USER_2 GHX_REPO_1 200 "$(graph_answer "$USER2_PATH_NODES" "$USER2_PATH_EDGES")"
pathfind "pathfinding, AD to OpenGraph" "$AD_USER" GHX_REPO_1 200 "$(graph_answer "$AD_USER $USER2_PATH_NODES" "$AD_SYNCED_U2 $USER2_PATH_EDGES")"
pathfind "pathfinding, non-traversable edge only" GHX_ORG_1 GHX_REPO_2 404 ""

echo "    clearing sourceless data, then the source kind"
clear_database '{"deleteSourceKinds":[0]}'
# Every node in the graph has a source kind, so clearing the sourceless data
# removes nothing. The count is pinned rather than read from the engine before
# the delete, which would make this check agree with whatever the engine
# serves, right or wrong: 131 is the 118 nodes that BloodHound v9.6.0's ingest
# and analysis of the SharpHound fixture leave before this phase (the synced AD
# user is one of them) plus the 13 that graph.json adds, its 12 nodes and the
# stub GHX_USER_9.
# That sum agrees with the 131 BloodTrail answered in the CI e2e run on main
# before this count was pinned (run 36621234497), which is where the number came
# from: it is not a measurement on stock PostgreSQL, and it holds for v9.6.0's
# ingest and analysis only, so re-derive it when the BloodHound version this
# validates changes.
cypher "sourceless delete keeps every node with a source kind" "MATCH (n) RETURN count(n) AS nodes" '.data.literals[0].value' "131"
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
