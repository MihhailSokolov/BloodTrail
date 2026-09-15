#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure one BloodHound deployment end to end on a shgen forest.

Run it once against stock BloodHound and once against the same deployment
with BloodTrail installed, then compare the two JSON reports. It measures
what an operator actually waits for:

  * upload   -- POSTing every collection file
  * ingest   -- upload end to the file-upload job reporting Complete
  * analysis -- the datapipe returning to idle (post-processing, derived
                edges, tagging)
  * queries  -- repeated timings of the three read paths BloodTrail serves:
                pathfinding (/graphs/shortest-path), Cypher
                (/graphs/cypher), and query-builder reads (entity panels)

Every phase is bounded by --deadline; the script never waits unbounded.

  python3 bench.py --port 8181 --data DIR --password PW --label stock \
      [--out report.json] [--repeats 5] [--deadline 7200]
"""
import argparse, glob, json, os, statistics, sys, time, urllib.error, urllib.parse, urllib.request


class API:
    def __init__(self, port, deadline):
        self.base = f"http://127.0.0.1:{port}"
        self.end = time.time() + deadline
        self.token = None

    def left(self):
        remain = self.end - time.time()
        if remain <= 0:
            sys.exit("bench: deadline exceeded")
        return remain

    def call(self, method, path, body=None, ctype=None, headers=None, timeout=None):
        req = urllib.request.Request(self.base + path, data=body, method=method)
        if ctype:
            req.add_header("Content-Type", ctype)
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        limit = min(timeout or 300, self.left())
        with urllib.request.urlopen(req, timeout=limit) as r:
            return r.status, r.read()

    def login(self, user, password):
        body = json.dumps({"login_method": "secret", "username": user, "secret": password}).encode()
        _, data = self.call("POST", "/api/v2/login", body, "application/json")
        self.token = json.loads(data)["data"]["session_token"]


def wait_for_api(api, timeout=600):
    end = time.time() + timeout
    while time.time() < end:
        try:
            code, _ = api.call("GET", "/api/version", timeout=10)
            return
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                return
        except Exception:
            pass
        time.sleep(3)
    sys.exit("bench: API never became ready")


def ingest(api, datadir, poll=5):
    files = sorted(glob.glob(os.path.join(datadir, "*.json")))
    if not files:
        sys.exit(f"bench: no JSON files in {datadir}")
    total_bytes = sum(os.path.getsize(f) for f in files)

    _, data = api.call("POST", "/api/v2/file-upload/start")
    job = json.loads(data)["data"]["id"]

    t0 = time.time()
    for path in files:
        with open(path, "rb") as fh:
            api.call("POST", f"/api/v2/file-upload/{job}", fh.read(), "application/json",
                     {"X-File-Upload-Name": os.path.basename(path)}, timeout=1800)
        print(f"    uploaded {os.path.basename(path)}", flush=True)
    api.call("POST", f"/api/v2/file-upload/{job}/end")
    upload_s = time.time() - t0

    # BloodHound runs ingest AND post-processing before the job reports
    # Complete (status 6 = ingesting, 7 = analyzing), so one wait covers both
    # phases. Timestamp each status transition rather than reporting a single
    # opaque number -- the split between parsing/upserting and analysis is
    # exactly what a with/without comparison wants to see separately.
    t1 = time.time()
    last, phase_start, phases = None, time.time(), {}
    while True:
        api.left()
        _, data = api.call("GET", "/api/v2/file-upload?limit=100")
        job_row = next((j for j in json.loads(data)["data"] if j["id"] == job), None)
        if job_row is None:
            sys.exit("bench: ingest job vanished")
        status = job_row["status"]
        if status != last:
            if last is not None:
                phases[f"status_{last}_s"] = round(time.time() - phase_start, 1)
            print(f"    status {status} at {int(time.time()-t1)}s", flush=True)
            last, phase_start = status, time.time()
        if status in (2, 8):  # Complete / PartiallyComplete
            if job_row.get("failed_files", 0):
                sys.exit(f"bench: ingest finished with failed files: {job_row}")
            break
        if status == 5:
            sys.exit(f"bench: ingest FAILED: {job_row}")
        time.sleep(poll)
    job_s = time.time() - t1

    # Any post-job analysis the datapipe still owes (usually already done).
    t2 = time.time()
    while True:
        api.left()
        _, data = api.call("GET", "/api/v2/datapipe/status")
        if json.loads(data)["data"]["status"] == "idle":
            break
        time.sleep(poll)
    settle_s = time.time() - t2

    return {"files": len(files), "bytes": total_bytes,
            "upload_s": round(upload_s, 1),
            "job_total_s": round(job_s, 1),
            "settle_s": round(settle_s, 1),
            "end_to_end_s": round(upload_s + job_s + settle_s, 1),
            "phases": phases}


def find_objectid(api, query, name_prefix):
    _, data = api.call("GET", "/api/v2/search?q=" + urllib.parse.quote(query))
    for hit in json.loads(data)["data"]:
        if hit["name"].startswith(name_prefix):
            return hit["objectid"]
    sys.exit(f"bench: search found no object named {name_prefix}")


def timed(api, fn, repeats):
    """Runs fn repeats+1 times, discarding the first (warm-up), and returns
    milliseconds p50/p95/min/max plus the result size the last call saw."""
    samples, size = [], None
    for i in range(repeats + 1):
        api.left()
        t = time.time()
        try:
            size = fn()
        except urllib.error.HTTPError as e:
            return {"error": f"HTTP {e.code}", "body": e.read()[:200].decode("utf-8", "replace")}
        except Exception as e:  # noqa: BLE001 - reported, not swallowed
            return {"error": str(e)}
        if i:
            samples.append((time.time() - t) * 1000)
    samples.sort()
    return {
        "p50_ms": round(statistics.median(samples), 1),
        "p95_ms": round(samples[max(0, int(len(samples) * 0.95) - 1)], 1),
        "min_ms": round(samples[0], 1),
        "max_ms": round(samples[-1], 1),
        "n": len(samples), "result_size": size,
    }


def queries(api, repeats):
    svc = find_objectid(api, "SVC-SQL00", "SVC-SQL00@MEGACORP.LOCAL")
    adm = find_objectid(api, "ADMINISTRATOR", "ADMINISTRATOR@MEGACORP.LOCAL")
    foothold = find_objectid(api, "ANNA.SMITH00000", "ANNA.SMITH00000@MEGACORP.LOCAL")
    da = find_objectid(api, "DOMAIN ADMINS", "DOMAIN ADMINS@MEGACORP.LOCAL")
    du = find_objectid(api, "DOMAIN USERS", "DOMAIN USERS@MEGACORP.LOCAL")

    def shortest(a, b):
        def run():
            _, data = api.call("GET", f"/api/v2/graphs/shortest-path?start_node={a}&end_node={b}")
            return len(json.loads(data)["data"].get("nodes") or {})
        return run

    def cypher(text, limit=1000):
        def run():
            body = json.dumps({"query": text, "include_properties": False}).encode()
            _, data = api.call("POST", "/api/v2/graphs/cypher", body, "application/json", timeout=900)
            payload = json.loads(data)["data"]
            return len(payload.get("nodes") or {})
        return run

    def members(group_id, limit=100):
        def run():
            _, data = api.call("GET", f"/api/v2/groups/{urllib.parse.quote(group_id)}/members?limit={limit}")
            return len(json.loads(data).get("data") or [])
        return run

    return {
        # Path engine: the two seeded attack paths.
        "path_kerberoast_to_da": timed(api, shortest(svc, adm), repeats),
        "path_foothold_to_da": timed(api, shortest(foothold, da), repeats),
        # Cypher interpreter: the flagship var-length prebuilt (all Domain
        # Admins by RID suffix), a property scan, and a point lookup.
        "cypher_all_domain_admins": timed(api, cypher(
            "MATCH p = (t:Group)<-[:MemberOf*1..]-(a) WHERE (a:User or a:Computer) "
            "AND t.objectid ENDS WITH '-512' RETURN p LIMIT 1000"), repeats),
        "cypher_kerberoastable": timed(api, cypher(
            "MATCH (u:User) WHERE u.hasspn = true RETURN u LIMIT 1000"), repeats),
        "cypher_objectid_lookup": timed(api, cypher(
            f"MATCH (u:User) WHERE u.objectid = '{svc}' RETURN u"), repeats),
        # Query-builder path: entity-panel membership listings.
        "members_domain_admins": timed(api, members(da), repeats),
        "members_domain_users": timed(api, members(du), repeats),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--user", default="admin")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--deadline", type=int, default=7200)
    ap.add_argument("--out", default=None)
    ap.add_argument("--skip-ingest", action="store_true",
                    help="measure queries only, against data already loaded")
    args = ap.parse_args()

    api = API(args.port, args.deadline)
    wait_for_api(api)
    api.login(args.user, args.password)
    print(f"[{args.label}] logged in", flush=True)

    report = {"label": args.label, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    if not args.skip_ingest:
        print(f"[{args.label}] ingesting {args.data}", flush=True)
        report["load"] = ingest(api, args.data)
        print(f"[{args.label}] load: {report['load']}", flush=True)

    print(f"[{args.label}] timing queries ({args.repeats} repeats each)", flush=True)
    report["queries"] = queries(api, args.repeats)
    for name, res in report["queries"].items():
        print(f"    {name}: {res}", flush=True)

    out = args.out or f"bench-{args.label}.json"
    with open(out, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"[{args.label}] wrote {out}", flush=True)


if __name__ == "__main__":
    main()
