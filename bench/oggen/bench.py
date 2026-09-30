#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Measure one BloodHound deployment on an oggen organization.

Run it against stock BloodHound on PostgreSQL and against the same
deployment with BloodTrail, each starting empty, then render the two
reports with ../shgen/compare.py (same report format), which also fails if
any query's result_signature or size differs between them. The phases:

  * ad       -- optional: a bench/shgen forest uploaded first, so the
                organization's sc_SyncedTo edges start at real AD users
  * load     -- PUT the extension schema, upload every OpenGraph file as one
                job, and wait for ingest, analysis and the datapipe
  * queries  -- repeated timings of Cypher and pathfinding over OpenGraph
                data, warm (the first run of each is discarded)
  * delete   -- optional: "clear database" for the sc_Base source kind,
                until the datapipe has run it and re-analyzed

  python3 bench.py --port 8181 --data OGGEN_OUT --password PW --label bloodtrail \\
      [--ad SHGEN_OUT] [--delete] [--out report.json] [--repeats 5] [--deadline 7200]
"""
import argparse, glob, hashlib, json, os, statistics, sys, time, urllib.error, urllib.request

EDGES = "sc_MemberOf|sc_HasRole|sc_CanWrite|sc_CanAdmin"


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
        """One request, riding out BloodHound's rate limiter (HTTP 429)."""
        delay = 1.0
        for _ in range(10):
            req = urllib.request.Request(self.base + path, data=body, method=method)
            if ctype:
                req.add_header("Content-Type", ctype)
            if self.token:
                req.add_header("Authorization", "Bearer " + self.token)
            for k, v in (headers or {}).items():
                req.add_header(k, v)
            try:
                with urllib.request.urlopen(req, timeout=min(timeout or 900, self.left())) as r:
                    return r.status, r.read()
            except urllib.error.HTTPError as e:
                if e.code != 429:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 16)
        sys.exit(f"bench: {method} {path} still rate-limited")

    def json(self, method, path, obj=None, timeout=None):
        body = json.dumps(obj).encode() if obj is not None else None
        _, data = self.call(method, path, body, "application/json" if body else None, timeout=timeout)
        return json.loads(data) if data else None

    def login(self, user, password):
        self.token = self.json("POST", "/api/v2/login", {"login_method": "secret", "username": user, "secret": password})["data"]["session_token"]


def wait_idle(api, poll=1.0):
    idle = 0
    while idle < 2:
        api.left()
        idle = idle + 1 if api.json("GET", "/api/v2/datapipe/status")["data"]["status"] == "idle" else 0
        time.sleep(poll)


def upload(api, files, poll=1.0):
    """One ingest job for files; the upload, job and datapipe-settle phases."""
    t0 = time.time()
    job = api.json("POST", "/api/v2/file-upload/start")["data"]["id"]
    for path in files:
        with open(path, "rb") as fh:
            api.call("POST", f"/api/v2/file-upload/{job}", fh.read(), "application/json",
                     {"X-File-Upload-Name": os.path.basename(path)}, timeout=1800)
    api.call("POST", f"/api/v2/file-upload/{job}/end")
    upload_s = time.time() - t0

    t1 = time.time()
    while True:
        rows = api.json("GET", "/api/v2/file-upload?sort_by=-id&limit=50")["data"]
        status = next(j["status"] for j in rows if j["id"] == job)
        if status in (2, 8):  # complete, partially complete
            break
        if status in (-1, 3, 4, 5):
            sys.exit(f"bench: ingest job {job} ended in status {status}")
        time.sleep(poll)
    job_s = time.time() - t1

    t2 = time.time()
    wait_idle(api)
    settle_s = time.time() - t2
    return {"files": len(files), "bytes": sum(os.path.getsize(f) for f in files),
            "upload_s": round(upload_s, 1), "job_total_s": round(job_s, 1),
            "settle_s": round(settle_s, 1), "end_to_end_s": round(upload_s + job_s + settle_s, 1)}


# Properties that differ between two ingests of the same data by construction:
# BloodHound stamps the ingest time on every node and edge it writes, and each
# arm ingests on its own.
VOLATILE_PROPERTIES = frozenset({"lastseen"})


def stable_properties(props):
    """A canonical string for a node's or an edge's properties, without the
    ingest-time stamp. Types are part of it: 1995, 1995.0 and "1995" differ."""
    kept = {k: v for k, v in (props or {}).items() if k not in VOLATILE_PROPERTIES}
    return json.dumps(kept, sort_keys=True, separators=(",", ":"))


def signature(resp):
    """A digest of a Cypher or pathfinding answer: every node's objectid, kinds
    and properties, every edge's endpoints, kind and properties, and any
    literals. Database ids, ordering and ingest times are left out, so two
    arms that each ingested the same data digest identically."""
    data = (resp or {}).get("data") or {}
    nodes = data.get("nodes") or {}
    oid = {k: v.get("objectId") for k, v in nodes.items()}
    canon = {
        "nodes": sorted([v.get("objectId") or "", sorted(v.get("kinds") or []), stable_properties(v.get("properties"))] for v in nodes.values()),
        "edges": sorted([oid.get(e["source"]) or "?", oid.get(e["target"]) or "?", e.get("kind") or "", stable_properties(e.get("properties"))] for e in data.get("edges") or []),
        "literals": data.get("literals"),
    }
    size = [len(canon["nodes"]), len(canon["edges"])]
    return size, hashlib.sha1(json.dumps(canon, sort_keys=True).encode()).hexdigest()[:16]


def timed(api, fn, repeats):
    """fn repeats+1 times, the first discarded as warm-up: p50/p95/min/max in
    milliseconds, and the answer's size and signature. Every run's answer is
    hashed, the warm-up's included: runs that disagree with each other are
    reported as result_signature "DISAGREE" with each run's signature in
    repeat_signatures, never as whichever answer came last."""
    samples, sizes, sigs = [], [], []
    for i in range(repeats + 1):
        t = time.time()
        try:
            code, resp = fn()
        except urllib.error.HTTPError as e:
            return {"error": f"HTTP {e.code}", "body": e.read()[:300].decode("utf-8", "replace")}
        if i:
            samples.append((time.time() - t) * 1000)
        size, sig = signature(resp) if code == 200 else ([0, 0], f"HTTP {code}")
        sizes.append(size)
        sigs.append(sig)
    samples.sort()
    result = {"p50_ms": round(statistics.median(samples), 1), "p95_ms": round(samples[max(0, int(len(samples) * 0.95) - 1)], 1),
              "min_ms": round(samples[0], 1), "max_ms": round(samples[-1], 1), "n": len(samples),
              "result_size": sizes[0], "result_signature": sigs[0]}
    if len(set(sigs)) > 1 or any(size != sizes[0] for size in sizes):
        result.update(result_signature="DISAGREE", repeats_disagree=True, repeat_signatures=sigs, repeat_sizes=sizes)
    return result


def queries(api, repeats, ad_user):
    def cypher(text):
        def run():
            try:
                return 200, api.json("POST", "/api/v2/graphs/cypher", {"query": text, "include_properties": True})
            except urllib.error.HTTPError as e:
                if e.code == 404:  # BloodHound's answer for an empty result
                    return 404, None
                raise
        return run

    def pathfind(start, end):
        def run():
            try:
                return 200, api.json("GET", f"/api/v2/graphs/shortest-path?start_node={start}&end_node={end}&only_traversable=true")
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return 404, None
                raise
        return run

    q = {
        "cypher_objectid_lookup": cypher("MATCH (n) WHERE n.objectid = 'SC_USER_0000042' RETURN n"),
        "cypher_property_scan": cypher("MATCH (n:sc_Repository) WHERE n.visibility = 'private' AND n.stars > 1990 RETURN n"),
        "cypher_array_property": cypher("MATCH (n:sc_Repository) WHERE 'security' IN n.topics AND n.archived = true RETURN n"),
        "cypher_team_closure": cypher("MATCH p=(u:sc_User)-[:sc_MemberOf*1..]->(t:sc_Team) WHERE u.objectid = 'SC_USER_0000000' RETURN p"),
        "cypher_user_repo_roles": cypher("MATCH p=(u:sc_User)-[:sc_MemberOf]->(:sc_Team)-[:sc_HasRole]->(:sc_RepoRole) WHERE u.objectid = 'SC_USER_0000002' RETURN p"),
        "cypher_shortest_path": cypher(f"MATCH p=shortestPath((u:sc_User)-[:{EDGES}*1..]->(r:sc_Repository)) WHERE u.objectid = 'SC_USER_0000000' AND r.objectid = 'SC_REPO_000000' RETURN p"),
        "cypher_all_shortest_users_to_repo": cypher(f"MATCH p=allShortestPaths((u:sc_User)-[:{EDGES}*1..]->(r:sc_Repository)) WHERE r.objectid = 'SC_REPO_000000' RETURN p"),
        "cypher_all_shortest_per_pair": cypher(f"MATCH p=allShortestPaths((u:sc_User)-[:{EDGES}*1..]->(r:sc_Repository)) WHERE u.objectid IN ['SC_USER_0000000', 'SC_USER_0000001', 'SC_USER_0000002'] AND r.objectid = 'SC_REPO_000000' RETURN p"),
        "cypher_count_nodes": cypher("MATCH (n:sc_Base) RETURN count(n) AS nodes"),
        "cypher_count_edges": cypher("MATCH ()-[r:sc_HasRole]->() RETURN count(r) AS edges"),
        "cypher_repos_with_team_admins": cypher("MATCH (r:sc_Repository)<-[:sc_CanAdmin]-(:sc_RepoRole)<-[:sc_HasRole]-(:sc_Team) RETURN count(DISTINCT r) AS repos"),
        "path_api_user_to_repo": pathfind("SC_USER_0000000", "SC_REPO_000000"),
    }
    if ad_user:
        q["cypher_hybrid_ad_to_repo"] = cypher(f"MATCH p=shortestPath((u:User)-[:sc_SyncedTo|{EDGES}*1..]->(r:sc_Repository)) WHERE u.objectid = '{ad_user}' AND r.objectid = 'SC_REPO_000000' RETURN p")
        q["path_api_hybrid_ad_to_repo"] = pathfind(ad_user, "SC_REPO_000000")

    results = {}
    for name, fn in q.items():
        results[name] = timed(api, fn, repeats)
        print(f"    {name}: {results[name]}", flush=True)
    return results


def clear_source_kind(api):
    """Clears the sc_Base source kind and waits for the datapipe to run the
    deletion and the analysis after it."""
    kinds = api.json("GET", "/api/v2/graphs/source-kinds")["data"]["kinds"]
    kind_id = next(k["id"] for k in kinds if k["name"] == "sc_Base")
    before = api.json("GET", "/api/v2/datapipe/status")["data"]["last_complete_analysis_at"]
    t0 = time.time()
    api.call("POST", "/api/v2/clear-database", json.dumps({"deleteSourceKinds": [kind_id]}).encode(), "application/json")
    while True:
        api.left()
        st = api.json("GET", "/api/v2/datapipe/status")["data"]
        if st["last_complete_analysis_at"] != before and st["status"] == "idle":
            break
        time.sleep(0.5)
    left = api.json("POST", "/api/v2/graphs/cypher", {"query": "MATCH (n:sc_Base) RETURN count(n) AS nodes"})["data"]["literals"][0]["value"]
    return {"clear_source_kind_s": round(time.time() - t0, 1), "sc_nodes_left": left}


def rss_mb(pid):
    with open(f"/proc/{pid}/status") as fh:
        for line in fh:
            if line.startswith("VmRSS:"):
                return round(int(line.split()[1]) / 1024)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", required=True)
    ap.add_argument("--data", required=True, help="an oggen -out directory")
    ap.add_argument("--password", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--user", default="admin")
    ap.add_argument("--ad", help="a bench/shgen output directory to upload first")
    ap.add_argument("--delete", action="store_true", help="finish by clearing the sc_Base source kind")
    ap.add_argument("--pid", type=int, help="the server's pid, to report its resident memory")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--deadline", type=int, default=7200)
    ap.add_argument("--out")
    args = ap.parse_args()

    api = API(args.port, args.deadline)
    api.login(args.user, args.password)
    report = {"label": args.label, "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

    ad_user = None
    if args.ad:
        ad_files = sorted(glob.glob(os.path.join(args.ad, "*.json")))
        print(f"[{args.label}] uploading the AD forest ({len(ad_files)} files)", flush=True)
        report["ad_load"] = upload(api, ad_files)
        print(f"[{args.label}] ad_load: {report['ad_load']}", flush=True)
        # The organization's user 0 is synced from the first AD user oggen read.
        for path in sorted(glob.glob(os.path.join(args.data, "data", "rels-*.json"))):
            with open(path) as fh:
                synced = [e["start"]["value"] for e in json.load(fh)["graph"]["edges"] if e["kind"] == "sc_SyncedTo" and e["end"]["value"] == "SC_USER_0000000"]
            if synced:
                ad_user = synced[0]
                break

    print(f"[{args.label}] registering the extension schema", flush=True)
    with open(os.path.join(args.data, "extension.json")) as fh:
        api.call("PUT", "/api/v2/extensions", fh.read().encode(), "application/json")

    files = sorted(glob.glob(os.path.join(args.data, "data", "*.json")))
    print(f"[{args.label}] uploading the organization ({len(files)} files)", flush=True)
    report["load"] = upload(api, files)
    print(f"[{args.label}] load: {report['load']}", flush=True)
    if args.pid:
        report["rss_mb_after_load"] = rss_mb(args.pid)

    print(f"[{args.label}] timing queries ({args.repeats} repeats each)", flush=True)
    report["queries"] = queries(api, args.repeats, ad_user)
    if args.pid:
        report["rss_mb_after_queries"] = rss_mb(args.pid)

    if args.delete:
        print(f"[{args.label}] clearing the sc_Base source kind", flush=True)
        report["delete"] = clear_source_kind(api)
        print(f"[{args.label}] delete: {report['delete']}", flush=True)

    out = args.out or f"og-bench-{args.label}.json"
    with open(out, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"[{args.label}] wrote {out}", flush=True)


if __name__ == "__main__":
    main()
