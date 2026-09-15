#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Time BloodHound's ENTIRE shipped query corpus against one deployment.

Where bench.py measures a handful of chosen queries, this runs every
non-disabled pre-built and selector query BloodHound ships (testdata/prebuilt)
and records each one's latency and result size. Run it against a stock
deployment and a BloodTrail one on identical data, then diff the two reports
to find every shape where BloodTrail is SLOWER -- which is the point: it is
built to hunt regressions, not to confirm wins.

  python3 sweep.py --port 8282 --password PW --label stock-pg \
      [--corpus ../../testdata/prebuilt] [--repeats 3] [--timeout 60]

Each query is run --repeats times after one discarded warm-up. A query that
exceeds --timeout is recorded as capped at that value rather than aborting the
sweep, so one pathological shape cannot hide the rest.
"""
import argparse, glob, json, os, statistics, sys, time, urllib.error, urllib.request


def load_corpus(path):
    queries, seen = [], set()
    for f in sorted(glob.glob(os.path.join(path, "*.json"))):
        if os.path.basename(f) == "NOTICE":
            continue
        try:
            entries = json.load(open(f))
        except Exception:
            continue
        if not isinstance(entries, list):
            continue
        for e in entries:
            if not isinstance(e, dict):
                continue
            q = e.get("query")
            if not q or e.get("disabled") or q in seen:
                continue
            seen.add(q)
            queries.append({"name": e.get("name") or "?", "source": os.path.basename(f), "query": q})
    return queries


class API:
    def __init__(self, port, pace=0.25):
        self.base = f"http://127.0.0.1:{port}"
        self.token = None
        self.pace = pace

    def call(self, method, path, body=None, ctype=None, timeout=60):
        req = urllib.request.Request(self.base + path, data=body, method=method)
        if ctype:
            req.add_header("Content-Type", ctype)
        if self.token:
            req.add_header("Authorization", "Bearer " + self.token)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()

    def settle(self):
        """A small fixed pause before each timed attempt, keeping the sweep
        under the API's sustained-rate threshold so the backoff above stays a
        rare fallback rather than the common path."""
        time.sleep(self.pace)

    def login(self, user, password):
        body = json.dumps({"login_method": "secret", "username": user, "secret": password}).encode()
        _, data = self.call("POST", "/api/v2/login", body, "application/json")
        self.token = json.loads(data)["data"]["session_token"]

    def cypher(self, text, timeout):
        """POSTs one Cypher query, transparently riding out the API's rate
        limiter. BloodHound throttles sustained API use (HTTP 429), and a
        sweep of 165 queries x 4 runs each trips it easily -- without this
        every later query would 'measure' the limiter instead of the engine,
        silently turning the back half of a comparison into noise. Backoff
        waits are NOT part of the returned timing: the caller times the
        successful attempt only."""
        body = json.dumps({"query": text, "include_properties": False}).encode()
        delay = 2.0
        for attempt in range(8):
            try:
                return self.call("POST", "/api/v2/graphs/cypher", body, "application/json", timeout=timeout)
            except urllib.error.HTTPError as e:
                if e.code != 429:
                    raise
                retry_after = e.headers.get("Retry-After") if e.headers else None
                wait = float(retry_after) if retry_after and retry_after.isdigit() else delay
                time.sleep(wait)
                delay = min(delay * 2, 30)
        raise RuntimeError("rate limited after 8 attempts")


def run_one(api, query, repeats, timeout):
    """Returns a record for one query: latency stats and result size, or the
    error/timeout that stopped it. A 404 means 'no results', which BloodHound
    returns for an empty graph response -- that is a legitimate timing, not a
    failure, so it is recorded with size 0."""
    samples, size, status = [], None, "ok"
    for i in range(repeats + 1):
        api.settle()
        start = time.time()
        try:
            _, body = api.cypher(query, timeout)
            payload = json.loads(body).get("data") or {}
            size = len(payload.get("nodes") or {})
        except urllib.error.HTTPError as e:
            raw = e.read()[:300].decode("utf-8", "replace")
            if e.code == 404:
                size, status = 0, "empty"
            else:
                return {"status": f"http_{e.code}", "detail": raw}
        except Exception as e:  # timeouts land here
            elapsed = time.time() - start
            if elapsed >= timeout * 0.9:
                return {"status": "timeout", "timeout_s": timeout}
            return {"status": "error", "detail": str(e)[:200]}
        if i:
            samples.append((time.time() - start) * 1000)
    samples.sort()
    return {
        "status": status,
        "p50_ms": round(statistics.median(samples), 1),
        "min_ms": round(samples[0], 1),
        "max_ms": round(samples[-1], 1),
        "result_size": size,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--user", default="admin")
    ap.add_argument("--corpus", default=os.path.join(os.path.dirname(__file__), "..", "..", "testdata", "prebuilt"))
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--pace", type=float, default=0.25, help="seconds to pause before each timed attempt, to stay under the API rate limiter")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    corpus = load_corpus(args.corpus)
    if not corpus:
        sys.exit(f"sweep: no queries found under {args.corpus}")
    print(f"[{args.label}] {len(corpus)} queries", flush=True)

    api = API(args.port, pace=args.pace)
    api.login(args.user, args.password)

    results = []
    for i, entry in enumerate(corpus, 1):
        rec = run_one(api, entry["query"], args.repeats, args.timeout)
        rec.update(name=entry["name"], source=entry["source"], query=entry["query"])
        results.append(rec)
        note = rec.get("p50_ms", rec["status"])
        print(f"  [{i}/{len(corpus)}] {str(note):>10}  {entry['name'][:60]}", flush=True)

    out = args.out or f"sweep-{args.label}.json"
    with open(out, "w") as fh:
        json.dump({"label": args.label, "results": results}, fh, indent=2)
    print(f"[{args.label}] wrote {out}", flush=True)


if __name__ == "__main__":
    main()
