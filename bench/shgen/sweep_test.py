# SPDX-License-Identifier: Apache-2.0
"""Tests for sweep.py's timing. Standard library only:

    python3 -m unittest discover -s bench/shgen -p '*_test.py'
"""
import contextlib
import http.server
import importlib.util
import json
import os
import threading
import time
import unittest

_spec = importlib.util.spec_from_file_location("shgen_sweep", os.path.join(os.path.dirname(os.path.abspath(__file__)), "sweep.py"))
sweep = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sweep)


@contextlib.contextmanager
def local_api(limited=(), latency=0.007):
    """A local HTTP server standing in for BloodHound: request number n
    (1-based, retries included) is answered HTTP 429 with Retry-After: 1 if n
    is in `limited`, and otherwise 200 after `latency` seconds. Yields its port."""
    seen = {"n": 0}
    lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length") or 0))
            with lock:
                seen["n"] += 1
                n = seen["n"]
            if n in limited:
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            time.sleep(latency)
            body = json.dumps({"data": {"nodes": {"1": {}, "2": {}}, "edges": []}}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()


class SweepTimingTest(unittest.TestCase):
    def test_a_rate_limited_query_is_timed_by_its_successful_attempt(self):
        # The 3rd request is answered HTTP 429 with Retry-After: 1, so the client
        # waits a second and retries. That second is not the engine's latency.
        with local_api(limited={3}) as port:
            api = sweep.API(port, pace=0)
            api.token = "t"
            record = sweep.run_one(api, "MATCH (n) RETURN n", 4, 10)
        self.assertEqual(record["status"], "ok", record)
        self.assertEqual(record["result_size"], 2)
        self.assertLess(record["max_ms"], 500, record)
        self.assertGreaterEqual(record["min_ms"], 7, record)

    def test_a_query_that_is_not_rate_limited_is_timed_as_before(self):
        with local_api(latency=0.03) as port:
            api = sweep.API(port, pace=0)
            api.token = "t"
            record = sweep.run_one(api, "MATCH (n) RETURN n", 3, 10)
        self.assertGreaterEqual(record["p50_ms"], 30, record)
        self.assertLess(record["p50_ms"], 500, record)


if __name__ == "__main__":
    unittest.main()
