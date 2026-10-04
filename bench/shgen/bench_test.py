# SPDX-License-Identifier: Apache-2.0
"""Tests for bench.py's timing. Standard library only:

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

_spec = importlib.util.spec_from_file_location("shgen_bench", os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench.py"))
bench = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench)


@contextlib.contextmanager
def local_api(latencies=()):
    """A local HTTP server standing in for BloodHound. Request number n
    (1-based) is answered 200 after latencies[n-1] seconds (7 ms past the end of
    the list). Yields its port."""
    seen = {"n": 0}
    lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            with lock:
                seen["n"] += 1
                n = seen["n"]
            time.sleep(latencies[n - 1] if n - 1 < len(latencies) else 0.007)
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


def shortest_path(api, decode_s=0.0):
    """What bench.py's own query runners do: one request, then decoding the answer."""
    def run():
        _, data = api.call("GET", "/api/v2/graphs/shortest-path?start_node=a&end_node=b")
        time.sleep(decode_s)
        return len(json.loads(data)["data"].get("nodes") or {})
    return run


class TimingTest(unittest.TestCase):
    """What timed() reports as latency."""

    def test_the_95th_percentile_of_five_samples_is_the_slowest(self):
        # One discarded warm-up, then 10, 20, 30, 40 and 200 ms. The old "p95" was
        # rank 4 of 5, the 40 ms one, and hid the 200 ms outlier.
        with local_api(latencies=[0.0, 0.01, 0.02, 0.03, 0.04, 0.2]) as port:
            api = bench.API(port, 60)
            result = bench.timed(api, shortest_path(api), 5)
        self.assertEqual(result["n"], 5)
        self.assertGreaterEqual(result["p95_ms"], 190, result)
        self.assertEqual(result["p95_ms"], result["max_ms"])

    def test_percentile_is_the_nearest_rank(self):
        self.assertEqual(bench.percentile([7], 95), 7)
        self.assertEqual(bench.percentile([1, 2, 3, 4, 5], 95), 5)
        self.assertEqual(bench.percentile(list(range(1, 8)), 95), 7)
        self.assertEqual(bench.percentile(list(range(1, 21)), 95), 19)
        self.assertEqual(bench.percentile(list(range(1, 101)), 95), 95)
        self.assertEqual(bench.percentile(list(range(1, 101)), 50), 50)

    def test_a_sample_is_the_request_not_what_is_done_with_its_answer(self):
        # Decoding the answer takes 300 ms here; the server answers in about 7 ms.
        with local_api() as port:
            api = bench.API(port, 60)
            result = bench.timed(api, shortest_path(api, decode_s=0.3), 3)
        self.assertLess(result["max_ms"], 200, result)
        self.assertGreaterEqual(result["min_ms"], 7, result)
        self.assertEqual(result["result_size"], 2)


if __name__ == "__main__":
    unittest.main()
