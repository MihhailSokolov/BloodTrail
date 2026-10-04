# SPDX-License-Identifier: Apache-2.0
"""Tests for compare.py: it must refuse to compare timings of different answers.
Standard library only:

    python3 -m unittest discover -s bench/shgen -p '*_test.py'
"""
import json
import os
import subprocess
import sys
import tempfile
import unittest

COMPARE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "compare.py")


def query(p50=10.0, size=(5, 4), signature="aaaaaaaaaaaaaaaa", **extra):
    q = {"p50_ms": p50, "p95_ms": p50 * 1.2, "min_ms": p50 * 0.9, "max_ms": p50 * 1.3, "n": 5,
         "result_size": list(size) if size is not None else None, "result_signature": signature}
    q.update(extra)
    return {k: v for k, v in q.items() if v is not None}


def report(label, **queries):
    return {"label": label, "load": {"upload_s": 10, "job_total_s": 20, "settle_s": 5, "end_to_end_s": 35}, "queries": queries}


def compare(*reports):
    """Runs compare.py on the reports; returns (exit status, stdout, stderr)."""
    with tempfile.TemporaryDirectory() as d:
        paths = []
        for i, r in enumerate(reports):
            path = os.path.join(d, f"report-{i}.json")
            with open(path, "w") as fh:
                json.dump(r, fh)
            paths.append(path)
        done = subprocess.run([sys.executable, COMPARE] + paths, capture_output=True, text=True, timeout=60)
    return done.returncode, done.stdout, done.stderr


class CompareTest(unittest.TestCase):
    def assertRefused(self, *reports, mention):
        code, out, err = compare(*reports)
        self.assertNotEqual(code, 0, f"compare.py accepted reports whose answers differ:\n{out}")
        for word in mention:
            self.assertIn(word, out + err)
        return out + err

    def test_the_same_answers_at_different_speeds_are_compared(self):
        code, out, err = compare(report("stock-pg", q1=query(p50=300.0)), report("bloodtrail", q1=query(p50=7.0)))
        self.assertEqual(code, 0, err)
        self.assertIn("`q1`", out)
        self.assertIn("(42.9x)", out)  # the speedup is still rendered

    def test_a_different_signature_is_refused(self):
        self.assertRefused(report("stock-pg", q1=query(signature="a" * 16)), report("bloodtrail", q1=query(signature="b" * 16)),
                           mention=["q1", "stock-pg", "bloodtrail"])

    def test_a_different_size_is_refused_even_with_equal_signatures(self):
        self.assertRefused(report("stock-pg", q1=query(size=(5, 4))), report("bloodtrail", q1=query(size=(3, 2))), mention=["q1"])

    def test_a_query_whose_repeats_disagreed_is_refused(self):
        flagged = query(signature="DISAGREE", repeats_disagree=True, repeat_signatures=["a" * 16, "b" * 16])
        self.assertRefused(report("stock-pg", q1=query()), report("bloodtrail", q1=flagged), mention=["q1", "bloodtrail"])
        # Two arms that both disagree with themselves have not agreed with each other either.
        self.assertRefused(report("stock-pg", q1=flagged), report("bloodtrail", q1=flagged), mention=["q1"])

    def test_an_error_on_one_arm_is_refused_and_the_same_error_on_both_is_not(self):
        err = {"error": "HTTP 500", "body": "boom"}
        self.assertRefused(report("stock-pg", q1=query()), report("bloodtrail", q1=err), mention=["q1"])
        code, out, stderr = compare(report("stock-pg", q1=err), report("bloodtrail", q1=err))
        self.assertEqual(code, 0, out + stderr)

    def test_a_query_missing_from_an_arm_is_refused(self):
        self.assertRefused(report("stock-pg", q1=query(), q2=query()), report("bloodtrail", q1=query()), mention=["q2"])
        self.assertRefused(report("stock-pg", q1=query()), report("bloodtrail", q1=query(), q2=query()), mention=["q2"])

    def test_a_signature_recorded_on_one_arm_only_is_refused(self):
        self.assertRefused(report("stock-pg", q1=query()), report("bloodtrail", q1=query(signature=None)), mention=["q1"])

    def test_every_later_report_is_checked_against_the_first(self):
        out = self.assertRefused(report("stock-pg", q1=query()), report("bloodtrail", q1=query()),
                                 report("third", q1=query(signature="c" * 16)), mention=["q1", "third"])
        self.assertNotIn("bloodtrail differs", out)

    def test_reports_without_signatures_are_compared_by_size(self):
        # bench/shgen/bench.py records only a node count.
        bare = lambda n: {"p50_ms": 5.0, "p95_ms": 6.0, "min_ms": 4.0, "max_ms": 7.0, "n": 5, "result_size": n}
        code, out, err = compare(report("stock-pg", q1=bare(120)), report("bloodtrail", q1=bare(120)))
        self.assertEqual(code, 0, err)
        self.assertRefused(report("stock-pg", q1=bare(120)), report("bloodtrail", q1=bare(119)), mention=["q1"])


if __name__ == "__main__":
    unittest.main()
