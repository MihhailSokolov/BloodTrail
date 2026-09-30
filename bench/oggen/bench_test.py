# SPDX-License-Identifier: Apache-2.0
"""Tests for bench.py's answer signature. Standard library only:

    python3 -m unittest discover -s bench/oggen -p '*_test.py'
"""
import importlib.util
import os
import unittest

_spec = importlib.util.spec_from_file_location("oggen_bench", os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench.py"))
bench = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bench)


def graph(props=None, edge_props=None, node_ids=("1", "2"), edge_kind="sc_MemberOf", reverse=False, literals=None):
    """A Cypher answer with two nodes and the edge between them; node_ids are
    the database ids BloodHound keys the nodes by."""
    a, b = node_ids
    src, dst = (b, a) if reverse else (a, b)
    edge = {"source": src, "target": dst, "kind": edge_kind}
    if edge_props is not None:
        edge["properties"] = edge_props
    node_a = {"objectId": "SC_USER_1", "kinds": ["sc_User"]}
    if props is not None:
        node_a["properties"] = props
    return {"data": {"nodes": {a: node_a, b: {"objectId": "SC_TEAM_1", "kinds": ["sc_Team"]}},
                     "edges": [edge], "literals": literals}}


def sig(answer):
    return bench.signature(answer)[1]


class SignatureTest(unittest.TestCase):
    def test_a_changed_node_property_changes_the_signature(self):
        base = graph(props={"stars": 1995, "visibility": "private", "topics": ["a", "b"]})
        for name, other in {
            "an integer became a string": graph(props={"stars": "1995", "visibility": "private", "topics": ["a", "b"]}),
            "an integer became a float": graph(props={"stars": 1995.5, "visibility": "private", "topics": ["a", "b"]}),
            "a value became null": graph(props={"stars": 1995, "visibility": None, "topics": ["a", "b"]}),
            "a property is missing": graph(props={"stars": 1995, "topics": ["a", "b"]}),
            "an array changed": graph(props={"stars": 1995, "visibility": "private", "topics": ["a"]}),
            "no properties at all": graph(props=None),
        }.items():
            with self.subTest(name):
                self.assertNotEqual(sig(base), sig(other))

    def test_a_changed_edge_property_changes_the_signature(self):
        self.assertNotEqual(sig(graph(edge_props={"role": "member"})), sig(graph(edge_props={"role": "maintainer"})))
        self.assertNotEqual(sig(graph(edge_props={"role": "member"})), sig(graph(edge_props=None)))

    def test_a_changed_edge_kind_or_direction_changes_the_signature(self):
        base = graph()
        self.assertNotEqual(sig(base), sig(graph(edge_kind="sc_HasRole")))
        self.assertNotEqual(sig(base), sig(graph(reverse=True)))

    def test_literals_are_part_of_the_answer(self):
        self.assertNotEqual(sig(graph(literals=[{"key": "n", "value": 4}])), sig(graph(literals=[{"key": "n", "value": 5}])))

    def test_what_differs_between_two_runs_of_the_same_data_does_not(self):
        # Each arm ingests on its own: database ids differ, node and edge order
        # differ, and BloodHound stamps its own ingest time on every node and edge.
        one = graph(props={"stars": 3, "lastseen": "2026-09-30T08:00:00Z"}, edge_props={"role": "member", "lastseen": "2026-09-30T08:00:01Z"}, node_ids=("1", "2"))
        two = graph(props={"stars": 3, "lastseen": "2026-09-30T09:15:42Z"}, edge_props={"role": "member", "lastseen": "2026-09-30T09:15:43Z"}, node_ids=("70", "9"))
        self.assertEqual(sig(one), sig(two))
        # ... but the same properties on a different node are a different answer.
        other = graph(props={"stars": 3}, edge_props={"role": "member"})
        other["data"]["nodes"]["1"]["objectId"] = "SC_USER_2"
        self.assertNotEqual(sig(one), sig(other))

    def test_size_is_the_node_and_edge_count(self):
        self.assertEqual(bench.signature(graph())[0], [2, 1])
        self.assertEqual(bench.signature({"data": {"nodes": {}, "edges": []}})[0], [0, 0])


class TimedTest(unittest.TestCase):
    """timed() records one signature for a query, from every run it made."""

    @staticmethod
    def run_timed(answers):
        remaining = iter(answers)
        api = bench.API(1, 60)  # fn below never touches the network
        return bench.timed(api, lambda: next(remaining), len(answers) - 1)

    def test_repeats_that_agree_report_their_signature(self):
        a = graph(props={"stars": 1})
        result = self.run_timed([(200, a)] * 4)
        self.assertEqual(result["result_signature"], sig(a))
        self.assertEqual(result["result_size"], [2, 1])
        self.assertNotIn("repeats_disagree", result)

    def test_a_repeat_that_answers_differently_is_flagged(self):
        a, b = graph(props={"stars": 1}), graph(props={"stars": 2})
        # The last answer equals the first: hashing only the last run would see nothing.
        result = self.run_timed([(200, a), (200, a), (200, b), (200, a)])
        self.assertEqual(result["result_signature"], "DISAGREE")
        self.assertTrue(result["repeats_disagree"])
        self.assertEqual(result["repeat_signatures"], [sig(a), sig(a), sig(b), sig(a)])

    def test_the_warm_up_answer_counts_too(self):
        a, b = graph(props={"stars": 1}), graph(props={"stars": 2})
        result = self.run_timed([(200, b), (200, a), (200, a), (200, a)])
        self.assertEqual(result["result_signature"], "DISAGREE")

    def test_an_empty_answer_has_its_own_signature(self):
        self.assertEqual(self.run_timed([(404, None)] * 3)["result_signature"], "HTTP 404")
        self.assertEqual(self.run_timed([(404, None), (200, graph()), (404, None)])["result_signature"], "DISAGREE")


if __name__ == "__main__":
    unittest.main()
