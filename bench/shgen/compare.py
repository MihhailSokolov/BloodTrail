#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Render bench.py reports side by side as a Markdown table.

    python3 compare.py bench-stock-pg.json bench-bloodtrail.json [...]

The FIRST report is the baseline every later one is expressed as a speedup
of, so pass the arm you want to compare against first. Query rows show p50
with the speedup in parentheses; load rows show the phase wall times.

A timing is only worth reading next to the same answer, so the reports are
also checked against each other: every later report must record, for every
query of the first, the same result_signature and result_size, the same error
(or none), and no disagreement among the query's own repeats. Anything else is
listed after the tables and the exit status is 1. (bench.py records both; the
shgen bench.py records only a node count, which is then all that is compared.
A query with LIMIT and no ORDER BY may legitimately return different rows on
two engines: give it an ORDER BY.)
"""
import json, sys


def load(path):
    with open(path) as fh:
        return json.load(fh)


def fmt_ms(v):
    if v is None:
        return "--"
    return f"{v:,.0f} ms" if v >= 10 else f"{v:.1f} ms"


def fmt_s(v):
    if v is None:
        return "--"
    return f"{v/60:.1f} min" if v >= 120 else f"{v:.0f} s"


def speedup(base, other):
    if not base or not other:
        return ""
    if other <= 0:
        return ""
    return f" ({base/other:.1f}x)" if base >= other else f" ({other/base:.1f}x slower)"


def shown(value):
    return "absent" if value is None else str(value)


def why_different(base_label, b, label, o):
    """Why the query's answer on report `label` is not the answer on the first
    report, or None if it is."""
    if b is None or o is None:
        return f"the query is missing from {base_label if b is None else label}"
    unstable = [name for name, q in ((base_label, b), (label, o)) if q.get("repeats_disagree")]
    if unstable:
        return "repeats of the same query returned different answers on " + " and ".join(unstable)
    if b.get("error") or o.get("error"):
        # The same failure on both arms compares nothing but is not a difference.
        if b.get("error") == o.get("error"):
            return None
        return f"error {shown(b.get('error'))} on {base_label}, {shown(o.get('error'))} on {label}"
    for key in ("result_signature", "result_size"):
        if b.get(key) != o.get(key):
            return f"{key} {shown(b.get(key))} on {base_label}, {shown(o.get(key))} on {label}"
    return None


def answer_mismatches(reports):
    """[(query, label, reason)] for every later report's query whose answer is
    not the first report's."""
    base = reports[0]
    found = []
    for r in reports[1:]:
        base_queries, queries = base.get("queries", {}), r.get("queries", {})
        for name in sorted(set(base_queries) | set(queries)):
            reason = why_different(base["label"], base_queries.get(name), r["label"], queries.get(name))
            if reason:
                found.append((name, r["label"], reason))
    return found


def main(paths):
    reports = [load(p) for p in paths]
    labels = [r["label"] for r in reports]
    base = reports[0]
    mismatches = answer_mismatches(reports)
    flagged = {(name, label) for name, label, _ in mismatches}

    print("### Load\n")
    print("| phase | " + " | ".join(labels) + " |")
    print("|---|" + "---|" * len(labels))
    for key, label in (("upload_s", "upload"), ("job_total_s", "ingest + analysis"),
                       ("settle_s", "datapipe settle"), ("end_to_end_s", "**end to end**")):
        cells = []
        for r in reports:
            v = (r.get("load") or {}).get(key)
            cells.append(fmt_s(v))
        print(f"| {label} | " + " | ".join(cells) + " |")

    print("\n### Queries (p50, warm)\n")
    print("| query | " + " | ".join(labels) + " |")
    print("|---|" + "---|" * len(labels))
    for name in base.get("queries", {}):
        cells = []
        b = base["queries"][name].get("p50_ms")
        for i, r in enumerate(reports):
            q = r.get("queries", {}).get(name, {})
            if "error" in q:
                cells.append(f"error: {q['error']}")
                continue
            v = q.get("p50_ms")
            cell = fmt_ms(v) + ("" if i == 0 else speedup(b, v))
            if (name, r["label"]) in flagged:
                cell += " **ANSWER DIFFERS**"
            cells.append(cell)
        print(f"| `{name}` | " + " | ".join(cells) + " |")

    if mismatches:
        print(f"\n### Answers differ ({len(mismatches)})\n")
        print(f"These queries were not answered the same way as on {base['label']}, so their timings compare different work.\n")
        for name, label, reason in mismatches:
            print(f"- `{name}`: {label} differs from {base['label']}: {reason}")
        print(f"compare: {len(mismatches)} query answer(s) differ between the reports; the timings above are not comparable", file=sys.stderr)
    return 1 if mismatches else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1:]))
