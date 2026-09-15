#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Diff two sweep.py reports, leading with wherever the second one is SLOWER.

    python3 sweepdiff.py sweep-stock-pg.json sweep-bloodtrail.json

The first report is the baseline. Output is ordered worst-regression-first,
because the question this answers is "where does BloodTrail lose?", not "how
often does it win". Result-size disagreements are reported separately and
matter more than any timing: they mean the two engines answered differently.
"""
import json, sys


def load(path):
    d = json.load(open(path))
    return d["label"], {r["query"]: r for r in d["results"]}


def main(base_path, other_path, slow_floor_ms=5.0, ratio_floor=1.2):
    base_label, base = load(base_path)
    other_label, other = load(other_path)

    regressions, wins, mismatches, statuses = [], [], [], []

    for q, b in base.items():
        o = other.get(q)
        if o is None:
            continue
        name = b.get("name", "?")
        if b.get("status") != o.get("status"):
            statuses.append((name, b.get("status"), o.get("status"), q))
            continue
        if b.get("status") not in ("ok", "empty"):
            continue
        if b.get("result_size") != o.get("result_size"):
            mismatches.append((name, b.get("result_size"), o.get("result_size"), q))
        bp, op = b.get("p50_ms"), o.get("p50_ms")
        if bp is None or op is None:
            continue
        # Ignore differences that are only noise on already-fast queries.
        if max(bp, op) < slow_floor_ms:
            continue
        if op > bp * ratio_floor:
            regressions.append((op / bp, bp, op, name, q))
        elif bp > op * ratio_floor:
            wins.append((bp / op, bp, op, name, q))

    print(f"baseline: {base_label}    compared: {other_label}")
    print(f"queries compared: {len(base)}\n")

    if mismatches:
        print(f"## RESULT-SIZE DISAGREEMENTS ({len(mismatches)}) -- correctness, not speed\n")
        for name, bs, os_, q in mismatches:
            print(f"  {name}: {base_label}={bs} {other_label}={os_}")
            print(f"      {q[:150]}")
        print()

    if statuses:
        print(f"## STATUS DIFFERENCES ({len(statuses)})\n")
        for name, bs, os_, q in statuses:
            print(f"  {name}: {base_label}={bs} {other_label}={os_}")
            print(f"      {q[:150]}")
        print()

    print(f"## SLOWER on {other_label} ({len(regressions)})\n")
    if not regressions:
        print("  (none above the noise floor)\n")
    for ratio, bp, op, name, q in sorted(regressions, reverse=True):
        print(f"  {ratio:5.1f}x slower  {bp:8.1f} -> {op:8.1f} ms   {name}")
        print(f"      {q[:150]}")
    print()

    print(f"## Faster on {other_label} ({len(wins)}) -- top 10\n")
    for ratio, bp, op, name, _ in sorted(wins, reverse=True)[:10]:
        print(f"  {ratio:5.1f}x faster  {bp:8.1f} -> {op:8.1f} ms   {name}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
