#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Render bench.py reports side by side as a Markdown table.

    python3 compare.py bench-stock-pg.json bench-bloodtrail.json [...]

The FIRST report is the baseline every later one is expressed as a speedup
of, so pass the arm you want to compare against first. Query rows show p50
with the speedup in parentheses; load rows show the phase wall times.
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


def main(paths):
    reports = [load(p) for p in paths]
    labels = [r["label"] for r in reports]
    base = reports[0]

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
            cells.append(fmt_ms(v) + ("" if i == 0 else speedup(b, v)))
        print(f"| `{name}` | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    main(sys.argv[1:])
