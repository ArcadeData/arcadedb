#!/usr/bin/env python3
"""Compare two runs of a query-suite example by their per-query answers.

Example 10 (graph OLAP) prints one line per query, `name: 0.095s, rows=10, hash=aab69106ef02`.
Given the log of an engine's run and the log of the pure-Python reference backend's run on the same
dataset and arguments, this exits non-zero unless both logs name the same queries with the same row
counts and hashes. CI runs it so that a query returning the wrong rows fails the job: a clean exit
only says the queries ran (humemai/arcadedb-embedded-python#12).

Usage: compare_query_hashes.py ENGINE_LOG REFERENCE_LOG
"""

from __future__ import annotations

import re
import sys

LINE = re.compile(r"^(\w+): [0-9.]+s, rows=(\d+), hash=([0-9a-f]+)\s*$")
# A line that starts like a query result but does not parse is a broken result, not log noise: dropping it would let
# the comparison pass on the queries that did parse.
RESULT_START = re.compile(r"^\w+: [0-9.]+s,")


def answers(path: str) -> dict[str, tuple[int, str]]:
    out: dict[str, tuple[int, str]] = {}
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = LINE.match(line.strip())
            if m:
                out[m.group(1)] = (int(m.group(2)), m.group(3))
            elif RESULT_START.match(line.strip()):
                raise SystemExit(
                    f"{path}: malformed query result line: {line.strip()!r}"
                )
    return out


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    engine, reference = answers(sys.argv[1]), answers(sys.argv[2])
    if not engine or not reference:
        print(
            f"no query results parsed (engine {len(engine)}, reference {len(reference)})"
        )
        return 1
    bad = 0
    for name in sorted(set(engine) | set(reference)):
        e, r = engine.get(name), reference.get(name)
        ok = e == r
        bad += not ok
        print(f"{'ok  ' if ok else 'DIFF'} {name}: engine {e}, reference {r}")
    print(
        f"{len(reference) - bad if bad <= len(reference) else 0} of {len(reference)} queries agree"
    )
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
