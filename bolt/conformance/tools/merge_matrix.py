#!/usr/bin/env python3
# Merge per-language/-version scenario maps into one compatibility matrix.
# Output is keyed scenario -> language -> driver_version -> status, plus a
# has_failures flag consumed by the nightly report job. The expected cell set is
# read from driver-versions.md so that file is the single source of truth; any
# drift with the job matrices surfaces as a missing_cells / unexpected_cells
# entry on the nightly run.
"""Merge per-language scenario maps into one Bolt compatibility matrix."""
import argparse
import json
import re
import sys

LANGUAGES = {"java", "javascript", "python", "csharp", "go"}
# A driver-versions.md table row: | <language> | <band> | <version> |
CELL_ROW_RE = re.compile(r"^\|\s*([a-z]+)\s*\|\s*[^|]+\|\s*([\w.]+)\s*\|")


def load_expected_cells(md_path):
    """Parse driver-versions.md into a sorted list of language:version cells."""
    cells = set()
    with open(md_path, encoding="utf-8") as fh:
        for line in fh:
            match = CELL_ROW_RE.match(line)
            if match and match.group(1) in LANGUAGES:
                cells.add(f"{match.group(1)}:{match.group(2)}")
    return sorted(cells)


def merge(matrices, expected_cells=None):
    """Combine per-cell records into one scenario-keyed compatibility matrix."""
    # Each expected cell is a "language:driver_version" string. has_failures is
    # forced true for any of: an explicit fail status; a missing cell (expected
    # but no artifact - its job died before emitting JUnit); an unexpected cell
    # (ran but not in driver-versions.md - matrix/doc drift); or an empty cell
    # (ran but produced zero recognized scenarios, e.g. the id never reached the
    # JUnit test name). The empty check is the coverage floor so a suite that
    # silently stops emitting scenarios can't read as a green night.
    expected = set(expected_cells or [])
    scenarios = {}
    languages = set()
    present_cells = set()
    empty = []
    has_failures = False
    for cell in matrices:
        lang = cell["language"]
        ver = cell["driver_version"]
        key = f"{lang}:{ver}"
        languages.add(lang)
        present_cells.add(key)
        if not cell["scenarios"]:
            empty.append(key)
        for scenario, status in cell["scenarios"].items():
            scenarios.setdefault(scenario, {}).setdefault(lang, {})[ver] = status
            if status == "fail":
                has_failures = True
    missing = sorted(expected - present_cells)
    unexpected = sorted(present_cells - expected) if expected else []
    if missing or unexpected or empty:
        has_failures = True
    return {
        "scenarios": scenarios,
        "languages": sorted(languages),
        "missing_cells": missing,
        "unexpected_cells": unexpected,
        "empty_cells": sorted(empty),
        "has_failures": has_failures,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cells", nargs="*", help="per-language JSON files")
    parser.add_argument("--expect-from", default="",
                        help="path to driver-versions.md; its rows are the expected cell set")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args(argv)
    expected = load_expected_cells(args.expect_from) if args.expect_from else []
    matrices = []
    for path in args.cells:
        with open(path, encoding="utf-8") as fh:
            matrices.append(json.load(fh))
    merged = merge(matrices, expected)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, indent=2, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
