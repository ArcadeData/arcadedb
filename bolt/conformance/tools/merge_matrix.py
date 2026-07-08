#!/usr/bin/env python3
"""
Merge per-language/-version scenario maps into one compatibility matrix.

Output is keyed scenario -> language -> driver_version -> status, plus a
``has_failures`` flag consumed by the nightly report job.
"""
import argparse
import json
import sys


def merge(matrices, expected_cells=None):
    """
    Combine per-cell records into one scenario-keyed compatibility matrix.

    Each expected cell is a ``"language:driver_version"`` string. A cell that is
    expected but absent (its job died before emitting any JUnit) is reported in
    ``missing_cells`` and forces ``has_failures`` true - so a single dead
    driver-version cell can't hide behind its language's other versions and read
    as a healthy night.
    """
    scenarios = {}
    languages = set()
    present_cells = set()
    has_failures = False
    for cell in matrices:
        lang = cell["language"]
        ver = cell["driver_version"]
        languages.add(lang)
        present_cells.add(f"{lang}:{ver}")
        for scenario, status in cell["scenarios"].items():
            scenarios.setdefault(scenario, {}).setdefault(lang, {})[ver] = status
            if status == "fail":
                has_failures = True
    missing = sorted(set(expected_cells or []) - present_cells)
    if missing:
        has_failures = True
    return {
        "scenarios": scenarios,
        "languages": sorted(languages),
        "missing_cells": missing,
        "has_failures": has_failures,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cells", nargs="*", help="per-language JSON files")
    parser.add_argument("--expect-cells", default="",
                        help="comma-separated language:driver_version cells that must be present")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args(argv)
    expected = [cell for cell in args.expect_cells.split(",") if cell]
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
