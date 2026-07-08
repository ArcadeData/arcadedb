#!/usr/bin/env python3
"""Merge per-language/-version scenario maps into one compatibility matrix.

Output is keyed scenario -> language -> driver_version -> status, plus a
``has_failures`` flag consumed by the nightly report job.
"""
import argparse
import json
import sys


def merge(matrices):
    """Combine per-cell records into one scenario-keyed compatibility matrix."""
    scenarios = {}
    languages = set()
    has_failures = False
    for cell in matrices:
        lang = cell["language"]
        ver = cell["driver_version"]
        languages.add(lang)
        for scenario, status in cell["scenarios"].items():
            scenarios.setdefault(scenario, {}).setdefault(lang, {})[ver] = status
            if status == "fail":
                has_failures = True
    return {
        "scenarios": scenarios,
        "languages": sorted(languages),
        "has_failures": has_failures,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cells", nargs="+", help="per-language JSON files")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args(argv)
    matrices = []
    for path in args.cells:
        with open(path, encoding="utf-8") as fh:
            matrices.append(json.load(fh))
    merged = merge(matrices)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, indent=2, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
