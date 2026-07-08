#!/usr/bin/env python3
r"""Convert a suite's JUnit XML into a normalized scenario-id -> status map.

Scenario IDs (pattern ``[A-Z]+-\d{3}``) are embedded in every suite's test
names; this reduces a language/driver-version run to a comparable map keyed by
those IDs so runs can be merged across languages.
"""
import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET

ID_RE = re.compile(r"\b([A-Z]+-\d{3})\b")


def parse_junit(xml_path):
    """Return a {scenario-id: "pass"|"fail"|"skip"} map from a JUnit report.

    A scenario asserted by several testcases fails if any of them fails, and is
    considered a pass if at least one passes and none fail.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()
    results = {}
    for tc in root.iter("testcase"):
        name = tc.get("name", "")
        match = ID_RE.search(name)
        if not match:
            continue
        scenario = match.group(1)
        status = "pass"
        for child in tc:
            tag = child.tag.split("}")[-1]
            if tag in ("failure", "error"):
                status = "fail"
                break
            if tag == "skipped":
                status = "skip"
                break
        prev = results.get(scenario)
        if prev == "fail" or status == "fail":
            results[scenario] = "fail"
        elif prev == "pass" or status == "pass":
            results[scenario] = "pass"
        else:
            results[scenario] = status
    return results


def load_known_ids(spec_path):
    """Collect every scenario id declared in spec.yaml (lines like ``- id: X``)."""
    ids = set()
    with open(spec_path, encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped.startswith("- id:"):
                ids.add(stripped.split("- id:", 1)[1].strip())
    return ids


def build_matrix(xml_path, language, driver_version, known_ids):
    """Build the per-cell record, rejecting scenario ids absent from the spec."""
    scenarios = parse_junit(xml_path)
    unknown = set(scenarios) - set(known_ids)
    if unknown:
        raise ValueError(f"JUnit references unknown scenario IDs: {sorted(unknown)}")
    return {"language": language, "driver_version": driver_version, "scenarios": scenarios}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("junit_xml")
    parser.add_argument("--language", required=True)
    parser.add_argument("--driver-version", required=True)
    parser.add_argument("--spec", required=True, help="path to spec.yaml")
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args(argv)
    known = load_known_ids(args.spec)
    matrix = build_matrix(args.junit_xml, args.language, args.driver_version, known)
    with open(args.output, "w", encoding="utf-8") as fh:
        json.dump(matrix, fh, indent=2, sort_keys=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
