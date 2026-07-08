#!/usr/bin/env python3
# Scenario IDs (pattern [A-Z]+-\d{3}) are embedded in every suite's test names;
# this reduces a language/driver-version run to a comparable map keyed by those
# IDs so runs can be merged across languages.
r"""Convert a suite's JUnit XML into a normalized scenario-id -> status map."""
import argparse
import json
import re
import sys
# The JUnit XML consumed here is produced by our own CI test runners
# (jest-junit, pytest, gotestsum, JunitXml.TestLogger, Maven failsafe), never by
# an external/untrusted source, so the stdlib parser's XML-entity concerns do not
# apply on this trust boundary.
import xml.etree.ElementTree as ET  # nosec B405

# Suites encode the scenario id in test names with either a hyphen
# (js/csharp/java "CONN-001") or an underscore (Go func "Test_CONN_001_...");
# both normalize to the hyphenated spec id. A plain \b word boundary can't be
# used because the underscore in "Test_CONN_001" is itself a word character, so
# letter/digit lookarounds delimit the id instead. The prefix is 1+ letters to
# match SPEC_ID_RE and the documented [A-Z]+-\d{3} pattern.
ID_RE = re.compile(r"(?<![A-Za-z])([A-Z]+)[-_](\d{3})(?!\d)")
SPEC_ID_RE = re.compile(r"^\s*-\s*id:\s*([A-Z]+-\d{3})\b")


def _local_name(tag):
    """Strip any ``{namespace}`` prefix from an XML tag."""
    return tag.split("}")[-1]


def parse_junit(xml_path):
    """Return a {scenario-id: "pass"|"fail"|"skip"} map from a JUnit report."""
    # A scenario asserted by several testcases fails if any of them fails, and is
    # considered a pass if at least one passes and none fail. Tags are matched by
    # local name so a namespaced JUnit document is handled the same as a plain one.
    tree = ET.parse(xml_path)  # nosec B314 - trusted CI-generated JUnit (see import note)
    root = tree.getroot()
    results = {}
    for tc in root.iter():
        if _local_name(tc.tag) != "testcase":
            continue
        name = tc.get("name", "")
        match = ID_RE.search(name)
        if not match:
            continue
        scenario = f"{match.group(1)}-{match.group(2)}"
        status = "pass"
        for child in tc:
            tag = _local_name(child.tag)
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
            match = SPEC_ID_RE.match(line)
            if match:
                ids.add(match.group(1))
    return ids


def build_matrix(xml_path, language, driver_version, known_ids):
    """Build the per-cell record, dropping scenario ids absent from the spec."""
    # Unknown ids are warned about and skipped rather than raised: this tool runs
    # inside a monitoring workflow where hard-failing the conversion would drop
    # the whole cell and hide the scenarios that did run. A cell that ends up with
    # zero recognized scenarios is caught downstream by merge_matrix's empty check.
    scenarios = parse_junit(xml_path)
    unknown = sorted(set(scenarios) - set(known_ids))
    if unknown:
        print(f"warning: dropping scenario IDs not in spec.yaml: {unknown}", file=sys.stderr)
        for scenario in unknown:
            del scenarios[scenario]
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
