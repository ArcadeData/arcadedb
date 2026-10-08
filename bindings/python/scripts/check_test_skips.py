#!/usr/bin/env python3
"""Fail a test run when a test skipped for a reason nobody has accepted.

A skip is a test that did not run. When it skips on "the runtime lacks a feature" or on a
missing dependency, a regression or a broken install turns the suite green having exercised
nothing, and a wrong empty answer can show up as a skip. This script reads the JUnit XML that
pytest wrote and exits 1 for every skip whose message is not on the list below, which is empty.
xfail results are not skips and are not checked here: a strict xfail already fails the suite
when it passes.

usage: check_test_skips.py test-results.xml [--platform linux/amd64]

Every entry names why the test cannot run on that platform, not why it is inconvenient.
Output is ASCII only: this runs on Windows runners, whose console encoding is not UTF-8.
"""

from __future__ import annotations

import argparse
import re
import sys
import xml.etree.ElementTree as ET  # nosec B405 - parses the JUnit file pytest just wrote
from typing import Iterable

# (message pattern, operating systems it is allowed on, why). Empty on purpose: every test runs on
# every platform in CI. A test file that cannot run on one is left out of collection in
# tests/conftest.py (collect_ignore), which reports nothing as skipped. Some skips are unavoidable
# (a Windows limitation, a case that needs an engine fix that is still upstream): add an entry for
# those, with the platform, the reason, and the upstream issue in the third field. For an engine
# bug prefer a strict xfail, which fails the suite as soon as the fix arrives.
ALLOWED_SKIPS: tuple[tuple[str, tuple[str, ...], str], ...] = ()


def skips(path: str) -> Iterable[tuple[str, str]]:
    """Yield (test id, message) for every pytest.skip in the JUnit file."""
    for case in ET.parse(path).getroot().iter("testcase"):  # nosec B314
        for child in case:
            if child.tag == "skipped" and child.get("type") != "pytest.xfail":
                name = f"{case.get('classname')}::{case.get('name')}"
                yield name, (child.get("message") or "").strip()


def unaccepted(path: str, operating_system: str) -> list[tuple[str, str]]:
    bad = []
    for name, message in skips(path):
        for pattern, systems, _why in ALLOWED_SKIPS:
            if operating_system in systems and re.match(pattern, message):
                break
        else:
            bad.append((name, message))
    return bad


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("junit_xml")
    parser.add_argument(
        "--platform",
        default=sys.platform,
        help="matrix platform such as linux/amd64, windows/amd64, darwin/arm64 (default: sys.platform)",
    )
    args = parser.parse_args(argv)
    head = args.platform.split("/")[0].lower()
    operating_system = {"win32": "windows"}.get(head, head)

    bad = unaccepted(args.junit_xml, operating_system)
    if not bad:
        print(f"OK: every skip is an accepted one ({operating_system})")
        return 0
    print(f"ERROR: {len(bad)} test(s) skipped for a reason that is not accepted:")
    for name, message in bad:
        print(f"  {name}: {message[:200]}")
    print()
    print(
        "A skip means the test did not run. Fix the cause (install the package, restore the"
    )
    print(
        "feature), turn the skip into an assertion, or, if the test truly cannot run on this"
    )
    print(
        "platform, add its message to ALLOWED_SKIPS in scripts/check_test_skips.py with the"
    )
    print("reason in the third field.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
