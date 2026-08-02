#!/usr/bin/env python3
#
# Fails when a surefire/failsafe report records a failing test.
#
# The test jobs run Maven with --fail-never, which prints "Build failures were ignored." and exits
# 0 even when a suite failed. Nothing in the Maven step can turn a failing test red, so until now
# the only thing that did was the dorny/test-reporter step: it reads the same XML and, with its
# default fail-on-error, fails itself. That works, but it makes a third-party action that talks to
# the GitHub API the sole arbiter of whether the build passed - an outage or a rate limit there is
# indistinguishable from a test failure, and the only way to make the outage non-fatal is
# continue-on-error, which would equally swallow the failures.
#
# This script is that arbiter instead. It reads the reports off the local disk, so it depends on
# nothing but the files Maven just wrote, and the reporter step above it is free to fail without
# taking the job with it.
#
# An empty result set is a failure too. Reports missing altogether means the suite never ran -
# a module that did not compile, a fork that died before writing anything - and reporting "no
# failures found" for a run that produced no evidence is exactly the false green this guards.
#
# Usage:
#   check-test-results.py surefire-reports            # unit tests
#   check-test-results.py failsafe-reports            # integration tests
#   check-test-results.py surefire-reports --root DIR # scan DIR instead of the repository
#
# @author Luca Garulli (l.garulli@arcadedata.com)

import os
import sys
from pathlib import Path
from xml.etree import ElementTree

# How many failing suites to name before the list is cut short. The reporter step publishes the
# full detail; this is only meant to say why the job is red without scrolling.
MAX_LISTED = 25


def report_files(root, reports_dir):
    """Every TEST*.xml written by surefire/failsafe under a directory named reports_dir."""
    found = []
    for current, directories, files in os.walk(root):
        # Nothing puts reports there, and walking them on a full build costs seconds.
        directories[:] = [d for d in directories if d not in (".git", "node_modules")]
        if Path(current).name != reports_dir:
            continue
        for name in files:
            if name.startswith("TEST") and name.endswith(".xml"):
                found.append(Path(current) / name)
    return sorted(found)


def suite_counts(path):
    """(tests, failures, errors) for one report, or None when the file cannot be read.

    A report surefire could not finish writing - a forked JVM killed mid-flush - is not evidence
    of success, so an unreadable file is reported rather than skipped.
    """
    try:
        root = ElementTree.parse(path).getroot()
    except (ElementTree.ParseError, OSError):
        return None

    # A testsuite may be wrapped in a testsuites element; sum whichever level carries the counts.
    suites = [root] if root.tag == "testsuite" else root.iter("testsuite")

    tests = failures = errors = 0
    for suite in suites:
        tests += int(suite.get("tests", 0))
        failures += int(suite.get("failures", 0))
        errors += int(suite.get("errors", 0))
    return tests, failures, errors


def main(arguments):
    root = Path(__file__).resolve().parents[2]
    positional = []
    index = 0
    while index < len(arguments):
        if arguments[index] == "--root":
            if index + 1 >= len(arguments):
                sys.exit("check-test-results: --root needs a directory")
            root = Path(arguments[index + 1])
            index += 2
        else:
            positional.append(arguments[index])
            index += 1

    if len(positional) != 1:
        sys.exit("usage: check-test-results.py <surefire-reports|failsafe-reports> [--root DIR]")
    reports_dir = positional[0]

    files = report_files(root, reports_dir)
    if not files:
        print(
            "check-test-results: no %s/TEST*.xml under %s.\n"
            "The suite produced no report at all, so there is nothing to prove it ran. Check the\n"
            "Maven step above: a module that failed to compile leaves exactly this trace." % (reports_dir, root),
            file=sys.stderr,
        )
        return 1

    tests = failures = errors = 0
    broken = []
    failed = []
    for path in files:
        counts = suite_counts(path)
        if counts is None:
            broken.append(path)
            continue
        tests += counts[0]
        failures += counts[1]
        errors += counts[2]
        if counts[1] or counts[2]:
            failed.append((path, counts[1], counts[2]))

    print("check-test-results: %d test(s) in %d %s file(s), %d failure(s), %d error(s)" % (tests, len(files), reports_dir, failures, errors))

    if not failed and not broken:
        return 0

    print("", file=sys.stderr)
    for path, suite_failures, suite_errors in failed[:MAX_LISTED]:
        print("  %s: %d failure(s), %d error(s)" % (path.relative_to(root), suite_failures, suite_errors), file=sys.stderr)
    if len(failed) > MAX_LISTED:
        print("  ... and %d more" % (len(failed) - MAX_LISTED), file=sys.stderr)
    for path in broken[:MAX_LISTED]:
        print("  %s: report is unreadable" % path.relative_to(root), file=sys.stderr)
    print(
        "\nMaven ran with --fail-never and exited 0 regardless. The test report is the verdict.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
