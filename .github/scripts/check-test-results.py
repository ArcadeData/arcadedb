#!/usr/bin/env python3
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
# SPDX-License-Identifier: Apache-2.0
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
# So is a test class whose every test ABORTED (issue #11209). Surefire records a failed assumption
# (assumeTrue, Assumptions.abort, JUnit 4's AssumptionViolatedException) as a skipped test case,
# not as a failure, so a class that aborted everything counted towards the tests and towards
# nothing else: the lane reported clean over a class that asserted nothing, the same false green
# reached through a different attribute. An aborted case is told from a disabled one (@Disabled,
# @EnabledIf, a tag) by the exception type surefire writes on its <skipped> element; a disabled
# test is a decision taken in the source, an abort is the runner's state deciding, so only the
# second is evidence that went missing. A class that aborts only some of its tests still ran the
# rest, and an assumption is often deliberate (a filesystem that reserves no blocks, a graph
# feature TinkerPop's suite gates on), so those are not failures: every aborted case is listed in
# the log instead, with its reason, so an assumption reading live state (#11030) is visible on the
# run where it fired rather than only once someone thinks to look. A class that is meant to run on
# some runners only (a filesystem, an OS, a tool on the PATH) says so with a condition annotation -
# @EnabledIf, @EnabledOnOs, @DisabledIf - which surefire records as disabled, not as aborted: that
# is the opt-out, and it keeps the decision in the source where a reviewer sees it.
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
    """(tests, failures, errors, skipped, cases, aborted) for one report, or None when the file cannot be read.

    cases is the number of <testcase> elements and aborted the (name, reason) of every case whose
    <skipped> element names an exception type: an assumption that failed, as opposed to a test
    disabled in the source, whose <skipped> carries a message only.

    A report surefire could not finish writing - a forked JVM killed mid-flush - is not evidence
    of success, so an unreadable file is reported rather than skipped.
    """
    try:
        root = ElementTree.parse(path).getroot()
    except (ElementTree.ParseError, OSError):
        return None

    # A testsuite may be wrapped in a testsuites element; sum whichever level carries the counts.
    suites = [root] if root.tag == "testsuite" else root.iter("testsuite")

    tests = failures = errors = skipped = cases = 0
    aborted = []
    for suite in suites:
        tests += int(suite.get("tests", 0))
        failures += int(suite.get("failures", 0))
        errors += int(suite.get("errors", 0))
        skipped += int(suite.get("skipped", 0))
        for case in suite.iter("testcase"):
            cases += 1
            skip = case.find("skipped")
            if skip is not None and skip.get("type"):
                aborted.append((case.get("name", "?"), abort_reason(skip)))
    return tests, failures, errors, skipped, cases, aborted


def abort_reason(skip):
    """The first line of what an aborted case says about why, without the exception class surefire prefixes."""
    text = (skip.get("message") or skip.text or "").strip()
    line = text.splitlines()[0] if text else skip.get("type")
    exception = skip.get("type") + ": "
    return line[len(exception):] if line.startswith(exception) else line


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

    tests = failures = errors = skipped = aborted = 0
    broken = []
    failed = []
    aborting = []
    all_aborted = []
    for path in files:
        counts = suite_counts(path)
        if counts is None:
            broken.append(path)
            continue
        tests += counts[0]
        failures += counts[1]
        errors += counts[2]
        skipped += counts[3]
        aborted += len(counts[5])
        if counts[1] or counts[2]:
            failed.append((path, counts[1], counts[2]))
        if counts[5]:
            aborting.append((path, counts[4], counts[5]))
            # Every case of the class aborted: it ran, and asserted nothing
            if len(counts[5]) == counts[4]:
                all_aborted.append((path, counts[4], counts[5][0][1]))

    print(
        "check-test-results: %d test(s) in %d %s file(s), %d failure(s), %d error(s), %d skipped (%d aborted by an assumption)"
        % (tests, len(files), reports_dir, failures, errors, skipped, aborted)
    )

    # Listed on a green run too: an assumption that fires is a test that did not run its assertions here, and the log
    # is the only place that says so
    if aborting:
        print("\nAborted by an assumption, so not asserted on this run:")
        for path, cases, suite_aborted in aborting[:MAX_LISTED]:
            print("  %s: %d of %d test(s) aborted, e.g. %s: %s" % (path.relative_to(root), len(suite_aborted), cases, suite_aborted[0][0], suite_aborted[0][1]))
        if len(aborting) > MAX_LISTED:
            print("  ... and %d more" % (len(aborting) - MAX_LISTED))

    if not failed and not broken and not all_aborted:
        return 0

    print("", file=sys.stderr)
    for path, suite_failures, suite_errors in failed[:MAX_LISTED]:
        print("  %s: %d failure(s), %d error(s)" % (path.relative_to(root), suite_failures, suite_errors), file=sys.stderr)
    if len(failed) > MAX_LISTED:
        print("  ... and %d more" % (len(failed) - MAX_LISTED), file=sys.stderr)
    for path in broken[:MAX_LISTED]:
        print("  %s: report is unreadable" % path.relative_to(root), file=sys.stderr)
    for path, cases, reason in all_aborted[:MAX_LISTED]:
        print("  %s: every one of its %d test(s) aborted, so the class asserted nothing (%s)" % (path.relative_to(root), cases, reason), file=sys.stderr)
    if len(all_aborted) > MAX_LISTED:
        print("  ... and %d more" % (len(all_aborted) - MAX_LISTED), file=sys.stderr)
    print(
        "\nMaven ran with --fail-never and exited 0 regardless. The test report is the verdict.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
