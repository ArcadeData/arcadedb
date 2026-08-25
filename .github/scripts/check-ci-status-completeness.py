#!/usr/bin/env python3
#
# Fails when a workflow's status-aggregator job does not transitively depend on every other job in
# the file.
#
# Context (issue #6569): when `setup` failed in mvn-test.yml, GitHub reported the fifteen jobs
# behind it as `skipping` rather than `failure`. A `skipped` check reads as "nothing to worry about"
# to a reviewer scanning the check list, and branch protection can be satisfied by a check that
# never ran at all - a single missing trailing newline silently took the whole test suite offline
# for every open PR until someone happened to investigate a red `setup` instead of assuming flake.
#
# The fix in mvn-test.yml is a final `ci-status` job with `if: always()` that reads
# `needs.*.result` and fails the run whenever a job it depends on is anything but `success`,
# turning "some required job never even ran" into one explicit, named failure instead of a
# checklist row that looks the same as "passed".
#
# That only holds while the aggregator's `needs` actually reaches every job whose failure or skip
# should be caught. It is very easy to add a new lane below `build-and-package` and forget to wire
# it into the aggregator too - the omission is invisible until the exact incident this exists to
# prevent happens again, for that one lane. This script makes the invariant enforced instead of
# remembered:
#
#     a job named 'ci-status', or whose id ends in '-status', must transitively `need` every
#     other job defined in the same workflow file.
#
# A workflow with no such job is not checked - not every workflow needs an aggregator, and this
# only activates once one opts in by name.
#
# Usage:
#   check-ci-status-completeness.py                 # check .github/workflows
#   check-ci-status-completeness.py FILE|DIR ...    # check the given workflows
#
# @author Luca Garulli (l.garulli@arcadedata.com)

import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - the workflow installs it explicitly
    sys.exit("check-ci-status-completeness: PyYAML is required (pip install pyyaml)")


def workflow_files(argv):
    targets = [Path(a) for a in argv] or [Path(".github/workflows")]
    files = []
    for target in targets:
        if target.is_dir():
            files.extend(sorted(p for p in target.iterdir() if p.suffix in (".yml", ".yaml")))
        else:
            files.append(target)
    return files


def needs_of(job):
    if not isinstance(job, dict):
        return []
    needs = job.get("needs")
    if isinstance(needs, str):
        return [needs]
    return [n for n in needs if isinstance(n, str)] if isinstance(needs, list) else []


def needs_closure(jobs, name, seen=None):
    """Every job `name` transitively depends on, direct or indirect."""
    seen = set() if seen is None else seen
    for parent in needs_of(jobs.get(name) or {}):
        if parent not in seen:
            seen.add(parent)
            needs_closure(jobs, parent, seen)
    return seen


def is_aggregator(job_id):
    return job_id == "ci-status" or job_id.endswith("-status")


def check(path):
    """Violations in one workflow file, as printable strings."""
    try:
        document = yaml.safe_load(path.read_text())
    except yaml.YAMLError as error:
        return [f"{path}: not parseable as YAML: {error}"]

    if not isinstance(document, dict):
        return []
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        return []
    jobs = {job_id: job for job_id, job in jobs.items() if isinstance(job, dict)}

    violations = []
    for job_id in jobs:
        if not is_aggregator(job_id):
            continue

        others = set(jobs) - {job_id}
        if not others:
            continue

        missing = sorted(others - needs_closure(jobs, job_id))
        if missing:
            violations.append(
                f"{path}: aggregator job '{job_id}' does not (transitively) need {missing}. "
                f"A job left out here can fail or be skipped without '{job_id}' ever noticing, "
                f"which is the exact failure mode issue #6569 exists to prevent - add {missing} to "
                f"'{job_id}.needs' (directly, or via a job already in its closure)."
            )
    return violations


def main():
    violations = []
    for path in workflow_files(sys.argv[1:]):
        violations.extend(check(path))

    if violations:
        print("Status-aggregator jobs that do not cover every job in their workflow:\n")
        for violation in violations:
            print(f"  {violation}")
        return 1

    print("check-ci-status-completeness: every aggregator job covers its whole workflow")
    return 0


if __name__ == "__main__":
    sys.exit(main())
