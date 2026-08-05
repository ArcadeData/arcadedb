#!/usr/bin/env python3
#
# Fails when a test-reporter step is allowed to fail in a job that has nothing else to fail on.
#
# `dorny/test-reporter` publishes a check run from the surefire/failsafe XML. It runs no tests, so
# when the GitHub check-run API is contended its failure says nothing about the code - it just
# turns a job red whose tests all passed. That is #5763, and the fix is `continue-on-error: true`
# on the reporter step.
#
# The fix is only safe while something else in the job still fails on a failing test, and that is
# not a given here. Four jobs in mvn-test.yml run Maven with `--fail-never`, which makes the test
# command exit 0 no matter how many tests failed; they carry a `check-test-results.py` step that
# reads the XML and supplies the verdict. Put `continue-on-error` on a reporter in a job shaped
# like that *without* the verdict step and the job can no longer fail at all: every red turns
# green, including the real ones. The nightly load / benchmark / ha-resilience workflows are
# exactly that shape today - `--fail-never` and no verdict step - which is why their reporters
# deliberately do not carry `continue-on-error`.
#
# So the invariant is not "every reporter should be continue-on-error". It is:
#
#     a reporter may only be allowed to fail in a job that can still fail on its own.
#
# This script enforces it. A job is reported when all three hold:
#
#   1. it has a `dorny/test-reporter` step with `continue-on-error: true`,
#   2. some `run:` step in it swallows test failures (`--fail-never`, `-Dmaven.test.failure.ignore`,
#      `-DtestFailureIgnore`, a trailing `|| true`),
#   3. and no step supplies an independent verdict (`check-test-results.py`).
#
# Nothing here guesses at which step "is the tests". A job whose test command exits non-zero on a
# failing test - the six e2e jobs, which run plain `mvnw verify` / `npm test` / `pytest` /
# `dotnet test` / `gotestsum` - already fails on its own, so its reporter is free to be decorative.
#
# Usage:
#   check-test-reporter-guards.py                 # check .github/workflows
#   check-test-reporter-guards.py FILE|DIR ...    # check the given workflows

import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - the workflow installs it explicitly
    sys.exit("check-test-reporter-guards: PyYAML is required (pip install pyyaml)")

REPORTER_ACTION = "dorny/test-reporter"

# The step that turns a failing test back into a failing job when the test command will not.
VERDICT_STEP = "check-test-results.py"

# Flags that make a test command exit 0 with failing tests. `|| true` is anchored at the end of a
# line so a `--fail-never` buried mid-pipeline is still seen, but an unrelated `|| true` guard on a
# `mkdir` is not mistaken for one.
SWALLOWERS = (
    re.compile(r"--fail-never\b"),
    re.compile(r"-Dmaven\.test\.failure\.ignore(=true)?\b"),
    re.compile(r"-DtestFailureIgnore(=true)?\b"),
    re.compile(r"\|\|\s*true\s*$", re.MULTILINE),
)


def action_of(step):
    uses = step.get("uses")
    return uses.split("@", 1)[0].strip() if isinstance(uses, str) else ""


def steps_of(job):
    steps = job.get("steps")
    return [step for step in steps if isinstance(step, dict)] if isinstance(steps, list) else []


def name_of(step, index):
    name = step.get("name")
    return name if isinstance(name, str) and name else step.get("uses") or "step #%d" % (index + 1)


# `continue-on-error` accepts an expression, and an expression cannot be resolved statically. It is
# read as "may be allowed to fail", because a reporter that is conditionally exempt in a job that
# cannot fail on its own is the same hazard on the runs where the condition holds.
def may_continue_on_error(step):
    value = step.get("continue-on-error", False)
    return value is True or (isinstance(value, str) and value.strip().lower() not in ("false", ""))


def swallowed_by(step):
    """The failure-swallowing flag in this step's `run:`, or None."""
    run = step.get("run")
    if not isinstance(run, str):
        return None
    for swallower in SWALLOWERS:
        found = swallower.search(run)
        if found:
            return found.group(0).strip()
    return None


def has_verdict(steps):
    return any(VERDICT_STEP in step.get("run", "") for step in steps if isinstance(step.get("run"), str))


def check_workflow(path):
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        return ["%s: is not valid YAML: %s" % (path, error)]

    jobs = (document or {}).get("jobs")
    if not isinstance(jobs, dict):
        return []

    violations = []
    for job_name, job in jobs.items():
        if not isinstance(job, dict):
            continue
        steps = steps_of(job)
        if has_verdict(steps):
            continue

        exempt = [
            (index, step)
            for index, step in enumerate(steps)
            if action_of(step) == REPORTER_ACTION and may_continue_on_error(step)
        ]
        if not exempt:
            continue

        swallowing = [(index, step, swallowed_by(step)) for index, step in enumerate(steps)]
        swallowing = [(index, step, flag) for index, step, flag in swallowing if flag]
        if not swallowing:
            continue

        for index, step in exempt:
            culprit, culprit_step, flag = swallowing[0]
            violations.append(
                "%s: job '%s', step '%s' is continue-on-error, but step '%s' runs the tests with "
                "'%s' and no '%s' step supplies a verdict\n"
                "    -> nothing in this job can fail on a failing test: add a %s step, or drop "
                "continue-on-error from the reporter"
                % (
                    path,
                    job_name,
                    name_of(step, index),
                    name_of(culprit_step, culprit),
                    flag,
                    VERDICT_STEP,
                    VERDICT_STEP,
                )
            )
    return violations


def workflow_files(arguments):
    default = Path(__file__).resolve().parents[2] / ".github" / "workflows"
    targets = [Path(a) for a in arguments] or [default]
    files = []
    for target in targets:
        if target.is_dir():
            files += sorted(p for p in target.iterdir() if p.suffix in (".yml", ".yaml"))
        elif target.exists():
            files.append(target)
        else:
            sys.exit("check-test-reporter-guards: no such file or directory: %s" % target)
    return files


def main(arguments):
    files = workflow_files(arguments)
    violations = [violation for path in files for violation in check_workflow(path)]

    if violations:
        print("Test-reporter steps exempted from failing a job that cannot fail on its own:\n", file=sys.stderr)
        for violation in violations:
            print("  %s\n" % violation, file=sys.stderr)
        print(
            "A reporter publishes results, it does not produce them, so it should never decide a\n"
            "job (#5763). But a job running with --fail-never has no other verdict unless\n"
            "check-test-results.py gives it one - exempting the reporter there makes the job green\n"
            "on every failure, which is worse than the false red it was meant to remove.",
            file=sys.stderr,
        )
        return 1

    print("check-test-reporter-guards: %d workflow(s) checked, no unguarded reporter exemptions" % len(files))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
