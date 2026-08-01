#!/usr/bin/env python3
#
# Fails when a workflow job downloads an artifact it does not wait for.
#
# `actions/download-artifact` reads the artifact store of the *current run*. It has no
# happens-before relationship with the job that uploads: if the producer has not finished by the
# time the consumer runs, the artifact simply is not there yet. GitHub expresses that ordering
# only through `needs`, and nothing in the actions themselves checks it - a consumer with the
# wrong `needs` is valid YAML that runs green and silently reads an incomplete input.
#
# That is how #5701 happened. `coverage-report` downloaded `ha-integration-coverage-reports` but
# listed only `[unit-tests, integration-tests, slow-unit-tests]` in `needs`, so it started ~14
# minutes before `ha-integration-tests` finished. The download failed, `continue-on-error: true`
# swallowed it, and the merged report went to Codecov without the ha-raft module - which Codecov
# read as 1,110 covered lines vanishing, and reported as a coverage regression on a PR that had
# touched 29 lines somewhere else entirely. Every symptom pointed at the diff; the cause was a
# missing entry in a `needs` list.
#
# Worse, it was a race, not a constant: when the other three jobs happened to finish late the
# artifact was there and the report was complete. So the check flipped between correct and wrong
# on identical config, which is exactly the shape of bug that survives code review.
#
# This script makes the ordering an enforced invariant instead of a convention: for every
# `download-artifact` step it resolves which job uploads that artifact and asserts the producer is
# in the consumer's transitive `needs` closure.
#
# Usage:
#   check-workflow-artifact-deps.py                 # check .github/workflows
#   check-workflow-artifact-deps.py FILE|DIR ...    # check the given workflows
#
# @author Luca Garulli (l.garulli@arcadedata.com)

import fnmatch
import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - the workflow installs it explicitly
    sys.exit("check-workflow-artifact-deps: PyYAML is required (pip install pyyaml)")

UPLOAD_ACTION = "actions/upload-artifact"
DOWNLOAD_ACTION = "actions/download-artifact"

# upload-artifact's documented default when `name` is omitted.
DEFAULT_ARTIFACT_NAME = "artifact"

EXPRESSION = re.compile(r"\$\{\{.*?\}\}")


# A matrix upload names its artifact `bolt-matrix-java-${{ matrix.version }}`; statically that is
# `bolt-matrix-java-*`. Keeping it as a glob is what lets the check still resolve the consumer that
# picks the whole family up with `pattern: bolt-matrix-*`, instead of reporting a family of
# artifacts nobody uploads.
def as_glob(value):
    """Collapse a run-time expression into the glob of names it can produce."""
    return EXPRESSION.sub("*", value)


def action_of(step):
    uses = step.get("uses")
    return uses.split("@", 1)[0].strip() if isinstance(uses, str) else ""


def steps_of(job):
    steps = job.get("steps")
    return [s for s in steps if isinstance(s, dict)] if isinstance(steps, list) else []


def needs_of(job):
    needs = job.get("needs")
    if isinstance(needs, str):
        return [needs]
    return [n for n in needs if isinstance(n, str)] if isinstance(needs, list) else []


def needs_closure(jobs, name, seen=None):
    """Every job that must finish before `name` starts, directly or transitively."""
    seen = set() if seen is None else seen
    for parent in needs_of(jobs.get(name) or {}):
        if parent not in seen:
            seen.add(parent)
            needs_closure(jobs, parent, seen)
    return seen


def producers_of(jobs):
    """Artifact glob -> the jobs that upload a name matching it."""
    produced = {}
    for job_name, job in jobs.items():
        for step in steps_of(job):
            if action_of(step) != UPLOAD_ACTION:
                continue
            with_ = step.get("with") or {}
            name = str(with_.get("name", DEFAULT_ARTIFACT_NAME))
            produced.setdefault(as_glob(name), set()).add(job_name)
    return produced


# `name` selects one artifact, `pattern` selects every artifact matching a glob, and neither means
# "download everything in the run" - which depends on every producing job. Both sides can be globs
# (a matrix uploader against a family-wide consumer), so they are matched in either direction:
# overlap in the names they can denote is enough to create the ordering requirement.
def matching_producers(produced, name, pattern):
    """Resolve a download step's selector to the set of jobs it reads from."""
    if name is None and pattern is None:
        return {job for jobs in produced.values() for job in jobs}

    selector = name if name is not None else pattern
    matched = set()
    for artifact, jobs in produced.items():
        if fnmatch.fnmatch(artifact, selector) or fnmatch.fnmatch(selector, artifact):
            matched |= jobs
    return matched


def check_workflow(path):
    """Return the list of violation strings found in one workflow file."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        return ["%s: cannot parse: %s" % (path, error)]

    if not isinstance(document, dict):
        return []
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        return []
    jobs = {name: job for name, job in jobs.items() if isinstance(job, dict)}

    produced = producers_of(jobs)
    violations = []

    for job_name, job in jobs.items():
        closure = None
        for step in steps_of(job):
            if action_of(step) != DOWNLOAD_ACTION:
                continue
            with_ = step.get("with") or {}

            # A cross-run download reads another run's store, so this run's ordering says nothing.
            if with_.get("run-id") is not None:
                continue

            # A selector is matched as the glob of names it can denote, so a matrix-interpolated
            # one still resolves. One that collapses to a bare "*" denotes nothing in particular,
            # and demanding every producer on the strength of it would be a guess, not a finding.
            name = as_glob(str(with_["name"])) if with_.get("name") is not None else None
            pattern = as_glob(str(with_["pattern"])) if with_.get("pattern") is not None else None
            if name == "*" or pattern == "*":
                continue

            selector = (
                "name: %s" % name
                if name is not None
                else "pattern: %s" % pattern if pattern is not None else "every artifact in the run"
            )
            step_name = step.get("name") or selector

            uploaders = matching_producers(produced, name, pattern)
            if not uploaders:
                violations.append(
                    "%s: job '%s', step '%s' downloads '%s', which no job in this workflow uploads"
                    % (path, job_name, step_name, selector)
                )
                continue

            if closure is None:
                closure = needs_closure(jobs, job_name)
            # A job reading back its own upload is ordered by step order, not by `needs`.
            missing = sorted(uploaders - closure - {job_name})
            if missing:
                violations.append(
                    "%s: job '%s', step '%s' downloads '%s' produced by %s, "
                    "but %s %s not in its 'needs' closure %s\n"
                    "    -> the download races the upload: add %s to '%s.needs'"
                    % (
                        path,
                        job_name,
                        step_name,
                        selector,
                        sorted(uploaders),
                        missing,
                        "is" if len(missing) == 1 else "are",
                        sorted(closure) or "[]",
                        missing,
                        job_name,
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
            sys.exit("check-workflow-artifact-deps: no such file or directory: %s" % target)
    return files


def main(arguments):
    files = workflow_files(arguments)
    violations = [v for path in files for v in check_workflow(path)]

    if violations:
        print("Artifact downloads that are not ordered after their upload:\n", file=sys.stderr)
        for violation in violations:
            print("  %s\n" % violation, file=sys.stderr)
        print(
            "A job only waits for what its 'needs' lists. Downloading an artifact from a job that\n"
            "is not in that list is a race: it usually resolves one way on a fast run and the other\n"
            "way on a slow one, so the job goes green on incomplete input.",
            file=sys.stderr,
        )
        return 1

    print("check-workflow-artifact-deps: %d workflow(s) checked, no unordered downloads" % len(files))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
