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
# Known boundary: it reads a job's `steps`, so a job that delegates to a reusable workflow
# (`jobs.<id>.uses:`) is opaque - uploads and downloads inside the called workflow are neither
# resolved nor checked. Because this check gates every other job, being wrong there would be
# expensive, so a workflow containing such a job stops reporting "no job uploads this": the
# producer may well be one of the steps it cannot see. Ordering violations it *can* see are still
# reported. Following `uses:` into local workflows would lift the restriction; nothing in this
# repository needs it yet.
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
MATRIX_REFERENCE = re.compile(r"\$\{\{\s*matrix\.([A-Za-z0-9_.-]+)\s*\}\}")


# A matrix upload names its artifact `bolt-matrix-java-${{ matrix.version }}`; statically that is
# `bolt-matrix-java-*`. Keeping it as a glob is what lets the check still resolve the consumer that
# picks the whole family up with `pattern: bolt-matrix-*`, instead of reporting a family of
# artifacts nobody uploads. It is only the fallback: a glob denotes more names than the step can
# actually produce, so `names_of` below resolves the matrix outright wherever it can.
def as_glob(value):
    """Collapse a run-time expression into the glob of names it can produce."""
    return EXPRESSION.sub("*", value)


def matrix_values(job, key):
    """The literal values `matrix.<key>` can take in this job, or None if they are not literal."""
    matrix = (job.get("strategy") or {}).get("matrix")
    if not isinstance(matrix, dict):
        return None

    values = set()
    candidates = matrix.get(key)
    candidates = list(candidates) if isinstance(candidates, list) else []
    # `include` entries contribute values of their own, and are the only source for a key that the
    # base matrix does not declare at all - which is how most of this repository's matrices read.
    include = matrix.get("include")
    if isinstance(include, list):
        candidates += [entry[key] for entry in include if isinstance(entry, dict) and key in entry]

    for value in candidates:
        if not isinstance(value, (str, int, float, bool)):
            return None
        values.add(str(value))
    return values or None


# Resolving the matrix matters because a glob denotes names the step cannot produce. `build-*`
# covers `build-logs`, so an unrelated job uploading that literal would be reported as something
# every consumer of it has to wait for - a false violation, in a check that gates the whole build.
# Expanding `${{ matrix.os }}` to exactly {ubuntu, macos} removes the guesswork.
#
# Independent keys are expanded as a cartesian product, which over-approximates an `include`-only
# matrix whose keys are correlated. That errs the safe way: it can only ask for an ordering that is
# already implied, never drop one that is missing.
def names_of(job, value):
    """The exact artifact names `value` can take, or None when it cannot be resolved."""
    names = {value}
    for token, key in {m.group(0): m.group(1) for m in MATRIX_REFERENCE.finditer(value)}.items():
        values = matrix_values(job, key)
        if values is None:
            return None
        names = {name.replace(token, resolved) for name in names for resolved in values}

    # Anything left is an expression over something no static read can resolve: an input, a
    # `fromJSON` matrix, a step output.
    return None if any("${{" in name for name in names) else names


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
    """The (names, glob, job) an upload step can produce, one entry per step."""
    produced = []
    for job_name, job in jobs.items():
        for step in steps_of(job):
            if action_of(step) != UPLOAD_ACTION:
                continue
            with_ = step.get("with") or {}
            name = str(with_.get("name", DEFAULT_ARTIFACT_NAME))
            produced.append((names_of(job, name), as_glob(name), job_name))
    return produced


# Two selectors require an ordering when they can denote the same artifact. Whenever a side is
# resolved to exact names, that side is compared by name and the answer is exact. Only when both
# are globs does this fall back to matching them against each other in either direction, which
# over-matches - the conservative end, since a spurious `needs` entry costs a wait and a missing
# one costs a silently incomplete download.
def overlaps(selector_names, selector_glob, is_pattern, producer_names, producer_glob):
    """True when a download selector and an upload can name the same artifact."""
    if producer_names is not None:
        if selector_names is not None and not is_pattern:
            return bool(selector_names & producer_names)
        return any(fnmatch.fnmatch(name, selector_glob) for name in producer_names)

    if selector_names is not None and not is_pattern:
        return any(fnmatch.fnmatch(name, producer_glob) for name in selector_names)

    return fnmatch.fnmatch(producer_glob, selector_glob) or fnmatch.fnmatch(
        selector_glob, producer_glob
    )


# `name` selects one artifact, `pattern` selects every artifact matching a glob, and neither means
# "download everything in the run" - which depends on every producing job.
def matching_producers(produced, selector_names, selector_glob, is_pattern):
    """Resolve a download step's selector to the set of jobs it reads from."""
    if selector_glob is None:
        return {job for _, _, job in produced}

    return {
        job
        for producer_names, producer_glob, job in produced
        if overlaps(selector_names, selector_glob, is_pattern, producer_names, producer_glob)
    }


def check_workflow(path):
    """Return the (violations, notes) found in one workflow file."""
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as error:
        return ["%s: cannot parse: %s" % (path, error)], []

    if not isinstance(document, dict):
        return [], []
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        return [], []
    jobs = {name: job for name, job in jobs.items() if isinstance(job, dict)}

    produced = producers_of(jobs)
    violations = []
    suppressed = []

    # A job that delegates to a reusable workflow has no steps to read, so an artifact this file
    # never mentions may still be uploaded inside it. See the boundary note in the header.
    opaque = any("uses" in job for job in jobs.values())

    for job_name, job in jobs.items():
        closure = None
        for step in steps_of(job):
            if action_of(step) != DOWNLOAD_ACTION:
                continue
            with_ = step.get("with") or {}

            # A cross-run download reads another run's store, so this run's ordering says nothing
            # about it. `run-id: ${{ github.run_id }}` is the exception: it names this run, so the
            # step is an ordinary consumer and skipping it would quietly disable the check.
            run_id = with_.get("run-id")
            if run_id is not None and "github.run_id" not in str(run_id):
                continue

            # A selector resolves to the exact names it can denote where the matrix allows, and to
            # the glob of them otherwise. One that collapses to a bare "*" denotes nothing in
            # particular, and demanding every producer on the strength of it would be a guess.
            raw = with_.get("name") if with_.get("name") is not None else with_.get("pattern")
            is_pattern = with_.get("name") is None and with_.get("pattern") is not None

            selector_names = selector_glob = None
            if raw is not None:
                selector_glob = as_glob(str(raw))
                if selector_glob == "*":
                    continue
                selector_names = names_of(job, str(raw))

            selector = (
                "%s: %s" % ("pattern" if is_pattern else "name", selector_glob)
                if selector_glob is not None
                else "every artifact in the run"
            )
            step_name = step.get("name") or selector

            uploaders = matching_producers(produced, selector_names, selector_glob, is_pattern)
            if not uploaders:
                message = (
                    "%s: job '%s', step '%s' downloads '%s', which no job in this workflow uploads"
                    % (path, job_name, step_name, selector)
                )
                # Suppressed, not dropped. A typo in an artifact name looks exactly like an
                # artifact a reusable workflow uploads, and this check gates every other job, so
                # the benefit of the doubt goes to the workflow - but silently discarding the
                # finding is how #5701 stayed invisible in the first place.
                (suppressed if opaque else violations).append(message)
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

    notes = (
        [
            "%s: %d missing-producer finding(s) not reported, because a job in this workflow "
            "delegates to a reusable workflow whose steps cannot be read:\n%s"
            % (path, len(suppressed), "\n".join("      %s" % s for s in suppressed))
        ]
        if suppressed
        else []
    )
    return violations, notes


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
    results = [check_workflow(path) for path in files]
    violations = [violation for found, _ in results for violation in found]
    notes = [note for _, suppressed in results for note in suppressed]

    for note in notes:
        print("check-workflow-artifact-deps: %s" % note)

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
