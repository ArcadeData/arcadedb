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

"""Keeps a release workflow from publishing an unsigned image or from signing after it has tagged.

Rules, applied per job (output: `<workflow>:<job>:<step>: <rule>`):

  (a) a step that pushes an image (`docker push`, `imagetools create`, `docker buildx build --push`, a `docker/build-push-action` with
      `push: true`, or `./mvnw` with `-Pdocker` and
      `deploy`) must be followed, later in the SAME job, by a step running `cosign sign`. Pushes that can
      never run on a release are listed in NIGHTLY_ONLY_PUSHES by workflow, job and step name; it is empty
      on purpose: native-image.yml pushes for releases and nightlies from the same step, so the sign step
      (not the push) is what is gated to releases.
      The one alternative is SIGNED_IN_LATER_JOB: a push job whose images are signed by ANOTHER job, because
      the push job runs build code and must not hold `id-token: write`. The mapping is verified, not trusted:
      the signing job must exist, list the push job in its own `needs` (directly, so it cannot run first or
      without it), be gated by a job-level `if:` that is EXACTLY RELEASE_GATE (whitespace-normalized, `${{ }}`
      stripped; containing it is not enough, `always() || <gate>` runs everywhere), have no job-level
      `continue-on-error`, and contain an EFFECTIVE `cosign sign` step.
      A `cosign sign` step is effective (counts as signing, here and in the same job) only when it has no
      `continue-on-error` and its `if:` is absent or exactly RELEASE_GATE: `if: false` skips it, and
      `continue-on-error: true` lets the run go on after it failed.
      In a workflow listed in RELEASE_ONLY_WORKFLOWS the whole run is a release, so its signing job carries no
      job-level `if:` at all; that is accepted only while the workflow's ONLY trigger is `workflow_dispatch`, and
      any `if:` there is rejected (it could only narrow or widen a gate the workflow does not need).
  (b) no step that tags the release (`git tag`, `git push origin "${RELEASE_VERSION}"`) may precede a
      `cosign sign` step in the same job: a signing failure after the tag leaves a tagged, unsigned release.
      Across jobs: a job that tags must need (directly or through other jobs) every job of the workflow that
      runs `cosign sign`, so it cannot tag before or without the signing, and its job-level `if:` must not
      contain `always()`, `failure()` or `cancelled()` (which run it after the signing job failed).
  (c) in a workflow with an `attach_to_release` input, a job that runs `cosign` or
      `attest-build-provenance` must contain a step that fails the run off main, BEFORE the first signing step, so
      a dispatch from a branch cannot sign: its `if:` tests `github.ref != 'refs/heads/main'`, or its `run:`
      compares the ref (`github.ref` / `$GITHUB_REF`) with `!=` to refs/heads/main; its `run:` exits non-zero
      (`exit 1`; a `::error::` annotation alone does not fail the step); `#` comment lines count for nothing; and
      it has no `continue-on-error`. An `== main` test fires ON main and is no guard. Expression function names
      and the release gate are compared case-insensitively, as GitHub evaluates them.

Usage:
  check-release-signing.py [workflow ...]   # defaults to mvn-release.yml and native-image.yml
  Exit: 0 clean, 1 violations, 2 usage error.
"""

import re
import sys
from pathlib import Path

import yaml

DEFAULT_WORKFLOWS = ("mvn-release.yml", "native-image.yml")

# (workflow file name, job id, step name) of pushes that can never run on a release.
NIGHTLY_ONLY_PUSHES = set()

# (workflow file name, push job id) -> id of the job that signs what the push job pushed. native-image.yml's
# `docker` and `manifest` run on every nightly too, and `docker` runs the e2e battery: an `id-token: write`
# there would let build or test code mint a certificate with the release signer identity on any run from
# main. So they push, and release-only jobs that run no build code sign.
RELEASE_GATE = "github.event_name == 'release' || inputs.attach_to_release"


def normalize_condition(condition):
    """An `if:` expression without its `${{ }}` wrapper and with its whitespace collapsed."""
    condition = str(condition or "").strip()
    match = re.fullmatch(r"\$\{\{(.*)\}\}", condition, re.S)
    if match:
        condition = match.group(1)
    return " ".join(condition.split())


SIGNED_IN_LATER_JOB = {
    ("native-image.yml", "docker"): "sign-native-images",
    ("native-image.yml", "manifest"): "sign-native-manifest",
    # mvn-release.yml's `docker` runs the Maven build that pushes the JVM image, so it holds no `id-token` either.
    ("mvn-release.yml", "docker"): "sign-image",
}

# Workflows that only ever run as a release (manual dispatch of mvn-release.yml): their signing jobs need no gate.
RELEASE_ONLY_WORKFLOWS = {"mvn-release.yml"}

PUSH = re.compile(r"docker\s+push|imagetools\s+create")
MVN_DOCKER_DEPLOY = re.compile(r"-P\S*docker")
DEPLOY = re.compile(r"\bdeploy\b")
SIGN = re.compile(r"cosign\s+sign\b")
TAG = re.compile(r"git\s+tag\b|git\s+push\s+origin\s+\"?\$\{?RELEASE_VERSION\}?\"?")
SIGNING_TOOL = re.compile(r"\bcosign\b|attest-build-provenance")
NONZERO_EXIT = re.compile(r"\bexit\s+[1-9][0-9]*\b")
# Job-level conditions that run a job after a job it needs failed or was cancelled.
# GitHub expression function names are case-insensitive (`Always()` runs like `always()`).
BYPASSES_NEEDS = re.compile(r"\b(?:always|failure|cancelled)\s*\(\s*\)", re.I)
# The off-main test of a guard: `github.ref != 'refs/heads/main'` in an `if:` (either operand order).
OFF_MAIN_IF = re.compile(r"github\.ref\s*!=\s*'refs/heads/main'|'refs/heads/main'\s*!=\s*github\.ref\b", re.I)
# The same in a script: the ref (as the expression or $GITHUB_REF) compared with `!=` to refs/heads/main on one line.
OFF_MAIN_RUN = re.compile(
    r"(?:github\.ref|\$\{?GITHUB_REF)\b[^\n!=]*!=\s*[\"']?refs/heads/main\b"
    r"|refs/heads/main[\"']?\s*!=\s*[\"']?(?:\$\{\{\s*github\.ref|\$\{?GITHUB_REF\b)", re.I)


BUILD_PUSH = re.compile(r"docker\s+(?:buildx\s+)?build\b[^\n;&|]*--push\b")


def pushes_image(step):
    """True when the step pushes an image (docker push, imagetools create, buildx --push, build-push-action, mvn -Pdocker deploy)."""
    uses = step.get("uses")
    if isinstance(uses, str) and uses.startswith("docker/build-push-action@"):
        return str((step.get("with") or {}).get("push", "")).strip().lower() == "true"
    run = step.get("run")
    if not isinstance(run, str):
        return False
    run = re.sub(r"\\\n", " ", run)  # fold continuations so flags on later lines belong to the command
    return bool(PUSH.search(run) or BUILD_PUSH.search(run)) or (
        "./mvnw" in run and bool(MVN_DOCKER_DEPLOY.search(run)) and bool(DEPLOY.search(run))
    )


def step_text(step):
    """The run script plus the `uses:` reference, for tools invoked as an action."""
    return f"{step.get('run') or ''}\n{step.get('uses') or ''}"


def continues_on_error(entry):
    """True unless `continue-on-error` is absent or literally false: an expression may evaluate to true."""
    value = entry.get("continue-on-error", False)
    return value is not False and str(value).strip().lower() != "false"


def is_release_gate(condition):
    """True when the `if:` is exactly RELEASE_GATE, ignoring `${{ }}`, whitespace and case (GitHub ignores case)."""
    return normalize_condition(condition).lower() == RELEASE_GATE.lower()


def is_effective_signer(step):
    """A `cosign sign` step that runs on a release and whose failure fails the job."""
    if not SIGN.search(step_text(step)) or continues_on_error(step):
        return False
    return "if" not in step or is_release_gate(step.get("if"))


def code_lines(run):
    """The script without its `#` comment lines, so a commented-out test or exit counts for nothing."""
    return "\n".join(line for line in str(run or "").splitlines() if not line.lstrip().startswith("#"))


def is_main_guard(step):
    """A step that fires OFF main and fails the run there.

    Either its `if:` tests `github.ref != 'refs/heads/main'`, or (with no such `if:`) its script compares the ref
    with `!=` to refs/heads/main; in both cases the script exits non-zero, and the step has no `continue-on-error`.
    An `== main` test (which fires ON main) or a test only inside a comment does not count.
    """
    if continues_on_error(step):
        return False
    code = code_lines(step.get("run"))
    if not NONZERO_EXIT.search(code):
        return False
    return bool(OFF_MAIN_IF.search(normalize_condition(step.get("if"))) or OFF_MAIN_RUN.search(code))


def has_attach_input(document):
    """True when the workflow has a workflow_dispatch input named attach_to_release."""
    triggers = document.get("on", document.get(True)) or {}
    if not isinstance(triggers, dict):
        return False
    dispatch = triggers.get("workflow_dispatch") or {}
    inputs = dispatch.get("inputs") if isinstance(dispatch, dict) else None
    return isinstance(inputs, dict) and "attach_to_release" in inputs


def needs_of(job):
    """The job's `needs` as a list, whether it was written as a string or a list."""
    needs = job.get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def triggers_of(document):
    """The names of the events that trigger the workflow."""
    triggers = document.get("on", document.get(True))
    if isinstance(triggers, str):
        return {triggers}
    if isinstance(triggers, list):
        return set(triggers)
    if isinstance(triggers, dict):
        return set(triggers)
    return set()


def later_job_problem(name, job_id, jobs, document):
    """None when SIGNED_IN_LATER_JOB names a valid signing job for this push job, else why not."""
    signer_id = SIGNED_IN_LATER_JOB.get((name, job_id))
    if signer_id is None:
        return "image push with no later effective `cosign sign` step in the job (none, skipped by its `if:`, or with `continue-on-error`)"
    signer = jobs.get(signer_id)
    if not isinstance(signer, dict):
        return f"SIGNED_IN_LATER_JOB names `{signer_id}`, which does not exist"
    if job_id not in needs_of(signer):
        return f"SIGNED_IN_LATER_JOB names `{signer_id}`, which does not need `{job_id}`"
    if name in RELEASE_ONLY_WORKFLOWS:
        if triggers_of(document) != {"workflow_dispatch"}:
            return f"SIGNED_IN_LATER_JOB names `{signer_id}` with no release gate, but {name} has a trigger other than workflow_dispatch"
        if "if" in signer:
            return f"SIGNED_IN_LATER_JOB names `{signer_id}`, which must have no job-level `if:` in the release-only {name}"
    elif not is_release_gate(signer.get("if")):
        return f"SIGNED_IN_LATER_JOB names `{signer_id}`, which is not gated by exactly `{RELEASE_GATE}`"
    if continues_on_error(signer):
        return f"SIGNED_IN_LATER_JOB names `{signer_id}`, which has a job-level `continue-on-error`"
    if not any(isinstance(s, dict) and is_effective_signer(s) for s in signer.get("steps") or []):
        return f"SIGNED_IN_LATER_JOB names `{signer_id}`, which has no effective `cosign sign` step (one with no `continue-on-error` and no `if:` but the release gate)"
    return None


def ancestors(job_id, jobs):
    """Every job `job_id` needs, directly or through other jobs."""
    seen, todo = set(), list(needs_of(jobs.get(job_id) or {}))
    while todo:
        j = todo.pop()
        if j in seen:
            continue
        seen.add(j)
        if isinstance(jobs.get(j), dict):
            todo.extend(needs_of(jobs[j]))
    return seen


def check(workflow, document):
    """Every rule violation of one workflow document, as `<workflow>:<job>:<step>: <rule>` lines."""
    name = workflow.name
    violations = []
    attach = has_attach_input(document)
    jobs = document.get("jobs") or {}
    signing_jobs = [
        j for j, job in jobs.items()
        if isinstance(job, dict) and any(isinstance(s, dict) and SIGN.search(step_text(s)) for s in job.get("steps") or [])
    ]
    for job_id, job in jobs.items():
        if not isinstance(job, dict):
            continue
        steps = [s for s in (job.get("steps") or []) if isinstance(s, dict)]
        label = lambda s: f"{name}:{job_id}:{s.get('name') or '<step %d>' % (steps.index(s) + 1)}"
        sign_indexes = [i for i, s in enumerate(steps) if SIGN.search(step_text(s))]
        effective_sign_indexes = [i for i, s in enumerate(steps) if is_effective_signer(s)]
        for i, s in enumerate(steps):
            run = s.get("run")
            if pushes_image(s) and (name, job_id, s.get("name")) not in NIGHTLY_ONLY_PUSHES:
                if not any(j > i for j in effective_sign_indexes):
                    problem = later_job_problem(name, job_id, jobs, document)
                    if problem:
                        violations.append(f"{label(s)}: (a) {problem}")
            if isinstance(run, str) and TAG.search(run) and any(j > i for j in sign_indexes):
                violations.append(f"{label(s)}: (b) release tag precedes a `cosign sign` step")
            if isinstance(run, str) and TAG.search(run):
                if signing_jobs and BYPASSES_NEEDS.search(normalize_condition(job.get("if"))):
                    violations.append(f"{label(s)}: (b) tags the release in a job whose `if:` runs it after the signing failed")
                for signing_job in signing_jobs:
                    if signing_job != job_id and signing_job not in ancestors(job_id, jobs):
                        violations.append(f"{label(s)}: (b) tags the release without needing `{signing_job}`, which signs the image")
        signers = [s for s in steps if SIGNING_TOOL.search(step_text(s))]
        if attach and signers:
            first_signer = steps.index(signers[0])
            if not any(is_main_guard(s) for s in steps[:first_signer]):
                violations.append(f"{label(signers[0])}: (c) signs in a workflow with attach_to_release but has no github.ref == refs/heads/main guard step BEFORE the first signing step")
    return violations


def main(argv):
    """Checks the workflows named on the command line (or the defaults); returns the exit status."""
    root = Path(__file__).resolve().parents[2]
    workflows = [Path(a) for a in argv[1:]] or [root / ".github" / "workflows" / n for n in DEFAULT_WORKFLOWS]
    violations = []
    for workflow in workflows:
        if not workflow.is_file():
            print(f"{workflow}: not found")
            return 2
        try:
            document = yaml.safe_load(workflow.read_text())
        except yaml.YAMLError as e:
            print(f"{workflow}: not parseable as YAML ({e})")
            return 2
        if isinstance(document, dict):
            violations.extend(check(workflow, document))
    if violations:
        print("\n".join(violations))
        return 1
    print(f"release signing is enforced in {len(workflows)} workflow(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
