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
# Asserts that every `needs.<job>.outputs.<name>` expression names an output the referenced job
# actually declares.
#
# GitHub Actions does not validate this. A reference to an output that no job declares is accepted
# as valid YAML and silently evaluates to the empty string at runtime, so the mistake produces a
# workflow that looks correct, parses correctly, and quietly passes nothing where a value was meant
# to go.
#
# That is not hypothetical here. `build-and-package` had no `outputs:` block at all while SEVEN jobs
# passed `needs.build-and-package.outputs.image-tag` as ARCADEDB_DOCKER_IMAGE. Every one of them
# received "". It survived for as long as it did because the consumers were each defensive in a way
# that hid it: e2e-go guards `!= ""`, js-bolt-conformance falls back with `||`, and two e2e-js
# suites hardcode `arcadedata/arcadedb:latest` and never read the variable. Since the saved image
# artifact carries only the `:latest` tag, those fallbacks all landed on the right image - so the
# suites tested the correct thing by luck, and the documented intent (e2e-go's README: "the
# branch-built image is docker loaded under the tag passed via ARCADEDB_DOCKER_IMAGE") had never
# actually held. It took a newly added consumer, strict enough to pass "" straight through to
# Docker, to surface it as `invalid reference format`.
#
# This is the same family as check-workflow-artifact-deps.py: valid-looking YAML that is silently
# wrong at runtime, where the failure mode is indistinguishable from working.
#
# Known boundaries, both deliberate:
#   - A job that delegates to a reusable workflow (`jobs.<id>.uses:`) declares its outputs inside
#     the called workflow, which this script does not follow. References INTO such a job are
#     skipped rather than reported, because this check gates every other job and a false violation
#     here is expensive. Nothing in this repository needs it yet.
#   - A `needs` reference to a job that does not exist at all is reported: that is never valid, and
#     it is the other half of the same typo.
#
# Usage:
#   check-workflow-needs-outputs.py                 # check .github/workflows
#   check-workflow-needs-outputs.py FILE|DIR ...    # check the given workflows
#
# @author Luca Garulli (l.garulli@arcadedata.com)

import re
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - the workflow installs it explicitly
    sys.exit("check-workflow-needs-outputs: PyYAML is required (pip install pyyaml)")

# `needs.<job>.outputs.<name>`, as it appears inside a ${{ }} expression. Job ids and output names
# are both restricted to the characters GitHub allows, so this does not need to parse expressions.
NEEDS_OUTPUT = re.compile(r"needs\.([A-Za-z_][A-Za-z0-9_-]*)\.outputs\.([A-Za-z_][A-Za-z0-9_-]*)")

# Only text inside ${{ }} is an expression. Without this gate a `run:` step that merely PRINTS the
# words needs.build.outputs.tag - an echo, a comment, an error message - would be read as a live
# reference and reported as a violation. Same gate the sibling artifact-deps script applies.
EXPRESSION = re.compile(r"\$\{\{.*?\}\}")


def workflow_files(argv):
    """Every workflow to check: the arguments if given, otherwise .github/workflows."""
    targets = [Path(a) for a in argv] or [Path(".github/workflows")]
    files = []
    for target in targets:
        if target.is_dir():
            files.extend(sorted(p for p in target.iterdir() if p.suffix in (".yml", ".yaml")))
        else:
            files.append(target)
    return files


def declared_outputs(job):
    """The output names a job declares, or None when the job cannot be resolved from here.

    None and an empty set mean different things: None is a reusable-workflow job whose outputs live
    somewhere this script does not read, and a reference into it is not evidence of a mistake. An
    empty set is a job that genuinely declares nothing, and any reference into it is a defect.
    """
    if not isinstance(job, dict):
        return None
    if "uses" in job:
        return None
    outputs = job.get("outputs")
    if outputs is None:
        return set()
    if not isinstance(outputs, dict):
        return set()
    return set(outputs.keys())


def needs_of(job):
    """The jobs a job lists as DIRECT dependencies.

    Direct, not transitive, and the distinction is the whole point. An artifact survives a
    transitive wait - it is uploaded to the run - which is why the sibling artifact-deps script
    resolves a full `needs` closure. The `needs` CONTEXT does not work that way: it carries "the
    outputs of all jobs that are defined as a dependency of the current job", so a job that reaches
    the producer only transitively reads an empty string, exactly like a job that never declared the
    dependency at all. Using a transitive closure here would accept precisely the reference this
    script exists to reject.
    """
    if not isinstance(job, dict):
        return []
    needs = job.get("needs")
    if isinstance(needs, str):
        return [needs]
    return [n for n in needs if isinstance(n, str)] if isinstance(needs, list) else []


def references(node, path="", found=None):
    """Every (job, output, where) triple reachable from a parsed workflow node."""
    if found is None:
        found = []
    if isinstance(node, str):
        for expression in EXPRESSION.findall(node):
            for job, output in NEEDS_OUTPUT.findall(expression):
                found.append((job, output, path))
    elif isinstance(node, dict):
        for key, value in node.items():
            references(value, f"{path}.{key}" if path else str(key), found)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            references(value, f"{path}[{index}]", found)
    return found


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

    violations = []
    for job_id, job in jobs.items():
        direct_needs = set(needs_of(job))
        # One line per distinct mistake. The same bad reference often appears twice in a job - once
        # in `env:` and again in a `run:` - and reporting it twice makes one typo look like two.
        reported = set()

        for target, output, where in references(job):
            if (target, output) in reported:
                continue

            if target not in jobs:
                reported.add((target, output))
                violations.append(
                    f"{path}: job '{job_id}' references needs.{target}.outputs.{output} at "
                    f"'{where}', but no job '{target}' exists in this workflow"
                )
                continue

            if target not in direct_needs:
                reported.add((target, output))
                declared = ", ".join(sorted(direct_needs)) if direct_needs else "nothing"
                violations.append(
                    f"{path}: job '{job_id}' references needs.{target}.outputs.{output} at "
                    f"'{where}', but '{job_id}' does not list '{target}' in its needs (needs: "
                    f"{declared}). The needs context carries only DIRECT dependencies, so this "
                    f"resolves to an empty string at runtime instead of failing."
                )
                continue

            available = declared_outputs(jobs[target])
            if available is None:
                # A reusable-workflow job: its outputs are declared in the called workflow.
                continue
            if output not in available:
                reported.add((target, output))
                declared = ", ".join(sorted(available)) if available else "none"
                violations.append(
                    f"{path}: job '{job_id}' references needs.{target}.outputs.{output} at "
                    f"'{where}', but job '{target}' declares no such output (declares: {declared}). "
                    f"GitHub Actions resolves this to an empty string at runtime instead of failing."
                )
    return violations


def main():
    violations = []
    for path in workflow_files(sys.argv[1:]):
        violations.extend(check(path))

    if violations:
        print("Undeclared workflow outputs referenced through `needs`:\n")
        for violation in violations:
            print(f"  {violation}")
        print(
            "\nEach of these evaluates to an empty string at runtime. Declare the output on the "
            "producing job, or stop referencing it."
        )
        return 1

    print("check-workflow-needs-outputs: every needs.<job>.outputs.<name> reference resolves")
    return 0


if __name__ == "__main__":
    sys.exit(main())
