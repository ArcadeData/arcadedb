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

"""Fails when a workflow runs `mvn deploy` with setup-java's server credentials but without
`-Dmaven.repository.credentialScope=id`.

Maven 3.10 matches a `<server>`'s credentials to a repository by ORIGIN, not only by id, and the id
`central` is pre-bound to Maven Central's own origin (https://repo.maven.apache.org). The snapshot
upload goes to https://central.sonatype.com/repository/maven-snapshots/ under that same id, so Maven
logs

    Not using credentials of server 'central' for repository https://central.sonatype.com/...:
    the origins declared for this id are [https://repo.maven.apache.org]

and sends the upload unauthenticated. Sonatype answers HTTP 401. That is #10023: every
`Java CI - deploy` run on main failed from 2026-10-08, the day the hosted runner image moved from
Maven 3.9.16 to 3.10.0, and no snapshot was published for a week. Nothing in the repository changed,
and the token was fine - it simply was never sent.

`-Dmaven.repository.credentialScope=id` restores the id-only matching. Maven 3.9 ignores it, so the
flag is safe on either runner image. It may be given on the command line or through `MAVEN_ARGS`,
which Maven reads itself.

The check applies to a job that wires server credentials through setup-java's `server-id` (that is
what writes the `<server>` into settings.xml) and to every `mvn`/`mvnw` invocation in it that runs
the `deploy` goal. The `-Pdeploy` PROFILE is not the goal and does not count.

Limits: a run script is split into commands on newlines, `;`, `&` and `|` after folding `\`
continuations, and only the first `mvn`/`mvnw` word of each command is examined. A deploy hidden
inside `$(...)`, a here-doc or `bash -c "..."` is NOT seen, nor is the flag set through
`.mvn/maven.config` or `MAVEN_OPTS`. Every deploy in this repository's workflows is a plain command
in a step's `run:`; keep it that way, or extend this check alongside.

Usage:
  check-deploy-credential-scope.py [workflow ...]   # defaults to .github/workflows/*.yml

@author Roberto Franchini (r.franchini@arcadedata.com)
"""

import re
import shlex
import sys
from pathlib import Path

import yaml

FLAG = "-Dmaven.repository.credentialScope=id"

MAVEN_EXECUTABLES = {"mvn", "mvnw"}

# `${{ ... }}` can hold quotes that shlex would mis-tokenize; its value is not knowable here anyway.
GHA_EXPRESSION = re.compile(r"\$\{\{.*?\}\}", re.DOTALL)
EXPRESSION_PLACEHOLDER = "GHA_EXPRESSION"


def env_sets_flag(*envs):
    """True when any of the given `env:` blocks puts the flag in MAVEN_ARGS."""
    for env in envs:
        if isinstance(env, dict) and FLAG in str(env.get("MAVEN_ARGS", "")).split():
            return True
    return False


def declares_server(job):
    """True when a setup-java step in the job writes a `<server>` into settings.xml."""
    for step in job.get("steps") or []:
        if not isinstance(step, dict):
            continue
        if "actions/setup-java" in str(step.get("uses", "")) and (step.get("with") or {}).get("server-id"):
            return True
    return False


def deploy_invocations(script):
    """Yields (words, whole invocation) for every Maven invocation in a run script that runs `deploy`."""
    folded = re.sub(r"\\\n", " ", GHA_EXPRESSION.sub(EXPRESSION_PLACEHOLDER, script))
    for command in re.split(r"[\n;&|]+", folded):
        if "mvn" not in command:
            continue
        try:
            words = shlex.split(command, comments=True)
        except ValueError:
            words = command.split()
        for start, word in enumerate(words):
            if Path(word).name not in MAVEN_EXECUTABLES:
                continue
            arguments = words[start + 1 :]
            if any(a == "deploy" or a.startswith("deploy:") for a in arguments):
                yield arguments, " ".join(command.split())
            break


def main(argv):
    root = Path(__file__).resolve().parents[2]
    workflows = [Path(a) for a in argv[1:]] or sorted((root / ".github" / "workflows").glob("*.yml"))

    if not workflows:
        print("no workflow files to check")
        return 1

    problems = []
    checked = 0

    for workflow in workflows:
        if not workflow.is_file():
            print(f"{workflow}: not found")
            return 1
        try:
            document = yaml.safe_load(workflow.read_text())
        except yaml.YAMLError as e:
            print(f"{workflow}: not parseable as YAML ({e})")
            return 1
        if not isinstance(document, dict):
            continue

        for job_id, job in (document.get("jobs") or {}).items():
            if not isinstance(job, dict) or not declares_server(job):
                continue
            for step in job.get("steps") or []:
                if not isinstance(step, dict) or not isinstance(step.get("run"), str):
                    continue
                scoped_by_env = env_sets_flag(document.get("env"), job.get("env"), step.get("env"))
                for arguments, command in deploy_invocations(step["run"]):
                    checked += 1
                    if scoped_by_env or FLAG in arguments:
                        continue
                    problems.append(
                        f"{workflow}: job '{job_id}', step '{step.get('name', '<unnamed step>')}' deploys "
                        f"without {FLAG}:\n    {command}"
                    )

    if problems:
        for problem in problems:
            print(problem)
        print()
        print("Maven 3.10 sends the credentials of server 'central' only to Maven Central's own origin")
        print("(https://repo.maven.apache.org), so an upload to central.sonatype.com under that id goes out")
        print("unauthenticated and fails with HTTP 401 - every snapshot deploy from 2026-10-08 did (#10023).")
        print(f"Add {FLAG} to the command, or to MAVEN_ARGS in the step's env.")
        return 1

    print(f"deploy credential scope is explicit: {checked} deploy invocation(s) across {len(workflows)} workflow(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
