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

"""Fails when a workflow step runs Maven as anything but the repository's Maven wrapper (`./mvnw`).

`.mvn/wrapper/maven-wrapper.properties` pins the Maven version the build is reviewed against. The
runner image's `mvn` is whatever version GitHub installed that week, so a step that calls it picks up
a Maven upgrade with no change in this repository and no review. That has broken publishing twice:

- 2026-10-08 the hosted runner image moved from Maven 3.9.16 to 3.10.0 and every `Java CI - deploy`
  snapshot upload failed with HTTP 401 for a week (#10023);
- a week later the same runner Maven made central-publishing-maven-plugin put local repository
  metadata into the release bundle, and the Central Portal rejected the 26.10.2 release (#10111).

#10111 moved the release workflow to `./mvnw`; #10134 moved every other step and added this check, so
a step written later cannot reintroduce the runner's Maven either.

Flagged: a command whose program is `mvn`, `mvn.cmd` or the Maven daemon (`mvnd`), by bare name or by
path. Accepted: `mvnw` / `mvnw.cmd` by any path (`./mvnw`, `../mvnw`). Each `run:` of a workflow
step or of a composite action step is checked, and so is every shell script of this repository that a
step runs as a command (`./test-builder-local.sh` under `working-directory: package`), resolved against
the step's or the job's working directory - package/test-builder-local.sh ran the runner's mvn that way.

How a script is read: `\\` continuations are folded, every `$(...)` and backtick command substitution
is checked as a script of its own, and the rest is tokenized like a shell (quotes respected) and split
into commands on newlines, `;`, `&`, `|`, `(` and `)`. A command's program is its first word after any
`NAME=value` assignments and command prefixes such as `time`, `sudo`, `then` or `do`. Words that only
mention Maven (an `echo "mvn ..."`, a comment, a file name such as `mvn-deploy.yml`) are not programs
and are not flagged.

Limits: Maven reached through `bash -c "..."`, `eval`, a here-doc, or a script the step runs through an
interpreter (`bash build.sh`) or by a path that holds a variable, is NOT seen. Every Maven call in this repository's workflows is a plain command in a step's `run:`;
keep it that way, or extend this check alongside.

Usage:
  check-maven-wrapper.py [workflow-or-action.yml ...]
      # defaults to .github/workflows/*.yml|*.yaml and .github/actions/**/action.yml|action.yaml

@author Roberto Franchini (r.franchini@arcadedata.com)
"""

import re
import shlex
import sys
from pathlib import Path

import yaml

RUNNER_MAVEN = {"mvn", "mvn.cmd", "mvnd", "mvnd.cmd", "mvnd.sh"}
WRAPPER = {"mvnw", "mvnw.cmd"}

# Words that run the NEXT word as the command, or that open a compound command whose body follows.
COMMAND_PREFIXES = {
    "sudo", "time", "exec", "command", "nohup", "env", "xargs", "builtin",
    "if", "then", "elif", "else", "while", "until", "do", "!", "{",
}

SEPARATOR_CHARS = set(";&|()")
ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")

# `${{ ... }}` can hold quotes that would mis-tokenize the command; its value is not knowable here anyway.
GHA_EXPRESSION = re.compile(r"\$\{\{.*?\}\}", re.DOTALL)
EXPRESSION_PLACEHOLDER = "GHA_EXPRESSION"


def closing_paren(script, start):
    """Index just past the `)` that closes the `$(` whose body starts at `start`, skipping quoted text."""
    depth = 1
    i = start
    n = len(script)
    while i < n:
        c = script[i]
        if c == "\\":
            i += 2
            continue
        if c in "'\"":
            end = script.find(c, i + 1)
            i = n if end < 0 else end + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return n


def extract_substitutions(script):
    """Splits a script into (outer text, [inner scripts]) for every `$(...)` and backtick substitution.

    The substitution is replaced by a placeholder word in the outer text, so the command it sits in
    stays one command, and its body is returned to be checked as a script of its own. Comments are
    dropped here, and single-quoted text (`'...'`, `$'...'`) becomes a placeholder word with nothing
    inside it taken as a substitution, as a shell would: a comment or a quoted fixture that mentions
    `$(mvn ...)` runs nothing."""
    inner = []
    out = []
    i = 0
    n = len(script)
    in_double = False
    while i < n:
        c = script[i]
        if c == "\\":
            out.append(script[i : i + 2])
            i += 2
        elif not in_double and (c == "'" or script.startswith("$'", i)):
            # `$'...'` honours backslash escapes, so `\'` does not end it; plain `'...'` has none.
            ansi = c == "$"
            j = i + (2 if ansi else 1)
            while j < n and script[j] != "'":
                j += 2 if ansi and script[j] == "\\" else 1
            # Its text is literal, so it is replaced by a placeholder: that keeps quote forms shlex does
            # not know (`$'it\'s'`) away from the tokenizer.
            out.append("QUOTED")
            i = j + 1
        elif not in_double and c == "#" and (i == 0 or script[i - 1] in " \t\n;&|()"):
            end = script.find("\n", i)
            i = n if end < 0 else end
        elif c == '"':
            in_double = not in_double
            out.append(c)
            i += 1
        elif script.startswith("$(", i) and not script.startswith("$((", i):
            j = closing_paren(script, i + 2)
            inner.append(script[i + 2 : j - 1] if j <= n and script[j - 1 : j] == ")" else script[i + 2 :])
            out.append("SUBSTITUTION")
            i = j
        elif c == "`":
            j = script.find("`", i + 1)
            if j < 0:
                j = n
            inner.append(script[i + 1 : j])
            out.append("SUBSTITUTION")
            i = j + 1
        else:
            out.append(c)
            i += 1
    return "".join(out), inner


def tokenize(script):
    """Shell-like tokens with `;&|()` runs as separate tokens and a `;` at every line end."""
    # The `;` goes AFTER the newline, so a trailing comment, which runs to the end of its line,
    # cannot swallow the command boundary.
    text = script.replace("\n", "\n;")
    try:
        lexer = shlex.shlex(text, posix=True, punctuation_chars=";&|()")
        lexer.whitespace_split = True
        lexer.commenters = "#"
        return list(lexer)
    except ValueError:
        # Unbalanced quotes: fall back to a plain split, which can only over-report.
        tokens = []
        for chunk in re.split(r"([;&|()]+)", text):
            if chunk and set(chunk) <= SEPARATOR_CHARS:
                tokens.append(chunk)
            else:
                tokens.extend(w for w in chunk.split() if not w.startswith("#"))
        return tokens


def programs(script):
    """Yields (program, whole command) for every command in a run script, substitutions included."""
    folded = re.sub(r"\\\r?\n", " ", GHA_EXPRESSION.sub(EXPRESSION_PLACEHOLDER, script))
    outer, inner = extract_substitutions(folded)
    for body in inner:
        yield from programs(body)

    command = []
    for token in tokenize(outer) + [";"]:
        if token and set(token) <= SEPARATOR_CHARS:
            words = command
            command = []
            start = 0
            while start < len(words) and (ASSIGNMENT.match(words[start]) or words[start] in COMMAND_PREFIXES):
                start += 1
            if start < len(words):
                yield words[start], " ".join(words[start:])
        else:
            command.append(token)


def default_working_directory(*holders):
    """The `defaults.run.working-directory` of the innermost holder (job, then workflow) that sets one."""
    for holder in holders:
        run = ((holder or {}).get("defaults") or {}).get("run") if isinstance(holder, dict) else None
        if isinstance(run, dict) and isinstance(run.get("working-directory"), str):
            return run["working-directory"]
    return None


def run_scripts(document):
    """Yields (location, run script, working directory or None) for every step of a workflow's jobs
    or of a composite action."""
    if not isinstance(document, dict):
        return
    step_lists = []
    for job_id, job in (document.get("jobs") or {}).items():
        if isinstance(job, dict):
            step_lists.append((f"job '{job_id}'", job.get("steps"), default_working_directory(job, document)))
    runs = document.get("runs")
    if isinstance(runs, dict):
        step_lists.append(("composite action", runs.get("steps"), None))
    for where, steps, job_directory in step_lists:
        for index, step in enumerate(steps or []):
            if isinstance(step, dict) and isinstance(step.get("run"), str):
                directory = step.get("working-directory")
                yield (
                    f"{where}, step '{step.get('name', f'#{index + 1}')}'",
                    step["run"],
                    directory if isinstance(directory, str) else job_directory,
                )


def repository_script(root, directory, program):
    """The shell script file a command runs, when it is one that exists on disk, else None."""
    if not program.endswith(".sh") or "$" in program:
        return None
    candidate = root / (directory or ".") / program
    return candidate.resolve() if candidate.is_file() else None


def default_files(root):
    github = root / ".github"
    files = sorted((github / "workflows").glob("*.yml")) + sorted((github / "workflows").glob("*.yaml"))
    files += sorted((github / "actions").glob("**/action.yml")) + sorted((github / "actions").glob("**/action.yaml"))
    return files


def main(argv):
    root = Path(__file__).resolve().parents[2]
    files = [Path(a) for a in argv[1:]] or default_files(root)

    if not files:
        print("no workflow files to check")
        return 1

    problems = []
    wrapped = 0

    for path in files:
        if not path.is_file():
            print(f"{path}: not found")
            return 1
        try:
            document = yaml.safe_load(path.read_text())
        except yaml.YAMLError as e:
            print(f"{path}: not parseable as YAML ({e})")
            return 1

        for where, script, directory in run_scripts(document):
            pending = [(where, script, directory)]
            seen = set()
            while pending:
                location, text, cwd = pending.pop()
                for program, command in programs(text):
                    name = Path(program).name
                    if name in WRAPPER:
                        wrapped += 1
                    elif name in RUNNER_MAVEN:
                        problems.append(f"{path}: {location} runs the runner's Maven:\n    {command}")
                    else:
                        called = repository_script(root, cwd, program)
                        if called is not None and called not in seen:
                            seen.add(called)
                            # A script's own relative calls resolve against the directory the step
                            # runs it from, which is the shell's working directory, not the script's.
                            pending.append((f"{location} -> {program}", called.read_text(errors="replace"), cwd))

    if problems:
        for problem in problems:
            print(problem)
        print()
        print("Invoke Maven as ./mvnw. The runner image's mvn changes version under the workflow with no")
        print("review: the move from 3.9.16 to 3.10.0 broke every snapshot deploy (#10023) and then the")
        print("26.10.2 release bundle (#10111). The wrapper pins the version in")
        print(".mvn/wrapper/maven-wrapper.properties, so a Maven upgrade arrives as a reviewable bump.")
        return 1

    print(f"every Maven invocation goes through the Maven wrapper: {wrapped} ./mvnw invocation(s) across {len(files)} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
